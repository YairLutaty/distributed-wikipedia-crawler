"""Parse stored Wikipedia HTML and publish discovered article links."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from crawler_core import normalize_article_links, retry_delay


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MAX_ATTEMPTS = int(os.getenv("PARSE_MAX_ATTEMPTS", "3"))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def extract_article_links(html: str) -> list[str] | None:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", id="mw-content-text")
    if content is None:
        return None
    hrefs = [link["href"] for link in content.find_all("a", href=True)]
    return normalize_article_links(hrefs)


def parse_fetched(fetched: dict[str, Any]) -> dict[str, Any] | None:
    html = Path(fetched["file_path"]).read_text(encoding="utf-8")
    links = extract_article_links(html)
    if links is None:
        return None
    return {
        "file_id": fetched["file_id"],
        "url": fetched["url"],
        "links": links,
        "parsed_at": utc_now(),
    }


async def run() -> None:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

    consumer = AIOKafkaConsumer(
        "parse_queue",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="parser_group",
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    await consumer.start()
    await producer.start()
    print("Parser waiting for files...", flush=True)

    try:
        async for message in consumer:
            result = None
            last_error: Exception | None = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    result = parse_fetched(message.value)
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt < MAX_ATTEMPTS:
                        await asyncio.sleep(retry_delay(attempt, 0.25, 2.0))

            if last_error is not None or result is None:
                await producer.send_and_wait(
                    "dead_letter_queue",
                    value={
                        "stage": "parse",
                        "input": message.value,
                        "error": (
                            repr(last_error)
                            if last_error
                            else "missing mw-content-text"
                        ),
                        "failed_at": utc_now(),
                    },
                )
            else:
                await producer.send_and_wait("validate_queue", value=result)
                print(f"Parsed/queued: {result['url']}", flush=True)
            await consumer.commit(
                {TopicPartition(message.topic, message.partition): message.offset + 1}
            )
    finally:
        await producer.stop()
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run())
