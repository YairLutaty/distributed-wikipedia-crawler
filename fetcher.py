"""Fetch Wikipedia pages and publish durable parse work."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crawler_core import page_file_id, page_file_path, retry_delay


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "html_pages"))
CONCURRENCY = int(os.getenv("FETCH_CONCURRENCY", "5"))
MAX_ATTEMPTS = int(os.getenv("FETCH_MAX_ATTEMPTS", "3"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "0.1"))
USER_AGENT = os.getenv(
    "CRAWLER_USER_AGENT",
    "portfolio-wikipedia-crawler/1.0 (set CRAWLER_USER_AGENT with contact details)",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


async def publish(producer: Any, topic: str, value: dict[str, Any]) -> None:
    """Publish with broker acknowledgement (producer is configured with acks=all)."""

    await producer.send_and_wait(topic, value=value)


async def queue_for_parsing(pages: Any, producer: Any, record: dict[str, Any]) -> None:
    """Publish pending parse work, then record that Kafka acknowledged it.

    A crash after publication and before the status update may create a duplicate,
    but a crash before publication leaves a recoverable ``pending`` record.
    """

    message = {
        "file_id": record["file_id"],
        "file_path": record["file_path"],
        "url": record["url"],
        "fetched_at": record["fetched_at"],
    }
    await publish(producer, "parse_queue", message)
    await pages.update_one(
        {"url": record["url"]},
        {"$set": {"parse_status": "queued", "parse_queued_at": utc_now()}},
    )


async def fetch_page(session: Any, url: str) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    async with session.get(url, headers={"User-Agent": USER_AGENT}) as response:
        response.raise_for_status()
        html = await response.text()

    destination = page_file_path(OUTPUT_DIR, url)
    temporary = destination.with_suffix(".tmp")
    temporary.write_text(html, encoding="utf-8")
    temporary.replace(destination)
    return {
        "file_id": page_file_id(url),
        "file_path": str(destination),
        "url": url,
        "fetched_at": utc_now(),
        "parse_status": "pending",
    }


async def process_url(session: Any, pages: Any, producer: Any, url: str) -> None:
    existing = await pages.find_one({"url": url})
    if (
        existing
        and existing.get("parse_status") == "queued"
        and Path(existing["file_path"]).is_file()
    ):
        return

    if existing and Path(existing["file_path"]).is_file():
        existing.pop("_id", None)
        record = existing
    else:
        last_error: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                record = await fetch_page(session, url)
                break
            except Exception as exc:
                last_error = exc
                if attempt < MAX_ATTEMPTS:
                    await asyncio.sleep(retry_delay(attempt, 0.5, 4.0))
        else:
            await publish(
                producer,
                "dead_letter_queue",
                {
                    "stage": "fetch",
                    "url": url,
                    "error": repr(last_error),
                    "failed_at": utc_now(),
                },
            )
            print(f"Dead-lettered after {MAX_ATTEMPTS} attempts: {url}", flush=True)
            return

        # Database and downstream-publication failures deliberately propagate. The
        # Kafka input offset then remains uncommitted and is replayed after restart.
        await pages.update_one(
            {"url": url},
            {"$set": record, "$setOnInsert": {"created_at": utc_now()}},
            upsert=True,
        )

    await queue_for_parsing(pages, producer, record)
    print(f"Fetched/queued: {url}", flush=True)


async def run() -> None:
    import aiohttp
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
    from motor.motor_asyncio import AsyncIOMotorClient

    mongo = AsyncIOMotorClient(MONGO_URI)
    pages = mongo["wiki_crawler"]["pages"]
    await pages.create_index("url", unique=True)

    consumer = AIOKafkaConsumer(
        "fetch_queue",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="fetcher_group",
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
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)

    print("Fetcher waiting for URLs...", flush=True)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                batches = await consumer.getmany(
                    timeout_ms=1000, max_records=CONCURRENCY
                )
                messages = [message for batch in batches.values() for message in batch]
                if not messages:
                    continue

                # One task per URL prevents concurrent writes for duplicates in a batch.
                urls = list(dict.fromkeys(message.value["url"] for message in messages))
                await asyncio.gather(
                    *(process_url(session, pages, producer, url) for url in urls)
                )
                offsets = {
                    TopicPartition(message.topic, message.partition): message.offset + 1
                    for message in messages
                }
                await consumer.commit(offsets)
                if REQUEST_DELAY_SECONDS:
                    await asyncio.sleep(REQUEST_DELAY_SECONDS)
    finally:
        await producer.stop()
        await consumer.stop()
        mongo.close()


if __name__ == "__main__":
    asyncio.run(run())
