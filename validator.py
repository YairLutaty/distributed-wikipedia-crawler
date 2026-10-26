"""Deduplicate parsed links and publish recoverable fetch work."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
SEED_URL = os.getenv("SEED_URL", "https://en.wikipedia.org/wiki/Book_of_Genesis")
RECOVERY_SCAN_LIMIT = int(os.getenv("RECOVERY_SCAN_LIMIT", "10000"))
MAX_FRONTIER_URLS = int(os.getenv("MAX_FRONTIER_URLS", "1000"))
BLOOM_KEY = "url_bf"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


async def bloom_contains(redis_client: Any, url: str) -> bool:
    return bool(await redis_client.execute_command("BF.EXISTS", BLOOM_KEY, url))


async def mark_seen(redis_client: Any, url: str) -> None:
    await redis_client.execute_command("BF.ADD", BLOOM_KEY, url)


async def ensure_pending(frontier: Any, redis_client: Any, url: str) -> dict[str, Any]:
    """Return the durable frontier record for ``url``.

    A Bloom-filter hit is only a hint. MongoDB is always checked so Bloom false
    positives cannot suppress a URL that has never entered the frontier.
    """

    if await bloom_contains(redis_client, url):
        existing = await frontier.find_one({"url": url})
        if existing:
            return existing

    await frontier.update_one(
        {"url": url},
        {
            "$setOnInsert": {
                "url": url,
                "publish_status": "pending",
                "discovered_at": utc_now(),
            }
        },
        upsert=True,
    )
    return await frontier.find_one({"url": url})


async def publish_pending(
    frontier: Any, redis_client: Any, producer: Any, record: dict[str, Any]
) -> bool:
    """Publish one pending frontier item and mark it queued after acknowledgement."""

    if record.get("publish_status") == "queued":
        await mark_seen(redis_client, record["url"])
        return False

    url = record["url"]
    await producer.send_and_wait("fetch_queue", value={"url": url})
    await frontier.update_one(
        {"url": url},
        {"$set": {"publish_status": "queued", "queued_at": utc_now()}},
    )
    await mark_seen(redis_client, url)
    return True


async def recover_pending(frontier: Any, redis_client: Any, producer: Any) -> int:
    recovered = 0
    cursor = frontier.find({"publish_status": "pending"}).limit(RECOVERY_SCAN_LIMIT)
    async for record in cursor:
        recovered += int(
            await publish_pending(frontier, redis_client, producer, record)
        )
    return recovered


async def run() -> None:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
    from motor.motor_asyncio import AsyncIOMotorClient
    from redis.asyncio import Redis
    from redis.exceptions import ResponseError

    mongo = AsyncIOMotorClient(MONGO_URI)
    frontier = mongo["wiki_crawler"]["url_frontier"]
    await frontier.create_index("url", unique=True)
    redis_client = Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    try:
        await redis_client.execute_command("BF.RESERVE", BLOOM_KEY, 0.01, 10_000_000)
    except ResponseError as exc:
        if "item exists" not in str(exc).lower():
            raise

    consumer = AIOKafkaConsumer(
        "validate_queue",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="validator_group",
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

    try:
        recovered = await recover_pending(frontier, redis_client, producer)
        seed = await ensure_pending(frontier, redis_client, SEED_URL)
        seeded = await publish_pending(frontier, redis_client, producer, seed)
        print(f"Validator ready (recovered={recovered}, seeded={seeded})", flush=True)

        async for message in consumer:
            links = sorted(set(message.value.get("links") or []))
            frontier_size = await frontier.count_documents({})
            remaining_capacity = max(0, MAX_FRONTIER_URLS - frontier_size)
            for url in links[:remaining_capacity]:
                record = await ensure_pending(frontier, redis_client, url)
                await publish_pending(frontier, redis_client, producer, record)
            await consumer.commit(
                {TopicPartition(message.topic, message.partition): message.offset + 1}
            )
    finally:
        await producer.stop()
        await consumer.stop()
        await redis_client.aclose()
        mongo.close()


if __name__ == "__main__":
    asyncio.run(run())
