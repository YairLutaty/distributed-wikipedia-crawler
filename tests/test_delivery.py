import unittest

from fetcher import queue_for_parsing
from validator import ensure_pending, publish_pending


class FakeProducer:
    def __init__(self, events):
        self.events = events

    async def send_and_wait(self, topic, value):
        self.events.append(("publish", topic, value))


class FailingProducer:
    async def send_and_wait(self, topic, value):
        raise RuntimeError("broker unavailable")


class FakePages:
    def __init__(self, events):
        self.events = events

    async def update_one(self, query, update, **kwargs):
        self.events.append(("update", query, update, kwargs))


class FakeRedis:
    def __init__(self, exists=False):
        self.exists = exists
        self.added = []

    async def execute_command(self, command, key, url):
        if command == "BF.EXISTS":
            return int(self.exists)
        if command == "BF.ADD":
            self.added.append(url)
            return 1
        raise AssertionError(command)


class FakeFrontier:
    def __init__(self, initial=None):
        self.documents = {} if initial is None else {initial["url"]: dict(initial)}
        self.updates = []

    async def find_one(self, query):
        return self.documents.get(query["url"])

    async def update_one(self, query, update, upsert=False):
        self.updates.append((query, update, upsert))
        document = self.documents.get(query["url"])
        if document is None and upsert:
            document = dict(update.get("$setOnInsert", {}))
            self.documents[query["url"]] = document
        if document is not None:
            document.update(update.get("$set", {}))


class DeliveryBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetcher_marks_queued_only_after_acknowledged_publish(self):
        events = []
        record = {
            "file_id": "page-1",
            "file_path": "html_pages/page-1.html",
            "url": "https://en.wikipedia.org/wiki/Ada_Lovelace",
            "fetched_at": "now",
        }
        await queue_for_parsing(FakePages(events), FakeProducer(events), record)
        self.assertEqual(events[0][0:2], ("publish", "parse_queue"))
        self.assertEqual(events[1][0], "update")
        self.assertEqual(events[1][2]["$set"]["parse_status"], "queued")

    async def test_fetcher_keeps_pending_state_when_publication_fails(self):
        events = []
        record = {
            "file_id": "page-1",
            "file_path": "html_pages/page-1.html",
            "url": "https://en.wikipedia.org/wiki/Ada_Lovelace",
            "fetched_at": "now",
        }
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await queue_for_parsing(FakePages(events), FailingProducer(), record)
        self.assertEqual(events, [])

    async def test_bloom_positive_is_confirmed_in_mongo(self):
        url = "https://en.wikipedia.org/wiki/False_positive"
        frontier = FakeFrontier()
        record = await ensure_pending(frontier, FakeRedis(exists=True), url)
        self.assertEqual(record["publish_status"], "pending")
        self.assertIn(url, frontier.documents)

    async def test_frontier_marks_queued_only_after_acknowledged_publish(self):
        events = []
        url = "https://en.wikipedia.org/wiki/Ada_Lovelace"
        frontier = FakeFrontier({"url": url, "publish_status": "pending"})
        producer = FakeProducer(events)
        redis_client = FakeRedis()
        published = await publish_pending(
            frontier, redis_client, producer, frontier.documents[url]
        )
        self.assertTrue(published)
        self.assertEqual(events[0][0:2], ("publish", "fetch_queue"))
        self.assertEqual(frontier.documents[url]["publish_status"], "queued")
        self.assertEqual(redis_client.added, [url])

    async def test_already_queued_frontier_item_is_not_republished(self):
        events = []
        url = "https://en.wikipedia.org/wiki/Ada_Lovelace"
        frontier = FakeFrontier({"url": url, "publish_status": "queued"})
        published = await publish_pending(
            frontier, FakeRedis(), FakeProducer(events), frontier.documents[url]
        )
        self.assertFalse(published)
        self.assertEqual(events, [])

    async def test_frontier_stays_pending_when_publication_fails(self):
        url = "https://en.wikipedia.org/wiki/Ada_Lovelace"
        frontier = FakeFrontier({"url": url, "publish_status": "pending"})
        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await publish_pending(
                frontier, FakeRedis(), FailingProducer(), frontier.documents[url]
            )
        self.assertEqual(frontier.documents[url]["publish_status"], "pending")


if __name__ == "__main__":
    unittest.main()
