# Distributed Wikipedia Crawler

A small event-driven crawler that follows English Wikipedia article links through a
three-stage pipeline. Kafka separates fetching, parsing, and URL validation; MongoDB
stores durable crawl state; RedisBloom accelerates duplicate checks; Docker Compose
runs the complete local stack.

This is an educational distributed-systems project, not a production web crawler.
Its most important design goal is recoverable **at-least-once** processing without
claiming exactly-once delivery.

## Architecture

```text
                          HTML volume
                              |
fetch_queue -> Fetcher -> parse_queue -> Parser -> validate_queue -> Validator
     ^             |                                             |       |
     |             +-> MongoDB pages              MongoDB frontier <-> RedisBloom
     +-------------------------------------------------------------------+
```

1. **Fetcher** downloads a page, writes its HTML to the shared volume, records a
   `pending` parse operation in MongoDB, and publishes it to `parse_queue`.
2. **Parser** extracts only same-origin, main-namespace Wikipedia article links and
   publishes a deterministic, deduplicated list to `validate_queue`.
3. **Validator** treats RedisBloom as a fast hint, confirms positives against the
   MongoDB frontier, and publishes unseen or still-pending URLs to `fetch_queue`.

All Kafka consumers disable automatic offset commits. A service commits an input only
after the next stage acknowledges publication (or after a bounded fetch/parse failure
is durably sent to `dead_letter_queue`). The fetcher processes up to five distinct URLs
concurrently and commits a fetched batch only after every URL is handled.

## Run locally

Requirements:

- Docker with Compose v2
- About 4 GB of free memory for Kafka, MongoDB, Redis Stack, and three Python services

```bash
git clone <repository-url>
cd distributed-wikipedia-crawler
cp .env.example .env
```

Before starting, edit `.env` and replace `CRAWLER_USER_AGENT` with a descriptive user
agent containing real contact information. Choose a narrow seed and review the current
[Wikimedia robot policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy)
and applicable rate limits.

```bash
docker compose up --build
```

The validator seeds the crawl once the infrastructure is healthy. Follow service logs:

```bash
docker compose logs -f fetcher parser validator
```

Inspect durable state:

```bash
docker compose exec mongo mongosh wiki_crawler --eval \
  'db.pages.countDocuments(); db.url_frontier.countDocuments()'
```

Stop services without deleting state:

```bash
docker compose down
```

To deliberately remove local crawl data, use `docker compose down --volumes`.

## Configuration

The main controls are documented in `.env.example`:

| Variable | Default | Purpose |
|---|---:|---|
| `SEED_URL` | Book of Genesis | Initial English Wikipedia article |
| `FETCH_CONCURRENCY` | 5 | Maximum distinct URLs fetched per batch |
| `FETCH_MAX_ATTEMPTS` | 3 | HTTP/file retries before dead-lettering |
| `REQUEST_TIMEOUT_SECONDS` | 20 | Total HTTP timeout per attempt |
| `REQUEST_DELAY_SECONDS` | 0.1 | Delay between completed fetch batches |
| `RECOVERY_SCAN_LIMIT` | 10000 | Pending frontier records replayed at startup |
| `MAX_FRONTIER_URLS` | 1000 | Safety cap on URLs admitted to the frontier |

## Recovery and delivery semantics

| Boundary | Implemented behavior | Remaining limitation |
|---|---|---|
| Kafka input -> downstream Kafka | Manual offset commits and acknowledged sends | A crash after send but before commit can replay a duplicate |
| Fetched page -> parse work | MongoDB `pending`/`queued` state | HTML volume and MongoDB are not one transaction |
| Frontier -> fetch work | MongoDB `pending`/`queued` state and startup replay | MongoDB and Kafka are not transactionally coupled; duplicates remain possible |
| Bloom filter -> frontier | Bloom positives are confirmed in MongoDB | Redis loss reduces speed but MongoDB remains authoritative |
| Poison input | Bounded fetch/parse retries then dead-letter topic | Dead letters require operator review; no automated replay tool is included |

These choices prevent the known database-before-publication loss windows while keeping
the design understandable. They do **not** establish exactly-once semantics, guaranteed
delivery under every infrastructure failure, or proven horizontal scalability.

## Tests

Python 3.12 is the reference version. For lightweight unit tests:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

The tests cover URL filtering, deterministic file naming, parser output, Bloom-filter
false-positive handling, and publication-before-status-update recovery boundaries.
They use fakes and do not require Kafka, MongoDB, Redis, or network access.

Static Compose validation:

```bash
docker compose config --quiet
```

### Verified integration smoke test

On 2026-10-26, the Compose stack was built and exercised locally with one fetch
worker and `MAX_FRONTIER_URLS=10`:

- 10 pages completed the fetch, parse, and validate stages.
- MongoDB contained 10 queued page records and 10 queued frontier records, with no
  pending records.
- All three Kafka consumer groups reported zero lag, and every dead-letter partition
  remained at offset zero.
- A queued frontier record was deliberately changed back to `pending`; after the
  validator restarted it reported `recovered=1`, restored the queued state, and the
  frontier remained at 10 unique records.

This is a functional integration check, not a performance or fault-tolerance
benchmark.

## Scope and limitations

- The repository contains no throughput benchmark, chaos test, or multi-host load test.
- The Compose deployment is a single-machine development topology with one broker.
- Fetching is limited to links discovered on English Wikipedia pages, but redirect,
  canonicalization, `robots.txt`, and crawl-priority policies are intentionally minimal.
- The default frontier cap is a safety control, not a sophisticated crawl-budget policy.
- The dead-letter topic has no management UI or replay command.
- External page content and downloaded HTML are not committed to Git.

No dataset, model artifact, credential, or captured Wikipedia page is included.
