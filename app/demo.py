#!/usr/bin/env python3
"""
CDC Pipeline Demo Application
─────────────────────────────
Continuously performs random CRUD operations on MongoDB and verifies
real-time replication to Elasticsearch, measuring end-to-end latency.
"""

import os
import sys
import time
import random
import string
import logging
from datetime import datetime, timezone

import pymongo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Configuration ───────────────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "appdb")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "users")
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "5"))

# ── Logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cdc-demo")

# ── Decorative constants ───────────────────────────────────────────────
DIVIDER = "═" * 65
THIN_DIV = "─" * 65

OPERATIONS = ["insert", "update", "delete"]
ROLES = ["engineer", "designer", "manager", "analyst", "devops", "intern"]
DOMAINS = ["example.com", "test.io", "demo.org", "sample.net"]


def random_name() -> str:
    first = random.choice([
        "Alice", "Bob", "Charlie", "Diana", "Eve",
        "Frank", "Grace", "Hank", "Iris", "Jack",
    ])
    last = "".join(random.choices(string.ascii_uppercase, k=1)) + \
           "".join(random.choices(string.ascii_lowercase, k=5))
    return f"{first} {last}"


def random_email(name: str) -> str:
    slug = name.lower().replace(" ", ".") + \
           str(random.randint(100, 999))
    return f"{slug}@{random.choice(DOMAINS)}"


def create_es_session() -> requests.Session:
    """Create an HTTP session with retry and exponential backoff."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def connect_mongo(max_retries: int = 10) -> pymongo.MongoClient:
    """Connect to MongoDB with exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            client = pymongo.MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=5000,
                directConnection=False,
            )
            # Force a connection check
            client.admin.command("ping")
            log.info("Connected to MongoDB at %s", MONGO_URI)
            return client
        except pymongo.errors.ConnectionFailure as exc:
            wait = min(2 ** attempt, 30)
            log.warning(
                "MongoDB connection attempt %d/%d failed: %s — retrying in %ds",
                attempt, max_retries, exc, wait,
            )
            time.sleep(wait)
    log.error("Could not connect to MongoDB after %d attempts", max_retries)
    sys.exit(1)


def query_es(session: requests.Session, doc_id: str) -> dict | None:
    """Query Elasticsearch for a document by _id with retry."""
    # Refresh index to get near-real-time results
    try:
        session.post(f"{ES_URL}/{ES_INDEX}/_refresh", timeout=5)
    except requests.RequestException:
        pass

    try:
        resp = session.get(
            f"{ES_URL}/{ES_INDEX}/_doc/{doc_id}",
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("_source")
        return None
    except requests.RequestException:
        return None


def do_insert(collection: pymongo.collection.Collection) -> tuple[str, dict]:
    """Insert a random user document, return (id_str, doc)."""
    name = random_name()
    doc = {
        "name": name,
        "email": random_email(name),
        "age": random.randint(18, 65),
        "role": random.choice(ROLES),
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    result = collection.insert_one(doc)
    doc_id = str(result.inserted_id)
    log.info("INSERT  │ _id=%s │ name=%s", doc_id, name)
    return doc_id, doc


def do_update(
    collection: pymongo.collection.Collection,
) -> tuple[str, dict] | tuple[None, None]:
    """Update a random existing user's email, return (id_str, updated_fields)."""
    docs = list(collection.find().limit(50))
    if not docs:
        log.warning("No documents to update — inserting instead")
        return None, None

    target = random.choice(docs)
    doc_id = str(target["_id"])
    new_email = random_email(target.get("name", "unknown"))
    new_age = random.randint(18, 65)

    collection.update_one(
        {"_id": target["_id"]},
        {"$set": {"email": new_email, "age": new_age}},
    )
    log.info("UPDATE  │ _id=%s │ email→%s, age→%d", doc_id, new_email, new_age)
    return doc_id, {"email": new_email, "age": new_age}


def do_delete(
    collection: pymongo.collection.Collection,
) -> str | None:
    """Delete a random existing user, return id_str or None."""
    docs = list(collection.find().limit(50))
    if not docs:
        log.warning("No documents to delete — skipping")
        return None

    target = random.choice(docs)
    doc_id = str(target["_id"])
    collection.delete_one({"_id": target["_id"]})
    log.info("DELETE  │ _id=%s │ name=%s", doc_id, target.get("name", "?"))
    return doc_id


def print_comparison(
    operation: str,
    doc_id: str,
    mongo_state: dict | None,
    es_state: dict | None,
    latency_ms: float,
) -> None:
    """Pretty-print the MongoDB vs Elasticsearch state comparison."""
    print(THIN_DIV)
    print(f"  Operation : {operation.upper()}")
    print(f"  Doc ID    : {doc_id}")
    print(f"  Latency   : {latency_ms:.1f} ms")
    print()

    if operation == "delete":
        mongo_label = "(deleted)"
        es_label = "(not found)" if es_state is None else str(es_state)
        match = es_state is None
    else:
        mongo_label = str(mongo_state) if mongo_state else "(none)"
        es_label = str(es_state) if es_state else "(not found)"
        match = es_state is not None

    print(f"  MongoDB   : {mongo_label[:120]}")
    print(f"  ES        : {es_label[:120]}")
    status = "✔ IN SYNC" if match else "⏳ PENDING"
    print(f"  Status    : {status}")
    print(THIN_DIV)
    print()


# ── Main Loop ──────────────────────────────────────────────────────────
def main() -> None:
    print()
    print(DIVIDER)
    print("  CDC Pipeline Demo — Real-Time MongoDB → Elasticsearch")
    print(DIVIDER)
    print(f"  MongoDB : {MONGO_URI}/{MONGO_DB}")
    print(f"  ES      : {ES_URL}/{ES_INDEX}")
    print(f"  Interval: {LOOP_INTERVAL}s")
    print(DIVIDER)
    print()

    client = connect_mongo()
    db = client[MONGO_DB]
    collection = db["users"]
    es_session = create_es_session()

    # Verify Elasticsearch is reachable
    for attempt in range(1, 11):
        try:
            resp = es_session.get(f"{ES_URL}/_cluster/health", timeout=5)
            if resp.status_code == 200:
                log.info("Connected to Elasticsearch at %s", ES_URL)
                break
        except requests.RequestException:
            pass
        wait = min(2 ** attempt, 30)
        log.warning("ES not ready (attempt %d/10) — retrying in %ds", attempt, wait)
        time.sleep(wait)
    else:
        log.error("Could not connect to Elasticsearch after 10 attempts")
        sys.exit(1)

    iteration = 0
    try:
        while True:
            iteration += 1
            operation = random.choice(OPERATIONS)
            log.info("─── Iteration %d │ Operation: %s ───", iteration, operation.upper())

            t_start = time.monotonic()
            doc_id = None
            mongo_state = None

            if operation == "insert":
                doc_id, mongo_state = do_insert(collection)

            elif operation == "update":
                doc_id, mongo_state = do_update(collection)
                if doc_id is None:
                    doc_id, mongo_state = do_insert(collection)
                    operation = "insert"

            elif operation == "delete":
                doc_id = do_delete(collection)
                if doc_id is None:
                    doc_id, mongo_state = do_insert(collection)
                    operation = "insert"

            if doc_id is None:
                log.warning("Skipping — no document to operate on")
                time.sleep(LOOP_INTERVAL)
                continue

            # Wait briefly, then query ES
            time.sleep(0.5)
            es_state = query_es(es_session, doc_id)
            t_end = time.monotonic()
            latency_ms = (t_end - t_start) * 1000

            print_comparison(operation, doc_id, mongo_state, es_state, latency_ms)

            time.sleep(LOOP_INTERVAL)

    except KeyboardInterrupt:
        print()
        log.info("Demo stopped by user (Ctrl+C)")
    finally:
        client.close()
        log.info("Connections closed — goodbye")


if __name__ == "__main__":
    main()
