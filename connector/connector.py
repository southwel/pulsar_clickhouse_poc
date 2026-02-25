#!/usr/bin/env python3
# Pulsar topic -> batch insert into ClickHouse (JSON).
import os
import sys
import time
import json
import logging
from typing import List, Optional

import pulsar
import requests

# Config from env
PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://localhost:6650")
PULSAR_TOPIC = os.environ.get("PULSAR_TOPIC", "events")
PULSAR_SUBSCRIPTION = os.environ.get("PULSAR_SUBSCRIPTION", "clickhouse-pipe")
CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_URL", "http://localhost:8123")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "default.events_merged")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
BATCH_TIMEOUT_MS = int(os.environ.get("BATCH_TIMEOUT_MS", "2000"))
INIT_SLEEP = int(os.environ.get("INIT_SLEEP", "5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _clickhouse_request_params(extra: Optional[dict] = None) -> dict:
    params = dict(extra) if extra else {}
    if CLICKHOUSE_USER:
        params["user"] = CLICKHOUSE_USER
    if CLICKHOUSE_PASSWORD:
        params["password"] = CLICKHOUSE_PASSWORD
    return params


def ensure_clickhouse_table() -> None:
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE}
    (
        ts DateTime64(3),
        event_id String,
        user_id String,
        event_type String,
        payload String
    )
    ENGINE = MergeTree()
    ORDER BY (user_id, ts)
    SETTINGS index_granularity = 8192
    """
    r = requests.post(
        CLICKHOUSE_URL,
        params=_clickhouse_request_params({"query": create_sql.strip()}),
        timeout=10,
    )
    r.raise_for_status()
    log.info("table %s ok", CLICKHOUSE_TABLE)


def insert_batch(rows: List[bytes]) -> None:
    if not rows:
        return
    body = b"\n".join(rows)
    query = f"INSERT INTO {CLICKHOUSE_TABLE} FORMAT JSONEachRow"
    r = requests.post(
        CLICKHOUSE_URL,
        params=_clickhouse_request_params({"query": query}),
        data=body,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    log.info("inserted %d rows", len(rows))


def run() -> None:
    log.info("connector starting: %s %s", PULSAR_URL, PULSAR_TOPIC)

    time.sleep(INIT_SLEEP)
    ensure_clickhouse_table()

    for attempt in range(1, 13):
        try:
            client = pulsar.Client(PULSAR_URL)
            consumer = client.subscribe(
                PULSAR_TOPIC,
                PULSAR_SUBSCRIPTION,
                consumer_type=pulsar.ConsumerType.Shared,
                initial_position=pulsar.InitialPosition.Earliest,
            )
            break
        except Exception as e:
            log.warning("Pulsar connect attempt %d failed: %s", attempt, e)
            if attempt == 12:
                raise
            time.sleep(5)

    batch: List[tuple] = []
    batch_deadline: Optional[float] = None

    try:
        while True:
            try:
                msg = consumer.receive(timeout_millis=min(500, BATCH_TIMEOUT_MS))
                raw = msg.data()
                batch.append((msg, raw))
                if batch_deadline is None:
                    batch_deadline = time.monotonic() + (BATCH_TIMEOUT_MS / 1000.0)
            except pulsar.Timeout:
                pass

            now = time.monotonic()
            flush = (
                len(batch) >= BATCH_SIZE
                or (batch and batch_deadline is not None and now >= batch_deadline)
            )
            if flush and batch:
                rows = [b for _, b in batch]
                try:
                    insert_batch(rows)
                    for msg, _ in batch:
                        consumer.acknowledge(msg)
                except Exception as e:
                    log.exception("Insert failed: %s", e)
                    for msg, _ in batch:
                        consumer.negative_acknowledge(msg)
                batch = []
                batch_deadline = None
    except pulsar.Interrupted:
        log.info("Interrupted, flushing remaining messages")
    finally:
        if batch:
            try:
                insert_batch([b for _, b in batch])
                for msg, _ in batch:
                    consumer.acknowledge(msg)
            except Exception as e:
                log.exception("Final flush failed: %s", e)
        consumer.close()
        client.close()
    log.info("stopped")


if __name__ == "__main__":
    run()
    sys.exit(0)
