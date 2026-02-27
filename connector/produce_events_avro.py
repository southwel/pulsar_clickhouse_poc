#!/usr/bin/env python3
import os
import sys
import time

import pulsar
from pulsar.schema import AvroSchema, Record, String

class EventRecord(Record):
    ts = String()
    event_id = String()
    user_id = String()
    event_type = String()
    payload = String()


PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://localhost:6650")
TOPIC = os.environ.get("PULSAR_TOPIC", "events")
MAX_RETRIES = int(os.environ.get("PULSAR_PRODUCER_RETRIES", "30"))
RETRY_DELAY = int(os.environ.get("PULSAR_PRODUCER_RETRY_DELAY", "5"))

EVENTS = [
    EventRecord(ts="2025-02-28 12:00:00.000", event_id="e1", user_id="u1", event_type="page_view", payload="{}"),
    EventRecord(ts="2025-02-28 12:00:01.000", event_id="e2", user_id="u1", event_type="click", payload='{"button":"submit"}'),
    EventRecord(ts="2025-02-28 12:00:02.000", event_id="e3", user_id="u2", event_type="page_view", payload="{}"),
    EventRecord(ts="2025-02-28 12:00:03.000", event_id="e4", user_id="u2", event_type="click", payload='{"button":"cancel"}'),
    EventRecord(ts="2025-02-28 12:00:04.000", event_id="e5", user_id="u1", event_type="page_view", payload="{}"),
]


def main():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = pulsar.Client(PULSAR_URL)
            producer = client.create_producer(TOPIC, schema=AvroSchema(EventRecord))
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"Failed to connect to Pulsar after {MAX_RETRIES} attempts: {e}", file=sys.stderr)
                sys.exit(1)
            print(f"Connect attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {RETRY_DELAY}s...", file=sys.stderr)
            time.sleep(RETRY_DELAY)
    for evt in EVENTS:
        producer.send(evt)
    producer.close()
    client.close()


if __name__ == "__main__":
    main()
