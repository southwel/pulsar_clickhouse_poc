#!/usr/bin/env python3
# Produce sample events to Pulsar. Usage: produce_events.py [json|avro] (default: json)
import json
import os
import sys
import time

import pulsar

PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://localhost:6650")
TOPIC = os.environ.get("PULSAR_TOPIC", "events")
MAX_RETRIES = int(os.environ.get("PULSAR_PRODUCER_RETRIES", "30"))
RETRY_DELAY = int(os.environ.get("PULSAR_PRODUCER_RETRY_DELAY", "5"))

EVENTS = [
    {"ts": "2025-02-28 12:00:00.000", "event_id": "e1", "user_id": "u1", "event_type": "page_view", "payload": "{}"},
    {"ts": "2025-02-28 12:00:01.000", "event_id": "e2", "user_id": "u1", "event_type": "click", "payload": '{"button":"submit"}'},
    {"ts": "2025-02-28 12:00:02.000", "event_id": "e3", "user_id": "u2", "event_type": "page_view", "payload": "{}"},
    {"ts": "2025-02-28 12:00:03.000", "event_id": "e4", "user_id": "u2", "event_type": "click", "payload": '{"button":"cancel"}'},
    {"ts": "2025-02-28 12:00:04.000", "event_id": "e5", "user_id": "u1", "event_type": "page_view", "payload": "{}"},
]


def main():
    use_avro = len(sys.argv) > 1 and sys.argv[1].lower() == "avro"
    if use_avro:
        try:
            from pulsar.schema import AvroSchema, Record, String
        except ImportError:
            print("Avro requires: pip install 'pulsar-client[avro]'", file=sys.stderr)
            sys.exit(1)
        class EventRecord(Record):
            ts = String()
            event_id = String()
            user_id = String()
            event_type = String()
            payload = String()
        to_send = [EventRecord(**evt) for evt in EVENTS]
    else:
        to_send = [json.dumps(evt).encode("utf-8") for evt in EVENTS]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = pulsar.Client(PULSAR_URL)
            if use_avro:
                producer = client.create_producer(TOPIC, schema=AvroSchema(EventRecord))
            else:
                producer = client.create_producer(TOPIC)
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"Failed to connect to Pulsar after {MAX_RETRIES} attempts: {e}", file=sys.stderr)
                sys.exit(1)
            print(f"Connect attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {RETRY_DELAY}s...", file=sys.stderr)
            time.sleep(RETRY_DELAY)

    for msg in to_send:
        producer.send(msg)
    producer.close()
    client.close()


if __name__ == "__main__":
    main()
