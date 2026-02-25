#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

docker compose build connector
docker compose up -d
sleep 90

docker compose restart connector
sleep 15

for i in 1 2 3 4 5 6 7 8 9 10; do
  if docker compose run --rm --entrypoint "" -e PULSAR_URL=pulsar://pulsar:6650 -e PULSAR_TOPIC=events connector python produce_events.py; then break; fi
  [ "$i" -eq 10 ] && exit 1
  sleep 15
done

sleep 10
docker compose exec -T clickhouse clickhouse-client -q "SELECT * FROM default.events_merged ORDER BY ts DESC LIMIT 10"
