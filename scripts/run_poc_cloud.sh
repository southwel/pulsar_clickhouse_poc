#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
C="-f docker-compose.cloud.yml"

[ ! -f .env ] && echo "need .env (see .env.example)" && exit 1
source .env
[ -z "$CLICKHOUSE_URL" ] || [ -z "$CLICKHOUSE_USER" ] || [ -z "$CLICKHOUSE_PASSWORD" ] && echo "set CLICKHOUSE_* in .env" && exit 1

docker compose $C build connector
docker compose $C up -d
sleep 90
docker compose $C restart connector
sleep 15

for i in 1 2 3 4 5 6 7 8 9 10; do
  if docker compose $C run --rm --entrypoint "" -e PULSAR_URL=pulsar://pulsar:6650 -e PULSAR_TOPIC=events connector python produce_events.py; then break; fi
  [ "$i" -eq 10 ] && exit 1
  sleep 15
done

sleep 10
curl -sS -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" "${CLICKHOUSE_URL}/?query=SELECT%20*%20FROM%20default.events_merged%20ORDER%20BY%20ts%20DESC%20LIMIT%2010"
