Pulsar → Python connector → ClickHouse. Same idea as a ClickPipe but for Pulsar (no native support today). Connector batches and inserts over HTTP. JSON by default; Avro works if you set MESSAGE_FORMAT=avro and use the events-avro topic + produce_events_avro.py.

Local: `./scripts/run_poc.sh` (needs Docker; Pulsar takes ~90s to come up).

Or by hand:
```bash
docker compose up -d
sleep 90
docker compose restart connector && sleep 15
docker compose run --rm --entrypoint "" -e PULSAR_URL=pulsar://pulsar:6650 -e PULSAR_TOPIC=events connector python produce_events.py
sleep 10
docker compose exec clickhouse clickhouse-client -q "SELECT * FROM default.events_merged ORDER BY ts DESC LIMIT 10"
```

ClickHouse Cloud: copy .env.example to .env, fill in URL/user/password, then `./scripts/run_poc_cloud.sh`.

No dedup — replay or use both JSON and Avro and you get duplicates.

Repo: https://github.com/southwel/pulsar_clickhouse_poc
