# Step 3: Route 3 operator runbook (no Airbyte)

## Scope and status

**Step 3 is complete** for the selected Python/no-Airbyte route. Orders and
clickstream ingestion are implemented; Debezium CDC reads the existing slot,
and the Python sink writes the seven published tables to GCS. Snapshot, CRUD,
restart/resume, slot/WAL and lag evidence is in
[`cdc-status.json`](../evidence/step3/cdc-status.json). For a team member who
has cloned the repository, follow the [reproduction guide](step3-reproduction.md).

The orders one-shot catch-up wrote 237 rows through watermark 225405; two rows
were already above that watermark at its post-run check. The orders path
overlaps the initial snapshots, and its cursor does not capture updates,
deletes or late lower IDs. See
[`bronze-verification.json`](../evidence/step3/bronze-verification.json).

Use one source stack and one production checkpoint/group identity. A separately
seeded database is a different source even when its IDs overlap. Never run
`docker compose down -v` for a normal restart; it deletes the source volumes.

## 1. Check the source services

On the operator's machine, start or inspect the supplied source stack:

```powershell
docker info
docker compose up -d
docker compose ps
docker compose logs --tail 30 seed migrate kafka-init
```

Confirm PostgreSQL, Kafka, the seeded source tables and the `clickstream` topic
are healthy. For host-run jobs, the defaults are PostgreSQL `127.0.0.1:5432`
and Kafka `localhost:9092`. Use only the operator's local machine or an
approved encrypted tunnel; do not expose the unauthenticated ports publicly.

The project `.env` is ignored by Git. Configure `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE`, `SOURCE_ID`,
`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`,
`GCP_PROJECT_ID` and `BRONZE_BUCKET` there. Use the operator's own ADC for
GCS. Never share credentials through Git or copy another member's ADC file.

## 2. Start the no-Airbyte CDC path

Read-only checks against the current source should confirm:

- `wal_level=logical` and the configured `max_slot_wal_keep_size`;
- the seven source tables exist;
- `marketplace_bronze_publication` contains exactly the seven intended tables;
- the `marketplace_bronze_slot` owner, plugin, activity and retained WAL.

The group confirmed the existing `marketplace_bronze_slot` belongs to this
pipeline. Start Debezium Connect (Compose creates Connect's internal topics,
the seven publication topics and heartbeat topic), register the connector from
local `.env`, then run the Bronze sink:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
docker compose up -d debezium-connect
uv run --locked ingestion/configure_cdc.py
uv run --locked ingestion/consume_cdc.py --batch-size 5000
```

Check `http://127.0.0.1:18083/connectors/marketplace-bronze/status` for
`RUNNING`, then check the slot's `active`, `wal_status`, and retained bytes in
`pg_replication_slots`. Do not create, drop, or rename the slot. The sink
commits each Kafka poll only after all matching GCS objects are durable. If it
stops, restart the same command with the same consumer group to resume; duplicate
objects can result from a crash after GCS upload and before Kafka commit. The
initial CDC snapshot overlaps the Step 2 snapshot and orders backfill. The
publication does not capture `TRUNCATE`; treat one as a source reset and
coordinate a full snapshot plus downstream reconciliation before resuming CDC.
Runtime evidence is in
[`cdc-status.json`](../evidence/step3/cdc-status.json).

## 3. Run the orders extractor

First review the existing local checkpoint at `data/state/orders.json`; keep it
stable across restarts. Run one bounded window:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv sync --locked
uv run --locked ingestion/extract_orders.py --once
```

The command writes append-only Parquet to
`bronze/python/orders/ingestion_date=.../` before it saves the next cursor. A
crash after upload can cause replay, so downstream readers must preserve and
reconcile duplicates. Do not use a local sink with the production checkpoint.

For a local-sink check against the database, use both a separate state file and
output directory:

```powershell
uv run --locked ingestion/extract_orders.py --once --local-output data/step3-local --state-file data/state/orders-local-test.json
```

The `order_id` cursor captures higher visible IDs only. It misses updates,
deletes and lower IDs that commit after the saved watermark; do not describe it
as CDC. To check restart behaviour, record the cursor/object before and after a
second run and measure any duplicate rows from actual output. Do not reset or
delete the production checkpoint to manufacture a restart test.

## 4. Run the Kafka consumer and isolated DLQ scenario

The production consumer reads `clickstream` using the configured group ID:

```powershell
uv run --locked ingestion/consume_clickstream.py
```

For a repeatable malformed-event scenario, use only a dedicated test topic and
group so test offsets cannot advance the production group:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic clickstream-step3-check --partitions 1 --replication-factor 1
uv run --locked ingestion/publish_test_events.py
uv run --locked ingestion/consume_clickstream.py --topic clickstream-step3-check --group-id marketplace-step3-check --idle-seconds 15
```

The publisher emits valid, malformed and valid events. Confirm the actual
offsets in the output, two accepted records, one rejection, the second valid
event after the rejection, and the committed next offset. In GCS, verify the
matching object under `bronze/kafka/clickstream-step3-check/` and the rejection
under `bronze/_dlq/clickstream-step3-check/`. Restart the same test group
without publishing new messages and confirm that committed records are not
emitted again. The current evidence reports this scenario. GCS contains the
matching two-row Parquet object, one DLQ object, and the test group's committed
offset is 3.

The consumer is at-least-once within Kafka retention. Writes and offset commits
are not one transaction; replay can duplicate objects. Invalid messages are
durably written to the DLQ before committing, so a failed DLQ write prevents
the poll from committing.

## 5. Record evidence and handover

Add only safe summaries to `docs/evidence/step3/`. Record:

- the date, source identity and job command;
- the extractor's row/batch count, before/after cursor and object paths;
- Kafka topic, test group, published offsets, accepted/rejected counts,
  committed offsets and restart result;
- GCS object existence, sizes and Parquet schema/provenance, checked by an
  authorised operator;
- CDC slot owner, plugin, activity, retained WAL and any owner-approved action;
- the precise scope of checks performed and any limitations.

Do not commit raw source rows, event bodies, passwords, ADC files, service
account keys or connector exports. Keep operator evidence distinct from local
simulations. Update the ADR and architecture only with observed results, and
preserve the current local checkpoint unless the source identity is deliberately
changed.
