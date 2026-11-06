# Step 3: Python ingestion (Route 3, no Airbyte)

**Status: complete.** The orders cursor, clickstream flow and seven-table CDC
pipeline have live evidence. See the [CDC run evidence](../docs/evidence/step3/cdc-status.json)
and the [fresh-clone reproduction guide](../docs/runbooks/step3-reproduction.md).

The selected route uses a hand-written PostgreSQL orders extractor, a Python
clickstream consumer, and Debezium with a Python CDC sink. Airbyte is not part
of this route. Local checks and an operator-reported live run are recorded. A
read-only GCS audit verified the
Step 2 snapshot and the Step 3 Parquet/DLQ objects; see
[`bronze-verification.json`](../docs/evidence/step3/bronze-verification.json)
and [`route3-live-run.json`](../docs/evidence/step3/route3-live-run.json).
The last one-shot catch-up wrote 237 rows through order ID 225405; the source
had two rows beyond that watermark at the post-run check. See the
[Route 3 runbook](../docs/runbooks/step3-docker-handoff.md) and
[failure-mode notes](../docs/ingestion-failure-modes.md).

## Install and local checks

Use Python 3.12 and the locked project dependencies:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv sync --locked
```

The recorded local tests use synthetic SQL rows, local object storage and fake
Kafka commit callbacks. They exercise Parquet writing, checkpoints, validation
and failures without PostgreSQL, Kafka or GCS calls. The results are recorded
in [`local-checks.json`](../docs/evidence/step3/local-checks.json); they are not
live delivery evidence.

## Runtime configuration

Copy `.env.example` to the ignored `.env` and configure the PostgreSQL, Kafka
and GCP settings for the operator's environment. `python-dotenv` reads the
file without replacing existing environment variables. Cloud writes use the
operator's own Application Default Credentials (ADC). Never commit passwords,
ADC files, service-account keys or raw connector exports.

The example's plaintext settings target the local Docker stack or an approved
encrypted tunnel. Do not expose the source's unauthenticated PostgreSQL or
Kafka ports to the public internet. The Kafka broker advertises `localhost` to
host clients; a remote operator needs a properly configured tunnel/listener.

## PostgreSQL orders extractor

Run one bounded window, then use the polling mode when continuous ingestion is
intended:

```powershell
uv run --locked ingestion/extract_orders.py --once
uv run --locked ingestion/extract_orders.py
```

The extractor reads only `public.orders`, uses parameterised SQL and a
read-only connection, and writes Parquet under
`bronze/python/orders/ingestion_date=.../`. Initial connection failures retry
up to five times with 1, 2, 4 and 8 second waits. Unrecovered query, storage or
authentication errors stop the job; restart it after resolving the cause.

Its cursor is `data/state/orders.json`. An OS lock prevents concurrent writers
on the same file. One operator owns this checkpoint. The extractor uploads a
batch before atomically saving its cursor, so a crash between those actions can
replay rows. Preserve the checkpoint across restarts and choose a new
`SOURCE_ID` and state file deliberately after a source rebuild.

The `order_id` cursor captures newly visible higher IDs. It does **not** capture
updates, deletes or lower IDs that commit after the watermark advances. It is
not CDC and does not make the orders path authoritative for change history.
The current checkpoint is recorded in the ignored local `data/state/` folder;
the live evidence file records the reported run without copying source rows.

To test PostgreSQL while keeping production state and GCS untouched, provide a
separate state file and local sink:

```powershell
uv run --locked ingestion/extract_orders.py --once --local-output data/step3-local --state-file data/state/orders-local-test.json
```

The CLI rejects local output if it could advance the default production state.

## Kafka clickstream consumer

Run the consumer against the `clickstream` topic:

```powershell
uv run --locked ingestion/consume_clickstream.py
```

It validates messages against [schema v1](clickstream.schema.json). Invalid
UTF-8/JSON, duplicate JSON keys, unknown fields or versions, and invalid event
envelopes are sent to `bronze/_dlq/`. Accepted Parquet keeps the original bytes,
common event fields and Kafka provenance. It partitions by UTC `event_time`;
late events can therefore create objects in earlier event-date partitions.

Auto-commit is disabled. All accepted and rejected objects in a poll must be
durable before the consumer commits each partition's next offset. A failed
write prevents that poll from committing. A crash after writing but before
commit can produce duplicate objects on replay. The guarantee is at-least-once
within Kafka retention, not exactly-once; expired Kafka records cannot be
recovered by this consumer.

For a live broker test, use a dedicated topic and group as described in the
[runbook](../docs/runbooks/step3-docker-handoff.md). Local-sink tests against a
real broker must use a separate group ID so they cannot advance the configured
GCS consumer group:

```powershell
uv run --locked ingestion/consume_clickstream.py --local-output data/step3-local --group-id marketplace-local-test
```

## CDC status

The no-Airbyte CDC path uses Debezium Connect with the existing
`marketplace_bronze_slot` and `marketplace_bronze_publication`. It snapshots the
seven published tables, then streams row-level changes into table-specific
Kafka topics. Register or update the connector after starting Compose:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
docker compose up -d debezium-connect
uv run --locked ingestion/configure_cdc.py
uv run --locked ingestion/consume_cdc.py --batch-size 5000
```

The connector config uses the local ignored `.env` `PGUSER`, `PGPASSWORD`,
`PGDATABASE`, and `PGPORT`; the password is sent to local Kafka Connect REST
and is never printed. It will not drop the slot when stopped. The consumer
stores raw Debezium key/value bytes, tombstones, headers, topic, partition,
offset, and Kafka timestamp under `bronze/cdc/<table>/ingestion_date=.../`.
Offsets are committed only after all Parquet objects for that poll are durable.
This is at-least-once delivery; a crash after writing but before commit can
leave duplicate rows, reconciled by topic/partition/offset. Debezium initial
snapshot records overlap the earlier `bronze/snapshot/` and orders backfill;
they are a separate CDC baseline, not additive table counts. The PostgreSQL
publication does not publish `TRUNCATE`; treat any source truncate as a reset
and coordinate a full snapshot plus downstream reconciliation before resuming.

Kafka Connect REST is published only at `127.0.0.1:18083`; it has no REST
authentication, so keep it loopback-only. See the [CDC inventory and runtime
evidence](../docs/evidence/step3/cdc-status.json).

## Airbyte preparation files

Files under `ingestion/airbyte/` and `validate_connector_export.py` are
unapplied preparation artifacts from the alternative connector route. They are
not used by the selected Route 3 jobs and are not evidence of a deployed
connector. They are retained in the repository for history/reference; no
Airbyte installation, credentials or connection are required to run Route 3.
