# Step 3: reproduce from a fresh clone

## Status and what “reproduce” means

Step 3 is complete for Route 3 (Python, no Airbyte). The verified run includes
the orders cursor, clickstream consumer, Debezium initial snapshot and CDC sink,
synthetic INSERT/UPDATE/DELETE events plus a tombstone, and restart/resume. The
point-in-time record is [`cdc-status.json`](../evidence/step3/cdc-status.json).

A fresh clone can reproduce the pipeline and its checks. It will not recreate
the exact GCS object names, counts, Kafka offsets or cursor from the 2026-11-05
run: those depend on source state, timestamps, generated traffic and external
GCS/Kafka state.

## What Git does not contain

The repository's [`.gitignore`](../../.gitignore) deliberately excludes:

| Ignored item | Why a fresh clone needs setup |
|---|---|
| `.env` and `.env.*` (except `.env.example`) | Local database/Kafka settings and passwords are not shared. Create your own file. |
| `data/` | Downloaded release assets, uv cache, local test output and `data/state/orders.json` cursor/checkpoint. |
| `*.parquet` | Generated local Parquet files. Keep any manually downloaded source dump under `data/` to keep it ignored; a `.dump` outside an ignored directory is not currently excluded by `.gitignore`. The source stack downloads its seed on first startup; Bronze output is in GCS. |
| `*-key.json`, `*credentials*.json`, `service-account*.json` | Cloud credentials. Authenticate as yourself; do not copy a teammate's credentials. |
| `.venv/`, `venv/`, `__pycache__/`, `*.pyc` | Recreated from the locked dependency files. |

Docker named volumes are also outside Git. They hold the local PostgreSQL and
Kafka state, including Kafka Connect's connector configuration and offsets. A
clone starts with new volumes; `docker compose down -v` deletes them. GCS is an
external bucket, not a folder in the repository.

## Prerequisites

- Windows with PowerShell, Docker Desktop / Compose v2, Python 3.12 and `uv`.
- A Google identity with permission to list and create objects in the selected
  Bronze bucket. Authenticate with your own ADC, for example
  `gcloud auth application-default login`.
- A seeded local Marketplace database and a Kafka broker. On first `docker
  compose up`, the source stack downloads its published seed asset.
- For CDC, the database must have the seven-table
  `marketplace_bronze_publication`, the `marketplace_ingest` login with
  `REPLICATION`, and a `pgoutput` slot named `marketplace_bronze_slot`.

## Clone and configure

Run from the repository root:

```powershell
Copy-Item .env.example .env
```

Edit `.env` locally. Set `PGPASSWORD` to the password for
`marketplace_ingest`; set `GCP_PROJECT_ID` and `BRONZE_BUCKET` to a bucket where
your identity can write. Keep the local defaults for the same-machine Compose
stack (`PGHOST=127.0.0.1`, Kafka `localhost:9092`). Never commit `.env`, ADC
files, passwords, service-account keys or raw connector configuration.

Install dependencies and start the local source:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv sync --locked
docker compose up -d
docker compose ps
```

Wait for PostgreSQL, Kafka, seed and migrations to complete. Keep one Compose
project per machine; if you run more than one clone on the same machine, give
each a different Compose project name and non-conflicting host ports.

## Prepare CDC on an independent local database

The group's live source already has the role, publication and slot. When using
that shared source, inspect and reuse its slot; **do not create another slot or
run a second connector against it**. One operator owns the connector and the
`marketplace-cdc-bronze-v1` sink group at a time.

For a new, isolated local Compose database only, the existing SQL bootstrap
creates the ingestion role and publication. Inspect the SQL and confirm its
role grants and publication table list before applying it. The filename is a
historical preparation path: this is plain PostgreSQL SQL, does not install or
run Airbyte, and does not create a slot:

```powershell
Get-Content -Raw ingestion/airbyte/setup_postgres.sql | docker compose exec -T db psql -U postgres -d marketplace
docker compose exec db psql -U postgres -d marketplace
```

In the interactive `psql` session, set the role password with `\password
marketplace_ingest` and use the same value in the ignored `.env` as
`PGPASSWORD`. Verify that the publication contains the seven intended tables.
On this independent database, check for `marketplace_bronze_slot`; only if it
does not exist, create this local slot once as the database owner:

```sql
SELECT slot_name, plugin, database, active
FROM pg_replication_slots
WHERE slot_name = 'marketplace_bronze_slot';

-- Only on a separate database with no row returned above:
SELECT *
FROM pg_create_logical_replication_slot('marketplace_bronze_slot', 'pgoutput');
```

Never run the create statement on the group's shared source. Do not drop or
recreate a slot to reset offsets.

## Run the Step 3 flows

Run these from the repository root. Use separate terminals for the continuous
consumers.

```powershell
# One bounded orders window; a fresh ignored cursor starts at order_id 0.
uv run --locked ingestion/extract_orders.py --once

# Continuous clickstream consumer.
uv run --locked ingestion/consume_clickstream.py
```

Start CDC. Compose creates Kafka Connect's internal topics, the seven table
topics and the Debezium heartbeat topic. The connector config uses the local
`.env` password and connects to the Compose database service `db`:

```powershell
docker compose up -d debezium-connect
uv run --locked ingestion/configure_cdc.py
uv run --locked ingestion/consume_cdc.py --batch-size 5000
```

Kafka Connect REST is available only on `http://127.0.0.1:18083`. Check
`http://127.0.0.1:18083/connectors/marketplace-bronze/status` for `RUNNING`.
Check the sink group and slot lag with the commands in the
[operator runbook](step3-docker-handoff.md). The first Debezium run on a new
Connect volume takes a new initial snapshot; later runs resume from Kafka
Connect offsets and the PostgreSQL slot.

## Preserve or isolate state

- Preserve `data/state/orders.json` and its `SOURCE_ID` to continue the same
  orders cursor. On a fresh clone this file is absent, so the extractor starts
  at zero and backfills the current visible orders into the configured Bronze
  bucket. Do not copy a cursor from another source identity.
- Preserve Docker volumes to continue the same Kafka and Connect offsets. Use
  `docker compose down` for a normal stop; do not use `down -v` unless you mean
  to delete the local database and Kafka state.
- For a separate team demonstration, use an isolated source database, its own
  slot, a scratch Bronze bucket and dedicated Kafka consumer group IDs. The
  `.env` setting `CDC_KAFKA_GROUP_ID` can select a separate CDC sink group.
  The source consumer and GCS paths are not a safe place for concurrent test
  runs against shared production state.
- Re-running against the shared GCS bucket appends new objects. A fresh
  clickstream group can replay all retained messages; a fresh CDC Connect
  volume can take another initial snapshot. Do not add these copies together.

The SQL cursor only finds higher visible `order_id` values; CDC is the path for
row-level inserts, updates and deletes on the seven published tables. The
publication excludes `TRUNCATE`; treat one as a source reset and coordinate a
full snapshot and downstream reconciliation before resuming CDC.
