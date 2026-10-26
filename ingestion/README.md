# Step 3: Three Roads into Bronze

The Python implementation and local fault-injection checks are ready. **Live
PostgreSQL, Kafka and Airbyte validation is pending** on a teammate's machine.
Docker cannot currently start on Luna's managed Windows laptop. Local test
success must not be recorded as proof of three deployed ingestion paths.

## Responsibilities

| Owner | Work |
|---|---|
| Luna | Python jobs, schema, local tests, failure-mode analysis and documentation |
| Docker operator: group member to assign | Start the provided source stack, configure Airbyte, arrange approved access and collect live evidence |
| Group / project administrator | Authorise GCP identities and connector credentials; review access and key revocation |

Use the [Docker handoff](../docs/runbooks/step3-docker-handoff.md) for the live
setup. Each member uses their own credentials. No account/password/key files
are shared through Git.

## Install and check locally with uv

From the repository root, use Python 3.12 and the locked dependencies:

```powershell
$env:UV_CACHE_DIR = Join-Path (Get-Location) 'data/uv-cache'
uv sync --locked
uv run --locked -m unittest discover -s tests -p test_ingestion.py -v
```

These tests use synthetic rows, a local object sink and fake SQL fetch / Kafka
commit callbacks. They exercise real Parquet writing, checkpoints, validation
and failures, without a broker, database or cloud call. The
[local evidence](../docs/evidence/step3/local-checks.json) records that limited
scope. The older Bronze-preservation checks remain in `tests/test_bronze_snapshot.py`.

## Runtime configuration

Copy `.env.example` to the ignored `.env`, then fill the PostgreSQL connection
and the operator's Kafka settings. `python-dotenv` loads this file without
overwriting existing environment variables. For cloud writes, set
`GCP_PROJECT_ID` and `BRONZE_BUCKET` and use your existing local ADC. No Google
Cloud CLI installation is required by the Python jobs.

The example's plaintext/no-SSL settings are for the operator's local Docker
stack or an approved encrypted tunnel. They are not settings for exposing
PostgreSQL/Kafka directly on the public internet. The simplest setup is to run
all live jobs on the Docker operator's machine; Luna can write and test the
code without Docker and connect remotely later if authorised access exists.

## Path 1: PostgreSQL connector

Follow [the handoff](../docs/runbooks/step3-docker-handoff.md). The
[connection plan](airbyte/connection-plan.json) is a human-readable proposal,
**not an applied Airbyte export**. It selects the seven required tables, CDC,
Incremental/Append, Parquet/Snappy and a dedicated `bronze/postgres/` prefix.
`customer` and `payment_details` are unselected; their secret fields never need
to reach Bronze. Do not enable automatic selection of newly discovered tables.

Once the operator exports the actual configured catalog to ignored local data,
check it and produce a credential-free selection summary:

```powershell
uv run --locked ingestion/validate_connector_export.py data/airbyte-configured-catalog.json
```

The gate rejects missing/extra selected tables, secret columns, overwrite modes
and accidental field omissions. A passing catalog is still not evidence of a
successful sync: commit redacted live settings, actual job IDs and object counts.

## Path 2: Hand-written SQL extractor

Once the real database is reachable:

```powershell
uv run --locked ingestion/extract_orders.py --once
uv run --locked ingestion/extract_orders.py
```

The first command drains one bounded order-ID window. The second keeps polling
every 60 seconds. Both use psycopg directly, parameterised SQL and a read-only
connection. Initial connection failures use up to five attempts, with waits of
1, 2, 4 and 8 seconds. A query/storage/authentication failure stops the job;
restart it after resolving the cause. No successful checkpoint is discarded.

Cursor state is `data/state/orders.json`, with an OS lock preventing concurrent
writers on that file. One operator owns this state file. Upload each batch,
then atomically save the cursor; a crash in between can duplicate rows. Do not
delete or share the checkpoint casually. Change `SOURCE_ID` and choose a new
state file deliberately after a source rebuild. A smaller source maximum is
detected, but other source resets cannot always be detected.

The `order_id` cursor captures new visible higher IDs. It misses updates,
deletes and lower IDs committing after the watermark has advanced. It does
not add Airbyte metadata or remove business duplicate orders. This path is
kept separate from the authoritative connector stream.

For testing a real SQL connection without writing to GCS, use **a separate test
state file** so local-only output cannot advance the production cursor:

```powershell
uv run --locked ingestion/extract_orders.py --once --local-output data/step3-local --state-file data/state/orders-local-test.json
```

The CLI rejects local output without an explicit state file different from its
default GCS state file. Keep any other production checkpoint separate as well.

## Path 3: Kafka consumer

Once Kafka is reachable:

```powershell
uv run --locked ingestion/consume_clickstream.py
```

The consumer enforces [schema v1](clickstream.schema.json), including UUIDs,
timestamps, event-specific fields and nested order items. Unknown versions or
fields, invalid JSON/UTF-8, duplicate JSON keys and null messages are rejected.
Accepted Parquet stores the complete original bytes plus source fields and
transport metadata; optional/nested event fields remain in `raw_value`.

Partition accepted events by **UTC `event_time`**, because it represents the
simulated business event; `emitted_at` is the producer's real wall clock. Late
events may therefore create files in older partitions. Preserve both original
timestamps and the exact payload. Invalid events use rejection date and retain
the original bytes as base64, rejection reason and timestamp under `bronze/_dlq/`.

Auto-commit is disabled. All valid and DLQ objects in a poll must be durable
before synchronously committing each partition's **next** offset. Failed writes
prevent commit; a crash after writes or a failed commit can cause duplicates on
replay. This is at-least-once within Kafka retention, not exactly-once. A new
consumer group starts at the earliest retained event. Expired offsets can reset
to that earliest available offset, so loss beyond retention is not recovered.

For local-sink tests of a real broker, use a separate group ID so local writes
cannot advance the production consumer group:

```powershell
uv run --locked ingestion/consume_clickstream.py --local-output data/step3-local --group-id marketplace-local-test
```

The CLI rejects local output if no separate group ID is supplied or if it equals
the configured GCS consumer group.

The [live malformed-event procedure](../docs/runbooks/step3-docker-handoff.md)
uses a separate topic and expects two valid events and one DLQ event. It has
not been run on this laptop. In Lesson 6, the group will package these jobs in
Dockerfiles; this step does not add new job images.
