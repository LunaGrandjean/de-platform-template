# Albert's Marketplace data platform: group repo

This repository is your group's data platform for **Albert's Marketplace**, the
fictional European beauty marketplace of the Data Engineering & Cloud
Infrastructure course (MSA-DAT09-02, Albert School).

It starts almost empty. Every lesson adds a layer on top of the previous one. By
the end of the course it holds the platform your group defends orally:
- ingestion
- a bronze, silver and gold warehouse
- containerised jobs and orchestration
- infrastructure as code
- cost controls and a security baseline

## Team

| Name | GitHub | Role |
|---|---|---|
| | | |
| | | |
| | | |
| | | |

## How this repo grows

### Step 1: Start the Platform

| Deliverable | Content |
|---|---|
| [Data flows](docs/data-flows.md) | Sources, consumers, frequencies, formats and three risk questions |
| [Target architecture ADR](docs/adr/0001-target-architecture.md) | Comparison of warehouse, lake, two-tier and lakehouse architectures |
| [Architecture](docs/architecture.md) | Context diagram, current status and Bronze/Silver/Gold guarantees |
| [Daily-demand star](docs/model/star-daily-demand.md) | Business process, grain, dimensions and measures |

The Step 1 architecture is a design; subsequent deployment status is tracked
in the architecture document and the step checklists below.

Step 1 completion checklist:

- [x] Document the source flows and their risks.
- [x] Compare four architecture options and record the target decision.
- [x] Add an architecture diagram and layer contracts.
- [x] Sketch the daily-demand star schema.
- [x] Complete the team table with all members and GitHub handles.
- [ ] Add the cohort and group identifier.
- [ ] Verify a private group repository named `de-<cohort>-<group>` was created
  from `de-platform-template`. Verify its privacy, name and template origin;
  a personal branch alone does not meet this course requirement.
- [ ] Add every member and `Charlestng` as collaborators and verify that all
  invitations are accepted.
- [ ] Email the private repository URL and group names to
  `charlestanguypro@gmail.com`, with subject
  `MSA-DAT09-02 | L01-REPO | <cohort> | <group>`.
- [ ] Have every member push at least one commit.
- [ ] Merge at least one pull request after another member reviews it.

Keep the course repository and its access intact until marks are published.
Submit changes through reviewed pull requests. Commit history is part of the
assessment. Keep a separate commit for each project step.

### Step 2: The Bronze Layer

The shared bucket is `gs://data-engineering-i1-marketplace-group`, in project
`data-engineering-i1`, region `europe-west1`. The initial snapshot contains
seven tables, 1,296,708 raw rows and 19 Snappy Parquet files, partitioned by
ingestion date and unique load ID. No source columns or parsed values were
cleaned, deduplicated or dropped.

| Deliverable | Content |
|---|---|
| [Setup and verification](docs/bronze-setup.md) | Reproducible uv commands, actual load and remaining group checks |
| [Bronze layout ADR](docs/adr/0002-bronze-layout.md) | Partition/format/codec decisions and measured evidence |
| [Data dictionary](docs/data-dictionary.md) | All 39 source columns, source and physical types, personal-data flags |
| [Applied lifecycle](infra/lifecycle.json) | Live non-deleting storage-class transitions at 30, 90 and 365 days |
| [Actual IAM](infra/iam.md) | Verified bucket bindings and effective-access limitations |
| [Cost baseline](docs/costs.md) | Dated published prices, measured volumes and a 100x scenario |
| [Architecture](docs/architecture.md) | Actual bucket, region, layout and planned readers |

Cloud operations run through `uv` and the Python SDK using local Application
Default Credentials; no administrator installation or `gcloud` is required.
Generated CSV/Parquet data and credentials are excluded from Git. The committed
[evidence](docs/evidence/step2/) contains metadata, counts and checksums only.

Group-member Google access still needs individual verification.

### Step 3: Route 3 — Python ingestion (no Airbyte) — COMPLETE

**Step 3 is complete.** The orders cursor, clickstream consumer and Debezium
CDC path are implemented; live Bronze writes, CDC CRUD events and restart/resume
were verified. The point-in-time evidence is in
[`cdc-status.json`](docs/evidence/step3/cdc-status.json). For a fresh clone,
follow the [Step 3 reproduction guide](docs/runbooks/step3-reproduction.md).

The group selected Route 3 from the course setup: use a hand-written psycopg
extractor, Python Kafka consumers and Debezium Connect for CDC; Airbyte is not
part of this route.
`psycopg[binary]` is included in the locked project environment, installed with
`uv sync --locked`.

The operator-reported Route 3 run on 2026-11-05 first wrote 13 new orders in
one batch, advancing the cursor to 225168. The Bronze audit verified the
existing paths, then a one-shot catch-up wrote 237 rows through order ID
225405. The orders prefix now has 50 Parquet objects and 225,381 rows; the
current source had two more rows at the post-run check. There are also 129 main
clickstream objects (29,144 rows), two rows in the isolated Kafka test topic,
and one test DLQ object. The orders prefix includes a 225,000-row backfill from
ID 1, so it overlaps the Step 2 orders snapshot and must not be summed with it.
Details: [Bronze verification](docs/evidence/step3/bronze-verification.json)
and [Route 3 run evidence](docs/evidence/step3/route3-live-run.json).

| Deliverable | Content / status |
|---|---|
| [Ingestion guide](ingestion/README.md) | Locked dependencies, commands, checkpoints and delivery guarantees |
| [Orders extractor](ingestion/extract_orders.py) | Hand-written parameterised psycopg extraction with persistent cursor and bounded retries |
| [Kafka consumer](ingestion/consume_clickstream.py) and [schema v1](ingestion/clickstream.schema.json) | Parquet/DLQ implementation; main and isolated-test objects and Parquet footers verified in GCS |
| [Debezium CDC connector](ingestion/configure_cdc.py) and [Bronze consumer](ingestion/consume_cdc.py) | Existing PostgreSQL publication/slot into Kafka and append-only `bronze/cdc/` |
| [CDC runtime evidence](docs/evidence/step3/cdc-status.json) | Snapshot, seven-table output, slot/WAL, heartbeat, restart/resume and synthetic CRUD verification |
| [Route 3 evidence](docs/evidence/step3/route3-live-run.json) and [Bronze audit](docs/evidence/step3/bronze-verification.json) | Reported job run plus read-only GCS inventory, schemas, row counts and freshness check |
| [Failure modes](docs/ingestion-failure-modes.md) and [architecture](docs/architecture.md) | Cursor limitations, at-least-once delivery and layer contracts |

The orders cursor captures visible higher `order_id` values; it does not detect
updates, deletes or lower IDs that commit late. Debezium uses the existing
publication and slot for row-level CDC on seven tables; the Python CDC consumer
preserves raw envelopes and tombstones with Kafka provenance in `bronze/cdc/`.
See [CDC runtime evidence](docs/evidence/step3/cdc-status.json). The Step 4
warehouse will consume the selected Bronze paths.

The evidence counts and object names describe the source and GCS bucket at
their recorded timestamps. A fresh clone creates new local state and later
objects, so it reproduces the pipeline and checks, not byte-identical counts.

## Start it

You need Docker Desktop (or Docker Engine with Compose v2), about 6 GB of free
disk and 3 GB of free RAM.

```
git clone https://github.com/TangoAI-AlbertSchool-Data-Eng-Cloud/albert-marketplace.git
cd albert-marketplace
docker compose up
```

The first start downloads the dataset (a 66 MB database dump, published as a
release asset) and restores it, which takes a minute or two. Later starts find
the data already there and skip it. Then:

| What | Where | |
|---|---|---|
| L01 · Architecture decision frame | Data-flow matrix, first ADR, architecture diagram v1, star schema sketch | `docs/` |
| L02 · Object storage foundations | Bronze layer on Cloud Storage, data dictionary v1 | `docs/` |
| L03 · Batch, streaming, CDC | Ingestion pipelines | `ingestion/` |
| L04 · SQL warehouses | Warehouse tables, three costed queries | `docs/` |
| L05 · dbt and the medallion pattern | Silver and gold models, tests | `dbt/` |
| L06 · Packaging data jobs | Dockerfiles for the ingestion job and dbt | `ingestion/`, `dbt/` |
| L07 · Airflow | A scheduled DAG | `orchestration/` |
| L08 · Terraform | The platform as code, `terraform plan` in CI | `infra/`, `.github/workflows/` |
| L09 · FinOps | Labels, a budget alert, one measured optimisation | `infra/`, `docs/` |
| L10 · Security and governance | Least-privilege access, PII redaction, data classification | `infra/`, `dbt/`, `docs/` |

Starter files for later lessons arrive as **pull requests** opened by the
teaching team. Review and merge them like any other change.

## How we work

- **Every change goes through a pull request,** reviewed by another member
  before it reaches `main`.
- **One decision, one ADR** in `docs/adr/`. Every member writes at least one. At
  the oral defence, each of you defends a technical choice unprepared.
- **The commit history is graded.** Commit in small steps, with messages that
  say why.
- **Never commit secrets:** no `.env` files, no service-account keys.
  `.gitignore` blocks the usual names; it cannot catch everything.
- **Keep `docs/architecture.md` current.** It is the diagram you present at the
  defence.

## Layout

```
docs/
  architecture.md    target architecture and medallion layers
  data-flows.md      data-flow matrix
  adr/               architecture decision records
  model/             dimensional model sketches
ingestion/           from L03
dbt/                 from L05
orchestration/       from L07
infra/               from L08
```

From another container on your machine, the database is `host.docker.internal:5432`
and Kafka is `host.docker.internal:29092`.

PostgreSQL runs with `wal_level=logical`; Debezium Connect reads from the
existing `marketplace_bronze_slot`. The orders cursor remains separate and does
not consume WAL. Monitor connector health and retained WAL; the configured
512 MB cap can invalidate a lagging slot.

## Ownership

**No licence is granted for this repository.**
- **The template:** this README, the `docs/` templates and the folder structure
  remain the course author's intellectual property. Do not reuse them outside
  the course.
- **Your work:** what your group writes here belongs to your group.
