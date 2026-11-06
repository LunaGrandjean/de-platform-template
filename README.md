# Albert's Marketplace data platform

This is our group's data platform for Albert's Marketplace, a fictional European
beauty-products marketplace used in the Data Engineering & Cloud Infrastructure
course at Albert School. Steps 1 and 2 document the platform design and establish
the raw Bronze layer in Google Cloud Storage.

The repository also includes the provided marketplace source system: its
operational PostgreSQL database, storefront and generators for historical data
and live traffic. Those components support the later ingestion exercises. The
course template supplies the group-repository structure and collaboration files.

**Everything in it is made up.** Every customer, address, phone number,
password and payment card is synthetic. Product and review text comes from a
public research dataset, and images are URLs into someone else's host, never
copies. See [`docs/dataset-card.md`](docs/dataset-card.md).

## Student data platform

The course platform documentation is developed on branch `Luna`. The source
system below provides its PostgreSQL data, released CSV extracts and Kafka
events. The platform targets daily units sold by product and country.

### Team

| Member | GitHub handle | Contribution |
|---|---|---|
| Luna Grandjean | [@LunaGrandjean](https://github.com/LunaGrandjean) | Initial platform design and documentation |
| Omar Abdellatif | [@abdellatifomar61-ship-it](https://github.com/abdellatifomar61-ship-it) | To be documented |
| Malek Saoudi | [@Msaoudi115](https://github.com/Msaoudi115) | Step 3 live Docker/Debezium deployment and evidence |
| Anas Mossad | [@realay04](https://github.com/realay04) | To be documented |
| Emile Gadenne | [@egadenne-ai](https://github.com/egadenne-ai) | Step 4 BigQuery warehouse, costed queries and ADR 0004 |

We are group 7 (`g07`). The cohort identifier has not been provided yet. Add it
before submitting the group repository.

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
See [CDC runtime evidence](docs/evidence/step3/cdc-status.json). Step 4 exposes the Python orders and main clickstream prefixes as external
tables; native sales tables still use the pinned Step 2 snapshot.

The evidence counts and object names describe the source and GCS bucket at
their recorded timestamps. A fresh clone creates new local state and later
objects, so it reproduces the pipeline and checks, not byte-identical counts.

### Step 4: SQL Warehouse

Bronze stays in the bucket and is read through seven BigQuery external tables
in `g07_bronze`. The two sales tables are copied, typed, into `g07_warehouse`,
partitioned by month and clustered by product. Measured with the cache off, a
month of daily demand reads 49x fewer bytes natively than from Bronze, and
filters that cannot prune are rejected before they run. The native tables cover
the Step 2 snapshot; the Python and main clickstream paths are separate
external tables. Malek's Debezium implementation and live evidence are included.
Review fixes add the MV SQL, corrected costs, fail-closed prechecks, pricing
comparison and incident runbook. See [live validation status](docs/evidence/step4/validation-status.json)
for outstanding BigQuery checks; a local test is not cloud deployment proof.

| Deliverable | Content |
|---|---|
| [Tuning and cap verification](docs/warehouse-tuning.md) | Processed-byte comparisons, aggregation ratios and reproducible live checks |
| [Daily-demand MV](warehouse/daily-demand.sql) | Provisional product/day aggregation; deployment tracked in evidence |
| [Pricing decision](docs/adr/0004-warehouse-pricing.md) | Workload assumptions, capacity break-even and review thresholds |
| [Slow-query runbook](docs/runbooks/slow-query.md) | Ordered investigation, evidence and escalation |
| [Warehouse and costed queries](docs/warehouse.md) | Datasets, tables, reproduction commands and three queries measured in bytes and dollars |
| [Warehouse ADR](docs/adr/0004-warehouse-tables.md) | External versus native tables, partition grain, clustering and the empty-column workaround |
| [Warehouse script](scripts/warehouse.py) | `external`, `native` and `measure` commands, with row-count and cast prechecks |
| [Evidence](docs/evidence/step4/) | External and native table metadata, job IDs and query measurements |
| [Dictionary](docs/data-dictionary.md) and [architecture](docs/architecture.md) | Warehouse schemas and updated status |

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
| Storefront (the shop) | http://localhost:8510 | Streamlit |
| Storefront API | http://localhost:8100/docs | FastAPI, [endpoints](storefront/README.md) |
| PostgreSQL | `localhost:5432` | database `marketplace`, user `postgres`, password `postgres` |
| Kafka | `localhost:9092` | topic `clickstream` |

The load generator starts with everything else: it shops through the API on a
simulated clock, so orders, payments and shipments keep arriving in the
database, and clickstream events keep arriving on the Kafka topic.

Stop it with `Ctrl+C`, and `docker compose down` to remove the containers. The
data lives in a Docker volume and survives both; `docker compose down -v`
deletes it, and the next start downloads the dataset again.

## Connect to it

From your machine:

```
psql postgresql://postgres:postgres@localhost:5432/marketplace
```

From another container on your machine (Airbyte, Airflow, dbt), the database is
`host.docker.internal:5432` and Kafka is `host.docker.internal:29092`.

PostgreSQL runs with `wal_level=logical`, so change data capture works: create a
replication slot, or point Airbyte or Debezium at it. A slot can become invalid when retained WAL exceeds the 512 MB setting;
it is not automatically dropped. Monitor the group slot and coordinate any
resnapshot rather than dropping it during routine operation.

## What is in here

| | |
|---|---|
| [`db/`](db/) | The schema, as dbmate migrations, and `load_legacy.sql`, which loads the original CSV extract with its defects |
| [`storefront/`](storefront/) | FastAPI backend and Streamlit shop ([README](storefront/README.md)) |
| [`generators/`](generators/) | `history/` builds the order history; `live/traffic.py` is the load generator |
| [`tests/`](tests/) | The checks behind the build spec's verified claims |
| [`docs/`](docs/) | [Dataset card](docs/dataset-card.md), [build spec](docs/build-spec.md), and `critique/`: original files the course picks apart |
| `data/` | Downloads and generated files. Never in git |

The schema is the contract between all of it: 25 tables from the original
extract, plus `ORDER_ITEMS` (order lines) and `PAYMENT` (one transaction per
order).

## Settings

Every setting has a default, so `docker compose up` needs no `.env`. Override
any of them in the environment or in a `.env` file next to `compose.yaml`.

| Variable | Default | |
|---|---|---|
| `DB_PORT`, `API_PORT`, `UI_PORT` | 5432, 8100, 8510 | host ports, if one is taken |
| `KAFKA_PORT`, `KAFKA_DOCKER_PORT` | 9092, 29092 | from your machine, from another container |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | postgres, postgres, marketplace | |
| `SEED_URL` | the release asset | set it empty to start on an empty database |
| `CLOCK_ACCELERATION` | 15 | how much faster than real time the live traffic runs |
| `ORDERS_PER_DAY` | 200 | orders per simulated day |
| `TRAFFIC_SEED` | 20260915 | the load generator's seed |
| `SIMULATION` | true | lets the load generator date orders on its own clock. Off, the storefront uses the real time |

To run without the live traffic:
`docker compose up db seed migrate storefront-api storefront-ui`.

## Rebuild the dataset

You do not need this to follow the course: the release asset is the dataset.
It is here because the course reads it.

The `build` profile holds the services that make the data. They need the
original CSV extract in `LEGACY_CSV_DIR` and, for the calibration, Online
Retail II in `RETAIL_XLSX_DIR`.

```
docker compose --profile build run --rm load-legacy        # CSVs -> database
docker compose --profile build run --rm calibrate          # -> history/calibration.json
docker compose --profile build run --rm fetch-places       # GeoNames -> history/places.json
docker compose --profile build run --rm generate-history --seed 7 --end-date 2026-09-15
docker compose --profile build run --rm release            # -> data/release/
```

`--scale` changes how many orders those three years hold, without touching the
customers or the catalogue: `--scale 0.1` for a tenth of them (a small copy for
a laptop or for CI), `--scale 5` for five times as many. Every rule the full
dataset follows still holds, duplicates included.

The history generator is deterministic: the same seed, end date and scale give
the same database every time. Tear these down with
`docker compose --profile build down`; a plain `down` leaves their containers
behind.

## Support tickets

120 messages to the company's support inbox, each labelled with the team that
should handle it, for the LLM course. They are a separate release,
[`tickets-v1.0.0`](https://github.com/TangoAI-AlbertSchool-Data-Eng-Cloud/albert-marketplace/releases/tag/tickets-v1.0.0):
download `tickets_v1.csv`. A language model wrote the wording from real rows of
the dataset; a person checked every label. How they were made, and how to
rebuild the first sixty without calling the model:
[docs/tickets-card.md](docs/tickets-card.md).

## Licence and attribution

The course template files and structure remain the course author's intellectual
property and are supplied for this course only. No licence is granted for those
template materials; the group's work belongs to the group, as stated in the
original template README. The source-system licences and attributions below
apply to the imported marketplace code and dataset.

- **Code: MIT** ([LICENSE](LICENSE)).
- **Data: CC BY-SA 4.0.** It is derived from:
  - McAuley Lab's Amazon Reviews'23: Hou, Y., Li, J., He, Z., Yan, A., Chen, X.,
    McAuley, J. (2024). *Bridging Language and Items for Retrieval and
    Recommendation.* arXiv:2403.03952
  - UCI Online Retail II, used for calibration only: Chen, D. (2012). *Online
    Retail II* [Dataset]. UCI Machine Learning Repository.
    https://doi.org/10.24432/C5CG6D. CC BY 4.0
  - GeoNames postal codes (https://www.geonames.org/), CC BY 4.0, for addresses
  - Eurostat population figures, for the country and region mix

The storefront and the original schema began as
[amazon-database-design](https://github.com/JedhaBootcamp/amazon-database-design)
and
[amazon-mockup-e-commerce](https://github.com/JedhaBootcamp/amazon-mockup-e-commerce).
