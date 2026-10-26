# ADR 0001: Use a two-tier lake and warehouse architecture

- Date: 2026-10-26
- Status: Proposed for group review
- Author: Luna Grandjean
- Scope: Target design for the course data platform; no deployment asserted

## Context

Albert's Marketplace produces relational transactions in PostgreSQL and JSON
clickstream events in Kafka. The first analytical question is daily units sold
by product and country. Raw arrivals must remain available for replay and
audit, including the source dataset's deliberate defects. The group needs a
design that can be explained and operated using Python and SQL, with storage
and query costs measured in later steps.

The project requires partitioned Parquet in a shared GCS bucket in
`europe-west1`, followed by selected native BigQuery tables. These constraints
favour a two-tier design, but the comparison below records why it also fits
the workload. It does not claim that other architectures are universally worse.

## Options and scoring

Scores run from 1 (poor fit) to 5 (strong fit). All six criteria have equal
weight. These are design judgements for this small course project, not
measured benchmarks or price quotations.

| Criterion | Warehouse | Lake | Two-tier: lake + warehouse | Lakehouse |
|---|---:|---:|---:|---:|
| Workload: repeated SQL joins and daily aggregations | 5 | 2 | 5 | 4 |
| Data types: relational tables plus raw JSON events | 3 | 5 | 5 | 5 |
| Freshness: hourly ingestion and daily reporting targets | 4 | 3 | 4 | 4 |
| Skills: Python and SQL with limited platform administration | 5 | 3 | 4 | 2 |
| Cost control: small workload, bounded scans and simple storage | 3 | 4 | 4 | 2 |
| Governance: raw evidence plus controlled analytical definitions | 3 | 2 | 4 | 4 |
| **Total / 30** | **23** | **19** | **26** | **21** |

### Warehouse

Load source data directly into warehouse-managed tables. This gives analysts
one SQL environment and few components. It is a weaker fit for this project
because we also need an independently retained, replayable raw arrival layer;
direct loads alone do not satisfy that requirement. A warehouse with a separate
raw object store becomes the two-tier option.

### Lake

Keep Parquet and event data in object storage and query it through an external
engine. This gives flexible, inexpensive raw retention, but the group would
still have to establish reliable table management, consistent joins and a
single demand definition. Querying raw files alone leaves those concerns
unresolved and does not meet Step 4's native-table requirement.

### Two-tier lake and warehouse

Retain raw Parquet in GCS and load selected working tables into BigQuery.
Python handles ingestion; SQL handles curation and reporting. This separates
raw evidence from the serving model while matching the course's storage and
warehouse requirements. The price is an extra copy of selected data, load
coordination and two access-control surfaces.

### Lakehouse

Use an open transactional table format, a catalogue and a compatible query
engine over object storage. This could combine lake flexibility with managed
table semantics. The extra choices and operating work are difficult to justify
for our current daily-demand workload. Reconsider if multiple engines need
shared transactional tables or if the team develops those operating skills.

## Decision

Choose the **two-tier lake and warehouse architecture**:

1. Keep the provided PostgreSQL/storefront/Kafka stack as the operational source.
2. Use one shared GCS bucket in `europe-west1` for raw Bronze Parquet. Select
   the partition key and compression through Step 2 measurements.
3. Add three ingestion paths in Step 3: a PostgreSQL connector for the seven
   required tables, a separate incremental Python `orders` extractor and a
   validating Kafka consumer with a dead-letter path.
4. In Step 4, load the tables justified by the SQL workload into native
   BigQuery working tables, colocated in the chosen region. Preserve GCS
   Bronze as arrival evidence.
5. Build Silver curation and the Gold daily-demand star in Lesson 5. Use a
   single authoritative relational path for sales; keep clickstream analysis
   separate from sales quantities.

The freshness goals are hourly relational batches and daily Gold refreshes.
They are proposed objectives, not observed service guarantees. Kafka can be
consumed continuously without promising real-time report updates.

## Trade-offs and consequences

- **Raw integrity:** Bronze retains admitted records, including duplicates and
  defects. Exclude password and card-secret columns before landing; raw
  integrity applies to the explicitly selected ingestion scope.
- **Replay and lineage:** identify source, stream and ingestion batch. Keep
  manual snapshot, connector and custom-extractor outputs separate. Record
  the handover from initial snapshots to automated ingestion.
- **Correctness:** warehouse loading does not fix duplicates or address fanout.
  Silver must reconcile business duplicates, handle ingestion replay and
  produce at most one country mapping per buyer before Gold aggregation.
- **Cost:** GCS storage, ingestion operations, warehouse storage and query scans
  all contribute. We accept selected duplicate storage, measure it in Step 2
  and Step 4, and use query byte caps before recurring analytical use.
- **Access:** restrict raw personal fields to ingestion/curation roles and
  expose aggregate Gold data to analysts. Commit verified IAM evidence later.
- **Operations:** a failed load must not advance the source cursor or publish
  an incomplete reporting batch. Step 3 will define per-path delivery and
  restart guarantees; Step 4 will document query investigation and cost caps.
- **Portability:** Parquet retains a portable replay source. The SQL serving
  layer introduces BigQuery-specific configuration and tuning work.

## Reconsideration triggers

Review the decision if daily reports miss the agreed refresh window, measured
warehouse/load costs exceed the group's budget, or a new use case needs
sub-minute reporting. Also review it if multiple engines require updates to
the same curated lake tables. Establish numerical cost and scan thresholds
from actual Step 2/4 measurements rather than inventing them here.

## Validation still required

Step 2 must measure partition layouts, sizes, object counts and query times.
Step 3 must test restart duplicates, missed cursor changes and failure modes.
Step 4 must compare uncached bytes processed, aggregation collapse ratios,
pricing and an intentionally exceeded query cap. None of these results is
claimed by this design ADR.

## Related documents

- [Flow matrix and risks](../data-flows.md)
- [Architecture and layer contracts](../architecture.md)
- [Daily-demand star schema](../model/star-daily-demand.md)
