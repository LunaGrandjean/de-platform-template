# ADR 0004: Read Bronze through external tables and query sales from month-partitioned native tables

- **Status:** accepted
- **Date:** 2026-11-05
- **Author:** Emile Gadenne
- **Lesson:** L04

## Context

Step 2 landed seven raw tables (1,296,708 rows, all fields as strings) as
Parquet in `gs://data-engineering-i1-marketplace-group`, load
`20261026T105159Z-2efcb86a`. Step 4 must make them queryable in BigQuery
(`data-engineering-i1`, `europe-west1`, on-demand billing at $7.50 per TiB
scanned) and show what the table layout does to the bill. Facts that shaped the
decision, all measured on 2026-11-05 (see [costed queries](../warehouse.md)):

- The sales history covers 37 months, 2023-09-15 to 2026-09-14. The target
  question (ADR 0001) is daily units per product and country, usually asked for
  a bounded period.
- `order_items` has no date of its own; its date is the parent order's.
- Six of the seven Parquet tables carry the legacy CSV index as a column with an
  **empty name**. BigQuery refuses to infer a schema from such a file
  ("invalid empty field name"). ADR 0002 keeps that column on purpose.
- In `orders`, `order_id` is unique (0 duplicate IDs) and every date, quantity
  and price parses; no order line is orphaned. Business-duplicate orders with
  different IDs remain, as Bronze intends.
- At this size a month of order lines is about 0.6 MB, far below the 64 MB
  where Google expects clustering to matter.

## Options considered

| Option | For | Against |
|---|---|---|
| A. Query Bronze only through external tables | No copy, nothing to refresh | Every query re-reads whole files: no partition pruning, no clustering, no cache. Dry runs report 0 bytes, so cost cannot be checked in advance |
| B. Load all seven tables natively | Fast, uniform | Two copies of the archive; pays warehouse storage for tables nobody aggregates yet |
| C. External tables for all of Bronze, plus native month-partitioned tables for the two sales tables | Bronze stays the single archive; the tables queried by date get pruning and clustering | Native tables must be rebuilt when a new load arrives |
| D. Rewrite Bronze without the empty-named column | Lets BigQuery infer schemas | Changes Bronze, breaks ADR 0002's "values and columns preserved" guarantee |

Partition grain was compared the same way: **day** would give about 1,100
partitions of about 20 KB; **year** would give 4 partitions that a one-month
question must read whole; **month** gives 37 partitions and matches the
questions' usual period.

## Criteria

- **Bytes scanned per question** (the bill on demand): C reads 49x less than A
  for a month of daily demand and 24x less for a quarter of order counts.
- **Bronze stays the one archive** (lesson 2, ADR 0002): A and C keep it; B
  duplicates it; D alters it.
- **Mistakes made impossible, not just discouraged:** only native tables accept
  `require_partition_filter`, which rejected both non-pruning filters measured
  (`FORMAT_DATE` and a subquery) before they read anything.
- **Effort and reproducibility:** one `uv` script (`scripts/warehouse.py`),
  ADC credentials, no extra tools, row counts checked against the Step 2
  manifest.

## Decision

Keep Bronze in the bucket and expose all seven tables as **external tables** in
`g07_bronze`, pinned to the manifest's load, with an **explicit STRING schema**
that names every column except the empty-named CSV index (left unread, never
removed). Build two **native tables** in `g07_warehouse`: `orders` and
`order_lines` (order items with their parent `order_date`), both **partitioned
by month** of `order_date` with `require_partition_filter`, and `order_lines`
**clustered by `p_id`**. They are typed copies, not Silver: no row is removed.

## Consequences

- **Easier:** period questions read only their months; an unfiltered or
  non-pruning query fails at no charge; all queries carry a 1 GB
  `maximum_bytes_billed` cap. Storage is about 33 MB, inside the free 10 GiB.
- **Clustering is modest here:** it cut a rare product's year from 3.8 MB to
  1.6 MB, but a best seller sold every month still reads all 3.8 MB. It is kept
  because it costs nothing and its benefit grows with partition size; it will
  be re-measured when Step 3's live data arrives.
- **At this scale the bill is flat:** BigQuery bills at least 10 MB per table
  referenced, so every query above was billed 10 or 20 MB (about $0.0001). The
  ratios, not the dollars, are what carry to larger data.
- **To watch:** native tables are a snapshot of one load and must be rebuilt
  (`uv run scripts/warehouse.py native`) after each new completed Bronze load,
  until Step 7 schedules it. Cost estimates for anything reading external tables
  need a real run, because dry runs report 0 bytes for them.
- **Out of scope:** duplicate business orders, country mapping and the
  daily-demand star are Silver and Gold work (Lesson 5). No materialised view
  was built: the product-by-day grain barely aggregates.
