# Step 4: warehouse tuning, materialisation and cost controls

Project `data-engineering-i1`, `europe-west1`. See the
[warehouse inventory and SQL](warehouse.md), [layout ADR](adr/0004-warehouse-tables.md),
[pricing ADR](adr/0004-warehouse-pricing.md), and
[slow-query runbook](runbooks/slow-query.md).

## Selected native tables

The seven external snapshot tables in `g07_bronze` read the pinned Step 2 load
`20261026T105159Z-2efcb86a`. `orders` (222,644 rows) and `order_items` (408,560)
are the repeatedly filtered sales inputs. Native `g07_warehouse.orders` is a
typed copy; `order_lines` adds the parent order date so lines can be pruned by
month. Both require a partition filter; only `order_lines` clusters by `p_id`.
The six prechecks must all be zero before replacing either native table.
Failure leaves current tables intact; it never repairs Bronze silently.

The less-used `review`, `product_reviews`, `product`, `customer_shipping` and
`shipping_details` stay external because the three current questions do not
need native working copies. The Step 3 Python orders and main clickstream
prefixes are external too. They grow independently and overlap the snapshot
or describe behaviour. Malek's seven-table Debezium CDC remains raw in GCS;
its envelopes, deletes and replay offsets need Lesson 5 reconciliation before
they feed a sales table. The initial CDC snapshot, Python backfill and Step 2
snapshot must never be summed together.

## Uncached processed-byte evidence

The following reproduce Emile's original 2026-11-05 measurements in actual
2026-11-06 jobs **before MV creation**, with `use_query_cache=False` and
`cache_hit=false`, preserved in [before-MV evidence](evidence/step4/costed-queries-before-mv.json). Ratios are **before /
after**; they use processed bytes, not billed bytes. These external-to-native
comparisons measure the combined effect of storage format, types and layout,
not partitioning or clustering in isolation.

| Table | Change | Same business query | Bytes before | Bytes after | Ratio |
|---|---|---|---:|---:|---:|
| `orders` + `order_lines` | External join → typed monthly native lines | Q1: units/product/day, Aug 2026 | 13,577,448 | 277,004 | 49.015× |
| `order_lines` | External join → monthly clustered native | Q2: rare product/month, Sep 2025–Aug 2026 | 8,879,045 | 1,625,204 | 5.463× |
| `orders` | External → typed monthly native | Q3: monthly order count, Q3 2026 | 2,671,728 | 109,808 | 24.331× |

The rare and popular products in the original evidence are **different
queries**, so their 2.3× difference alone does not isolate the clustering
effect. `uv run --locked scripts/warehouse.py tune` adds a controlled
comparison of the **same product/date/price predicate** against a temporary
unclustered monthly copy and the shared clustered table. It saves both SQLs,
job IDs, cache flags, processed bytes and ratios, including an all-products
control without a product filter (clustering cannot help that predicate), to
`docs/evidence/step4/controlled-tuning.json`. The private scratch name expires
after one hour and is deleted in `finally`. The price predicate makes the
daily SUM(qty) MV ineligible to answer this comparison.

Keep any observed **1.0×** result in that file and explain it: tiny partitions
or a hot product present in every storage block can leave nothing to prune.
Do not infer a measured 1.0× result merely from a dry-run upper bound. The
original popular-product job read its full estimated bytes, but it has no
unclustered control. Live completion status is recorded in
[validation-status.json](evidence/step4/validation-status.json).

The 2026-11-06 controlled runs completed with cache off and the MV excluded
by the extra price predicate ([SQL, job IDs and execution reads](evidence/step4/controlled-tuning.json)):

| Monthly `order_lines` query | Unclustered bytes | Clustered bytes | Ratio |
|---|---:|---:|---:|
| Rare product `0990995119` | 5,955,136 | 2,553,892 | **2.331789?** |
| Best seller `B085BB7B1M` | 5,955,136 | 5,955,136 | **1.0?** |
| All products, no product predicate | 4,331,008 | 4,331,008 | **1.0?** |

Clustering saved reads for the rare product. It saved nothing for the hot
product that spans the eligible blocks, or when there is no product filter.
The different selected columns explain why these controlled byte counts are
larger than Q2 below. The scratch table was removed after the benchmark.


## Daily demand and collapse ratio

The committed [MV SQL](../warehouse/daily-demand.sql) defines product/day
`SUM(qty)` on the native snapshot, partitioned by month and clustered by
product. It retains business duplicates and is a provisional performance
aggregate; country mapping and final sales reconciliation remain Lesson 5.

An independent **2026-11-06 actual GCS read** of selected columns in the pinned
snapshot measured the following ([objects, generations and counts](evidence/step4/snapshot-collapse.json)):

| Grain | Order lines | Distinct aggregate keys | Lines per aggregate row |
|---|---:|---:|---:|
| Product/day | 408,560 | 356,981 | **1.144487×** |
| Product/month | 408,560 | 165,141 | **2.474007×** |

Daily aggregation reduces row count by only **12.62%**. Monthly reduces it by
**59.58%** but loses the daily detail required by the target question. Keep
the daily MV to satisfy the current daily workload and demonstrate the
materialisation trade-off, with modest savings expected. Do not claim the
collapse ratio equals scan savings: the original line has more columns, and
BigQuery can use query rewriting. Record actual MV/base processed bytes.

`uv run --locked scripts/warehouse.py materialize` creates the MV, explicitly
refreshes it, computes both ratios in BigQuery, compares the actual August
results to the base aggregation, and records table type, refresh status,
job IDs and uncached measurements in `materialization.json`. The source-read
measurement above is independently confirmed by the successful BigQuery run.
The MV is now deployed as `g07_warehouse.daily_demand`; its type is
`MATERIALIZED_VIEW`, last-refresh time and job IDs are in
[materialization.json](evidence/step4/materialization.json). All **8,649** August
product/day rows and their units agree with an independent base-table query.
That check also computes `SUM(price_at_purchase)`, absent from the MV, so its
execution plan demonstrably reads `order_lines` rather than reusing the MV. Background refresh is disabled
for this fixed snapshot; after an intentional base-table replacement, recreate
the dependent MV if BigQuery invalidates it, then rerun the command. Do not
drop the shared base during normal validation. No scheduled refresh is claimed.

## Three questions and nominal costs

All SQL is in [the script](../scripts/warehouse.py) and the recorded evidence.
The following preserves the **before-MV** costs for a clean layout comparison.
The course estimate is `bytes_processed / 2^40 * $7.50`, before allowance:

| Business question | External processed bytes | Native processed bytes | Native/external ratio | External USD | Native USD |
|---|---:|---:|---:|---:|---:|
| Q1: Which products sold how many units each day in Aug 2026? | 13,577,448 | 277,004 | 1 / 49.015 | 0.000092615 | 0.000001889 |
| Q2: How did one rare product sell each month over a year? | 8,879,045 | 1,625,204 | 1 / 5.463 | 0.000060566 | 0.000011086 |
| Q3: How many orders were placed each month in Q3 2026? | 2,671,728 | 109,808 | 1 / 24.331 | 0.000018224 | 0.000000749 |

Evidence keeps the separate `usd_from_billed_bytes_before_allowance` value,
which includes BigQuery billing minima. These small native queries were billed
10 MiB each and external joins 20 MiB; the processed-byte cost illustrates
layout benefits while the invoice may remain flat. The first 1 TiB/month is
free at the applicable account, shared with other workloads; do not assume
the group still has all of it. See the pricing ADR for workload assumptions,
capacity scenarios and numerical crossover/reconsideration thresholds.


## After MV creation: automatic rewrite is visible

A fresh uncached run ([current query evidence](evidence/step4/costed-queries.json))
shows BigQuery smart tuning automatically reading `daily_demand` for Q1 and Q2,
even though their SQL names `order_lines`. The committed `read_steps` prove
the physical input. Q3 continues reading `orders`.

| Query | External bytes | Current native/MV bytes | Before/after ratio | Current nominal USD |
|---|---:|---:|---:|---:|
| Q1: units/product/day in August | 13,577,448 | 242,172 | 56.065? | 0.000001651906 |
| Q2: rare product/month over a year | 8,879,045 | 1,419,544 | 6.255? | 0.000009683008 |
| Q3: orders/month in Q3 | 2,671,728 | 109,808 | 24.331? | 0.000000749023 |

Q1 fell from 277,004 bytes before MV creation to **242,172** bytes, a further
12.57% reduction. Asking the base aggregation and the MV explicitly now both
reads 242,172 bytes because the base query is rewritten. This is not a cache
hit and must not be interpreted as an isolated 1.0? materialisation benefit.
The independent correctness check reads 435,292 bytes because it deliberately
adds a price aggregate; use the preserved 277,004-byte pre-MV query for the
like-for-like performance baseline.

See [Google's smart-tuning documentation](https://cloud.google.com/bigquery/docs/materialized-views-use).

## Query cap and actual failure evidence

The SDK sets `maximum_bytes_billed=1_000_000_000` on normal query jobs in
`scripts/warehouse.py` (decimal 1 GB), with no project-wide default implied.
The biggest recorded snapshot build processed about 16.35 MB, so this permits
the exercise while blocking an accidental much larger scan. For console
queries, set **More → Query settings → Advanced options → Maximum bytes billed**
to the same value; the runbook gives the full procedure.

`uv run --locked scripts/warehouse.py verify-cap` uses a normal native query
with a valid partition filter, checks that its dry-run estimate is over
**1,048,576 bytes**, then runs it with that lower cap. A real maximum-bytes
error is required; a partition-filter rejection, access error or successful
query cannot produce passing evidence. The exact returned error, job ID,
cache setting and thresholds are saved to `cost-cap.json`. Do not count the
offline unit test's simulated exception as a live cap test.



The live probe succeeded on **2026-11-06** in the sense that BigQuery rejected
it for the intended reason ([full error and job ID](evidence/step4/cost-cap.json)):

```text
Query exceeded limit for bytes billed: 1048576. 10485760 or higher required.
reason: bytesBilledLimitExceeded
```

The API returned HTTP 500 for this budget rejection. The collector checks the
limit reason/message, never treats an arbitrary 500 as success, and disables
job retries for this intentional failure. The 10 MiB requirement is the
billing minimum even though the dry run estimates fewer processed bytes.

## Reproduce the verified live checks

Use an identity with GCS object read, BigQuery Job User on the project,
BigQuery Data Viewer on `g07_bronze` and BigQuery Data Editor on
`g07_warehouse`. Listing dataset metadata is insufficient to read its data.
With these permissions, from the repository root:

```powershell
uv sync --locked
uv run --locked scripts/warehouse.py complete
uv run --locked python -m unittest discover -s tests -p 'test_*.py'
```

`complete` validates the existing tables, trips the low cost cap, runs the
controlled comparison, refreshes the three-query evidence and creates/checks
the MV. It does **not** replace the native base tables, touch Bronze, change
IAM, reset the CDC slot or start Docker. Commit the generated evidence and
inspect the generated evidence. The command writes successful validation
status only after every live check passes. The 2026-11-06 run is complete.
