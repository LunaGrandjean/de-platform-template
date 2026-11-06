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

The following are Emile's 2026-11-05 actual jobs, with
`use_query_cache=False` and `cache_hit=false`, preserved in
[costed-queries.json](evidence/step4/costed-queries.json). Ratios are **before /
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
measurement above is not proof of MV deployment. Background refresh is disabled
for this fixed snapshot; after an intentional base-table replacement, recreate
the dependent MV if BigQuery invalidates it, then rerun the command. Do not
drop the shared base during normal validation. No scheduled refresh is claimed.

## Three questions and nominal costs

All SQL is in [the script](../scripts/warehouse.py) and the recorded evidence.
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

## Reproduce missing live checks

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
update the validation-status file after inspecting the real results.
