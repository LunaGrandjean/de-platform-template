# Step 4: BigQuery warehouse and three costed queries

## What exists

All resources are in project `data-engineering-i1`, region `europe-west1`, and
carry the labels `group=g07` and `step=l04`. The design is recorded in
[ADR 0004](adr/0004-warehouse-tables.md).

| Dataset | Table | Kind | Rows | Layout |
|---|---|---|---:|---|
| `g07_bronze` | `orders`, `order_items`, `review`, `product_reviews`, `product`, `customer_shipping`, `shipping_details` | External, Parquet in the Bronze bucket | 1,296,708 in all, equal to the Step 2 manifest | Load `20261026T105159Z-2efcb86a` only; explicit STRING schema |
| `g07_warehouse` | `orders` | Native | 222,644 | Month partitions on `order_date` (37), partition filter required |
| `g07_warehouse` | `order_lines` | Native | 408,560 | Month partitions on `order_date` (37), clustered by `p_id`, partition filter required |

Native sizes: `orders` 12.0 MB, `order_lines` 21.2 MB; partitions average 0.32
and 0.57 MB. Prechecks before the build found 0 duplicate `order_id`, 0
unparsed dates, quantities or prices, and 0 order lines without an order.
Evidence: [external tables](evidence/step4/external-tables.json),
[native tables](evidence/step4/native-tables.json).

## Reproduce

From the repository root, with Application Default Credentials that can read
the bucket and run BigQuery jobs:

```bash
uv sync --locked
uv run --locked scripts/warehouse.py external   # g07_bronze: create and count-check the external tables
uv run --locked scripts/warehouse.py native     # g07_warehouse: prechecks, then rebuild the native tables
uv run --locked scripts/warehouse.py measure    # run the queries below, cache off; writes the evidence file
```

`external` and `native` replace their own tables only; neither writes to the
bucket. Every query runs with `maximum_bytes_billed` = 1 GB.

## Three costed queries

Measured on 2026-11-05 with the query cache off. Bytes are what BigQuery
reported as processed; the full SQL, job IDs, dry-run bytes and billed bytes are
in [costed-queries.json](evidence/step4/costed-queries.json). Dollars use the
on-demand price of $7.50 per TiB.

### Q1. Units sold per product and day, August 2026

The daily-demand question of ADR 0001, for one month.

| Variant | Bytes processed | vs native |
|---|---:|---:|
| Bronze external tables (join orders and order items) | 13,577,448 | 49x |
| **Native `order_lines`, `order_date >= '2026-08-01' AND < '2026-09-01'`** | **277,004** | 1x |
| Native, `FORMAT_DATE('%Y-%m', order_date) = '2026-08'` | rejected before running | - |
| Native, `order_date IN (subquery of dates)` | rejected before running | - |

Both rejected forms are filters BigQuery cannot resolve to partitions before
reading; `require_partition_filter` turns that mistake into a free error.

### Q2. Monthly units of one product over twelve months

Clustering by `p_id` inside month partitions. The products were picked from the
data: the best seller of the period (`B085BB7B1M`, sold in all 12 months) and a
product sold in only one month (`0990995119`).

| Variant | Dry-run estimate | Bytes processed |
|---|---:|---:|
| **Rare product, native** | 3,789,632 | **1,625,204** |
| Best seller, native | 3,789,632 | 3,789,632 |
| Rare product, Bronze external tables | 0 (not estimable) | 8,879,045 |

The dry run gives the partition-pruned upper bound; clustering then skipped
blocks for the rare product only (2.3x less). The best seller appears in every
block, so nothing is skipped.

### Q3. Orders per month, July to September 2026

| Variant | Bytes processed | vs one column |
|---|---:|---:|
| Bronze external `orders` | 2,671,728 | 24x |
| **Native, one column (`order_date`)** | **109,808** | 1x |
| Native, `SELECT * ... LIMIT 10` | 741,248 | 6.8x |

`LIMIT` caps the rows returned, not the bytes read.

## What the bill actually is

BigQuery bills at least 10 MB per table a query references, so every query
above was billed 10,485,760 bytes (20,971,520 for the two-table Bronze joins):
about $0.00007 to $0.00014, inside the free first TiB of each month. At this
scale the layout does not change the invoice; the ratios above are what scale
with the data. Storage of the native tables (about 33 MB) is inside the free
10 GiB; the external tables store nothing in BigQuery.

## Limits

- The native tables reflect one Bronze load. Rebuild them with `native` after a
  new completed load; Step 3's live paths are not included yet.
- Native tables are typed copies, not Silver: duplicate business orders and the
  buyer-to-country mapping are still unresolved.
- Dry runs report 0 bytes for external tables; their cost is only known after
  a real run.
