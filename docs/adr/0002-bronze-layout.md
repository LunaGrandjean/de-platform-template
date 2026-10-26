# ADR 0002: Preserve raw CSV values in daily ingestion partitions

- Date: 2026-10-26
- Status: Implemented for the initial seven-table snapshot
- Author: Luna Grandjean
- Bucket: `gs://data-engineering-i1-marketplace-group`
- Project / region: `data-engineering-i1` / `europe-west1`

## Context

Bronze must retain arrival evidence, including duplicates, unnamed CSV index
columns, leading zeros and NA-like strings. The initial source is seven local
CSV exports: `orders`, `order_items`, `review`, `product_reviews`, `product`,
`customer_shipping` and `shipping_details`. Several have no business-date
column, so a shared layout must not require joining or filtering source rows.
The next lesson will add connector, custom SQL and Kafka ingestion.

## Decision

Use Parquet with **Snappy** compression and the following object layout:

```text
gs://data-engineering-i1-marketplace-group/
  bronze/snapshot/<table>/ingestion_date=YYYY-MM-DD/load_id=<unique-load>/part-NNNNN.parquet
  bronze/_manifests/<unique-load>.json
```

`ingestion_date` is the UTC arrival date, recorded in the manifest and directory,
not substituted for `orders.order_date`. `load_id` separates arrivals on the same
day. Each file contains at most 100,000 source rows, in source order. Uploads
use a generation precondition to prohibit overwrite. A completion manifest is
written only after all objects pass checksum verification.

Store every CSV field as a Parquet string with its original column name,
including the empty index header `""`. This avoids inference changing phones,
decimal spellings, empty strings or literal `NA`/`NULL` values. No source column
or duplicate is removed. CSV quoting and line-ending bytes are not retained in
Parquet; the guarantee covers parsed field values and row multiplicity. Source
CSV hashes identify the input bytes. Preserve the original released files if
byte-level CSV reproduction is required.

## Exercise 1 evidence and alternatives

These are Luna's recorded exercise results, transcribed from the earlier
`data-engineering/report.md` and `rapport_data_engineering.md`. They are prior
measurements, not new jobs against this group bucket. The original job IDs,
query-cache setting and repetition count were not recorded, so timings are
indicative and must not be presented as a controlled uncached benchmark.

| Exercise measurement | Daily business date (`dt`) | Daily date + product (`dt`, `p_id`) |
|---|---:|---:|
| Parquet objects reported | 45 | 8,649 |
| Bytes reported | 661,116 | 24,781,824 |
| BigQuery bytes processed, approximately | 0.075 MiB | 0.075 MiB |
| Query execution time | 444 ms | 1,546 ms |
| Units sold for August 2026 | 33,231 | 33,231 |

The product layout used about 192.2 times as many objects and 37.5 times the
storage, with no reported scan reduction. One measured execution was about
3.48 times slower. The daily layout's reported 45 objects exceed the month's
31 days; without the old object manifest, we cannot establish whether multiple
files or earlier writes contributed. This limits the comparison's precision.

The same exercise reported a 10,373,925-byte `order_items.csv` and a
3,546,854-byte unpartitioned Snappy Parquet file: a 2.93:1 size ratio and 65.8%
reduction. That conversion used inferred numeric types. Our raw-preserving
string representation and chunking differ, so we do not promise that ratio.
The [transcribed measurements](../evidence/step2/exercise-1-measurements.json)
record the source-report hashes and the limits of this evidence. The exercise's
units total is a raw comparison result, not a reconciled Gold sales measure.

Alternatives considered:

| Alternative | Benefit | Reason for rejection in the initial Bronze layout |
|---|---|---|
| Partition `orders` by `order_date` | Prunes business-date reads | Does not apply directly to the other six tables. Deriving dates for order lines introduces a join and possible fanout. Keep the raw landing path free of such enrichment. |
| Partition by date and product | Can prune selective product reads | Prior exercise showed many small files and no measured scan benefit. Customer/address tables also lack a product key. |
| Keep CSV only | Closest to the released byte representation and simple inspection | Required output is Parquet; repeated CSV parsing has less predictable typing and limited column projection. |
| Parquet with Zstandard or gzip | Could reduce stored bytes further | Not benchmarked here. Snappy is available in the installed PyArrow runtime and gives a simple baseline. Do not claim a measured codec-speed advantage. |

The exercise supports avoiding product partitions. It does **not** directly
benchmark ingestion-date versus business-date partitioning. We choose ingestion
date for uniform arrival evidence, accepting less business-date pruning in
Bronze; Step 4 will tune selected warehouse working tables by query workload.

## Actual Step 2 measurements

Load `20261026T105159Z-2efcb86a`, ingestion date `2026-10-26`:

| Table | Rows retained | Source CSV bytes | Parquet bytes | Parquet objects |
|---|---:|---:|---:|---:|
| `orders` | 222,644 | 13,335,140 | 10,485,105 | 3 |
| `order_items` | 408,560 | 10,373,925 | 4,792,367 | 5 |
| `review` | 111,322 | 27,371,182 | 18,243,129 | 2 |
| `product_reviews` | 111,322 | 2,667,957 | 2,058,823 | 2 |
| `product` | 42,858 | 16,107,395 | 7,268,120 | 1 |
| `customer_shipping` | 200,001 | 8,777,892 | 8,025,788 | 3 |
| `shipping_details` | 200,001 | 17,230,494 | 10,075,933 | 3 |
| **Total** | **1,296,708** | **95,863,985** | **60,949,265** | **19** |

The combined size ratio is about 1.57:1, a 36.4% reduction. A 9,208-byte JSON
completion manifest is stored separately. Every parsed source value and row
position was hashed and checked against a Parquet re-read; all 19 uploaded
objects were then checked against GCS size, MD5 and SHA-256 metadata.

See the [landing manifest](../evidence/step2/landing-manifest.json) and
[upload receipt](../evidence/step2/upload-receipt.json). No new BigQuery query-time
or scan benchmark is claimed for this raw string layout.

## Consequences and redesign triggers

- Small daily increments can create small files; do not create a file per row.
  Revisit buffering if the median object remains below 1 MiB for seven loads.
- A large one-off snapshot shares one ingestion-date partition. Use the manifest
  and source columns to identify its business-date coverage.
- An order-date filter alone cannot prune ingestion-date partitions. Revisit
  the design if repeated Bronze reads scan over ten times the relevant data.
- Empty source field names may require an explicit alias such as `_csv_index`
  in a later warehouse working copy. Retain the original header in Bronze and
  document the reversible mapping before loading the warehouse.
- Multiple snapshots deliberately coexist. Consumers must select a completed
  load; a wildcard across all loads would duplicate data.
- Current lifecycle rules transition `bronze/` objects to Nearline at age 30,
  Coldline at age 90 and Archive at age 365 days, without deletion. Reassess
  these transitions if historical data is replayed frequently; colder classes
  have retrieval charges and minimum-duration conditions.
- `customer` and `payment_details` are outside this landing scope. Adding them
  requires secret-column exclusion before any Bronze write, not downstream
  cleanup.

## Related evidence

- [Applied lifecycle](../../infra/lifecycle.json)
- [Actual IAM bindings](../../infra/iam.md)
- [Data dictionary](../data-dictionary.md)
- [Cost baseline](../costs.md)
- [Cloud Storage lifecycle behaviour](https://cloud.google.com/storage/docs/lifecycle)
