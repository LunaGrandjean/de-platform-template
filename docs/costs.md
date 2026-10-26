# Bronze storage and operations cost baseline

Price lookup date: **2026-10-26**. Currency: USD, before tax, credits or negotiated
discounts. Scope: `gs://data-engineering-i1-marketplace-group`, a single-region,
flat-namespace GCS bucket in `europe-west1`, initially using Standard storage.
This is a model based on measured bytes, not an actual billing export.

## Published price references

Use the [Google Cloud Storage pricing page](https://cloud.google.com/storage/pricing)
with Belgium (`europe-west1`) selected. The Standard reference rate used is
**$0.020/GiB-month** (approximately $0.000027397/GiB-hour for a 730-hour month).
The regional flat-namespace operation rates are **$0.005 per 1,000 Class A**
operations and **$0.0004 per 1,000 Class B** operations.

The estimate includes stored bytes, initial object writes and one object read
per day for 30 days. It excludes BigQuery queries/storage, network transfer,
additional metadata/list requests and retries. No GCS free-tier discount is
assumed for this European region. Refresh prices before using this baseline for
a budget decision; a browser's default region may display Iowa instead of Belgium.

## Measured inventory

The [landing manifest](evidence/step2/landing-manifest.json),
[upload receipt](evidence/step2/upload-receipt.json) and actual
[GCS inventory](evidence/step2/inventory.json) establish:

- 7 tables, 1,296,708 rows and 19 Parquet objects.
- Parquet storage: 60,949,265 bytes.
- One completion-manifest object: 9,208 bytes, measured in GCS.
- Baseline total: **60,958,473 bytes** (about 0.05677 GiB), **20 objects**.

The inventory represents one completed snapshot. Existing classroom-exercise
buckets and future automated ingestion are outside this group's baseline.

## One-month model and 100x growth

Assumptions: retain one snapshot for a month in Standard, write each object
once, and read each object once per day. The 100x case scales bytes **and object
counts** by 100, representing 100 snapshot-equivalents at the same file shape;
it is a scenario, not an observed deployment.

```text
storage = total_bytes / 2^30 * 0.020
initial_writes = object_count / 1000 * 0.005
monthly_reads = object_count * 30 / 1000 * 0.0004
first_month = storage + initial_writes + monthly_reads
```

| Item | Current snapshot | 100x scenario |
|---|---:|---:|
| Retained bytes | 60,958,473 | 6,095,847,300 |
| Objects | 20 | 2,000 |
| Monthly storage | $0.001135 | $0.113544 |
| Initial Class A writes (one-time) | $0.000100 | $0.010000 |
| Class B reads over 30 days | $0.000240 | $0.024000 |
| **First-month model total** | **$0.001475** | **$0.147544** |

Initial writes are not a recurring monthly charge if no new data is written.
A snapshot added every day would accumulate roughly 30 snapshot-equivalents
after a month; it must not be budgeted as one static snapshot. Real operations
also include the bucket creation/inspection, IAM lookups, checksums and metadata
verification used during setup. Their counts were not captured as billing
metrics, so the table estimates the steady object workload rather than the
exact cost of this session.

## Lifecycle and cost limits

The [applied lifecycle](../infra/lifecycle.json) moves `bronze/` objects to
Nearline at age 30 days, Coldline at 90 days and Archive at 365 days. It contains
no deletion action. The table above models the initial Standard month only;
do not extrapolate its read price to colder classes.

Colder classes have retrieval charges and minimum storage durations. Each
lifecycle transition also incurs an operation at the destination class's rate.
The bucket's seven-day soft-delete policy can retain charged bytes after
deletion, although this pipeline performs no deletes. Review these factors
before changing retention or replaying archived snapshots. See the published
[storage-class conditions](https://cloud.google.com/storage/docs/storage-classes)
and [lifecycle behaviour](https://cloud.google.com/storage/docs/lifecycle).

Step 4 will add native BigQuery storage, three query-cost examples and query
byte caps. Later FinOps work will compare this model with actual usage and
revisit file sizes, refresh frequency and long-term retention.
