# Investigate a slow BigQuery query

Owner: group 7. Project `data-engineering-i1`, location `europe-west1`.
Use this procedure for `g07_bronze` / `g07_warehouse`; do not run exploratory
SQL against PostgreSQL. The initial per-query budget is **1,000,000,000 bytes**.

## 1. Capture the incident before rerunning

1. Open Google Cloud Console → **BigQuery**. Select `data-engineering-i1`.
2. Open **Job history → Project history** (Personal history if project history
   is unavailable), locate the slow job and click its job ID.
3. Copy the full SQL, job ID, location, start/end time, error, bytes processed,
   bytes billed, cache hit, total slot milliseconds and any reservation ID
   from **Job information**. Save metadata and screenshots under
   `docs/evidence/step4/incidents/<date>-<job-id>/`; omit result rows and secrets.
4. Record the normal reference job from
   [query evidence](../evidence/step4/costed-queries.json), selected date range,
   table names and the affected user/deadline. Compare like-for-like queries.

## 2. Establish a safe uncached baseline

1. Click **Open query in editor** (or copy the saved SQL to a new SQL query).
2. Click **More → Query settings → Cache preferences** and uncheck
   **Use cached results**. Under **Advanced options → Maximum bytes billed**,
   enter `1000000000`; save. UI wording can vary; the SDK equivalent is
   `QueryJobConfig(use_query_cache=False, maximum_bytes_billed=1_000_000_000)`.
3. Read the editor's processing estimate before clicking **Run**. If it exceeds
   1 GB, stop and ask the query owner to narrow the dates/columns. Do not raise
   the cap to get a benchmark. GCS external-table estimates may be zero/unknown;
   prefer a bounded native-table query and keep the cap.
4. If the previous baseline processed zero bytes or has `cache_hit=true`, mark
   it **cached, not comparable**. Rerun the bounded query once with the cache
   disabled. Save the new job ID and verify `cache_hit=false`.

## 3. Check partition and column pruning

1. In **Explorer → g07_warehouse → order_lines → Details**, check the
   partition column (`order_date`), type (`MONTH`), required partition filter,
   clustering (`p_id`), row count and logical bytes. Do the same for `orders`.
2. In the query, require a direct bounded predicate, for example
   `order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'`.
   A missing filter can cause a bytes jump, or a partition-filter error on our
   guarded tables. Replace non-pruning expressions such as a dynamic subquery;
   save the old and new SQL and their uncached processed-byte counts.
3. Select only needed columns. `SELECT * ... LIMIT 10` still reads selected
   columns in the eligible partitions; LIMIT is not a cost cap.
4. To inspect grain and partition sizes without reading source rows, run:

   ```sql
   SELECT table_name, COUNT(DISTINCT partition_id) AS partition_count,
          SUM(total_rows) AS rows, SUM(total_logical_bytes) AS logical_bytes,
          SAFE_DIVIDE(SUM(total_logical_bytes),
                      COUNT(DISTINCT partition_id)) AS mean_partition_bytes
   FROM `data-engineering-i1.g07_warehouse.INFORMATION_SCHEMA.PARTITIONS`
   WHERE table_name IN ('orders', 'order_lines')
     AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
   GROUP BY table_name;
   ```

5. Compare partition count, individual sizes and the query's usual time window
   with [warehouse tuning](../warehouse-tuning.md). Roughly 37 tiny monthly
   partitions were observed in the snapshot. A sudden partition-count increase
   may mean a switch to daily grain; a large partition may mean skew or an
   excessively coarse grain. Test an alternative in an expiring scratch table
   first. Do not repartition a shared table during an incident.

## 4. Locate the expensive stage and skew

1. In the job, open **Execution details / Execution graph**. Select the stage
   with the largest slot time or elapsed time; inspect its READ, JOIN,
   AGGREGATE and SHUFFLE steps, rows read/written and shuffle bytes/spill.
2. Compare **average** and **maximum** compute/read/wait durations across
   workers in the stage details (the Jobs API also exposes `computeMsAvg`,
   `computeMsMax`, `readMsAvg/Max` and `waitMsAvg/Max`). A maximum over 3× the
   average is a triage signal for skew, not proof of its cause.
3. If compute/read is skewed, inspect key/partition distribution in a bounded
   aggregate: a hot or NULL join key, one very large month, or repeated source
   keys can dominate a few workers. If wait time dominates across workers,
   check overlapping jobs and slot contention instead of rewriting SQL blindly.
4. If a join/aggregate stage dominates, check whether filtering before the join
   or projecting fewer fields reduces its input. Do not deduplicate raw data
   as an incident shortcut; business reconciliation belongs to Lesson 5.

## 5. Check for a data-shape change

1. Compare stage input/output rows to the reference job. Output exceeding the
   expected join cardinality suggests duplicate parent keys or fan-out; a drop
   suggests missing keys, NULL casts or an accidental filter.
2. Compare source/native row counts, the snapshot load ID, min/max dates, and
   the build's six prechecks with
   [native evidence](../evidence/step4/native-tables.json). An orphan or failed
   cast must stop the next rebuild. Keep the previous working tables.
3. Check that the query did not combine the snapshot, Python backfill and CDC
   snapshot as additive sales. These overlap. The warehouse currently uses
   the pinned Step 2 snapshot only; CDC envelopes need explicit reconciliation.
4. Record the suspected producer/schema/load change and preserve Bronze. Do
   not restart Debezium, reset offsets, delete a slot or overwrite a snapshot
   as part of a query investigation.

## 6. Stop, verify and escalate

After one bounded uncached rerun and one targeted candidate fix (or 15 minutes),
stop if the cause remains unclear, the cap is reached, access is denied,
results differ, or a shared-table change is needed. Escalate to the query owner
and group warehouse maintainer with both job IDs, SQL diff, processed bytes,
timings, stage screenshots, partition sizes, row-count changes and the affected
deadline. Ask the project administrator for access/capacity issues.

Accept a fix only when business results agree, cache is off, processed bytes
and elapsed time are recorded, the 1 GB cap remains, and the change has peer
review. Record a ratio of 1.0× when a candidate does not improve scanning;
do not hide it or replace it with a cached run.

References checked 2026-11-06: [query plans](https://cloud.google.com/bigquery/docs/query-plan-explanation),
[cost controls](https://cloud.google.com/bigquery/docs/best-practices-costs),
[partition metadata](https://cloud.google.com/bigquery/docs/information-schema-partitions).
