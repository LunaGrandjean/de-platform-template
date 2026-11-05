"""Expose the Bronze snapshot to BigQuery and build the Step 4 working tables.

Bronze stays in the bucket: `external` declares one BigQuery external table per
landed table, pinned to a single completed load. `native` copies the two sales
tables into typed, month-partitioned native tables. Neither command cleans,
deduplicates or drops source rows; reconciliation belongs to Silver (Step 5).

Uses Application Default Credentials and the BigQuery Python SDK; no `bq` or
`gcloud` is required. Every query runs with a `maximum_bytes_billed` cap.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJECT = "data-engineering-i1"
LOCATION = "europe-west1"
BUCKET = "data-engineering-i1-marketplace-group"
BRONZE_DATASET = "g07_bronze"
WAREHOUSE_DATASET = "g07_warehouse"
MANIFEST = Path("docs/evidence/step2/landing-manifest.json")
EVIDENCE_DIR = Path("docs/evidence/step4")
MAX_BYTES_BILLED = 1_000_000_000  # 1 GB: far above this dataset, far below a mistake

LABELS = {"group": "g07", "layer": "", "step": "l04"}


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def ensure_dataset(client: bigquery.Client, name: str, layer: str, description: str) -> None:
    dataset = bigquery.Dataset(f"{PROJECT}.{name}")
    dataset.location = LOCATION
    dataset.description = description
    dataset.labels = {**LABELS, "layer": layer}
    client.create_dataset(dataset, exists_ok=True)


def external(client: bigquery.Client, manifest: dict) -> None:
    """One external table per Bronze table, reading only the manifest's load.

    The legacy CSV index column has an empty name, which BigQuery rejects when
    it infers a Parquet schema. An explicit STRING schema of the named columns
    reads the same files unchanged and leaves that index column unread.
    """
    ensure_dataset(client, BRONZE_DATASET, "bronze",
                   "Group 7: external tables over the Bronze Parquet snapshot in GCS")
    created = []
    for table in manifest["tables"]:
        columns = [name for name in table["columns"] if name != ""]
        config = bigquery.ExternalConfig("PARQUET")
        config.source_uris = [f"gs://{BUCKET}/{table['object_prefix']}/*.parquet"]
        config.autodetect = False
        ref = bigquery.Table(f"{PROJECT}.{BRONZE_DATASET}.{table['table']}",
                             schema=[bigquery.SchemaField(name, "STRING") for name in columns])
        ref.external_data_configuration = config
        ref.description = (f"Bronze {table['table']}, load {manifest['load_id']} "
                           f"({table['rows']} rows). All fields are source text.")
        ref.labels = {**LABELS, "layer": "bronze"}
        client.delete_table(ref, not_found_ok=True)
        client.create_table(ref)
        count = next(iter(client.query(
            f"SELECT COUNT(*) AS n FROM `{ref.reference}`",
            job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)).result())).n
        if count != table["rows"]:
            raise ValueError(f"{table['table']}: {count} rows read, manifest says {table['rows']}")
        created.append({"table": str(ref.reference), "columns": columns,
                        "unread_columns": [n for n in table["columns"] if n == ""],
                        "source_uris": config.source_uris, "rows": count})
        print(f"{ref.reference}: {count} rows, matches manifest", flush=True)
    write_json(EVIDENCE_DIR / "external-tables.json", {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "load_id": manifest["load_id"], "tables": created})


# Step 3 append-only paths. Unlike the snapshot, they grow, so each table reads
# its whole prefix and exposes the directory date as a partition column.
# bronze/kafka/clickstream-step3-check/ (the malformed-event test) is left out,
# and bronze/cdc/ waits until its file layout is settled.
STEP3_TABLES = {
    "python_orders": {
        "prefix": "bronze/python/orders",
        "partition": "ingestion_date",
        "description": "Step 3 custom SQL extract of orders (insert-only ID cursor). Overlaps the snapshot orders: do not sum both.",
    },
    "kafka_clickstream": {
        "prefix": "bronze/kafka/clickstream",
        "partition": "event_date",
        "description": "Step 3 accepted clickstream events with raw payload and Kafka provenance, by UTC event date.",
    },
}


def footer_rows(prefix: str) -> tuple[int, int]:
    """Row and object counts from the Parquet footers, read independently of BigQuery."""
    from google.cloud import storage
    import pyarrow.parquet as pq

    bucket = storage.Client(project=PROJECT).bucket(BUCKET)
    rows = objects = 0
    for blob in bucket.list_blobs(prefix=prefix + "/"):
        if blob.name.endswith(".parquet"):
            with blob.open("rb") as stream:
                rows += pq.ParquetFile(stream).metadata.num_rows
            objects += 1
    return rows, objects


def external_step3(client: bigquery.Client) -> None:
    created = []
    for name, spec in STEP3_TABLES.items():
        config = bigquery.ExternalConfig("PARQUET")
        config.source_uris = [f"gs://{BUCKET}/{spec['prefix']}/*.parquet"]
        hive = bigquery.HivePartitioningOptions()
        hive.mode = "AUTO"
        hive.source_uri_prefix = f"gs://{BUCKET}/{spec['prefix']}/"
        hive.require_partition_filter = True
        config.hive_partitioning = hive
        ref = bigquery.Table(f"{PROJECT}.{BRONZE_DATASET}.{name}")
        ref.external_data_configuration = config
        ref.description = spec["description"]
        ref.labels = {**LABELS, "layer": "bronze"}
        client.delete_table(ref, not_found_ok=True)
        client.create_table(ref)
        expected, objects = footer_rows(spec["prefix"])
        count = next(iter(client.query(
            f"SELECT COUNT(*) AS n FROM `{ref.reference}` WHERE {spec['partition']} IS NOT NULL",
            job_config=bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)).result())).n
        if count != expected:
            raise ValueError(f"{name}: {count} rows read, Parquet footers say {expected}")
        table = client.get_table(ref)
        created.append({"table": str(ref.reference), "source_uris": config.source_uris,
                        "partition_column": spec["partition"], "require_partition_filter": True,
                        "objects": objects, "rows": count,
                        "columns": [field.name for field in table.schema]})
        print(f"{ref.reference}: {count} rows in {objects} objects, matches Parquet footers", flush=True)
    write_json(EVIDENCE_DIR / "external-tables-step3.json", {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(), "tables": created,
        "note": "Prefix-wide tables: counts are a point in time and grow with new Step 3 files."})


NATIVE_SQL = {
    # Typed copy of Bronze orders. Duplicate business orders are kept.
    "orders": """
CREATE OR REPLACE TABLE `{wh}.orders`
PARTITION BY DATE_TRUNC(order_date, MONTH)
OPTIONS (require_partition_filter = TRUE,
         description = "Typed Bronze orders, month partitions; not deduplicated (Silver's job)",
         labels = [("group", "g07"), ("layer", "warehouse"), ("step", "l04")])
AS
SELECT
  SAFE_CAST(order_id AS INT64) AS order_id,
  NULLIF(buyer_id, '') AS buyer_id,
  SAFE_CAST(discount_id AS INT64) AS discount_id,
  SAFE_CAST(payment_id AS INT64) AS payment_id,
  SAFE_CAST(order_date AS DATE) AS order_date
FROM `{br}.orders`
""",
    # order_items has no date: it takes its parent's order_date so it can be
    # partitioned, and is clustered by product for per-product questions.
    "order_lines": """
CREATE OR REPLACE TABLE `{wh}.order_lines`
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY p_id
OPTIONS (require_partition_filter = TRUE,
         description = "Bronze order_items with the parent order_date; month partitions, clustered by product",
         labels = [("group", "g07"), ("layer", "warehouse"), ("step", "l04")])
AS
SELECT
  SAFE_CAST(i.order_id AS INT64) AS order_id,
  i.p_id,
  SAFE_CAST(i.qty AS INT64) AS qty,
  SAFE_CAST(i.price_at_purchase AS NUMERIC) AS price_at_purchase,
  SAFE_CAST(o.order_date AS DATE) AS order_date
FROM `{br}.order_items` AS i
LEFT JOIN `{br}.orders` AS o USING (order_id)
""",
}

# Checks run before building: the join above must not multiply order lines,
# and casts must not silently turn source values into NULLs.
PRECHECK_SQL = """
SELECT
  (SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM `{br}.orders`) AS duplicate_order_ids,
  (SELECT COUNTIF(SAFE_CAST(order_date AS DATE) IS NULL) FROM `{br}.orders`) AS unparsed_dates,
  (SELECT COUNTIF(SAFE_CAST(order_id AS INT64) IS NULL) FROM `{br}.orders`) AS unparsed_order_ids,
  (SELECT COUNTIF(SAFE_CAST(qty AS INT64) IS NULL) FROM `{br}.order_items`) AS unparsed_qty,
  (SELECT COUNTIF(SAFE_CAST(price_at_purchase AS NUMERIC) IS NULL) FROM `{br}.order_items`) AS unparsed_prices,
  (SELECT COUNT(*) FROM `{br}.order_items` AS i
     LEFT JOIN `{br}.orders` AS o USING (order_id) WHERE o.order_id IS NULL) AS orphan_lines
"""


def native(client: bigquery.Client) -> None:
    ensure_dataset(client, WAREHOUSE_DATASET, "warehouse",
                   "Group 7: native typed working tables loaded from Bronze (Step 4)")
    names = {"br": f"{PROJECT}.{BRONZE_DATASET}", "wh": f"{PROJECT}.{WAREHOUSE_DATASET}"}
    capped = bigquery.QueryJobConfig(maximum_bytes_billed=MAX_BYTES_BILLED)
    checks = dict(next(iter(client.query(PRECHECK_SQL.format(**names), job_config=capped).result())))
    print("Prechecks:", checks, flush=True)
    if checks["duplicate_order_ids"]:
        raise ValueError("order_id is not unique in Bronze orders; the order_lines join would multiply rows")
    built = []
    for name, sql in NATIVE_SQL.items():
        job = client.query(sql.format(**names), job_config=capped)
        job.result()
        table = client.get_table(f"{names['wh']}.{name}")
        built.append({"table": str(table.reference), "rows": table.num_rows,
                      "logical_bytes": table.num_bytes,
                      "partitioning": table.time_partitioning.to_api_repr() if table.time_partitioning else None,
                      "clustering": table.clustering_fields,
                      "require_partition_filter": table.require_partition_filter,
                      "build_bytes_processed": job.total_bytes_processed,
                      "build_job_id": job.job_id})
        print(f"{table.reference}: {table.num_rows} rows, {table.num_bytes} bytes", flush=True)
    write_json(EVIDENCE_DIR / "native-tables.json", {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "prechecks": checks, "tables": built})


PRICE_PER_TIB_USD = 7.50  # BigQuery on-demand, europe-west1 (first TiB each month free)

# Three questions, each asked several ways. {br}/{wh} are the two datasets;
# {rare}/{best} are product IDs chosen from the data (see pick_products).
MEASURED_QUERIES = {
    "q1_daily_units_one_month": {
        "question": "Units sold per product and day in August 2026 (the daily-demand fact, one month)",
        "variants": {
            "bronze_external": """
SELECT o.order_date, i.p_id, SUM(SAFE_CAST(i.qty AS INT64)) AS units
FROM `{br}.order_items` AS i JOIN `{br}.orders` AS o USING (order_id)
WHERE o.order_date >= '2026-08-01' AND o.order_date < '2026-09-01'
GROUP BY 1, 2""",
            "native_range_filter": """
SELECT order_date, p_id, SUM(qty) AS units
FROM `{wh}.order_lines`
WHERE order_date >= DATE '2026-08-01' AND order_date < DATE '2026-09-01'
GROUP BY 1, 2""",
            "native_format_date_filter": """
SELECT order_date, p_id, SUM(qty) AS units
FROM `{wh}.order_lines`
WHERE FORMAT_DATE('%Y-%m', order_date) = '2026-08'
GROUP BY 1, 2""",
            "native_subquery_filter": """
SELECT order_date, p_id, SUM(qty) AS units
FROM `{wh}.order_lines`
WHERE order_date IN (SELECT DATE '2026-08-01' + n FROM UNNEST(GENERATE_ARRAY(0, 30)) AS n)
GROUP BY 1, 2""",
        },
    },
    "q2_one_product_one_year": {
        "question": "Monthly units of one product over the last twelve months (clustering by p_id)",
        "variants": {
            "rare_product": """
SELECT DATE_TRUNC(order_date, MONTH) AS month, SUM(qty) AS units
FROM `{wh}.order_lines`
WHERE order_date >= DATE '2025-09-01' AND order_date < DATE '2026-09-01' AND p_id = '{rare}'
GROUP BY 1""",
            "best_seller": """
SELECT DATE_TRUNC(order_date, MONTH) AS month, SUM(qty) AS units
FROM `{wh}.order_lines`
WHERE order_date >= DATE '2025-09-01' AND order_date < DATE '2026-09-01' AND p_id = '{best}'
GROUP BY 1""",
            "rare_product_bronze_external": """
SELECT DATE_TRUNC(SAFE_CAST(o.order_date AS DATE), MONTH) AS month, SUM(SAFE_CAST(i.qty AS INT64)) AS units
FROM `{br}.order_items` AS i JOIN `{br}.orders` AS o USING (order_id)
WHERE o.order_date >= '2025-09-01' AND o.order_date < '2026-09-01' AND i.p_id = '{rare}'
GROUP BY 1""",
        },
    },
    "q3_orders_per_month_one_quarter": {
        "question": "Orders per month in Q3 2026, and what SELECT * with LIMIT costs",
        "variants": {
            "bronze_external": """
SELECT SUBSTR(order_date, 1, 7) AS month, COUNT(*) AS orders
FROM `{br}.orders`
WHERE order_date >= '2026-07-01' AND order_date < '2026-10-01'
GROUP BY 1""",
            "native_one_column": """
SELECT DATE_TRUNC(order_date, MONTH) AS month, COUNT(*) AS orders
FROM `{wh}.orders`
WHERE order_date >= DATE '2026-07-01' AND order_date < DATE '2026-10-01'
GROUP BY 1""",
            "native_select_star_limit_10": """
SELECT *
FROM `{wh}.orders`
WHERE order_date >= DATE '2026-07-01' AND order_date < DATE '2026-10-01'
LIMIT 10""",
        },
    },
}


def pick_products(client: bigquery.Client, names: dict) -> dict:
    """The best seller and a product sold in exactly one month of the window."""
    sql = """
WITH per_product AS (
  SELECT p_id, SUM(qty) AS units, COUNT(DISTINCT DATE_TRUNC(order_date, MONTH)) AS months
  FROM `{wh}.order_lines`
  WHERE order_date >= DATE '2025-09-01' AND order_date < DATE '2026-09-01'
  GROUP BY p_id)
SELECT
  (SELECT p_id FROM per_product ORDER BY units DESC, p_id LIMIT 1) AS best,
  (SELECT p_id FROM per_product WHERE months = 1 ORDER BY p_id LIMIT 1) AS rare,
  (SELECT MAX(months) FROM per_product) AS months_in_window"""
    row = next(iter(client.query(sql.format(**names), job_config=bigquery.QueryJobConfig(
        maximum_bytes_billed=MAX_BYTES_BILLED)).result()))
    return dict(row)


def measure_one(client: bigquery.Client, sql: str) -> dict:
    """Dry run first (free), then a real run with the cache off."""
    result: dict = {"sql": sql.strip()}
    try:
        dry = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        result["dry_run_bytes"] = dry.total_bytes_processed
        job = client.query(sql, job_config=bigquery.QueryJobConfig(
            use_query_cache=False, maximum_bytes_billed=MAX_BYTES_BILLED))
        rows = list(job.result())
        result.update({
            "status": "ran", "rows_returned": len(rows), "job_id": job.job_id,
            "bytes_processed": job.total_bytes_processed,
            "bytes_billed": job.total_bytes_billed,
            "cache_hit": job.cache_hit,
            "usd_at_on_demand_price": round(job.total_bytes_billed / 2**40 * PRICE_PER_TIB_USD, 6),
        })
    except Exception as error:  # a rejected query is a result worth recording
        result.update({"status": "rejected", "error": str(error).splitlines()[0][:300]})
    return result


def measure(client: bigquery.Client) -> None:
    names = {"br": f"{PROJECT}.{BRONZE_DATASET}", "wh": f"{PROJECT}.{WAREHOUSE_DATASET}"}
    products = pick_products(client, names)
    print("Products:", products, flush=True)
    results = {}
    for key, spec in MEASURED_QUERIES.items():
        results[key] = {"question": spec["question"], "variants": {}}
        for variant, sql in spec["variants"].items():
            outcome = measure_one(client, sql.format(**names, **products))
            results[key]["variants"][variant] = outcome
            shown = outcome.get("bytes_processed", outcome.get("error"))
            print(f"{key} / {variant}: {outcome['status']}, {shown}", flush=True)
    write_json(EVIDENCE_DIR / "costed-queries.json", {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "price_per_tib_usd": PRICE_PER_TIB_USD, "products": products,
        "note": "Real runs with use_query_cache=False. bytes_billed includes BigQuery's 10 MB minimum per table referenced.",
        "queries": results})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["external", "external-step3", "native", "measure"])
    args = parser.parse_args()
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    if args.command == "external":
        external(client, json.loads(MANIFEST.read_text(encoding="utf-8")))
    elif args.command == "external-step3":
        external_step3(client)
    elif args.command == "native":
        native(client)
    else:
        measure(client)


if __name__ == "__main__":
    main()
