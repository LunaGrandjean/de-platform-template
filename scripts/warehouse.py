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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["external", "native"])
    args = parser.parse_args()
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    if args.command == "external":
        external(client, json.loads(MANIFEST.read_text(encoding="utf-8")))
    else:
        native(client)


if __name__ == "__main__":
    main()
