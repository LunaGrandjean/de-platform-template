"""Measure daily/monthly collapse directly from the pinned GCS Parquet snapshot.

This is an independent source calculation, not proof that a BigQuery MV exists.
Only order IDs, dates, product IDs and quantities are read; no buyer data.
"""

import json
from datetime import datetime, timezone

import pyarrow.parquet as pq
from google.cloud import storage

if __package__:
    from .warehouse import BUCKET, EVIDENCE_DIR, MANIFEST, PROJECT, write_json
else:
    from warehouse import BUCKET, EVIDENCE_DIR, MANIFEST, PROJECT, write_json


def main():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    bucket = storage.Client(project=PROJECT).bucket(BUCKET)
    sources = []

    def rows(name, columns):
        spec = next(table for table in manifest["tables"] if table["table"] == name)
        count = 0
        for blob in bucket.list_blobs(prefix=spec["object_prefix"] + "/"):
            if not blob.name.endswith(".parquet"):
                continue
            sources.append({"object": blob.name, "generation": blob.generation,
                            "columns_read": columns})
            with blob.open("rb") as stream:
                table = pq.ParquetFile(stream).read(columns=columns)
            for row in table.to_pylist():
                count += 1
                yield row
        if count != spec["rows"]:
            raise ValueError(f"{name}: count differs from pinned manifest")

    dates = {}
    for row in rows("orders", ["order_id", "order_date"]):
        if row["order_id"] in dates:
            raise ValueError("Duplicate parent order ID")
        dates[row["order_id"]] = row["order_date"]
    days, months = set(), set()
    lines = units = 0
    for row in rows("order_items", ["order_id", "p_id", "qty"]):
        day = dates[row["order_id"]]
        days.add((row["p_id"], day))
        months.add((row["p_id"], day[:7]))
        lines += 1
        units += int(row["qty"])
    result = {"order_lines": lines, "product_days": len(days), "product_months": len(months),
              "daily_collapse_ratio": lines / len(days),
              "monthly_collapse_ratio": lines / len(months), "total_units": units}
    write_json(EVIDENCE_DIR / "snapshot-collapse.json", {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "method": "Read selected columns of generation-pinned GCS Parquet; Python distinct-key sets",
        "load_id": manifest["load_id"], "counts": result, "source_objects": sources,
        "limitation": "Independent source measurement only; see materialization.json for BigQuery MV/job verification"})
    print(json.dumps(result))


if __name__ == "__main__":
    main()
