"""Preserve selected CSV records as ingestion-date-partitioned Bronze Parquet.

Every source column is retained with its original name, including an empty CSV
header. Values are stored as strings: empty strings, NA-like text, leading
zeros, duplicate records and decimal spellings are not interpreted or cleaned.
This preserves parsed CSV values, not the original CSV quoting/byte layout.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

TABLES = (
    "orders", "order_items", "review", "product_reviews", "product",
    "customer_shipping", "shipping_details",
)


def file_sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def update_rows(digest, rows) -> None:
    for row in rows:
        digest.update(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        digest.update(b"\n")


def prepare_table(source: Path, destination: Path, table: str, batch_rows: int) -> dict:
    source_hash = file_sha256(source)
    source_digest = hashlib.sha256()
    parquet_digest = hashlib.sha256()
    files = []
    row_count = 0
    with source.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        headers = next(reader)
        if len(headers) != len(set(headers)):
            raise ValueError(f"Duplicate CSV column names in {source.name}")
        schema = pa.schema([(name, pa.string()) for name in headers], metadata={
            b"source_table": table.encode(),
            b"source_csv_sha256": source_hash.encode(),
            b"source_csv_header": json.dumps(headers).encode(),
        })
        destination.mkdir(parents=True, exist_ok=False)
        for part in itertools.count():
            rows = list(itertools.islice(reader, batch_rows))
            if not rows:
                break
            if any(len(row) != len(headers) for row in rows):
                raise ValueError(f"Wrong CSV field count in {source.name}; no successful manifest written")
            update_rows(source_digest, rows)
            arrow_table = pa.Table.from_arrays(
                [pa.array(values, type=pa.string()) for values in zip(*rows)], schema=schema,
            )
            target = destination / f"part-{part:05d}.parquet"
            with target.open("xb") as output:
                pq.write_table(arrow_table, output, compression="snappy")
            restored = pq.ParquetFile(target).read()
            if restored.column_names != headers:
                raise ValueError(f"Column names changed while writing {table}")
            restored_rows = zip(*(column.to_pylist() for column in restored.columns))
            update_rows(parquet_digest, restored_rows)
            row_count += len(rows)
            files.append({
                "name": target.name, "rows": len(rows),
                "bytes": target.stat().st_size, "sha256": file_sha256(target),
            })
    if not files:
        # Preserve even an empty table's header/schema.
        target = destination / "part-00000.parquet"
        with target.open("xb") as output:
            pq.write_table(pa.Table.from_batches([], schema=schema), output, compression="snappy")
        files.append({"name": target.name, "rows": 0,
                      "bytes": target.stat().st_size, "sha256": file_sha256(target)})
    if source_digest.digest() != parquet_digest.digest():
        raise ValueError(f"CSV/Parquet value verification failed for {table}")
    if file_sha256(source) != source_hash:
        raise ValueError(f"Source CSV changed while reading {table}")
    return {
        "table": table, "source_file": source.name, "source_sha256": source_hash,
        "source_bytes": source.stat().st_size, "columns": headers,
        "physical_type": "string", "compression": "SNAPPY", "rows": row_count,
        "parsed_rows_sha256": source_digest.hexdigest(),
        "parquet_round_trip_verified": True,
        "parquet_bytes": sum(item["bytes"] for item in files), "files": files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--ingestion-date", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument("--evidence", type=Path, default=Path("docs/evidence/step2/landing-manifest.json"))
    args = parser.parse_args()
    if args.batch_rows <= 0:
        parser.error("--batch-rows must be positive")
    for table in TABLES:
        if not (args.source_dir / f"{table}.csv").is_file():
            parser.error(f"Missing input: {table}.csv")
    load_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    manifest = {"load_id": load_id, "ingestion_date": args.ingestion_date.isoformat(),
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "local_verified", "tables": []}
    for table in TABLES:
        prefix = f"bronze/snapshot/{table}/ingestion_date={args.ingestion_date}/load_id={load_id}"
        result = prepare_table(args.source_dir / f"{table}.csv", args.output_dir / prefix,
                               table, args.batch_rows)
        result["object_prefix"] = prefix
        manifest["tables"].append(result)
        print(f"{table}: {result['rows']:,} rows, {len(result['files'])} files, values verified", flush=True)
    manifest["total_rows"] = sum(item["rows"] for item in manifest["tables"])
    manifest["total_parquet_bytes"] = sum(item["parquet_bytes"] for item in manifest["tables"])
    manifest["total_parquet_objects"] = sum(len(item["files"]) for item in manifest["tables"])
    text = json.dumps(manifest, indent=2) + "\n"
    local_manifest = args.output_dir / f"manifest-{load_id}.json"
    local_manifest.write_text(text, encoding="utf-8")
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_text(text, encoding="utf-8")
    print(f"Local manifest: {local_manifest}")


if __name__ == "__main__":
    main()
