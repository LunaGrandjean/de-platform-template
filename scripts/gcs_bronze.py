"""Create, inspect and populate the group's GCS Bronze bucket using ADC and uv.

No gcloud installation or service-account key file is needed. Creation refuses
to replace an existing bucket. Uploads are create-only and resumable: existing
objects must match their local SHA-256, size and MD5 before they are reused.
No command deletes objects or changes IAM bindings on an existing bucket.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage

from prepare_bronze import file_sha256

LIFECYCLE_RULES = [
    {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
     "condition": {"age": 30, "matchesPrefix": ["bronze/"], "matchesStorageClass": ["STANDARD"]}},
    {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
     "condition": {"age": 90, "matchesPrefix": ["bronze/"], "matchesStorageClass": ["STANDARD", "NEARLINE"]}},
    {"action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},
     "condition": {"age": 365, "matchesPrefix": ["bronze/"], "matchesStorageClass": ["STANDARD", "NEARLINE", "COLDLINE"]}},
]


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def inspect(client, bucket_name: str, evidence_dir: Path) -> storage.Bucket:
    bucket = client.get_bucket(bucket_name, timeout=30, retry=None)
    if bucket.location.lower() != "europe-west1":
        raise ValueError(f"Wrong bucket region: {bucket.location}")
    policy = bucket.get_iam_policy(requested_policy_version=3, timeout=30, retry=None)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    write_json(evidence_dir / "bucket.json", bucket._properties)
    write_json(evidence_dir / "bucket-iam.json", policy.to_api_repr())
    write_json(evidence_dir / "verification.json", {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "bucket": bucket_name, "project": client.project,
        "method": "Google Cloud Storage Python SDK: buckets.get and buckets.getIamPolicy",
        "scope": "Bucket policy only; inherited project/org grants are not enumerated here.",
    })
    objects = [{"name": blob.name, "bytes": int(blob.size),
                "generation": blob.generation, "storage_class": blob.storage_class,
                "md5": blob.md5_hash}
               for blob in client.list_blobs(bucket, prefix="bronze/", timeout=30, retry=None)]
    write_json(evidence_dir / "inventory.json", {
        "bucket": bucket.name, "prefix": "bronze/", "objects": objects,
        "total_objects": len(objects), "total_bytes": sum(item["bytes"] for item in objects),
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    # Export what was actually read back from GCS, not the proposed rules.
    write_json(Path("infra/lifecycle.json"), {"rule": list(bucket.lifecycle_rules)})
    print(f"Verified gs://{bucket.name}, region={bucket.location}, class={bucket.storage_class}", flush=True)
    return bucket


def upload(client, bucket, manifest_path: Path, local_root: Path, evidence_dir: Path) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    receipt = {"bucket": bucket.name, "load_id": manifest["load_id"], "objects": []}
    for table in manifest["tables"]:
        for item in table["files"]:
            name = table["object_prefix"] + "/" + item["name"]
            path = local_root / name
            if file_sha256(path) != item["sha256"]:
                raise ValueError(f"Local object changed: {name}")
            with path.open("rb") as stream:
                md5 = base64.b64encode(hashlib.file_digest(stream, "md5").digest()).decode()
            blob = bucket.get_blob(name, timeout=30, retry=None)
            if blob is None:
                blob = bucket.blob(name)
                blob.metadata = {"sha256": item["sha256"], "load_id": manifest["load_id"]}
                blob.upload_from_filename(path, if_generation_match=0, timeout=60, checksum="md5")
                blob.reload(timeout=30, retry=None)
            if (int(blob.size) != item["bytes"] or blob.md5_hash != md5
                    or (blob.metadata or {}).get("sha256") != item["sha256"]):
                raise ValueError(f"Remote object differs; refusing overwrite: {name}")
            receipt["objects"].append({"name": name, "bytes": int(blob.size),
                                       "generation": blob.generation, "md5": blob.md5_hash,
                                       "sha256": item["sha256"]})
        print(f"Remote objects verified: {table['table']}", flush=True)
    name = f"bronze/_manifests/{manifest['load_id']}.json"
    manifest_blob = bucket.get_blob(name, timeout=30, retry=None)
    text = manifest_path.read_text(encoding="utf-8")
    if manifest_blob is None:
        manifest_blob = bucket.blob(name)
        manifest_blob.upload_from_string(text, content_type="application/json", if_generation_match=0,
                                         timeout=60, checksum="md5")
    elif manifest_blob.download_as_text(timeout=30) != text:
        raise ValueError("Existing remote manifest differs; refusing overwrite")
    manifest_blob.reload(timeout=30, retry=None)
    receipt.update({"verified_at_utc": datetime.now(timezone.utc).isoformat(),
                    "parquet_objects": len(receipt["objects"]),
                    "parquet_bytes": sum(item["bytes"] for item in receipt["objects"]),
                    "remote_manifest": name, "remote_manifest_bytes": int(manifest_blob.size),
                    "status": "uploaded_and_verified"})
    write_json(evidence_dir / "upload-receipt.json", receipt)
    print(f"Uploaded and verified {receipt['parquet_objects']} Parquet objects; completion manifest present.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "inspect", "upload"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--local-root", type=Path, default=Path("data/bronze"))
    parser.add_argument("--evidence-dir", type=Path, default=Path("docs/evidence/step2"))
    args = parser.parse_args()
    if args.action == "upload" and args.manifest is None:
        parser.error("upload requires --manifest")
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(credentials, refresh_timeout=20)
    client = storage.Client(project=args.project, credentials=credentials, _http=session)
    if args.action == "create":
        bucket = client.bucket(args.bucket)
        bucket.storage_class = "STANDARD"
        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
        bucket.iam_configuration.public_access_prevention = "enforced"
        bucket.lifecycle_rules = LIFECYCLE_RULES
        bucket.labels = {"application": "albert-marketplace", "layer": "bronze", "managed-by": "python"}
        client.create_bucket(bucket, location="europe-west1", timeout=30, retry=None)
        print(f"Created gs://{args.bucket}", flush=True)
    bucket = inspect(client, args.bucket, args.evidence_dir)
    if args.action == "upload":
        upload(client, bucket, args.manifest, args.local_root, args.evidence_dir)


if __name__ == "__main__":
    main()
