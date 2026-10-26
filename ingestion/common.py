"""Small shared storage helpers for append-only Bronze writes."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_name(name: str) -> str:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise ValueError("Object names must be relative paths without parent traversal")
    return name


class LocalStore:
    """Explicit local test sink, not evidence of a cloud deployment."""

    def __init__(self, root: Path):
        self.root = root

    def put(self, name: str, content: bytes, content_type: str) -> None:
        target = self.root / safe_name(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())


class GCSStore:
    def __init__(self, project: str, bucket_name: str):
        import google.auth
        from google.auth.transport.requests import AuthorizedSession
        from google.cloud import storage

        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        session = AuthorizedSession(credentials, refresh_timeout=20)
        client = storage.Client(project=project, credentials=credentials, _http=session)
        self.bucket = client.get_bucket(bucket_name, timeout=20, retry=None)
        if self.bucket.location.lower() != "europe-west1":
            raise ValueError("Bronze bucket must be in europe-west1")

    def put(self, name: str, content: bytes, content_type: str) -> None:
        from google.cloud.storage.retry import DEFAULT_RETRY

        blob = self.bucket.blob(safe_name(name))
        blob.metadata = {"sha256": hashlib.sha256(content).hexdigest()}
        blob.upload_from_string(content, content_type=content_type, if_generation_match=0,
                                checksum="md5", timeout=20,
                                retry=DEFAULT_RETRY.with_deadline(60))


def make_store(local_output: Path | None = None):
    if local_output is not None:
        return LocalStore(local_output)
    return GCSStore(os.environ["GCP_PROJECT_ID"], os.environ["BRONZE_BUCKET"])


def parquet_object(store, prefix: str, rows: list[dict], schema: pa.Schema) -> str:
    buffer = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buffer, compression="snappy")
    name = f"{prefix}/{uuid.uuid4().hex}.parquet"
    store.put(name, buffer.getvalue().to_pybytes(), "application/octet-stream")
    return name


def json_object(store, prefix: str, value: dict) -> str:
    name = f"{prefix}/{uuid.uuid4().hex}.json"
    content = (json.dumps(value, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
    store.put(name, content, "application/json")
    return name


def load_environment() -> None:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)


def kafka_options() -> dict:
    """Never log this dictionary: SASL values can be credentials."""
    options = {"bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
               "security_protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
               "api_version_auto_timeout_ms": 10_000}
    for variable, option in (("KAFKA_SASL_MECHANISM", "sasl_mechanism"),
                             ("KAFKA_SASL_USERNAME", "sasl_plain_username"),
                             ("KAFKA_SASL_PASSWORD", "sasl_plain_password"),
                             ("KAFKA_SSL_CAFILE", "ssl_cafile")):
        if os.environ.get(variable):
            options[option] = os.environ[variable]
    return options
