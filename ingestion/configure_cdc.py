"""Register the no-Airbyte Debezium connector against the existing PG slot."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

from dotenv import load_dotenv


CONNECTOR_NAME = "marketplace-bronze"
TABLES = (
    "public.customer_shipping",
    "public.order_items",
    "public.orders",
    "public.product",
    "public.product_reviews",
    "public.review",
    "public.shipping_details",
)


def request(method: str, url: str, payload: dict | None = None) -> tuple[int, object]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read()
            return response.status, json.loads(data) if data else None
    except urllib.error.HTTPError as error:
        data = error.read()
        try:
            detail = json.loads(data)
        except (ValueError, UnicodeError):
            detail = None
        return error.code, detail


def main() -> int:
    load_dotenv()
    base = os.environ.get("CONNECT_URL", "http://127.0.0.1:18083").rstrip("/")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            status, _ = request("GET", f"{base}/connectors")
        except (OSError, TimeoutError):
            time.sleep(2)
            continue
        if status == 200:
            break
        time.sleep(2)
    else:
        print("Kafka Connect REST API did not become ready", file=sys.stderr)
        return 1

    user = os.environ.get("PGUSER", "marketplace_ingest")
    password = os.environ.get("PGPASSWORD")
    if not password:
        print("PGPASSWORD is required in the ignored .env file", file=sys.stderr)
        return 2
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "db",
        "database.port": os.environ.get("PGPORT", "5432"),
        "database.user": user,
        "database.password": password,
        "database.dbname": os.environ.get("PGDATABASE", "marketplace"),
        "topic.prefix": "marketplace",
        "plugin.name": "pgoutput",
        "slot.name": "marketplace_bronze_slot",
        "slot.drop.on.stop": "false",
        "publication.name": "marketplace_bronze_publication",
        "publication.autocreate.mode": "disabled",
        "table.include.list": ",".join(TABLES),
        "snapshot.mode": "initial",
        "heartbeat.interval.ms": "10000",
        "tombstones.on.delete": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    status, _ = request("PUT", f"{base}/connectors/{CONNECTOR_NAME}/config", config)
    if status not in (200, 201):
        # Do not print REST response bodies; connector errors may echo secrets.
        print(f"Connector registration failed (HTTP {status}); response omitted", file=sys.stderr)
        return 1
    print(f"Registered {CONNECTOR_NAME} using existing slot marketplace_bronze_slot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
