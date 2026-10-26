"""Check a real configured Airbyte catalog and emit a credential-free summary.

Supports platform syncCatalog entries and protocol configured-catalog entries.
This checks selection/modes/schema only; it does NOT certify that a connector is
running, that CDC is enabled or that destination credentials/format are correct.
Never commit raw connection/source/destination exports containing credentials.
"""

import argparse
import json
from pathlib import Path

if __package__:
    from .common import utc_now
else:
    from common import utc_now

REQUIRED = {"orders", "order_items", "review", "product_reviews", "product",
            "customer_shipping", "shipping_details"}
FORBIDDEN = {"pwd", "card_no", "cvv"}
EXPECTED_FIELDS = {
    "orders": {"order_id", "buyer_id", "discount_id", "payment_id", "order_date"},
    "order_items": {"order_id", "p_id", "qty", "price_at_purchase"},
    "review": {"review_id", "buyer_id", "r_desc", "title", "rating", "seller_product_flag"},
    "product_reviews": {"p_id", "review_id"},
    "product": {"p_id", "p_name", "p_desc", "price", "qty", "category_id"},
    "customer_shipping": {"address_id", "c_id", "is_default"},
    "shipping_details": {"address_id", "street_address", "city", "state", "zip", "country", "phone"},
}


def check_catalog(document: dict) -> dict:
    catalog = document.get("syncCatalog", document)
    entries = catalog.get("streams")
    if not isinstance(entries, list):
        raise ValueError("Expected a syncCatalog/streams configured catalog")
    streams = []
    names = set()
    for entry in entries:
        stream = entry["stream"]
        config = entry.get("config")
        if config is not None:
            if config.get("selected") is not True:
                continue
            mode, destination = config.get("syncMode"), config.get("destinationSyncMode")
        else:
            # In protocol configured catalogs, each listed stream is selected.
            mode, destination = entry.get("sync_mode"), entry.get("destination_sync_mode")
        name = stream["name"]
        if name not in REQUIRED or stream.get("namespace") != "public" or name in names:
            raise ValueError("Selected streams must be exactly the seven distinct public tables")
        if mode != "incremental" or destination != "append":
            raise ValueError("Every selected stream must use Incremental / Append")
        schema = stream.get("jsonSchema", stream.get("json_schema", {}))
        properties = schema.get("properties")
        if not isinstance(properties, dict) or not properties:
            raise ValueError("A nonempty discovered source schema is required")
        if not EXPECTED_FIELDS[name].issubset(properties):
            raise ValueError("Expected source columns are missing from the discovered schema")
        if FORBIDDEN.intersection(field.lower() for field in properties):
            raise ValueError("A forbidden secret field appears in a selected source schema")
        # Column filtering is unnecessary for the seven safe tables. Prevent
        # accidental omission of other source columns through selectedFields.
        if config is not None and config.get("fieldSelectionEnabled"):
            selected = {tuple(item["fieldPath"]) for item in config.get("selectedFields", [])}
            if selected != {(field,) for field in properties}:
                raise ValueError("All source fields of each safe selected table must be retained")
        names.add(name)
        streams.append({"name": name, "namespace": "public", "sync_mode": mode,
                        "destination_sync_mode": destination, "fields": sorted(properties)})
    if names != REQUIRED:
        raise ValueError("One or more required connector tables are missing")
    return {"status": "catalog_checked_runtime_not_verified", "checked_at_utc": utc_now(),
            "selected_tables": sorted(names), "streams": sorted(streams, key=lambda row: row["name"]),
            "unselected_secret_tables": ["customer", "payment_details"],
            "limitations": ["Does not prove live CDC settings, credentials, destination format or successful sync."]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Real export kept under ignored data/")
    parser.add_argument("--output", type=Path, default=Path("docs/evidence/step3/connector-catalog.json"))
    args = parser.parse_args()
    result = check_catalog(json.loads(args.input.read_text(encoding="utf-8")))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print("Seven Incremental/Append streams checked; no live deployment is asserted.")


if __name__ == "__main__":
    main()
