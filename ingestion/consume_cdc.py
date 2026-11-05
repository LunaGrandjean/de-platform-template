"""Durably append raw Debezium change events from Kafka to GCS Bronze."""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

if __package__:
    from .common import kafka_options, load_environment, make_store, parquet_object, utc_now
else:
    from common import kafka_options, load_environment, make_store, parquet_object, utc_now

LOGGER = logging.getLogger(__name__)
TOPIC_PREFIX = "marketplace.public."
TABLES = {"customer_shipping", "order_items", "orders", "product",
          "product_reviews", "review", "shipping_details"}
SCHEMA = pa.schema([
    ("raw_key", pa.binary()), ("raw_value", pa.binary()),
    ("_is_tombstone", pa.bool_()), ("_kafka_topic", pa.string()),
    ("_kafka_partition", pa.int32()), ("_kafka_offset", pa.int64()),
    ("_kafka_timestamp_ms", pa.int64()), ("_kafka_headers_json", pa.string()),
    ("_ingested_at", pa.string()),
])


def topic_table(topic: str) -> str:
    table = topic.removeprefix(TOPIC_PREFIX)
    if not topic.startswith(TOPIC_PREFIX) or table not in TABLES:
        raise ValueError("Unexpected CDC topic")
    return table


def event_row(message) -> dict:
    headers = [{"name": name,
                "value_base64": None if value is None else base64.b64encode(value).decode("ascii")}
               for name, value in (message.headers or [])]
    return {
        "raw_key": message.key,
        "raw_value": message.value,
        "_is_tombstone": message.value is None,
        "_kafka_topic": message.topic,
        "_kafka_partition": message.partition,
        "_kafka_offset": message.offset,
        "_kafka_timestamp_ms": message.timestamp,
        "_kafka_headers_json": json.dumps(headers, separators=(",", ":")),
        "_ingested_at": utc_now(),
    }


def process_batch(consumer, records: dict, store) -> dict:
    grouped = defaultdict(list)
    offsets = {}
    for partition, messages in records.items():
        for message in messages:
            table = topic_table(message.topic)
            grouped[table].append(event_row(message))
            offsets[partition] = OffsetAndMetadata(message.offset + 1, "", -1)
    day = datetime.now(timezone.utc).date().isoformat()
    objects = []
    for table, rows in grouped.items():
        objects.append(parquet_object(store, f"bronze/cdc/{table}/ingestion_date={day}", rows, SCHEMA))
    if offsets:
        consumer.commit(offsets=offsets)
    return {"events": sum(map(len, grouped.values())), "tables": sorted(grouped), "objects": objects}


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default=os.environ.get("CDC_KAFKA_GROUP_ID", "marketplace-cdc-bronze-v1"))
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--idle-seconds", type=float, default=0, help="Exit after this idle interval; 0 runs continuously")
    parser.add_argument("--local-output", type=Path, help="Local sink instead of GCS")
    args = parser.parse_args()
    if args.batch_size < 1 or args.idle_seconds < 0:
        parser.error("Batch size must be positive and idle interval nonnegative")
    if args.local_output is not None and args.group_id == os.environ.get(
            "CDC_KAFKA_GROUP_ID", "marketplace-cdc-bronze-v1"):
        parser.error("--local-output requires a separate --group-id")
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    consumer = None
    try:
        store = make_store(args.local_output)
        consumer = KafkaConsumer(**kafka_options(), group_id=args.group_id,
                                 enable_auto_commit=False, auto_offset_reset="earliest",
                                 max_poll_records=args.batch_size, max_poll_interval_ms=300_000)
        consumer.subscribe(pattern=r"^marketplace\.public\.(customer_shipping|order_items|orders|product|product_reviews|review|shipping_details)$")
        last_activity = time.monotonic()
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=args.batch_size)
            if records:
                result = process_batch(consumer, records, store)
                print(json.dumps(result), flush=True)
                last_activity = time.monotonic()
            elif args.idle_seconds and time.monotonic() - last_activity >= args.idle_seconds:
                break
    except KeyboardInterrupt:
        LOGGER.warning("CDC consumer stopped; only durably written polls have committed")
    except Exception as error:
        LOGGER.error("CDC consumer stopped (%s); failed poll offsets must be replayed", type(error).__name__)
        raise SystemExit(1) from None
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)


if __name__ == "__main__":
    main()
