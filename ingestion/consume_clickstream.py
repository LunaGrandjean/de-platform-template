"""At-least-once Kafka -> GCS delivery, conditional on retained source offsets.

Auto-commit is disabled. Persist ALL accepted Parquet and rejected-event DLQ
objects for a poll before synchronously committing each partition's next offset.
Storage failure prevents any commit for that poll. A crash/rebalance after a
write but before commit can produce duplicate objects on replay; preserve them
in Bronze and reconcile by topic/partition/offset later. This is not exactly-once.
No cross-partition ordering or atomic multi-object visibility is promised.
"""

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
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata

if __package__:
    from .common import json_object, kafka_options, load_environment, make_store, parquet_object, utc_now
else:
    from common import json_object, kafka_options, load_environment, make_store, parquet_object, utc_now

LOGGER = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).with_name("clickstream.schema.json")
PARQUET_SCHEMA = pa.schema([
    ("schema_version", pa.int32()), ("event_id", pa.string()), ("event_type", pa.string()),
    ("event_time", pa.string()), ("emitted_at", pa.string()),
    ("buyer_id", pa.string()), ("session_id", pa.string()), ("raw_value", pa.binary()),
    ("_kafka_topic", pa.string()), ("_kafka_partition", pa.int32()),
    ("_kafka_offset", pa.int64()), ("_kafka_key", pa.binary()),
    ("_kafka_timestamp_ms", pa.int64()), ("_ingested_at", pa.string()),
])


def validator():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("Non-finite JSON numeric constant")


def decode_event(raw: bytes | None, check) -> dict:
    if raw is None:
        raise ValueError("Kafka tombstone/null payload is not a clickstream event")
    event = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_keys, parse_constant=reject_constant)
    check.validate(event)
    # Python's JSON parser can represent escaped lone surrogates and numeric
    # overflow. Reject them before Arrow string conversion can poison a poll.
    json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return event


def event_date(event: dict) -> str:
    text = event["event_time"].replace("Z", "+00:00").replace("z", "+00:00")
    return datetime.fromisoformat(text).astimezone(timezone.utc).date().isoformat()


def rejected_row(message, error: Exception) -> dict:
    raw = message.value
    if isinstance(error, ValidationError):
        reason = "JSON Schema rejection at /" + "/".join(map(str, error.absolute_path)) + ": " + str(error.validator)
    else:
        # Avoid echoing source data in error messages or CLI logs.
        reason = type(error).__name__ + ": invalid UTF-8/JSON/event envelope"
    return {"original_value_base64": None if raw is None else base64.b64encode(raw).decode(),
            "original_key_base64": None if message.key is None else base64.b64encode(message.key).decode(),
            "topic": message.topic, "partition": message.partition, "offset": message.offset,
            "rejection_reason": reason, "rejected_at_utc": utc_now()}


def process_batch(consumer, records: dict, store, check) -> dict:
    valid_by_day = defaultdict(list)
    rejects = []
    offsets = {}
    for partition, messages in records.items():
        for message in messages:
            try:
                event = decode_event(message.value, check)
                day = event_date(event)
            except (UnicodeError, ValueError, ValidationError, OverflowError, RecursionError) as error:
                rejects.append((message.topic, rejected_row(message, error)))
            else:
                row = {name: event[name] for name in PARQUET_SCHEMA.names[:7]}
                row.update({"raw_value": message.value, "_kafka_topic": message.topic,
                            "_kafka_partition": message.partition, "_kafka_offset": message.offset,
                            "_kafka_key": message.key, "_kafka_timestamp_ms": message.timestamp,
                            "_ingested_at": utc_now()})
                valid_by_day[(message.topic, day)].append(row)
            offsets[partition] = OffsetAndMetadata(message.offset + 1, "", -1)
    objects = []
    for (topic, day), rows in valid_by_day.items():
        objects.append(parquet_object(store, f"bronze/kafka/{topic}/event_date={day}", rows, PARQUET_SCHEMA))
    for topic, rejection in rejects:
        day = rejection["rejected_at_utc"][:10]
        objects.append(json_object(store, f"bronze/_dlq/{topic}/rejection_date={day}", rejection))
    if offsets:
        consumer.commit(offsets=offsets)
    return {"accepted": sum(len(rows) for rows in valid_by_day.values()),
            "rejected": len(rejects), "objects": objects}


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "clickstream"))
    parser.add_argument("--group-id", help="Defaults to KAFKA_GROUP_ID for GCS")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--idle-seconds", type=float, default=0, help="Exit after this idle interval; 0 runs continuously")
    parser.add_argument("--local-output", type=Path, help="Local test output instead of GCS")
    args = parser.parse_args()
    if args.batch_size < 1 or args.idle_seconds < 0:
        parser.error("Batch size must be positive and idle interval nonnegative")
    default_group = os.environ.get("KAFKA_GROUP_ID", "marketplace-bronze-v1")
    if args.local_output is not None and (args.group_id is None or args.group_id == default_group):
        parser.error("--local-output requires a separate --group-id; never commit the default GCS consumer group locally")
    args.group_id = args.group_id or default_group
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    consumer = None
    try:
        check = validator()
        store = make_store(args.local_output)
        consumer = KafkaConsumer(args.topic, **kafka_options(), group_id=args.group_id,
                                 enable_auto_commit=False, auto_offset_reset="earliest",
                                 max_poll_records=args.batch_size, max_poll_interval_ms=300_000)
        last_activity = time.monotonic()
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=args.batch_size)
            if records:
                result = process_batch(consumer, records, store, check)
                last_activity = time.monotonic()
                print(json.dumps(result), flush=True)
            elif args.idle_seconds and time.monotonic() - last_activity >= args.idle_seconds:
                break
    except KeyboardInterrupt:
        LOGGER.warning("Consumer stopped; only durably written polls have committed offsets")
    except Exception as error:
        LOGGER.error("Consumer stopped (%s); failed poll offsets must be replayed", type(error).__name__)
        raise SystemExit(1) from None
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)


if __name__ == "__main__":
    main()
