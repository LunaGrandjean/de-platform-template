"""Publish valid -> malformed -> valid to an isolated Kafka test topic.

Run only when the real broker is available. This is a live verification helper,
not a fixture pretending that a broker was tested. All messages share a key so
their relative order is preserved within the selected partition.
"""

import argparse
import json
import uuid

from kafka import KafkaProducer

if __package__:
    from .common import kafka_options, load_environment, utc_now
else:
    from common import kafka_options, load_environment, utc_now


def test_payloads() -> list[bytes]:
    base = {"schema_version": 1, "event_time": utc_now(), "emitted_at": utc_now(),
            "buyer_id": "step3-test-buyer", "session_id": str(uuid.uuid4())}
    first = {**base, "event_id": str(uuid.uuid4()), "event_type": "product_viewed",
             "p_id": "TEST000001", "price": 1.25}
    last = {**base, "event_id": str(uuid.uuid4()), "event_type": "added_to_cart",
            "p_id": "TEST000001", "price": 1.25, "qty": 2}
    return [json.dumps(first).encode(), b'{"schema_version": 1, "event_type":',
            json.dumps(last).encode()]


def main() -> None:
    load_environment()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", default="clickstream-step3-check")
    args = parser.parse_args()
    producer = KafkaProducer(**kafka_options(), acks="all", retries=3, request_timeout_ms=15_000)
    try:
        metadata = []
        for payload in test_payloads():
            item = producer.send(args.topic, key=b"step3-test-buyer", value=payload).get(timeout=30)
            metadata.append({"topic": item.topic, "partition": item.partition, "offset": item.offset})
        print(json.dumps({"expected_accepted": 2, "expected_rejected": 1, "records": metadata}))
    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    main()
