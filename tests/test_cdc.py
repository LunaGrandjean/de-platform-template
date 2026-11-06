"""Verify Malek's CDC sink keeps bytes/tombstones and commits only after writes."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from kafka import TopicPartition
from ingestion import consume_cdc as cdc


def message(value, offset=1, topic="marketplace.public.product"):
    return SimpleNamespace(key=b'{"p_id":"test"}', value=value, topic=topic,
                           partition=0, offset=offset, timestamp=1000,
                           headers=[("trace", b"\x00\xff")])


class CDCTests(unittest.TestCase):
    def test_original_bytes_and_tombstones_survive(self):
        raw = b'{"op":"d", "before":{"p_id":"test"}}'
        row = cdc.event_row(message(raw))
        self.assertEqual(row["raw_value"], raw)
        self.assertFalse(row["_is_tombstone"])
        self.assertTrue(cdc.event_row(message(None))["_is_tombstone"])
        self.assertIn("AP8=", row["_kafka_headers_json"])

    def test_write_failure_does_not_commit(self):
        consumer = Mock()
        records = {TopicPartition("marketplace.public.product", 0): [message(b"{}", 2)]}
        with patch.object(cdc, "parquet_object", side_effect=OSError("offline write failed")):
            with self.assertRaises(OSError):
                cdc.process_batch(consumer, records, Mock())
        consumer.commit.assert_not_called()

    def test_all_table_writes_precede_next_offset_commit(self):
        consumer = Mock()
        written = []
        records = {TopicPartition("marketplace.public.product", 0): [message(b"{}", 2)],
                   TopicPartition("marketplace.public.orders", 0): [message(None, 8, "marketplace.public.orders")]}
        def write(*args):
            consumer.commit.assert_not_called()
            written.append(args[1])
            return "offline.parquet"
        with patch.object(cdc, "parquet_object", side_effect=write):
            cdc.process_batch(consumer, records, Mock())
        self.assertEqual(len(written), 2)
        offsets = consumer.commit.call_args.kwargs["offsets"]
        self.assertEqual(sorted(value.offset for value in offsets.values()), [3, 9])

    def test_unselected_secret_table_is_rejected_before_writes(self):
        consumer = Mock()
        msg = message(b"{}", topic="marketplace.public.payment_details")
        with patch.object(cdc, "parquet_object") as write, self.assertRaises(ValueError):
            cdc.process_batch(consumer, {TopicPartition(msg.topic, 0): [msg]}, Mock())
        write.assert_not_called()
        consumer.commit.assert_not_called()
