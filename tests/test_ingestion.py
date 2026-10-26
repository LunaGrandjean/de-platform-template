"""Offline fault injection. These checks do not connect to PostgreSQL/Kafka/GCS."""

import copy
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
import pyarrow.parquet as pq
from jsonschema import ValidationError
from kafka import TopicPartition

from ingestion import extract_orders as extractor
from ingestion.common import LocalStore
from ingestion.consume_clickstream import decode_event, event_date, process_batch, validator
from ingestion.publish_test_events import test_payloads
from ingestion.validate_connector_export import EXPECTED_FIELDS, REQUIRED, check_catalog


def order(number):
    return {"order_id": number, "buyer_id": "synthetic-buyer", "discount_id": None,
            "payment_id": 42, "order_date": date(2026, 10, 26)}


def fetcher(rows):
    return lambda lower, upper, limit: [row for row in sorted(rows, key=lambda item: item["order_id"])
                                       if lower < row["order_id"] <= upper][:limit]


def read_parquet(root):
    rows = []
    for path in sorted(root.rglob("*.parquet")):
        rows.extend(pq.ParquetFile(path).read().to_pylist())
    return rows


class LocalCase(unittest.TestCase):
    def setUp(self):
        directory = Path("data/test-temp")
        directory.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=directory)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = LocalStore(self.root / "objects")
        self.state = self.root / "state/orders.json"


class ExtractorTests(LocalCase):
    def test_restart_reads_only_new_ids_and_keeps_business_twins(self):
        rows = [order(1), order(2)]
        first = extractor.extract_window(fetcher(rows), 2, self.store, self.state, "test", 1)
        self.assertEqual(first["rows"], 2)
        rows.append(order(3))
        second = extractor.extract_window(fetcher(rows), 3, self.store, self.state, "test", 2)
        self.assertEqual(second["rows"], 1)
        landed = read_parquet(self.root / "objects")
        self.assertEqual(sorted(row["order_id"] for row in landed), [1, 2, 3])
        self.assertTrue(all(row["discount_id"] is None for row in landed))
        self.assertEqual(extractor.read_cursor(self.state, "test"), 3)
        for path in (self.root / "objects").rglob("*.parquet"):
            metadata = pq.ParquetFile(path).schema_arrow.metadata
            self.assertEqual(metadata[b"source_id"], b"test")
            self.assertEqual(metadata[b"source_table"], b"public.orders")

    def test_upload_failure_never_advances_cursor(self):
        with patch.object(self.store, "put", side_effect=OSError("synthetic storage outage")):
            with self.assertRaises(OSError):
                extractor.extract_window(fetcher([order(1)]), 1, self.store, self.state, "test")
        self.assertFalse(self.state.exists())

    def test_crash_after_upload_replays_two_rows(self):
        source = fetcher([order(1), order(2)])
        with patch.object(extractor, "save_cursor", side_effect=OSError("synthetic checkpoint failure")):
            with self.assertRaises(OSError):
                extractor.extract_window(source, 2, self.store, self.state, "test")
        self.assertEqual(extractor.read_cursor(self.state, "test"), 0)
        extractor.extract_window(source, 2, self.store, self.state, "test")
        ids = [row["order_id"] for row in read_parquet(self.root / "objects")]
        self.assertEqual(len(ids) - len(set(ids)), 2)

    def test_late_lower_id_and_updates_are_not_captured(self):
        source = [order(1), order(3)]
        extractor.extract_window(fetcher(source), 3, self.store, self.state, "test")
        source[0]["buyer_id"] = "updated-buyer"
        source.append(order(2))  # Models a lower ID committing after the watermark advanced.
        result = extractor.extract_window(fetcher(source), 3, self.store, self.state, "test")
        self.assertEqual(result["rows"], 0)
        landed = read_parquet(self.root / "objects")
        self.assertEqual(sorted(row["order_id"] for row in landed), [1, 3])
        self.assertTrue(all(row["buyer_id"] == "synthetic-buyer" for row in landed))

    def test_deletes_are_not_propagated(self):
        extractor.extract_window(fetcher([order(1)]), 1, self.store, self.state, "test")
        extractor.extract_window(fetcher([]), 1, self.store, self.state, "test")
        self.assertEqual(len(read_parquet(self.root / "objects")), 1)

    def test_source_watermark_going_backwards_fails_closed(self):
        extractor.save_cursor(self.state, "test", 10, "synthetic-object")
        with self.assertRaisesRegex(ValueError, "backwards"):
            extractor.extract_window(fetcher([]), 2, self.store, self.state, "test")

    def test_corrupt_and_wrong_source_state_fail_closed(self):
        extractor.save_cursor(self.state, "test", 1, "synthetic-object")
        with self.assertRaises(ValueError):
            extractor.read_cursor(self.state, "other-source")
        self.state.write_text("not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            extractor.read_cursor(self.state, "test")

    def test_source_schema_change_does_not_drop_columns(self):
        changed = {**order(1), "new_column": "retain or stop"}
        with self.assertRaises(ValueError):
            extractor.extract_window(fetcher([changed]), 1, self.store, self.state, "test")
        self.assertFalse(self.state.exists())

    def test_retry_backoff_recovers(self):
        attempts, delays = [], []
        result = object()
        def connect(**kwargs):
            attempts.append(kwargs)
            if len(attempts) < 3:
                raise psycopg.OperationalError("synthetic connection outage")
            return result
        self.assertIs(extractor.connect_with_retry(5, delays.append, connect), result)
        self.assertEqual(delays, [1, 2])
        self.assertEqual(len(attempts), 3)

    def test_retry_exhaustion_is_bounded(self):
        delays = []
        def connect(**kwargs):
            raise psycopg.OperationalError("synthetic connection outage")
        with self.assertRaises(psycopg.OperationalError):
            extractor.connect_with_retry(5, delays.append, connect)
        self.assertEqual(delays, [1, 2, 4, 8])

    def test_state_lock_excludes_a_second_writer(self):
        with extractor.state_lock(self.state):
            with self.assertRaises(OSError):
                with extractor.state_lock(self.state):
                    self.fail("A second writer acquired the same state lock")


class FakeConsumer:
    def __init__(self, fail=False):
        self.commits = []
        self.fail = fail

    def commit(self, *, offsets):
        if self.fail:
            raise RuntimeError("synthetic commit failure")
        self.commits.append(offsets)


def message(raw, offset, partition=0):
    return SimpleNamespace(topic="clickstream-test", partition=partition, offset=offset,
                           value=raw, key=b"test-key", timestamp=1793016000000)


class ClickstreamTests(LocalCase):
    def setUp(self):
        super().setUp()
        self.check = validator()
        self.partition = TopicPartition("clickstream-test", 0)

    def records(self):
        return {self.partition: [message(raw, index) for index, raw in enumerate(test_payloads())]}

    def test_invalid_event_is_durable_and_valid_following_event_continues(self):
        consumer, records = FakeConsumer(), self.records()
        result = process_batch(consumer, records, self.store, self.check)
        self.assertEqual((result["accepted"], result["rejected"]), (2, 1))
        self.assertEqual(consumer.commits[0][self.partition].offset, 3)
        rows = read_parquet(self.root / "objects")
        self.assertEqual({row["raw_value"] for row in rows}, {records[self.partition][0].value, records[self.partition][2].value})
        rejected = list((self.root / "objects/bronze/_dlq").rglob("*.json"))
        self.assertEqual(len(rejected), 1)
        entry = json.loads(rejected[0].read_text())
        self.assertEqual(entry["offset"], 1)
        self.assertIn("original_value_base64", entry)
        self.assertIn("rejected_at_utc", entry)

    def test_dlq_failure_prevents_commit_even_after_a_valid_write(self):
        consumer = FakeConsumer()
        original = self.store.put
        def fail_dlq(name, content, kind):
            if "/_dlq/" in name:
                raise OSError("synthetic DLQ outage")
            original(name, content, kind)
        with patch.object(self.store, "put", side_effect=fail_dlq):
            with self.assertRaises(OSError):
                process_batch(consumer, self.records(), self.store, self.check)
        self.assertEqual(consumer.commits, [])
        self.assertEqual(len(read_parquet(self.root / "objects")), 2)

    def test_parquet_failure_prevents_commit(self):
        consumer = FakeConsumer()
        with patch.object(self.store, "put", side_effect=OSError("synthetic storage outage")):
            with self.assertRaises(OSError):
                process_batch(consumer, self.records(), self.store, self.check)
        self.assertEqual(consumer.commits, [])

    def test_failed_offset_commit_causes_two_replayed_valid_rows(self):
        records = self.records()
        with self.assertRaises(RuntimeError):
            process_batch(FakeConsumer(fail=True), records, self.store, self.check)
        process_batch(FakeConsumer(), records, self.store, self.check)
        rows = read_parquet(self.root / "objects")
        offsets = [row["_kafka_offset"] for row in rows]
        self.assertEqual(len(offsets) - len(set(offsets)), 2)

    def test_only_explicit_next_offsets_are_committed_per_partition(self):
        valid = test_payloads()[0]
        other = TopicPartition("clickstream-test", 1)
        records = {self.partition: [message(valid, 5)], other: [message(valid, 10, 1)]}
        consumer = FakeConsumer()
        process_batch(consumer, records, self.store, self.check)
        self.assertEqual({key.partition: value.offset for key, value in consumer.commits[0].items()}, {0: 6, 1: 11})

    def test_partition_uses_utc_event_time_not_emission_time(self):
        event = json.loads(test_payloads()[0])
        event["event_time"] = "2026-10-26T00:30:00+02:00"
        event["emitted_at"] = "2026-10-27T12:00:00Z"
        self.check.validate(event)
        self.assertEqual(event_date(event), "2026-10-25")
        process_batch(FakeConsumer(), {self.partition: [message(json.dumps(event).encode(), 0)]}, self.store, self.check)
        self.assertTrue(list((self.root / "objects").rglob("event_date=2026-10-25/*.parquet")))
        lower_case = {**event, "event_time": "2026-10-26t00:30:00z"}
        self.check.validate(lower_case)
        self.assertEqual(event_date(lower_case), "2026-10-26")

    def test_invalid_utf8_tombstones_duplicate_keys_and_nonfinite_json_go_to_dlq(self):
        payloads = [b"\xff", None, b'{"a":1,"a":2}', b'{"price":NaN}']
        surrogate = json.loads(test_payloads()[0])
        surrogate["buyer_id"] = "\ud800"
        payloads.append(json.dumps(surrogate).encode())
        consumer = FakeConsumer()
        result = process_batch(consumer, {self.partition: [message(raw, i) for i, raw in enumerate(payloads)]}, self.store, self.check)
        self.assertEqual((result["accepted"], result["rejected"]), (0, 5))
        self.assertEqual(consumer.commits[0][self.partition].offset, 5)

    def test_schema_version_date_and_unknown_field_are_enforced(self):
        base = json.loads(test_payloads()[0])
        for change in ({"schema_version": 2}, {"event_time": "2026-10-26"},
                       {"unknown_field": "new schema required"}, {"event_id": "not-a-uuid"}):
            with self.subTest(change=change), self.assertRaises(ValidationError):
                decode_event(json.dumps({**base, **change}).encode(), self.check)

    def test_order_event_validates_nested_line_quantities(self):
        base = json.loads(test_payloads()[0])
        event = {key: value for key, value in base.items() if key not in {"p_id", "price"}}
        event.update(event_type="order_placed", order_id=1, payment_method="PayPal", amount=2.5,
                     currency="EUR", items=[{"p_id": "TEST000001", "qty": 2, "price": 1.25}])
        self.check.validate(event)
        event["items"][0]["qty"] = 0
        with self.assertRaises(ValidationError):
            self.check.validate(event)


def catalog_fixture():
    return {"syncCatalog": {"streams": [
        {"stream": {"name": name, "namespace": "public",
                    "jsonSchema": {"type": "object", "properties": {field: {} for field in EXPECTED_FIELDS[name]}}},
         "config": {"selected": True, "syncMode": "incremental", "destinationSyncMode": "append"}}
        for name in sorted(REQUIRED)
    ]}}


class ConnectorCatalogTests(unittest.TestCase):
    def test_seven_streams_pass_and_summary_does_not_copy_credentials(self):
        catalog = catalog_fixture()
        catalog["password"] = "synthetic-do-not-export"
        result = check_catalog(catalog)
        self.assertEqual(set(result["selected_tables"]), REQUIRED)
        self.assertNotIn("synthetic-do-not-export", json.dumps(result))

    def test_missing_stream_is_rejected(self):
        catalog = catalog_fixture()
        catalog["syncCatalog"]["streams"].pop()
        with self.assertRaises(ValueError):
            check_catalog(catalog)

    def test_secret_table_selection_is_rejected(self):
        catalog = catalog_fixture()
        entry = copy.deepcopy(catalog["syncCatalog"]["streams"][0])
        entry["stream"]["name"] = "customer"
        catalog["syncCatalog"]["streams"].append(entry)
        with self.assertRaises(ValueError):
            check_catalog(catalog)

    def test_secret_column_and_overwrite_mode_are_rejected(self):
        catalog = catalog_fixture()
        entry = catalog["syncCatalog"]["streams"][0]
        entry["stream"]["jsonSchema"]["properties"]["pwd"] = {"type": "string"}
        with self.assertRaises(ValueError):
            check_catalog(catalog)
        del entry["stream"]["jsonSchema"]["properties"]["pwd"]
        entry["config"]["destinationSyncMode"] = "overwrite"
        with self.assertRaises(ValueError):
            check_catalog(catalog)

    def test_accidental_source_column_omission_is_rejected(self):
        catalog = catalog_fixture()
        config = catalog["syncCatalog"]["streams"][0]["config"]
        config.update(fieldSelectionEnabled=True, selectedFields=[])
        with self.assertRaises(ValueError):
            check_catalog(catalog)


if __name__ == "__main__":
    unittest.main()
