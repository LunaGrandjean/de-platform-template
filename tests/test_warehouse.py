"""Offline regression tests for warehouse safety and evidence, without Google access."""

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from google.api_core.exceptions import BadRequest, Forbidden
from scripts import warehouse


def passing_checks():
    return dict(duplicate_order_ids=0, unparsed_dates=0, unparsed_order_ids=0,
                unparsed_qty=0, unparsed_prices=0, orphan_lines=0)


class WarehouseTests(unittest.TestCase):
    def test_every_bad_precheck_stops_before_any_table_replacement(self):
        for key in passing_checks():
            with self.subTest(key=key):
                checks = passing_checks()
                checks[key] = 1
                client = Mock()
                client.query.return_value.result.return_value = [checks]
                with patch.object(warehouse, "ensure_dataset"), self.assertRaises(ValueError):
                    warehouse.native(client)
                self.assertEqual(client.query.call_count, 1)
                client.get_table.assert_not_called()

    def test_missing_precheck_fails_closed(self):
        checks = passing_checks()
        del checks["unparsed_prices"]
        with self.assertRaises(ValueError):
            warehouse.validate_prechecks(checks)

    def test_processed_and_billed_costs_remain_distinct(self):
        client = Mock()
        job = SimpleNamespace(result=lambda: [], job_id="offline-only", total_bytes_processed=277004,
                              total_bytes_billed=10485760, cache_hit=False, to_api_repr=lambda: {})
        client.query.side_effect = [SimpleNamespace(total_bytes_processed=277004), job]
        result = warehouse.measure_one(client, "SELECT 1")
        self.assertAlmostEqual(result["usd_at_on_demand_price"], 0.0000018895, places=10)
        self.assertGreater(result["usd_from_billed_bytes_before_allowance"], result["usd_at_on_demand_price"])
        config = client.query.call_args.kwargs["job_config"]
        self.assertFalse(config.use_query_cache)
        self.assertEqual(config.maximum_bytes_billed, warehouse.MAX_BYTES_BILLED)

    def test_auth_error_is_not_misreported_as_tuning_result(self):
        client = Mock()
        client.query.side_effect = Forbidden("Access denied")
        with self.assertRaises(Forbidden):
            warehouse.measure_one(client, "SELECT 1")

    def test_unexpected_sql_error_is_not_misreported_as_expected_rejection(self):
        client = Mock()
        client.query.side_effect = BadRequest("Syntax error")
        with self.assertRaises(BadRequest):
            warehouse.measure_one(client, "bad sql")

    def test_partition_error_is_preserved_as_evidence(self):
        client = Mock()
        client.query.side_effect = BadRequest("requires a filter for partition elimination")
        result = warehouse.measure_one(client, "SELECT 1")
        self.assertEqual(result["status"], "rejected")
        self.assertIn("partition elimination", result["error"])

    def test_cap_rejects_auth_error_and_does_not_write_fake_proof(self):
        client = Mock()
        client.query.side_effect = [SimpleNamespace(total_bytes_processed=3_000_000), Forbidden("Access denied")]
        with patch.object(warehouse, "write_json") as write, self.assertRaises(Forbidden):
            warehouse.verify_cap(client)
        write.assert_not_called()

    def test_cap_requires_an_actual_limit_error(self):
        client = Mock()
        client.query.side_effect = [SimpleNamespace(total_bytes_processed=3_000_000),
                                   BadRequest("Query exceeded limit for bytes billed: 1048576")]
        with patch.object(warehouse, "write_json") as write:
            warehouse.verify_cap(client)
        self.assertEqual(write.call_args.args[1]["status"], "rejected_by_cost_cap")
        self.assertFalse(write.call_args.args[1]["use_query_cache"])

    def test_successful_probe_does_not_count_as_cap_evidence(self):
        client = Mock()
        client.query.side_effect = [SimpleNamespace(total_bytes_processed=3_000_000), Mock()]
        with patch.object(warehouse, "write_json") as write, self.assertRaises(ValueError):
            warehouse.verify_cap(client)
        write.assert_not_called()

    def test_completion_status_is_not_written_if_live_materialization_fails(self):
        client = Mock()
        client.query.return_value.result.return_value = [passing_checks()]
        with patch.object(warehouse, "verify_cap"), patch.object(warehouse, "tune"), \
                patch.object(warehouse, "measure"), \
                patch.object(warehouse, "materialize", side_effect=Forbidden("Access denied")), \
                patch.object(warehouse, "write_json") as write:
            with self.assertRaises(Forbidden):
                warehouse.complete(client)
        write.assert_not_called()

    def test_tuning_failure_still_removes_its_own_scratch_table(self):
        client = Mock()
        with patch.object(warehouse, "pick_products", return_value={"rare": "r", "best": "b"}), \
                patch.object(warehouse, "measure_one", side_effect=RuntimeError("offline failure")):
            with self.assertRaises(RuntimeError):
                warehouse.tune(client)
        target = client.delete_table.call_args.args[0]
        self.assertTrue(target.startswith("data-engineering-i1.g07_warehouse._step4_unclustered_"))
        self.assertTrue(client.delete_table.call_args.kwargs["not_found_ok"])


if __name__ == "__main__":
    unittest.main()
