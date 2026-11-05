"""Regression checks for the CSV preservation contract, without cloud access."""

import csv
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from scripts.prepare_bronze import prepare_table


class BronzePreservationTests(unittest.TestCase):
    def setUp(self):
        root = Path("data/test-temp")
        root.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def source(self, headers, rows):
        path = self.root / "input.csv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(headers)
            writer.writerows(rows)
        return path

    def test_values_headers_and_duplicate_rows_survive_chunking(self):
        headers = ["", "phone", "text", "price"]
        row = ["0001", "0600000000", 'NA\nNULL, "quoted"', "0.0004300"]
        rows = [row, row, ["2", "", "None", "0.00"]]
        result = prepare_table(self.source(headers, rows), self.root / "output", "sample", 1)
        restored = []
        for item in result["files"]:
            table = pq.ParquetFile(self.root / "output" / item["name"]).read()
            self.assertEqual(table.column_names, headers)
            restored.extend([list(values) for values in zip(*(col.to_pylist() for col in table.columns))])
        self.assertEqual(restored, rows)
        self.assertEqual(result["rows"], 3)
        self.assertTrue(result["parquet_round_trip_verified"])

    def test_malformed_record_fails_instead_of_dropping_fields(self):
        source = self.source(["id", "value"], [["1", "valid"], ["2", "extra", "field"]])
        with self.assertRaisesRegex(ValueError, "field count"):
            prepare_table(source, self.root / "output", "sample", 10)

    def test_existing_destination_is_never_overwritten(self):
        source = self.source(["id"], [["1"]])
        destination = self.root / "output"
        prepare_table(source, destination, "sample", 10)
        before = (destination / "part-00000.parquet").read_bytes()
        with self.assertRaises(FileExistsError):
            prepare_table(source, destination, "sample", 10)
        self.assertEqual((destination / "part-00000.parquet").read_bytes(), before)

    def test_empty_table_retains_its_schema(self):
        headers = ["", "phone"]
        result = prepare_table(self.source(headers, []), self.root / "output", "sample", 10)
        table = pq.ParquetFile(self.root / "output/part-00000.parquet").read()
        self.assertEqual(table.column_names, headers)
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(result["rows"], 0)


if __name__ == "__main__":
    unittest.main()
