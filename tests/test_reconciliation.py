import os
import sqlite3
import tempfile
import unittest

from crosscheck.adapters.sqlite import SQLiteAdapter
from crosscheck.config import TableConfig
from crosscheck.engine.reconciler import Reconciler


class TestReconciliation(unittest.TestCase):

    def setUp(self):
        self.src_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tgt_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.src_path = self.src_file.name
        self.tgt_path = self.tgt_file.name
        self.src_file.close()
        self.tgt_file.close()

        self._init_db(self.src_path)
        self._init_db(self.tgt_path)

        self.src_adapter = SQLiteAdapter(self.src_path)
        self.tgt_adapter = SQLiteAdapter(self.tgt_path)
        self.src_adapter.connect()
        self.tgt_adapter.connect()

    def tearDown(self):
        self.src_adapter.close()
        self.tgt_adapter.close()
        if os.path.exists(self.src_path):
            os.remove(self.src_path)
        if os.path.exists(self.tgt_path):
            os.remove(self.tgt_path)

    def _init_db(self, path: str):
        conn = sqlite3.connect(path)
        conn.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount REAL,
                status TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _insert_orders(self, path: str, rows: list[tuple]):
        conn = sqlite3.connect(path)
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        conn.close()

    def test_identical_datasets_are_consistent(self):
        rows = [(i, 100 + (i % 50), round(10.5 * i, 2), "COMPLETED") for i in range(1, 1001)]
        self._insert_orders(self.src_path, rows)
        self._insert_orders(self.tgt_path, rows)

        config = TableConfig(
            name="orders",
            key_column="id",
            columns=["customer_id", "amount", "status"],
            bucket_size=200,
        )

        reconciler = Reconciler(self.src_adapter, self.tgt_adapter, config)
        res = reconciler.reconcile()

        self.assertTrue(res.is_consistent)
        self.assertEqual(res.source_count, 1000)
        self.assertEqual(res.target_count, 1000)
        self.assertEqual(res.mismatched_buckets, 0)
        self.assertEqual(res.total_discrepancies, 0)

    def test_detects_missing_in_target(self):
        src_rows = [(i, 101, 50.0, "PAID") for i in range(1, 501)]
        tgt_rows = [r for r in src_rows if r[0] != 250]

        self._insert_orders(self.src_path, src_rows)
        self._insert_orders(self.tgt_path, tgt_rows)

        config = TableConfig(
            name="orders",
            key_column="id",
            columns=["customer_id", "amount", "status"],
            bucket_size=100,
        )

        reconciler = Reconciler(self.src_adapter, self.tgt_adapter, config)
        res = reconciler.reconcile()

        self.assertFalse(res.is_consistent)
        self.assertEqual(res.missing_in_target, [250])
        self.assertEqual(res.missing_in_source, [])
        self.assertEqual(res.value_mismatches, [])

    def test_detects_value_mismatch(self):
        src_rows = [(i, 101, 50.0, "PAID") for i in range(1, 501)]
        tgt_rows = [(i, 101, 50.0 if i != 310 else 99.9, "PAID") for i in range(1, 501)]

        self._insert_orders(self.src_path, src_rows)
        self._insert_orders(self.tgt_path, tgt_rows)

        config = TableConfig(
            name="orders",
            key_column="id",
            columns=["customer_id", "amount", "status"],
            bucket_size=100,
        )

        reconciler = Reconciler(self.src_adapter, self.tgt_adapter, config)
        res = reconciler.reconcile()

        self.assertFalse(res.is_consistent)
        self.assertEqual(len(res.value_mismatches), 1)
        diff = res.value_mismatches[0]
        self.assertEqual(diff.key, 310)
        self.assertIn("amount", diff.diff_fields)
        self.assertEqual(diff.diff_fields["amount"], (50.0, 99.9))

    def test_empty_tables_are_consistent(self):
        config = TableConfig(
            name="orders",
            key_column="id",
            columns=["customer_id", "amount", "status"],
            bucket_size=100,
        )
        reconciler = Reconciler(self.src_adapter, self.tgt_adapter, config)
        res = reconciler.reconcile()
        self.assertTrue(res.is_consistent)


if __name__ == "__main__":
    unittest.main()
