import hashlib
import sqlite3
from typing import Any, Optional

from crosscheck.adapters.base import DatabaseAdapter


def _hash_row(*values: Any) -> str:
    tokens = ["" if v is None else str(v) for v in values]
    return hashlib.md5("|".join(tokens).encode("utf-8")).hexdigest()


def _hash_str(val: Optional[str]) -> str:
    if not val:
        return ""
    return hashlib.md5(val.encode("utf-8")).hexdigest()


class SQLiteAdapter(DatabaseAdapter):

    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function("cc_row_hash", -1, _hash_row)
        self.conn.create_function("cc_md5", 1, _hash_str)

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_bounds(
        self, table: str, key_col: str, where: Optional[str] = None
    ) -> tuple[int, int, int]:
        clause = f"WHERE {where}" if where else ""
        query = f"SELECT MIN({key_col}), MAX({key_col}), COUNT(*) FROM {table} {clause}"
        cur = self.conn.cursor()
        cur.execute(query)
        min_id, max_id, count = cur.fetchone()
        return (min_id or 0, max_id or 0, count or 0)

    def get_bucket_checksums(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        min_id: int,
        max_id: int,
        bucket_size: int,
        where: Optional[str] = None,
    ) -> dict[int, tuple[int, str]]:
        cols_sql = ", ".join(cols)
        where_extra = f"AND ({where})" if where else ""

        query = f"""
            WITH partitioned AS (
                SELECT
                    {key_col} AS _k,
                    (({key_col} - ?) / ?) AS _b_idx,
                    cc_row_hash({cols_sql}) AS _r_hash
                FROM {table}
                WHERE {key_col} BETWEEN ? AND ? {where_extra}
            )
            SELECT
                _b_idx,
                COUNT(*),
                cc_md5(GROUP_CONCAT(_r_hash, '' ORDER BY _k))
            FROM partitioned
            GROUP BY _b_idx
            ORDER BY _b_idx
        """

        cur = self.conn.cursor()
        cur.execute(query, (min_id, bucket_size, min_id, max_id))
        return {row[0]: (row[1], row[2]) for row in cur.fetchall()}

    def get_row_hashes(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        min_key: int,
        max_key: int,
        where: Optional[str] = None,
    ) -> dict[int, str]:
        cols_sql = ", ".join(cols)
        where_extra = f"AND ({where})" if where else ""

        query = f"""
            SELECT {key_col}, cc_row_hash({cols_sql})
            FROM {table}
            WHERE {key_col} BETWEEN ? AND ? {where_extra}
            ORDER BY {key_col}
        """

        cur = self.conn.cursor()
        cur.execute(query, (min_key, max_key))
        return {row[0]: row[1] for row in cur.fetchall()}

    def get_rows(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        keys: list[int],
    ) -> dict[int, dict[str, Any]]:
        if not keys:
            return {}

        placeholders = ", ".join("?" for _ in keys)
        all_cols = list(dict.fromkeys([key_col] + cols))
        cols_sql = ", ".join(all_cols)

        query = f"SELECT {cols_sql} FROM {table} WHERE {key_col} IN ({placeholders})"
        cur = self.conn.cursor()
        cur.execute(query, keys)

        result = {}
        for row in cur.fetchall():
            d = dict(row)
            result[d[key_col]] = d
        return result
