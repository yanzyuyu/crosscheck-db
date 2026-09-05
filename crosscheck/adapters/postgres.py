from typing import Any, Optional

from crosscheck.adapters.base import DatabaseAdapter


class PostgresAdapter(DatabaseAdapter):

    def __init__(
        self,
        dbname: str,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: Optional[str] = None,
    ):
        self.conn_params = {
            "dbname": dbname,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
        }
        self.conn = None

    def connect(self) -> None:
        try:
            import psycopg
            self.conn = psycopg.connect(**{k: v for k, v in self.conn_params.items() if v is not None})
        except ImportError:
            try:
                import psycopg2
                self.conn = psycopg2.connect(**{k: v for k, v in self.conn_params.items() if v is not None})
            except ImportError:
                raise ImportError("Postgres driver not found. Install psycopg with: pip install 'psycopg[binary]'")

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def _build_row_hash_expr(self, cols: list[str]) -> str:
        parts = [f"COALESCE({c}::text, '')" for c in cols]
        return f"MD5(CONCAT_WS('|', {', '.join(parts)}))"

    def get_bounds(
        self, table: str, key_col: str, where: Optional[str] = None
    ) -> tuple[int, int, int]:
        clause = f"WHERE {where}" if where else ""
        query = f"SELECT MIN({key_col}), MAX({key_col}), COUNT(*) FROM {table} {clause}"
        with self.conn.cursor() as cur:
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
        row_hash_expr = self._build_row_hash_expr(cols)
        where_extra = f"AND ({where})" if where else ""

        query = f"""
            WITH partitioned AS (
                SELECT
                    {key_col} AS _k,
                    (({key_col} - %s) / %s) AS _b_idx,
                    {row_hash_expr} AS _r_hash
                FROM {table}
                WHERE {key_col} BETWEEN %s AND %s {where_extra}
            )
            SELECT
                _b_idx,
                COUNT(*),
                MD5(STRING_AGG(_r_hash, '' ORDER BY _k))
            FROM partitioned
            GROUP BY _b_idx
            ORDER BY _b_idx
        """

        with self.conn.cursor() as cur:
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
        row_hash_expr = self._build_row_hash_expr(cols)
        where_extra = f"AND ({where})" if where else ""

        query = f"""
            SELECT {key_col}, {row_hash_expr}
            FROM {table}
            WHERE {key_col} BETWEEN %s AND %s {where_extra}
            ORDER BY {key_col}
        """

        with self.conn.cursor() as cur:
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

        all_cols = list(dict.fromkeys([key_col] + cols))
        cols_sql = ", ".join(all_cols)
        query = f"SELECT {cols_sql} FROM {table} WHERE {key_col} = ANY(%s)"

        with self.conn.cursor() as cur:
            cur.execute(query, (keys,))
            col_names = [desc[0] for desc in cur.description]
            result = {}
            for row in cur.fetchall():
                d = dict(zip(col_names, row))
                result[d[key_col]] = d
            return result
