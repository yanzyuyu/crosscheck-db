from typing import Any, Optional

from crosscheck.adapters.base import DatabaseAdapter


class BigQueryAdapter(DatabaseAdapter):

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = None

    def connect(self) -> None:
        try:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=self.project_id)
        except ImportError:
            raise ImportError("BigQuery driver not found. Install with: pip install google-cloud-bigquery")

    def close(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def _qualify(self, table: str) -> str:
        return f"`{self.project_id}.{self.dataset_id}.{table}`"

    def _build_row_hash_expr(self, cols: list[str]) -> str:
        parts = [f"COALESCE(CAST({c} AS STRING), '')" for c in cols]
        concat_expr = ", '|', ".join(parts)
        return f"TO_HEX(MD5(CONCAT({concat_expr})))"

    def get_bounds(
        self, table: str, key_col: str, where: Optional[str] = None
    ) -> tuple[int, int, int]:
        from google.cloud import bigquery

        clause = f"WHERE {where}" if where else ""
        query = f"SELECT MIN({key_col}), MAX({key_col}), COUNT(*) FROM {self._qualify(table)} {clause}"
        job = self.client.query(query)
        row = list(job.result())[0]
        return (row[0] or 0, row[1] or 0, row[2] or 0)

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
        from google.cloud import bigquery

        row_hash_expr = self._build_row_hash_expr(cols)
        where_extra = f"AND ({where})" if where else ""

        query = f"""
            WITH partitioned AS (
                SELECT
                    {key_col} AS _k,
                    DIV({key_col} - @min_id, @bucket_size) AS _b_idx,
                    {row_hash_expr} AS _r_hash
                FROM {self._qualify(table)}
                WHERE {key_col} BETWEEN @min_id AND @max_id {where_extra}
            )
            SELECT
                _b_idx,
                COUNT(*) AS cnt,
                TO_HEX(MD5(STRING_AGG(_r_hash, '' ORDER BY _k))) AS b_hash
            FROM partitioned
            GROUP BY _b_idx
            ORDER BY _b_idx
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_id", "INT64", min_id),
                bigquery.ScalarQueryParameter("max_id", "INT64", max_id),
                bigquery.ScalarQueryParameter("bucket_size", "INT64", bucket_size),
            ]
        )
        job = self.client.query(query, job_config=job_config)
        return {row["_b_idx"]: (row["cnt"], row["b_hash"]) for row in job.result()}

    def get_row_hashes(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        min_key: int,
        max_key: int,
        where: Optional[str] = None,
    ) -> dict[int, str]:
        from google.cloud import bigquery

        row_hash_expr = self._build_row_hash_expr(cols)
        where_extra = f"AND ({where})" if where else ""

        query = f"""
            SELECT {key_col}, {row_hash_expr} AS r_hash
            FROM {self._qualify(table)}
            WHERE {key_col} BETWEEN @min_key AND @max_key {where_extra}
            ORDER BY {key_col}
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("min_key", "INT64", min_key),
                bigquery.ScalarQueryParameter("max_key", "INT64", max_key),
            ]
        )
        job = self.client.query(query, job_config=job_config)
        return {row[key_col]: row["r_hash"] for row in job.result()}

    def get_rows(
        self,
        table: str,
        key_col: str,
        cols: list[str],
        keys: list[int],
    ) -> dict[int, dict[str, Any]]:
        if not keys:
            return {}

        from google.cloud import bigquery

        all_cols = list(dict.fromkeys([key_col] + cols))
        cols_sql = ", ".join(all_cols)
        query = f"SELECT {cols_sql} FROM {self._qualify(table)} WHERE {key_col} IN UNNEST(@keys)"

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("keys", "INT64", keys),
            ]
        )
        job = self.client.query(query, job_config=job_config)
        result = {}
        for row in job.result():
            d = dict(row.items())
            result[d[key_col]] = d
        return result
