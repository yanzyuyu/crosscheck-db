import time
from dataclasses import dataclass, field
from typing import Any

from crosscheck.adapters.base import DatabaseAdapter
from crosscheck.config import TableConfig
from crosscheck.engine.bucket import get_bucket_range


@dataclass
class ValueDiff:
    key: int
    source_data: dict[str, Any]
    target_data: dict[str, Any]
    diff_fields: dict[str, tuple[Any, Any]]


@dataclass
class ReconciliationResult:
    source_count: int
    target_count: int
    total_buckets: int
    matched_buckets: int
    mismatched_buckets: int
    missing_in_target: list[int] = field(default_factory=list)
    missing_in_source: list[int] = field(default_factory=list)
    value_mismatches: list[ValueDiff] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def is_consistent(self) -> bool:
        return (
            self.source_count == self.target_count
            and not self.missing_in_target
            and not self.missing_in_source
            and not self.value_mismatches
        )

    @property
    def total_discrepancies(self) -> int:
        return (
            len(self.missing_in_target)
            + len(self.missing_in_source)
            + len(self.value_mismatches)
        )


class Reconciler:

    def __init__(
        self,
        source: DatabaseAdapter,
        target: DatabaseAdapter,
        table_config: TableConfig,
    ):
        self.source = source
        self.target = target
        self.config = table_config

    def reconcile(self, fetch_details: bool = True) -> ReconciliationResult:
        start_time = time.perf_counter()

        src_min, src_max, src_count = self.source.get_bounds(
            self.config.name, self.config.key_column, self.config.where_clause
        )
        tgt_min, tgt_max, tgt_count = self.target.get_bounds(
            self.config.name, self.config.key_column, self.config.where_clause
        )

        if src_count == 0 and tgt_count == 0:
            duration = time.perf_counter() - start_time
            return ReconciliationResult(
                source_count=0,
                target_count=0,
                total_buckets=0,
                matched_buckets=0,
                mismatched_buckets=0,
                duration_seconds=round(duration, 4),
            )

        min_id = min(src_min, tgt_min) if (src_count and tgt_count) else (src_min or tgt_min)
        max_id = max(src_max, tgt_max) if (src_count and tgt_count) else (src_max or tgt_max)

        src_buckets = self.source.get_bucket_checksums(
            self.config.name,
            self.config.key_column,
            self.config.columns,
            min_id,
            max_id,
            self.config.bucket_size,
            self.config.where_clause,
        )
        tgt_buckets = self.target.get_bucket_checksums(
            self.config.name,
            self.config.key_column,
            self.config.columns,
            min_id,
            max_id,
            self.config.bucket_size,
            self.config.where_clause,
        )

        all_bucket_indices = sorted(set(src_buckets.keys()) | set(tgt_buckets.keys()))
        matched = 0
        mismatched_buckets = []

        for b_idx in all_bucket_indices:
            src_val = src_buckets.get(b_idx)
            tgt_val = tgt_buckets.get(b_idx)
            if src_val and tgt_val and src_val == tgt_val:
                matched += 1
            else:
                mismatched_buckets.append(b_idx)

        missing_in_target = []
        missing_in_source = []
        value_diff_keys = []

        for b_idx in mismatched_buckets:
            b_start, b_end = get_bucket_range(min_id, self.config.bucket_size, b_idx)
            b_end = min(b_end, max_id)

            src_hashes = self.source.get_row_hashes(
                self.config.name,
                self.config.key_column,
                self.config.columns,
                b_start,
                b_end,
                self.config.where_clause,
            )
            tgt_hashes = self.target.get_row_hashes(
                self.config.name,
                self.config.key_column,
                self.config.columns,
                b_start,
                b_end,
                self.config.where_clause,
            )

            src_keys = set(src_hashes.keys())
            tgt_keys = set(tgt_hashes.keys())

            missing_in_target.extend(sorted(src_keys - tgt_keys))
            missing_in_source.extend(sorted(tgt_keys - src_keys))

            common_keys = src_keys & tgt_keys
            for k in sorted(common_keys):
                if src_hashes[k] != tgt_hashes[k]:
                    value_diff_keys.append(k)

        value_mismatches = []
        if fetch_details and value_diff_keys:
            src_rows = self.source.get_rows(
                self.config.name, self.config.key_column, self.config.columns, value_diff_keys
            )
            tgt_rows = self.target.get_rows(
                self.config.name, self.config.key_column, self.config.columns, value_diff_keys
            )

            for k in value_diff_keys:
                s_data = src_rows.get(k, {})
                t_data = tgt_rows.get(k, {})
                diffs = {}
                for col in self.config.columns:
                    s_val = s_data.get(col)
                    t_val = t_data.get(col)
                    if str(s_val) != str(t_val):
                        diffs[col] = (s_val, t_val)

                value_mismatches.append(
                    ValueDiff(
                        key=k,
                        source_data=s_data,
                        target_data=t_data,
                        diff_fields=diffs,
                    )
                )

        duration = time.perf_counter() - start_time

        return ReconciliationResult(
            source_count=src_count,
            target_count=tgt_count,
            total_buckets=len(all_bucket_indices),
            matched_buckets=matched,
            mismatched_buckets=len(mismatched_buckets),
            missing_in_target=missing_in_target,
            missing_in_source=missing_in_source,
            value_mismatches=value_mismatches,
            duration_seconds=round(duration, 4),
        )
