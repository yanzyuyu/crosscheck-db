import argparse
import sys
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from crosscheck.adapters.sqlite import SQLiteAdapter
from crosscheck.adapters.postgres import PostgresAdapter
from crosscheck.adapters.bigquery import BigQueryAdapter
from crosscheck.config import TableConfig
from crosscheck.engine.reconciler import Reconciler, ReconciliationResult


def build_adapter(driver: str, conn_str: str):
    if driver == "sqlite":
        return SQLiteAdapter(conn_str)
    elif driver == "postgres":
        import urllib.parse
        p = urllib.parse.urlparse(conn_str)
        return PostgresAdapter(
            dbname=p.path.lstrip("/"),
            host=p.hostname or "localhost",
            port=p.port or 5432,
            user=p.username or "postgres",
            password=p.password,
        )
    elif driver == "bigquery":
        parts = conn_str.split("/")
        if len(parts) != 2:
            raise ValueError("BigQuery format must be: project_id/dataset_id")
        return BigQueryAdapter(project_id=parts[0], dataset_id=parts[1])
    else:
        raise ValueError(f"Unsupported driver: {driver}")


def render_report(result: ReconciliationResult, console: Console) -> None:
    console.print()
    if result.is_consistent:
        console.print(
            Panel(
                f"[bold green][MATCH] DATA CONSISTENT[/bold green]\n"
                f"Source: {result.source_count:,} rows | Target: {result.target_count:,} rows\n"
                f"Buckets: {result.matched_buckets}/{result.total_buckets} verified\n"
                f"Completed in {result.duration_seconds}s",
                title="Reconciliation Result",
                border_style="green",
            )
        )
        return

    console.print(
        Panel(
            f"[bold red][DIFF] DATA DRIFT DETECTED[/bold red]\n"
            f"Source: {result.source_count:,} rows | Target: {result.target_count:,} rows\n"
            f"Mismatched Buckets: {result.mismatched_buckets}/{result.total_buckets}\n"
            f"Total Discrepancies: [bold red]{result.total_discrepancies}[/bold red]\n"
            f"Completed in {result.duration_seconds}s",
            title="Reconciliation Result",
            border_style="red",
        )
    )

    if result.missing_in_target:
        console.print(f"\n[yellow]Missing in Target ({len(result.missing_in_target)} rows):[/yellow]")
        console.print(f"  IDs: {result.missing_in_target[:20]}{' ...' if len(result.missing_in_target) > 20 else ''}")

    if result.missing_in_source:
        console.print(f"\n[yellow]Missing in Source ({len(result.missing_in_source)} rows):[/yellow]")
        console.print(f"  IDs: {result.missing_in_source[:20]}{' ...' if len(result.missing_in_source) > 20 else ''}")

    if result.value_mismatches:
        console.print(f"\n[yellow]Value Mismatches ({len(result.value_mismatches)} rows):[/yellow]")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Key ID", justify="right")
        table.add_column("Field")
        table.add_column("Source Value", style="green")
        table.add_column("Target Value", style="red")

        for diff in result.value_mismatches[:50]:
            for field_name, (s_val, t_val) in diff.diff_fields.items():
                table.add_row(str(diff.key), field_name, str(s_val), str(t_val))

        console.print(table)
        if len(result.value_mismatches) > 50:
            console.print(f"  ... and {len(result.value_mismatches) - 50} more rows.")


def main():
    parser = argparse.ArgumentParser(description="CrossCheck: Fast Cross-Database Reconciliation")
    parser.add_argument("--source-driver", default="sqlite", choices=["sqlite", "postgres", "bigquery"])
    parser.add_argument("--source-conn", required=True, help="Connection string (file path or URI)")
    parser.add_argument("--target-driver", default="sqlite", choices=["sqlite", "postgres", "bigquery"])
    parser.add_argument("--target-conn", required=True, help="Connection string (file path or URI)")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--key", required=True, help="Primary key column")
    parser.add_argument("--columns", required=True, help="Comma-separated column names to compare")
    parser.add_argument("--bucket-size", type=int, default=5000, help="Bucket size (default: 5000)")
    parser.add_argument("--where", default=None, help="Optional WHERE filter")

    args = parser.parse_args()
    console = Console()

    cols = [c.strip() for c in args.columns.split(",") if c.strip()]
    table_cfg = TableConfig(
        name=args.table,
        key_column=args.key,
        columns=cols,
        bucket_size=args.bucket_size,
        where_clause=args.where,
    )

    src_adapter = build_adapter(args.source_driver, args.source_conn)
    tgt_adapter = build_adapter(args.target_driver, args.target_conn)

    src_adapter.connect()
    tgt_adapter.connect()

    try:
        reconciler = Reconciler(src_adapter, tgt_adapter, table_cfg)
        console.print(f"[cyan]Comparing table '{args.table}' ({len(cols)} columns, bucket size: {args.bucket_size:,})...[/cyan]")
        result = reconciler.reconcile()
        render_report(result, console)
        sys.exit(0 if result.is_consistent else 1)
    finally:
        src_adapter.close()
        tgt_adapter.close()


if __name__ == "__main__":
    main()
