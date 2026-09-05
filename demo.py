import os
import random
import sqlite3
import time

from rich.console import Console
from rich.panel import Panel

from crosscheck.adapters.sqlite import SQLiteAdapter
from crosscheck.cli import render_report
from crosscheck.config import TableConfig
from crosscheck.engine.reconciler import Reconciler

console = Console()


def create_mock_database(path: str, count: int):
    if os.path.exists(path):
        os.remove(path)

    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY,
            account_id INTEGER,
            amount REAL,
            currency TEXT,
            status TEXT
        )
    """)

    batch_size = 10000
    statuses = ["SUCCESS", "PENDING", "FAILED"]
    currencies = ["IDR", "USD", "SGD"]

    for batch_start in range(1, count + 1, batch_size):
        batch = []
        for i in range(batch_start, min(batch_start + batch_size, count + 1)):
            batch.append((
                i,
                1000 + (i % 200),
                round(float((i * 17) % 500000 + 10000), 2),
                currencies[i % 3],
                statuses[i % 3],
            ))
        cur.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?)", batch)

    conn.commit()
    conn.close()


def main():
    prod_db = "demo_production.db"
    wh_db = "demo_warehouse.db"
    row_count = 50000

    console.print(Panel(
        "[bold cyan]CrossCheck: Live Reconciliation Demo[/bold cyan]\n"
        f"Simulating audit between Primary Database vs Data Warehouse ({row_count:,} transactions)",
        border_style="cyan"
    ))

    console.print("[dim]Generating 50,000 transactions in production...[/dim]")
    create_mock_database(prod_db, row_count)

    console.print("[dim]Replicating to data warehouse...[/dim]")
    create_mock_database(wh_db, row_count)

    console.print("[yellow]Simulating 3 real-world silent data drifts in warehouse:[/yellow]")
    console.print("  1. Dropped record during ETL sync: [red]Deleted ID #12450[/red]")
    console.print("  2. Ghost/Phantom write in target:   [red]Inserted ID #50001[/red]")
    console.print("  3. Rounding/Precision mismatch:     [red]Modified ID #41820 amount (Rp85,000.00 -> Rp85,001.00)[/red]")

    wh_conn = sqlite3.connect(wh_db)
    wh_conn.execute("DELETE FROM transactions WHERE id = 12450")
    wh_conn.execute("INSERT INTO transactions VALUES (50001, 1042, 99000.0, 'IDR', 'SUCCESS')")
    wh_conn.execute("UPDATE transactions SET amount = 85001.0 WHERE id = 41820")
    wh_conn.commit()
    wh_conn.close()

    table_cfg = TableConfig(
        name="transactions",
        key_column="id",
        columns=["account_id", "amount", "currency", "status"],
        bucket_size=2500,
    )

    src_adapter = SQLiteAdapter(prod_db)
    tgt_adapter = SQLiteAdapter(wh_db)
    src_adapter.connect()
    tgt_adapter.connect()

    try:
        reconciler = Reconciler(src_adapter, tgt_adapter, table_cfg)
        console.print(f"\n[bold green]Running crosscheck (bucket size = {table_cfg.bucket_size:,})...[/bold green]")
        
        t0 = time.perf_counter()
        result = reconciler.reconcile()
        t1 = time.perf_counter()

        render_report(result, console)

        throughput = int(row_count / (t1 - t0)) if (t1 - t0) > 0 else 0
        console.print(f"\n[dim]Throughput: {throughput:,} rows/sec[/dim]")

    finally:
        src_adapter.close()
        tgt_adapter.close()
        if os.path.exists(prod_db):
            os.remove(prod_db)
        if os.path.exists(wh_db):
            os.remove(wh_db)


if __name__ == "__main__":
    main()
