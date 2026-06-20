"""Definition-of-Done gate for a case study.

Runs every showcase query in queries.cypher against the running server and
asserts each returns >= 1 row with no error. Prints a timing table. Exits
non-zero if ANY query errors or returns zero rows — this is what makes a GIF
trustworthy to hand to an SME (no empty tables, no silent Cypher quirks).

Usage: python3 validate_queries.py [queries.cypher] [--base-url URL] [--graph G]
"""
from __future__ import annotations

import argparse
import sys

from rich.console import Console
from rich.table import Table

from samyama_http import SamyamaClient, parse_queries

console = Console()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("queries", nargs="?", default="queries.cypher")
    ap.add_argument("--base-url", default="http://127.0.0.1:8080")
    ap.add_argument("--graph", default="default")
    ap.add_argument("--min-rows", type=int, default=1)
    args = ap.parse_args()

    client = SamyamaClient(args.base_url, args.graph)
    nodes, edges = client.node_count(), client.edge_count()
    console.print(f"[bold]Validating[/bold] {args.queries} against "
                  f"{nodes:,} nodes / {edges:,} edges\n")

    queries = parse_queries(args.queries)
    if not queries:
        console.print("[red]No queries found — check the // @query markers.[/red]")
        return 2

    table = Table(title="Showcase query results")
    table.add_column("#", justify="right")
    table.add_column("Query")
    table.add_column("Rows", justify="right")
    table.add_column("ms", justify="right")
    table.add_column("Status")

    failures = 0
    for i, q in enumerate(queries, start=1):
        try:
            res = client.query(q.cypher)
            ok = res.rows >= args.min_rows
            status = "[green]PASS[/green]" if ok else "[red]EMPTY[/red]"
            if not ok:
                failures += 1
            table.add_row(str(i), q.title, str(res.rows), f"{res.elapsed_ms:.1f}", status)
        except Exception as e:  # noqa: BLE001
            failures += 1
            table.add_row(str(i), q.title, "-", "-", f"[red]ERROR[/red] {e}")

    console.print(table)
    if failures:
        console.print(f"\n[bold red]FAILED[/bold red]: {failures}/{len(queries)} "
                      f"queries returned no rows or errored.")
        return 1
    console.print(f"\n[bold green]OK[/bold green]: all {len(queries)} queries returned "
                  f">= {args.min_rows} row(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
