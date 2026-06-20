"""Narrated-demo helpers shared by every case study.

Generalizes the rich-Console scaffold first written for powergrid-kg/telecom-kg
so each domain's demo.py is just: a title, a list of showcase queries (read from
queries.cypher), and a closing takeaway. Recorded with asciinema and rendered to
GIF by record_gif.sh.
"""
from __future__ import annotations

import os
import time

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from samyama_http import SamyamaClient, parse_queries

console = Console()


def pause(s: float = 1.3) -> None:
    # Honour a global speed knob so recording cadence is tunable without edits.
    time.sleep(s * float(os.environ.get("DEMO_PACE", "1.0")))


def title_card(domain: str, tagline: str, source: str) -> None:
    console.print(Panel.fit(
        f"[bold]Samyama · {domain}[/bold]\n{tagline}\n[dim]{source}[/dim]",
        border_style="cyan",
    ))
    pause(1.4)


def step(n: int, title: str) -> None:
    console.print()
    console.rule(f"[bold cyan]{n} · {title}")
    pause(0.5)


def show_scale(client: SamyamaClient) -> None:
    nodes, edges = client.node_count(), client.edge_count()
    console.print(f"  [green]imported[/green] [bold]{nodes:,}[/bold] nodes · "
                  f"[bold]{edges:,}[/bold] edges")
    pause()


def run_query(client: SamyamaClient, q, idx: int) -> None:
    """Run one ShowcaseQuery, render result as a table with timing."""
    console.print(f"  [dim]Q{idx}.[/dim] [bold white]{q.title}[/bold white]")
    if q.insight:
        console.print(f"       [italic dim]{q.insight}[/italic dim]")
    one_line = " ".join(q.cypher.split())
    console.print(f"  [dim]cypher>[/dim] [yellow]{one_line}[/yellow]")
    res = client.query(q.cypher)
    table = Table(show_edge=False, pad_edge=False, box=None, header_style="bold magenta")
    for col in res.columns:
        table.add_column(str(col))
    for row in res.records[:8]:
        table.add_row(*[_fmt(v) for v in row])
    console.print(table)
    console.print(f"  [green]→[/green] {res.rows} row(s) in [bold]{res.elapsed_ms:.1f} ms[/bold]")
    # Hold on the result so a reader can take in the query AND its answer. GIFs
    # can't be paused in a browser, so the read time has to be built into the
    # recording (and demo.cast can be replayed pausably with `asciinema play`).
    pause(3.2)


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.3g}"
    if isinstance(v, dict):  # node/edge object — show a label or id
        props = v.get("properties", {})
        return str(props.get("name") or props.get("title") or v.get("id", v))
    return str(v)


def takeaway(text: str) -> None:
    console.print()
    console.print(Panel.fit(f"[bold green]{text}[/bold green]", border_style="green"))
    pause(1.6)


def run_demo(domain: str, tagline: str, source: str, takeaway_text: str,
             queries_path: str = "queries.cypher",
             base_url: str | None = None, graph: str | None = None) -> None:
    """Full narrated run: title → scale → each showcase query → takeaway."""
    base_url = base_url or os.environ.get("SG_BASE_URL", "http://127.0.0.1:8080")
    graph = graph or os.environ.get("SG_GRAPH", "default")
    client = SamyamaClient(base_url, graph)
    title_card(domain, tagline, source)
    step(1, "Snapshot imported into Samyama")
    show_scale(client)
    queries = parse_queries(queries_path)
    for i, q in enumerate(queries, start=1):
        step(i + 1, q.title)
        run_query(client, q, i)
    takeaway(takeaway_text)
