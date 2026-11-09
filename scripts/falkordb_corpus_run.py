#!/usr/bin/env python3
"""Run the FalkorDB idiom corpus on FalkorDB and on Samyama (DX-13, #1787).

`tests/falkordb_compatibility_corpus.rs` measures what the compatibility report
*accepts*: parse and plan, no engine. The migration guide also states how many
idioms *run*, and that number needs two live servers. It used to be produced by
hand, with nothing recorded about the seed graph or the order, so nobody could
reproduce it. This is that measurement, written down.

What it does, in order:

1. Requires an empty Samyama graph with no indexes or constraints, resets the
   dedicated FalkorDB corpus graph, seeds both servers (SEED, below), and
   writes `people.csv` into the shared import directory. Restart the dedicated
   ephemeral Samyama server before each run; deleting nodes leaves schema behind.
2. Runs every corpus query **in file order, on one graph**: the corpus builds
   on itself (an index is created, then queried, then dropped), so running each
   query on a fresh graph would refuse things a user's script would not.
3. Records, per query and per engine, whether it ran or the error it returned.

"Runs" means the server returned a reply that is not an error. It does not
mean the rows match FalkorDB's: the reply shapes differ by design (see the
guide's protocol table), and a query that runs and returns something different
is the guide's section 3, not this count.

Start the two servers first, sharing one import directory:

    mkdir -p /tmp/fk-import
    # graph module 4.22.1, the version the guide was checked against
    docker run -d --name fk -p 127.0.0.1:6391:6379 \\
        -v /tmp/fk-import:/var/lib/FalkorDB/import \\
        falkordb/falkordb@sha256:b0bdcac2cbaff989e77cb0610439b7a9289e86de033ccbbd4368b66fb680ae7a
    target/release/samyama --ephemeral --port 6392 --http-port 8092 \\
        --import-dir /tmp/fk-import

Then:

    python3 scripts/falkordb_corpus_run.py --falkordb 6391 --samyama 6392 \\
        --import-dir /tmp/fk-import --json run.json

Exit status: 0 when every query ran on FalkorDB (the corpus is valid), 1 when
one did not (the corpus holds a query its own source engine refuses), 2 when a
server could not be reached or the Samyama server was not fresh.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

CORPUS = Path(__file__).resolve().parent.parent / "tests/fixtures/compat/falkordb-idioms.cypher"

# The graph every idiom runs against. Each name, label and property here is one
# the corpus refers to: Alice and Bob, ages for ORDER BY, a KNOWS edge with
# `since` for the relationship index, a Movie for the full-text and UNION idioms.
SEED = ("CREATE (a:Person {name: 'Alice', age: 30}), (b:Person {name: 'Bob', age: 25}), "
        "(a)-[:KNOWS {since: 2020}]->(b), (:Movie {title: 'The Matrix'})")
PEOPLE_CSV = "name\nErin\nFrank\n"

FALKOR_GRAPH = "corpus"
SAMYAMA_GRAPH = "default"   # the only graph key Samyama accepts


class RespError(Exception):
    pass


class Conn:
    """The smallest RESP client that will do: arrays of bulk strings out, any reply in."""

    def __init__(self, port: int, host: str = "127.0.0.1", timeout: float = 60):
        self.s = socket.create_connection((host, port), timeout=timeout)
        self.f = self.s.makefile("rb")

    def call(self, *args: str):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a.encode()
            out.append(b"$%d\r\n%s\r\n" % (len(b), b))
        self.s.sendall(b"".join(out))
        return self._read()

    def _read(self):
        line = self.f.readline()
        if not line:
            raise ConnectionError("connection closed")
        t, rest = line[:1], line[1:-2]
        if t == b"-":
            return RespError(rest.decode(errors="replace"))
        if t in (b"+", b","):
            return rest.decode()
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self.f.read(n + 2)[:-2].decode(errors="replace")
        if t in (b"*", b"~", b">"):
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        if t == b"%":
            return [[self._read(), self._read()] for _ in range(int(rest))]
        if t in (b"_", b"#"):
            return None if t == b"_" else rest == b"t"
        raise ValueError(f"unknown RESP type {line!r}")


def split_queries(text: str) -> list[str]:
    """`samyama::compat::split_queries` (src/compat.rs), line for line, so this
    runs exactly the queries the corpus test counts: `//`, `#` and `--` lines
    dropped; a query ends at a trailing `;`, a lone `;`, or a blank line."""
    out: list[str] = []
    current: list[str] = []

    def push():
        q = " ".join(current).strip()
        if q:
            out.append(q)
        current.clear()

    for line in text.splitlines():
        t = line.strip()
        if t.startswith(("//", "#", "--")):
            continue
        if t in (";", ""):
            push()
        elif t.endswith(";"):
            current.append(t[:-1])
            push()
        else:
            current.append(t)
    push()
    return out


def error_of(reply) -> str | None:
    """The error a reply carries, if any. FalkorDB can also put a run-time
    error inside an otherwise successful array reply."""
    if isinstance(reply, RespError):
        return str(reply)
    if isinstance(reply, list):
        for item in reply:
            e = error_of(item)
            if e:
                return e
    return None


def ensure_fresh_samyama(conn: Conn) -> None:
    """Refuse leftover data or schema before modifying either engine or the CSV."""
    for query in ("MATCH (n) RETURN count(n)", "SHOW INDEXES", "SHOW CONSTRAINTS"):
        reply = conn.call("GRAPH.QUERY", SAMYAMA_GRAPH, query)
        error = error_of(reply)
        if error:
            raise RuntimeError(f"could not inspect Samyama ({query}): {error}")
        # Samyama RESP results contain a header followed directly by data rows.
        if (not isinstance(reply, list) or not reply
                or not isinstance(reply[0], list) or not reply[0]
                or not all(isinstance(row, list) for row in reply[1:])):
            raise RuntimeError(f"unexpected Samyama response to {query}")
        rows = reply[1:]
        if query.startswith("MATCH"):
            if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not int:
                raise RuntimeError("unexpected Samyama node count response")
            occupied = rows[0][0] != 0
        else:
            occupied = bool(rows)
        if occupied:
            raise RuntimeError("Samyama contains data or schema; restart a dedicated "
                               "--ephemeral server before each corpus run")


def reset_falkordb(conn: Conn) -> None:
    """Reset only the dedicated graph; never measure after a refused reset."""
    def graphs():
        reply = conn.call("GRAPH.LIST")
        error = error_of(reply)
        if error:
            raise RuntimeError(f"could not list FalkorDB graphs: {error}")
        if not isinstance(reply, list) or not all(isinstance(g, str) for g in reply):
            raise RuntimeError("unexpected FalkorDB graph list response")
        return reply

    # An absent graph needs no deletion. Every error deleting an existing graph
    # is fatal, including ACL refusals; a successful query is not proof of reset.
    if FALKOR_GRAPH in graphs():
        error = error_of(conn.call("GRAPH.DELETE", FALKOR_GRAPH))
        if error:
            raise RuntimeError(f"could not reset FalkorDB graph: {error}")
        if FALKOR_GRAPH in graphs():
            raise RuntimeError("FalkorDB corpus graph remains after reset")


def run(conn: Conn, graph: str, queries: list[str]) -> list[str | None]:
    return [error_of(conn.call("GRAPH.QUERY", graph, q)) for q in queries]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--falkordb", type=int, required=True, help="FalkorDB RESP port")
    ap.add_argument("--samyama", type=int, required=True, help="Samyama RESP port")
    ap.add_argument("--import-dir", type=Path, required=True,
                    help="directory both servers read LOAD CSV files from")
    ap.add_argument("--corpus", type=Path, default=CORPUS)
    ap.add_argument("--json", type=Path, help="write per-query results here")
    args = ap.parse_args(argv)

    queries = split_queries(args.corpus.read_text())

    try:
        fk, sg = Conn(args.falkordb), Conn(args.samyama)
        ensure_fresh_samyama(sg)
        reset_falkordb(fk)
        (args.import_dir / "people.csv").write_text(PEOPLE_CSV)
        for conn, graph in ((fk, FALKOR_GRAPH), (sg, SAMYAMA_GRAPH)):
            e = error_of(conn.call("GRAPH.QUERY", graph, SEED))
            if e:
                raise RuntimeError(f"could not seed {graph}: {e}")
        fk_res = run(fk, FALKOR_GRAPH, queries)
        sg_res = run(sg, SAMYAMA_GRAPH, queries)
    except (OSError, ConnectionError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    rows = [{"query": q, "falkordb_error": f, "samyama_error": s}
            for q, f, s in zip(queries, fk_res, sg_res)]
    fk_ok = sum(r["falkordb_error"] is None for r in rows)
    sg_ok = sum(r["samyama_error"] is None for r in rows)

    for r in rows:
        if r["samyama_error"] or r["falkordb_error"]:
            print(f"  FalkorDB {'ok ' if r['falkordb_error'] is None else 'ERR'}  "
                  f"Samyama {'ok ' if r['samyama_error'] is None else 'ERR'}  {r['query'][:70]}")
            if r["samyama_error"]:
                print(f"      samyama: {r['samyama_error'][:110]}")
            if r["falkordb_error"]:
                print(f"      falkordb: {r['falkordb_error'][:110]}")
    print(f"\nqueries: {len(rows)}\nrun on FalkorDB: {fk_ok} of {len(rows)}\n"
          f"run on Samyama: {sg_ok} of {len(rows)}")

    if args.json:
        args.json.write_text(json.dumps({"queries": len(rows), "falkordb_runs": fk_ok,
                                         "samyama_runs": sg_ok, "rows": rows}, indent=2) + "\n")
    return 0 if fk_ok == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
