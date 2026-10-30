#!/usr/bin/env python3
"""Audit the columns of a `.sgsnap`: are they actually filled in with anything?

A snapshot can load cleanly, report the right node counts and the right labels,
and still carry columns with nothing in them. #1815 found three, by hand, in
published snapshots:

  * `sider`: `SideEffect.name` is present on 5,805 nodes and holds 2 distinct
    values, `PT` and `LLT`. Those are MedDRA term types, not terms; the loader
    wrote the type column where the name belongs.
  * `health-systems`: `Country.income_level` and `Country.who_region` exist on
    all 233 `Country` nodes and are empty on every one.

Every existing check passes on both, because they count nodes and labels and
never look at what is inside a property. A column that is always present and
always empty is worse than an absent one: `exists(c.who_region)` is true, and
grouping by region silently produces one empty bucket.

This reads a snapshot and, for every node label and edge type, reports per
property how many records carry it, how many of those are empty, and how many
distinct non-empty values there are. It flags the shapes above:

    always-empty  present on records, empty on every one of them
    constant      one distinct non-empty value across at least --min-records
    few-values    2..--few distinct values across at least --min-records
                  (not reported for boolean columns, which have two by nature)

A flag means "look at this", not "this is wrong": `kind: "production"` on
every `System` node is constant and may be true. It is a cheap, generic
screen, so it can run over every published snapshot rather than being found by
hand in three.

A `.sgsnap` is gzipped newline-delimited JSON (an uncompressed one is read too),
so this needs no engine and no build:

    python3 scripts/snapshot_property_audit.py football.sgsnap
    python3 scripts/snapshot_property_audit.py football.sgsnap --flagged-only
    python3 scripts/snapshot_property_audit.py football.sgsnap --strict   # exit 1 on a flag

Exit status: 0 normally; 1 with `--strict` when anything is flagged; 2 when the
file cannot be read as a snapshot.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

# A column with more distinct values than this is not low-cardinality by any
# threshold a caller can ask for, so the exact set is no longer worth holding.
MAX_TRACKED = 100_000
GZIP_MAGIC = b"\x1f\x8b"


class SnapshotError(Exception):
    """The file is not a snapshot this script can read."""


class Column:
    """What one property looks like across the records of one group."""

    __slots__ = ("present", "empty", "values", "saturated", "only_bool")

    def __init__(self) -> None:
        self.present = 0
        self.empty = 0
        self.values: set[str] = set()
        self.saturated = False
        self.only_bool = True

    def add(self, value: object) -> None:
        self.present += 1
        if is_empty(value):
            self.empty += 1
            return
        if not isinstance(value, bool):
            self.only_bool = False
        if self.saturated:
            return
        # JSON text, not the value: it tells 1 from "1" from true, and it makes
        # lists and objects hashable.
        self.values.add(json.dumps(value, sort_keys=True))
        if len(self.values) > MAX_TRACKED:
            self.saturated = True

    @property
    def non_empty(self) -> int:
        return self.present - self.empty

    @property
    def distinct(self) -> int:
        return len(self.values)


def is_empty(value: object) -> bool:
    """Null, blank text, or an empty list/object: present but says nothing."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return not value
    return False


def open_snapshot(path: Path):
    """A text stream over the snapshot, gzipped or not."""
    try:
        with path.open("rb") as f:
            head = f.read(2)
    except OSError as e:
        raise SnapshotError(f"{path}: {e.strerror or e}") from e
    if head == GZIP_MAGIC:
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def audit(lines) -> dict:
    """Read snapshot lines; return the header, record counts and per-group columns."""
    groups: dict[str, dict[str, Column]] = {}
    group_size: dict[str, int] = {}
    header = None
    nodes = edges = 0

    def bump(group: str, props: dict) -> None:
        group_size[group] = group_size.get(group, 0) + 1
        cols = groups.setdefault(group, {})
        for key, value in props.items():
            cols.setdefault(key, Column()).add(value)

    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            raise SnapshotError(f"line {number}: not JSON ({e.msg})") from e
        if not isinstance(rec, dict):
            raise SnapshotError(f"line {number}: expected a JSON object")
        if header is None:
            if rec.get("format") != "sgsnap":
                raise SnapshotError("line 1: not an sgsnap header (no \"format\": \"sgsnap\"). "
                                    "An encrypted snapshot has to be decrypted first.")
            header = rec
            continue
        kind = rec.get("t")
        props = rec.get("props") or {}
        if not isinstance(props, dict):
            raise SnapshotError(f"line {number}: \"props\" is not an object")
        if kind == "n":
            nodes += 1
            # A node with several labels counts under each of them.
            for label in rec.get("labels") or ["(no label)"]:
                bump(f"({label})", props)
        elif kind == "e":
            edges += 1
            bump(f"[:{rec.get('type', '(no type)')}]", props)
        # Index (`i`) and hierarchy (`h`) records carry no properties.

    if header is None:
        raise SnapshotError("empty file: no header line")
    return {"header": header, "nodes": nodes, "edges": edges,
            "groups": groups, "group_size": group_size}


def flags_for(col: Column, min_records: int, few: int) -> list[str]:
    out = []
    if col.present and col.empty == col.present:
        out.append("always-empty")
    elif col.non_empty >= min_records and not col.saturated:
        if col.distinct == 1:
            out.append("constant")
        elif 2 <= col.distinct <= few and not col.only_bool:
            out.append("few-values")
    return out


def rows(result: dict, min_records: int, few: int) -> list[dict]:
    out = []
    for group in sorted(result["groups"]):
        for prop in sorted(result["groups"][group]):
            col = result["groups"][group][prop]
            out.append({
                "group": group,
                "property": prop,
                "records": result["group_size"][group],
                "present": col.present,
                "empty": col.empty,
                "distinct": col.distinct,
                "distinct_is_lower_bound": col.saturated,
                "flags": flags_for(col, min_records, few),
            })
    return out


def render(path: Path, result: dict, table: list[dict], flagged_only: bool) -> str:
    header = result["header"]
    out = [f"snapshot: {path.name}   {result['nodes']:,} nodes, {result['edges']:,} edges"]
    for what, got in (("node_count", result["nodes"]), ("edge_count", result["edges"])):
        if header.get(what) is not None and header[what] != got:
            out.append(f"warning: header says {what} {header[what]:,}, the file holds {got:,}")
    shown = [r for r in table if r["flags"]] if flagged_only else table
    if shown:
        w = max(len(r["group"]) for r in shown)
        p = max(len(r["property"]) for r in shown)
        out.append("")
        out.append(f"{'group':<{w}}  {'property':<{p}}  {'records':>8}  {'present':>8}  "
                   f"{'empty':>8}  {'distinct':>9}  flags")
        for r in shown:
            distinct = f"{r['distinct']:,}" + ("+" if r["distinct_is_lower_bound"] else "")
            out.append(f"{r['group']:<{w}}  {r['property']:<{p}}  {r['records']:>8,}  "
                       f"{r['present']:>8,}  {r['empty']:>8,}  {distinct:>9}  "
                       f"{', '.join(r['flags'])}")
    flagged = sum(1 for r in table if r["flags"])
    out.append("")
    out.append(f"{flagged} of {len(table)} properties flagged"
               + ("" if flagged else " -- nothing to look at"))
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report empty and low-information properties in a .sgsnap.")
    ap.add_argument("snapshot", type=Path)
    ap.add_argument("--min-records", type=int, default=100, metavar="N",
                    help="constant / few-values only count columns with at least N non-empty "
                         "values; a small column can legitimately repeat (default 100)")
    ap.add_argument("--few", type=int, default=3, metavar="N",
                    help="few-values means 2..N distinct values (default 3)")
    ap.add_argument("--flagged-only", action="store_true", help="print only the flagged properties")
    ap.add_argument("--json", action="store_true", help="print the table as JSON")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any property is flagged")
    args = ap.parse_args(argv)

    try:
        with open_snapshot(args.snapshot) as f:
            result = audit(f)
    except (SnapshotError, UnicodeDecodeError, EOFError, gzip.BadGzipFile) as e:
        print(f"{args.snapshot}: {e}", file=sys.stderr)
        return 2

    table = rows(result, args.min_records, args.few)
    if args.json:
        print(json.dumps({"snapshot": args.snapshot.name, "nodes": result["nodes"],
                          "edges": result["edges"], "properties": table}, indent=2))
    else:
        print(render(args.snapshot, result, table, args.flagged_only))
    return 1 if args.strict and any(r["flags"] for r in table) else 0


if __name__ == "__main__":
    sys.exit(main())
