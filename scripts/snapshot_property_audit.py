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

Groups are named `(Label)` for nodes (a node with several labels counts once
under each), `[:TYPE]` for edges, and `<unlabeled>` for a node with no label.

Distinct values are counted the way the engine will see them: it trims every
string on import, so `"PT"` and `"PT "` are one value, and so are strings
nested in a list or a map.

What it reads, and what it refuses
----------------------------------
A `.sgsnap` is gzipped newline-delimited JSON (an uncompressed one is read too),
so this needs no engine and no build. It is not a full validator, but a report
that is clean because part of the file was never audited is worse than no
report, so it exits 2 instead of reading on when:

  * the header is not an `sgsnap` header of version 1 or 2, or its
    `node_count` / `edge_count` differ from what the file holds (a truncated
    file looks exactly like a small clean one);
  * a record is not one of the engine's types: `n` (node), `e` (edge), `i`
    (index catalog) or `h` (hierarchy declaration). The engine's own loader
    skips an unknown type; here a skipped record is a record never audited;
  * a node or edge lacks a field the engine requires (`id`, `labels` as an
    array of strings, `props` as an object; for an edge also `src`, `tgt` and a
    string `type`);
  * the JSON holds NaN or Infinity, a lone surrogate, or is nested or large
    beyond what Python will read.

    python3 scripts/snapshot_property_audit.py football.sgsnap
    python3 scripts/snapshot_property_audit.py football.sgsnap --flagged-only
    python3 scripts/snapshot_property_audit.py football.sgsnap --strict   # exit 1 on a flag

Exit status: 0 normally; 1 with `--strict` when anything is flagged; 2 when the
file cannot be read as a snapshot or an option is out of range.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
import zlib
from pathlib import Path

# A column with more distinct values than this is not low-cardinality by any
# threshold a caller can ask for, so the exact set is no longer worth holding.
MAX_TRACKED = 100_000
GZIP_MAGIC = b"\x1f\x8b"
SUPPORTED_VERSIONS = (1, 2)
# Record types the engine writes (src/snapshot/mod.rs): node, edge, index
# catalog and hierarchy declaration. The last two carry no properties.
METADATA_RECORDS = ("i", "h")
# A group is named for what it holds: `(Label)` for a node label, `[:TYPE]` for
# an edge type. A node with no label goes in this one instead. It starts with
# neither "(" nor "[:", so it cannot equal the name of any real label or type --
# including one literally called "no label" or "" -- and the group name can be
# both the identity and the display text.
UNLABELED = "<unlabeled>"


class SnapshotError(Exception):
    """The file is not a snapshot this script can read."""


class Column:
    """What one property looks like across the records of one group."""

    __slots__ = ("present", "empty", "values", "saturated", "only_bool")

    def __init__(self) -> None:
        self.present = 0
        self.empty = 0
        self.values: set[bytes] = set()
        self.saturated = False
        self.only_bool = True

    def add(self, value: object) -> None:
        self.present += 1
        if is_empty(value):
            self.empty += 1
            return
        if not isinstance(value, bool):
            self.only_bool = False
        # JSON text of the value as the engine will hold it: it tells 1 from
        # "1" from true, and it makes lists and objects comparable. Encoding it
        # refuses a lone surrogate, which no JSON reader the engine uses accepts.
        data = json.dumps(normalize(value), sort_keys=True, ensure_ascii=False).encode("utf-8")
        if self.saturated:
            return
        # A 16-byte digest, not the text: a column of long descriptions or
        # vectors would otherwise hold 100,000 full copies of itself.
        self.values.add(hashlib.blake2b(data, digest_size=16).digest())
        if len(self.values) > MAX_TRACKED:
            self.saturated = True

    @property
    def non_empty(self) -> int:
        return self.present - self.empty

    @property
    def distinct(self) -> int:
        return len(self.values)


def normalize(value: object) -> object:
    """The value after the engine's import: every string trimmed, however nested."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return [normalize(v) for v in value]
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in value.items()}
    return value


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
    """A text stream over the snapshot, gzipped or not. Lines end at "\\n" only,
    as the engine reads them; a bare "\\r" inside a line is JSON whitespace."""
    try:
        with path.open("rb") as f:
            head = f.read(2)
    except OSError as e:
        raise SnapshotError(f"{path}: {e.strerror or e}") from e
    if head == GZIP_MAGIC:
        return gzip.open(path, "rt", encoding="utf-8", newline="\n")
    return path.open("r", encoding="utf-8", newline="\n")


def _no_constant(name: str):
    raise ValueError(f"{name} is not valid JSON")


def _require_id(rec: dict, key: str, number: int) -> None:
    v = rec.get(key)
    if isinstance(v, bool) or not isinstance(v, int) or v < 0:
        raise SnapshotError(f"line {number}: \"{key}\" is not a non-negative integer")


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
            key.encode("utf-8")
            cols.setdefault(key, Column()).add(value)

    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line, parse_constant=_no_constant)
        except json.JSONDecodeError as e:
            raise SnapshotError(f"line {number}: not JSON ({e.msg})") from e
        except RecursionError as e:
            raise SnapshotError(f"line {number}: nested too deeply to read") from e
        except ValueError as e:   # NaN / Infinity, or an integer too long for Python to convert
            raise SnapshotError(f"line {number}: {e}") from e
        if not isinstance(rec, dict):
            raise SnapshotError(f"line {number}: expected a JSON object")
        if header is None:
            if rec.get("format") != "sgsnap":
                raise SnapshotError("line 1: not an sgsnap header (no \"format\": \"sgsnap\"). "
                                    "An encrypted snapshot has to be decrypted first.")
            version = rec.get("version")
            if isinstance(version, bool) or not isinstance(version, int) or version not in SUPPORTED_VERSIONS:
                raise SnapshotError(f"line 1: unsupported snapshot version {json.dumps(version)} "
                                    f"(expected 1 or 2)")
            header = rec
            continue
        kind = rec.get("t")
        # The engine's own loader skips record types it does not know, so a
        # snapshot with a misspelt or missing "t" still loads there. Here a
        # skipped record is a record that was never audited, and the report
        # would read as clean, so an unknown type is an error.
        if kind not in ("n", "e") + METADATA_RECORDS:
            shown = "missing" if "t" not in rec else json.dumps(kind)
            raise SnapshotError(f"line {number}: unknown record type {shown} "
                                f"(expected one of \"n\", \"e\", \"i\", \"h\")")
        if kind in METADATA_RECORDS:
            continue
        # The engine requires "props" on every node and edge. Falsy payloads
        # such as [], false, 0 and "" are malformed and must not read as empty.
        props = rec.get("props")
        if not isinstance(props, dict):
            raise SnapshotError(f"line {number}: \"props\" is not an object")
        _require_id(rec, "id", number)
        try:
            if kind == "n":
                labels = rec.get("labels")
                # A bare string would be iterated letter by letter into one group each.
                if not isinstance(labels, list) or not all(isinstance(x, str) for x in labels):
                    raise SnapshotError(f"line {number}: \"labels\" is not an array of strings")
                nodes += 1
                for label in labels:
                    label.encode("utf-8")
                # A node with several labels counts once under each of them, even
                # if a label is repeated. No label at all is legal.
                for group in [f"({label})" for label in dict.fromkeys(labels)] or [UNLABELED]:
                    bump(group, props)
            else:
                _require_id(rec, "src", number)
                _require_id(rec, "tgt", number)
                etype = rec.get("type")
                if not isinstance(etype, str):
                    raise SnapshotError(f"line {number}: \"type\" is not a string")
                edges += 1
                etype.encode("utf-8")
                bump(f"[:{etype}]", props)
        except UnicodeEncodeError as e:
            raise SnapshotError(f"line {number}: invalid Unicode (a lone surrogate)") from e

    if header is None:
        raise SnapshotError("empty file: no header line")
    # A file that stops early is otherwise indistinguishable from a small clean one.
    for what, got in (("node_count", nodes), ("edge_count", edges)):
        claimed = header.get(what)
        if isinstance(claimed, int) and not isinstance(claimed, bool) and claimed != got:
            raise SnapshotError(f"header says {what} {claimed:,} but the file holds {got:,} "
                                f"(truncated or modified?)")
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
    out = [f"snapshot: {path.name}   {result['nodes']:,} nodes, {result['edges']:,} edges"]
    shown = [r for r in table if r["flags"]] if flagged_only else table
    if shown:
        head = ("group", "property", "records", "present", "empty", "distinct", "flags")
        body = [(r["group"], r["property"], f"{r['records']:,}", f"{r['present']:,}",
                 f"{r['empty']:,}",
                 f"{r['distinct']:,}" + ("+" if r["distinct_is_lower_bound"] else ""),
                 ", ".join(r["flags"])) for r in shown]
        # Widths come from the headings as well as the data, so a short name or a
        # large count never pushes a column out of line with its heading.
        widths = [max(len(h), *(len(row[i]) for row in body)) for i, h in enumerate(head)]
        left = (0, 1, 6)

        def line(cells) -> str:
            return "  ".join(c.ljust(w) if i in left else c.rjust(w)
                             for i, (c, w) in enumerate(zip(cells, widths))).rstrip()

        out.append("")
        out.append(line(head))
        out.extend(line(row) for row in body)
    flagged = sum(1 for r in table if r["flags"])
    out.append("")
    out.append(f"{flagged} of {len(table)} properties flagged"
               + ("" if flagged else " -- nothing to look at"))
    return "\n".join(out)


def emit(text: str) -> None:
    """Print, and stay quiet if the reader went away (`... | head`)."""
    try:
        print(text)
        sys.stdout.flush()
    except BrokenPipeError:
        # Point stdout at /dev/null so the interpreter's own flush at exit has
        # nothing left to fail on.
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except (OSError, ValueError):
            pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report empty and low-information properties in a .sgsnap.")
    ap.add_argument("snapshot", type=Path)
    ap.add_argument("--min-records", type=int, default=100, metavar="N",
                    help="constant / few-values only count columns with at least N non-empty "
                         "values; a small column can legitimately repeat (default 100)")
    ap.add_argument("--few", type=int, default=3, metavar="N",
                    help=f"few-values means 2..N distinct values (default 3, at most {MAX_TRACKED:,}: "
                         "distinct values are not tracked beyond that)")
    ap.add_argument("--flagged-only", action="store_true",
                    help="list only the flagged properties (in the JSON too)")
    ap.add_argument("--json", action="store_true", help="print the result as JSON")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any property is flagged")
    args = ap.parse_args(argv)
    # A label can hold characters the terminal's encoding cannot print.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")
    # A column past the tracking cap is never classified, so a threshold above
    # the cap would silently miss columns it should flag.
    if not 2 <= args.few <= MAX_TRACKED:
        ap.error(f"--few must be between 2 and {MAX_TRACKED:,}")
    if args.min_records < 1:
        ap.error("--min-records must be at least 1")

    try:
        with open_snapshot(args.snapshot) as f:
            result = audit(f)
    except UnicodeDecodeError:
        print(f"{args.snapshot}: not UTF-8 text (an encrypted or binary file? "
              "an encrypted snapshot has to be decrypted first)", file=sys.stderr)
        return 2
    except (SnapshotError, EOFError, gzip.BadGzipFile, zlib.error, RecursionError) as e:
        print(f"{args.snapshot}: {e}", file=sys.stderr)
        return 2

    table = rows(result, args.min_records, args.few)
    flagged = [r for r in table if r["flags"]]
    if args.json:
        emit(json.dumps({"snapshot": args.snapshot.name, "nodes": result["nodes"],
                         "edges": result["edges"], "properties_total": len(table),
                         "properties_flagged": len(flagged),
                         "properties": flagged if args.flagged_only else table}, indent=2))
    else:
        emit(render(args.snapshot, result, table, args.flagged_only))
    # --strict judges the whole table, not what --flagged-only chose to show.
    return 1 if args.strict and flagged else 0


if __name__ == "__main__":
    sys.exit(main())
