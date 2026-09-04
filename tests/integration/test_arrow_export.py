"""The Parquet and Arrow export, read in pandas, against the same query's JSON.

The Rust suite (`tests/arrow_parquet_export.rs`) checks the writer with Arrow's
own readers. This checks the half that one cannot: that **pandas** opens what we
write and gets the same values back. Spec 09 asks for exactly that — "read in
pandas/polars, compare against the direct query result" — and a round-trip that
only Arrow can read would satisfy the letter of it and none of the point.

Needs a running server and `pip install pandas pyarrow`:

    cargo run --release -- --ephemeral --http-port 8080 &
    python3 tests/integration/test_arrow_export.py

Set `SAMYAMA_HTTP` to point at another host. Exits non-zero on any mismatch.
"""
import io, json, os, sys, urllib.request, uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.environ.get("SAMYAMA_HTTP", "http://localhost:8080")
# A label of its own per run, so the fixture is three rows whether or not the
# server has been used before. Re-running against a live graph should not make
# the answer depend on how many times this has been run.
LABEL = "ArrowExportProbe" + uuid.uuid4().hex[:8]
Q = (f"MATCH (d:{LABEL}) RETURN d.ord AS ord, d.title AS title, d.score AS score, "
     "d.live AS live, d.tags AS tags, d.counts AS counts ORDER BY ord")


def post(path, body, raw=False):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return r.read() if raw else json.loads(r.read())


for c in [
    f'CREATE (:{LABEL} {{ord: 1, title: "café — naïve 日本語 🌏", score: 1.5, live: true, '
    'tags: ["a", "b"], counts: [1, 2, 3]})',
    f'CREATE (:{LABEL} {{ord: 2, title: "plain", live: false}})',
    f'CREATE (:{LABEL} {{ord: 3, title: "", score: -0.25, live: true, tags: [], '
    'counts: [7]})',
]:
    post("/api/query", {"graph": "default", "query": c})

js = post("/api/query", {"graph": "default", "query": Q})
cols = js["columns"]
rows = js["records"]

pqt = pq.read_table(pa.BufferReader(
    post("/api/query/export", {"graph": "default", "query": Q, "format": "parquet"}, raw=True)))
ipc = pa.ipc.open_stream(io.BytesIO(
    post("/api/query/export", {"graph": "default", "query": Q, "format": "arrow"}, raw=True))
).read_all()

df = pqt.to_pandas()
print("pandas dtypes:")
print(df.dtypes.to_string())
print()
print(df.to_string())
print()

fail = []
if list(pqt.column_names) != cols:
    fail.append(f"columns {pqt.column_names} != {cols}")
if pqt.num_rows != 3:
    fail.append(f"{pqt.num_rows} rows, expected the 3 this run created")
if pqt.num_rows != len(rows):
    fail.append(f"{pqt.num_rows} parquet rows != {len(rows)} json rows")
if not pqt.equals(ipc):
    fail.append("the parquet and arrow-stream tables differ")

def norm(v):
    if v is None or (isinstance(v, float) and v != v):
        return None
    if hasattr(v, "tolist"):
        v = v.tolist()
    if isinstance(v, list):
        return [norm(x) for x in v]
    if isinstance(v, float) and v.is_integer():
        return v
    return v

for i, row in enumerate(rows):
    for j, col in enumerate(cols):
        want, got = norm(row[j]), norm(df.iloc[i][col])
        # JSON has no int/float distinction to preserve; compare numerically.
        if isinstance(want, (int, float)) and isinstance(got, (int, float)):
            same = float(want) == float(got)
        else:
            same = want == got
        if not same:
            fail.append(f"row {i} col {col}: json {want!r} != parquet {got!r}")

print("FAIL:" if fail else "PASS — parquet and arrow match the JSON result on every cell")
for f in fail:
    print(" ", f)
sys.exit(1 if fail else 0)
