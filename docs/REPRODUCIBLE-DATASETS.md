# Reproducible datasets

A training dataset is only useful for an experiment if it can be built again
byte for byte, and if anyone holding the bytes can say what they are a rebuild
of. `POST /api/query/export` makes an export addressable by
**(snapshot hash, query, seed)** (ML-08).

```bash
curl -sD headers.txt -X POST http://localhost:8080/api/query/export \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n:Item) WHERE rand() < 0.1 RETURN n.id AS id, rand() AS r ORDER BY id",
       "format": "parquet", "seed": 42}' \
  -o sample.parquet
```

## What the response carries

The body is the file. The manifest travels in headers, so it never corrupts
the data it describes:

| Header | Value |
|---|---|
| `x-samyama-dataset-manifest` | JSON: `snapshot_hash`, `query`, `seed`, `format`, `graph`, `engine_version`, `rows`, `content_hash`. Non-ASCII characters are `\u` escaped, so any JSON parser reads it. |
| `x-samyama-snapshot-hash` | `sha256:…` over the graph the query read |
| `x-samyama-content-hash` | `sha256:…` over the body bytes |
| `x-samyama-seed` | the seed, or `none` |

## Rebuilding

Send the manifest's `query`, `seed` and `format` back, with its
`snapshot_hash` as a pin:

```json
{"query": "...", "seed": 42, "format": "parquet", "snapshot_hash": "sha256:..."}
```

The same graph gives the same bytes. A different graph is refused with
**409** naming the hash it has now, rather than quietly building a different
dataset. Drop `snapshot_hash` to export from the graph as it is.

## What the hashes cover

**`snapshot_hash`** is computed from the data, not from `snapshot_version`. It
covers what a query can see:
- every live node's id, labels and properties;
- every relationship's id, endpoints, type and properties, including bulk-loaded ones.

It leaves out indexes, constraints, creation times and superseded versions. A
`.sgsnap` round trip into a fresh server keeps it. Each export computes it
afresh, in one pass over the graph.

## What `seed` covers

`seed` drives `rand()` and `randomUUID()` for that statement. A filter that
draws random numbers is never split across threads, so the draws come in row
order. The same seed means the same draws **on the same engine version**. The
generator is `rand` 0.8's `StdRng`, which is why the manifest records
`engine_version`.

**Without a seed, a query that calls `rand()` is not reproducible**, and the
manifest says so with `"seed": null`. Algorithms that take their own `seed`
(`algo.split`, the samplers, `or.solve`) are reproducible from that
argument whether or not the export is seeded.

Unknown fields in the request are refused with 400. A `seed` used to be
dropped silently.
