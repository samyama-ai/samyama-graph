# ADR-021: Columnar Property Store

## Status
**Shipped** (v1.0.0, 2026-04-11)

## Date
2026-05-05

## Context

Per-node `HashMap<String, PropertyValue>` storage cost is dominated by HashMap overhead, not by value size. A minimal `HashMap` is ~48 B; a one-entry `HashMap` is ~224 B. For a 100 M-node graph where the average node has 2–3 properties out of a union of ~80 keys, the per-node map design pays roughly 22 GB of pure overhead before any property values land in memory.

Two further requirements pushed us toward a columnar design:

1. **Two-phase loading**: stub topology first (cheap), properties second (deferred). Per-node HashMaps make Phase A pay for Phase B unconditionally.
2. **Late materialization** (ADR-012): query operators carry `Value::NodeRef(id)` and resolve properties only when needed. The resolver wants O(1) `(node, key) → value` access independent of the rest of the node's properties.

Constraints:
- Property values are heterogeneous (Int, Float, String, Bool, Map, Array, Vector).
- Sparsity is high: most (node, key) pairs are absent.
- Mutability is required: loaders write during Phase B; transactions write at runtime.
- Persistence must round-trip: snapshot export/import must preserve all properties.

## Decision

We will store node and edge primitive properties in a **per-property-key sparse column** keyed by entity index.

```rust
pub enum Column {
    Int(HashMap<usize, i64>),
    Float(HashMap<usize, f64>),
    String(HashMap<usize, String>),
    Bool(HashMap<usize, bool>),
}

pub struct ColumnStore {
    columns: HashMap<String, Column>,
}
```

Sparsity is the load-bearing property: only `(idx, key)` pairs that are explicitly set consume memory. Type-monomorphic columns avoid the variant tag overhead per cell and let downstream operators (e.g., aggregation) read a typed slice when one is available.

`Map`, `Array`, and `Vector` values fall through to the legacy per-node `PropertyMap` because their irregular sizes make per-cell columnar layout awkward. Vector embeddings remain in the per-node map and are consumed by HNSW.

## Consequences

### Positive

- **5–10× memory reduction** on multi-label graphs. Verified against the 187 M / 1.29 B hero run (project_hero_results.md): property-store memory ~36 GB vs an estimated ~200 GB+ for per-node HashMaps.
- **Late materialisation friendly**: `get_property(idx, key)` is O(1) without touching unrelated columns.
- **Loader-friendly**: Phase A creates stubs without paying property cost.
- **Cypher-friendly**: `RETURN n.title` for an `Article`-only filter scans the `title` column, not the full node arena.

### Negative

- **Two stores coexist** (legacy per-node `PropertyMap` and `ColumnStore`). Reads consult both. Cognitive load on every property-touching code path.
- **Type set on first write**: `Int` first → `Float` writes silently dropped. We accept this for deterministic loaders; ad-hoc writers can lose data.
- **String values clone on read**. `Arc<str>` interning would close this gap; not yet done.
- **Persistence is not yet column-native**: column store rebuilds on load from the legacy persistence path. ADR-021 follow-up.

### Neutral

- Adjacency, MVCC version chains, and indexes are unaffected — they continue to live in their own structures.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Per-node `HashMap<String, PropertyValue>` (legacy) | HashMap overhead dominates at scale (~22 GB on 100 M nodes). |
| Dense `Vec<Option<V>>` per column | Wastes memory for sparse columns; e.g. 1.6 GB for a 5 %-dense Int column on 100 M nodes. |
| Apache Arrow `RecordBatch` | Immutable buffers fight runtime mutation. Strong analytics-side fit, wrong runtime fit. |
| Type-erased value cells | Per-cell tag cost dominates; tagged-enum at column level is cheaper. |

## Follow-ups

1. Retire the legacy per-node `PropertyMap` once all readers have migrated to `ColumnStore` (post v1.1).
2. Adaptive physical layout: pick `Vec<V> + bitmap` when density > 30 %, `HashMap` when sparse.
3. Native columnar persistence: store columns directly as RocksDB blobs keyed by `(tenant, "col", column_name)`.
4. String interning: replace `String` cells with `Arc<str>` to remove read-side clones.
5. Type promotion: upcast `Int` → `Float` on first conflicting write rather than silently dropping.

## References

- Code: `samyama-graph/src/graph/storage/columnar.rs`
- Wiki: [[storage-columnar-property-store.md]], [[columnar-first-property-graph.md]], [[two-phase-loading.md]]
- Related ADRs: ADR-012 (Late Materialization), ADR-024 (Edge Arena Removal), ADR-020 (MVCC)
