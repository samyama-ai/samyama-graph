# ADR-028: Label and Property-Key Interning

## Status
**Shipped** (v1.0.0, 2026-04-11) — files the gap surfaced in §2.3 of the Engineering Compendium.

## Date
2026-05-05

## Context

Edge types are interned to `u16` (`edge_type_table: Vec<EdgeType>` + `edge_type_to_id: HashMap<EdgeType, u16>` + per-edge `edge_type_ids: Vec<u16>`). At 1 B edges this saves roughly 22 GB versus a per-edge `String`-typed edge type.

Labels are **not** interned. `Node.labels: HashSet<Label>` and `Label(String)` mean every node carries independent `String` allocations for each label, regardless of how many other nodes carry the same labels. The `label_index: HashMap<Label, HashSet<NodeId>>` does keep one canonical `Label` per distinct label as the *map key*, but the per-node `Node.labels` set holds independent clones.

Quantified at hero scale (187 M nodes, ~1.5 labels per node average): roughly 5–12 GB of avoidable RAM. Plus per-call `String::from` allocation cost in three known hotspots (planner statistics, expand-with-label-filter, `rebuild_label_index`).

A symmetric gap exists for **property keys**: the columnar store interns column names by virtue of its `HashMap<String, Column>` shape, but the legacy per-node `PropertyMap` (still alive for nodes created via `create_node_with_properties`) holds independent `String` keys per (node, key) pair.

## Decision

We will intern labels to a `LabelId(u16)` and property keys to a `PropertyKeyId(u16)`, symmetric with the existing edge-type interning.

### Surfaces affected

1. **Storage**: `Node.labels: SmallVec<[LabelId; 2]>` (most nodes have 1–2 labels). `label_index: HashMap<LabelId, RoaringBitmap>` (combine with the §3.1 bitset proposal).
2. **AST / parser**: `Label(String)` in the AST is preserved through parsing; conversion to `LabelId` happens at the planner boundary so `EXPLAIN` text retains label names.
3. **Cost model / catalog**: `GraphCatalog`'s `label_counts: HashMap<LabelId, usize>` and `TriplePattern { source_label: LabelId, edge_type: u16, target_label: LabelId }`.
4. **Indexes**: `IndexManager`'s `PropertyIndexKey { label: LabelId, property: PropertyKeyId }`.
5. **Cypher result encoding**: labels round-trip back to `String` at the protocol layer.
6. **Persistence**: WAL and snapshot continue to carry `String` labels — interning is in-memory only. The label table reconstructs on load.

### Migration

Mechanical but broad. ~30 files touched per a quick grep. We will land it as a single PR rather than a multi-step migration to avoid a long-lived dual-form representation.

## Consequences

### Positive
- **5–12 GB RAM saved** at hero scale.
- **Per-call `String::from` allocation** removed from the three known planner hotspots.
- **`SmallVec<[LabelId; 2]>` vs `HashSet<Label>`** — also closes the per-node `HashSet` overhead for the multi-label case.
- **Symmetric mental model** with edge-type interning. One pattern, not two.
- **Unblocks** the §3.1 bitset / Roaring label-index proposal (bitsets need integer keys).

### Negative
- **Wide PR.** Touches AST, planner, executor, store, persistence boundary, index, catalog. Test coverage is good but the migration risk is real.
- **`Label` newtype changes shape.** Any external code that depends on `Label::from(&str)` continues to work but a new public `LabelId` type appears in some signatures. Source compatibility for downstream Rust callers needs an audit.
- **Property keys are a smaller win.** The columnar store already interns at the column level; the savings come from retiring the legacy `PropertyMap`. ADR-021's follow-up on dual-store sunset is the prerequisite.
- **`u16` ceiling = 65 535 distinct labels per tenant.** No realistic tenant approaches this; an `EdgeTypeIdOverflow`-style error path is added defensively.

### Neutral
- Snapshot format unchanged (labels remain strings on the wire — §1.3 / ADR-022). Interning rebuilds on import.
- Cypher surface unchanged.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Status quo (string-typed labels) | Pays 5–12 GB at hero, plus per-call alloc cost. |
| `Arc<str>` instead of `u16` | Pays 16 B per reference vs 2 B; better than `String`, worse than `u16` for low-cardinality. |
| `&'static str` interner | Cannot accommodate user-defined labels. |
| Per-tenant intern table backed by FlatBuffer | Compile-time fixed; no good for dynamic schemas. |
| Defer until §3.1 bitset migration | The two changes compose; doing labels first unblocks bitsets cleanly. |

## Follow-ups

1. After this lands, complete the §3.1 bitset / Roaring label-index migration.
2. After ADR-021's legacy `PropertyMap` sunset, retire per-node `String` property keys.
3. Document the `LabelId`/`PropertyKeyId` API in the contributor guide.

## References

- Wiki: [[topics/memory-string-interning.md]] (full critique and engineering surface).
- Code (current state): `samyama-graph/src/graph/types.rs:93,126`, `src/graph/store.rs:559,562,1042-1062`.
- Related ADRs: ADR-021 (Columnar Property Store — covers property values; this ADR covers property *keys*), ADR-029 (IndexManager), ADR-015 (Graph-Native Query Planning), ADR-017 (Adjacency-Aware Aggregation Planning).
