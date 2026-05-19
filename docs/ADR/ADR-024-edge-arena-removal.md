# ADR-024: Edge Arena Removal (DS-07c)

## Status
**Shipped** (v1.0.0, 2026-04-11) — retroactively documenting the v0.8.0 / v0.9.0 / v1.0.0 shipped change.

## Date
2026-05-05 (decision originally made 2026-03 in the DS-07 plan).

## Context

Pre-DS-07c, `GraphStore` maintained two edge representations:

1. **Edge arena** (`edges: Vec<Vec<Edge>>`) — full `Edge` objects with `source`, `target`, `edge_type`, `properties`, MVCC pointer. ~709 B/edge fully populated.
2. **Adjacency lists** (`outgoing/incoming: Vec<Vec<(NodeId, EdgeId)>>`) — lightweight topology.

A `create_edge_stub` shortcut bypassed the arena and gave 13.6× memory savings, but it created an inconsistency: `get_edge()` returned `None`, snapshot export had two paths, and consumers had to remember which path applied.

At 1 B edges, the arena cost was ~52 GB of edge-store memory even after the stub optimisation. The hero benchmark target (1.29 B edges on x2iedn) required us to push the cost lower.

## Decision

Remove the edge arena entirely. Replace it with two narrower structures:

```rust
edge_endpoints:  Vec<(NodeId, NodeId)>,           // dense, 16 B/edge, indexed by EdgeId.as_usize()
edge_properties: HashMap<EdgeId, PropertyMap>,    // sparse, 0 B for property-less edges
```

Edge type stays in the catalog; reads reconstruct edges as a lightweight `EdgeView { id, source, target, edge_type }`. `get_edge` is retained for compatibility but builds the `Edge` on demand.

Phased migration (shipped over v0.8.0 → v1.0.0):
- **Phase 1–2**: Add `edge_endpoints`, `edge_properties`, `EdgeView` in dual-write mode.
- **Phase 3**: Migrate `record.rs`, `operator.rs`, `snapshot.rs` to the new fallbacks.
- **Phase 4a**: Unify snapshot export to the adjacency-only path (this drove `.sgsnap` v1 → v2).
- **Phase 4b**: Remove the `edges` arena field (v0.8.0 OSS, v1.0.0 retired in enterprise).
- **Phase 5**: Cleanup + benchmark validation.

## Consequences

### Positive
- **65 % reduction** in edge-store memory at scale (52 GB → 18 GB at 1 B edges per project memory). Verified at 1.29 B-edge hero.
- Single source of truth for edge endpoints (no more "which path was used").
- Snapshot export simplified to one path.
- Loader memory ceiling stops scaling with edge count's full property cost.

### Negative
- 16 B/edge endpoint vector duplicates topology already present in adjacency lists.
- `HashMap<EdgeId, PropertyMap>` allocates a fresh map on first property write per edge; pooling deferred.
- Edge property snapshots clone the full map on version commit; `Arc`-shared maps deferred.
- v1 snapshots no longer round-trip; users on v1 snapshots must re-export.
- `EdgeView` is read-only and lossy (no properties); callers needing properties pay a `get_edge` clone.
- Edge `created_at` / `updated_at` dropped in v2 snapshots.

### Neutral
- Adjacency lists, MVCC version chains, and the index layer all continue to work. The change is local to edge storage.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Keep edge arena, optimise per-edge cost | Cost floor is too high (~709 B/edge fully populated). |
| Single packed `Vec<EdgeRecord>` with offsets to a property heap | Most compact but variable-length property part dominates the design. Strong fit for a future immutable read-only tier. |
| Sorted CSR with separate property side-table | Conflates topology with edge identity; deletes require recompaction. Adjacency already plays this role for traversal. |
| Implicit endpoints (recover from adjacency) | `EdgeId → endpoints` would become O(degree); breaks late materialisation guarantees. |

## Follow-ups

1. **Tombstone bit for deleted edges** — currently we leave holes; reuse is manual.
2. **`Arc<PropertyMap>` for edge property snapshots** to make version commits O(1) rather than O(map size).
3. **Promote `EdgeView` as the public read type** and demote `Edge` to a debugging convenience.
4. **Pool the per-edge property map allocation** for the 0 → 1 property growth path.

## References

- Code: `samyama-graph/src/graph/store.rs:497–1540` (edge layout), `src/graph/edge.rs`, `src/snapshot/format.rs`
- Wiki: [[storage-edge-endpoints-layout.md]], [[edge-store-removal.md]]
- Related ADRs: ADR-022 (Snapshot Format — drove the v2 transition), ADR-021 (Columnar Property Store — symmetric node-side change), ADR-020 (MVCC — versioning sits atop this layout).
