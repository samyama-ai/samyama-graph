# ADR-029: Index Manager — Property, Unique, Composite Indexes

## Status
**Shipped** (v1.0.0, 2026-04-11) — retroactively documenting the shipped IndexManager.

## Date
2026-05-05

## Context

Cypher exposes three closely related index concepts:

1. **Property index** — `CREATE INDEX FOR (n:Label) ON (n.prop)`. Speeds up equality and range predicates on `(label, prop)`.
2. **Unique constraint** — `CREATE CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE`. Same shape as a property index but enforces uniqueness on insert / update.
3. **Composite index** — `CREATE INDEX FOR (n:Label) ON (n.p1, n.p2)`. Speeds up multi-predicate equality on the compound key.

The implementation must:
- Persist index definitions across restart (the *definition*; the index data rebuilds on load).
- Be consultable by the query planner at sub-millisecond cost on every plan.
- Coexist with MVCC version chains without versioning the index itself (current scope).
- Be extensible to a future class of indexes (e.g., text, geo) without rewriting the dispatch.

The pre-IndexManager code held property indexes directly on `GraphStore`, conflated unique constraints with regular indexes, and had no clear extension path. v0.6 lifted the responsibility into a dedicated `IndexManager` to clean this up.

## Decision

We will provide a single `IndexManager` (`src/index/manager.rs`) responsible for the three index kinds above, with a `PropertyIndex` (B-Tree) as the underlying data structure for all three.

```rust
pub struct PropertyIndexKey { label: Label, property: String }

pub struct IndexManager {
    indices: RwLock<HashMap<PropertyIndexKey, Arc<RwLock<PropertyIndex>>>>,
    unique_constraints: RwLock<HashMap<PropertyIndexKey, Arc<RwLock<PropertyIndex>>>>,
    // composite indexes use a concatenated PropertyValue tuple as the BTree key
}
```

Inner `PropertyIndex`:

```rust
pub struct PropertyIndex {
    index: BTreeMap<PropertyValue, HashSet<NodeId>>,
}
```

The B-Tree gives ordered iteration, range queries, and O(log n) point lookup at one consistent cost.

The Cypher planner consults `IndexManager::has_index(label, property)` on every plan; if present, an `IndexScanOperator` is emitted instead of `LabelScan + Filter`. **Both inline-property AST forms (`MATCH (n:Person {email: 'x'})`) and WHERE-clause forms (`MATCH (n:Person) WHERE n.email = 'x'`) lower to the same plan** — fixed in v0.6.1.

Unique-constraint check is **two-phase**: `check_unique_constraint` returns `Err` if a violating value is present, and the caller is responsible for invoking it before the write. Atomicity is provided by the upper transaction layer (§5).

## Consequences

### Positive
- Clean separation of `GraphStore` (graph data) from `IndexManager` (search structures).
- Three index kinds share one underlying data structure.
- Consistent dispatch: planner asks `has_index`, gets `Arc<RwLock<PropertyIndex>>`, uses `get` or `range`.
- Inline + WHERE forms unified at the planner level.

### Negative
- **B-Tree key is `PropertyValue` clone**: doubles string allocations (index + column store). Mitigation: future `Arc<PropertyValue>` keys.
- **Unique-constraint check is not atomic at the index layer**: relies on the transaction layer to serialise. Race window exists if used outside a transaction.
- **No MVCC versioning of indexes**: readers at an earlier snapshot see the *latest* index state. Documented as a known RC-rather-than-SI behaviour; aligned with §1.6 / ADR-020.
- **Composite index Cypher coverage is partial**: not every multi-predicate AST shape is lowered to a composite scan.
- **Range-predicate plan coverage is partial**: `BETWEEN` and chained `>`/`<` AST shapes don't always route to `PropertyIndex::range`.

### Neutral
- Indexes are not persisted columnar to RocksDB; they rebuild on load from `GraphStore`. Same storage trade-off as the columnar property store.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| Hash index per property | No range queries, no ordered iteration. |
| RocksDB-backed (LSM) indexes | RAM-resident wins on the read hot path; persistence isn't the bottleneck. |
| Specialised structures per index kind | Three index kinds, same B-Tree shape — splitting is premature. |

## Follow-ups

1. `Arc<PropertyValue>` keys to deduplicate index + column-store string allocations.
2. Atomic unique-constraint check at the index layer (`upsert_if_absent`).
3. MVCC-aware indexes — see ADR-020 follow-ups.
4. Complete composite-index Cypher coverage.
5. Complete range-predicate planner lowering (BETWEEN, chained inequalities).
6. Index-only scan when projection is index-covered.
7. Parallel index rebuild on `CREATE INDEX` against large tenants.

## References

- Code: `samyama-graph/src/index/manager.rs`, `src/index/property_index.rs`, `src/query/executor/operator.rs:4150` (IndexScanOperator), `src/query/executor/planner.rs:3237` (inline + WHERE unification)
- Wiki: [[index-property.md]], [[index-label.md]], [[index-edge-type-and-degree.md]], [[feedback_index_scan_where.md]]
- Related ADRs: ADR-015 (Graph-Native Query Planning), ADR-020 (MVCC — index versioning gap), ADR-021 (Columnar Property Store — symmetric for property storage).
