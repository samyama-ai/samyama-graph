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
- Index **contents** are not persisted to RocksDB; they are rebuilt from the rows on recovery. Same storage trade-off as the columnar property store, plus one property a persisted posting list cannot have: a rebuilt index cannot disagree with the rows it describes.
- Index **definitions** are persisted, as one record per tenant in the `indices` column family — see "Index catalog" below. Until 2026-09-27 they were not, and the sentence that used to stand here ("they rebuild on load from `GraphStore`") described something no code did: nothing wrote a definition and nothing read one, so a restart returned every row and no index (#1477).

## Index catalog (2026-09-27, #1477)

The three index registries — `IndexManager` (property + unique constraint),
`FullTextIndexes`, `VectorIndexManager` — are in-memory. What survives a restart
is a `crate::index::catalog::IndexCatalog`: one bincode record per tenant under
`catalog:<tenant>` in the `indices` column family of the same RocksDB database
the rows live in.

- **Declarations only.** `IndexDefinition` carries what each registry needs to
  be rebuilt and nothing it contains. For a vector index that is the name, label,
  property, dimensions, metric and quantization; dimensions and metric because an
  index rebuilt at the wrong dimension silently skips every vector, quantization
  because NDS-09 makes it a stated choice.
- **A snapshot, not a log.** The whole catalog is rewritten whenever it changes,
  so a `DROP` takes effect by absence. A log of creates and drops would have to
  be replayed to stay correct, and an index that comes back after being dropped
  is worse than one that never persisted.
- **Written by `apply_mutations`**, the same call that persists the rows, gated
  on a dirty flag every DDL path sets. `CREATE INDEX` produces no `Mutation`, so
  a persist path that only ran when there were row mutations would never fire for
  the statement that needs it.
- **Restored after the rows, never before.** `PersistenceManager::recover_into`
  loads nodes and edges, then re-declares each definition and builds it from what
  is now in the store. `insert_recovered_node` maintains none of the three
  registries, so declaring first would leave every index empty — and an empty
  vector index answers every search with no rows and no error, which is worse
  than the missing index it replaces.

The `.sgsnap` path carries the same catalog since #1506: export writes
`GraphStore::index_catalog()` as a `"t":"i"` line and import hands it to
`restore_index_catalog` after the rows are in, so property, unique-constraint,
full-text and vector definitions cross a snapshot boundary with their names,
dimensions, metric and quantization. Discovery (`rebuild_vector_index_full`)
now runs only for a snapshot that carries no catalog line. See ADR-022.

## Lookup by external key (2026-09-30, #542)

A node imported from another system usually arrives with its own key — a
primary key, a UUID, a business id — and an incremental load has to find the
node that key was loaded into last time. `NodeId`s are assigned by the store and
reused after a delete, so they cannot be that key. The key goes in a property
under a unique constraint, and the constraint's index answers the lookup:

```rust
store.create_unique_constraint(&Label::new("Acct"), "krid")?;
// ... load ...
let id: Option<NodeId> = store.find_node_by_unique(&Label::new("Acct"), "krid", &PropertyValue::Integer(42))?;
```

- **From the index, never a scan.** Without a unique constraint on the pair it
  returns `GraphError::NoUniqueConstraint`. A plain index is not enough: it does
  not promise one node per key. For a non-unique property, use Cypher
  (`MATCH (n:Acct {region: $r})`), which uses a property index when there is one.
- **Exact equality**, as the B-Tree keys it: `Integer(1)` does not find
  `Float(1.0)`.
- **Checked against the node.** A candidate that is gone, has lost the label or
  no longer holds the value is not returned, so an entry a write path failed to
  drop answers `None` rather than the wrong node.
- **The index now follows every write.** A key changed with `SET`, removed with
  `REMOVE`, or on a deleted node used to stay registered to the node that gave it
  up, so reloading a deleted row was refused as a duplicate of nothing;
  `create_node_with_properties` never registered its keys at all. All four are
  maintained, and a rolled-back delete puts the node's entries back.
- In the Rust SDK: `EmbeddedClient::find_node_by_unique` and
  `RemoteClient::find_node_by_unique`. The remote one sends
  `MATCH (n:Label {prop: $key}) RETURN id(n)` with the key bound, and does not
  check for the constraint first.

User-chosen `NodeId`s — the issue's other option — are a storage change and are
not part of this.

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
