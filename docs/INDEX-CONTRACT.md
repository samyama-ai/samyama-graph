# The index contract

What every index structure in this engine has to do, stated once, so that a new
structure is added by implementing six clauses rather than by editing the
planner. Four structures implement it today: the **B-tree property index**
(and the unique constraint built on it), the **full-text index**, the **vector
index** (HNSW) and the **hierarchy index** (OEH, ADR-035). This page is the
contract; the ADRs linked from each clause hold the reasoning.

NDS-01 names the six clauses: build, maintain under MVCC, cost, plan, persist,
snapshot. CH-NDS-FW measures the first five per structure against a live
server, and this document is the sixth.

## The clauses

| clause | the structure must | where it is enforced |
|---|---|---|
| **build** | be created by a DDL statement that returns before the structure is usable, from the rows already in the store, and appear in `SHOW INDEXES` under its label, property and kind | `CREATE INDEX`, `CREATE CONSTRAINT ... IS UNIQUE`, `CREATE FULLTEXT INDEX`, `CREATE VECTOR INDEX`, `CREATE HIERARCHY INDEX`; `GraphStore::index_catalog()` is what `SHOW INDEXES` prints |
| **maintain under MVCC** | reflect every committed write, be untouched by a statement that fails, and never answer from a row the reader cannot see | the write paths in `GraphStore` call the registry on every property set, removal, label change and delete; a failed statement is undone through the same setters (#1601, #1603), so the indexes are undone with it |
| **cost** | tell the planner what a lookup through it costs, so it is chosen when it is cheaper and not otherwise | `cost_model.rs` for the B-tree lookup; selectivity for the planner's anchor choice (`choose_anchor_index`); hierarchy regime detection in ADR-035 §7 |
| **plan** | be selected by the planner from the query's shape, never by a user hint, and be visible in `EXPLAIN` by name | `IndexScan` (`find_index_predicate`, `inline_index_scan`), `FullTextSearch`, `VectorSearch`, `HierarchyDescendantScan`; an unindexed predicate plans none of them |
| **persist** | come back after a restart on the same data directory with the rows, listed and used, with no step the operator has to remember | the index catalog (`crate::index::catalog`), one record per tenant in the `indices` column family, written by `apply_mutations` when the dirty flag is set and restored after the rows by `PersistenceManager::recover_into` (#1477) |
| **snapshot** | cross an `.sgsnap` export and import as a declaration the importing store rebuilds from the imported rows, so a store with no index does not get one invented | the `"t":"i"` catalog line (#1506) and the hierarchy declaration lines (ADR-035 §6); import declares exactly what the file says and reports it under `indexes_restored` |

## What each structure declares

A declaration is what the structure needs to be rebuilt and nothing it contains
(`IndexDefinition`):

| structure | declaration | queried through |
|---|---|---|
| B-tree property index | label, property | a `WHERE n.prop = v` or inline `{prop: v}` the planner turns into `IndexScan` |
| unique constraint | label, property | the same lookup; consulted on every write (`eb02825`, #542) |
| full-text | **name**, label, property | `db.index.fulltext.queryNodes(name, text)` |
| vector | name, label, property, dimensions, metric, quantization | `db.index.vector.queryNodes(label, property, vector, k)` or `(name, k, vector)`; `/api/vector-search` |
| hierarchy | the edge type, direction, and the hierarchy's regime parameters | the pattern shapes ADR-035 §8 detects |

Dimensions and metric are in the vector declaration because an index rebuilt at
the wrong dimension silently skips every vector; the name is in the full-text
and vector declarations because the procedures address the index by name and by
nothing else.

## A missing structure is an error, not an empty answer

Each structure's query surface fails loudly when the structure it names does
not exist:

- B-tree: there is no name to miss; the predicate plans as `Filter` over
  `NodeScan` and `EXPLAIN` shows it.
- full-text: `no full-text index named 'x'`.
- vector: `no vector index on :Label(property)`, listing the ones that exist.
  Until #1660 this returned `[]` with status 200, so an index that was never
  created, or had not come back after a restart, read exactly like one in which
  nothing was similar.

This is part of the contract because the persist and snapshot clauses are only
checkable if their failure is visible: a structure that answers `[]` when absent
passes every "does it come back" probe that looks at status codes.

## Adding a structure

1. Give it a registry on `GraphStore` and a `IndexDefinition` variant carrying
   its declaration. Teach `index_catalog()` to list it and
   `restore_index_catalog` to rebuild it from the rows.
2. Call the registry from the write paths that can change what it indexes, and
   make sure the undo of a failed statement reaches it through the same calls.
3. Give the cost model a number for a lookup through it, and the planner a
   detector that selects it from query shape. Name its operator in `describe()`
   so `EXPLAIN` shows it.
4. Make its query surface error when the structure is absent.
5. Add it to CH-NDS-FW's `STRUCTURES` so the five behavioural clauses are
   measured for it, and to the tables above.

Steps 1 and 2 are the whole of persist and snapshot: the catalog, the
`.sgsnap` line and the restore order are shared, so a structure that declares
itself correctly survives a restart and a snapshot without further work.

## Related

- [ADR-029](./ADR/ADR-029-index-manager.md) — the index manager and the catalog.
- [ADR-022](./ADR/ADR-022-snapshot-format.md) — the `.sgsnap` format.
- [ADR-035](./ADR/ADR-035-oeh-hierarchy-index.md) — the hierarchy index.
- [FULL-TEXT-SEARCH.md](./FULL-TEXT-SEARCH.md) — the full-text index.
- [ACID_GUARANTEES.md](./ACID_GUARANTEES.md) — the transaction model the maintain clause rests on.
