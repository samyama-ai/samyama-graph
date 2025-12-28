# ADR-011: Implement Cypher CRUD Operations (DELETE, SET, REMOVE)

## Status
**Proposed**

## Date
2025-12-27

## Context

Samyama's OpenCypher query engine (Phase 2) currently supports read operations (`MATCH`, `WHERE`, `RETURN`) and node/edge creation (`CREATE`), but lacks essential mutation operations for a complete graph database:

| Operation | Neo4j | Samyama (Current) | Gap |
|-----------|-------|-------------------|-----|
| `DELETE` | Yes | No | Critical |
| `DETACH DELETE` | Yes | No | Critical |
| `SET` properties | Yes | No | Critical |
| `REMOVE` properties | Yes | No | Important |
| `MERGE` | Yes | No | Future |

Without these operations, users cannot:
1. Delete nodes or edges via Cypher queries
2. Update properties on existing nodes/edges
3. Remove obsolete properties
4. Use the RESP protocol for full CRUD operations

This significantly limits Samyama's utility as a production graph database.

## Decision

**We will implement essential CRUD operations in the following order:**

### Phase 1: DELETE Operations

```cypher
-- Strict delete (fails if node has edges)
DELETE n

-- Cascade delete (removes connected edges automatically)
DETACH DELETE n
```

**Rationale**: DELETE is the simplest mutation (removes data) and unblocks the most use cases.

### Phase 2: SET Operations

```cypher
-- Individual property
SET n.prop = value

-- Replace all properties
SET n = {props}

-- Merge properties
SET n += {props}
```

**Rationale**: SET enables property updates, the most common mutation after reads.

### Phase 3: REMOVE Operations

```cypher
-- Remove properties
REMOVE n.prop1, n.prop2
```

**Rationale**: REMOVE completes the property lifecycle (create, update, delete).

### Architecture

We extend the existing Volcano iterator model (ADR-007):

```
┌─────────────────────────────────────────────────────────────────┐
│  Query Pipeline (Extended)                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  NodeScan → Filter → SET → REMOVE → DELETE → Project → Limit   │
│     │         │        │      │        │         │        │     │
│     ▼         ▼        ▼      ▼        ▼         ▼        ▼     │
│   next()   next()   next_mut() ...  next_mut()  next()  next()  │
│                         │                │                      │
│                         └───────┬────────┘                      │
│                                 ▼                               │
│                        MutQueryExecutor                         │
│                        (&mut GraphStore)                        │
│                                 │                               │
│                                 ▼                               │
│                        PersistenceManager                       │
│                        (WAL + RocksDB)                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Operator Trait Extension**: Add `next_mut()` and `next_mut_with_ctx()` to the `Operator` trait for write operations.

2. **Execution Order**: Follow Neo4j's clause ordering:
   ```
   MATCH → WHERE → SET → REMOVE → DELETE → CREATE → RETURN
   ```

3. **DELETE Semantics**:
   - `DELETE n` fails if node has connected edges (Neo4j default)
   - `DETACH DELETE n` automatically removes connected edges

4. **SET Semantics**:
   - `SET n.prop = value`: Update single property
   - `SET n = {map}`: Replace ALL properties (destructive)
   - `SET n += {map}`: Merge into existing (non-destructive)

5. **Persistence**: All mutations logged to WAL before applying.

6. **Error Handling**: Clear, actionable error messages with suggestions.

## Consequences

### Positive

✅ **Complete CRUD**: Samyama becomes a fully functional graph database
✅ **Neo4j Compatibility**: Familiar Cypher semantics for Neo4j users
✅ **RESP Support**: Full mutations via `GRAPH.QUERY` command
✅ **Durability**: All mutations persisted via WAL
✅ **Composable**: Operators chain naturally with existing ones
✅ **Testable**: Each operator is independently testable

### Negative

⚠️ **Complexity**: ~1,300 new LOC across grammar, parser, operators
⚠️ **Performance**: Mutation operators require mutable store access
⚠️ **Testing**: ~125 new tests needed for complete coverage

### Neutral

➡️ **No Transaction Isolation**: Single-statement atomicity only (future work)
➡️ **No Query Optimizer Changes**: Mutations use existing execution strategy

## Alternatives Considered

### Alternative 1: Rust API Only

Continue requiring users to use the Rust API for mutations.

**Rejected because**:
- Poor developer experience
- Cannot use RESP protocol for mutations
- Inconsistent with read operations via Cypher

### Alternative 2: Implement MERGE First

Start with `MERGE` (upsert) instead of separate CREATE/DELETE/SET.

**Rejected because**:
- MERGE is more complex (requires matching + conditional logic)
- DELETE and SET are more commonly used
- MERGE can be implemented later on top of these primitives

### Alternative 3: Minimal SET Only

Implement only `SET n.prop = value` without replace/merge variants.

**Rejected because**:
- Replace (`SET n = {}`) is needed for bulk property updates
- Merge (`SET n += {}`) is idiomatic for partial updates
- Grammar extension is minimal for full support

### Alternative 4: Push-Based Execution

Use push-based execution instead of pull-based (Volcano) for mutations.

**Rejected because**:
- Breaks consistency with existing operator model
- Would require significant refactoring
- Volcano model handles mutations adequately

## Implementation Plan

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| 1. DELETE | 3-4 days | Grammar, AST, Parser, Operator, Tests |
| 2. REMOVE | 1-2 days | Grammar, AST, Parser, Operator, Tests |
| 3. SET | 3-4 days | Grammar, AST, Parser, Operator (3 variants), Tests |
| 4. Persistence | 2-3 days | WAL entries, PersistenceManager methods, Recovery |
| 5. Polish | 1-2 days | RESP integration, Error messages, Documentation |

**Total**: ~12-15 days

## Related Decisions

- [ADR-007: Volcano Iterator Model](./ADR-007-volcano-iterator-execution.md) - Execution model extended for mutations
- [ADR-002: RocksDB Persistence](./ADR-002-use-rocksdb-for-persistence.md) - Persistence layer for mutation durability
- [ADR-003: RESP Protocol](./ADR-003-use-resp-protocol.md) - Protocol for mutation commands

## References

- [Full Design Document](../plans/2025-12-27-cypher-crud-operations-design.md)
- [OpenCypher DELETE Specification](https://opencypher.org/resources/)
- [Neo4j Cypher Manual: DELETE](https://neo4j.com/docs/cypher-manual/current/clauses/delete/)
- [Neo4j Cypher Manual: SET](https://neo4j.com/docs/cypher-manual/current/clauses/set/)
- [Neo4j Cypher Manual: REMOVE](https://neo4j.com/docs/cypher-manual/current/clauses/remove/)

---

**Last Updated**: 2025-12-27
**Status**: Proposed
