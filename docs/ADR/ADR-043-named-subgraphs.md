# ADR-043: Named Subgraphs — Persisted Scoped Views Over One Tenant Graph

## Status
**Accepted** (first phase shipped, #337)

## Date
2026-11-25

## Context

A tenant graph often holds several domains at once — clinical trials, drugs,
patients, sites. Users want to query and describe one slice of it without
copying data into a second graph and without writing the same label and type
filters into every query. The OSS build serves a single graph (`default`), and
`graph_name` / `graph` already exist on every query surface, so a slice can be
addressed the way a graph is.

## Decision

A **named subgraph** is a persisted, read-only view of the tenant graph, defined
by node labels, relationship types and optional per-label property predicates.

### Membership

- A node is in the view if any of its labels is in scope and that label admits
  it: the label has no predicate, or the predicate evaluates to `true`.
- A relationship is in the view if its type is in scope **and** both endpoints
  are in the view (the induced-subgraph rule). A relationship type in scope can
  therefore still be absent when one end is outside.
- Member nodes keep all their labels and properties; a label outside the scope
  is not hidden on a node that is in the view through another label. A scoped
  query answers what the equivalent hand-filtered query answers on the whole
  graph, nothing more.

### Enforcement is in the operators, not in the query text

The query is never rewritten. `QueryPlanner::with_subgraph(scope)` plans the
query as usual with whole-graph shortcuts turned off (label/edge-type/edge
count answers, hierarchy and adjacency-aggregate detectors, the graph-native
enumerator), then `restrict_plan_to_subgraph` walks the plan and hands the
scope to every operator:

- leaf operators (node scan, index scan, node-by-id) produce only members;
- expansion operators (single hop, variable length, expand-into) follow only
  relationships in the view, so traversal cannot leave it;
- row operators (filter, project, aggregate, sort, …) are passed through.

The walk is **fail-closed**: an operator that does not implement
`restrict_to_subgraph`, or whose children cannot all be reached, fails the plan
with "… is not supported inside a named subgraph yet". Expressions that read the
store directly — pattern predicates, `EXISTS {}`, pattern comprehensions and the
hierarchy functions — are refused before planning for the same reason. A query
is refused rather than run against the whole graph.

Writes addressed to a subgraph are refused.

### Persistence

Definitions are a new, last variant of `IndexDefinition` in the index catalog,
so they are written to the `indices` column family, restored by
`PersistenceManager::recover_into`, and carried in the `.sgsnap` catalog with
no new storage path. RESP writes the catalog before replying; HTTP goes through
`AppState::mutate`, which persists a dirty catalog.

### Surfaces

- RESP: `GRAPH.SUBGRAPH.CREATE | LIST | INFO | DROP`; `GRAPH.QUERY` and
  `GRAPH.RO_QUERY` accept a subgraph name as the graph name.
- HTTP: `POST/GET /api/subgraphs`, `GET/DELETE /api/subgraphs/{name}`,
  `GET /api/schema?subgraph=`, and `graph` in `POST /api/query`.
- `GraphStore::schema_summary_for(scope)` is the scoped `schema_summary()`.

## Consequences

- No data is copied; a predicate follows the data as it changes.
- Membership is evaluated per node touched, so a scoped scan costs a predicate
  evaluation per candidate. Scoped counts are exact rather than O(1).
- Operators not yet reviewed (shortest path, CALL procedures, subqueries, …)
  refuse scoped queries until they are taught the scope.

## Not in this phase

Scoped NLQ over RESP/HTTP, scoped algorithms (`CALL algo.*`), scoped snapshot
export, SDK methods, subgraphs of tenants other than `default`, reads inside
transactions over HTTP, streamed scoped results, and a benchmark of scoped
versus hand-filtered queries.
