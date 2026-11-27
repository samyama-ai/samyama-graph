# Agent change lifecycle

The agent change endpoints use the append-only HTTP audit log. Start the
server with `--audit-log <path>`; agent-attributed writes and schema proposals
are refused when the log is not enabled. Configure an auth file as well so
schema application has an authenticated Admin approver.

## Data writes

Send ordinary Cypher writes to `POST /api/query` with an `agent_id`:

```json
{
  "query": "MATCH (n:Person {name: 'Ada'}) SET n.role = 'reviewer'",
  "agent_id": "research-agent-7"
}
```

The response includes `x-samyama-change-id`. Fetch the recorded statement,
affected IDs, and before/after entity images with:

```text
GET /api/agent/changes/<change-id>
```

Revert only while the touched entities still match the recorded after image:

```text
POST /api/agent/changes/<change-id>/revert
```

The engine refuses a stale revert with `409`. Recreating deleted nodes or
relationships can allocate new IDs; the revert response returns the old-to-new
ID maps. Agent-attributed writes currently target the default graph and cannot
use an open transaction or streamed response.

The enrichment lifecycle endpoints accept the same optional `agent_id` field
alongside `query`. For example:

```json
{
  "query": "MATCH (n:Person) RETURN n",
  "agent_id": "research-agent-7"
}
```

Use that payload with `/api/enrich`, `/api/verify`, or `/api/retract`. When the
operation changes agent-generated data, the response includes the same
`x-samyama-change-id` header and the before/after record is available from the
change lookup and guarded revert endpoints. Supplying `agent_id` without
`--audit-log` is rejected.

## Schema proposals

An agent can propose one catalog-backed schema operation: `CREATE INDEX`,
`DROP INDEX`, `CREATE CONSTRAINT`, `CREATE VECTOR INDEX`, full-text index
create/drop, point index create/drop, graph projection create/drop, hierarchy
index create/drop, structure index create/drop, or `CALL db.retention.set/drop`:

```text
POST /api/agent/schema/proposals
```

```json
{
  "agent_id": "research-agent-7",
  "statement": "CREATE INDEX ON :Person(email)",
  "rationale": "Speed up lookups by email"
}
```

The response contains the catalog diff and a proposal ID. A separately
authenticated Admin applies it:

```text
POST /api/agent/schema/proposals/<proposal-id>/apply
```

The applied catalog snapshots are written to the audit log and can be reverted
through the same change lookup and revert endpoints. Revert checks that the
catalog still matches the applied after image. Rebuild and refresh statements
are maintenance operations, not reversible schema proposals. The query DDL
surface also has no `DROP VECTOR INDEX` statement; an applied vector-index
creation can still be reverted by change ID.

Named subgraphs use their existing HTTP API instead of the schema proposal
route. Add `agent_id` to the create JSON body or pass it as a query parameter
when deleting (`DELETE /api/subgraphs/<name>?agent_id=research-agent-7`). These
mutations also return a change ID and can be reverted through the guarded
change endpoint.
