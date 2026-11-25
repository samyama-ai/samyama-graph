# Change data capture

A durable, per-tenant feed of committed changes, read over HTTP (#2025). A
consumer (a search index, an embedding pipeline, a fraud rule, a cache) keeps
the offset it has reached and asks for what came after, instead of
re-querying or re-indexing the graph.

## Turning it on

The feed is off by default. Start the server with a retention, the number of
events kept per tenant:

```sh
samyama --data-dir ./data --cdc-retention 1000000
# or SAMYAMA_CDC_RETENTION=1000000
```

It needs a data directory. It is off by default because it costs something on
every write: each touched row is decoded to compare it with its new state, and
each changed entity writes its events beside its row.

## Reading it

```sh
curl 'http://localhost:8080/api/cdc/events?after=0&limit=100'
```

```json
{
  "graph": "default",
  "events": [
    {"offset": 1, "tx": 1, "tenant": "default", "ts": 1764060000000,
     "op": "node_created", "id": 0, "labels": ["Person"], "properties": {"name": "ada", "age": 36}, "commit": true},
    {"offset": 2, "tx": 2, "tenant": "default", "ts": 1764060000500,
     "op": "property_set", "entity": "node", "id": 0, "key": "age", "old": 36, "value": 37, "commit": true}
  ],
  "next": 2, "oldest": 1, "latest": 2, "more": false
}
```

| Parameter | Default | |
|---|---|---|
| `after` | 0 | Events with offsets above this. 0 is the beginning. |
| `limit` | 100 | At most 1000. |
| `graph` | `default` | The tenant. A credential bound to a tenant reads only that one. |
| `wait` | 0 | Seconds (at most 30) to hold the request when there is nothing yet; it answers as soon as a write commits. |

A consumer loop: read with `after=<next>`, apply the events, store `next`
durably on its side, repeat. With `wait=30` the loop does not spin.

### Events

| `op` | Fields |
|---|---|
| `node_created` | `id`, `labels`, `properties` |
| `node_deleted` | `id`, and the `labels` and `properties` it had |
| `edge_created` / `edge_deleted` | `id`, `type`, `source`, `target`, `properties` |
| `label_added` / `label_removed` | `id`, `label` |
| `property_set` | `entity` (`node`/`edge`), `id`, `key`, `value`, `old` (null when new) |
| `property_removed` | `entity`, `id`, `key`, `old` |
| `graph_dropped` | none: the tenant's graph was deleted; discard what was built from earlier events |

Every event also has `offset`, `tx`, `tenant` and `ts` (commit time, ms since
the epoch). Within one write, nodes are created before the edges that use
them, and edges are deleted before their nodes.

## Guarantees

- **Committed writes only.** Events are produced where a write is persisted:
  auto-commit statements over RESP and HTTP, `GRAPH.COMMIT`,
  `/api/tx/:id/commit`, imports, in-process writers and the TTL sweeper. A
  rolled-back transaction, or a statement that fails, emits nothing.
- **One write, one `tx`.** Every event of a committed write shares its `tx`
  number, and the last one has `"commit": true`.
- **Net change per write.** A write that sets a property five times is one
  `property_set` from the value before the write to the value after it. A
  node created and deleted in one write emits nothing.
- **Durable, and in step with the data.** Events go into the same RocksDB
  write batch as the rows they describe, so after a crash there is no
  committed row without its event and no event for a row that did not land.
  Offsets and `tx` numbers survive restarts and never go back, including
  across `GRAPH.DELETE`.
- **Ordered per tenant.** Offsets count from 1, per tenant, with no gaps
  except those left by retention.
- **Bounded.** The newest `--cdc-retention` events per tenant are kept. A
  read whose `after` is older than the oldest event held gets **410 Gone**
  with `oldest` and `latest`: the consumer has missed changes and should
  rebuild from the graph, then resume from `latest`. It is never handed a
  page with a gap in front of it.

## How it relates to `GET /api/changes`

`GET /api/changes` (#1771) answers *whether* anything changed since a token,
from memory, for a view deciding whether to re-run its query. The change feed
answers *what* changed, and is durable. A view can keep using the token; a
pipeline that applies changes incrementally reads the feed.

## Why not read the logical WAL

The logical WAL (`src/persistence/wal.rs`) is not the recovery source: it may
be deleted without losing data, checkpoints truncate it, and its records hold
each entity's whole property map with no prior state, no time and no commit
boundary. A feed derived from it could not say what a write changed, nor be
trusted to have kept it. The feed is therefore derived at the point of
persistence, from the stored row and the new state, and stored beside the
rows.

## Not yet

- Sinks that push the feed elsewhere (Kafka, webhooks). A consumer reads it
  over HTTP today; sinks are a follow-up.
- A RESP command and a server-sent-events stream; the long poll covers the
  same need for now.
- Index definitions (`CREATE INDEX`) and vector-index changes are not events.
- Retention is one count for all tenants; per-tenant retention and
  retention by age are follow-ups.
