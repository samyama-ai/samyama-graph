# Where a row came from, and whether it may leave

Two different things are called provenance and only one of them is on this page.

| | Question | Where it lives |
|---|---|---|
| **Execution** provenance | which build, which snapshot answered this query | `engine_version` and `snapshot_version` on every query response ([#1035](https://github.com/samyama-ai/samyama-graph/issues/1035)) |
| **Data** provenance | which source this fact came from, under what licence | this page |

Execution provenance is a fact about a run. Data provenance outlives every run
and follows the row into an export, which is why it lives in the data.

---

## Five reserved property keys

```cypher
CREATE (f:Fact {
  name: 'median_income_2026',
  __source_uri:     'https://census.example/2026',
  __source_version: 'release-2026-03',
  __retrieved_at:   '2026-04-01T00:00:00Z',
  __license:        'CC0-1.0',
  __redistributable: true
})
```

| Key | Meaning |
|---|---|
| `__source_uri` | where the fact came from |
| `__source_version` | the version, revision or release of that source |
| `__retrieved_at` | when it was fetched |
| `__license` | the licence the source grants |
| `__redistributable` | whether this row may leave, as a boolean |

They are properties, not a parallel store, for one reason: **properties survive
every path a row already takes.** A snapshot, an RDF export, a CSV, a GraphML
file and a plain `RETURN n` all carry properties. A sidecar table would have to
be threaded through each of those and would be forgotten by the sixth.

`__source_uri` without `__source_version` is **not reproducible**. "From
Wikidata" does not let anyone fetch what the row was built from; "from Wikidata,
dump 2026-08-01" does. The report counts these separately.

---

## Absence is not permission

`__redistributable` has three states, not two:

| Value | Meaning |
|---|---|
| `true` | may leave |
| `false` | may not |
| *absent* | **unknown** — nobody has said |

Unknown is a real answer. Every row in an existing graph is unknown, so reading
that silence as `true` would mark a whole database redistributable because
nobody said otherwise, and reading it as `false` would empty every export on the
day this shipped.

So the caller chooses:

```bash
cargo run --release --example provenance_report -- --policy require-permission
```

| Policy | Emits | Is it a guarantee? |
|---|---|---|
| `count-only` (default) | everything, counting what is unmarked or forbidden | no — it changes nothing, by design |
| `withhold-forbidden` | drops rows marked `false`; **unmarked rows still leave** | no |
| `require-permission` | only rows marked `true` | **yes** |

Only the third is a guarantee, and the difference is the unmarked row — which is
most rows in most graphs. The report always prints how many, so the gap between
the policy you chose and the guarantee you wanted is a number rather than an
assumption.

A misspelt policy is an error, not the default. `--policy require-permision`
exits non-zero rather than quietly emitting everything, because that is how a
licence guarantee is lost to a typo.

---

## The derivation path

A derived fact names what it rests on with `DERIVED_FROM`:

```cypher
CREATE (d:Derived {name: 'median_income'})
CREATE (d)-[:DERIVED_FROM]->(census)
CREATE (d)-[:DERIVED_FROM]->(survey)
```

`provenance::derivation_path` walks those edges breadth-first and returns every
step with its own provenance and redistribution marking:

```
  depth 0 node 1    Derived    (no source)                  [no licence]
  depth 1 node 2    Source     https://census.example/2026  [CC0-1.0]
  depth 1 node 3    Source     https://panel.example        [proprietary]
  depth 2 node 4    Source     (no source)                  [no licence]
```

Three things it deliberately does:

- **Only `DERIVED_FROM` edges.** A walk over every edge would return the whole
  connected component and call it a derivation — an answer that is always
  available and never true.
- **Returns steps that recorded nothing.** The derived fact above has no
  provenance of its own. An empty entry says "this step recorded nothing", which
  *is* the answer; dropping it would silently shorten the chain.
- **Terminates on a cycle.** A derivation graph is a claim made by whoever built
  it, and nothing stops them claiming a cycle. A governance query that hangs is
  the worst way for one to fail.

---

## The other provenance: what a model wrote (`_generated`)

The keys above say where data came *from*. They say nothing about whether a
human or a language model put it there, and until #1413 nothing did: a value
promoted out of enrichment quarantine was written onto the real property with no
marker at all, and an edge materialized from a model's list of targets carried
nothing whatsoever.

`_generated` is the reserved key for that question. It is a map, so **one**
predicate answers it for every shape:

```cypher
// no model wrote this node's data
MATCH (n) WHERE n._generated IS NULL RETURN n
// no model drew this edge
MATCH (a)-[r]->(b) WHERE r._generated IS NULL RETURN r
```

| field | meaning |
|---|---|
| `created` | the node or edge itself was produced by a model |
| `properties` | the properties whose *current* value was produced by a model |

Three rules decide where it goes.

- **Written at promotion, not at quarantine.** A quarantined answer is not in
  the graph. Marking it would mark a value the engine has not believed.
- **On the artifact the model made, and no further.** A node the model drew an
  edge *from* is not marked: its own data is ingested. Tainting it would make
  the exclusion predicate hide real rows and make a retraction look like a
  delete.
- **Off again on retraction.** `agent::enrich::retract` removes promoted
  properties, deletes materialized edges, and deletes target nodes the model
  created once nothing else points at them — a node the model merely *named*
  stays. The quarantine entry survives with its status back at
  `pending_verification`: retraction withdraws the belief, not the evidence.

`retract` is a compensating pass, not a transaction. The engine has no
statement-level rollback (LANG-07), so a failure part-way through leaves a
partly-retracted graph.

Not done: no HTTP endpoint exposes `retract` (`/api/enrich` and `/api/verify`
exist), the confidence written into quarantine is the constant
`LLM_DEFAULT_CONFIDENCE = 0.4` for every answer rather than anything the model
reports, and `examples/agentic_enrichment_demo.rs` bypasses this path entirely
(#1413).

---

## What this does not do yet

- **Nothing enforces the keys.** `__license: 42` is accepted, as any property
  is. The reader coerces a string `"false"` to `false` because CSV, JSON and RDF
  imports all deliver booleans as text, but there is no schema and no validation.
- **Relationships carry no source provenance.** This model is node-level. An
  edge asserting a relationship between two differently-licensed sources has no
  licence of its own. The one exception is an edge a *model* drew: see below.
- **No export applies a policy by default.** `screen()` is available to every
  export path and `count-only` is the default everywhere, so nothing changes
  until a caller asks. Wiring `require-permission` into the snapshot and RDF
  exports is the obvious next step and is not done.
