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

## What this does not do yet

- **Nothing enforces the keys.** `__license: 42` is accepted, as any property
  is. The reader coerces a string `"false"` to `false` because CSV, JSON and RDF
  imports all deliver booleans as text, but there is no schema and no validation.
- **Relationships carry no provenance.** The model is node-level. An edge
  asserting a relationship between two differently-licensed sources has no
  licence of its own.
- **No export applies a policy by default.** `screen()` is available to every
  export path and `count-only` is the default everywhere, so nothing changes
  until a caller asks. Wiring `require-permission` into the snapshot and RDF
  exports is the obvious next step and is not done.
