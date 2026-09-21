# Migrating from Neo4j

Start by finding out whether your queries run. Everything else on this page is
about what to do with the answer.

## 1. Point the report at your queries

```bash
cargo run --release --example compatibility_report -- --queries your_queries.cypher
```

The file holds one query per statement, separated by a `;` at the end of a line
or by a blank line; `//` and `#` start a comment. A `.json` file is read as an
array of strings instead, which is the shape most query logs export in. Add
`--json report.json` for a machine-readable version.

You get a count and, more usefully, the refusals **grouped by cause**:

```
  queries read   59
  accepted       54  (91.5%)
  refused        5

  Refusals by cause, commonest first:
       1x  NotFound  Unknown procedure: apoc.periodic.iterate
       1x  SyntaxError  Parse error: expected label
       …
```

A migration is planned against causes. Six hundred failures from one missing
function is one afternoon; six failures from six different causes is six.

**"Accepted" means it parses and plans.** It is a claim about the language, not
about your data and not about the answer — an accepted query can still return
the wrong rows, and the tool would not know. It is the strongest thing that can
be said without your database; a percentage that claimed more would be a guess.

Planning runs against an empty store, so a refusal that depends on your schema —
a declared index, a registered procedure — may not be one for you.

## 2. What the causes usually mean

| Cause | What to do |
|---|---|
| `Unknown procedure: apoc.*` | There is no APOC namespace. Most `apoc.text.*` and `apoc.coll.*` calls have a plain Cypher equivalent; `apoc.periodic.iterate` has no equivalent and the loop moves to your application. |
| `Unknown function` | Check `docs/CYPHER_COMPATIBILITY.md` for the supported list. A function that is genuinely missing is worth an issue — a named one gets prioritised over a count. |
| `Parse error: expected …` | A construct the grammar does not take yet. The `expected` list says what it wanted at that point, which usually identifies the construct. |
| `is not yet supported in this clause position` | Clause **order**. Cypher allows almost any order and this engine's grammar enumerates the ones it knows; rewriting the clauses usually gets you through. |
| `Unknown procedure: db.*` | Some of Neo4j's `db.*` introspection exists and some does not. `CALL db.labels()` works; check the matrix for the rest. |

## 3. Moving the data

**`apoc.export.json`, read directly.** On the Neo4j side:

```cypher
CALL apoc.export.json.all('graph.json', {})
```

then here:

```bash
cargo run --release --example neo4j_import -- --file graph.json --json report.json
```

Both APOC shapes are read: the default JSON Lines and the single object written
by `{jsonFormat:'JSON'}`. Relationships are buffered and resolved at the end, so
a file that writes a relationship before its endpoints imports correctly — APOC
does not promise nodes come first.

Read the report, not just the exit code. Three things do not survive a JSON
round trip, and the importer counts each rather than papering over it:

| In the report | What it means |
|---|---|
| `dangling_edges`, `missing_endpoints` | A relationship whose endpoints were not in the export. A subgraph export produces these by construction; the ids are named so you can widen the export query. |
| `values_that_look_temporal`, `values_that_look_spatial` | JSON has no date and no point, so APOC writes them as strings and maps, and that is how they arrive. **They are not converted.** A converter cannot tell your version string `"1.8.0"` from a date, and silently turning one into the other is invisible until a comparison behaves oddly. Convert deliberately: `MATCH (n:Label) SET n.when = datetime(n.when)`. |
| `null_properties_dropped` | Neo4j cannot store a null, so a null in the file came from the exporter. The property is left absent, which is what the source graph had. |

`--require-lossless` turns any of those into a non-zero exit, which is the form
to use in a script. Node ids are **not** preserved: Neo4j ids are not stable
across databases and treating them as keys is the migration mistake that
outlives the migration. Use a property you control.

Two other routes:

- **Cypher script.** `apoc.export.cypher.all` on the Neo4j side, then replay it
  here. Slower, and the most faithful for small graphs, because it carries
  types the JSON form cannot.
- **CSV.** `LOAD CSV WITH HEADERS FROM 'file:///…'` is supported. `http` and
  `https` are deliberately not — a server that fetches arbitrary URLs on a
  client's behalf is an SSRF primitive — so stage the file locally.

## 4. What you should know before you commit

Said here rather than discovered later:

- **There is no authentication.** The HTTP API reads no credential and accepts
  any origin ([#1328](https://github.com/samyama-ai/samyama-graph/issues/1328)).
  Bind to localhost or put it behind something that authenticates.
- **Row order without `ORDER BY`** is guaranteed only for a label scan
  ([#1364](https://github.com/samyama-ai/samyama-graph/issues/1364)). Neo4j is
  no stricter in principle and much more stable in practice, so a query that
  paged without `ORDER BY` and worked there needs one here.
- **Leaving again is documented**, with its losses named, in
  [`LEAVING-SAMYAMA.md`](LEAVING-SAMYAMA.md). Read it before you arrive, not
  after: the measure of lock-in is what the exit costs, and we would rather you
  checked.
- **What we send anywhere** is in [`DATA-HANDLING.md`](DATA-HANDLING.md). The
  short version is nothing, unless you configure an LLM feature.

## 5. Tell us what refused

`benchmarks/compat/neo4j-idioms.cypher` is the corpus this engine is measured
against, and it is written from Neo4j's documentation and from application code
rather than from our feature list — one derived from our features would report
100% of whatever we already do.

If your report names a cause that is not in that file, it is a gap we are not
measuring. Send the query, or add it: an idiom in the corpus is a gap somebody
is counting, and a gap nobody counts is a gap nobody closes.
