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

There is no Neo4j dump importer yet
([#1362 is RDF](https://github.com/samyama-ai/samyama-graph/issues/1362); a dump
reader is INT-02 and not built). Two routes that do work:

- **Cypher script.** `apoc.export.cypher.all` on the Neo4j side, then replay it
  here. Slow for large graphs and the most faithful for small ones.
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
