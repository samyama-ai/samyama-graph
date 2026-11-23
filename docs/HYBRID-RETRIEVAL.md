# Hybrid retrieval

`db.hybrid.search` combines the [full-text index](./FULL-TEXT-SEARCH.md)
(BM25) and a vector index (HNSW) into one ranked list, optionally widened by a
bounded walk around the best hits (#1279). It is the retrieval half of a
retrieval-augmented answer: the rows it returns are what a caller hands to a
language model.

Keyword and vector search fail differently. BM25 misses a paraphrase, or a
document that uses a synonym; an embedding misses an exact identifier, a gene
symbol or a drug code that the model never saw. Running both and fusing the
lists finds what either alone would miss, and the graph walk adds what the
best hits are connected to without a traversal over the whole graph.

## Calling it

```cypher
CALL db.hybrid.search(
  'doc_text',                 // full-text index name
  'statin induced myopathy',  // query text
  'doc_emb',                  // vector index name, or {label: 'Doc', property: 'emb'}
  $questionEmbedding,         // query vector
  {k: 10, hops: 1, relTypes: ['CITES'], maxExpanded: 50}
)
YIELD node, score, bm25Rank, vectorRank, foundBy, hops, timings
RETURN node.title, score, foundBy
```

| argument | type | meaning |
|---|---|---|
| `fulltextIndex` | string or null | A full-text index name, as `db.index.fulltext.queryNodes` takes |
| `queryText` | string or null | The BM25 query, in the [full-text query syntax](./FULL-TEXT-SEARCH.md#query-syntax-all-of-it) |
| `vectorIndex` | string, map or null | A vector index name, or `{label, property}` for an index created without one |
| `queryVector` | list of numbers or null | The query embedding. The engine does not embed text: compute it with the model that embedded the documents |
| `config` | map, optional | Below |

Either query may be null or empty, and its stage is then skipped: a text
without a vector is BM25 alone, through the same fusion and expansion. Both
null is an error. An index that is named must exist, even when its query is
null — a misspelt name is refused with the names that do exist, rather than
returning fewer rows. The arguments may be `$parameters`.

### Config

| key | default | meaning |
|---|---|---|
| `k` | 10 | How many fused candidates become seeds |
| `candidates` | `max(50, k)` | How many hits each of BM25 and the vector search contributes to the fusion |
| `fusion` | `'rrf'` | `'rrf'` or `'weighted'`; see [Fusion](#fusion) |
| `rrfK` | 60 | RRF's rank offset |
| `bm25Weight`, `vectorWeight` | 1, 1 | Each list's weight, in either fusion |
| `ef` | the index's | The vector search's width, as `queryNodes`' `{ef}` |
| `hops` | 0 | How far the expansion walks from each seed; at most 5. 0 is no expansion |
| `relTypes` | any | Relationship types the expansion follows: a list, or one name |
| `direction` | `'both'` | `'out'`, `'in'` or `'both'` |
| `maxExpanded` | 100 | The most nodes that are not seeds the expansion may add |
| `graphWeight` | 1 | The weight of the graph term in the rerank |
| `decay` | 0.5 | The graph term's factor per hop, in [0, 1] |
| `sourceProperty` | see [Provenance](#provenance) | The property holding a hit's source URI |

Any other key is an error, as a misspelt key silently ignored would run a
different search from the one written.

### Columns

| column | type | meaning |
|---|---|---|
| `node` | node | The hit |
| `score` | float | What the rows are ordered by: `fusedScore + graphWeight * graphScore` |
| `fusedScore` | float | The fusion's score; 0 for a node only the expansion found |
| `graphScore` | float | The graph term before `graphWeight`; 0 without expansion |
| `bm25Rank`, `bm25Score` | integer, float or null | 1-based rank and BM25 score in the BM25 list, null if not in it |
| `vectorRank`, `vectorScore` | integer, float or null | 1-based rank and similarity in the vector list (the same similarity `queryNodes` yields as `score`), null if not in it |
| `foundBy` | list of strings | `'bm25'`, `'vector'` and `'graph'`, for each way the node was found |
| `hops` | integer | 0 for a seed; otherwise steps from the seed nearest it |
| `seed` | node | The node itself for a seed; otherwise the seed contributing most to its graph term |
| `timings` | map | Milliseconds per stage: `bm25Ms`, `vectorMs`, `fusionMs`, `expansionMs`, `rerankMs`, `totalMs`. The same map on every row of one call |
| `provenance` | map | The citation payload; see [Provenance](#provenance) |

### Provenance

Every row carries a `provenance` map unless the call `YIELD`s other columns,
so a bare `CALL db.hybrid.search(...)` returns enough to render a citation
without the caller knowing the corpus's property names (AI-08, #1707). Every
key is always present; what is unknown is null.

| key | value |
|---|---|
| `nodeId` | The node's id, as `id(node)` |
| `labels` | Its labels, sorted |
| `source` | Its source URI, or null if it has none |
| `sourceProperty` | The property `source` was read from, or null |
| `score` | The row's `score` |
| `foundBy` | The row's `foundBy` |
| `bm25Rank`, `vectorRank` | The row's ranks, null if not in that list |
| `seedId`, `hops` | The seed it was reached from and how far; itself and 0 for a seed |
| `index` | `{fulltext, vector}`: the indexes searched, null for one not used. A vector index given as `{label, property}` reads `Label.property` |

`source` is read from `sourceProperty` when the call names one, and only from
it. Otherwise it is the first of `source_uri`, `sourceUri`, `uri`, `url`,
`source`, `src` the node has. A value that is not a string is given as its
text.

Rows come best first, ties broken by fused score and then node id, so a
search is reproducible. There is no `LIMIT` of its own beyond `k` and
`maxExpanded`: at most `k + maxExpanded` rows.

## The stages

1. **BM25** — the full-text index's top `candidates` for `queryText`,
   exactly what `db.index.fulltext.queryNodes` returns.
2. **Vector** — the vector index's `candidates` nearest to `queryVector`, as
   `db.index.vector.queryNodes` finds them, with the same approximation.
3. **Fusion** — the two lists become one, and its top `k` are the seeds.
4. **Expansion** — a breadth-first walk from the seeds, at most `hops` deep.
5. **Rerank** — each node's fused score plus its graph term.

### Fusion

**Reciprocal Rank Fusion** (the default) uses ranks only:

```
fused(n) = bm25Weight / (rrfK + bm25Rank(n)) + vectorWeight / (rrfK + vectorRank(n))
```

with a term dropped for a list `n` is not in. BM25 scores are unbounded and
depend on the corpus; vector similarities are bounded and depend on the model.
Ranks avoid reconciling the two scales, which is why RRF is the default. A
node at rank 1 in one list and absent from the other scores `1/61`; one at
rank 2 in both scores `2/62`, and wins.

**Weighted** fusion (`fusion: 'weighted'`) uses the scores:

```
fused(n) = bm25Weight * bm25Score(n) / maxBm25 + vectorWeight * vectorScore(n) / maxVector
```

each list divided by its own best score, so the best hit of each list
contributes its full weight. Division by the maximum, not min-max scaling:
min-max would score the last candidate of each list 0, as if it had not been
found. A negative similarity counts as 0.

### Expansion

From every seed, a breadth-first walk over `relTypes` in `direction`, at most
`hops` steps. Nodes are admitted nearest level first and, within a level, from
the higher-ranked seed first, until `maxExpanded` nodes that are not seeds have
been admitted; the walk continues only through seeds and admitted nodes. Both
caps are hard: a hub cannot pull in its whole neighbourhood, and a node beyond
the cap is neither returned nor walked through.

### Rerank

```
graphScore(n) = Σ fusedScore(s) * decay^d(s, n)    over seeds s ≠ n within hops of n
score(n)      = fusedScore(n) + graphWeight * graphScore(n)
```

A node is rewarded for being close to good seeds — a citation shared by two
strong hits outranks one reached from a single weak hit, and a seed connected
to other seeds rises above an isolated one. With `hops: 0`, `score` is the
fused score.

## Latency

`timings` reports each stage's wall-clock time for that call, so a caller can
see where the time went in a single query. For distributions,
`benches/hybrid_retrieval_benchmark.rs` runs a synthetic corpus and prints
p50 and p95 per stage, and for the same search through Cypher:

```
cargo bench --bench hybrid_retrieval_benchmark -- --docs 100000 --queries 500 --hops 2
```

No figures are quoted here; they depend on the host and the corpus. Measure on
yours, in `--release`.

## What this does not do

- **No language model.** Embedding the question and writing the answer happen
  outside the engine. The flow in #1279 ends in an LLM stage; its latency is
  the caller's to measure, alongside `timings.totalMs`.
- **No learned reranker.** The rerank is the formula above. A cross-encoder
  reranker would run in the caller on the rows returned.
- **One index of each kind per call.** To search two vector indexes, call it
  twice and fuse in the caller, or `UNION` the calls.
- **The expansion is unweighted and untyped beyond `relTypes`.** Relationship
  properties do not affect the walk or the graph term.
