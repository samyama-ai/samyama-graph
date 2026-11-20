# Graph + vector hybrid benchmark (#1987)

Vector-only benchmarks measure what a vector database is built for. This one
measures queries that combine similarity, traversal and predicates in one
statement: the GraphRAG shape.

```sh
# Samyama, in process, synthetic graph at the default size (what the nightly sweep runs)
cargo bench --bench graph_vector_hybrid_benchmark

# Larger graph, results to CSV
cargo bench --bench graph_vector_hybrid_benchmark -- --papers 50000 --people 100000 \
    --methods 1000 --problems 1000 --dim 128 --queries 500 --csv hybrid.csv

# A real snapshot that already carries embeddings
cargo bench --bench graph_vector_hybrid_benchmark -- --snapshot vectors.sgsnap --embedding-property embedding

# Export the graph, the queries and the reference answers for the competitor script
cargo bench --bench graph_vector_hybrid_benchmark -- --export data/hybrid
docker compose -f benches/graph_vector_hybrid/docker-compose.yml up -d
pip install -r benches/graph_vector_hybrid/requirements.txt
python3 benches/graph_vector_hybrid/run_competitors.py --data data/hybrid --csv competitors.csv
```

Run on a quiet host, in `--release` (which `cargo bench` is). CI compiles the
bench and runs `tests/graph_vector_hybrid_harness.rs`, which checks the
generator, the reference, the export, the snapshot reader, and that the three
Cypher templates return the reference's answers on a small graph. **No
benchmark numbers have been collected in this PR**, for Samyama or for any
competitor.

## Data

The default is a seeded synthetic research graph, about twice the size of the
research-kit `vectors.sgsnap`: 5,000 papers, 10,000 people, 200 methods, 200
problems, 50 topics, 64-dimensional embeddings. Every node has a topic;
embeddings cluster around a centre per topic, and edges prefer the same topic
(80% of authors, 70% of citations, 80% of method and problem links), so vector
similarity and graph proximity are correlated without being the same thing.

| node | edges |
|---|---|
| `Paper {uid, emb}` | `(:Paper)-[:CITES]->(:Paper)`, earlier papers only |
| `Person {uid}` | `(:Person)-[:AUTHORED]->(:Paper)`, 1–4 authors per paper |
| `Method {uid, emb}` | `(:Person)-[:COAUTHOR]->(:Person)`, one edge per pair who share a paper |
| `Problem {uid, emb}` | `(:Paper)-[:USES_METHOD]->(:Method)`, `(:Paper)-[:ADDRESSES]->(:Problem)` |

Synthetic embeddings are not text embeddings. They have cluster structure,
which HNSW needs to behave as it does on real data, but recall on them says
nothing about how well a real model's neighbours answer a real question.

`--snapshot FILE` reads the same labels and edge types out of a `.sgsnap`
instead, and reloads them into the schema above. Papers must all carry a
vector in `--embedding-property` (default `embedding`); methods and problems
are used only when they all have one, and template 3 is skipped otherwise. A
snapshot without `COAUTHOR` gets it derived from shared authorship. The
research-kit snapshots do not carry embeddings yet, so they have to be embedded
first (the issue suggests bge-base, 768-d); this PR does not do that.

## Templates

All three are single Cypher statements. They use Neo4j's spelling of the vector
call, `db.index.vector.queryNodes('paper_emb', k, $q)`, so the exported text
runs unchanged on Neo4j 5. `uid` breaks ties in every `ORDER BY`.

1. **Seeded expansion** — the 10 nearest papers to a text, every paper within
   two `CITES` hops of them in either direction (seeds included), reranked by
   similarity to the text; top 10.
2. **Traversal-filtered k-NN** — among papers authored by anyone within two
   `COAUTHOR` hops of a person (the person included), the 10 nearest to a text.
   Written traversal-first, so it is exact over the candidates it reaches.
3. **Vector join** — for a method, the 5 most similar problems that no paper
   yet links to it (no paper both `USES_METHOD` it and `ADDRESSES` the
   problem): the link-prediction shape. One query per method.
4. **Hybrid text (BM25 + vector + graph expansion)** — **not implemented.** It
   needs `CALL db.hybrid.search(...)` from #1279 (PR #2284), which is not on
   `dev`. Chaining `db.index.fulltext.queryNodes` and a vector call by hand
   would measure a fusion written for this bench, not the feature, and there
   is no labelled subset yet to compute the nDCG@10 the issue asks for.

A query text is a paper's embedding plus noise. For template 2 the paper is
one the person wrote, so the question is about their neighbourhood's work.

## Metrics

Per template: **recall** (share of the reference answer returned, as a set),
**exact** (instances whose answer is the same set as the reference), **QPS**
(instances over the wall time of the timed pass), **p50/p99** per-query
latency, and mean rows returned.

The reference is brute force in Rust over the generated graph, with no index,
no planner and no Cypher: exact k-NN for the seeds, breadth-first search for
the hops, f64 cosine for every ranking. It is computed before timing starts.
Answers are compared as sets because two nodes whose scores differ by less
than the engine's f32 rounding may swap places, which is not a wrong answer.

On Samyama the only approximate step is the HNSW search for template 1's
seeds, so templates 2 and 3 should score 1.0 and template 1's recall is the
cost of that approximation after expansion and rerank. A query error aborts
the run rather than being scored as an empty answer.

Samyama is measured in process through `QueryEngine::execute_with_params` with
the result cache off: parsing, planning and execution, no network. Ten
untimed queries warm each template first; then every instance runs once,
sequentially, on one thread.

## Competitors

`benches/graph_vector_hybrid/run_competitors.py` reads the export and runs:

- **Neo4j 5.26 (community)** — the same Cypher text against Neo4j's vector
  index, over Bolt.
- **Qdrant 1.12 + Neo4j** — the two-system pipeline: vectors only in Qdrant,
  graph only in Neo4j, glued by the client and timed end to end including
  every round trip. Template 1 is Qdrant k-NN, then Neo4j expansion, then a
  Qdrant search restricted to the ids reached. Template 2 is the Neo4j
  traversal, then a Qdrant search restricted to the papers reached. Template
  3 fetches the linked problems from Neo4j and the method vector from Qdrant,
  then searches problems excluding the linked ones.

Both are scored against the same reference with the same metrics.
Versions are pinned in `docker-compose.yml` and `requirements.txt`.

The script is not run in CI and has not been run for this PR. Its timings are
over the network and Samyama's here are in process, so the two sets of numbers
are **not comparable** as they stand: a fair comparison needs Samyama measured
over RESP or HTTP on the same host. At the default size Qdrant does not build
an HNSW index (the collection is under its indexing threshold) and searches
exactly; say so with any number it produces.

## Not done

- Template 4 (hybrid text), pending #1279 / PR #2284, and nDCG@10 on a
  labelled subset.
- Embedding the research-kit snapshots, and OGBN-papers100M or the 1.44B-edge
  federation graph for scale.
- FalkorDB, Memgraph and ArangoDB adapters.
- Samyama over the network (RESP / HTTP), which a published comparison with
  the competitor script needs.
- Concurrency: every system runs one client, sequentially.
- Published results and plots.
