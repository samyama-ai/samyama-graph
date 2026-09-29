# benchmarks/ — workload data, not benchmark code

This directory holds the **inputs and recorded results** that benchmarks run
against. It contains no Rust. Three similarly named places exist in this
repository, and they hold different things:

| path | what it holds | who requires the name |
|---|---|---|
| `benches/` | Criterion micro-benchmarks and LDBC / FinBench / Graphalytics harnesses — the code `cargo bench` runs | Cargo |
| `benchmarks/` (here) | corpora, parameter files and recorded results | nobody |
| `crates/samyama-optimization/src/benchmarks/` | a Rust module of optimization test functions (classic single-objective, multi-objective, CEC loaders) | nobody |

To add a timing harness, put it in `benches/`. To add the queries, fixtures or
parameters a harness or example reads, put them here.

What is here:

- `hier/` — hierarchy query corpus (`queries.json`), its generator, and
  recorded results with `results/PROVENANCE.json`. Read by
  `examples/hier_benchmark.rs`; see `hier/README.md`.
- `compat/neo4j-idioms.cypher` — Neo4j Cypher idioms, read by
  `tests/compatibility_corpus.rs`.
- `migrate/` — Neo4j APOC export fixtures (full and partial), read by
  `tests/neo4j_import.rs`.
- `ldbc-sf1-params.*.json` — LDBC SNB SF1 substitution parameters for
  `benches/ldbc_benchmark.rs`.

Renaming this directory (for example to `workloads/`) is tracked in #1452 and
left to a maintainer: `hier/results/PROVENANCE.json` and the defaults in
`examples/hier_benchmark.rs` record paths under `benchmarks/`.
