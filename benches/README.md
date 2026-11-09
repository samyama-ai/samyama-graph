# benches/ — benchmark code and the inputs it reads

Cargo requires the name `benches/` for the Criterion harnesses, so this one
directory holds both the code `cargo bench` runs and the data that code reads.
Other similarly named places hold different things:

| path | what it holds |
|---|---|
| `benches/` (here) | Criterion micro-benchmarks and LDBC / FinBench / Graphalytics harnesses (`*.rs`), plus their inputs (below) |
| `tests/fixtures/` | inputs read by correctness and compatibility tests, e.g. `compat/` and `migrate/` |
| `crates/samyama-optimization/src/benchmarks/` | a Rust module of optimization test functions (classic single-objective, multi-objective, CEC loaders) |

To add a timing harness, put it here. To add the queries or parameters a
harness or example reads, put them in a subdirectory here.

Inputs in this directory:

- `hier/` — hierarchy query corpus (`queries.json`), its generator, and
  recorded results with `results/PROVENANCE.json`. Read by
  `examples/hier_benchmark.rs` and by tests; see `hier/README.md`.
- `ldbc/ldbc-sf1-params.*.json` — LDBC SNB SF1 substitution parameters for
  `ldbc_benchmark.rs`.
