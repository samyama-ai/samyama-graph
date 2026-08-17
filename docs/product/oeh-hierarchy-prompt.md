# Prompt: OEH hierarchy index + ontology-scale HIER benchmark for samyama-graph (OSS)

You are working in `/home/vm-1/projects/graph_ws/samyama-graph` (OSS, Rust, v0.9.0).
Read `CLAUDE.md` first. TDD at all times; `cargo test && cargo fmt -- --check && cargo clippy -- -D warnings`
must pass before every commit. `export PATH="$HOME/.cargo/bin:$PATH"`.

## Source of the capability

Implement the capabilities of **arXiv:2606.24677 — "One Index for Subsumption and Roll-up
across Time, Geography, and Ontology"** (Mandarapu & Kunkunuru) inside the engine.
The validated Python reference implementation is a sibling repo:
`../relationship-hierarchy-index/src/relhier/` (`poset.py`, `oeh.py`, `oracle.py`,
`baselines/{grail,pll,transitive_closure}.py`, `datasets/`). Port its semantics, not its code
shape — it is pure Python and single-threaded; you are writing a storage-engine index.

The core claim to reproduce in the engine: time, geography and ontology are all
**subsumption posets** with one shared workload — **order testing** (`is x under y?`) and
**hierarchical roll-up** (aggregate a measure over everything under `y`) — and one
structure-selected index answers both, with the roll-up answered *from* the index
(index-resident), not by a join-group-aggregate the index merely filters.

## Part 1 — Engine capability: the OEH index

Add a new declarable index type alongside `src/index/{property_index,manager}.rs`.
Write `docs/ADR/ADR-035-oeh-hierarchy-index.md` first (next free number; 034 is taken by
the unified-memory seam) and let the ADR drive the implementation.

1. **Structural probe.** Given a relationship type (or a set of them) forming the covering
   relation, classify the poset cheaply: tree / low-width DAG / high-width DAG. Reproduce
   the reference's `~8√n` width cap and its *decline* path — above the cap the index must
   refuse to build and the planner must fall back to the existing traversal/2-hop route.
   The decline is a feature; do not paper over it.
2. **Tree → nested-set order embedding.** DFS `[in,out]` interval per node, 2 ints/node;
   subsumption is 2-D containment in O(1). The subtree of `y` is a contiguous in-order
   range, so roll-up is a **Fenwick range-sum in O(log n)**.
3. **Low-width DAG → chain decomposition** (Jagadish chain index). Descendants on a chain
   are a contiguous suffix ⇒ set-semantics roll-up is Σ of per-chain suffix sums —
   **exact and double-count-free**. Double counting on multi-parent DAGs is the single
   easiest correctness bug here; make it an explicit test class.
4. **Monoid roll-up.** Parameterize over a commutative monoid: `SUM`, `COUNT`, `MIN`, `MAX`,
   and a generic `(identity, combine)` seam. Non-invertible monoids (MIN/MAX) cannot use a
   Fenwick difference — use a sparse table / segment tree for those; state the choice in the ADR.
5. **Persistence + lifecycle.** Serialize into the `.sgsnap` snapshot format (see
   ADR-022) so an imported snapshot does not silently lose the index. OEH is a **static**
   index: define write behaviour explicitly — mark stale on mutation of the hierarchy edge
   type, serve from the stale index only when the query opts in, and provide an explicit
   rebuild. Online insert/delete is out of scope for v1; say so in the ADR's non-goals.
   Note the known quirk: after snapshot import, `node.properties` is empty and values must
   be read via node_columns — the measure attribute the roll-up aggregates lives in the
   columnar store, so wire it there, not through the property map.

## Part 2 — Query surface and planner integration

The index is worthless if users must call it explicitly. Make the optimizer pick it up.

- **DDL**: `CREATE HIERARCHY INDEX <name> ON ()-[:IS_A|PART_OF]->() [MEASURE <Label>.<prop>]`
  and the matching `DROP`/`SHOW`. Follow the existing Cypher DDL grammar; heed the PEG
  ordering trap in the parser (longest alternative first) documented in the repo's lessons.
- **Predicate rewrite**: `MATCH (x)-[:IS_A*]->(y)` / `*0..` / `EXISTS { (x)-[:IS_A*]->(y) }`
  used as an existence test must rewrite to an O(1) order test, not a var-length expansion.
- **Roll-up rewrite**: `MATCH (d)-[:IS_A*0..]->(root {id:$r}) RETURN sum(d.measure)` must
  rewrite to a single index-resident range-sum. This is the headline result — a rewrite that
  turns O(subtree) into O(log n).
- **New physical operators** visible in `EXPLAIN`: `HierarchyOrderTest`, `HierarchyRollup`,
  `HierarchyDescendantScan`. Add cost-model entries so the planner chooses them on cost,
  and add planner regression tests asserting the plan shape (not just the answer).
- Also expose the direct functions `subsumes(a, b)` and `hierarchy_rollup(root, 'measure', 'sum')`
  for cases the rewrite cannot reach.
- Cypher literal quirks apply to the tests: use double-quoted strings, and beware
  float-vs-int comparison in `WHERE` silently returning empty.

## Part 3 — Ontology-enriched large-scale benchmark data

Take the existing large-scale corpus (the 200M-node / multi-KG federation and the
500-query mega benchmark) and add the hierarchy backbones it currently lacks. The KGs are
siblings in `graph_ws/`: `pubmed-kg`, `clinicaltrials-kg`, `druginteractions-kg`,
`mitre-attack-kg`, `nvd-cve-kg`, `d3fend-kg`, `telecom-kg`, `powergrid-kg`, `pathways-kg`.

Attach, as loaders under `examples/` following the existing `*_loader` pattern:

| Axis | Ontology / dimension | Attach to |
|---|---|---|
| Ontology (taxonomic) | **NCBI Taxonomy** (1.3M, tree) | pubmed-kg organisms |
| Ontology (disease) | **MONDO** / **ICD-10** (tree-ish) | clinicaltrials-kg conditions, pubmed-kg MeSH |
| Ontology (drug) | **ATC** (strict 5-level tree) | druginteractions-kg |
| Ontology (bio-process) | **Gene Ontology** (high-width DAG) | pathways-kg — expected to **decline**, keep it |
| Geography | **GeoNames** (330k) + country/state/city/zip | trial sites, telecom cells, grid substations |
| Time | generated **calendar dimension** (day ⊑ month ⊑ quarter ⊑ year, ~2.6M rows) | every timestamped edge |
| Threat | **MITRE ATT&CK** tactic ⊑ technique ⊑ sub-technique, **CWE** tree | mitre-attack-kg, nvd-cve-kg |
| Industry/asset | **NAICS** or **ISA-95** equipment hierarchy | telecom-kg, powergrid-kg |

Ground rules: public-data KGs stay public on GitHub with a Gitea mirror; anything
license-restricted (UMLS/SNOMED — see the UMLS licence process) must be gated behind an
opt-in loader flag and must not be committed as data. Record provenance in the S3
MANIFEST/PROVENANCE/INDEX convention under the `samyama-ai` profile. Any big load runs on
**spot instances only**, resumable, and you verify no orphaned instances afterwards.

## Part 4 — The new benchmark category: HIER

Create a new benchmark category for hierarchy-heavy complex queries — the existing LDBC
SNB / BI / FinBench / Graphalytics suites contain almost no subsumption or roll-up, which
is exactly why this capability is invisible today. Deliver:

- `benches/hierarchy_benchmark.rs` (Criterion, follows `ldbc_bi_benchmark.rs` conventions)
- `benchmarks/hier/` — the query corpus as data (YAML/JSON: id, class, cypher, params,
  expected-shape, oracle spec), a runner, and committed `results/*.csv` + figures.

**Target ≥100 queries across these classes** (name them H1…H10, keep IDs out of engine source):

- **H1 Order test** — is `x` under `y`, over each axis, at each depth.
- **H2 Single roll-up** — aggregate a measure over a subtree, across subtree sizes spanning
  4 orders of magnitude.
- **H3 Level roll-up** — group-by-level (`GROUP BY depth`), the classic OLAP shape.
- **H4 Cross-hierarchy conjunction** — *the paper's whole point*: ontology × time × geography
  in one query ("all trials for any descendant of MONDO:X, in any city under state Y,
  in any month under 2025"). One index type, three axes, no per-silo machinery.
- **H5 Hierarchy × traversal** — k-hop graph traversal whose predicate is a subsumption test
  (drug interaction paths where both endpoints are under an ATC class).
- **H6 Anti-subsumption / negation** — `NOT` under `y`, and set-difference of two subtrees.
- **H7 LCA / merge-base** — lowest common ancestor; validate against `git merge-base`
  the way the reference repo does.
- **H8 Top-k over roll-up** — rank subtrees by rolled-up measure; forces roll-up in a loop.
- **H9 Hierarchy-filtered vector search** — ANN restricted to a subtree (this is where a
  graph engine beats a time-series DB; the paper's TimescaleDB baseline cannot express it).
- **H10 Temporal roll-up windows** — sliding month/quarter aggregates, head-to-head with
  the TimescaleDB continuous-aggregate numbers from the paper.

**Baselines, per query**: (a) same Cypher with the OEH index disabled (recursive var-length
traversal) — the honest in-engine before/after; (b) the 2-hop/PLL path where the index
declines; (c) where the data permits, Neo4j and TigerGraph via
`../samyama-graph-competitor-benchmarks`; (d) for H10, TimescaleDB continuous aggregates.

**Report**: latency p50/p99, build time, index bytes/node, and speedup — plus a
*regime plot* showing where the index declines and the fallback takes over. Reproduce the
paper's claims in-engine or report the delta honestly: ~half the space and 6–7× faster build
vs PLL at query parity on trees; index-resident roll-up in the single-digit-µs regime.

**Correctness is the gate, not a footnote.** Every HIER query is validated against a
brute-force oracle (port `oracle.py`) on a small graph and against a materialized
ground-truth table at scale. A roll-up that double-counts on a multi-parent DAG is a
release blocker. Exactness against TimescaleDB must match to the unit
(the reference gets day 704,800 / month 21,168,000 exactly).

## Phasing (fail-fast — do not start a stage until the prior gate is green)

1. ADR-035 written and self-consistent → 2. poset + probe + nested-set + oracle tests →
3. chain decomposition + double-count tests → 4. monoid roll-up (Fenwick / segment tree) →
5. snapshot persistence + staleness → 6. DDL + planner rewrites + EXPLAIN + cost model →
7. ontology loaders + provenance → 8. HIER corpus + runner + baselines →
9. results, figures, `docs/BENCHMARKS.md` + README, ADR closed out.

Stop and report at any gate that cannot be made green, rather than routing around it.

## Deliverables and process

- Code, tests (aim 95%+ coverage on new modules), ADR-035, `docs/BENCHMARKS.md` section,
  README mention, committed results CSVs and figures.
- Ship **GitHub-first** on `samyama-graph`, then mirror to Gitea. Raise, approve and merge
  the PR in the same turn. No `Co-Authored-By` trailers.
- No benchmark IDs, customer or prospect names in engine source — describe patterns
  domain-agnostically; the HIER IDs live in `benchmarks/hier/` data files only.
- Update the `samyama-cloud` wiki with what changed.

## Honest scope to preserve

Carry the reference repo's "Honest scope" section into the ADR: OEH is static; order
embedding / nested sets / dominance-as-subsumption are classical prior art built upon, not
claimed; the contribution is the **unification** plus **index-resident roll-up**. Genuinely
low-width multi-parent DAGs are rare in practice — if the ontology corpus mostly yields
trees and high-width DAGs, say that in the results rather than selecting datasets to hide it.
