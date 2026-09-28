# `examples/`
Every program in this directory, what it does, and what data it needs.

`cargo run --release --example <name>` runs any of the `.rs` files (the two non-Rust
programs say how below). The names are an interface: `scripts/run_all_examples.sh`,
`scripts/verify-sweep.sh`, `scripts/regression-test.sh`, `.github/workflows/ci.yml` and
external knowledge-graph repositories invoke them by name, so they do not get renamed.

**Start here** if you are new: the demos on built-in data. They need nothing but a
checkout.

**Most of this directory is internal.** Of the 122 programs, 57 are performance
investigations, conformance checks or maintainer tools — written to answer one question
about the engine, usually once, often to kill a hypothesis. They print verdicts and
timings, not results you can use. Those sections say *Internal* in their opening line;
you are not expected to run them. The 8 benchmark runners are runnable but need a quiet
host and an external corpus to mean anything.

Purposes below are taken from each file's own header comment.

| Kind | Count | Meant for a user? |
|---|---:|---|
| [Demos on built-in data](#demos-on-built-in-data) | 20 | Yes |
| [UC1-UC5 optimization use cases](#uc1-uc5-optimization-use-cases) | 9 | Yes |
| [Dataset loaders and post-processing passes](#dataset-loaders-and-post-processing-passes) | 17 | Yes |
| [Import, export and user-facing tools](#import-export-and-user-facing-tools) | 10 | Yes |
| [Benchmark runners](#benchmark-runners) | 8 | No - internal |
| [Performance investigations](#performance-investigations) | 22 | No - internal |
| [Conformance and correctness checks](#conformance-and-correctness-checks) | 32 | No - internal |
| [Maintainer tools](#maintainer-tools) | 3 | No - internal |
| [Hardware client sketch](#hardware-client-sketch) | 1 | No - internal |

Helper code shared between programs lives in the 17 `*_common/` directories and in
`examples/common/`; they hold no runnable program.


## Demos on built-in data
These build their own graph in memory (or load a small bundled fixture) and print a
narrated walkthrough. They are the intended starting point.

| Program | What it does | Data it needs |
|---|---|---|
| `agentic_enrichment_demo` | Generation-Augmented Knowledge: the database notices a gap in its own knowledge, asks a model to fill it, and writes the answer back. | Built-in. Needs the `claude` CLI on PATH for the generation step. |
| `amr_stewardship_demo` | Paper 8 problem 6 — antimicrobial-resistance antibiotic stewardship: regimen selection over a KG built from NCBI AMRFinderPlus `ReferenceGeneCatalog.txt`. | Downloads / expects the public-domain AMRFinderPlus catalogue; `--out` for results. |
| `banking_demo` | Enterprise banking: fraud patterns, money-laundering structuring, OFAC screening, customer relationship networks. | Built-in (synthetic TSV fixtures). |
| `clinical_trials_demo` | Clinical-trial intelligence: trial/drug/condition/site/patient KG, patient-trial matching by 128-dim vector search. | Built-in. |
| `clinical_trial_sites_demo` | Paper 8 problem 2 — clinical-trial site selection: maximise trial count plus country diversity subject to a site budget. | `--snapshot` pointing at the public clinicaltrials (AACT) snapshot. |
| `cluster_demo` | Raft high availability: 3-node voter cluster, leader election, quorum writes, partition and split-brain handling, learner read replicas. | Built-in. |
| `cypher_problem_demo` | End-to-end optimization: solve a Cypher-grounded least-squares fit with a Rao-family solver and print cache-hit statistics. | Built-in. |
| `drug_repurposing_demo` | Paper 8 problem 1 — drug-repurposing portfolio selection: maximise target-gene coverage over a chosen number of drugs. | `--snapshot` pointing at the druginteractions snapshot (default `../druginteractions-kg/data/druginteractions.sgsnap`). |
| `enterprise_soc_demo` | Security operations centre: APT campaign investigation over network topology, MITRE ATT&CK, CVE threat intel with embeddings, attack paths. | Built-in. |
| `grid_dispatch_demo` | Paper 8 problem 5 — economic and environmental power-grid dispatch over generators and hourly demand. | `--data-dir` with public smart-grid sample CSVs; `--out` for results. |
| `healthcare_allocation_demo` | Paper 8 problem 4 — healthcare resource allocation under equity constraints (WHO SPAR + NHWA + GAVI + Global Fund + IHME). | `--snapshot` pointing at the public health-systems snapshot. |
| `industrial_kg_demo` | Industrial asset operations: Site/Location/Equipment/Sensor hierarchy, failure modes by vector similarity, maintenance optimization (IBM AssetOpsBench, ISO 14224, ISA-95). | Built-in. |
| `knowledge_graph_demo` | Enterprise knowledge graph: documents/employees/projects/technologies, semantic search, PageRank knowledge hubs, WCC topic clustering. | Built-in. |
| `pca_demo` | Principal component analysis on a citation network: 8 numeric properties reduced to 3 components, then HNSW similarity on the reduced vectors. | Built-in. |
| `sdk_demo` | The `samyama-sdk` `EmbeddedClient`: nodes and edges via Cypher, read-only queries, status, result handling, the algorithm extension trait. | Built-in. |
| `smart_manufacturing_demo` | Industry 4.0 digital twin: factory graph, production scheduling by Cuckoo Search, failure-cascade prediction, energy cost by Jaya, defect root cause. | Built-in. |
| `social_network_demo` | Social network analysis over a 2,000-node community: PageRank influencers, WCC/SCC communities, BFS diffusion, force-directed SVG output. | Built-in. |
| `supply_chain_demo` | Global pharmaceutical supply chain, 100+ entities across 30+ countries: topology, Suez-blockage disruption impact, port optimization. | Built-in. |
| `wildfire_evac_demo` | Paper 8 problem 7 — wildfire evacuation routing: multi-source/multi-sink capacitated assignment over the Paradise, CA road network. | Fetches a public-domain OpenStreetMap Overpass export at runtime (no auth); `--out` for results. |
| `simple_client_demo.py` | Python RESP client: GRAPH.QUERY over the RESP protocol, batch execution, connection retry. | Needs a running server: `cargo run -- --port 6379`. Python 3, no dependencies. |

## UC1-UC5 optimization use cases
Five use cases from the SGE + Optimization use-case catalog, each in a pair: a synthetic
fixture that runs from a clean checkout, and a `*_real` counterpart that targets a
deployed Samyama instance holding the real KG (`SAMYAMA_URL`, default
`http://localhost:8080`). The pattern under test is the Cypher-driven fitness evaluator —
the optimizer's inner loop queries the graph instead of carrying its own copy of the
topology.

| Program | What it does | Data it needs |
|---|---|---|
| `uc1_trial_site_selection` | UC1 synthetic — clinical-trial site selection with NSGA-II, site-count bounds and a region-diversity floor. | Built-in fixture. |
| `uc1_aact_real` | UC1 real — the same problem over live AACT data: real sites, enrolment rate and a country-tier cost proxy derived from the graph. | A deployed instance holding the AACT KG (`SAMYAMA_URL`). |
| `uc2_combo_dosing` | UC2 synthetic — drug-combination dosing: a continuous dose vector, 3 objectives (efficacy, side effect, interaction) from two Cypher queries per candidate. | Built-in fixture. |
| `uc2_drug_interactions_real` | UC2 real — the same over the DrugBank + DGIdb + SIDER KG, with contraindication re-cast as overlapping side-effect profiles (the KG has no drug-drug edges). | A deployed instance holding the Drug Interactions KG. |
| `uc3_capacity_planning` | UC3 — hospital network capacity planning with BMR: 15 continuous variables over 5 facilities against an M/M/c wait-time model. | Built-in fixture. |
| `uc4_kg_completion` | UC4 synthetic — biomedical KG edge completion: QO-Jaya tunes a structural link-predictor, scored by Hits@5 on a held-out TREATS split. | Built-in fixture. |
| `uc4_aact_real` | UC4 real — the same over live AACT data, using the 3-hop Intervention/ArmGroup/ClinicalTrial/Condition path as the implicit treatment relationship. | A deployed instance holding the AACT KG. |
| `uc5_agent_routing` | UC5 synthetic — agentic tool-call plan optimization with NSGA-II over accuracy, latency and token cost. | Built-in fixture. |
| `uc5_age_telemetry_real` | UC5 real — the same, with the (Question)-[:USED_TOOL]->(Tool) history populated organically by running the AGE plan executor against real Cypher tools. | A deployed instance plus the AGE plan executor. |

## Dataset loaders and post-processing passes
Each loader reads an external dataset into a `GraphStore` through the Rust SDK and
optionally writes a `.sgsnap` snapshot. None of them ship the data; you supply a
`--data-dir`. `scripts/download_ldbc_snb.sh`, `scripts/download_finbench.sh` and
`scripts/download_graphalytics.sh` fetch three of them. The last two entries are
post-processing passes that run *after* a loader, over an already-populated store.

| Program | What it does | Data it needs |
|---|---|---|
| `aact_loader` | The full ClinicalTrials.gov (AACT) pipe-delimited dump: 575K studies, ~2M nodes, ~10M edges in 3-8 minutes. | `--data-dir data/aact`; `--snapshot` optional. |
| `civic_loader` | CIViC (Clinical Interpretations of Variants in Cancer, CC0): variants bridged to existing :Gene nodes by symbol and :Variant nodes by ClinVar ID. | Nightly TSV bundle from civicdb.org/downloads/nightly/. |
| `cricket_loader` | Cricsheet ball-by-ball cricket JSON: 21K matches, ~36K nodes, ~1.4M edges. | `--data-dir data/cricket`. |
| `druginteractions_loader` | DrugBank (CC0), DGIdb and SIDER: drugs, genes, side effects. | `--data-dir data/druginteractions`. |
| `finbench_loader` | LDBC FinBench, from CSV on disk or generated synthetically. | `--data-dir`, or `--generate` for synthetic. `scripts/download_finbench.sh`. |
| `football_loader` | DataHub World Cup CSVs: tournaments, teams, players, matches, goals, stadiums, managers. | `--data-dir` holding the seven named CSVs. |
| `hgnc_ensembl_loader` | The canonical Gene identity layer from HGNC `hgnc_complete_set.txt`, bridged to existing :Protein (UniProt) nodes via :SAME_AS. | HGNC complete set; Ensembl GFF3 in a follow-up pass. |
| `imdb_loader` | IMDB non-commercial TSVs: movies, series, persons, genres, ratings — ~100K-300K nodes, ~1M-3M edges by vote threshold. | `--data-dir` with `title.basics.tsv`, `title.ratings.tsv` etc., plain or `.gz`. |
| `ldbc_loader` | LDBC SNB Scale Factor 1: ~3.18M nodes, ~17M edges. | `data/ldbc-sf1/...CsvBasic-LongDateFormatter/`; `scripts/download_ldbc_snb.sh`. |
| `legal_judgments_loader` | Indian Supreme Court judgments (2016) CSV: cases, judges, parties, acts, topics — a public PostgreSQL+AGE+pgvector demo reproduced on one engine. | `--data-dir`. |
| `oncokb_loader` | OncoKB oncology-evidence JSON exports. License-gated (free academic registration); runs offline against fixtures without a token. | Pre-downloaded OncoKB v1 API JSON files. |
| `ontology_loader` | A real hierarchy in its published format, with an OEH index declared over it — so the structural probe's verdict is about the real ontology, not a generator. | `--path` to the published ontology (NCBI Taxonomy, ATC, Gene Ontology). |
| `pathways_loader` | Reactome, STRING and Gene Ontology, through direct API calls rather than Cypher. | `--data-dir data/pathways`; `--phases` to select. |
| `pubmed_loader` | PubMed pipe-delimited flat files produced by `parse_pubmed_xml.py`: articles, authors, MeSH terms, chemicals, citations, grants. | `--data-dir data/pubmed-parsed`. |
| `surveillance_loader` | WHO GHO disease surveillance: countries, diseases, vaccine coverage, health indicators. | `--data-dir data/surveillance`. |
| `aact_biomarker_extractor` | Post-processing pass: mines `eligibilities.txt` free text to add first-class :Biomarker nodes and REQUIRES_BIOMARKER / TARGETS_GENE edges. | Runs after the AACT and HGNC loaders, or over a snapshot holding :ClinicalTrial and :Gene. |
| `property_bridge` | Post-processing pass: adds :SAME_AS (or any chosen type) between two labels whose nodes share a property value — the generic cross-KG identity bridge. | `--snapshot` of an already-loaded graph. |

## Import, export and user-facing tools
Things a user of the engine would run on their own data — migration in and out, and
checking their own queries.

| Program | What it does | Data it needs |
|---|---|---|
| `compatibility_report` | Point it at your `.cypher` files; it reports which of your queries this engine accepts (INT-11). Answers 'will my queries run', not 'which features exist'. | `--queries <dir or file>`; `--json` for machine output. |
| `cypher_lint` | Parse-checks every Cypher statement in a file, with no database — so schema and query files in KG repos cannot silently stop parsing (#513). | Your own `.cypher` files. |
| `cypher_query_runner` | Imports a snapshot and runs every `.cypher` file in a directory, splitting on `;`; prints columns, sample rows and total row counts. | `--snapshot` plus a query directory. |
| `export_cypher` | Emits the whole graph as standard Cypher `CREATE` statements — the exit path that needs no agreement from anybody (INT-07). | A loaded graph; `--out`. |
| `graphml_export` | Writes the graph as GraphML for Gephi, yEd and Cytoscape, and prints a loss report of what the format could not carry (INT-05). | `--snapshot`; `--out`, `--json`. |
| `neo4j_import` | Imports a Neo4j graph from `apoc.export.json` output (INT-02). The data half of a migration; `compatibility_report` is the query half. | `--file` with the APOC JSON export. |
| `parse_check` | Reads queries from stdin, one per line, and prints `ok` or `fail` per line — for scripts bisecting a construct over hundreds of variants. | stdin. |
| `provenance_report` | Where the rows in a graph came from and what may leave it, under a chosen `--policy` (TRUST-03, ML-09, EVAL-10). | `--snapshot`; `--policy`, `--json`. |
| `samyama_to_neo4j` | Converts a `.sgsnap` into `nodes-<Label>.csv` / `rels-<TYPE>.csv` for `neo4j-admin database import`. | A `.sgsnap` file and an output directory. |
| `supply_chain_snapshot` | Paper 8 problem 3 — builds the Supply Chain India KG (ports, cities, road distances from OSM-Dijkstra) and exports it as `.sgsnap`. | `--nodes india_nodes.json --spec p3_spec.json` (produced by `build_spec.py`). |

## Benchmark runners
Timed runs over a corpus. They report latency, so they need a quiet host to mean
anything; a number from a loaded machine is not a measurement.

| Program | What it does | Data it needs |
|---|---|---|
| `b3_runner` | Paper 5 B3 — loads N snapshots into one `GraphStore` and runs queries from a CSV (id,name,kg,category,hops,cypher). | `--snapshots a.sgsnap,b.sgsnap,... --queries <csv>`. |
| `hier_benchmark` | The HIER corpus (`benchmarks/hier/queries.json`) run twice — with the four hierarchy indexes and with none. Equal answers are the gate; the latency difference is the result (ADR-035). | Generated in-process; `--out` for results. |
| `hier_export_csv` | Dumps the HIER graph from the same `hier_common::build()` the benchmark runs against, as CSV, so another engine holds the identical graph. | Generated in-process; `--out`. |
| `itbench_substrate_bench` | PERF-19 — the agent turn budget (p95 <= 500 ms, p99 <= 2 s, nothing over 5 s) measured on the substrate study's query mix, which this file finally writes down. | Built-in query mix; `--json`. |
| `ldbc_http_serve` | Serves an LDBC SNB extract over the HTTP API so Samyama is measured across the same wire as the engines it is compared with (PERF-04), not in-process. | `--data-dir <ldbc extract>`. |
| `mesh_scale_bench` | Hierarchical roll-up over the real MeSH tree with a literature-shaped fact table — does the hierarchy index still pay on a real ontology at real volume? | Built-in / MeSH tree data. |
| `query_probe` | Times arbitrary Cypher against an LDBC extract, one line per query; each `--q "<label>=<cypher>"` is a separate variant. The general form of `is7_probe` and `ic11_probe`. | `--data-dir <ldbc extract>`. |
| `unified_benchmark` | 200+ queries over up to 9 KGs loaded into one graph, each by whichever of snapshot import or direct Rust loader is fastest for it. | Snapshots and data dirs for the 9 KGs; `--queries`. |

## Performance investigations
**Internal.** Each answers one question about one slow query, usually tied to a specific
issue number, and most were written to kill a hypothesis rather than to be run again.
They print timings and plans, not results you can use. Several need an LDBC SF1 or SF10
extract via `--data-dir`.

| Program | What it does | Data it needs |
|---|---|---|
| `bi11_ab` | A/B for the pinned-endpoint lookup in `EXISTS` on LDBC BI-11 (#1071 follow-up). | `--data-dir` (LDBC extract). |
| `bi11_explain` | EXPLAIN / PROFILE for BI-11 against a real SF1 extract — the plan is cost-based, so an empty store cannot answer it (#681). | `--data-dir` (LDBC SF1). |
| `bi17_intersect` | What BI-17 is worth if the closing hop of the friend triangle is an intersection (#1082). | `--data-dir` (LDBC extract). |
| `bi17_plan` | Prints the plan BI-17 gets, so the pushdown question has an answer rather than an assumption (#1078). | `--data-dir` (LDBC extract). |
| `bi17_profile` | Where BI-17's time actually goes: candidates considered against rows emitted, as a measured ratio (#1078). | `--data-dir` (LDBC extract). |
| `bi17_scaling` | Whether the 7.88x-faster BI-17 scales the way the old one did — checked at SF1 by restricting the pattern, without needing SF10. | `--data-dir` (LDBC SF1). |
| `bi17_width` | Does a wider record make an expand's rows more expensive? Tests the record-width hypothesis for the 2.5x gap between the first and second expand (#1078). | `--data-dir` (LDBC extract). |
| `bi_timeout_probe` | Why BI-11 and BI-17 do not finish inside the 120 s limit at SF10 — profiles SF1 and reads the shape of the growth (#1065). | `--data-dir` (LDBC SF1). |
| `cycle_close_pin` | Is the closing hop of a cycle pinned on every planner path? Only one of the three expand-building sites passed the bound variable in (#195). | Built-in. |
| `finbench_index_probe` | Does `MATCH (a:Account {id: N})` lower to an IndexScan once an index exists — or did the query just get a warm cache? | Built-in / FinBench data. |
| `fsync_cost` | What durability costs, now that the WAL and RocksDB actually sync (REL-03, #1309). | Built-in; `--json`. |
| `ic11_probe` | Where LDBC IC11's expand spends its time: walking the whole adjacency, or the plan feeding it (#665). | `--data-dir` (LDBC extract). |
| `ic1_anchor_probe` | Does IC1's time go into choosing the plan or walking the edges, at a KNOWS degree where three hops actually explode? | Built-in (generated degree ladder). |
| `ic1_distinct_multiplicity` | `RETURN DISTINCT` decides whether a var-length segment enumerates paths — re-measures the 505,660-row claim in #1054. | Built-in. |
| `ic1_index_regression` | Did indexing `Person.firstName` make IC1 slower? CH-REGRESS read 1.2x in the same window the index landed. | Built-in. |
| `ic1_inline_vs_where` | Does an inline property on the far side of a var-length pattern drop rows? Asks the inline form, the WHERE form and bare reachability. | Built-in. |
| `is7_forms` | IS7 as we run it (`EXISTS`) against the form the competitors are given (`OPTIONAL MATCH` + `IS NOT NULL`) (#725). | `--data-dir` (LDBC extract). |
| `is7_probe` | Where LDBC IS7's time goes — ~93% profiles into `Project`, so: the subquery, the property reads, or the plan feeding both (#618). | `--data-dir` (LDBC extract). |
| `ldbc_name_index_probe` | Do the LDBC `name`/`firstName` anchors lower to an IndexScan? Inline properties and deferred `WHERE` predicates are two different planner paths. | Built-in / LDBC extract. |
| `result_path_probe` | What the client path costs on top of the executor, per returned row — `client.query_readonly` and `QueryExecutor::execute` disagree by 3.3x on IS3 (#718). | `--data-dir` (LDBC extract). |
| `shortestpath_plan_probe` | Which operators `shortestPath` between two indexed anchors actually plans — bidirectional BFS did not move CR-3 at all, so the BFS was not the cost. | Built-in. |
| `varlen_emit_cost` | How much of a var-length expansion is walking and how much is emitting — measured by pointing the pattern at a label nothing carries. | Built-in. |

## Conformance and correctness checks
**Internal.** These answer a requirement or spec question by *running* the engine rather
than by reading its source, and print a verdict per case. Most exist because the claim
they check had previously been made from a grep, a `match` arm or a stale hand-maintained
list, and was wrong. They are run by CI and by `scripts/verify-sweep.sh`; a newcomer does
not need them.

| Program | What it does | Data it needs |
|---|---|---|
| `algo15_primitives` | ALGO-15 — are the four causal/temporal primitives shipped, and do they return the supporting paths rather than only a score? | Built-in; `--json`. |
| `algo_compose` | ALGO-04 — which in-query composition forms (`MATCH ... CALL algo.x(subgraph) YIELD ... WHERE ... RETURN`) actually work, run rather than listed. | Built-in; `--json`. |
| `algo_coverage` | ALGO-01 — how many algorithms are callable from Cypher, counted by execution. Counting `match` arms in the source is what put a false claim in ADR-037. | Built-in; `--json`. |
| `algo_parity_check` | ALGO-02 regression — re-runs the algorithms against NetworkX's *recorded* answers, so no Python is needed. Live parity is a separate check in the benchmarks repo. | Built-in recorded fixtures. |
| `algo_parity_export` | ALGO-02 — runs every shipped algorithm on reference graphs and emits the graphs *and* the answers as JSON, so both sides of a NetworkX comparison ran on the same edge list. | Built-in. |
| `api_contract` | API-01 — the OpenAPI document and the HTTP server describe the same API, as a per-commit gate. #613 found one phantom and five undocumented endpoints by hand, once. | Built-in; `--json`. |
| `api_surface_probe` | TRUST-06, API-07 — what the shipped HTTP surface actually returns, driven through `HttpServer::router()` because a grep cannot see a field added by a layer or a serde flatten. | Built-in. |
| `cypher_matrix_probe` | Executes one representative query per row of `docs/CYPHER_COMPATIBILITY.md`, so the matrix is measured rather than remembered (#437). Establishes support, not correctness. | Built-in; `--json`. |
| `cypher_probe` | Sweep 1 — standard Cypher scalar expressions with hand-computed answers, hunting constructs that parse, run and return the wrong thing without erroring (#571, #572). | Built-in. |
| `cypher_probe2` | Sweep 2 — mutations, path functions, temporal and aggregate edge cases. Found three silent wrong answers. | Built-in. |
| `cypher_probe3` | Sweep 3 — MERGE, FOREACH, UNION, OPTIONAL MATCH, CALL and subqueries: the clause-level surface. | Built-in. |
| `cypher_probe4` | Sweep 4 — type coercion, comparison and ordering across types, where the failure mode is a silently empty result that looks like a true negative. | Built-in. |
| `doc_check` | CH-DOC-EXEC (DX-04) — pulls every fenced `cypher` block out of the repo's markdown and parses it, so a documented query cannot rot unnoticed. | The repo's own markdown; `--json`. |
| `error_quality` | LANG-12 — do errors carry a machine-readable code and the offending span? Checked by provoking errors and inspecting them. | Built-in; `--json`. |
| `error_uniformity` | API-03 — does the same fault produce the same code, message and repair suggestion on the embedded, HTTP and RESP surfaces? Runs one fault corpus three ways in one process. | Built-in; `--json`. |
| `export_loss_report` | INT-06 — does a snapshot export say what it dropped for *this* graph, rather than carrying a standing disclaimer? | Built-in; `--json`. |
| `feature_matrix` | API-02 — capability x surface parity across the HTTP API, RESP, the Rust/Python/TypeScript SDKs and the MCP server, generated from source. | Built-in; `--out`. |
| `gds_aliases` | INT-04 — does a `gds.*` procedure name reach our implementation with matching semantics, which is a different question from whether dispatch resolves it? | Built-in; `--json`. |
| `import_invariants` | CH-IMPORT (LANG-15, REL-06) — the same graph built by Cypher, by snapshot restore and through the store API must be the same graph. The failure they share is partial agreement. | Built-in; `--json`. |
| `language_features` | LANG-09, LANG-11, LANG-13, LANG-16, NDS-11 — `LOAD CSV`, `ANALYZE`, index DDL and the rest that the TCK has no scenario for, so the suite that owned them could not see them. | Built-in; `--json`. |
| `nds_probes` | NDS-03, 03b, 04, 05, 06, 07, 08, 09, 10, 13 — which native data structures exist, asked of the query surface. Ten requirements that read `unmeasured` because nothing could run them. | Built-in; `--json`. |
| `optimization_determinism` | OPT-08 — is every solver deterministic given a seed? One stray `thread_rng()` makes the promise false while the API still looks right. | Built-in; `--json`. |
| `optimization_survey` | OPT-01, OPT-02, OPT-12, OPT-13 — what the optimizer ships, what a user can actually call (measured by calling, not by counting files), and how each solver does. | Built-in; `--json`. |
| `parallel_scaling` | ALGO-09 — how far each frontier-based algorithm scales with cores, against the >=0.6-efficiency-at-16-cores target whose whole baseline was 'Rayon, unmeasured'. | Built-in; `--json`. Needs a quiet multi-core host. |
| `plan_budget_probe` | PERF-05 — does the planner bound intermediate cardinality, and does `EXPLAIN` expose the budget? Asked of the engine, not of a grep for a field name. | Built-in. |
| `rdf_round_trip` | NDS-15 — does an RDF round trip actually move data? The old check grepped the function bodies for `TODO`, which a one-line call to a no-op passes. | Built-in; `--json`. |
| `schema_introspection_survey` | AI-05 — what a model can learn about a graph and in how many calls: six named things, a call count and whether the token budget is honoured. | Built-in; `--json`. |
| `shortestpath_differential` | Bidirectional `shortestPath` must agree with the one-sided walk on length and on reachability, even when several shortest paths are tied. | Built-in. |
| `shortestpath_target_index_correctness` | An indexed target scan must return the same rows as the scan it replaced, including where the index answers only part of the question. | Built-in. |
| `snapshot_portability` | HW-07, KG-11 — write a `.sgsnap` on one machine, load it on another, compare. `--write` then `--verify ... --against`. | Built-in; two machines for the real test. |
| `tck_runner` | The openCypher TCK, with `Scenario Outline:` blocks expanded against their `Examples:` rows (#756) — the measurement behind LANG-01's `CH-TCK >= 85%` gate. | The TCK corpus; `--json`. |
| `varlen_target_prop_differential` | Pruning a var-length target inside the operator must not change the answer: the inline form and the `WHERE` form take different paths over the same question (#1063). | Built-in. |

## Maintainer tools
**Internal.** Ad-hoc tools for whoever is maintaining the repository.

| Program | What it does | Data it needs |
|---|---|---|
| `issue_triage` | Runs the reproducer from each open issue and reports what the engine does now, against the expected answer taken from the issue itself. Triage, not a test suite. | Built-in reproducers. |
| `phase1b_smoke` | Phase 1b end-to-end smoke run: HGNC + Ensembl then CIViC into one in-process store, so the bridge indexes populate; reports counts and runs dedup queries. | The HGNC, Ensembl and CIViC source files. |
| `test_kg_queries` | Runs a CSV of KG queries (id,name,...,cypher) against an `EmbeddedClient` and times them. No header comment in the file; purpose read from the code. | A query CSV and a loaded KG. |

## Hardware client sketch
**Internal / reference.** Not a Cargo example; it does not build with `cargo run`.

| Program | What it does | Data it needs |
|---|---|---|
| `esp32/samyama_esp32.ino` | Writes to a Samyama Edge node from an ESP32 over RESP (API-15). RESP rather than HTTP because a TLS stack, a JSON encoder and a chunked reader are most of an ESP32's flash and all of its RAM headroom. Nothing here heap-allocates. | Arduino/ESP32 toolchain and a reachable Samyama RESP endpoint. |

## Terms used in the file headers

Several terms appear in these headers with no definition in this repository. Each one is
recorded below: what it refers to, and whether a definition exists here. Where no
definition could be established, that is said plainly rather than guessed at.

| Term | What it refers to | Defined in this repository? |
|---|---|---|
| **Paper 8** | A numbered research paper, external to this repository. Seven of its problems have a program here: 1 drug repurposing, 2 trial site selection, 3 supply chain, 4 healthcare allocation, 5 grid dispatch, 6 AMR stewardship, 7 wildfire evacuation. | **No.** The nearest gloss is `src/optimization/mod.rs:9` — "the primitive Paper 8 builds the 7 real-world problems on top of" — and `crates/samyama-optimization/src/benchmarks/mod.rs:3` — "Built for Paper 8 (graph-grounded optimization)". No list of papers exists here; `docs/README.md` points papers at `samyama-cloud/book/src/`. |
| **Paper 5 B3** | Benchmark "B3" from another numbered paper; `b3_runner.rs` is its Samyama-side runner. | **No.** What B3 measures is stated nowhere in this repository. `B3` also appears unexplained in `crates/samyama-gpu/PORT-STATUS.md:39`, `src/query/executor/planner.rs:5803` and `src/query/executor/operator.rs:10580`, alongside `CT##` query ids that are likewise undefined. |
| **SGE** | Used in the UC1–UC5 headers for the graph engine the optimizer's fitness function queries ("queries SGE for current beds"). | **No.** The acronym is never expanded anywhere in the repository, including `docs/GLOSSARY.md`. Context reads as the Samyama graph engine, but that is inference, not a definition found here. |
| **Requirement IDs** — `ALGO-01`, `ALGO-04`, `ALGO-09`, `ALGO-15`, `API-01`, `API-02`, `API-03`, `API-07`, `API-15`, `AI-05`, `DX-04`, `EVAL-10`, `HW-07`, `INT-02`, `INT-04`…`INT-11`, `KG-11`, `LANG-01`, `LANG-09`…`LANG-16`, `ML-09`, `NDS-03`…`NDS-15`, `OPT-01`, `OPT-02`, `OPT-08`, `OPT-12`, `OPT-13`, `PERF-04`, `PERF-05`, `PERF-19`, `REL-03`, `REL-06`, `TRUST-03`, `TRUST-06` | Identifiers from a product requirements specification, each with a baseline and an "H1" target. Headers cite it as "spec 01", "spec 06", "the spec's H1 gate". | **No.** There is no requirements table, no `docs/specs/`, no `docs/MANDATE.md` and no `SCORECARD.json` in this repository. The one pointer to the real location is `docs/CYPHER_COMPATIBILITY.md:66`, which links `samyama-cloud/docs/product/spec/18-conformance-harness.md` — so "spec 06" means file `06-*.md` of that numbered series in the private `samyama-cloud` repository. `docs/ADR/ADR-037-bolt-protocol-feasibility.md:59` similarly attributes `API-08` to `12-clients-apis-sdks-crates.md`. A few IDs carry a one-clause gloss at a use site (`README.md:602` for PERF-17, `docs/ADR/ADR-038-memory-allocator.md:95` for PERF-10), but there is no canonical table anywhere here. |
| **Check IDs** — `CH-TCK`, `CH-IMPORT`, `CH-DOC-EXEC`, `CH-ERR`, `CH-NDS-*`, `CH-OPT-BENCH`, `CH-REGRESS` | Named conformance suites that measure the requirements above. Several programs here *are* the adapter for one. | **No.** Defined with the spec, in `samyama-cloud`. |
| **The scorecard** | The single file recording where the product stands against all 254 requirements. | **Not here.** `docs/BENCHMARKS.md:49-54` says `SCORECARD.json` "lives with the cross-engine results", i.e. in the benchmarks repository. |
| **LDBC**, **SNB**, **IC1**…**IC14**, **IS1**…**IS7** | Linked Data Benchmark Council Social Network Benchmark, interactive read workload. `SF1` / `SF10` are scale factors. | **Yes.** `docs/BENCHMARKS.md:355` expands it and links ldbcouncil.org; per-query names are tabulated at `docs/BENCHMARKS.md:402-420` (e.g. "IS7 — Replies to Post", "IC1 — Transitive Friends by Name"). Not in `docs/GLOSSARY.md`. |
| **BI-11**, **BI-17**, **SNB-BI** | Queries of the LDBC SNB Business Intelligence workload. | **Partly.** The suite is expanded in `benches/ldbc_bi_benchmark.rs:1` and listed in `docs/BENCHMARKS.md:11`; individual BI query ids appear only as bare labels (`docs/ADR/ADR-038-memory-allocator.md:33`) and **BI-17 appears in no `.md` file at all**. What it asks is stated only in the headers of `bi17_*.rs`: count friend triangles, `MATCH (a:Person)-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(a) WHERE a.id < b.id < c.id`. |
| **CR-3**, **CR-8**, **CR-11** | Custom-read query ids used in performance headers. | **No.** Zero occurrences outside code comments. |
| **ADR-035**, **ADR-037**, **ADR-038** | Architecture Decision Records. | **Yes**, in `docs/ADR/`. |
| **OEH** | The index type `ontology_loader` declares over a hierarchy. | **No.** The acronym is not expanded here. |
| **GAK** | Generation-Augmented Knowledge — inverts RAG: the database notices a gap in its own knowledge, asks a model to fill it, writes the answer back. | **Yes**, in `agentic_enrichment_demo.rs`'s own header. |
| **HIER** | The hierarchy-heavy benchmark corpus at `benchmarks/hier/queries.json`. | **Yes**, the corpus is in this repository. |
