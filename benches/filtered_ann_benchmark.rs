//! In-process synthetic filtered-ANN sweep (#1985). See docs/benchmarks/filtered-ann.md.
#[path = "ann_common/mod.rs"]
mod ann_common;
#[path = "filtered_ann/mod.rs"]
mod filtered_ann;
use ann_common::{percentile, recall_at_k, synthetic, Vectors};
use filtered_ann::graph::{anchor, cypher_exact, cypher_post, edge_type, knn_graph, two_hop};
use filtered_ann::{oracle, predicate, Correlation};
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::index::Quantization;
use samyama::{
    graph::NodeId,
    vector::{DistanceMetric, HnswParams, VectorIndexManager},
};
use std::collections::{HashMap, HashSet};
use std::{io::Write, time::Instant};

fn positive(s: &str) -> Result<usize, String> {
    s.parse::<usize>()
        .ok()
        .filter(|x| *x > 0)
        .ok_or_else(|| format!("expected positive integer, got {s}"))
}
fn run() -> Result<(), String> {
    let (mut n, mut dim, mut nq) = (10_000, 32, 100);
    let mut efs = vec![32, 128, 512];
    let mut output = None;
    let mut graph_output = None;
    let mut args = std::env::args().skip(1).filter(|x| x != "--bench");
    while let Some(flag) = args.next() {
        let v = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--n" => n = positive(&v)?,
            "--dim" => dim = positive(&v)?,
            "--queries" => nq = positive(&v)?,
            "--ef" => efs = v.split(',').map(positive).collect::<Result<_, _>>()?,
            "--csv" => output = Some(v),
            "--graph-csv" => graph_output = Some(v),
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if n < 10_000 || n > u32::MAX as usize {
        return Err("--n must be 10000..=u32::MAX, so 0.1% contains k=10".into());
    }
    for ef in &efs {
        samyama::vector::index::check_ef(*ef)?;
    }
    if output.is_none() && graph_output.is_none() {
        return Err("--csv FILE is required (or --graph-csv FILE for the graph sweep)".into());
    }
    let base = synthetic(n, dim, 1985);
    let queries = synthetic(nq, dim, 1986);
    if let Some(path) = graph_output {
        graph_sweep(&base, &queries, &efs, &path)?;
    }
    let Some(path) = output else {
        return Ok(());
    };
    let manager = VectorIndexManager::new();
    manager
        .create_index_with_params(
            None,
            "V",
            "v",
            dim,
            DistanceMetric::L2,
            Quantization::None,
            HnswParams::default(),
        )
        .map_err(|e| e.to_string())?;
    for i in 0..n {
        manager
            .add_vector("V", "v", NodeId::new(i as u64), &base.row(i).to_vec())
            .map_err(|e| e.to_string())?;
    }
    let index = manager.get_index("V", "v").ok_or("missing index")?;
    let index = index.read().map_err(|e| e.to_string())?;
    let mut csv = std::io::BufWriter::new(std::fs::File::create(path).map_err(|e| e.to_string())?);
    writeln!(csv, "dataset,n,dim,queries,seed,selectivity,correlation,strategy,ef,k,recall,qps,p50_ms,p99_ms,short_results,filter_violations").map_err(|e| e.to_string())?;
    for permille in [1, 10, 100, 500] {
        let count = n * permille / 1000;
        for correlation in [
            Correlation::Positive,
            Correlation::None,
            Correlation::Negative,
        ] {
            let masks: Vec<_> = (0..nq)
                .map(|i| predicate(&base, queries.row(i), count, correlation, 1985 + i as u64))
                .collect();
            let truth: Vec<_> = (0..nq)
                .map(|i| oracle(&base, queries.row(i), &masks[i], 10))
                .collect();
            for strategy in ["auto", "exact", "post"] {
                for &ef in &efs {
                    let mut recall = 0.0;
                    let (mut short, mut violations) = (0, 0);
                    let mut times = Vec::new();
                    let started = Instant::now();
                    for qi in 0..nq {
                        let mask = &masks[qi];
                        let keep = |id: NodeId| mask[id.as_u64() as usize];
                        let at = Instant::now();
                        let got = match strategy {
                            "auto" => manager.search_filtered_ef(
                                "V",
                                "v",
                                queries.row(qi),
                                10,
                                Some(ef),
                                keep,
                            ),
                            "exact" => index.search_exact_where(queries.row(qi), 10, &keep),
                            _ => index
                                .search_ef(queries.row(qi), ef.max(10).min(n), Some(ef))
                                .map(|r| {
                                    r.into_iter().filter(|(id, _)| keep(*id)).take(10).collect()
                                }),
                        }
                        .map_err(|e| e.to_string())?;
                        times.push(at.elapsed());
                        let ids: Vec<u32> = got.iter().map(|(id, _)| id.as_u64() as u32).collect();
                        short += usize::from(ids.len() < 10);
                        violations += ids.iter().filter(|i| !mask[**i as usize]).count();
                        recall += recall_at_k(&ids, &truth[qi], 10);
                    }
                    let elapsed = started.elapsed().as_secs_f64();
                    times.sort();
                    writeln!(csv, "synthetic-l2,{n},{dim},{nq},1985,{},{},{strategy},{ef},10,{},{},{},{},{short},{violations}", count as f64/n as f64, correlation.name(), recall/nq as f64, nq as f64/elapsed, percentile(&times,50.0).as_secs_f64()*1000.0, percentile(&times,99.0).as_secs_f64()*1000.0).map_err(|e| e.to_string())?;
                }
            }
        }
    }
    csv.flush().map_err(|e| e.to_string())
}
/// Degrees of the k-NN graph. At the default n = 10,000 their two-hop reach
/// is roughly 0.1%, 1% and 5-10% of the corpus; the CSV records the mean
/// actually measured.
const DEGREES: [usize; 3] = [3, 10, 31];

fn write(store: &mut GraphStore, q: &str) -> Result<(), String> {
    let query = parse_query(q).map_err(|e| format!("{q}: {e:?}"))?;
    MutQueryExecutor::new(store, "default".into())
        .execute(&query)
        .map(|_| ())
        .map_err(|e| format!("{q}: {e:?}"))
}

/// The `id` column of a Cypher result, as corpus positions.
fn ids_of(store: &GraphStore, q: &str) -> Result<Vec<u32>, String> {
    let query = parse_query(q).map_err(|e| format!("{e:?}"))?;
    let batch = QueryExecutor::new(store)
        .execute(&query)
        .map_err(|e| format!("{e:?}"))?;
    batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => Ok(*i as u32),
            other => Err(format!("id column: {other:?}")),
        })
        .collect()
}

/// "The k nearest among the nodes within two hops of X" (#1985), answered
/// four ways against one in-process store:
///
/// - `traverse+auto`: walk two hops through the store's adjacency, then hand
///   the reach to the vector search as its filter. The in-process shape of
///   what a two-system pipeline does over the network (graph DB returns ids,
///   vector DB filters on them), with neither hop.
/// - `traverse+exact`: the same walk, then an exact scan of the reach.
/// - `cypher-post`: the statement a user writes today -- search, then MATCH
///   the anchor to the hits. The traversal filters the top k afterwards.
/// - `cypher-exact`: MATCH the reach, ORDER BY `vector.distance`, LIMIT k.
///
/// The traversal is inside the timed region for every strategy. Statement
/// text is parsed outside it; planning and execution are inside.
fn graph_sweep(base: &Vectors, queries: &Vectors, efs: &[usize], path: &str) -> Result<(), String> {
    let (n, dim, nq) = (base.len(), base.dim, queries.len());
    let max_degree = *DEGREES.iter().max().unwrap();
    let adj = knn_graph(base, max_degree);

    let mut store = GraphStore::new();
    write(&mut store, "CREATE INDEX FOR (n:V) ON (n.id)")?;
    store
        .create_vector_index("V", "v", dim, DistanceMetric::L2)
        .map_err(|e| e.to_string())?;
    let mut node_of = Vec::with_capacity(n);
    for i in 0..n {
        let mut props = HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("v".to_string(), PropertyValue::Vector(base.row(i).to_vec()));
        node_of.push(store.create_node_with_properties("default", vec![Label::new("V")], props));
    }
    let position: HashMap<NodeId, u32> = node_of
        .iter()
        .enumerate()
        .map(|(i, id)| (*id, i as u32))
        .collect();
    for degree in DEGREES {
        let t = edge_type(degree);
        for (i, list) in adj.iter().enumerate() {
            for &j in list.iter().take(degree) {
                store
                    .create_edge(node_of[i], node_of[j as usize], t.as_str())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    let index = store
        .vector_index
        .get_index("V", "v")
        .ok_or("missing index")?;

    let mut csv = std::io::BufWriter::new(std::fs::File::create(path).map_err(|e| e.to_string())?);
    writeln!(csv, "dataset,n,dim,queries,seed,degree,mean_reach,correlation,strategy,ef,k,recall,qps,p50_ms,p99_ms,short_results,filter_violations").map_err(|e| e.to_string())?;
    for degree in DEGREES {
        let t = edge_type(degree);
        for correlation in [
            Correlation::Positive,
            Correlation::None,
            Correlation::Negative,
        ] {
            let anchors: Vec<usize> = (0..nq)
                .map(|i| anchor(base, queries.row(i), correlation, 1985 + i as u64))
                .collect();
            let masks: Vec<Vec<bool>> = anchors.iter().map(|&x| two_hop(&adj, x, degree)).collect();
            let truth: Vec<Vec<u32>> = (0..nq)
                .map(|i| oracle(base, queries.row(i), &masks[i], 10))
                .collect();
            let reach: usize = masks.iter().map(|m| m.iter().filter(|b| **b).count()).sum();
            let mean_reach = reach as f64 / nq as f64 / n as f64;

            // Two hops through the store, as a set of node ids.
            let traverse = |x: usize| -> HashSet<NodeId> {
                let mut seen = HashSet::new();
                for (_, _, a, ty) in store.get_outgoing_edge_targets(node_of[x]) {
                    if ty.as_str() != t {
                        continue;
                    }
                    seen.insert(a);
                    for (_, _, b, ty) in store.get_outgoing_edge_targets(a) {
                        if ty.as_str() == t {
                            seen.insert(b);
                        }
                    }
                }
                seen
            };

            let mut runs: Vec<(&str, Option<usize>)> = Vec::new();
            for &ef in efs {
                runs.push(("traverse+auto", Some(ef)));
            }
            runs.push(("traverse+exact", None));
            runs.push(("cypher-post", None));
            runs.push(("cypher-exact", None));
            for (strategy, ef) in runs {
                let statements: Vec<String> = (0..nq)
                    .map(|qi| match strategy {
                        "cypher-post" => cypher_post(degree, anchors[qi], queries.row(qi), 10),
                        "cypher-exact" => cypher_exact(degree, anchors[qi], queries.row(qi), 10),
                        _ => String::new(),
                    })
                    .collect();
                let mut recall = 0.0;
                let (mut short, mut violations) = (0, 0);
                let mut times = Vec::new();
                let started = Instant::now();
                for qi in 0..nq {
                    let at = Instant::now();
                    let ids: Vec<u32> = match strategy {
                        "traverse+auto" | "traverse+exact" => {
                            let reached = traverse(anchors[qi]);
                            let keep = |id: NodeId| reached.contains(&id);
                            let got = if strategy == "traverse+auto" {
                                store.vector_index.search_filtered_ef(
                                    "V",
                                    "v",
                                    queries.row(qi),
                                    10,
                                    ef,
                                    keep,
                                )
                            } else {
                                index.read().map_err(|e| e.to_string())?.search_exact_where(
                                    queries.row(qi),
                                    10,
                                    &keep,
                                )
                            }
                            .map_err(|e| e.to_string())?;
                            got.iter().map(|(id, _)| position[id]).collect()
                        }
                        _ => ids_of(&store, &statements[qi])?,
                    };
                    times.push(at.elapsed());
                    short += usize::from(ids.len() < truth[qi].len());
                    violations += ids.iter().filter(|i| !masks[qi][**i as usize]).count();
                    recall += recall_at_k(&ids, &truth[qi], 10);
                }
                let elapsed = started.elapsed().as_secs_f64();
                times.sort();
                let ef = ef.map(|e| e.to_string()).unwrap_or_else(|| "-".into());
                writeln!(csv, "synthetic-l2-knn-graph,{n},{dim},{nq},1985,{degree},{mean_reach},{},{strategy},{ef},10,{},{},{},{},{short},{violations}", correlation.name(), recall/nq as f64, nq as f64/elapsed, percentile(&times,50.0).as_secs_f64()*1000.0, percentile(&times,99.0).as_secs_f64()*1000.0).map_err(|e| e.to_string())?;
            }
        }
    }
    csv.flush().map_err(|e| e.to_string())
}

fn main() {
    // `scripts/verify-sweep.sh` runs every `benches/*.rs` with no arguments,
    // so this is the normal way the nightly invokes this bench -- not a usage
    // mistake, and it must not fail the sweep. `ann_benchmark` answers the
    // same case with `SKIP:` and exit 0 (#2151), as do `ldbc_benchmark`,
    // `ldbc_bi_benchmark`, `ingest_profile`, `cold_start_share`, `plan_share`,
    // `persist_row_cost` and `result_cache_gain`. This one exited 2 instead,
    // which is what failed the nightly on 2026-11-14, the first sweep after
    // the bench reached `main`.
    //
    // Only the *complete absence* of arguments is a skip. A partial
    // invocation -- `--n 20000` with no `--csv` -- still fails with
    // "--csv FILE is required": the caller asked for a run and spelled it
    // wrong, and must be told so rather than handed a silent skip.
    let named: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    if named.is_empty() {
        eprintln!("SKIP: filtered_ann_benchmark needs an output path.");
        eprintln!("      Pass --csv FILE and/or --graph-csv FILE, optionally with --n, --dim,");
        eprintln!("      --queries and --ef.");
        eprintln!(
            "      Skipping rather than failing: the benchmark did not run, it did not break."
        );
        return;
    }
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(2);
    }
}
