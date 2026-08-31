//! PERF-19 — the agent turn budget, measured on the substrate study's query mix.
//!
//! Spec 01 PERF-19 asks for p95 ≤ 500 ms, p99 ≤ 2 s and no query over 5 s,
//! **"measured on the substrate study's query mix"**. That mix had never been
//! written down, so PERF-19 was reported as `unmeasured` when it was in fact
//! unmeasurable — a distinction the H1 gate could not see, because a missing
//! measurement and a missing definition look identical from the outside.
//!
//! The mix is now frozen in
//! `samyama-cloud/docs/product/studies/PREREGISTRATION-agentic-substrate.md`:
//! fourteen MCP tools over the itbench-kg schema. Arm D gives the agent tools
//! rather than free-form Cypher, so the tools' queries *are* the mix.
//!
//! This runs them against the built scenario graphs, with **no model spend**.
//! A substrate that answers in 14 seconds cannot sit inside a 100-turn loop at
//! all, which is why spec 19 sequences the study behind this number rather
//! than beside it.
//!
//!     cargo run --release --example itbench_substrate_bench -- \
//!         --graph-dir ../itbench-kg/graph --json out.json
//!
//! The graph directory is read only. It is produced by a different repo's ETL
//! and this makes no claim to own it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::QueryEngine;

/// One tool in the arm-D surface.
///
/// `needs` records what a tool reads that the graph may not carry, so a tool
/// that cannot run is reported as *unrunnable for a named reason* rather than
/// as a fast query returning nothing. Zero rows in 0.1 ms is the most
/// flattering possible latency and the least informative one.
struct Tool {
    name: &'static str,
    group: &'static str,
    cypher: &'static str,
    needs: Option<&'static str>,
}

/// The fourteen, in the pre-registration's order and grouping.
const TOOLS: &[Tool] = &[
    // ---- Orientation: where the agent starts.
    Tool {
        name: "alerts_firing",
        group: "orientation",
        cypher: "MATCH (a:Alert)-[:FIRES_ON]->(e) \
                 RETURN a.name AS alert, a.severity AS severity, a.namespace AS ns, e.name AS entity",
        needs: None,
    },
    Tool {
        name: "unhealthy_pods",
        group: "orientation",
        cypher: "MATCH (p:Pod) WHERE p.phase <> \"Running\" \
                 RETURN p.name AS pod, p.namespace AS ns, p.phase AS phase, p.restarts AS restarts",
        needs: None,
    },
    Tool {
        name: "entity_neighbourhood",
        group: "orientation",
        cypher: "MATCH (p:Pod)-[r]-(n) RETURN p.name AS entity, type(r) AS rel, n.name AS other LIMIT 200",
        needs: None,
    },
    // ---- Topology: what an entity is part of.
    Tool {
        name: "owner_chain",
        group: "topology",
        cypher: "MATCH (d:Deployment)-[:OWNS*1..3]->(p:Pod) \
                 RETURN d.name AS owner, p.name AS pod, p.namespace AS ns",
        needs: None,
    },
    Tool {
        name: "pods_of_service",
        group: "topology",
        cypher: "MATCH (s:Service)-[:SELECTS]->(p:Pod) \
                 RETURN s.name AS service, collect(p.name) AS pods",
        needs: None,
    },
    Tool {
        name: "co_located_pods",
        group: "topology",
        // The noisy-neighbour hypothesis, and the reason RUNS_ON is in the
        // schema at all: two pods on one node share a failure domain that no
        // amount of walking CALLS will reveal.
        cypher: "MATCH (a:Pod)-[:RUNS_ON]->(n:Node)<-[:RUNS_ON]-(b:Pod) \
                 WHERE a.name <> b.name RETURN n.name AS node, count(b) AS neighbours",
        needs: None,
    },
    Tool {
        name: "config_consumers",
        group: "topology",
        // The single most important tool in the mix: ConfigMaps are the
        // largest class of gold root cause that the graph contains. MOUNTS is
        // specified in itbench-kg/SCHEMA.md and produces zero edges in all 40
        // scenarios, so this returns nothing today (itbench-kg#1).
        cypher: "MATCH (c:ConfigMap)<-[:MOUNTS]-(p) \
                 RETURN c.name AS config, c.namespace AS ns, collect(p.name) AS consumers",
        needs: Some("MOUNTS edges — itbench-kg#1: specified in SCHEMA.md, zero built"),
    },
    // ---- Dependency: what talks to what.
    Tool {
        name: "dependency_chain",
        group: "dependency",
        cypher: "MATCH (s:Service)<-[:CALLS*1..4]-(up:Service) \
                 RETURN s.name AS service, collect(DISTINCT up.name) AS upstream",
        needs: None,
    },
    Tool {
        name: "impact_analysis",
        group: "dependency",
        cypher: "MATCH (s:Service)-[:CALLS*1..4]->(down:Service) \
                 RETURN s.name AS service, collect(DISTINCT down.name) AS downstream",
        needs: None,
    },
    Tool {
        name: "criticality_ranking",
        group: "dependency",
        cypher: "CALL algo.pageRank({edgeType: \"CALLS\"}) YIELD node, score \
                 RETURN node.name AS entity, score ORDER BY score DESC LIMIT 20",
        needs: None,
    },
    // ---- Causal: the four ALGO-15 primitives, built in H1 for this study.
    //
    // All four are earliest-arrival over timestamped edges, because temporal
    // reachability is not transitive -- that is what distinguishes them from
    // walking CALLS. No node or edge in any of the 40 graphs carries a
    // timestamp (itbench-kg#3), so all four are unrunnable here. The data
    // exists in the snapshot (`activeAt` on every alert); the ETL drops it.
    Tool {
        name: "symptom_explanation",
        group: "causal",
        cypher: "CALL algo.symptomExplanation([{symptoms}], \
                 {edgeType: \"CALLS\", timeProperty: \"firstSeen\"}) \
                 YIELD node, explains, onset RETURN node.name AS cause, explains, onset LIMIT 10",
        needs: Some("edge timestamps — itbench-kg#3: no node or edge carries a time"),
    },
    Tool {
        name: "temporal_reachability",
        group: "causal",
        cypher: "CALL algo.temporalReachability({src}, \
                 {edgeType: \"CALLS\", timeProperty: \"firstSeen\"}) \
                 YIELD node, time RETURN node.name AS entity, time LIMIT 50",
        needs: Some("edge timestamps — itbench-kg#3"),
    },
    Tool {
        name: "temporal_shortest_path",
        group: "causal",
        // `path, times, arrival` -- not `node, time` like its three
        // neighbours. Guessing wrong costs a whole run: the procedure returns
        // *no rows* when there is no time-respecting route, so a wrong YIELD
        // name silently succeeds on exactly the scenarios where there is
        // nothing to return, and errors on the ones that found an answer. 37
        // of 40 scenarios failed and 3 passed, which reads like a flaky
        // procedure rather than a wrong query.
        cypher: "CALL algo.temporalShortestPath({src}, {dst}, \
                 {edgeType: \"CALLS\", timeProperty: \"firstSeen\"}) \
                 YIELD path, times, arrival RETURN path, times, arrival",
        needs: Some("edge timestamps — itbench-kg#3"),
    },
    Tool {
        name: "propagation_ranking",
        group: "causal",
        cypher: "CALL algo.propagationRanking({src}, \
                 {edgeType: \"CALLS\", timeProperty: \"firstSeen\"}) \
                 YIELD node, time, rank RETURN node.name AS entity, time, rank LIMIT 20",
        needs: Some("edge timestamps — itbench-kg#3"),
    },
];

/// Indexes for the properties the mix filters and joins on.
///
/// Without them the engine full-scans a label for every `WHERE x.prop = ...`,
/// which would make this a measurement of the scan rather than of the tool.
const INDEXES: &[&str] = &[
    "CREATE INDEX ON :Pod(name)",
    "CREATE INDEX ON :Pod(phase)",
    "CREATE INDEX ON :Service(name)",
    "CREATE INDEX ON :Deployment(name)",
    "CREATE INDEX ON :ConfigMap(name)",
    "CREATE INDEX ON :Alert(name)",
    "CREATE INDEX ON :Node(name)",
];

fn scalar(v: &serde_json::Value) -> Option<PropertyValue> {
    match v {
        serde_json::Value::String(s) => Some(PropertyValue::String(s.clone())),
        serde_json::Value::Bool(b) => Some(PropertyValue::Boolean(*b)),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(PropertyValue::Integer)
            .or_else(|| n.as_f64().map(PropertyValue::Float)),
        _ => None,
    }
}

struct Loaded {
    store: GraphStore,
    nodes: usize,
    edges: usize,
    /// Edges that named an endpoint the node file does not contain. Reported
    /// rather than ignored: a dangling edge is a hole in the substrate, and
    /// silently dropping it would make the graph look complete.
    dangling: usize,
    edge_types: HashMap<String, usize>,
    /// The entity an alert fires on, and the alerting set.
    ///
    /// The causal primitives take a *source* -- they answer "what could this
    /// have reached, in time", which has no meaning without a starting point.
    /// An agent would start from a symptom, so the bench does: the target of
    /// the first `FIRES_ON` edge in sorted order, and up to five of them for
    /// `symptomExplanation`. Sorted, so the same scenario picks the same
    /// anchor on every run and a latency change is the engine's.
    anchor: Option<NodeId>,
    /// A second alerting entity, for the one primitive that takes a target.
    /// "Did this symptom propagate to that one, and by what route" is the
    /// question an agent asks with two symptoms in hand.
    second: Option<NodeId>,
    symptoms: Vec<NodeId>,
}

fn load(dir: &Path) -> std::io::Result<Loaded> {
    let mut store = GraphStore::new();
    let mut by_id: HashMap<String, NodeId> = HashMap::new();
    let mut nodes = 0usize;

    for line in std::fs::read_to_string(dir.join("nodes.jsonl"))?.lines() {
        let Ok(v): Result<serde_json::Value, _> = serde_json::from_str(line) else { continue };
        let (Some(id), Some(label)) = (v["id"].as_str(), v["label"].as_str()) else { continue };
        let nid = store.create_node_with_labels(vec![Label::new(label)]);
        for (k, val) in v.as_object().into_iter().flatten() {
            if k == "id" || k == "label" {
                continue;
            }
            // Nested maps (`labels`, `selector`, `data`) are flattened away
            // rather than stored: none of the fourteen tools reads them, and a
            // property nobody queries is load time spent to no purpose.
            if let Some(pv) = scalar(val) {
                store.set_column_property(nid, k, pv);
            }
        }
        by_id.insert(id.to_string(), nid);
        nodes += 1;
    }

    let mut edges = 0usize;
    let mut dangling = 0usize;
    let mut edge_types: HashMap<String, usize> = HashMap::new();
    let mut fires_on: Vec<(String, NodeId)> = Vec::new();
    for line in std::fs::read_to_string(dir.join("edges.jsonl"))?.lines() {
        let Ok(v): Result<serde_json::Value, _> = serde_json::from_str(line) else { continue };
        let (Some(s), Some(t), Some(ty)) =
            (v["src"].as_str(), v["dst"].as_str(), v["type"].as_str())
        else {
            continue;
        };
        match (by_id.get(s), by_id.get(t)) {
            (Some(&a), Some(&b)) => {
                if store.create_edge(a, b, ty).is_ok() {
                    edges += 1;
                    *edge_types.entry(ty.to_string()).or_insert(0) += 1;
                    if ty == "FIRES_ON" {
                        fires_on.push((s.to_string(), b));
                    }
                }
            }
            _ => dangling += 1,
        }
    }

    let idx = QueryEngine::new();
    for stmt in INDEXES {
        let _ = idx.execute_mut(stmt, &mut store, "default");
    }

    fires_on.sort();
    let anchor = fires_on.first().map(|(_, n)| *n);
    let second = fires_on.iter().map(|(_, n)| *n).find(|n| Some(*n) != anchor);
    let symptoms: Vec<NodeId> = fires_on.iter().take(5).map(|(_, n)| *n).collect();

    Ok(Loaded { store, nodes, edges, dangling, edge_types, anchor, second, symptoms })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    // Nearest-rank. Stated because "the p95" is three different numbers
    // depending on the convention, and a budget compared against the wrong one
    // is a budget nobody can reproduce.
    let rank = (p / 100.0 * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |name: &str| -> Option<String> {
        args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
    };
    let graph_dir = PathBuf::from(
        arg("--graph-dir").unwrap_or_else(|| "../itbench-kg/graph".to_string()),
    );
    let runs: usize = arg("--runs").and_then(|s| s.parse().ok()).unwrap_or(5);
    let limit: usize = arg("--scenarios").and_then(|s| s.parse().ok()).unwrap_or(usize::MAX);

    let mut scenarios: Vec<PathBuf> = match std::fs::read_dir(&graph_dir) {
        Ok(rd) => rd
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.join("nodes.jsonl").exists() && p.join("edges.jsonl").exists())
            .collect(),
        Err(e) => {
            eprintln!("cannot read {}: {e}", graph_dir.display());
            eprintln!("expected itbench-kg/graph with Scenario-*/{{nodes,edges}}.jsonl");
            std::process::exit(66);
        }
    };
    scenarios.sort();
    scenarios.truncate(limit);
    if scenarios.is_empty() {
        eprintln!("no scenario graphs under {}", graph_dir.display());
        std::process::exit(66);
    }

    let engine = QueryEngine::new();
    let mut samples: Vec<f64> = Vec::new();
    let mut per_tool: HashMap<&str, Vec<f64>> = HashMap::new();
    let mut errors: Vec<String> = Vec::new();
    let mut empty: HashMap<&str, usize> = HashMap::new();
    let mut no_anchor: HashMap<&str, usize> = HashMap::new();
    let mut totals = (0usize, 0usize, 0usize);
    let mut edge_types: HashMap<String, usize> = HashMap::new();

    for dir in &scenarios {
        let name = dir.file_name().and_then(|s| s.to_str()).unwrap_or("?").to_string();
        let loaded = match load(dir) {
            Ok(l) => l,
            Err(e) => {
                errors.push(format!("{name}: load failed: {e}"));
                continue;
            }
        };
        totals.0 += loaded.nodes;
        totals.1 += loaded.edges;
        totals.2 += loaded.dangling;
        for (k, v) in loaded.edge_types {
            *edge_types.entry(k).or_insert(0) += v;
        }

        // `symptomExplanation` takes [[id, seenAt], ...]. With no timestamp in
        // the graph every `seenAt` is 0, which is the honest encoding of "the
        // data does not say when" -- and makes every symptom simultaneous, so
        // the primitive cannot order cause before effect. That is the point of
        // itbench-kg#3 and is why the tool is marked blocked even though the
        // call now has the right shape.
        let symptoms_lit = loaded
            .symptoms
            .iter()
            // `.as_u64()`, not the `Display` impl: `NodeId` renders as
            // "NodeId(123)", which the parser rejects with "requires a source
            // as argument 1 (a node id)" -- a message that reads exactly like
            // the engine not supporting the call. It was substituted wrong.
            .map(|n| format!("[{}, 0]", n.as_u64()))
            .collect::<Vec<_>>()
            .join(", ");

        for tool in TOOLS {
            let Some(cypher) = ({
                let mut c = tool.cypher.to_string();
                if c.contains("{src}") {
                    match loaded.anchor {
                        Some(a) => c = c.replace("{src}", &a.as_u64().to_string()),
                        None => c.clear(),
                    }
                }
                if c.contains("{dst}") {
                    match loaded.second {
                        Some(d) => c = c.replace("{dst}", &d.as_u64().to_string()),
                        None => c.clear(),
                    }
                }
                if c.contains("{symptoms}") {
                    if symptoms_lit.is_empty() {
                        c.clear();
                    } else {
                        c = c.replace("{symptoms}", &symptoms_lit);
                    }
                }
                if c.is_empty() { None } else { Some(c) }
            }) else {
                // No alert in this scenario, so no symptom to start from. Not
                // an error and not a zero-latency success: recorded as a
                // scenario the tool has nothing to run on.
                *no_anchor.entry(tool.name).or_insert(0) += 1;
                continue;
            };
            let cypher = cypher.as_str();
            // One warm-up, discarded: the first execution of a query pays for
            // parse and plan, and an agent's second turn does not.
            let _ = engine.execute(cypher, &loaded.store);
            for _ in 0..runs {
                let t0 = Instant::now();
                match engine.execute(cypher, &loaded.store) {
                    Ok(batch) => {
                        let ms = t0.elapsed().as_secs_f64() * 1000.0;
                        samples.push(ms);
                        per_tool.entry(tool.name).or_default().push(ms);
                        if batch.records.is_empty() {
                            *empty.entry(tool.name).or_insert(0) += 1;
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string().lines().next().unwrap_or("error").to_string();
                        errors.push(format!("{name}/{}: {msg}", tool.name));
                        break;
                    }
                }
            }
        }
    }

    let mut sorted = samples.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p95 = percentile(&sorted, 95.0);
    let p99 = percentile(&sorted, 99.0);
    let max = sorted.last().copied().unwrap_or(f64::NAN);

    let tools_json: Vec<serde_json::Value> = TOOLS
        .iter()
        .map(|t| {
            let mut v = per_tool.get(t.name).cloned().unwrap_or_default();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let n = v.len();
            serde_json::json!({
                "tool": t.name,
                "group": t.group,
                "samples": n,
                "median_ms": if n > 0 { v[n / 2] } else { f64::NAN },
                "p95_ms": percentile(&v, 95.0),
                "max_ms": v.last().copied().unwrap_or(f64::NAN),
                // A tool that returns nothing on every scenario is not a fast
                // tool. It is reported beside its latency so the two are never
                // read apart.
                "empty_on_all_scenarios": empty.get(t.name).copied().unwrap_or(0) >= n && n > 0,
                "scenarios_without_an_anchor": no_anchor.get(t.name).copied().unwrap_or(0),
                "blocked_by": t.needs,
            })
        })
        .collect();

    let out = serde_json::json!({
        "scenarios": scenarios.len(),
        "runs_per_query": runs,
        "tools": TOOLS.len(),
        "tools_blocked": TOOLS.iter().filter(|t| t.needs.is_some()).count(),
        "nodes_loaded": totals.0,
        "edges_loaded": totals.1,
        "dangling_edges": totals.2,
        "edge_types": edge_types,
        "samples": samples.len(),
        "p95_ms": p95,
        "p99_ms": p99,
        "max_ms": max,
        "budget": {"p95_ms": 500.0, "p99_ms": 2000.0, "max_ms": 5000.0},
        "within_budget": p95 <= 500.0 && p99 <= 2000.0 && max <= 5000.0,
        "per_tool": tools_json,
        "errors": errors,
    });

    let text = serde_json::to_string_pretty(&out).unwrap();
    match arg("--json") {
        Some(p) => std::fs::write(p, &text).unwrap(),
        None => println!("{text}"),
    }
    eprintln!(
        "PERF-19 over {} scenarios x {} tools: p95 {:.1} ms, p99 {:.1} ms, max {:.1} ms \
         (budget 500 / 2000 / 5000) -- {}",
        scenarios.len(),
        TOOLS.len(),
        p95,
        p99,
        max,
        if out["within_budget"].as_bool().unwrap_or(false) { "within" } else { "OVER" }
    );
}
