//! UC5-real — Tool-Call Plan Optimisation over real AGE telemetry
//!
//! Counterpart to `examples/uc5_agent_routing.rs` (synthetic fixture).
//! Here the `(Question)-[:USED_TOOL]->(Tool)` history graph is populated
//! organically by running the AGE plan executor against registered real
//! Cypher tools — no synthesised latency/hit-rate numbers.
//!
//! Pipeline:
//!   1. Load a small domain fixture (drug/gene/pathway) into a GraphStore
//!      so the tools have something non-trivial to query.
//!   2. Register 4 real CypherTool variants, each bound to a different
//!      fixed query (count / schema / sample / neighbour).
//!   3. Run the plan executor over 20 simulated prompts, each with a
//!      small random plan — produces ~60–80 USED_TOOL edges with real
//!      timings.
//!   4. Build a MultiObjectiveProblem that reads the same graph via
//!      Cypher (avg latency / token_cost / hit_rate per tool) and run
//!      NSGA-II pop=40 iter=30 on the 3 objectives.
//!
//! Run:  cargo run --release --example uc5_age_telemetry_real
//!
//! # Expected output (honest real-world result)
//!
//! With well-formed tools the Pareto collapses to the cheapest tool that
//! has hit_rate = 1.0 — a correct plan-pruning signal. In our fixture
//! `count_nodes` hits every time at ~1.5 ms / 10 tokens, dominating every
//! multi-tool plan on all three objectives. To see a non-trivial front
//! you need tools whose *aggregate* hit_rate is < 1.0 (i.e. each tool
//! fails on some questions); `rare_probe` contributes that variance
//! (hit_rate 0.5) but alone isn't enough. This is the interesting
//! research direction for UC5-production.
//!
//! [[SGE + Optimization — Phase 2 Results]]

use async_trait::async_trait;
use samyama::agent::{AgentRuntime, Tool, ToolCall, ToolPlan};
use samyama::agent::tools::CypherTool;
use samyama::graph::{GraphStore, Label};
use samyama::persistence::tenant::{AgentConfig, LLMProvider};
use samyama::query::QueryEngine;
use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

// ── Real Cypher tools ──────────────────────────────────────────────────
//
// One struct per fixed query template. All hit the real QueryEngine so
// latency is the actual execution time, not a synthesised constant.

struct StaticCypherTool {
    name: &'static str,
    description: &'static str,
    query: String,
    engine: Arc<QueryEngine>,
    store: Arc<RwLock<GraphStore>>,
}

#[async_trait]
impl Tool for StaticCypherTool {
    fn name(&self) -> &str { self.name }
    fn description(&self) -> &str { self.description }
    fn parameters(&self) -> Value { json!({"type": "object", "properties": {}}) }
    async fn execute(&self, _args: Value) -> samyama::agent::AgentResult<Value> {
        let store = self.store.read().await;
        let batch = self.engine.execute(&self.query, &*store)
            .map_err(|e| samyama::agent::AgentError::ToolError(format!("cypher: {e}")))?;
        // Wrap as `{records: [[...]]}` so the executor's hit-rate heuristic
        // (which looks for `records`) scores empty batches as 0.5.
        let records: Vec<Vec<Value>> = batch.records.iter()
            .map(|_| vec![json!("_")]) // rows exist, content not relevant here
            .collect();
        Ok(json!({ "columns": batch.columns, "records": records }))
    }
}

// ── Fixture ────────────────────────────────────────────────────────────

fn load_fixture(store: &mut GraphStore) {
    // Large enough that queries take measurable wall time. 4000 drugs,
    // 200 genes, 50 pathways with dense TARGETS / PART_OF edges — the
    // 4-hop drug_neighbours query will scan ~4000² candidate pairs and
    // genuinely differ in latency from a plain node count.
    const N_DRUGS: usize = 4_000;
    const N_GENES: usize = 200;
    const N_PATHWAYS: usize = 50;

    let mut drugs = Vec::with_capacity(N_DRUGS);
    for i in 0..N_DRUGS {
        let nid = store.create_node("Drug");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("name", format!("drug_{i:05}"));
        }
        drugs.push(nid);
    }
    let mut genes = Vec::with_capacity(N_GENES);
    for i in 0..N_GENES {
        let nid = store.create_node("Gene");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("gid", format!("G{i:04}"));
        }
        genes.push(nid);
    }
    let mut pathways = Vec::with_capacity(N_PATHWAYS);
    for i in 0..N_PATHWAYS {
        let nid = store.create_node("Pathway");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("pid", format!("P{i:03}"));
        }
        pathways.push(nid);
    }
    // Each drug targets 2 genes (LCG for determinism); each gene sits on 1 pathway.
    for (i, &d) in drugs.iter().enumerate() {
        let g1 = genes[(i * 7) % N_GENES];
        let g2 = genes[(i * 13 + 3) % N_GENES];
        store.create_edge(d, g1, "TARGETS").unwrap();
        store.create_edge(d, g2, "TARGETS").unwrap();
    }
    for (i, &g) in genes.iter().enumerate() {
        store.create_edge(g, pathways[i % N_PATHWAYS], "PART_OF").unwrap();
    }
}

// ── Problem: read telemetry from the live graph ────────────────────────

const PLAN_LEN: usize = 5;
const TOOLS: &[&str] = &["count_nodes", "list_labels", "sample_drugs", "drug_neighbours", "rare_probe"];
const SKIP: usize = 5;
const QUESTION_ID: &str = "uc5_plan_probe";

struct TelemetryProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
}

impl MultiObjectiveProblem for TelemetryProblem {
    fn dim(&self) -> usize { 2 * PLAN_LEN }
    fn num_objectives(&self) -> usize { 3 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        let mut lo = Array1::zeros(2 * PLAN_LEN);
        let mut hi = Array1::zeros(2 * PLAN_LEN);
        for i in 0..PLAN_LEN {
            hi[2 * i] = (TOOLS.len() + 1) as f64 - 1e-6;
            hi[2 * i + 1] = 1.0;
        }
        (lo, hi)
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let mut plan: Vec<(usize, bool)> = Vec::with_capacity(PLAN_LEN);
        for i in 0..PLAN_LEN {
            plan.push((
                (x[2 * i].floor() as usize).min(TOOLS.len()),
                x[2 * i + 1] >= 0.5,
            ));
        }
        let mut seen: Vec<usize> = Vec::new();
        let mut dup = false;
        for (t, _) in &plan {
            if *t == SKIP { continue; }
            if seen.contains(t) { dup = true; break; }
            seen.push(*t);
        }
        let active: Vec<&str> = seen.iter().map(|&i| TOOLS[i]).collect();
        if active.is_empty() { return vec![0.0, 0.0, 0.0]; }

        let id_list = active.iter().map(|t| format!("\"{t}\""))
            .collect::<Vec<_>>().join(", ");
        // Telemetry is written on any graph the executor was bound to; here
        // the same EmbeddedClient owns it.
        let q = format!(
            "MATCH (q:Question)-[r:USED_TOOL]->(t:Tool) \
             WHERE t.tid IN [{id_list}] \
             RETURN t.tid AS tid, avg(r.latency_ms) AS lat, \
                    avg(r.token_cost) AS tok, avg(r.hit_rate) AS acc"
        );
        let client = self.client.clone();
        let r = self.handle
            .block_on(async move { client.query_readonly("default", &q).await })
            .expect("telemetry cypher");

        let mut stats = HashMap::<String, (f64, f64, f64)>::new();
        for row in &r.records {
            stats.insert(
                row[0].as_str().unwrap_or("").to_string(),
                (row[1].as_f64().unwrap_or(0.0), row[2].as_f64().unwrap_or(0.0), row[3].as_f64().unwrap_or(0.0)),
            );
        }

        let mut total_lat = 0.0;
        let mut total_tok = 0.0;
        let mut group_max = 0.0;
        let mut first = true;
        let mut miss = 1.0;
        for (t, par) in &plan {
            if *t == SKIP { continue; }
            let (lat, tok, acc) = stats.get(TOOLS[*t]).copied().unwrap_or((0.0, 0.0, 0.0));
            total_tok += tok;
            miss *= 1.0 - acc.clamp(0.0, 1.0);
            if *par && !first {
                group_max = f64::max(group_max, lat);
            } else {
                total_lat += group_max;
                group_max = lat;
                first = false;
            }
        }
        total_lat += group_max;
        let accuracy = 1.0 - miss;
        let pen = if dup { 1e6 } else { 0.0 };
        vec![-accuracy + pen, total_lat + pen, total_tok + pen]
    }
}

// ── Telemetry generation ───────────────────────────────────────────────

fn random_plan(rng: &mut impl rand::Rng, n_tools: usize) -> ToolPlan {
    let mut picks: Vec<usize> = (0..n_tools).collect();
    // shuffle + take 2-4
    for i in (1..picks.len()).rev() {
        picks.swap(i, rng.gen_range(0..=i));
    }
    let take = rng.gen_range(2..=4).min(picks.len());
    let calls = picks.into_iter().take(take).enumerate().map(|(i, idx)| ToolCall {
        tool: TOOLS[idx].to_string(),
        args: json!({}),
        parallel_with_prev: i > 0 && rng.gen_bool(0.3),
    }).collect();
    ToolPlan { calls }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UC5-real — Tool-Call Plan Optimisation on live AGE telemetry");
    println!("=============================================================\n");

    // Single shared store — both the tools AND the telemetry writes land here,
    // so the optimizer queries the same physical graph the executor populated.
    let store = Arc::new(RwLock::new({
        let mut g = GraphStore::new();
        load_fixture(&mut g);
        g
    }));
    let engine = Arc::new(QueryEngine::new());

    let mut rt = AgentRuntime::new(AgentConfig {
        enabled: true,
        provider: LLMProvider::Mock,
        model: "mock".into(),
        api_key: None,
        api_base_url: None,
        system_prompt: None,
        tools: vec![],
        policies: std::collections::HashMap::new(),
    }).with_store(store.clone());

    // 4 real Cypher tools — different query templates → different real latencies.
    rt.register_tool(Arc::new(StaticCypherTool {
        name: "count_nodes",
        description: "Count all nodes",
        query: "MATCH (n) RETURN count(n) AS c".into(),
        engine: engine.clone(), store: store.clone(),
    }));
    rt.register_tool(Arc::new(StaticCypherTool {
        name: "list_labels",
        description: "Aggregate nodes by label",
        query: "MATCH (n) RETURN labels(n) AS l, count(n) AS c".into(),
        engine: engine.clone(), store: store.clone(),
    }));
    rt.register_tool(Arc::new(StaticCypherTool {
        name: "sample_drugs",
        description: "List drug names",
        query: "MATCH (d:Drug) RETURN d.name AS n".into(),
        engine: engine.clone(), store: store.clone(),
    }));
    rt.register_tool(Arc::new(StaticCypherTool {
        name: "drug_neighbours",
        description: "Drugs that share a pathway (4-hop scan — intentionally slow)",
        query: "MATCH (d1:Drug)-[:TARGETS]->(:Gene)-[:PART_OF]->(p:Pathway)\
                <-[:PART_OF]-(:Gene)<-[:TARGETS]-(d2:Drug) \
                WHERE d1 <> d2 RETURN d1.name AS a, d2.name AS b".into(),
        engine: engine.clone(), store: store.clone(),
    }));
    // Probes a drug name that doesn't exist — always returns empty, so the
    // executor's hit_rate heuristic scores it 0.5. Introduces real variance
    // into the accuracy objective without requiring mocked network calls.
    rt.register_tool(Arc::new(StaticCypherTool {
        name: "rare_probe",
        description: "Look up a specific rare drug by exact name",
        query: "MATCH (d:Drug {name: \"drug_99999\"}) RETURN d.name AS n".into(),
        engine: engine.clone(), store: store.clone(),
    }));

    // Also register CypherTool under "cypher" for completeness (unused in plans
    // but proves AGE can carry arbitrary tools alongside).
    rt.register_tool(Arc::new(CypherTool::new(engine.clone(), store.clone())));

    // Simulate 40 prompts, each with a random 2-4-step plan → real telemetry.
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let prompts = [
        "list drugs", "count things", "drug graph shape", "pathway overview",
        "sample of drugs", "how many genes", "drug neighbours for pathway",
        "nodes by label", "all drugs", "show graph structure",
    ];
    let mut total_edges = 0;
    for i in 0..40 {
        let plan = random_plan(&mut rng, TOOLS.len());
        let prompt = prompts[i % prompts.len()];
        // Vary prompt slightly so the qid doesn't always collide.
        let seeded_prompt = format!("{prompt} ({i})");
        let r = rt.execute_plan(&seeded_prompt, &plan).await?;
        total_edges += r.records.len();
    }
    println!("[telemetry] ran 40 plans, wrote {} USED_TOOL edges", total_edges);

    // Inspect telemetry before optimising.
    {
        let g = store.read().await;
        let q_nodes = g.get_nodes_by_label(&Label::new("Question")).len();
        let t_nodes = g.get_nodes_by_label(&Label::new("Tool")).len();
        println!("[telemetry] {q_nodes} Question nodes, {t_nodes} Tool nodes");
    }

    let client = Arc::new(EmbeddedClient::with_store(store.clone()));
    let probe_summary = client.query_readonly("default",
        "MATCH (q:Question)-[r:USED_TOOL]->(t:Tool) \
         RETURN t.tid AS tid, avg(r.latency_ms) AS lat, avg(r.token_cost) AS tok, \
                avg(r.hit_rate) AS acc, count(r) AS n").await?;
    println!("\n[per-tool real-world stats]");
    println!("  {:>16}  {:>8}  {:>8}  {:>8}  {:>4}", "tool", "lat_ms", "tokens", "hit", "n");
    for row in &probe_summary.records {
        println!("  {:>16}  {:>8.2}  {:>8.1}  {:>8.3}  {:>4}",
            row[0].as_str().unwrap_or(""),
            row[1].as_f64().unwrap_or(0.0),
            row[2].as_f64().unwrap_or(0.0),
            row[3].as_f64().unwrap_or(0.0),
            row[4].as_i64().unwrap_or(0));
    }

    // Run UC5's NSGA-II on the real telemetry.
    let problem = Arc::new(TelemetryProblem {
        client: client.clone(),
        handle: Handle::current(),
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 40,
        max_iterations: 30,
    });
    println!("\n[solve] NSGA-II pop=40 iter=30 on live telemetry");
    let p = problem.clone();
    let (front, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let res = solver.solve(&*p);
        (res.pareto_front, t0.elapsed().as_millis())
    }).await?;
    println!("[done] Pareto has {} plans, wall {wall_ms} ms", front.len());

    let mut rows: Vec<_> = front.iter().collect();
    rows.sort_by(|a, b| a.fitness[0].partial_cmp(&b.fitness[0]).unwrap());
    println!("\n[pareto] top 10 by accuracy:");
    println!("  {:>8}  {:>8}  {:>8}   plan", "accuracy", "lat_ms", "tokens");
    for ind in rows.iter().take(10) {
        let mut repr = Vec::new();
        for i in 0..PLAN_LEN {
            let t = (ind.variables[2 * i].floor() as usize).min(TOOLS.len());
            if t == SKIP { continue; }
            let par = ind.variables[2 * i + 1] >= 0.5;
            let arrow = if repr.is_empty() { "" } else if par { " ∥ " } else { " → " };
            repr.push(format!("{arrow}{}", TOOLS[t]));
        }
        println!("  {:>8.3}  {:>8.1}  {:>8.1}   {}",
            -ind.fitness[0], ind.fitness[1], ind.fitness[2], repr.join(""));
    }

    let any_dup = rows.iter().any(|ind| {
        let mut seen: Vec<usize> = Vec::new();
        for i in 0..PLAN_LEN {
            let t = (ind.variables[2 * i].floor() as usize).min(TOOLS.len());
            if t == SKIP { continue; }
            if seen.contains(&t) { return true; }
            seen.push(t);
        }
        false
    });
    println!("\n[check] Pareto plans with duplicate tools: {any_dup} (must be false)");
    Ok(())
}
