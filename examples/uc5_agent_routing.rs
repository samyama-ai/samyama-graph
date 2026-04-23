//! UC5 — Agentic Routing / Tool-Call Plan Optimisation via SGE + NSGA-II
//!
//! For a single question, search over tool-call plans — ordered sequences
//! of length 5 with per-slot parallel-with-previous flags — against three
//! objectives (accuracy, latency, token cost). Historical
//! (Question)-[:USED_TOOL]->(Tool) edges in SGE provide the cost
//! distributions; the fitness evaluator queries SGE for mean latency /
//! token-cost / hit-rate per selected tool and composes the plan metrics.
//!
//! Simplified vs. enterprise AGE: this example lives in OSS and uses a
//! synthetic question + 5 synthetic tools. The Cypher-driven fitness
//! pattern is the same one AGE will use once the write path is live.
//!
//! Decision vector (10-dim continuous, rounded at eval):
//! - x[2i]   ∈ [0, 5.999] — tool index for slot i (6 = "skip")
//! - x[2i+1] ∈ [0, 1]     — parallel-with-previous flag (>=0.5 = true)
//!
//! Hard constraint (penalty): no tool appears twice. Plans with any dup
//! carry 1e6 on every objective.
//!
//! Run:  cargo run --release --example uc5_agent_routing
//!
//! [[Use-Case 5 — Agentic Routing / Tool-Call Plan Optimisation]]

use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;

// ── Fixture ────────────────────────────────────────────────────────────

const QUESTION_ID: &str = "Q0";
/// Tool ids. Slot value 5 means "skip this slot".
const TOOLS: &[&str] = &["cypher", "vector_search", "web_search", "mcp_calc", "mcp_python"];
const SKIP: usize = 5;
const PLAN_LEN: usize = 5;

/// (tool, latency_ms, token_cost, hit_rate) — three historical runs per
/// tool for the single question, perturbed so avg() is non-degenerate.
const HISTORY: &[(&str, f64, f64, f64)] = &[
    // cypher: cheap, fast, high hit when question is structured
    ("cypher",         80.0,   40.0, 0.85),
    ("cypher",        100.0,   45.0, 0.80),
    ("cypher",         90.0,   42.0, 0.90),
    // vector_search: medium
    ("vector_search", 150.0,  120.0, 0.60),
    ("vector_search", 180.0,  130.0, 0.55),
    ("vector_search", 160.0,  115.0, 0.65),
    // web_search: slow, broad recall
    ("web_search",    800.0,  200.0, 0.55),
    ("web_search",    700.0,  210.0, 0.60),
    ("web_search",    900.0,  190.0, 0.50),
    // mcp_calc: fast, deterministic, narrow
    ("mcp_calc",       20.0,   10.0, 0.40),
    ("mcp_calc",       25.0,   12.0, 0.35),
    ("mcp_calc",       18.0,    9.0, 0.45),
    // mcp_python: flexible, expensive
    ("mcp_python",    400.0,  300.0, 0.75),
    ("mcp_python",    450.0,  320.0, 0.70),
    ("mcp_python",    380.0,  290.0, 0.80),
];

// ── Problem ────────────────────────────────────────────────────────────

struct RoutingProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
    call_count: std::sync::atomic::AtomicUsize,
}

impl MultiObjectiveProblem for RoutingProblem {
    fn dim(&self) -> usize { 2 * PLAN_LEN }
    fn num_objectives(&self) -> usize { 3 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        let mut lo = Array1::zeros(2 * PLAN_LEN);
        let mut hi = Array1::zeros(2 * PLAN_LEN);
        for i in 0..PLAN_LEN {
            lo[2 * i] = 0.0;
            hi[2 * i] = (TOOLS.len() + 1) as f64 - 1e-6;  // 0..5 (5 = skip)
            lo[2 * i + 1] = 0.0;
            hi[2 * i + 1] = 1.0;
        }
        (lo, hi)
    }

    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        self.call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Decode the plan.
        let mut plan: Vec<(usize, bool)> = Vec::with_capacity(PLAN_LEN);
        for i in 0..PLAN_LEN {
            let tool = (x[2 * i].floor() as usize).min(TOOLS.len());
            let parallel = x[2 * i + 1] >= 0.5;
            plan.push((tool, parallel));
        }

        // Duplicate tool → heavy penalty on every objective.
        let mut seen: Vec<usize> = Vec::new();
        let mut dup = false;
        for (t, _) in &plan {
            if *t == SKIP { continue; }
            if seen.contains(t) { dup = true; break; }
            seen.push(*t);
        }
        let active: Vec<&str> = seen.iter().map(|&i| TOOLS[i]).collect();
        if active.is_empty() {
            // No-op plan is dominated on accuracy.
            return vec![0.0, 0.0, 0.0];
        }

        let id_list = active.iter().map(|t| format!("\"{t}\""))
            .collect::<Vec<_>>().join(", ");
        let q = format!(
            "MATCH (:Question {{qid: \"{QUESTION_ID}\"}})-[r:USED_TOOL]->(t:Tool) \
             WHERE t.tid IN [{id_list}] \
             RETURN t.tid AS tid, avg(r.latency_ms) AS lat, \
                    avg(r.token_cost) AS tok, avg(r.hit_rate) AS acc"
        );
        let r = self.run_cypher(&q);

        let mut stats: HashMap<String, (f64, f64, f64)> = HashMap::new();
        for row in &r.records {
            let tid = row[0].as_str().unwrap_or("").to_string();
            let lat = row[1].as_f64().unwrap_or(0.0);
            let tok = row[2].as_f64().unwrap_or(0.0);
            let acc = row[3].as_f64().unwrap_or(0.0);
            stats.insert(tid, (lat, tok, acc));
        }

        // Compose plan-level metrics.
        // Latency: sequential chunks add; a slot flagged parallel overlaps
        // with the previous non-skip slot (take max of the group).
        let mut total_lat = 0.0_f64;
        let mut total_tok = 0.0_f64;
        let mut group_max = 0.0_f64;
        let mut first_in_group = true;
        // Accuracy = 1 - prod(1 - hit_rate_i).
        let mut miss_prob = 1.0_f64;

        for (t, par) in &plan {
            if *t == SKIP { continue; }
            let (lat, tok, acc) = stats.get(TOOLS[*t]).copied().unwrap_or((0.0, 0.0, 0.0));
            total_tok += tok;
            miss_prob *= 1.0 - acc.clamp(0.0, 1.0);
            if *par && !first_in_group {
                // Overlap with group.
                group_max = group_max.max(lat);
            } else {
                total_lat += group_max;
                group_max = lat;
                first_in_group = false;
            }
        }
        total_lat += group_max;

        let accuracy = 1.0 - miss_prob;
        let penalty = if dup { 1e6 } else { 0.0 };
        vec![-accuracy + penalty, total_lat + penalty, total_tok + penalty]
    }
}

impl RoutingProblem {
    fn run_cypher(&self, q: &str) -> samyama_sdk::QueryResult {
        let client = self.client.clone();
        let q_owned = q.to_string();
        self.handle
            .block_on(async move { client.query_readonly("default", &q_owned).await })
            .unwrap_or_else(|e| panic!("cypher: {e}\nquery: {q}"))
    }
}

// ── Driver ─────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("UC5 — Agentic Routing (Tool-Call Plan Optimisation) via SGE + NSGA-II");
    println!("=====================================================================\n");

    let client = Arc::new(EmbeddedClient::new());
    {
        let mut store = client.store_write().await;
        let q_node = store.create_node("Question");
        if let Some(node) = store.get_node_mut(q_node) {
            node.set_property("qid", QUESTION_ID);
            node.set_property("text", "How has the gene MTHFR been implicated in cardiovascular disease?");
        }
        let mut tool_id = HashMap::<&str, samyama::graph::NodeId>::new();
        for t in TOOLS {
            let nid = store.create_node("Tool");
            if let Some(node) = store.get_node_mut(nid) {
                node.set_property("tid", *t);
            }
            tool_id.insert(*t, nid);
        }
        for (t, lat, tok, acc) in HISTORY {
            let eid = store.create_edge(q_node, tool_id[t], "USED_TOOL").unwrap();
            store.set_edge_property(eid, "latency_ms", *lat).unwrap();
            store.set_edge_property(eid, "token_cost", *tok).unwrap();
            store.set_edge_property(eid, "hit_rate", *acc).unwrap();
        }
    }
    println!(
        "[load] 1 Question, {} Tool nodes, {} USED_TOOL edges",
        TOOLS.len(),
        HISTORY.len()
    );

    let problem = Arc::new(RoutingProblem {
        client: client.clone(),
        handle: Handle::current(),
        call_count: std::sync::atomic::AtomicUsize::new(0),
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 40,
        max_iterations: 30,
    });

    println!("\n[solve] NSGA-II pop=40 iter=30, dim={}, objectives=(-accuracy, latency, tokens)", 2 * PLAN_LEN);
    let p = problem.clone();
    let (front, calls, wall_ms) = tokio::task::spawn_blocking(move || {
        let t0 = std::time::Instant::now();
        let res = solver.solve(&*p);
        let calls = p.call_count.load(std::sync::atomic::Ordering::Relaxed);
        (res.pareto_front, calls, t0.elapsed().as_millis())
    }).await?;

    println!(
        "[done] {} cypher evaluations, wall {} ms ({:.2} ms/eval)",
        calls, wall_ms, wall_ms as f64 / calls.max(1) as f64
    );

    // Sort by accuracy descending.
    let mut rows: Vec<_> = front.iter().collect();
    rows.sort_by(|a, b| a.fitness[0].partial_cmp(&b.fitness[0]).unwrap());

    println!("\n[pareto] {} non-dominated plans:", rows.len());
    println!("  {:>8}  {:>8}  {:>8}   plan", "accuracy", "lat_ms", "tokens");
    for ind in rows.iter().take(12) {
        let mut plan_repr: Vec<String> = Vec::new();
        for i in 0..PLAN_LEN {
            let tool = (ind.variables[2 * i].floor() as usize).min(TOOLS.len());
            if tool == SKIP { continue; }
            let par = ind.variables[2 * i + 1] >= 0.5;
            let arrow = if plan_repr.is_empty() { "" } else if par { " ∥ " } else { " → " };
            plan_repr.push(format!("{arrow}{}", TOOLS[tool]));
        }
        let plan_str: String = plan_repr.join("");
        println!(
            "  {:>8.3}  {:>8.0}  {:>8.0}   {plan_str}",
            -ind.fitness[0], ind.fitness[1], ind.fitness[2]
        );
    }
    if rows.len() > 12 {
        println!("  ... ({} more)", rows.len() - 12);
    }

    // Sanity: no Pareto plan uses a duplicate tool.
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
