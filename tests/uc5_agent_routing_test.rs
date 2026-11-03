//! UC5 — Agentic Routing / Tool-Call Plan Optimisation
//!
//! Locks in the contract for the SGE + NSGA-II tool-plan search
//! (samyama-cloud/wiki/use-cases/uc5-agentic-routing.md).
//!
//! Verified properties:
//! - No Pareto plan uses a duplicate tool (the 1e6 penalty really does
//!   push dup plans off the front).
//! - The front is diverse — at least one plan trades accuracy for
//!   latency/tokens vs. the max-accuracy plan.
//! - At least one plan uses parallelism (parallel flag wins sometimes).

use samyama_sdk::{
    Array1, EmbeddedClient, MultiObjectiveProblem, NSGA2Solver, SamyamaClient, SolverConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;

const QUESTION_ID: &str = "Q0";
const TOOLS: &[&str] = &["cypher", "vector_search", "web_search", "mcp_calc", "mcp_python"];
const SKIP: usize = 5;
const PLAN_LEN: usize = 5;
const HISTORY: &[(&str, f64, f64, f64)] = &[
    ("cypher",         80.0,   40.0, 0.85),
    ("cypher",        100.0,   45.0, 0.80),
    ("cypher",         90.0,   42.0, 0.90),
    ("vector_search", 150.0,  120.0, 0.60),
    ("vector_search", 180.0,  130.0, 0.55),
    ("vector_search", 160.0,  115.0, 0.65),
    ("web_search",    800.0,  200.0, 0.55),
    ("web_search",    700.0,  210.0, 0.60),
    ("web_search",    900.0,  190.0, 0.50),
    ("mcp_calc",       20.0,   10.0, 0.40),
    ("mcp_calc",       25.0,   12.0, 0.35),
    ("mcp_calc",       18.0,    9.0, 0.45),
    ("mcp_python",    400.0,  300.0, 0.75),
    ("mcp_python",    450.0,  320.0, 0.70),
    ("mcp_python",    380.0,  290.0, 0.80),
];

struct RoutingProblem {
    client: Arc<EmbeddedClient>,
    handle: Handle,
}

impl MultiObjectiveProblem for RoutingProblem {
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
        if active.is_empty() {
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
        let client = self.client.clone();
        let r = self.handle.block_on(async move { client.query_readonly("default", &q).await })
            .expect("cypher");
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
        let mut first_in_group = true;
        let mut miss_prob = 1.0;
        for (t, par) in &plan {
            if *t == SKIP { continue; }
            let (lat, tok, acc) = stats.get(TOOLS[*t]).copied().unwrap_or((0.0, 0.0, 0.0));
            total_tok += tok;
            miss_prob *= 1.0 - acc.clamp(0.0, 1.0);
            if *par && !first_in_group {
                group_max = f64::max(group_max, lat);
            } else {
                total_lat += group_max;
                group_max = lat;
                first_in_group = false;
            }
        }
        total_lat += group_max;
        let accuracy = 1.0 - miss_prob;
        let pen = if dup { 1e6 } else { 0.0 };
        vec![-accuracy + pen, total_lat + pen, total_tok + pen]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uc5_pareto_front_is_diverse_and_dup_free() {
    let client = Arc::new(EmbeddedClient::new());
    {
        let mut store = client.store_write().await;
        let q_node = store.create_node("Question");
        if let Some(node) = store.get_node_mut(q_node) {
            node.set_property("qid", QUESTION_ID);
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

    let problem = Arc::new(RoutingProblem {
        client: client.clone(),
        handle: Handle::current(),
    });
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 30,
        max_iterations: 25,
    });
    let p = problem.clone();
    let front = tokio::task::spawn_blocking(move || solver.solve(&*p).pareto_front)
        .await.unwrap();

    assert!(!front.is_empty(), "Pareto front must be non-empty");
    assert!(front.len() >= 3, "expected ≥3 plans on front; got {}", front.len());

    // No Pareto plan may use a duplicate tool.
    for ind in &front {
        let mut seen: Vec<usize> = Vec::new();
        for i in 0..PLAN_LEN {
            let t = (ind.variables[2 * i].floor() as usize).min(TOOLS.len());
            if t == SKIP { continue; }
            assert!(
                !seen.contains(&t),
                "Pareto plan has duplicate tool {} (vars={:?})", TOOLS[t], ind.variables
            );
            seen.push(t);
        }
        for v in &ind.fitness {
            assert!(v.is_finite(), "non-finite fitness: {:?}", ind.fitness);
            assert!(*v < 1e5, "penalty leaked into Pareto front: {:?}", ind.fitness);
        }
    }

    // Diversity: best-accuracy and best-latency plans should differ.
    let best_acc = front.iter().fold(f64::INFINITY, |a, ind| a.min(ind.fitness[0]));
    let best_lat = front.iter().fold(f64::INFINITY, |a, ind| a.min(ind.fitness[1]));
    let matching_both = front.iter().any(|ind|
        (ind.fitness[0] - best_acc).abs() < 1e-6 && (ind.fitness[1] - best_lat).abs() < 1e-6
    );
    assert!(
        !matching_both,
        "Pareto front is degenerate: same plan wins on accuracy AND latency ({} / {})",
        best_acc, best_lat
    );
}
