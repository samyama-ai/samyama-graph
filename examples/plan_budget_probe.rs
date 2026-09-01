//! Does the planner bound intermediate cardinality? (PERF-05)
//!
//! PERF-05's H1 target is "budget + `EXPLAIN` exposure": a per-operator row
//! budget, and the ability to see it. This asks the engine directly rather
//! than grepping for a field name, because the interesting answer is not
//! "there is no `row_budget` symbol" but "nothing bounds the rows, and
//! `EXPLAIN` does not say how many it expects".
//!
//! The blowup query is deliberately small. A probe that actually explodes
//! memory to prove memory can explode is a probe that gets run once.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::QueryEngine;

fn fixture(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..n {
        let node = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", node, "i", PropertyValue::Integer(i as i64)).unwrap();
    }
    s
}

fn explain(engine: &QueryEngine, store: &GraphStore, q: &str) -> String {
    match engine.execute(&format!("EXPLAIN {q}"), store) {
        Ok(batch) => batch
            .records
            .first()
            .and_then(|r| r.get("plan"))
            .map(|v| format!("{v:?}"))
            .unwrap_or_default(),
        Err(e) => format!("<EXPLAIN failed: {e}>"),
    }
}

/// 400^3 = 64,000,000 intermediate rows, which crosses the shipped default
/// budget of 50,000,000. Deliberately sized against the *default* and not
/// against a budget this probe sets for itself: a guard that only works when
/// the test configures it is not a guard the shipped engine has.
const NODES: usize = 400;

fn main() {
    let store = fixture(NODES);
    let engine = QueryEngine::new();

    // A three-way cartesian product: NODES^3 intermediate rows. Nothing in the
    // query says "stop", so only the engine can.
    let blowup = "MATCH (a:N), (b:N), (c:N) RETURN count(*)";
    let plan = explain(&engine, &store, blowup);

    // Does EXPLAIN say how many rows it *expects*, per operator?
    //
    // Deliberately not a search for "rows": the plan now prints a row-budget
    // section that contains the word, and matching it would report row
    // estimates that do not exist -- a probe answering yes about a feature
    // nobody built. Only phrasings that can mean an estimate count, and the
    // budget section is cut out before looking.
    let without_budget = plan
        .to_lowercase()
        .split("--- row budget ---")
        .next()
        .unwrap_or_default()
        .to_string();
    let exposes_rows = ["estimated_rows", "est_rows", "est. rows", "cardinality", "estimated rows"]
        .iter()
        .any(|k| without_budget.contains(k));
    // Does it expose a budget, a spill, or a re-plan -- the three things
    // PERF-05 names as the response to an exploding intermediate?
    let exposes_budget = ["budget", "spill", "re-plan", "replan"]
        .iter()
        .any(|k| plan.to_lowercase().contains(k));
    debug_assert!(
        !exposes_rows || !without_budget.is_empty(),
        "row estimates must be found outside the budget section or not at all"
    );

    println!("PERF-05 explain_exposes_row_estimates={exposes_rows}");
    println!("PERF-05 explain_exposes_budget_or_spill={exposes_budget}");

    // And is the cardinality actually bounded at execution? A budget that
    // exists would have to refuse or degrade this; an unbounded planner just
    // runs it.
    let started = std::time::Instant::now();
    let outcome = match engine.execute(blowup, &store) {
        Ok(b) => {
            let n = b.records.first().and_then(|r| r.get("count(*)")).map(|v| format!("{v:?}"));
            format!("completed, count={}", n.unwrap_or_else(|| "?".into()))
        }
        Err(e) => {
            let msg = e.to_string();
            // A budget refusing the query is the *pass* case, and it must be
            // distinguishable from the query merely failing.
            let budgeted = ["budget", "too many rows", "cardinality", "exceeds"]
                .iter().any(|k| msg.to_lowercase().contains(k));
            format!("refused ({}) budget_related={budgeted}", msg.chars().take(90).collect::<String>())
        }
    };
    println!(
        "PERF-05 unbounded_product_of_{NODES}_nodes: {outcome} in {:?}",
        started.elapsed()
    );
    println!("PERF-05 plan_head: {}", plan.chars().take(160).collect::<String>().replace('\n', " | "));
}
