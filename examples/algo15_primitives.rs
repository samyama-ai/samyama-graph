//! ALGO-15: are the four causal primitives shipped, and do they return the
//! supporting paths?
//!
//! The requirement is one sentence with two halves:
//!
//! > **Causal/temporal primitives** — time-respecting reachability, temporal
//! > shortest path, propagation ranking, symptom explanation; **each returning
//! > the supporting paths, not just a score**
//!
//! The first half is a count and was effectively known: all four dispatch from
//! Cypher. The second half is the substantive one, it was never measured, and
//! it is where the gap is — a ranking that says "this node scores 0.8" and
//! cannot say *why* is the thing an operator cannot act on, which is the whole
//! reason the clause is in the requirement.
//!
//! Measured by calling each primitive on a graph whose edges carry times and
//! inspecting the columns it binds. A column is evidence of a path when it
//! carries a sequence of nodes; a score, a timestamp or a count is not,
//! however useful it is on its own.
//!
//!     cargo run --release --example algo15_primitives -- --json out.json

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// A five-service incident with times on every call.
///
/// Timestamps are the point: without them the primitives degenerate to a plain
/// walk and every one of them looks like it works. The ITBench graphs have no
/// timestamps at all, which is why the substrate suite cannot answer this
/// question and this probe builds its own graph rather than borrowing one.
fn incident() -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for name in ["api", "db", "cache", "auth", "ui"] {
        let n = s.create_node_with_labels([Label::new("Svc")]);
        s.set_node_property("default", n, "name", PropertyValue::String(name.into())).unwrap();
        ids.push(n);
    }
    for (a, b, t) in [(0usize, 1usize, 10i64), (1, 2, 5), (0, 3, 12), (3, 4, 20)] {
        let e = s.create_edge(ids[a], ids[b], "CALLS").unwrap();
        s.set_edge_property(e, "at", PropertyValue::Integer(t)).unwrap();
    }
    (s, ids)
}

/// Does this value carry a sequence of nodes — a path — rather than a scalar?
fn is_path(v: &Value) -> bool {
    match v {
        // A list of node ids or of nodes. The engine returns paths as an array
        // of integers today; accepting either shape means this keeps measuring
        // the requirement if that representation changes.
        Value::Property(PropertyValue::Array(a)) => a.len() >= 2
            && a.iter().all(|x| matches!(x, PropertyValue::Integer(_))),
        Value::List(a) => a.len() >= 2,
        _ => false,
    }
}

fn main() {
    let (store, ids) = incident();
    let a = ids[0].as_u64();
    // `ui`, not `cache`. The fixture's `cache` is reachable from `api` only by
    // ignoring time -- 0->1 fires at t=10 and 1->2 at t=5, so the second edge
    // has already fired. Asking for a path there correctly returns no rows,
    // and a probe that asked it would have measured an empty result and
    // reported the primitive as returning no supporting path. A probe needs
    // data its question can actually be answered from.
    let b = ids[4].as_u64();
    // A symptom pair that is genuinely explainable, for the same reason.
    let c = ids[3].as_u64();
    let cfg = "{edgeType: \"CALLS\", timeProperty: \"at\"}";

    // Every primitive, called with the YIELD clause it actually binds. Guessing
    // wrong here would report a shipped primitive as missing, and a wrong YIELD
    // name fails only when the query *succeeds* -- no rows means nothing reads
    // the binding.
    let cases: Vec<(&str, String)> = vec![
        ("temporalReachability",
         format!("CALL algo.temporalReachability({a}, {cfg}) YIELD node, time \
                  RETURN node, time")),
        ("temporalShortestPath",
         format!("CALL algo.temporalShortestPath({a}, {b}, {cfg}) \
                  YIELD path, times, arrival RETURN path, times, arrival")),
        ("propagationRanking",
         format!("CALL algo.propagationRanking({a}, {cfg}) YIELD node, time, rank \
                  RETURN node, time, rank")),
        ("symptomExplanation",
         format!("CALL algo.symptomExplanation([[{b}, 30], [{c}, 30]], {cfg}) \
                  YIELD node, explains, onset RETURN node, explains, onset")),
    ];

    let mut rows = Vec::new();
    for (name, cypher) in &cases {
        let (dispatches, rows_returned, columns, path_col, err) = match parse_query(cypher) {
            Err(e) => (false, 0usize, Vec::new(), None, Some(format!("{e}"))),
            Ok(q) => match QueryExecutor::new(&store).execute(&q) {
                Err(e) => (false, 0, Vec::new(), None, Some(format!("{e}"))),
                Ok(batch) => {
                    let cols: Vec<String> = batch.columns.clone();
                    // The first column whose value is a sequence of nodes.
                    let path_col = batch.records.first().and_then(|r| {
                        cols.iter().find(|c| r.get(c).is_some_and(is_path)).cloned()
                    });
                    (true, batch.records.len(), cols, path_col, None)
                }
            },
        };
        rows.push(serde_json::json!({
            "primitive": name,
            "dispatches": dispatches,
            "rows": rows_returned,
            "columns": columns,
            "supporting_path_column": path_col,
            "error": err,
        }));
    }

    let shipped = rows.iter().filter(|r| r["dispatches"] == true).count();
    let with_paths = rows.iter().filter(|r| !r["supporting_path_column"].is_null()).count();

    let json = serde_json::json!({
        "primitives": cases.len(),
        "shipped": shipped,
        "returning_supporting_paths": with_paths,
        "detail": rows,
    });
    let args: Vec<String> = std::env::args().collect();
    let text = serde_json::to_string_pretty(&json).unwrap();
    match args.iter().position(|x| x == "--json").and_then(|i| args.get(i + 1)) {
        Some(p) => std::fs::write(p, &text).unwrap(),
        None => println!("{text}"),
    }
    eprintln!(
        "ALGO-15: {shipped} of {} primitives dispatch; {with_paths} return a supporting path",
        cases.len()
    );
}
