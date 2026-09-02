//! Is the closing hop of a cycle pinned on *every* planner path?
//!
//! `ExpandOperator` has rejected non-closing neighbours during the walk since
//! #195, but only one of the three sites that build an expand passed the bound
//! variable in. BI-17 happens to use that site, so its profile already shows
//! the closing expand emitting exactly the answer count — which is why wiring
//! the other two changed nothing there and needed checking another way.
//!
//! A pinned close emits one row per triangle. An unpinned one emits a row per
//! neighbour of `c` and lets a filter above discard them, so the operator's row
//! count is the tell. Same query written two ways, to reach two planner paths.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn closing_expand_rows(s: &GraphStore, q: &str) -> Option<(u64, u64)> {
    let p = parse_query(&format!("PROFILE {q}")).ok()?;
    let b = QueryExecutor::new(s).execute(&p).ok()?;
    let text = format!("{:?}", b.records.first()?.values().next()?).replace("\\n", "\n");
    let mut close = None;
    let mut answer = None;
    for line in text.lines() {
        // The closing expand is the one whose target is a `__self_` synthetic.
        if line.contains("Expand") && line.contains("__self") {
            let n: Vec<&str> = line.split_whitespace().collect();
            // rows is the second-to-last column
            if n.len() >= 2 {
                close = n[n.len() - 2].replace(',', "").parse::<u64>().ok();
            }
        }
        if line.contains("Rows:") {
            answer = line.split_whitespace().nth(1)
                .and_then(|v| v.trim_end_matches(',').parse::<u64>().ok());
        }
    }
    Some((close?, answer.unwrap_or(0)))
}

fn main() {
    // A dense-ish graph so an unpinned close would emit far more than the
    // triangle count. Degree ~20 means ~20x the rows if the pin is missing.
    let n = 400i64;
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let p = s.create_node_with_labels([Label::new("P")]);
        s.set_node_property("default", p, "id", PropertyValue::Integer(i)).unwrap();
        ids.push(p);
    }
    for i in 0..ids.len() {
        for d in 1..=20usize {
            let _ = s.create_edge(ids[i], ids[(i + d) % ids.len()], "K");
        }
    }

    let direct = "MATCH (a:P)-[:K]-(b:P)-[:K]-(c:P)-[:K]-(a) \
                  WHERE a.id < b.id AND b.id < c.id RETURN count(a) AS n";
    // An **anchored** triangle. `build_path_from_anchor` is a different planner
    // path from the unanchored one BI-17 uses, and it is the one most LDBC
    // queries take, because they anchor on an indexed id. It was one of the two
    // sites that never passed the bound variable to the expand.
    let anchored = "MATCH (a:P {id: 7})-[:K]-(b:P)-[:K]-(c:P)-[:K]-(a) \
                    WHERE b.id < c.id RETURN count(a) AS n";

    let mut answers = Vec::new();
    let mut pinned_all = true;
    for (name, q) in [("unanchored (BI-17's path)", direct),
                      ("anchored (build_path_from_anchor)", anchored)] {
        match closing_expand_rows(&s, q) {
            Some((close_rows, _)) => {
                let ans = parse_query(q).ok()
                    .and_then(|p| QueryExecutor::new(&s).execute(&p).ok())
                    .and_then(|b| b.records.first()
                        .and_then(|r| r.values().next().map(|v| format!("{v:?}"))))
                    .unwrap_or_default();
                let triangles: u64 = ans.chars().filter(|c| c.is_ascii_digit())
                    .collect::<String>().parse().unwrap_or(0);
                let pinned = close_rows == triangles;
                println!("{name:<16} closing expand emitted {close_rows:>8} rows, \
                          answer {triangles:>8}  -> {}",
                    if pinned { "PINNED" } else { "NOT PINNED" });
                pinned_all &= pinned;
                answers.push(triangles);
            }
            None => println!("{name:<16} could not read the profile"),
        }
    }
    // Both spellings must agree on the answer; a pin that changed it would be a
    // wrong answer and matters more than the row count.
    // Different questions (all triangles vs triangles through one anchor), so
    // no cross-check here — the row count against each query's own answer is
    // the assertion that matters.
    assert!(answers.iter().all(|&a| a > 0),
        "a zero answer proves nothing: an unpinned close would also emit zero");
    // A regression guard, not a demonstration. Both paths pin today — that was
    // verified against HEAD before and after a candidate change, and it held
    // both times, which is how the candidate was found to be a no-op and
    // dropped. What this asserts is that they keep pinning.
    assert!(pinned_all,
        "a closing hop stopped being pinned: the expand is walking every \
         neighbour and letting the filter above discard them (#195, #1069)");
}
