//! ALGO-04: which in-query composition forms actually work?
//!
//! The requirement is `MATCH … CALL algo.x(subgraph) YIELD … WHERE … RETURN` in
//! one statement with no explicit projection step. Its baseline in spec 06 lists
//! what works and what does not, and on 2026-09-18 that list was stale in three
//! directions at once — two limitations had been fixed and one working feature
//! did not work. A list maintained by hand drifts; this runs the forms.
//!
//! Each form is judged by **executing** it and checking the row count, not by
//! whether it parses. `YIELD node AS a … RETURN count(*)` parses, runs, and
//! never touches the alias, which is exactly how "aliased yields work" came to
//! be written down while `RETURN a` failed (#1318).
//!
//! ```text
//! cargo run --release --example algo_compose -- --json compose.json
//! ```

use samyama::graph::{EdgeType, GraphStore};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

struct Form {
    id: &'static str,
    /// What the form composes, in the requirement's terms.
    about: &'static str,
    query: &'static str,
    /// Rows a working engine returns on the fixture below. `None` means any
    /// number of rows counts, for forms whose shape is the point.
    expect: Option<usize>,
}

/// Five nodes in a chain: 5 nodes, 4 edges. Row counts chosen so a form that
/// silently returns nothing, or collapses to one aggregate row, is visibly
/// different from one that works.
fn fixture() -> GraphStore {
    let mut s = GraphStore::new();
    let ns: Vec<_> = (0..5).map(|_| s.create_node("N")).collect();
    for i in 0..4 {
        s.create_edge(ns[i], ns[i + 1], EdgeType::new("R")).unwrap();
    }
    s
}

const FORMS: &[Form] = &[
    Form { id: "where-after-yield", about: "filter the algorithm's output in the same statement",
           query: "CALL algo.pageRank() YIELD node, score WHERE score > 0.0 RETURN node",
           expect: Some(5) },
    Form { id: "match-before-call", about: "a pattern before the call, no explicit projection",
           query: "MATCH (n:N) CALL algo.pageRank() YIELD node, score RETURN node",
           expect: Some(25) },
    Form { id: "aliased-yield", about: "rename a yielded column and use the new name",
           query: "CALL algo.pageRank() YIELD node AS a, score AS b RETURN a, b",
           expect: Some(5) },
    Form { id: "order-by-after-yield", about: "order by a yielded column",
           query: "CALL algo.pageRank() YIELD node, score RETURN node ORDER BY score DESC",
           expect: Some(5) },
    Form { id: "limit-after-yield", about: "take the top k of the algorithm's output",
           query: "CALL algo.pageRank() YIELD node, score RETURN node ORDER BY score DESC LIMIT 2",
           expect: Some(2) },
    Form { id: "yielded-var-in-later-match", about: "traverse from what the algorithm returned",
           query: "CALL algo.pageRank() YIELD node, score MATCH (node)-[:R]->(m) RETURN m",
           expect: Some(4) },
    Form { id: "aggregate-over-yield", about: "aggregate the algorithm's output",
           query: "CALL algo.pageRank() YIELD node, score RETURN count(node) AS n, sum(score) AS t",
           expect: Some(1) },
    Form { id: "with-after-yield", about: "a WITH stage between the call and the return",
           query: "CALL algo.pageRank() YIELD node, score WITH node, score WHERE score > 0.0 RETURN node",
           expect: Some(5) },
    Form { id: "call-then-call", about: "chain two algorithms in one statement",
           query: "CALL algo.pageRank() YIELD node, score CALL algo.wcc() YIELD node AS n2, componentId RETURN n2",
           expect: None },
];

fn main() {
    let out = std::env::args().collect::<Vec<_>>();
    let json_out = out.iter().position(|a| a == "--json").and_then(|i| out.get(i + 1)).cloned();
    let store = fixture();
    let mut results = Vec::new();

    for f in FORMS {
        let (state, detail, rows) = match parse_query(f.query) {
            Err(e) => ("parse-error", e.to_string(), None),
            Ok(p) => match QueryExecutor::new(&store).execute(&p) {
                Err(e) => ("exec-error", e.to_string(), None),
                Ok(r) => {
                    let n = r.records.len();
                    match f.expect {
                        Some(want) if n != want => (
                            "wrong-rows",
                            format!("expected {want} rows, got {n}"),
                            Some(n),
                        ),
                        _ => ("works", String::new(), Some(n)),
                    }
                }
            },
        };
        println!("{:28} {:12} {}", f.id, state, detail.chars().take(60).collect::<String>());
        results.push(serde_json::json!({
            "id": f.id, "about": f.about, "query": f.query,
            "state": state, "rows": rows,
            "detail": detail.chars().take(160).collect::<String>(),
        }));
    }

    let doc = serde_json::json!({
        "fixture": {"nodes": 5, "edges": 4, "shape": "chain"},
        "forms": results,
    });
    match json_out {
        Some(p) => { std::fs::write(&p, serde_json::to_string_pretty(&doc).unwrap()).unwrap();
                     eprintln!("wrote {p}"); }
        None => println!("{}", serde_json::to_string_pretty(&doc).unwrap()),
    }
}
