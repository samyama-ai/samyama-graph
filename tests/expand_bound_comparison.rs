//! A comparison between an expansion's target and a variable already bound --
//! `MATCH (a)-[:R]->(b) WHERE b.p < a.p` -- is applied *during* the walk, not
//! to the records it produces (#1069).
//!
//! `ExpandOperator` could prune on an equality against a literal (#656) and
//! nothing else, so this shape built a record for every neighbour and a filter
//! then threw most of them away. On LDBC BI-17, before its cyclic prune, that
//! was 25M records built to keep 8.5M.
//!
//! The pushdown is **additive**: the filter stays, and the expand rejects an
//! edge only when the engine's own comparison says false or null. So the tests
//! below are of two kinds. The differential ones compare every answer against
//! the same query written with `WITH ... WHERE`, which the planner cannot push
//! into the expand; they are what notices a pushdown that rejects too much.
//! The row-count ones read `PROFILE` and pin what the expand emits; they are
//! what notices a pushdown that stopped happening.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `N` nodes whose `p` spans integers, floats equal to some of those integers,
/// a NaN, strings, a boolean and absent; every node also has an integer `q`.
/// Edges are directed and deliberately irregular, with a few self-loops.
fn graph() -> GraphStore {
    const N: usize = 60;
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..N).map(|_| store.create_node("N")).collect();
    for (i, &n) in ns.iter().enumerate() {
        let p = match i % 8 {
            0 => None,
            1 | 2 => Some(PropertyValue::Integer((i % 7) as i64)),
            3 => Some(PropertyValue::Float((i % 7) as f64)),
            4 => Some(PropertyValue::Float((i % 7) as f64 + 0.5)),
            5 => Some(PropertyValue::String(format!("s{}", i % 5))),
            6 => Some(PropertyValue::Boolean(i % 3 == 0)),
            _ => Some(if i % 3 == 0 {
                PropertyValue::Float(f64::NAN)
            } else {
                PropertyValue::Integer(3)
            }),
        };
        if let Some(p) = p {
            store.set_node_property("default", n, "p", p).unwrap();
        }
        store
            .set_node_property("default", n, "q", (i % 5) as i64)
            .unwrap();
    }
    for i in 0..N {
        for d in [1usize, 3, 7] {
            store.create_edge(ns[i], ns[(i * 5 + d) % N], "R").unwrap();
        }
        if i % 11 == 0 {
            store.create_edge(ns[i], ns[i], "R").unwrap();
        }
    }
    store
}

fn run(store: &GraphStore, cypher: &str) -> Vec<Vec<String>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let batch = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let mut rows: Vec<Vec<String>> = batch
        .records
        .iter()
        .map(|r| {
            batch
                .columns
                .iter()
                .map(|c| format!("{:?}", r.get(c)))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

/// Rows each `Expand` in the plan emitted, top to bottom.
fn expand_rows(store: &GraphStore, cypher: &str) -> Vec<u64> {
    let q = parse_query(&format!("PROFILE {cypher}")).expect("PROFILE should parse");
    let batch = QueryExecutor::new(store)
        .execute(&q)
        .expect("PROFILE should run");
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("expected a plan string, got {other:?}"),
    };
    let rows: Vec<u64> = text
        .lines()
        .take_while(|l| !l.starts_with("Hottest"))
        .filter(|l| l.trim_start().starts_with("Expand "))
        .map(|l| {
            let cols: Vec<&str> = l.split_whitespace().collect();
            cols[cols.len() - 2].parse().expect("rows column")
        })
        .collect();
    assert!(!rows.is_empty(), "no Expand in the plan:\n{text}");
    rows
}

const OPS: [&str; 6] = ["<", "<=", ">", ">=", "=", "<>"];

/// Assert `pattern WHERE pred RETURN ret` answers exactly what the unpushable
/// `pattern WITH <every variable> WHERE pred RETURN ret` does, and return the
/// row count.
///
/// The variables are listed rather than written `WITH *`: after a
/// `MATCH ... WITH a MATCH ...` chain, `WITH *` keeps only `a`.
fn agree(store: &GraphStore, pattern: &str, pred: &str, ret: &str) -> usize {
    let vars: Vec<&str> = ["a", "b", "c", "r"]
        .into_iter()
        .filter(|v| {
            pattern.contains(&format!("({v}:"))
                || pattern.contains(&format!("({v})"))
                || pattern.contains(&format!("[{v}:"))
        })
        .collect();
    let pushed = format!("{pattern} WHERE {pred} RETURN {ret}");
    let reference = format!(
        "{pattern} WITH {} WHERE {pred} RETURN {ret}",
        vars.join(", ")
    );
    let got = run(store, &pushed);
    assert_eq!(
        got,
        run(store, &reference),
        "`{pushed}` disagrees with `{reference}`"
    );
    got.len()
}

#[test]
fn every_operator_and_direction_answers_what_the_filter_answers() {
    let store = graph();
    let mut nonempty = 0;
    for dir in ["-[:R]->", "<-[:R]-", "-[:R]-"] {
        let pattern = format!("MATCH (a:N){dir}(b:N)");
        for op in OPS {
            // Both operand orders: the pushed check must hand the comparison
            // its operands the way round they were written.
            for pred in [format!("b.p {op} a.p"), format!("a.p {op} b.p")] {
                if agree(&store, &pattern, &pred, "id(a) AS a, id(b) AS b") > 0 {
                    nonempty += 1;
                }
            }
        }
    }
    // A differential test over answers that are all empty tests nothing.
    assert!(nonempty >= 30, "only {nonempty} of 36 queries had any rows");
}

#[test]
fn comparisons_with_other_conjuncts_and_with_or_agree() {
    let store = graph();
    let p = "MATCH (a:N)-[:R]->(b:N)";
    let r = "id(a) AS a, id(b) AS b";
    for pred in [
        "b.p < a.p AND b.q > 1",
        "b.q = a.q AND b.p >= a.p",
        "b.p < a.p AND a.p < 5",
        // Not a conjunct, so not pushed; still has to be right.
        "b.p < a.p OR b.q = 1",
        "NOT (b.p < a.p)",
        // Mixed with the literal-equality pushdown on the same target.
        "b.q = 2 AND b.p > a.p",
        // An absent property on the bound side is null on every edge.
        "b.p < a.missing",
        "b.q < a.q",
    ] {
        agree(&store, p, pred, r);
    }
}

#[test]
fn longer_patterns_and_the_bound_start_builder_agree() {
    let store = graph();
    for (pattern, pred, ret) in [
        // Compared with a variable bound two hops back, and with a previous
        // segment's relationship.
        (
            "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)",
            "c.p > a.p",
            "id(a), id(b), id(c)",
        ),
        (
            "MATCH (a:N)-[:R]->(b:N)-[:R]-(c:N)",
            "a.p < b.p AND b.p < c.p",
            "id(a), id(b), id(c)",
        ),
        (
            "MATCH (a:N)-[r:R]->(b:N)-[:R]->(c:N)",
            "c.q <= id(r) % 5 AND c.p <> b.p",
            "id(a), id(c)",
        ),
        // A variable-length segment: not pushed into, and must not break the
        // single hop after it.
        ("MATCH (a:N)-[:R*1..2]->(b:N)", "b.p < a.p", "id(a), id(b)"),
        (
            "MATCH (a:N)-[:R*1..2]->(b:N)-[:R]->(c:N)",
            "c.p >= a.p",
            "id(a), id(b), id(c)",
        ),
        // The builder a MATCH starting from a WITH variable goes through.
        (
            "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N)",
            "b.p < a.p",
            "id(a), id(b)",
        ),
        (
            "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N)-[:R]->(c:N)",
            "c.p = a.p",
            "id(a), id(c)",
        ),
    ] {
        agree(&store, pattern, pred, ret);
    }
}

/// The point of the change: the expand emits only the rows the comparison
/// keeps, where the unpushed plan emits one per edge.
#[test]
fn the_expand_emits_only_the_rows_that_survive() {
    // Integer `p` everywhere, so exactly half the non-self edges survive `<`.
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..400).map(|_| store.create_node("N")).collect();
    for (i, &n) in ns.iter().enumerate() {
        store
            .set_node_property("default", n, "p", ((i * 7919) % 400) as i64)
            .unwrap();
    }
    let mut edges = 0u64;
    for i in 0..ns.len() {
        for d in 1..=10 {
            store
                .create_edge(ns[i], ns[(i + d * 13) % ns.len()], "R")
                .unwrap();
            edges += 1;
        }
    }

    let pushed = "MATCH (a:N)-[:R]->(b:N) WHERE b.p < a.p RETURN count(*) AS n";
    let unpushed = "MATCH (a:N)-[:R]->(b:N) WITH * WHERE b.p < a.p RETURN count(*) AS n";
    let q = parse_query(pushed).unwrap();
    let answer = match QueryExecutor::new(&store).execute(&q).unwrap().records[0].get("n") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n as u64,
        other => panic!("a count expected, got {other:?}"),
    };
    assert_eq!(run(&store, pushed), run(&store, unpushed));

    let before = expand_rows(&store, unpushed);
    let after = expand_rows(&store, pushed);
    eprintln!(
        "{edges} edges, {answer} survive; Expand emitted {before:?} unpushed, {after:?} pushed"
    );
    assert_eq!(
        before,
        vec![edges],
        "the reference plan should expand every edge"
    );
    assert_eq!(
        after,
        vec![answer],
        "the pushed expand should emit only survivors"
    );
    assert!(
        answer > 0 && answer < edges,
        "fixture should keep some rows and drop some"
    );

    // Two hops, pruned against a variable bound two segments back.
    let two = "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) WHERE c.p < a.p RETURN count(*) AS n";
    let two_ref = "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) WITH * WHERE c.p < a.p RETURN count(*) AS n";
    assert_eq!(run(&store, two), run(&store, two_ref));
    let (b, a) = (expand_rows(&store, two_ref), expand_rows(&store, two));
    eprintln!("two hops: Expand emitted {b:?} unpushed, {a:?} pushed");
    assert_eq!(b.len(), 2);
    assert_eq!(a[1], b[1], "the first hop has nothing to compare against");
    assert!(
        a[0] < b[0],
        "the second hop should be pruned: {a:?} vs {b:?}"
    );

    // The builder for a MATCH that starts from a WITH variable.
    let bound = "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WHERE b.p < a.p RETURN count(*) AS n";
    let bound_ref =
        "MATCH (a:N) WITH a MATCH (a)-[:R]->(b:N) WITH a, b WHERE b.p < a.p RETURN count(*) AS n";
    assert_eq!(run(&store, bound), run(&store, bound_ref));
    assert_eq!(expand_rows(&store, bound_ref), vec![edges]);
    assert_eq!(expand_rows(&store, bound), vec![answer]);
}
