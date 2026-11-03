//! Every parent operator can carry a write beneath it (#870).
//!
//! A pass-through operator's default `next_mut` delegates to `next`, which
//! reads its input **read-only** — so any write beneath it refuses with
//! "requires mutable store access".
//!
//! This class has now been fixed five times: #622 (barriers), #624 (joins),
//! #649 (`SKIP`, `LIMIT`), #866 (`SORT`, `FILTER`), and #870 (six more).
//! #649's comment called `SKIP` and `LIMIT` *"the last two pass-through
//! operators that still had it"*. They were not.
//!
//! So this file does not test the operators a failing query happened to name.
//! [`every_parent_operator_implements_next_mut`] reads the source and fails if
//! any `impl PhysicalOperator` block that has children lacks `next_mut` — the
//! sixth time is meant to be the last.

use std::collections::BTreeSet;

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

/// Reads `operator.rs` and reports any operator that has children but no
/// `next_mut`.
///
/// Source-reading is the point: a behavioural test can only cover the shapes
/// someone thought to write, and every previous round of this defect was found
/// by a query nobody had thought to write.
///
/// Verified by removing `UnwindOperator::next_mut` and watching this fail while
/// [`a_write_runs_beneath_each_parent_operator`] below **still passed** — which
/// is the whole argument for reading the source rather than trusting a
/// behavioural sample.
#[test]
fn every_parent_operator_implements_next_mut() {
    let src = include_str!("../src/query/executor/operator.rs");

    let mut blocks: Vec<(String, usize)> = Vec::new();
    for (offset, _) in src.match_indices("impl PhysicalOperator for ") {
        let rest = &src[offset + "impl PhysicalOperator for ".len()..];
        let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
        blocks.push((name, offset));
    }
    assert!(
        blocks.len() >= 40,
        "expected to find the operator impls; found {} — has the file moved?",
        blocks.len()
    );

    let mut missing = BTreeSet::new();
    for (i, (name, start)) in blocks.iter().enumerate() {
        let end = blocks.get(i + 1).map(|(_, o)| *o).unwrap_or(src.len());
        let body = &src[*start..end];
        // An operator with children is one that pulls from an input.
        let has_children = body.contains("fn children_mut");
        if has_children && !body.contains("fn next_mut") {
            missing.insert(name.clone());
        }
    }

    assert!(
        missing.is_empty(),
        "these operators have children but no `next_mut`, so a write beneath them \
         will refuse with \"requires mutable store access\": {missing:?}"
    );
}

fn run(cypher: &str) -> Result<usize, String> {
    let mut store = GraphStore::new();
    for setup in ["CREATE (a:A {n: 1}), (b:B {n: 2})", "CREATE (:C)-[:R]->(:D)"] {
        let q = parse_query(setup).expect("setup parses");
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .expect("setup runs");
    }
    let q = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .map(|b| b.records.len())
        .map_err(|e| format!("{e:?}"))
}

/// One write beneath each kind of parent, so the source check above is backed
/// by queries that actually exercise the paths.
#[test]
fn a_write_runs_beneath_each_parent_operator() {
    for cypher in [
        // UNWIND
        "UNWIND [1, 2] AS x CREATE (:N {v: x})",
        "MATCH (a:A) WITH a UNWIND [1, 2] AS x CREATE (a)-[:R {v: x}]->(:N)",
        // Expand
        "MATCH (c:C)-[:R]->(d:D) CREATE (d)-[:BACK]->(c)",
        // ExpandInto
        "MATCH (c:C), (d:D) MERGE (c)-[:R2]->(d)",
        // Var-length expand
        "MATCH (c:C)-[:R*1..2]->(d) CREATE (:Seen {x: 1})",
        // Sort and Filter above a write
        "UNWIND [2, 1] AS x CREATE (n:N {v: x}) WITH n ORDER BY n.v RETURN n",
        "UNWIND [1, 2] AS x CREATE (n:N {v: x}) WITH n WHERE n.v > 1 RETURN n",
    ] {
        assert!(run(cypher).is_ok(), "{cypher}\n  -> {:?}", run(cypher));
    }
}

/// Reads still stream — `next_mut` is only reached when the query writes, so
/// making the write path eager must not have made every plan eager.
#[test]
fn reads_are_unaffected() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:A)-[:R]->(:B)").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    for (cypher, want) in [
        ("MATCH (n) RETURN n", 2),
        ("MATCH (a)-->(b) RETURN a, b", 1),
        ("UNWIND [1, 2, 3] AS x RETURN x", 3),
        ("MATCH (a)-[*1..2]->(b) RETURN a, b", 1),
    ] {
        let p = parse_query(cypher).expect("parses");
        let rows = QueryExecutor::new(&store).execute(&p).expect("runs").records.len();
        assert_eq!(rows, want, "{cypher}");
    }
}
