//! A MATCH after `CALL db.index.vector.queryNodes(...) YIELD node` expands
//! from the yielded nodes (#2045).
//!
//! It used to be planned on its own -- a scan of every node, expanded over the
//! whole graph -- and then hash-joined to the k rows the search returned. That
//! happened both with the MATCH straight after the CALL and with a `WITH node`
//! between them. These tests read the plan to check that the MATCH starts from
//! the search, and compare the rows with the same MATCH anchored on the search
//! hits by id, which is what the join computed.

use std::collections::HashMap;

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

const N: i64 = 300;
const QUERY_VEC: &str = "[0.3, -0.2, 0.5, 0.1]";

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// `N` `:Doc` nodes with `id` and a 4-dimensional `v`, every third also
/// `:Hot`, each citing two others. A few `:Other` nodes cite into them, so a
/// plan that scans every node has something besides `:Doc` to scan.
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "CREATE VECTOR INDEX doc_v FOR (n:Doc) ON (n.v) \
         OPTIONS {dimensions: 4, similarity: 'l2'}",
    );
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    };
    let nodes: Vec<String> = (0..N)
        .map(|i| {
            let labels = if i % 3 == 0 { "Doc:Hot" } else { "Doc" };
            format!(
                "(:{labels} {{id:{i}, v:[{:?},{:?},{:?},{:?}]}})",
                next(),
                next(),
                next(),
                next()
            )
        })
        .collect();
    for chunk in nodes.chunks(100) {
        write(&mut s, &format!("CREATE {}", chunk.join(", ")));
    }

    let by_id: HashMap<i64, NodeId> = s
        .get_nodes_by_label(&Label::new("Doc"))
        .iter()
        .map(|n| match s.node_property(n.id, "id") {
            Some(PropertyValue::Integer(i)) => (i, n.id),
            other => panic!("{other:?}"),
        })
        .collect();
    for i in 0..N {
        for j in [(i * 7 + 1) % N, (i * 13 + 5) % N] {
            s.create_edge(by_id[&i], by_id[&j], "CITES").unwrap();
        }
    }
    for k in 0..10 {
        let other = s.create_node_with_labels([Label::new("Other")]);
        s.create_edge(other, by_id[&(k * 11)], "CITES").unwrap();
    }
    s
}

fn plan(s: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("parse");
    let text = match QueryExecutor::new(s).execute(&query).unwrap().records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t
            .lines()
            .take_while(|l| !l.starts_with("---"))
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("{other:?}"),
    };
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "the plan tree stops at an operator with no describe, so what is below \
         it cannot be asserted about (#1826):\n{text}"
    );
    text
}

/// Every row, as its integer columns in `cols` order, sorted.
fn rows(s: &GraphStore, cypher: &str, cols: &[&str]) -> Vec<Vec<i64>> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut out: Vec<Vec<i64>> = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e}"))
        .records
        .iter()
        .map(|r| {
            cols.iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::Integer(i))) => *i,
                    other => panic!("{cypher}: column {c} is {other:?}"),
                })
                .collect()
        })
        .collect();
    out.sort();
    out
}

/// The ids of the search's k hits, as a Cypher list literal.
fn hits(s: &GraphStore, k: usize) -> String {
    let ids: Vec<String> = rows(
        s,
        &format!(
            "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, {k}) YIELD node \
             RETURN node.id AS id"
        ),
        &["id"],
    )
    .into_iter()
    .map(|r| r[0].to_string())
    .collect();
    assert_eq!(
        ids.len(),
        k,
        "the search must return k hits for the comparison to mean anything"
    );
    format!("[{}]", ids.join(", "))
}

fn assert_seeded(plan: &str) {
    assert!(plan.contains("VectorSearch"), "{plan}");
    assert!(
        plan.contains("Expand"),
        "the MATCH must expand from the hits:\n{plan}"
    );
    assert!(
        !plan.contains("NodeScan"),
        "the MATCH must not scan the graph; it starts from the search:\n{plan}"
    );
    assert!(
        !plan.contains("HashJoin"),
        "the MATCH must not be joined back to the search:\n{plan}"
    );
}

#[test]
fn match_straight_after_the_call_expands_from_the_hits() {
    let s = store();
    let q = format!(
        "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, 20) YIELD node, score \
         MATCH (node)-[:CITES]->(hop1:Doc)-[:CITES]->(hop2:Doc) \
         RETURN hop2.id AS id, count(hop1) AS cnt"
    );
    let p = plan(&s, &q);
    assert_seeded(&p);
    assert!(!p.contains("CartesianProduct"), "{p}");

    let reference = format!(
        "MATCH (node:Doc) WHERE node.id IN {} \
         MATCH (node)-[:CITES]->(hop1:Doc)-[:CITES]->(hop2:Doc) \
         RETURN hop2.id AS id, count(hop1) AS cnt",
        hits(&s, 20)
    );
    let got = rows(&s, &q, &["id", "cnt"]);
    assert!(!got.is_empty());
    assert_eq!(got, rows(&s, &reference, &["id", "cnt"]));
}

#[test]
fn match_after_with_expands_from_the_hits() {
    let s = store();
    let q = format!(
        "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, 20) YIELD node, score \
         WITH node, score \
         MATCH (node)-[:CITES]->(hop1:Doc)-[:CITES]->(hop2:Doc) \
         RETURN hop2.id AS id, count(hop1) AS cnt"
    );
    assert_seeded(&plan(&s, &q));

    let reference = format!(
        "MATCH (node:Doc) WHERE node.id IN {} \
         MATCH (node)-[:CITES]->(hop1:Doc)-[:CITES]->(hop2:Doc) \
         RETURN hop2.id AS id, count(hop1) AS cnt",
        hits(&s, 20)
    );
    let got = rows(&s, &q, &["id", "cnt"]);
    assert!(!got.is_empty());
    assert_eq!(got, rows(&s, &reference, &["id", "cnt"]));
}

/// A label the MATCH puts on the yielded node still filters it. The expand
/// from a bound start does not read the start's labels; the join it replaces
/// did, through its scan.
#[test]
fn a_label_on_the_yielded_node_still_filters() {
    let s = store();
    let hit_ids = hits(&s, 30);
    for with in ["", "WITH node "] {
        let q = format!(
            "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, 30) YIELD node \
             {with}MATCH (node:Hot)-[:CITES]->(m) \
             RETURN node.id AS a, m.id AS b"
        );
        assert_seeded(&plan(&s, &q));
        let reference = format!(
            "MATCH (node:Doc:Hot) WHERE node.id IN {hit_ids} \
             MATCH (node)-[:CITES]->(m) RETURN node.id AS a, m.id AS b"
        );
        let got = rows(&s, &q, &["a", "b"]);
        assert!(!got.is_empty(), "{q}");
        assert!(got.iter().all(|r| r[0] % 3 == 0), "{q}: {got:?}");
        assert_eq!(got, rows(&s, &reference, &["a", "b"]), "{q}");
    }
}

/// So does an inline property on it.
#[test]
fn an_inline_property_on_the_yielded_node_still_filters() {
    let s = store();
    let hit_ids = hits(&s, 10);
    let id: i64 = hit_ids
        .trim_matches(|c| c == '[' || c == ']')
        .split(", ")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let q = format!(
        "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, 10) YIELD node \
         MATCH (node {{id: {id}}})-[:CITES]->(m) RETURN node.id AS a, m.id AS b"
    );
    assert_seeded(&plan(&s, &q));
    let got = rows(&s, &q, &["a", "b"]);
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(got.iter().all(|r| r[0] == id), "{got:?}");
}

/// A WHERE naming the yielded node and the expanded ones is applied whole,
/// including the conjunct on the yielded node alone, which the expand from a
/// bound start leaves to the filter above it.
#[test]
fn a_where_over_the_yielded_and_expanded_nodes_is_applied() {
    let s = store();
    let q = format!(
        "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, 30) YIELD node \
         MATCH (node)-[:CITES]->(m:Doc) WHERE node.id % 2 = 0 AND m.id > 100 \
         RETURN node.id AS a, m.id AS b"
    );
    assert_seeded(&plan(&s, &q));
    let reference = format!(
        "MATCH (node:Doc) WHERE node.id IN {} \
         MATCH (node)-[:CITES]->(m:Doc) WHERE node.id % 2 = 0 AND m.id > 100 \
         RETURN node.id AS a, m.id AS b",
        hits(&s, 30)
    );
    let got = rows(&s, &q, &["a", "b"]);
    assert!(!got.is_empty());
    assert!(got.iter().all(|r| r[0] % 2 == 0 && r[1] > 100), "{got:?}");
    assert_eq!(got, rows(&s, &reference, &["a", "b"]));
}

/// A MATCH that does not start from the yielded node keeps the plan it had.
#[test]
fn a_match_not_starting_from_the_yielded_node_is_unchanged() {
    let s = store();
    let q = format!(
        "CALL db.index.vector.queryNodes('Doc', 'v', {QUERY_VEC}, 5) YIELD node \
         MATCH (o:Other)-[:CITES]->(d:Doc) RETURN count(*) AS c"
    );
    // Five hits crossed with the ten `:Other` citations.
    assert_eq!(rows(&s, &q, &["c"]), vec![vec![50]]);
}
