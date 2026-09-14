//! An inline target property pins a variable-length target the way the same
//! equality in `WHERE` does (samyama-graph#1199).
//!
//! When a variable-length pattern's far end is one known node, the operator
//! answers "can each source reach it" with one reachability search out of that
//! node instead of expanding every source's neighbourhood (`with_pinned_target`,
//! the LDBC IC6 shape). The planner decided that from `WHERE` predicates only on
//! single-segment patterns: `(q:P {id: 7})` was applied as a filter after the
//! walk and never pinned, while `WHERE q.id = 7` did.
//!
//! Pinning changes the plan, not the answer: the inline-property filter still
//! runs. So each shape is also compared, row for row, with its `WHERE` form and
//! with a store that has no index (which cannot pin at all).
//!
//! A pin is only sound when the query cannot count the paths. Where it can --
//! `count(q)` -- the operator enumerates trails, and a pinned target answered
//! once per source: 1 where the walk gives 2 (#1202). Those shapes must not pin,
//! in either form.

use samyama::graph::{GraphStore, NodeId};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn rows(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| format!("{:?}", out.columns.iter().map(|c| r.get(c).cloned()).collect::<Vec<_>>()))
        .collect();
    got.sort();
    got
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let out = QueryExecutor::new(store).execute(&q).unwrap();
    format!("{:?}", out.records[0].get("plan"))
}

/// Forty people in a ring with chords, `id` unique.
fn people(indexed: bool) -> GraphStore {
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..40)
        .map(|i| {
            let n = store.create_node("P");
            store.set_node_property("default", n, "id", i as i64).unwrap();
            n
        })
        .collect();
    for i in 0..40 {
        store.create_edge(ids[i], ids[(i + 1) % 40], "KNOWS").unwrap();
        store.create_edge(ids[i], ids[(i * 7 + 3) % 40], "KNOWS").unwrap();
    }
    if indexed {
        let q = parse_query("CREATE INDEX ON :P(id)").unwrap();
        MutQueryExecutor::new(&mut store, "default".to_string()).execute(&q).unwrap();
    }
    store
}

/// Anchored at person 1, with a target each direction reaches: 8 is three
/// hops away undirected, 1 -> 2 -> 17 is two hops out, and 0 -> 1 is one hop in.
/// (An unanchored source is not a pin case in either form: the planner anchors
/// on the indexed target and walks back from it.)
///
/// `RETURN DISTINCT` without an aggregate: the number of paths is invisible,
/// so these are the shapes that pin.
const INLINE: &[&str] = &[
    "MATCH (p:P {id: 1})-[:KNOWS*1..3]-(q:P {id: 8}) RETURN DISTINCT q.id AS q",
    "MATCH (p:P {id: 1})-[:KNOWS*1..2]->(q:P {id: 17}) RETURN DISTINCT q.id AS q",
    "MATCH (p:P {id: 1})<-[:KNOWS*1..3]-(q:P {id: 0}) RETURN DISTINCT q.id AS q",
];

/// The same targets counted: the number of paths is visible, so none may pin.
const COUNTED: &[&str] = &[
    "MATCH (p:P {id: 1})-[:KNOWS*1..3]-(q:P {id: 8}) RETURN count(q) AS n",
    "MATCH (p:P {id: 1})-[:KNOWS*1..2]->(q:P {id: 17}) RETURN count(q) AS n",
    "MATCH (p:P {id: 1})<-[:KNOWS*1..3]-(q:P {id: 0}) RETURN count(q) AS n",
];

/// The same question with the target's equality written in `WHERE`.
fn where_form(inline: &str) -> String {
    let start = inline.find("(q:P {id: ").unwrap();
    let end = start + inline[start..].find("})").unwrap() + 2;
    let id = &inline[start + "(q:P {id: ".len()..end - 2];
    let s = format!("{}(q:P){}", &inline[..start], &inline[end..]);
    let (head, ret) = s.split_once(" RETURN ").unwrap();
    format!("{head} WHERE q.id = {id} RETURN {ret}")
}

#[test]
fn an_inline_target_property_pins_as_where_does() {
    let store = people(true);
    for shape in INLINE {
        let w = where_form(shape);
        assert!(plan(&store, &w).contains("target pinned"), "the WHERE form should pin: `{w}`");
        assert!(
            plan(&store, shape).contains("target pinned"),
            "`{shape}` does not pin its target:\n{}",
            plan(&store, shape)
        );
    }
}

#[test]
fn pinning_an_inline_target_changes_no_answer() {
    let indexed = people(true);
    let walked = people(false);
    for shape in INLINE.iter().chain(COUNTED) {
        let expected = rows(&walked, shape);
        assert!(!plan(&walked, shape).contains("target pinned"), "an unindexed store cannot pin");
        let reached = !expected.is_empty()
            && (!COUNTED.contains(shape) || expected != vec!["[Some(Property(Integer(0)))]".to_string()]);
        assert!(reached, "`{shape}` should reach its target: {expected:?}");
        assert_eq!(rows(&indexed, shape), expected, "`{shape}` answers differently on the indexed store");
        assert_eq!(rows(&indexed, &where_form(shape)), expected, "the WHERE form of `{shape}` differs");
    }
}

/// #1202: where the query can count paths, a pin answered once per source.
/// The undirected shape has two trails from 1 to 8 within three hops.
#[test]
fn a_counted_target_is_not_pinned_and_counts_every_path() {
    let indexed = people(true);
    let walked = people(false);
    for shape in COUNTED {
        for form in [shape.to_string(), where_form(shape)] {
            assert!(!plan(&indexed, &form).contains("target pinned"), "`{form}` counts paths and must not pin");
        }
    }
    let undirected = COUNTED[0];
    assert_eq!(rows(&walked, undirected), vec!["[Some(Property(Integer(2)))]".to_string()]);
    assert_eq!(rows(&indexed, undirected), rows(&walked, undirected));
    assert_eq!(rows(&indexed, &where_form(undirected)), rows(&walked, undirected));
}

/// Two nodes match: that is a filter, not a pin.
#[test]
fn an_inline_property_matching_two_nodes_does_not_pin() {
    let mut store = people(true);
    let extra = store.create_node("P");
    store.set_node_property("default", extra, "id", 7i64).unwrap();
    let shape = "MATCH (p:P {id: 1})-[:KNOWS*1..3]-(q:P {id: 7}) RETURN count(q) AS n";
    assert!(!plan(&store, shape).contains("target pinned"), "two nodes have id 7, so nothing is pinned");
}
