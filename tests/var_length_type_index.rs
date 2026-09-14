//! A variable-length expand must answer the same whether it walks each node's
//! whole adjacency or reads the per-type index (samyama-graph#1197).
//!
//! `VarLengthExpandOperator` walked every relationship of every node it
//! visited and kept the ones of the requested type. At LDBC SNB SF10 that type
//! check was 84.5% of IC1's CPU: a Person has ~60 `KNOWS` edges and ~1,200
//! others. The single-hop `ExpandOperator` has read `store.type_adjacency`
//! since #748; this does the same for the variable-length walk.
//!
//! The comparison is exact and per anchor. Each query is run once per anchor
//! person on a store that has never built an index -- one anchor's walk stays
//! under the operator's build threshold, and the test asserts nothing was
//! built -- and again on a store whose index an all-anchor run has built, so
//! every walk reads it. Rows are compared as sorted lists: the index lists
//! neighbours by `(target, edge)`, the walk in adjacency order, and no answer
//! here is ordered.
//!
//! Path and relationship variables are compared by length only. A
//! shortest-path walk keeps one path per end node, and between parallel edges
//! which one it keeps depends on visiting order, which is not part of the
//! answer.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
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

const PEOPLE: usize = 300;

/// People joined by a selective `KNOWS` (two out, two in per person) and a
/// bulk `LIKES` (twelve out per person): the shape the index exists for. Some
/// `KNOWS` edges are doubled, because multiplicity is where an index that
/// dropped or repeated an edge would show; one is a self-loop, which the
/// undirected walk must take once from each side as the store's walk does.
fn social(compact: bool) -> GraphStore {
    let mut store = GraphStore::new();
    let people: Vec<NodeId> = (0..PEOPLE)
        .map(|i| {
            let n = store.create_node("P");
            store.set_node_property("default", n, "id", i as i64).unwrap();
            n
        })
        .collect();
    let org = store.create_node("O");
    for (i, &p) in people.iter().enumerate() {
        for (k, step) in [(0, 7usize), (1, 13)] {
            let q = people[(i * step + 1 + k) % PEOPLE];
            let e = store.create_edge(p, q, "KNOWS").unwrap();
            store.set_edge_property(e, "w", (i % 3) as i64).unwrap();
            if i % 50 == 0 && k == 0 {
                let twin = store.create_edge(p, q, "KNOWS").unwrap();
                store.set_edge_property(twin, "w", 2i64).unwrap();
            }
        }
        for j in 0..12 {
            store.create_edge(p, people[(i * 5 + j + 1) % PEOPLE], "LIKES").unwrap();
        }
        if i % 4 == 0 {
            store.create_edge(p, org, "WORKS_AT").unwrap();
        }
    }
    store.create_edge(people[0], people[0], "KNOWS").unwrap();
    if compact {
        store.compact_adjacency();
    }
    store
}

/// Each shape the operator walks: both directions and undirected, the
/// first-reach walk (`*1..n`), trail enumeration (`*2..n`), zero-length,
/// inline relationship properties, path and relationship variables, and a
/// target label. `{A}` is the anchor.
const SHAPES: &[&str] = &[
    "MATCH (p:P {id: {A}})-[:KNOWS*1..2]->(f) RETURN f.id",
    "MATCH (p:P {id: {A}})<-[:KNOWS*1..2]-(f) RETURN f.id",
    "MATCH (p:P {id: {A}})-[:KNOWS*1..3]-(f) RETURN f.id",
    "MATCH (p:P {id: {A}})-[:KNOWS*2..3]-(f) RETURN f.id",
    "MATCH (p:P {id: {A}})-[:KNOWS*2..2]->(f) RETURN f.id",
    "MATCH (p:P {id: {A}})-[:KNOWS*0..2]->(f) RETURN f.id",
    "MATCH (p:P {id: {A}})-[:KNOWS*1..2 {w: 1}]->(f) RETURN f.id",
    "MATCH path = (p:P {id: {A}})-[:KNOWS*1..2]-(f) RETURN f.id, length(path)",
    "MATCH (p:P {id: {A}})-[r:KNOWS*2..3]->(f) RETURN f.id, size(r)",
    "MATCH (p:P {id: {A}})-[:KNOWS*1..2]->(f:P) RETURN count(f)",
    "MATCH (p:P {id: {A}})-[:KNOWS*1..2]-(f) RETURN DISTINCT f.id",
];

/// Anchors spread over the id range, including the self-loop's and a doubled
/// edge's owner.
const ANCHORS: &[usize] = &[0, 1, 50, 99, 150, 217, 299];

fn at(shape: &str, anchor: usize) -> String {
    shape.replace("{A}", &anchor.to_string())
}

/// The same shape with no anchor, over every person: enough walks to cross
/// the operator's build threshold, so the index gets built.
fn every_anchor(shape: &str) -> String {
    shape.replace(" {id: {A}}", "")
}

fn compare(compact: bool) {
    let walked = social(compact);
    let indexed = social(compact);
    for shape in SHAPES {
        let expected: Vec<Vec<String>> = ANCHORS.iter().map(|&a| rows(&walked, &at(shape, a))).collect();
        assert_eq!(
            walked.type_adjacency_cached(),
            0,
            "one anchor's walk built a type index, so `{shape}` has no index-free reference"
        );

        let all = rows(&indexed, &every_anchor(shape));
        assert!(!all.is_empty(), "`{shape}` over every person matched nothing");
        assert!(
            indexed.type_adjacency_cached() > 0,
            "`{shape}` over every person never built a type index, so nothing was compared"
        );
        for (i, &a) in ANCHORS.iter().enumerate() {
            let got = rows(&indexed, &at(shape, a));
            assert_eq!(got, expected[i], "`{}` (compacted: {compact}) differs with the index", at(shape, a));
        }
        // The all-anchor run crossed the threshold mid-query; a second run
        // reads the index from its first walk. Both must agree.
        assert_eq!(rows(&indexed, &every_anchor(shape)), all, "`{shape}`: cold and warm runs differ");
    }
}

#[test]
fn every_var_length_shape_answers_the_same_from_the_index() {
    compare(false);
}

#[test]
fn every_var_length_shape_answers_the_same_from_the_index_after_compaction() {
    compare(true);
}

/// The pinned-target walk: when the destination resolves to one node, the
/// operator answers "can this source reach it" from one reachability search
/// out of the target, run once per operator. That search reads the index too.
///
/// It makes no forward walks, so it never builds an index itself; a
/// non-pinned run builds one first.
#[test]
fn the_pinned_target_search_answers_the_same_from_the_index() {
    // Targets each direction can reach from some anchor: person 1 has a KNOWS
    // edge to 8, and person 0 one to 1.
    //
    // The target's equality is in `WHERE`, not inline: on a single-segment
    // pattern an inline `(q:P {id: 7})` is applied as a filter after the walk
    // and never pins, while the same equality in `WHERE` does.
    let shapes = [
        "MATCH (p:P)-[:KNOWS*1..3]-(q:P) WHERE p.id = {A} AND q.id = 7 RETURN count(q)",
        "MATCH (p:P)-[:KNOWS*1..2]->(q:P) WHERE p.id = {A} AND q.id = 8 RETURN count(q)",
        "MATCH (p:P)<-[:KNOWS*1..3]-(q:P) WHERE p.id = {A} AND q.id = 0 RETURN count(q)",
    ];
    // The planner pins a target only through a property index that resolves
    // it to exactly one node.
    let (mut walked, mut indexed) = (social(false), social(false));
    for store in [&mut walked, &mut indexed] {
        let q = parse_query("CREATE INDEX ON :P(id)").unwrap();
        MutQueryExecutor::new(store, "default".to_string()).execute(&q).unwrap();
    }
    let (walked, indexed) = (walked, indexed);
    // The shape this test exists for: the planner pinned the target.
    for shape in shapes {
        let q = parse_query(&format!("EXPLAIN {}", at(shape, 1))).unwrap();
        let out = QueryExecutor::new(&walked).execute(&q).unwrap();
        let plan = format!("{:?}", out.records[0].get("plan"));
        assert!(plan.contains("target pinned"), "`{shape}` does not run the pinned-target search:\n{plan}");
    }
    rows(&indexed, &every_anchor("MATCH (p:P {id: {A}})-[:KNOWS*1..3]-(f) RETURN f.id"));
    assert!(indexed.type_adjacency_cached() >= 2, "both directions of KNOWS should be indexed");
    for shape in shapes {
        // Per anchor only on the walk-only store: over every person the
        // planner may walk forward from each one, which crosses the build
        // threshold and would leave that store with an index of its own.
        let expected: Vec<Vec<String>> = ANCHORS.iter().map(|&a| rows(&walked, &at(shape, a))).collect();
        assert_eq!(walked.type_adjacency_cached(), 0, "`{shape}` built an index on the walk-only store");
        assert!(expected.iter().any(|r| r.iter().any(|row| !row.contains("Integer(0)"))), "`{shape}` reached the target from no anchor");
        for (i, &a) in ANCHORS.iter().enumerate() {
            assert_eq!(rows(&indexed, &at(shape, a)), expected[i], "`{}`", at(shape, a));
        }
    }
}

/// A relationship created after the index was built must be walked. The store
/// drops its index on any write; an operator holding one from before must not
/// keep using it.
#[test]
fn an_edge_added_after_the_index_was_built_is_walked() {
    let mut store = social(false);
    // `*1..2`, not `*1..1`: the planner may turn a single hop into `Expand`,
    // which has its own index and threshold.
    let shape = "MATCH (p:P {id: {A}})-[:KNOWS*1..2]->(f) RETURN f.id";
    rows(&store, &every_anchor(shape));
    assert!(store.type_adjacency_cached() > 0);
    let before = rows(&store, &at(shape, 5));

    // The anchor's node, looked up rather than assumed: node ids start at 1.
    let q = parse_query("MATCH (p:P {id: 5}) RETURN p").unwrap();
    let out = QueryExecutor::new(&store).execute(&q).unwrap();
    let five: NodeId = out.records[0].get("p").and_then(|v| v.node_id()).unwrap();
    let target = store.create_node("P");
    store.set_node_property("default", target, "id", 9_999i64).unwrap();
    store.create_edge(five, target, "KNOWS").unwrap();

    let after = rows(&store, &at(shape, 5));
    assert_eq!(after.len(), before.len() + 1, "the new KNOWS edge was not walked: {after:?}");
    assert!(after.iter().any(|r| r.contains("9999")));
    let _ = PropertyValue::Null;
}

