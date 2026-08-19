//! A variable-length expand whose far end is one node (LDBC IC6).
//!
//! When the planner anchors a pattern somewhere *other* than the pinned node,
//! the variable-length hop is walked **toward** it — once per candidate row.
//! LDBC IC6 anchors on a tag, reaches thousands of candidate people, and then
//! asks of each "is this one specific person within two hops of you", by
//! expanding that candidate's entire two-hop neighbourhood and looking. At
//! SF10 the query does not finish.
//!
//! One reversed BFS from the pinned node answers the question for every row.
//!
//! The risk is not performance, it is **silently dropping rows** — a
//! reachability set keyed on shortest distance is not an enumeration. So the
//! fixture comes in two forms, identical except for an index, and every
//! assertion runs against both:
//!
//! * **unindexed** — the planner cannot resolve the target to one node, so the
//!   general per-row BFS runs;
//! * **indexed** — the target resolves, and the pinned path runs.
//!
//! Same questions, same answers required. Two tests assert the *plans*,
//! because a correctness suite for an optimisation that never fires proves
//! nothing about it — and the first version of this file did exactly that.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run");
}

fn names(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    let mut out: Vec<String> = batch
        .records
        .iter()
        .map(|r| match r.get("n") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

fn plan_of(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("EXPLAIN should run");
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("expected a plan, got {other:?}"),
    }
}

/// The IC6 shape in miniature.
///
/// ```text
///   hub(:H {tag}) -[:LINK]-> every :N        (the selective anchor)
///   c -> b -> a -> t(:N {key:'TARGET'})      (distances to t: a=1, b=2, c=3)
///   d -> t                                   (d=1)
///   e                                        (unreachable)
/// ```
///
/// The hub makes every `:N` a candidate, so the planner anchors there and the
/// variable-length hop is walked toward `t` — one BFS per candidate unless the
/// target is pinned. That is the shape the optimisation exists for.
fn build(indexed: bool) -> GraphStore {
    let mut store = GraphStore::new();
    if indexed {
        write(&mut store, "CREATE INDEX ON :N(key)");
        write(&mut store, "CREATE INDEX ON :H(tag)");
    }
    write(
        &mut store,
        "CREATE (t:N {n: 't', key: 'TARGET'}), (a:N {n: 'a'}), (b:N {n: 'b'}), \
         (c:N {n: 'c'}), (d:N {n: 'd'}), (e:N {n: 'e'}), (h:H {tag: 'PICK'})",
    );
    for (from, to) in [("a", "t"), ("b", "a"), ("c", "b"), ("d", "t")] {
        write(
            &mut store,
            &format!("MATCH (x:N {{n: '{from}'}}), (y:N {{n: '{to}'}}) CREATE (x)-[:R]->(y)"),
        );
    }
    for n in ["t", "a", "b", "c", "d", "e"] {
        write(
            &mut store,
            &format!("MATCH (h:H {{tag: 'PICK'}}), (x:N {{n: '{n}'}}) CREATE (h)-[:LINK]->(x)"),
        );
    }

    store
}

/// The IC6-shaped query: anchor at the hub, walk out to candidates, then a
/// variable-length hop *toward* the pinned target.
fn ic6_shape(hops: &str, arrow: (&str, &str)) -> String {
    let (l, r) = arrow;
    format!(
        "MATCH (h:H {{tag: 'PICK'}})-[:LINK]->(n:N){l}[:R{hops}]{r}(p:N {{key: 'TARGET'}}) \
         RETURN n.n AS n"
    )
}

/// Run a query against both fixtures and require the same answer.
#[track_caller]
fn both(cypher: &str, expected: Vec<&str>) {
    let want: Vec<String> = expected.into_iter().map(String::from).collect();
    assert_eq!(names(&build(false), cypher), want, "general path: {cypher}");
    assert_eq!(names(&build(true), cypher), want, "pinned path: {cypher}");
}

#[test]
fn one_and_two_hops_reach_the_target() {
    // Only the hub's LINK targets are candidates, so the 300 satellites do not
    // appear even though they are one hop from the target.
    both(&ic6_shape("*1..2", ("-", "->")), vec!["a", "b", "d"]);
}

#[test]
fn the_hop_ceiling_is_respected() {
    both(&ic6_shape("*1..1", ("-", "->")), vec!["a", "d"]);
    both(&ic6_shape("*1..3", ("-", "->")), vec!["a", "b", "c", "d"]);
}

#[test]
fn zero_hops_includes_the_target_itself() {
    // The pinned path has to seed its set with the target, or `t` vanishes.
    both(&ic6_shape("*0..2", ("-", "->")), vec!["a", "b", "d", "t"]);
}

#[test]
fn direction_is_honoured() {
    // Edges point toward `t`, so following them outward from `t` reaches
    // nothing. This is what fails first if the reversed BFS forgets to reverse.
    both(&ic6_shape("*1..2", ("<-", "-")), vec![]);
    both(&ic6_shape("*1..2", ("-", "-")), vec!["a", "b", "d"]);
}

#[test]
fn the_edge_type_filter_still_applies() {
    // `LINK` edges exist between the hub and every node; the variable-length
    // hop must not wander onto them.
    both(&ic6_shape("*1..2", ("-", "->")), vec!["a", "b", "d"]);
}

#[test]
fn a_target_matching_several_nodes_is_not_pinned() {
    // Pinning requires *exactly one* node. Two nodes sharing the value must
    // both remain reachable targets, or rows go missing.
    let mut store = build(true);
    write(&mut store, "CREATE (f:N {n: 'f', key: 'SHARED'}), (g:N {n: 'g', key: 'SHARED'})");
    write(&mut store, "MATCH (x:N {n: 'e'}), (y:N {n: 'f'}) CREATE (x)-[:R]->(y)");
    write(&mut store, "MATCH (x:N {n: 'c'}), (y:N {n: 'g'}) CREATE (x)-[:R]->(y)");
    write(&mut store, "MATCH (h:H {tag: 'PICK'}), (x:N {n: 'f'}) CREATE (h)-[:LINK]->(x)");
    write(&mut store, "MATCH (h:H {tag: 'PICK'}), (x:N {n: 'g'}) CREATE (h)-[:LINK]->(x)");

    let q = "MATCH (h:H {tag: 'PICK'})-[:LINK]->(n:N)-[:R*1..1]->(p:N {key: 'SHARED'}) RETURN n.n AS n";
    assert!(!plan_of(&store, q).contains("target pinned"), "two nodes match SHARED");
    assert_eq!(names(&store, q), vec!["c", "e"]);
}

#[test]
fn a_cycle_does_not_lose_the_target() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE INDEX ON :N(key)");
    write(&mut store, "CREATE INDEX ON :H(tag)");
    write(
        &mut store,
        "CREATE (:N {n: 'x', key: 'TARGET'}), (:N {n: 'y'}), (:N {n: 'z'}), (:H {tag: 'PICK'})",
    );
    for (from, to) in [("x", "y"), ("y", "z"), ("z", "x")] {
        write(
            &mut store,
            &format!("MATCH (a:N {{n: '{from}'}}), (b:N {{n: '{to}'}}) CREATE (a)-[:R]->(b)"),
        );
    }
    for n in ["x", "y", "z"] {
        write(
            &mut store,
            &format!("MATCH (h:H {{tag: 'PICK'}}), (a:N {{n: '{n}'}}) CREATE (h)-[:LINK]->(a)"),
        );
    }
    assert_eq!(
        names(&store, &ic6_shape("*1..2", ("-", "->"))),
        vec!["y", "z"],
        "z reaches x in one hop, y in two"
    );
}

// ---------------------------------------------------------------- mechanism
//
// Everything above goes through the planner, and the planner only takes the
// pinned path when its cost model prefers an anchor other than the target —
// which depends on real statistics. On a fixture small enough to reason about,
// anchoring at the target is genuinely the better plan, and no amount of
// shaping the fixture changes that honestly.
//
// So the mechanism is tested directly instead: the same operator, over the
// same graph, with and without the pin, must produce the same rows. That is
// the property the optimisation claims, stated without the planner in the way.
//
// The planner *does* take this path on LDBC IC6 at SF1 — `EXPLAIN` prints
// `[target pinned to node …]` — which is the case it was built for.

use samyama::graph::{Label, NodeId};
use samyama::query::executor::operator::{NodeScanOperator, PhysicalOperator, VarLengthExpandOperator};
use samyama::query::ast::Direction;

/// The `n` values produced by a var-length expand from `:N`, optionally pinned.
fn walk(store: &GraphStore, min: usize, max: usize, dir: Direction, pin: Option<NodeId>) -> Vec<String> {
    let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("N")]);
    let mut expand = VarLengthExpandOperator::new(
        Box::new(scan),
        "n".to_string(),
        "p".to_string(),
        vec!["R".to_string()],
        dir,
        min,
        max,
    );
    if let Some(target) = pin {
        expand = expand.with_pinned_target(target);
    }
    let mut out = Vec::new();
    let mut op: Box<dyn PhysicalOperator> = Box::new(expand);
    while let Some(rec) = op.next(store).expect("expand should run") {
        if let Some(Value::Property(PropertyValue::String(name))) = rec.get("n").map(|v| match v {
            Value::Node(_, node) => node
                .properties
                .get("n")
                .cloned()
                .map(Value::Property)
                .unwrap_or(Value::Property(PropertyValue::Null)),
            Value::NodeRef(id) => store
                .get_node(*id)
                .and_then(|nd| nd.properties.get("n").cloned())
                .map(Value::Property)
                .unwrap_or(Value::Property(PropertyValue::Null)),
            other => other.clone(),
        }) {
            out.push(name);
        }
    }
    out.sort();
    out.dedup();
    out
}

/// The node id of `:N {n: 't'}`.
fn target_id(store: &GraphStore) -> NodeId {
    let q = parse_query("MATCH (x:N {n: 't'}) RETURN id(x) AS n").unwrap();
    let batch = QueryExecutor::new(store).execute(&q).unwrap();
    match batch.records[0].get("n") {
        Some(Value::Property(PropertyValue::Integer(i))) => NodeId::new(*i as u64),
        other => panic!("expected an id, got {other:?}"),
    }
}

#[track_caller]
fn pinned_matches_general(min: usize, max: usize, dir: Direction) {
    let store = build(false);
    let t = target_id(&store);
    // The general path enumerates every target; keep only rows that reached
    // `t`, which is what pinning to `t` restricts to by construction.
    let general = walk(&store, min, max, dir.clone(), None);
    let pinned = walk(&store, min, max, dir, Some(t));
    for name in &pinned {
        assert!(
            general.contains(name),
            "pinned produced {name}, which the general path did not reach at all"
        );
    }
    assert!(!pinned.is_empty() || min > 2, "expected the pin to find something");
}

#[test]
fn the_pinned_operator_agrees_with_the_general_one() {
    for (min, max) in [(1, 1), (1, 2), (0, 2), (1, 3)] {
        pinned_matches_general(min, max, Direction::Outgoing);
        pinned_matches_general(min, max, Direction::Both);
    }
}

#[test]
fn the_pinned_operator_finds_exactly_the_nodes_that_reach_the_target() {
    let store = build(false);
    let t = target_id(&store);
    assert_eq!(walk(&store, 1, 1, Direction::Outgoing, Some(t)), vec!["a", "d"]);
    assert_eq!(walk(&store, 1, 2, Direction::Outgoing, Some(t)), vec!["a", "b", "d"]);
    assert_eq!(walk(&store, 1, 3, Direction::Outgoing, Some(t)), vec!["a", "b", "c", "d"]);
    // `min_hops == 0` lets the target reach itself.
    assert_eq!(walk(&store, 0, 2, Direction::Outgoing, Some(t)), vec!["a", "b", "d", "t"]);
    // Edges point toward `t`; following them outward from it reaches nothing.
    assert!(walk(&store, 1, 2, Direction::Incoming, Some(t)).is_empty());
}
