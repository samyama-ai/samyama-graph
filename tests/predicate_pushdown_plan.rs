//! A `WHERE` conjunct is applied as soon as its variables are bound (#328).
//!
//! The path builders held every predicate mentioning a non-anchor variable
//! until the whole path had been expanded. On LDBC IC3 that put a filter on
//! `m` — bound by the *first* expand — above the *second* one, so 409,960 rows
//! were carried through an expand that 622 of them survived.
//!
//! These assert on `EXPLAIN`, because the defect is a plan shape and a plan
//! shape is checkable without a profiler or a 21M-edge dataset. They also
//! assert the answers, because moving a filter earlier is only safe if it
//! removes exactly the rows the later one would have.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `Person -> Post -> Tag`, with a date on the post and a name on the tag, so
/// a three-variable pattern has one predicate per variable.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let mut person = |store: &mut GraphStore, i: i64| -> NodeId {
        let id = store.create_node("Person");
        let _ = store.set_node_property("default", id, "age".to_string(), PropertyValue::Integer(i % 60));
        id
    };
    let people: Vec<NodeId> = (0..40).map(|i| person(&mut store, i)).collect();

    let tags: Vec<NodeId> = (0..5)
        .map(|i| {
            let id = store.create_node("Tag");
            let _ = store.set_node_property(
                "default",
                id,
                "name".to_string(),
                PropertyValue::String(format!("tag{i}")),
            );
            id
        })
        .collect();

    for (i, &p) in people.iter().enumerate() {
        for j in 0..5 {
            let post = store.create_node("Post");
            let _ = store.set_node_property(
                "default",
                post,
                "created".to_string(),
                PropertyValue::Integer(((i * 5 + j) % 100) as i64),
            );
            store.create_edge(p, post, "WROTE").unwrap();
            store.create_edge(post, tags[(i + j) % tags.len()], "HAS_TAG").unwrap();
        }
    }
    store
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("EXPLAIN should run");
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => {
            t.lines().take_while(|l| !l.starts_with("---")).collect::<Vec<_>>().join("\n")
        }
        other => panic!("expected a plan string, got {other:?}"),
    }
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&query).expect("query should run").records.len()
}

/// Line index of the first row matching **every** fragment in `needles`.
///
/// EXPLAIN prints the tree root-first, so a **larger** index is deeper and
/// therefore runs **earlier**. Getting that backwards is easy — the first
/// version of these tests asserted the opposite and failed against correct
/// output.
///
/// Matching on a set of fragments rather than one literal string is
/// deliberate: the planner is free to anchor a pattern at whichever end is
/// cheaper and render the traversal reversed, so `(post)-[:HAS_TAG]->(t)` may
/// legitimately appear as `(t)<-[:HAS_TAG]-(post)`. Asserting the rendered
/// direction would be asserting a planning decision this test is not about.
fn depth_of(plan: &str, needles: &[&str]) -> usize {
    plan.lines()
        .position(|l| needles.iter().all(|n| l.contains(n)))
        .unwrap_or_else(|| panic!("no line containing all of {needles:?} in:\n{plan}"))
}

/// Asserts `earlier` executes before `later` — i.e. sits deeper in the tree.
fn runs_before(plan: &str, earlier: &[&str], later: &[&str]) {
    let e = depth_of(plan, earlier);
    let l = depth_of(plan, later);
    assert!(
        e > l,
        "{earlier:?} should run before {later:?} (deeper in the tree):\n{plan}"
    );
}

#[test]
fn a_predicate_on_an_intermediate_variable_runs_before_the_next_expand() {
    // The IC3 shape. `post.created` is bound by the first expand; the filter
    // must sit below the second one, not above it.
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (p:Person)-[:WROTE]->(post:Post)-[:HAS_TAG]->(t:Tag) \
         WHERE post.created > 50 RETURN t.name",
    );

    runs_before(&text, &["Filter", "post.created"], &["Expand", "WROTE"]);
}

#[test]
fn each_conjunct_lands_at_its_own_earliest_point() {
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (p:Person)-[:WROTE]->(post:Post)-[:HAS_TAG]->(t:Tag) \
         WHERE p.age > 10 AND post.created > 50 AND t.name = \"tag1\" RETURN t.name",
    );

    // Whichever end the planner anchors on, the predicate on the *middle*
    // variable must not be left above the expand that binds the far one.
    runs_before(&text, &["Filter", "post.created"], &["Expand", "WROTE"]);
    // And the tag predicate reaches its scan, since `t` is an endpoint.
    // Matched with "Filter" because `t.name` also appears in the projection.
    runs_before(&text, &["Filter", "t.name"], &["Expand", "HAS_TAG"]);
}

#[test]
fn moving_the_filter_earlier_does_not_change_the_answer() {
    // The whole safety argument: the rows it removes are the rows the later
    // filter would have removed. Checked against a hand-computed count.
    let store = fixture();
    let cypher = "MATCH (p:Person)-[:WROTE]->(post:Post)-[:HAS_TAG]->(t:Tag) \
                  WHERE post.created > 50 RETURN t.name";
    // 200 posts, `created` = (i*5+j) % 100, so exactly the ones above 50.
    let expected = (0..200).filter(|k| (k % 100) > 50).count();
    assert_eq!(rows(&store, cypher), expected);
}

#[test]
fn a_predicate_spanning_two_variables_waits_for_the_second() {
    // Correctness guard on the "as soon as bound" rule: a predicate is only
    // ready when *every* variable it names is bound.
    let store = fixture();
    let cypher = "MATCH (p:Person)-[:WROTE]->(post:Post)-[:HAS_TAG]->(t:Tag) \
                  WHERE p.age < post.created RETURN t.name";
    let text = plan(&store, cypher);
    // It needs both `p` and `post`, so it cannot be deeper than the expand
    // that binds the second of them.
    let filter = depth_of(&text, &["p.age < post.created"]);
    let wrote = depth_of(&text, &["Expand", "WROTE"]);
    assert!(filter < wrote, "the predicate must not run before both are bound:\n{text}");
    // And it still answers.
    assert!(rows(&store, cypher) > 0);
}

#[test]
fn a_single_hop_pattern_is_unchanged() {
    let store = fixture();
    let cypher = "MATCH (p:Person)-[:WROTE]->(post:Post) WHERE post.created > 90 RETURN post.created";
    let expected = (0..200).filter(|k| (k % 100) > 90).count();
    assert_eq!(rows(&store, cypher), expected);
}

#[test]
fn an_optional_match_keeps_its_null_rows() {
    // This asserted that the person with no post is excluded. That is not
    // Cypher: `WHERE` after an `OPTIONAL MATCH` scopes to the optional match,
    // so a row with nothing to match keeps its place with `m` NULL. TCK
    // MatchWhere6 [6] is the same shape and Neo4j 5 returns the null row —
    // checked against its actual output, not just the scenario text (#667).
    //
    // The predicate is still pushed early, which is what this file is about;
    // what changed is that it is no longer *also* applied above the join,
    // where it deleted exactly the rows the OPTIONAL MATCH produces.
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let _ = store.set_node_property("default", a, "age".to_string(), PropertyValue::Integer(20));
    let lonely = store.create_node("Person");
    let _ = store.set_node_property("default", lonely, "age".to_string(), PropertyValue::Integer(20));
    let post = store.create_node("Post");
    let _ = store.set_node_property("default", post, "created".to_string(), PropertyValue::Integer(99));
    store.create_edge(a, post, "WROTE").unwrap();

    let cypher = "MATCH (p:Person) OPTIONAL MATCH (p)-[:WROTE]->(m:Post) \
                  WHERE m.created > 50 RETURN p";
    assert_eq!(
        rows(&store, cypher),
        2,
        "both people survive; only `m` is nulled for the one with no post"
    );
}

#[test]
fn a_variable_length_segment_also_gets_its_predicate_early() {
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (p:Person)-[:WROTE*1..1]->(post:Post)-[:HAS_TAG]->(t:Tag) \
         WHERE post.created > 50 RETURN t.name",
    );
    // Matched on the relationship type rather than on `Expand`, because the
    // planner may anchor at the far end and lower this segment to a
    // `VarLengthExpand` traversed in reverse (#328 anchor selection). Which
    // operator implements the hop is not what this test is about.
    runs_before(&text, &["Filter", "post.created"], &["WROTE"]);
}

#[test]
fn a_predicate_on_an_edge_variable_is_ready_with_its_edge() {
    let store = fixture();
    let cypher = "MATCH (p:Person)-[w:WROTE]->(post:Post)-[:HAS_TAG]->(t:Tag) \
                  WHERE post.created > 50 RETURN t.name";
    // Binding an edge variable must not disturb the placement.
    let text = plan(&store, cypher);
    runs_before(&text, &["Filter", "post.created"], &["Expand", "WROTE"]);
    assert!(rows(&store, cypher) > 0);
}
