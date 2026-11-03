//! A later MATCH that reaches a bound variable continues the walk from it,
//! whichever end of the path the variable is written at (#1613).
//!
//! `MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: ...}) MATCH (liker:Person)-[:LIKES]->(p)`
//! planned its second clause standalone -- a scan of every `:Person` expanded
//! over every `LIKES` -- and hash-joined back to the handful of posts the
//! first clause had in hand. The pushdown that chains a clause onto the rows
//! already bound (#711) required the bound variable to be the path's *start*;
//! a path written towards it is the same relation read the other way, so it
//! is now walked from its end. LDBC BI-6 and BI-8 are this shape.
//!
//! The same builder also applies a far-end node's inline properties during
//! the walk rather than to the rows it produces, which is what BI-9's
//! `(t2:Tag {name: "Afghanistan"})` needed: it was a filter over every tag of
//! every post in each forum.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `hubs` posts, each tagged with the one `:Tag {name: "x"}` and with
/// `tags_per_post` other tags; `people` persons who each like every post.
fn fixture(hubs: usize, tags_per_post: usize, people: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let x = store.create_node("Tag");
    let _ = store.set_node_property("default", x, "name".to_string(), PropertyValue::String("x".into()));
    let mut posts = Vec::new();
    for i in 0..hubs {
        let p = store.create_node("Post");
        let _ = store.set_node_property("default", p, "id".to_string(), PropertyValue::Integer(i as i64));
        store.create_edge(p, x, "HAS_TAG").unwrap();
        for j in 0..tags_per_post {
            let t = store.create_node("Tag");
            let _ = store.set_node_property(
                "default",
                t,
                "name".to_string(),
                PropertyValue::String(format!("t{i}_{j}")),
            );
            store.create_edge(p, t, "HAS_TAG").unwrap();
        }
        posts.push(p);
    }
    for i in 0..people {
        let person = store.create_node("Person");
        let _ = store.set_node_property("default", person, "id".to_string(), PropertyValue::Integer(i as i64));
        for p in &posts {
            store.create_edge(person, *p, "LIKES").unwrap();
        }
    }
    store
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("n") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{cypher}: {other:?}"),
    }
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_path_written_towards_the_bound_variable_is_walked_from_it() {
    let store = fixture(3, 2, 5);
    let towards = "MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: 'x'}) \
                   MATCH (liker:Person)-[:LIKES]->(p) RETURN count(liker) AS n";
    let text = plan(&store, towards);
    assert!(!text.contains("HashJoin"), "{text}");
    assert!(text.contains("(p)<-[:LIKES]-(liker)"), "{text}");
    // The same question with the clause written from `p`, which already chained.
    let from = "MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: 'x'}) \
                MATCH (p)<-[:LIKES]-(liker:Person) RETURN count(liker) AS n";
    assert_eq!(count(&store, towards), count(&store, from));
    assert_eq!(count(&store, towards), 15);

    // A path variable keeps the clause standalone: the path is bound as
    // written, and reversing it would reverse what `p2` holds.
    let named = "MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: 'x'}) \
                 MATCH p2=(liker:Person)-[:LIKES]->(p) RETURN count(p2) AS n";
    assert!(plan(&store, named).contains("HashJoin"), "{}", plan(&store, named));
    assert_eq!(count(&store, named), 15);
}

#[test]
fn a_bound_variable_in_the_middle_still_joins() {
    // `m` is bound and sits between two new variables: chaining would rebind
    // the far side, so this shape keeps the join (#360).
    let store = fixture(2, 1, 2);
    let q = "MATCH (m:Post)-[:HAS_TAG]->(t:Tag {name: 'x'}) \
             MATCH (a:Person)-[:LIKES]->(m)-[:HAS_TAG]->(b:Tag) RETURN count(*) AS n";
    assert!(plan(&store, q).contains("HashJoin"), "{}", plan(&store, q));
    assert_eq!(count(&store, q), 2 * 2 * 2);
}

/// The far-end inline property is applied during the walk: pinned as a
/// **ratio in one process** against the same walk with the property moved to
/// a WHERE, which has been pushed down since #656. Equal work, so the ratio
/// is near one; before this the inline form built a row per tag and read the
/// property from each.
#[test]
fn an_inline_property_on_the_far_end_is_applied_during_the_walk() {
    use std::time::Instant;
    let store = fixture(200, 40, 1);
    let inline = "MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: 'x'}) \
                  MATCH (p)-[:HAS_TAG]->(t2:Tag {name: 't7_3'}) RETURN count(*) AS n";
    let where_form = "MATCH (p:Post)-[:HAS_TAG]->(t:Tag {name: 'x'}) \
                      MATCH (p)-[:HAS_TAG]->(t2:Tag) WHERE t2.name = 't7_3' RETURN count(*) AS n";
    assert_eq!(count(&store, inline), 1);
    assert_eq!(count(&store, inline), count(&store, where_form));
    let best = |cypher: &str| {
        let query = parse_query(cypher).unwrap();
        let mut best = f64::MAX;
        for _ in 0..9 {
            let t = Instant::now();
            QueryExecutor::new(&store).execute(&query).unwrap();
            best = best.min(t.elapsed().as_secs_f64());
        }
        best
    };
    let _ = best(where_form);
    let _ = best(inline);
    let w = best(where_form);
    let i = best(inline);
    let ratio = i / w;
    eprintln!("inline far-end property: {i:.6}s against {w:.6}s for the WHERE form, {ratio:.2}x");
    assert!(
        ratio < 2.0,
        "the inline property costs {ratio:.2}x the WHERE form of the same constraint \
         ({i:.6}s against {w:.6}s) (#1613)"
    );
}
