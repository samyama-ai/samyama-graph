//! Reading an entity the query already deleted (#905).
//!
//! ```cypher
//! MATCH (n) DELETE n RETURN n.num              -- returned null, reported success
//! MATCH ()-[r]->() DELETE r RETURN type(r)     -- "Edge not found"
//! ```
//!
//! Both directions were wrong, from one cause: whether a read resolves through
//! the store or through the reference it already holds.
//!
//! A property read went to the store, got nothing, and answered `null` — which
//! is *also* the honest answer for a property nobody set. The caller could not
//! tell "this is unset" from "you deleted this two clauses ago". openCypher
//! makes it an error, and the reason is exactly that ambiguity.
//!
//! `type()` is the opposite case. A relationship's type is **structural data
//! the reference carries** — `Value::EdgeRef(id, src, dst, type)` — so it does
//! not need the store at all. Projection materialised the reference before the
//! function ran, and materialising something deleted failed.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::graph::PropertyValue;
use samyama::query::parser::parse_query;

fn setup(cypher: &str) -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(cypher).expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn run(store: &mut GraphStore, cypher: &str) -> Result<Vec<samyama::query::executor::Record>, String> {
    let q = parse_query(cypher).map_err(|e| format!("{e:?}"))?;
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .map(|b| b.records)
        .map_err(|e| format!("{e}"))
}

#[test]
fn a_property_of_a_deleted_node_is_an_error_not_a_null() {
    let mut store = setup("CREATE ({num: 0})");
    let err = run(&mut store, "MATCH (n) DELETE n RETURN n.num").expect_err("must fail");
    assert!(err.contains("deleted"), "{err}");
}

#[test]
fn a_property_of_a_deleted_relationship_is_an_error_too() {
    let mut store = setup("CREATE ()-[:T {num: 0}]->()");
    let err = run(&mut store, "MATCH ()-[r]->() DELETE r RETURN r.num").expect_err("must fail");
    assert!(err.contains("deleted"), "{err}");
}

/// The type survives the delete, because the reference carries it.
#[test]
fn the_type_of_a_deleted_relationship_still_reads() {
    let mut store = setup("CREATE ()-[:T]->()");
    let rows = run(&mut store, "MATCH ()-[r]->() DELETE r RETURN type(r) AS t").expect("must succeed");
    assert_eq!(
        rows.first().and_then(|r| r.get("t")),
        Some(&Value::Property(PropertyValue::String("T".to_string())))
    );
}

/// The distinction this restores: an unset property is still null, and still
/// not an error.
#[test]
fn an_unset_property_on_a_live_entity_is_still_null() {
    let store = setup("CREATE (:A)");
    let q = parse_query("MATCH (n:A) RETURN n.nothing AS v").expect("parses");
    let rows = QueryExecutor::new(&store).execute(&q).expect("runs").records;
    assert!(
        matches!(
            rows.first().and_then(|r| r.get("v")),
            Some(Value::Null) | Some(Value::Property(PropertyValue::Null))
        ),
        "{:?}", rows.first()
    );
}

/// And an ordinary read of a live entity is untouched.
#[test]
fn a_live_entity_reads_normally() {
    let store = setup("CREATE (:A {num: 7})-[:T {w: 1}]->(:B)");
    for (cypher, want) in [
        ("MATCH (n:A) RETURN n.num AS v", 7i64),
        ("MATCH ()-[r:T]->() RETURN r.w AS v", 1),
    ] {
        let q = parse_query(cypher).expect("parses");
        let rows = QueryExecutor::new(&store).execute(&q).expect("runs").records;
        assert_eq!(
            rows.first().and_then(|r| r.get("v")),
            Some(&Value::Property(PropertyValue::Integer(want))),
            "{cypher}"
        );
    }
}
