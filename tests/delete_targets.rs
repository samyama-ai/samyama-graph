//! `DELETE` takes a node, a relationship or a path (#887).
//!
//! ```cypher
//! MATCH (n) DELETE n:Person     -- a label test
//! MATCH (a) DELETE x            -- nothing named x
//! MATCH () DELETE 1 + 1         -- a number
//! ```
//!
//! All three ran and deleted nothing, reporting success. **Deleting nothing is
//! a legitimate outcome** — `MATCH (n:Nope) DELETE n` deletes nothing too — so
//! a caller could not tell "there was nothing to delete" from "I did not
//! understand what you asked me to delete".
//!
//! The scoping is most of this file. Only shapes that *cannot* be an entity are
//! refused; a function call and a **property access** are left alone, because
//! Cypher allows an expression that resolves to an entity.
//!
//! I found the property boundary the expensive way: rejecting property access
//! cost two passing scenarios, because `WITH {key: u} AS nodes DELETE nodes.key`
//! is valid — a map field can hold an entity. [`a_map_field_may_be_deleted`]
//! is that lesson, kept.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> bool {
    parse_query(cypher).is_err()
}

/// The four the TCK names.
#[test]
fn delete_refuses_what_cannot_be_an_entity() {
    assert!(refused("MATCH (n) DELETE n:Person"), "a label test");
    assert!(refused("MATCH ()-[r:T]-() DELETE r:T"), "a relationship-type test");
    assert!(refused("MATCH (a) DELETE x"), "an unbound name");
    assert!(refused("MATCH () DELETE 1 + 1"), "arithmetic");
    assert!(refused("MATCH () DELETE 42"), "a literal");
    assert!(refused("MATCH () DELETE 'a'"), "a string literal");
}

/// The ordinary forms still work, on every kind of entity.
#[test]
fn the_ordinary_forms_are_untouched() {
    for cypher in [
        "MATCH (n) DELETE n",
        "MATCH (n) DETACH DELETE n",
        "MATCH ()-[r]->() DELETE r",
        "MATCH p = ()-->() DELETE p",
        "MATCH (a), (b) DELETE a, b",
        "MATCH (n) WITH n DELETE n",
        "UNWIND [1] AS i MATCH (n) DELETE n",
    ] {
        assert!(!refused(cypher), "refused `{cypher}`");
    }
}

/// **A map field may hold an entity**, so a property access is not refused —
/// and it really deletes.
#[test]
fn a_map_field_may_be_deleted() {
    let cypher = "MATCH (u:User) WITH {key: u} AS nodes DELETE nodes.key";
    assert!(!refused(cypher));

    let mut store = GraphStore::new();
    for setup in ["CREATE (:User), (:User)"] {
        let q = parse_query(setup).expect("setup parses");
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .expect("setup runs");
    }
    let q = parse_query(cypher).expect("parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("runs");
    let count = parse_query("MATCH (n) RETURN n")
        .ok()
        .and_then(|p| QueryExecutor::new(&store).execute(&p).ok())
        .map(|b| b.records.len())
        .expect("count runs");
    assert_eq!(count, 0, "both users should be gone");
}

/// A function call is left alone too — statically deciding whether it yields an
/// entity is a different job.
#[test]
fn a_function_call_is_not_refused() {
    assert!(!refused("MATCH p = ()-->() DELETE head(nodes(p))"));
}
