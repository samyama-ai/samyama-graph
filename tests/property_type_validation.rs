//! A property is a scalar or a list of scalars (#975).
//!
//! ```cypher
//! CREATE (a) SET a.maplist = [{num: 1}]
//! ```
//!
//! succeeded, storing an `Array([Map(..)])` — something `properties(a)` hands
//! back and no Cypher expression can build. openCypher wants
//! `TypeError: InvalidPropertyType`.
//!
//! SET has its **own** value conversion, a fourth copy of the same logic, so
//! the shared `storable_property` never saw the value. The check is applied to
//! the result instead, which covers whichever converter produced it.
//!
//! A **bare** map is deliberately left alone: storing one is a documented
//! extension (NDS-08, nested map properties) rather than an accident, and
//! turning that off is not this fix's decision. The scenario is about a list
//! *containing* one.

use samyama::graph::GraphStore;
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

fn run(cypher: &str) -> Result<(), String> {
    let mut store = GraphStore::new();
    let q = parse_query(cypher).map_err(|e| format!("parse: {e:?}"))?;
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

#[test]
fn a_list_of_maps_is_refused_by_set() {
    let err = run("CREATE (a) SET a.maplist = [{num: 1}]").unwrap_err();
    assert!(err.contains("TypeError"), "{err}");
}

#[test]
fn the_message_names_the_property() {
    let err = run("CREATE (a) SET a.maplist = [{num: 1}]").unwrap_err();
    assert!(err.contains("maplist"), "{err}");
}

#[test]
fn a_list_of_maps_is_refused_by_create_and_merge() {
    // The shared converter covers these; asserted so a future refactor cannot
    // fix one path and leave the others.
    assert!(run("CREATE ({maplist: [{num: 1}]})").is_err());
    assert!(run("MERGE ({maplist: [{num: 1}]})").is_err());
}

#[test]
fn a_nested_list_of_maps_is_refused_too() {
    assert!(run("CREATE (a) SET a.x = [[{num: 1}]]").is_err());
}

#[test]
fn a_list_of_scalars_is_fine() {
    run("CREATE (a) SET a.ok = [1, 2, 3]").unwrap();
    run("CREATE (a) SET a.ok = ['a', 'b']").unwrap();
    run("CREATE (a) SET a.ok = [1.5, true]").unwrap();
    run("CREATE (a) SET a.ok = []").unwrap();
}

#[test]
fn a_bare_map_is_left_alone() {
    // Not part of this fix. If storing one is ever withdrawn that is a
    // separate, deliberate decision — this test records which side of the line
    // it is on today.
    run("CREATE (a) SET a.m = {num: 1}").unwrap();
}

#[test]
fn ordinary_scalar_properties_are_unaffected() {
    run("CREATE (a) SET a.n = 1, a.s = 'x', a.b = true, a.f = 1.5").unwrap();
    run("CREATE (a) SET a.gone = null").unwrap();
}
