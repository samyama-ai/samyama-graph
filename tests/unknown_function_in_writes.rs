//! An unknown function in an updating clause is refused, like one in RETURN (#1897).
//!
//! ```cypher
//! MATCH (p:V {name: 'Alice'}) SET p.embedding = vecf32([0.1, 0.2])
//! ```
//!
//! reported success and stored nothing. `RETURN vecf32(...)` was refused at
//! compile time (#947), but that check walked the reading clauses only, so
//! the same call on the right of a SET reached the executor. There the failed
//! call became `null`, and a null SET *removes* the property: a statement
//! that could not run reported success and deleted what was there.
//!
//! Two fixes, both pinned here: the compile-time check covers every
//! updating clause, and a SET whose value fails at run time is an error.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn refused(cypher: &str) -> String {
    match parse_query(cypher) {
        Ok(_) => panic!("`{cypher}` should be refused"),
        Err(e) => format!("{e:?}"),
    }
}

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

fn name_of_alice(s: &GraphStore, prop: &str) -> Option<PropertyValue> {
    let q = format!("MATCH (p:V {{name: 'Alice'}}) RETURN p.{prop} AS v");
    let p = parse_query(&q).unwrap();
    let out = QueryExecutor::new(s).execute(&p).unwrap();
    match out.records[0].get("v") {
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
        Some(Value::Property(v)) => Some(v.clone()),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn the_reported_statement_is_refused() {
    let err = refused("MATCH (p:V {name: 'Alice'}) SET p.embedding = vecf32([0.1, 0.2])");
    assert!(err.contains("vecf32"), "the message names the function: {err}");
    refused("MATCH (p:V {name: 'Alice'}) SET p.x = noSuchFunction(1)");
}

#[test]
fn every_updating_clause_is_checked() {
    // Each position an updating clause evaluates an expression in.
    for q in [
        "MATCH (n) SET n = {x: noSuchFunction(1)}",
        "MATCH (n) SET n += {x: noSuchFunction(1)}",
        "MATCH (n) SET n.a = 1 SET n.b = noSuchFunction(1)",
        "MERGE (n:V {name: 'A'}) ON CREATE SET n.x = noSuchFunction(1)",
        "MERGE (n:V {name: 'A'}) ON MATCH SET n.x = noSuchFunction(1)",
        "MERGE (n:V {name: 'A'}) ON CREATE SET n = {x: noSuchFunction(1)}",
        "MERGE (n:V {name: 'A'}) SET n.x = noSuchFunction(1)",
        "CREATE (n:V {x: noSuchFunction(1)})",
        "FOREACH (x IN noSuchFunction(1) | CREATE (:V))",
        "MATCH (n) FOREACH (x IN [1] | SET n.x = noSuchFunction(x))",
        "UNWIND [1] AS i MATCH (n) SET n.x = noSuchFunction(i)",
    ] {
        let err = refused(q);
        assert!(err.contains("noSuchFunction"), "`{q}`: {err}");
    }
}

#[test]
fn known_functions_in_updating_clauses_are_still_accepted() {
    // The check must not over-reject: these are the shapes it now walks.
    for q in [
        "MATCH (n) SET n.x = toUpper('a'), n.t = timestamp()",
        "MATCH (n) SET n = {x: size([1, 2])}",
        "MERGE (n:V {name: 'A'}) ON CREATE SET n.id = randomUUID()",
        "CREATE (n:V {x: abs(-1)})",
        "MATCH (n) FOREACH (x IN range(1, 3) | SET n.x = x)",
        // Namespaced names stay allowed, as they are in RETURN.
        "MATCH (n) SET n.d = date.truncate('day', date())",
    ] {
        assert!(parse_query(q).is_ok(), "`{q}` should be accepted: {:?}", parse_query(q).err());
    }
}

#[test]
fn a_set_whose_value_fails_at_run_time_is_an_error_and_writes_nothing() {
    // Past the compile-time check, so it exercises the executor: integer
    // division by zero is a run-time error, and dividing by `p.x - 5` keeps it
    // out of reach of any constant folding. It used to become null, and a null SET
    // removed the property it was meant to write.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:V {name: 'Alice', x: 5})");
    let p = parse_query("MATCH (p:V {name: 'Alice'}) SET p.x = 1 / (p.x - 5)").expect("parses");
    let result = MutQueryExecutor::new(&mut s, "default".into()).execute(&p);
    let err = result.expect_err("a failing value is the statement's error");
    assert!(format!("{err}").contains("Division by zero"), "{err}");
    assert_eq!(name_of_alice(&s, "x"), Some(PropertyValue::Integer(5)), "x is left as it was");
}
