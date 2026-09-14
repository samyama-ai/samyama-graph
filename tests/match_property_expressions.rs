//! A MATCH property whose value is not a literal filters like a WHERE.
//!
//! `MATCH (n:N {id: r.id})` was refused at planning: "MATCH does not yet
//! support a non-literal property value". So the standard batch lookup,
//! `UNWIND $rows AS r MATCH (n:N {id: r.id})`, was an error (TCK Unwind1 [6]).
//! For a MATCH the two forms mean the same thing, so the property becomes
//! `WHERE n.id = r.id`.
//!
//! Where that rewrite could change what the query means it is not made, and
//! the refusal stands: a group with an OPTIONAL MATCH (its WHERE filters only
//! the optional part) and an anonymous node (nothing to name in the WHERE).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into()).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

/// Column `c` of every row, in result order.
fn column(s: &GraphStore, q: &str) -> Result<Vec<String>, String> {
    let p = parse_query(q).map_err(|e| e.to_string())?;
    let out = QueryExecutor::new(s).execute(&p).map_err(|e| e.to_string())?;
    Ok(out
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::String(x))) => x.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect())
}

fn three_ns() -> GraphStore {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:N {id: 1, v: 'a'}), (:N {id: 2, v: 'b'}), (:N {id: 3, v: 'c'})");
    s
}

#[test]
fn a_batch_lookup_by_an_unwound_value() {
    let s = three_ns();
    assert_eq!(column(&s, "UNWIND [1, 3] AS i MATCH (n:N {id: i}) RETURN n.v AS c ORDER BY c").unwrap(), vec!["a", "c"]);
}

#[test]
fn a_property_from_an_earlier_match() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A {k: 2}), (:B {k: 1, name: 'x'}), (:B {k: 2, name: 'y'})");
    assert_eq!(column(&s, "MATCH (a:A) MATCH (b:B {k: a.k}) RETURN b.name AS c").unwrap(), vec!["y"]);
}

#[test]
fn combined_with_a_written_where() {
    let s = three_ns();
    assert_eq!(
        column(&s, "UNWIND [1, 2, 3] AS i MATCH (n:N {id: i}) WHERE n.v <> 'b' RETURN n.v AS c ORDER BY c").unwrap(),
        vec!["a", "c"]
    );
}

#[test]
fn after_a_with() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A {k: 2}), (:B {k: 1, name: 'x'}), (:B {k: 2, name: 'y'})");
    assert_eq!(column(&s, "MATCH (a:A) WITH a.k AS k MATCH (b:B {k: k}) RETURN b.name AS c").unwrap(), vec!["y"]);
}

#[test]
fn a_relationship_property() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (a:A {k: 7})-[:R {w: 7}]->(:B), (a)-[:R {w: 8}]->(:B)");
    assert_eq!(column(&s, "MATCH (a:A)-[r:R {w: a.k}]->(b:B) RETURN count(r) AS c").unwrap(), vec!["1"]);
}

/// TCK Unwind1 [6], with the parameter as a literal.
#[test]
fn tck_creating_nodes_from_an_unwound_list() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:Year {year: 2016})");
    let p = parse_query(
        "UNWIND [{year: 2016, id: 1}, {year: 2016, id: 2}] AS event \
         MATCH (y:Year {year: event.year}) \
         MERGE (e:Event {id: event.id}) \
         MERGE (y)<-[:IN]-(e) \
         RETURN e.id AS c ORDER BY c",
    )
    .unwrap();
    let out = MutQueryExecutor::new(&mut s, "default".into()).execute(&p).unwrap();
    let got: Vec<String> = out.records.iter().map(|r| format!("{:?}", r.get("c"))).collect();
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(column(&s, "MATCH (:Year)<-[r:IN]-(:Event) RETURN count(r) AS c").unwrap(), vec!["2"]);
}

/// Where the rewrite would change meaning, the refusal stands.
#[test]
fn optional_and_anonymous_patterns_are_still_refused() {
    let s = three_ns();
    assert!(column(&s, "UNWIND [1] AS i OPTIONAL MATCH (n:N {id: i}) RETURN n.v AS c").is_err());
    assert!(column(&s, "UNWIND [1] AS i MATCH (:N {id: i}) RETURN i AS c").is_err());
}

/// Control: a literal property is unchanged.
#[test]
fn a_literal_property_still_works() {
    let s = three_ns();
    assert_eq!(column(&s, "MATCH (n:N {id: 2}) RETURN n.v AS c").unwrap(), vec!["b"]);
}
