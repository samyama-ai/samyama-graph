//! A SET written after MERGE runs after the MERGE.
//!
//! `plan_inner` attached write clauses by kind -- CREATE, DELETE, SET, REMOVE,
//! FOREACH, MERGE -- while the grammar fixes their textual order as CREATE,
//! MERGE, DELETE, FOREACH, SET, REMOVE. So in
//! `UNWIND rows AS r MERGE (n {id: r.id}) SET n.x = r.x` the SET ran first,
//! before `n` was bound: it wrote nothing, silently, and the MERGE then created
//! `n` without `x`. That is the standard bulk-upsert pattern (TCK Unwind1 [14]).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into()).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

/// Column `c` of every row, sorted.
fn column(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::String(x))) => x.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn unwind_merge_then_set_writes_the_merged_node() {
    let mut s = GraphStore::new();
    write(&mut s, "UNWIND [{id: 1, x: 'a'}, {id: 2, x: 'b'}] AS r MERGE (n:N {id: r.id}) SET n.x = r.x");
    assert_eq!(column(&s, "MATCH (n:N) RETURN n.x AS c"), vec!["a", "b"]);
}

/// Both MERGE branches: id 1 exists and is matched, id 2 is created.
#[test]
fn set_after_merge_applies_on_match_and_on_create() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:N {id: 1})");
    write(&mut s, "UNWIND [{id: 1, x: 'a'}, {id: 2, x: 'b'}] AS r MERGE (n:N {id: r.id}) SET n.x = r.x");
    assert_eq!(column(&s, "MATCH (n:N) RETURN n.x AS c"), vec!["a", "b"]);
    assert_eq!(column(&s, "MATCH (n:N) RETURN count(n) AS c"), vec!["2"]);
}

#[test]
fn match_merge_then_set_writes_the_merged_node() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A {k: 7})");
    write(&mut s, "MATCH (a:A) MERGE (b:B {k: a.k}) SET b.from = a.k");
    assert_eq!(column(&s, "MATCH (b:B) RETURN b.from AS c"), vec!["7"]);
}

#[test]
fn merge_then_remove_removes_from_the_merged_node() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:N {id: 1, y: 1})");
    write(&mut s, "UNWIND [1] AS i MERGE (n:N {id: i}) REMOVE n.y");
    assert_eq!(column(&s, "MATCH (n:N) WHERE n.y IS NULL RETURN count(n) AS c"), vec!["1"]);
}

/// TCK Unwind1 [14], with the parameter as a literal.
#[test]
fn tck_unwind_with_merge() {
    let mut s = GraphStore::new();
    let p = parse_query(
        "UNWIND [{login: 'login1', name: 'name1'}, {login: 'login2', name: 'name2'}] AS prop \
         MERGE (p:Person {login: prop.login}) SET p.name = prop.name RETURN p.name AS c",
    )
    .unwrap();
    let out = MutQueryExecutor::new(&mut s, "default".into()).execute(&p).unwrap();
    let mut names: Vec<String> = out.records.iter().map(|r| format!("{:?}", r.get("c"))).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            format!("{:?}", Some(Value::Property(PropertyValue::String("name1".into())))),
            format!("{:?}", Some(Value::Property(PropertyValue::String("name2".into())))),
        ]
    );
}

/// Control: a leading MERGE had its own, correct ordering.
#[test]
fn leading_merge_then_set_still_works() {
    let mut s = GraphStore::new();
    write(&mut s, "MERGE (m:M {id: 1}) SET m.x = 5");
    assert_eq!(column(&s, "MATCH (m:M) RETURN m.x AS c"), vec!["5"]);
}
