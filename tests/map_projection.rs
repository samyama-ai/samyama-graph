//! Map projection, `v {.name, key: expr, other, .*}` (#1239).
//!
//! It was a parse error. It is how Neo4j-shaped clients shape a result row
//! into a map. Every expected answer here is Neo4j 2026.04.0's on the same
//! graph; maps are compared key by key, since their order carries no meaning.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let q = "CREATE (a:P {n: 'a', v: 1}), (b:P {n: 'b', v: 2}), (c:P {n: 'c', v: 3}), (a)-[:K]->(b), (b)-[:K]->(c)";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    s
}

fn render(p: &PropertyValue) -> String {
    match p {
        PropertyValue::Null => "null".into(),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Array(items) => format!("[{}]", items.iter().map(render).collect::<Vec<_>>().join(",")),
        PropertyValue::Map(m) => {
            let mut kv: Vec<String> = m.iter().map(|(k, v)| format!("{k}={}", render(v))).collect();
            kv.sort();
            kv.join(",")
        }
        other => format!("{other:?}"),
    }
}

fn value(v: &Value) -> String {
    match v {
        Value::Property(p) => render(p),
        Value::Null => "null".into(),
        Value::List(items) => format!("[{}]", items.iter().map(value).collect::<Vec<_>>().join(",")),
        // Already ordered by key (a BTreeMap).
        Value::Map(m) => m.iter().map(|(k, v)| format!("{k}={}", value(v))).collect::<Vec<_>>().join(","),
        other => format!("{other:?}"),
    }
}

fn cell(v: Option<&Value>) -> String {
    v.map(value).unwrap_or_else(|| "null".into())
}

/// Column `c` of every row, sorted.
fn col(q: &str) -> Vec<String> {
    let s = graph();
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(&s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out.records.iter().map(|r| cell(r.get("c"))).collect();
    v.sort();
    v
}

#[test]
fn properties_and_computed_keys() {
    assert_eq!(col("MATCH (p:P {n: 'a'}) RETURN p {.n, .v} AS c"), vec!["n=a,v=1"]);
    assert_eq!(col("MATCH (p:P {n: 'a'}) RETURN p {.n, double: p.v * 2} AS c"), vec!["double=2,n=a"]);
}

#[test]
fn all_properties() {
    assert_eq!(col("MATCH (p:P {n: 'a'}) RETURN p {.*} AS c"), vec!["n=a,v=1"]);
}

#[test]
fn a_variable_item_keeps_its_name() {
    assert_eq!(col("MATCH (p:P {n: 'a'}) WITH p, 7 AS seven RETURN p {.n, seven} AS c"), vec!["n=a,seven=7"]);
}

#[test]
fn a_missing_property_is_a_null_entry() {
    assert_eq!(col("MATCH (p:P {n: 'a'}) RETURN p {.n, .missing} AS c"), vec!["missing=null,n=a"]);
}

/// A null variable -- an OPTIONAL MATCH that found nothing -- projects to
/// null, not to a map of nulls.
#[test]
fn a_null_variable_projects_to_null() {
    assert_eq!(col("MATCH (p:P {n: 'c'}) OPTIONAL MATCH (p)-[:K]->(q) RETURN q {.n} AS c"), vec!["null"]);
}

#[test]
fn a_map_value_and_a_pattern_comprehension() {
    assert_eq!(col("WITH {a: 1, b: 'x'} AS m RETURN m {.a} AS c"), vec!["a=1"]);
    assert_eq!(col("MATCH (p:P {n: 'a'}) RETURN p {.n, outs: [(p)-[:K]->(q) | q.n]} AS c"), vec!["n=a,outs=[b]"]);
}

/// `EXISTS {` sits ahead of map projection in the grammar and still parses.
#[test]
fn exists_is_still_a_subquery() {
    assert_eq!(col("MATCH (p:P) WHERE EXISTS { (p)-[:K]->() } RETURN p.n AS c"), vec!["a", "b"]);
}

/// Neo4j answers `{v: 1, n: "a", extra: 1}`; that needs a map union, so the
/// form is refused with a message rather than answered with half its keys.
#[test]
fn all_properties_with_other_keys_is_refused() {
    let err = parse_query("MATCH (p:P) RETURN p {.*, extra: 1} AS c").expect_err("refused");
    assert!(err.to_string().contains("not supported"), "{err}");
}
