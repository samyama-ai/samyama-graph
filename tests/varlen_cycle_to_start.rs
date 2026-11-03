//! A variable-length pattern back to its own start variable, `(x)-[*]->(x)`,
//! must end at that node (#1237).
//!
//! The walk rebound `x` to wherever each path ended, so the closing `(x)`
//! constrained nothing: `MATCH (x:N)-[:T*]->(x)` counted all 12 `:T` paths of
//! a graph with no `:T` cycle. Every expected answer here is Neo4j 2026.04.0's
//! on the same graph, the controls included.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `a→b→c→d`, `a→d`, `d→e` over `:T`, and `e→a` over `:U`: two directed cycles
/// through `:U` (`a→b→c→d→e→a`, `a→d→e→a`), none over `:T` alone.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let q = "CREATE (a:N {n: 'a'}), (b:N {n: 'b'}), (c:N {n: 'c'}), (d:N {n: 'd'}), (e:N {n: 'e'}), \
             (a)-[:T {w: 1}]->(b), (b)-[:T {w: 2}]->(c), (c)-[:T {w: 3}]->(d), (a)-[:T {w: 9}]->(d), \
             (d)-[:T {w: 4}]->(e), (e)-[:U {w: 5}]->(a)";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    s
}

fn cell(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "null".into(),
        other => format!("{other:?}"),
    }
}

/// Every row as `col1|col2|...`, sorted.
fn check(q: &str, expected: &[&str]) {
    let s = graph();
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(&s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| out.columns.iter().map(|c| cell(r.get(c))).collect::<Vec<_>>().join("|"))
        .collect();
    got.sort();
    assert_eq!(got, expected, "{q}");
}

const PER_NODE: &[&str] = &["a|2", "b|1", "c|1", "d|2", "e|2"];

#[test]
fn no_cycle_over_the_type_means_no_rows() {
    check("MATCH (x:N)-[:T*]->(x) RETURN count(*) AS c", &["0"]);
    check("MATCH (x:N)-[*2..2]->(x) RETURN count(*) AS c", &["0"]);
}

#[test]
fn cycles_are_counted_per_start_node() {
    check("MATCH (x:N)-[*]->(x) RETURN x.n, count(*) AS c", PER_NODE);
    check("MATCH (x:N)-[*1..5]->(x) RETURN x.n, count(*) AS c", PER_NODE);
    check("MATCH (x)-[*]->(x) RETURN count(*) AS c", &["8"]);
    check("MATCH (x:N)<-[*]-(x) RETURN count(*) AS c", &["8"]);
    check("MATCH (x:N)-[*]-(x) RETURN count(*) AS c", &["24"]);
}

#[test]
fn the_path_ends_where_it_started() {
    check(
        "MATCH p = (x:N)-[*]->(x) RETURN x.n, length(p) AS len",
        &["a|3", "a|5", "b|5", "c|5", "d|3", "d|5", "e|3", "e|5"],
    );
    // Every node lies on a cycle, so a walk that never returns to its start
    // (a BFS that marks it visited) would find none of them.
    check("MATCH (x:N)-[*]->(x) RETURN DISTINCT x.n", &["a", "b", "c", "d", "e"]);
}

#[test]
fn a_start_bound_by_an_earlier_clause() {
    check("MATCH (x:N) MATCH (x)-[*]->(x) RETURN x.n, count(*) AS c", PER_NODE);
    check("MATCH (x:N) OPTIONAL MATCH (x)-[:T*]->(x) RETURN x.n, count(*) AS c", &["a|1", "b|1", "c|1", "d|1", "e|1"]);
}

/// Right before and after: the WHERE form, a start pinned by a property, and a
/// fixed-length cycle.
#[test]
fn controls() {
    check("MATCH (x:N)-[*]->(y) WHERE x = y RETURN x.n, count(*) AS c", PER_NODE);
    check("MATCH (x:N {n: 'a'})-[*]->(x) RETURN count(*) AS c", &["2"]);
    check("MATCH (x:N)-[:T]->()-[:T]->(x) RETURN count(*) AS c", &["0"]);
    check("MATCH (a:N {n: 'a'})-[:T*]->(x) RETURN x.n, count(*) AS c", &["b|1", "c|1", "d|2", "e|2"]);
}
