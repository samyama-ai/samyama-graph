//! A fourth sweep: type coercion, comparison and ordering across types.
//!
//! Sweeps 1–3 covered scalar expressions, writes/paths/temporal, and
//! clause-level surface; all three pass. This covers where values of different
//! types meet — a classic source of *silently empty* results, and the subject
//! of a standing note that "float-vs-int WHERE silently returns nothing".
//!
//! A query that returns no rows looks like a true negative. That is what makes
//! this class worth sweeping deliberately rather than waiting to trip over it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn render(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(p)) => match p {
            PropertyValue::Integer(n) => n.to_string(),
            PropertyValue::Float(f) => format!("{f}"),
            PropertyValue::String(s) => format!("\"{s}\""),
            PropertyValue::Boolean(b) => b.to_string(),
            PropertyValue::Null => "null".into(),
            PropertyValue::Array(a) => format!("[{}]", a.iter().map(|x| match x {
                PropertyValue::Integer(n) => n.to_string(),
                PropertyValue::Float(f) => format!("{f}"),
                PropertyValue::String(s) => format!("\"{s}\""),
                other => format!("{other:?}"),
            }).collect::<Vec<_>>().join(", ")),
            other => format!("{other:?}"),
        },
        Some(Value::Null) | None => "null".into(),
        other => format!("{other:?}"),
    }
}

/// Nodes carrying the same logical numbers stored as Integer and as Float,
/// so a comparison that fails to coerce shows up as a missing row.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    for (name, age_i, score_f) in [("Ada", 36i64, 9.5f64), ("Bob", 41, 7.0), ("Cy", 20, 10.0)] {
        let id = store.create_node("P");
        let _ = store.set_node_property("default", id, "name", PropertyValue::String(name.into()));
        let _ = store.set_node_property("default", id, "age", PropertyValue::Integer(age_i));
        let _ = store.set_node_property("default", id, "score", PropertyValue::Float(score_f));
    }
    store
}

fn main() {
    let store = fixture();
    let cases: &[(&str, &str, &str)] = &[
        // --- int property against a float literal, and the reverse
        ("int prop = float literal", "MATCH (p:P) WHERE p.age = 36.0 RETURN count(p) AS r", "1"),
        ("int prop > float literal", "MATCH (p:P) WHERE p.age > 35.5 RETURN count(p) AS r", "2"),
        ("int prop < float literal", "MATCH (p:P) WHERE p.age < 36.5 RETURN count(p) AS r", "2"),
        ("float prop = int literal", "MATCH (p:P) WHERE p.score = 7 RETURN count(p) AS r", "1"),
        ("float prop > int literal", "MATCH (p:P) WHERE p.score > 9 RETURN count(p) AS r", "2"),
        ("float prop <= int literal", "MATCH (p:P) WHERE p.score <= 10 RETURN count(p) AS r", "3"),
        // --- arithmetic mixing
        ("int + float", "RETURN 1 + 1.5 AS r", "2.5"),
        ("int / int truncates", "RETURN 7 / 2 AS r", "3"),
        ("int / float", "RETURN 7 / 2.0 AS r", "3.5"),
        ("float compared after arithmetic", "MATCH (p:P) WHERE p.age * 1.0 = 36.0 RETURN count(p) AS r", "1"),
        // --- comparison across incompatible types (Cypher: null, not error)
        ("int vs string is null", "RETURN 1 = \"1\" AS r", "false"),
        ("int < string", "RETURN (1 < \"a\") IS NULL AS r", "true"),
        ("null comparison", "RETURN (1 < null) IS NULL AS r", "true"),
        // --- IN with mixed numeric types
        ("int IN a float list", "MATCH (p:P) WHERE p.age IN [36.0, 99.0] RETURN count(p) AS r", "1"),
        ("float IN an int list", "MATCH (p:P) WHERE p.score IN [7, 99] RETURN count(p) AS r", "1"),
        // --- ordering across types
        ("ORDER BY an int property", "MATCH (p:P) RETURN p.name AS r ORDER BY p.age ASC LIMIT 1", "\"Cy\""),
        ("ORDER BY a float property", "MATCH (p:P) RETURN p.name AS r ORDER BY p.score DESC LIMIT 1", "\"Cy\""),
        // --- boundary values
        ("large integer round-trips", "RETURN 9007199254740993 AS r", "9007199254740993"),
        ("negative zero float", "RETURN -0.0 = 0.0 AS r", "true"),
        ("integer overflow-ish literal", "RETURN 9223372036854775807 AS r", "9223372036854775807"),
        // --- string/number conversion
        ("toInteger of a float", "RETURN toInteger(3.9) AS r", "3"),
        ("toInteger of a bad string", "RETURN toInteger(\"abc\") IS NULL AS r", "true"),
        ("toFloat of an int", "RETURN toFloat(3) AS r", "3"),
        ("toString of a float", "RETURN toString(1.5) AS r", "\"1.5\""),
        // --- aggregate typing
        ("sum of ints stays int", "MATCH (p:P) RETURN sum(p.age) AS r", "97"),
        ("sum of floats is float", "MATCH (p:P) RETURN sum(p.score) AS r", "26.5"),
        ("avg of ints is float", "MATCH (p:P) RETURN avg(p.age) AS r", "32.333333333333336"),
        ("min across ints", "MATCH (p:P) RETURN min(p.age) AS r", "20"),
        ("max across floats", "MATCH (p:P) RETURN max(p.score) AS r", "10"),
        // --- boolean coercion
        ("boolean property filter", "RETURN true AND (1 < 2) AS r", "true"),
    ];

    let mut wrong = Vec::new();
    let mut errored = Vec::new();
    for (label, cypher, expected) in cases {
        match parse_query(cypher) {
            Err(e) => errored.push((label.to_string(), cypher.to_string(), format!("parse: {e:?}"))),
            Ok(q) => match QueryExecutor::new(&store).execute(&q) {
                Err(e) => errored.push((label.to_string(), cypher.to_string(), format!("exec: {e:?}"))),
                Ok(b) => {
                    let got = b.records.first().map(|r| render(r.get("r"))).unwrap_or("<no rows>".into());
                    if got != *expected {
                        wrong.push((label.to_string(), cypher.to_string(), expected.to_string(), got));
                    }
                }
            },
        }
    }
    println!("{} cases: {} ok, {} wrong answer, {} errored",
        cases.len(), cases.len()-wrong.len()-errored.len(), wrong.len(), errored.len());
    if !wrong.is_empty() {
        println!("\n=== WRONG ANSWER (parses, runs, returns the wrong thing) ===");
        for (l,q,e,g) in &wrong { println!("  {l}\n     {q}\n     expected {e}, got {g}"); }
    }
    if !errored.is_empty() {
        println!("\n=== ERRORED (at least it says so) ===");
        for (l,q,e) in &errored { println!("  {l}\n     {q}\n     {e}"); }
    }
}
