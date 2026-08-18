//! A battery of standard Cypher expressions with hand-computed answers.
//!
//! Looking for the class of bug that #571 and #572 turned out to be: a
//! construct that parses, runs, and returns the wrong thing without erroring.
//! Each case states what it should return; anything else is reported.

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
            PropertyValue::Array(a) => format!(
                "[{}]",
                a.iter().map(|x| match x {
                    PropertyValue::Integer(n) => n.to_string(),
                    PropertyValue::String(s) => format!("\"{s}\""),
                    PropertyValue::Float(f) => format!("{f}"),
                    other => format!("{other:?}"),
                }).collect::<Vec<_>>().join(", ")
            ),
            other => format!("{other:?}"),
        },
        Some(Value::Null) | None => "null".into(),
        Some(other) => format!("{other:?}"),
    }
}

fn main() {
    let mut store = GraphStore::new();
    // A tiny fixture some cases need.
    let a = store.create_node("P");
    let _ = store.set_node_property("default", a, "name", PropertyValue::String("Ada".into()));
    let _ = store.set_node_property("default", a, "age", PropertyValue::Integer(36));
    let b = store.create_node("P");
    let _ = store.set_node_property("default", b, "name", PropertyValue::String("Bob".into()));
    let _ = store.set_node_property("default", b, "age", PropertyValue::Integer(41));
    store.create_edge(a, b, "KNOWS").unwrap();

    // (cypher, expected rendering of column `r`)
    let cases: &[(&str, &str)] = &[
        // --- list functions
        ("RETURN size([1,2,3]) AS r", "3"),
        ("RETURN head([1,2,3]) AS r", "1"),
        ("RETURN last([1,2,3]) AS r", "3"),
        ("RETURN tail([1,2,3]) AS r", "[2, 3]"),
        ("RETURN reverse([1,2,3]) AS r", "[3, 2, 1]"),
        ("RETURN range(1,3) AS r", "[1, 2, 3]"),
        ("RETURN [x IN [1,2,3,4] WHERE x > 2] AS r", "[3, 4]"),
        ("RETURN [x IN [1,2,3] | x * 2] AS r", "[2, 4, 6]"),
        ("RETURN reduce(s = 0, x IN [1,2,3] | s + x) AS r", "6"),
        ("RETURN [1,2,3][1] AS r", "2"),
        ("RETURN [1,2,3,4][1..3] AS r", "[2, 3]"),
        // --- predicate functions
        ("RETURN all(x IN [1,2,3] WHERE x > 0) AS r", "true"),
        ("RETURN any(x IN [1,2,3] WHERE x > 2) AS r", "true"),
        ("RETURN none(x IN [1,2,3] WHERE x > 5) AS r", "true"),
        ("RETURN single(x IN [1,2,3] WHERE x = 2) AS r", "true"),
        // --- map
        ("WITH {a: 1, b: 2} AS m RETURN m.a AS r", "1"),
        ("WITH {a: 1, b: 2} AS m RETURN keys(m) AS r", "[\"a\", \"b\"]"),
        ("WITH {a: 1, b: 2} AS m RETURN size(keys(m)) AS r", "2"),
        ("WITH {a: 1} AS m RETURN m[\"a\"] AS r", "1"),
        // --- string functions
        ("RETURN toUpper(\"ab\") AS r", "\"AB\""),
        ("RETURN toLower(\"AB\") AS r", "\"ab\""),
        ("RETURN trim(\"  x \") AS r", "\"x\""),
        ("RETURN substring(\"abcdef\", 1, 3) AS r", "\"bcd\""),
        ("RETURN left(\"abcdef\", 2) AS r", "\"ab\""),
        ("RETURN right(\"abcdef\", 2) AS r", "\"ef\""),
        ("RETURN replace(\"abc\", \"b\", \"X\") AS r", "\"aXc\""),
        ("RETURN split(\"a,b,c\", \",\") AS r", "[\"a\", \"b\", \"c\"]"),
        ("RETURN \"abc\" CONTAINS \"b\" AS r", "true"),
        ("RETURN \"abc\" STARTS WITH \"a\" AS r", "true"),
        ("RETURN \"abc\" ENDS WITH \"c\" AS r", "true"),
        // --- numeric
        ("RETURN abs(-3) AS r", "3"),
        ("RETURN ceil(1.2) AS r", "2"),
        ("RETURN floor(1.8) AS r", "1"),
        ("RETURN round(1.5) AS r", "2"),
        ("RETURN sign(-9) AS r", "-1"),
        ("RETURN toInteger(\"42\") AS r", "42"),
        ("RETURN toFloat(\"1.5\") AS r", "1.5"),
        ("RETURN toString(42) AS r", "\"42\""),
        ("RETURN 7 % 3 AS r", "1"),
        ("RETURN 2 ^ 3 AS r", "8"),
        // --- null handling / three-valued logic
        ("RETURN coalesce(null, 2) AS r", "2"),
        ("RETURN null IS NULL AS r", "true"),
        ("RETURN 1 IS NOT NULL AS r", "true"),
        ("RETURN null = null AS r", "null"),
        // --- CASE
        ("RETURN CASE WHEN 1 > 0 THEN \"y\" ELSE \"n\" END AS r", "\"y\""),
        ("RETURN CASE 2 WHEN 1 THEN \"a\" WHEN 2 THEN \"b\" ELSE \"c\" END AS r", "\"b\""),
        // --- aggregation
        ("UNWIND [1,2,3] AS x RETURN sum(x) AS r", "6"),
        ("UNWIND [1,2,3] AS x RETURN avg(x) AS r", "2"),
        ("UNWIND [1,2,2] AS x RETURN count(DISTINCT x) AS r", "2"),
        ("UNWIND [1,2,3] AS x RETURN collect(x) AS r", "[1, 2, 3]"),
        ("UNWIND [3,1,2] AS x RETURN min(x) AS r", "1"),
        ("UNWIND [3,1,2] AS x RETURN max(x) AS r", "3"),
        // --- graph functions
        ("MATCH (p:P) WHERE p.name = \"Ada\" RETURN labels(p) AS r", "[\"P\"]"),
        ("MATCH (p:P)-[e:KNOWS]->(q) RETURN type(e) AS r", "\"KNOWS\""),
        ("MATCH (p:P) WHERE p.name = \"Ada\" RETURN size(keys(p)) AS r", "2"),
        ("MATCH p = (a:P)-[:KNOWS]->(b:P) RETURN length(p) AS r", "1"),
        ("MATCH (p:P) WHERE p.name = \"Ada\" RETURN exists(p.age) AS r", "true"),
        ("MATCH (p:P) RETURN count(p) AS r", "2"),
        ("MATCH (a:P) WHERE EXISTS { MATCH (a)-[:KNOWS]->() } RETURN count(a) AS r", "1"),
        // --- ordering / distinct
        ("UNWIND [1,1,2] AS x RETURN count(x) AS r", "3"),
        // --- narrowing the three gaps found by the first sweep
        ("RETURN reverse(\"abc\") AS r", "\"cba\""),
        ("RETURN [x IN [1,2,3,4] WHERE x > 2 | x] AS r", "[3, 4]"),
        ("RETURN [x IN [1,2,3,4] WHERE x > 2] AS r", "[3, 4]"),
        ("RETURN size([x IN [1,2,3,4] WHERE x > 2 | x]) AS r", "2"),
        ("RETURN 2 ^ 3 AS r", "8"),
        ("RETURN 2 * 3 AS r", "6"),
        ("RETURN 10 / 4 AS r", "2"),
        ("RETURN 10.0 / 4 AS r", "2.5"),
        ("RETURN -3 + 1 AS r", "-2"),
        ("RETURN sqrt(9) AS r", "3"),
        ("RETURN toString(1.5) AS r", "\"1.5\""),
        ("RETURN [1,2] + [3] AS r", "[1, 2, 3]"),
        ("RETURN \"a\" + \"b\" AS r", "\"ab\""),
        ("RETURN 1 IN [1,2] AS r", "true"),
        ("RETURN NOT false AS r", "true"),
        ("RETURN true AND false AS r", "false"),
        ("RETURN true XOR false AS r", "true"),
    ];

    let mut wrong: Vec<(String,String,String)> = Vec::new();
    let mut errored: Vec<(String,String)> = Vec::new();

    for (cypher, expected) in cases {
        match parse_query(cypher) {
            Err(e) => errored.push((cypher.to_string(), format!("parse: {e:?}"))),
            Ok(q) => match QueryExecutor::new(&store).execute(&q) {
                Err(e) => errored.push((cypher.to_string(), format!("exec: {e:?}"))),
                Ok(batch) => {
                    let got = batch.records.first().map(|r| render(r.get("r"))).unwrap_or("<no rows>".into());
                    if got != *expected {
                        wrong.push((cypher.to_string(), expected.to_string(), got));
                    }
                }
            },
        }
    }

    println!("{} cases: {} ok, {} wrong answer, {} errored",
        cases.len(), cases.len()-wrong.len()-errored.len(), wrong.len(), errored.len());

    if !wrong.is_empty() {
        println!("\n=== WRONG ANSWER (parses, runs, returns the wrong thing) ===");
        for (c,e,g) in &wrong { println!("  {c}\n     expected {e}, got {g}"); }
    }
    if !errored.is_empty() {
        println!("\n=== ERRORED (at least it says so) ===");
        for (c,e) in &errored { println!("  {c}\n     {e}"); }
    }
}
