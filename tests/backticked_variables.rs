//! Backtick-delimited *variables* (#1373).
//!
//! `escaped_name` reached `property_key` in #847 and `label`/`edge_type` after
//! it, so three of the four cases #1373 reported now work. `variable` was the
//! one left:
//!
//! ```text
//! MATCH (`my node`:N) RETURN `my node`     -- parse error
//! ```
//!
//! # Why the read sites are the whole risk
//!
//! The rule is **atomic**, so `as_str()` hands back the text with its
//! backticks still attached and every read site has to strip them. There are
//! twenty-five spelled `Rule::variable => … .as_str().to_string()`. A site
//! left unstripped does not fail: it binds the variable under the name
//! `` `my node` `` and every later reference looks for `my node`, so the query
//! parses, runs, and returns null or nothing. That is what #847 cost when one
//! of fifteen property-key sites was missed, and it is why the cases below
//! walk the variable through the clauses that read it rather than asserting a
//! parse.
//!
//! A backticked name and its plain spelling are **different variables**: the
//! backticks are a quoting device, so `` `n` `` and `n` denote the same one.
//! Both directions are asserted.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Run `cypher` against a fresh store seeded by `setup`, returning column `r`.
fn run(setup: &[&str], cypher: &str) -> Vec<Value> {
    let mut store = GraphStore::new();
    for s in setup {
        let q = parse_query(s).unwrap_or_else(|e| panic!("setup {s}\n  parse: {e:?}"));
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .unwrap_or_else(|e| panic!("setup {s}\n  exec: {e:?}"));
    }
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    batch
        .records
        .iter()
        .map(|rec| rec.get("r").cloned().unwrap_or(Value::Null))
        .collect()
}

fn str_col(vals: &[Value]) -> Vec<String> {
    vals.iter()
        .map(|v| match v {
            Value::Property(PropertyValue::String(s)) => s.clone(),
            other => panic!("wanted a string, got {other:?}"),
        })
        .collect()
}

fn int_col(vals: &[Value]) -> Vec<i64> {
    vals.iter()
        .map(|v| match v {
            Value::Property(PropertyValue::Integer(i)) => *i,
            other => panic!("wanted an integer, got {other:?}"),
        })
        .collect()
}

const SEED: &[&str] = &[
    "CREATE (:N {name: 'Ada', n: 1})",
    "CREATE (:N {name: 'Grace', n: 2})",
];

#[test]
fn a_backticked_variable_binds_and_reads_back() {
    // The case in the issue. If the MATCH site strips and the RETURN site does
    // not -- or the reverse -- this is where it shows.
    let got = run(SEED, "MATCH (`my node`:N) RETURN `my node`.name AS r");
    let mut names = str_col(&got);
    names.sort();
    assert_eq!(names, vec!["Ada", "Grace"]);
}

#[test]
fn backticks_are_quoting_and_not_part_of_the_name() {
    // `n` and n are the same variable, so mixing the spellings inside one
    // query must resolve to one binding rather than to two.
    let got = run(SEED, "MATCH (`n`:N) WHERE n.n = 1 RETURN `n`.name AS r");
    assert_eq!(str_col(&got), vec!["Ada"]);

    let got = run(SEED, "MATCH (n:N) WHERE `n`.n = 2 RETURN n.name AS r");
    assert_eq!(str_col(&got), vec!["Grace"]);
}

#[test]
fn a_backticked_variable_survives_every_clause_that_reads_one() {
    // One case per read site the parser has, because a site that forgets to
    // strip produces a *running* query with the wrong answer, not an error.
    let mut got = str_col(&run(
        SEED,
        "MATCH (`my node`:N) WHERE `my node`.n > 0 RETURN `my node`.name AS r",
    ));
    got.sort();
    assert_eq!(got, vec!["Ada", "Grace"], "WHERE");

    let got = int_col(&run(
        SEED,
        "MATCH (`x`:N) WITH `x` AS `y` WHERE `y`.n = 2 RETURN `y`.n AS r",
    ));
    assert_eq!(got, vec![2], "WITH, and an aliased backticked name");

    let got = int_col(&run(&[], "UNWIND [3, 1, 2] AS `the value` RETURN `the value` AS r"));
    assert_eq!(got, vec![3, 1, 2], "UNWIND");

    let got = int_col(&run(
        SEED,
        "MATCH (`my node`:N) RETURN `my node`.n AS r ORDER BY `my node`.n DESC",
    ));
    assert_eq!(got, vec![2, 1], "ORDER BY");

    let got = int_col(&run(
        SEED,
        "MATCH (`my node`:N) RETURN count(`my node`) AS r",
    ));
    assert_eq!(got, vec![2], "inside an aggregate");

    let got = int_col(&run(&[], "WITH 7 AS `odd name` RETURN `odd name` + 1 AS r"));
    assert_eq!(got, vec![8], "in an arithmetic expression");
}

#[test]
fn a_backticked_variable_works_on_a_relationship_and_a_path() {
    let seed = &[
        "CREATE (:P {name: 'a'})",
        "CREATE (:P {name: 'b'})",
        "MATCH (x:P {name: 'a'}), (y:P {name: 'b'}) CREATE (x)-[:R {w: 5}]->(y)",
    ];
    let got = int_col(&run(
        seed,
        "MATCH (`from`:P)-[`the edge`:R]->(`to`:P) RETURN `the edge`.w AS r",
    ));
    assert_eq!(got, vec![5], "edge variable");

    let got = int_col(&run(
        seed,
        "MATCH `the path` = (:P)-[:R]->(:P) RETURN length(`the path`) AS r",
    ));
    assert_eq!(got, vec![1], "path variable");
}

#[test]
fn a_backticked_variable_can_be_written_through() {
    // The mutating executor reads variables from its own sites.
    let mut store = GraphStore::new();
    for s in [
        "CREATE (`new node`:W {n: 1})",
        "MATCH (`w`:W) SET `w`.n = 42",
    ] {
        let q = parse_query(s).unwrap_or_else(|e| panic!("{s}\n  parse: {e:?}"));
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .unwrap_or_else(|e| panic!("{s}\n  exec: {e:?}"));
    }
    let q = parse_query("MATCH (`w`:W) RETURN `w`.n AS r").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    assert_eq!(
        batch.records.first().and_then(|r| r.get("r")),
        Some(&Value::Property(PropertyValue::Integer(42))),
        "CREATE and SET both bound the backticked variable"
    );
}

#[test]
fn a_literal_backtick_inside_a_variable_name_is_doubled() {
    // Same escape the property keys use: `` inside the delimiters is one
    // backtick. If the doubling is not collapsed, the name binds with two.
    let got = int_col(&run(&[], "WITH 1 AS `od``d` RETURN `od``d` AS r"));
    assert_eq!(got, vec![1]);
}

#[test]
fn an_undelimited_variable_is_unchanged() {
    // The guard against a fix that strips something off ordinary names.
    let got = int_col(&run(&[], "WITH 1 AS plain RETURN plain AS r"));
    assert_eq!(got, vec![1]);
}
