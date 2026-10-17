//! Every index kind is created by the same two spellings (NDS-11, #1668).
//!
//! `CREATE INDEX ON :L(p)` and `CREATE VECTOR INDEX ... ON :L(p)` worked;
//! `CREATE TEXT INDEX` and `CREATE FULLTEXT INDEX` in that form did not, and
//! the plain index did not accept Neo4j 5's `FOR (n:L) ON (n.p)`. Each kind now
//! takes both, with an optional name. An unnamed full-text or text index is
//! named from what it covers, because `db.index.fulltext.queryNodes` addresses
//! it by name. `CREATE POINT INDEX` parses and is refused with the reason: the
//! R-tree it asks for is #1618.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    for (i, body) in ["graph engines index data", "vectors and text"].iter().enumerate() {
        let n = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", n, "x", PropertyValue::Integer(i as i64)).unwrap();
        s.set_node_property("default", n, "body", PropertyValue::String(body.to_string())).unwrap();
    }
    s
}

fn run(s: &mut GraphStore, q: &str) -> Result<(), String> {
    let query = parse_query(q).map_err(|e| e.to_string())?;
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn show(s: &GraphStore) -> Vec<String> {
    let batch = QueryExecutor::new(s).execute(&parse_query("SHOW INDEXES").unwrap()).unwrap();
    batch
        .records
        .iter()
        .map(|r| {
            ["label", "property", "type"]
                .iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::String(t))) => t.clone(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn every_kind_accepts_both_spellings() {
    for (q, expect) in [
        ("CREATE INDEX ON :N(x)", "N|x|BTREE"),
        ("CREATE INDEX FOR (n:N) ON (n.x)", "N|x|BTREE"),
        ("CREATE INDEX nx FOR (n:N) ON (n.x)", "N|x|BTREE"),
        ("CREATE RANGE INDEX FOR (n:N) ON (n.x)", "N|x|BTREE"),
        ("CREATE TEXT INDEX ON :N(body)", "N|body|FULLTEXT[text_N_body]"),
        ("CREATE TEXT INDEX tb FOR (n:N) ON (n.body)", "N|body|FULLTEXT[tb]"),
        ("CREATE FULLTEXT INDEX ON :N(body)", "N|body|FULLTEXT[fulltext_N_body]"),
        ("CREATE FULLTEXT INDEX fb FOR (n:N) ON (n.body)", "N|body|FULLTEXT[fb]"),
    ] {
        let mut s = store();
        run(&mut s, q).unwrap_or_else(|e| panic!("{q}: {e}"));
        let rows = show(&s);
        assert!(rows.iter().any(|r| r == expect), "{q}: SHOW INDEXES {rows:?}, wanted {expect}");
    }
}

#[test]
fn an_unnamed_text_index_is_queryable_by_its_derived_name() {
    let mut s = store();
    run(&mut s, "CREATE TEXT INDEX ON :N(body)").unwrap();
    let batch = QueryExecutor::new(&s)
        .execute(
            &parse_query(
                "CALL db.index.fulltext.queryNodes('text_N_body', 'vectors') YIELD node RETURN node.x AS x",
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].get("x"), Some(&Value::Property(PropertyValue::Integer(1))));
}

#[test]
fn a_named_range_index_is_used_by_the_planner() {
    let mut s = store();
    run(&mut s, "CREATE INDEX nx FOR (n:N) ON (n.x)").unwrap();
    let plan = QueryExecutor::new(&s)
        .execute(&parse_query("EXPLAIN MATCH (n:N) WHERE n.x = 1 RETURN n").unwrap())
        .unwrap();
    let text = format!("{:?}", plan.records[0]);
    assert!(text.contains("IndexScan"), "{text}");
}

#[test]
fn a_point_index_is_refused_with_the_reason() {
    for q in ["CREATE POINT INDEX ON :N(loc)", "CREATE POINT INDEX pl FOR (n:N) ON (n.loc)"] {
        let mut s = store();
        let err = run(&mut s, q).expect_err(q);
        assert!(err.contains("#1618") && err.contains("R-tree"), "{q}: {err}");
        assert!(show(&s).is_empty(), "{q} created something: {:?}", show(&s));
    }
}
