//! EXPLAIN prints each operator's estimated rows and cost (NDS-12, #1670).
//!
//! PROFILE has set an estimate beside each operator's actual count since #1624,
//! and EXPLAIN printed none: it named the structure chosen and not the numbers
//! that chose it. Every operator line now ends `[estimated rows=N cost=C]` --
//! cost being the rows produced by the operator and everything under it -- or
//! `[estimated rows=?]` for an operator with no model, rather than a number
//! that does not describe it.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store(indexed: bool) -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..400 {
        let n = s.create_node_with_labels([Label::new("P")]);
        s.set_node_property("default", n, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", n, "city", PropertyValue::String(format!("c{}", i % 4))).unwrap();
    }
    if indexed {
        MutQueryExecutor::new(&mut s, "default".into())
            .execute(&parse_query("CREATE INDEX ON :P(id)").unwrap())
            .unwrap();
    }
    s
}

fn explain(s: &GraphStore, q: &str) -> Vec<String> {
    let batch = QueryExecutor::new(s).execute(&parse_query(&format!("EXPLAIN {q}")).unwrap()).unwrap();
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    text.split("--- ").next().unwrap().lines().filter(|l| !l.trim().is_empty()).map(String::from).collect()
}

fn rows_on(line: &str) -> Option<f64> {
    let at = line.find("[estimated rows=")? + "[estimated rows=".len();
    let rest = &line[at..];
    let end = rest.find([' ', ']'])?;
    rest[..end].parse().ok()
}

#[test]
fn every_operator_line_carries_an_estimate() {
    let s = store(false);
    let lines = explain(&s, "MATCH (p:P) WHERE p.city = 'c1' RETURN p.id ORDER BY p.id LIMIT 5");
    assert!(lines.len() >= 3, "{lines:?}");
    for l in &lines {
        assert!(l.contains("[estimated rows="), "no estimate on `{l}`");
    }
    // The scan has a model, so it carries a number and a cost.
    let scan = lines.iter().find(|l| l.contains("Scan")).expect("a scan line");
    assert!(rows_on(scan).is_some() && scan.contains("cost="), "{scan}");
}

#[test]
fn the_estimate_explains_the_choice_of_an_index() {
    // The same predicate with and without an index: the indexed plan's scan is
    // estimated far below the label scan's, which is why it was chosen.
    let scan = explain(&store(false), "MATCH (p:P) WHERE p.id = 7 RETURN p");
    let index = explain(&store(true), "MATCH (p:P) WHERE p.id = 7 RETURN p");
    let leaf = |lines: &[String]| lines.iter().rev().find_map(|l| rows_on(l)).expect("a leaf estimate");
    assert!(index.iter().any(|l| l.contains("IndexScan")), "{index:?}");
    assert!(leaf(&index) < leaf(&scan), "index leaf {} is not below scan leaf {}", leaf(&index), leaf(&scan));
}

#[test]
fn the_cost_of_a_parent_includes_its_children() {
    let lines = explain(&store(false), "MATCH (p:P) WHERE p.city = 'c1' RETURN p");
    let cost = |l: &str| -> Option<f64> {
        let at = l.find("cost=")? + 5;
        l[at..].trim_end_matches(']').parse().ok()
    };
    let costs: Vec<f64> = lines.iter().filter_map(|l| cost(l)).collect();
    assert!(costs.len() >= 2, "{lines:?}");
    assert!(costs.windows(2).all(|w| w[0] >= w[1]), "cost does not grow towards the root: {lines:?}");
}
