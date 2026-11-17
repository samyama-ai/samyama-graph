//! `EXPLAIN (FORMAT JSON)` / `PROFILE (FORMAT JSON)` against the text plans
//! they replace (#2013).

use super::*;
use crate::query::executor::QueryExecutor;
use crate::query::parser::parse_query;
use std::collections::HashMap;

/// Three people, two of them over 30, one KNOWS edge.
fn people() -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for age in [25i64, 35, 45] {
        let id = store.create_node("Person");
        store.set_node_property("default", id, "age", age).unwrap();
        ids.push(id);
    }
    store.create_edge(ids[0], ids[1], "KNOWS").unwrap();
    store
}

const QUERY: &str = "MATCH (p:Person) WHERE p.age > 30 RETURN p.age AS age";

fn plan_of(store: &GraphStore, cypher: &str) -> PropertyValue {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse `{cypher}`: {e}"));
    let batch = QueryExecutor::new(store).execute(&q).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    assert_eq!(batch.columns, vec!["plan".to_string()]);
    assert_eq!(batch.records.len(), 1);
    match batch.records[0].get("plan") {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("plan column: {other:?}"),
    }
}

fn field<'a>(v: &'a PropertyValue, key: &str) -> &'a PropertyValue {
    match v {
        PropertyValue::Map(m) => m.get(key).unwrap_or_else(|| panic!("no `{key}` in {v:?}")),
        other => panic!("not a map: {other:?}"),
    }
}

fn children(v: &PropertyValue) -> &[PropertyValue] {
    match field(v, "children") {
        PropertyValue::Array(a) => a,
        other => panic!("children: {other:?}"),
    }
}

/// Every operator in pre-order.
fn walk<'a>(v: &'a PropertyValue, out: &mut Vec<&'a PropertyValue>) {
    out.push(v);
    for c in children(v) {
        walk(c, out);
    }
}

#[test]
fn explain_json_is_the_text_plan_as_a_tree() {
    let store = people();
    let text = match plan_of(&store, &format!("EXPLAIN {QUERY}")) {
        PropertyValue::String(s) => s,
        other => panic!("plain EXPLAIN must stay text: {other:?}"),
    };
    let json = plan_of(&store, &format!("EXPLAIN (FORMAT JSON) {QUERY}"));
    assert_eq!(field(&json, "format_version"), &PropertyValue::Integer(FORMAT_VERSION));
    assert_eq!(field(&json, "kind"), &PropertyValue::String("explain".into()));

    // The operator lines of the text plan, in order, against the tree in pre-order.
    let lines: Vec<&str> = text.lines().take_while(|l| !l.is_empty()).collect();
    let mut nodes = Vec::new();
    walk(field(&json, "root"), &mut nodes);
    assert_eq!(nodes.len(), lines.len(), "{text}");
    for (i, (node, line)) in nodes.iter().zip(&lines).enumerate() {
        assert_eq!(field(node, "id"), &PropertyValue::Integer(i as i64));
        let PropertyValue::String(op) = field(node, "operator") else { panic!() };
        assert!(line.trim_start_matches(['+', '-', ' ']).starts_with(op.as_str()), "{line} vs {op}");
        if let PropertyValue::Float(rows) = field(node, "estimated_rows") {
            assert!(line.contains(&format!("rows={}", rows.round())), "{line} vs {rows}");
        }
    }
    assert_eq!(field(&json, "total_estimated_rows"), field(nodes[0], "estimated_rows"));
    assert_eq!(field(&json, "total_estimated_cost"), field(nodes[0], "estimated_cost"));
}

#[test]
fn explain_json_carries_the_hash_budget_and_candidates() {
    let store = people();
    let q = parse_query(&format!("EXPLAIN (FORMAT JSON) {QUERY}")).unwrap();
    let batch = QueryExecutor::new(&store).with_plan_hash(true).execute(&q).unwrap();
    let Some(Value::Property(json)) = batch.records[0].get("plan") else { panic!() };
    // The same digest `/api/query` reports as `plan_hash`.
    assert_eq!(
        field(json, "plan_hash"),
        &PropertyValue::String(format!("{:016x}", batch.plan_hash.unwrap()))
    );
    // A bare executor runs with the budget disabled, which the plan says as null.
    assert_eq!(field(json, "row_budget"), &PropertyValue::Null);
    let budgeted = QueryExecutor::new(&store).with_row_budget(1000).execute(&q).unwrap();
    let Some(Value::Property(json_b)) = budgeted.records[0].get("plan") else { panic!() };
    assert_eq!(field(json_b, "row_budget"), &PropertyValue::Integer(1000));
    // The default planner records no candidates.
    assert_eq!(field(json, "candidate_plans"), &PropertyValue::Array(vec![]));
    assert_eq!(field(json, "candidates_evaluated"), &PropertyValue::Null);
    // And the hash is stable across runs.
    assert_eq!(
        field(json, "plan_hash"),
        field(&plan_of(&store, &format!("EXPLAIN (FORMAT JSON) {QUERY}")), "plan_hash")
    );
}

#[test]
fn candidate_plans_are_numbered_as_cypher_plan_takes_them() {
    let diag = PlanDiagnostics {
        candidates_evaluated: 3,
        chosen_plan_cost: 2.0,
        candidate_costs: vec![("A".into(), 2.0), ("B".into(), 5.0), ("C".into(), 9.0)],
        chosen: 1,
    };
    let fields: HashMap<_, _> = candidates(Some(diag)).into_iter().collect();
    assert_eq!(fields["candidates_evaluated"], PropertyValue::Integer(3));
    let PropertyValue::Array(plans) = &fields["candidate_plans"] else { panic!() };
    let chosen: Vec<_> = plans.iter().map(|p| field(p, "chosen").clone()).collect();
    assert_eq!(
        chosen,
        vec![PropertyValue::Boolean(false), PropertyValue::Boolean(true), PropertyValue::Boolean(false)]
    );
    assert_eq!(field(&plans[1], "plan"), &PropertyValue::Integer(2));
    assert_eq!(field(&plans[2], "cost"), &PropertyValue::Float(9.0));
}

#[test]
fn profile_json_reports_what_ran() {
    let store = people();
    assert!(matches!(plan_of(&store, &format!("PROFILE {QUERY}")), PropertyValue::String(_)));
    let json = plan_of(&store, &format!("PROFILE (FORMAT JSON) {QUERY}"));
    assert_eq!(field(&json, "kind"), &PropertyValue::String("profile".into()));
    assert_eq!(field(&json, "total_actual_rows"), &PropertyValue::Integer(2));
    let root = field(&json, "root");
    assert_eq!(field(root, "actual_rows"), &PropertyValue::Integer(2));
    let mut nodes = Vec::new();
    walk(root, &mut nodes);
    // Rows, not pull calls: unfiltered, the scan hands back all three people
    // in fewer calls than rows, so the two counts cannot coincide.
    let all = plan_of(&store, "PROFILE (FORMAT JSON) MATCH (p:Person) RETURN p.age AS age");
    let mut scanned = Vec::new();
    walk(field(&all, "root"), &mut scanned);
    let leaf = scanned.iter().find(|n| children(n).is_empty()).unwrap();
    assert_eq!(field(leaf, "actual_rows"), &PropertyValue::Integer(3), "{leaf:?}");
    assert_ne!(field(leaf, "actual_rows"), field(leaf, "calls"), "{leaf:?}");
    for node in &nodes {
        let (PropertyValue::Float(time), PropertyValue::Float(own)) =
            (field(node, "time_ms"), field(node, "self_time_ms"))
        else {
            panic!("{node:?}")
        };
        assert!(*own <= *time, "self time above inclusive time: {node:?}");
        assert!(matches!(field(node, "calls"), PropertyValue::Integer(n) if *n >= 1));
    }
    // The same plan EXPLAIN describes.
    let explained = plan_of(&store, &format!("EXPLAIN (FORMAT JSON) {QUERY}"));
    assert_eq!(field(&json, "plan_hash"), field(&explained, "plan_hash"));
    let mut planned = Vec::new();
    walk(field(&explained, "root"), &mut planned);
    let ops = |ns: &[&PropertyValue]| ns.iter().map(|n| field(n, "operator").clone()).collect::<Vec<_>>();
    assert_eq!(ops(&nodes), ops(&planned));
}

#[test]
fn format_json_parses_in_any_case_and_only_after_explain_or_profile() {
    let q = parse_query(&format!("explain (format json) {QUERY}")).unwrap();
    assert!(q.explain && q.plan_json && !q.profile);
    let q = parse_query(&format!("PROFILE (FORMAT JSON) {QUERY}")).unwrap();
    assert!(q.profile && q.plan_json);
    let q = parse_query(&format!("EXPLAIN {QUERY}")).unwrap();
    assert!(q.explain && !q.plan_json);
    assert!(parse_query(&format!("(FORMAT JSON) {QUERY}")).is_err());
    assert!(parse_query(&format!("EXPLAIN (FORMAT XML) {QUERY}")).is_err());
}

#[test]
fn explain_json_on_a_write_describes_it_and_writes_nothing() {
    // A write is explained on the write executor's own path; JSON must reach it too.
    let mut store = people();
    let before = store.node_count();
    let q = parse_query("EXPLAIN (FORMAT JSON) CREATE (n:Person {age: 50}) RETURN n").unwrap();
    let batch = crate::query::executor::MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap();
    let Some(Value::Property(json)) = batch.records[0].get("plan") else { panic!("{batch:?}") };
    assert_eq!(field(json, "kind"), &PropertyValue::String("explain".into()));
    assert!(matches!(field(field(json, "root"), "operator"), PropertyValue::String(_)));
    assert_eq!(store.node_count(), before, "EXPLAIN must not run the write");
}
