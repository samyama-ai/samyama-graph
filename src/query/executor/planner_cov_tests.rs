//! Coverage-driven tests for `planner.rs`: the pure rewriting helpers, the
//! statement-level plans, procedure planning and its argument checking, and
//! query shapes that exercise the less common planner branches end to end.

use super::*;
use crate::graph::{GraphStore, Label, NodeId, PropertyValue};
use crate::query::executor::record::{RecordBatch, Value};
use crate::query::executor::OperatorDescription;
use crate::query::parser::parse_query;
use crate::query::QueryEngine;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn q(s: &str) -> Query {
    parse_query(s).unwrap_or_else(|e| panic!("parse {s}: {e:?}"))
}

/// The expression of the first RETURN item of `MATCH (a), (b), (n), (x) RETURN <s>`.
fn expr(s: &str) -> Expression {
    let query = q(&format!("MATCH (a), (b), (n), (x) RETURN {s} AS __e"));
    query.return_clause.expect("return").items[0].expression.clone()
}

/// A WHERE predicate over `a`, `b`, `n` and `x`.
fn pred(s: &str) -> Expression {
    q(&format!("MATCH (a), (b), (n), (x) WHERE {s} RETURN a")).where_clause.expect("where").predicate
}

fn var(s: &str) -> Expression {
    Expression::Variable(s.to_string())
}

fn prop(v: &str, p: &str) -> Expression {
    Expression::Property { variable: v.to_string(), property: p.to_string() }
}

fn int(i: i64) -> Expression {
    Expression::Literal(PropertyValue::Integer(i))
}

fn vars_of(e: &Expression) -> Vec<String> {
    let mut s = HashSet::new();
    QueryPlanner::collect_expression_variables(e, &mut s);
    let mut v: Vec<String> = s.into_iter().collect();
    v.sort();
    v
}

fn engine() -> QueryEngine {
    QueryEngine::new()
}

fn run(store: &mut GraphStore, s: &str) -> RecordBatch {
    engine().execute_mut(s, store, "default").unwrap_or_else(|e| panic!("{s}: {e}"))
}

fn run_err(store: &mut GraphStore, s: &str) -> String {
    match engine().execute_mut(s, store, "default") {
        Ok(b) => panic!("{s}: expected an error, got {} rows", b.records.len()),
        Err(e) => e.to_string(),
    }
}

fn read(store: &GraphStore, s: &str) -> RecordBatch {
    engine().execute(s, store).unwrap_or_else(|e| panic!("{s}: {e}"))
}

fn native(store: &GraphStore, s: &str) -> RecordBatch {
    let planner = QueryPlanner::with_config(PlannerConfig { graph_native: true, max_candidate_plans: 64 });
    crate::query::executor::QueryExecutor::with_planner(store, planner)
        .execute(&q(s))
        .unwrap_or_else(|e| panic!("{s}: {e}"))
}

fn plan_of(store: &GraphStore, s: &str) -> ExecutionPlan {
    QueryPlanner::new().plan(&q(s), store).unwrap_or_else(|e| panic!("plan {s}: {e}"))
}

fn plan_err(store: &GraphStore, s: &str) -> String {
    match QueryPlanner::new().plan(&q(s), store) {
        Ok(_) => panic!("{s}: expected a planning error"),
        Err(e) => e.to_string(),
    }
}

/// Every operator name in the plan tree, depth first.
fn op_names(plan: &ExecutionPlan) -> Vec<String> {
    fn walk(d: &OperatorDescription, out: &mut Vec<String>) {
        out.push(d.name.clone());
        for c in &d.children {
            walk(c, out);
        }
    }
    let mut out = Vec::new();
    walk(&plan.root.describe(), &mut out);
    out
}

fn has_op(store: &GraphStore, s: &str, name: &str) -> bool {
    op_names(&plan_of(store, s)).iter().any(|n| n == name)
}

fn value_to_pv(v: &Value) -> PropertyValue {
    match v {
        Value::Property(p) => p.clone(),
        Value::NodeRef(id) | Value::Node(id, _) => PropertyValue::Integer(id.as_u64() as i64),
        Value::Null => PropertyValue::Null,
        other => PropertyValue::String(format!("{other:?}")),
    }
}

fn col(b: &RecordBatch, c: &str) -> Vec<PropertyValue> {
    b.records
        .iter()
        .map(|r| r.get(c).map(value_to_pv).unwrap_or(PropertyValue::Null))
        .collect()
}

fn ints(b: &RecordBatch, c: &str) -> Vec<i64> {
    col(b, c)
        .into_iter()
        .map(|v| match v {
            PropertyValue::Integer(i) => i,
            other => panic!("column {c}: expected an integer, got {other:?}"),
        })
        .collect()
}

fn sorted_ints(b: &RecordBatch, c: &str) -> Vec<i64> {
    let mut v = ints(b, c);
    v.sort();
    v
}

fn strs(b: &RecordBatch, c: &str) -> Vec<String> {
    col(b, c)
        .into_iter()
        .map(|v| match v {
            PropertyValue::String(s) => s,
            PropertyValue::Null => "<null>".to_string(),
            other => panic!("column {c}: expected a string, got {other:?}"),
        })
        .collect()
}

fn sorted_strs(b: &RecordBatch, c: &str) -> Vec<String> {
    let mut v = strs(b, c);
    v.sort();
    v
}

/// Five people in two cities with a KNOWS chain A->B->C->D, A->C, and E alone.
fn people() -> GraphStore {
    let mut s = GraphStore::new();
    run(
        &mut s,
        "CREATE (a:Person {name: 'A', age: 30, city: 'X'}), \
                (b:Person {name: 'B', age: 25, city: 'X'}), \
                (c:Person {name: 'C', age: 35, city: 'Y'}), \
                (d:Person {name: 'D', age: 40, city: 'Y'}), \
                (e:Person {name: 'E', age: 20, city: 'Y'}), \
                (a)-[:KNOWS {since: 2000}]->(b), \
                (b)-[:KNOWS {since: 2001}]->(c), \
                (c)-[:KNOWS {since: 2002}]->(d), \
                (a)-[:KNOWS {since: 2003}]->(c)",
    );
    s
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

#[test]
fn rewrite_sort_key_recurses_into_binary_and_unary() {
    let projections = vec![(expr("count(a)"), "n".to_string())];
    assert_eq!(rewrite_sort_key(&expr("count(a)"), &projections), var("n"));
    let rewritten = rewrite_sort_key(&expr("count(a) + 1"), &projections);
    assert_eq!(rewritten, Expression::Binary { left: Box::new(var("n")), op: BinaryOp::Add, right: Box::new(int(1)) });
    match rewrite_sort_key(&expr("-count(a)"), &projections) {
        Expression::Unary { expr, .. } => assert_eq!(*expr, var("n")),
        other => panic!("expected a unary, got {other:?}"),
    }
    // Nothing matches: left alone.
    assert_eq!(rewrite_sort_key(&prop("a", "x"), &projections), prop("a", "x"));
}

#[test]
fn extract_nested_aggregates_reaches_collections_and_comprehensions() {
    let cases = [
        ("[count(a), 1]", 1),
        ("{c: count(a), m: max(a.x)}", 2),
        ("[v IN collect(a.x) | v * 2]", 1),
        ("all(ok IN collect(a.x) WHERE ok > 0)", 1),
        ("reduce(t = sum(a.x), v IN collect(a.y) | t + v)", 2),
        ("collect(a.x)[0]", 1),
        ("collect(a.x)[1..count(a)]", 2),
        ("CASE WHEN count(a) > 1 THEN min(a.x) ELSE max(a.x) END", 3),
        ("CASE count(a) WHEN 1 THEN 'one' END", 1),
        ("-sum(a.x)", 1),
        ("toString(count(*))", 1),
        ("percentileCont(a.x, 0.9)", 1),
        ("stDev(a.x) + stDevP(a.x) + avg(a.x)", 3),
    ];
    for (src, n) in cases {
        let mut counter = 0;
        let (rewritten, aggs) = extract_nested_aggregates(&expr(src), &mut counter);
        assert_eq!(aggs.len(), n, "{src}: {aggs:?}");
        assert_eq!(counter, n, "{src}");
        assert!(!expression_has_aggregate(&rewritten), "{src} still aggregates: {rewritten:?}");
        assert!(expression_has_aggregate(&expr(src)), "{src} should be seen as an aggregate");
    }
    // count(*) counts a literal 1; percentileCont keeps its second argument.
    let mut c = 0;
    let (_, aggs) = extract_nested_aggregates(&expr("count(*)"), &mut c);
    assert_eq!(aggs[0].expr, int(1));
    let (_, aggs) = extract_nested_aggregates(&expr("percentileDisc(a.x, 0.25)"), &mut c);
    assert!(aggs[0].percentile.is_some());
}

#[test]
fn expression_has_aggregate_is_false_for_plain_expressions() {
    for src in [
        "a.x + 1",
        "-a.x",
        "toUpper(a.name)",
        "[a.x, 1]",
        "{k: a.x}",
        "CASE WHEN a.x > 1 THEN 1 END",
        "a.l[0]",
        "a.l[1..2]",
        "reduce(t = 0, v IN a.l | t + v)",
        "any(v IN a.l WHERE v > 0)",
    ] {
        assert!(!expression_has_aggregate(&expr(src)), "{src}");
    }
}

#[test]
fn substitute_aliases_rewrites_compound_keys() {
    let items = vec![(prop("a", "age"), "age".to_string()), (var("a"), "person".to_string())];
    assert_eq!(substitute_aliases(&var("age"), &items), prop("a", "age"));
    assert_eq!(substitute_aliases(&var("other"), &items), var("other"));
    assert_eq!(substitute_aliases(&prop("person", "name"), &items), prop("a", "name"));
    // `age` aliases a property, not a variable: `age.x` stays as written.
    assert_eq!(substitute_aliases(&prop("age", "x"), &items), prop("age", "x"));
    match substitute_aliases(&expr("a.age + 1"), &[(prop("a", "age"), "a".to_string())]) {
        Expression::Binary { .. } => {}
        other => panic!("{other:?}"),
    }
    let bin = Expression::Binary { left: Box::new(var("age")), op: BinaryOp::Add, right: Box::new(int(1)) };
    assert_eq!(
        substitute_aliases(&bin, &items),
        Expression::Binary { left: Box::new(prop("a", "age")), op: BinaryOp::Add, right: Box::new(int(1)) }
    );
    let neg = Expression::Unary { op: crate::query::ast::UnaryOp::Minus, expr: Box::new(var("age")) };
    assert!(matches!(substitute_aliases(&neg, &items), Expression::Unary { expr, .. } if *expr == prop("a", "age")));
    let f = Expression::Function { name: "abs".into(), args: vec![var("age")], distinct: false };
    assert!(matches!(substitute_aliases(&f, &items), Expression::Function { args, .. } if args == vec![prop("a", "age")]));
    assert_eq!(substitute_aliases(&int(3), &items), int(3));
    assert_eq!(resolve_sort_key(&var("age"), &items, SortPosition::BeforeProjection), prop("a", "age"));
}

#[test]
fn multiplicity_is_observable_by_query_shape() {
    // Plain RETURN: observable.
    assert!(multiplicity_is_observable(&q("MATCH (a)-[:R*1..2]->(b) RETURN b")));
    // RETURN DISTINCT without aggregate: not observable.
    assert!(!multiplicity_is_observable(&q("MATCH (a)-[:R*1..2]->(b) RETURN DISTINCT b")));
    // RETURN DISTINCT over an aggregate: observable.
    assert!(multiplicity_is_observable(&q("MATCH (a)-[:R*1..2]->(b) RETURN DISTINCT count(b)")));
    // First WITH DISTINCT decides.
    assert!(!multiplicity_is_observable(&q("MATCH (a)-[:R*1..2]->(b) WITH DISTINCT b RETURN count(b)")));
    assert!(multiplicity_is_observable(&q("MATCH (a)-[:R*1..2]->(b) WITH b RETURN DISTINCT b")));
    // No RETURN at all.
    let mut none = q("MATCH (a) RETURN a");
    none.return_clause = None;
    assert!(multiplicity_is_observable(&none));
}

#[test]
fn multiplicity_is_observable_walks_pipeline_clauses() {
    let mut query = q("MATCH (a) RETURN a");
    query.clauses = vec![
        Clause::Match(query.match_clauses[0].clone()),
        Clause::Return(crate::query::ast::ReturnClause { items: q("MATCH (b) RETURN b").return_clause.unwrap().items, distinct: true }),
    ];
    assert!(!multiplicity_is_observable(&query));
    query.clauses[1] = Clause::With(q("MATCH (b) WITH b RETURN b").with_clause.unwrap());
    assert!(multiplicity_is_observable(&query));
    let mut w = q("MATCH (b) WITH DISTINCT b RETURN b").with_clause.unwrap();
    w.distinct = true;
    query.clauses[1] = Clause::With(w);
    assert!(!multiplicity_is_observable(&query));
    // Neither WITH nor RETURN among the clauses.
    query.clauses = vec![Clause::Match(query.match_clauses[0].clone())];
    assert!(multiplicity_is_observable(&query));
}

#[test]
fn take_exists_bodies_splits_semi_joins_from_other_conjuncts() {
    let query = q(
        "MATCH (p:Person) WHERE p.age > 1 AND EXISTS { MATCH (p)-[:KNOWS]->(q) WITH q RETURN q } \
         AND NOT EXISTS { MATCH (p)<-[:KNOWS]-(r) WITH r RETURN r } AND p.age < 99 RETURN p",
    );
    let (stripped, joins) = take_exists_bodies(&query).expect("two bodies");
    assert_eq!(joins.len(), 2);
    assert!(!joins[0].1, "first EXISTS is positive");
    assert!(joins[1].1, "second is negated");
    let kept = stripped.where_clause.expect("the plain conjuncts remain").predicate;
    assert_eq!(flatten_and_predicates(&kept).len(), 2);
    // Only EXISTS bodies: no WHERE remains.
    let only = q("MATCH (p:Person) WHERE EXISTS { MATCH (p)-->(q) WITH q RETURN q } RETURN p");
    let (stripped, joins) = take_exists_bodies(&only).unwrap();
    assert_eq!(joins.len(), 1);
    assert!(stripped.where_clause.is_none());
    // No EXISTS at all.
    assert!(take_exists_bodies(&q("MATCH (p) WHERE p.x = 1 RETURN p")).is_none());
    assert!(take_exists_bodies(&q("MATCH (p) RETURN p")).is_none());
    assert!(exists_body_of(&pred("NOT a.x")).is_none());
    assert!(exists_body_of(&int(1)).is_none());
}

#[test]
fn hoist_match_property_exprs_moves_non_literal_properties_into_where() {
    let mut query = q("MATCH (a:P {k: 1}) MATCH (b:P {k: a.k}) WHERE b.z = 1 RETURN b");
    assert!(has_hoistable_match_properties(&query));
    hoist_match_property_exprs(&mut query);
    assert!(!has_hoistable_match_properties(&query));
    let conj = flatten_and_predicates(&query.where_clause.unwrap().predicate);
    assert!(conj.contains(&Expression::Binary { left: Box::new(prop("b", "k")), op: BinaryOp::Eq, right: Box::new(prop("a", "k")) }), "{conj:?}");
    assert_eq!(conj.len(), 2);

    // No WHERE yet: one is created. Edge and target properties are hoisted too.
    let mut query = q("MATCH (a:P) MATCH (a)-[r:R {w: a.w}]->(b {k: a.k}) RETURN b");
    hoist_match_property_exprs(&mut query);
    let conj = flatten_and_predicates(&query.where_clause.unwrap().predicate);
    assert_eq!(conj.len(), 2, "{conj:?}");

    // After a WITH the post-WITH WHERE receives them.
    let mut query = q("MATCH (a:P) WITH a MATCH (b:P {k: a.k}) RETURN b");
    hoist_match_property_exprs(&mut query);
    assert!(query.post_with_where_clause.is_some());

    // A var-length relationship keeps its expression (and is refused later).
    let mut query = q("MATCH (a:P) MATCH (a)-[r:R*1..2 {w: a.w}]->(b) RETURN b");
    hoist_match_property_exprs(&mut query);
    assert!(has_hoistable_match_properties(&query));
}

#[test]
fn hoist_match_property_exprs_in_extra_stages_and_pipeline_clauses() {
    let mut query = q("MATCH (a:P) WITH a MATCH (b:P) WITH a, b MATCH (c:P {k: b.k}) RETURN c");
    assert!(has_hoistable_match_properties(&query));
    hoist_match_property_exprs(&mut query);
    assert!(!has_hoistable_match_properties(&query));

    // Pipeline form: the MATCH's WHERE is the following clause, or a new one.
    let base = q("MATCH (a:P) RETURN a");
    let hoistable = q("MATCH (a:P) MATCH (b:P {k: a.k}) RETURN b").match_clauses[1].clone();
    let mut query = base.clone();
    query.clauses = vec![Clause::Match(hoistable.clone()), Clause::Return(base.return_clause.clone().unwrap())];
    assert!(has_hoistable_match_properties(&query));
    hoist_match_property_exprs(&mut query);
    assert!(matches!(query.clauses[1], Clause::Where(_)));
    assert_eq!(query.clauses.len(), 3);

    let mut query = base.clone();
    query.clauses = vec![
        Clause::Match(hoistable),
        Clause::Where(WhereClause { predicate: pred("a.x = 1") }),
        Clause::Return(base.return_clause.unwrap()),
    ];
    hoist_match_property_exprs(&mut query);
    assert_eq!(query.clauses.len(), 3);
    match &query.clauses[1] {
        Clause::Where(w) => assert_eq!(flatten_and_predicates(&w.predicate).len(), 2),
        other => panic!("{other:?}"),
    }
}

#[test]
fn and_into_conjoins_in_place() {
    let mut target = pred("a.x = 1");
    and_into(&mut target, pred("b.y = 2"));
    assert!(matches!(target, Expression::Binary { op: BinaryOp::And, .. }));
    assert_eq!(flatten_and_predicates(&target).len(), 2);
}

#[test]
fn collect_expression_variables_handles_every_scoping_form() {
    assert_eq!(vars_of(&expr("toUpper(a.name)")), vec!["a"]);
    assert_eq!(vars_of(&expr("[a.x, b]")), vec!["a", "b"]);
    assert_eq!(vars_of(&expr("{k: a.x, j: n}")), vec!["a", "n"]);
    assert_eq!(vars_of(&expr("a.l[b.i]")), vec!["a", "b"]);
    assert_eq!(vars_of(&expr("a.l[b.i..n.j]")), vec!["a", "b", "n"]);
    assert_eq!(vars_of(&expr("CASE a.x WHEN b.y THEN n ELSE x END")), vec!["a", "b", "n", "x"]);
    // Loop variables are local.
    assert_eq!(vars_of(&expr("[v IN a.l WHERE v > b.min | v + n.k]")), vec!["a", "b", "n"]);
    assert_eq!(vars_of(&expr("any(v IN a.l WHERE v = b.k)")), vec!["a", "b"]);
    assert_eq!(vars_of(&expr("reduce(t = b.s, v IN a.l | t + v)")), vec!["a", "b"]);
    // A pattern comprehension names the pattern's variables.
    let pc = vars_of(&expr("[(a)-[r:R]->(m) WHERE m.k > b.k | m.name]"));
    for v in ["a", "b", "m", "r"] {
        assert!(pc.contains(&v.to_string()), "{v} missing from {pc:?}");
    }
    assert_eq!(vars_of(&Expression::PathVariable("p".into())), vec!["p"]);
    // EXISTS names its pattern and the WHERE inside it.
    let ex = vars_of(&pred("EXISTS { MATCH (a)-[:R]->(m) WHERE m.k = b.k }"));
    assert!(ex.contains(&"a".to_string()) && ex.contains(&"b".to_string()), "{ex:?}");
}

#[test]
fn implies_distinct_recognises_renamed_comparisons() {
    assert!(QueryPlanner::implies_distinct(&pred("a.name < b.name"), "a", "b"));
    assert!(QueryPlanner::implies_distinct(&pred("b.name > a.name"), "a", "b"));
    assert!(QueryPlanner::implies_distinct(&pred("a <> b"), "a", "b"));
    assert!(QueryPlanner::implies_distinct(&pred("id(a) < id(b)"), "a", "b"));
    // Different properties, equality, non-binary, unsupported shapes: no.
    assert!(!QueryPlanner::implies_distinct(&pred("a.name < b.age"), "a", "b"));
    assert!(!QueryPlanner::implies_distinct(&pred("a.name = b.name"), "a", "b"));
    assert!(!QueryPlanner::implies_distinct(&pred("NOT a.x"), "a", "b"));
    assert!(!QueryPlanner::implies_distinct(&pred("a.x + 1 < b.x + 1"), "a", "b"));
    assert!(!QueryPlanner::implies_distinct(&pred("a.x < a.y"), "a", "b"));
}

#[test]
fn inside_optional_needs_own_and_new_variables() {
    let own: HashSet<String> = ["o".to_string(), "p".to_string()].into_iter().collect();
    let p = "p".to_string();
    let earlier: HashSet<&String> = [&p].into_iter().collect();
    let set = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<HashSet<_>>();
    assert!(QueryPlanner::inside_optional(&set(&["o"]), &own, &earlier));
    assert!(!QueryPlanner::inside_optional(&set(&[]), &own, &earlier));
    assert!(!QueryPlanner::inside_optional(&set(&["p"]), &own, &earlier), "only an outer variable");
    assert!(!QueryPlanner::inside_optional(&set(&["o", "z"]), &own, &earlier), "a foreign variable");
}

#[test]
fn flip_comparison_op_mirrors_inequalities() {
    assert_eq!(flip_comparison_op(&BinaryOp::Lt), BinaryOp::Gt);
    assert_eq!(flip_comparison_op(&BinaryOp::Gt), BinaryOp::Lt);
    assert_eq!(flip_comparison_op(&BinaryOp::Le), BinaryOp::Ge);
    assert_eq!(flip_comparison_op(&BinaryOp::Ge), BinaryOp::Le);
    assert_eq!(flip_comparison_op(&BinaryOp::Eq), BinaryOp::Eq);
}

#[test]
fn find_id_predicate_accepts_both_operand_orders_and_lists() {
    let preds = vec![pred("a.x = 1"), pred("5 = id(n)")];
    assert_eq!(find_id_predicate("n", &preds), Some((1, vec![NodeId::new(5)])));
    assert_eq!(find_id_predicate("n", &[pred("id(n) IN [1, 2]")]), Some((0, vec![NodeId::new(1), NodeId::new(2)])));
    // A negative id, a mixed list, another variable, another operator.
    assert_eq!(find_id_predicate("n", &[pred("id(n) = -1")]), None);
    assert_eq!(find_id_predicate("n", &[pred("id(n) IN [1, 'x']")]), None);
    assert_eq!(find_id_predicate("n", &[pred("id(n) IN []")]), None);
    assert_eq!(find_id_predicate("n", &[pred("id(n) IN 3")]), None);
    assert_eq!(find_id_predicate("n", &[pred("id(a) = 1")]), None);
    assert_eq!(find_id_predicate("n", &[pred("id(n) > 1")]), None);
    assert_eq!(find_id_predicate("n", &[pred("id(n) = 'x'")]), None);
    assert_eq!(find_id_predicate("n", &[pred("NOT a.x")]), None);
}

#[test]
fn named_path_handles_mints_edge_handles_and_skips_anonymous_nodes() {
    let pattern = q("MATCH p = (a)-[:R]->(b)-[r:S]->(c) RETURN p").match_clauses[0].pattern.clone();
    let handles = named_path_handles(&pattern);
    assert_eq!(handles.len(), 1);
    let (pv, nodes, edges) = &handles[0];
    assert_eq!(pv, "p");
    assert_eq!(nodes, &vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    assert_eq!(edges[0], "__merge_path_edge_1");
    assert_eq!(edges[1], "r");
    // Anonymous node in the path, unnamed path, anonymous start.
    assert!(named_path_handles(&q("MATCH p = (a)-[:R]->() RETURN p").match_clauses[0].pattern).is_empty());
    assert!(named_path_handles(&q("MATCH (a)-[:R]->(b) RETURN a").match_clauses[0].pattern).is_empty());
    assert!(named_path_handles(&q("MATCH p = ()-[:R]->(b) RETURN p").match_clauses[0].pattern).is_empty());
}

#[test]
fn lookup_node_and_hop_accept_only_simple_shapes() {
    let path = |s: &str| q(&format!("MATCH {s} RETURN 1")).match_clauses[0].pattern.paths[0].clone();
    assert!(QueryPlanner::lookup_node(&path("(a:N)")).is_some());
    assert!(QueryPlanner::lookup_node(&path("(a)")).is_none());
    assert!(QueryPlanner::lookup_node(&path("(a:N:M)")).is_none());
    assert!(QueryPlanner::lookup_node(&path("(a:N {k: 1})")).is_none());
    assert!(QueryPlanner::lookup_node(&path("p = (a:N)")).is_none());
    assert!(QueryPlanner::lookup_node(&path("(:N)")).is_none());

    assert!(matches!(QueryPlanner::lookup_hop(&path("(a:N)")), Some(None)));
    assert!(matches!(QueryPlanner::lookup_hop(&path("(a:N)-[:R]->(b)")), Some(Some(_))));
    assert!(QueryPlanner::lookup_hop(&path("(a:N)-[:R*1..2]->(b)")).is_none());
    assert!(QueryPlanner::lookup_hop(&path("(a:N)-[:R {w: 1}]->(b)")).is_none());
    assert!(QueryPlanner::lookup_hop(&path("(a:N)-[:R]->()")).is_none());
    assert!(QueryPlanner::lookup_hop(&path("(a:N)-[:R]->(b)-[:R]->(c)")).is_none());
}

#[test]
fn lookup_key_requires_an_index_and_a_bound_key() {
    let mut store = GraphStore::new();
    store.property_index.create_index(Label::new("N"), "id".to_string());
    let n = Label::new("N");
    let bound = |v: &str| v == "r";
    let preds = vec![pred("n.id = x.id")];
    // `x` is not bound.
    assert!(QueryPlanner::lookup_key("n", &n, &preds, bound, &store).is_none());
    let preds = vec![pred("a.id = 1"), pred("x.id = n.id")];
    let bound_x = |v: &str| v == "x";
    let got = QueryPlanner::lookup_key("n", &n, &preds, bound_x, &store).expect("key on the right");
    assert_eq!(got.0, 1);
    assert_eq!(got.1, "id");
    // A literal key has no variables; a key naming the node itself; no index.
    assert!(QueryPlanner::lookup_key("n", &n, &[pred("n.id = 1")], bound_x, &store).is_none());
    assert!(QueryPlanner::lookup_key("n", &n, &[pred("n.id = n.other")], |_| true, &store).is_none());
    assert!(QueryPlanner::lookup_key("n", &n, &[pred("n.name = x.name")], bound_x, &store).is_none());
    assert!(QueryPlanner::lookup_key("n", &n, &[pred("n.id > x.id")], bound_x, &store).is_none());
}

#[test]
fn execution_plan_new_has_default_diagnostics() {
    let plan = ExecutionPlan::new(Box::new(crate::query::executor::operator::SingleRowOperator::new()), vec!["x".into()], true);
    assert!(plan.is_write);
    assert_eq!(plan.output_columns, vec!["x".to_string()]);
    assert_eq!(plan.candidates_evaluated, 0);
    assert_eq!(plan.chosen_plan_cost, 0.0);
    assert!(plan.candidate_costs.is_empty());
}

// ---------------------------------------------------------------------------
// Statement-level plans
// ---------------------------------------------------------------------------

#[test]
fn analyze_plans_a_read_only_statistics_refresh() {
    let store = GraphStore::new();
    let plan = plan_of(&store, "ANALYZE");
    assert!(!plan.is_write);
    assert_eq!(plan.output_columns, super::super::analyze_ops::analyze_columns());
}

#[test]
fn hierarchy_index_ddl_plans() {
    let store = GraphStore::new();
    let plan = plan_of(&store, "SHOW HIERARCHY INDEXES");
    assert!(!plan.is_write);
    assert_eq!(plan.output_columns, super::super::hierarchy_ops::hierarchy_info_columns());

    let plan = plan_of(&store, "CREATE HIERARCHY INDEX tax ON ()-[:IS_A]->() MEASURE Drug.units AGGREGATE sum, max");
    assert!(plan.is_write);
    let plan = plan_of(&store, "CREATE HIERARCHY INDEX tax ON ()<-[:PARENT_OF]-() MEASURE units");
    assert!(plan.is_write, "no AGGREGATE defaults to sum");
    let plan = plan_of(&store, "CREATE HIERARCHY INDEX tax ON ()-[:IS_A|PART_OF]->()");
    assert!(plan.is_write);

    let err = plan_err(&store, "CREATE HIERARCHY INDEX tax ON ()-[:IS_A]->() MEASURE units AGGREGATE avg");
    assert!(err.contains("unsupported hierarchy aggregate 'avg'"), "{err}");

    let plan = plan_of(&store, "DROP HIERARCHY INDEX tax");
    assert!(plan.is_write);
    assert!(plan.output_columns.is_empty());
    let plan = plan_of(&store, "REBUILD HIERARCHY INDEX tax");
    assert!(plan.is_write);
}

#[test]
fn hierarchy_index_lifecycle_end_to_end() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (r:C {code: 'root'}), (a:C {code: 'a'}), (b:C {code: 'b'}), (a)-[:IS_A]->(r), (b)-[:IS_A]->(a)");
    let created = run(&mut s, "CREATE HIERARCHY INDEX tax ON ()-[:IS_A]->()");
    assert_eq!(created.records.len(), 1);
    let shown = run(&mut s, "SHOW HIERARCHY INDEXES");
    assert_eq!(shown.records.len(), 1);
    let rebuilt = run(&mut s, "REBUILD HIERARCHY INDEX tax");
    assert_eq!(rebuilt.records.len(), 1);
    run(&mut s, "DROP HIERARCHY INDEX tax");
    assert!(run(&mut s, "SHOW HIERARCHY INDEXES").records.is_empty());
}

#[test]
fn fulltext_and_vector_index_ddl_plans() {
    let store = GraphStore::new();
    let plan = plan_of(&store, "CREATE FULLTEXT INDEX docs FOR (d:Doc) ON EACH [d.title, d.body]");
    assert!(plan.is_write);
    assert_eq!(op_names(&plan), vec!["CreateFullTextIndex"]);
    let plan = plan_of(&store, "DROP FULLTEXT INDEX docs");
    assert!(plan.is_write);
    let plan = plan_of(&store, "CREATE VECTOR INDEX emb FOR (d:Doc) ON (d.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}");
    assert!(plan.is_write);
    assert!(plan.output_columns.is_empty());
}

// ---------------------------------------------------------------------------
// Procedures
// ---------------------------------------------------------------------------

fn call(store: &GraphStore, s: &str) -> ExecutionResult<OperatorBox> {
    let query = q(s);
    let cc = query.call_clause.as_ref().expect("a CALL clause");
    QueryPlanner::new().plan_call(cc, store)
}

fn call_err(store: &GraphStore, s: &str) -> String {
    match call(store, s) {
        Ok(_) => panic!("{s}: expected an error"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn fulltext_query_nodes_argument_checks() {
    let mut s = GraphStore::new();
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('docs')");
    assert!(e.contains("takes (indexName, query)"), "{e}");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('a', 'b', 1, 2)");
    assert!(e.contains("takes (indexName, query)"), "{e}");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes(1, 'q')");
    assert!(e.contains("the index name must be a string literal"), "{e}");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('docs', 2)");
    assert!(e.contains("the query must be a string literal"), "{e}");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('docs', 'q', 0)");
    assert!(e.contains("limit must be a positive integer"), "{e}");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('docs', 'q', 5)");
    assert!(e.contains("no full-text index named 'docs'") && e.contains("None has been created"), "{e}");

    run(&mut s, "CREATE FULLTEXT INDEX titles FOR (d:Doc) ON (d.title)");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('docs', 'q')");
    assert!(e.contains("Known index names: titles"), "{e}");
    let e = call_err(&s, "CALL db.index.fulltext.queryNodes('titles', 'q') YIELD node, rank");
    assert!(e.contains("yields `node` and `score`, not `rank`"), "{e}");
    let op = call(&s, "CALL db.index.fulltext.queryNodes('titles', 'q', 3) YIELD node AS d, score AS s").unwrap();
    assert_eq!(op.describe().name, "FullTextSearch");
}

#[test]
fn fulltext_query_nodes_end_to_end_with_renamed_yields() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (:Doc {title: 'graph databases'}), (:Doc {title: 'cooking pasta'})");
    run(&mut s, "CREATE FULLTEXT INDEX titles FOR (d:Doc) ON (d.title)");
    let b = run(&mut s, "CALL db.index.fulltext.queryNodes('titles', 'graph') YIELD node AS d, score AS s RETURN d.title AS t");
    assert_eq!(strs(&b, "t"), vec!["graph databases".to_string()]);
}

#[test]
fn vector_query_nodes_argument_checks() {
    let mut s = GraphStore::new();
    let e = call_err(&s, "CALL db.index.vector.queryNodes('Doc', 'emb')");
    assert!(e.contains("takes either (label, property, queryVector, k)"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('Doc', 'emb', [1.0])");
    assert!(e.contains("takes either"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('emb', 2, [1.0, 0.0])");
    assert!(e.contains("no vector index named 'emb'") && e.contains("No vector index has been created"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes(1, 'emb', [1.0], 2)");
    assert!(e.contains("First argument (label) must be a string literal"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('Doc', 1, [1.0], 2)");
    assert!(e.contains("Second argument (property) must be a string literal"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('Doc', 'emb', 'x', 2)");
    assert!(e.contains("the query vector must be a list of numbers"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('Doc', 'emb', $v, 2)");
    assert!(e.contains("the query vector must be a vector literal"), "{e}");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('Doc', 'emb', [1.0], 'k')");
    assert!(e.contains("k must be an integer literal"), "{e}");

    let op = call(&s, "CALL db.index.vector.queryNodes('Doc', 'emb', [1.0, 0.0], 2) YIELD node AS d, score AS sc, other").unwrap();
    assert_eq!(op.describe().name, "VectorSearch");

    run(&mut s, "CREATE VECTOR INDEX emb FOR (d:Doc) ON (d.embedding) OPTIONS {dimensions: 2, similarity: 'cosine'}");
    let op = call(&s, "CALL db.index.vector.queryNodes('emb', 2, [1.0, 0.0]) YIELD node").unwrap();
    assert_eq!(op.describe().name, "VectorSearch");
    let e = call_err(&s, "CALL db.index.vector.queryNodes('nope', 2, [1.0, 0.0])");
    assert!(e.contains("Known index names: emb"), "{e}");
}

#[test]
fn schema_procedures_plan_to_their_operators() {
    let s = GraphStore::new();
    let name = |p: &str| call(&s, p).unwrap().describe().name;
    assert_eq!(name("CALL db.checkIntegrity()"), "CheckIntegrity");
    assert_eq!(name("CALL db.labels()"), "ShowLabels");
    assert_eq!(name("CALL db.relationshipTypes()"), "ShowRelationshipTypes");
    assert_eq!(name("CALL db.propertyKeys()"), "ShowPropertyKeys");
    assert_eq!(name("CALL db.schema.visualization()"), "SchemaVisualization");
    assert_eq!(name("CALL db.schema.forLLM()"), "SchemaForLlm");
    assert_eq!(name("CALL db.schema.forLLM(4000)"), "SchemaForLlm");
}

#[test]
fn schema_for_llm_budget_checks() {
    let s = GraphStore::new();
    let e = call_err(&s, "CALL db.schema.forLLM('big')");
    assert!(e.contains("optional positive integer"), "{e}");
    let e = call_err(&s, "CALL db.schema.forLLM(1)");
    assert!(e.contains("leaves no room for a schema"), "{e}");
    let e = call_err(&s, "CALL db.schema.forLLM(4000, 2)");
    assert!(e.contains("takes one optional argument"), "{e}");
}

#[test]
fn gds_write_mode_and_unknown_procedures_are_refused() {
    let s = GraphStore::new();
    let e = call_err(&s, "CALL gds.pageRank.write({})");
    assert!(e.contains("GDS's") && e.contains("write"), "{e}");
    let e = call_err(&s, "CALL no.such.procedure()");
    assert!(e.contains("Unknown procedure: no.such.procedure"), "{e}");
    assert!(call(&s, "CALL algo.pageRank()").is_ok(), "an algorithm name is routed to the algorithm operator");
}

// ---------------------------------------------------------------------------
// Special statement shapes, end to end
// ---------------------------------------------------------------------------

#[test]
fn merge_only_binds_a_named_path_and_applies_a_trailing_set() {
    let mut s = GraphStore::new();
    let b = run(&mut s, "MERGE p = (a:M {k: 1})-[:R]->(b:M {k: 2}) RETURN length(p) AS len");
    assert_eq!(ints(&b, "len"), vec![1]);
    let b = run(&mut s, "MERGE (m:M {k: 5}) SET m.x = 7 RETURN m.x AS x");
    assert_eq!(ints(&b, "x"), vec![7]);
    // Merging again matches and still applies the bare SET.
    let b = run(&mut s, "MERGE (m:M {k: 5}) SET m.x = 8 RETURN m.x AS x");
    assert_eq!(ints(&b, "x"), vec![8]);
    assert_eq!(ints(&read(&s, "MATCH (m:M) RETURN count(m) AS c"), "c"), vec![3]);
}

#[test]
fn create_only_with_order_skip_and_limit_still_creates() {
    let mut s = GraphStore::new();
    let b = run(&mut s, "CREATE (n:N {v: 3}) RETURN n.v AS v ORDER BY v DESC");
    assert_eq!(ints(&b, "v"), vec![3]);
    let b = run(&mut s, "CREATE (n:N {v: 4}) RETURN n.v AS v LIMIT 0");
    assert!(b.records.is_empty());
    let b = run(&mut s, "CREATE (n:N {v: 5}) RETURN n.v AS v SKIP 1");
    assert!(b.records.is_empty());
    assert_eq!(sorted_ints(&read(&s, "MATCH (n:N) RETURN n.v AS v"), "v"), vec![3, 4, 5]);
}

#[test]
fn leading_foreach_runs_against_one_row() {
    let mut s = GraphStore::new();
    let plan = plan_of(&s, "FOREACH (i IN [1, 2, 3] | CREATE (:F {i: i}))");
    assert!(plan.is_write);
    assert!(plan.output_columns.is_empty());
    run(&mut s, "FOREACH (i IN [1, 2, 3] | CREATE (:F {i: i}))");
    assert_eq!(sorted_ints(&read(&s, "MATCH (f:F) RETURN f.i AS i"), "i"), vec![1, 2, 3]);
}

#[test]
fn an_empty_query_is_a_planning_error() {
    let s = GraphStore::new();
    let e = QueryPlanner::new().plan(&Query::new(), &s).err().expect("error").to_string();
    assert!(e.contains("at least one MATCH, CALL, CREATE, or RETURN"), "{e}");
}

#[test]
fn standalone_with_return_projects_one_row() {
    let s = GraphStore::new();
    let b = read(&s, "WITH 2 AS x, 'k' AS y RETURN x * 3 AS z, y");
    assert_eq!(ints(&b, "z"), vec![6]);
    assert_eq!(strs(&b, "y"), vec!["k".to_string()]);
}

// ---------------------------------------------------------------------------
// EXISTS subqueries planned as semi-joins
// ---------------------------------------------------------------------------

#[test]
fn exists_with_a_full_body_is_a_semi_join() {
    let s = people();
    // Who knows someone older than 30?  A->C(35), B->C(35), C->D(40).
    let b = read(&s, "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q) WITH q WHERE q.age > 30 RETURN q } RETURN p.name AS n");
    assert_eq!(sorted_strs(&b, "n"), vec!["A", "B", "C"]);
    let b = read(&s, "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(q) WITH q WHERE q.age > 30 RETURN q } RETURN p.name AS n");
    assert_eq!(sorted_strs(&b, "n"), vec!["D", "E"]);
    // Mixed with ordinary conjuncts.
    let b = read(
        &s,
        "MATCH (p:Person) WHERE p.age > 26 AND EXISTS { MATCH (p)-[:KNOWS]->(q) WITH q WHERE q.age > 30 RETURN q } \
         AND p.city = 'X' RETURN p.name AS n",
    );
    assert_eq!(strs(&b, "n"), vec!["A"]);
}

#[test]
fn exists_body_that_writes_is_refused() {
    let s = people();
    let mut query = q("MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q) WITH q RETURN q } RETURN p");
    // Turn the body into a writing query.
    if let Some(WhereClause { predicate: Expression::ExistsSubquery { body: Some(b), .. } }) = query.where_clause.as_mut() {
        b.set_clauses = q("MATCH (n) SET n.x = 1").set_clauses;
    } else {
        panic!("expected an EXISTS body");
    }
    let e = QueryPlanner::new().plan(&query, &s).err().expect("error").to_string();
    assert!(e.contains("an EXISTS { } subquery cannot write"), "{e}");
}

// ---------------------------------------------------------------------------
// MATCH property expressions
// ---------------------------------------------------------------------------

#[test]
fn match_property_expressions_filter_like_where() {
    let mut s = people();
    let b = read(&s, "MATCH (a:Person {name: 'A'}) MATCH (b:Person {city: a.city}) RETURN b.name AS n");
    assert_eq!(sorted_strs(&b, "n"), vec!["A", "B"]);
    let b = read(&s, "UNWIND ['C', 'E'] AS nm MATCH (p:Person {name: nm}) RETURN p.age AS age");
    assert_eq!(sorted_ints(&b, "age"), vec![20, 35]);
    let b = read(&s, "MATCH (a:Person {name: 'A'})-[k:KNOWS {since: 2003}]->(c) MATCH (c)-[k2:KNOWS {since: k.since - 1}]->(d) RETURN d.name AS n");
    assert_eq!(strs(&b, "n"), vec!["D"]);
    let b = read(&s, "MATCH (a:Person {name: 'B'}) WITH a MATCH (o:Person {city: a.city}) WHERE o.name <> 'B' RETURN o.name AS n");
    assert_eq!(strs(&b, "n"), vec!["A"]);
    // Pipeline form.
    let b = run(&mut s, "CREATE (t:T {city: 'Y'}) WITH t MATCH (p:Person {city: t.city}) RETURN count(p) AS c");
    assert_eq!(ints(&b, "c"), vec![3]);
}

#[test]
fn var_length_relationship_property_expression_is_refused() {
    let s = people();
    let e = plan_err(&s, "MATCH (a:Person) MATCH (a)-[r:KNOWS*1..2 {since: a.age}]->(b) RETURN b");
    assert!(e.contains("MATCH does not yet support a non-literal property value (`since`)"), "{e}");
}

#[test]
fn reject_unevaluated_property_exprs_checks_every_position() {
    let base = q("MATCH (a)-[r:R]->(b) RETURN a");
    let e = q("MATCH (a) RETURN a.x AS e").return_clause.unwrap().items[0].expression.clone();
    let mut exprs = HashMap::new();
    exprs.insert("k".to_string(), e);

    let mut start = base.clone();
    start.match_clauses[0].pattern.paths[0].start.property_exprs = Some(exprs.clone());
    let mut node = base.clone();
    node.match_clauses[0].pattern.paths[0].segments[0].node.property_exprs = Some(exprs.clone());
    let mut edge = base.clone();
    edge.match_clauses[0].pattern.paths[0].segments[0].edge.property_exprs = Some(exprs.clone());
    for query in [&start, &node, &edge] {
        let err = QueryPlanner::reject_unevaluated_property_exprs(query).unwrap_err().to_string();
        assert!(err.contains("MATCH does not yet support a non-literal property value (`k`)"), "{err}");
    }
    // The same through the clause list.
    for bad in [start, node, edge] {
        let mut query = base.clone();
        query.match_clauses.clear();
        query.clauses = vec![Clause::Match(bad.match_clauses[0].clone())];
        assert!(QueryPlanner::reject_unevaluated_property_exprs(&query).is_err());
    }
    let mut ok = base.clone();
    ok.clauses = vec![Clause::Return(base.return_clause.clone().unwrap())];
    assert!(QueryPlanner::reject_unevaluated_property_exprs(&ok).is_ok());
}

// ---------------------------------------------------------------------------
// OPTIONAL MATCH join predicates
// ---------------------------------------------------------------------------

#[test]
fn optional_match_where_spanning_both_sides_is_a_join_condition() {
    let s = people();
    // Everyone, with the people they know who are older than they are.
    let b = read(
        &s,
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WHERE f.age > p.age AND f.city <> 'Z' \
         RETURN p.name AS p, f.name AS f ORDER BY p, f",
    );
    assert_eq!(strs(&b, "p"), vec!["A", "B", "C", "D", "E"]);
    assert_eq!(strs(&b, "f"), vec!["C", "C", "D", "<null>", "<null>"]);
}

#[test]
fn optional_match_where_naming_only_outer_variables_keeps_rows() {
    let s = people();
    let b = read(
        &s,
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WHERE p.age > 100 RETURN p.name AS p, f.name AS f",
    );
    assert_eq!(b.records.len(), 5, "every person is kept with a null friend");
    assert!(strs(&b, "f").iter().all(|f| f == "<null>"));
}

#[test]
fn optional_match_not_starting_from_a_bound_variable_is_a_left_outer_join() {
    let s = people();
    // The optional pattern starts at an unbound `f` and ends at the bound `p`.
    let b = read(
        &s,
        "MATCH (p:Person) OPTIONAL MATCH (f:Person)-[:KNOWS]->(p) WHERE f.age < p.age \
         RETURN p.name AS p, f.name AS f ORDER BY p, f",
    );
    // B<-A(30>25 no), C<-B(25<35), C<-A(30<35), D<-C(35<40).
    assert_eq!(strs(&b, "p"), vec!["A", "B", "C", "C", "D", "E"]);
    assert_eq!(strs(&b, "f"), vec!["<null>", "<null>", "A", "B", "C", "<null>"]);
}

#[test]
fn optional_match_sharing_nothing_pairs_with_nulls() {
    let s = people();
    let b = read(&s, "MATCH (p:Person {name: 'E'}) OPTIONAL MATCH (x:Nope) RETURN p.name AS p, x AS x");
    assert_eq!(strs(&b, "p"), vec!["E"]);
    assert_eq!(col(&b, "x"), vec![PropertyValue::Null]);
    let b = read(&s, "MATCH (p:Person {name: 'E'}) OPTIONAL MATCH (x:Person {name: 'A'}) WHERE x.age > p.age RETURN x.name AS x");
    assert_eq!(strs(&b, "x"), vec!["A"]);
    let b = read(&s, "OPTIONAL MATCH (x:Nope) RETURN x AS x");
    assert_eq!(col(&b, "x"), vec![PropertyValue::Null]);
}

#[test]
fn optional_match_after_with_uses_stage_join_predicates() {
    let s = people();
    let b = read(
        &s,
        "MATCH (p:Person) WITH p OPTIONAL MATCH (p)-[:KNOWS]->(f) WHERE f.age > p.age \
         RETURN p.name AS p, f.name AS f ORDER BY p, f",
    );
    assert_eq!(strs(&b, "f"), vec!["C", "C", "D", "<null>", "<null>"]);
    let b = read(
        &s,
        "MATCH (p:Person) WITH p OPTIONAL MATCH (f:Person)-[:KNOWS]->(p) WHERE f.age < p.age AND f.age > 0 \
         RETURN p.name AS p, f.name AS f ORDER BY p, f",
    );
    assert_eq!(strs(&b, "f"), vec!["<null>", "<null>", "A", "B", "C", "<null>"]);
    let b = read(
        &s,
        "MATCH (p:Person {name: 'E'}) WITH p OPTIONAL MATCH (x:Nope) WHERE x.k = p.age RETURN p.name AS p, x AS x",
    );
    assert_eq!(col(&b, "x"), vec![PropertyValue::Null]);
    let b = read(
        &s,
        "MATCH (p:Person {name: 'E'}) WITH p OPTIONAL MATCH (x:Person) WHERE x.name = 'A' RETURN x.name AS x",
    );
    assert_eq!(strs(&b, "x"), vec!["A"]);
}

#[test]
fn unwind_feeding_an_optional_match_is_planned_first() {
    let s = people();
    let b = read(
        &s,
        "UNWIND ['A', 'Q'] AS nm OPTIONAL MATCH (p:Person) WHERE p.name = nm RETURN nm, p.age AS age ORDER BY nm",
    );
    assert_eq!(strs(&b, "nm"), vec!["A", "Q"]);
    assert_eq!(col(&b, "age"), vec![PropertyValue::Integer(30), PropertyValue::Null]);
}

// ---------------------------------------------------------------------------
// UNWIND, LOAD CSV, CALL placement
// ---------------------------------------------------------------------------

#[test]
fn leading_unwinds_multiply_and_filter_after_with() {
    let s = people();
    let b = read(&s, "UNWIND [1, 2] AS x UNWIND [10, 20] AS y MATCH (p:Person {name: 'A'}) RETURN x + y AS v ORDER BY v");
    assert_eq!(ints(&b, "v"), vec![11, 12, 21, 22]);
    let b = read(&s, "UNWIND [30, 40] AS a MATCH (p:Person) WHERE p.age = a WITH p RETURN p.name AS n ORDER BY n");
    assert_eq!(strs(&b, "n"), vec!["A", "D"]);
}

#[test]
fn trailing_unwind_referenced_by_where_is_hoisted() {
    let s = people();
    let b = read(&s, "MATCH (p:Person) UNWIND [1, 2] AS x WITH p, x WHERE x > 1 RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![5]);
    let b = read(&s, "MATCH (p:Person {name: 'A'}) UNWIND [1, 2, 3] AS x RETURN sum(x) AS total");
    assert_eq!(ints(&b, "total"), vec![6]);
}

#[test]
fn leading_unwind_uses_an_index_probe_per_row() {
    let mut s = people();
    run(&mut s, "CREATE INDEX ON :Person(name)");
    let query = "UNWIND ['A', 'D'] AS nm MATCH (p:Person) WHERE p.name = nm RETURN p.age AS age";
    assert!(has_op(&s, query, "CorrelatedIndexLookup"), "{:?}", op_names(&plan_of(&s, query)));
    assert_eq!(sorted_ints(&read(&s, query), "age"), vec![30, 40]);
    // Two nodes looked up per row, the second keyed on the first.
    let query = "UNWIND [{a: 'A', b: 'C'}] AS r MATCH (x:Person), (y:Person) WHERE x.name = r.a AND y.name = r.b RETURN x.age + y.age AS s";
    assert_eq!(ints(&read(&s, query), "s"), vec![65]);
    // A lookup followed by one hop, with a literal property on the target.
    let query = "UNWIND ['A'] AS nm MATCH (a:Person)-[:KNOWS]->(b:Person {city: 'Y'}) WHERE a.name = nm RETURN b.name AS n";
    assert_eq!(strs(&read(&s, query), "n"), vec!["C"]);
}

#[test]
fn later_match_is_an_index_lookup_keyed_on_earlier_rows() {
    let mut s = people();
    run(&mut s, "CREATE INDEX ON :Person(name)");
    let query = "MATCH (a:Person {name: 'A'}) MATCH (b:Person) WHERE b.name = a.name + '' RETURN b.age AS age";
    assert!(has_op(&s, query, "CorrelatedIndexLookup"), "{:?}", op_names(&plan_of(&s, query)));
    assert_eq!(ints(&read(&s, query), "age"), vec![30]);
}

#[test]
fn load_csv_plans_as_the_source_of_the_pipeline() {
    let s = people();
    let plan = plan_of(&s, "LOAD CSV WITH HEADERS FROM 'people.csv' AS row MATCH (p:Person) WHERE p.name = row.name RETURN p.age");
    let names = op_names(&plan);
    assert!(names.contains(&"LoadCsv".to_string()), "{names:?}");
    let plan = plan_of(&s, "LOAD CSV FROM 'x.csv' AS row RETURN row");
    assert!(op_names(&plan).contains(&"LoadCsv".to_string()));
}

#[test]
fn procedure_call_joins_with_a_match() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (:Doc {title: 'graph databases'}), (:Doc {title: 'graph theory'}), (:Doc {title: 'pasta'})");
    run(&mut s, "CREATE FULLTEXT INDEX titles FOR (d:Doc) ON (d.title)");
    // Shares `node` with the MATCH: a join.
    let b = run(&mut s, "MATCH (node:Doc) CALL db.index.fulltext.queryNodes('titles', 'graph') YIELD node RETURN node.title AS t ORDER BY t");
    assert_eq!(strs(&b, "t"), vec!["graph databases", "graph theory"]);
    // Shares nothing: a cartesian product.
    let b = run(&mut s, "MATCH (d:Doc) CALL db.index.fulltext.queryNodes('titles', 'pasta') YIELD node RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![3]);
}

// ---------------------------------------------------------------------------
// Correlated CALL { WITH ... }
// ---------------------------------------------------------------------------

#[test]
fn correlated_call_runs_its_body_per_row() {
    let s = people();
    let b = read(
        &s,
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(f) RETURN count(f) AS c } RETURN p.name AS n, c ORDER BY n",
    );
    assert_eq!(strs(&b, "n"), vec!["A", "B", "C", "D", "E"]);
    assert_eq!(ints(&b, "c"), vec![2, 1, 1, 0, 0]);
    let b = read(
        &s,
        "MATCH (p:Person {name: 'A'}) CALL { WITH * MATCH (p)-[:KNOWS]->(f) RETURN f } RETURN f.name AS f ORDER BY f",
    );
    assert_eq!(strs(&b, "f"), vec!["B", "C"]);
}

#[test]
fn correlated_call_errors() {
    let s = people();
    let e = plan_err(&s, "MATCH (p:Person) CALL { WITH zz MATCH (zz)-->(f) RETURN f } RETURN f");
    assert!(e.contains("`zz` is not defined before the subquery") || e.contains("zz"), "{e}");
    let e = plan_err(&s, "MATCH (p:Person) CALL { WITH p MATCH (p)-->(f) RETURN f AS p } RETURN p");
    assert!(e.contains("already defined outside it"), "{e}");
    let e = plan_err(&s, "MATCH (p:Person) CALL { WITH p MATCH (p)-->(f) SET f.x = 1 RETURN f } RETURN f");
    assert!(e.contains("writes inside CALL { WITH ... } are not supported yet"), "{e}");
}

#[test]
fn correlated_call_structural_errors() {
    let s = people();
    let mut query = q("MATCH (p:Person) CALL { WITH p MATCH (p)-->(f) RETURN f } RETURN f");
    let cc = query.correlated_call.clone().expect("a correlated call");
    // A WITH after the CALL.
    let mut with_after = query.clone();
    with_after.with_clause = q("MATCH (x) WITH x RETURN x").with_clause;
    let e = QueryPlanner::new().plan(&with_after, &s).err().unwrap().to_string();
    assert!(e.contains("a WITH after CALL { WITH ... } is not supported yet"), "{e}");
    // A body without a MATCH.
    let mut cc2 = cc.clone();
    cc2.body.match_clauses.clear();
    query.correlated_call = Some(cc2);
    let e = QueryPlanner::new().plan(&query, &s).err().unwrap().to_string();
    assert!(e.contains("body without a MATCH"), "{e}");
    // No MATCH before it.
    let mut none_before = q("MATCH (p:Person) CALL { WITH p MATCH (p)-->(f) RETURN f } RETURN f");
    none_before.match_clauses.clear();
    none_before.return_clause = q("UNWIND [1] AS p RETURN p").return_clause;
    none_before.unwind_clause = q("UNWIND [1] AS p RETURN p").unwind_clause;
    let e = QueryPlanner::new().plan(&none_before, &s).err().map(|e| e.to_string());
    assert!(e.is_some(), "a correlated call with nothing to correlate against must not plan");
}

// ---------------------------------------------------------------------------
// Pattern shapes
// ---------------------------------------------------------------------------

#[test]
fn comma_separated_patterns_keep_relationships_distinct() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (d:L {n: 'd'})-[:K]->(d)");
    // One self-loop cannot be used twice.
    let b = read(&s, "MATCH (p)-[:K]->(q), (q)-[:K]->(r) RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![0]);
    // Named relationships: the same rule, with the existing names.
    let b = read(&s, "MATCH (p)-[r1:K]->(q), (q)-[r2:K]->(r) RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![0]);
    // The same variable twice is the same relationship, not a pair.
    let b = read(&s, "MATCH (p)-[r1:K]->(q), (q)-[r1:K]->(r) RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![1]);

    let s = people();
    let b = read(&s, "MATCH (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c) RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![3]);
    // Disjoint types are never compared.
    let b = read(&s, "MATCH (a)-[:KNOWS]->(b), (a)-[:LIKES]->(c) RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![0]);
}

#[test]
fn comma_patterns_kept_apart_by_the_where_need_no_check() {
    let s = people();
    for (w, expect) in [
        ("t1.name < t2.name", 1),
        ("t2.name > t1.name", 1),
        ("t1 <> t2", 2),
        ("id(t1) < id(t2)", 1),
    ] {
        let query = format!("MATCH (p)-[:KNOWS]->(t1), (p)-[:KNOWS]->(t2) WHERE {w} RETURN count(*) AS c");
        assert_eq!(ints(&read(&s, &query), "c"), vec![expect], "{query}");
    }
    // Shared end instead of shared start.
    let b = read(&s, "MATCH (t1)-[:KNOWS]->(p), (t2)-[:KNOWS]->(p) WHERE t1.name < t2.name RETURN p.name AS p");
    assert_eq!(strs(&b, "p"), vec!["C"]);
    // Middle-shared chain: a1 == b0.
    let b = read(&s, "MATCH (x)-[:KNOWS]->(m), (m)-[:KNOWS]->(y) WHERE x.name < y.name RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![3]);
    let b = read(&s, "MATCH (m)-[:KNOWS]->(y), (x)-[:KNOWS]->(m) WHERE x.name < y.name RETURN count(*) AS c");
    assert_eq!(ints(&b, "c"), vec![3]);
}

#[test]
fn id_predicates_anchor_the_scan() {
    let s = people();
    let id_c = ints(&read(&s, "MATCH (p:Person {name: 'C'}) RETURN id(p) AS i"), "i")[0];
    let query = format!("MATCH (p:Person) WHERE id(p) = {id_c} RETURN p.name AS n");
    assert!(has_op(&s, &query, "NodeById"));
    assert_eq!(strs(&read(&s, &query), "n"), vec!["C"]);
    let query = format!("MATCH (a)-[:KNOWS]->(b:Person) WHERE id(b) = {id_c} RETURN a.name AS n ORDER BY n");
    assert_eq!(strs(&read(&s, &query), "n"), vec!["A", "B"]);
    let query = format!("MATCH (a)-[:KNOWS]->(b) WHERE id(b) IN [{id_c}] RETURN count(a) AS c");
    assert_eq!(ints(&read(&s, &query), "c"), vec![2]);
}

#[test]
fn indexed_target_predicate_is_an_index_scan() {
    let mut s = people();
    run(&mut s, "CREATE INDEX ON :Person(name)");
    let b = read(&s, "MATCH (a)-[:KNOWS]->(b:Person) WHERE b.name = 'C' RETURN a.name AS n ORDER BY n");
    assert_eq!(strs(&b, "n"), vec!["A", "B"]);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b:Person {name: 'D'}) RETURN a.name AS n");
    assert_eq!(strs(&b, "n"), vec!["C"]);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE 'B' = b.name RETURN a.name AS n");
    assert_eq!(strs(&b, "n"), vec!["A"]);
}

#[test]
fn zero_length_named_path_binds_one_node() {
    let s = people();
    let b = read(&s, "MATCH p = (a:Person {name: 'A'}) RETURN length(p) AS len, size(nodes(p)) AS n");
    assert_eq!(ints(&b, "len"), vec![0]);
    assert_eq!(ints(&b, "n"), vec![1]);
    let b = read(&s, "MATCH p = (a:Person {name: 'A'}) WITH p RETURN length(p) AS len");
    assert_eq!(ints(&b, "len"), vec![0]);
}
