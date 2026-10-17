//! Range predicates are estimated from an equi-depth histogram (#478).
//!
//! Every range comparison was costed at a fixed third of the label -- 0.3 in
//! the planner's anchor choice, 1/3 in EXPLAIN -- whatever the data. On a
//! skewed property `v < 2` may keep 1% of the label and `v > 2` 90%, and both
//! were "a third". Each check below compares an estimate with the count the
//! query actually returns, and the bar is PERF-08's: within 2x.

use samyama::graph::{GraphStore, Histogram, HistogramAxis, Label, PropertyValue};
use samyama::query::ast::BinaryOp;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(s: &mut GraphStore, q: &str) {
    MutQueryExecutor::new(s, "default".into())
        .execute(&parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}")))
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn count(s: &GraphStore, q: &str) -> f64 {
    let b = QueryExecutor::new(s).execute(&parse_query(q).unwrap()).unwrap_or_else(|e| panic!("{q}: {e}"));
    match b.records[0].get("n") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n as f64,
        other => panic!("{q}: {other:?}"),
    }
}

fn explain(s: &GraphStore, q: &str) -> String {
    let b = QueryExecutor::new(s).execute(&parse_query(&format!("EXPLAIN {q}")).unwrap()).unwrap();
    match b.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// `n` nodes of `label` whose `v` is geometric: half are 0, a quarter 1, an
/// eighth 2 and so on -- the head-heavy shape LDBC's generator gives counts
/// and dates. `w` is uniform on 0..1000 for contrast.
fn skewed(label: &str, n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    run(
        &mut s,
        &format!(
            "UNWIND range(0, {}) AS i CREATE (:{label} {{i: i, \
             v: toInteger(floor(log(toFloat({n}) / (i + 1)) / log(2.0))), w: (i * 7919) % 1000}})",
            n - 1
        ),
    );
    s
}

fn within_2x(estimate: f64, actual: f64) -> bool {
    let (e, a) = (estimate.max(1.0), actual.max(1.0));
    e / a <= 2.0 && a / e <= 2.0
}

#[test]
fn ranges_on_a_skewed_property_are_within_2x() {
    // 1000 nodes: the sample is the whole label, so this checks the
    // histogram and the estimator, not the luck of a sample.
    let s = skewed("P", 1000);
    let stats = s.statistics();
    let label = Label::new("P");
    let mut misses = Vec::new();
    let mut checked = 0;
    for x in 0..9i64 {
        for (op, sym) in [(BinaryOp::Lt, "<"), (BinaryOp::Le, "<="), (BinaryOp::Gt, ">"), (BinaryOp::Ge, ">=")] {
            let actual = count(&s, &format!("MATCH (p:P) WHERE p.v {sym} {x} RETURN count(p) AS n"));
            if actual < 5.0 {
                continue; // A handful of rows is not where a plan goes wrong.
            }
            let sel = stats
                .estimate_comparison_selectivity(&label, "v", &op, &PropertyValue::Integer(x))
                .expect("v has a histogram");
            checked += 1;
            if !within_2x(sel * 1000.0, actual) {
                misses.push(format!("v {sym} {x}: estimate {:.0}, actual {actual}", sel * 1000.0));
            }
        }
    }
    assert!(checked >= 20, "{checked}");
    assert!(misses.is_empty(), "{misses:#?}");
}

#[test]
fn less_than_and_at_most_differ_by_the_mass_at_the_value() {
    // Half the label is v = 0: `v < 0` keeps nothing and `v <= 0` keeps half,
    // and a histogram alone sees a run of equal bounds either way.
    let s = skewed("P", 1000);
    let stats = s.statistics();
    let est = |op: BinaryOp| stats.estimate_comparison_selectivity(&Label::new("P"), "v", &op, &PropertyValue::Integer(0)).unwrap();
    assert!(est(BinaryOp::Lt) < 0.01, "{}", est(BinaryOp::Lt));
    assert!((0.4..0.6).contains(&est(BinaryOp::Le)), "{}", est(BinaryOp::Le));
    assert!((0.4..0.6).contains(&est(BinaryOp::Gt)), "{}", est(BinaryOp::Gt));
    assert!(est(BinaryOp::Ge) > 0.99, "{}", est(BinaryOp::Ge));
}

#[test]
fn a_sampled_label_is_within_2x_too() {
    // 20,000 nodes: estimated from a 1,000-node sample.
    let s = skewed("P", 20_000);
    let stats = s.statistics();
    let label = Label::new("P");
    for (op, sym, x) in [(BinaryOp::Lt, "<", 300i64), (BinaryOp::Ge, ">=", 900), (BinaryOp::Lt, "<", 50)] {
        let actual = count(&s, &format!("MATCH (p:P) WHERE p.w {sym} {x} RETURN count(p) AS n"));
        let sel = stats.estimate_comparison_selectivity(&label, "w", &op, &PropertyValue::Integer(x)).unwrap();
        assert!(within_2x(sel * 20_000.0, actual), "w {sym} {x}: estimate {:.0}, actual {actual}", sel * 20_000.0);
    }
    for (op, sym, x) in [(BinaryOp::Le, "<=", 1i64), (BinaryOp::Gt, ">", 0), (BinaryOp::Ge, ">=", 3)] {
        let actual = count(&s, &format!("MATCH (p:P) WHERE p.v {sym} {x} RETURN count(p) AS n"));
        let sel = stats.estimate_comparison_selectivity(&label, "v", &op, &PropertyValue::Integer(x)).unwrap();
        assert!(within_2x(sel * 20_000.0, actual), "v {sym} {x}: estimate {:.0}, actual {actual}", sel * 20_000.0);
    }
}

#[test]
fn a_two_sided_range_uses_both_ends() {
    let s = skewed("P", 1000);
    let stats = s.statistics();
    let (lo, hi) = (PropertyValue::Integer(200), PropertyValue::Integer(260));
    let sel = stats.estimate_range_selectivity(&Label::new("P"), "w", Some((&lo, false)), Some((&hi, true))).unwrap();
    let actual = count(&s, "MATCH (p:P) WHERE p.w > 200 AND p.w <= 260 RETURN count(p) AS n");
    assert!(within_2x(sel * 1000.0, actual), "estimate {:.0}, actual {actual}", sel * 1000.0);
}

#[test]
fn explain_estimates_an_index_range_scan_from_the_histogram() {
    let mut s = skewed("P", 1000);
    run(&mut s, "CREATE INDEX ON :P(v)");
    let q = "MATCH (p:P) WHERE p.v >= 6 RETURN p.i";
    let plan = explain(&s, q);
    let line = plan.lines().find(|l| l.contains("IndexScan")).unwrap_or_else(|| panic!("{plan}"));
    let rows: f64 = line
        .split("estimated rows=")
        .nth(1)
        .and_then(|r| r.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|r| r.parse().ok())
        .unwrap_or_else(|| panic!("{line}"));
    let actual = count(&s, "MATCH (p:P) WHERE p.v >= 6 RETURN count(p) AS n");
    // A third of the label would be 333 against about 16.
    assert!(within_2x(rows, actual), "{line} vs actual {actual}");
}

#[test]
fn the_planner_anchors_on_the_range_that_keeps_fewer_rows() {
    // `a.v > 5` keeps ~2% of 1,000 :A; `b.w >= 10` keeps 99% of 600 :B. At a
    // third each, :B looked cheaper (200 against 333) and the plan scanned
    // nearly all of it.
    let mut s = skewed("A", 1000);
    run(&mut s, "UNWIND range(0, 599) AS i CREATE (:B {j: i, w: (i * 31) % 1000})");
    run(&mut s, "MATCH (a:A), (b:B) WHERE b.j = a.i % 600 AND a.i % 3 = 0 CREATE (a)-[:R]->(b)");
    run(&mut s, "CREATE INDEX ON :A(v)");
    run(&mut s, "CREATE INDEX ON :B(w)");
    let q = "MATCH (a:A)-[:R]->(b:B) WHERE a.v > 5 AND b.w >= 10 RETURN count(*) AS n";
    let plan = explain(&s, q);
    let first_scan = plan.lines().rev().find(|l| l.contains("Scan")).unwrap_or_else(|| panic!("{plan}"));
    assert!(first_scan.contains("var=a"), "anchored on {first_scan}\n{plan}");
}

#[test]
fn histograms_order_temporal_values_and_text_on_their_own_axes() {
    let mut s = GraphStore::new();
    run(&mut s, "UNWIND range(0, 399) AS i CREATE (:E {d: date('2020-01-01') + duration({days: i}), name: 'n' + toString(1000 + i)})");
    let stats = s.statistics();
    let label = Label::new("E");
    let d = |iso: &str| {
        let b = QueryExecutor::new(&s).execute(&parse_query(&format!("RETURN date('{iso}') AS d")).unwrap()).unwrap();
        match b.records[0].get("d") {
            Some(Value::Property(v)) => v.clone(),
            other => panic!("{other:?}"),
        }
    };
    let sel = stats.estimate_comparison_selectivity(&label, "d", &BinaryOp::Lt, &d("2020-02-10")).unwrap();
    assert!(within_2x(sel * 400.0, 40.0), "{sel}");
    // A number is not on a date's axis: no estimate, and the caller keeps its guess.
    assert_eq!(stats.estimate_comparison_selectivity(&label, "d", &BinaryOp::Lt, &PropertyValue::Integer(5)), None);
    let sel = stats
        .estimate_comparison_selectivity(&label, "name", &BinaryOp::Ge, &PropertyValue::String("n1300".into()))
        .unwrap();
    assert!(within_2x(sel * 400.0, 100.0), "{sel}");
}

#[test]
fn values_of_mixed_kinds_get_no_histogram() {
    let h = Histogram::build(&[PropertyValue::Integer(1), PropertyValue::String("a".into()), PropertyValue::Integer(3)]);
    assert_eq!(h, None);
    let h = Histogram::build(&[PropertyValue::Integer(1), PropertyValue::Float(2.5), PropertyValue::Integer(3)]).unwrap();
    assert!(matches!(h, Histogram::Numeric { axis: HistogramAxis::Number, .. }));
    assert_eq!(h.fraction_below(&PropertyValue::Integer(0)), Some(0.0));
    assert_eq!(h.fraction_below(&PropertyValue::Integer(9)), Some(1.0));
    assert_eq!(h.fraction_below(&PropertyValue::String("x".into())), None);
}
