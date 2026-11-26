//! `CALL algo.or.lp(...)`: a user-supplied linear or mixed-integer program,
//! solved exactly by HiGHS (OPT-05, #1766).
//!
//! # What was missing
//!
//! Every solver a client could reach was a population metaheuristic: it
//! returns the best point it sampled and cannot say whether that is the
//! optimum. No surface accepted a linear program, so a problem where a
//! guaranteed optimum matters had nowhere to go.
//!
//! # What these tests pin
//!
//! - Known optima come back exactly: a textbook transportation LP (Winston's
//!   Powerco, optimum $1,020) and MILPs checked against enumeration of every
//!   integer point.
//! - Integrality is enforced: the MILP's answer is integral and worse than its
//!   LP relaxation.
//! - Infeasible and unbounded programs are answers, one row with `status`,
//!   no point and no gap; never an optimum and never an empty result.
//! - The result envelope (OPT-09): `gap` is 0 exactly for a proven optimum.
//! - A program that cannot be read is refused with the reason.
//! - It reads no graph, so it runs read-only.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::executor::QueryExecutor;
use samyama::query::parse_query;
use std::collections::HashMap;

const YIELDS: &str =
    "status, values, objective, feasible, gap, mip_gap, iterations, nodes, total_time_ms, solver";

fn lp(config: &str) -> Result<HashMap<String, Value>, String> {
    let store = GraphStore::new();
    let q = parse_query(&format!("CALL algo.or.lp({config}) YIELD {YIELDS}"))
        .map_err(|e| e.to_string())?;
    let r = QueryExecutor::new(&store)
        .execute(&q)
        .map_err(|e| e.to_string())?;
    assert_eq!(r.records.len(), 1, "always one row");
    Ok(YIELDS
        .split(", ")
        .map(|k| {
            (
                k.to_string(),
                r.records[0].get(k).cloned().unwrap_or(Value::Null),
            )
        })
        .collect())
}

fn text(row: &HashMap<String, Value>, k: &str) -> String {
    row[k]
        .as_property()
        .and_then(|p| p.as_string())
        .unwrap_or_else(|| panic!("{k}: {:?}", row[k]))
        .to_string()
}

fn num(row: &HashMap<String, Value>, k: &str) -> Option<f64> {
    match &row[k] {
        Value::Null => None,
        v => v.as_property().and_then(|p| p.as_float()),
    }
}

fn values(row: &HashMap<String, Value>) -> Option<HashMap<String, f64>> {
    match &row["values"] {
        Value::Null => None,
        Value::Property(PropertyValue::Map(m)) => Some(
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_float().unwrap()))
                .collect(),
        ),
        other => panic!("values: {other:?}"),
    }
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-6
}

#[test]
fn a_small_lp_returns_its_proven_optimum_with_the_envelope() {
    // max 3x + 2y, x + y <= 4, x + 3y <= 6. Vertices (0,0) 0, (4,0) 12,
    // (3,1) 11, (0,2) 4: optimum 12 at (4, 0).
    let row = lp("{sense: 'max', objective: {x: 3, y: 2}, constraints: [\
                  {terms: {x: 1, y: 1}, op: '<=', rhs: 4}, \
                  {terms: {x: 1, y: 3}, op: '<=', rhs: 6}]}")
    .unwrap();
    assert_eq!(text(&row, "status"), "optimal");
    assert!(close(num(&row, "objective").unwrap(), 12.0), "{row:?}");
    let x = values(&row).unwrap();
    assert!(close(x["x"], 4.0) && close(x["y"], 0.0), "{x:?}");
    assert_eq!(num(&row, "gap"), Some(0.0));
    assert_eq!(
        row["feasible"].as_property().and_then(|p| p.as_boolean()),
        Some(true)
    );
    assert_eq!(num(&row, "mip_gap"), None, "an LP has no MIP gap");
    assert_eq!(text(&row, "solver"), "HiGHS");
    assert!(num(&row, "total_time_ms").unwrap() >= 0.0);
}

/// Winston, *Operations Research*, §7.1: Powerco's three plants supply
/// 35, 50 and 40 million kWh to four cities demanding 45, 20, 30 and 30, at
/// the per-unit costs below. The published optimum is $1,020.
#[test]
fn the_powerco_transportation_problem_costs_1020() {
    let supply = [35, 50, 40];
    let demand = [45, 20, 30, 30];
    let cost = [[8, 6, 10, 9], [9, 12, 13, 7], [14, 9, 16, 5]];
    let var = |i: usize, j: usize| format!("x{}{}", i + 1, j + 1);
    let mut objective = Vec::new();
    for i in 0..3 {
        for j in 0..4 {
            objective.push(format!("{}: {}", var(i, j), cost[i][j]));
        }
    }
    let mut constraints = Vec::new();
    for (i, s) in supply.iter().enumerate() {
        let terms: Vec<String> = (0..4).map(|j| format!("{}: 1", var(i, j))).collect();
        constraints.push(format!(
            "{{terms: {{{}}}, op: '<=', rhs: {s}}}",
            terms.join(", ")
        ));
    }
    for (j, d) in demand.iter().enumerate() {
        let terms: Vec<String> = (0..3).map(|i| format!("{}: 1", var(i, j))).collect();
        constraints.push(format!(
            "{{terms: {{{}}}, op: '>=', rhs: {d}}}",
            terms.join(", ")
        ));
    }
    let row = lp(&format!(
        "{{objective: {{{}}}, constraints: [{}]}}",
        objective.join(", "),
        constraints.join(", ")
    ))
    .unwrap();
    assert_eq!(text(&row, "status"), "optimal");
    assert!(close(num(&row, "objective").unwrap(), 1020.0), "{row:?}");
    // The point it returns meets every supply and demand, and costs what it says.
    let x = values(&row).unwrap();
    for (i, s) in supply.iter().enumerate() {
        assert!((0..4).map(|j| x[&var(i, j)]).sum::<f64>() <= *s as f64 + 1e-6);
    }
    for (j, d) in demand.iter().enumerate() {
        assert!((0..3).map(|i| x[&var(i, j)]).sum::<f64>() >= *d as f64 - 1e-6);
    }
    let total: f64 = (0..3)
        .flat_map(|i| (0..4).map(move |j| (i, j)))
        .map(|(i, j)| x[&var(i, j)] * cost[i][j] as f64)
        .sum();
    assert!(close(total, 1020.0));
}

/// A small integer program checked against all of its integer points.
#[test]
fn a_milp_matches_enumeration_and_is_worse_than_its_relaxation() {
    // max 5a + 4b + 3c, 2a + 3b + c <= 5, 4a + b + 2c <= 11, 3a + 4b + 2c <= 7,
    // each of a, b, c an integer in [0, 3]. The integer optimum is 11; the LP
    // relaxation is 35/3, at a = 7/3: along 3a + 2c = 7 the objective is
    // 10.5 + a/2, so it is largest where a is.
    let mut best = f64::NEG_INFINITY;
    for a in 0..=3 {
        for b in 0..=3 {
            for c in 0..=3 {
                if 2 * a + 3 * b + c <= 5 && 4 * a + b + 2 * c <= 11 && 3 * a + 4 * b + 2 * c <= 7 {
                    best = best.max((5 * a + 4 * b + 3 * c) as f64);
                }
            }
        }
    }
    let body = "sense: 'max', objective: {a: 5, b: 4, c: 3}, constraints: [\
                {terms: {a: 2, b: 3, c: 1}, op: '<=', rhs: 5}, \
                {terms: {a: 4, b: 1, c: 2}, op: '<=', rhs: 11}, \
                {terms: {a: 3, b: 4, c: 2}, op: '<=', rhs: 7}], \
                bounds: {a: [0, 3], b: [0, 3], c: [0, 3]}";
    // `algo.or.milp` is the same procedure.
    let store = GraphStore::new();
    let q = parse_query(&format!("CALL algo.or.milp({{{body}, integer: ['a', 'b', 'c']}}) YIELD status, objective, values, nodes")).unwrap();
    let r = QueryExecutor::new(&store).execute(&q).unwrap();
    let rec = &r.records[0];
    assert_eq!(
        rec.get("status")
            .and_then(|v| v.as_property())
            .and_then(|p| p.as_string()),
        Some("optimal")
    );
    let objective = rec
        .get("objective")
        .and_then(|v| v.as_property())
        .and_then(|p| p.as_float())
        .unwrap();
    assert!(close(objective, best), "{objective} vs enumerated {best}");
    if let Some(Value::Property(PropertyValue::Map(m))) = rec.get("values") {
        for (k, v) in m {
            let v = v.as_float().unwrap();
            assert!(close(v, v.round()), "{k} = {v} is not integral");
        }
    } else {
        panic!("values: {:?}", rec.get("values"));
    }

    let relaxed = lp(&format!("{{{body}}}")).unwrap();
    assert!(close(best, 11.0));
    assert!(
        close(num(&relaxed, "objective").unwrap(), 35.0 / 3.0),
        "the relaxation: {relaxed:?}"
    );
}

#[test]
fn an_infeasible_program_is_one_row_with_no_point_and_no_gap() {
    let row = lp(
        "{objective: {x: 1}, constraints: [{terms: {x: 1}, op: '>=', rhs: 5}], \
                  bounds: {x: [0, 3]}, integer: ['x']}",
    )
    .unwrap();
    let status = text(&row, "status");
    assert!(
        status == "infeasible" || status == "infeasible_or_unbounded",
        "{row:?}"
    );
    assert!(values(&row).is_none());
    assert_eq!((num(&row, "objective"), num(&row, "gap")), (None, None));
    assert_ne!(
        row["feasible"].as_property().and_then(|p| p.as_boolean()),
        Some(true)
    );
}

#[test]
fn an_unbounded_program_is_one_row_with_no_point() {
    let row = lp("{sense: 'max', objective: {x: 1}, constraints: [{terms: {x: 1, y: -1}, op: '<=', rhs: 1}]}").unwrap();
    let status = text(&row, "status");
    assert!(
        status == "unbounded" || status == "infeasible_or_unbounded",
        "{row:?}"
    );
    assert!(values(&row).is_none());
    assert_eq!(num(&row, "gap"), None);
}

#[test]
fn a_program_that_cannot_be_read_is_refused_with_its_reason() {
    for (config, says) in [
        (
            "{objective: {x: 1}, constraints: [{terms: {x: 1}, op: '<', rhs: 1}]}",
            "`op`",
        ),
        (
            "{objective: {x: 1}, bounds: {x: [3, 1]}}",
            "above its upper bound",
        ),
        ("{objectiv: {x: 1}}", "objectiv"),
        ("{sense: 'up', objective: {x: 1}}", "`sense`"),
        ("{objective: {x: 1}, timeLimitMs: 0}", "timeLimitMs"),
        ("{}", "no variables"),
        ("'not a map'", "one config map"),
    ] {
        let err = lp(config).expect_err(config);
        assert!(err.contains(says), "{config}: {err}");
    }
}

#[test]
fn a_time_limit_is_never_reported_as_a_proven_optimum() {
    // A 40-item 0/1 knapsack with a 1 ms limit. Whatever it ends as, a gap of
    // 0 appears only beside `optimal`.
    let mut obj = Vec::new();
    let mut terms = Vec::new();
    let mut bounds = Vec::new();
    let mut ints = Vec::new();
    for i in 0..40 {
        obj.push(format!("x{i}: {}", 1000 + (i * 7919) % 997));
        terms.push(format!("x{i}: {}", 1000 + (i * 104_729) % 991));
        bounds.push(format!("x{i}: [0, 1]"));
        ints.push(format!("'x{i}'"));
    }
    let row = lp(&format!(
        "{{sense: 'max', objective: {{{}}}, constraints: [{{terms: {{{}}}, op: '<=', rhs: 20000.5}}], \
          bounds: {{{}}}, integer: [{}], timeLimitMs: 1}}",
        obj.join(", "),
        terms.join(", "),
        bounds.join(", "),
        ints.join(", ")
    ))
    .unwrap();
    let status = text(&row, "status");
    assert!(status == "optimal" || status == "time_limit", "{row:?}");
    assert_eq!(
        num(&row, "gap") == Some(0.0),
        status == "optimal",
        "{row:?}"
    );
}
