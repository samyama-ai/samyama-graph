//! `or.solve` reads a whole-number cost as that number (OPT-07, #2264).
//!
//! # What was wrong
//!
//! The objective `or.solve` minimises is Σ value × cost, each cost read from
//! the node's `costProperty`. The cost was read with `as_float`, which is
//! `None` for an `Integer`, and a missing cost defaults to `1.0`. So a graph
//! written the ordinary way, `CREATE (:Item {cost: 3})`, was solved as if
//! every cost were `1.0`: a different problem from the one in the graph,
//! answered without an error. Every earlier test stored its costs as
//! `PropertyValue::Float`, which is why none of them saw it.
//!
//! # What these tests pin
//!
//! - the `objective` a solve reports equals Σ value × cost recomputed from
//!   the values it wrote back and the costs as stored -- for integer costs,
//!   single- and multi-objective. This does not depend on how good the
//!   solver's answer is, only on which problem it was given;
//! - integer and float costs of the same value give the same answer under the
//!   same seed;
//! - a node without the property still costs `1.0`, as documented;
//! - a cost that is not a number is refused, naming the property, and
//!   nothing is written.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, Record};
use samyama::query::parse_query;

const T: &str = "default";

/// Unit costs chosen so that reading any of them as `1.0` changes the
/// objective by far more than the tolerance below.
const COSTS: &[i64] = &[3, 7, 11];

/// Bounds that keep every value away from zero, so a cost misread as `1.0`
/// cannot hide behind a zero value.
const LOWER: f64 = 1.0;
const UPPER: f64 = 2.0;

const SEED: i64 = 7;
const TOLERANCE: f64 = 1e-9;

/// How each item's cost is stored.
#[derive(Clone, Copy)]
enum Stored {
    Integer,
    Float,
    Absent,
}

/// One `:Item` per cost, each with a `name` and `cost` (and `risk`, a second
/// cost property for the multi-objective case, twice the cost).
fn store(stored: Stored) -> GraphStore {
    let mut s = GraphStore::new();
    for (i, &cost) in COSTS.iter().enumerate() {
        let n = s.create_node("Item");
        s.set_node_property(T, n, "name", format!("item{i}"))
            .unwrap();
        match stored {
            Stored::Integer => {
                s.set_node_property(T, n, "cost", PropertyValue::Integer(cost))
                    .unwrap();
                s.set_node_property(T, n, "risk", PropertyValue::Integer(2 * cost))
                    .unwrap();
            }
            Stored::Float => {
                s.set_node_property(T, n, "cost", PropertyValue::Float(cost as f64))
                    .unwrap();
                s.set_node_property(T, n, "risk", PropertyValue::Float(2.0 * cost as f64))
                    .unwrap();
            }
            Stored::Absent => {}
        }
    }
    s
}

/// Run `or.solve` on `s`, with `extra` appended to the config, and return
/// its one row or the engine's message.
fn solve(s: &mut GraphStore, extra: &str) -> Result<Record, String> {
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'Item', property: 'qty', lowerBound: {LOWER:?}, \
         upperBound: {UPPER:?}, populationSize: 10, maxIterations: 10, seed: {SEED}{extra}}})"
    ))
    .map_err(|e| e.to_string())?;
    let mut ex = MutQueryExecutor::new(s, T.to_string());
    let r = ex.execute(&q).map_err(|e| e.to_string())?;
    assert_eq!(r.records.len(), 1, "or.solve returns one row");
    Ok(r.records[0].clone())
}

/// Every `:Item`'s written value and stored `prop`, read back through Cypher
/// so the test sees what a client sees. An absent property reads as `1.0`.
fn written(s: &mut GraphStore, prop: &str) -> Vec<(f64, f64)> {
    let q = parse_query(&format!(
        "MATCH (i:Item) RETURN i.qty AS qty, i.{prop} AS c ORDER BY i.name"
    ))
    .unwrap();
    let r = MutQueryExecutor::new(s, T.to_string()).execute(&q).unwrap();
    let num = |rec: &Record, col: &str| -> Option<f64> {
        match rec.get(col).and_then(|v| v.as_property()) {
            Some(PropertyValue::Float(f)) => Some(*f),
            Some(PropertyValue::Integer(i)) => Some(*i as f64),
            Some(PropertyValue::Null) | None => None,
            other => panic!("`{col}` is a number, got {other:?}"),
        }
    };
    r.records
        .iter()
        .map(|rec| {
            (
                num(rec, "qty").expect("every item was written"),
                num(rec, "c").unwrap_or(1.0),
            )
        })
        .collect()
}

fn sum_value_times_cost(pairs: &[(f64, f64)]) -> f64 {
    pairs.iter().map(|(v, c)| v * c).sum()
}

/// The `objective` column: a number, or for a multi-objective solve a list.
fn objective(row: &Record) -> Vec<f64> {
    match row.get("objective").and_then(|v| v.as_property()) {
        Some(PropertyValue::Float(f)) => vec![*f],
        Some(PropertyValue::Array(a)) => a
            .iter()
            .map(|p| {
                p.as_float()
                    .unwrap_or_else(|| panic!("objective element {p:?}"))
            })
            .collect(),
        Some(PropertyValue::Vector(v)) => v.iter().map(|&x| x as f64).collect(),
        other => panic!("`objective` is a number or a list, got {other:?}"),
    }
}

fn assert_close(actual: f64, expected: f64, what: &str) {
    assert!(
        (actual - expected).abs() <= TOLERANCE * expected.abs().max(1.0),
        "{what}: objective {actual}, but Σ value × cost of what was written is {expected}"
    );
}

#[test]
fn an_integer_cost_is_the_objective_s_cost() {
    let mut s = store(Stored::Integer);
    let row = solve(&mut s, ", costProperty: 'cost'").unwrap();
    let pairs = written(&mut s, "cost");
    let expected = sum_value_times_cost(&pairs);
    // With every cost read as 1.0 the objective would be Σ value alone.
    let misread: f64 = pairs.iter().map(|(v, _)| v).sum();
    assert!(
        (expected - misread).abs() > 1.0,
        "the fixture separates the two readings ({expected} vs {misread})"
    );
    assert_close(objective(&row)[0], expected, "Jaya, integer costs");
}

#[test]
fn integer_and_float_costs_give_the_same_answer() {
    let mut a = store(Stored::Integer);
    let mut b = store(Stored::Float);
    let ra = solve(&mut a, ", costProperty: 'cost'").unwrap();
    let rb = solve(&mut b, ", costProperty: 'cost'").unwrap();
    assert_eq!(objective(&ra), objective(&rb), "same seed, same costs");
    assert_eq!(written(&mut a, "cost"), written(&mut b, "cost"));
}

#[test]
fn a_multi_objective_solve_reads_integer_costs_too() {
    let mut s = store(Stored::Integer);
    let row = solve(
        &mut s,
        ", algorithm: 'NSGA2', costProperties: ['cost', 'risk']",
    )
    .unwrap();
    let obj = objective(&row);
    assert_eq!(obj.len(), 2, "one objective per cost property: {obj:?}");
    assert_close(
        obj[0],
        sum_value_times_cost(&written(&mut s, "cost")),
        "NSGA2, cost",
    );
    assert_close(
        obj[1],
        sum_value_times_cost(&written(&mut s, "risk")),
        "NSGA2, risk",
    );
}

#[test]
fn a_node_without_the_cost_property_still_costs_one() {
    let mut s = store(Stored::Absent);
    let row = solve(&mut s, ", costProperty: 'cost'").unwrap();
    let pairs = written(&mut s, "cost");
    assert!(pairs.iter().all(|&(_, c)| c == 1.0));
    assert_close(
        objective(&row)[0],
        sum_value_times_cost(&pairs),
        "absent costs",
    );
}

#[test]
fn a_cost_that_is_not_a_number_is_refused_and_nothing_is_written() {
    let mut s = store(Stored::Integer);
    let q = parse_query("MATCH (i:Item {name: 'item1'}) SET i.cost = 'seven'").unwrap();
    MutQueryExecutor::new(&mut s, T.to_string())
        .execute(&q)
        .unwrap();

    let err = solve(&mut s, ", costProperty: 'cost'").expect_err("a string cost is refused");
    assert!(
        err.contains("cost property `cost`") && err.contains("must be a number"),
        "the refusal names the property: {err}"
    );
    let q = parse_query("MATCH (i:Item) WHERE i.qty IS NOT NULL RETURN count(*) AS n").unwrap();
    let r = MutQueryExecutor::new(&mut s, T.to_string())
        .execute(&q)
        .unwrap();
    let n = r.records[0].get("n").and_then(|v| v.as_property()).cloned();
    assert_eq!(n, Some(PropertyValue::Integer(0)), "nothing was written");
}
