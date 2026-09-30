//! Additional unit tests for the row budget wrapper: every pull path charges,
//! every optional method forwards, and both bounds report themselves.

use super::*;
use crate::graph::PropertyValue;
use crate::query::executor::Value;

/// An amplifying leaf that yields `n` rows per pass.
struct Amp {
    n: usize,
    at: usize,
    amplifies: bool,
}

fn amp(n: usize) -> OperatorBox {
    Box::new(Amp {
        n,
        at: 0,
        amplifies: true,
    })
}

impl PhysicalOperator for Amp {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.at >= self.n {
            return Ok(None);
        }
        self.at += 1;
        let mut r = Record::new();
        r.bind("i", Value::Property(PropertyValue::Integer(self.at as i64)));
        Ok(Some(r))
    }
    fn reset(&mut self) {
        self.at = 0;
    }
    fn amplifies_rows(&self) -> bool {
        self.amplifies
    }
    fn try_push_limit(&mut self, n: usize) -> bool {
        n == 11
    }
    fn hint_early_stop(&mut self, n: usize) -> bool {
        n == 12
    }
    fn retain_property_reads(&mut self, variable: &str, _property: &str) -> bool {
        variable == "keep"
    }
    fn take_retained_reads(&mut self) -> Option<Vec<Option<PropertyValue>>> {
        Some(vec![None])
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Amp".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

fn coded(e: ExecutionError) -> (String, String) {
    match e {
        ExecutionError::Coded { code, message } => (code.to_string(), message),
        other => panic!("expected a coded error, got {other:?}"),
    }
}

#[test]
fn next_mut_is_charged_against_the_per_operator_budget() {
    let mut store = GraphStore::new();
    let mut plan = amp(5);
    enforce(&mut plan, 3);
    for _ in 0..3 {
        assert!(plan.next_mut(&mut store, "default").unwrap().is_some());
    }
    let (code, msg) = coded(plan.next_mut(&mut store, "default").unwrap_err());
    assert_eq!(code, error_code::ROW_BUDGET_EXCEEDED);
    assert!(
        msg.contains("operator Amp produced more than 3 rows"),
        "{msg}"
    );
}

#[test]
fn a_batch_is_charged_for_every_row_it_carries() {
    let store = GraphStore::new();
    let mut plan = amp(10);
    enforce(&mut plan, 4);
    let batch = plan.next_batch(&store, 4).unwrap().unwrap();
    assert_eq!(batch.records.len(), 4);
    let (_, msg) = coded(plan.next_batch(&store, 4).unwrap_err());
    assert!(msg.contains("more than 4 rows"), "{msg}");
}

#[test]
fn an_empty_batch_costs_nothing() {
    let store = GraphStore::new();
    let mut plan = amp(0);
    enforce(&mut plan, 1);
    assert!(plan.next_batch(&store, 8).unwrap().is_none());
    assert!(plan.next(&store).unwrap().is_none());
}

#[test]
fn a_mutable_batch_is_charged_too() {
    let mut store = GraphStore::new();
    let mut plan = amp(10);
    enforce(&mut plan, 5);
    let batch = plan
        .next_batch_mut(&mut store, "default", 5)
        .unwrap()
        .unwrap();
    assert_eq!(batch.records.len(), 5);
    assert!(plan.next_batch_mut(&mut store, "default", 5).is_err());
}

#[test]
fn reset_clears_the_per_pass_count_but_not_the_query_total() {
    let store = GraphStore::new();
    let mut plan = amp(1);
    // Per-operator budget 1 => query total 20.
    enforce(&mut plan, 1);
    let mut passes = 0;
    let err = loop {
        match plan.next(&store) {
            Ok(Some(_)) => {}
            Ok(None) => {
                passes += 1;
                plan.reset();
            }
            Err(e) => break e,
        }
        assert!(passes < 100, "the total budget never fired");
    };
    assert_eq!(passes, 20, "each pass stays within the per-pass bound");
    let (code, msg) = coded(err);
    assert_eq!(code, error_code::ROW_BUDGET_EXCEEDED);
    assert!(msg.contains("the plan produced 21 rows in total"), "{msg}");
    assert!(msg.contains("row budget is 20"), "{msg}");
    assert!(msg.contains("Operator Amp was the one"), "{msg}");
}

#[test]
fn the_wrapper_forwards_every_optional_method() {
    let mut plan = amp(0);
    enforce(&mut plan, 10);
    assert!(plan.try_push_limit(11));
    assert!(!plan.try_push_limit(1));
    assert!(plan.hint_early_stop(12));
    assert!(!plan.hint_early_stop(1));
    assert!(plan.retain_property_reads("keep", "p"));
    assert!(!plan.retain_property_reads("other", "p"));
    assert_eq!(plan.take_retained_reads(), Some(vec![None]));
    assert!(plan.is_mutating());
    assert!(plan.amplifies_rows());
    assert!(plan.children_mut().is_empty());
    assert_eq!(plan.describe().name, "Amp");
}

#[test]
fn a_non_amplifying_operator_is_not_wrapped() {
    let store = GraphStore::new();
    let mut plan: OperatorBox = Box::new(Amp {
        n: 10,
        at: 0,
        amplifies: false,
    });
    enforce(&mut plan, 2);
    let mut rows = 0;
    while plan.next(&store).unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, 10, "a scan is never refused");
}

#[test]
fn a_zero_budget_disables_enforcement() {
    let store = GraphStore::new();
    let mut plan = amp(10);
    enforce(&mut plan, 0);
    let mut rows = 0;
    while plan.next(&store).unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, 10);
}

#[test]
fn query_total_with_no_budget_or_no_rows_never_fires() {
    let unlimited = QueryTotal {
        produced: std::sync::atomic::AtomicU64::new(0),
        budget: 0,
    };
    assert_eq!(unlimited.charge(1_000_000), None);
    let limited = QueryTotal {
        produced: std::sync::atomic::AtomicU64::new(0),
        budget: 2,
    };
    assert_eq!(limited.charge(0), None);
    assert_eq!(limited.charge(2), None);
    assert_eq!(limited.charge(1), Some(3));
}

#[test]
fn the_vacated_placeholder_yields_nothing() {
    let store = GraphStore::new();
    let mut v = Vacated;
    v.reset();
    assert!(v.next(&store).unwrap().is_none());
}
