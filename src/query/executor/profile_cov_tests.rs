//! Additional unit tests for PROFILE instrumentation: every forwarded
//! operator method, batch counting and report formatting edge cases.

use super::*;
use crate::graph::PropertyValue;
use crate::query::executor::Value;

/// A leaf that yields `n` integer rows and answers every optional trait method
/// distinctively, so forwarding through the wrapper is observable.
struct Probe {
    n: usize,
    at: usize,
    details: String,
    resets: usize,
}

fn probe(n: usize) -> Probe {
    Probe {
        n,
        at: 0,
        details: String::new(),
        resets: 0,
    }
}

impl PhysicalOperator for Probe {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.at >= self.n {
            return Ok(None);
        }
        let mut r = Record::new();
        r.bind("i", Value::Property(PropertyValue::Integer(self.at as i64)));
        self.at += 1;
        Ok(Some(r))
    }
    fn reset(&mut self) {
        self.at = 0;
        self.resets += 1;
    }
    fn try_push_limit(&mut self, n: usize) -> bool {
        n == 7
    }
    fn hint_early_stop(&mut self, n: usize) -> bool {
        n == 3
    }
    fn retain_property_reads(&mut self, variable: &str, property: &str) -> bool {
        variable == "n" && property == "x"
    }
    fn take_retained_reads(&mut self) -> Option<Vec<Option<PropertyValue>>> {
        Some(vec![Some(PropertyValue::Integer(1)), None])
    }
    fn is_mutating(&self) -> bool {
        true
    }
    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Probe".to_string(),
            details: self.details.clone(),
            children: Vec::new(),
        }
    }
}

#[test]
fn the_wrapper_forwards_every_optional_method() {
    let mut plan: OperatorBox = Box::new(probe(0));
    let nodes = instrument(&mut plan);
    assert_eq!(nodes.len(), 1);
    assert!(plan.try_push_limit(7));
    assert!(!plan.try_push_limit(8));
    assert!(plan.hint_early_stop(3));
    assert!(!plan.hint_early_stop(4));
    assert!(plan.retain_property_reads("n", "x"));
    assert!(!plan.retain_property_reads("n", "y"));
    assert_eq!(
        plan.take_retained_reads(),
        Some(vec![Some(PropertyValue::Integer(1)), None])
    );
    assert!(plan.is_mutating());
    assert!(plan.children_mut().is_empty());
    assert_eq!(plan.describe().name, "Probe");
}

#[test]
fn reset_is_forwarded_so_a_wrapped_plan_replays() {
    let store = GraphStore::new();
    let mut plan: OperatorBox = Box::new(probe(2));
    let nodes = instrument(&mut plan);
    assert!(plan.next(&store).unwrap().is_some());
    assert!(plan.next(&store).unwrap().is_some());
    assert!(plan.next(&store).unwrap().is_none());
    plan.reset();
    assert!(plan.next(&store).unwrap().is_some());
    assert_eq!(nodes[0].rows(), 3);
    assert_eq!(nodes[0].calls(), 4);
}

#[test]
fn next_mut_is_counted_like_next() {
    let mut store = GraphStore::new();
    let mut plan: OperatorBox = Box::new(probe(3));
    let nodes = instrument(&mut plan);
    let mut rows = 0;
    while plan.next_mut(&mut store, "default").unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, 3);
    assert_eq!(nodes[0].rows(), 3);
    assert_eq!(nodes[0].calls(), 4, "three rows and the terminating pull");
}

#[test]
fn batches_count_every_row_they_carry() {
    let store = GraphStore::new();
    let mut plan: OperatorBox = Box::new(probe(5));
    let nodes = instrument(&mut plan);
    let first = plan.next_batch(&store, 4).unwrap().unwrap();
    assert_eq!(first.records.len(), 4);
    let second = plan.next_batch(&store, 4).unwrap().unwrap();
    assert_eq!(second.records.len(), 1);
    assert!(plan.next_batch(&store, 4).unwrap().is_none());
    assert_eq!(nodes[0].rows(), 5);
    assert_eq!(nodes[0].calls(), 3);
}

#[test]
fn mutable_batches_count_every_row_they_carry() {
    let mut store = GraphStore::new();
    let mut plan: OperatorBox = Box::new(probe(3));
    let nodes = instrument(&mut plan);
    let batch = plan
        .next_batch_mut(&mut store, "default", 10)
        .unwrap()
        .unwrap();
    assert_eq!(batch.records.len(), 3);
    assert!(plan
        .next_batch_mut(&mut store, "default", 10)
        .unwrap()
        .is_none());
    assert_eq!(nodes[0].rows(), 3);
    assert_eq!(nodes[0].calls(), 2);
}

#[test]
fn the_vacated_placeholder_yields_nothing() {
    let store = GraphStore::new();
    let mut v = Vacated;
    assert!(v.next(&store).unwrap().is_none());
    v.reset();
    assert!(v.next(&store).unwrap().is_none());
}

#[test]
fn a_report_over_zero_wall_time_does_not_divide_by_zero() {
    let mut plan: OperatorBox = Box::new(probe(0));
    let nodes = instrument(&mut plan);
    let text = report(&nodes, Duration::ZERO, Some(Duration::ZERO));
    assert!(text.contains("(0.0%)"), "{text}");
    assert!(text.contains("instrumentation costs 0.0x"), "{text}");
    // Nothing ran, so no operator has exclusive time and none is ranked.
    let hottest = text
        .split("Hottest operators by exclusive time:\n")
        .nth(1)
        .unwrap();
    assert!(!hottest.starts_with("  1."), "{text}");
    assert!(!text.contains("Outside the operator tree"), "{text}");
}

#[test]
fn a_report_names_time_outside_the_tree_and_the_uninstrumented_run() {
    let store = GraphStore::new();
    let mut plan: OperatorBox = Box::new(probe(10));
    let nodes = instrument(&mut plan);
    while plan.next(&store).unwrap().is_some() {}
    // A wall time far above anything the operators could have taken.
    let text = report(
        &nodes,
        Duration::from_secs(3600),
        Some(Duration::from_secs(1800)),
    );
    assert!(text.contains("Outside the operator tree"), "{text}");
    assert!(text.contains("instrumentation costs 2.0x"), "{text}");
    assert!(
        text.contains("Uninstrumented execution of the same plan"),
        "{text}"
    );
}

#[test]
fn long_details_are_truncated_in_the_tree() {
    let mut p = probe(0);
    p.details = "a".repeat(100);
    let mut plan: OperatorBox = Box::new(p);
    let nodes = instrument(&mut plan);
    assert_eq!(nodes[0].details.len(), 100);
    let text = report(&nodes, Duration::from_millis(1), None);
    let expected = format!("Probe ({})", "a".repeat(24));
    assert!(text.contains(&expected), "{text}");
    assert!(!text.contains(&"a".repeat(25)), "{text}");
}
