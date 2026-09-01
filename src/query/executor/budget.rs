//! A per-operator row budget, so an exploding intermediate fails instead of
//! running (`PERF-05`).
//!
//! # What this answers
//!
//! Nothing bounded intermediate cardinality. A three-way cartesian product
//! over 200 nodes materialized eight million rows and finished, and the same
//! query over 2,000 nodes materializes eight *billion* -- it does not fail,
//! it just does not come back, and the operator responsible is invisible
//! while it happens. `examples/plan_budget_probe.rs` measured that; this is
//! the response to it.
//!
//! # How it works
//!
//! The same shape as `PROFILE` (see `profile.rs`): the plan is a Volcano tree
//! of `Box<dyn PhysicalOperator>`, and every node is wrapped in a
//! [`BudgetedOperator`] that counts the rows it hands upward and refuses once
//! it passes the budget. Wrapping rather than editing each operator is what
//! makes this tractable -- there are dozens of operators and they construct in
//! many places, and a rule enforced in one wrapper cannot be forgotten by the
//! next operator someone adds.
//!
//! # Choices worth stating
//!
//! * **The error names the operator and the budget.** "Query failed" would
//!   send someone hunting through a plan; "CartesianProduct produced more than
//!   50,000,000 rows" is a place to look and a number to change. A budget that
//!   refuses anonymously is barely better than a hang.
//! * **A refusal is a client error, not a crash.** It carries its own code so
//!   a caller can branch on it, rather than joining the 144 sites behind the
//!   generic runtime code that `LANG-12` exists to break up.
//! * **The default is generous on purpose.** The point is to stop explosions,
//!   not to second-guess large-but-real queries. `SNB-Interactive` at SF10
//!   does not approach 50M rows through any single operator; a cartesian
//!   blowup passes it in the first second. Set `SAMYAMA_ROW_BUDGET=0` to
//!   disable, or any other value to change it.
//! * **It counts rows produced, not rows retained.** An operator that streams
//!   a billion rows through without holding them is still doing a billion
//!   rows of work, and that is the thing worth refusing.
//! * **Only amplifying operators are budgeted**, via
//!   `PhysicalOperator::amplifies_rows`. This is the load-bearing decision. A
//!   blanket per-operator budget refuses `MATCH (n) RETURN count(n)` on a
//!   187M-node graph -- one we publish ourselves -- because the scan produces
//!   187M rows. But a scan is bounded by the data; it is large, not
//!   exploding. What turns a large graph into an impossible one is an
//!   operator whose output is the product of its inputs, and today that is
//!   `CartesianProduct`. The default is `false` so a new operator is never
//!   silently enrolled: a missed explosion leaves today's behaviour, while a
//!   false refusal breaks a query that works.
//!
//! # What it does not catch, stated rather than discovered
//!
//! The count resets with the operator, so a budgeted operator driven by a
//! nested loop starts again on each `reset()`. A cartesian product re-executed
//! a million times can therefore do a million budgets' worth of work without
//! ever crossing one. Cartesian products sit near the top of the plans this
//! guards against, so it holds in practice -- but it is a per-pass bound, not
//! a per-query one, and reading it as the latter would be wrong.

use crate::graph::GraphStore;
use crate::query::error_code;
use crate::query::executor::operator::{
    OperatorBox, OperatorDescription, PhysicalOperator,
};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, RecordBatch};

/// Rows a single operator may produce before the query is refused.
///
/// Chosen to sit far above any legitimate operator in the benchmark suites and
/// far below what an unbounded cartesian product reaches in its first moments.
pub const DEFAULT_ROW_BUDGET: u64 = 50_000_000;

/// The budget in force, from `SAMYAMA_ROW_BUDGET` if set.
///
/// `0` disables enforcement. An unparseable value falls back to the default
/// rather than to unlimited: a typo in an environment variable must not
/// silently turn a guard off, which is the failure mode where the guard is
/// discovered to have been absent only after the incident.
pub fn configured_budget() -> u64 {
    match std::env::var("SAMYAMA_ROW_BUDGET") {
        Ok(v) => v.trim().parse::<u64>().unwrap_or(DEFAULT_ROW_BUDGET),
        Err(_) => DEFAULT_ROW_BUDGET,
    }
}

/// Wraps one operator and refuses once it has produced more than `budget`.
struct BudgetedOperator {
    inner: OperatorBox,
    /// The operator's name, captured before wrapping so the message names the
    /// plan the planner produced rather than "BudgetedOperator".
    name: String,
    produced: u64,
    budget: u64,
}

impl BudgetedOperator {
    fn charge(&mut self, rows: usize) -> ExecutionResult<()> {
        self.produced += rows as u64;
        if self.produced > self.budget {
            return Err(ExecutionError::Coded {
                code: error_code::ROW_BUDGET_EXCEEDED,
                message: format!(
                    "operator {} produced more than {} rows (the per-operator row \
                     budget) and the query was refused rather than run to \
                     completion. This is usually an unintended cartesian product \
                     -- check for a MATCH with no relationship joining its \
                     patterns. Raise or disable the budget with SAMYAMA_ROW_BUDGET \
                     (0 disables it).",
                    self.name, self.budget
                ),
            });
        }
        Ok(())
    }
}

impl PhysicalOperator for BudgetedOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        let out = self.inner.next(store)?;
        self.charge(usize::from(out.is_some()))?;
        Ok(out)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        let out = self.inner.next_mut(store, tenant_id)?;
        self.charge(usize::from(out.is_some()))?;
        Ok(out)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let out = self.inner.next_batch(store, batch_size)?;
        self.charge(out.as_ref().map_or(0, |b| b.records.len()))?;
        Ok(out)
    }

    fn next_batch_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let out = self.inner.next_batch_mut(store, tenant_id, batch_size)?;
        self.charge(out.as_ref().map_or(0, |b| b.records.len()))?;
        Ok(out)
    }

    // Everything else forwards, for the reason `ProfiledOperator` gives: an
    // operator that overrides `next_batch` must keep using its own override,
    // and `try_push_limit` must still reach the scan underneath or enforcing a
    // budget would change the plan it is enforcing on.
    fn try_push_limit(&mut self, n: usize) -> bool {
        self.inner.try_push_limit(n)
    }

    fn reset(&mut self) {
        self.produced = 0;
        self.inner.reset()
    }

    fn is_mutating(&self) -> bool {
        self.inner.is_mutating()
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        self.inner.children_mut()
    }

    /// Forwarded, like `describe`. `enforce` runs once per plan today, so
    /// nothing re-walks a wrapped tree -- but a wrapper that answered `false`
    /// here would quietly make a second pass skip the very operator the first
    /// pass wrapped, and that is a bug best not left available.
    fn amplifies_rows(&self) -> bool {
        self.inner.amplifies_rows()
    }

    fn describe(&self) -> OperatorDescription {
        self.inner.describe()
    }
}

/// A stand-in used only while a slot is being swapped. Never executed.
struct Vacated;

impl PhysicalOperator for Vacated {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Ok(None)
    }
    fn reset(&mut self) {}
}

fn wrap(slot: &mut OperatorBox, budget: u64) {
    // Name taken before wrapping, so the refusal names the planner's operator.
    let name = slot.describe().name;
    let amplifies = slot.amplifies_rows();
    for child in slot.children_mut() {
        wrap(child, budget);
    }
    // Only amplifying operators are budgeted. A scan of a 187M-node graph
    // produces 187M rows and is reading the data it was asked for; refusing it
    // would break `MATCH (n) RETURN count(n)` on graphs we publish. Wrapping
    // everything also put a virtual call on every operator of every query,
    // which is the cost `PROFILE` is opt-in to avoid.
    if amplifies {
        let inner = std::mem::replace(slot, Box::new(Vacated));
        *slot = Box::new(BudgetedOperator { inner, name, produced: 0, budget });
    }
}

/// Wrap every node of `root` so each refuses past `budget` rows.
///
/// A budget of `0` is a no-op and leaves the tree untouched, so a disabled
/// budget costs nothing at all -- not a branch, not a counter.
pub fn enforce(root: &mut OperatorBox, budget: u64) {
    if budget == 0 {
        return;
    }
    wrap(root, budget);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Label, PropertyValue};
    use crate::query::QueryEngine;

    fn store(n: usize) -> GraphStore {
        let mut s = GraphStore::new();
        for i in 0..n {
            let node = s.create_node_with_labels([Label::new("N")]);
            s.set_node_property("default", node, "i", PropertyValue::Integer(i as i64)).unwrap();
        }
        s
    }

    /// The budget refuses a blowup, and says which operator and what limit.
    #[test]
    fn an_exploding_operator_is_refused_by_name() {
        let s = store(120);
        let engine = QueryEngine::new().with_row_budget(10_000);
        let err = engine
            .execute("MATCH (a:N), (b:N), (c:N) RETURN count(*)", &s)
            .expect_err("1,728,000 rows must not pass a 10,000 row budget");
        let msg = err.to_string();
        assert!(msg.contains(error_code::ROW_BUDGET_EXCEEDED), "{msg}");
        assert!(msg.contains("10000"), "the message must name the budget: {msg}");
        assert!(
            msg.contains("CartesianProduct"),
            "the message must name the operator, not just fail: {msg}"
        );
    }

    /// The converse, and the one that matters: a budget that refuses
    /// everything would pass the test above while making the engine useless.
    #[test]
    fn a_query_inside_the_budget_is_untouched() {
        let s = store(120);
        let engine = QueryEngine::new().with_row_budget(10_000);
        let batch = engine
            .execute("MATCH (a:N) RETURN count(*)", &s)
            .expect("120 rows are well inside a 10,000 row budget");
        assert_eq!(batch.records.len(), 1);
    }

    /// The case the `amplifies_rows` distinction exists for, and the one a
    /// blanket per-operator budget got wrong: a plain scan producing far more
    /// rows than the budget must run. `MATCH (n) RETURN count(n)` over a
    /// 187M-node graph is a query we publish results for; refusing it as an
    /// "explosion" would be a false refusal against our own data.
    #[test]
    fn a_large_scan_is_not_an_explosion_and_is_not_refused() {
        let s = store(500);
        let engine = QueryEngine::new().with_row_budget(100);
        let batch = engine
            .execute("MATCH (n:N) RETURN n", &s)
            .expect("a 500-row scan under a 100-row budget must still run: a scan \
                     is bounded by the data, not amplifying");
        assert_eq!(batch.records.len(), 500);
    }

    /// Disabling it must actually disable it, or `SAMYAMA_ROW_BUDGET=0` is a
    /// lie people will discover in an incident.
    #[test]
    fn a_zero_budget_enforces_nothing() {
        let s = store(60);
        let engine = QueryEngine::new().with_row_budget(0);
        engine
            .execute("MATCH (a:N), (b:N), (c:N) RETURN count(*)", &s)
            .expect("a zero budget must not refuse anything");
    }

    /// A bad environment value must not silently disable the guard.
    #[test]
    fn an_unparseable_budget_falls_back_to_the_default_not_to_unlimited() {
        // Scoped: the variable is process-wide and other tests read it.
        let prev = std::env::var("SAMYAMA_ROW_BUDGET").ok();
        std::env::set_var("SAMYAMA_ROW_BUDGET", "banana");
        let got = configured_budget();
        match prev {
            Some(v) => std::env::set_var("SAMYAMA_ROW_BUDGET", v),
            None => std::env::remove_var("SAMYAMA_ROW_BUDGET"),
        }
        assert_eq!(got, DEFAULT_ROW_BUDGET);
    }
}
