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

/// Rows the whole plan may produce, across every pass, before the query is
/// refused.
///
/// A multiple of the per-operator budget rather than equal to it: a plan with
/// several amplifying operators, or one legitimately re-executed by a nested
/// loop, does more total work than any single pass and is not thereby a runaway.
/// The point of this bound is the case the per-pass one cannot see at all, not
/// to second-guess the per-pass number.
pub const QUERY_BUDGET_MULTIPLE: u64 = 20;

/// A counter shared by every budgeted operator in one plan.
///
/// `reset()` clears the per-pass count and deliberately does **not** clear this.
/// That difference is the whole point: a cartesian product re-executed by a
/// nested loop starts its per-pass count again on every iteration, so a million
/// passes of 50,000 rows each never cross a 50,000,000 per-pass budget while
/// doing twenty times a budget's worth of work. The module said so and nothing
/// enforced it (PERF-05).
#[derive(Debug)]
struct QueryTotal {
    produced: std::sync::atomic::AtomicU64,
    budget: u64,
}

impl QueryTotal {
    fn charge(&self, rows: u64) -> Option<u64> {
        if self.budget == 0 || rows == 0 {
            return None;
        }
        let total = self
            .produced
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed)
            + rows;
        (total > self.budget).then_some(total)
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
    /// Shared across the plan, and not cleared by `reset()`.
    total: std::sync::Arc<QueryTotal>,
}

impl BudgetedOperator {
    fn charge(&mut self, rows: usize) -> ExecutionResult<()> {
        // The per-query bound first: it is the one that catches work spread
        // across passes, and reporting the per-pass number for a query that
        // crossed the total would name the wrong limit.
        if let Some(total) = self.total.charge(rows as u64) {
            return Err(ExecutionError::Coded {
                code: error_code::ROW_BUDGET_EXCEEDED,
                message: format!(
                    "the plan produced {} rows in total (the per-query row budget \
                     is {}) and the query was refused rather than run to \
                     completion. No single operator crossed the per-operator \
                     budget: this is work spread across repeated passes, usually a \
                     cartesian product driven by a nested loop. Operator {} was the \
                     one that crossed the total. Raise or disable the budget with \
                     SAMYAMA_ROW_BUDGET (0 disables both bounds).",
                    total, self.total.budget, self.name
                ),
            });
        }
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

fn wrap(slot: &mut OperatorBox, budget: u64, total: &std::sync::Arc<QueryTotal>) {
    // Name taken before wrapping, so the refusal names the planner's operator.
    let name = slot.describe().name;
    let amplifies = slot.amplifies_rows();
    for child in slot.children_mut() {
        wrap(child, budget, total);
    }
    // Only amplifying operators are budgeted. A scan of a 187M-node graph
    // produces 187M rows and is reading the data it was asked for; refusing it
    // would break `MATCH (n) RETURN count(n)` on graphs we publish. Wrapping
    // everything also put a virtual call on every operator of every query,
    // which is the cost `PROFILE` is opt-in to avoid.
    if amplifies {
        let inner = std::mem::replace(slot, Box::new(Vacated));
        *slot = Box::new(BudgetedOperator {
            inner,
            name,
            produced: 0,
            budget,
            total: std::sync::Arc::clone(total),
        });
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
    // One counter per plan, shared by every wrapped operator in it, so the two
    // bounds answer different questions: `budget` is what one operator may do in
    // one pass, and this is what the whole plan may do across all of them.
    let total = std::sync::Arc::new(QueryTotal {
        produced: std::sync::atomic::AtomicU64::new(0),
        budget: budget.saturating_mul(QUERY_BUDGET_MULTIPLE),
    });
    wrap(root, budget, &total);
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

    /// The hole this module documented and did not close: work spread across
    /// repeated passes.
    ///
    /// `reset()` clears the per-pass count, so an amplifying operator driven by a
    /// nested loop starts again on every iteration. Each pass can stay under the
    /// per-operator budget while the plan as a whole does many budgets' worth of
    /// work — "a per-pass bound, not a per-query one", which this module stated and
    /// nothing enforced.
    ///
    /// Tested at the mechanism rather than through a query, because **no query
    /// shape the planner produces today drives an amplifying operator across
    /// passes**: `UNWIND` plans *above* `CartesianProduct` rather than driving it,
    /// and there is no `Apply` or nested-loop operator. Writing a query-level test
    /// would mean writing one that cannot fail. See
    /// `no_plan_today_re_executes_an_amplifying_operator`, which pins that reason
    /// so this test's justification fails when it stops being true.
    #[test]
    fn reset_clears_the_pass_count_and_not_the_query_total() {
        let total = std::sync::Arc::new(QueryTotal {
            produced: std::sync::atomic::AtomicU64::new(0),
            budget: 250,
        });
        let mut op = BudgetedOperator {
            inner: Box::new(Vacated),
            name: "CartesianProduct".to_string(),
            produced: 0,
            budget: 100,
            total: std::sync::Arc::clone(&total),
        };

        // Three passes of 90 rows: each is inside the 100-row per-pass budget, and
        // together they cross the 250-row total. This is the shape the per-pass
        // bound cannot see.
        for pass in 0..2 {
            op.charge(90).unwrap_or_else(|e| panic!("pass {pass} of 90 rows is inside the per-pass budget: {e}"));
            assert_eq!(op.produced, 90, "the pass count must not accumulate across passes");
            op.reset();
            assert_eq!(op.produced, 0, "reset must clear the pass count");
        }

        let err = op.charge(90).expect_err("270 rows across three passes must cross a 250-row total");
        let msg = err.to_string();
        assert!(msg.contains(error_code::ROW_BUDGET_EXCEEDED), "{msg}");
        assert!(
            msg.contains("per-query row budget"),
            "a query that crossed the total must not be reported against the \
             per-operator limit, which it never crossed: {msg}"
        );
        assert!(msg.contains("CartesianProduct"), "the message must name the operator: {msg}");
    }

    /// Why the test above is a unit test.
    ///
    /// If a planner change introduces a nested-loop or `Apply` operator, an
    /// amplifying operator becomes reachable across passes and the per-query bound
    /// becomes exercisable — and testable — through a query. This fails at that
    /// point, which is when the reasoning above needs revisiting.
    #[test]
    fn no_plan_today_re_executes_an_amplifying_operator() {
        let s = store(6);
        let engine = QueryEngine::new();
        for q in [
            "UNWIND range(1, 3) AS k MATCH (a:N), (b:N) RETURN count(*)",
            "MATCH (x:N) WITH x MATCH (a:N), (b:N) RETURN count(*)",
            "UNWIND range(1,2) AS k MATCH (a:N) WITH k, a MATCH (b:N), (c:N) RETURN count(*)",
        ] {
            let plan = engine
                .execute(&format!("EXPLAIN {q}"), &s)
                .expect("EXPLAIN")
                .records[0]
                .get("plan")
                .map(|v| format!("{v:?}"))
                .unwrap_or_default();
            assert!(
                !plan.contains("Apply") && !plan.contains("NestedLoop"),
                "a plan now drives an operator across passes; the per-query budget \
                 is reachable from a query and should be tested through one: {q}\n{plan}"
            );
        }
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
