//! Per-operator wall-clock and row attribution for `PROFILE` (`CH-PROFILE-01`).
//!
//! # What this answers
//!
//! `PROFILE` used to report one number: how long the whole query took. That is
//! the same information the client already had, so the first question anyone
//! asks about a 6-second complex read — *where did the six seconds go?* — had
//! no answer inside the engine, and the only way to get one was a sampling
//! profiler on a debug build.
//!
//! The roadmap gate is explicit about the bar: **≥90% of wall-clock
//! attributed** for `IC1/IC6/IC9/BI-6/CR-7`. The charter is equally explicit
//! about why it comes first — deep multi-hop traversal is 1–4 orders of
//! magnitude behind competitors, and "measurement before construction" is the
//! rule that the fabricated-speedup episode exists to enforce. Optimising the
//! wrong operator is the expensive failure here, and it is the one that a
//! single total makes likely.
//!
//! # How it works
//!
//! The plan is a Volcano tree of `Box<dyn PhysicalOperator>`. Instrumentation
//! wraps every node in a [`ProfiledOperator`] that times the call and counts
//! the rows that come back. Because a parent's `next()` calls its children's
//! `next()`, a node's measured time **includes its children**; the exclusive
//! ("self") time is that total minus the totals of its children, which is what
//! the report ranks by.
//!
//! Two consequences worth stating rather than discovering:
//!
//! * **The instrumented run is slower than the real one.** Two `Instant::now()`
//!   calls per `next()` is ~40 ns against operators that can do useful work in
//!   less than that. On a row-at-a-time plan producing millions of intermediate
//!   rows the overhead is real, so the report prints the instrumented total
//!   next to the uninstrumented one and never claims the two are the same
//!   number. Proportions are what a profile is for; absolute latency comes from
//!   the benchmark.
//! * **Nothing is instrumented unless `PROFILE` was asked for.** The wrapper
//!   is built at profile time and the ordinary execution path never allocates
//!   it, so a normal query pays nothing at all — not a branch, not a counter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::graph::GraphStore;
use crate::query::executor::operator::{
    OperatorBox, OperatorDescription, PhysicalOperator,
};
use crate::query::executor::{ExecutionResult, Record, RecordBatch};

/// Counters written by one instrumented operator.
///
/// Atomics rather than a `Cell` because `PhysicalOperator` is `Send`; the
/// operations are all `Relaxed` since nothing orders on them and the reader
/// runs after execution has finished.
#[derive(Debug, Default)]
pub struct NodeCounters {
    /// Nanoseconds spent inside this operator's `next`/`next_batch`,
    /// **including** its children.
    nanos: AtomicU64,
    /// Rows this operator handed to its parent.
    rows: AtomicU64,
    /// Times this operator was pulled from. A high call count against a low
    /// row count is an operator being asked and answering nothing.
    calls: AtomicU64,
}

/// One node of the instrumented plan, in pre-order.
pub struct ProfileNode {
    pub name: String,
    pub details: String,
    pub depth: usize,
    /// Index into the same `Vec`; `None` for the root.
    pub parent: Option<usize>,
    counters: Arc<NodeCounters>,
}

impl ProfileNode {
    /// Time in this operator and everything below it.
    pub fn inclusive(&self) -> Duration {
        Duration::from_nanos(self.counters.nanos.load(Ordering::Relaxed))
    }

    /// Rows produced.
    pub fn rows(&self) -> u64 {
        self.counters.rows.load(Ordering::Relaxed)
    }

    /// Pull calls received.
    pub fn calls(&self) -> u64 {
        self.counters.calls.load(Ordering::Relaxed)
    }
}

/// A wrapper that times the operator it holds and forwards everything else.
///
/// Deliberately forwards rather than reimplements: an operator that overrides
/// `next_batch` for vectorised execution must keep using its own override, and
/// `try_push_limit` must still reach the scan underneath or instrumenting a
/// query would change the plan it measures.
struct ProfiledOperator {
    inner: OperatorBox,
    counters: Arc<NodeCounters>,
}

impl ProfiledOperator {
    fn record<T>(&self, started: Instant, produced: usize, out: T) -> T {
        self.counters
            .nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.counters.calls.fetch_add(1, Ordering::Relaxed);
        if produced > 0 {
            self.counters.rows.fetch_add(produced as u64, Ordering::Relaxed);
        }
        out
    }
}

impl PhysicalOperator for ProfiledOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        let started = Instant::now();
        let result = self.inner.next(store);
        let produced = usize::from(matches!(result, Ok(Some(_))));
        self.record(started, produced, result)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        let started = Instant::now();
        let result = self.inner.next_mut(store, tenant_id);
        let produced = usize::from(matches!(result, Ok(Some(_))));
        self.record(started, produced, result)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let started = Instant::now();
        let result = self.inner.next_batch(store, batch_size);
        let produced = match &result {
            Ok(Some(batch)) => batch.records.len(),
            _ => 0,
        };
        self.record(started, produced, result)
    }

    fn next_batch_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let started = Instant::now();
        let result = self.inner.next_batch_mut(store, tenant_id, batch_size);
        let produced = match &result {
            Ok(Some(batch)) => batch.records.len(),
            _ => 0,
        };
        self.record(started, produced, result)
    }

    fn try_push_limit(&mut self, n: usize) -> bool {
        self.inner.try_push_limit(n)
    }

    fn reset(&mut self) {
        self.inner.reset()
    }

    fn is_mutating(&self) -> bool {
        self.inner.is_mutating()
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        self.inner.children_mut()
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

/// Wrap every node of `root` in a [`ProfiledOperator`], returning the nodes in
/// pre-order so the report can be printed as a tree.
///
/// Names and details are taken **before** wrapping, so the report shows the
/// plan the planner produced rather than a tree of wrappers.
pub fn instrument(root: &mut OperatorBox) -> Vec<ProfileNode> {
    let mut nodes = Vec::new();
    wrap(root, None, 0, &mut nodes);
    nodes
}

fn wrap(slot: &mut OperatorBox, parent: Option<usize>, depth: usize, nodes: &mut Vec<ProfileNode>) {
    let description = slot.describe();
    let index = nodes.len();
    let counters = Arc::new(NodeCounters::default());
    nodes.push(ProfileNode {
        name: description.name,
        details: description.details,
        depth,
        parent,
        counters: Arc::clone(&counters),
    });

    for child in slot.children_mut() {
        wrap(child, Some(index), depth + 1, nodes);
    }

    let inner = std::mem::replace(slot, Box::new(Vacated));
    *slot = Box::new(ProfiledOperator { inner, counters });
}

/// Exclusive time per node: its own total minus its children's totals.
///
/// Clamped at zero. A child can out-measure its parent by a few nanoseconds
/// because the two timers are taken at slightly different points; reporting a
/// negative self-time would be worse than reporting nothing.
fn self_times(nodes: &[ProfileNode]) -> Vec<Duration> {
    let mut child_totals = vec![Duration::ZERO; nodes.len()];
    for node in nodes {
        if let Some(parent) = node.parent {
            child_totals[parent] += node.inclusive();
        }
    }
    nodes
        .iter()
        .zip(&child_totals)
        .map(|(node, children)| node.inclusive().saturating_sub(*children))
        .collect()
}

/// Render the profile: the plan tree annotated with time and rows, then the
/// operators ranked by exclusive time.
///
/// `wall` is the measured duration of the instrumented execution. The
/// attributed fraction is printed because the gate is stated as a fraction,
/// and because a low one means this report is not yet answering the question.
pub fn report(nodes: &[ProfileNode], wall: Duration, uninstrumented: Option<Duration>) -> String {
    let mut out = String::new();
    if nodes.is_empty() {
        return "--- Profile ---\n(no operators instrumented)\n".to_string();
    }

    let selves = self_times(nodes);
    let root_total = nodes[0].inclusive();
    let attributed: Duration = selves.iter().copied().sum();
    let pct = |d: Duration| {
        if wall.is_zero() {
            0.0
        } else {
            d.as_secs_f64() / wall.as_secs_f64() * 100.0
        }
    };

    out.push_str("--- Profile (per operator) ---\n");
    out.push_str(&format!(
        "{:<38} {:>10} {:>10} {:>7} {:>12} {:>10}\n",
        "operator", "self", "total", "self %", "rows", "calls"
    ));
    for (node, self_time) in nodes.iter().zip(&selves) {
        let indent = "  ".repeat(node.depth);
        let mut label = format!("{}{}", indent, node.name);
        if !node.details.is_empty() {
            // The details can be a whole predicate; a plan tree that wraps is
            // unreadable, so it is truncated here and printed in full by
            // EXPLAIN, which exists for exactly that.
            let detail: String = node.details.chars().take(24).collect();
            label.push_str(&format!(" ({})", detail));
        }
        label.truncate(38);
        out.push_str(&format!(
            "{:<38} {:>9.2}ms {:>9.2}ms {:>6.1}% {:>12} {:>10}\n",
            label,
            self_time.as_secs_f64() * 1000.0,
            node.inclusive().as_secs_f64() * 1000.0,
            pct(*self_time),
            node.rows(),
            node.calls(),
        ));
    }

    // Ranked, because on a deep plan the tree order is not the cost order and
    // the whole purpose is to say what to look at first.
    let mut ranked: Vec<(usize, Duration)> = selves.iter().copied().enumerate().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1));
    out.push_str("\nHottest operators by exclusive time:\n");
    for (i, (index, self_time)) in ranked.iter().take(5).enumerate() {
        if self_time.is_zero() {
            break;
        }
        out.push_str(&format!(
            "  {}. {:<28} {:>9.2}ms  {:>5.1}%  {} rows\n",
            i + 1,
            nodes[*index].name,
            self_time.as_secs_f64() * 1000.0,
            pct(*self_time),
            nodes[*index].rows(),
        ));
    }

    out.push_str(&format!(
        "\nInstrumented execution: {:.2}ms; attributed to operators: {:.2}ms ({:.1}%)\n",
        wall.as_secs_f64() * 1000.0,
        attributed.as_secs_f64() * 1000.0,
        pct(attributed),
    ));
    if root_total < wall {
        out.push_str(&format!(
            "Outside the operator tree: {:.2}ms (planning, result assembly, deadline checks)\n",
            (wall - root_total).as_secs_f64() * 1000.0
        ));
    }
    if let Some(plain) = uninstrumented {
        out.push_str(&format!(
            "Uninstrumented execution of the same plan: {:.2}ms — instrumentation costs {:.1}x.\n\
             Use the proportions above; take absolute latency from the benchmark, not from here.\n",
            plain.as_secs_f64() * 1000.0,
            if plain.is_zero() { 0.0 } else { wall.as_secs_f64() / plain.as_secs_f64() },
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Yields a fixed list of records, then nothing. A leaf, so it has no
    /// `children_mut` override and the walk must stop at it.
    struct FixedRows {
        records: Vec<Record>,
        cursor: usize,
    }

    impl PhysicalOperator for FixedRows {
        fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
            let out = self.records.get(self.cursor).cloned();
            if out.is_some() {
                self.cursor += 1;
            }
            Ok(out)
        }
        fn reset(&mut self) {
            self.cursor = 0;
        }
        fn describe(&self) -> OperatorDescription {
            OperatorDescription {
                name: "FixedRows".to_string(),
                details: format!("{} rows", self.records.len()),
                children: Vec::new(),
            }
        }
    }

    /// A plan of `FixedRows -> Limit`, so there is a parent and a child to
    /// attribute between.
    fn two_level_plan(rows: usize) -> OperatorBox {
        use crate::query::executor::operator::LimitOperator;
        let mut records = Vec::new();
        for i in 0..rows {
            let mut r = Record::new();
            r.bind(
                "n".to_string(),
                crate::query::executor::Value::Property(crate::graph::PropertyValue::Integer(i as i64)),
            );
            records.push(r);
        }
        Box::new(LimitOperator::new(
            Box::new(FixedRows { records, cursor: 0 }),
            rows,
        ))
    }

    fn drain(op: &mut OperatorBox, store: &GraphStore) -> usize {
        let mut n = 0;
        while let Some(_r) = op.next(store).unwrap() {
            n += 1;
        }
        n
    }

    #[test]
    fn instrumentation_finds_every_node_of_the_tree() {
        let mut plan = two_level_plan(3);
        let nodes = instrument(&mut plan);
        assert_eq!(nodes.len(), 2, "root and its input");
        assert_eq!(nodes[0].depth, 0);
        assert_eq!(nodes[0].parent, None);
        assert_eq!(nodes[1].depth, 1);
        assert_eq!(nodes[1].parent, Some(0));
    }

    #[test]
    fn instrumentation_does_not_change_the_answer() {
        // The failure that would make a profile worse than useless: measuring
        // a plan that no longer produces what the real one does.
        let store = GraphStore::new();
        let mut plain = two_level_plan(5);
        let expected = drain(&mut plain, &store);

        let mut profiled = two_level_plan(5);
        let _nodes = instrument(&mut profiled);
        assert_eq!(drain(&mut profiled, &store), expected);
    }

    #[test]
    fn rows_are_counted_per_operator() {
        let store = GraphStore::new();
        let mut plan = two_level_plan(4);
        let nodes = instrument(&mut plan);
        drain(&mut plan, &store);
        assert_eq!(nodes[0].rows(), 4, "the limit passed 4 rows up");
        assert_eq!(nodes[1].rows(), 4, "the input produced 4");
        assert!(nodes[0].calls() >= 5, "one pull per row plus the terminating one");
    }

    #[test]
    fn a_parents_total_includes_its_child() {
        let store = GraphStore::new();
        let mut plan = two_level_plan(200);
        let nodes = instrument(&mut plan);
        drain(&mut plan, &store);
        assert!(
            nodes[0].inclusive() >= nodes[1].inclusive(),
            "parent {:?} should include child {:?}",
            nodes[0].inclusive(),
            nodes[1].inclusive()
        );
    }

    #[test]
    fn self_time_never_goes_negative() {
        let store = GraphStore::new();
        let mut plan = two_level_plan(50);
        let nodes = instrument(&mut plan);
        drain(&mut plan, &store);
        // Duration cannot be negative, so the check that matters is that the
        // saturating subtraction is what produced them: the sum of self times
        // must not exceed the root's inclusive time.
        let selves = self_times(&nodes);
        let sum: Duration = selves.iter().copied().sum();
        assert!(sum <= nodes[0].inclusive() + Duration::from_millis(1));
    }

    #[test]
    fn the_report_names_the_hottest_operator_and_states_the_attributed_fraction() {
        let store = GraphStore::new();
        let mut plan = two_level_plan(500);
        let nodes = instrument(&mut plan);
        let started = Instant::now();
        drain(&mut plan, &store);
        let wall = started.elapsed();

        let text = report(&nodes, wall, None);
        assert!(text.contains("Hottest operators by exclusive time"), "{text}");
        assert!(text.contains("attributed to operators"), "{text}");
        assert!(text.contains("Limit"), "the tree must name real operators: {text}");
        assert!(text.contains("FixedRows"), "children must appear too: {text}");
    }

    #[test]
    fn an_empty_tree_reports_rather_than_panicking() {
        let text = report(&[], Duration::from_millis(1), None);
        assert!(text.contains("no operators instrumented"), "{text}");
    }
}
