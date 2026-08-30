//! Physical operators for query execution using the Volcano iterator model.
//!
//! # Volcano Iterator Model (ADR-007)
//!
//! The Volcano model (Goetz Graefe, 1990s) is the dominant query execution paradigm in
//! relational and graph databases. Each operator implements a `next()` method that returns
//! one record at a time, pulling from child operators on demand. This creates a pipeline
//! where data flows from leaf operators (scans) up through filters, joins, and projections
//! to the root operator that produces final results.
//!
//! # Physical Operators
//!
//! | Operator | Description |
//! |---|---|
//! | `NodeScanOperator` | Scans all nodes matching a label (like a table scan in SQL) |
//! | `IndexScanOperator` | Uses a B-tree index to find nodes matching a predicate |
//! | `FilterOperator` | Evaluates a WHERE predicate, discarding non-matching records |
//! | `ExpandOperator` | Traverses edges from bound nodes to discover neighbors (graph-native; no SQL equivalent without expensive JOINs) |
//! | `ExpandIntoOperator` | Checks if an edge exists between two already-bound nodes (a semi-join) |
//! | `ProjectOperator` | Evaluates RETURN expressions, materializing `NodeRef` → `Node` for output |
//! | `LimitOperator` / `SkipOperator` | LIMIT and SKIP clauses |
//! | `SortOperator` | ORDER BY with multi-key comparison |
//! | `AggregateOperator` | GROUP BY + aggregation functions (count, sum, avg, min, max, collect) |
//! | `JoinOperator` | Hash join for multi-pattern MATCH queries |
//! | `LeftOuterJoinOperator` | For OPTIONAL MATCH (preserves unmatched left rows with NULLs) |
//! | `CartesianProductOperator` | Cross product for disconnected patterns |
//! | `UnwindOperator` | Expands arrays into individual rows |
//! | `MergeOperator` | MERGE (upsert): CREATE if not exists, otherwise match |
//! | `ShortestPathOperator` | BFS/Dijkstra for `shortestPath()` function |
//! | `VectorSearchOperator` | HNSW approximate nearest neighbor search |
//! | `AlgorithmOperator` | Runs graph algorithms (PageRank, WCC, SCC, etc.) |
//! | DDL operators | `CreateIndex`, `DropIndex`, `CreateConstraint`, `ShowIndexes`, etc. |
//!
//! # Expression Evaluation
//!
//! The `eval_expression()` function recursively evaluates AST expressions against a record.
//! It handles property access (`n.name`), arithmetic (`a + b`), comparisons (`a > b`),
//! boolean logic (`AND`/`OR`/`NOT`), function calls (`toUpper()`, `count()`), CASE
//! expressions, list operations, and more.
//!
//! # Type Coercion and NULL Propagation
//!
//! Integer/Float automatic promotion (widening), String concatenation via `+`, and NULL
//! propagation following three-valued logic: any operation involving NULL returns NULL,
//! except `IS NULL` / `IS NOT NULL`.
//!
//! # Late Materialization
//!
//! Operators work with `Value::NodeRef(id)` instead of full `Value::Node(id, clone)`.
//! Property access goes through `resolve_property()`, which looks up the property from
//! the [`GraphStore`] on demand. Full materialization only happens at `ProjectOperator`
//! when the query returns a node variable. See ADR-012.
//!
//! # Metaheuristic Optimization Solvers
//!
//! `AlgorithmOperator` integrates 16 solvers from `samyama-optimization` (Jaya, Rao,
//! TLBO, Firefly, Cuckoo, GWO, GA, SA, Bat, ABC, GSA, NSGA2, MOTLBO, HS, FPA) for
//! solving continuous optimization problems within graph queries.
//!
//! # Rust Patterns
//!
//! - `Box<dyn PhysicalOperator>` — dynamic dispatch via trait objects for operator trees
//! - `&GraphStore` — lifetime-bounded borrow of the graph during execution
//! - `HashMap` — build phase of hash joins in `JoinOperator`
//! - `BTreeSet` — sorted unique results where ordering matters

use std::sync::Arc;
use crate::query::executor::record::PropertyCursor;
use crate::graph::{GraphStore, Label, NodeId, EdgeType};
use crate::query::ast::{Expression, BinaryOp, UnaryOp, Direction, Pattern};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value, RecordBatch};
use crate::graph::PropertyValue;
use std::collections::{BTreeSet, HashMap, HashSet};
use rayon::prelude::*;
use samyama_optimization::common::{Problem, SolverConfig, MultiObjectiveProblem};
use samyama_optimization::algorithms::{JayaSolver, RaoSolver, RaoVariant, TLBOSolver, FireflySolver, CuckooSolver, GWOSolver, GASolver, SASolver, BatSolver, ABCSolver, GSASolver, NSGA2Solver, MOTLBOSolver, HSSolver, FPASolver};
use ndarray::Array1;

// Thread-local query deadline for cooperative timeout inside operator materialization loops.
// Set by QueryExecutor before execution, checked by JoinOperator/AggregateOperator/SortOperator.
thread_local! {
    static QUERY_DEADLINE: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

/// Set the query deadline for the current thread (called by QueryExecutor)
pub fn set_query_deadline(deadline: Option<std::time::Instant>) {
    QUERY_DEADLINE.with(|d| d.set(deadline));
}

/// Check if the query deadline has been exceeded; returns Err if so
fn check_deadline() -> ExecutionResult<()> {
    QUERY_DEADLINE.with(|d| {
        if let Some(deadline) = d.get() {
            if std::time::Instant::now() > deadline {
                return Err(ExecutionError::RuntimeError("Query timed out".to_string()));
            }
        }
        Ok(())
    })
}

/// Extract node ID from a Value for identity comparison
fn node_id_of(v: &Value) -> Option<NodeId> {
    match v {
        Value::NodeRef(id) | Value::Node(id, _) => Some(*id),
        _ => None,
    }
}


/// Cypher equality with three-valued logic, for values that may contain null.
///
/// `None` means *unknown*. The rule is not "any null makes it null": a
/// definitive difference wins over an unknown one, because two lists that
/// differ in length or in a known element are unequal whatever the nulls say.
///
/// ```text
/// [1]        = [null]      -> null    (might be equal, cannot tell)
/// [1, null]  = [1, 2]      -> null
/// [1, null]  = [2, 3]      -> false   (the first pair settles it)
/// [1]        = [1, null]   -> false   (lengths differ)
/// [null]     = [null]      -> null
/// ```
///
/// Scalar nulls are handled by the caller's existing three-valued guard; this
/// is only reached for values that are not themselves null, which is why a
/// bare `Null` here can only appear *inside* a list or map.
fn cypher_equals(a: &PropertyValue, b: &PropertyValue) -> Option<bool> {
    use PropertyValue::*;
    match (a, b) {
        (Null, _) | (_, Null) => None,
        (Array(x), Array(y)) => {
            // A length difference is definitive -- no element comparison can
            // rescue it, so this is `false` and not `null`.
            if x.len() != y.len() {
                return Some(false);
            }
            let mut unknown = false;
            for (xi, yi) in x.iter().zip(y.iter()) {
                match cypher_equals(xi, yi) {
                    Some(false) => return Some(false),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            if unknown { None } else { Some(true) }
        }
        (Map(x), Map(y)) => {
            // Differing key sets are definitive, for the same reason.
            if x.len() != y.len() || !x.keys().all(|k| y.contains_key(k)) {
                return Some(false);
            }
            let mut unknown = false;
            for (k, xv) in x {
                match cypher_equals(xv, &y[k]) {
                    Some(false) => return Some(false),
                    None => unknown = true,
                    Some(true) => {}
                }
            }
            if unknown { None } else { Some(true) }
        }
        // **A number equals a number across the two representations.**
        // `1 = 1.0` is true in Cypher, and `PropertyValue`'s derived `PartialEq`
        // compares variants, so it was answering false -- a wrong answer in the
        // most ordinary comparison there is, and one that reads as a legitimate
        // "these differ" (#860).
        //
        // Compared as integers rather than through `as f64`, so no bit is lost
        // above 2^53: a float with a fractional part cannot equal an integer,
        // and one without is exact.
        (Integer(x), Float(y)) | (Float(y), Integer(x)) => Some(
            y.is_finite()
                && y.fract() == 0.0
                && *y >= i64::MIN as f64
                && *y <= i64::MAX as f64
                && (*y as i64) == *x,
        ),
        _ => Some(a == b),
    }
}

/// The instant "now" means for the duration of one statement.
///
/// Cypher fixes the clock **once per query**, not once per call. Without that,
/// `duration.inSeconds(datetime(), datetime())` returns `PT0.00000016S` -- the
/// two calls land microseconds apart -- where the TCK requires exactly `PT0S`.
///
/// It is not only a test artefact. `WHERE n.created < datetime() AND
/// n.expires > datetime()` should test one instant against both bounds, and a
/// row arriving between the two reads is judged against a moving target.
///
/// A thread-local rather than a parameter because `eval_function` is reached
/// from many operators and threading a clock through all of them would be a
/// large change for a small need. Set by `QueryExecutor::execute` at the start
/// of a statement and cleared by the guard on the way out, including on an
/// early return, so a stale value cannot leak into the next statement (#793).
pub(crate) mod statement_clock {
    use std::cell::Cell;

    thread_local! {
        static NOW: Cell<Option<i64>> = const { Cell::new(None) };
    }

    /// Fix "now" for this statement. The returned guard clears it on drop.
    pub fn begin() -> Guard {
        NOW.with(|c| c.set(Some(chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0))));
        Guard
    }

    /// The statement's instant, or the wall clock when no statement is active
    /// -- a direct `eval_function` call from a test, for instance.
    pub fn now() -> chrono::DateTime<chrono::Utc> {
        match NOW.with(|c| c.get()) {
            Some(n) => chrono::DateTime::from_timestamp_nanos(n),
            None => chrono::Utc::now(),
        }
    }

    pub struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            NOW.with(|c| c.set(None));
        }
    }
}


/// Build a `Duration` with `nanos` carried into `seconds`.
///
/// `nanos` is a sub-second remainder in `0..1_000_000_000`, and every producer
/// of a `Duration` has to leave it that way. `duration()` already does -- it
/// combines seconds and nanos into one i128 total before splitting (#814) --
/// but duration *arithmetic* added the fields straight across, so adding a
/// duration to itself gave `nanos: 1000000006` and rendered as
/// `P25Y10M58DT67H56M26.1000000006S` where openCypher says
/// `...M27.000000006S`: one second short, with the missing second sitting
/// unrendered inside the nanoseconds field (#1001).
///
/// Months and days are *not* carried into each other or into seconds. Cypher
/// keeps the three groups separate on purpose -- a month is not 30 days and a
/// day is not always 86,400 seconds across a DST boundary -- so normalising
/// them would change answers rather than tidy them.
///
/// `i128` for the same reason `duration()` uses it: the nanosecond product
/// overflows `i64` past roughly 292 years.
fn normalized_duration(months: i64, days: i64, seconds: i128, nanos: i128) -> PropertyValue {
    let total = seconds * 1_000_000_000 + nanos;
    // Truncating division, exactly as `duration()` does -- **not** floor
    // division. The two differ only for a negative total, and the difference
    // is not academic: flooring gives `{seconds: -1, nanos: 999_999_999}`,
    // which is the same instant but which the renderer, reading the sign off
    // each field, prints as `PT-1.999999999S` instead of `PT-0.000000001S`.
    // The representation carries the sign in `nanos`, and a normaliser that
    // does not follow that convention produces a correct value that renders
    // wrong.
    PropertyValue::Duration {
        months,
        days,
        seconds: (total / 1_000_000_000) as i64,
        nanos: (total % 1_000_000_000) as i32,
    }
}


/// Shared binary operator evaluation used by Project, Aggregate, and Sort operators
fn eval_binary_op(op: &BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
    // Identity comparison for the three entity kinds (Cypher: n1 = n2, r1 = r2,
    // p1 = p2).
    //
    // Only *nodes* were handled, so `r = r` on a relationship and `p1 = p2` on
    // two paths raised "Binary op requires property values" -- an error from
    // the most ordinary comparison there is, and the reason `WITH a MATCH ...
    // WHERE a = b` could not be written at all (#860).
    if matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
        let same = match (&left, &right) {
            (Value::NodeRef(a) | Value::Node(a, _), Value::NodeRef(b) | Value::Node(b, _)) => {
                Some(a == b)
            }
            (
                Value::EdgeRef(a, ..) | Value::Edge(a, _),
                Value::EdgeRef(b, ..) | Value::Edge(b, _),
            ) => Some(a == b),
            // Paths are equal when they visit the same nodes and traverse the
            // same relationships in the same order. The TCK's scenario is a
            // self-loop, where a path and its reverse have the identical node
            // sequence -- so structural equality is what it asks for, and a
            // direction-insensitive rule would be inventing something it does
            // not test.
            (
                Value::Path { nodes: n1, edges: e1 },
                Value::Path { nodes: n2, edges: e2 },
            ) => Some(n1 == n2 && e1 == e2),
            // An entity is never equal to a non-entity, and that is `false`
            // rather than an error or a null: the two are comparable, they
            // simply differ.
            (Value::NodeRef(_) | Value::Node(..) | Value::EdgeRef(..) | Value::Edge(..)
                | Value::Path { .. }, other)
            | (other, Value::NodeRef(_) | Value::Node(..) | Value::EdgeRef(..) | Value::Edge(..)
                | Value::Path { .. })
                if !matches!(other, Value::Property(PropertyValue::Null) | Value::Null) =>
            {
                Some(false)
            }
            _ => None,
        };
        if let Some(eq) = same {
            return Ok(Value::Property(PropertyValue::Boolean(
                if matches!(op, BinaryOp::Eq) { eq } else { !eq }
            )));
        }
    }

    // Ordering an entity against anything is **null**, not an error.
    //
    // Cypher's rule is that comparing across types yields null except between
    // numbers, and a node, relationship or path is just another type that does
    // not order. Raising instead took down the whole query: `Comparison2`
    // builds a list of one value per type and compares every pair, so a single
    // `TypeError` on `node < ''` lost all 90 rows including the numeric pair
    // the scenario is actually about (#840).
    //
    // Only the ordering operators. `=` and `<>` on two entities are identity
    // and are handled above; arithmetic on an entity stays an error, because
    // there `null` really would hide a mistake.
    if matches!(op, BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge)
        && (!matches!(left, Value::Property(_) | Value::Null)
            || !matches!(right, Value::Property(_) | Value::Null))
    {
        return Ok(Value::Property(PropertyValue::Null));
    }

    // Concatenation, at the `Value` level, before either side is narrowed to a
    // `PropertyValue`.
    //
    // A list has two spellings: `Value::Property(PropertyValue::Array)`, which
    // the parser folds a literal into, and `Value::List`, which an expression
    // builds because a `PropertyValue` cannot hold an entity. `+` only
    // understood the first, so `[1,2] + [3]` worked and
    // `[a.list2[1], a.list2[0]] + a.list` raised "Binary op requires property
    // values" -- the same operator, on the same kind of thing, decided by how
    // the list happened to be written (#986).
    //
    // Doing it here rather than after narrowing also keeps a list of nodes
    // concatenable, which narrowing would have destroyed.
    if matches!(op, BinaryOp::Add) {
        let as_items = |v: &Value| -> Option<Vec<Value>> {
            match v {
                Value::List(items) => Some(items.clone()),
                Value::Property(PropertyValue::Array(items)) => {
                    Some(items.iter().cloned().map(Value::Property).collect())
                }
                _ => None,
            }
        };
        // ...and hand back the *narrower* spelling whenever it fits. Returning
        // `Value::List` unconditionally was correct arithmetic and a
        // regression anyway: `IN` and slicing read the `Array` spelling, so
        // `[1]+[2] IN [3]+[4]` and `… + […][1..3]` broke -- the mirror image
        // of the bug being fixed. A list of properties concatenates to a list
        // of properties; only an entity forces the wider spelling.
        let narrow = |items: Vec<Value>| -> Value {
            let props: Option<Vec<PropertyValue>> =
                items.iter().map(|v| v.as_property().cloned()).collect();
            match props {
                Some(p) => Value::Property(PropertyValue::Array(p)),
                None => Value::List(items),
            }
        };
        match (as_items(&left), as_items(&right)) {
            (Some(mut a), Some(b)) => {
                a.extend(b);
                return Ok(narrow(a));
            }
            // Cypher appends a non-list to a list and prepends one to a list.
            // Null is not appended: `[1] + null` is null, which the narrowing
            // below already yields, so it must not be caught here.
            (Some(mut a), None) if !right.is_null() => {
                a.push(right);
                return Ok(narrow(a));
            }
            (None, Some(b)) if !left.is_null() => {
                let mut out = vec![left];
                out.extend(b);
                return Ok(narrow(out));
            }
            _ => {}
        }
    }

    let left_prop = match left {
        Value::Property(p) => p,
        Value::Null => PropertyValue::Null,
        _ => return Err(ExecutionError::TypeError("Binary op requires property values".to_string())),
    };
    let right_prop = match right {
        Value::Property(p) => p,
        Value::Null => PropertyValue::Null,
        _ => return Err(ExecutionError::TypeError("Binary op requires property values".to_string())),
    };
    // Cypher's three-valued logic: any comparison with a null operand is *unknown*, not
    // true or false, and a WHERE treats unknown as "exclude". Evaluating `null <> 1` as
    // true kept every row whose property was simply absent — the opposite of what the
    // predicate asks. `IS NULL` / `IS NOT NULL` are postfix operators and unaffected;
    // they remain the way to test for absence.
    if matches!(
        op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
    ) && (matches!(left_prop, PropertyValue::Null) || matches!(right_prop, PropertyValue::Null))
    {
        return Ok(Value::Property(PropertyValue::Null));
    }

    let result = match op {
        // Three-valued, because a null *inside* a list makes the comparison
        // unknown rather than false. The guard above only catches a null
        // operand; `[1] = [null]` has no null operand and was answering
        // `false` (#783).
        BinaryOp::Eq => match cypher_equals(&left_prop, &right_prop) {
            Some(v) => PropertyValue::Boolean(v),
            None => PropertyValue::Null,
        },
        BinaryOp::Ne => match cypher_equals(&left_prop, &right_prop) {
            Some(v) => PropertyValue::Boolean(!v),
            None => PropertyValue::Null,
        },
        BinaryOp::Pow => match (&left_prop, &right_prop) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => {
                // Cypher's `^` is float exponentiation even over integers:
                // `2 ^ 3` is 8.0, and `2 ^ -1` has to be 0.5 rather than 0.
                //
                // `as_float` returns `None` for an `Integer`, so it cannot be
                // used alone here -- doing that made `2 ^ 3` answer
                // "^ requires numeric operands".
                let numeric = |p: &PropertyValue| -> Option<f64> {
                    p.as_float().or_else(|| p.as_integer().map(|i| i as f64))
                };
                match (numeric(&left_prop), numeric(&right_prop)) {
                    (Some(base), Some(exp)) => PropertyValue::Float(base.powf(exp)),
                    _ => return Err(ExecutionError::TypeError("^ requires numeric operands".to_string())),
                }
            }
        },
        BinaryOp::Xor => match (&left_prop, &right_prop) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => PropertyValue::Boolean(l != r),
            _ => return Err(ExecutionError::TypeError("XOR requires boolean operands".to_string())),
        },
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            // **NaN orders as false, not null.** `partial_cmp` returns `None`
            // for it, which this branch maps to null -- the same answer it
            // gives for two values that cannot be compared at all. Cypher
            // separates the two: incomparable is null, NaN is false, and all
            // four operators are false including `>=` against itself (#855).
            //
            // Only against another **number**. `0.0/0.0 < 'a'` is still null,
            // because comparing across types is null before NaN is considered
            // -- returning false there says "I compared them and they did not
            // order", which is a different claim. A blanket NaN rule cost one
            // scenario exactly that way.
            let is_nan = |p: &PropertyValue| matches!(p, PropertyValue::Float(f) if f.is_nan());
            let numeric = |p: &PropertyValue| {
                matches!(p, PropertyValue::Float(_) | PropertyValue::Integer(_))
            };
            if (is_nan(&left_prop) || is_nan(&right_prop))
                && numeric(&left_prop)
                && numeric(&right_prop)
            {
                return Ok(Value::Property(PropertyValue::Boolean(false)));
            }
            let cmp = cypher_ordering(&left_prop, &right_prop);
            match (op, cmp) {
                (BinaryOp::Lt, Some(std::cmp::Ordering::Less)) => PropertyValue::Boolean(true),
                (BinaryOp::Le, Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)) => PropertyValue::Boolean(true),
                (BinaryOp::Gt, Some(std::cmp::Ordering::Greater)) => PropertyValue::Boolean(true),
                (BinaryOp::Ge, Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)) => PropertyValue::Boolean(true),
                (_, None) => PropertyValue::Null,
                _ => PropertyValue::Boolean(false),
            }
        }
        // Cypher three-valued logic: false AND x → false, true AND null → null, etc.
        BinaryOp::And => match (&left_prop, &right_prop) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => PropertyValue::Boolean(*l && *r),
            (PropertyValue::Boolean(false), _) | (_, PropertyValue::Boolean(false)) => PropertyValue::Boolean(false),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("AND requires booleans".to_string())),
        },
        BinaryOp::Or => match (&left_prop, &right_prop) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => PropertyValue::Boolean(*l || *r),
            (PropertyValue::Boolean(true), _) | (_, PropertyValue::Boolean(true)) => PropertyValue::Boolean(true),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("OR requires booleans".to_string())),
        },
        BinaryOp::Add => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l + r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l + r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => PropertyValue::Float(*l as f64 + r),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => PropertyValue::Float(l + *r as f64),
            (PropertyValue::String(l), PropertyValue::String(r)) => PropertyValue::String(format!("{}{}", l, r)),
            // List concatenation, and appending or prepending a scalar. Cypher
            // defines all three for `+`; none of them worked (#578).
            (PropertyValue::Array(l), PropertyValue::Array(r)) => {
                let mut out = l.clone();
                out.extend(r.iter().cloned());
                PropertyValue::Array(out)
            }
            // Null is excluded here so the propagation arm below still governs
            // it. `[1,2] + null` appending a null element is arguably Cypher's
            // answer, but it is a judgement call and this is not the change to
            // make it in -- the existing rule is that any null operand makes the
            // result null (#457), and it stays.
            (PropertyValue::Array(l), scalar) if !matches!(scalar, PropertyValue::Null) => {
                let mut out = l.clone();
                out.push(scalar.clone());
                PropertyValue::Array(out)
            }
            (scalar, PropertyValue::Array(r)) if !matches!(scalar, PropertyValue::Null) => {
                let mut out = vec![scalar.clone()];
                out.extend(r.iter().cloned());
                PropertyValue::Array(out)
            }
            // DateTime + Duration
            (PropertyValue::DateTime(dt), PropertyValue::Duration { months, days, seconds, .. }) |
            (PropertyValue::Duration { months, days, seconds, .. }, PropertyValue::DateTime(dt)) => {
                add_duration_to_datetime(*dt, *months, *days, *seconds)
            }
            // Any of the five temporal types + Duration (#689). Without these
            // arms, teaching the constructors to produce real types would have
            // silently removed `datetime(...) + duration(...)`, which Cypher
            // requires and which the suite already covered.
            (t @ (PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. }),
             PropertyValue::Duration { months, days, seconds, nanos })
            | (PropertyValue::Duration { months, days, seconds, nanos },
               t @ (PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. })) => {
                shift_temporal(t, *months, *days, *seconds, *nanos as i64)?
            }
            // Duration + Duration
            (PropertyValue::Duration { months: m1, days: d1, seconds: s1, nanos: n1 },
             PropertyValue::Duration { months: m2, days: d2, seconds: s2, nanos: n2 }) => {
                normalized_duration(m1 + m2, d1 + d2, *s1 as i128 + *s2 as i128,
                                    *n1 as i128 + *n2 as i128)
            }
            // Cypher null propagation: any arithmetic with a null operand is null,
            // not an error. Without this, `p.a + p.missing` aborts the whole query --
            // and a property absent on some nodes is the ordinary state of a property
            // graph, not an exceptional one (#457). The logical and comparison operators
            // already propagate null this way; arithmetic was the outlier.
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Add requires numeric or string operands".to_string())),
        },
        BinaryOp::Sub => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l - r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l - r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => PropertyValue::Float(*l as f64 - r),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => PropertyValue::Float(l - *r as f64),
            // DateTime - Duration
            (PropertyValue::DateTime(dt), PropertyValue::Duration { months, days, seconds, .. }) => {
                add_duration_to_datetime(*dt, -*months, -*days, -*seconds)
            }
            (t @ (PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. }),
             PropertyValue::Duration { months, days, seconds, nanos }) => {
                shift_temporal(t, -*months, -*days, -*seconds, -(*nanos as i64))?
            }
            // Two temporals of the same kind subtract to a Duration.
            (a @ (PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. }),
             b @ (PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. })) => temporal_difference(a, b)?,
            // DateTime - DateTime = Duration
            (PropertyValue::DateTime(a), PropertyValue::DateTime(b)) => {
                let diff_ms = a - b;
                let total_seconds = diff_ms / 1000;
                PropertyValue::Duration { months: 0, days: total_seconds / 86400, seconds: total_seconds % 86400, nanos: ((diff_ms % 1000) * 1_000_000) as i32 }
            }
            // Duration - Duration
            (PropertyValue::Duration { months: m1, days: d1, seconds: s1, nanos: n1 },
             PropertyValue::Duration { months: m2, days: d2, seconds: s2, nanos: n2 }) => {
                normalized_duration(m1 - m2, d1 - d2, *s1 as i128 - *s2 as i128,
                                    *n1 as i128 - *n2 as i128)
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Sub requires numeric operands".to_string())),
        },
        BinaryOp::Mul => match (&left_prop, &right_prop) {
            // duration * number, either way round (#787).
            (PropertyValue::Duration { months, days, seconds, nanos }, n)
            | (n, PropertyValue::Duration { months, days, seconds, nanos })
                if matches!(n, PropertyValue::Integer(_) | PropertyValue::Float(_)) =>
            {
                let f = match n {
                    PropertyValue::Integer(i) => *i as f64,
                    PropertyValue::Float(f) => *f,
                    _ => unreachable!("guarded above"),
                };
                scale_duration(*months, *days, *seconds, *nanos, f)?
            }
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l * r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l * r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => PropertyValue::Float(*l as f64 * r),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => PropertyValue::Float(l * *r as f64),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Mul requires numeric operands".to_string())),
        },
        BinaryOp::Div => match (&left_prop, &right_prop) {
            // duration / number. Not commutative, so only this order.
            (PropertyValue::Duration { months, days, seconds, nanos }, n)
                if matches!(n, PropertyValue::Integer(_) | PropertyValue::Float(_)) =>
            {
                let f = match n {
                    PropertyValue::Integer(i) => *i as f64,
                    PropertyValue::Float(f) => *f,
                    _ => unreachable!("guarded above"),
                };
                if f == 0.0 {
                    return Err(ExecutionError::RuntimeError(
                        "cannot divide a duration by zero".to_string(),
                    ));
                }
                scale_duration(*months, *days, *seconds, *nanos, 1.0 / f)?
            }
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => return Err(ExecutionError::RuntimeError("Division by zero".to_string())),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l / r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l / r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => PropertyValue::Float(*l as f64 / r),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => PropertyValue::Float(l / *r as f64),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Div requires numeric operands".to_string())),
        },
        BinaryOp::Mod => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => return Err(ExecutionError::RuntimeError("Modulo by zero".to_string())),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l % r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l % r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => PropertyValue::Float(*l as f64 % r),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => PropertyValue::Float(l % *r as f64),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Mod requires numeric operands".to_string())),
        },
        BinaryOp::StartsWith => string_position_op(StringPositionOp::StartsWith, &left_prop, &right_prop),
        BinaryOp::EndsWith => string_position_op(StringPositionOp::EndsWith, &left_prop, &right_prop),
        BinaryOp::Contains => string_position_op(StringPositionOp::Contains, &left_prop, &right_prop),
        BinaryOp::In => match eval_in_list(&left_prop, &right_prop) {
            Some(v) => v,
            None => return Err(ExecutionError::TypeError("IN requires a list on the right".to_string())),
        },
        BinaryOp::RegexMatch => match (&left_prop, &right_prop) {
            (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                let re = regex::Regex::new(pattern).map_err(|e| ExecutionError::RuntimeError(format!("Invalid regex: {}", e)))?;
                PropertyValue::Boolean(re.is_match(text))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("=~ requires string operands".to_string())),
        },
    };
    Ok(Value::Property(result))
}

/// Shared unary operator evaluation
fn eval_unary_op(op: &UnaryOp, val: Value) -> ExecutionResult<Value> {
    match op {
        UnaryOp::IsNull => {
            let is_null = matches!(val, Value::Null | Value::Property(PropertyValue::Null));
            Ok(Value::Property(PropertyValue::Boolean(is_null)))
        }
        UnaryOp::IsNotNull => {
            let is_null = matches!(val, Value::Null | Value::Property(PropertyValue::Null));
            Ok(Value::Property(PropertyValue::Boolean(!is_null)))
        }
        UnaryOp::Not => match val {
            Value::Property(PropertyValue::Boolean(b)) => Ok(Value::Property(PropertyValue::Boolean(!b))),
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
            _ => Err(ExecutionError::TypeError("NOT requires boolean".to_string())),
        },
        UnaryOp::Minus => match val {
            Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(-i))),
            Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(-f))),
            // -null is null, matching NOT above and the binary arithmetic ops (#457).
            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
            _ => Err(ExecutionError::TypeError("Negation requires numeric type".to_string())),
        },
    }
}

/// Shared list/map indexing evaluation.
///
/// Takes the store because indexing a node or relationship by name reads a
/// property, which may live in the column store rather than inline (#673).
fn eval_index(collection: Value, index: Value, store: &GraphStore) -> ExecutionResult<Value> {
    match (&collection, &index) {
        // Any value that reads as a list, so an all-float literal -- which
        // parses as a `Vector` -- indexes rather than returning null (#605).
        (Value::Property(p), Value::Property(PropertyValue::Integer(i)))
            if p.as_list_items().is_some() =>
        {
            let arr = p.as_list_items().unwrap();
            let idx = if *i < 0 { (arr.len() as i64 + *i) as usize } else { *i as usize };
            Ok(arr.get(idx).map(|v| Value::Property(v.clone())).unwrap_or(Value::Null))
        }
        (Value::Property(PropertyValue::Map(map)), Value::Property(PropertyValue::String(key))) => {
            Ok(map.get(key).map(|v| Value::Property(v.clone())).unwrap_or(Value::Null))
        }
        // A map holding entities — `{k: collect(a)}` (#670).
        (Value::Map(map), Value::Property(PropertyValue::String(key))) => {
            Ok(map.get(key).cloned().unwrap_or(Value::Null))
        }
        // A *list* holding entities — `[a, 1]` where `a` is a node. The
        // PropertyValue arm above cannot serve this: a PropertyValue list
        // cannot hold an entity, so such a list is a `Value::List` and
        // indexing it fell through to the catch-all and answered null.
        (Value::List(items), Value::Property(PropertyValue::Integer(i))) => {
            let idx = if *i < 0 { items.len() as i64 + *i } else { *i };
            if idx < 0 {
                return Ok(Value::Null);
            }
            Ok(items.get(idx as usize).cloned().unwrap_or(Value::Null))
        }
        // Indexing a *node or relationship* by name reads its property.
        // `startNode(r).id` desugars to `startNode(r)["id"]`, and without this
        // it answered null — parsing was only half the work, and the half that
        // fails silently (#673).
        (Value::Node(..) | Value::NodeRef(_) | Value::Edge(..) | Value::EdgeRef(..),
         Value::Property(PropertyValue::String(key))) => {
            Ok(Value::Property(collection.resolve_property(key, store)))
        }

        // Null in, null out — an unknown collection or index has an unknown
        // element, which is Cypher's answer and not an error.
        (Value::Null | Value::Property(PropertyValue::Null), _)
        | (_, Value::Null | Value::Property(PropertyValue::Null)) => Ok(Value::Null),

        // Everything else is a **type error**, not null (#789).
        //
        // The catch-all here used to answer null for every unhandled pair, so
        // `true[0]` and `[1,2]['x']` returned a value where Cypher raises. That
        // is the failure mode this codebase keeps producing: a wrong answer
        // that looks like a legitimate "no such element".
        //
        // The two cases are distinguished because the TCK does: indexing a
        // *non-list* is one error, indexing a list with a *non-integer* is
        // another, and reporting one for the other sends the reader to the
        // wrong operand.
        (Value::Property(p), _) if p.as_list_items().is_some() => {
            Err(ExecutionError::TypeError(format!(
                "a list index must be an integer, not {}",
                type_name_of(&index)
            )))
        }
        (Value::List(_), _) => Err(ExecutionError::TypeError(format!(
            "a list index must be an integer, not {}",
            type_name_of(&index)
        ))),
        (Value::Property(PropertyValue::Map(_)) | Value::Map(_), _) => {
            Err(ExecutionError::TypeError(format!(
                "a map key must be a string, not {}",
                type_name_of(&index)
            )))
        }
        _ => Err(ExecutionError::TypeError(format!(
            "cannot index {}: it is not a list or a map",
            type_name_of(&collection)
        ))),
    }
}

/// A value's type, for an error message that names the operand rather than
/// saying something went wrong somewhere.
fn type_name_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Property(p) => p.type_name(),
        Value::Node(..) | Value::NodeRef(_) => "Node",
        Value::Edge(..) | Value::EdgeRef(..) => "Relationship",
        Value::Path { .. } => "Path",
        Value::List(_) => "List",
        Value::Map(_) => "Map",
    }
}

fn eval_list_slice(collection: Value, start: Option<Value>, end: Option<Value>) -> ExecutionResult<Value> {
    // A bound that is present and **null** makes the whole slice null:
    // `[1,2,3][1..null]` is null, not `[2,3]` (#845).
    //
    // An *absent* bound is a different thing and still means "to the end":
    // `[1,2,3][1..]` is `[2,3]`. The two were indistinguishable here because
    // both arms fell through to the same `_` default, so a null bound was
    // silently read as an omitted one -- and the result is a perfectly good
    // list, which is why nothing downstream noticed.
    let is_null = |b: &Option<Value>| {
        matches!(b, Some(Value::Property(PropertyValue::Null)) | Some(Value::Null))
    };
    if is_null(&start) || is_null(&end) {
        return Ok(Value::Property(PropertyValue::Null));
    }
    match &collection {
        Value::Property(PropertyValue::Array(arr)) => {
            let len = arr.len() as i64;
            let resolve_idx = |idx: i64| -> usize {
                let resolved = if idx < 0 { (len + idx).max(0) } else { idx.min(len) };
                resolved as usize
            };
            let s = match start {
                Some(Value::Property(PropertyValue::Integer(i))) => resolve_idx(i),
                _ => 0,
            };
            let e = match end {
                Some(Value::Property(PropertyValue::Integer(i))) => resolve_idx(i),
                _ => len as usize,
            };
            if s >= e || s >= arr.len() {
                Ok(Value::Property(PropertyValue::Array(vec![])))
            } else {
                let sliced: Vec<PropertyValue> = arr[s..e.min(arr.len())].to_vec();
                Ok(Value::Property(PropertyValue::Array(sliced)))
            }
        }
        _ => Ok(Value::Null),
    }
}


/// Read `<variable>.<property>` from a record.
///
/// One implementation, because there were **eight** and they had already
/// drifted: some error on an unbound variable, some read it as null. Adding
/// the `Value::Map` case to one of them fixed nothing, because the projection
/// path uses a different copy — which is how `WITH {k: collect(a)} AS m RETURN
/// m.k` still answered null after the "fix" (#670).
fn read_property(
    record: &Record,
    variable: &str,
    property: &str,
    store: &GraphStore,
    missing_is_null: bool,
) -> ExecutionResult<Value> {
    let val = match record.get(variable) {
        Some(v) => v,
        None if missing_is_null => &Value::Null,
        None => return Err(ExecutionError::VariableNotFound(variable.to_string())),
    };
    // A map holding entities cannot answer through `resolve_property`, which
    // returns a `PropertyValue` and so would degrade a node to null.
    if let Value::Map(entries) = val {
        return Ok(entries.get(property).cloned().unwrap_or(Value::Null));
    }
    // A property read of something the query has already deleted is an error,
    // not a null. `resolve_property` answers `null` for a missing entity, which
    // is also the honest answer for a property nobody set -- so `MATCH (n)
    // DELETE n RETURN n.num` was indistinguishable from reading an unset
    // property, and reported success (#905).
    match val {
        Value::NodeRef(id) | Value::Node(id, _) if store.get_node(*id).is_none() => {
            return Err(ExecutionError::EntityNotFound(format!("node {}", id.as_u64())));
        }
        Value::EdgeRef(id, ..) | Value::Edge(id, _) if store.get_edge(*id).is_none() => {
            return Err(ExecutionError::EntityNotFound(format!("relationship {}", id.as_u64())));
        }
        _ => {}
    }
    Ok(Value::Property(val.resolve_property(property, store)))
}

/// Standalone expression evaluator usable from any operator
pub(crate) fn eval_expression(expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
    match expr {
        Expression::Variable(var) => {
            record.get(var).cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
        }
        Expression::Property { variable, property } => {
            read_property(record, variable, property, store, false)
        }
        // A collection literal whose elements are expressions. The
        // all-literal form never reaches here -- the grammar matches it as a
        // `PropertyValue` first -- so this is only the case that could not be
        // expressed before (#654).
        Expression::ListExpr(items) => Ok(Value::List(
            items.iter().map(|e| eval_expression(e, record, store)).collect::<ExecutionResult<Vec<_>>>()?,
        )),
        Expression::MapExpr(entries) => {
            let mut out = std::collections::BTreeMap::new();
            for (k, e) in entries {
                out.insert(k.clone(), eval_expression(e, record, store)?);
            }
            Ok(Value::Map(out))
        }
        Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
        Expression::Binary { left, op, right } => {
            let l = eval_expression(left, record, store)?;
            let r = eval_expression(right, record, store)?;
            eval_binary_op(op, l, r)
        }
        Expression::Unary { op, expr: e } => {
            let val = eval_expression(e, record, store)?;
            eval_unary_op(op, val)
        }
        Expression::Function { name, args, .. } => {
            let arg_vals: Vec<Value> = args.iter()
                .map(|a| eval_expression(a, record, store))
                .collect::<Result<_, _>>()?;
            eval_function(name, &arg_vals, Some(store))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), |e| eval_expression(e, record, store))
        }
        Expression::Index { expr: e, index } => {
            let collection = eval_expression(e, record, store)?;
            let idx = eval_expression(index, record, store)?;
            eval_index(collection, idx, store)
        }
        Expression::ListSlice { expr: e, start, end } => {
            let collection = eval_expression(e, record, store)?;
            let s = match start { Some(s) => Some(eval_expression(s, record, store)?), None => None };
            let en = match end { Some(e) => Some(eval_expression(e, record, store)?), None => None };
            eval_list_slice(collection, s, en)
        }
        Expression::ExistsSubquery { pattern, where_clause, .. } => {
            eval_exists_subquery(pattern, where_clause.as_deref(), record, store)
        }
        Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
            eval_list_comprehension(variable, list_expr, filter.as_deref(), map_expr, record, store)
        }
        Expression::PredicateFunction { name, variable, list_expr, predicate } => {
            eval_predicate_function(name, variable, list_expr, predicate, record, store)
        }
        Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
            eval_reduce(accumulator, init, variable, list_expr, expression, record, store)
        }
        Expression::PatternComprehension { pattern, filter, projection } => {
            eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store)
        }
        Expression::PathVariable(var) => {
            record.get(var).cloned()
                .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
        }
        Expression::Parameter(name) => {
            // Parameters are resolved by substituting them with bound variables prefixed with `$`
            // The executor is responsible for binding params to `$name` before execution
            record.get(&format!("${}", name)).cloned()
                .ok_or_else(|| ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name)))
        }
    }
}

/// Hop ceiling for an unbounded variable-length pattern (`*`) inside an EXISTS
/// subquery. Bounded patterns use their own maximum. Traversal already
/// terminates through relationship isomorphism (no edge is reused within a
/// single path), so this is a guard against pathological fan-out rather than a
/// correctness requirement.
const EXISTS_UNBOUNDED_MAX_HOPS: usize = 15;

/// Evaluate EXISTS { MATCH pattern WHERE cond }
///
/// The subquery is matched with the outer record's bindings held fixed: a
/// variable already bound by the outer query pins that position to exactly the
/// node it is bound to, instead of being matched freely. That is what makes
/// `NOT EXISTS { MATCH (a)-[:R]-(b) }` mean "these two specific nodes are not
/// connected" rather than "a has no R edge at all" — the latter is true for
/// almost every row in a dense graph and silently empties the result set.
///
/// Returns true as soon as one complete path matches and the inner WHERE holds
/// with every subquery variable bound.
/// Refuse a hierarchy function when there is no hierarchy to consult.
///
/// `subsumes()` used to answer **false** whenever no index covered its
/// arguments, on the stated reasoning that two nodes in no declared hierarchy
/// are not in a subsumption relation. That is right when hierarchies exist and
/// these nodes are outside them. It is a guess when *nothing* is declared, or
/// when every index is stale — and it is a guess shaped exactly like an answer:
/// on `c-[:BROADER]->b-[:BROADER]->a` the index and a plain traversal both say
/// three nodes are subsumed by `a`, and with no index `subsumes` said zero,
/// silently (#721).
///
/// There is no traversal fallback to offer, because without a declaration
/// there is no relationship type to walk — the hierarchy *is* the declaration.
/// So the honest answer is an error naming what is missing.
fn require_a_hierarchy(store: &GraphStore, func: &str) -> ExecutionResult<()> {
    if store.hierarchy_index.any_usable() {
        return Ok(());
    }
    let detail = if store.hierarchy_index.is_empty() {
        "no hierarchy index is declared"
    } else {
        "every declared hierarchy index is stale or was declined"
    };
    Err(ExecutionError::RuntimeError(format!(
        "{func}(): {detail}. Declare one with `CREATE HIERARCHY INDEX <name> ON \
         ()-[:TYPE]->() ...`, REBUILD a stale one, or write the test as a \
         variable-length traversal."
    )))
}

fn eval_exists_subquery(
    pattern: &crate::query::ast::Pattern,
    where_clause: Option<&crate::query::ast::WhereClause>,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    for path in &pattern.paths {
        // Candidate start nodes: pinned when the start variable is already bound
        // by the outer query, otherwise every node carrying the required label.
        let start_candidates: Vec<NodeId> =
            match path.start.variable.as_deref().and_then(|v| record.get(v)) {
                Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => vec![*id],
                // Bound to something that is not a node — cannot match.
                Some(_) => continue,
                None => match path.start.labels.first() {
                    Some(label) => store.get_nodes_by_label(label).iter().map(|n| n.id).collect(),
                    None => store.all_nodes().iter().map(|n| n.id).collect(),
                },
            };

        for start_id in start_candidates {
            if !exists_node_matches(store, start_id, &path.start) {
                continue;
            }

            let mut bindings = record.clone();
            if let Some(var) = path.start.variable.as_deref() {
                bindings.bind(var.to_string(), Value::NodeRef(start_id));
            }

            if exists_match_segment(path, 0, start_id, &bindings, &[], where_clause, store)? {
                return Ok(Value::Property(PropertyValue::Boolean(true)));
            }
        }
    }
    Ok(Value::Property(PropertyValue::Boolean(false)))
}

/// Check a node against a pattern's labels and inline property constraints.
fn exists_node_matches(
    store: &GraphStore,
    id: NodeId,
    pat: &crate::query::ast::NodePattern,
) -> bool {
    let node = match store.get_node(id) {
        Some(n) => n,
        None => return false,
    };
    if !pat.labels.iter().all(|l| node.labels.contains(l)) {
        return false;
    }
    if let Some(props) = &pat.properties {
        // Columnar store first, sparse map as fallback. Reading only `node.properties`
        // made every inline constraint inside EXISTS { } fail on a columnar graph — and
        // after a snapshot import that map is *always* empty (ADR-021), so EXISTS matched
        // nothing at all on imported data while the equivalent WHERE inside the subquery
        // worked (#346).
        let idx = id.as_u64() as usize;
        let matches_all = props.iter().all(|(k, v)| {
            match store.node_columns.get_property(idx, k) {
                PropertyValue::Null => node.properties.get(k).is_some_and(|pv| pv == v),
                col => &col == v,
            }
        });
        if !matches_all {
            return false;
        }
    }
    true
}

/// Visit the neighbours of `node` reachable by an edge matching `edge_pat`.
///
/// This used to build a `Vec<(Edge, NodeId)>`: it fetched **every** edge
/// incident to the node in the pattern's direction — all types, both
/// directions for `-[:R]-` — cloned each one whole (an owned `Edge` carries its
/// type string and its whole property map), and only then filtered by type and
/// applied the pinned-target check.
///
/// For LDBC IS7 that is `EXISTS { MATCH (op)-[:KNOWS]-(author) }` evaluated per
/// output row, where `op` is a Person with a few hundred incident edges of
/// which ~20 are `:KNOWS`, and exactly one can satisfy the pinned `author`. It
/// cost 95% of IS7 — 0.56 ms of 0.59 at SF1, and IS7 is 26.8 ms at SF10 against
/// FalkorDB's 0.66 on the same host (#618).
///
/// `ExpandOperator` has walked adjacency this way since #520; the `EXISTS`
/// evaluator simply never inherited it. Type ids are resolved once, the pin and
/// the isomorphism check run inside the walk, and the `Edge` is materialised
/// only for a survivor whose pattern actually needs it — an edge variable to
/// bind or an edge property to test.
///
/// `visit` returns `Ok(true)` to stop the walk; the return value says whether
/// it did.
fn exists_for_each_neighbor(
    store: &GraphStore,
    node: NodeId,
    edge_pat: &crate::query::ast::EdgePattern,
    pinned_target: Option<NodeId>,
    visited_edges: &[crate::graph::EdgeId],
    mut visit: impl FnMut(crate::graph::EdgeId, NodeId) -> ExecutionResult<bool>,
) -> ExecutionResult<bool> {
    // `None` means "no type filter". A named type the graph has never seen
    // contributes no id, so the resolved set is empty and matches nothing --
    // which is correct, and is why the two cases are kept apart (#520).
    let type_ids: Option<Vec<u16>> = if edge_pat.types.is_empty() {
        None
    } else {
        Some(
            edge_pat
                .types
                .iter()
                .filter_map(|t| store.edge_type_id(&EdgeType::new(t.as_str())))
                .collect(),
        )
    };
    let type_filter = type_ids.as_deref();

    // Only a pattern that names edge properties needs the edge itself here.
    let edge_props = edge_pat.properties.as_ref();

    let mut stop = false;
    let mut err: Option<ExecutionError> = None;
    let mut keep = |eid: crate::graph::EdgeId, other: NodeId, stop: &mut bool, err: &mut Option<ExecutionError>| {
        if *stop || err.is_some() {
            return;
        }
        if let Some(target) = pinned_target {
            if other != target {
                return;
            }
        }
        if visited_edges.contains(&eid) {
            return;
        }
        if let Some(props) = edge_props {
            match store.get_edge(eid) {
                Some(e) => {
                    if !props.iter().all(|(k, v)| e.properties.get(k).is_some_and(|pv| pv == v)) {
                        return;
                    }
                }
                None => return,
            }
        }
        match visit(eid, other) {
            Ok(true) => *stop = true,
            Ok(false) => {}
            Err(e) => *err = Some(e),
        }
    };

    if matches!(edge_pat.direction, Direction::Outgoing | Direction::Both) {
        store.for_each_outgoing_neighbor(node, type_filter, |target, eid| {
            keep(eid, target, &mut stop, &mut err);
        });
    }
    if matches!(edge_pat.direction, Direction::Incoming | Direction::Both) {
        store.for_each_incoming_neighbor(node, type_filter, |source, eid| {
            // A self-relationship is incident to its node twice. Undirected
            // matching traverses each relationship once, so the outgoing walk
            // above has already taken it (#640).
            if matches!(edge_pat.direction, Direction::Both) && source == node {
                return;
            }
            keep(eid, source, &mut stop, &mut err);
        });
    }
    match err {
        Some(e) => Err(e),
        None => Ok(stop),
    }
}

/// Match `path.segments[seg_idx..]` starting from `current`. Once every segment
/// is consumed, the inner WHERE is evaluated with all subquery variables bound.
fn exists_match_segment(
    path: &crate::query::ast::PathPattern,
    seg_idx: usize,
    current: NodeId,
    bindings: &Record,
    visited_edges: &[crate::graph::EdgeId],
    where_clause: Option<&crate::query::ast::WhereClause>,
    store: &GraphStore,
) -> ExecutionResult<bool> {
    if seg_idx == path.segments.len() {
        return Ok(match where_clause {
            Some(wc) => matches!(
                eval_expression(&wc.predicate, bindings, store)?,
                Value::Property(PropertyValue::Boolean(true))
            ),
            None => true,
        });
    }

    let segment = &path.segments[seg_idx];
    let (min_hops, max_hops) = match &segment.edge.length {
        Some(len) => (
            len.min.unwrap_or(1),
            len.max.unwrap_or(EXISTS_UNBOUNDED_MAX_HOPS),
        ),
        None => (1, 1),
    };

    exists_expand_hops(
        path, seg_idx, current, 0, min_hops, max_hops, bindings, visited_edges, where_clause, store,
    )
}

/// Expand a single segment, which spans several hops for a variable-length
/// pattern. Backtracks across every legal hop count in `min_hops..=max_hops`.
#[allow(clippy::too_many_arguments)]
fn exists_expand_hops(
    path: &crate::query::ast::PathPattern,
    seg_idx: usize,
    current: NodeId,
    depth: usize,
    min_hops: usize,
    max_hops: usize,
    bindings: &Record,
    visited_edges: &[crate::graph::EdgeId],
    where_clause: Option<&crate::query::ast::WhereClause>,
    store: &GraphStore,
) -> ExecutionResult<bool> {
    let segment = &path.segments[seg_idx];

    // This hop count is a legal length for the segment — try to close it here.
    if depth >= min_hops && exists_node_matches(store, current, &segment.node) {
        // If this segment's variable is already bound — by the outer query or by
        // an earlier segment — the position must be that exact node.
        let pinned_ok = match segment.node.variable.as_deref().and_then(|v| bindings.get(v)) {
            Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => *id == current,
            Some(_) => false,
            None => true,
        };
        if pinned_ok {
            let mut next = bindings.clone();
            if let Some(var) = segment.node.variable.as_deref() {
                next.bind(var.to_string(), Value::NodeRef(current));
            }
            if exists_match_segment(
                path, seg_idx + 1, current, &next, visited_edges, where_clause, store,
            )? {
                return Ok(true);
            }
        }
    }

    if depth >= max_hops {
        return Ok(false);
    }

    // If the node this segment lands on is *already bound* and the segment is a
    // single hop, only that node can close it. Recursing into every neighbour
    // and rejecting them one level down is the same answer at O(degree) cost —
    // and it clones the binding record per neighbour to get there.
    //
    // LDBC BI-11 is the case that makes this matter: `(t)<-[:HAS_TAG]-(post)`
    // with `post` bound walks every node carrying that tag, ~250 of them on
    // SF1, for each of ~1.19M outer rows (#681).
    //
    // Restricted to single-hop segments deliberately: in a variable-length
    // segment the pin applies to the far end, not to the intermediate
    // positions this loop is walking through, so filtering here would cut off
    // legitimate paths.
    let pinned_target: Option<NodeId> = if max_hops == 1 {
        match segment.node.variable.as_deref().and_then(|v| bindings.get(v)) {
            Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => Some(*id),
            _ => None,
        }
    } else {
        None
    };

    // The pin and the isomorphism check happen inside the walk, before any
    // allocation: a neighbour that cannot close the segment costs a comparison
    // rather than an `Edge` clone and a record clone.
    exists_for_each_neighbor(
        store,
        current,
        &segment.edge,
        pinned_target,
        visited_edges,
        |eid, neighbor| {
            let mut next_visited = visited_edges.to_vec();
            next_visited.push(eid);

            let mut next = bindings.clone();
            if let Some(var) = segment.edge.variable.as_deref() {
                // Only a pattern that binds the edge needs it materialised.
                if let Some(edge) = store.get_edge(eid) {
                    next.bind(
                        var.to_string(),
                        Value::EdgeRef(edge.id, edge.source, edge.target, edge.edge_type.clone()),
                    );
                }
            }

            exists_expand_hops(
                path,
                seg_idx,
                neighbor,
                depth + 1,
                min_hops,
                max_hops,
                &next,
                &next_visited,
                where_clause,
                store,
            )
        },
    )
}

/// Evaluate list comprehension: [x IN list WHERE cond | expr]
fn eval_list_comprehension(
    variable: &str,
    list_expr: &Expression,
    filter: Option<&Expression>,
    map_expr: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let list_val = eval_expression(list_expr, record, store)?;

    // `as_list_items` rather than an `Array` match: an all-float list literal
    // parses as a `Vector`, and returning the empty list for it made a
    // comprehension over one silently produce nothing (#605).
    // A list holding **entities** is a `Value::List`, not a `PropertyValue`
    // list — a PropertyValue list cannot hold a node or relationship. So
    // `[x IN [r, 1] | type(x)]` fell to the catch-all and returned `[]`:
    // an empty list where a TypeError belongs, and indistinguishable from a
    // comprehension that legitimately filtered everything out (#799).
    let items: Vec<Value> = match list_val {
        Value::Property(ref p) if p.as_list_items().is_some() => p
            .as_list_items()
            .unwrap()
            .into_iter()
            .map(Value::Property)
            .collect(),
        Value::List(items) => items,
        // Null in, null out. Anything else is not a list at all, and saying so
        // beats returning an empty one.
        Value::Null | Value::Property(PropertyValue::Null) => {
            return Ok(Value::Property(PropertyValue::Null))
        }
        other => {
            return Err(ExecutionError::TypeError(format!(
                "a list comprehension needs a list, not {}",
                type_name_of(&other)
            )))
        }
    };

    let mut result = Vec::new();
    for item in items {
        let mut inner_record = record.clone();
        inner_record.bind(variable.to_string(), item);

        // Apply filter
        if let Some(f) = filter {
            let cond = eval_expression(f, &inner_record, store)?;
            if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) {
                continue;
            }
        }

        // Apply map expression
        result.push(eval_expression(map_expr, &inner_record, store)?);
    }

    // A projection that yields **entities** stays a `Value::List`.
    //
    // Every mapped value used to be forced into a `PropertyValue`, and anything
    // that is not one -- a node, a relationship, a path, a nested entity list --
    // became `Null`. So `[x IN collect(p) | head(nodes(x))]` answered
    // `[null, null]` where two nodes belong: a list of the right length, full of
    // nothing, which no caller can distinguish from a projection that
    // legitimately produced nulls (#863).
    //
    // #800 fixed the *input* side of this same distinction -- a list holding
    // entities is a `Value::List`, not a `PropertyValue` -- and left the output
    // side converting them away.
    //
    // A list of plain property values still comes back as `PropertyValue::Array`,
    // because that is what every existing caller of a comprehension expects and
    // what the storage layer can hold.
    if result.iter().all(|v| matches!(v, Value::Property(_))) {
        let props = result
            .into_iter()
            .map(|v| match v {
                Value::Property(p) => p,
                _ => unreachable!("checked above"),
            })
            .collect();
        return Ok(Value::Property(PropertyValue::Array(props)));
    }
    Ok(Value::List(result))
}

/// Evaluate predicate functions: all(x IN list WHERE pred), any(...), none(...), single(...)
fn eval_predicate_function(
    name: &str,
    variable: &str,
    list_expr: &Expression,
    predicate: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let list_val = eval_expression(list_expr, record, store)?;
    // A list that holds entities is a `Value::List`; only a list of plain
    // property values is a `PropertyValue`. Matching on the latter alone meant
    // `any(x IN nodes WHERE ...)` fell through to `false` for every quantifier,
    // silently — the caller gets a boolean it will branch on rather than an
    // error it would notice (Quantifier1-4, scenarios 8 and 9).
    let items: Vec<Value> = match list_val {
        Value::Property(ref p) if p.as_list_items().is_some() => p
            .as_list_items()
            .unwrap()
            .into_iter()
            .map(Value::Property)
            .collect(),
        Value::List(items) => items,
        _ => return Ok(Value::Property(PropertyValue::Boolean(false))),
    };

    // Quantifiers are **three-valued**: a predicate that evaluates to null on
    // some element makes the answer null unless the elements that *did* decide
    // already settle it (#826).
    //
    // Counting nulls as "not true" -- which is what tracking only `true_count`
    // does -- collapses the third value into `false`, and `false` is a
    // perfectly usable answer that the caller will branch on:
    //
    //     any(x IN [0, null] WHERE x = 2)     null, was false
    //     all(x IN [2, null] WHERE x = 2)     null, was false
    //     single(x IN [2, null] WHERE x = 2)  null, was true
    //
    // The last one is the worst of the three: it flips to the *opposite*
    // certainty rather than to a weaker one.
    let mut true_count = 0usize;
    let mut false_count = 0usize;
    let mut unknown = false;
    for item in &items {
        let mut inner_record = record.clone_with_capacity(1);
        inner_record.bind(variable.to_string(), item.clone());
        match eval_expression(predicate, &inner_record, store)? {
            Value::Property(PropertyValue::Boolean(true)) => true_count += 1,
            Value::Property(PropertyValue::Boolean(false)) => false_count += 1,
            Value::Property(PropertyValue::Null) | Value::Null => unknown = true,
            // A predicate that is neither boolean nor null keeps its existing
            // treatment. Cypher would raise here; making that change without a
            // scenario to check it against would be guessing.
            _ => false_count += 1,
        }
    }

    // Each quantifier has one outcome it can be *certain* of from a single
    // element. Reaching it beats an unknown; failing to reach it does not,
    // because the unknown elements could have supplied it.
    let decided = match name {
        "all" => (false_count > 0).then_some(false),
        "any" => (true_count > 0).then_some(true),
        "none" => (true_count > 0).then_some(false),
        // `single` is the one that can be settled by *either* certainty:
        // two trues rule it out no matter what the unknowns hold.
        "single" => (true_count > 1).then_some(false),
        _ => Some(false),
    };
    let result = match decided {
        Some(v) => PropertyValue::Boolean(v),
        None if unknown => PropertyValue::Null,
        None => PropertyValue::Boolean(match name {
            "all" => true,
            "any" => false,
            "none" => true,
            "single" => true_count == 1,
            _ => false,
        }),
    };
    Ok(Value::Property(result))
}

/// Evaluate reduce(acc = init, x IN list | expr)
fn eval_reduce(
    accumulator: &str,
    init: &Expression,
    variable: &str,
    list_expr: &Expression,
    expression: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    let init_val = eval_expression(init, record, store)?;
    let list_val = eval_expression(list_expr, record, store)?;
    // See the note in `eval_list_comprehension`: an all-float list literal is a
    // `Vector`, and giving up here returned the seed unchanged (#605).
    //
    // A list holding **entities** is a `Value::List`, and it fell into the same
    // give-up arm: `reduce(acc = 0, x IN nodes(p) | acc + 1)` returned **0** for
    // a two-node path. The seed is a legitimate answer for an empty list, so
    // nothing distinguishes "nothing to fold" from "I did not recognise your
    // list" (#863).
    let items: Vec<Value> = match list_val {
        Value::Property(ref p) if p.as_list_items().is_some() => p
            .as_list_items()
            .unwrap()
            .into_iter()
            .map(Value::Property)
            .collect(),
        Value::List(items) => items,
        _ => return Ok(init_val),
    };

    let mut acc = init_val;
    for item in items {
        let mut inner_record = record.clone();
        inner_record.bind(accumulator.to_string(), acc);
        inner_record.bind(variable.to_string(), item);
        acc = eval_expression(expression, &inner_record, store)?;
    }
    Ok(acc)
}

/// Evaluate pattern comprehension: `[(a)-[:REL]->(b) | expr]`
fn eval_pattern_comprehension(
    pattern: &Pattern,
    filter: Option<&Expression>,
    projection: &Expression,
    record: &Record,
    store: &GraphStore,
) -> ExecutionResult<Value> {
    // `Vec<Value>`, not `Vec<PropertyValue>`: `[p = (n)-->() | p]` projects
    // *paths*, and a `PropertyValue` cannot hold one (#662). An all-scalar
    // comprehension is still returned as a `PropertyValue::Array` at the end,
    // so nothing that consumed the old shape changes.
    let mut results: Vec<Value> = Vec::new();

    for path in &pattern.paths {
        let start_var = path.start.variable.as_deref();
        let start_labels = &path.start.labels;

        // Get candidate start nodes
        let start_node_ids: Vec<NodeId> = if let Some(var) = start_var {
            if let Some(val) = record.get(var) {
                match val {
                    Value::NodeRef(id) | Value::Node(id, _) => vec![*id],
                    _ => vec![],
                }
            } else if let Some(first_label) = start_labels.first() {
                store.get_nodes_by_label(first_label).iter().map(|n| n.id).collect()
            } else {
                store.all_nodes().iter().map(|n| n.id).collect()
            }
        } else if let Some(first_label) = start_labels.first() {
            store.get_nodes_by_label(first_label).iter().map(|n| n.id).collect()
        } else {
            store.all_nodes().iter().map(|n| n.id).collect()
        };

        for node_id in &start_node_ids {
            let node = match store.get_node(*node_id) {
                Some(n) => n,
                None => continue,
            };
            let has_all_labels = start_labels.iter().all(|l| node.labels.contains(l));
            if !has_all_labels { continue; }

            if path.segments.is_empty() {
                let mut temp_record = record.clone();
                if let Some(var) = start_var {
                    temp_record.bind(var.to_string(), Value::NodeRef(*node_id));
                }
                if let Some(f) = filter {
                    let cond = eval_expression(f, &temp_record, store)?;
                    if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) { continue; }
                }
                results.push(eval_expression(projection, &temp_record, store)?);
            } else {
                // One-hop traversal for pattern comprehension
                for segment in &path.segments {
                    let edge_types: Vec<&str> = segment.edge.types.iter().map(|t| t.as_str()).collect();
                    let edges = match segment.edge.direction {
                        Direction::Outgoing => store.get_outgoing_edges(*node_id),
                        Direction::Incoming => store.get_incoming_edges(*node_id),
                        Direction::Both => {
                            let mut all = store.get_outgoing_edges(*node_id);
                            all.extend(store.get_incoming_edges(*node_id));
                            all
                        }
                    };
                    for edge in &edges {
                        if !edge_types.is_empty() && !edge_types.contains(&edge.edge_type.as_str()) {
                            continue;
                        }
                        let target_id = if edge.source == *node_id { edge.target } else { edge.source };
                        if !segment.node.labels.is_empty() {
                            if let Some(target) = store.get_node(target_id) {
                                let matches = segment.node.labels.iter().all(|l| target.labels.contains(l));
                                if !matches { continue; }
                            } else { continue; }
                        }
                        let mut temp_record = record.clone_with_capacity(2);
                        if let Some(var) = start_var {
                            temp_record.bind(var.to_string(), Value::NodeRef(*node_id));
                        }
                        if let Some(ref var) = segment.node.variable {
                            temp_record.bind(var.clone(), Value::NodeRef(target_id));
                        }
                        if let Some(ref var) = segment.edge.variable {
                            temp_record.bind(var.clone(), Value::EdgeRef(edge.id, edge.source, edge.target, edge.edge_type.clone()));
                        }
                        // `[p = (a)-->(b) | p]` — the named path, bound for the
                        // projection to read.
                        if let Some(ref pv) = path.path_variable {
                            temp_record.bind(
                                pv.clone(),
                                Value::Path { nodes: vec![*node_id, target_id], edges: vec![edge.id] },
                            );
                        }
                        if let Some(f) = filter {
                            let cond = eval_expression(f, &temp_record, store)?;
                            if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) { continue; }
                        }
                        results.push(eval_expression(projection, &temp_record, store)?);
                    }
                }
            }
        }
    }

    // Kept as a `PropertyValue::Array` when every element is a scalar, which
    // is what it always was; only a comprehension projecting entities needs
    // the wider shape.
    if results.iter().all(|v| matches!(v, Value::Property(_))) {
        let scalars = results
            .into_iter()
            .map(|v| match v {
                Value::Property(p) => p,
                _ => PropertyValue::Null,
            })
            .collect();
        return Ok(Value::Property(PropertyValue::Array(scalars)));
    }
    Ok(Value::List(results))
}

/// Evaluate a boolean predicate expression standalone (for parallel batch filtering).
/// Creates a temporary FilterOperator per call — no shared mutable state.
pub fn eval_predicate_standalone(predicate: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<bool> {
    // NullScan: produces nothing, never called — just satisfies the OperatorBox type
    struct NullScan;
    impl PhysicalOperator for NullScan {
        fn next(&mut self, _: &GraphStore) -> ExecutionResult<Option<Record>> { Ok(None) }
        fn reset(&mut self) {}
    }
    let evaluator = FilterOperator::new(Box::new(NullScan), predicate.clone());
    evaluator.evaluate_predicate(record, store)
}

/// Extract a node id from an evaluated value, whether it arrived materialized or as a
/// late-materialization reference (ADR-012).
fn value_node_id(v: &Value) -> Option<NodeId> {
    match v {
        Value::Node(id, _) => Some(*id),
        Value::NodeRef(id) => Some(*id),
        _ => None,
    }
}

/// `left IN right`, where `right` is any value that reads as a list.
///
/// Two things the previous `arr.contains(&left)` got wrong. It rejected a
/// `Vector`, which is what an all-float list literal parses as (#605). And it
/// compared with `PartialEq`, so `7.0 IN [7, 99]` was false even though
/// `p.score = 7` matches a float 7.0 -- `IN` disagreed with `=` about whether
/// an integer and a float can be equal.
fn eval_in_list(left: &PropertyValue, right: &PropertyValue) -> Option<PropertyValue> {
    let items = right.as_list_items()?;
    let numeric = |p: &PropertyValue| -> Option<f64> {
        match p {
            PropertyValue::Integer(i) => Some(*i as f64),
            PropertyValue::Float(f) => Some(*f),
            _ => None,
        }
    };
    // Cypher's IN is three-valued, and the answers that are neither true nor
    // false are where it gets interesting:
    //
    //   null IN [null]            null   -- nothing can be known about it
    //   4 IN [1, null, 3]         null   -- the null might have been the 4
    //   1 IN [1, null]            true   -- a definite match wins regardless
    //   null IN []                false  -- nothing to compare with at all
    //   [1] IN [[1, null]]        false  -- different lengths cannot be equal
    //   [1, 2] IN [[null, 'foo']] false  -- position 1 settles it
    //
    // `PartialEq` on `PropertyValue` is derived, so `Null == Null` is `true`
    // and the first of these answered `true` while the second answered
    // `false`. Both are values a caller will branch on, which makes this worse
    // than an error (#647).
    //
    // The last two are why "does either side contain a null" is not the rule:
    // a null makes a comparison unknown only when the comparison would
    // otherwise have to look at it. A length mismatch, or a definite
    // difference at any other position, settles the answer without ever
    // reaching the null.

    /// Cypher equality, three-valued: `None` means unknown.
    fn eq3(a: &PropertyValue, b: &PropertyValue) -> Option<bool> {
        match (a, b) {
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => None,
            (PropertyValue::Array(x), PropertyValue::Array(y)) => {
                if x.len() != y.len() {
                    return Some(false);
                }
                let mut unknown = false;
                for (i, j) in x.iter().zip(y.iter()) {
                    match eq3(i, j) {
                        // One definite difference is enough, whatever else the
                        // lists contain.
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
                if unknown { None } else { Some(true) }
            }
            (PropertyValue::Map(x), PropertyValue::Map(y)) => {
                if x.len() != y.len() || !x.keys().all(|k| y.contains_key(k)) {
                    return Some(false);
                }
                let mut unknown = false;
                for (key, xv) in x.iter() {
                    match y.get(key).map(|yv| eq3(xv, yv)) {
                        Some(Some(false)) | None => return Some(false),
                        Some(None) => unknown = true,
                        Some(Some(true)) => {}
                    }
                }
                if unknown { None } else { Some(true) }
            }
            _ => {
                if a == b {
                    return Some(true);
                }
                // 1 and 1.0 are the same number.
                let as_f64 = |p: &PropertyValue| -> Option<f64> {
                    match p {
                        PropertyValue::Integer(i) => Some(*i as f64),
                        PropertyValue::Float(f) => Some(*f),
                        _ => None,
                    }
                };
                Some(match (as_f64(a), as_f64(b)) {
                    (Some(x), Some(y)) => x == y,
                    _ => false,
                })
            }
        }
    }

    let mut unknown = false;
    for item in items.iter() {
        match eq3(left, item) {
            Some(true) => return Some(PropertyValue::Boolean(true)),
            Some(false) => {}
            None => unknown = true,
        }
    }

    // An empty list falls out of this as `false`, which is what Cypher says:
    // with nothing to compare against there is nothing undecidable.
    Some(if unknown {
        PropertyValue::Null
    } else {
        PropertyValue::Boolean(false)
    })
}

/// Property names in a stable order.
///
/// Lexicographic: stable, what a reader expects, and cheap over the handful of
/// keys a node or map has (#577).
/// Labels, sorted, as a list of strings.
///
/// `Node::labels` is a `HashSet`, whose iteration order varies per process
/// because `RandomState` seeds each one differently. Returning it raw made
/// `labels(n)` answer `['L','B']` on one run and `['B','L']` on the next for
/// the same node — two identical queries over identical data producing
/// different rows, which is the determinism requirement (LANG-14) failing in
/// the smallest possible way. It surfaced as a TCK scenario that passed or
/// failed depending on the run.
///
/// Sorted rather than insertion-ordered because insertion order is not
/// recorded anywhere: a set has no order to preserve, and inventing one that
/// survives a snapshot round trip would be a storage change. Sorted is
/// deterministic, and it is the same contract `keys()` already offers.
fn sorted_labels(node: &crate::graph::Node) -> Vec<PropertyValue> {
    let mut labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
    labels.sort_unstable();
    labels.into_iter().map(|l| PropertyValue::String(l.to_string())).collect()
}

fn sorted_keys(mut keys: Vec<PropertyValue>) -> Vec<PropertyValue> {
    keys.sort_by(|a, b| match (a, b) {
        (PropertyValue::String(x), PropertyValue::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });
    keys
}

/// Ordering between two property values, or `None` when Cypher cannot order
/// them.
///
/// Cypher orders numbers against numbers, strings against strings, booleans
/// against booleans, and temporal values against their own kind. Anything else
/// is *unknown* — `0 > 'x'` is null, not false and not an error (#607).
///
/// This deliberately does not reuse `PropertyValue`'s `Ord`. That order is
/// total by design because it backs the B-tree property index, so it answers
/// every comparison confidently, including the ones Cypher says have no
/// answer. Using it for a query-level comparison is what turned "unknown" into
/// a definite `false`.
fn cypher_ordering(left: &PropertyValue, right: &PropertyValue) -> Option<std::cmp::Ordering> {
    use PropertyValue::*;
    match (left, right) {
        (Integer(l), Integer(r)) => Some(l.cmp(r)),
        (Float(l), Float(r)) => l.partial_cmp(r),
        (Integer(l), Float(r)) => (*l as f64).partial_cmp(r),
        (Float(l), Integer(r)) => l.partial_cmp(&(*r as f64)),
        (String(l), String(r)) => Some(l.cmp(r)),
        (Boolean(l), Boolean(r)) => Some(l.cmp(r)),
        (DateTime(l), DateTime(r)) => Some(l.cmp(r)),
        // A timestamp against a raw epoch integer stays comparable: this is
        // long-standing behaviour here and the two are the same quantity.
        (DateTime(l), Integer(r)) | (Integer(l), DateTime(r)) => Some(l.cmp(r)),
        (
            Duration { months: m1, days: d1, seconds: s1, nanos: n1 },
            Duration { months: m2, days: d2, seconds: s2, nanos: n2 },
        ) => Some(m1.cmp(m2).then(d1.cmp(d2)).then(s1.cmp(s2)).then(n1.cmp(n2))),

        // The five temporal types compare within their own kind (#689).
        //
        // This function is a *second* comparison path -- `cypher_order` in
        // `graph::property` is the other -- and its `_ => None` fallthrough
        // meant every new temporal type silently compared as null the moment
        // the constructors started producing them. Nine `Temporal7` scenarios
        // that had been passing went red, which is the duplicated-evaluator
        // shape this codebase keeps producing: patching the copy in front of
        // you fixes nothing.
        // Lists order **lexicographically**, then by length: `[1, 0] >= [1]`
        // is true because the shared prefix is equal and the left is longer.
        // There was no arm for this at all, so every list comparison fell to
        // `_ => None` and answered null (#855).
        //
        // A null at a position that has to be compared makes the answer
        // undecidable, which is what `None` means here. It is not reached by a
        // list that is merely *longer* than the other: `[1, null] >= [1]` is
        // decided by the length, the null never compared.
        (Array(l), Array(r)) => {
            for (a, b) in l.iter().zip(r.iter()) {
                if matches!(a, Null) || matches!(b, Null) {
                    return None;
                }
                match cypher_ordering(a, b) {
                    Some(std::cmp::Ordering::Equal) => continue,
                    other => return other,
                }
            }
            Some(l.len().cmp(&r.len()))
        }
        (Date(l), Date(r)) => Some(l.cmp(r)),
        (LocalTime(l), LocalTime(r)) => Some(l.cmp(r)),
        (
            Time { nanos: n1, offset_seconds: o1 },
            Time { nanos: n2, offset_seconds: o2 },
        ) => Some(
            (n1 - (*o1 as i64) * 1_000_000_000).cmp(&(n2 - (*o2 as i64) * 1_000_000_000)),
        ),
        (LocalDateTime { secs: s1, nanos: n1 }, LocalDateTime { secs: s2, nanos: n2 }) => {
            Some(s1.cmp(s2).then(n1.cmp(n2)))
        }
        (
            ZonedDateTime { secs: s1, nanos: n1, .. },
            ZonedDateTime { secs: s2, nanos: n2, .. },
        ) => Some(s1.cmp(s2).then(n1.cmp(n2))),

        // Cross-kind temporal comparison is null in Cypher, not an ordering:
        // "is this date greater than that time" has no answer. Falls through
        // to `None` below, which is that null -- spelled out here because the
        // wildcard is exactly what hid the bug above.
        _ => None,
    }
}

/// `STARTS WITH` / `ENDS WITH` / `CONTAINS` over one pair of operands.
///
/// Two strings answer the question; anything else is null. Cypher does not
/// treat `1 STARTS WITH 'a'` as a caller error — the TCK asks for all 36
/// pairings from `[1, 3.14, true, [], {}, null]` and expects null for every
/// one (String8/9/10 scenario 8).
///
/// This exists because the comparison was implemented twice and both copies
/// had independently settled on raising instead. Both now call here.

/// The map a temporal constructor was handed, whichever shape it arrived in.
///
/// A map *literal* containing variables evaluates to `Value::Map`, not
/// `Value::Property(PropertyValue::Map)` — so `datetime({date: d, time: t})`
/// never matched the map arm and fell through to "requires a string or map
/// argument". That is 174 `Temporal3` scenarios: the selection form was
/// implemented and unreachable through the shape the executor actually
/// produces (#772).
fn temporal_arg_map(v: &Value) -> Option<std::collections::HashMap<String, PropertyValue>> {
    match v {
        Value::Property(PropertyValue::Map(m)) => Some(m.clone()),
        Value::Map(entries) => {
            let mut out = std::collections::HashMap::new();
            for (k, val) in entries {
                match val {
                    Value::Property(p) => { out.insert(k.clone(), p.clone()); }
                    Value::Null => { out.insert(k.clone(), PropertyValue::Null); }
                    // A node or a path inside a temporal map is not something
                    // to coerce; leave it out so the component reader reports
                    // the missing field rather than inventing a value.
                    _ => {}
                }
            }
            Some(out)
        }
        _ => None,
    }
}

/// Days-since-epoch of any temporal value that has a date part.
///
/// `None` for `LocalTime`/`Time`, which genuinely have no date — the caller
/// turns that into a type error rather than substituting the epoch, because a
/// substituted 1970-01-01 reads as a real answer (#689).
fn date_part_of(v: &PropertyValue) -> Option<i32> {
    match v {
        PropertyValue::Date(d) => Some(*d),
        PropertyValue::LocalDateTime { secs, .. } => Some(secs.div_euclid(86_400) as i32),
        PropertyValue::ZonedDateTime { secs, offset_seconds, .. } => {
            Some((secs + *offset_seconds as i64).div_euclid(86_400) as i32)
        }
        PropertyValue::DateTime(ms) => Some(ms.div_euclid(86_400_000) as i32),
        _ => None,
    }
}

/// Split a (days-since-epoch, nanoseconds-of-day) pair into the
/// (seconds, nanos) a `LocalDateTime` or `ZonedDateTime` stores.
///
/// Computing `days * 86_400 * 1_000_000_000` first **overflows i64**: that
/// product spans only about ±292 years from 1970, so `year: 1` silently became
/// 1754 and `year: 9999` became 1815. Well-formed date-times, wrapped.
///
/// Seconds are the wider unit and cover ±292 *billion* years, so the days go
/// through seconds and only the sub-day remainder is ever counted in
/// nanoseconds (#814).
fn day_and_nanos_to_secs(days: i32, nanos_of_day: i64) -> (i64, u32) {
    let secs = days as i64 * 86_400 + nanos_of_day.div_euclid(1_000_000_000);
    (secs, nanos_of_day.rem_euclid(1_000_000_000) as u32)
}


/// Apply a map's date components on top of an already-selected date.
///
/// `{date: other, day: 28}` keeps `other`'s year and month and replaces the
/// day; `{date: other, ordinalDay: 28}` replaces the whole day-of-year, so it
/// moves the month too. The four spellings do not mix — naming `ordinalDay`
/// means the calendar fields are not consulted — which is why this dispatches
/// on which keys are present rather than defaulting each field.
fn apply_date_overrides(
    base: i32,
    map: &std::collections::HashMap<String, PropertyValue>,
) -> Result<i32, ExecutionError> {
    use chrono::Datelike;
    const KEYS: &[&str] = &[
        "year", "month", "day", "ordinalDay", "week", "dayOfWeek", "quarter", "dayOfQuarter",
    ];
    if !KEYS.iter().any(|k| map.contains_key(*k)) {
        return Ok(base);
    }
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let cur = epoch
        .checked_add_signed(chrono::Duration::days(base as i64))
        .ok_or_else(|| ExecutionError::RuntimeError("date out of range".into()))?;
    let g = |k: &str| map.get(k).and_then(|v| v.as_integer());
    let year = g("year").unwrap_or(cur.year() as i64) as i32;

    let out = if let Some(ord) = g("ordinalDay") {
        chrono::NaiveDate::from_yo_opt(year, ord as u32)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("invalid ordinalDay {ord}")))?
    } else if map.contains_key("week") || map.contains_key("dayOfWeek") {
        // In a week date the year is the **ISO week year**, which is not the
        // calendar year near a year boundary: 1816-12-30 is a Monday belonging
        // to 1817-W01, so `{date: date('1816-12-30'), week: 2}` is in January
        // 1817. Defaulting to `cur.year()` sent it to 1816-W02, eleven and a
        // half months early -- a real date, in the wrong year (#851).
        //
        // An explicit `year` still wins, and is likewise read as the week year:
        // `{date: date('1816-12-31'), year: 1817, week: 2}` is 1817-W02.
        let week_year = g("year").unwrap_or(cur.iso_week().year() as i64) as i32;
        let week = g("week").unwrap_or(cur.iso_week().week() as i64) as u32;
        let dow = g("dayOfWeek").unwrap_or(cur.weekday().number_from_monday() as i64) as u32;
        chrono::NaiveDate::from_isoywd_opt(week_year, week, weekday_from_iso_num(dow))
            .ok_or_else(|| ExecutionError::RuntimeError(format!("invalid week date {week_year}-W{week}-{dow}")))?
    } else if map.contains_key("quarter") || map.contains_key("dayOfQuarter") {
        let q = g("quarter").unwrap_or(((cur.month() - 1) / 3 + 1) as i64);
        // The day within the quarter defaults to the **current** one, exactly
        // as `week`/`dayOfWeek` above default from `cur`. Defaulting to 1 made
        // `{date: <1984-11-11>, quarter: 3}` answer 1984-07-01 instead of
        // 1984-08-11: an override that names one component silently reset two
        // others, and the result is a perfectly ordinary date (#838).
        let cur_quarter_start = chrono::NaiveDate::from_ymd_opt(
            cur.year(),
            ((cur.month() - 1) / 3) * 3 + 1,
            1,
        )
        .ok_or_else(|| ExecutionError::RuntimeError("date out of range".into()))?;
        let cur_day_of_quarter = cur.signed_duration_since(cur_quarter_start).num_days() + 1;
        let d = g("dayOfQuarter").unwrap_or(cur_day_of_quarter);
        let start = chrono::NaiveDate::from_ymd_opt(year, ((q - 1) * 3 + 1) as u32, 1)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("invalid quarter {q}")))?;
        start
            .checked_add_signed(chrono::Duration::days(d - 1))
            .ok_or_else(|| ExecutionError::RuntimeError(format!("invalid dayOfQuarter {d}")))?
    } else {
        let month = g("month").unwrap_or(cur.month() as i64) as u32;
        let day = g("day").unwrap_or(cur.day() as i64) as u32;
        chrono::NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("invalid date {year}-{month}-{day}")))?
    };
    Ok(out.signed_duration_since(epoch).num_days() as i32)
}

/// Apply a map's clock components on top of an already-selected time.
///
/// Replaces only the fields named: `{time: t, second: 42}` keeps the hour,
/// minute and fraction from `t`. The three sub-second components are additive
/// with each other and together replace the selected fraction only if any is
/// given — the same rule `compose_date_and_time` uses, kept in one place so
/// the two cannot drift (#802).
fn apply_time_overrides(
    base: i64,
    map: &std::collections::HashMap<String, PropertyValue>,
) -> i64 {
    let g = |k: &str| map.get(k).and_then(|v| v.as_integer());
    let mut hour = base / 3_600_000_000_000;
    let mut minute = base / 60_000_000_000 % 60;
    let mut second = base / 1_000_000_000 % 60;
    let mut sub = base % 1_000_000_000;
    if let Some(v) = g("hour") { hour = v; }
    if let Some(v) = g("minute") { minute = v; }
    if let Some(v) = g("second") { second = v; }
    if ["millisecond", "microsecond", "nanosecond"].iter().any(|k| map.contains_key(*k)) {
        sub = g("millisecond").unwrap_or(0) * 1_000_000
            + g("microsecond").unwrap_or(0) * 1_000
            + g("nanosecond").unwrap_or(0);
    }
    (hour * 3600 + minute * 60 + second) * 1_000_000_000 + sub
}


fn weekday_from_iso_num(dow: u32) -> chrono::Weekday {
    use chrono::Weekday::*;
    match dow { 1 => Mon, 2 => Tue, 3 => Wed, 4 => Thu, 5 => Fri, 6 => Sat, _ => Sun }
}


/// Nanoseconds-since-midnight of any temporal value that has a time part.
fn time_part_of(v: &PropertyValue) -> Option<i64> {
    match v {
        PropertyValue::LocalTime(n) => Some(*n),
        PropertyValue::Time { nanos, .. } => Some(*nanos),
        PropertyValue::LocalDateTime { secs, nanos } => {
            Some(secs.rem_euclid(86_400) * 1_000_000_000 + *nanos as i64)
        }
        PropertyValue::ZonedDateTime { secs, nanos, offset_seconds, .. } => {
            let local = secs + *offset_seconds as i64;
            Some(local.rem_euclid(86_400) * 1_000_000_000 + *nanos as i64)
        }
        PropertyValue::DateTime(ms) => Some(ms.rem_euclid(86_400_000) * 1_000_000),
        _ => None,
    }
}

/// The date and time a composite constructor's map describes.
///
/// Handles both the component form (`{year, month, day, hour, ...}`) and the
/// *selection* form (`{date: d, time: t}`), which Cypher allows and which the
/// TCK's Temporal3 exercises heavily. A map may mix them: `{date: d, hour: 9}`
/// takes the date from `d` and the clock from the components.
fn compose_date_and_time(
    map: &std::collections::HashMap<String, PropertyValue>,
) -> Result<(i32, i64), ExecutionError> {
    use crate::query::executor::temporal as tmp;
    use chrono::Datelike;

    // A selected value is the *base*; individual components then override
    // parts of it. `{date: d, time: t, second: 42}` keeps 12:31 from `t` and
    // replaces only the second -- reading the components as a whole clock
    // instead gives 00:00:42, which is a plausible-looking wrong answer.
    let base_date = map.get("date").or_else(|| map.get("datetime")).and_then(date_part_of);
    let base_time = map.get("time").or_else(|| map.get("datetime")).and_then(time_part_of);

    let mut days = match base_date {
        Some(d) => d,
        None if map.contains_key("year") => tmp::date_days(map)?,
        None => {
            return Err(ExecutionError::RuntimeError(
                "a date-time needs a date: give `year` or `date`".to_string(),
            ))
        }
    };

    // Date overrides on top of a selected date, through the same function
    // `date()` uses.
    //
    // This was a **second implementation** of the rule, and it tested for
    // `week`, `dayOfWeek`, `ordinalDay`, `quarter` and `dayOfQuarter` in its
    // condition and then handled only `year`/`month`/`day`. So
    // `localdatetime({date: date('1816-12-31'), week: 2})` entered the branch,
    // recomputed the same y/m/d it started with, and returned the date
    // unchanged -- a correct-looking value from an override that did nothing.
    //
    // Routing both through `apply_date_overrides` also gives the composite
    // constructors the week-year rule (#851) and the `quarter` fix (#838),
    // neither of which they had (#851).
    if base_date.is_some() {
        days = apply_date_overrides(days, map)?;
    }

    let clock_keys = ["hour", "minute", "second", "millisecond", "microsecond", "nanosecond"];
    let has_clock = clock_keys.iter().any(|k| map.contains_key(*k));
    let nanos = match (base_time, has_clock) {
        // Nothing selected: read the components as a whole clock.
        (None, _) => {
            if has_clock { tmp::time_of_day_nanos(map)? } else { 0 }
        }
        (Some(t), false) => t,
        // Selected *and* overridden: replace only the fields named.
        (Some(t), true) => {
            let mut hour = t / 3_600_000_000_000;
            let mut minute = t / 60_000_000_000 % 60;
            let mut second = t / 1_000_000_000 % 60;
            let mut sub = t % 1_000_000_000;
            if let Some(v) = map.get("hour").and_then(|v| v.as_integer()) { hour = v; }
            if let Some(v) = map.get("minute").and_then(|v| v.as_integer()) { minute = v; }
            if let Some(v) = map.get("second").and_then(|v| v.as_integer()) { second = v; }
            // The three sub-second fields are additive with each other, and
            // together replace the selected fraction only if any is given.
            if ["millisecond", "microsecond", "nanosecond"].iter().any(|k| map.contains_key(*k)) {
                let ms = map.get("millisecond").and_then(|v| v.as_integer()).unwrap_or(0);
                let us = map.get("microsecond").and_then(|v| v.as_integer()).unwrap_or(0);
                let ns = map.get("nanosecond").and_then(|v| v.as_integer()).unwrap_or(0);
                sub = ms * 1_000_000 + us * 1_000 + ns;
            }
            (hour * 3600 + minute * 60 + second) * 1_000_000_000 + sub
        }
    };
    Ok((days, nanos))
}

/// Seconds/nanos of a naive date-time string, with no zone.
///
/// Delegates to the ISO parsers so every spelling the date and time parsers
/// accept works here too — `20150721T21:40`, `2015-W30-2T214032.142`,
/// `2015-202T21:40:32`. The previous list of `chrono` format strings covered
/// four shapes out of the corpus's dozen (#775).
fn parse_naive_date_time(s: &str) -> Result<(i64, u32), ExecutionError> {
    use crate::query::executor::temporal as tmp;
    let t = s.trim();
    let (date_part, time_part) = match t.split_once('T') {
        Some((d, ti)) => (d, Some(ti)),
        None => (t, None),
    };
    let days = tmp::parse_iso_date(date_part)? as i64;
    let nanos = match time_part {
        Some(ti) if !ti.is_empty() => tmp::parse_iso_time(ti)?,
        _ => 0,
    };
    let total = days * 86_400 * 1_000_000_000 + nanos;
    Ok((
        total.div_euclid(1_000_000_000),
        total.rem_euclid(1_000_000_000) as u32,
    ))
}

/// The `PropertyValue` inside a `Value`, when there is one.
fn value_as_property(v: &Value) -> Option<PropertyValue> {
    match v {
        Value::Property(p) => Some(p.clone()),
        _ => None,
    }
}

fn string_position_op(
    op: StringPositionOp,
    left: &PropertyValue,
    right: &PropertyValue,
) -> PropertyValue {
    match (left, right) {
        (PropertyValue::String(l), PropertyValue::String(r)) => {
            let r = r.as_str();
            PropertyValue::Boolean(match op {
                StringPositionOp::StartsWith => l.starts_with(r),
                StringPositionOp::EndsWith => l.ends_with(r),
                StringPositionOp::Contains => l.contains(r),
            })
        }
        _ => PropertyValue::Null,
    }
}

#[derive(Clone, Copy)]
enum StringPositionOp {
    StartsWith,
    EndsWith,
    Contains,
}

/// Functions that are asked *about* null and so must see it themselves.
///
/// Everything else propagates. Keeping the exceptions explicit — rather than
/// letting each arm decide — is what stops the rule from drifting apart again:
/// twenty-two TCK scenarios failed because twenty-two arms each answered the
/// question separately, and most answered it with a type error.
const NULL_TOLERANT_FUNCTIONS: &[&str] = &["coalesce", "exists"];

/// Every variable name an expression reads.
///
/// Names only -- `a.num` contributes `a` -- because that is what a record is
/// keyed on. Used to decide which pre-projection bindings a `WITH ... ORDER BY`
/// has to carry (#970).
fn collect_expression_names(expr: &Expression, out: &mut HashSet<String>) {
    match expr {
        Expression::Variable(v) | Expression::PathVariable(v) => {
            out.insert(v.clone());
        }
        Expression::Property { variable, .. } => {
            out.insert(variable.clone());
        }
        Expression::Binary { left, right, .. } => {
            collect_expression_names(left, out);
            collect_expression_names(right, out);
        }
        Expression::Unary { expr, .. } => collect_expression_names(expr, out),
        Expression::Function { args, .. } => {
            for a in args {
                collect_expression_names(a, out);
            }
        }
        Expression::ListExpr(items) => {
            for e in items {
                collect_expression_names(e, out);
            }
        }
        Expression::MapExpr(entries) => {
            for (_, e) in entries {
                collect_expression_names(e, out);
            }
        }
        Expression::Index { expr, index } => {
            collect_expression_names(expr, out);
            collect_expression_names(index, out);
        }
        Expression::ListSlice { expr, start, end } => {
            collect_expression_names(expr, out);
            for e in start.iter().chain(end.iter()) {
                collect_expression_names(e, out);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            for e in operand.iter().chain(else_result.iter()) {
                collect_expression_names(e, out);
            }
            for (w, t) in when_clauses {
                collect_expression_names(w, out);
                collect_expression_names(t, out);
            }
        }
        _ => {}
    }
}

/// Every function `eval_function` dispatches on.
///
/// Exists so an unknown name can be rejected at **compile time** rather than
/// producing an empty result at run time. `RETURN foo(a)` used to succeed with
/// zero rows -- a misspelled `lenght(x)` or `toLowerCase(s)` returned an empty
/// result set from a query that reported success, and the reader concluded
/// something about their data (#947).
///
/// Worse, the run-time error only fires on a row that reaches the expression,
/// so over an empty graph the call never ran and the query "succeeded". A
/// compile-time check does not depend on the data.
///
/// **One list, cross-checked.** `tests/function_reachability.rs` extracts the
/// arms straight from this file's dispatcher and asserts every one can be
/// named in Cypher, so this list and the dispatcher cannot drift into
/// rejecting a function that works -- which is much worse than accepting one
/// that does not.
///
/// `true` and `false` are here because they are dispatcher *arms*, not because
/// `true()` is a function anybody should write. Leaving them out made that
/// reachability test fail, and narrowing an existing guard to keep a new check
/// green is the wrong way round.
pub const KNOWN_FUNCTIONS: &[&str] = &[
    "abs", "acos", "asin", "atan", "atan2", "bfs", "breadthfirstsearch", "cdlp", "ceil",
    "coalesce", "components", "connectedcomponents", "cos", "cosh", "cosine", "cot", "date",
    "date.truncate", "datetime", "datetime.fromepoch", "datetime.fromepochmillis",
    "datetime.truncate", "degrees", "dijkstra", "duration",
    "duration.between", "duration.indays", "duration.inmonths", "duration.inseconds",
    "duration_between", "e", "elementid", "endnode", "exists", "exp", "false", "floor",
    "haslabels", "haversin", "head", "hierarchy_lca", "hierarchy_rollup", "id", "isempty",
    "isnan", "keys", "l2", "labelpropagation", "labels", "last", "lcc", "left", "length",
    "localdatetime", "localdatetime.truncate", "localtime", "localtime.truncate", "log",
    "log10", "louvain", "ltrim", "maxflow", "mst", "nodes", "or.solve", "pagerank",
    "pagerank2", "percentilecont", "percentiledisc", "pi", "prank", "properties", "radians",
    "rand", "randomuuid", "range", "relationships", "rels", "replace", "reverse", "right",
    "round", "rtrim", "scc", "shortestpath", "shortestpathweighted", "sign", "sin", "sinh",
    "size", "split", "sqrt", "startnode", "stdev", "stdevp", "substring", "subsumes", "tail",
    "tan", "tanh", "time", "time.truncate", "timestamp", "toboolean", "tobooleanornull",
    "tofloat", "tofloatornull", "toint", "tointeger", "tointegerornull", "tolower",
    "tolowercase", "tostring", "tostringornull", "toupper", "touppercase", "trianglecount",
    "trim", "true", "type", "valuetype", "wcc", "weightedpath",
];

/// Is `name` a function this engine implements?
///
/// Case-insensitive, because Cypher's function names are. Aggregates are not
/// here -- they are dispatched by the planner into `AggregateOperator`, and
/// `validate.rs` checks them against `AGGREGATE_NAMES`.
///
/// **A namespaced name is always allowed.** `date.realtime`, `datetime.statement`
/// and friends are tolerated at run time -- several of them exist only to
/// propagate null -- and rejecting them at compile time turned 345 passing
/// scenarios into errors on the first attempt at this check. The name that
/// actually costs a user a debugging session is the un-namespaced typo
/// (`lenght`, `toLowerCase`), and that is what this catches. Narrower on
/// purpose: over-rejecting a valid query is the worse failure.
pub fn is_known_function(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.contains('.') || KNOWN_FUNCTIONS.contains(&lower.as_str())
}


/// Shared function evaluation for scalar functions (not aggregates)
pub fn eval_function(name: &str, args: &[Value], store: Option<&GraphStore>) -> ExecutionResult<Value> {
    let lowered = name.to_lowercase();

    // Null in, null out. In Cypher null means "unknown", so a question asked
    // about it has an unknown answer rather than being a caller error —
    // `labels(null)` is the row where an OPTIONAL MATCH did not match, and
    // raising there aborts a query Cypher answers with a null column.
    if !NULL_TOLERANT_FUNCTIONS.contains(&lowered.as_str())
        && args.iter().any(|a| matches!(a, Value::Null | Value::Property(PropertyValue::Null)))
    {
        return Ok(Value::Null);
    }

    match lowered.as_str() {
        // Hierarchy functions (ADR-035) — the direct surface for what the planner
        // rewrites cannot reach: an order test inside an arbitrary predicate, or a
        // roll-up in the middle of a larger projection.
        //
        //   subsumes(x, y)                  -- is x under y?
        //   subsumes(x, y, 'idx')           -- ...using a named hierarchy
        //   hierarchy_rollup(root, 'sum')   -- index-resident fold under root
        //   hierarchy_rollup(root, 'sum', 'idx')
        "subsumes" => {
            let store = store.ok_or_else(|| ExecutionError::RuntimeError(
                "subsumes() requires graph context".to_string()))?;
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "subsumes(child, ancestor[, index]) takes 2 or 3 arguments".to_string()));
            }
            let x = value_node_id(&args[0]).ok_or_else(|| ExecutionError::RuntimeError(
                "subsumes(): first argument must be a node".to_string()))?;
            let y = value_node_id(&args[1]).ok_or_else(|| ExecutionError::RuntimeError(
                "subsumes(): second argument must be a node".to_string()))?;
            let entry = match args.get(2) {
                Some(v) => {
                    let n = extract_string(v)?;
                    store.hierarchy_index.usable_named(&n).ok_or_else(|| {
                        ExecutionError::RuntimeError(format!(
                            "subsumes(): no usable hierarchy index named '{n}'                              (it may be stale — REBUILD it — or declined)"))
                    })?
                }
                None => match store.hierarchy_index.usable_containing(&[x, y]) {
                    Some(e) => e,
                    // Both nodes outside every hierarchy: they are not in a subsumption
                    // relation anyone declared, which is FALSE rather than an error --
                    // but only once there is a hierarchy to be outside of (#721).
                    None => {
                        require_a_hierarchy(store, "subsumes")?;
                        return Ok(Value::Property(PropertyValue::Boolean(false)));
                    }
                },
            };
            let guard = entry.read().unwrap();
            let answer = guard
                .index
                .as_ref()
                .and_then(|i| i.subsumes_ids(x, y))
                .unwrap_or(false);
            Ok(Value::Property(PropertyValue::Boolean(answer)))
        }
        "hierarchy_rollup" => {
            let store = store.ok_or_else(|| ExecutionError::RuntimeError(
                "hierarchy_rollup() requires graph context".to_string()))?;
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "hierarchy_rollup(root, op[, index]) takes 2 or 3 arguments".to_string()));
            }
            let root = value_node_id(&args[0]).ok_or_else(|| ExecutionError::RuntimeError(
                "hierarchy_rollup(): first argument must be a node".to_string()))?;
            let op_name = extract_string(&args[1])?;
            let op = crate::index::hierarchy::RollupOp::parse(&op_name).ok_or_else(|| {
                ExecutionError::RuntimeError(format!(
                    "hierarchy_rollup(): unsupported aggregate '{op_name}': expected sum, count, min or max"))
            })?;
            let entry = match args.get(2) {
                Some(v) => {
                    let n = extract_string(v)?;
                    store.hierarchy_index.usable_named(&n).ok_or_else(|| {
                        ExecutionError::RuntimeError(format!(
                            "hierarchy_rollup(): no usable hierarchy index named '{n}'"))
                    })?
                }
                None => match store.hierarchy_index.usable_containing(&[root]) {
                    Some(e) => e,
                    // Null for a root outside every hierarchy; an error when
                    // there is no hierarchy at all (#721).
                    None => {
                        require_a_hierarchy(store, "hierarchy_rollup")?;
                        return Ok(Value::Property(PropertyValue::Null));
                    }
                },
            };
            let guard = entry.read().unwrap();
            let value = guard
                .index
                .as_ref()
                .and_then(|i| i.rollup_id(root, op))
                .unwrap_or(crate::index::hierarchy::RollupValue::Null);
            Ok(Value::Property(
                crate::query::executor::hierarchy_ops::rollup_to_property(value),
            ))
        }
        "hierarchy_lca" => {
            let store = store.ok_or_else(|| ExecutionError::RuntimeError(
                "hierarchy_lca() requires graph context".to_string()))?;
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "hierarchy_lca(a, b[, index]) takes 2 or 3 arguments".to_string()));
            }
            let a = value_node_id(&args[0]).ok_or_else(|| ExecutionError::RuntimeError(
                "hierarchy_lca(): first argument must be a node".to_string()))?;
            let b = value_node_id(&args[1]).ok_or_else(|| ExecutionError::RuntimeError(
                "hierarchy_lca(): second argument must be a node".to_string()))?;
            let entry = match args.get(2) {
                Some(v) => {
                    let n = extract_string(v)?;
                    store.hierarchy_index.usable_named(&n).ok_or_else(|| {
                        ExecutionError::RuntimeError(format!(
                            "hierarchy_lca(): no usable hierarchy index named '{n}'"))
                    })?
                }
                None => match store.hierarchy_index.usable_containing(&[a, b]) {
                    Some(e) => e,
                    // No common ancestor for two nodes outside every hierarchy;
                    // an error when there is no hierarchy at all (#721).
                    None => {
                        require_a_hierarchy(store, "hierarchy_lca")?;
                        return Ok(Value::Property(PropertyValue::Array(Vec::new())));
                    }
                },
            };
            let guard = entry.read().unwrap();
            // A DAG can have several incomparable lowest common ancestors, so this is a
            // list even when a tree would always yield exactly one.
            let ids = guard
                .index
                .as_ref()
                .and_then(|i| i.lowest_common_ancestors_ids(a, b))
                .unwrap_or_default();
            Ok(Value::Property(PropertyValue::Array(
                ids.into_iter()
                    .map(|id| PropertyValue::Integer(id.as_u64() as i64))
                    .collect(),
            )))
        }
        // String functions
        "toupper" | "touppercase" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.to_uppercase())))
        }
        "tolower" | "tolowercase" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.to_lowercase())))
        }
        "trim" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.trim().to_string())))
        }
        "split" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(
                    "split() requires 2 arguments: split(string, delimiter)".to_string(),
                ));
            }
            let s = extract_string(&args[0])?;
            let delim = extract_string(&args[1])?;
            // Cypher splits on an empty delimiter into single characters; Rust's
            // split("") would additionally yield empty strings at both ends.
            let parts: Vec<PropertyValue> = if delim.is_empty() {
                s.chars().map(|c| PropertyValue::String(c.to_string())).collect()
            } else {
                s.split(delim.as_str())
                    .map(|p| PropertyValue::String(p.to_string()))
                    .collect()
            };
            Ok(Value::Property(PropertyValue::Array(parts)))
        }
        "ltrim" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.trim_start().to_string())))
        }
        "rtrim" => {
            let s = extract_string(&args[0])?;
            Ok(Value::Property(PropertyValue::String(s.trim_end().to_string())))
        }
        "replace" => {
            if args.len() < 3 { return Err(ExecutionError::RuntimeError("replace() requires 3 arguments".to_string())); }
            let s = extract_string(&args[0])?;
            let from = extract_string(&args[1])?;
            let to = extract_string(&args[2])?;
            Ok(Value::Property(PropertyValue::String(s.replace(&from, &to))))
        }
        "substring" => {
            if args.len() < 2 { return Err(ExecutionError::RuntimeError("substring() requires at least 2 arguments".to_string())); }
            let s = extract_string(&args[0])?;
            let start = extract_int(&args[1])? as usize;
            let chars: Vec<char> = s.chars().collect();
            if start >= chars.len() {
                return Ok(Value::Property(PropertyValue::String(String::new())));
            }
            let result = if args.len() >= 3 {
                let len = extract_int(&args[2])? as usize;
                chars[start..std::cmp::min(start + len, chars.len())].iter().collect()
            } else {
                chars[start..].iter().collect()
            };
            Ok(Value::Property(PropertyValue::String(result)))
        }
        "left" => {
            let s = extract_string(&args[0])?;
            let n = extract_int(&args[1])? as usize;
            Ok(Value::Property(PropertyValue::String(s.chars().take(n).collect())))
        }
        "right" => {
            let s = extract_string(&args[0])?;
            let n = extract_int(&args[1])? as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            Ok(Value::Property(PropertyValue::String(chars[start..].iter().collect())))
        }
        // Cypher's reverse() takes a list as well as a string, and this took
        // only a string -- `reverse([1,2,3])` answered
        // `TypeError("Expected string argument")` (#578).
        "reverse" => match &args[0] {
            Value::Property(p) if p.as_list_items().is_some() => {
                let mut reversed = p.as_list_items().unwrap();
                reversed.reverse();
                Ok(Value::Property(PropertyValue::Array(reversed)))
            }
            Value::Property(PropertyValue::Null) | Value::Null => {
                Ok(Value::Property(PropertyValue::Null))
            }
            other => {
                let text = extract_string(other)?;
                Ok(Value::Property(PropertyValue::String(text.chars().rev().collect())))
            }
        },
        "tostring" => {
            let val = &args[0];
            let s = match val {
                Value::Property(PropertyValue::String(s)) => s.clone(),
                Value::Property(PropertyValue::Integer(i)) => i.to_string(),
                Value::Property(PropertyValue::Float(f)) => f.to_string(),
                Value::Property(PropertyValue::Boolean(b)) => b.to_string(),
                Value::Property(PropertyValue::DateTime(millis)) => {
                    use chrono::TimeZone;
                    chrono::Utc.timestamp_millis_opt(*millis).single()
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| format!("DateTime({})", millis))
                }
                // Through the one function that owns the format (#769). The old
                // `P{}M{}DT{}S` emitted `P0M0DT0S` for a zero duration where
                // Cypher writes `PT0S`, and dropped nanoseconds entirely.
                Value::Property(p @ PropertyValue::Duration { .. }) => p.to_cypher_string(),
                // The five temporal types render through the one function that
                // owns the format, so `toString()` and the TCK harness cannot
                // disagree about what a value looks like (#689).
                Value::Property(p @ PropertyValue::Date(_))
                | Value::Property(p @ PropertyValue::LocalTime(_))
                | Value::Property(p @ PropertyValue::Time { .. })
                | Value::Property(p @ PropertyValue::LocalDateTime { .. })
                | Value::Property(p @ PropertyValue::ZonedDateTime { .. }) => p.to_cypher_string(),
                Value::Null | Value::Property(PropertyValue::Null) => "null".to_string(),
                _ => return Err(ExecutionError::TypeError("Cannot convert to string".to_string())),
            };
            Ok(Value::Property(PropertyValue::String(s)))
        }
        "tointeger" | "toint" => {
            match &args[0] {
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(*i))),
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Integer(*f as i64))),
                // Cypher yields null for a string it cannot parse, rather than
                // failing the query. Erroring made `toInteger` unusable for the
                // thing it is mostly used for -- checking whether input is a
                // number at all (#606).
                // A string holding a **float** converts too, truncating:
                // `toInteger('2.9')` is 2, the same as `toInteger(2.9)`. Only
                // `parse::<i64>()` was tried, so it answered null -- the same
                // null it gives for `'foo'`, which is the answer that means
                // "not a number at all" (#885).
                Value::Property(PropertyValue::String(s)) => Ok(Value::Property(
                    s.trim()
                        .parse::<i64>()
                        .ok()
                        .or_else(|| {
                            s.trim().parse::<f64>().ok().filter(|f| f.is_finite()).map(|f| f as i64)
                        })
                        .map(PropertyValue::Integer)
                        .unwrap_or(PropertyValue::Null),
                )),
                Value::Null | Value::Property(PropertyValue::Null) => {
                    Ok(Value::Property(PropertyValue::Null))
                }
                _ => Err(ExecutionError::TypeError("Cannot convert to integer".to_string())),
            }
        }
        "tofloat" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(*f))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Float(*i as f64))),
                // Null rather than an error, as with `toInteger` (#606).
                Value::Property(PropertyValue::String(s)) => Ok(Value::Property(
                    s.parse::<f64>().map(PropertyValue::Float).unwrap_or(PropertyValue::Null),
                )),
                Value::Null | Value::Property(PropertyValue::Null) => {
                    Ok(Value::Property(PropertyValue::Null))
                }
                _ => Err(ExecutionError::TypeError("Cannot convert to float".to_string())),
            }
        }
        // Size/length
        "size" | "length" => {
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => Ok(Value::Property(PropertyValue::Integer(s.len() as i64))),
                Value::Path { edges, .. } => Ok(Value::Property(PropertyValue::Integer(edges.len() as i64))),
                // A list of entities -- what a variable-length relationship
                // variable binds -- is a list, and `size()` counts it (#652).
                Value::List(items) => Ok(Value::Property(PropertyValue::Integer(items.len() as i64))),
                Value::Property(p) if p.as_list_items().is_some() => Ok(Value::Property(
                    PropertyValue::Integer(p.as_list_items().unwrap().len() as i64),
                )),
                _ => Err(ExecutionError::TypeError("size() requires string, list, or path".to_string())),
            }
        }
        // Path functions.
        //
        // These return the nodes and relationships themselves. They used to
        // return their **integer ids**, because a `PropertyValue::Array`
        // cannot hold an entity -- so `nodes(p)` answered `[1, 2]` where
        // Cypher answers `[(:A), (:B)]`, and anything reading a property off
        // an element got nothing (#652).
        "nodes" => {
            match &args[0] {
                Value::Path { nodes, .. } => Ok(Value::List(
                    nodes.iter().map(|id| Value::NodeRef(*id)).collect(),
                )),
                _ => Err(ExecutionError::TypeError("nodes() requires a path".to_string())),
            }
        }
        "relationships" | "rels" => {
            match &args[0] {
                Value::Path { edges, .. } => Ok(Value::List(
                    edges
                        .iter()
                        .map(|id| match store.and_then(|s| s.get_edge(*id)) {
                            Some(e) => Value::EdgeRef(*id, e.source, e.target, e.edge_type.clone()),
                            None => Value::Null,
                        })
                        .collect(),
                )),
                _ => Err(ExecutionError::TypeError("relationships() requires a path".to_string())),
            }
        }
        // Math functions
        "abs" => {
            match &args[0] {
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(i.abs()))),
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(f.abs()))),
                _ => Err(ExecutionError::TypeError("abs() requires numeric".to_string())),
            }
        }
        "ceil" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Integer(f.ceil() as i64))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(*i))),
                _ => Err(ExecutionError::TypeError("ceil() requires numeric".to_string())),
            }
        }
        "floor" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Integer(f.floor() as i64))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(*i))),
                _ => Err(ExecutionError::TypeError("floor() requires numeric".to_string())),
            }
        }
        "round" => {
            if args.len() >= 2 {
                // CY-23: round(x, precision) — e.g. round(3.14159, 2) → 3.14
                let v = extract_float(&args[0])?;
                let precision = extract_float(&args[1])? as i32;
                let factor = 10f64.powi(precision);
                Ok(Value::Property(PropertyValue::Float((v * factor).round() / factor)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Integer(f.round() as i64))),
                    Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(*i))),
                    _ => Err(ExecutionError::TypeError("round() requires numeric".to_string())),
                }
            }
        }
        "sqrt" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(f.sqrt()))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Float((*i as f64).sqrt()))),
                _ => Err(ExecutionError::TypeError("sqrt() requires numeric".to_string())),
            }
        }
        "sign" => {
            match &args[0] {
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(i.signum()))),
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Integer(if *f > 0.0 { 1 } else if *f < 0.0 { -1 } else { 0 }))),
                _ => Err(ExecutionError::TypeError("sign() requires numeric".to_string())),
            }
        }
        "log" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(f.ln()))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Float((*i as f64).ln()))),
                _ => Err(ExecutionError::TypeError("log() requires numeric".to_string())),
            }
        }
        "exp" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(f.exp()))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Float((*i as f64).exp()))),
                _ => Err(ExecutionError::TypeError("exp() requires numeric".to_string())),
            }
        }
        "rand" => {
            use rand::Rng;
            let val = rand::thread_rng().gen::<f64>();
            Ok(Value::Property(PropertyValue::Float(val)))
        }
        "timestamp" => {
            let ts = statement_clock::now().timestamp_millis();
            Ok(Value::Property(PropertyValue::Integer(ts)))
        }
        // Type/meta functions
        "coalesce" => {
            for arg in args {
                if !matches!(arg, Value::Null | Value::Property(PropertyValue::Null)) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "head" => {
            match &args[0] {
                Value::Property(p) if p.as_list_items().is_some() => Ok(p
                    .as_list_items()
                    .unwrap()
                    .first()
                    .map(|v| Value::Property(v.clone()))
                    .unwrap_or(Value::Null)),
                // A list that holds entities is a Value::List, not a
                // PropertyValue list — see eval_index.
                Value::List(items) => Ok(items.first().cloned().unwrap_or(Value::Null)),
                _ => Err(ExecutionError::TypeError("head() requires list".to_string())),
            }
        }
        "last" => {
            match &args[0] {
                Value::Property(p) if p.as_list_items().is_some() => Ok(p
                    .as_list_items()
                    .unwrap()
                    .last()
                    .map(|v| Value::Property(v.clone()))
                    .unwrap_or(Value::Null)),
                Value::List(items) => Ok(items.last().cloned().unwrap_or(Value::Null)),
                _ => Err(ExecutionError::TypeError("last() requires list".to_string())),
            }
        }
        "tail" => {
            match &args[0] {
                Value::Property(PropertyValue::Array(arr)) => {
                    let tail: Vec<PropertyValue> = arr.iter().skip(1).cloned().collect();
                    Ok(Value::Property(PropertyValue::Array(tail)))
                }
                Value::List(items) => {
                    Ok(Value::List(items.iter().skip(1).cloned().collect()))
                }
                _ => Err(ExecutionError::TypeError("tail() requires list".to_string())),
            }
        }
        // Meta functions — work on nodes/edges
        "id" => {
            match &args[0] {
                Value::NodeRef(id) | Value::Node(id, _) => Ok(Value::Property(PropertyValue::Integer(id.as_u64() as i64))),
                Value::EdgeRef(id, ..) | Value::Edge(id, _) => Ok(Value::Property(PropertyValue::Integer(id.as_u64() as i64))),
                _ => Err(ExecutionError::TypeError("id() requires node or edge".to_string())),
            }
        }
        // `n:A:B` as a value, produced by the parser's postfix label check.
        // True when the node carries *every* named label; null when the
        // subject is null, following Cypher's three-valued logic. On a
        // relationship the same syntax is a type test -- see below.
        "haslabels" => {
            let wanted: Vec<String> = match &args[1] {
                Value::Property(PropertyValue::Array(items)) => items
                    .iter()
                    .filter_map(|v| match v {
                        PropertyValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect(),
                _ => return Err(ExecutionError::TypeError("hasLabels expects a label list".into())),
            };
            // On a relationship, `r:T` is a **type** test, and Cypher allows
            // it: `MATCH ()-[r]->() RETURN r:T2` asks whether this
            // relationship is a T2. We raised a TypeError and killed the query
            // (#914).
            //
            // A relationship has exactly one type, so a multi-label test is
            // false rather than an error -- `r:A:B` asks for something no
            // relationship can be, which is a question with an answer.
            let edge_type = match &args[0] {
                Value::Edge(_, e) => Some(e.edge_type.as_str().to_string()),
                Value::EdgeRef(_, _, _, ty) => Some(ty.as_str().to_string()),
                _ => None,
            };
            if let Some(ty) = edge_type {
                let matches = wanted.len() == 1 && wanted[0] == ty;
                return Ok(Value::Property(PropertyValue::Boolean(matches)));
            }
            let node = match &args[0] {
                Value::Node(_, n) => Some((**n).clone()),
                Value::NodeRef(id) => {
                    let s = store.ok_or_else(|| {
                        ExecutionError::RuntimeError("hasLabels on NodeRef requires store".into())
                    })?;
                    s.get_node(*id).cloned()
                }
                Value::Property(PropertyValue::Null) => {
                    return Ok(Value::Property(PropertyValue::Null))
                }
                _ => {
                    return Err(ExecutionError::TypeError(
                        "a label test requires a node or a relationship".to_string(),
                    ))
                }
            };
            let Some(node) = node else {
                return Ok(Value::Property(PropertyValue::Null));
            };
            let has_all = wanted
                .iter()
                .all(|w| node.labels.iter().any(|l| l.as_str() == w));
            Ok(Value::Property(PropertyValue::Boolean(has_all)))
        }
        "labels" => {
            match &args[0] {
                Value::Node(_, node) => {
                    Ok(Value::Property(PropertyValue::Array(sorted_labels(node))))
                }
                Value::NodeRef(id) => {
                    let s = store.ok_or_else(|| ExecutionError::RuntimeError("labels() on NodeRef requires store".to_string()))?;
                    let node = s.get_node(*id).ok_or_else(|| ExecutionError::RuntimeError(format!("Node {} not found", id.as_u64())))?;
                    Ok(Value::Property(PropertyValue::Array(sorted_labels(node))))
                }
                _ => Err(ExecutionError::TypeError("labels() requires a node".to_string())),
            }
        }
        "type" => {
            match &args[0] {
                Value::Edge(_, edge) => {
                    Ok(Value::Property(PropertyValue::String(edge.edge_type.as_str().to_string())))
                }
                Value::EdgeRef(_, _, _, et) => {
                    Ok(Value::Property(PropertyValue::String(et.as_str().to_string())))
                }
                _ => Err(ExecutionError::TypeError("type() requires an edge".to_string())),
            }
        }
        // Sorted, because the underlying maps are `HashMap`s and Rust seeds
        // their hasher randomly *per process* -- so this returned a different
        // order on every run of the same query over the same data. Cypher does
        // not specify an order, which makes any particular one conformant; it
        // does not make a *different one each time* acceptable. A result that
        // cannot be diffed, cached or compared across versions undermines
        // Axiom 3 at the level of the answer rather than the timing (#577).
        "keys" => {
            match &args[0] {
                Value::Node(id, node) => {
                    // Properties live in row storage *and* in the columnar store, and a
                    // snapshot import populates only the latter -- so reading
                    // `node.properties` alone reported an imported node as having no
                    // properties at all, even while `n.name` returned a value (#333).
                    let keys: Vec<PropertyValue> = match store {
                        Some(s) => s
                            .node_properties_full(*id)
                            .keys()
                            .map(|k| PropertyValue::String(k.clone()))
                            .collect(),
                        None => node
                            .properties
                            .keys()
                            .map(|k| PropertyValue::String(k.clone()))
                            .collect(),
                    };
                    Ok(Value::Property(PropertyValue::Array(sorted_keys(keys))))
                }
                Value::NodeRef(id) => {
                    let s = store.ok_or_else(|| ExecutionError::RuntimeError("keys() on NodeRef requires store".to_string()))?;
                    if s.get_node(*id).is_none() {
                        return Err(ExecutionError::RuntimeError(format!("Node {} not found", id.as_u64())));
                    }
                    let keys: Vec<PropertyValue> = s
                        .node_properties_full(*id)
                        .keys()
                        .map(|k| PropertyValue::String(k.clone()))
                        .collect();
                    Ok(Value::Property(PropertyValue::Array(sorted_keys(keys))))
                }
                Value::Edge(_, edge) => {
                    let keys: Vec<PropertyValue> = edge.properties.keys()
                        .map(|k| PropertyValue::String(k.clone()))
                        .collect();
                    Ok(Value::Property(PropertyValue::Array(sorted_keys(keys))))
                }
                Value::EdgeRef(eid, _, _, _) => {
                    let s = store.ok_or_else(|| ExecutionError::RuntimeError("keys() on EdgeRef requires store".to_string()))?;
                    let edge = s.get_edge(*eid).ok_or_else(|| ExecutionError::RuntimeError(format!("Edge {} not found", eid.as_u64())))?;
                    let keys: Vec<PropertyValue> = edge.properties.keys()
                        .map(|k| PropertyValue::String(k.clone()))
                        .collect();
                    Ok(Value::Property(PropertyValue::Array(sorted_keys(keys))))
                }
                // keys() over a map property. Cypher defines keys() on maps as
                // well as nodes and edges, and without this a map property can
                // be stored and read whole but never enumerated (#452).
                Value::Property(PropertyValue::Map(m)) => {
                    let keys: Vec<PropertyValue> = m
                        .keys()
                        .map(|k| PropertyValue::String(k.clone()))
                        .collect();
                    Ok(Value::Property(PropertyValue::Array(sorted_keys(keys))))
                }
                _ => Err(ExecutionError::TypeError("keys() requires a node, edge, or map".to_string())),
            }
        }
        "exists" => {
            let is_null = matches!(&args[0], Value::Null | Value::Property(PropertyValue::Null));
            Ok(Value::Property(PropertyValue::Boolean(!is_null)))
        }
        // startNode/endNode — return source/target node of an edge
        "startnode" => {
            match &args[0] {
                Value::Edge(_, edge) => Ok(Value::NodeRef(edge.source)),
                Value::EdgeRef(_, src, _, _) => Ok(Value::NodeRef(*src)),
                _ => Err(ExecutionError::TypeError("startNode() requires an edge".to_string())),
            }
        }
        "endnode" => {
            match &args[0] {
                Value::Edge(_, edge) => Ok(Value::NodeRef(edge.target)),
                Value::EdgeRef(_, _, tgt, _) => Ok(Value::NodeRef(*tgt)),
                _ => Err(ExecutionError::TypeError("endNode() requires an edge".to_string())),
            }
        }
        // range() — generate integer list
        "range" => {
            if args.len() < 2 { return Err(ExecutionError::RuntimeError("range() requires at least 2 arguments".to_string())); }
            let start = extract_int(&args[0])?;
            let end = extract_int(&args[1])?;
            let step = if args.len() >= 3 { extract_int(&args[2])? } else { 1 };
            if step == 0 { return Err(ExecutionError::RuntimeError("range() step cannot be 0".to_string())); }
            let mut result = Vec::new();
            let mut i = start;
            if step > 0 {
                while i <= end {
                    result.push(PropertyValue::Integer(i));
                    i += step;
                }
            } else {
                while i >= end {
                    result.push(PropertyValue::Integer(i));
                    i += step;
                }
            }
            Ok(Value::Property(PropertyValue::Array(result)))
        }
        // date/datetime/duration constructors
        // The five temporal constructors (#689). Each produces its own type
        // now; before this they all produced `PropertyValue::DateTime(millis)`,
        // so `date()`, `time()` and `localdatetime()` were indistinguishable
        // once evaluated and nanoseconds were destroyed at construction.
        "date" => {
            use crate::query::executor::temporal as tmp;
            if args.is_empty() {
                let days = (statement_clock::now().timestamp() / 86_400) as i32;
                return Ok(Value::Property(PropertyValue::Date(days)));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => {
                    Ok(Value::Property(tmp::parse_date(s)?))
                }
                Value::Map(_) | Value::Property(PropertyValue::Map(_)) => {
                    let map = &temporal_arg_map(&args[0]).expect("matched a map arm");
                    // Selection: `date({date: d})` takes the date part of
                    // another temporal — and any *other* component in the map
                    // then overrides part of it. `{date: other, day: 28}` keeps
                    // the year and month and replaces only the day.
                    //
                    // This returned the selected date unchanged, so every
                    // override was silently discarded (#802). The composite
                    // constructors already layer overrides this way; `date()`
                    // has its own path and did not.
                    if let Some(src) = map.get("date").or_else(|| map.get("datetime")) {
                        if let Some(base) = date_part_of(src) {
                            return Ok(Value::Property(PropertyValue::Date(
                                apply_date_overrides(base, map)?,
                            )));
                        }
                    }
                    Ok(Value::Property(PropertyValue::Date(tmp::date_days(map)?)))
                }
                Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                Value::Property(PropertyValue::Date(d)) => {
                    Ok(Value::Property(PropertyValue::Date(*d)))
                }
                other => match value_as_property(other).and_then(|p| date_part_of(&p)) {
                    Some(d) => Ok(Value::Property(PropertyValue::Date(d))),
                    None => Err(ExecutionError::TypeError(
                        "date() requires a string, a map, or a temporal value".to_string(),
                    )),
                },
            }
        }
        "localtime" => {
            use crate::query::executor::temporal as tmp;
            if args.is_empty() {
                let now = statement_clock::now();
                let nanos = now.timestamp().rem_euclid(86_400) * 1_000_000_000
                    + now.timestamp_subsec_nanos() as i64;
                return Ok(Value::Property(PropertyValue::LocalTime(nanos)));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => {
                    let (nanos, _) = tmp::parse_time_parts(s)?;
                    Ok(Value::Property(PropertyValue::LocalTime(nanos)))
                }
                Value::Map(_) | Value::Property(PropertyValue::Map(_)) => {
                    let map = &temporal_arg_map(&args[0]).expect("matched a map arm");
                    // A selected time, with any clock components in the map
                    // layered on top — the same rule `date()` needs (#802).
                    if let Some(src) = map.get("time").or_else(|| map.get("datetime")) {
                        if let Some(n) = time_part_of(src) {
                            return Ok(Value::Property(PropertyValue::LocalTime(
                                apply_time_overrides(n, map),
                            )));
                        }
                    }
                    Ok(Value::Property(PropertyValue::LocalTime(tmp::time_of_day_nanos(map)?)))
                }
                Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                other => match value_as_property(other).and_then(|p| time_part_of(&p)) {
                    Some(n) => Ok(Value::Property(PropertyValue::LocalTime(n))),
                    None => Err(ExecutionError::TypeError(
                        "localtime() requires a string, a map, or a temporal value".to_string(),
                    )),
                },
            }
        }
        "time" => {
            use crate::query::executor::temporal as tmp;
            if args.is_empty() {
                let now = statement_clock::now();
                let nanos = now.timestamp().rem_euclid(86_400) * 1_000_000_000
                    + now.timestamp_subsec_nanos() as i64;
                return Ok(Value::Property(PropertyValue::Time { nanos, offset_seconds: 0 }));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => {
                    let (nanos, off) = tmp::parse_time_parts(s)?;
                    Ok(Value::Property(PropertyValue::Time {
                        nanos,
                        offset_seconds: off.unwrap_or(0),
                    }))
                }
                Value::Map(_) | Value::Property(PropertyValue::Map(_)) => {
                    let map = &temporal_arg_map(&args[0]).expect("matched a map arm");
                    let named = match map.get("timezone").and_then(|v| v.as_string()) {
                        Some(tz) => Some(tmp::parse_timezone(&tz)?.0),
                        None => None,
                    };
                    if let Some(src) = map.get("time").or_else(|| map.get("datetime")) {
                        if let Some(n) = time_part_of(src) {
                            // Selecting a time from a zoned value **inherits its
                            // offset**. Defaulting to 0 relabelled 12:00+01:00 as
                            // 12:00Z -- a different instant, rendered as a
                            // perfectly good time (#838).
                            let src_offset = offset_seconds_of(src);
                            let from = src_offset.unwrap_or(0);
                            // A `timezone` given alongside a **zoned** source
                            // converts the instant rather than renaming the
                            // offset: `{time: <12:00+01:00>, timezone: '+05:00'}`
                            // is 16:00+05:00, the same moment read elsewhere.
                            // Overrides apply after the conversion, so
                            // `second: 42` on that gives 16:00:42+05:00.
                            //
                            // A **local** source has no instant to convert
                            // from, so the timezone labels its clock and leaves
                            // it where it is:
                            // `{time: localtime('12:31'), timezone: '+05:00'}`
                            // is 12:31+05:00, not 16:31. Converting
                            // unconditionally gets the zoned rows right and
                            // these four wrong -- the same asymmetry as #821,
                            // one constructor over.
                            let to = named.unwrap_or(from);
                            let shifted = if src_offset.is_some() {
                                (n + (to - from) as i64 * 1_000_000_000)
                                    .rem_euclid(86_400 * 1_000_000_000)
                            } else {
                                n
                            };
                            return Ok(Value::Property(PropertyValue::Time {
                                nanos: apply_time_overrides(shifted, map),
                                offset_seconds: to,
                            }));
                        }
                    }
                    // Built from components, `timezone` names the offset the
                    // clock is *in*; there is no source instant to convert.
                    Ok(Value::Property(PropertyValue::Time {
                        nanos: tmp::time_of_day_nanos(map)?,
                        offset_seconds: named.unwrap_or(0),
                    }))
                }
                Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                other => match value_as_property(other) {
                    // `time(<zoned value>)` keeps the value's own offset (#838).
                    Some(p) => match time_part_of(&p) {
                        Some(n) => Ok(Value::Property(PropertyValue::Time {
                            nanos: n,
                            offset_seconds: offset_seconds_of(&p).unwrap_or(0),
                        })),
                        None => Err(ExecutionError::TypeError(
                            "time() requires a string, a map, or a temporal value".to_string(),
                        )),
                    },
                    None => Err(ExecutionError::TypeError(
                        "time() requires a string, a map, or a temporal value".to_string(),
                    )),
                },
            }
        }
        "localdatetime" => {
            if args.is_empty() {
                let now = statement_clock::now();
                return Ok(Value::Property(PropertyValue::LocalDateTime {
                    secs: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos(),
                }));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => {
                    let (secs, nanos) = parse_naive_date_time(s)?;
                    Ok(Value::Property(PropertyValue::LocalDateTime { secs, nanos }))
                }
                Value::Map(_) | Value::Property(PropertyValue::Map(_)) => {
                    let map = &temporal_arg_map(&args[0]).expect("matched a map arm");
                    crate::query::executor::temporal::reject_unknown_map(map)?;
                    let (d, t) = compose_date_and_time(map)?;
                    let (secs, nanos) = day_and_nanos_to_secs(d, t);
                    Ok(Value::Property(PropertyValue::LocalDateTime { secs, nanos }))
                }
                Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                // A bare temporal value: take its date and time parts.
                // `date()` and `time()` already accepted this; the composite
                // constructors did not, so `localdatetime(other)` was a type
                // error for every `other` the TCK hands it (#772).
                Value::Property(p) if date_part_of(p).is_some() => {
                    let d = date_part_of(p).unwrap_or(0);
                    let t = time_part_of(p).unwrap_or(0);
                    let (secs, nanos) = day_and_nanos_to_secs(d, t);
                    Ok(Value::Property(PropertyValue::LocalDateTime { secs, nanos }))
                }
                _ => Err(ExecutionError::TypeError(
                    "localdatetime() requires a string, a map, or a temporal value".to_string(),
                )),
            }
        }
        "datetime" => {
            use crate::query::executor::temporal as tmp;
            if args.is_empty() {
                let now = statement_clock::now();
                return Ok(Value::Property(PropertyValue::ZonedDateTime {
                    secs: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos(),
                    offset_seconds: 0,
                    zone: None,
                }));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => {
                    use crate::query::executor::temporal as tmp;
                    // `2015-07-21T21:40:32.142+02:00[Europe/Stockholm]` carries
                    // an offset *and* a zone, and they are not redundant: the
                    // offset is what the value had when written, the zone is
                    // the rule it follows. Either may also appear alone.
                    let (body, zone_name) = tmp::split_zone_suffix(s);
                    let (clock, explicit_offset) = match body.rsplit_once('T') {
                        Some(_) | None => {
                            // Reuse the offset splitter, which knows not to
                            // mistake a date dash for an offset sign.
                            let (c, o) = tmp::parse_datetime_offset(body)?;
                            (c, o)
                        }
                    };
                    let (secs_naive, nanos) = parse_naive_date_time(clock)?;
                    let local_days = secs_naive.div_euclid(86_400);
                    let local_nanos = secs_naive.rem_euclid(86_400) * 1_000_000_000 + nanos as i64;

                    let (offset_seconds, zone) = match zone_name {
                        Some(z) => {
                            let spec = tmp::parse_timezone_spec(z)?;
                            // A zone suffix wins over a written offset for the
                            // *rule*, but the written offset is authoritative
                            // for the instant it recorded -- keep it when given.
                            let off = explicit_offset
                                .map(Ok)
                                .unwrap_or_else(|| tmp::resolve_offset(&spec, local_days, local_nanos))?;
                            (off, tmp::zone_name(&spec))
                        }
                        None => (explicit_offset.unwrap_or(0), None),
                    };
                    let utc = secs_naive - offset_seconds as i64;
                    Ok(Value::Property(PropertyValue::ZonedDateTime {
                        secs: utc,
                        nanos,
                        offset_seconds,
                        zone,
                    }))
                }
                Value::Map(_) | Value::Property(PropertyValue::Map(_)) => {
                    let map = &temporal_arg_map(&args[0]).expect("matched a map arm");
                    crate::query::executor::temporal::reject_unknown_map(map)?;
                    // An epoch is a complete specification on its own, and is
                    // handled first: without this the map fell through to the
                    // component defaults and returned 1970-01-01 *silently*,
                    // which reads as a plausible date rather than a failure
                    // (#595).
                    if let Some(millis) = map.get("epochMillis").and_then(|v| v.as_integer()) {
                        return Ok(Value::Property(PropertyValue::ZonedDateTime {
                            secs: millis.div_euclid(1000),
                            nanos: (millis.rem_euclid(1000) * 1_000_000) as u32,
                            offset_seconds: 0,
                            zone: None,
                        }));
                    }
                    if let Some(secs) = map.get("epochSeconds").and_then(|v| v.as_integer()) {
                        return Ok(Value::Property(PropertyValue::ZonedDateTime {
                            secs,
                            nanos: 0,
                            offset_seconds: 0,
                            zone: None,
                        }));
                    }
                    let spec = match map.get("timezone").and_then(|v| v.as_string()) {
                        Some(tz) => Some(tmp::parse_timezone_spec(&tz)?),
                        None => None,
                    };
                    let (d, t) = compose_date_and_time(map)?;
                    // Seconds, not nanoseconds: the nanosecond product spans
                    // only ±292 years from 1970 and wraps for anything outside
                    // 1678..2262 (#814).
                    let (local_secs, local_nanos) = day_and_nanos_to_secs(d, t);
                    // A named zone has no single offset -- Europe/Stockholm is
                    // +01:00 in October and +02:00 in July -- so it is resolved
                    // against *this* local date, not at parse time (#767).
                    let (offset_seconds, zone) = match &spec {
                        Some(sp) => (
                            tmp::resolve_offset(sp, d as i64, t)?,
                            tmp::zone_name(sp),
                        ),
                        // No target zone given: a **zoned source keeps its
                        // own**. Defaulting to UTC re-labelled the value —
                        // `datetime({datetime: s})` turned `12:00+02:00` into
                        // `12:00Z`, the same wall clock two hours earlier.
                        // Found by a test written for the re-zoning case, which
                        // is the neighbouring behaviour (#809).
                        None => match map.get("datetime").or_else(|| map.get("time")) {
                            Some(PropertyValue::ZonedDateTime { offset_seconds, zone, .. }) => {
                                (*offset_seconds, zone.clone())
                            }
                            Some(PropertyValue::Time { offset_seconds, .. }) => (*offset_seconds, None),
                            _ => (0, None),
                        },
                    };
                    // Selecting a **zoned** source into a different zone
                    // converts the instant rather than copying the wall clock:
                    // 12:00 in Europe/Stockholm (+01:00) is 11:00 UTC, which is
                    // 01:00 in Pacific/Honolulu (-10:00) — not 12:00.
                    //
                    // The components are read as local time in the *target*
                    // zone only when the source had no zone of its own. With a
                    // zoned source the same reading is an instant, and treating
                    // it as local time shifts every value by the difference
                    // between the two offsets (#809).
                    let source_offset = map
                        .get("datetime")
                        .or_else(|| map.get("time"))
                        .and_then(|v| match v {
                            PropertyValue::ZonedDateTime { offset_seconds, .. }
                            | PropertyValue::Time { offset_seconds, .. } => Some(*offset_seconds),
                            _ => None,
                        });
                    // The components describe local time; store the instant.
                    let utc_secs = match source_offset {
                        // A re-zoning: the reading is already the source's wall
                        // clock, so undo *its* offset, not the target's.
                        Some(src) if spec.is_some() => local_secs - src as i64,
                        _ => local_secs - offset_seconds as i64,
                    };
                    Ok(Value::Property(PropertyValue::ZonedDateTime {
                        secs: utc_secs,
                        nanos: local_nanos,
                        offset_seconds,
                        zone,
                    }))
                }
                Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                // A bare temporal value. A value with no zone of its own is
                // read as UTC, which is what Cypher specifies for widening a
                // local value into a zoned one.
                Value::Property(p) if date_part_of(p).is_some() => {
                    let off = match p {
                        PropertyValue::ZonedDateTime { offset_seconds, .. } => *offset_seconds,
                        _ => 0,
                    };
                    let zone = match p {
                        PropertyValue::ZonedDateTime { zone, .. } => zone.clone(),
                        _ => None,
                    };
                    let d = date_part_of(p).unwrap_or(0);
                    let t = time_part_of(p).unwrap_or(0);
                    let (local_secs, nanos) = day_and_nanos_to_secs(d, t);
                    Ok(Value::Property(PropertyValue::ZonedDateTime {
                        secs: local_secs - off as i64,
                        nanos,
                        offset_seconds: off,
                        zone,
                    }))
                }
                _ => Err(ExecutionError::TypeError(
                    "datetime() requires a string, a map, or a temporal value".to_string(),
                )),
            }
        }
        "duration" => {
            if args.is_empty() {
                return Err(ExecutionError::RuntimeError("duration() requires an argument".to_string()));
            }
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => {
                    parse_iso_duration(s)
                }
                Value::Property(PropertyValue::Map(map)) => {
                    // Every component may be **fractional**, and the fraction
                    // carries into the next smaller unit (#829):
                    //
                    //     duration({months: 0.75})   ->  P22DT19H51M49.5S
                    //     duration({weeks: 2.5})     ->  P17DT12H
                    //     duration({months: 5, days: 1.5})  ->  P5M1DT12H
                    //
                    // `as_integer()` returns `None` for a float, so each of
                    // these silently became zero: `duration({months: 0.75})`
                    // was `PT0S`, a well-formed duration nothing downstream
                    // could question. (#787 was the same shape, one component
                    // over.)
                    let num = |key: &str| -> f64 {
                        map.get(key)
                            .and_then(|v| v.as_integer().map(|i| i as f64).or_else(|| v.as_float()))
                            .unwrap_or(0.0)
                    };
                    // A mean Gregorian month is 365.2425/12 days, which is
                    // **exactly 2,629,746 seconds**. Carrying in seconds rather
                    // than in days keeps the arithmetic on integers-as-floats:
                    // 0.75 months is 1,972,309.5s exactly, where 0.75 x
                    // 30.436875 days is not exactly representable and lands a
                    // hundred nanoseconds short of the expected 49.5S.
                    const SECS_PER_MEAN_MONTH: f64 = 2_629_746.0;

                    let months_f = num("years") * 12.0 + num("months");
                    let months = months_f.trunc();
                    // A month's fraction becomes whole days first, then time --
                    // 0.75 months is 22 days *and* 19:51:49.5, not 22.83 days.
                    let month_carry_secs = (months_f - months) * SECS_PER_MEAN_MONTH;
                    let carry_days = (month_carry_secs / 86_400.0).trunc();
                    let month_rem_secs = month_carry_secs - carry_days * 86_400.0;

                    let days_f = num("days") + num("weeks") * 7.0 + carry_days;
                    let days = days_f.trunc();

                    let secs_f = num("hours") * 3600.0
                        + num("minutes") * 60.0
                        + num("seconds")
                        + (days_f - days) * 86_400.0
                        + month_rem_secs;
                    let secs = secs_f.trunc();

                    // Sub-second components are additive with each other and
                    // with whatever fell out of the seconds (#787).
                    let nanos_f = num("milliseconds") * 1_000_000.0
                        + num("microseconds") * 1_000.0
                        + num("nanoseconds")
                        + (secs_f - secs) * 1_000_000_000.0;
                    // Ties to even: `.round()` goes half away from zero, which
                    // biases an exact `...000.5` upward every time.
                    let nanos_total = nanos_f.round_ties_even() as i64;

                    // Combine seconds and nanoseconds into one total **before**
                    // splitting, then split truncating toward zero. Neither
                    // split alone is right, and each looks right on the cases
                    // the other gets wrong:
                    //
                    //   {seconds: 2, milliseconds: -1}  is PT1.999S
                    //     truncating in place: (2s, -1ms)   mixed signs
                    //     Euclidean:           (1s, +999ms) correct
                    //
                    //   {nanoseconds: -1}               is PT-0.000000001S
                    //     truncating in place: (0s, -1ns)   correct
                    //     Euclidean:           (-1s, +999999999ns) mixed signs
                    //
                    // The invariant (#806) is that the components share the
                    // sign of the **total**, so the total is what has to be
                    // formed first. i128 because seconds can be large and the
                    // nanosecond product overflows i64 past ~292 years (#814).
                    let total_nanos = secs as i128 * 1_000_000_000 + nanos_total as i128;
                    Ok(Value::Property(PropertyValue::Duration {
                        months: months as i64,
                        days: days as i64,
                        seconds: (total_nanos / 1_000_000_000) as i64,
                        nanos: (total_nanos % 1_000_000_000) as i32,
                    }))
                }
                _ => Err(ExecutionError::TypeError("duration() requires string or map argument".to_string())),
            }
        }
        // duration component accessors
        // `<type>.truncate(unit, value, map)` for all five namespaces (#769).
        // The namespace names the *result* type, so `date.truncate` over a
        // datetime returns a Date.
        // `datetime.fromepoch(seconds, nanoseconds)` and
        // `datetime.fromepochmillis(millis)`. Both name an instant in UTC, so
        // the result is a `ZonedDateTime` at offset 0 with no zone id --
        // an epoch has no locality to attach.
        //
        // Nanoseconds are carried whole rather than folded into the seconds:
        // `datetime.fromepoch(416779, 999999999)` is
        // 1970-01-05T19:46:19.999999999Z, which no millisecond-based
        // representation can express (#1003).
        "datetime.fromepoch" | "datetime.fromepochmillis" => {
            let want = if lowered == "datetime.fromepoch" { 2 } else { 1 };
            if args.len() != want {
                return Err(ExecutionError::RuntimeError(format!(
                    "{name}() requires {want} argument(s)"
                )));
            }
            let (secs, nanos) = if want == 2 {
                (extract_int(&args[0])?, extract_int(&args[1])?)
            } else {
                let millis = extract_int(&args[0])?;
                // `div_euclid`, not `/`: a negative epoch millisecond is a real
                // instant before 1970, and truncating toward zero would place
                // it in the wrong second with a negative nanosecond remainder.
                (millis.div_euclid(1_000), millis.rem_euclid(1_000) * 1_000_000)
            };
            if !(0..1_000_000_000).contains(&nanos) {
                return Err(ExecutionError::RuntimeError(
                    "nanoseconds must be in 0..1000000000".to_string(),
                ));
            }
            Ok(Value::Property(PropertyValue::ZonedDateTime {
                secs,
                nanos: nanos as u32,
                offset_seconds: 0,
                zone: None,
            }))
        }
        "date.truncate" | "time.truncate" | "localtime.truncate"
        | "localdatetime.truncate" | "datetime.truncate" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(format!("{lowered}() requires a unit and a temporal value")));
            }
            let unit = match &args[0] {
                Value::Property(PropertyValue::String(u)) => u.clone(),
                _ => return Err(ExecutionError::TypeError(
                    "the first argument of truncate() is the unit, as a string".to_string(),
                )),
            };
            let value = match &args[1] {
                Value::Property(PropertyValue::Null) | Value::Null => {
                    return Ok(Value::Property(PropertyValue::Null))
                }
                Value::Property(p) => p.clone(),
                _ => return Err(ExecutionError::TypeError(
                    "truncate() needs a temporal value".to_string(),
                )),
            };
            let empty = std::collections::HashMap::new();
            let overrides = match args.get(2) {
                Some(Value::Property(PropertyValue::Map(m))) => m.clone(),
                _ => empty,
            };
            let target = lowered.split('.').next().unwrap_or("date");
            Ok(Value::Property(crate::query::executor::temporal::truncate(
                target, &unit, &value, &overrides,
            )?))
        }
        // `duration.inSeconds/inDays/inMonths(a, b)` — the difference between
        // two temporals, expressed in one unit. `duration.between` was already
        // implemented; these three were not, and all four were unreachable from
        // Cypher until the grammar learned to parse a dotted name.
        "duration.inseconds" | "duration.indays" | "duration.inmonths" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError(format!("{lowered}() requires 2 arguments")));
            }
            let (a, b) = match (&args[0], &args[1]) {
                (Value::Property(PropertyValue::Null), _) | (_, Value::Property(PropertyValue::Null))
                | (Value::Null, _) | (_, Value::Null) => {
                    return Ok(Value::Property(PropertyValue::Null))
                }
                (Value::Property(a), Value::Property(b)) => (a.clone(), b.clone()),
                _ => return Err(ExecutionError::TypeError(format!("{lowered}() needs two temporal values"))),
            };
            // `duration.inX(lhs, rhs)` is **rhs - lhs**, the same orientation
            // as `duration.between`: the duration you would add to `lhs` to
            // reach `rhs`. This had the operands the other way round, so every
            // result carried the wrong sign -- and half the TCK rows expect a
            // negative answer, so it looked correct on exactly the half that
            // happened to be positive (#775).
            // `inMonths` needs the **calendar** month count, so it uses the
            // calendar difference; `inDays` and `inSeconds` want elapsed time
            // and use the plain one. Dividing an elapsed day count by 30 gave
            // `P11M29D...` where `P1Y` belongs — 12 months' worth of days that
            // never became a year, because 365 / 30 is 12.16 and the truncation
            // lands one month short (#812).
            let diff = if lowered == "duration.inmonths" {
                temporal_difference_calendar(&b, &a)?
            } else {
                temporal_difference(&b, &a)?
            };
            let (months, days, seconds, nanos) = match diff {
                PropertyValue::Duration { months, days, seconds, nanos } => (months, days, seconds, nanos),
                _ => (0, 0, 0, 0),
            };
            Ok(Value::Property(match lowered.as_str() {
                // Each truncates to its own unit and **discards** the
                // remainder, rather than rounding it: `inDays` of 30 hours is
                // one day, not one and a quarter.
                "duration.inseconds" => PropertyValue::Duration {
                    months: 0, days: 0, seconds: days * 86_400 + seconds, nanos,
                },
                "duration.indays" => PropertyValue::Duration {
                    months: 0, days: days + seconds / 86_400, seconds: 0, nanos: 0,
                },
                _ => PropertyValue::Duration {
                    months, days: 0, seconds: 0, nanos: 0,
                },
            }))
        }
        "duration_between" | "duration.between" => {
            if args.len() < 2 { return Err(ExecutionError::RuntimeError("duration.between() requires 2 arguments".to_string())); }
            // Accepts any two temporals, not only the legacy millisecond
            // `DateTime`. It matched that one variant alone, so the moment the
            // constructors started returning real types (#689) every
            // `duration.between(date(...), date(...))` became a type error --
            // and the grammar fix (#769) that finally made this function
            // reachable from Cypher exposed exactly that on 20 scenarios.
            //
            // Argument order is (from, to): `between(a, b)` is b - a.
            match (&args[0], &args[1]) {
                (Value::Property(a), Value::Property(b)) => {
                    Ok(Value::Property(temporal_difference_calendar(b, a).map_err(|_| {
                        ExecutionError::TypeError(
                            "duration.between() requires two temporal arguments".to_string(),
                        )
                    })?))
                }
                _ => Err(ExecutionError::TypeError("duration.between() requires two temporal arguments".to_string())),
            }
        }
        // CY-20: properties() — return all properties as a map
        "properties" => {
            match &args[0] {
                Value::Node(id, node) => {
                    // See keys(): row storage alone is incomplete after a snapshot import.
                    let props = match store {
                        Some(s) => s.node_properties_full(*id),
                        None => node.properties.clone(),
                    };
                    Ok(Value::Property(PropertyValue::Map(props)))
                }
                Value::NodeRef(id) => {
                    let s = store.ok_or_else(|| ExecutionError::RuntimeError("properties() on NodeRef requires store".to_string()))?;
                    if s.get_node(*id).is_none() {
                        return Err(ExecutionError::RuntimeError(format!("Node {} not found", id.as_u64())));
                    }
                    Ok(Value::Property(PropertyValue::Map(s.node_properties_full(*id))))
                }
                Value::Edge(_, edge) => {
                    Ok(Value::Property(PropertyValue::Map(edge.properties.clone())))
                }
                Value::EdgeRef(eid, _, _, _) => {
                    let s = store.ok_or_else(|| ExecutionError::RuntimeError("properties() on EdgeRef requires store".to_string()))?;
                    let edge = s.get_edge(*eid).ok_or_else(|| ExecutionError::RuntimeError(format!("Edge {} not found", eid.as_u64())))?;
                    Ok(Value::Property(PropertyValue::Map(edge.properties.clone())))
                }
                Value::Property(PropertyValue::Map(m)) => Ok(Value::Property(PropertyValue::Map(m.clone()))),
                Value::Null => Ok(Value::Null),
                _ => Err(ExecutionError::TypeError("properties() requires a node, edge, or map".to_string())),
            }
        }
        // CY-21: isEmpty() — check if collection/string/map is empty
        "isempty" => {
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => Ok(Value::Property(PropertyValue::Boolean(s.is_empty()))),
                Value::Property(PropertyValue::Array(a)) => Ok(Value::Property(PropertyValue::Boolean(a.is_empty()))),
                Value::Property(PropertyValue::Map(m)) => Ok(Value::Property(PropertyValue::Boolean(m.is_empty()))),
                Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
                _ => Err(ExecutionError::TypeError("isEmpty() requires a string, list, or map".to_string())),
            }
        }
        // CY-18: percentileCont / percentileDisc
        "percentilecont" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError("percentileCont requires 2 arguments".to_string()));
            }
            // This is an aggregation function — handled in GroupByOperator, not here.
            // Scalar fallback for single-value case:
            Ok(args[0].clone())
        }
        "percentiledisc" => {
            if args.len() < 2 {
                return Err(ExecutionError::RuntimeError("percentileDisc requires 2 arguments".to_string()));
            }
            Ok(args[0].clone())
        }
        // CY-19: stDev / stDevP
        "stdev" | "stdevp" => {
            // Aggregation function — handled in GroupByOperator.
            // Scalar fallback:
            Ok(Value::Property(PropertyValue::Float(0.0)))
        }
        // CY-17: Trigonometric functions
        "sin" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.sin()))) }
        "cos" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.cos()))) }
        "tan" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.tan()))) }
        "cot" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(1.0 / v.tan()))) }
        "asin" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.asin()))) }
        "acos" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.acos()))) }
        "atan" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.atan()))) }
        "atan2" => {
            let y = extract_float(&args[0])?;
            let x = extract_float(&args[1])?;
            Ok(Value::Property(PropertyValue::Float(y.atan2(x))))
        }
        "sinh" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.sinh()))) }
        "cosh" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.cosh()))) }
        "tanh" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.tanh()))) }
        "degrees" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.to_degrees()))) }
        "radians" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.to_radians()))) }
        "pi" => Ok(Value::Property(PropertyValue::Float(std::f64::consts::PI))),
        "haversin" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float((1.0 - v.cos()) / 2.0))) }
        // CY-22: Math constants & minor functions
        "e" => Ok(Value::Property(PropertyValue::Float(std::f64::consts::E))),
        "log10" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Float(v.log10()))) }
        "isnan" => { let v = extract_float(&args[0])?; Ok(Value::Property(PropertyValue::Boolean(v.is_nan()))) }
        // CY-24: elementId()
        "elementid" => {
            match &args[0] {
                Value::NodeRef(id) | Value::Node(id, _) => Ok(Value::Property(PropertyValue::String(format!("node:{}", id.as_u64())))),
                Value::EdgeRef(id, ..) | Value::Edge(id, _) => Ok(Value::Property(PropertyValue::String(format!("edge:{}", id.as_u64())))),
                _ => Err(ExecutionError::TypeError("elementId() requires a node or edge".to_string())),
            }
        }
        // CY-25: randomUUID()
        "randomuuid" => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
            let seed = t.as_nanos();
            // Simple v4-like UUID (not cryptographically secure, but unique enough)
            let uuid = format!("{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
                (seed & 0xFFFFFFFF) as u32,
                ((seed >> 32) & 0xFFFF) as u16,
                ((seed >> 48) & 0x0FFF) as u16,
                (0x8000 | ((seed >> 60) & 0x3FFF)) as u16,
                (seed >> 76) as u64 ^ (seed & 0xFFFFFFFFFFFF) as u64,
            );
            Ok(Value::Property(PropertyValue::String(uuid)))
        }
        // CY-26: valueType()
        "valuetype" => {
            let type_name = match &args[0] {
                Value::Property(PropertyValue::Integer(_)) => "INTEGER",
                Value::Property(PropertyValue::Float(_)) => "FLOAT",
                Value::Property(PropertyValue::String(_)) => "STRING",
                Value::Property(PropertyValue::Boolean(_)) => "BOOLEAN",
                Value::Property(PropertyValue::Array(_)) => "LIST",
                Value::Property(PropertyValue::Map(_)) => "MAP",
                Value::Property(PropertyValue::Null) => "NULL",
                Value::NodeRef(_) | Value::Node(_, _) => "NODE",
                Value::EdgeRef(..) | Value::Edge(_, _) => "RELATIONSHIP",
                Value::Path { .. } => "PATH",
                Value::Null => "NULL",
                _ => "ANY",
            };
            Ok(Value::Property(PropertyValue::String(type_name.to_string())))
        }
        // CY-27: toBoolean and OrNull variants
        "toboolean" => {
            match &args[0] {
                Value::Property(PropertyValue::Boolean(b)) => Ok(Value::Property(PropertyValue::Boolean(*b))),
                Value::Property(PropertyValue::String(s)) => match s.to_lowercase().as_str() {
                    "true" => Ok(Value::Property(PropertyValue::Boolean(true))),
                    "false" => Ok(Value::Property(PropertyValue::Boolean(false))),
                    // A string that is not a boolean converts to null, not to
                    // an error: `toBoolean('')` is a *question* about the
                    // string, and "no" is an answer. Erroring killed the whole
                    // query -- `UNWIND [null, '', ' tru '] AS x RETURN
                    // toBoolean(x)` returned nothing at all rather than three
                    // nulls (#907).
                    _ => Ok(Value::Property(PropertyValue::Null)),
                },
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Boolean(*i != 0))),
                Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Null),
                _ => Err(ExecutionError::TypeError("toBoolean() requires a boolean, string, or integer".to_string())),
            }
        }
        "tobooleanornull" => {
            match &args[0] {
                Value::Property(PropertyValue::Boolean(b)) => Ok(Value::Property(PropertyValue::Boolean(*b))),
                Value::Property(PropertyValue::String(s)) => match s.to_lowercase().as_str() {
                    "true" => Ok(Value::Property(PropertyValue::Boolean(true))),
                    "false" => Ok(Value::Property(PropertyValue::Boolean(false))),
                    _ => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
            }
        }
        "tointegerornull" => {
            match &args[0] {
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(*i))),
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Integer(*f as i64))),
                Value::Property(PropertyValue::String(s)) => Ok(s.parse::<i64>().map(|i| Value::Property(PropertyValue::Integer(i))).unwrap_or(Value::Null)),
                _ => Ok(Value::Null),
            }
        }
        "tofloatornull" => {
            match &args[0] {
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(*f))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Float(*i as f64))),
                Value::Property(PropertyValue::String(s)) => Ok(s.parse::<f64>().map(|f| Value::Property(PropertyValue::Float(f))).unwrap_or(Value::Null)),
                _ => Ok(Value::Null),
            }
        }
        "tostringornull" => {
            match &args[0] {
                Value::Property(PropertyValue::String(s)) => Ok(Value::Property(PropertyValue::String(s.clone()))),
                Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::String(i.to_string()))),
                Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::String(f.to_string()))),
                Value::Property(PropertyValue::Boolean(b)) => Ok(Value::Property(PropertyValue::String(b.to_string()))),
                _ => Ok(Value::Null),
            }
        }
        _ => Err(ExecutionError::RuntimeError(format!("Unknown function: {}", name))),
    }
}

/// Helper: extract string from Value
fn extract_string(val: &Value) -> ExecutionResult<String> {
    match val {
        Value::Property(PropertyValue::String(s)) => Ok(s.clone()),
        _ => Err(ExecutionError::TypeError("Expected string argument".to_string())),
    }
}

/// Helper: extract integer from Value
fn extract_int(val: &Value) -> ExecutionResult<i64> {
    match val {
        Value::Property(PropertyValue::Integer(i)) => Ok(*i),
        _ => Err(ExecutionError::TypeError("Expected integer argument".to_string())),
    }
}

/// Helper: extract float from Value (with integer promotion)
fn extract_float(val: &Value) -> ExecutionResult<f64> {
    match val {
        Value::Property(PropertyValue::Float(f)) => Ok(*f),
        Value::Property(PropertyValue::Integer(i)) => Ok(*i as f64),
        _ => Err(ExecutionError::TypeError("Expected numeric argument".to_string())),
    }
}

/// Add duration components to a DateTime (millis timestamp)

/// `duration * number` and `duration / number`.
///
/// Scaling a duration is not scaling three independent numbers. The TCK pins
/// the rule:
///
/// ```text
/// P12Y5M14DT16H13M10.000000001S * 0.5  ->  P6Y2M22DT13H21M8S
/// ```
///
/// 149 months halved is 74.5, and the half **carries into days at 30 days per
/// month** — 14 days becomes 22. Hours do *not* carry into days: the input's
/// 16h doubles to `32H` and stays there, because a day is not always 24 hours
/// once zones are involved and Cypher declines to assume it is.
///
/// So months and the days-and-below part scale separately, with one carry
/// between them and none below.
fn scale_duration(
    months: i64,
    days: i64,
    seconds: i64,
    nanos: i32,
    factor: f64,
) -> Result<PropertyValue, ExecutionError> {
    if !factor.is_finite() {
        return Err(ExecutionError::RuntimeError(
            "cannot scale a duration by a non-finite number".to_string(),
        ));
    }
    // The mean Gregorian month: 365.2425 / 12. **Not 30.**
    //
    // Derived from the TCK rather than assumed. `P12Y5M14DT16H13M10S * 0.5`
    // is `P6Y2M22DT13H21M8S`, and reaching 13:21:08 from a half-month carry
    // requires 30.4369 days per month; 30 gives 08:06:35, which is a
    // plausible-looking wrong answer.
    const DAYS_PER_MONTH: f64 = 365.2425 / 12.0;

    let scaled_months = months as f64 * factor;
    let whole_months = scaled_months.trunc();
    let carry_days = (scaled_months - whole_months) * DAYS_PER_MONTH;

    // Days and the sub-day part scale separately: a day is not always 24 hours
    // once zones are involved, so Cypher does not carry hours into days.
    // Doubling 14D16H gives `28DT32H`, not `29DT8H` -- normalising looks
    // tidier and is wrong.
    let scaled_days = days as f64 * factor + carry_days;
    let whole_days = scaled_days.trunc();
    let day_remainder_nanos = (scaled_days - whole_days) * 86_400.0 * 1e9;

    // The sub-second part in integers where it can be. A float round-trip of
    // (seconds * 1e9 + nanos) loses the last nanosecond at these magnitudes --
    // it rendered `8.000000001S` where the TCK expects `8S`.
    let sub_day_exact = (seconds as i128) * 1_000_000_000 + nanos as i128;
    // Each part is rounded **before** they are added, not after. Adding first
    // then rounding turns 29195000000000.5 + 18873000000000.027 into one extra
    // nanosecond, and the TCK's halving case expects exactly `8S`. Two roundings
    // of well-conditioned quantities beat one rounding of their sum here,
    // because the .5 and the .027 are independent artefacts.
    let scaled_sub = if factor == factor.trunc() && factor.abs() < 1e15 {
        // An integral factor multiplies exactly, with no float involved.
        sub_day_exact * (factor as i128)
    } else {
        // Ties to **even**, not away from zero. `58390000000001 * 0.5` is
        // exactly `...000.5`, and `.round()` takes it up to `...001` where the
        // TCK expects `8S` with no fraction. Half-away-from-zero also biases
        // every exact tie upward, which accumulates; banker's rounding does
        // not, and is what the reference agrees with.
        (sub_day_exact as f64 * factor).round_ties_even() as i128
    };
    let sub_day = scaled_sub + day_remainder_nanos.round_ties_even() as i128;

    Ok(PropertyValue::Duration {
        months: whole_months as i64,
        days: whole_days as i64,
        seconds: (sub_day / 1_000_000_000) as i64,
        nanos: (sub_day % 1_000_000_000) as i32,
    })
}

/// Nanoseconds since the epoch that a temporal value denotes, and a way back.
///
/// `Date` is midnight and `LocalTime`/`Time` have no date, so these are the
/// only two functions that decide what "the same value, shifted" means. Keeping
/// that decision in one place is the point: the shift used to be written inline
/// per operator, which is how `+` and `-` came to disagree about whether months
/// were calendar months.
/// The zone a value carries, if it carries one.
///
/// A `ZonedDateTime` may name a *region* whose offset depends on the moment it
/// is applied to — `Europe/Stockholm` is +02:00 in summer and +01:00 in winter
/// — so the name is kept rather than collapsed to the stored offset. A `Time`
/// only ever carries a fixed offset.
///
/// The legacy `DateTime` is deliberately absent: it exists for snapshot
/// compatibility and is already read as UTC everywhere.
/// The UTC offset a value carries, if it carries one.
///
/// A `LocalTime` or `LocalDateTime` has none; a `Date` has none. Only the two
/// zoned types answer, and the caller decides what "none" means -- inheriting
/// zero and inheriting nothing are different (#838).
fn offset_seconds_of(v: &PropertyValue) -> Option<i32> {
    match v {
        PropertyValue::Time { offset_seconds, .. }
        | PropertyValue::ZonedDateTime { offset_seconds, .. } => Some(*offset_seconds),
        PropertyValue::DateTime(_) => Some(0),
        _ => None,
    }
}

fn zone_of(v: &PropertyValue) -> Option<crate::query::executor::temporal::TzSpec> {
    use crate::query::executor::temporal::{parse_timezone_spec, TzSpec};
    match v {
        PropertyValue::ZonedDateTime { zone, offset_seconds, .. } => Some(
            zone.as_deref()
                .and_then(|z| parse_timezone_spec(z).ok())
                .unwrap_or(TzSpec::Offset(*offset_seconds)),
        ),
        PropertyValue::Time { offset_seconds, .. } => Some(TzSpec::Offset(*offset_seconds)),
        _ => None,
    }
}

/// Both values as instants, with an unzoned value read **in the other's zone**.
///
/// Cypher does not compare a local temporal against a zoned one by treating the
/// local one as UTC. The local one is placed in the zoned one's zone, and only
/// then are the two instants compared:
///
/// ```text
/// duration.between(localdatetime('2015-07-21T21:40:32.142'),
///                  datetime('2015-07-21T21:40:32.142+0100'))   ->  PT0S
/// ```
///
/// Reading the left side as UTC gives `PT1H`. Every wrong answer in this class
/// was off by exactly one offset, which is why it looked like a sign or
/// rounding bug rather than a missing rule.
///
/// Two consequences that are easy to miss:
///
///   * **A value with no date borrows the other's.** `duration.between` of a
///     `time` and a `datetime` compares them on the datetime's day — otherwise
///     the time-of-day sits at the epoch and the answer is decades.
///   * **DST is resolved at the local side's own wall clock**, not the zoned
///     side's. On 2017-10-29 Stockholm falls back at 03:00, so a local
///     midnight is still +02:00 while 04:00 is already +01:00:
///
/// ```text
/// duration.between(localdatetime({year: 2017, month: 10, day: 29, hour: 0}),
///                  datetime({year: 2017, month: 10, day: 29, hour: 4,
///                            timezone: 'Europe/Stockholm'}))    ->  PT5H
/// ```
///
/// `PT5H`, not the `PT4H` the wall clocks suggest. That one hour is the whole
/// reason this cannot be done by subtracting local readings (#821).
///
/// Returns `None` when neither side is zoned, leaving the existing local-only
/// paths untouched.
fn zone_aligned_instants(a: &PropertyValue, b: &PropertyValue) -> Option<(i128, i128)> {
    use crate::query::executor::temporal::resolve_offset;
    let (za, zb) = (zone_of(a), zone_of(b));
    if za.is_none() && zb.is_none() {
        return None;
    }
    let (da, db) = (date_part_of(a), date_part_of(b));
    // A date-less value borrows the other's day. When neither has one — two
    // `time`s — the day is arbitrary and cancels.
    let (day_a, day_b) = (da.or(db).unwrap_or(0), db.or(da).unwrap_or(0));
    // A clock-less value is midnight, which is what a bare `date` means.
    let (ta, tb) = (time_part_of(a).unwrap_or(0), time_part_of(b).unwrap_or(0));
    let instant = |own: &Option<_>, other: &Option<_>, day: i32, nanos_of_day: i64| {
        // Own zone first; the other's only when there is none of one's own.
        let spec: Option<crate::query::executor::temporal::TzSpec> =
            own.clone().or_else(|| other.clone());
        let offset = match &spec {
            Some(s) => resolve_offset(s, day as i64, nanos_of_day).ok()?,
            None => 0,
        };
        // Seconds carry the day, so a far-off year cannot overflow the way a
        // nanosecond product does (#814).
        let secs = day as i128 * 86_400 + nanos_of_day.div_euclid(1_000_000_000) as i128
            - offset as i128;
        Some(secs * 1_000_000_000 + nanos_of_day.rem_euclid(1_000_000_000) as i128)
    };
    Some((
        instant(&za, &zb, day_a, ta)?,
        instant(&zb, &za, day_b, tb)?,
    ))
}

/// A `Value` as something a property can hold, or `None` if it cannot.
///
/// The distinction that matters is `Value::List` versus
/// `PropertyValue::Array`. `eval_expression` produces the latter only when
/// every element was already a literal, so `{xs: [date('1984-10-11')]}` --
/// or `[1 + 1]`, or `[abs(-1)]` -- arrives as a `Value::List`.
///
/// Five write paths each tested for `Value::Property` separately and each got
/// this wrong in its own way: two raised "refers to a variable that is not
/// bound here" about a query with no variables in it, one raised "must be a
/// scalar" about a list, and the relationship path **silently stored nothing**
/// -- `CREATE ()-[:R {xs: [date(...)]}]->()` succeeded with `xs` null (#831).
///
/// `Value::Null` is deliberately not handled here: the paths disagree about
/// whether an unbound variable is an error or a null property, and that
/// disagreement is theirs to keep.
fn storable_property(v: &Value) -> Option<PropertyValue> {
    match v {
        // A list literal reaches here already folded into an `Array`, so the
        // element check has to look *inside* it: `[{num: 1}]` arrives as
        // `Array([Map(..)])` and was stored whole, giving a node a property no
        // Cypher expression can produce (#975).
        Value::Property(p) if !property_is_storable(p) => None,
        Value::Property(p) => Some(p.clone()),
        // A list built in the query rather than folded by the parser. Each
        // element is checked as a *list element*, which is stricter than as a
        // property in its own right: a bare map may be stored (NDS-08), a map
        // inside a list may not.
        Value::List(items) => items
            .iter()
            .map(|i| match i {
                Value::Property(PropertyValue::Map(_)) | Value::Map(_) => None,
                other => storable_property(other),
            })
            .collect::<Option<Vec<_>>>()
            .map(PropertyValue::Array),
        // A property can hold neither an entity nor a map.
        _ => None,
    }
}

/// Can this value be a property?
///
/// A property is a scalar or a list of scalars. A **map** is neither, at any
/// depth: `SET a.maplist = [{num: 1}]` must raise a TypeError rather than
/// storing something `properties(a)` can hand back but no query can build.
///
/// A bare map is left alone here. Storing one is a documented extension
/// (NDS-08, nested map properties) rather than an accident, and turning that
/// off is a decision this fix is not entitled to make; the TCK scenario is
/// about a list *containing* one.
fn property_is_storable(p: &PropertyValue) -> bool {
    match p {
        PropertyValue::Array(items) => items
            .iter()
            .all(|i| !matches!(i, PropertyValue::Map(_)) && property_is_storable(i)),
        _ => true,
    }
}

fn temporal_epoch_nanos(v: &PropertyValue) -> Option<i128> {
    match v {
        PropertyValue::Date(d) => Some(*d as i128 * 86_400 * 1_000_000_000),
        PropertyValue::LocalTime(n) => Some(*n as i128),
        PropertyValue::Time { nanos, offset_seconds } => {
            Some(*nanos as i128 - *offset_seconds as i128 * 1_000_000_000)
        }
        PropertyValue::LocalDateTime { secs, nanos } => {
            Some(*secs as i128 * 1_000_000_000 + *nanos as i128)
        }
        PropertyValue::ZonedDateTime { secs, nanos, .. } => {
            Some(*secs as i128 * 1_000_000_000 + *nanos as i128)
        }
        PropertyValue::DateTime(ms) => Some(*ms as i128 * 1_000_000),
        _ => None,
    }
}

/// Shift a temporal value by a duration, keeping its own type.
///
/// Months are calendar months, so they are applied to the date rather than as
/// a fixed number of seconds -- adding one month to 31 January is 28 February,
/// not 3 March. Days and below are exact.
fn shift_temporal(
    v: &PropertyValue,
    months: i64,
    days: i64,
    seconds: i64,
    nanos: i64,
) -> Result<PropertyValue, ExecutionError> {
    use chrono::Datelike;
    // A `Date` has no clock, so a duration's **sub-day part is dropped** rather
    // than applied — and dropped *before* the components are combined.
    //
    // `days: -14` with a `+15h49m` remainder combines to -13.34 days, and
    // truncating that gives -13: one day off, because the fractional part
    // belongs to the clock the date does not have. Keeping the days field and
    // discarding the rest gives -14, which is what the calendar arithmetic
    // means.
    //
    // Addition looked correct throughout — a positive remainder truncates back
    // to the same day — so this only showed up on subtraction and on
    // mixed-sign durations (#817).
    let drop_sub_day = matches!(v, PropertyValue::Date(_));
    // A time of day has no calendar, so a duration's **date part is dropped**:
    // months and days cannot move a clock. This is the mirror of the rule
    // above, and it is why `localtime('12:31:14') + duration({months: 1,
    // days: -14, hours: 16})` is a time sixteen hours later and not an error
    // (#853).
    let drop_date_part = matches!(v, PropertyValue::LocalTime(_) | PropertyValue::Time { .. });
    let exact = if drop_sub_day {
        // Only the *sub-day remainder* is dropped, not the whole seconds
        // field. A duration's seconds can hold entire days -- `P25Y10M58D` +
        // `T67H56M27S` is 67 hours, nearly three days of them -- and those days
        // are calendar days a date can move by. Discarding the field wholesale
        // lost them: `date('1984-10-11') + duration({...})` answered 1997-10-10
        // where openCypher says 1997-10-11, and the subtraction missed by a day
        // the other way (#1001).
        //
        // The whole days come out by truncation toward zero, which is what
        // keeps #817 intact: there, `days: -14` with a `+15h49m` remainder
        // yields zero whole days, so `days` stays -14 rather than becoming the
        // -13 that combining and then truncating produced.
        let whole_days_in_seconds =
            (seconds as i128 * 1_000_000_000 + nanos as i128) / (86_400 * 1_000_000_000);
        (days as i128 + whole_days_in_seconds) * 86_400 * 1_000_000_000
    } else if drop_date_part {
        seconds as i128 * 1_000_000_000 + nanos as i128
    } else {
        days as i128 * 86_400 * 1_000_000_000
            + seconds as i128 * 1_000_000_000
            + nanos as i128
    };
    let months = if drop_date_part { 0 } else { months };

    // Calendar months first, on whatever date part the value has.
    let month_shift_nanos = if months == 0 {
        0i128
    } else {
        let day0 = match v {
            PropertyValue::Date(d) => *d as i64,
            PropertyValue::LocalDateTime { secs, .. } => secs.div_euclid(86_400),
            PropertyValue::ZonedDateTime { secs, offset_seconds, .. } => {
                (secs + *offset_seconds as i64).div_euclid(86_400)
            }
            // A time of day has no calendar to move.
            _ => return Err(ExecutionError::TypeError(
                "cannot add months to a value with no date part".to_string(),
            )),
        };
        let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|e| e.checked_add_signed(chrono::Duration::days(day0)))
            .ok_or_else(|| ExecutionError::RuntimeError("date out of range".into()))?;
        let total = base.year() as i64 * 12 + (base.month0() as i64) + months;
        let (y, m0) = (total.div_euclid(12), total.rem_euclid(12));
        // Clamp the day into the target month, which is what a calendar month
        // shift means: 31 Jan + 1 month is 28/29 Feb.
        let last = days_in_month(y as i32, m0 as u32 + 1);
        let shifted = chrono::NaiveDate::from_ymd_opt(y as i32, m0 as u32 + 1, base.day().min(last))
            .ok_or_else(|| ExecutionError::RuntimeError("date out of range".into()))?;
        (shifted.signed_duration_since(base).num_days() as i128) * 86_400 * 1_000_000_000
    };

    let total = temporal_epoch_nanos(v)
        .ok_or_else(|| ExecutionError::TypeError("not a temporal value".to_string()))?
        + month_shift_nanos
        + exact;
    Ok(rebuild_temporal_like(v, total))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    let (ny, nm) = if month == 12 { (year + 1, 1) } else { (year, month + 1) };
    let first = chrono::NaiveDate::from_ymd_opt(year, month, 1);
    let next = chrono::NaiveDate::from_ymd_opt(ny, nm, 1);
    match (first, next) {
        (Some(a), Some(b)) => b.signed_duration_since(a).num_days() as u32,
        _ => 31,
    }
}

/// Put an epoch-nanosecond total back into the same type it came from, so a
/// `Date` plus a duration is still a `Date`.
fn rebuild_temporal_like(like: &PropertyValue, total_nanos: i128) -> PropertyValue {
    const DAY: i128 = 86_400 * 1_000_000_000;
    match like {
        PropertyValue::Date(_) => PropertyValue::Date(total_nanos.div_euclid(DAY) as i32),
        PropertyValue::LocalTime(_) => {
            PropertyValue::LocalTime(total_nanos.rem_euclid(DAY) as i64)
        }
        PropertyValue::Time { offset_seconds, .. } => PropertyValue::Time {
            nanos: (total_nanos + *offset_seconds as i128 * 1_000_000_000).rem_euclid(DAY) as i64,
            offset_seconds: *offset_seconds,
        },
        PropertyValue::LocalDateTime { .. } => PropertyValue::LocalDateTime {
            secs: total_nanos.div_euclid(1_000_000_000) as i64,
            nanos: total_nanos.rem_euclid(1_000_000_000) as u32,
        },
        PropertyValue::ZonedDateTime { offset_seconds, zone, .. } => PropertyValue::ZonedDateTime {
            secs: total_nanos.div_euclid(1_000_000_000) as i64,
            nanos: total_nanos.rem_euclid(1_000_000_000) as u32,
            offset_seconds: *offset_seconds,
            zone: zone.clone(),
        },
        _ => PropertyValue::DateTime((total_nanos / 1_000_000) as i64),
    }
}

/// `a - b` for two temporals: the duration between them.
/// `a - b` for two temporals, in **calendar** components.
///
/// ```text
/// duration.between(date('1984-10-11'), date('2015-06-24'))  ->  P30Y8M13D
/// ```
///
/// Not `P11213D`. Cypher counts whole months first and leaves the remainder in
/// days, because a month has no fixed length — the answer must be the one you
/// get by *counting off* years and months on a calendar, not by dividing
/// elapsed time.
///
/// The `-` operator on two temporals stays a plain elapsed difference
/// (`temporal_difference` below); only `duration.between` is calendar-aware.
/// That split is Cypher's, and collapsing the two would make the same
/// subtraction disagree with itself depending on which spelling was used
/// (#804).
fn temporal_difference_calendar(
    a: &PropertyValue,
    b: &PropertyValue,
) -> Result<PropertyValue, ExecutionError> {
    use chrono::Datelike;

    // Only the components the two values **share** are compared. A date has no
    // time and a time has no date, so `duration.between(date(...),
    // localtime('16:30'))` is `PT16H30M` — the clock difference alone, with the
    // date side contributing nothing.
    //
    // Treating the missing part as zero instead gave `P-5396DT-7H-30M`: the
    // date's midnight measured against a time of day, which is a real duration
    // between two instants that were never comparable (#807).
    // A value with **no** temporal parts at all is not a temporal value, and
    // is rejected before any of the shared-component logic below. That is
    // different from a date having no clock: `duration.between("a", dt)` must
    // fail, while `duration.between(date, localtime)` must not. Defaulting a
    // missing part to zero conflated the two and made the string case answer
    // (#807).
    for v in [a, b] {
        if date_part_of(v).is_none() && time_part_of(v).is_none() {
            return Err(ExecutionError::TypeError(format!(
                "duration.between() needs temporal values, not {}",
                v.type_name()
            )));
        }
    }

    let (da, db) = (date_part_of(a), date_part_of(b));
    if let Some(shared) = shared_component_difference(a, b) {
        let _ = (da, db);
        return shared;
    }
    let (Some(da), Some(db)) = (da, db) else {
        return temporal_difference(a, b);
    };

    // Within one month the calendar answer *is* the elapsed one, and the plain
    // form gives it in the shape the TCK wants: `PT6H`, not `P0M0DT6H`. Going
    // through the month arithmetic for these produced four regressions —
    // correct values, wrong shape, which a diff reports as breakage.
    //
    // The threshold is a differing (year, month), not a day count: 31 Jan to
    // 1 Feb is one day apart and *does* cross a month.
    {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let same_month = |x: i32, y: i32| {
            match (
                epoch.checked_add_signed(chrono::Duration::days(x as i64)),
                epoch.checked_add_signed(chrono::Duration::days(y as i64)),
            ) {
                (Some(p), Some(q)) => {
                    use chrono::Datelike;
                    p.year() == q.year() && p.month() == q.month()
                }
                _ => false,
            }
        };
        if same_month(da, db) {
            return temporal_difference(a, b);
        }
    }

    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let to_date = |d: i32| {
        epoch
            .checked_add_signed(chrono::Duration::days(d as i64))
            .ok_or_else(|| ExecutionError::RuntimeError("date out of range".into()))
    };
    let (start, end) = (to_date(db)?, to_date(da)?);
    // The clock parts must be compared as **instants** when both sides carry an
    // offset, exactly as in the time-only path (#807). `time_part_of` returns
    // the local wall clock, which is right for rendering and wrong here:
    // 21:40:36.143+0200 and 21:40:32.142+0100 read as 4ms apart locally and are
    // really 59m55.999s apart, so the borrow fired and stole a month —
    // `P11M` where `P1Y` belongs (#812).
    let both_zoned = matches!(
        (a, b),
        (PropertyValue::ZonedDateTime { .. }, PropertyValue::ZonedDateTime { .. })
    );
    let clock = |v: &PropertyValue| -> i64 {
        let local = time_part_of(v).unwrap_or(0);
        match v {
            PropertyValue::ZonedDateTime { offset_seconds, .. } if both_zoned => {
                local - *offset_seconds as i64 * 1_000_000_000
            }
            _ => local,
        }
    };
    let (ta, tb) = (clock(a), clock(b));

    // Whole months, then leftover days, then the clock. Borrowing works as it
    // does on paper: if the clock difference is negative, one day is not yet
    // complete; if the day count then goes negative, one month is not either.
    let mut months = (end.year() as i64 - start.year() as i64) * 12
        + (end.month() as i64 - start.month() as i64);
    // The clock difference keeps its own sign; it is only borrowed into days
    // when the *whole* result is positive. Borrowing unconditionally produced
    // `P-28DT2H19M27.858S` where `P-27DT-21H-40M-32.142S` was expected — the
    // same instant pair, with the components disagreeing in sign, which is the
    // one thing a duration's components may not do (#775 again, one level up).
    let nanos = ta - tb;
    let forward = da > db || (da == db && nanos >= 0);
    let (mut nanos, mut day_adjust) = if nanos < 0 && forward {
        (nanos + 86_400 * 1_000_000_000, -1i64)
    } else if nanos > 0 && !forward {
        (nanos - 86_400 * 1_000_000_000, 1i64)
    } else {
        (nanos, 0i64)
    };
    let _ = &mut nanos;
    let _ = &mut day_adjust;
    let mut days = end
        .signed_duration_since(shift_months_clamped(start, months)?)
        .num_days()
        + day_adjust;
    // Borrow toward zero, not toward negative infinity. Going **backwards**, a
    // partial month stays as days rather than becoming a whole negative month
    // plus positive days: 2015-07-21 back to 2015-06-24 is `P-27D`, not
    // `P-1M3D`. Both describe the same instant pair and only one is what
    // Cypher writes.
    if months > 0 && days < 0 {
        months -= 1;
        days = end
            .signed_duration_since(shift_months_clamped(start, months)?)
            .num_days()
            + day_adjust;
    } else if months < 0 && days > 0 {
        months += 1;
        days = end
            .signed_duration_since(shift_months_clamped(start, months)?)
            .num_days()
            + day_adjust;
    }

    Ok(PropertyValue::Duration {
        months,
        days,
        // Truncating, not Euclidean — the same trap #775 fixed in
        // `temporal_difference` and that I reintroduced here. `div_euclid`
        // floors toward negative infinity, so -78032.142s splits into
        // (-78033s, +858ms) and renders `-33.858S` where `-32.142S` belongs.
        // A duration's components must share a sign.
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

/// Move a date by whole months, clamping the day into the target month —
/// 31 January plus one month is 28 February.
fn shift_months_clamped(
    d: chrono::NaiveDate,
    months: i64,
) -> Result<chrono::NaiveDate, ExecutionError> {
    use chrono::Datelike;
    let total = d.year() as i64 * 12 + d.month0() as i64 + months;
    let (y, m0) = (total.div_euclid(12), total.rem_euclid(12));
    let last = days_in_month(y as i32, m0 as u32 + 1);
    chrono::NaiveDate::from_ymd_opt(y as i32, m0 as u32 + 1, d.day().min(last))
        .ok_or_else(|| ExecutionError::RuntimeError("date out of range".into()))
}

/// Only the components two temporals **share**, when one of them has no date.
///
/// A date has no time and a time has no date, so
/// `duration.between(date(...), localtime('16:30'))` is `PT16H30M` — the clock
/// difference alone, with the date side contributing nothing. Treating the
/// missing part as zero instead gave `P-5396DT-7H-30M`: a real duration between
/// two instants that were never comparable (#807).
///
/// Returns `None` when **both** sides carry a date, which is the ordinary case
/// the callers handle themselves.
///
/// This lived inside `temporal_difference_calendar`, so `duration.between` had
/// the rule and `duration.inDays`/`inSeconds` did not — the same difference
/// measured two ways disagreed by fifteen years, and only on the mixed pairs
/// (#849).
fn shared_component_difference(
    a: &PropertyValue,
    b: &PropertyValue,
) -> Option<Result<PropertyValue, ExecutionError>> {
    if date_part_of(a).is_some() && date_part_of(b).is_some() {
        return None;
    }
    // Both sides have a clock and at least one carries a zone: they are real
    // instants on a shared day, and only that treatment gets the
    // daylight-saving rows right (#821). A side with no clock at all -- a bare
    // `date` against a `localtime` -- falls through to the reading below,
    // since there is no instant to compare.
    if time_part_of(a).is_some() && time_part_of(b).is_some() {
        if let Some((na, nb)) = zone_aligned_instants(a, b) {
            let diff = na - nb;
            return Some(Ok(PropertyValue::Duration {
                months: 0,
                days: 0,
                seconds: (diff / 1_000_000_000) as i64,
                nanos: (diff % 1_000_000_000) as i32,
            }));
        }
    }
    // Neither side is zoned: compare the local readings. A date's clock is
    // midnight.
    let (ta, tb) = (
        time_part_of(a).unwrap_or(0),
        time_part_of(b).unwrap_or(0),
    );
    let diff = ta - tb;
    Some(Ok(PropertyValue::Duration {
        months: 0,
        days: 0,
        seconds: diff / 1_000_000_000,
        nanos: (diff % 1_000_000_000) as i32,
    }))
}

fn temporal_difference(
    a: &PropertyValue,
    b: &PropertyValue,
) -> Result<PropertyValue, ExecutionError> {
    // Only the shared components, when one side has no date (#807). This lived
    // in the calendar function alone, so `duration.between` had the rule and
    // `duration.inDays`/`inSeconds` did not (#849).
    if let Some(shared) = shared_component_difference(a, b) {
        return shared;
    }
    // When either side carries a zone the other is read in it, rather than as
    // UTC (#821).
    let (na, nb) = match zone_aligned_instants(a, b) {
        Some(pair) => pair,
        None => match (temporal_epoch_nanos(a), temporal_epoch_nanos(b)) {
            (Some(x), Some(y)) => (x, y),
            _ => return Err(ExecutionError::TypeError("not temporal values".to_string())),
        },
    };
    let diff = na - nb;
    // Truncating division, not Euclidean. `div_euclid`/`rem_euclid` floor
    // toward negative infinity, so a difference of -0.4s split into
    // (seconds, nanos) becomes (-1, +600_000_000) and renders as `PT-1.6S`
    // instead of `PT-0.4S`. The components of a duration must share a sign;
    // `/` and `%` truncate toward zero and do (#775).
    let secs_total = (diff / 1_000_000_000) as i64;
    Ok(PropertyValue::Duration {
        months: 0,
        days: secs_total / 86_400,
        seconds: secs_total % 86_400,
        nanos: (diff % 1_000_000_000) as i32,
    })
}

fn add_duration_to_datetime(dt_millis: i64, months: i64, days: i64, seconds: i64) -> PropertyValue {
    use chrono::{Datelike, Months, Duration, TimeZone};
    let dt = chrono::Utc.timestamp_millis_opt(dt_millis).single();
    match dt {
        Some(mut datetime) => {
            // Add months
            if months > 0 {
                if let Some(d) = datetime.checked_add_months(Months::new(months as u32)) {
                    datetime = d;
                }
            } else if months < 0 {
                if let Some(d) = datetime.checked_sub_months(Months::new((-months) as u32)) {
                    datetime = d;
                }
            }
            // Add days and seconds
            let total_secs = days * 86400 + seconds;
            if let Some(d) = datetime.checked_add_signed(Duration::seconds(total_secs)) {
                datetime = d;
            }
            PropertyValue::DateTime(datetime.timestamp_millis())
        }
        None => PropertyValue::Null,
    }
}

/// ISO 8601's alternative duration form: `P<date>T<time>`, where the date is
/// `YYYY-MM-DD` (or `YYYYMMDD`) and the time is `hh:mm:ss[.fff]` (or `hhmmss`).
///
/// Returns `None` when the input is not in that shape, so the unit scanner
/// keeps every string it already handled. The two forms are mutually
/// exclusive -- one has unit letters and the other has separators -- so a
/// shape test is enough to route between them and neither needs to know about
/// the other.
///
/// The fields are durations, not calendar positions: the year field may exceed
/// any real year and the month field is a count of months, so no date
/// validation applies.
fn parse_extended_duration(date_part: &str, time_part: &str) -> Option<Value> {
    /// `a-b-c` with every field all digits, or one run of `n` digits split
    /// into fixed-width fields.
    fn fields(s: &str, widths: [usize; 3]) -> Option<[i128; 3]> {
        let parts: Vec<&str> = if s.contains('-') || s.contains(':') {
            s.split(|c| c == '-' || c == ':').collect()
        } else {
            if s.len() != widths.iter().sum::<usize>() {
                return None;
            }
            let (a, b) = s.split_at(widths[0]);
            let (b, c) = b.split_at(widths[1]);
            vec![a, b, c]
        };
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit())) {
            return None;
        }
        let mut out = [0i128; 3];
        for (i, p) in parts.iter().enumerate() {
            out[i] = p.parse::<i128>().ok()?;
        }
        Some(out)
    }

    // A fraction is allowed only on the seconds, and only in the time half.
    let (time_head, frac) = match time_part.split_once(|c| c == '.' || c == ',') {
        Some((h, f)) if f.bytes().all(|b| b.is_ascii_digit()) && !f.is_empty() => (h, f),
        Some(_) => return None,
        None => (time_part, ""),
    };

    let [years, months_f, days] = fields(date_part, [4, 2, 2])?;
    let [hours, minutes, seconds] = if time_head.is_empty() {
        [0, 0, 0]
    } else {
        fields(time_head, [2, 2, 2])?
    };
    // A bare date with no `T` is ambiguous against the unit form only in ways
    // the digit test already excludes, but an empty date is not a duration.
    if date_part.is_empty() {
        return None;
    }

    const NPS: i128 = 1_000_000_000;
    let mut nanos = frac
        .chars()
        .chain(std::iter::repeat('0'))
        .take(9)
        .fold(0i128, |acc, c| acc * 10 + c.to_digit(10).unwrap_or(0) as i128);
    nanos += (hours * 3600 + minutes * 60 + seconds) * NPS;

    Some(Value::Property(PropertyValue::Duration {
        months: (years * 12 + months_f) as i64,
        days: days as i64,
        seconds: (nanos / NPS) as i64,
        nanos: (nanos % NPS) as i32,
    }))
}

/// Parse ISO 8601 duration string (e.g. "P1Y2M3DT4H5M6S")
/// Parse an ISO-8601 duration, with **per-component signs** (#853).
///
/// `toString` renders a mixed-sign duration as `P1DT-0.001S`, and Cypher
/// requires `duration(toString(d)) = d`. The old scanner accepted only digits
/// and `.` into its number buffer, so a `-` was silently skipped and the value
/// came back positive -- a round trip that returned a *different duration* and
/// reported success.
///
/// It also computed the fraction as `(val - val.floor()) * 1e9` in `f64`, so
/// `PT-2.001S` came back as `PT2.000999999S`: wrong sign and one nanosecond
/// short. The fraction is now read from its digits, which is exact.
///
/// Time components are summed in nanoseconds and split once at the end, so the
/// result's seconds and nanoseconds share the sign of their total (#806).
fn parse_iso_duration(s: &str) -> ExecutionResult<Value> {
    let text = s.trim();
    if !text.starts_with('P') && !text.starts_with('p') {
        return Err(ExecutionError::RuntimeError(format!("Invalid duration format: {}", s)));
    }
    let rest = &text[1..];
    let (date_part, time_part) = match rest.find(|c: char| c == 'T' || c == 't') {
        Some(idx) => (&rest[..idx], &rest[idx + 1..]),
        None => (rest, ""),
    };

    // ISO 8601's *alternative* duration form, which spells the components as a
    // date and a clock rather than with unit letters:
    //
    //     P2012-02-02T14:37:21.545   ==   P2012Y2M2DT14H37M21.545S
    //
    // The unit scanner below cannot read it -- there are no units to read, and
    // its `-` handling is a per-component sign, so the separators looked like
    // signs on empty numbers and the whole string scanned to zero. `duration()`
    // returned `PT0S` for a perfectly valid duration, with no error (#1005).
    if let Some(v) = parse_extended_duration(date_part, time_part) {
        return Ok(v);
    }

    const NPS: i128 = 1_000_000_000;
    // A mean Gregorian month is exactly 2,629,746 seconds (#829).
    const MEAN_MONTH_NANOS: i128 = 2_629_746 * NPS;

    let mut months: i128 = 0;
    let mut days: i128 = 0;
    let mut time_nanos: i128 = 0;

    /// One component: an optional sign, digits, an optional fraction, a unit.
    ///
    /// Returns the value scaled to `unit_nanos`, exactly -- the fraction is
    /// read from its digits rather than through a float, so `.001` is a
    /// million nanoseconds and not 999,999.
    fn scaled(sign: i128, int_digits: &str, frac_digits: &str, unit_nanos: i128) -> i128 {
        let whole: i128 = int_digits.parse().unwrap_or(0);
        let mut out = whole * unit_nanos;
        if !frac_digits.is_empty() {
            let num: i128 = frac_digits.parse().unwrap_or(0);
            let denom = 10i128.checked_pow(frac_digits.len() as u32).unwrap_or(1);
            out += num * unit_nanos / denom;
        }
        sign * out
    }

    let mut scan = |section: &str, is_time: bool| -> Result<(), ExecutionError> {
        let mut sign: i128 = 1;
        let mut int_digits = String::new();
        let mut frac_digits = String::new();
        let mut in_fraction = false;
        for ch in section.chars() {
            match ch {
                '-' if int_digits.is_empty() && frac_digits.is_empty() => sign = -1,
                '+' if int_digits.is_empty() && frac_digits.is_empty() => sign = 1,
                '.' | ',' => in_fraction = true,
                c if c.is_ascii_digit() => {
                    if in_fraction { frac_digits.push(c) } else { int_digits.push(c) }
                }
                unit => {
                    // Years, weeks and a fractional month or day contribute to
                    // the next unit down, using the same constants the map
                    // constructor derives (#829).
                    match unit {
                        'Y' | 'y' => months += scaled(sign, &int_digits, &frac_digits, 12),
                        // A week's fraction carries into the day and then into
                        // the clock, like a day's does: `P2.5W` is 17 days and
                        // 12 hours, not 17 days. Scaling straight to whole days
                        // truncated the half away (#885).
                        'W' | 'w' => {
                            let total =
                                scaled(sign, &int_digits, &frac_digits, NPS * 86_400 * 7);
                            days += total / (NPS * 86_400);
                            time_nanos += total % (NPS * 86_400);
                        }
                        'D' | 'd' if !is_time => {
                            let total = scaled(sign, &int_digits, &frac_digits, NPS * 86_400);
                            days += total / (NPS * 86_400);
                            time_nanos += total % (NPS * 86_400);
                        }
                        'M' | 'm' if !is_time => {
                            let total = scaled(sign, &int_digits, &frac_digits, MEAN_MONTH_NANOS);
                            months += total / MEAN_MONTH_NANOS;
                            let rem = total % MEAN_MONTH_NANOS;
                            days += rem / (NPS * 86_400);
                            time_nanos += rem % (NPS * 86_400);
                        }
                        'H' | 'h' => time_nanos += scaled(sign, &int_digits, &frac_digits, NPS * 3600),
                        'M' | 'm' => time_nanos += scaled(sign, &int_digits, &frac_digits, NPS * 60),
                        'S' | 's' => time_nanos += scaled(sign, &int_digits, &frac_digits, NPS),
                        _ => {}
                    }
                    sign = 1;
                    int_digits.clear();
                    frac_digits.clear();
                    in_fraction = false;
                }
            }
        }
        Ok(())
    };
    scan(date_part, false)?;
    scan(time_part, true)?;

    Ok(Value::Property(PropertyValue::Duration {
        months: months as i64,
        days: days as i64,
        seconds: (time_nanos / NPS) as i64,
        nanos: (time_nanos % NPS) as i32,
    }))
}

/// Shared CASE expression evaluation
fn eval_case<F>(
    operand: Option<&Expression>,
    when_clauses: &[(Expression, Expression)],
    else_result: Option<&Expression>,
    eval_fn: F,
) -> ExecutionResult<Value>
where
    F: Fn(&Expression) -> ExecutionResult<Value>,
{
    if let Some(op_expr) = operand {
        // Simple CASE: CASE expr WHEN val THEN result
        let op_val = eval_fn(op_expr)?;
        for (when_expr, then_expr) in when_clauses {
            let when_val = eval_fn(when_expr)?;
            if op_val == when_val {
                return eval_fn(then_expr);
            }
        }
    } else {
        // Searched CASE: CASE WHEN condition THEN result
        for (when_expr, then_expr) in when_clauses {
            let when_val = eval_fn(when_expr)?;
            if matches!(when_val, Value::Property(PropertyValue::Boolean(true))) {
                return eval_fn(then_expr);
            }
        }
    }
    // ELSE clause or NULL
    if let Some(else_expr) = else_result {
        eval_fn(else_expr)
    } else {
        Ok(Value::Null)
    }
}

/// Optimization problem wrapper for GraphStore
struct GraphOptimizationProblem {
    /// Static cost coefficients (e.g. price per unit) for single objective
    costs: Vec<f64>,
    /// Multiple cost coefficient vectors for multi-objective
    multi_costs: Vec<Vec<f64>>,
    /// Budget constraint (optional)
    budget: Option<f64>,
    /// Minimum total sum constraint (optional)
    min_total: Option<f64>,
    /// Dimensions
    dim: usize,
    /// Bounds
    lower: f64,
    upper: f64,
}

impl Problem for GraphOptimizationProblem {
    fn dim(&self) -> usize {
        self.dim
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::from_elem(self.dim, self.lower),
            Array1::from_elem(self.dim, self.upper)
        )
    }

    fn objective(&self, variables: &Array1<f64>) -> f64 {
        // Minimize sum(variable * cost)
        let mut sum = 0.0;
        for i in 0..self.dim {
            sum += variables[i] * self.costs[i];
        }
        sum
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        let mut penalty = 0.0;
        
        // 1. Budget Constraint: sum(x * cost) <= budget
        if let Some(budget) = self.budget {
            let mut total_cost = 0.0;
            for i in 0..self.dim {
                total_cost += variables[i] * self.costs[i];
            }
            if total_cost > budget {
                penalty += (total_cost - budget).powi(2);
            }
        }

        // 2. Minimum Total Constraint: sum(x) >= min_total
        if let Some(min_total) = self.min_total {
            let mut total_val = 0.0;
            for i in 0..self.dim {
                total_val += variables[i];
            }
            if total_val < min_total {
                penalty += (min_total - total_val).powi(2) * 100.0; // High weight for feasibility
            }
        }

        penalty
    }
}

impl MultiObjectiveProblem for GraphOptimizationProblem {
    fn num_objectives(&self) -> usize {
        self.multi_costs.len()
    }

    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        let mut results = Vec::with_capacity(self.multi_costs.len());
        for costs in &self.multi_costs {
            let mut sum = 0.0;
            for i in 0..self.dim {
                sum += variables[i] * costs[i];
            }
            results.push(sum);
        }
        results
    }

    fn dim(&self) -> usize { self.dim }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (
            Array1::from_elem(self.dim, self.lower),
            Array1::from_elem(self.dim, self.upper)
        )
    }
}

/// Physical operator trait - all operators implement this
/// Drain a pass-through operator's input **mutably**, once, and replace it with
/// the rows it produced.
///
/// A pass-through operator's default `next_mut` delegates to `next`, which
/// reads its input read-only -- so any write beneath it refuses outright with
/// "requires mutable store access". That defect has now been fixed four times
/// on different operators (#622 barriers, #624 joins, #649 SKIP and LIMIT, #866
/// SORT and FILTER), each time for the ones a failing query happened to name.
///
/// This is the shared body for the rest. Draining first is not merely
/// convenient: it lets each operator keep its single `next` implementation
/// instead of growing a second, mutable copy of its own logic -- which is the
/// duplication that produced most of this cycle's defects. It also matches
/// Cypher, where a write is eager anyway; and `next_mut` is only reached when
/// the query writes, so a read-only plan still streams (#870).
fn drain_input_for_write(
    input: &mut OperatorBox,
    store: &mut GraphStore,
    tenant_id: &str,
) -> ExecutionResult<()> {
    if input.is_materialized() {
        return Ok(());
    }
    let mut rows = Vec::new();
    while let Some(r) = input.next_mut(store, tenant_id)? {
        rows.push(r);
    }
    *input = Box::new(MaterializedOperator::new(rows));
    Ok(())
}

pub trait PhysicalOperator: Send {
    /// Get the next record from this operator (read-only operations)
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>>;

    /// Try to push a result-cardinality hint down to scan operators so they
    /// can stop yielding once `n` rows are produced (early termination).
    ///
    /// Default returns `false` — meaning the operator either changes
    /// cardinality unpredictably (Filter, Sort, Distinct, Aggregate, Expand
    /// without selectivity info) or simply doesn't implement the hint.
    /// Pass-through operators (Project) override to forward the hint to
    /// their input. Scan operators (NodeScanOperator) override to set
    /// `early_limit`.
    ///
    /// Returns `true` if the hint was accepted somewhere in the subtree.
    /// The caller may still need to apply a `LimitOperator` on top — this
    /// hint is purely an optimization to avoid unnecessary work upstream.
    /// Whether this operator replays an already-computed set of rows.
    ///
    /// Used by `drain_input_for_write` to tell "I have already drained my
    /// input" from "I have not", without giving every pass-through operator a
    /// bookkeeping field of its own (#870).
    fn is_materialized(&self) -> bool {
        false
    }

    fn try_push_limit(&mut self, _n: usize) -> bool {
        false
    }

    /// Get the next batch of records (Vectorized Execution)
    /// Defaults to accumulating records from next()
    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(record) => records.push(record),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records, columns: Vec::new() }))
        }
    }

    /// Get the next batch of records for mutating operations
    fn next_batch_mut(&mut self, store: &mut GraphStore, tenant_id: &str, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next_mut(store, tenant_id)? {
                Some(record) => records.push(record),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records, columns: Vec::new() }))
        }
    }

    /// Get the next record from this operator (write operations that mutate the store)
    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // Default implementation calls the read-only version
        self.next(store)
    }

    /// Reset the operator to start from the beginning
    fn reset(&mut self);

    /// The predicate this operator filters on, if it is a filter.
    ///
    /// Exists so the planner can tell whether a filter it is about to add is
    /// the one already sitting underneath. `x AND x` is idempotent, so the
    /// second evaluation is pure cost -- ~130 ms on LDBC IC9, where the same
    /// compound predicate was evaluated 389,461 times twice over (#519).
    fn filter_predicate(&self) -> Option<&Expression> {
        None
    }

    /// The operators this one pulls from, in the order `describe()` lists
    /// them.
    ///
    /// Defaults to none, which is right for every leaf (scans, DDL, static
    /// inputs). Operators that hold an input must override it, or a tree walk
    /// stops at them.
    ///
    /// This exists so a pass can rewrite the tree in place — `PROFILE` wraps
    /// every node to attribute wall-clock (`CH-PROFILE-01`), and it is the
    /// mutable counterpart of the children `describe()` already returns for
    /// EXPLAIN.
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        Vec::new()
    }

    /// Returns true if this operator mutates the graph store
    fn is_mutating(&self) -> bool {
        false
    }

    /// Describe this operator for EXPLAIN output
    /// Returns (operator_name, details, children)
    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Unknown".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Description of an operator for EXPLAIN output
pub struct OperatorDescription {
    pub name: String,
    pub details: String,
    pub children: Vec<OperatorDescription>,
}

impl OperatorDescription {
    /// Format the operator tree as a string
    pub fn format(&self, indent: usize) -> String {
        let mut result = String::new();
        let prefix = if indent == 0 {
            String::new()
        } else {
            format!("{}+- ", "   ".repeat(indent - 1))
        };

        if self.details.is_empty() {
            result.push_str(&format!("{}{}\n", prefix, self.name));
        } else {
            result.push_str(&format!("{}{} ({})\n", prefix, self.name, self.details));
        }

        for child in &self.children {
            result.push_str(&child.format(indent + 1));
        }
        result
    }
}

/// Binding power, matching the parser's precedence ladder.
///
/// Kept in step with `PRATT_PARSER` in `query::parser`: OR < XOR < AND < NOT <
/// comparison < +- < */% < ^. If the two ever disagree, EXPLAIN prints a
/// predicate that means something the engine did not run, which is the exact
/// failure #541 describes.
fn binary_precedence(op: &BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => 1,
        BinaryOp::Xor => 2,
        BinaryOp::And => 3,
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::In
        | BinaryOp::StartsWith
        | BinaryOp::EndsWith
        | BinaryOp::Contains
        | BinaryOp::RegexMatch => 4,
        BinaryOp::Add | BinaryOp::Sub => 5,
        BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => 6,
        BinaryOp::Pow => 7,
    }
}

/// `expr` rendered, bracketed only where dropping the brackets would reparse
/// differently.
///
/// `bias` is 1 for the side that must not re-associate -- the right operand of
/// a left-associative operator, the left operand of a right-associative one --
/// so `a - (b - c)` keeps its brackets while `(a - b) - c` does not need them.
fn parenthesised(expr: &Expression, parent: u8, bias: u8) -> String {
    let text = format_expression(expr);
    match expr {
        Expression::Binary { op, .. } if binary_precedence(op) + bias <= parent => {
            format!("({text})")
        }
        _ => text,
    }
}

/// Format an Expression for EXPLAIN output
fn format_expression(expr: &Expression) -> String {
    match expr {
        Expression::Variable(v) => v.clone(),
        Expression::Property { variable, property } => format!("{}.{}", variable, property),
        Expression::Literal(val) => format!("{:?}", val),
        Expression::Binary { left, op, right } => {
            let op_str = match op {
                BinaryOp::Eq => "=", BinaryOp::Ne => "<>", BinaryOp::Lt => "<",
                BinaryOp::Le => "<=", BinaryOp::Gt => ">", BinaryOp::Ge => ">=",
                BinaryOp::And => "AND", BinaryOp::Or => "OR",
                BinaryOp::Add => "+", BinaryOp::Sub => "-",
                BinaryOp::Mul => "*", BinaryOp::Div => "/", BinaryOp::Mod => "%",
                BinaryOp::Pow => "^", BinaryOp::Xor => "XOR",
                BinaryOp::StartsWith => "STARTS WITH", BinaryOp::EndsWith => "ENDS WITH",
                BinaryOp::Contains => "CONTAINS", BinaryOp::In => "IN",
                BinaryOp::RegexMatch => "=~",
            };
            // Parenthesised where precedence would otherwise change the
            // meaning. `A AND B AND (C OR D)` printed as `A AND B AND C OR D`,
            // which reads as `(A AND B AND C) OR D` -- a different predicate,
            // and one that looks like a P0 wrong answer in a plan that is
            // actually correct (#541).
            let parent = binary_precedence(op);
            let right_assoc = matches!(op, BinaryOp::Pow);
            // The side that must not re-associate takes the bias: for a
            // left-associative operator that is the *right* operand, since
            // `a - (b - c)` differs from `a - b - c` while `(a - b) - c` does
            // not. Reversed, this brackets every left operand of an AND chain
            // and leaves `a - (b - c)` unbracketed -- both of which the tests
            // below caught.
            let left_str = parenthesised(left, parent, if right_assoc { 0 } else { 1 });
            let right_str = parenthesised(right, parent, if right_assoc { 1 } else { 0 });
            format!("{} {} {}", left_str, op_str, right_str)
        }
        Expression::Unary { op, expr } => {
            let op_str = match op {
                UnaryOp::Not => "NOT", UnaryOp::Minus => "-",
                UnaryOp::IsNull => "IS NULL", UnaryOp::IsNotNull => "IS NOT NULL",
            };
            // A unary operator binds tighter than every binary one, so a binary
            // operand always needs bracketing: `NOT (a AND b)` is not
            // `NOT a AND b`.
            let operand = match expr.as_ref() {
                Expression::Binary { .. } => format!("({})", format_expression(expr)),
                _ => format_expression(expr),
            };
            match op {
                UnaryOp::IsNull | UnaryOp::IsNotNull => format!("{} {}", operand, op_str),
                _ => format!("{} {}", op_str, operand),
            }
        }
        Expression::Function { name, args, distinct } => {
            let arg_strs: Vec<String> = args.iter().map(format_expression).collect();
            if *distinct {
                format!("{}(DISTINCT {})", name, arg_strs.join(", "))
            } else {
                format!("{}({})", name, arg_strs.join(", "))
            }
        }
        Expression::PathVariable(v) => format!("path({})", v),
        Expression::Parameter(p) => format!("${}", p),
        _ => "...".to_string(),
    }
}

/// Type alias for boxed operators
pub type OperatorBox = Box<dyn PhysicalOperator>;

/// Node scan operator: MATCH (n:Person)
pub struct NodeScanOperator {
    /// Variable name to bind nodes to
    variable: String,
    /// Labels to filter by
    labels: Vec<Label>,
    /// NodeIds to iterate. Populated lazily on first `next()` via `initialize()`.
    node_ids: Vec<NodeId>,
    /// True after `initialize()` has run — distinguishes "not yet initialized"
    /// from "initialized but label legitimately empty".
    initialized: bool,
    /// Current index
    current: usize,
    /// Early limit: stop producing after this many rows (for LIMIT pushdown).
    /// When set on a single-label scan, the store is asked for only that
    /// many ids — avoiding the full label-set materialization.
    early_limit: Option<usize>,
    /// Count of rows produced (for early limit tracking)
    produced: usize,
}

impl NodeScanOperator {
    /// Create a new node scan operator
    pub fn new(variable: String, labels: Vec<Label>) -> Self {
        Self {
            variable,
            labels,
            node_ids: Vec::new(),
            initialized: false,
            current: 0,
            early_limit: None,
            produced: 0,
        }
    }

    /// Set early limit for LIMIT pushdown optimization
    pub fn with_early_limit(mut self, limit: usize) -> Self {
        self.early_limit = Some(limit);
        self
    }

    fn initialize(&mut self, store: &GraphStore) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Three cases:
        //   1. No labels  → scan all nodes (rare)
        //   2. Single label → direct copy of label_index entry; no dedup
        //   3. Multi-label → **intersection**
        //
        // Case 3 used to be a union, and said so. `(a:A:B)` in Cypher is a
        // conjunction: the node carries A *and* B. Returning the union made
        // `MATCH (a:A:B)` answer six rows where two are correct, and
        // `(a:A:B:C)` return every labelled node in the graph (#944) --
        // strictly more rows than asked for, from a query that reported
        // success, so the filter failed open.
        //
        // Sort behavior is conditional:
        //   - With early_limit (LIMIT pushdown): skip sort — only `limit`
        //     ids returned, sort cost is wasted, fast-termination matters more.
        //   - Without early_limit (full scan): KEEP sort. Sorted NodeIds give
        //     sequential memory access during downstream Expand, which improves
        //     cache locality and dominates the sort cost on full scans.
        //     Empirically: removing the sort unconditionally regressed
        //     full-scan aggregations by 10-30%.
        if self.labels.is_empty() {
            self.node_ids = store.all_nodes().into_iter().map(|n| n.id).collect();
        } else if self.labels.len() == 1 {
            self.node_ids = store.node_ids_by_label(&self.labels[0], self.early_limit);
        } else {
            // Intersection, driven from the *smallest* label set: the answer
            // is a subset of it, so every other label can only remove. That
            // also makes this cheaper than the union it replaces, which always
            // walked all of them.
            //
            // `early_limit` counts nodes that actually match. The union
            // counted insertions, so with a LIMIT it could stop before finding
            // any node carrying all the labels.
            let mut sets: Vec<Vec<NodeId>> = self
                .labels
                .iter()
                .map(|l| store.node_ids_by_label(l, None))
                .collect();
            sets.sort_unstable_by_key(|v| v.len());
            let (smallest, rest) = sets.split_first().expect("labels.len() > 1");
            let rest: Vec<HashSet<NodeId>> =
                rest.iter().map(|v| v.iter().copied().collect()).collect();
            let cap = self.early_limit.unwrap_or(usize::MAX);
            self.node_ids = smallest
                .iter()
                .copied()
                .filter(|id| rest.iter().all(|s| s.contains(id)))
                .take(cap)
                .collect();
        }

        // Sort only when no early_limit (preserves cache locality on full scans).
        if self.early_limit.is_none() {
            self.node_ids.sort_unstable_by_key(|id| id.as_u64());
        }
    }
}

impl PhysicalOperator for NodeScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        // Check early limit
        if let Some(limit) = self.early_limit {
            if self.produced >= limit {
                return Ok(None);
            }
        }

        let node_id = self.node_ids[self.current];
        self.current += 1;
        self.produced += 1;

        let mut record = Record::new();
        record.bind(self.variable.clone(), Value::NodeRef(node_id));

        Ok(Some(record))
    }

    fn try_push_limit(&mut self, n: usize) -> bool {
        // Honor the most restrictive limit if one is already set.
        self.early_limit = Some(match self.early_limit {
            None => n,
            Some(existing) => existing.min(n),
        });
        true
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        // Apply early limit to batch size
        let effective_batch = if let Some(limit) = self.early_limit {
            let remaining = limit.saturating_sub(self.produced);
            if remaining == 0 { return Ok(None); }
            batch_size.min(remaining)
        } else {
            batch_size
        };

        let end = (self.current + effective_batch).min(self.node_ids.len());
        let slice = &self.node_ids[self.current..end];
        self.current = end;

        // Parallel record construction for large batches
        let records: Vec<Record> = if slice.len() >= 1024 {
            let var = &self.variable;
            slice.par_iter().map(|&node_id| {
                let mut record = Record::new();
                record.bind(var.clone(), Value::NodeRef(node_id));
                record
            }).collect()
        } else {
            slice.iter().map(|&node_id| {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                record
            }).collect()
        };
        self.produced += records.len();

        Ok(Some(RecordBatch {
            records,
            columns: vec![self.variable.clone()]
        }))
    }

    fn reset(&mut self) {
        self.current = 0;
        self.produced = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let details = if self.labels.is_empty() {
            format!("var={}, all labels", self.variable)
        } else {
            format!("var={}, labels={:?}", self.variable, self.labels.iter().map(|l| l.as_str()).collect::<Vec<_>>())
        };
        OperatorDescription {
            name: "NodeScan".to_string(),
            details,
            children: Vec::new(),
        }
    }
}

/// Label count operator: O(1) shortcut for `MATCH (n:Label) RETURN count(n)`.
/// Instead of scanning all nodes and counting, reads the count directly from the label index.
pub struct LabelCountOperator {
    labels: Vec<Label>,
    alias: String,
    emitted: bool,
}

impl LabelCountOperator {
    pub fn new(labels: Vec<Label>, alias: String) -> Self {
        Self {
            labels,
            alias,
            emitted: false,
        }
    }
}

impl PhysicalOperator for LabelCountOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        let count = if self.labels.is_empty() {
            store.node_count() as i64
        } else {
            self.labels
                .iter()
                .map(|l| store.label_node_count(l) as i64)
                .min()
                .unwrap_or(0)
        };

        let mut record = Record::new();
        record.bind(
            self.alias.clone(),
            Value::Property(PropertyValue::Integer(count)),
        );
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.emitted = false;
    }

    fn describe(&self) -> OperatorDescription {
        let label_str = if self.labels.is_empty() {
            "all".to_string()
        } else {
            self.labels.iter().map(|l| l.as_str()).collect::<Vec<_>>().join(", ")
        };
        OperatorDescription {
            name: "LabelCount".to_string(),
            details: format!("labels=[{}], alias={}", label_str, self.alias),
            children: Vec::new(),
        }
    }
}

/// Edge count operator: resolves `MATCH ()-[r:TYPE]->() RETURN count(r)` from the stats
/// cache, for one edge type or for all of them.
///
/// The *grouped* form (`RETURN type(r), count(r)`) already had an O(1) path, and node label
/// counts have had one for longer -- but a count filtered to a single edge type fell back to
/// a full Expand + Aggregate. On a billion-edge federation that is the difference between
/// answering from metadata and hitting the 120s timeout (#304), while the structurally
/// simpler grouped query returned instantly.
pub struct EdgeCountOperator {
    /// `None` counts every edge, whatever its type.
    edge_type: Option<String>,
    alias: String,
    executed: bool,
}

impl EdgeCountOperator {
    pub fn new(edge_type: Option<String>, alias: String) -> Self {
        Self { edge_type, alias, executed: false }
    }

    fn count(&self, store: &GraphStore) -> i64 {
        let stats = store.statistics();
        match &self.edge_type {
            Some(t) => stats
                .edge_type_counts
                .iter()
                .find(|(et, _)| et.as_str() == t.as_str())
                .map(|(_, c)| *c as i64)
                .unwrap_or(0),
            None => stats.edge_type_counts.values().map(|c| *c as i64).sum(),
        }
    }
}

impl PhysicalOperator for EdgeCountOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;
        let mut record = Record::new();
        record.bind(
            self.alias.clone(),
            Value::Property(PropertyValue::Integer(self.count(store))),
        );
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "EdgeCount".to_string(),
            details: match &self.edge_type {
                Some(t) => format!("type={}, alias={}", t, self.alias),
                None => format!("all types, alias={}", self.alias),
            },
            children: Vec::new(),
        }
    }
}

/// Edge type count operator: resolves `MATCH ()-[r]->() RETURN type(r), count(r)` from stats cache.
/// Returns one row per edge type with its count, avoiding a full edge scan.
pub struct EdgeTypeCountOperator {
    type_alias: String,
    count_alias: String,
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl EdgeTypeCountOperator {
    pub fn new(type_alias: String, count_alias: String) -> Self {
        Self {
            type_alias,
            count_alias,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }
}

impl PhysicalOperator for EdgeTypeCountOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            let stats = store.statistics();
            let mut records = Vec::new();
            for (edge_type, count) in &stats.edge_type_counts {
                let mut record = Record::new();
                record.bind(
                    self.type_alias.clone(),
                    Value::Property(PropertyValue::String(edge_type.to_string())),
                );
                record.bind(
                    self.count_alias.clone(),
                    Value::Property(PropertyValue::Integer(*count as i64)),
                );
                records.push(record);
            }
            self.results = records.into_iter();
            self.executed = true;
        }
        Ok(self.results.next())
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            let stats = store.statistics();
            let mut records = Vec::new();
            for (edge_type, count) in &stats.edge_type_counts {
                let mut record = Record::new();
                record.bind(
                    self.type_alias.clone(),
                    Value::Property(PropertyValue::String(edge_type.to_string())),
                );
                record.bind(
                    self.count_alias.clone(),
                    Value::Property(PropertyValue::Integer(*count as i64)),
                );
                records.push(record);
            }
            self.results = records.into_iter();
            self.executed = true;
        }
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = self.results.next() {
                batch.push(record);
            } else {
                break;
            }
        }
        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records: batch, columns: Vec::new() }))
        }
    }

    fn reset(&mut self) {
        self.executed = false;
        self.results = Vec::new().into_iter();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "EdgeTypeCount".to_string(),
            details: format!("type_alias={}, count_alias={}", self.type_alias, self.count_alias),
            children: Vec::new(),
        }
    }
}

/// Filter operator: WHERE n.age > 30
/// Rows must be worth at least this much per-row predicate work before a batch
/// is filtered in parallel.
///
/// Fitted to a measured crossover, not chosen. Interleaved A/B over 1,000,000
/// rows, only the threshold varying (#559):
///
/// | predicate                                  | cost | parallel | sequential |
/// |--------------------------------------------|-----:|---------:|-----------:|
/// | `i.v > 500`                                |    2 |  404.8ms | **252.0ms**|
/// | `i.v > 500 AND i.w > 5`                    |    5 |  486.7ms | **327.4ms**|
/// | `i.name CONTAINS "99"`                     |    5 |  525.7ms | **285.8ms**|
/// | `toUpper(i.name) CONTAINS "99"`            |   13 |  512.1ms | **363.8ms**|
/// | 4 conjuncts, two of them string operations |   25 |**461.5ms**|  604.9ms  |
///
/// So the crossover sits between 13 and 25, and 20 puts every measured case on
/// the side that won. The exact figure is host-dependent — it is a ratio
/// between per-row work and cross-core coordination — so it is a threshold
/// with a reproducer rather than a constant to be trusted.
const PARALLEL_PREDICATE_COST: u32 = 20;

/// Roughly what evaluating `expr` costs per row, in units where reading one
/// property is 1.
///
/// This exists because the previous rule went parallel on **batch size**, which
/// says nothing about how much work a predicate does — and with a batch size of
/// 65,536, every batch qualified. A `Record` holds `Arc<str>` binding names, so
/// moving records across threads churns atomic refcounts on cache lines every
/// thread shares; against a predicate as cheap as one comparison there is
/// nothing to amortise that against, and parallel filtering lost 1.4-1.8x on
/// every predicate a real query writes.
///
/// Absolute accuracy does not matter. Only the side of `PARALLEL_PREDICATE_COST`
/// the answer lands on does, so the weights are deliberately coarse.
fn predicate_cost(expr: &Expression) -> u32 {
    match expr {
        // Cost of the elements, plus a little for building the collection.
        Expression::ListExpr(items) => 1 + items.iter().map(predicate_cost).sum::<u32>(),
        Expression::MapExpr(entries) => {
            1 + entries.iter().map(|(_, e)| predicate_cost(e)).sum::<u32>()
        }
        // The unit. A scattered column read plus the match to unwrap it.
        Expression::Property { .. } => 1,
        // Free: already in the record, or in the expression.
        Expression::Literal(_) | Expression::Variable(_) | Expression::Parameter(_) => 0,
        Expression::PathVariable(_) => 0,
        Expression::Binary { left, op, right } => {
            // String comparisons scan and often allocate; numeric ones are a
            // register compare.
            let op_cost = match op {
                BinaryOp::Contains
                | BinaryOp::StartsWith
                | BinaryOp::EndsWith
                | BinaryOp::RegexMatch => 4,
                _ => 1,
            };
            op_cost + predicate_cost(left) + predicate_cost(right)
        }
        Expression::Unary { expr, .. } => 1 + predicate_cost(expr),
        // A call allocates its result and usually its arguments.
        Expression::Function { args, .. } => {
            8 + args.iter().map(predicate_cost).sum::<u32>()
        }
        Expression::Case { operand, when_clauses, else_result } => {
            1 + operand.as_deref().map(predicate_cost).unwrap_or(0)
                + when_clauses.iter().map(|(w, t)| predicate_cost(w) + predicate_cost(t)).sum::<u32>()
                + else_result.as_deref().map(predicate_cost).unwrap_or(0)
        }
        Expression::Index { expr, index } => 1 + predicate_cost(expr) + predicate_cost(index),
        Expression::ListSlice { expr, start, end } => {
            2 + predicate_cost(expr)
                + start.as_deref().map(predicate_cost).unwrap_or(0)
                + end.as_deref().map(predicate_cost).unwrap_or(0)
        }
        // These run a query or a loop per row. Whatever the body costs, the
        // per-row work is large enough that coordination is worth paying.
        Expression::ExistsSubquery { .. } => 50,
        Expression::ListComprehension { list_expr, filter, map_expr, .. } => {
            20 + predicate_cost(list_expr)
                + filter.as_deref().map(predicate_cost).unwrap_or(0)
                + predicate_cost(map_expr)
        }
        Expression::PredicateFunction { list_expr, predicate, .. } => {
            20 + predicate_cost(list_expr) + predicate_cost(predicate)
        }
        Expression::Reduce { init, list_expr, expression, .. } => {
            20 + predicate_cost(init) + predicate_cost(list_expr) + predicate_cost(expression)
        }
        Expression::PatternComprehension { .. } => 50,
    }
}

pub struct FilterOperator {
    /// Input operator
    input: OperatorBox,
    /// Predicate expression
    predicate: Expression,
    /// Whether this predicate is expensive enough per row to be worth filtering
    /// across threads. Computed once, from the expression, not from batch size.
    parallel: bool,
}

impl FilterOperator {
    /// Create a new filter operator
    pub fn new(input: OperatorBox, predicate: Expression) -> Self {
        let parallel = Self::predicate_is_parallel(&predicate);
        Self { input, predicate, parallel }
    }

    /// Whether this predicate is worth filtering across threads.
    ///
    /// `SAMYAMA_FILTER_PARALLEL_COST` overrides the threshold. It exists so
    /// `benches/filter_throughput.rs` can run both sides **interleaved in one
    /// process**, which is the only way to A/B this reliably: the effect is a
    /// ratio between per-row work and cross-core coordination, so it moves with
    /// the host, and comparing two separate benchmark runs measured 16% drift
    /// on an otherwise idle dedicated box (#529). A threshold fitted from
    /// across-run numbers would be fitted to noise.
    ///
    /// Unset in normal use. `0` forces parallel, a large value forces
    /// sequential.
    pub fn predicate_is_parallel(predicate: &Expression) -> bool {
        let threshold = std::env::var("SAMYAMA_FILTER_PARALLEL_COST")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(PARALLEL_PREDICATE_COST);
        predicate_cost(predicate) >= threshold
    }


    fn evaluate_predicate(&self, record: &Record, _store: &GraphStore) -> ExecutionResult<bool> {
        let result = self.evaluate_expression(&self.predicate, record, _store)?;

        match result {
            Value::Property(PropertyValue::Boolean(b)) => Ok(b),
            Value::Null | Value::Property(PropertyValue::Null) => Ok(false),
            _ => Err(ExecutionError::TypeError("Predicate must evaluate to boolean".to_string())),
        }
    }

    fn evaluate_expression(&self, expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            // Delegates rather than adding a sixth copy of this logic; the
            // standalone evaluator is the one implementation (#654).
            Expression::ListExpr(_) | Expression::MapExpr(_) => {
                eval_expression(expr, record, store)
            }
            Expression::Variable(var) => {
                record.get(var)
                    .cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Property { variable, property } => {
                return read_property(record, variable, property, store, false);
                #[allow(unreachable_code)]
                Ok(Value::Null)
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = self.evaluate_expression(left, record, store)?;
                let right_val = self.evaluate_expression(right, record, store)?;
                self.evaluate_binary_op(op, left_val, right_val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args.iter()
                    .map(|a| self.evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Unary { op, expr } => {
                let val = self.evaluate_expression(expr, record, store)?;
                match op {
                    UnaryOp::IsNull => {
                        let is_null = matches!(val, Value::Null | Value::Property(PropertyValue::Null));
                        Ok(Value::Property(PropertyValue::Boolean(is_null)))
                    }
                    UnaryOp::IsNotNull => {
                        let is_null = matches!(val, Value::Null | Value::Property(PropertyValue::Null));
                        Ok(Value::Property(PropertyValue::Boolean(!is_null)))
                    }
                    UnaryOp::Not => {
                        match val {
                            Value::Property(PropertyValue::Boolean(b)) => Ok(Value::Property(PropertyValue::Boolean(!b))),
                            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                            _ => return Err(ExecutionError::TypeError("NOT requires boolean".to_string())),
                        }
                    }
                    UnaryOp::Minus => {
                        match val {
                            Value::Property(PropertyValue::Integer(i)) => Ok(Value::Property(PropertyValue::Integer(-i))),
                            Value::Property(PropertyValue::Float(f)) => Ok(Value::Property(PropertyValue::Float(-f))),
                            Value::Null | Value::Property(PropertyValue::Null) => Ok(Value::Property(PropertyValue::Null)),
                            _ => Err(ExecutionError::TypeError("Negation requires numeric type".to_string())),
                        }
                    }
                }
            }
            Expression::Case { operand, when_clauses, else_result } => {
                eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), |e| self.evaluate_expression(e, record, store))
            }
            Expression::Index { expr, index } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let idx = self.evaluate_expression(index, record, store)?;
                eval_index(collection, idx, store)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let s = match start { Some(s) => Some(self.evaluate_expression(s, record, store)?), None => None };
                let en = match end { Some(e) => Some(self.evaluate_expression(e, record, store)?), None => None };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery { pattern, where_clause, .. } => {
                eval_exists_subquery(pattern, where_clause.as_deref(), record, store)
            }
            Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
                eval_list_comprehension(variable, list_expr, filter.as_deref(), map_expr, record, store)
            }
            Expression::PredicateFunction { name, variable, list_expr, predicate } => {
                eval_predicate_function(name, variable, list_expr, predicate, record, store)
            }
            Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
                eval_reduce(accumulator, init, variable, list_expr, expression, record, store)
            }
            Expression::PatternComprehension { pattern, filter, projection } => {
                eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store)
            }
            Expression::PathVariable(var) => {
                record.get(var).cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned()
                    .ok_or_else(|| ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name)))
            }
        }
    }

    /// The filter's binary operator, **delegated** to `eval_binary_op` (#860).
    ///
    /// This was a second, drifted implementation -- 67 lines against 346 -- and
    /// its own comment already said so: *"Note this is a second comparison
    /// implementation, the two must agree, and did not."* It agreed on the easy
    /// things and diverged on every rule added since, so a `WHERE` attached to a
    /// `MATCH` quietly used a weaker comparison engine than the same expression
    /// in a `RETURN` or after a `WITH`:
    ///
    /// ```text
    /// MATCH ()-[a]->() MATCH ()-[b]->() WHERE a = b   TypeError
    /// MATCH ()-[a]->() MATCH ()-[b]->() RETURN a = b  true
    /// ```
    ///
    /// It was missing relationship and path identity, the three-valued list and
    /// map equality of `cypher_equals`, the NaN and list ordering rules, and
    /// integer-float equality -- every comparison rule fixed this cycle applied
    /// everywhere *except* the clause most queries filter in.
    ///
    /// The seventeen `coerced_eq` / `compare_*` / `arithmetic_*` helpers it
    /// called are **deleted**, not left unused. One of them carried a rule
    /// Cypher does not have -- a String/Boolean coercion that made
    /// `i.active = 'true'` match the boolean `true` -- and keeping them as dead
    /// code invites the next change to route through them again, which is how
    /// the two engines drifted apart in the first place.
    fn evaluate_binary_op(&self, op: &BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
        eval_binary_op(op, left, right)
    }

    // evaluate_function removed — FilterOperator now delegates to global eval_function
}

impl PhysicalOperator for FilterOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn filter_predicate(&self) -> Option<&Expression> {
        Some(&self.predicate)
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next(store)? {
            if self.evaluate_predicate(&record, store)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    // A pass-through operator's default `next_mut` delegates to `next`, which
    // reads its input read-only -- so a write beneath a FILTER refused outright
    // with "requires mutable store access". Same defect class as #649, which
    // fixed it for SKIP and LIMIT and named them "the last two pass-through
    // operators that still had it"; SORT and FILTER also had it (#866).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next_mut(store, tenant_id)? {
            if self.evaluate_predicate(&record, store)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut filtered_records = Vec::new();

        while filtered_records.len() < batch_size {
            if let Some(batch) = self.input.next_batch(store, batch_size)? {
                let records = batch.records;
                // Parallel only where the predicate is expensive enough to pay
                // for moving records across threads (#559). A small batch is
                // still not worth splitting whatever the predicate costs.
                if self.parallel && records.len() >= 256 {
                    let predicate = self.predicate.clone();
                    let passed: Vec<Record> = records.into_par_iter()
                        .filter(|record| {
                            eval_predicate_standalone(&predicate, record, store).unwrap_or(false)
                        })
                        .collect();
                    filtered_records.extend(passed);
                } else {
                    for record in records {
                        if self.evaluate_predicate(&record, store)? {
                            filtered_records.push(record);
                        }
                    }
                }
            } else {
                break;
            }
        }

        if filtered_records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: filtered_records,
                columns: Vec::new(), // Filter doesn't change columns
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Filter".to_string(),
            details: format_expression(&self.predicate),
            children: vec![self.input.describe()],
        }
    }
}

/// Expand operator: `-[:KNOWS]->`
/// The path the walk has built so far, with this hop added.
///
/// Every expand in a chain binds the *same* path variable, and each one used
/// to bind a fresh two-node path for its own hop — so the last hop won and
/// `MATCH p = (a)-[:R]->(b)-[:R]->(c)` produced a path of two nodes with
/// `length(p) = 1` (#631). The variable-length expand assembles its whole path
/// in one place and never had the defect; the fixed multi-hop spelling is
/// built hop by hop, so each hop has to add to what came before.
///
/// A path is only continued when it actually ends where this hop starts.
/// Anything else — a comma-separated pattern, a path variable rebound across
/// disconnected parts — starts afresh rather than inventing an edge between
/// two unrelated nodes.
fn extend_path(
    base: Option<&Value>,
    source_id: NodeId,
    target_id: NodeId,
    edge_id: crate::graph::EdgeId,
) -> Value {
    if let Some(Value::Path { nodes, edges }) = base {
        if nodes.last() == Some(&source_id) {
            let mut nodes = nodes.clone();
            let mut edges = edges.clone();
            nodes.push(target_id);
            edges.push(edge_id);
            return Value::Path { nodes, edges };
        }
    }
    Value::Path {
        nodes: vec![source_id, target_id],
        edges: vec![edge_id],
    }
}

/// Per-direction type indexes for one expand: `(outgoing, incoming)`.
///
/// `None` = not yet asked. `Some(None)` = asked and declined, so the walk
/// stands and is not re-asked per row.
type TypeIndexPair = (
    Option<std::sync::Arc<crate::graph::TypeAdjacency>>,
    Option<std::sync::Arc<crate::graph::TypeAdjacency>>,
);
type TypeIndexSlot = Option<Option<TypeIndexPair>>;

pub struct ExpandOperator {
    /// Input operator
    input: OperatorBox,
    /// Source variable.
    ///
    /// `Arc<str>` rather than `String` because `Record::bind` takes
    /// `impl Into<Arc<str>>`: passing a `String` allocates twice per bind --
    /// once to clone the `String`, once to build the `Arc<str>` from it -- for
    /// a name that is fixed for the whole query. On IC5 that was 3.4 million
    /// allocations for the target variable alone (#564).
    source_var: Arc<str>,
    /// Target variable
    target_var: Arc<str>,
    /// Edge variable (optional)
    edge_var: Option<Arc<str>>,
    /// Edge types to expand (empty = all types)
    edge_types: Vec<String>,
    /// Target node labels to filter (empty = any label)
    target_labels: Vec<Label>,
    /// Equality predicates on the *target* node, checked during the adjacency
    /// walk rather than after it.
    ///
    /// LDBC IC11 is the case this exists for. Its plan is
    /// `Filter(org.name = ...) <- Expand((friend)-[:WORK_AT]->(org))`, so
    /// every friend-of-friend's employer was materialised into a record and
    /// then discarded — 11.5x Neo4j, and the only complex read still outside
    /// the PERF-01 target. Applied here, a non-matching employer never
    /// becomes a row (#656).
    target_props: Vec<(String, PropertyValue)>,
    /// The variables to bind to null when a source record matches nothing.
    ///
    /// `Some` marks this expand as an `OPTIONAL MATCH`: a source row that finds
    /// no edge still produces a row, with the variables the clause introduces
    /// set to null. `None` is an ordinary expand, where such a row disappears.
    ///
    /// Only the variables the clause *introduces* are listed. In
    /// `OPTIONAL MATCH (op)-[k:KNOWS]-(author)` with both endpoints already
    /// bound, that is `k` alone — nulling `author` would erase a binding the
    /// row already had (#726).
    optional_null_vars: Option<Vec<String>>,
    /// Whether the current source record has emitted anything yet.
    emitted_for_current: bool,
    /// The exact set of nodes the target may be, resolved once at plan time.
    ///
    /// `target_props` above still costs a `get_node` and a property compare per
    /// candidate edge. On LDBC IC11 that is ~29,000 lookups at ~10us each and
    /// the expand became 74% of the query — the filter moved earlier and got
    /// *more* expensive per candidate. Resolving `org.name = "..."` to the one
    /// matching Organisation once, and testing membership here, replaces that
    /// with a hash lookup (#665).
    target_ids: Option<std::collections::HashSet<NodeId>>,
    /// Source records this expand has processed, for amortising the type index.
    ///
    /// `GraphStore::type_adjacency` costs one pass over the adjacency to build.
    /// That is a large loss for a query touching a handful of rows and a large
    /// win for one touching thousands, and the operator is the only place that
    /// knows which it is — so it counts, and asks only once the walk it would
    /// replace has clearly become the dominant cost (#738).
    rows_seen: usize,
    /// The type index for this expand's single edge type, once requested.
    /// `Some(None)` means asked and declined, so it is not asked again.
    #[allow(clippy::type_complexity)]
    type_index: TypeIndexSlot,
    /// Direction
    direction: Direction,
    /// Current input record
    current_record: Option<Record>,
    /// Current edges as `(EdgeId, source, target)`.
    ///
    /// The edge *type* is deliberately absent. It is read only when the
    /// pattern binds an edge variable, and resolving it means cloning a
    /// `String`; carrying it here cost one clone per surviving edge — 409,960
    /// of them on LDBC IC9, whose pattern binds no edge variable at all
    /// (#520).
    current_edges: Vec<(crate::graph::EdgeId, NodeId, NodeId)>,
    /// Current edge index
    edge_index: usize,
    /// Path variable name for named paths (CY-04)
    path_variable: Option<String>,
    /// `edge_types` resolved to interned ids, cached after the first use.
    /// `Some(vec)` once resolved; the wildcard case never populates it.
    type_ids: Option<Vec<u16>>,
    /// Enforce relationship isomorphism: refuse an edge this pattern has
    /// already traversed, and record the ones it takes (#684).
    ///
    /// Off for a single-hop pattern, which cannot violate the rule and should
    /// not pay for the bookkeeping.
    track_edges: bool,
    /// Pin the target to whatever this variable is bound to on the current
    /// row, resolved per input record rather than at plan time.
    ///
    /// A pattern that closes back onto a bound variable — LDBC BI-17's
    /// `(a)-[:KNOWS]-(b)-[:KNOWS]-(c)-[:KNOWS]-(a)` — is planned as an expand
    /// into a synthetic `__self_a_2` followed by `__self_a_2 = a`. That is
    /// correct and enumerates every neighbour of `c` to keep one: ~41 per
    /// person on SF1, over ~17.8M candidate paths. Pinning here rejects the
    /// other 40 during the adjacency walk, before a record exists (#195).
    ///
    /// Distinct from `target_ids`, which is a plan-time constant set; this one
    /// changes with every row.
    target_bound_var: Option<Arc<str>>,
    /// This expand begins a MATCH clause, so it clears the inherited history
    /// first. Isomorphism is per-clause — `MATCH (a)-[:R]-(b) MATCH (b)-[:R]-(c)`
    /// may legitimately reuse the edge.
    starts_clause: bool,
}

impl ExpandOperator {
    /// Create a new expand operator
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        edge_var: Option<String>,
        edge_types: Vec<String>,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            source_var: source_var.into(),
            target_var: target_var.into(),
            edge_var: edge_var.map(Into::into),
            edge_types,
            target_labels: Vec::new(),
            target_props: Vec::new(),
            target_ids: None,
            rows_seen: 0,
            type_index: None,
            optional_null_vars: None,
            emitted_for_current: false,
            track_edges: false,
            starts_clause: false,
            target_bound_var: None,
            direction,
            current_record: None,
            current_edges: Vec::new(),
            edge_index: 0,
            path_variable: None,
            type_ids: None,
        }
    }

    /// Set path variable for named path materialization (CY-04)
    pub fn with_path_variable(mut self, var: String) -> Self {
        self.path_variable = Some(var.into());
        self
    }

    /// Set target node labels to filter during expansion
    pub fn with_target_labels(mut self, labels: Vec<Label>) -> Self {
        self.target_labels = labels;
        self
    }

    /// Equality predicates the target node must satisfy, applied during the
    /// walk. Additive: the planner leaves its own filter in place, so this can
    /// only reduce what is materialised, never change what is returned.
    pub fn with_target_props(mut self, props: Vec<(String, PropertyValue)>) -> Self {
        self.target_props = props;
        self
    }

    /// The resolved node set for the target, when the planner could compute it.
    /// Preferred over `target_props`: an id membership test costs a hash
    /// lookup where the property check costs a node fetch (#665).
    /// Pin this expand's target to the node already bound to `var`.
    pub fn with_target_bound_var(mut self, var: String) -> Self {
        self.target_bound_var = Some(var.into());
        self
    }

    /// Rows after which the type index is worth its build.
    ///
    /// Below this the walk is cheaper than one pass over the adjacency; above
    /// it, decisively not. IC11 feeds this expand 13,306 rows.
    const TYPE_INDEX_AFTER_ROWS: usize = 512;

    /// Enforce relationship isomorphism for this expand.
    ///
    /// `starts_clause` marks the first expand of a MATCH clause, which drops
    /// any history inherited from an earlier clause.
    pub fn with_edge_isolation(mut self, starts_clause: bool) -> Self {
        self.track_edges = true;
        self.starts_clause = starts_clause;
        self
    }

    /// Make this expand an `OPTIONAL MATCH`: a source row that matches nothing
    /// still emits, with `null_vars` bound to null.
    pub fn with_optional(mut self, null_vars: Vec<String>) -> Self {
        self.optional_null_vars = Some(null_vars);
        self
    }

    pub fn with_target_ids(mut self, ids: std::collections::HashSet<NodeId>) -> Self {
        self.target_ids = Some(ids);
        self
    }

    /// The edge-type filter as interned ids, resolved once per query.
    ///
    /// `None` means the pattern named no types — the wildcard. A pattern that
    /// named types none of which exist returns `Some(empty)`, which matches
    /// nothing; conflating the two makes `-[:NO_SUCH_TYPE]->` follow every
    /// edge in the graph (#520).
    fn ensure_type_ids(&mut self, store: &GraphStore) {
        if self.edge_types.is_empty() || self.type_ids.is_some() {
            return;
        }
        let ids = self
            .edge_types
            .iter()
            .filter_map(|t| store.edge_type_id(&EdgeType::new(t.as_str())))
            .collect();
        self.type_ids = Some(ids);
    }

    /// The null-filled row an `OPTIONAL MATCH` owes a source record that
    /// matched nothing, or `None` if it owes nothing.
    ///
    /// Consumes `current_record`, so it fires at most once per source row; the
    /// caller then falls through to pulling the next input.
    fn take_unmatched_optional_row(&mut self) -> Option<Record> {
        let null_vars = self.optional_null_vars.as_ref()?;
        if self.emitted_for_current {
            return None;
        }
        let base = self.current_record.take()?;
        let mut rec = base.clone_with_capacity(null_vars.len());
        for v in null_vars {
            rec.bind(v.clone(), Value::Null);
        }
        self.emitted_for_current = true;
        Some(rec)
    }

    fn load_edges(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<()> {
        let source_val = record.get(&self.source_var)
            .ok_or_else(|| ExecutionError::VariableNotFound(self.source_var.to_string()))?;

        // Expanding from null yields nothing; it is not an error. An
        // `OPTIONAL MATCH` that matched nothing binds null, and a following
        // `MATCH (a)-->(b)` on that row must simply produce no rows — Cypher
        // says so, and raising "a is not a node" fails the whole query over a
        // row that should quietly disappear (#671).
        if matches!(source_val, Value::Null) || matches!(source_val.as_property(), Some(PropertyValue::Null)) {
            self.current_edges.clear();
            self.edge_index = 0;
            return Ok(());
        }

        let node_id = source_val.node_id()
            .ok_or_else(|| ExecutionError::TypeError(format!("{} is not a node", self.source_var)))?;

        // Filter on the interned edge-type id *during* the adjacency walk, and
        // resolve the `EdgeType` string only for the edges that survive.
        //
        // This used to materialise every incident edge as
        // `(EdgeId, NodeId, NodeId, EdgeType)` -- cloning a type string per
        // edge -- and filter the resulting Vec by comparing those strings. An
        // LDBC `Person` has ~41 `KNOWS` edges and ~900 others (inbound
        // `HAS_CREATOR` from every post and comment they wrote, `LIKES`,
        // `HAS_MEMBER`, `HAS_INTEREST`), so a `[:KNOWS]` expansion cloned ~900
        // strings to keep 41 (#520).
        self.ensure_type_ids(store);

        // Refill the existing buffer. Allocating a fresh `Vec` per source
        // record meant one allocation plus roughly log2(degree) reallocations
        // as it doubled, and a free of the previous one -- once per source, for
        // a buffer that is the same shape every time (#564).
        let mut collected = std::mem::take(&mut self.current_edges);
        collected.clear();
        let type_filter = self.type_ids.as_deref();

        // Target-label sets, resolved once per source record rather than per
        // edge, and applied *during* the walk rather than by a `retain`
        // afterwards.
        //
        // The old code collected every incident edge and then retained the ones
        // whose target carried the labels, testing each with
        // `get_node(id).has_label(label)` -- a `Vec` index, a version-chain
        // walk, a 128-byte `Node`, and a `HashSet<Label>` probe hashing a
        // *string*. At 2.22M edges visited per LDBC IC9 run that was **26.7% of
        // the profile**, the single largest symbol, ahead of every property
        // read. Probing `label_index` by `NodeId` instead is one hash of a u64
        // (#592).
        //
        // A label no node carries yields `None`, which matches nothing -- so
        // the whole expansion is empty, which is correct and is why the empty
        // case is distinguished from "no labels required".
        // ...and by a bit rather than a hash, since #592's `HashSet<NodeId>`
        // probe is itself a random access into a structure the size of the
        // label. Measured, that grows 10.2 -> 36.7 ns per candidate edge as the
        // label goes from 300k to 1.2M nodes — 35% of the whole traversal —
        // while the storage walk under it stays flat (#730).
        let label_sets: Option<Vec<std::sync::Arc<Vec<u64>>>> =
            if self.target_labels.is_empty() {
                None
            } else {
                Some(
                    self.target_labels
                        .iter()
                        .map(|l| store.label_bitset(l))
                        .collect::<Option<Vec<_>>>()
                        .unwrap_or_default(),
                )
            };
        let target_props = &self.target_props;
        let target_ids = self.target_ids.as_ref();
        // Relationship isomorphism (#684): an edge this pattern already walked
        // is not a candidate. Checked here, during the adjacency walk, so a
        // rejected edge never becomes a record.
        //
        // Filtered to the edges this expand could actually walk. An edge of a
        // type outside `type_filter` is not a candidate for re-traversal, so
        // keeping it only lengthens a `contains` that runs **per candidate
        // edge**. On LDBC IC6 the clause walks `HAS_TAG`, `HAS_CREATOR` and
        // `KNOWS`, so each expand inherits edges it could never take; the same
        // reasoning applied to `VarLengthExpandOperator` is what took IC6 from
        // forty minutes back to 309 ms (#734).
        let used_owned: Vec<crate::graph::EdgeId>;
        let used_edges: &[crate::graph::EdgeId] = if self.track_edges && !self.starts_clause {
            let inherited = record.used_edge_slice();
            if inherited.is_empty() {
                &[]
            } else {
                used_owned = inherited
                    .iter()
                    .copied()
                    .filter(|&e| store.edge_traversable_by(e, type_filter))
                    .collect();
                &used_owned
            }
        } else {
            &[]
        };
        // Resolved once per input record: the node the pattern must close on.
        let pinned_target: Option<NodeId> = self
            .target_bound_var
            .as_ref()
            .and_then(|v| record.get(v))
            .and_then(|v| v.node_id());
        let keeps = |target: NodeId, eid: crate::graph::EdgeId| -> bool {
            // A closing hop can only land on the node it closes onto.
            if let Some(p) = pinned_target {
                if target != p {
                    return false;
                }
            }
            // Relationship isomorphism first: it is a comparison of a few u64s
            // against a slice that is empty for every single-hop pattern, so it
            // is cheaper than anything below and rejects the most rows on the
            // patterns that need it.
            if used_edges.contains(&eid) {
                return false;
            }
            // Then the cheapest discriminator: if the planner resolved the
            // target to a known set, membership settles it without touching the node.
            if let Some(ids) = target_ids {
                if !ids.contains(&target) {
                    return false;
                }
            }
            let label_ok = match &label_sets {
                None => true,
                // `Some(empty)` means a required label exists on no node.
                Some(sets) if sets.len() < self.target_labels.len() => false,
                Some(sets) => sets
                    .iter()
                    .all(|s| GraphStore::bitset_contains(s, target)),
            };
            if !label_ok {
                return false;
            }
            if target_props.is_empty() {
                return true;
            }
            match store.get_node(target) {
                Some(node) => target_props
                    .iter()
                    .all(|(k, v)| node.get_property(k).map_or(false, |p| p == v)),
                None => false,
            }
        };

        // A selective type against a high-degree node is the case #738 is
        // about: IC11 visits ~6.6M edges to use ~29,000, because an LDBC
        // `Person` has ~495 outgoing edges of which 2.2 are `WORK_AT`. Once
        // enough rows have gone through to pay for it, ask the store for an
        // index of just this type and walk that instead.
        //
        // Only for a single type: with two, the union would have to be merged
        // and the saving no longer obviously beats the walk.
        let single_type = match type_filter {
            Some([t]) => Some(*t),
            _ => None,
        };
        self.rows_seen += 1;
        if self.type_index.is_none() && self.rows_seen > Self::TYPE_INDEX_AFTER_ROWS {
            if let Some(t) = single_type {
                // An undirected pattern walks both sides, so it needs both
                // indexes or neither — half an index would mean half the walk
                // takes the fast path and the accounting for self-loops below
                // stops lining up.
                let out = store.type_adjacency(t, true);
                let inc = match self.direction {
                    Direction::Outgoing => None,
                    _ => store.type_adjacency(t, false),
                };
                let usable = match self.direction {
                    Direction::Outgoing => out.is_some(),
                    Direction::Incoming => inc.is_some(),
                    Direction::Both => out.is_some() && inc.is_some(),
                };
                self.type_index = Some(if usable { Some((out, inc)) } else { None });
            }
        }
        let typed = match (&self.type_index, single_type) {
            (Some(Some(pair)), Some(_)) => Some(pair.clone()),
            _ => None,
        };
        let empty: [(NodeId, crate::graph::EdgeId); 0] = [];
        let out_of = |p: &Option<TypeIndexPair>, n: NodeId| -> Vec<(NodeId, crate::graph::EdgeId)> {
            match p { Some((Some(i), _)) => i.neighbors(n).to_vec(), _ => empty.to_vec() }
        };
        let in_of = |p: &Option<TypeIndexPair>, n: NodeId| -> Vec<(NodeId, crate::graph::EdgeId)> {
            match p { Some((_, Some(i))) => i.neighbors(n).to_vec(), _ => empty.to_vec() }
        };

        match self.direction {
            Direction::Outgoing => {
                if typed.is_some() {
                    for (target, eid) in out_of(&typed, node_id) {
                        if keeps(target, eid) {
                            collected.push((eid, node_id, target));
                        }
                    }
                } else {
                    store.for_each_outgoing_neighbor(node_id, type_filter, |target, eid| {
                        if keeps(target, eid) {
                            collected.push((eid, node_id, target));
                        }
                    });
                }
            }
            Direction::Incoming => {
                if typed.is_some() {
                    for (source, eid) in in_of(&typed, node_id) {
                        if keeps(source, eid) {
                            collected.push((eid, source, node_id));
                        }
                    }
                } else {
                    store.for_each_incoming_neighbor(node_id, type_filter, |source, eid| {
                        if keeps(source, eid) {
                            collected.push((eid, source, node_id));
                        }
                    });
                }
            }
            Direction::Both if typed.is_some() => {
                for (target, eid) in out_of(&typed, node_id) {
                    if keeps(target, eid) {
                        collected.push((eid, node_id, target));
                    }
                }
                for (source, eid) in in_of(&typed, node_id) {
                    // Same self-loop rule as the walk below: an edge incident
                    // to its own node appears in both indexes and must be
                    // taken once (#640).
                    if source == node_id {
                        continue;
                    }
                    if keeps(source, eid) {
                        collected.push((eid, source, node_id));
                    }
                }
            }
            Direction::Both => {
                store.for_each_outgoing_neighbor(node_id, type_filter, |target, eid| {
                    if keeps(target, eid) {
                        collected.push((eid, node_id, target));
                    }
                });
                store.for_each_incoming_neighbor(node_id, type_filter, |source, eid| {
                    // A self-relationship is incident to its node twice -- once
                    // outgoing, once incoming -- and the walk above has already
                    // taken it. Undirected matching traverses each
                    // relationship once, so `MATCH ()--()` over a single
                    // `(a)-[:LOOP]->(a)` is one match and not two (#640).
                    if source == node_id {
                        return;
                    }
                    if keeps(source, eid) {
                        collected.push((eid, source, node_id));
                    }
                });
            }
        }

        // No `retain` here any more: the labels were applied during the walk
        // above, so a non-matching edge was never pushed. The retain also
        // compacted the vector, and its `Direction::Both` arm ran
        // `store.get_node(node_id)` per edge purely to recover a value it
        // already had (#592).
        self.current_edges = collected;

        self.edge_index = 0;
        Ok(())
    }
}

impl PhysicalOperator for ExpandOperator {
    // A write beneath this operator refused with "requires mutable store
    // access", because the default `next_mut` delegates to `next` and `next`
    // reads its input read-only. Shared body rather than a second, mutable copy
    // of this operator's own logic -- see `drain_input_for_write` (#870).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        drain_input_for_write(&mut self.input, store, tenant_id)?;
        self.next(store)
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            // If we have edges from current record, return them
            if self.edge_index < self.current_edges.len() {
                let (edge_id, src, tgt) = self.current_edges[self.edge_index];
                self.edge_index += 1;

                // Room for the target, and for the edge and path variables when
                // the pattern names them -- otherwise the first bind below
                // reallocates a Vec that was cloned at exact capacity (#562).
                let extra = 1
                    + self.edge_var.is_some() as usize
                    + self.path_variable.is_some() as usize;
                let mut new_record =
                    self.current_record.as_ref().unwrap().clone_with_capacity(extra);

                // Determine target node based on direction
                let target_id = match self.direction {
                    Direction::Outgoing => tgt,
                    Direction::Incoming => src,
                    Direction::Both => {
                        let source_val = new_record.get(&self.source_var).unwrap();
                        let source_id = source_val.node_id().unwrap();
                        if src == source_id { tgt } else { src }
                    }
                };

                new_record.bind(self.target_var.clone(), Value::NodeRef(target_id));

                if let Some(edge_var) = &self.edge_var {
                    // Resolved here rather than carried: only a pattern that
                    // names the edge ever reads its type.
                    let edge_type = store
                        .get_edge_type(edge_id)
                        .unwrap_or_else(|| EdgeType::new(""));
                    new_record.bind(edge_var.clone(), Value::EdgeRef(edge_id, src, tgt, edge_type));
                }

                // CY-04: Materialize named path variable
                if let Some(ref path_var) = self.path_variable {
                    let source_id = new_record.get(&self.source_var)
                        .and_then(|v| v.node_id())
                        .unwrap_or(src);
                    let extended =
                        extend_path(new_record.get(path_var), source_id, target_id, edge_id);
                    new_record.bind(path_var.clone(), extended);
                }

                // Relationship isomorphism (#684): remember what this pattern
                // has walked so a later segment cannot take the same edge back.
                if self.track_edges {
                    if self.starts_clause {
                        new_record.clear_used_edges();
                    }
                    new_record.mark_edge_used(edge_id);
                }

                self.emitted_for_current = true;
                return Ok(Some(new_record));
            }

            // An OPTIONAL MATCH source row that matched nothing still emits,
            // once, with the variables this clause introduces set to null.
            if let Some(row) = self.take_unmatched_optional_row() {
                return Ok(Some(row));
            }

            // Need new input record
            if let Some(record) = self.input.next(store)? {
                self.current_record = Some(record.clone());
                self.emitted_for_current = false;
                self.load_edges(&record, store)?;
            } else {
                return Ok(None);
            }
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut expanded_records = Vec::with_capacity(batch_size);

        while expanded_records.len() < batch_size {
            // If we have edges from current record, process them
            if self.edge_index < self.current_edges.len() {
                let take = (batch_size - expanded_records.len()).min(self.current_edges.len() - self.edge_index);

                for i in 0..take {
                    let (edge_id, src, tgt) = self.current_edges[self.edge_index + i];
                    // Room for the target, and for the edge and path variables when
                // the pattern names them -- otherwise the first bind below
                // reallocates a Vec that was cloned at exact capacity (#562).
                let extra = 1
                    + self.edge_var.is_some() as usize
                    + self.path_variable.is_some() as usize;
                let mut new_record =
                    self.current_record.as_ref().unwrap().clone_with_capacity(extra);

                    let target_id = match self.direction {
                        Direction::Outgoing => tgt,
                        Direction::Incoming => src,
                        Direction::Both => {
                            let source_val = new_record.get(&self.source_var).unwrap();
                            let source_id = source_val.node_id().unwrap();
                            if src == source_id { tgt } else { src }
                        }
                    };

                    new_record.bind(self.target_var.clone(), Value::NodeRef(target_id));
                    if let Some(edge_var) = &self.edge_var {
                        // Resolved here rather than carried: only a pattern
                        // that names the edge ever reads its type.
                        let edge_type = store
                            .get_edge_type(edge_id)
                            .unwrap_or_else(|| EdgeType::new(""));
                        new_record.bind(edge_var.clone(), Value::EdgeRef(edge_id, src, tgt, edge_type));
                    }
                    // CY-04: Materialize named path variable in batch mode
                    if let Some(ref path_var) = self.path_variable {
                        let source_id = new_record.get(&self.source_var)
                            .and_then(|v| v.node_id())
                            .unwrap_or(src);
                        let extended =
                            extend_path(new_record.get(path_var), source_id, target_id, edge_id);
                        new_record.bind(path_var.clone(), extended);
                    }
                    // Same isomorphism bookkeeping as the single-row path
                    // above. Missing it here would make the answer depend on
                    // whether the plan happened to run batched (#684).
                    if self.track_edges {
                        if self.starts_clause {
                            new_record.clear_used_edges();
                        }
                        new_record.mark_edge_used(edge_id);
                    }
                    expanded_records.push(new_record);
                }
                self.edge_index += take;
                self.emitted_for_current = true;
            } else {
                // Same rule as the single-row path: an OPTIONAL MATCH source
                // row that matched nothing still emits once. Missing it here
                // would make the answer depend on whether the plan happened to
                // run batched, which is the shape of #684.
                if let Some(row) = self.take_unmatched_optional_row() {
                    expanded_records.push(row);
                    continue;
                }
                // Need new input record
                if let Some(record) = self.input.next(store)? {
                    self.current_record = Some(record.clone());
                    self.emitted_for_current = false;
                    self.load_edges(&record, store)?;
                } else {
                    break;
                }
            }
        }

        if expanded_records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records: expanded_records,
                columns: Vec::new(), // Columns determined by output variables
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.current_record = None;
        self.current_edges.clear();
        self.edge_index = 0;
        // Without this, a re-run would think the first source record had
        // already emitted and would swallow its null row (#726).
        self.emitted_for_current = false;
    }

    fn describe(&self) -> OperatorDescription {
        let dir_str = match self.direction {
            Direction::Outgoing => format!("({})-[:{}]->({})", self.source_var, if self.edge_types.is_empty() { "*".to_string() } else { self.edge_types.join("|") }, self.target_var),
            Direction::Incoming => format!("({})<-[:{}]-({})", self.source_var, if self.edge_types.is_empty() { "*".to_string() } else { self.edge_types.join("|") }, self.target_var),
            Direction::Both => format!("({})--[:{}]--({})", self.source_var, if self.edge_types.is_empty() { "*".to_string() } else { self.edge_types.join("|") }, self.target_var),
        };
        OperatorDescription {
            name: "Expand".to_string(),
            details: dir_str,
            children: vec![self.input.describe()],
        }
    }
}

/// Variable-length expand operator: `(a)-[:R*min..max]-(b)`.
///
/// For each input record it performs a breadth-first traversal from the source
/// node along the given direction/edge-types and emits one output record per
/// **distinct** target node reachable in `[min, max]` hops (BFS guarantees the
/// shortest depth is seen first, so each target is emitted once). This is the
/// node-reachability semantics relied on by traversal/failure-propagation
/// queries; enumerating every distinct path is the job of `shortestPath` /
/// `allShortestPaths` instead.
///
/// `target_labels` restrict only the *emitted* endpoint (intermediate nodes are
/// unrestricted, matching Cypher). An optional `path_variable` is materialized
/// with the BFS route (shortest path source→target).
pub struct VarLengthExpandOperator {
    input: OperatorBox,
    source_var: String,
    target_var: String,
    edge_types: Vec<String>,
    target_labels: Vec<Label>,
    direction: Direction,
    min_hops: usize,
    max_hops: usize,
    path_variable: Option<String>,
    /// The pattern's own name for the relationships traversed, bound to a
    /// list of them (#652).
    rel_variable: Option<String>,
    /// This segment is being walked against the direction it was written in.
    ///
    /// The planner may anchor a variable-length segment at whichever end is
    /// more selective and walk back along it -- `(a)-[:R*1..2]->(b)` read from
    /// `b` is `(b)<-[:R*1..2]-(a)`, and the pairs are identical. The *order* of
    /// what it collects is not: the walk produces relationships starting from
    /// the anchor, and `r` must list them in the pattern's direction.
    ///
    /// `MATCH (a)-[r:REL*2..2]->(b:End) RETURN r` anchored on `b:End` and
    /// answered `[{num: 2}, {num: 1}]` where the graph reads 1 then 2 (#933).
    /// Two right relationships in the wrong order, from a query that reported
    /// success.
    reversed_walk: bool,
    /// Inline property constraints on the relationship, e.g.
    /// `-[:R* {year: 1988}]->`.
    ///
    /// Applies to **every** hop, not just the first or last: `-[:R* {k: v}]->`
    /// means every relationship on the path has `k = v`. So it is enforced
    /// inside the walk rather than as a filter above this operator -- a filter
    /// above cannot see the intermediate hops at all.
    ///
    /// The planner had nowhere to put these and dropped them, so
    /// `MATCH (a)-[:WORKED_WITH* {year: 1988}]->(b)` returned every path in
    /// the graph (#934). A filter that silently does not filter, failing
    /// *open*.
    ///
    /// Pruning, not cost: a hop that fails the predicate ends that branch.
    edge_properties: std::collections::HashMap<String, PropertyValue>,
    /// Output records buffered for the current input record.
    pending: std::collections::VecDeque<Record>,
    /// `edge_types` resolved to interned ids, cached after the first use.
    ///
    /// Resolving is a hash lookup per type against the store, and the answer
    /// cannot change during a query. Empty vec inside the `Some` means "no
    /// filter"; a requested type the graph has never seen simply contributes
    /// no id, so it matches nothing -- which is correct.
    type_ids: Option<Vec<u16>>,
    /// The target is pinned to exactly this node.
    ///
    /// Set when the planner can resolve the far endpoint to a single node at
    /// plan time — `MATCH (p:Person {id: 42})-[:KNOWS*1..2]-(friend)` read from
    /// the `friend` side. The question is then not "what is reachable from each
    /// friend" but "is *this one node* reachable from each friend", and those
    /// have very different costs.
    pinned_target: Option<NodeId>,
    /// Nodes from which `pinned_target` is reachable within the hop bounds,
    /// computed once on first use.
    ///
    /// One BFS outward from the pinned node, reversed, answers the question for
    /// every input row. Without it each row runs its own BFS: LDBC IC6 feeds
    /// thousands of candidate friends into this operator and each one expanded
    /// its own two-hop neighbourhood to discover whether one specific person
    /// was in it. At SF10 that is the difference between finishing and hitting
    /// the query timeout.
    target_reach: Option<std::collections::HashSet<NodeId>>,
    /// Enumerate trails even when `min_hops < 2`, because the query can see
    /// how many times a node is reached.
    ///
    /// The BFS marks a node visited at the depth it is first reached, so
    /// `(a)-[:R*1..2]-(x)` over a triangle answers `b, c` where openCypher
    /// answers `b, b, c, c` — `b` is reached directly and again via `c`. That
    /// is a wrong answer, and the only correct walk is to enumerate trails.
    ///
    /// It is not the default because enumeration is not affordable where
    /// nothing can observe the difference: LDBC IC1's `KNOWS*1..3` reaches
    /// ~4,900 nodes and every LDBC var-length query dedups its result. The
    /// planner sets this when the multiplicity is observable and leaves the
    /// BFS in place when a `DISTINCT` absorbs it (#710).
    enumerate_trails: bool,
    /// Enforce relationship isomorphism across the clause, not just within
    /// this segment.
    ///
    /// The BFS already cannot repeat an edge *inside* one walk — a node-visited
    /// set makes every emitted path simple — but it never knew what the rest of
    /// the clause had walked. `MATCH (a)-[:R]-(y)-[:R*1..1]-(z)` over a single
    /// edge therefore answered one row where openCypher answers none: the
    /// segment took the same edge back (#710).
    ///
    /// `ExpandOperator` has carried this since #684; the var-length operator is
    /// the path that did not inherit it.
    track_edges: bool,
    /// Marks the first expand of a MATCH clause, which starts with a clean
    /// history rather than inheriting one from an earlier clause.
    starts_clause: bool,
}

impl VarLengthExpandOperator {
    /// Create a new variable-length expand operator. `max_hops == usize::MAX`
    /// means unbounded (BFS still terminates via the visited set).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        edge_types: Vec<String>,
        direction: Direction,
        min_hops: usize,
        max_hops: usize,
    ) -> Self {
        Self {
            input,
            source_var,
            target_var,
            edge_types,
            target_labels: Vec::new(),
            direction,
            min_hops,
            max_hops,
            path_variable: None,
            rel_variable: None,
            reversed_walk: false,
            edge_properties: std::collections::HashMap::new(),
            pending: std::collections::VecDeque::new(),
            type_ids: None,
            pinned_target: None,
            target_reach: None,
            enumerate_trails: false,
            track_edges: false,
            starts_clause: false,
        }
    }

    /// Set target node labels to filter the emitted endpoint.
    pub fn with_target_labels(mut self, labels: Vec<Label>) -> Self {
        self.target_labels = labels;
        self
    }

    /// Set path variable for named-path materialization.
    pub fn with_path_variable(mut self, var: String) -> Self {
        self.path_variable = Some(var);
        self
    }

    /// Bind the pattern's relationship variable to the relationships traversed.
    ///
    /// `MATCH (a)-[r:T*]->(b)` makes `r` a **list of relationships**, one per
    /// hop. The variable was simply dropped, so the query failed with
    /// "Variable not found: r" -- the traversal was right and its own name for
    /// what it traversed did not exist (#652).
    /// Mark this segment as walked against its written direction, so a bound
    /// path or relationship list is emitted in the pattern's order rather than
    /// the walk's. See [`Self::reversed_walk`].
    pub fn with_reversed_walk(mut self) -> Self {
        self.reversed_walk = true;
        self
    }

    /// Constrain every relationship on the path by these properties. See
    /// [`Self::edge_properties`].
    pub fn with_edge_properties(
        mut self,
        props: std::collections::HashMap<String, PropertyValue>,
    ) -> Self {
        self.edge_properties = props;
        self
    }

    /// Does this edge satisfy the segment's inline property constraints?
    fn edge_props_ok(&self, eid: crate::graph::EdgeId, store: &GraphStore) -> bool {
        if self.edge_properties.is_empty() {
            return true;
        }
        store.get_edge(eid).is_some_and(|edge| {
            self.edge_properties
                .iter()
                .all(|(k, v)| edge.properties.get(k).is_some_and(|have| have == v))
        })
    }

    /// Is this segment's relationship variable already bound to a list?
    ///
    /// Only a *list* counts. A relationship variable bound to a single
    /// relationship is a different question -- and an error the validator
    /// still rejects -- so this must not fire on one.
    fn walk_is_bound(&self, record: &Record) -> bool {
        let Some(rv) = self.rel_variable.as_ref() else { return false };
        matches!(
            record.get(rv),
            Some(Value::List(_)) | Some(Value::Property(PropertyValue::Array(_)))
        )
    }

    /// Verify the one path `rs` names, and emit it if it is legal.
    ///
    /// Everything the search would enforce is enforced here too, because a
    /// pattern does not stop meaning what it says when its walk is handed to
    /// it: the length bounds, the type filter, the inline property filter,
    /// relationship isomorphism, and the direction each edge is traversed in.
    /// Leaving any of them out would make the bound form quietly more
    /// permissive than the searched form -- the same query, two answers.
    fn walk_bound_list(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<()> {
        let Some(rv) = self.rel_variable.clone() else { return Ok(()) };
        let source_val = record
            .get(&self.source_var)
            .ok_or_else(|| ExecutionError::VariableNotFound(self.source_var.clone()))?;
        if matches!(source_val, Value::Null)
            || matches!(source_val.as_property(), Some(PropertyValue::Null))
        {
            return Ok(());
        }
        let source_id = source_val.node_id().ok_or_else(|| {
            ExecutionError::TypeError(format!("{} is not a node", self.source_var))
        })?;

        let ids: Vec<crate::graph::EdgeId> = match record.get(&rv) {
            Some(Value::List(items)) => items.iter().filter_map(|v| v.edge_id()).collect(),
            // A list that reached here as a property array cannot hold an
            // entity (see `Value` vs `PropertyValue`), so it can never name a
            // relationship. An empty walk is the honest reading.
            Some(Value::Property(PropertyValue::Array(_))) => Vec::new(),
            _ => return Ok(()),
        };
        // A non-relationship somewhere in the list means the list is not a
        // walk. Matching nothing is right; matching the prefix that happened
        // to be relationships is not.
        let named = match record.get(&rv) {
            Some(Value::List(items)) => items.len(),
            _ => 0,
        };
        if ids.len() != named || ids.len() < self.min_hops || ids.len() > self.max_hops {
            return Ok(());
        }

        self.ensure_type_ids(store);
        let mut at = source_id;
        let mut nodes = vec![at];
        let mut seen: Vec<crate::graph::EdgeId> = Vec::with_capacity(ids.len());
        for eid in &ids {
            // Relationship isomorphism inside the list itself: a walk that
            // reuses an edge is not a walk, whether a BFS built it or a WITH
            // did.
            if seen.contains(eid) {
                return Ok(());
            }
            let Some(edge) = store.get_edge(*eid) else { return Ok(()) };
            if !self.edge_types.is_empty()
                && !self.edge_types.iter().any(|t| t.as_str() == edge.edge_type.as_str())
            {
                return Ok(());
            }
            if !self.edge_props_ok(*eid, store) {
                return Ok(());
            }
            // Where this edge lands, given where we stand and which way the
            // pattern lets us cross it. `Both` may be crossed either way; the
            // directed forms may not, and an edge that does not touch `at` at
            // all fails whichever direction is written.
            let next = match self.direction {
                Direction::Outgoing if edge.source == at => edge.target,
                Direction::Incoming if edge.target == at => edge.source,
                Direction::Both if edge.source == at => edge.target,
                Direction::Both if edge.target == at => edge.source,
                _ => return Ok(()),
            };
            seen.push(*eid);
            at = next;
            nodes.push(at);
        }

        // An edge this clause already walked is not available to this segment
        // (#710), exactly as in the searched form.
        if self.track_edges && !self.starts_clause {
            let used = record.used_edge_slice();
            if ids.iter().any(|e| used.contains(e)) {
                return Ok(());
            }
        }
        // A bound target must be the node the walk actually reaches.
        if let Some(bound) = record.get(&self.target_var).and_then(|v| v.node_id()) {
            if bound != at {
                return Ok(());
            }
        }
        if let Some(pinned) = self.pinned_target {
            if pinned != at {
                return Ok(());
            }
        }
        if !self.target_labels.is_empty() {
            let ok = store.get_node(at).is_some_and(|n| {
                self.target_labels.iter().all(|l| n.labels.contains(l))
            });
            if !ok {
                return Ok(());
            }
        }
        // `buffer_walk` rebinds `rs` from `edges`, which is the same list it
        // was given -- so the binding survives unchanged, which is what the
        // pattern asks for. `reversed_walk` does not apply: the list is
        // already in the pattern's order.
        let base = record.clone();
        let reversed = std::mem::replace(&mut self.reversed_walk, false);
        self.buffer_walk(&base, at, nodes, ids, store);
        self.reversed_walk = reversed;
        Ok(())
    }

    pub fn with_rel_variable(mut self, var: String) -> Self {
        self.rel_variable = Some(var);
        self
    }

    /// The edge-type filter as interned ids, resolved once per query.
    ///
    /// Returns `None` when the pattern named no types at all -- the wildcard.
    /// A pattern that named types none of which exist in the graph returns
    /// `Some(empty)`, which matches nothing. Collapsing those two cases makes
    /// `-[:NO_SUCH_TYPE*1..3]->` follow every edge in the graph; a test does
    /// exactly that, and it failed against the first version of this.
    fn ensure_type_ids(&mut self, store: &GraphStore) {
        if self.edge_types.is_empty() || self.type_ids.is_some() {
            return;
        }
        let ids = self
            .edge_types
            .iter()
            .filter_map(|t| store.edge_type_id(&EdgeType::new(t.as_str())))
            .collect();
        self.type_ids = Some(ids);
    }

    /// Visit each one-hop neighbour of `node` honouring direction and the
    /// edge-type filter, without allocating.
    ///
    /// This used to build a `Vec` of `(EdgeId, NodeId, NodeId, EdgeType)` per
    /// node -- three allocations and an `EdgeType` **string clone per incident
    /// edge** -- and then filter it by comparing those strings. The filter was
    /// therefore paid *after* materialising every incident edge, which on a
    /// real graph is most of the cost: an LDBC `Person` has ~41 `KNOWS` edges
    /// and ~900 others (inbound `HAS_CREATOR` from every post and comment they
    /// wrote, `LIKES`, `HAS_MEMBER`, `HAS_INTEREST`), so `KNOWS*1..3` from one
    /// person enumerated ~9.3M edges to traverse 404K (#520).
    ///
    /// Filtering on the interned type id inside the walk skips a non-matching
    /// edge in a compare rather than a string clone.
    /// Neighbours in an explicitly given direction.
    ///
    /// `for_each_neighbor` uses `self.direction`; the reversed BFS needs the
    /// opposite one, and taking the direction as an argument keeps a second
    /// near-copy of the match out of the file.
    fn neighbors_in(
        node: NodeId,
        type_ids: Option<&[u16]>,
        direction: &Direction,
        edge_properties: &std::collections::HashMap<String, PropertyValue>,
        store: &GraphStore,
        visit: &mut impl FnMut(NodeId),
    ) {
        // The reachability BFS has to honour the same constraint as the
        // forward walk, or a pinned target is reported reachable by a path the
        // pattern excludes -- and the two would then disagree about the same
        // question depending on which end the planner anchored.
        let mut with_edge = |nb: NodeId, e: crate::graph::EdgeId| {
            if edge_properties.is_empty()
                || store.get_edge(e).is_some_and(|edge| {
                    edge_properties
                        .iter()
                        .all(|(k, v)| edge.properties.get(k).is_some_and(|have| have == v))
                })
            {
                visit(nb)
            }
        };
        match direction {
            Direction::Outgoing => store.for_each_outgoing_neighbor(node, type_ids, &mut with_edge),
            Direction::Incoming => store.for_each_incoming_neighbor(node, type_ids, &mut with_edge),
            Direction::Both => {
                store.for_each_outgoing_neighbor(node, type_ids, &mut with_edge);
                store.for_each_incoming_neighbor(node, type_ids, &mut with_edge);
            }
        }
    }

    fn for_each_neighbor(
        &self,
        node: NodeId,
        type_ids: Option<&[u16]>,
        store: &GraphStore,
        mut visit: impl FnMut(NodeId, crate::graph::EdgeId),
    ) {
        // Wrapped here rather than at each call site: every walk in this
        // operator enumerates through this one function, so the constraint
        // cannot be missed by a path that forgot to check it.
        let mut visit = |nb: NodeId, eid: crate::graph::EdgeId| {
            if self.edge_props_ok(eid, store) {
                visit(nb, eid);
            }
        };
        match self.direction {
            Direction::Outgoing => store.for_each_outgoing_neighbor(node, type_ids, &mut visit),
            Direction::Incoming => store.for_each_incoming_neighbor(node, type_ids, &mut visit),
            Direction::Both => {
                store.for_each_outgoing_neighbor(node, type_ids, &mut visit);
                store.for_each_incoming_neighbor(node, type_ids, &mut visit);
            }
        }
    }

    /// Pin the target to a single node the planner resolved at plan time.
    ///
    /// Only valid when the destination really is that one node; the operator
    /// then answers "can this source reach it" rather than enumerating.
    pub fn with_pinned_target(mut self, target: NodeId) -> Self {
        self.pinned_target = Some(target);
        self
    }

    /// Enumerate trails rather than walking shortest paths, because the query
    /// can observe how many times a node is reached. See `enumerate_trails`.
    pub fn with_trail_enumeration(mut self) -> Self {
        self.enumerate_trails = true;
        self
    }

    /// Enforce relationship isomorphism for this segment against the whole
    /// clause, not just within the segment. Same contract as
    /// `ExpandOperator::with_edge_isolation`: `starts_clause` marks the first
    /// expand of a MATCH, which begins with a clean history.
    pub fn with_edge_isolation(mut self, starts_clause: bool) -> Self {
        self.track_edges = true;
        self.starts_clause = starts_clause;
        self
    }

    /// Can the pinned target be reached from `source` within the hop bounds?
    ///
    /// Answered from a set built by one BFS *outward from the target*, walking
    /// edges in the opposite direction, which is the same relation read the
    /// other way round. Built once and reused for every input row.
    ///
    /// Restricted by the caller to `min_hops <= 1` and no path variable, and
    /// both restrictions are load-bearing:
    ///
    /// * with `min_hops >= 2` a node whose *shortest* distance is 1 may still
    ///   have a conforming longer walk, and a set keyed on shortest distance
    ///   would wrongly exclude it;
    /// * a path variable needs the actual path, which a membership test does
    ///   not produce.
    fn target_reaches(&mut self, source: NodeId, store: &GraphStore) -> bool {
        let target = match self.pinned_target {
            Some(t) => t,
            None => return false,
        };
        if self.target_reach.is_none() {
            self.ensure_type_ids(store);
            let type_ids = self.type_ids.clone();
            let type_filter = type_ids.as_deref();

            // Reversed: the operator walks source -> target, so reaching the
            // target from a source means walking target -> source backwards.
            let reversed = match self.direction {
                Direction::Outgoing => Direction::Incoming,
                Direction::Incoming => Direction::Outgoing,
                Direction::Both => Direction::Both,
            };

            let mut reach: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
            let mut visited: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
            visited.insert(target);
            if self.min_hops == 0 {
                reach.insert(target);
            }
            let mut frontier = vec![target];
            let mut depth = 0usize;
            while !frontier.is_empty() && depth < self.max_hops {
                depth += 1;
                let mut next = Vec::new();
                for &cur in &frontier {
                    Self::neighbors_in(cur, type_filter, &reversed, &self.edge_properties, store, &mut |nb| {
                        if visited.insert(nb) {
                            next.push(nb);
                            if depth >= self.min_hops {
                                reach.insert(nb);
                            }
                        }
                    });
                }
                frontier = next;
            }
            self.target_reach = Some(reach);
        }
        self.target_reach.as_ref().is_some_and(|r| r.contains(&source))
    }

    /// Enumerate every path from `source` of length `min_hops..=max_hops`
    /// whose relationships are distinct, and emit its far end.
    ///
    /// Used only when `min_hops >= 2`, where the shortest-path walk in
    /// `expand_from` returns an empty result rather than a smaller one (#710).
    /// Nodes may repeat along the path; relationships may not — that is
    /// openCypher's rule, and the same one #684 established for fixed-length
    /// patterns.
    ///
    /// Depth-first with an explicit stack, so the working set is one path
    /// rather than one frontier. An unbounded upper bound is still bounded in
    /// practice by edge-distinctness, but the number of trails is not, so
    /// `MAX_TRAILS` caps the walk and the cap is reported rather than
    /// silently truncating: a benchmark that quietly runs a subset reports a
    /// denominator that does not exist (#683), and the same is true of a
    /// query.
    fn expand_trails(
        &mut self,
        record: &Record,
        source_id: NodeId,
        store: &GraphStore,
    ) -> ExecutionResult<()> {
        /// Enough for any pattern a person writes by hand, small enough that a
        /// runaway `*2..` cannot hang the query.
        const MAX_TRAILS: usize = 1_000_000;

        // An empty interval matches nothing. `*1..0` and `*..0` (which parses
        // as `*1..0`) are legal and must return no rows — TCK Match5 [12] and
        // [13].
        //
        // The emit test below is `depth >= min_hops`, checked *before* the
        // `depth < max_hops` that decides whether to descend, so without this
        // a `*1..0` emits every neighbour at depth 1 and then stops. The bug
        // was latent while this path was reachable only for `min_hops >= 2`:
        // `*2..1` walks to depth 1, never reaches depth 2, and emits nothing by
        // accident. `*1..0` has no such accident to save it.
        if self.min_hops > self.max_hops {
            return Ok(());
        }

        self.ensure_type_ids(store);
        let type_ids: Option<Vec<u16>> = self.type_ids.clone();
        let type_filter = type_ids.as_deref();

        // The path so far, as (node, edge-that-reached-it). `edges` is what
        // enforces relationship uniqueness.
        let mut path: Vec<(NodeId, crate::graph::EdgeId)> = Vec::new();
        // Seeded with what the clause has already walked, so a trail cannot
        // retake an edge an earlier segment used (#710) — filtered to the edges
        // this segment could actually walk, for the reason in `expand_from`.
        let mut edges: Vec<crate::graph::EdgeId> = if self.track_edges && !self.starts_clause {
            record
                .used_edge_slice()
                .iter()
                .copied()
                .filter(|&e| store.edge_traversable_by(e, type_filter))
                .collect()
        } else {
            Vec::new()
        };
        let inherited_len = edges.len();
        // Frontier per depth: the neighbours still to try at each level.
        let mut stack: Vec<Vec<(NodeId, crate::graph::EdgeId)>> = Vec::new();

        // Not a closure over `self`: `buffer` below needs `&mut self`, and a
        // captured `&self` would still be alive.
        macro_rules! collect {
            ($cur:expr, $used:expr) => {{
                let mut out = Vec::new();
                self.for_each_neighbor($cur, type_filter, store, |nb, eid| {
                    if !$used.contains(&eid) {
                        out.push((nb, eid));
                    }
                });
                out
            }};
        }

        stack.push(collect!(source_id, edges));
        let mut trails = 0usize;

        while let Some(frontier) = stack.last_mut() {
            let Some((nb, eid)) = frontier.pop() else {
                stack.pop();
                path.pop();
                if edges.len() > inherited_len {
                    edges.pop();
                }
                continue;
            };
            path.push((nb, eid));
            edges.push(eid);
            let depth = path.len();

            if depth >= self.min_hops && self.emit_ok(nb, store) {
                trails += 1;
                if trails > MAX_TRAILS {
                    return Err(ExecutionError::PlanningError(format!(
                        "variable-length pattern produced more than {MAX_TRAILS} paths; \
                         bound it with an upper hop limit or a more selective start"
                    )));
                }
                // The trail, handed over directly.
                //
                // It used to be flattened into a `parent` map keyed by node and
                // reconstructed from that -- which cannot represent a trail
                // that **revisits a node**. Every later visit overwrote the
                // earlier one, so reconstruction walked back along the wrong
                // edges and stopped early: an undirected `*3..3` came back as
                // a two-hop path (#976). Undirected walks over a small graph
                // revisit constantly, which is why this shows up there and not
                // on a directed chain.
                //
                // `path` is already the walk, in order. There was never a
                // reason to go through a lossy intermediate.
                let mut trail_nodes = Vec::with_capacity(path.len() + 1);
                trail_nodes.push(source_id);
                trail_nodes.extend(path.iter().map(|(n, _)| *n));
                let trail_edges: Vec<crate::graph::EdgeId> =
                    path.iter().map(|(_, e)| *e).collect();
                self.buffer_trail(record, nb, trail_nodes, trail_edges, store);
            }

            if depth < self.max_hops {
                stack.push(collect!(nb, edges));
            } else {
                path.pop();
                edges.pop();
            }
        }
        Ok(())
    }

    /// BFS from the source bound in `record`, buffering one output record per
    /// distinct reachable target in `[min_hops, max_hops]`.
    fn expand_from(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<()> {
        // `MATCH (first)-[rs*]->(second)` where `rs` is *already bound* to a
        // list of relationships is not a search at all. openCypher reads it as
        // "the walk is exactly `rs`", so there is one candidate path and the
        // only question is whether it is legal. Running the BFS here would
        // rebind `rs` to whatever it found and answer a different question
        // (#984).
        if self.walk_is_bound(record) {
            return self.walk_bound_list(record, store);
        }
        let source_val = record
            .get(&self.source_var)
            .ok_or_else(|| ExecutionError::VariableNotFound(self.source_var.clone()))?;
        // See `ExpandOperator::load_edges`: a null source expands to nothing
        // rather than failing (#671).
        if matches!(source_val, Value::Null) || matches!(source_val.as_property(), Some(PropertyValue::Null)) {
            self.pending.clear();
            return Ok(());
        }
        let source_id = source_val.node_id().ok_or_else(|| {
            ExecutionError::TypeError(format!("{} is not a node", self.source_var))
        })?;

        // Edges an earlier segment of this clause already walked. This segment
        // may not retake them (#710). Empty for the first segment of a clause
        // and for any single-segment pattern.
        //
        // **Filtered by this segment's own type filter**, which is not a
        // refinement but the difference between IC6 taking 226 ms and taking
        // over forty minutes. The planner reverses IC6 to anchor on its
        // selective `:Tag`, so the var-length segment runs *last*:
        //
        //   VarLengthExpand ((friend)-[:KNOWS*1..2]-(p) [target pinned])
        //
        // The edges it inherits are `HAS_TAG` and `HAS_CREATOR`; the segment
        // walks `:KNOWS`. They can never collide, so isolation has nothing to
        // do here — but a non-empty `inherited` disabled the pinned-target
        // shortcut below and turned one membership test per row into a whole
        // BFS per row (#734).
        //
        // An edge of a type this segment cannot traverse is not a candidate for
        // re-traversal, so dropping it changes no answer. An untyped segment
        // filters nothing, which is correct: it can walk anything.
        let inherited: Vec<crate::graph::EdgeId> = if self.track_edges && !self.starts_clause {
            self.ensure_type_ids(store);
            let types = self.type_ids.clone();
            record
                .used_edge_slice()
                .iter()
                .copied()
                .filter(|&e| store.edge_traversable_by(e, types.as_deref()))
                .collect()
        } else {
            Vec::new()
        };

        // Pinned target: one membership test instead of a BFS per row. The
        // planner only sets this when the destination resolves to a single
        // node, there is no path variable, and `min_hops <= 1`.
        //
        // Skipped when the clause has already walked edges: the reachability
        // set is built once, before any row, so it cannot know which edges this
        // row is forbidden. Falling through to the BFS below answers the same
        // question with the ban applied.
        //
        // The shortcut still does not *record* what it walked — it answers
        // "reachable" without knowing by which route — so a later segment of
        // the same clause may retake one of those edges. That is the behaviour
        // this operator had everywhere before isolation existed, so it is a
        // remaining gap and not a regression; closing it means either
        // reconstructing the route (which is what the shortcut exists to avoid)
        // or knowing at plan time that no segment follows. #710.
        if let (true, Some(target)) = (inherited.is_empty(), self.pinned_target) {
            if self.target_reaches(source_id, store) && self.emit_ok(target, store) {
                let empty: std::collections::HashMap<NodeId, (NodeId, crate::graph::EdgeId)> =
                    std::collections::HashMap::new();
                self.buffer(record, target, &empty, source_id, store);
            }
            return Ok(());
        }

        // A lower bound above one cannot be answered by a shortest-path walk.
        //
        // The BFS below marks a node visited at the depth it is first reached
        // and never reconsiders it, so `*2..2` over a triangle finds `b` and
        // `c` at depth 1, declines to emit them (depth < min_hops), and then
        // has nothing left at depth 2 — **zero rows, no error**, where
        // openCypher matches `a-b-c` and `a-c-b`. Nodes may repeat in a
        // variable-length match; only relationships may not.
        //
        // Enumerating trails is the general answer and is not affordable on
        // the shapes that matter (#710): LDBC IC1's `KNOWS*1..3` reaches ~4,900
        // nodes, and IC6 needs the pinned-target walk to stay cheap. Both have
        // `min_hops == 1`, as does every LDBC pattern, so the enumeration is
        // taken only where the BFS is not merely lossy but wrong.
        // `min_hops == 0` stays on the BFS: `expand_trails` walks outward from
        // the source and has no way to emit the source itself, so routing
        // `*0..n` into it silently drops the zero-length match — caught by
        // `zero_hops_includes_the_target_itself`.
        if self.min_hops >= 2 || (self.enumerate_trails && self.min_hops >= 1) {
            return self.expand_trails(record, source_id, store);
        }

        // parent[node] = (predecessor, edge used) for path reconstruction.
        let mut parent: std::collections::HashMap<NodeId, (NodeId, crate::graph::EdgeId)> =
            std::collections::HashMap::new();
        let mut visited: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
        visited.insert(source_id);

        // Depth 0 endpoint (only relevant when min_hops == 0).
        if self.min_hops == 0 && self.emit_ok(source_id, store) {
            self.buffer(record, source_id, &parent, source_id, store);
        }

        // Cloned rather than borrowed: the BFS below calls `&mut self` methods
        // to buffer output records, so an outstanding borrow of `self.type_ids`
        // would not live. This is once per source record, not once per row.
        self.ensure_type_ids(store);
        let type_ids: Option<Vec<u16>> = self.type_ids.clone();
        let type_filter = type_ids.as_deref();

        let mut frontier = vec![source_id];
        let mut depth = 0usize;
        while !frontier.is_empty() && depth < self.max_hops {
            depth += 1;
            let mut next = Vec::new();
            for &cur in &frontier {
                // The closure borrows the BFS state; buffering an output
                // record needs `&mut self`, so discovery and emission are
                // separated. The emission order is unchanged: `next` is filled
                // in exactly the order the old code emitted in.
                self.for_each_neighbor(cur, type_filter, store, |nb, eid| {
                    // An edge an earlier segment of this clause already walked
                    // is not available to this one (#710).
                    if inherited.contains(&eid) {
                        return;
                    }
                    if visited.insert(nb) {
                        parent.insert(nb, (cur, eid));
                        next.push(nb);
                    }
                });
            }
            if depth >= self.min_hops {
                for &nb in &next {
                    if self.emit_ok(nb, store) {
                        self.buffer(record, nb, &parent, source_id, store);
                    }
                }
            }
            frontier = next;
        }
        Ok(())
    }

    /// Whether `node` qualifies as an emitted endpoint (target-label filter).
    fn emit_ok(&self, node: NodeId, store: &GraphStore) -> bool {
        if self.target_labels.is_empty() {
            return true;
        }
        match store.get_node(node) {
            Some(n) => self.target_labels.iter().all(|l| n.has_label(l)),
            None => false,
        }
    }

    /// Build and buffer an output record binding the target (and optional path).
    /// `buffer`, given the walk directly instead of a parent map.
    ///
    /// The BFS has only a parent map, which is fine there: it visits each node
    /// once. A trail may revisit one, and a map keyed by node cannot hold that
    /// (#976).
    fn buffer_trail(
        &mut self,
        base: &Record,
        target: NodeId,
        nodes: Vec<NodeId>,
        edges: Vec<crate::graph::EdgeId>,
        store: &GraphStore,
    ) {
        self.buffer_walk(base, target, nodes, edges, store)
    }

    fn buffer(
        &mut self,
        base: &Record,
        target: NodeId,
        parent: &std::collections::HashMap<NodeId, (NodeId, crate::graph::EdgeId)>,
        source: NodeId,
        store: &GraphStore,
    ) {
        let mut rec = base.clone();
        rec.bind(self.target_var.clone(), Value::NodeRef(target));

        // Record what this segment walked, so a later segment of the same
        // clause cannot retake it (#710). Reconstructed from `parent` rather
        // than from the path variable, because isolation applies whether or not
        // the pattern names the path.
        let (nodes, edges) = reconstruct_path(parent, source, target);
        self.buffer_walk(base, target, nodes, edges, store)
    }

    /// Shared by both walks: bind the target, record isolation, and materialise
    /// a path or relationship list from the walk it was given.
    fn buffer_walk(
        &mut self,
        base: &Record,
        target: NodeId,
        nodes: Vec<NodeId>,
        edges: Vec<crate::graph::EdgeId>,
        store: &GraphStore,
    ) {
        let mut rec = base.clone();
        rec.bind(self.target_var.clone(), Value::NodeRef(target));

        if self.track_edges {
            if self.starts_clause {
                rec.clear_used_edges();
            }
            let walked = edges.clone();
            for e in walked {
                rec.mark_edge_used(e);
            }
        }
        if self.path_variable.is_some() || self.rel_variable.is_some() {
            let (mut nodes, mut edges) = (nodes, edges);
            // `reconstruct_path` returns the walk in the order it was walked,
            // which is the pattern's order only when the two agree. When the
            // planner anchored the far end and walked back, they do not.
            if self.reversed_walk {
                nodes.reverse();
                edges.reverse();
            }
            if let Some(ref rv) = self.rel_variable {
                // A list of relationships, not of ids: `r` is the same kind of
                // thing a single-hop `[r]` binds, one per hop.
                // Resolved from the store rather than left as placeholders:
                // an `EdgeRef` carrying a blank type renders as `[:]`, which
                // is not what any caller means by a relationship.
                rec.bind(
                    rv.clone(),
                    Value::List(
                        edges
                            .iter()
                            .map(|e| match store.get_edge(*e) {
                                Some(edge) => Value::EdgeRef(
                                    *e,
                                    edge.source,
                                    edge.target,
                                    edge.edge_type.clone(),
                                ),
                                None => Value::Null,
                            })
                            .collect(),
                    ),
                );
            }
            if let Some(ref pv) = self.path_variable {
                // **Extend** an already-bound path rather than replacing it.
                //
                // Every segment of a pattern gets the same path variable, so
                // with two of them the second overwrote the first and `p` held
                // only the last segment's walk:
                // `MATCH p = (a)-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c)`
                // returned three paths of the right count and every one a
                // segment short (#966).
                //
                // The incoming path ends where this walk begins, so its final
                // node is dropped to avoid repeating the join node.
                let (nodes, edges) = match base.get(pv) {
                    Some(Value::Path { nodes: pn, edges: pe }) if !pn.is_empty() => {
                        let mut n = pn.clone();
                        n.pop();
                        n.extend(nodes);
                        let mut e = pe.clone();
                        e.extend(edges);
                        (n, e)
                    }
                    _ => (nodes, edges),
                };
                rec.bind(pv.clone(), Value::Path { nodes, edges });
            }
        }
        self.pending.push_back(rec);
    }
}

/// Reconstruct the BFS path source→target from a parent map.
fn reconstruct_path(
    parent: &std::collections::HashMap<NodeId, (NodeId, crate::graph::EdgeId)>,
    source: NodeId,
    target: NodeId,
) -> (Vec<NodeId>, Vec<crate::graph::EdgeId>) {
    let mut nodes = vec![target];
    let mut edges = Vec::new();
    let mut cur = target;
    while cur != source {
        if let Some(&(prev, eid)) = parent.get(&cur) {
            edges.push(eid);
            nodes.push(prev);
            cur = prev;
        } else {
            break;
        }
    }
    nodes.reverse();
    edges.reverse();
    (nodes, edges)
}

impl PhysicalOperator for VarLengthExpandOperator {
    // A write beneath this operator refused with "requires mutable store
    // access", because the default `next_mut` delegates to `next` and `next`
    // reads its input read-only. Shared body rather than a second, mutable copy
    // of this operator's own logic -- see `drain_input_for_write` (#870).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        drain_input_for_write(&mut self.input, store, tenant_id)?;
        self.next(store)
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            if let Some(rec) = self.pending.pop_front() {
                return Ok(Some(rec));
            }
            match self.input.next(store)? {
                Some(record) => self.expand_from(&record, store)?,
                None => return Ok(None),
            }
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.pending.clear();
    }

    fn describe(&self) -> OperatorDescription {
        let types = if self.edge_types.is_empty() {
            "*".to_string()
        } else {
            self.edge_types.join("|")
        };
        let max = if self.max_hops == usize::MAX {
            String::new()
        } else {
            self.max_hops.to_string()
        };
        OperatorDescription {
            name: "VarLengthExpand".to_string(),
            details: format!(
                "({})-[:{}*{}..{}]-({}){}",
                self.source_var,
                types,
                self.min_hops,
                max,
                self.target_var,
                // Whether the pinned-target path is in use changes the cost of
                // this operator by orders of magnitude, so it belongs in the
                // plan. An optimisation you cannot see in EXPLAIN is one nobody
                // can tell has stopped firing.
                match self.pinned_target {
                    Some(t) => format!(" [target pinned to node {}]", t.as_u64()),
                    None => String::new(),
                }
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// Project operator: RETURN n.name, n.age
pub struct ProjectOperator {
    /// Input operator
    input: OperatorBox,
    /// Expressions to project
    projections: Vec<(Expression, String)>, // (expr, alias)
}

impl ProjectOperator {
    /// Create a new project operator
    pub fn new(input: OperatorBox, projections: Vec<(Expression, String)>) -> Self {
        Self { input, projections }
    }

    fn evaluate_expression(&self, expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            // Delegates rather than adding a sixth copy of this logic; the
            // standalone evaluator is the one implementation (#654).
            Expression::ListExpr(_) | Expression::MapExpr(_) => {
                eval_expression(expr, record, store)
            }
            Expression::Variable(var) => {
                let val = record.get(var)
                    .cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))?;
                // Materialize refs at projection time (RETURN n)
                // A reference the store can no longer resolve is kept as a
                // reference rather than refused. It still carries the
                // structural data it was built with, which is what survives a
                // delete: `MATCH ()-[r]->() DELETE r RETURN type(r)` is a
                // legal query, and materialising `r` first turned it into
                // "Edge not found" (#905). A *property* read of the same
                // reference does fail -- see `read_property`.
                match val {
                    Value::NodeRef(id) => Ok(match store.get_node(id) {
                        Some(node) => Value::Node(id, Box::new(node.clone())),
                        None => Value::NodeRef(id),
                    }),
                    Value::EdgeRef(id, src, dst, ref ty) => Ok(match store.get_edge(id) {
                        Some(edge) => Value::Edge(id, Box::new(edge.clone())),
                        None => Value::EdgeRef(id, src, dst, ty.clone()),
                    }),
                    other => Ok(other),
                }
            }
            Expression::Property { variable, property } => {
                return read_property(record, variable, property, store, false);
                #[allow(unreachable_code)]
                Ok(Value::Null)
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = self.evaluate_expression(left, record, store)?;
                let right_val = self.evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = self.evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args.iter()
                    .map(|a| self.evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case { operand, when_clauses, else_result } => {
                eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), |e| self.evaluate_expression(e, record, store))
            }
            Expression::Index { expr, index } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let idx = self.evaluate_expression(index, record, store)?;
                eval_index(collection, idx, store)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = self.evaluate_expression(expr, record, store)?;
                let s = match start { Some(s) => Some(self.evaluate_expression(s, record, store)?), None => None };
                let en = match end { Some(e) => Some(self.evaluate_expression(e, record, store)?), None => None };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery { pattern, where_clause, .. } => {
                eval_exists_subquery(pattern, where_clause.as_deref(), record, store)
            }
            Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
                eval_list_comprehension(variable, list_expr, filter.as_deref(), map_expr, record, store)
            }
            Expression::PredicateFunction { name, variable, list_expr, predicate } => {
                eval_predicate_function(name, variable, list_expr, predicate, record, store)
            }
            Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
                eval_reduce(accumulator, init, variable, list_expr, expression, record, store)
            }
            Expression::PatternComprehension { pattern, filter, projection } => {
                eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store)
            }
            Expression::PathVariable(var) => {
                record.get(var).cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned()
                    .ok_or_else(|| ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name)))
            }
        }
    }
}

impl PhysicalOperator for ProjectOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next(store)? {
            let mut new_record = Record::new();

            for (expr, alias) in &self.projections {
                let value = self.evaluate_expression(expr, &record, store)?;
                new_record.bind(alias.clone(), value);
            }

            Ok(Some(new_record))
        } else {
            Ok(None)
        }
    }

    fn try_push_limit(&mut self, n: usize) -> bool {
        // Project preserves cardinality 1:1 — forward the hint directly.
        self.input.try_push_limit(n)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if let Some(batch) = self.input.next_batch(store, batch_size)? {
            let mut projected_records = Vec::with_capacity(batch.records.len());
            let columns: Vec<String> = self.projections.iter().map(|(_, a)| a.clone()).collect();

            for record in batch.records {
                let mut new_record = Record::new();
                for (expr, alias) in &self.projections {
                    let value = self.evaluate_expression(expr, &record, store)?;
                    new_record.bind(alias.clone(), value);
                }
                projected_records.push(new_record);
            }

            Ok(Some(RecordBatch {
                records: projected_records,
                columns,
            }))
        } else {
            Ok(None)
        }
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            let mut new_record = Record::new();
            for (expr, alias) in &self.projections {
                let value = self.evaluate_expression(expr, &record, store)?;
                new_record.bind(alias.clone(), value);
            }
            Ok(Some(new_record))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let cols: Vec<String> = self.projections.iter().map(|(e, a)| {
            let expr_str = format_expression(e);
            if expr_str == *a { a.clone() } else { format!("{} AS {}", expr_str, a) }
        }).collect();
        OperatorDescription {
            name: "Project".to_string(),
            details: cols.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

/// Aggregation type
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
    PercentileCont,
    PercentileDisc,
    StDev,
    StDevP,
}

/// Aggregation function definition
#[derive(Debug, Clone)]
pub struct AggregateFunction {
    pub func: AggregateType,
    pub expr: Expression,
    pub alias: String,
    pub distinct: bool,
    /// The percentile argument of `percentileCont` / `percentileDisc`.
    ///
    /// The struct held one expression, so the **second** argument was dropped
    /// at extraction and the aggregator's `pct` stayed at its initial `0.5`:
    /// every percentile call returned the median, whatever was asked for
    /// (#871).
    pub percentile: Option<Expression>,
}

/// Internal state for an aggregator
#[derive(Debug, Clone)]
enum AggregatorState {
    Count(i64),
    /// CountDistinct uses a hybrid set: a `HashSet<u64>` fast path for
    /// Node/Edge IDs (the common `count(DISTINCT node_var)` case where each
    /// insert is a single u64), promoting to `BTreeSet<PropertyValue>` only
    /// if a non-ID Property value is seen. The fast path avoids both the
    /// `PropertyValue` wrapping and the O(log n) tree comparisons.
    CountDistinct(CountDistinctSet),
    /// Sum tracks both an integer accumulator and a float accumulator;
    /// `int_only` flips false the first time a non-integer input is seen.
    /// At finalize time, `int_only=true` returns Integer, otherwise Float.
    /// This preserves type fidelity for `SUM(integer_column)` which is the
    /// expected outcome of merging integer counts (e.g. ADR-017's
    /// per-node count post-aggregation).
    Sum { int_acc: i64, float_acc: f64, int_only: bool },
    Avg { sum: f64, count: i64 },
    Min(Option<PropertyValue>),
    Max(Option<PropertyValue>),
    /// `Vec<Value>`, not `Vec<PropertyValue>`: `collect(n)` over nodes is one
    /// of the most common aggregates in Cypher, and a `PropertyValue` cannot
    /// hold an entity. `value.as_property()` returned `None` for a node and
    /// the element was skipped, so `collect(a)` over two nodes produced an
    /// **empty list** — no error, just nothing (#669).
    Collect(Vec<Value>),
    CollectDistinct(BTreeSet<PropertyValue>),
    Percentile { values: Vec<f64>, pct: f64, cont: bool },
    StDev { values: Vec<f64>, population: bool },
}

/// Backing storage for COUNT DISTINCT. Starts empty, picks the appropriate
/// representation on first insert, and promotes to the slow path only if a
/// non-ID property value arrives.
#[derive(Debug, Clone)]
enum CountDistinctSet {
    /// No values seen yet.
    Empty,
    /// Fast path: only Node/Edge IDs (u64) have been inserted.
    Ids(rustc_hash::FxHashSet<u64>),
    /// Slow path: arbitrary `PropertyValue` (covers strings, floats, mixed
    /// types). Used when the input column is property-typed or when we see
    /// a property value after starting on the Ids path.
    Props(BTreeSet<PropertyValue>),
}

impl CountDistinctSet {
    fn new() -> Self {
        Self::Empty
    }

    fn insert_id(&mut self, id: u64) {
        match self {
            Self::Empty => {
                let mut s = rustc_hash::FxHashSet::default();
                s.insert(id);
                *self = Self::Ids(s);
            }
            Self::Ids(s) => {
                s.insert(id);
            }
            Self::Props(s) => {
                // Mixed input — keep using the slow path with consistent
                // wrapping so cardinality reflects all unique values seen.
                s.insert(PropertyValue::Integer(id as i64));
            }
        }
    }

    fn insert_prop(&mut self, p: PropertyValue) {
        match self {
            Self::Empty => {
                let mut s = BTreeSet::new();
                s.insert(p);
                *self = Self::Props(s);
            }
            Self::Props(s) => {
                s.insert(p);
            }
            Self::Ids(ids) => {
                // Promote: a non-id Property arrived after we started on
                // the fast path. Drain ids into a Props set wrapped as
                // Integer, then add the new property.
                let mut s = BTreeSet::new();
                for id in ids.drain() {
                    s.insert(PropertyValue::Integer(id as i64));
                }
                s.insert(p);
                *self = Self::Props(s);
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Ids(s) => s.len(),
            Self::Props(s) => s.len(),
        }
    }

    /// Absorb another set. Used when two group identities turn out to share a
    /// key tuple and their partial aggregates have to become one.
    fn merge(&mut self, other: Self) {
        match other {
            Self::Empty => {}
            Self::Ids(ids) => {
                for id in ids {
                    self.insert_id(id);
                }
            }
            Self::Props(props) => {
                for p in props {
                    self.insert_prop(p);
                }
            }
        }
    }
}

impl AggregatorState {
    fn new(func: &AggregateType, distinct: bool) -> Self {
        match (func, distinct) {
            (AggregateType::Count, true) => AggregatorState::CountDistinct(CountDistinctSet::new()),
            (AggregateType::Count, false) => AggregatorState::Count(0),
            (AggregateType::Sum, _) => AggregatorState::Sum { int_acc: 0, float_acc: 0.0, int_only: true },
            (AggregateType::Avg, _) => AggregatorState::Avg { sum: 0.0, count: 0 },
            (AggregateType::Min, _) => AggregatorState::Min(None),
            (AggregateType::Max, _) => AggregatorState::Max(None),
            (AggregateType::Collect, true) => AggregatorState::CollectDistinct(BTreeSet::new()),
            (AggregateType::Collect, false) => AggregatorState::Collect(Vec::new()),
            (AggregateType::PercentileCont, _) => AggregatorState::Percentile { values: Vec::new(), pct: 0.5, cont: true },
            (AggregateType::PercentileDisc, _) => AggregatorState::Percentile { values: Vec::new(), pct: 0.5, cont: false },
            (AggregateType::StDev, _) => AggregatorState::StDev { values: Vec::new(), population: false },
            (AggregateType::StDevP, _) => AggregatorState::StDev { values: Vec::new(), population: true },
        }
    }

    /// Set the percentile from the call's second argument.
    ///
    /// Cypher requires it in `[0, 1]` and raises otherwise. The finalizer used
    /// to clamp the index with `.min(n - 1)` instead, so an out-of-range
    /// percentile quietly returned the last element (#871).
    fn set_percentile(&mut self, value: &Value) -> ExecutionResult<()> {
        let AggregatorState::Percentile { pct, .. } = self else {
            return Ok(());
        };
        let p = match value.as_property() {
            Some(PropertyValue::Float(f)) => *f,
            Some(PropertyValue::Integer(i)) => *i as f64,
            // A null percentile leaves the aggregate undecidable; Cypher's own
            // answer is null, which the finalizer already gives for no values.
            Some(PropertyValue::Null) | None => return Ok(()),
            Some(other) => {
                return Err(ExecutionError::TypeError(format!(
                    "percentile must be a number between 0 and 1, not {}",
                    other.type_name()
                )))
            }
        };
        if !(0.0..=1.0).contains(&p) {
            return Err(ExecutionError::RuntimeError(format!(
                "percentile must be between 0.0 and 1.0 inclusive, got {p}"
            )));
        }
        *pct = p;
        Ok(())
    }

    fn update(&mut self, value: &Value) {
        match self {
            AggregatorState::Count(c) => {
                if !value.is_null() {
                    *c += 1;
                }
            }
            AggregatorState::CountDistinct(set) => {
                match value {
                    // A list is distinguished by its rendering, which is the
                    // cheapest total order available over mixed element types.
                    Value::List(items) => set.insert_prop(PropertyValue::String(format!("{items:?}"))),
                    Value::Map(entries) => {
                        set.insert_prop(PropertyValue::String(format!("{entries:?}")))
                    }
                    Value::Property(prop) => {
                        if !prop.is_null() {
                            set.insert_prop(prop.clone());
                        }
                    }
                    Value::NodeRef(id) | Value::Node(id, _) => {
                        set.insert_id(id.0);
                    }
                    Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                        set.insert_id(id.0);
                    }
                    Value::Path { .. } => {
                        // Paths are not countable as distinct — ignore
                    }
                    Value::Null => {}
                }
            }
            AggregatorState::Sum { int_acc, float_acc, int_only } => {
                if let Some(prop) = value.as_property() {
                    // Try integer path first to preserve type. If we ever see
                    // a non-integer numeric value, flip int_only false and
                    // start accumulating floats (after promoting the existing
                    // integer accumulator into float space).
                    if let PropertyValue::Integer(i) = prop {
                        let i = *i;
                        if *int_only {
                            *int_acc += i;
                        } else {
                            *float_acc += i as f64;
                        }
                    } else if let Some(f) = prop.as_float() {
                        if *int_only {
                            *int_only = false;
                            *float_acc = *int_acc as f64;
                        }
                        *float_acc += f;
                    }
                    // Non-numeric values silently skipped (matches pre-existing behaviour).
                }
            }
            AggregatorState::Avg { sum, count } => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() { *sum += f; *count += 1; }
                    else if let Some(i) = prop.as_integer() { *sum += i as f64; *count += 1; }
                }
            }
            // Aggregates ignore nulls — Cypher's rule, and the reason min/max must SKIP a
            // null input rather than compare it. Comparing is what broke both, in opposite
            // directions: while null sorted smallest it won every min(), and once ordering
            // was corrected so null sorts greatest (#369) it would have won every max().
            // Neither is a comparator problem; the accumulator simply must not see nulls.
            // Cypher's orderability, not `PropertyValue`'s derived `Ord`.
            //
            // That `Ord` backs the B-tree property index and orders Boolean,
            // Number, String, ..., Array, Map, Null; Cypher orders
            // Map < List < String < Boolean < Number < null. Over mixed input
            // the two disagree about which value is smallest, so
            // `min([1, 'a', [1,2], 0.2, 'b'])` answered `0.2` where openCypher
            // answers `[1, 2]`, and `max` answered the list where the answer
            // is `1` (#960).
            //
            // `graph::property::cypher_order` is the comparator ORDER BY uses.
            // Both orders exist on purpose and neither can be dropped -- see
            // its doc comment -- so the fix is for each caller to ask for the
            // one it means.
            AggregatorState::Min(curr) => {
                if let Some(prop) = value.as_property() {
                    if matches!(prop, PropertyValue::Null) {
                        return;
                    }
                    let smaller = curr.as_ref().is_none_or(|c| {
                        crate::graph::property::cypher_order(prop, c)
                            == std::cmp::Ordering::Less
                    });
                    if smaller {
                        *curr = Some(prop.clone());
                    }
                }
            }
            AggregatorState::Max(curr) => {
                if let Some(prop) = value.as_property() {
                    if matches!(prop, PropertyValue::Null) {
                        return;
                    }
                    let larger = curr.as_ref().is_none_or(|c| {
                        crate::graph::property::cypher_order(prop, c)
                            == std::cmp::Ordering::Greater
                    });
                    if larger {
                        *curr = Some(prop.clone());
                    }
                }
            }
            AggregatorState::Collect(items) => {
                // collect() drops nulls, like every other aggregate (#358) — otherwise the
                // list carries holes that every consumer has to filter again.
                let is_null = matches!(value, Value::Null)
                    || matches!(value.as_property(), Some(PropertyValue::Null));
                if !is_null {
                    items.push(value.clone());
                }
            }
            AggregatorState::CollectDistinct(set) => {
                if let Some(prop) = value.as_property().filter(|p| !matches!(p, PropertyValue::Null)) {
                    if !prop.is_null() {
                        set.insert(prop.clone());
                    }
                }
            }
            AggregatorState::Percentile { values, .. } => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() { values.push(f); }
                    else if let Some(i) = prop.as_integer() { values.push(i as f64); }
                }
            }
            AggregatorState::StDev { values, .. } => {
                if let Some(prop) = value.as_property() {
                    if let Some(f) = prop.as_float() { values.push(f); }
                    else if let Some(i) = prop.as_integer() { values.push(i as f64); }
                }
            }
        }
    }

    /// Fold another partial aggregate of the same shape into this one.
    ///
    /// Every aggregate here is associative and commutative, which is what makes
    /// grouping on identity and merging afterwards legal: partial states over a
    /// partition of the input combine to the state over the whole input.
    /// `collect` is the one to look at twice — list order is the order rows
    /// arrived, which was already unspecified across groups, and concatenation
    /// keeps each partial run contiguous.
    fn merge(&mut self, other: Self) {
        match (self, other) {
            (AggregatorState::Count(a), AggregatorState::Count(b)) => *a += b,
            (AggregatorState::CountDistinct(a), AggregatorState::CountDistinct(b)) => a.merge(b),
            (
                AggregatorState::Sum { int_acc, float_acc, int_only },
                AggregatorState::Sum { int_acc: bi, float_acc: bf, int_only: bint },
            ) => {
                if *int_only && bint {
                    *int_acc += bi;
                } else {
                    // Whichever side is still integral has to be promoted
                    // before the two float accumulators can be added.
                    if *int_only {
                        *int_only = false;
                        *float_acc = *int_acc as f64;
                    }
                    *float_acc += if bint { bi as f64 } else { bf };
                }
            }
            (AggregatorState::Avg { sum, count }, AggregatorState::Avg { sum: bs, count: bc }) => {
                *sum += bs;
                *count += bc;
            }
            // The same comparator as the accumulator above, or a parallel
            // aggregation would answer differently from a sequential one.
            (AggregatorState::Min(a), AggregatorState::Min(b)) => {
                if let Some(b) = b {
                    let smaller = a.as_ref().is_none_or(|c| {
                        crate::graph::property::cypher_order(&b, c)
                            == std::cmp::Ordering::Less
                    });
                    if smaller {
                        *a = Some(b);
                    }
                }
            }
            (AggregatorState::Max(a), AggregatorState::Max(b)) => {
                if let Some(b) = b {
                    let larger = a.as_ref().is_none_or(|c| {
                        crate::graph::property::cypher_order(&b, c)
                            == std::cmp::Ordering::Greater
                    });
                    if larger {
                        *a = Some(b);
                    }
                }
            }
            (AggregatorState::Collect(a), AggregatorState::Collect(b)) => a.extend(b.clone()),
            (AggregatorState::CollectDistinct(a), AggregatorState::CollectDistinct(b)) => a.extend(b),
            (AggregatorState::Percentile { values, .. }, AggregatorState::Percentile { values: b, .. }) => {
                values.extend(b)
            }
            (AggregatorState::StDev { values, .. }, AggregatorState::StDev { values: b, .. }) => {
                values.extend(b)
            }
            // Both sides are built from the same `AggregateFunction`, so a
            // mismatch is a construction bug rather than bad input.
            (a, b) => debug_assert!(false, "cannot merge {a:?} with {b:?}"),
        }
    }

    fn result(&self) -> Value {
        match self {
            AggregatorState::Count(c) => Value::Property(PropertyValue::Integer(*c)),
            AggregatorState::CountDistinct(set) => Value::Property(PropertyValue::Integer(set.len() as i64)),
            AggregatorState::Sum { int_acc, float_acc, int_only } => {
                if *int_only {
                    Value::Property(PropertyValue::Integer(*int_acc))
                } else {
                    Value::Property(PropertyValue::Float(*float_acc))
                }
            }
            AggregatorState::Avg { sum, count } => {
                if *count == 0 { Value::Null }
                else { Value::Property(PropertyValue::Float(*sum / *count as f64)) }
            }
            AggregatorState::Min(val) => val.clone().map(Value::Property).unwrap_or(Value::Null),
            AggregatorState::Max(val) => val.clone().map(Value::Property).unwrap_or(Value::Null),
            AggregatorState::Collect(items) => {
                // A list of scalars stays a `PropertyValue::Array`, exactly as
                // before, so every existing consumer of that shape is
                // untouched; only a collection holding entities needs the
                // wider `Value::List` (#669).
                if items.iter().all(|v| matches!(v, Value::Property(_))) {
                    Value::Property(PropertyValue::Array(
                        items
                            .iter()
                            .map(|v| match v {
                                Value::Property(p) => p.clone(),
                                _ => PropertyValue::Null,
                            })
                            .collect(),
                    ))
                } else {
                    Value::List(items.clone())
                }
            }
            AggregatorState::CollectDistinct(set) => Value::Property(PropertyValue::Array(set.iter().cloned().collect())),
            AggregatorState::Percentile { values, pct, cont } => {
                if values.is_empty() { return Value::Null; }
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = sorted.len();
                if *cont {
                    // Linear interpolation
                    let idx = pct * (n - 1) as f64;
                    let lo = idx.floor() as usize;
                    let hi = idx.ceil() as usize;
                    let frac = idx - lo as f64;
                    let result = sorted[lo] * (1.0 - frac) + sorted[hi.min(n - 1)] * frac;
                    Value::Property(PropertyValue::Float(result))
                } else {
                    // Nearest rank
                    let idx = (pct * n as f64).ceil() as usize;
                    Value::Property(PropertyValue::Float(sorted[idx.saturating_sub(1).min(n - 1)]))
                }
            }
            AggregatorState::StDev { values, population } => {
                if values.is_empty() { return Value::Null; }
                let n = values.len() as f64;
                let mean = values.iter().sum::<f64>() / n;
                let variance: f64 = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>();
                let denom = if *population { n } else { (n - 1.0).max(1.0) };
                Value::Property(PropertyValue::Float((variance / denom).sqrt()))
            }
        }
    }
}

/// Aggregate operator: GROUP BY + Aggregations
pub struct AggregateOperator {
    input: OperatorBox,
    group_by: Vec<(Expression, String)>, // (expr, alias)
    aggregates: Vec<AggregateFunction>,
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl AggregateOperator {
    pub fn new(
        input: OperatorBox, 
        group_by: Vec<(Expression, String)>, 
        aggregates: Vec<AggregateFunction>
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }


    fn evaluate_expression(expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            // Delegates rather than adding a sixth copy of this logic; the
            // standalone evaluator is the one implementation (#654).
            Expression::ListExpr(_) | Expression::MapExpr(_) => {
                eval_expression(expr, record, store)
            }
            Expression::Variable(var) => {
                Ok(record.get(var).cloned().unwrap_or(Value::Null))
            }
            Expression::Property { variable, property } => {
                read_property(record, variable, property, store, true)
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = Self::evaluate_expression(left, record, store)?;
                let right_val = Self::evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = Self::evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args.iter()
                    .map(|a| Self::evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case { operand, when_clauses, else_result } => {
                eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), |e| Self::evaluate_expression(e, record, store))
            }
            Expression::Index { expr, index } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let idx = Self::evaluate_expression(index, record, store)?;
                eval_index(collection, idx, store)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let s = match start { Some(s) => Some(Self::evaluate_expression(s, record, store)?), None => None };
                let en = match end { Some(e) => Some(Self::evaluate_expression(e, record, store)?), None => None };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery { pattern, where_clause, .. } => {
                eval_exists_subquery(pattern, where_clause.as_deref(), record, store)
            }
            Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
                eval_list_comprehension(variable, list_expr, filter.as_deref(), map_expr, record, store)
            }
            Expression::PredicateFunction { name, variable, list_expr, predicate } => {
                eval_predicate_function(name, variable, list_expr, predicate, record, store)
            }
            Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
                eval_reduce(accumulator, init, variable, list_expr, expression, record, store)
            }
            Expression::PatternComprehension { pattern, filter, projection } => {
                eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store)
            }
            Expression::PathVariable(var) => {
                record.get(var).cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned()
                    .ok_or_else(|| ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name)))
            }
        }
    }
}

impl PhysicalOperator for AggregateOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    /// Pull the input **mutably** before aggregating.
    ///
    /// Both this and `WithBarrierOperator` need it, for the same reason: the
    /// default `next_mut` delegates to `next`, which reads its input
    /// read-only — so any write operator below a materialising operator never
    /// ran its mutating path. `MATCH (n) SET n.x = 1 WITH n RETURN n.x` returned the
    /// *old* value: the query succeeded, the barrier produced rows, and the
    /// write silently did not happen. That is the real reason the grammar only
    /// ever allowed writes after the last projection, and it had to be fixed
    /// before a write before a `WITH` could be planned at all.
    ///
    /// The input is drained into a `MaterializedOperator` rather than
    /// threading `&mut GraphStore` through `execute_all` and its grouping
    /// helpers: the barrier is the most intricate operator here, and this
    /// leaves its aggregation untouched. A barrier already materialises its
    /// whole input, so nothing is buffered that would not have been.
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            let mut rows = Vec::new();
            while let Some(batch) = self.input.next_batch_mut(store, tenant_id, 65536)? {
                rows.extend(batch.records);
            }
            self.input = Box::new(MaterializedOperator::new(rows));
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    fn next_batch_mut(
        &mut self,
        store: &mut GraphStore,
        tenant_id: &str,
        batch_size: usize,
    ) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            match self.next_mut(store, tenant_id)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records, columns: vec![] }))
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = self.results.next() {
                batch.push(record);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records: batch, columns: Vec::new() }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
    }

    fn describe(&self) -> OperatorDescription {
        let agg_strs: Vec<String> = self.aggregates.iter().map(|a| {
            format!("{}({}) AS {}", format!("{:?}", a.func).to_lowercase(), format_expression(&a.expr), a.alias)
        }).collect();
        let group_strs: Vec<String> = self.group_by.iter().map(|(e, a)| format!("{} AS {}", format_expression(e), a)).collect();
        let mut details = Vec::new();
        if !group_strs.is_empty() { details.push(format!("group_by=[{}]", group_strs.join(", "))); }
        details.push(format!("aggs=[{}]", agg_strs.join(", ")));
        OperatorDescription {
            name: "Aggregate".to_string(),
            details: details.join(", "),
            children: vec![self.input.describe()],
        }
    }
}


/// Per-row evaluation of one expression, specialised where it is a plain
/// `x.prop`.
///
/// Built once per fold rather than held on the operator, because the operator
/// borrows `self.aggregates` immutably across the loop and a cursor needs
/// `&mut` to memoise its column.
enum RowReader {
    /// `x.prop` — the column is located once (#557).
    Cursor(PropertyCursor),
    /// Anything else: a literal, an arithmetic expression, a function call.
    General(Expression),
}

impl RowReader {
    fn for_expression(expr: &Expression) -> Self {
        match expr {
            Expression::Property { variable, property } => {
                RowReader::Cursor(PropertyCursor::new(variable.as_str(), property.as_str()))
            }
            other => RowReader::General(other.clone()),
        }
    }

    fn read(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
        match self {
            RowReader::Cursor(cursor) => Ok(Value::Property(cursor.read(record, store))),
            RowReader::General(expr) => AggregateOperator::evaluate_expression(expr, record, store),
        }
    }
}

/// The group key of a row, before the key expressions are evaluated.
///
/// Grouping on a node's *identity* is at least as fine as grouping on any
/// tuple of its properties: two rows with the same node necessarily agree on
/// every property of it. It is not equivalent — two *different* nodes may
/// share a key tuple — which is why the identity path has a merge step.
///
/// The point of the enum is that building one costs nothing for a node: a
/// `u64` copy, against resolving and cloning a property per row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum IdentityKey {
    Node(u64),
    Edge(u64),
    /// Anything else. Boxed because `Value` is 144 bytes -- `Value::Node`
    /// embeds a whole `Node` inline (#570) -- and carrying that inline would
    /// set the width of every entry in the group table for the sake of a case
    /// that is reached only where a key expression reads a property of
    /// something that is not a graph element. That is a degenerate query, not
    /// a hot path.
    Other(Box<Value>),
}

impl IdentityKey {
    fn of(value: Option<&Value>) -> Self {
        match value {
            Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => IdentityKey::Node(id.0),
            Some(Value::EdgeRef(id, ..)) | Some(Value::Edge(id, _)) => IdentityKey::Edge(id.0),
            Some(other) => IdentityKey::Other(Box::new(other.clone())),
            None => IdentityKey::Other(Box::new(Value::Null)),
        }
    }

    /// A `Value` to evaluate the group-by expressions against, rebuilt from the
    /// key rather than stored beside it.
    ///
    /// Keeping a representative `Value` per group cost 144 bytes an entry on
    /// top of the key, for something the id already determines. `NodeRef`
    /// resolves properties from the store, which is what a materialised
    /// `Value::Node` would do here too -- this is the read executor, and a
    /// node's properties come from the store either way.
    fn probe_value(&self, store: &GraphStore) -> Value {
        match self {
            IdentityKey::Node(id) => Value::NodeRef(NodeId(*id)),
            IdentityKey::Edge(id) => {
                let edge_id = crate::graph::EdgeId(*id);
                match store.get_edge(edge_id) {
                    Some(edge) => Value::EdgeRef(
                        edge_id,
                        edge.source,
                        edge.target,
                        edge.edge_type.clone(),
                    ),
                    None => Value::Null,
                }
            }
            IdentityKey::Other(value) => (**value).clone(),
        }
    }
}

impl AggregateOperator {
    /// True when every aggregate can be satisfied by counting rows, so the
    /// argument expressions never have to be evaluated.
    ///
    /// Only `count(*)` qualifies — a literal, which is never null.
    ///
    /// `count(var)` used to qualify too, on the reasoning that a bound node or
    /// edge cannot be null per row. `OPTIONAL MATCH` breaks exactly that: it
    /// binds the variable to `Null` on a row that did not match, so
    /// `MATCH (p) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN count(f)` counted the
    /// unmatched rows and reported 1 friend for a person with none (#600).
    ///
    /// Giving up the variable case costs little: evaluating one is
    /// `record.get(var)` at ~4 ns, against a property read at ~17 ns. The fast
    /// path exists to avoid *property* evaluation (#358), and it still does.
    fn all_simple_count(aggregates: &[AggregateFunction]) -> bool {
        aggregates.iter().all(|a| {
            matches!(a.func, AggregateType::Count)
                && !a.distinct
                && matches!(a.expr, Expression::Literal(_))
        })
    }

    /// The single variable every group-by key is a property of, if there is one.
    ///
    /// `RETURN forum.id, forum.title, count(*)` — LDBC IC5's shape — keys on
    /// two properties of one node. Evaluating them per row resolves a property
    /// (and clones a string) 1.7M times to distinguish 96,862 groups that the
    /// node id already distinguishes.
    fn identity_group_variable(&self) -> Option<String> {
        let mut found: Option<&str> = None;
        for (expr, _) in &self.group_by {
            match expr {
                Expression::Property { variable, .. } => match found {
                    None => found = Some(variable),
                    Some(v) if v == variable => {}
                    Some(_) => return None,
                },
                _ => return None,
            }
        }
        found.map(|v| v.to_string())
    }

    /// Group on the identity of `var`, then resolve the key expressions once
    /// per group rather than once per row (#521).
    ///
    /// Two phases, because identity grouping is finer than key grouping:
    ///
    /// 1. fold rows into partial aggregates keyed by `var`'s identity — no
    ///    property resolution, no allocation, a `u64` hash per row;
    /// 2. resolve each group's key tuple once and merge any groups whose
    ///    tuples are equal.
    ///
    /// Phase 2 is what keeps this exactly equivalent to the general path. Two
    /// distinct nodes carrying the same `(id, title)` are one group in Cypher,
    /// and without the merge they would come out as two.
    fn execute_all_by_identity(&mut self, var: &str, store: &GraphStore) -> ExecutionResult<()> {
        let all_simple_count = Self::all_simple_count(&self.aggregates);
        let mut readers: Vec<RowReader> =
            self.aggregates.iter().map(|a| RowReader::for_expression(&a.expr)).collect();

        // Keyed on identity alone. Phase 2 rebuilds what it needs to resolve
        // properties against from the key itself, so no `Value` is stored per
        // group -- which at IC5's 96,862 groups took the table from ~320 bytes
        // an entry to ~40 (#570).
        let mut groups: rustc_hash::FxHashMap<IdentityKey, Vec<AggregatorState>> =
            rustc_hash::FxHashMap::default();

        let batch_size = 65536;
        let mut batch_count = 0u64;
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            batch_count += 1;
            if batch_count % 10 == 0 {
                check_deadline()?;
            }
            for record in batch.records {
                let key = IdentityKey::of(record.get(var));
                let states = groups.entry(key).or_insert_with(|| {
                    self.aggregates
                        .iter()
                        .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
                        .collect()
                });

                if all_simple_count {
                    for state in states.iter_mut() {
                        if let AggregatorState::Count(c) = state {
                            *c += 1;
                        }
                    }
                } else {
                    for (i, reader) in readers.iter_mut().enumerate() {
                        let val = reader.read(&record, store)?;
                        states[i].update(&val);
                        if let Some(p) = &self.aggregates[i].percentile {
                            states[i].set_percentile(&eval_expression(p, &record, store)?)?;
                        }
                    }
                }
            }
        }

        // Phase 2. Every key expression is a property of `var` (that is what
        // `identity_group_variable` established), so a record binding `var`
        // alone is enough to evaluate them.
        let mut merged: rustc_hash::FxHashMap<Vec<Value>, Vec<AggregatorState>> =
            rustc_hash::FxHashMap::with_capacity_and_hasher(groups.len(), Default::default());
        for (key, states) in groups {
            let mut probe = Record::new();
            probe.bind(var.to_string(), key.probe_value(store));
            let mut tuple = Vec::with_capacity(self.group_by.len());
            for (expr, _) in &self.group_by {
                tuple.push(Self::evaluate_expression(expr, &probe, store)?);
            }
            match merged.entry(tuple) {
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    for (dst, src) in slot.get_mut().iter_mut().zip(states) {
                        dst.merge(src);
                    }
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(states);
                }
            }
        }

        let mut output_records = Vec::with_capacity(merged.len());
        for (tuple, states) in merged {
            let mut record = Record::new();
            for (i, (_, alias)) in self.group_by.iter().enumerate() {
                record.bind(alias.clone(), tuple[i].clone());
            }
            for (i, agg) in self.aggregates.iter().enumerate() {
                record.bind(agg.alias.clone(), states[i].result());
            }
            output_records.push(record);
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }

    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Fast path: no group-by (global aggregate) — avoids HashMap entirely
        if self.group_by.is_empty() {
            return self.execute_all_no_group(store);
        }
        // Fast path: the keys are all properties of one variable, so its
        // identity groups the rows and the properties are resolved per group.
        if let Some(var) = self.identity_group_variable() {
            return self.execute_all_by_identity(&var, store);
        }
        // Fast path: single group-by key avoids Vec allocation per record
        if self.group_by.len() == 1 {
            return self.execute_all_single_key(store);
        }

        // FxHashMap is 2-3x faster than std HashMap on simple keys (no
        // SipHash overhead). Aggregation is a hot path on B3 — CT08, CT10
        // each insert ~1M (group_key, aggregator) pairs.
        let mut groups: rustc_hash::FxHashMap<Vec<Value>, Vec<AggregatorState>> =
            rustc_hash::FxHashMap::default();
        let all_simple_count = Self::all_simple_count(&self.aggregates);
        let mut key_readers: Vec<RowReader> =
            self.group_by.iter().map(|(e, _)| RowReader::for_expression(e)).collect();
        let mut readers: Vec<RowReader> =
            self.aggregates.iter().map(|a| RowReader::for_expression(&a.expr)).collect();

        // Reused across rows. `entry` needs an owned key, so building the tuple
        // straight into the map allocated a `Vec` per input row and freed it
        // again on every hit -- 1.68M allocations for 96,862 groups on IC5.
        // Probing first means the allocation happens once per group.
        let mut scratch: Vec<Value> = Vec::with_capacity(self.group_by.len());

        let batch_size = 65536;
        let mut batch_count = 0u64;
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            batch_count += 1;
            if batch_count % 10 == 0 { check_deadline()?; }
            for record in batch.records {
                scratch.clear();
                for reader in key_readers.iter_mut() {
                    scratch.push(reader.read(&record, store)?);
                }

                let states = match groups.get_mut(&scratch) {
                    Some(states) => states,
                    None => groups.entry(scratch.clone()).or_insert_with(|| {
                        self.aggregates.iter().map(|agg| AggregatorState::new(&agg.func, agg.distinct)).collect()
                    }),
                };

                if all_simple_count {
                    for state in states.iter_mut() {
                        if let AggregatorState::Count(c) = state {
                            *c += 1;
                        }
                    }
                } else {
                    for (i, reader) in readers.iter_mut().enumerate() {
                        let val = reader.read(&record, store)?;
                        states[i].update(&val);
                        if let Some(p) = &self.aggregates[i].percentile {
                            states[i].set_percentile(&eval_expression(p, &record, store)?)?;
                        }
                    }
                }
            }
        }

        let mut output_records = Vec::new();
        for (key, states) in groups {
            let mut record = Record::new();
            for (i, (_, alias)) in self.group_by.iter().enumerate() {
                record.bind(alias.clone(), key[i].clone());
            }
            for (i, agg) in self.aggregates.iter().enumerate() {
                record.bind(agg.alias.clone(), states[i].result());
            }
            output_records.push(record);
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }

    /// Optimized path for single group-by key: uses FxHashMap<Value, ...> instead of FxHashMap<Vec<Value>, ...>
    fn execute_all_single_key(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut groups: rustc_hash::FxHashMap<Value, Vec<AggregatorState>> =
            rustc_hash::FxHashMap::default();
        let mut key_reader = RowReader::for_expression(&self.group_by[0].0);
        let mut readers: Vec<RowReader> =
            self.aggregates.iter().map(|a| RowReader::for_expression(&a.expr)).collect();

        let all_simple_count = Self::all_simple_count(&self.aggregates);

        let batch_size = 65536;
        let mut batch_count = 0u64;
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            batch_count += 1;
            if batch_count % 10 == 0 { check_deadline()?; }
            for record in batch.records {
                let key = key_reader.read(&record, store)?;

                let states = groups.entry(key).or_insert_with(|| {
                    self.aggregates.iter().map(|agg| AggregatorState::new(&agg.func, agg.distinct)).collect()
                });

                if all_simple_count {
                    // Fast path: just increment all counters without evaluating aggregate expressions
                    for state in states.iter_mut() {
                        if let AggregatorState::Count(c) = state {
                            *c += 1;
                        }
                    }
                } else {
                    for (i, reader) in readers.iter_mut().enumerate() {
                        let val = reader.read(&record, store)?;
                        states[i].update(&val);
                        if let Some(p) = &self.aggregates[i].percentile {
                            states[i].set_percentile(&eval_expression(p, &record, store)?)?;
                        }
                    }
                }
            }
        }

        let group_alias = &self.group_by[0].1;
        let mut output_records = Vec::new();
        for (key, states) in groups {
            let mut record = Record::new();
            record.bind(group_alias.clone(), key);
            for (i, agg) in self.aggregates.iter().enumerate() {
                record.bind(agg.alias.clone(), states[i].result());
            }
            output_records.push(record);
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }

    /// Optimized path for no group-by: single global aggregate, no HashMap needed
    fn execute_all_no_group(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut states: Vec<AggregatorState> = self.aggregates.iter()
            .map(|agg| AggregatorState::new(&agg.func, agg.distinct))
            .collect();

        let all_simple_count = Self::all_simple_count(&self.aggregates);
        let mut readers: Vec<RowReader> =
            self.aggregates.iter().map(|a| RowReader::for_expression(&a.expr)).collect();

        let batch_size = 65536;
        let mut batch_count = 0u64;
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            batch_count += 1;
            if batch_count % 10 == 0 { check_deadline()?; }

            if all_simple_count {
                // Ultra-fast: just count rows
                let row_count = batch.records.len() as i64;
                for state in states.iter_mut() {
                    if let AggregatorState::Count(c) = state {
                        *c += row_count;
                    }
                }
            } else {
                for record in batch.records {
                    for (i, reader) in readers.iter_mut().enumerate() {
                        let val = reader.read(&record, store)?;
                        states[i].update(&val);
                        if let Some(p) = &self.aggregates[i].percentile {
                            states[i].set_percentile(&eval_expression(p, &record, store)?)?;
                        }
                    }
                }
            }
        }

        let mut record = Record::new();
        for (i, agg) in self.aggregates.iter().enumerate() {
            record.bind(agg.alias.clone(), states[i].result());
        }
        self.results = vec![record].into_iter();
        self.executed = true;
        Ok(())
    }
}

/// Adjacency-aware count aggregate (ADR-017 Phase 1).
///
/// For patterns like `MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j.title, count(a) AS articles`
/// this computes each group's count by reading the adjacency list of the
/// grouped endpoint directly, instead of walking every edge through a generic
/// aggregate. Input must produce the grouped endpoint nodes.
///
/// Output per row: `{ grouped_var: NodeRef(id), count_alias: Integer(degree) }`.
/// Downstream operators (Project, Sort, Limit) consume these like any other
/// aggregate result.
pub struct AdjacencyCountAggregateOperator {
    input: OperatorBox,
    grouped_var: String,
    count_alias: String,
    edge_type: EdgeType,
    /// Direction relative to the grouped endpoint:
    /// - Outgoing = out-degree of the grouped node on this edge type
    /// - Incoming = in-degree of the grouped node on this edge type
    direction: Direction,
    /// Optional property names to group on, in user-RETURN order.
    /// When empty, emit one record per input node (per-node mode — fast,
    /// correct only when downstream cannot collapse rows).
    /// When non-empty, accumulate per-group counts in an internal HashMap
    /// and emit one record per distinct property-value combination —
    /// avoids the planner-side post-aggregate hash-group entirely.
    group_by_props: Vec<String>,
    /// The label the pattern requires of the *neighbour*, if any.
    ///
    /// A degree is not the number of pattern matches: it counts every edge of
    /// the type whatever sits at the far end. Without this the operator
    /// answered `MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name, count(f)` by
    /// counting Ada's edge to an `:Animal` as well (#601).
    neighbor_label: Option<Label>,
    /// `count(DISTINCT neighbor)` semantics. When true, build_grouped_iter
    /// accumulates a HashSet of neighbor NodeIds per group instead of
    /// summing degrees — required when the same neighbor may appear under
    /// multiple grouped nodes that share a group key, or when parallel
    /// edges of the same type exist between the same pair.
    count_distinct: bool,
    /// Lazy-built grouped output, populated on first `next()` when
    /// `group_by_props` is non-empty.
    grouped_iter: Option<std::vec::IntoIter<GroupedRow>>,
}

/// One pre-aggregated row for the in-operator group-by mode.
struct GroupedRow {
    /// Property values in the order of `group_by_props` (used by the
    /// downstream Project to look up `g.prop` directly).
    prop_values: Vec<PropertyValue>,
    /// Sum of per-node degrees across all nodes in this group.
    count: i64,
    /// One representative node from this group. Used by Project so that
    /// expressions like `g` (the variable itself) still bind to a real
    /// NodeRef. All nodes in the group share the same `prop_values`.
    sample_node: NodeId,
}

impl AdjacencyCountAggregateOperator {
    pub fn new(
        input: OperatorBox,
        grouped_var: String,
        count_alias: String,
        edge_type: EdgeType,
        direction: Direction,
    ) -> Self {
        Self {
            input,
            grouped_var,
            count_alias,
            edge_type,
            direction,
            group_by_props: Vec::new(),
            count_distinct: false,
            neighbor_label: None,
            grouped_iter: None,
        }
    }

    /// Require the counted neighbour to carry this label.
    pub fn with_neighbor_label(mut self, label: Option<Label>) -> Self {
        self.neighbor_label = label;
        self
    }

    /// Enable the in-operator group-by path. When non-empty, the operator
    /// accumulates per-group counts in a HashMap keyed on the listed
    /// property values of the grouped node, then emits one record per
    /// group rather than per node. This is the correctness-preserving
    /// fast path that replaces the planner-side post-aggregate.
    pub fn with_group_by_props(mut self, props: Vec<String>) -> Self {
        self.group_by_props = props;
        self
    }

    /// Switch to `count(DISTINCT neighbor)` semantics. Forces the grouped
    /// path even with variable-only GROUP BY (so per-node parallel edges
    /// dedupe correctly).
    pub fn with_count_distinct(mut self, distinct: bool) -> Self {
        self.count_distinct = distinct;
        self
    }

    fn build_grouped_iter(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.count_distinct {
            self.build_grouped_iter_distinct(store)
        } else {
            self.build_grouped_iter_sum(store)
        }
    }

    fn build_grouped_iter_sum(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut groups: rustc_hash::FxHashMap<Vec<PropertyValue>, (NodeId, i64)> =
            rustc_hash::FxHashMap::default();

        loop {
            let input_record = match self.input.next(store)? {
                Some(r) => r,
                None => break,
            };

            let node_id = match input_record.get(&self.grouped_var) {
                Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => *id,
                _ => continue,
            };

            let value_for_lookup = Value::NodeRef(node_id);
            let key: Vec<PropertyValue> = self
                .group_by_props
                .iter()
                .map(|p| value_for_lookup.resolve_property(p, store))
                .collect();

            let degree = self.degree_filtered(store, node_id) as i64;
            let entry = groups.entry(key).or_insert((node_id, 0));
            entry.1 += degree;
        }

        let rows: Vec<GroupedRow> = groups
            .into_iter()
            // A required MATCH yields no row for a node the pattern does not
            // match, so a group whose count is zero must not be emitted. The
            // detector rejects OPTIONAL MATCH, where the zeros would be
            // wanted, so this is unconditional (#601).
            .filter(|(_, (_, count))| *count > 0)
            .map(|(prop_values, (sample_node, count))| GroupedRow {
                prop_values,
                count,
                sample_node,
            })
            .collect();
        self.grouped_iter = Some(rows.into_iter());
        Ok(())
    }

    fn build_grouped_iter_distinct(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut groups: rustc_hash::FxHashMap<
            Vec<PropertyValue>,
            (NodeId, rustc_hash::FxHashSet<NodeId>),
        > = rustc_hash::FxHashMap::default();

        loop {
            let input_record = match self.input.next(store)? {
                Some(r) => r,
                None => break,
            };
            let node_id = match input_record.get(&self.grouped_var) {
                Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => *id,
                _ => continue,
            };
            let key: Vec<PropertyValue> = if self.group_by_props.is_empty() {
                vec![PropertyValue::Integer(node_id.as_u64() as i64)]
            } else {
                let v = Value::NodeRef(node_id);
                self.group_by_props
                    .iter()
                    .map(|p| v.resolve_property(p, store))
                    .collect()
            };
            let entry = groups
                .entry(key)
                .or_insert_with(|| (node_id, rustc_hash::FxHashSet::default()));
            self.collect_neighbors_into(store, node_id, &mut entry.1);
        }

        let rows: Vec<GroupedRow> = groups
            .into_iter()
            .map(|(prop_values, (sample_node, neighbor_set))| GroupedRow {
                prop_values,
                count: neighbor_set.len() as i64,
                sample_node,
            })
            .collect();
        self.grouped_iter = Some(rows.into_iter());
        Ok(())
    }

    /// Append every neighbor of `node_id` reachable via `edge_type` in
    /// the operator's direction to `set`. Uses the typed-walk closures
    /// for the same allocation-/clone-free reasons as `degree_filtered`.
    fn collect_neighbors_into(
        &self,
        store: &GraphStore,
        node_id: NodeId,
        set: &mut rustc_hash::FxHashSet<NodeId>,
    ) {
        match self.direction {
            Direction::Outgoing => {
                store.for_each_outgoing_neighbor_of_type(node_id, &self.edge_type, |tgt| {
                    set.insert(tgt);
                });
            }
            Direction::Incoming => {
                store.for_each_incoming_neighbor_of_type(node_id, &self.edge_type, |src| {
                    set.insert(src);
                });
            }
            Direction::Both => {
                store.for_each_outgoing_neighbor_of_type(node_id, &self.edge_type, |tgt| {
                    set.insert(tgt);
                });
                store.for_each_incoming_neighbor_of_type(node_id, &self.edge_type, |src| {
                    set.insert(src);
                });
            }
        }
    }

    /// Count the incident edges of `node_id` whose type matches
    /// `edge_type` in the operator's direction. Uses the typed-walk
    /// helpers (`incoming_degree_for_type` / `outgoing_degree_for_type`)
    /// which avoid Vec alloc + per-edge EdgeType clone.
    fn degree_filtered(&self, store: &GraphStore, node_id: NodeId) -> usize {
        let Some(label) = &self.neighbor_label else {
            // No constraint on the far end, so the degree *is* the match count
            // and the adjacency index answers in O(1). This is the shape the
            // operator exists for.
            return match self.direction {
                Direction::Outgoing => store.outgoing_degree_for_type(node_id, &self.edge_type),
                Direction::Incoming => store.incoming_degree_for_type(node_id, &self.edge_type),
                Direction::Both => {
                    store.outgoing_degree_for_type(node_id, &self.edge_type)
                        + store.incoming_degree_for_type(node_id, &self.edge_type)
                }
            };
        };

        // Constrained: walk and count the neighbours that carry the label.
        // O(degree) rather than O(1), which is what a correct answer costs --
        // and still cheaper than materialising a row per edge. The membership
        // probe is one hash of a `NodeId` (#592).
        let Some(members) = store.nodes_with_label(label) else {
            // No node carries the label, so nothing matches.
            return 0;
        };
        // `Some(&[])` matches no edge; a wildcard would be `None`, which is not
        // what an unknown edge type means (#520).
        let type_ids: Vec<u16> = store.edge_type_id(&self.edge_type).into_iter().collect();
        let filter = Some(type_ids.as_slice());

        let mut count = 0usize;
        match self.direction {
            Direction::Outgoing => {
                store.for_each_outgoing_neighbor(node_id, filter, |target, _| {
                    if members.contains(&target) {
                        count += 1;
                    }
                });
            }
            Direction::Incoming => {
                store.for_each_incoming_neighbor(node_id, filter, |source, _| {
                    if members.contains(&source) {
                        count += 1;
                    }
                });
            }
            Direction::Both => {
                store.for_each_outgoing_neighbor(node_id, filter, |target, _| {
                    if members.contains(&target) {
                        count += 1;
                    }
                });
                store.for_each_incoming_neighbor(node_id, filter, |source, _| {
                    if members.contains(&source) {
                        count += 1;
                    }
                });
            }
        }
        count
    }
}

impl PhysicalOperator for AdjacencyCountAggregateOperator {
    // A write beneath this operator refused with "requires mutable store
    // access", because the default `next_mut` delegates to `next` and `next`
    // reads its input read-only. Shared body rather than a second, mutable copy
    // of this operator's own logic -- see `drain_input_for_write` (#870).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        drain_input_for_write(&mut self.input, store, tenant_id)?;
        self.next(store)
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        // Grouped path: accumulate per-(prop_values) counts on first call,
        // then emit one record per group. Correctness-preserving fast path
        // for `RETURN n.prop, count(...)` patterns where multiple nodes may
        // share the same property value (NULL collapse, non-unique attrs).
        // Also activates for `count(DISTINCT)` so parallel-edge dedup is
        // honored even with variable-only GROUP BY.
        if !self.group_by_props.is_empty() || self.count_distinct {
            if self.grouped_iter.is_none() {
                self.build_grouped_iter(store)?;
            }
            let row = match self.grouped_iter.as_mut().and_then(|it| it.next()) {
                Some(r) => r,
                None => return Ok(None),
            };
            // Bind the sample node — downstream Project will resolve
            // properties from it. All nodes in the group share the same
            // values for `group_by_props`, so any group member is correct.
            let _ = row.prop_values;
            let mut out = Record::new();
            out.bind(self.grouped_var.clone(), Value::NodeRef(row.sample_node));
            out.bind(
                self.count_alias.clone(),
                Value::Property(PropertyValue::Integer(row.count)),
            );
            return Ok(Some(out));
        }

        loop {
            let input_record = match self.input.next(store)? {
                Some(r) => r,
                None => return Ok(None),
            };

            let node_id = match input_record.get(&self.grouped_var) {
                Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => *id,
                _ => {
                    // Upstream didn't bind the grouped variable to a node —
                    // indicates a planner bug; skip rather than crash.
                    continue;
                }
            };

            let count = self.degree_filtered(store, node_id);
            if count == 0 {
                // A required MATCH yields no row for a node the pattern does
                // not match. The detector rejects OPTIONAL MATCH, where the
                // zero would be wanted (#601).
                continue;
            }

            let mut out = input_record;
            out.bind(
                self.count_alias.clone(),
                Value::Property(PropertyValue::Integer(count as i64)),
            );
            return Ok(Some(out));
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.grouped_iter = None;
    }

    fn describe(&self) -> OperatorDescription {
        let dir = match self.direction {
            Direction::Outgoing => "->",
            Direction::Incoming => "<-",
            Direction::Both => "--",
        };
        OperatorDescription {
            name: "AdjacencyCountAggregate".to_string(),
            details: format!(
                "({}){}[:{}]{} count AS {}",
                self.grouped_var,
                dir,
                self.edge_type.as_str(),
                dir,
                self.count_alias
            ),
            children: vec![self.input.describe()],
        }
    }
}

/// Limit operator: LIMIT 10
/// Drains its input completely on the first pull, then replays the rows.
///
/// Cypher's rule is that `SKIP` and `LIMIT` trim the **result set** and not the
/// **side effects**: `CREATE (n:N) RETURN n LIMIT 0` still creates the node.
/// Without this, `LimitOperator(0)` never pulls, so the create beneath it never
/// runs and the write is silently skipped -- a query that reports success and
/// changes nothing (#866).
///
/// It is the standard "eager" barrier, placed between a write and a row-count
/// clause. Only there: making every `LIMIT` eager would undo the whole point of
/// a limit on a read, which is to stop early.
pub struct EagerOperator {
    input: OperatorBox,
    skip: usize,
    limit: Option<usize>,
    buffered: Vec<Record>,
    idx: usize,
    drained: bool,
}

impl EagerOperator {
    /// Wrap `input` so it runs to completion, then replay it trimmed by `skip`
    /// and `limit`.
    ///
    /// The trimming is **this operator's job** rather than a `Skip`/`Limit`
    /// above it, because `LimitOperator(0)` returns without pulling at all --
    /// so a lazy barrier beneath it is never reached and the write never runs.
    /// Being outermost is what makes the write happen.
    pub fn new(input: OperatorBox, skip: usize, limit: Option<usize>) -> Self {
        Self { input, skip, limit, buffered: Vec::new(), idx: 0, drained: false }
    }

    fn emit(&mut self) -> Option<Record> {
        let start = self.skip;
        let end = match self.limit {
            Some(l) => (start + l).min(self.buffered.len()),
            None => self.buffered.len(),
        };
        let pos = start + self.idx;
        if pos >= end {
            return None;
        }
        self.idx += 1;
        Some(self.buffered[pos].clone())
    }
}

impl PhysicalOperator for EagerOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.drained {
            while let Some(r) = self.input.next(store)? {
                self.buffered.push(r);
            }
            self.drained = true;
        }
        Ok(self.emit())
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.drained {
            while let Some(r) = self.input.next_mut(store, tenant_id)? {
                self.buffered.push(r);
            }
            self.drained = true;
        }
        Ok(self.emit())
    }

    /// **Refuses a pushed-down limit.** Accepting one would let the limit reach
    /// the write again, which is the defect this operator exists to prevent.
    fn try_push_limit(&mut self, _n: usize) -> bool {
        false
    }

    fn reset(&mut self) {
        self.input.reset();
        self.buffered.clear();
        self.idx = 0;
        self.drained = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Eager".to_string(),
            details: String::new(),
            children: vec![self.input.describe()],
        }
    }
}

/// Binds `p` for a **named path on a write pattern**: `CREATE p = (a)-[:R]->(b)`.
///
/// The parser has always captured `path_variable` on a `CREATE`/`MERGE`
/// pattern, and the write operators never bound it, so `RETURN p` failed with
/// `VariableNotFound("p")` — a query that parses and then cannot name what it
/// just made (#876).
///
/// It reads the node and relationship variables the write already bound and
/// assembles the path from them, rather than teaching every write operator to
/// build one. Anonymous positions get a synthetic handle from the planner for
/// the same reason edges do: something has to be nameable for the path to
/// reference it.
pub struct BindPathOperator {
    input: OperatorBox,
    /// `(path variable, node handles in order, relationship handles in order)`.
    paths: Vec<(String, Vec<String>, Vec<String>)>,
}

impl BindPathOperator {
    /// Wrap `input`, binding each named path from the handles listed.
    pub fn new(input: OperatorBox, paths: Vec<(String, Vec<String>, Vec<String>)>) -> Self {
        Self { input, paths }
    }

    fn bind(&self, mut record: Record) -> Record {
        for (path_var, node_vars, edge_vars) in &self.paths {
            let nodes: Vec<NodeId> =
                node_vars.iter().filter_map(|v| record.get(v).and_then(|x| x.node_id())).collect();
            // A path whose nodes are not all bound is not a path; leaving the
            // variable unbound gives the caller the same "not found" it had
            // before, rather than a plausible shorter path.
            if nodes.len() != node_vars.len() {
                continue;
            }
            let edges: Vec<crate::graph::types::EdgeId> = edge_vars
                .iter()
                .filter_map(|v| match record.get(v) {
                    Some(Value::EdgeRef(id, ..)) | Some(Value::Edge(id, _)) => Some(*id),
                    _ => None,
                })
                .collect();
            if edges.len() != edge_vars.len() {
                continue;
            }
            record.bind(path_var.clone(), Value::Path { nodes, edges });
        }
        record
    }
}

impl PhysicalOperator for BindPathOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Ok(self.input.next(store)?.map(|r| self.bind(r)))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        Ok(self.input.next_mut(store, tenant_id)?.map(|r| self.bind(r)))
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "BindPath".to_string(),
            details: self.paths.iter().map(|(p, _, _)| p.clone()).collect::<Vec<_>>().join(", "),
            children: vec![self.input.describe()],
        }
    }
}

pub struct LimitOperator {
    /// Input operator
    input: OperatorBox,
    /// Maximum number of records
    limit: usize,
    /// Current count
    count: usize,
}

impl LimitOperator {
    /// Create a new limit operator
    pub fn new(input: OperatorBox, limit: usize) -> Self {
        Self { input, limit, count: 0 }
    }
}

impl PhysicalOperator for LimitOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        if let Some(record) = self.input.next(store)? {
            self.count += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    // A pass-through operator's default `next_mut` delegates to `next`, which
    // reads its input read-only -- so a LIMIT above a write made the write
    // operators refuse outright: `UNWIND [...] AS x CREATE (n) RETURN n.num
    // LIMIT 2` failed with "requires mutable store access". Same defect class
    // as the barriers in #622 and the joins in #624, in the last two
    // pass-through operators that still had it (#649).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            self.count += 1;
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn try_push_limit(&mut self, n: usize) -> bool {
        // Forward the more restrictive of (incoming hint, our own limit).
        // This handles `RETURN ... LIMIT 5 LIMIT 3` and similar nested-limit
        // patterns correctly.
        let effective = self.limit.min(n);
        self.input.try_push_limit(effective)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if self.count >= self.limit {
            return Ok(None);
        }

        let remaining = self.limit - self.count;
        let request_size = batch_size.min(remaining);

        if let Some(mut batch) = self.input.next_batch(store, request_size)? {
            if batch.records.len() > remaining {
                batch.records.truncate(remaining);
            }
            self.count += batch.records.len();
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.count = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Limit".to_string(),
            details: format!("{}", self.limit),
            children: vec![self.input.describe()],
        }
    }
}

/// Sort operator: ORDER BY n.age ASC
pub struct SortOperator {
    input: OperatorBox,
    sort_items: Vec<(Expression, bool)>, // (expr, ascending)
    records: Vec<Record>,
    current: usize,
    executed: bool,
    /// Upper bound on the rows anything above this operator can observe, from
    /// a `LIMIT` (plus any `SKIP`) pushed down through `try_push_limit`.
    ///
    /// `None` means every row is observable and the whole input must be
    /// sorted. When it is set, only the first `k` rows in sort order can ever
    /// be read, so the rest are discarded as they arrive instead of being
    /// sorted and then thrown away (#518).
    limit_hint: Option<usize>,
}

impl SortOperator {
    pub fn new(input: OperatorBox, sort_items: Vec<(Expression, bool)>) -> Self {
        Self {
            input,
            sort_items,
            records: Vec::new(),
            current: 0,
            executed: false,
            limit_hint: None,
        }
    }

    /// The sort key for one record: each `ORDER BY` expression evaluated once.
    ///
    /// This is the whole of the fix in #518. The comparator used to evaluate
    /// both sides' expressions on **every comparison**, so a sort of n rows
    /// performed ~2·n·log₂(n) evaluations rather than n. On LDBC IC9 that was
    /// 389,461 rows -> ~14.5 million property resolutions where 389,461 would
    /// do, and `Sort` was 68.6% of the query.
    fn key_of(&self, record: &Record, store: &GraphStore) -> Vec<Value> {
        self.sort_items
            .iter()
            .map(|(expr, _)| {
                // Errors are folded to Null, which is what the comparator did
                // before and what ORDER BY over a missing property means.
                //
                // The key is a `Value`, not a `PropertyValue`: going through
                // `as_property()` turned every node, relationship and path
                // into `Null` and sorted them all together at the end (#917).
                Self::evaluate_expression(expr, record, store).unwrap_or(Value::Null)
            })
            .collect()
    }

    /// `key_of`, but reading each `x.prop` key through a cursor that located
    /// its column once (#557).
    ///
    /// Only plain property expressions take the cursor; anything else -- an
    /// arithmetic expression, a function call -- falls back to `key_of`'s
    /// walker, and produces the same value either way.
    fn key_of_cached(
        readers: &mut [PropertyCursor],
        sort_items: &[(Expression, bool)],
        record: &Record,
        store: &GraphStore,
    ) -> Vec<Value> {
        let mut key = Vec::with_capacity(sort_items.len());
        let mut cursor = readers.iter_mut();
        for (expr, _) in sort_items {
            match expr {
                // A property is always a `PropertyValue`; the cursor stays.
                Expression::Property { .. } => {
                    let c = cursor.next().expect("one cursor per property key");
                    key.push(Value::Property(c.read(record, store)));
                }
                other => key.push(
                    Self::evaluate_expression(other, record, store).unwrap_or(Value::Null),
                ),
            }
        }
        key
    }

    /// Compare two precomputed keys under the per-column sort directions.
    fn cmp_keys(a: &[Value], b: &[Value], items: &[(Expression, bool)]) -> std::cmp::Ordering {
        for (i, (_, ascending)) in items.iter().enumerate() {
            let (Some(x), Some(y)) = (a.get(i), b.get(i)) else {
                continue;
            };
            // Cypher's orderability, not the index's: `ORDER BY` puts a
            // string before a number and a list before both, where the `Ord`
            // backing the property index does the opposite. See
            // `graph::property::cypher_order` for why both orders exist, and
            // `record::cypher_order_value` for the entity ranks it cannot
            // express.
            let ord = crate::query::executor::record::cypher_order_value(x, y);
            if ord != std::cmp::Ordering::Equal {
                return if *ascending { ord } else { ord.reverse() };
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Keep only the `k` smallest rows under the sort order, discarding the
    /// rest.
    ///
    /// `select_nth_unstable_by` partitions in O(n) rather than sorting, so
    /// trimming a buffer is cheaper than sorting it and is done repeatedly as
    /// the input streams in. Rows tied with the k-th are dropped along with
    /// the rest of the tail; Cypher does not define a tie-break for
    /// `ORDER BY … LIMIT`, so any k of a tied set is a valid answer, but two
    /// runs may therefore disagree about *which* — the same latitude the
    /// unstable sort below already takes.
    fn trim_to(keyed: &mut Vec<(Vec<Value>, Record)>, k: usize, items: &[(Expression, bool)]) {
        if k == 0 {
            keyed.clear();
            return;
        }
        if keyed.len() <= k {
            return;
        }
        keyed.select_nth_unstable_by(k - 1, |a, b| Self::cmp_keys(&a.0, &b.0, items));
        keyed.truncate(k);
    }

    fn evaluate_expression(expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            // Delegates rather than adding a sixth copy of this logic; the
            // standalone evaluator is the one implementation (#654).
            Expression::ListExpr(_) | Expression::MapExpr(_) => {
                eval_expression(expr, record, store)
            }
            Expression::Variable(var) => {
                record.get(var)
                    .cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Property { variable, property } => {
                return read_property(record, variable, property, store, false);
                #[allow(unreachable_code)]
                Ok(Value::Null)
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = Self::evaluate_expression(left, record, store)?;
                let right_val = Self::evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = Self::evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args.iter()
                    .map(|a| Self::evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case { operand, when_clauses, else_result } => {
                eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), |e| Self::evaluate_expression(e, record, store))
            }
            Expression::Index { expr, index } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let idx = Self::evaluate_expression(index, record, store)?;
                eval_index(collection, idx, store)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let s = match start { Some(s) => Some(Self::evaluate_expression(s, record, store)?), None => None };
                let en = match end { Some(e) => Some(Self::evaluate_expression(e, record, store)?), None => None };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery { pattern, where_clause, .. } => {
                eval_exists_subquery(pattern, where_clause.as_deref(), record, store)
            }
            Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
                eval_list_comprehension(variable, list_expr, filter.as_deref(), map_expr, record, store)
            }
            Expression::PredicateFunction { name, variable, list_expr, predicate } => {
                eval_predicate_function(name, variable, list_expr, predicate, record, store)
            }
            Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
                eval_reduce(accumulator, init, variable, list_expr, expression, record, store)
            }
            Expression::PatternComprehension { pattern, filter, projection } => {
                eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store)
            }
            Expression::PathVariable(var) => {
                record.get(var).cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned()
                    .ok_or_else(|| ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name)))
            }
        }
    }
}

impl PhysicalOperator for SortOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    /// Accept the hint, and stop it here.
    ///
    /// A sort does not change cardinality, so it is safe for it to know that
    /// only the first `n` rows will ever be read -- that is exactly a top-N.
    /// It is *not* safe to pass the hint further down: the input must still
    /// produce every row, or the sort would be ordering an arbitrary prefix.
    ///
    /// Returning `true` records that the hint was consumed rather than
    /// ignored, which is what the contract on this method means.
    fn try_push_limit(&mut self, n: usize) -> bool {
        self.limit_hint = Some(match self.limit_hint {
            Some(existing) => existing.min(n),
            None => n,
        });
        true
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let record = self.records[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    // Same as the FILTER above: a write beneath a SORT refused with "requires
    // mutable store access", which is what `CREATE (n) RETURN n ORDER BY n.x`
    // hit (#866).
    //
    // The input is drained **mutably first** and replaced with the rows it
    // produced, so the ordinary `execute_all` does the sorting. Duplicating the
    // decorate-sort-undecorate path -- with its limit hint and its amortised
    // trimming -- would be a second implementation of the one thing this
    // operator does.
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            let mut rows = Vec::new();
            while let Some(r) = self.input.next_mut(store, tenant_id)? {
                rows.push(r);
            }
            self.input = Box::new(MaterializedOperator::new(rows));
            self.execute_all(store)?;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let record = self.records[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        if self.current >= self.records.len() {
            return Ok(None);
        }

        let end = (self.current + batch_size).min(self.records.len());
        let batch = self.records[self.current..end].to_vec();
        self.current = end;

        Ok(Some(RecordBatch { records: batch, columns: Vec::new() }))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.records.clear();
        self.current = 0;
        self.executed = false;
        // `limit_hint` is deliberately kept: it is a property of the plan the
        // planner built, not state from a previous execution.
    }

    fn describe(&self) -> OperatorDescription {
        let items: Vec<String> = self.sort_items.iter().map(|(e, asc)| {
            format!("{} {}", format_expression(e), if *asc { "ASC" } else { "DESC" })
        }).collect();
        OperatorDescription {
            name: "Sort".to_string(),
            details: items.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

impl SortOperator {
    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Decorate-sort-undecorate: evaluate each ORDER BY expression once per
        // row and sort the resulting keys, rather than re-evaluating both
        // sides inside the comparator on every one of the ~n·log₂(n)
        // comparisons (#518).
        let batch_size = 65536;
        let bound = self.limit_hint;

        // With a bound, the buffer is trimmed back to k whenever it grows
        // past a working size, so peak memory is O(k) rather than O(n). The
        // floor keeps the trimming amortised: for a small k the partition
        // would otherwise run on nearly every batch.
        let trim_at = bound.map(|k| k.saturating_mul(2).max(4096));

        // One cursor per property-valued sort key, in the order those keys
        // appear, so `key_of_cached` can walk the two together.
        let mut readers: Vec<PropertyCursor> = self
            .sort_items
            .iter()
            .filter_map(|(expr, _)| match expr {
                Expression::Property { variable, property } => {
                    Some(PropertyCursor::new(variable.as_str(), property.as_str()))
                }
                _ => None,
            })
            .collect();

        let mut keyed: Vec<(Vec<Value>, Record)> = Vec::new();
        while let Some(batch) = self.input.next_batch(store, batch_size)? {
            keyed.reserve(batch.records.len());
            for record in batch.records {
                let key = Self::key_of_cached(&mut readers, &self.sort_items, &record, store);
                keyed.push((key, record));
            }
            if let (Some(k), Some(threshold)) = (bound, trim_at) {
                if keyed.len() >= threshold {
                    Self::trim_to(&mut keyed, k, &self.sort_items);
                }
            }
        }
        if let Some(k) = bound {
            Self::trim_to(&mut keyed, k, &self.sort_items);
        }

        let sort_items = &self.sort_items;
        keyed.sort_by(|a, b| Self::cmp_keys(&a.0, &b.0, sort_items));

        self.records = keyed.into_iter().map(|(_, record)| record).collect();
        self.executed = true;
        Ok(())
    }
}

/// Index scan operator: MATCH (n:Person) WHERE n.id = 1
pub struct IndexScanOperator {
    variable: String,
    label: Label,
    property: String,
    op: BinaryOp,
    value: PropertyValue,
    node_ids: Vec<NodeId>,
    current: usize,
}

impl IndexScanOperator {
    pub fn new(variable: String, label: Label, property: String, op: BinaryOp, value: PropertyValue) -> Self {
        Self {
            variable,
            label,
            property,
            op,
            value,
            node_ids: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) {
        if !self.node_ids.is_empty() {
            return;
        }

        if let Some(index_lock) = store.property_index.get_index(&self.label, &self.property) {
            let index = index_lock.read().unwrap();
            self.node_ids = match self.op {
                BinaryOp::Eq => index.get(&self.value),
                BinaryOp::Gt => {
                    use std::ops::Bound::Excluded;
                    use std::ops::Bound::Unbounded;
                    index.range((Excluded(self.value.clone()), Unbounded))
                },
                BinaryOp::Ge => {
                    use std::ops::Bound::Included;
                    use std::ops::Bound::Unbounded;
                    index.range((Included(self.value.clone()), Unbounded))
                },
                BinaryOp::Lt => {
                    use std::ops::Bound::Excluded;
                    use std::ops::Bound::Unbounded;
                    index.range((Unbounded, Excluded(self.value.clone())))
                },
                BinaryOp::Le => {
                    use std::ops::Bound::Included;
                    use std::ops::Bound::Unbounded;
                    index.range((Unbounded, Included(self.value.clone())))
                },
                _ => Vec::new(),
            };
        }
    }
}

impl PhysicalOperator for IndexScanOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store);

        while self.current < self.node_ids.len() {
            let node_id = self.node_ids[self.current];
            self.current += 1;

            if store.has_node(node_id) {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.initialize(store);

        if self.current >= self.node_ids.len() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(batch_size);
        while records.len() < batch_size && self.current < self.node_ids.len() {
            let node_id = self.node_ids[self.current];
            self.current += 1;

            if store.has_node(node_id) {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                records.push(record);
            }
        }

        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records, columns: vec![self.variable.clone()] }))
        }
    }

    fn reset(&mut self) {
        self.current = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let op_str = match self.op {
            BinaryOp::Eq => "=", BinaryOp::Gt => ">", BinaryOp::Ge => ">=",
            BinaryOp::Lt => "<", BinaryOp::Le => "<=", _ => "?",
        };
        OperatorDescription {
            name: "IndexScan".to_string(),
            details: format!("var={}, {}.{} {} {:?}", self.variable, self.label, self.property, op_str, self.value),
            children: Vec::new(),
        }
    }
}

/// Vector search operator: CALL db.index.vector.queryNodes(...)
pub struct VectorSearchOperator {
    /// Label to search in
    label: String,
    /// Property key to search in
    property_key: String,
    /// Query vector
    query_vector: Vec<f32>,
    /// Number of neighbors to return
    k: usize,
    /// Variable name for matched nodes
    node_var: String,
    /// Variable name for similarity scores (optional)
    score_var: Option<String>,
    /// Search results
    results: Vec<(NodeId, f32)>,
    /// Current index in results
    current: usize,
}

impl VectorSearchOperator {
    pub fn new(
        label: String,
        property_key: String,
        query_vector: Vec<f32>,
        k: usize,
        node_var: String,
        score_var: Option<String>,
    ) -> Self {
        Self {
            label,
            property_key,
            query_vector,
            k,
            node_var,
            score_var,
            results: Vec::new(),
            current: 0,
        }
    }

    fn initialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if !self.results.is_empty() || self.current > 0 {
            return Ok(());
        }

        self.results = store.vector_search(
            &self.label,
            &self.property_key,
            &self.query_vector,
            self.k,
        ).map_err(|e| ExecutionError::GraphError(e.to_string()))?;

        Ok(())
    }
}

impl PhysicalOperator for VectorSearchOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.initialize(store)?;

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let (node_id, score) = &self.results[self.current];
        self.current += 1;

        let mut record = Record::new();
        record.bind(self.node_var.clone(), Value::NodeRef(*node_id));

        if let Some(score_var) = &self.score_var {
            record.bind(score_var.clone(), Value::Property(PropertyValue::Float(*score as f64)));
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "VectorSearch".to_string(),
            details: format!("{}.{}, k={}", self.label, self.property_key, self.k),
            children: Vec::new(),
        }
    }
}

/// Cartesian product operator: MATCH (a:X), (b:Y)
/// Produces all combinations of records from left and right inputs
pub struct CartesianProductOperator {
    left: OperatorBox,
    right: OperatorBox,
    left_records: Vec<Record>,
    left_index: usize,
    current_right: Option<Record>,
    left_materialized: bool,
    /// Set once the left input has been drained through `next_mut`.
    /// Without it a second call would re-drain an already-consumed side.
    left_drained_mut: bool,
}

impl CartesianProductOperator {
    pub fn new(left: OperatorBox, right: OperatorBox) -> Self {
        Self {
            left,
            right,
            left_records: Vec::new(),
            left_index: 0,
            current_right: None,
            left_materialized: false,
            left_drained_mut: false,
        }
    }

    fn materialize_left(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.left_materialized {
            return Ok(());
        }
        let mut count = 0u64;
        while let Some(record) = self.left.next(store)? {
            self.left_records.push(record);
            count += 1;
            if count % 10000 == 0 { check_deadline()?; }
        }
        self.left_materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for CartesianProductOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.left, &mut self.right]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize_left(store)?;
        if self.left_records.is_empty() {
            return Ok(None);
        }
        loop {
            if self.current_right.is_none() {
                self.current_right = self.right.next(store)?;
                self.left_index = 0;
                if self.current_right.is_none() {
                    return Ok(None);
                }
            }
            if self.left_index < self.left_records.len() {
                let left_record = &self.left_records[self.left_index];
                let right_record = self.current_right.as_ref().unwrap();
                let mut merged = left_record.clone();
                for (key, value) in right_record.bindings() {
                    merged.bind(key.clone(), value.clone());
                }
                self.left_index += 1;
                return Ok(Some(merged));
            } else {
                self.current_right = None;
            }
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.materialize_left(store)?;
        if self.left_records.is_empty() {
            return Ok(None);
        }

        let mut results = Vec::with_capacity(batch_size);
        while results.len() < batch_size {
            if self.current_right.is_none() {
                self.current_right = self.right.next(store)?;
                self.left_index = 0;
                if self.current_right.is_none() {
                    break;
                }
            }

            let take = (batch_size - results.len()).min(self.left_records.len() - self.left_index);
            let right_record = self.current_right.as_ref().unwrap();

            for i in 0..take {
                let left_record = &self.left_records[self.left_index + i];
                let mut merged = left_record.clone();
                for (key, value) in right_record.bindings() {
                    merged.bind(key.clone(), value.clone());
                }
                results.push(merged);
            }

            self.left_index += take;
            if self.left_index >= self.left_records.len() {
                self.current_right = None;
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records: results, columns: Vec::new() }))
        }
    }


    // A clause pipeline can put writes on the left of a join: `CREATE (n:C)
    // WITH n MATCH (a:A) RETURN a, n` plans the CREATE below this operator.
    // Draining that side with the read-only `next` makes the write operators
    // refuse — they cannot reach a mutable store — so the left input is drained
    // once with `next_mut` and replaced by its own rows. Everything below has
    // then already run, and the read-only path is correct for the rest.
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.left_drained_mut {
            let mut rows = Vec::new();
            let mut count = 0u64;
            while let Some(record) = self.left.next_mut(store, tenant_id)? {
                rows.push(record);
                count += 1;
                if count % 10000 == 0 {
                    check_deadline()?;
                }
            }
            self.left = Box::new(MaterializedOperator::new(rows));
            self.left_drained_mut = true;
        }
        self.next(store)
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.left_index = 0;
        self.current_right = None;
        self.left_materialized = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CartesianProduct".to_string(),
            details: String::new(),
            children: vec![self.left.describe(), self.right.describe()],
        }
    }
}

/// Join operator: Joins two inputs on a shared variable
pub struct JoinOperator {
    left: OperatorBox,
    right: OperatorBox,
    /// **Every** variable shared between the two sides, not just one.
    ///
    /// Joining on a single shared variable silently drops the correlation carried by the
    /// others and returns a cartesian product across them. The planner used to pass the
    /// first element of a `HashSet` intersection, so which correlation was enforced — and
    /// therefore whether the answer was right — varied between runs of the same query on
    /// the same data (#360).
    join_vars: Vec<String>,
    left_records: HashMap<Vec<Value>, Vec<Record>>,
    right_records: Vec<Record>,
    current_right_index: usize,
    current_left_list_index: usize,
    materialized: bool,
    /// Set once the left input has been drained through `next_mut`.
    /// Without it a second call would re-drain an already-consumed side.
    left_drained_mut: bool,
}

impl JoinOperator {
    /// The composite key: every join variable's value, in a fixed order. `None` when the
    /// record does not bind them all, in which case it cannot match anything.
    fn key_of(record: &Record, vars: &[String]) -> Option<Vec<Value>> {
        vars.iter().map(|v| record.get(v).cloned()).collect()
    }

    pub fn new(left: OperatorBox, right: OperatorBox, join_vars: Vec<String>) -> Self {
        Self {
            left,
            right,
            join_vars,
            left_records: HashMap::new(),
            right_records: Vec::new(),
            current_right_index: 0,
            current_left_list_index: 0,
            materialized: false,
            left_drained_mut: false,
        }
    }

    fn materialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.materialized {
            return Ok(());
        }

        // Materialize left into a hash map (with periodic timeout check)
        let mut count = 0u64;
        while let Some(record) = self.left.next(store)? {
            if let Some(key) = Self::key_of(&record, &self.join_vars) {
                self.left_records.entry(key).or_default().push(record);
            }
            count += 1;
            if count % 10000 == 0 { check_deadline()?; }
        }

        // Materialize right into a list
        count = 0;
        while let Some(record) = self.right.next(store)? {
            self.right_records.push(record);
            count += 1;
            if count % 10000 == 0 { check_deadline()?; }
        }

        self.materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for JoinOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.left, &mut self.right]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize(store)?;

        while self.current_right_index < self.right_records.len() {
            let right_record = &self.right_records[self.current_right_index];
            if let Some(join_key) = Self::key_of(right_record, &self.join_vars) {
                if let Some(left_list) = self.left_records.get(&join_key) {
                    if self.current_left_list_index < left_list.len() {
                        let left_record = &left_list[self.current_left_list_index];
                        self.current_left_list_index += 1;

                        // Merge records
                        let mut merged = left_record.clone();
                        for (key, value) in right_record.bindings() {
                            merged.bind(key.clone(), value.clone());
                        }
                        return Ok(Some(merged));
                    }
                }
            }
            
            // Move to next right record
            self.current_right_index += 1;
            self.current_left_list_index = 0;
        }

        Ok(None)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.materialize(store)?;
        let mut results = Vec::with_capacity(batch_size);

        while results.len() < batch_size && self.current_right_index < self.right_records.len() {
            let right_record = &self.right_records[self.current_right_index];
            if let Some(join_key) = Self::key_of(right_record, &self.join_vars) {
                if let Some(left_list) = self.left_records.get(&join_key) {
                    let take = (batch_size - results.len()).min(left_list.len() - self.current_left_list_index);
                    
                    for i in 0..take {
                        let left_record = &left_list[self.current_left_list_index + i];
                        let mut merged = left_record.clone();
                        for (key, value) in right_record.bindings() {
                            merged.bind(key.clone(), value.clone());
                        }
                        results.push(merged);
                    }
                    
                    self.current_left_list_index += take;
                    if self.current_left_list_index >= left_list.len() {
                        self.current_right_index += 1;
                        self.current_left_list_index = 0;
                    }
                } else {
                    self.current_right_index += 1;
                    self.current_left_list_index = 0;
                }
            } else {
                self.current_right_index += 1;
                self.current_left_list_index = 0;
            }
        }

        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records: results, columns: Vec::new() }))
        }
    }


    // A clause pipeline can put writes on the left of a join: `CREATE (n:C)
    // WITH n MATCH (a:A) RETURN a, n` plans the CREATE below this operator.
    // Draining that side with the read-only `next` makes the write operators
    // refuse — they cannot reach a mutable store — so the left input is drained
    // once with `next_mut` and replaced by its own rows. Everything below has
    // then already run, and the read-only path is correct for the rest.
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.left_drained_mut {
            let mut rows = Vec::new();
            let mut count = 0u64;
            while let Some(record) = self.left.next_mut(store, tenant_id)? {
                rows.push(record);
                count += 1;
                if count % 10000 == 0 {
                    check_deadline()?;
                }
            }
            self.left = Box::new(MaterializedOperator::new(rows));
            self.left_drained_mut = true;
        }
        self.next(store)
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.right_records.clear();
        self.current_right_index = 0;
        self.current_left_list_index = 0;
        self.materialized = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "HashJoin".to_string(),
            details: format!("on={}", self.join_vars.join(",")),
            children: vec![self.left.describe(), self.right.describe()],
        }
    }
}

/// Left outer join operator for OPTIONAL MATCH
/// Iterates left records and probes right records by join variable.
/// When no right match exists, emits the left record with NULL for right-only variables.
pub struct LeftOuterJoinOperator {
    left: OperatorBox,
    right: OperatorBox,
    /// Every shared variable — see [`JoinOperator::join_vars`] (#360).
    join_vars: Vec<String>,
    right_only_vars: Vec<String>,
    /// A predicate spanning both sides, evaluated **inside** the join.
    ///
    /// `OPTIONAL MATCH (x)-[:E1]->(y) WHERE x.val < y.val` scopes its WHERE to
    /// the optional match. Applied as an ordinary filter above the join it
    /// deletes the null-filled rows the OPTIONAL MATCH exists to produce —
    /// `MATCH (x:X) OPTIONAL MATCH ... WHERE y.val > 4` returned one row where
    /// Cypher returns three. A pair failing this predicate is *not a match*,
    /// so the left row is still emitted with nulls (#667).
    join_predicate: Option<Expression>,
    // Materialized data
    left_records: Vec<Record>,
    right_hash: HashMap<Vec<Value>, Vec<Record>>,
    // Iteration state
    current_left_idx: usize,
    current_right_match_idx: usize,
    null_emitted: bool,
    /// Whether any right row for the current left row satisfied
    /// `join_predicate`. A left row whose every candidate fails it has *no
    /// match*, so it is still emitted with nulls (#667).
    any_match_for_left: bool,
    materialized: bool,
    /// Set once the left input has been drained through `next_mut`.
    /// Without it a second call would re-drain an already-consumed side.
    left_drained_mut: bool,
}

impl LeftOuterJoinOperator {
    pub fn new(
        left: OperatorBox,
        right: OperatorBox,
        join_vars: Vec<String>,
        right_only_vars: Vec<String>,
    ) -> Self {
        Self {
            left,
            right,
            join_vars,
            right_only_vars,
            join_predicate: None,
            left_records: Vec::new(),
            right_hash: HashMap::new(),
            current_left_idx: 0,
            current_right_match_idx: 0,
            null_emitted: false,
            any_match_for_left: false,
            materialized: false,
            left_drained_mut: false,
        }
    }

    /// A predicate that must hold for a left/right pair to count as a match.
    ///
    /// Rows failing it are not filtered out — the left row is emitted with the
    /// right side null, which is what distinguishes a join condition from a
    /// WHERE above the join (#667).
    pub fn with_join_predicate(mut self, predicate: Expression) -> Self {
        self.join_predicate = Some(predicate);
        self
    }

    fn materialize(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        if self.materialized {
            return Ok(());
        }

        // Materialize left as flat list (with timeout check)
        let mut count = 0u64;
        while let Some(record) = self.left.next(store)? {
            self.left_records.push(record);
            count += 1;
            if count % 10000 == 0 { check_deadline()?; }
        }

        // Materialize right into a hash map by join variable
        count = 0;
        while let Some(record) = self.right.next(store)? {
            if let Some(val) = JoinOperator::key_of(&record, &self.join_vars) {
                self.right_hash.entry(val.clone()).or_default().push(record);
            }
            count += 1;
            if count % 10000 == 0 { check_deadline()?; }
        }

        self.materialized = true;
        Ok(())
    }
}

impl PhysicalOperator for LeftOuterJoinOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.left, &mut self.right]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.materialize(store)?;

        while self.current_left_idx < self.left_records.len() {
            let left_record = &self.left_records[self.current_left_idx];

            if let Some(join_val) = JoinOperator::key_of(left_record, &self.join_vars) {
                if let Some(right_list) = self.right_hash.get(&join_val) {
                    // Has right matches — emit merged records
                    while self.current_right_match_idx < right_list.len() {
                        let right_record = &right_list[self.current_right_match_idx];
                        self.current_right_match_idx += 1;

                        let mut merged = left_record.clone();
                        for (key, value) in right_record.bindings() {
                            merged.bind(key.clone(), value.clone());
                        }
                        // A pair failing the join predicate is not a match.
                        // Skipping it here rather than filtering above the join
                        // is the whole point: the left row survives, with nulls,
                        // if nothing else matches (#667).
                        if let Some(pred) = &self.join_predicate {
                            let holds = matches!(
                                eval_expression(pred, &merged, store)?,
                                Value::Property(PropertyValue::Boolean(true))
                            );
                            if !holds {
                                continue;
                            }
                        }
                        self.any_match_for_left = true;
                        return Ok(Some(merged));
                    }
                    // Every candidate failed the predicate: this left row has no
                    // match at all, so it gets the null treatment.
                    if !self.any_match_for_left && !self.null_emitted {
                        self.null_emitted = true;
                        let mut merged = left_record.clone();
                        for var in &self.right_only_vars {
                            merged.bind(var.clone(), Value::Null);
                        }
                        return Ok(Some(merged));
                    }
                    // Exhausted right matches for this left record — advance
                } else if !self.null_emitted {
                    // No right matches — emit left record with NULLs
                    self.null_emitted = true;
                    let mut merged = left_record.clone();
                    for var in &self.right_only_vars {
                        merged.bind(var.clone(), Value::Null);
                    }
                    return Ok(Some(merged));
                }
            } else if !self.null_emitted {
                // Left record has no join var value — emit with NULLs
                self.null_emitted = true;
                let mut merged = left_record.clone();
                for var in &self.right_only_vars {
                    merged.bind(var.clone(), Value::Null);
                }
                return Ok(Some(merged));
            }

            // Move to next left record
            self.current_left_idx += 1;
            self.current_right_match_idx = 0;
            self.null_emitted = false;
            self.any_match_for_left = false;
        }

        Ok(None)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut results = Vec::with_capacity(batch_size);
        while results.len() < batch_size {
            match self.next(store)? {
                Some(record) => results.push(record),
                None => break,
            }
        }
        if results.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records: results, columns: Vec::new() }))
        }
    }


    // A clause pipeline can put writes on the left of a join: `CREATE (n:C)
    // WITH n MATCH (a:A) RETURN a, n` plans the CREATE below this operator.
    // Draining that side with the read-only `next` makes the write operators
    // refuse — they cannot reach a mutable store — so the left input is drained
    // once with `next_mut` and replaced by its own rows. Everything below has
    // then already run, and the read-only path is correct for the rest.
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.left_drained_mut {
            let mut rows = Vec::new();
            let mut count = 0u64;
            while let Some(record) = self.left.next_mut(store, tenant_id)? {
                rows.push(record);
                count += 1;
                if count % 10000 == 0 {
                    check_deadline()?;
                }
            }
            self.left = Box::new(MaterializedOperator::new(rows));
            self.left_drained_mut = true;
        }
        self.next(store)
    }

    fn reset(&mut self) {
        self.left.reset();
        self.right.reset();
        self.left_records.clear();
        self.right_hash.clear();
        self.current_left_idx = 0;
        self.current_right_match_idx = 0;
        self.null_emitted = false;
        self.materialized = false;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "LeftOuterJoin".to_string(),
            details: format!("on={}", self.join_vars.join(",")),
            children: vec![self.left.describe(), self.right.describe()],
        }
    }
}

/// Create node operator: CREATE (n:Person {name: "Alice"})
pub struct CreateNodeOperator {
    /// Nodes to create (label, properties, variable)
    nodes_to_create: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>, Option<HashMap<String, Expression>>)>,
    /// Created node IDs (for returning)
    created_nodes: Vec<(NodeId, Option<String>)>,
    /// Current index for iteration
    current: usize,
    /// Whether creation has been executed
    executed: bool,
}

impl CreateNodeOperator {
    /// Create a new CreateNodeOperator
    pub fn new(
        nodes: Vec<(
            Vec<Label>,
            HashMap<String, PropertyValue>,
            Option<String>,
            Option<HashMap<String, Expression>>,
        )>,
    ) -> Self {
        Self {
            nodes_to_create: nodes,
            created_nodes: Vec::new(),
            current: 0,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateNodeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        // Read-only version cannot create nodes
        Err(ExecutionError::RuntimeError(
            "CreateNodeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // First call: create all nodes
        if !self.executed {
            for (labels, properties, variable, property_exprs) in &self.nodes_to_create {
                // The whole label set at once, which for `CREATE ({...})` is
                // empty. Passing a "primary" label meant an unlabelled node was
                // created with `Label("")` (#625).
                let node_id = store.create_node_with_labels(labels.iter().cloned());

                // A CREATE with no input row has nothing bound, so a non-literal value can
                // only be a constant (`{n: 1 + 2}`). Anything referring to a variable is an
                // error rather than a silent null -- quietly storing nothing for a property
                // is the failure this change exists to remove.
                let mut evaluated: HashMap<String, PropertyValue> = HashMap::new();
                if let Some(exprs) = property_exprs {
                    let empty = Record::new();
                    for (key, expr) in exprs {
                        match eval_expression(expr, &empty, store).ok().as_ref().and_then(storable_property) {
                            Some(p) => {
                                evaluated.insert(key.clone(), p);
                            }
                            None => {
                                let _ = store.delete_node(tenant_id, node_id);
                                return Err(ExecutionError::RuntimeError(format!(
                                    "CREATE property `{key}` refers to a variable that is not bound here; bind it first with MATCH, WITH or UNWIND"
                                )));
                            }
                        }
                    }
                }

                // Set properties using store.set_node_property to trigger indexing
                for (key, value) in properties.iter().chain(evaluated.iter()) {
                    if let Err(e) = store.set_node_property(tenant_id, node_id, key.clone(), value.clone())
                    {
                        // A rejected property must not leave a half-built node behind.
                        let _ = store.delete_node(tenant_id, node_id);
                        return Err(ExecutionError::GraphError(e.to_string()));
                    }
                }

                self.created_nodes.push((node_id, variable.clone()));
            }
            self.executed = true;
        }

        // One row, binding *every* node this CREATE made — not one row per node.
        //
        // `CREATE (a), (b) RETURN a, b` is a single row in Cypher with both
        // bound. Emitting a record per node gave two rows, neither of which had
        // both, so the RETURN failed with "Variable not found: b" while the
        // nodes themselves were created correctly (#614). The relationship form
        // was unaffected because `CreateNodesAndEdgesOperator` merges the
        // bindings above this operator — which is why the bug looked like it
        // was about commas rather than about rows.
        if self.current > 0 || self.created_nodes.is_empty() {
            return Ok(None);
        }
        self.current = 1;

        let mut record = Record::new();
        for (idx, (node_id, variable)) in self.created_nodes.iter().enumerate() {
            let node = store.get_node(*node_id).ok_or_else(|| {
                ExecutionError::RuntimeError(format!("Created node {:?} not found", node_id))
            })?;
            // Anonymous nodes still get a name, so persistence and edge wiring
            // can find them; it is kept out of `output_columns` by the planner.
            let bind_name = match variable {
                Some(var) => var.clone(),
                None => format!("__created_node_{idx}"),
            };
            record.bind(bind_name, Value::Node(*node_id, Box::new(node.clone())));
        }

        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.current = 0;
        // Note: We don't reset executed flag - nodes are already created
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create property index operator: CREATE INDEX ON :Person(id)
pub struct CreateIndexOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl CreateIndexOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self { label, property, executed: false }
    }
}

impl PhysicalOperator for CreateIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateIndexOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        store.property_index.create_index(self.label.clone(), self.property.clone());

        // Backfill index
        // Since we have mutable access to store, we can get nodes
        // But we need to avoid borrowing store while mutating property_index if we accessed it differently
        // Here we use get_nodes_by_label which borrows store.
        // property_index is inside store. 
        // IndexManager uses RwLock internally so it handles its own mutability.
        
        // We collect entries to release the borrow on nodes
        // Check both Node HashMap AND ColumnStore (for stub-loaded graphs)
        let mut entries = Vec::new();
        let nodes = store.get_nodes_by_label(&self.label);

        for node in nodes {
            // Try Node HashMap first
            if let Some(val) = node.get_property(&self.property) {
                entries.push((node.id, val.clone()));
            } else {
                // Fall back to ColumnStore (create_node_stub + set_column_property path)
                let col_val = store.node_columns.get_property(node.id.as_u64() as usize, &self.property);
                if !col_val.is_null() {
                    entries.push((node.id, col_val));
                }
            }
        }

        for (node_id, val) in entries {
            store.property_index.index_insert(&self.label, &self.property, val, node_id);
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Create vector index operator: CREATE VECTOR INDEX ...
pub struct CreateVectorIndexOperator {
    label: Label,
    property_key: String,
    dimensions: usize,
    similarity: String,
    executed: bool,
}

impl CreateVectorIndexOperator {
    pub fn new(label: Label, property_key: String, dimensions: usize, similarity: String) -> Self {
        Self {
            label,
            property_key,
            dimensions,
            similarity,
            executed: false,
        }
    }
}

impl PhysicalOperator for CreateVectorIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateVectorIndexOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        let metric = match self.similarity.to_lowercase().as_str() {
            "cosine" => crate::vector::DistanceMetric::Cosine,
            "l2" => crate::vector::DistanceMetric::L2,
            _ => return Err(ExecutionError::RuntimeError(format!("Unsupported similarity metric: {}", self.similarity))),
        };

        store.create_vector_index(self.label.as_str(), &self.property_key, self.dimensions, metric)
            .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

        // Backfill nodes that already carry the embedding. Registering the index without
        // populating it leaves every search returning nothing on a graph that was loaded
        // before the index was declared — which is the normal order for a bulk import
        // followed by DDL. `rebuild_vector_index` reads both the inline map and the
        // columnar store, so it covers whichever tier the vectors landed in.
        store.rebuild_vector_index();

        self.executed = true;
        
        // Return an empty record or a success record
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Composite create index operator: CREATE INDEX ON :Label(prop1, prop2, ...)
pub struct CompositeCreateIndexOperator {
    label: Label,
    properties: Vec<String>,
    executed: bool,
}

impl CompositeCreateIndexOperator {
    pub fn new(label: Label, properties: Vec<String>) -> Self {
        Self { label, properties, executed: false }
    }
}

impl PhysicalOperator for CompositeCreateIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CompositeCreateIndexOperator requires mutable store access.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        // Create individual indexes for each property
        for property in &self.properties {
            store.property_index.create_index(self.label.clone(), property.clone());

            // Backfill each index (check both HashMap and ColumnStore)
            let mut entries = Vec::new();
            let nodes = store.get_nodes_by_label(&self.label);
            for node in nodes {
                if let Some(val) = node.get_property(property) {
                    entries.push((node.id, val.clone()));
                } else {
                    let col_val = store.node_columns.get_property(node.id.as_u64() as usize, property);
                    if !col_val.is_null() {
                        entries.push((node.id, col_val));
                    }
                }
            }
            for (node_id, val) in entries {
                store.property_index.index_insert(&self.label, property, val, node_id);
            }
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CreateCompositeIndex".to_string(),
            details: format!(":{}({})", self.label.as_str(), self.properties.join(", ")),
            children: Vec::new(),
        }
    }
}

/// Create unique constraint operator
pub struct CreateConstraintOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl CreateConstraintOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self { label, property, executed: false }
    }
}

impl PhysicalOperator for CreateConstraintOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateConstraintOperator requires mutable store access.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        // Check existing data for uniqueness violations
        let nodes = store.get_nodes_by_label(&self.label);
        let mut seen_values: std::collections::HashSet<PropertyValue> = std::collections::HashSet::new();
        for node in nodes {
            if let Some(val) = node.get_property(&self.property) {
                if !val.is_null() && !seen_values.insert(val.clone()) {
                    return Err(ExecutionError::RuntimeError(format!(
                        "Cannot create unique constraint: duplicate value {:?} for :{}({})",
                        val, self.label.as_str(), self.property
                    )));
                }
            }
        }

        // Create the constraint
        store.property_index.create_unique_constraint(self.label.clone(), self.property.clone());

        // Backfill constraint index
        let mut entries = Vec::new();
        let nodes = store.get_nodes_by_label(&self.label);
        for node in nodes {
            if let Some(val) = node.get_property(&self.property) {
                entries.push((node.id, val.clone()));
            }
        }
        for (node_id, val) in entries {
            store.property_index.constraint_insert(&self.label, &self.property, val, node_id);
        }

        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "CreateConstraint".to_string(),
            details: format!("UNIQUE :{}({})", self.label.as_str(), self.property),
            children: Vec::new(),
        }
    }
}

/// Drop index operator: DROP INDEX ON :Label(property)
pub struct DropIndexOperator {
    label: Label,
    property: String,
    executed: bool,
}

impl DropIndexOperator {
    pub fn new(label: Label, property: String) -> Self {
        Self { label, property, executed: false }
    }
}

impl PhysicalOperator for DropIndexOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "DropIndexOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if self.executed {
            return Ok(None);
        }

        if !store.property_index.has_index(&self.label, &self.property) {
            return Err(ExecutionError::RuntimeError(
                format!("Index on :{}({}) does not exist", self.label.as_str(), self.property)
            ));
        }

        store.property_index.drop_index(&self.label, &self.property);
        self.executed = true;
        Ok(Some(Record::new()))
    }

    fn reset(&mut self) {
        self.executed = false;
    }

    fn is_mutating(&self) -> bool {
        true
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "DropIndex".to_string(),
            details: format!(":{}({})", self.label.as_str(), self.property),
            children: Vec::new(),
        }
    }
}

/// `RETURN DISTINCT` — deduplicate whole result rows.
///
/// Sits above the projection, so it sees exactly the columns the user asked for and
/// deduplicates on the tuple of those values. Deduplicating anywhere lower would be wrong:
/// two rows that differ only in a column that is not projected are the *same* row as far
/// as `DISTINCT` is concerned.
///
/// Streaming rather than materializing: a row is emitted the first time its key is seen
/// and dropped thereafter, so `DISTINCT ... LIMIT k` stops as soon as `k` distinct rows
/// exist instead of building the whole result first.
///
/// Null is a value here. openCypher deduplicates `NULL` against `NULL` even though
/// `NULL = NULL` is unknown in a predicate — `DISTINCT` uses equivalence, not equality —
/// and [`Value`]'s `Eq`/`Hash` already implement that, along with comparing nodes and
/// edges by identity so a materialized `Node` and a lazy `NodeRef` for the same node
/// deduplicate against each other.
pub struct DistinctOperator {
    input: OperatorBox,
    seen: HashSet<Vec<(std::sync::Arc<str>, Value)>>,
}

impl DistinctOperator {
    /// Wrap `input`, emitting each distinct row once.
    pub fn new(input: OperatorBox) -> Self {
        Self {
            input,
            seen: HashSet::new(),
        }
    }

    /// The deduplication key: every binding, ordered by column name so that two records
    /// that bound the same columns in a different order still collide.
    fn key(record: &Record) -> Vec<(std::sync::Arc<str>, Value)> {
        record.dedup_key()
    }
}

impl PhysicalOperator for DistinctOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next(store)? {
            if self.seen.insert(Self::key(&record)) {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        while let Some(record) = self.input.next_mut(store, tenant_id)? {
            if self.seen.insert(Self::key(&record)) {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn reset(&mut self) {
        self.seen.clear();
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Distinct".to_string(),
            details: String::new(),
            children: vec![self.input.describe()],
        }
    }
}

/// Show indexes operator: SHOW INDEXES
pub struct ShowIndexesOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowIndexesOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowIndexesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let indexes = store.property_index.list_indexes();
            let mut records = Vec::new();
            for (label, property) in indexes {
                let mut record = Record::new();
                record.bind("label".to_string(), Value::Property(PropertyValue::String(label.as_str().to_string())));
                record.bind("property".to_string(), Value::Property(PropertyValue::String(property)));
                record.bind("type".to_string(), Value::Property(PropertyValue::String("BTREE".to_string())));
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }

        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowIndexes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show constraints operator: SHOW CONSTRAINTS
pub struct ShowConstraintsOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowConstraintsOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowConstraintsOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let constraints = store.property_index.list_constraints();
            let mut records = Vec::new();
            for (label, property) in constraints {
                let mut record = Record::new();
                record.bind("label".to_string(), Value::Property(PropertyValue::String(label.as_str().to_string())));
                record.bind("property".to_string(), Value::Property(PropertyValue::String(property)));
                record.bind("type".to_string(), Value::Property(PropertyValue::String("UNIQUE".to_string())));
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }

        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowConstraints".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show labels operator: CALL db.labels()
pub struct ShowLabelsOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowLabelsOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowLabelsOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut labels: Vec<String> = store.all_labels().iter().map(|l| l.as_str().to_string()).collect();
            labels.sort();
            let mut records = Vec::new();
            for label in labels {
                let mut record = Record::new();
                record.bind("label".to_string(), Value::Property(PropertyValue::String(label)));
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowLabels".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show relationship types operator: CALL db.relationshipTypes()
pub struct ShowRelationshipTypesOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowRelationshipTypesOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowRelationshipTypesOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut types: Vec<String> = store.all_edge_types().iter().map(|t| t.as_str().to_string()).collect();
            types.sort();
            let mut records = Vec::new();
            for edge_type in types {
                let mut record = Record::new();
                record.bind("relationshipType".to_string(), Value::Property(PropertyValue::String(edge_type)));
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowRelationshipTypes".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Show property keys operator: CALL db.propertyKeys()
pub struct ShowPropertyKeysOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl ShowPropertyKeysOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for ShowPropertyKeysOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut keys = std::collections::BTreeSet::new();
            let stats = store.statistics();
            for ((_, prop), _) in &stats.property_stats {
                keys.insert(prop.clone());
            }
            for edge_type in store.all_edge_types() {
                let edges = store.get_edges_by_type(edge_type);
                for edge in edges.iter().take(1000) {
                    for key in edge.properties.keys() {
                        keys.insert(key.clone());
                    }
                }
            }
            let mut records = Vec::new();
            for key in keys {
                let mut record = Record::new();
                record.bind("propertyKey".to_string(), Value::Property(PropertyValue::String(key)));
                records.push(record);
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "ShowPropertyKeys".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Schema visualization operator: CALL db.schema.visualization()
pub struct SchemaVisualizationOperator {
    results: Option<std::vec::IntoIter<Record>>,
}

impl SchemaVisualizationOperator {
    pub fn new() -> Self {
        Self { results: None }
    }
}

impl PhysicalOperator for SchemaVisualizationOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.results.is_none() {
            let mut seen = std::collections::HashSet::new();
            let mut records = Vec::new();
            for edge_type in store.all_edge_types() {
                let edges = store.get_edges_by_type(edge_type);
                for edge in edges.iter().take(1000) {
                    if let (Some(src_node), Some(tgt_node)) = (store.get_node(edge.source), store.get_node(edge.target)) {
                        for src_label in &src_node.labels {
                            for tgt_label in &tgt_node.labels {
                                let key = format!("{}|{}|{}", src_label.as_str(), edge_type.as_str(), tgt_label.as_str());
                                if seen.insert(key) {
                                    let mut record = Record::new();
                                    record.bind("source_label".to_string(), Value::Property(PropertyValue::String(src_label.as_str().to_string())));
                                    record.bind("relationship_type".to_string(), Value::Property(PropertyValue::String(edge_type.as_str().to_string())));
                                    record.bind("target_label".to_string(), Value::Property(PropertyValue::String(tgt_label.as_str().to_string())));
                                    records.push(record);
                                }
                            }
                        }
                    }
                }
            }
            self.results = Some(records.into_iter());
        }
        Ok(self.results.as_mut().unwrap().next())
    }

    fn reset(&mut self) {
        self.results = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "SchemaVisualization".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

/// Create edge operator: `CREATE (a)-[:KNOWS]->(b)`
pub struct CreateEdgeOperator {
    /// Input operator (provides source/target nodes from MATCH)
    input: Option<OperatorBox>,
    /// Edge pattern to create: (source_var, target_var, edge_type, properties, edge_var)
    edge_pattern: (String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>),
    /// Created edges
    created_edges: Vec<(crate::graph::EdgeId, Option<String>)>,
    /// Current index
    current: usize,
    /// Whether we've processed input
    processed: bool,
}

impl CreateEdgeOperator {
    /// Create a new CreateEdgeOperator
    pub fn new(
        input: Option<OperatorBox>,
        source_var: String,
        target_var: String,
        edge_type: EdgeType,
        properties: HashMap<String, PropertyValue>,
        edge_var: Option<String>,
    ) -> Self {
        Self {
            input,
            edge_pattern: (source_var, target_var, edge_type, properties, edge_var),
            created_edges: Vec::new(),
            current: 0,
            processed: false,
        }
    }
}

impl PhysicalOperator for CreateEdgeOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        self.input.iter_mut().collect()
    }

    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateEdgeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        let (source_var, target_var, edge_type, properties, edge_var) = &self.edge_pattern;

        // Process input records and create edges
        if !self.processed {
            if let Some(ref mut input) = self.input {
                // Create edge for each input record
                while let Some(record) = input.next_mut(store, tenant_id)? {
                    let source_val = record.get(source_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                    let target_val = record.get(target_var)
                        .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                    let source_id = source_val.node_id()
                        .ok_or_else(|| ExecutionError::TypeError(format!("{} is not a node", source_var)))?;
                    let target_id = target_val.node_id()
                        .ok_or_else(|| ExecutionError::TypeError(format!("{} is not a node", target_var)))?;

                    let edge_id = store.create_edge(source_id, target_id, edge_type.clone())
                        .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                    // Set properties on edge via DS-07c sparse map
                    for (key, value) in properties {
                        store.set_edge_property_sparse(edge_id, key.clone(), value.clone());
                    }

                    self.created_edges.push((edge_id, edge_var.clone()));
                }
            }
            self.processed = true;
        }

        // Return created edges one by one
        if self.current >= self.created_edges.len() {
            return Ok(None);
        }

        let (edge_id, variable) = &self.created_edges[self.current];
        self.current += 1;

        let edge = store.get_edge(*edge_id)
            .ok_or_else(|| ExecutionError::RuntimeError(format!("Created edge {:?} not found", edge_id)))?;

        let mut record = Record::new();
        // Always bind created edge — use variable name if provided, otherwise
        // generate an internal name so persistence code can discover it.
        let bind_name = match variable {
            Some(var) => var.clone(),
            None => format!("__created_edge_{}", self.current - 1),
        };
        record.bind(bind_name, Value::Edge(*edge_id, Box::new(edge.clone())));

        Ok(Some(record))
    }

    fn reset(&mut self) {
        if let Some(ref mut input) = self.input {
            input.reset();
        }
        self.current = 0;
        self.processed = false;
        self.created_edges.clear();
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Combined operator for CREATE patterns with both nodes and edges
/// Example: `CREATE (a:Person)-[:KNOWS]->(b:Person)`
/// This operator first creates all nodes, then creates edges between them
pub struct CreateNodesAndEdgesOperator {
    /// Node creation operator
    node_operator: OperatorBox,
    /// Edges to create: (source_var, target_var, edge_type, literal properties,
    /// edge_var, property expressions).
    ///
    /// The expressions are the row-dependent half -- `{num: x}` from an UNWIND.
    /// Without them `CREATE ()-[r:R {num: x}]->()` made the relationship and
    /// left `r.num` null, which is a wrong answer rather than a missing one
    /// (#649). The node side of this operator has carried them since #467.
    edges_to_create: Vec<EdgeToCreate>,
    /// Variable to NodeId mapping (built during node creation)
    var_to_node_id: HashMap<String, NodeId>,
    /// Created edges
    created_edges: Vec<(crate::graph::EdgeId, crate::graph::Edge, Option<String>)>,
    /// Current phase: 0 = creating nodes, 1 = creating edges, 2 = returning results
    phase: usize,
    /// Current index for returning results
    result_index: usize,
    /// All results to return (nodes first, then edges)
    results: Vec<(Option<String>, Value)>,
}

impl CreateNodesAndEdgesOperator {
    /// Create a new CreateNodesAndEdgesOperator
    pub fn new(
        node_operator: OperatorBox,
        edges_to_create: Vec<EdgeToCreate>,
    ) -> Self {
        Self {
            node_operator,
            edges_to_create,
            var_to_node_id: HashMap::new(),
            created_edges: Vec::new(),
            phase: 0,
            result_index: 0,
            results: Vec::new(),
        }
    }
}

impl PhysicalOperator for CreateNodesAndEdgesOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.node_operator]
    }

    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "CreateNodesAndEdgesOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // Phase 0: Create all nodes and collect their IDs
        if self.phase == 0 {
            while let Some(record) = self.node_operator.next_mut(store, tenant_id)? {
                // Extract variable and node from record
                for (var, value) in record.bindings().iter() {
                    if let Value::Node(node_id, node) = value {
                        self.var_to_node_id.insert(var.to_string(), *node_id);
                        self.results.push((Some(var.to_string()), Value::Node(*node_id, node.clone())));
                    }
                }
            }
            self.phase = 1;
        }

        // Phase 1: Create all edges
        if self.phase == 1 {
            for (source_var, target_var, edge_type, properties, edge_var, exprs) in
                &self.edges_to_create
            {
                let source_id = self.var_to_node_id.get(source_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                let target_id = self.var_to_node_id.get(target_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                // Non-literal property values, evaluated before the edge is
                // created so the immutable borrow ends first.
                //
                // This operator discarded them (`_exprs`), and the literal map
                // carries a `Null` placeholder for each -- so
                // `CREATE ()-[:R {xs: [date('1984-10-11')]}]->()` reported
                // success and stored `xs = null`. Silent data loss on the write
                // path, which no read can distinguish from a property that was
                // never set (#831).
                let mut evaluated: Vec<(String, PropertyValue)> = Vec::new();
                if let Some(exprs) = exprs {
                    let empty = Record::new();
                    for (key, expr) in exprs {
                        if let Some(pv) =
                            storable_property(&eval_expression(expr, &empty, store)?)
                        {
                            evaluated.push((key.clone(), pv));
                        }
                    }
                }

                let edge_id = store.create_edge(*source_id, *target_id, edge_type.clone())
                    .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                // Set properties on edge via DS-07c sparse map. The evaluated
                // expressions go last so they overwrite the placeholders.
                for (key, value) in properties {
                    store.set_edge_property_sparse(edge_id, key.clone(), value.clone());
                }
                for (key, value) in evaluated {
                    store.set_edge_property_sparse(edge_id, key, value);
                }

                // Always track created edges for persistence (even without variable names)
                if let Some(edge) = store.get_edge(edge_id) {
                    let var_name = edge_var.clone().or_else(|| Some(format!("__created_edge_{}", self.created_edges.len())));
                    self.results.push((var_name, Value::Edge(edge_id, Box::new(edge.clone()))));
                    self.created_edges.push((edge_id, edge, edge_var.clone()));
                }
            }
            self.phase = 2;
        }

        // Phase 2: Emit a single record with ALL pattern bindings.
        //
        // openCypher semantics: `CREATE (a)-[r:R]->(b) RETURN a.name, b.name`
        // produces ONE row where a, r, and b are all in scope. Emitting one
        // record per created entity used to leave RETURN unable to resolve
        // the second variable (regression #196).
        if self.result_index > 0 {
            return Ok(None);
        }
        self.result_index = 1;

        let mut record = Record::new();
        for (var, value) in &self.results {
            if let Some(v) = var {
                record.bind(v.clone(), value.clone());
            }
        }
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.node_operator.reset();
        self.var_to_node_id.clear();
        self.created_edges.clear();
        self.phase = 0;
        self.result_index = 0;
        self.results.clear();
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Operator for MATCH...CREATE queries
/// Example: `MATCH (a:Trial {id: 'NCT001'}), (b:Condition {mesh_id: 'D001'}) CREATE (a)-[:STUDIES]->(b)`
/// This operator takes matched nodes and creates edges between them
/// `(source_var, target_var, edge_type, literal properties, edge_var,
/// property expressions)` for one relationship a CREATE has to build.
pub type EdgeToCreate = (
    String,
    String,
    EdgeType,
    HashMap<String, PropertyValue>,
    Option<String>,
    Option<HashMap<String, Expression>>,
);

pub struct MatchCreateEdgeOperator {
    /// Input operator (MATCH results)
    input: OperatorBox,
    /// Nodes in the CREATE pattern that the MATCH did not bind, and must therefore be
    /// created fresh for every matched row: (handle, labels, properties). Previously these
    /// were ignored entirely, so `MATCH (p) CREATE (p)-[:R]->(c:C {..})` created neither
    /// the node nor the edge and reported success.
    nodes_to_create: Vec<(String, Vec<Label>, HashMap<String, PropertyValue>, Option<HashMap<String, Expression>>)>,
    /// Edges to create: (source_var, target_var, edge_type, literal properties,
    /// edge_var, property expressions).
    ///
    /// The expressions are the row-dependent half -- `{num: x}` from an UNWIND.
    /// Without them `CREATE ()-[r:R {num: x}]->()` made the relationship and
    /// left `r.num` null, which is a wrong answer rather than a missing one
    /// (#649). The node side of this operator has carried them since #467.
    edges_to_create: Vec<EdgeToCreate>,
    /// Whether edges have been created for current batch
    done: bool,
    /// Results to return
    results: Vec<Record>,
    /// Current result index
    result_index: usize,
}

impl MatchCreateEdgeOperator {
    /// Create a new MatchCreateEdgeOperator
    pub fn new(
        input: OperatorBox,
        edges_to_create: Vec<EdgeToCreate>,
    ) -> Self {
        Self::with_nodes(input, Vec::new(), edges_to_create)
    }

    /// As `new`, plus the CREATE-pattern nodes the MATCH did not bind. Those are created
    /// once per matched row, then bound under their handle so the edge wiring below can
    /// reference them exactly like a matched variable.
    pub fn with_nodes(
        input: OperatorBox,
        nodes_to_create: Vec<(String, Vec<Label>, HashMap<String, PropertyValue>, Option<HashMap<String, Expression>>)>,
        edges_to_create: Vec<EdgeToCreate>,
    ) -> Self {
        Self {
            input,
            nodes_to_create,
            edges_to_create,
            done: false,
            results: Vec::new(),
            result_index: 0,
        }
    }
}

impl PhysicalOperator for MatchCreateEdgeOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "MatchCreateEdgeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // First pass: process all matched records and create edges
        if !self.done {
            while let Some(record) = self.input.next_mut(store, tenant_id)? {
                // CREATE runs once per matched row, so any pattern node the MATCH did not
                // bind is a *new* node for this row. Bind it under its handle first; the
                // edge wiring below then treats it exactly like a matched variable.
                let mut record = record;
                for (handle, labels, properties, property_exprs) in &self.nodes_to_create {
                    let node_id = store.create_node_with_labels(labels.iter().cloned());
                    // Non-literal property values (`{id: row.id}`) are evaluated against
                    // this row, so each created node gets the value belonging to its own
                    // match rather than a constant.
                    let mut evaluated: HashMap<String, PropertyValue> = HashMap::new();
                    if let Some(exprs) = property_exprs {
                        for (key, expr) in exprs {
                            let value = eval_expression(expr, &record, store)?;
                            let pv = match value {
                                Value::Null => PropertyValue::Null,
                                other => match storable_property(&other) {
                                    Some(p) => p,
                                    None => {
                                        return Err(ExecutionError::TypeError(format!(
                                            "property `{key}` must be a scalar, got {other:?}"
                                        )))
                                    }
                                },
                            };
                            evaluated.insert(key.clone(), pv);
                        }
                    }
                    for (key, value) in properties.iter().chain(evaluated.iter()) {
                        if let Err(e) = store.set_node_property(tenant_id, node_id, key.clone(), value.clone())
                        {
                            // A rejected property must not leave a half-built node behind.
                            let _ = store.delete_node(tenant_id, node_id);
                            return Err(ExecutionError::GraphError(e.to_string()));
                        }
                    }
                    if let Some(node) = store.get_node(node_id) {
                        record.bind(handle.clone(), Value::Node(node_id, Box::new(node.clone())));
                    }
                }

                // For each matched record, create the specified edges
                for (source_var, target_var, edge_type, properties, edge_var, property_exprs) in
                    &self.edges_to_create
                {
                    // Get source node ID from record bindings
                    let source_id = match record.get(source_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue, // Skip if source not found
                    };

                    // Get target node ID from record bindings
                    let target_id = match record.get(target_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue, // Skip if target not found
                    };

                    // Create the edge
                    let edge_id = store.create_edge(source_id, target_id, edge_type.clone())
                        .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                    // Set properties on edge via DS-07c sparse map
                    for (key, value) in properties {
                        store.set_edge_property_sparse(edge_id, key.clone(), value.clone());
                    }
                    // Property values that are expressions rather than
                    // literals -- crucially including the loop variable. These
                    // live in `property_exprs`, and not evaluating them made
                    // `CREATE ()-[r:R {num: x}]->()` build the right number of
                    // relationships with none of the data (#649), the same
                    // defect the node side had in #467.
                    if let Some(exprs) = property_exprs {
                        for (key, expr) in exprs {
                            if let Some(pv) =
                                storable_property(&eval_expression(expr, &record, store)?)
                            {
                                store.set_edge_property_sparse(edge_id, key.clone(), pv);
                            }
                        }
                    }

                    // Property values that are expressions rather than
                    // literals -- crucially including the loop variable. These
                    // live in `property_exprs`, and not evaluating them made
                    // `CREATE ()-[r:R {num: x}]->()` build the right number of
                    // relationships with none of the data (#649), the same
                    // defect the node side had in #467.
                    if let Some(exprs) = property_exprs {
                        for (key, expr) in exprs {
                            if let Some(pv) =
                                storable_property(&eval_expression(expr, &record, store)?)
                            {
                                store.set_edge_property_sparse(edge_id, key.clone(), pv);
                            }
                        }
                    }

                    // Build result record with the created edge
                    let mut result_record = record.clone();
                    if let Some(edge) = store.get_edge(edge_id) {
                        let value = Value::Edge(edge_id, Box::new(edge));
                        // Under the pattern's own name as well as the internal
                        // one. Binding only `_edge` meant
                        // `CREATE ()-[r:R {num: x}]->() RETURN r.num` could not
                        // find `r`: the edge existed, with the right
                        // properties, under a name the query never wrote
                        // (#649).
                        if let Some(var) = edge_var {
                            result_record.bind(var.clone(), value.clone());
                        }
                        result_record.bind("_edge".to_string(), value);
                    }
                    self.results.push(result_record);
                }

                // A CREATE that only adds nodes (no relationship segment) still produced a
                // row for this match; without this the row -- and any RETURN over it --
                // would vanish.
                if self.edges_to_create.is_empty() {
                    self.results.push(record);
                }
            }
            self.done = true;
        }

        // Return results one by one
        if self.result_index >= self.results.len() {
            return Ok(None);
        }

        let result = self.results[self.result_index].clone();
        self.result_index += 1;
        Ok(Some(result))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.done = false;
        self.results.clear();
        self.result_index = 0;
    }

    fn is_mutating(&self) -> bool {
        true
    }
}

/// Operator for MATCH...MERGE edge patterns.
/// Checks if edge exists between bound endpoints before creating.
pub struct MatchMergeEdgeOperator {
    input: OperatorBox,
    /// (source_var, target_var, edge_type, properties, edge_var, undirected)
    ///
    /// `undirected` is `-[r:T]-`, which matches a relationship either way
    /// round. It used to be dropped, so this operator looked only for
    /// `source -> target` and MERGE created a duplicate beside an existing
    /// `target -> source` (#938).
    edges_to_merge: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>, bool)>,
    on_create_set: Vec<(String, String, Expression)>,
    on_match_set: Vec<(String, String, Expression)>,
    /// Whole-entity `ON CREATE`/`ON MATCH SET`; see `MergeOperator` (#874).
    on_create_entity_set: Vec<(String, bool, Expression)>,
    on_match_entity_set: Vec<(String, bool, Expression)>,
    done: bool,
    results: Vec<Record>,
    result_index: usize,
}

impl MatchMergeEdgeOperator {
    pub fn new(
        input: OperatorBox,
        edges_to_merge: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>, bool)>,
        on_create_set: Vec<(String, String, Expression)>,
        on_match_set: Vec<(String, String, Expression)>,
    ) -> Self {
        Self {
            input,
            edges_to_merge,
            on_create_set,
            on_match_set,
            on_create_entity_set: Vec::new(),
            on_match_entity_set: Vec::new(),
            done: false,
            results: Vec::new(),
            result_index: 0,
        }
    }

    /// Attach the whole-entity `ON CREATE`/`ON MATCH SET` items (#874).
    pub fn with_entity_sets(
        mut self,
        on_create: Vec<(String, bool, Expression)>,
        on_match: Vec<(String, bool, Expression)>,
    ) -> Self {
        self.on_create_entity_set = on_create;
        self.on_match_entity_set = on_match;
        self
    }
}

impl PhysicalOperator for MatchMergeEdgeOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError("MatchMergeEdgeOperator requires mutable store access".to_string()))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.done {
            while let Some(record) = self.input.next_mut(store, tenant_id)? {
                for (source_var, target_var, edge_type, properties, edge_var, undirected) in
                    &self.edges_to_merge
                {
                    let source_id = match record.get(source_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue,
                    };
                    let target_id = match record.get(target_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue,
                    };

                    // **Every** existing match, not the first. MERGE is
                    // match-or-create, and when it matches it binds each match
                    // as its own row: over two `:TYPE` relationships between
                    // the same pair, `MERGE (a)-[r:TYPE]->(b) RETURN count(r)`
                    // is 2, and taking one made it 1 (#968). The node half of
                    // the same defect was #956.
                    //
                    // For `-[r:T]-`, either way round; the pattern's own
                    // direction comes first, so when relationships exist both
                    // ways the order is deterministic.
                    let mut existing_all =
                        store.edges_between(source_id, target_id, Some(edge_type));
                    if *undirected {
                        existing_all
                            .extend(store.edges_between(target_id, source_id, Some(edge_type)));
                    }
                    // The pattern's inline properties narrow what counts as a
                    // match. `edges_between` knows only the endpoints and the
                    // type, so without this `MERGE (a)-[r:T {k: v}]->(b)`
                    // matched a relationship with the wrong `k` and created
                    // nothing -- and, once every match was emitted rather than
                    // the first, returned it as an extra row.
                    if !properties.is_empty() {
                        existing_all.retain(|eid| {
                            store.get_edge(*eid).is_some_and(|e| {
                                properties.iter().all(|(k, v)| {
                                    e.properties.get(k).is_some_and(|have| have == v)
                                })
                            })
                        });
                    }

                    for edge_id in &existing_all {
                        let edge_id = *edge_id;
                        let mut result_record = record.clone();
                        {
                        // Edge exists — apply ON MATCH SET
                        for (var, prop, expr) in &self.on_match_set {
                            if edge_var.as_deref() == Some(var) || var == "_edge" {
                                let val = eval_expression(expr, &result_record, store)?;
                                // Setting null removes the property (#874).
                                match val {
                                    Value::Property(PropertyValue::Null) | Value::Null => {
                                        store.remove_edge_property(edge_id, prop);
                                    }
                                    Value::Property(pv) => {
                                        let _ = store.set_edge_property(edge_id, prop.clone(), pv);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        let matched_target = Value::EdgeRef(
                            edge_id,
                            source_id,
                            target_id,
                            edge_type.clone(),
                        );
                        for (var, merge, expr) in &self.on_match_entity_set {
                            if edge_var.as_deref() == Some(var) || var == "_edge" {
                                let value = eval_expression(expr, &result_record, store)?;
                                apply_entity_assignment(
                                    &matched_target, &value, *merge, store, tenant_id,
                                )?;
                            }
                        }
                        if let Some(ref ev) = edge_var {
                            if let Some(edge) = store.get_edge(edge_id) {
                                result_record.bind(ev.clone(), Value::Edge(edge_id, Box::new(edge.clone())));
                            }
                        }
                        }
                            self.results.push(result_record);
                    }

                    let mut result_record = record.clone();
                    if existing_all.is_empty() {
                        // Nothing matched — create it + apply ON CREATE SET
                        let edge_id = store.create_edge(source_id, target_id, edge_type.clone())
                            .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                        for (key, value) in properties {
                            let _ = store.set_edge_property(edge_id, key.clone(), value.clone());
                        }

                        for (var, prop, expr) in &self.on_create_set {
                            if edge_var.as_deref() == Some(var) || var == "_edge" {
                                let val = eval_expression(expr, &result_record, store)?;
                                // Setting null removes the property (#874).
                                match val {
                                    Value::Property(PropertyValue::Null) | Value::Null => {
                                        store.remove_edge_property(edge_id, prop);
                                    }
                                    Value::Property(pv) => {
                                        let _ = store.set_edge_property(edge_id, prop.clone(), pv);
                                    }
                                    _ => {}
                                }
                            }
                        }

                        // `ON CREATE SET r = a` / `r += {…}` (#874).
                        let target = Value::EdgeRef(
                            edge_id,
                            source_id,
                            target_id,
                            edge_type.clone(),
                        );
                        for (var, merge, expr) in &self.on_create_entity_set {
                            if edge_var.as_deref() == Some(var) || var == "_edge" {
                                let value = eval_expression(expr, &result_record, store)?;
                                apply_entity_assignment(&target, &value, *merge, store, tenant_id)?;
                            }
                        }

                        if let Some(ref ev) = edge_var {
                            if let Some(edge) = store.get_edge(edge_id) {
                                result_record.bind(ev.clone(), Value::Edge(edge_id, Box::new(edge.clone())));
                            }
                        }
                        // Inside the branch. Left outside, a pattern that
                        // *matched* also pushed this bare record, with the
                        // relationship variable unbound -- VariableNotFound at
                        // read time, and an extra row before that.
                        self.results.push(result_record);
                    }
                }
            }
            self.done = true;
        }

        if self.result_index >= self.results.len() {
            return Ok(None);
        }
        let result = self.results[self.result_index].clone();
        self.result_index += 1;
        Ok(Some(result))
    }

    fn reset(&mut self) {
        self.input.reset();
        self.done = false;
        self.results.clear();
        self.result_index = 0;
    }

    fn is_mutating(&self) -> bool { true }
}

/// Emits a single empty record. Used for standalone RETURN queries (CY-30).
pub struct SingleRowOperator {
    emitted: bool,
}

impl SingleRowOperator {
    pub fn new() -> Self { Self { emitted: false } }
}

impl PhysicalOperator for SingleRowOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(Record::new()))
        }
    }

    fn reset(&mut self) { self.emitted = false; }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription { name: "SingleRow".to_string(), details: String::new(), children: Vec::new() }
    }
}

/// Emits a fixed, already-computed set of records.
///
/// Used as the source when a `CALL {}` subquery has been executed and its
/// results become the input stream for the enclosing query.
pub struct MaterializedOperator {
    records: Vec<Record>,
    idx: usize,
}

impl MaterializedOperator {
    /// Create an operator that replays `records` in order.
    pub fn new(records: Vec<Record>) -> Self {
        Self { records, idx: 0 }
    }
}

impl PhysicalOperator for MaterializedOperator {
    fn is_materialized(&self) -> bool {
        true
    }

    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.idx >= self.records.len() {
            Ok(None)
        } else {
            let r = self.records[self.idx].clone();
            self.idx += 1;
            Ok(Some(r))
        }
    }

    fn reset(&mut self) { self.idx = 0; }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Materialized".to_string(),
            details: format!("{} rows", self.records.len()),
            children: Vec::new(),
        }
    }
}

/// Algorithm operator: CALL algo.pageRank(...)
pub struct AlgorithmOperator {
    /// Procedure name
    name: String,
    /// Arguments
    args: Vec<crate::query::ast::Expression>,
    /// Result records
    results: Vec<Record>,
    /// Current index
    current: usize,
    /// Whether algorithm has run
    executed: bool,
}

impl AlgorithmOperator {
    /// Every algorithm this operator dispatches, with its calling convention.
    ///
    /// `Unknown algorithm: algo.bfs` gave the caller nothing to go on -- the
    /// name they wanted (`algo.shortestPath`) is not guessable from it, and
    /// the procedures do not share an argument shape either, so even the right
    /// name fails on the first attempt. Listing both removes two rounds of
    /// trial and error.
    fn available() -> &'static str {
        "pageRank({config}), shortestPath(source, target), weightedPath(source, target, weightProperty), \
maxFlow(source, sink [, capacityProperty]), mst([weightProperty]), cdlp([label, edgeType, config]), \
lcc([label, edgeType]), wcc(), scc(), triangleCount(), or.solve({config})"
    }

    /// Error for a procedure name that does not dispatch, naming what does.
    fn unknown_algorithm(name: &str) -> ExecutionError {
        let hint = match name.to_lowercase().replace("algo.", "").as_str() {
            "bfs" | "breadthfirstsearch" => " -- for an unweighted path, use algo.shortestPath",
            "dijkstra" | "shortestpathweighted" => " -- for a weighted path, use algo.weightedPath",
            "pagerank2" | "prank" => " -- did you mean algo.pageRank?",
            "louvain" | "labelpropagation" => " -- for community detection, use algo.cdlp",
            "connectedcomponents" | "components" => " -- use algo.wcc or algo.scc",
            _ => "",
        };
        ExecutionError::RuntimeError(format!(
            "Unknown algorithm: {name}{hint}. Available: {}",
            Self::available()
        ))
    }

    pub fn new(name: String, args: Vec<crate::query::ast::Expression>) -> Self {
        Self {
            name,
            args,
            results: Vec::new(),
            current: 0,
            executed: false,
        }
    }

    fn execute_pagerank(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?, config_map?)
        let mut label = None;
        let mut edge_type = None;
        let mut config = crate::algo::PageRankConfig::default();

        if self.args.len() > 0 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }
        
        // Parse optional config map
        for arg in &self.args {
            if let Expression::Literal(PropertyValue::Map(m)) = arg {
                if let Some(PropertyValue::Integer(i)) = m.get("iterations") {
                    config.iterations = *i as usize;
                }
                if let Some(PropertyValue::Float(f)) = m.get("damping") {
                    config.damping_factor = *f;
                }
            }
        }

        // Build view and run
        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let scores = crate::algo::page_rank(&view, config);

        // Convert to records
        for (algo_id, score) in scores {
            let node_id = NodeId::new(algo_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(node_id) {
                record.bind("node".to_string(), Value::Node(node_id, Box::new(node.clone())));
                record.bind("score".to_string(), Value::Property(PropertyValue::Float(score)));
                self.results.push(record);
            }
        }
        
        // Sort by score descending
        self.results.sort_by(|a, b| {
            let score_a = a.get("score").unwrap().as_property().unwrap().as_float().unwrap();
            let score_b = b.get("score").unwrap().as_property().unwrap().as_float().unwrap();
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(())
    }

    fn execute_shortest_path(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source_node, target_node, config?)
        if self.args.len() < 2 {
            return Err(ExecutionError::RuntimeError("shortestPath requires source and target".to_string()));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Source must be integer ID".to_string())),
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Target must be integer ID".to_string())),
        };

        let mut weight_prop = None;
        if self.args.len() > 2 {
            if let Expression::Literal(PropertyValue::Map(m)) = &self.args[2] {
                if let Some(PropertyValue::String(s)) = m.get("weight_property") {
                    weight_prop = Some(s.clone());
                }
            }
        }
        
        // Build view
        let view = crate::algo::build_view(store, None, None, weight_prop.as_deref());
        
        // Run Algorithm
        let result = if weight_prop.is_some() {
            crate::algo::dijkstra(&view, source_id, target_id)
        } else {
            crate::algo::bfs(&view, source_id, target_id)
        };

        if let Some(result) = result {
             let mut record = Record::new();
             record.bind("cost".to_string(), Value::Property(PropertyValue::Float(result.cost)));
             
             // Construct path list
             let mut path_nodes = Vec::new();
             for nid_u64 in result.path {
                 path_nodes.push(PropertyValue::Integer(nid_u64 as i64));
             }
             record.bind("path".to_string(), Value::Property(PropertyValue::Array(path_nodes)));
             
             self.results.push(record);
        }

        Ok(())
    }

    fn execute_wcc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?)
        let mut label = None;
        let mut edge_type = None;

        if self.args.len() > 0 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }

        // Build view and run WCC
        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let result = crate::algo::weakly_connected_components(&view);

        // Convert to records
        // For WCC, we return (node, componentId)
        for (node_id, component_id) in result.node_component {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, Box::new(node.clone())));
                record.bind("componentId".to_string(), Value::Property(PropertyValue::Integer(component_id as i64)));
                self.results.push(record);
            }
        }
        
        // Sort by componentId
        self.results.sort_by(|a, b| {
            let cid_a = a.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            let cid_b = b.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            cid_a.cmp(&cid_b)
        });

        Ok(())
    }

    /// CALL algo.cdlp(label?, edge_type?, {maxIterations}?) YIELD node, communityId
    ///
    /// Community detection by label propagation (LDBC CDLP). CPU-first; routes to the
    /// GPU automatically above the size threshold when built with `--features gpu`.
    fn execute_cdlp(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?, config_map?)
        let mut label = None;
        let mut edge_type = None;
        let mut config = crate::algo::CdlpConfig::default();

        if !self.args.is_empty() {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }
        for arg in &self.args {
            if let Expression::Literal(PropertyValue::Map(m)) = arg {
                if let Some(PropertyValue::Integer(i)) = m.get("maxIterations") {
                    config.max_iterations = *i as usize;
                }
            }
        }

        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let result = crate::algo::cdlp(&view, &config);

        for (node_id, community_id) in result.labels {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, Box::new(node.clone())));
                record.bind(
                    "communityId".to_string(),
                    Value::Property(PropertyValue::Integer(community_id as i64)),
                );
                self.results.push(record);
            }
        }

        // Sort by communityId for deterministic output.
        self.results.sort_by(|a, b| {
            let ca = a.get("communityId").unwrap().as_property().unwrap().as_integer().unwrap();
            let cb = b.get("communityId").unwrap().as_property().unwrap().as_integer().unwrap();
            ca.cmp(&cb)
        });

        Ok(())
    }

    /// CALL algo.lcc(label?, edge_type?) YIELD node, coefficient
    ///
    /// Local clustering coefficient (LDBC LCC). CPU-first; routes to the GPU automatically
    /// above the size threshold when built with `--features gpu`.
    fn execute_lcc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (label?, edge_type?)
        let mut label = None;
        let mut edge_type = None;

        if !self.args.is_empty() {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[0] {
                label = Some(s.clone());
            }
        }
        if self.args.len() > 1 {
            if let Expression::Literal(PropertyValue::String(s)) = &self.args[1] {
                edge_type = Some(s.clone());
            }
        }

        let view = crate::algo::build_view(store, label.as_deref(), edge_type.as_deref(), None);
        let result = crate::algo::local_clustering_coefficient(&view);

        for (node_id, coeff) in result.coefficients {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, Box::new(node.clone())));
                record.bind(
                    "coefficient".to_string(),
                    Value::Property(PropertyValue::Float(coeff)),
                );
                self.results.push(record);
            }
        }

        // Sort by coefficient descending.
        self.results.sort_by(|a, b| {
            let ca = a.get("coefficient").unwrap().as_property().unwrap().as_float().unwrap();
            let cb = b.get("coefficient").unwrap().as_property().unwrap().as_float().unwrap();
            cb.partial_cmp(&ca).unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(())
    }

    fn execute_weighted_path(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source_node_id, target_node_id, weight_property)
        if self.args.len() < 3 {
            return Err(ExecutionError::RuntimeError("weightedPath requires source, target, and weight property".to_string()));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Source must be integer ID".to_string())),
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Target must be integer ID".to_string())),
        };
        
        let weight_prop = match &self.args[2] {
            Expression::Literal(PropertyValue::String(s)) => s.clone(),
            _ => return Err(ExecutionError::TypeError("Weight property must be a string".to_string())),
        };

        // Build view with weights
        let view = crate::algo::build_view(store, None, None, Some(&weight_prop));
        
        if let Some(result) = crate::algo::dijkstra(&view, source_id, target_id) {
             let mut record = Record::new();
             record.bind("cost".to_string(), Value::Property(PropertyValue::Float(result.cost)));
             
             // Construct path list
             let mut path_nodes = Vec::new();
             for nid_u64 in result.path {
                 let nid = NodeId::new(nid_u64);
                 // We add just IDs for now, or could fetch full nodes if needed
                 path_nodes.push(PropertyValue::Integer(nid.as_u64() as i64));
             }
             record.bind("path".to_string(), Value::Property(PropertyValue::Array(path_nodes)));
             
             self.results.push(record);
        }

        Ok(())
    }
    fn execute_or_solve(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<()> {
        if self.args.is_empty() {
             return Err(ExecutionError::RuntimeError("algo.or.solve requires a config map".to_string()));
        }

        let config_map = match &self.args[0] {
            Expression::Literal(PropertyValue::Map(m)) => m,
            _ => return Err(ExecutionError::TypeError("First argument must be a map".to_string())),
        };

        // Extract parameters
        let algorithm = config_map.get("algorithm").and_then(|v| v.as_string()).unwrap_or("Jaya");
        let label_str = config_map.get("label").and_then(|v| v.as_string())
            .ok_or_else(|| ExecutionError::RuntimeError("Missing 'label' in config".to_string()))?;
        let property = config_map.get("property").and_then(|v| v.as_string())
            .ok_or_else(|| ExecutionError::RuntimeError("Missing 'property' in config".to_string()))?;
        
        let min_val = config_map.get("min").and_then(|v| v.as_float()).unwrap_or(0.0);
        let max_val = config_map.get("max").and_then(|v| v.as_float()).unwrap_or(100.0);
        
        // Objective: minimize sum(variable * cost_property)
        let cost_prop = config_map.get("cost_property").and_then(|v| v.as_string());
        
        // Support multiple objectives
        let mut cost_props: Vec<String> = Vec::new();
        if let Some(cp) = cost_prop {
            cost_props.push(cp.to_string());
        } else if let Some(PropertyValue::Array(arr)) = config_map.get("cost_properties") {
            for v in arr {
                if let Some(s) = v.as_string() { cost_props.push(s.to_string()); }
            }
        }

        let budget = config_map.get("budget").and_then(|v| v.as_float());
        let min_total = config_map.get("min_total").and_then(|v| v.as_float());
        
        let pop_size = config_map.get("population_size").and_then(|v| v.as_integer()).unwrap_or(50) as usize;
        let max_iter = config_map.get("max_iterations").and_then(|v| v.as_integer()).unwrap_or(100) as usize;

        // 1. Gather nodes and costs
        let label = Label::new(label_str);
        
        let mut node_ids = Vec::new();
        let mut single_costs = Vec::new();
        let mut multi_costs = vec![Vec::new(); cost_props.len()];
        
        {
            let nodes = store.get_nodes_by_label(&label);
            for node in nodes {
                node_ids.push(node.id);
                
                // Single cost (for single objective solvers)
                if cost_props.len() == 1 {
                    let cost = node.get_property(&cost_props[0]).and_then(|v| v.as_float()).unwrap_or(1.0);
                    single_costs.push(cost);
                } else if !cost_props.is_empty() {
                    for (i, cp) in cost_props.iter().enumerate() {
                        let cost = node.get_property(cp).and_then(|v| v.as_float()).unwrap_or(1.0);
                        multi_costs[i].push(cost);
                    }
                } else {
                    single_costs.push(1.0);
                }
            }
        }

        if node_ids.is_empty() {
             return Ok(());
        }

        // 2. Setup Problem
        let problem = GraphOptimizationProblem {
            costs: single_costs,
            multi_costs,
            budget,
            min_total,
            dim: node_ids.len(),
            lower: min_val,
            upper: max_val,
        };

        let solver_config = SolverConfig {
            population_size: pop_size,
            max_iterations: max_iter,
        };

        // 3. Run Solver
        if algorithm == "NSGA2" || algorithm == "MOTLBO" || cost_props.len() > 1 {
            let res = match algorithm {
                "MOTLBO" => MOTLBOSolver::new(solver_config).solve(&problem),
                _ => NSGA2Solver::new(solver_config).solve(&problem), // Default multi
            };

            // Write back first individual from Pareto Front
            if let Some(best) = res.pareto_front.first() {
                for (i, &val) in best.variables.iter().enumerate() {
                    let node_id = node_ids[i];
                    let _ = store.set_node_property(tenant_id, node_id, property.to_string(), PropertyValue::Float(val));
                }
            }

            let mut record = Record::new();
            if let Some(best) = res.pareto_front.first() {
                let fitness_props: Vec<PropertyValue> = best.fitness.iter().map(|&f| PropertyValue::Float(f)).collect();
                record.bind("fitness".to_string(), Value::Property(PropertyValue::Array(fitness_props)));
            }
            record.bind("algorithm".to_string(), Value::Property(PropertyValue::String(algorithm.to_string())));
            record.bind("front_size".to_string(), Value::Property(PropertyValue::Integer(res.pareto_front.len() as i64)));
            self.results.push(record);

        } else {
            let result = match algorithm {
                "Rao1" => RaoSolver::new(solver_config, RaoVariant::Rao1).solve(&problem),
                "Rao2" => RaoSolver::new(solver_config, RaoVariant::Rao2).solve(&problem),
                "Rao3" => RaoSolver::new(solver_config, RaoVariant::Rao3).solve(&problem),
                "TLBO" => TLBOSolver::new(solver_config).solve(&problem),
                "Firefly" => FireflySolver::new(solver_config).solve(&problem),
                "Cuckoo" => CuckooSolver::new(solver_config).solve(&problem),
                "GWO" => GWOSolver::new(solver_config).solve(&problem),
                "GA" => GASolver::new(solver_config).solve(&problem),
                "SA" => SASolver::new(solver_config).solve(&problem),
                "Bat" => BatSolver::new(solver_config).solve(&problem),
                "ABC" => ABCSolver::new(solver_config).solve(&problem),
                "GSA" => GSASolver::new(solver_config).solve(&problem),
                "HS" => HSSolver::new(solver_config).solve(&problem),
                "FPA" => FPASolver::new(solver_config).solve(&problem),
                _ => JayaSolver::new(solver_config).solve(&problem), // Default to Jaya
            };

            // 4. Write back results
            for (i, &val) in result.best_variables.iter().enumerate() {
                let node_id = node_ids[i];
                let _ = store.set_node_property(tenant_id, node_id, property.to_string(), PropertyValue::Float(val));
            }

            // 5. Return result record
            let mut record = Record::new();
            record.bind("fitness".to_string(), Value::Property(PropertyValue::Float(result.best_fitness)));
            record.bind("algorithm".to_string(), Value::Property(PropertyValue::String(algorithm.to_string())));
            record.bind("iterations".to_string(), Value::Property(PropertyValue::Integer(max_iter as i64)));
            
            // Yield history as an array for plotting
            let history_props: Vec<PropertyValue> = result.history.into_iter().map(PropertyValue::Float).collect();
            record.bind("history".to_string(), Value::Property(PropertyValue::Array(history_props)));
            
            self.results.push(record);
        }

        Ok(())
    }

    fn execute_max_flow(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (source, sink, capacity_property?)
        if self.args.len() < 2 {
            return Err(ExecutionError::RuntimeError("maxFlow requires source and sink".to_string()));
        }

        let source_id = match &self.args[0] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Source must be integer ID".to_string())),
        };

        let target_id = match &self.args[1] {
            Expression::Literal(PropertyValue::Integer(id)) => *id as u64,
            _ => return Err(ExecutionError::TypeError("Sink must be integer ID".to_string())),
        };

        let cap_prop = if self.args.len() > 2 {
            match &self.args[2] {
                Expression::Literal(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        // Build view
        let view = crate::algo::build_view(store, None, None, cap_prop.as_deref());
        
        // edmonds_karp expects u64 (AlgoNodeId), not crate::graph::NodeId
        if let Some(result) = crate::algo::edmonds_karp(&view, source_id, target_id) {
            let mut record = Record::new();
            record.bind("max_flow".to_string(), Value::Property(PropertyValue::Float(result.max_flow)));
            self.results.push(record);
        } else {
             // No flow found or invalid nodes
             let mut record = Record::new();
             record.bind("max_flow".to_string(), Value::Property(PropertyValue::Float(0.0)));
             self.results.push(record);
        }

        Ok(())
    }

    fn execute_mst(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Arguments: (weight_property?)
        let weight_prop = if self.args.len() > 0 {
            match &self.args[0] {
                Expression::Literal(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            }
        } else {
            None
        };

        let view = crate::algo::build_view(store, None, None, weight_prop.as_deref());
        let result = crate::algo::prim_mst(&view);

        // Return total weight
        let mut summary = Record::new();
        summary.bind("total_weight".to_string(), Value::Property(PropertyValue::Float(result.total_weight)));
        self.results.push(summary);

        // Return edges
        for (u_u64, v_u64, w) in result.edges {
            let u = NodeId::new(u_u64);
            let v = NodeId::new(v_u64);
            
            let mut record = Record::new();
            if let Some(node_u) = store.get_node(u) {
                record.bind("source".to_string(), Value::Node(u, Box::new(node_u.clone())));
            }
            if let Some(node_v) = store.get_node(v) {
                record.bind("target".to_string(), Value::Node(v, Box::new(node_v.clone())));
            }
            record.bind("weight".to_string(), Value::Property(PropertyValue::Float(w)));
            self.results.push(record);
        }

        Ok(())
    }

    fn execute_triangle_count(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Build view (undirected treatment is handled in the algorithm)
        let view = crate::algo::build_view(store, None, None, None);
        let count = crate::algo::count_triangles(&view);

        let mut record = Record::new();
        record.bind("triangles".to_string(), Value::Property(PropertyValue::Integer(count as i64)));
        self.results.push(record);

        Ok(())
    }

    fn execute_scc(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        // Build view and run SCC
        let view = crate::algo::build_view(store, None, None, None);
        let result = crate::algo::strongly_connected_components(&view);

        // For SCC, we return (node, componentId)
        for (node_id, component_id) in result.node_component {
            let nid = NodeId::new(node_id);
            let mut record = Record::new();
            if let Some(node) = store.get_node(nid) {
                record.bind("node".to_string(), Value::Node(nid, Box::new(node.clone())));
                record.bind("componentId".to_string(), Value::Property(PropertyValue::Integer(component_id as i64)));
                self.results.push(record);
            }
        }
        
        // Sort by componentId
        self.results.sort_by(|a, b| {
            let cid_a = a.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            let cid_b = b.get("componentId").unwrap().as_property().unwrap().as_integer().unwrap();
            cid_a.cmp(&cid_b)
        });

        Ok(())
    }
}

impl AlgorithmOperator {
    /// Canonical form of a procedure name for algorithm dispatch.
    ///
    /// Dispatch matched `"algo.pageRank"` exactly, so `algo.pagerank` -- the spelling
    /// anyone would try first -- reported "Unknown algorithm" while the algorithm was
    /// there all along (#198). The namespace is optional and the name is matched
    /// case-insensitively, so `pagerank`, `algo.pagerank`, `algo.pageRank` and
    /// `samyama.pageRank` all reach the same implementation.
    pub fn canonical_name(name: &str) -> String {
        let bare = name
            .strip_prefix("algo.")
            .or_else(|| name.strip_prefix("samyama."))
            .or_else(|| name.strip_prefix("gds."))
            .unwrap_or(name);
        bare.to_ascii_lowercase()
    }

    /// Is this a name the algorithm operator can run?
    pub fn is_algorithm(name: &str) -> bool {
        matches!(
            Self::canonical_name(name).as_str(),
            "pagerank"
                | "shortestpath"
                | "wcc"
                | "scc"
                | "weightedpath"
                | "maxflow"
                | "mst"
                | "trianglecount"
                | "cdlp"
                | "lcc"
                | "or.solve"
        )
    }
}

impl PhysicalOperator for AlgorithmOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            match Self::canonical_name(&self.name).as_str() {
                "pagerank" => self.execute_pagerank(store)?,
                "shortestpath" => self.execute_shortest_path(store)?,
                "wcc" => self.execute_wcc(store)?,
                "scc" => self.execute_scc(store)?,
                "weightedpath" => self.execute_weighted_path(store)?,
                "maxflow" => self.execute_max_flow(store)?,
                "mst" => self.execute_mst(store)?,
                "trianglecount" => self.execute_triangle_count(store)?,
                "cdlp" => self.execute_cdlp(store)?,
                "lcc" => self.execute_lcc(store)?,
                "or.solve" => return Err(ExecutionError::RuntimeError("algo.or.solve requires write access (MutQueryExecutor)".to_string())),
                _ => return Err(Self::unknown_algorithm(&self.name)),
            }
            self.executed = true;
        }

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let record = self.results[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
         if !self.executed {
            match Self::canonical_name(&self.name).as_str() {
                "or.solve" => self.execute_or_solve(store, tenant_id)?,
                // For read-only algos, we can just call the immutable implementations
                // But we need to borrow store immutably.
                // Since we have &mut store, we can reborrow as &store
                "pagerank" => self.execute_pagerank(store)?,
                "shortestpath" => self.execute_shortest_path(store)?,
                "wcc" => self.execute_wcc(store)?,
                "scc" => self.execute_scc(store)?,
                "weightedpath" => self.execute_weighted_path(store)?,
                "maxflow" => self.execute_max_flow(store)?,
                "mst" => self.execute_mst(store)?,
                "trianglecount" => self.execute_triangle_count(store)?,
                "cdlp" => self.execute_cdlp(store)?,
                "lcc" => self.execute_lcc(store)?,
                _ => return Err(Self::unknown_algorithm(&self.name)),
            }
            self.executed = true;
        }

        if self.current >= self.results.len() {
            return Ok(None);
        }

        let record = self.results[self.current].clone();
        self.current += 1;
        Ok(Some(record))
    }

    fn is_mutating(&self) -> bool {
        self.name == "algo.or.solve"
    }

    fn reset(&mut self) {
        self.current = 0;
        self.executed = false;
        self.results.clear();
    }
}

/// Skip operator: SKIP n
pub struct SkipOperator {
    input: OperatorBox,
    skip: usize,
    skipped: usize,
}

impl SkipOperator {
    pub fn new(input: OperatorBox, skip: usize) -> Self {
        Self { input, skip, skipped: 0 }
    }
}

impl PhysicalOperator for SkipOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    /// Widen the hint by this operator's own skip before passing it on.
    ///
    /// If something above needs `n` rows and this operator discards the first
    /// `k`, the subtree has to produce `n + k`. Forwarding `n` unchanged would
    /// make a bounded sort below keep too few rows and silently lose the tail
    /// of the page (#518).
    ///
    /// Before this, `SkipOperator` used the default and blocked the hint
    /// entirely, so nothing below a SKIP ever terminated early.
    fn try_push_limit(&mut self, n: usize) -> bool {
        self.input.try_push_limit(n.saturating_add(self.skip))
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while self.skipped < self.skip {
            if self.input.next(store)?.is_some() {
                self.skipped += 1;
            } else {
                return Ok(None);
            }
        }
        self.input.next(store)
    }

    // See `LimitOperator::next_mut`: skipping a row still has to *produce* it,
    // and producing it may be a write (#649).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        while self.skipped < self.skip {
            if self.input.next_mut(store, tenant_id)?.is_some() {
                self.skipped += 1;
            } else {
                return Ok(None);
            }
        }
        self.input.next_mut(store, tenant_id)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        // Return the *remainder* of the batch the skip lands in.
        //
        // The previous version consumed a whole batch to count off `skip`
        // records and then discarded it, rows past the boundary included,
        // before asking the input for another batch it had already exhausted.
        // `execute_plan` pulls 1024 rows at a time, so any result of 1024 rows
        // or fewer arrives in one batch and `SKIP n` returned nothing at all
        // (#523).
        //
        // `SKIP … LIMIT` hid it: `LimitOperator` requests exactly as many rows
        // as remain under its limit, so with `SKIP 2 LIMIT 2` the skip
        // consumed a 2-row batch and the next request returned the right two.
        // The boundary aligning with the batch size is not a partial fix.
        loop {
            let Some(batch) = self.input.next_batch(store, batch_size)? else {
                return Ok(None);
            };

            let available = batch.records.len();
            if self.skipped + available <= self.skip {
                // The whole batch falls inside the skip.
                self.skipped += available;
                continue;
            }

            let drop = self.skip - self.skipped;
            self.skipped += drop;
            let records: Vec<Record> = batch.records.into_iter().skip(drop).collect();
            if records.is_empty() {
                continue;
            }
            return Ok(Some(RecordBatch { records, columns: batch.columns }));
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.skipped = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Skip".to_string(),
            details: format!("{}", self.skip),
            children: vec![self.input.describe()],
        }
    }
}

/// Delete operator: DELETE n or DETACH DELETE n
pub struct DeleteOperator {
    input: OperatorBox,
    /// The expressions written after `DELETE`, not the names among them.
    ///
    /// This held `Vec<String>` and the planner filtered the clause down to
    /// `Expression::Variable`, so every other way of naming an entity --
    /// a map field, a list element, a path -- was dropped on the floor and
    /// the delete silently did nothing (#891).
    targets: Vec<Expression>,
    detach: bool,
}

impl DeleteOperator {
    pub fn new(input: OperatorBox, targets: Vec<Expression>, detach: bool) -> Self {
        Self { input, targets, detach }
    }

    /// Entities reachable from a `DELETE` target, in the order they are found.
    ///
    /// A path or a list is a container of entities, and Cypher deletes what is
    /// inside it. Nested containers recurse; anything that is not an entity is
    /// ignored here -- `validate_delete_targets` is what refuses those (#887).
    fn collect_entities(value: &Value, nodes: &mut Vec<crate::graph::types::NodeId>, edges: &mut Vec<crate::graph::types::EdgeId>) {
        match value {
            Value::Node(id, _) | Value::NodeRef(id) => nodes.push(*id),
            Value::Edge(id, _) | Value::EdgeRef(id, ..) => edges.push(*id),
            Value::Path { nodes: path_nodes, edges: path_edges } => {
                edges.extend(path_edges.iter().copied());
                nodes.extend(path_nodes.iter().copied());
            }
            Value::List(items) => {
                for item in items {
                    Self::collect_entities(item, nodes, edges);
                }
            }
            Value::Map(entries) => {
                for item in entries.values() {
                    Self::collect_entities(item, nodes, edges);
                }
            }
            _ => {}
        }
    }
}

impl PhysicalOperator for DeleteOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            let mut nodes: Vec<crate::graph::types::NodeId> = Vec::new();
            let mut edges: Vec<crate::graph::types::EdgeId> = Vec::new();
            for target in &self.targets {
                // An unresolvable target is not a silent no-op here: it means
                // the row never bound what the query named, and deleting the
                // rest of the row while ignoring that is how #887 hid.
                let value = eval_expression(target, &record, store)?;
                Self::collect_entities(&value, &mut nodes, &mut edges);
            }
            // Edges first: deleting a node may take its edges with it, and an
            // edge id that has already gone is not an error worth reporting.
            for edge_id in edges {
                let _ = store.delete_edge(edge_id);
            }
            for node_id in nodes {
                if self.detach {
                    let out_edges: Vec<_> = store.get_outgoing_edges(node_id).iter().map(|e| e.id).collect();
                    let in_edges: Vec<_> = store.get_incoming_edges(node_id).iter().map(|e| e.id).collect();
                    for eid in out_edges.into_iter().chain(in_edges) {
                        let _ = store.delete_edge(eid);
                    }
                } else {
                    // A plain DELETE must refuse a node that still has
                    // relationships. `store.delete_node` removes them itself,
                    // so without this check `DELETE n` silently behaved as
                    // `DETACH DELETE n` -- the graph stayed consistent and
                    // relationships the query never mentioned disappeared
                    // (#946). The whole reason Cypher separates the two is
                    // that this is a decision the user has to make out loud.
                    //
                    // Checked here, not at plan time: `MATCH (a)-[r]->(b)
                    // DELETE r, a` is legal because the relationship goes
                    // first, and by this point the node is unconnected. That
                    // is why edges are deleted before nodes above.
                    //
                    // Both directions, or `DELETE b` on `(a)-[r]->(b)` would
                    // still cascade.
                    let attached = store.get_outgoing_edges(node_id).len()
                        + store.get_incoming_edges(node_id).len();
                    if attached > 0 {
                        return Err(ExecutionError::ConstraintVerificationFailed(format!(
                            "Cannot delete node {}, because it still has {} relationship(s). \
                             Delete them first, or use DETACH DELETE.",
                            node_id.as_u64(),
                            attached
                        )));
                    }
                }
                let _ = store.delete_node(tenant_id, node_id);
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let vars = self
            .targets
            .iter()
            .map(|t| match t {
                Expression::Variable(v) | Expression::PathVariable(v) => v.clone(),
                Expression::Property { variable, property } => format!("{}.{}", variable, property),
                other => format!("{:?}", other),
            })
            .collect::<Vec<_>>()
            .join(", ");
        OperatorDescription {
            name: if self.detach { "DetachDelete" } else { "Delete" }.to_string(),
            details: vars,
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool { true }
}

/// Set property operator: SET n.name = "Alice"
/// Apply `SET x = <map|node>` or `SET x += <map|node>` to one entity.
///
/// Shared, because `SET` and `MERGE ... ON CREATE/ON MATCH SET` both need it
/// and the second had **no implementation at all**: `parse_merge_clause`
/// matched only `set_item` and `set_label_item`, so a `set_entity_item` fell
/// through its `match` and the clause the user wrote was silently discarded
/// (#874).
///
/// `=` replaces -- every property not in the incoming map goes away -- and `+=`
/// merges. Removing the leftovers first keeps the two spellings from differing
/// only in what they forgot to clear.
fn apply_entity_assignment(
    target: &Value,
    value: &Value,
    merge: bool,
    store: &mut GraphStore,
    tenant_id: &str,
) -> ExecutionResult<()> {
    let incoming = SetPropertyOperator::source_properties(value, store)?;
    match target {
        Value::NodeRef(id) | Value::Node(id, _) => {
            let id = *id;
            if !merge {
                for key in store.node_properties_full(id).keys().cloned().collect::<Vec<_>>() {
                    if !incoming.contains_key(&key) {
                        store.remove_node_property(id, &key);
                    }
                }
            }
            for (k, v) in incoming {
                store
                    .set_node_property(tenant_id, id, k, v)
                    .map_err(|e| ExecutionError::GraphError(e.to_string()))?;
            }
        }
        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
            let id = *id;
            if !merge {
                let existing: Vec<String> = store
                    .get_edge(id)
                    .map(|e| e.properties.keys().cloned().collect())
                    .unwrap_or_default();
                for key in existing {
                    if !incoming.contains_key(&key) {
                        store.remove_edge_property(id, &key);
                    }
                }
            }
            for (k, v) in incoming {
                let _ = store.set_edge_property(id, k, v);
            }
        }
        _ => {}
    }
    Ok(())
}

pub struct SetPropertyOperator {
    input: OperatorBox,
    items: Vec<(String, String, Expression)>, // (variable, property, value_expr)
    /// Whole-entity assignments: `(variable, merge, value)`. `merge` is `+=`.
    entity_items: Vec<(String, bool, Expression)>,
}

impl SetPropertyOperator {
    pub fn new(input: OperatorBox, items: Vec<(String, String, Expression)>) -> Self {
        Self { input, items, entity_items: Vec::new() }
    }

    /// With whole-entity assignments (`SET n = {…}`, `SET n += {…}`).
    pub fn with_entity_items(
        input: OperatorBox,
        items: Vec<(String, String, Expression)>,
        entity_items: Vec<(String, bool, Expression)>,
    ) -> Self {
        Self { input, items, entity_items }
    }

    /// The properties a right-hand side contributes.
    ///
    /// A map contributes its entries; a node or relationship contributes its
    /// own properties, which is what makes `SET a = b` a copy. Anything else
    /// is a type error rather than a silent no-op — assigning a scalar to an
    /// entity has no sensible meaning and guessing one would hide the mistake.
    pub(crate) fn source_properties(
        value: &Value,
        store: &GraphStore,
    ) -> ExecutionResult<HashMap<String, PropertyValue>> {
        match value {
            Value::Property(PropertyValue::Map(m)) => Ok(m.clone().into_iter().collect()),
            Value::Node(id, _) | Value::NodeRef(id) => {
                Ok(store.node_properties_full(*id).into_iter().collect())
            }
            Value::Edge(_, e) => Ok(e.properties.clone().into_iter().collect()),
            Value::Property(PropertyValue::Null) => Ok(HashMap::new()),
            other => Err(ExecutionError::TypeError(format!(
                "SET <entity> = expects a map or another entity, got {other:?}"
            ))),
        }
    }
}

impl PhysicalOperator for SetPropertyOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            // Evaluate all SET expressions first (immutable borrow of store)
            let evaluated: Vec<_> = self.items.iter().map(|(var, prop, expr)| {
                let val = match eval_expression(expr, &record, store) {
                    Ok(v) => match v {
                        Value::Property(pv) => pv,
                        Value::Null => PropertyValue::Null,
                        // Degrades to ids, as the node and edge arms below do:
                        // a `PropertyValue` cannot hold an entity.
                        Value::Map(entries) => PropertyValue::Map(
                            entries
                                .iter()
                                .map(|(k, v)| {
                                    (
                                        k.clone(),
                                        match v {
                                            Value::Property(p) => p.clone(),
                                            Value::NodeRef(id) | Value::Node(id, _) => {
                                                PropertyValue::Integer(id.as_u64() as i64)
                                            }
                                            Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                                                PropertyValue::Integer(id.as_u64() as i64)
                                            }
                                            _ => PropertyValue::Null,
                                        },
                                    )
                                })
                                .collect(),
                        ),
                        Value::List(items) => PropertyValue::Array(
                            items
                                .iter()
                                .map(|i| match i {
                                    Value::Property(p) => p.clone(),
                                    Value::NodeRef(id) | Value::Node(id, _) => {
                                        PropertyValue::Integer(id.as_u64() as i64)
                                    }
                                    Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                                        PropertyValue::Integer(id.as_u64() as i64)
                                    }
                                    _ => PropertyValue::Null,
                                })
                                .collect(),
                        ),
                        Value::NodeRef(id) => PropertyValue::Integer(id.as_u64() as i64),
                        Value::Node(id, _) => PropertyValue::Integer(id.as_u64() as i64),
                        Value::EdgeRef(id, ..) => PropertyValue::Integer(id.as_u64() as i64),
                        Value::Edge(id, _) => PropertyValue::Integer(id.as_u64() as i64),
                        Value::Path { .. } => PropertyValue::Null,
                    },
                    Err(_) => PropertyValue::Null,
                };
                (var.clone(), prop.clone(), val)
            }).collect();

            // A property is a scalar or a list of scalars. `SET a.maplist =
            // [{num: 1}]` stored an `Array([Map(..)])` -- something
            // `properties(a)` hands back and no Cypher expression can build
            // (#975). SET has its own value conversion, a fourth copy of this
            // logic, so the shared `storable_property` never saw it; the check
            // is applied to the result instead, which covers whichever
            // converter produced it.
            for (_, prop, val) in &evaluated {
                if !property_is_storable(val) {
                    return Err(ExecutionError::TypeError(format!(
                        "InvalidPropertyType: `{prop}` cannot hold a map inside a list. \
                         A property is a scalar or a list of scalars."
                    )));
                }
            }

            // Apply mutations via store methods (syncs columnar + row + index)
            for (var, prop, val) in &evaluated {

                if let Some(node_val) = record.get(var) {
                    // `SET n.prop = null` **removes** the property. Storing a
                    // null instead left the key present, so `keys(n)` still
                    // listed it and a later `n.prop IS NULL` could not tell an
                    // explicitly-nulled property from a removed one -- which is
                    // the whole distinction (#874).
                    let remove = matches!(val, PropertyValue::Null);
                    match node_val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            if remove {
                                store.remove_node_property(*id, prop);
                            } else {
                                store.set_node_property(tenant_id, *id, prop.clone(), val.clone())
                                    .map_err(|e| ExecutionError::GraphError(e.to_string()))?;
                            }
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            if remove {
                                store.remove_edge_property(*id, prop);
                            } else {
                                let _ = store.set_edge_property(*id, prop.clone(), val.clone());
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Whole-entity assignment. Evaluated after the per-property items
            // so that `SET n.a = 1, n = {b: 2}` behaves as written rather than
            // as ordered by implementation detail.
            for (var, merge, expr) in &self.entity_items {
                let Some(target) = record.get(var).cloned() else { continue };
                let value = eval_expression(expr, &record, store)?;
                apply_entity_assignment(&target, &value, *merge, store, tenant_id)?;
            }

            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let mut sets: Vec<String> = self.items.iter().map(|(v, p, e)| format!("{}.{} = {}", v, p, format_expression(e))).collect();
        sets.extend(self.entity_items.iter().map(|(v, merge, e)| {
            format!("{} {} {}", v, if *merge { "+=" } else { "=" }, format_expression(e))
        }));
        OperatorDescription {
            name: "SetProperty".to_string(),
            details: sets.join(", "),
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool { true }
}

/// Remove property operator: REMOVE n.name
/// Adds and removes labels on the nodes flowing through it.
///
/// One operator for both directions because they share everything but the
/// call: each must go through `GraphStore`, which maintains `label_index`.
/// A label added to the node but not to the index is invisible to
/// `MATCH (n:Label)` and to expansion filtering (#592) -- that is, invisible
/// to exactly the queries that look for it.
///
/// Before this existed the planner matched only `RemoveItem::Property` and
/// **dropped** `RemoveItem::Label` while still reporting the statement as a
/// successful write, so `REMOVE n:Label` was a silent no-op (#596).
pub struct LabelMutationOperator {
    input: OperatorBox,
    /// `(variable, label)` pairs to add.
    add: Vec<(String, Label)>,
    /// `(variable, label)` pairs to remove.
    remove: Vec<(String, Label)>,
}

impl LabelMutationOperator {
    pub fn new(input: OperatorBox, add: Vec<(String, Label)>, remove: Vec<(String, Label)>) -> Self {
        Self { input, add, remove }
    }
}

impl PhysicalOperator for LabelMutationOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            for (var, label) in &self.add {
                if let Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) = record.get(var) {
                    let id = *id;
                    store
                        .add_label_to_node(tenant_id, id, label.clone())
                        .map_err(|e| ExecutionError::RuntimeError(e.to_string()))?;
                }
            }
            for (var, label) in &self.remove {
                if let Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) = record.get(var) {
                    let id = *id;
                    // A label the node does not carry is a no-op, which is
                    // Cypher's answer and not an error.
                    store
                        .remove_label_from_node(id, label)
                        .map_err(|e| ExecutionError::RuntimeError(e.to_string()))?;
                }
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let mut parts: Vec<String> = self
            .add
            .iter()
            .map(|(v, l)| format!("+{}:{}", v, l.as_str()))
            .collect();
        parts.extend(self.remove.iter().map(|(v, l)| format!("-{}:{}", v, l.as_str())));
        OperatorDescription {
            name: "LabelMutation".to_string(),
            details: parts.join(", "),
            children: vec![self.input.describe()],
        }
    }
}

pub struct RemovePropertyOperator {
    input: OperatorBox,
    items: Vec<(String, String)>, // (variable, property)
}

impl RemovePropertyOperator {
    pub fn new(input: OperatorBox, items: Vec<(String, String)>) -> Self {
        Self { input, items }
    }
}

impl PhysicalOperator for RemovePropertyOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        self.input.next(store)
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            for (var, prop) in &self.items {
                if let Some(node_val) = record.get(var) {
                    // Through the store, so *both* the column and the row are
                    // cleared. Removing from the row alone left the value
                    // readable, because `resolve_property` reads the column
                    // first -- so REMOVE reported success and did nothing
                    // (#594).
                    match node_val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            let id = *id;
                            store.remove_node_property(id, prop);
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            let id = *id;
                            store.remove_edge_property(id, prop);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        self.input.next_batch(store, batch_size)
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let removes: Vec<String> = self.items.iter().map(|(v, p)| format!("{}.{}", v, p)).collect();
        OperatorDescription {
            name: "RemoveProperty".to_string(),
            details: removes.join(", "),
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool { true }
}

/// UNWIND operator - expands a list expression into individual rows
pub struct UnwindOperator {
    input: OperatorBox,
    expression: Expression,
    variable: String,
    buffer: Vec<Record>,
    buffer_idx: usize,
}

impl UnwindOperator {
    pub fn new(input: OperatorBox, expression: Expression, variable: String) -> Self {
        Self { input, expression, variable, buffer: Vec::new(), buffer_idx: 0 }
    }
}

impl PhysicalOperator for UnwindOperator {
    // A write beneath this operator refused with "requires mutable store
    // access", because the default `next_mut` delegates to `next` and `next`
    // reads its input read-only. Shared body rather than a second, mutable copy
    // of this operator's own logic -- see `drain_input_for_write` (#870).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        drain_input_for_write(&mut self.input, store, tenant_id)?;
        self.next(store)
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            if self.buffer_idx < self.buffer.len() {
                let record = self.buffer[self.buffer_idx].clone();
                self.buffer_idx += 1;
                return Ok(Some(record));
            }

            let record = match self.input.next(store)? {
                Some(r) => r,
                None => return Ok(None),
            };

            let list_val = eval_expression(&self.expression, &record, store)?;

            // A `Value::List` is what a collection literal containing
            // expressions evaluates to (#654). Without this arm `UNWIND
            // [date({...})] AS d` iterated nothing and returned no rows --
            // success, with the loop body never running.
            if let Value::List(values) = list_val {
                self.buffer.clear();
                self.buffer_idx = 0;
                for value in values {
                    let mut new_record = record.clone();
                    new_record.bind(self.variable.clone(), value);
                    self.buffer.push(new_record);
                }
                continue;
            }
            let items = match list_val {
                Value::Property(PropertyValue::Array(arr)) => arr,
                Value::Property(PropertyValue::Vector(vec)) => {
                    vec.into_iter().map(|f| PropertyValue::Float(f as f64)).collect()
                }
                _ => vec![],
            };

            self.buffer.clear();
            self.buffer_idx = 0;
            for item in items {
                let mut new_record = record.clone();
                new_record.bind(self.variable.clone(), Value::Property(item));
                self.buffer.push(new_record);
            }
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() { Ok(None) } else { Ok(Some(RecordBatch { records, columns: vec![self.variable.clone()] })) }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.buffer.clear();
        self.buffer_idx = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Unwind".to_string(),
            details: format!("{} AS {}", format_expression(&self.expression), self.variable),
            children: vec![self.input.describe()],
        }
    }
}

/// MERGE operator - upsert: match or create pattern
pub struct MergeOperator {
    /// Upstream rows, when this MERGE runs inside a clause pipeline.
    ///
    /// `None` is the established shape — a leaf that runs once. With an input
    /// the clause runs **per incoming row**, which is what
    /// `UNWIND [...] AS x MERGE (:N {v: x})` means, and it must keep the row's
    /// existing bindings so `... WITH a MERGE (a)-[:R]->(b)` can see `a`.
    input: Option<OperatorBox>,
    pattern: Pattern,
    on_create_set: Vec<(String, String, Expression)>,
    on_match_set: Vec<(String, String, Expression)>,
    /// `(variable, labels)` from `ON CREATE SET n:Label`.
    on_create_labels: Vec<(String, Vec<Label>)>,
    /// `(variable, labels)` from `ON MATCH SET n:Label`.
    on_match_labels: Vec<(String, Vec<Label>)>,
    /// `(variable, is_merge, value)` from `ON CREATE SET n = {…}` / `n += {…}`.
    ///
    /// The grammar always parsed these; `parse_merge_clause` matched only
    /// `set_item` and `set_label_item`, so they fell through and the clause was
    /// silently discarded (#874).
    on_create_entity_set: Vec<(String, bool, Expression)>,
    /// The same for `ON MATCH SET`.
    on_match_entity_set: Vec<(String, bool, Expression)>,
    executed: bool,
    /// Rows this MERGE still owes its caller.
    ///
    /// MERGE binds **every** match, not the first one. `MATCH (a) MERGE (b)`
    /// over a two-node graph is four rows, and `MERGE (b)` on its own is two;
    /// taking the first match and stopping made both of them one per input row
    /// (#956). So one input row can produce many output rows and they queue
    /// here.
    pending: std::collections::VecDeque<Record>,
}

impl MergeOperator {
    /// Attach the whole-entity `ON CREATE`/`ON MATCH SET` items.
    ///
    /// A builder rather than two more `new` parameters, so the existing call
    /// sites keep compiling and the addition stays reviewable (#874).
    pub fn with_entity_sets(
        mut self,
        on_create: Vec<(String, bool, Expression)>,
        on_match: Vec<(String, bool, Expression)>,
    ) -> Self {
        self.on_create_entity_set = on_create;
        self.on_match_entity_set = on_match;
        self
    }

    pub fn new(
        pattern: Pattern,
        on_create_set: Vec<(String, String, Expression)>,
        on_match_set: Vec<(String, String, Expression)>,
        on_create_labels: Vec<(String, Vec<Label>)>,
        on_match_labels: Vec<(String, Vec<Label>)>,
    ) -> Self {
        Self {
            input: None,
            pattern,
            on_create_set,
            on_match_set,
            on_create_labels,
            on_match_labels,
            on_create_entity_set: Vec::new(),
            on_match_entity_set: Vec::new(),
            executed: false,
            pending: std::collections::VecDeque::new(),
        }
    }

    /// Run this MERGE once per row of `input`, extending each row.
    pub fn with_input(mut self, input: OperatorBox) -> Self {
        self.input = Some(input);
        self
    }

    /// Add the labels an `ON CREATE` / `ON MATCH` branch asks for.
    ///
    /// Resolved through the record rather than against a single variable name,
    /// because a MERGE pattern binds several variables and the branch may name
    /// any of them.
    fn apply_labels(
        items: &[(String, Vec<Label>)],
        record: &Record,
        store: &mut GraphStore,
        tenant_id: &str,
    ) {
        for (var, labels) in items {
            let node_id = match record.get(var) {
                Some(Value::NodeRef(id)) => *id,
                Some(Value::Node(id, _)) => *id,
                _ => continue,
            };
            for label in labels {
                let _ = store.add_label_to_node(tenant_id, node_id, label.clone());
            }
        }
    }

    /// Does a node satisfy the pattern's labels and inline properties?

    /// A pattern's properties with every expression resolved against this row.
    ///
    /// `property_exprs` holds the values that are not literals -- `{v: x}` from
    /// an UNWIND, `{id: row.id}` from a batch upsert, `{name: a.name}` from a
    /// bound node. MERGE did not evaluate them at all, so it matched on the
    /// labels alone and `UNWIND ['a','b'] AS x MERGE (n:N {v: x})` found the
    /// first `:N` for every row and created **one** node instead of two. The
    /// planner refused the query outright rather than answer it wrongly; this
    /// is what makes it answerable (#642).
    ///
    /// The resolved map is used for matching *and* for creation, which is the
    /// property that matters: a MERGE that searched on one set of values and
    /// wrote another would create a node its own pattern could not find, and
    /// running the query twice would make two.
    fn resolved_props(
        literals: Option<&HashMap<String, PropertyValue>>,
        exprs: Option<&HashMap<String, Expression>>,
        record: &Record,
        store: &GraphStore,
    ) -> ExecutionResult<Option<HashMap<String, PropertyValue>>> {
        let has_exprs = exprs.map_or(false, |e| !e.is_empty());
        if !has_exprs {
            return Ok(literals.cloned());
        }
        let mut out = literals.cloned().unwrap_or_default();
        for (key, expr) in exprs.into_iter().flatten() {
            match eval_expression(expr, record, store)? {
                Value::Null => {
                    out.insert(key.clone(), PropertyValue::Null);
                }
                other => match storable_property(&other) {
                    Some(pv) => {
                        out.insert(key.clone(), pv);
                    }
                    None => {
                        return Err(ExecutionError::TypeError(format!(
                            "MERGE property `{key}` must be a scalar value, got {other:?}"
                        )));
                    }
                },
            }
        }
        Ok(Some(out))
    }

    fn node_matches(node: &crate::graph::Node, labels: &[Label], props: Option<&HashMap<String, PropertyValue>>) -> bool {
        if !labels.iter().all(|l| node.labels.contains(l)) {
            return false;
        }
        match props {
            Some(required) => required
                .iter()
                .all(|(k, v)| node.properties.get(k).map_or(false, |pv| pv == v)),
            None => true,
        }
    }

    /// MERGE over a pattern that contains relationships: find the whole pattern or create
    /// the whole pattern.
    ///
    /// Note this creates *fresh* nodes when the pattern as a whole is absent, even where
    /// nodes with the same labels and properties already exist -- openCypher's documented
    /// behaviour, and the reason the idiomatic way to add an edge between existing nodes is
    /// to bind them first (`MATCH (a),(b) MERGE (a)-[:R]->(b)`), which reuses them.
    /// The node a MERGE pattern variable is already bound to, if any.
    ///
    /// A variable that the incoming row already binds is **not** a search: it
    /// names one node, and MERGE neither looks for another nor makes one. Both
    /// merge paths ignored the row entirely, so
    /// `CREATE (a) WITH a MERGE (x) MERGE (y) MERGE (x)-[:T]->(y)` re-created
    /// `x` and `y` and left three nodes where the pattern named one (#893).
    fn bound_node(record: &Record, variable: Option<&String>) -> Option<NodeId> {
        match record.get(variable?) {
            Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => Some(*id),
            _ => None,
        }
    }

    fn merge_path(
        &self,
        path: &crate::query::ast::PathPattern,
        base: Record,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // Flatten the path into nodes and the relationships between them.
        let mut pattern_nodes: Vec<&crate::query::ast::NodePattern> = vec![&path.start];
        // (from_index, to_index, type, properties, variable, undirected)
        //
        // `undirected` is carried because `Direction::Both` used to be folded
        // into `Outgoing` here, so `MERGE (a)-[r:KNOWS]-(b)` only ever looked
        // for `a -> b`. An existing `b -> a` did not match and MERGE created a
        // second relationship beside it (#938).
        let mut pattern_rels: Vec<(usize, usize, EdgeType, HashMap<String, PropertyValue>, Option<String>, bool)> = Vec::new();
        for segment in &path.segments {
            pattern_nodes.push(&segment.node);
            let to = pattern_nodes.len() - 1;
            let from = to - 1;
            let edge_type = segment
                .edge
                .types
                .first()
                .cloned()
                .unwrap_or_else(|| EdgeType::new("RELATED_TO"));
            let props = Self::resolved_props(
                segment.edge.properties.as_ref(),
                segment.edge.property_exprs.as_ref(),
                &base,
                store,
            )?
            .unwrap_or_default();
            let (a, b) = match segment.edge.direction {
                Direction::Incoming => (to, from),
                Direction::Outgoing | Direction::Both => (from, to),
            };
            // An undirected pattern *creates* left-to-right, which is why the
            // orientation above is still (from, to) for `Both`. Only matching
            // has to look both ways.
            let undirected = matches!(segment.edge.direction, Direction::Both);
            pattern_rels.push((a, b, edge_type, props, segment.edge.variable.clone(), undirected));
        }

        // Candidate node ids per pattern position.
        //
        // An unlabelled pattern node used to yield an **empty** candidate set,
        // so the pattern was treated as absent and created. That made
        // `MERGE (a)` add a node to a graph that already had one -- the most
        // basic MERGE there is, matching nothing and creating always (#889).
        //
        // A node with no label is a full scan by definition; there is no index
        // to narrow it, and every engine pays that for an unlabelled MERGE. The
        // shortcut bought a scan and sold the semantics.
        //
        // Resolved once per pattern node, before the search, because the same
        // values decide both what is matched and what would be created.
        let mut node_props: Vec<Option<HashMap<String, PropertyValue>>> =
            Vec::with_capacity(pattern_nodes.len());
        for np in &pattern_nodes {
            node_props.push(Self::resolved_props(
                np.properties.as_ref(),
                np.property_exprs.as_ref(),
                &base,
                store,
            )?);
        }

        // A variable the row already binds is the whole candidate set for its
        // position -- one node, decided before the search rather than by it.
        let bound: Vec<Option<NodeId>> = pattern_nodes
            .iter()
            .map(|np| Self::bound_node(&base, np.variable.as_ref()))
            .collect();

        let mut candidates: Vec<Vec<NodeId>> = Vec::with_capacity(pattern_nodes.len());
        for (i, np) in pattern_nodes.iter().enumerate() {
            if let Some(id) = bound[i] {
                candidates.push(vec![id]);
                continue;
            }
            let mut ids = Vec::new();
            match np.labels.first() {
                Some(first_label) => {
                    for node in store.get_nodes_by_label(first_label) {
                        if Self::node_matches(node, &np.labels, node_props[i].as_ref()) {
                            ids.push(node.id);
                        }
                    }
                }
                None => {
                    for node in store.all_nodes() {
                        if Self::node_matches(node, &np.labels, node_props[i].as_ref()) {
                            ids.push(node.id);
                        }
                    }
                }
            }
            candidates.push(ids);
        }

        // Backtracking search for an assignment satisfying every relationship. Patterns are
        // a handful of nodes, so this stays trivial.
        let found = if candidates.iter().any(|c| c.is_empty()) {
            None
        } else {
            let mut assignment: Vec<NodeId> = Vec::with_capacity(pattern_nodes.len());
            Self::search(&candidates, &pattern_rels, store, &mut assignment)
        };

        // Seeded from the incoming row so a MERGE inside a clause
        // pipeline keeps the bindings its predecessors produced.
        let mut record = base;

        if let Some(assignment) = found {
            for (i, np) in pattern_nodes.iter().enumerate() {
                if let Some(var) = &np.variable {
                    record.bind(var.clone(), Value::NodeRef(assignment[i]));
                }
            }
            // The relationships the search accepted, bound under the names the
            // pattern gave them. Only the nodes were bound, so `MERGE
            // (a)-[r:R]->(b) RETURN r` failed with VariableNotFound and a named
            // path had nothing to build from (#903).
            let mut matched_edges: Vec<crate::graph::types::EdgeId> = Vec::with_capacity(pattern_rels.len());
            for (from, to, ty, props, var, undirected) in &pattern_rels {
                let Some(edge_id) =
                    Self::merge_edge_match(store, assignment[*from], assignment[*to], ty, props, *undirected)
                else {
                    continue;
                };
                matched_edges.push(edge_id);
                if let Some(var) = var {
                    record.bind(
                        var.clone(),
                        Value::EdgeRef(edge_id, assignment[*from], assignment[*to], ty.clone()),
                    );
                }
            }
            if let Some(path_var) = &path.path_variable {
                record.bind(
                    path_var.clone(),
                    Value::Path { nodes: assignment.clone(), edges: matched_edges },
                );
            }
            let sets = self.on_match_set.clone();
            self.apply_sets(&sets, &record, store, tenant_id)?;
            let entity_sets = self.on_match_entity_set.clone();
            self.apply_entity_sets(&entity_sets, &record, store, tenant_id)?;
            let labels = self.on_match_labels.clone();
            Self::apply_labels(&labels, &record, store, tenant_id);
            return Ok(Some(record));
        }

        // Create the entire pattern.
        let mut created: Vec<NodeId> = Vec::with_capacity(pattern_nodes.len());
        for (i, np) in pattern_nodes.iter().enumerate() {
            // A bound variable is reused, never re-created: creating the whole
            // pattern means creating the parts of it that do not exist yet.
            if let Some(id) = bound[i] {
                created.push(id);
                continue;
            }
            // `MERGE ({...})` has no labels, and defaulting to "Node" gave the
            // node a label the query never wrote (#625).
            let node_id = store.create_node_with_labels(np.labels.iter().cloned());
            if let Some(required) = node_props[i].as_ref() {
                for (k, v) in required {
                    if let Err(e) = store.set_node_property(tenant_id, node_id, k.clone(), v.clone())
                    {
                        // A rejected property must not leave a half-built node behind.
                        let _ = store.delete_node(tenant_id, node_id);
                        return Err(ExecutionError::GraphError(e.to_string()));
                    }
                }
            }
            if let Some(var) = &np.variable {
                record.bind(var.clone(), Value::NodeRef(node_id));
            }
            created.push(node_id);
        }
        let mut created_edges: Vec<crate::graph::types::EdgeId> = Vec::with_capacity(pattern_rels.len());
        for (from, to, edge_type, props, var, _undirected) in &pattern_rels {
            let edge_id = store
                .create_edge(created[*from], created[*to], edge_type.clone())
                .map_err(|e| ExecutionError::GraphError(e.to_string()))?;
            for (k, v) in props {
                store.set_edge_property_sparse(edge_id, k.clone(), v.clone());
            }
            created_edges.push(edge_id);
            if let Some(var) = var {
                record.bind(
                    var.clone(),
                    Value::EdgeRef(edge_id, created[*from], created[*to], edge_type.clone()),
                );
            }
        }
        if let Some(path_var) = &path.path_variable {
            record.bind(
                path_var.clone(),
                Value::Path { nodes: created.clone(), edges: created_edges },
            );
        }

        let sets = self.on_create_set.clone();
self.apply_sets(&sets, &record, store, tenant_id)?;
        let entity_sets = self.on_create_entity_set.clone();
        self.apply_entity_sets(&entity_sets, &record, store, tenant_id)?;
        Ok(Some(record))
    }

    /// Assign candidate nodes position by position, keeping only assignments whose
    /// relationships all exist in the store.
    /// The edge from `src` to `dst` that satisfies a MERGE pattern segment.
    ///
    /// One implementation, used by the search *and* by the binding that follows
    /// it, so a relationship variable cannot be bound to an edge the search did
    /// not accept.
    ///
    /// The properties are part of the question. The search compared type and
    /// endpoints only, so `MERGE (a)-[:R {k: 1}]->(b)` matched a bare `:R`
    /// edge, bound nothing, and left the graph without the property the query
    /// asked for (#903).
    /// The existing relationship a MERGE pattern segment matches, if any.
    ///
    /// `undirected` comes from `-[r:T]-`, where the pattern matches a
    /// relationship in *either* direction. Folding that into "outgoing" meant
    /// an existing `b -> a` never matched `MERGE (a)-[r:T]-(b)` and MERGE
    /// created a second relationship beside it (#938) -- a duplicate write
    /// from a clause whose entire purpose is not to write when the thing is
    /// already there.
    fn merge_edge_match(
        store: &GraphStore,
        src: NodeId,
        dst: NodeId,
        ty: &EdgeType,
        props: &HashMap<String, PropertyValue>,
        undirected: bool,
    ) -> Option<crate::graph::types::EdgeId> {
        let props_ok = |eid: crate::graph::types::EdgeId| {
            props.is_empty()
                || store.get_edge(eid).is_some_and(|edge| {
                    props
                        .iter()
                        .all(|(k, v)| edge.properties.get(k).is_some_and(|have| have == v))
                })
        };
        let forward = store
            .get_outgoing_edge_targets(src)
            .iter()
            .find(|(eid, _s, t, et)| *t == dst && et == ty && props_ok(*eid))
            .map(|(eid, ..)| *eid);
        if forward.is_some() || !undirected {
            return forward;
        }
        // The other way round. Checked second so a relationship written in the
        // pattern's own direction is preferred when both exist.
        store
            .get_outgoing_edge_targets(dst)
            .iter()
            .find(|(eid, _s, t, et)| *t == src && et == ty && props_ok(*eid))
            .map(|(eid, ..)| *eid)
    }

    fn search(
        candidates: &[Vec<NodeId>],
        rels: &[(usize, usize, EdgeType, HashMap<String, PropertyValue>, Option<String>, bool)],
        store: &GraphStore,
        assignment: &mut Vec<NodeId>,
    ) -> Option<Vec<NodeId>> {
        let i = assignment.len();
        if i == candidates.len() {
            return Some(assignment.clone());
        }
        for &cand in &candidates[i] {
            // A pattern node cannot bind to a node already used at another position.
            if assignment.contains(&cand) {
                continue;
            }
            assignment.push(cand);
            // Check every relationship whose endpoints are now both assigned.
            let ok = rels.iter().all(|(from, to, ty, props, _v, undirected)| {
                if *from > i || *to > i {
                    return true;
                }
                Self::merge_edge_match(store, assignment[*from], assignment[*to], ty, props, *undirected)
                    .is_some()
            });
            if ok {
                if let Some(found) = Self::search(candidates, rels, store, assignment) {
                    return Some(found);
                }
            }
            assignment.pop();
        }
        None
    }

    /// Apply ON CREATE / ON MATCH SET items for any variable bound by the pattern.
    fn apply_sets(
        &self,
        sets: &[(String, String, Expression)],
        record: &Record,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<()> {
        for (var, prop, expr) in sets {
            let Some(node_id) = record.get(var).and_then(|v| v.node_id()) else {
                continue;
            };
            let val = eval_expression(expr, record, store)?;
            // `SET n.prop = null` removes the property (#874), the same rule
            // the plain `SET` clause follows.
            match val {
                Value::Property(PropertyValue::Null) | Value::Null => {
                    store.remove_node_property(node_id, prop);
                }
                Value::Property(pv) => {
                    let _ = store.set_node_property(tenant_id, node_id, prop.clone(), pv);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// `ON CREATE SET n = {…}` / `n += {…}` (#874).
    fn apply_entity_sets(
        &self,
        sets: &[(String, bool, Expression)],
        record: &Record,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<()> {
        for (var, merge, expr) in sets {
            let Some(target) = record.get(var).cloned() else { continue };
            let value = eval_expression(expr, record, store)?;
            apply_entity_assignment(&target, &value, *merge, store, tenant_id)?;
        }
        Ok(())
    }
}

impl PhysicalOperator for MergeOperator {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "MergeOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        // With an upstream, the clause runs once per incoming row and each row
        // seeds the merge; without one it is a leaf that runs exactly once.
        // Taking the input out first keeps the borrow checker happy while the
        // merge body holds `&mut store`.
        // One input row can owe several output rows: MERGE binds every match,
        // not the first (#956).
        if let Some(r) = self.pending.pop_front() {
            return Ok(Some(r));
        }

        let base = match self.input.take() {
            Some(mut input) => {
                let row = input.next_mut(store, tenant_id)?;
                self.input = Some(input);
                match row {
                    Some(r) => r,
                    None => return Ok(None),
                }
            }
            None => {
                if self.executed {
                    return Ok(None);
                }
                self.executed = true;
                Record::new()
            }
        };

        let path = self.pattern.paths.first()
            .ok_or_else(|| ExecutionError::PlanningError("MERGE pattern has no paths".to_string()))?;

        // A MERGE pattern containing relationships is match-or-create over the *whole*
        // pattern, per openCypher: if the entire pattern does not already exist, the
        // entire pattern is created. Previously only `path.start` was considered and the
        // segments were ignored outright, so `MERGE (a:X {..})-[:R]->(b:X {..})` matched
        // the first node, created no relationship, and reported success.
        if !path.segments.is_empty() {
            return self.merge_path(path, base, store, tenant_id);
        }

        let start = &path.start;
        let start_var = start.variable.clone().unwrap_or_else(|| "n".to_string());
        let labels = &start.labels;
        // The same map decides what is matched and what is created, so a value
        // that came from the row cannot make MERGE search for one thing and
        // write another.
        let resolved = Self::resolved_props(
            start.properties.as_ref(),
            start.property_exprs.as_ref(),
            &base,
            store,
        )?;
        let props = resolved.as_ref();

        // Search for an existing node matching the labels and properties.
        //
        // An **unlabelled** pattern searched nothing at all, so `MERGE (a)`
        // added a node to a graph that already had one, and `MERGE (a {p: 1})`
        // added a second alongside the node it should have matched. The most
        // basic MERGE there is, matching never and creating always (#889).
        //
        // A node with no label is a full scan by definition -- there is no
        // index to narrow it, and every engine pays that for an unlabelled
        // MERGE. The shortcut bought a scan and sold the semantics.
        //
        // Through `Self::node_matches` rather than a third copy of the same
        // comparison: this was the second, and it drifted from the first by
        // exactly this gap.
        // **Every** match, not the first. MERGE is match-or-create, and when it
        // matches it binds each match as its own row -- `MATCH (a) MERGE (b)`
        // over two nodes is four rows. Taking the first and stopping made it
        // one per input row, silently, with the extra rows simply absent
        // (#956).
        let bound = Self::bound_node(&base, start.variable.as_ref());
        let matched: Vec<NodeId> = match bound {
            // A variable the row already bound is not a search: it is that one
            // node.
            Some(id) => vec![id],
            None => {
                let candidates: Vec<&crate::graph::Node> = match labels.first() {
                    Some(first_label) => store.get_nodes_by_label(first_label),
                    None => store.all_nodes(),
                };
                candidates
                    .into_iter()
                    .filter(|node| Self::node_matches(node, labels, props))
                    .map(|node| node.id)
                    .collect()
            }
        };
        let matched_node_id = matched.first().copied();

        let node_id;
        let mut record = base;

        if let Some(existing_id) = matched_node_id {
            node_id = existing_id;
            record.bind(start_var.clone(), Value::NodeRef(node_id));

            for (var, prop, expr) in &self.on_match_set {
                if var == &start_var {
                    let val = eval_expression(expr, &record, store)?;
                    if let Value::Property(pv) = val {
                        let _ = store.set_node_property(tenant_id, node_id, prop.clone(), pv);
                    }
                }
            }
            Self::apply_labels(&self.on_match_labels, &record, store, tenant_id);

            // The rest of the matches, each its own row. ON MATCH SET applies
            // to every one of them, not only the first.
            for extra in matched.iter().skip(1) {
                let mut r = record.clone();
                r.bind(start_var.clone(), Value::NodeRef(*extra));
                for (var, prop, expr) in &self.on_match_set {
                    if var == &start_var {
                        let val = eval_expression(expr, &r, store)?;
                        if let Value::Property(pv) = val {
                            let _ = store.set_node_property(tenant_id, *extra, prop.clone(), pv);
                        }
                    }
                }
                Self::apply_labels(&self.on_match_labels, &r, store, tenant_id);
                self.pending.push_back(r);
            }
        } else {
            node_id = store.create_node_with_labels(labels.iter().cloned());

            if let Some(required_props) = props {
                for (k, v) in required_props {
                    if let Err(e) = store.set_node_property(tenant_id, node_id, k.clone(), v.clone())
                    {
                        // A rejected property must not leave a half-built node behind.
                        let _ = store.delete_node(tenant_id, node_id);
                        return Err(ExecutionError::GraphError(e.to_string()));
                    }
                }
            }

            record.bind(start_var.clone(), Value::NodeRef(node_id));

            for (var, prop, expr) in &self.on_create_set {
                if var == &start_var {
                    let val = eval_expression(expr, &record, store)?;
                    match val {
                        Value::Property(PropertyValue::Null) | Value::Null => {
                            store.remove_node_property(node_id, prop);
                        }
                        Value::Property(pv) => {
                            let _ = store.set_node_property(tenant_id, node_id, prop.clone(), pv);
                        }
                        _ => {}
                    }
                }
            }
            let entity_sets = self.on_create_entity_set.clone();
            self.apply_entity_sets(&entity_sets, &record, store, tenant_id)?;
            Self::apply_labels(&self.on_create_labels, &record, store, tenant_id);
        }

        Ok(Some(record))
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store) {
                Ok(Some(r)) => records.push(r),
                _ => break,
            }
        }
        if records.is_empty() { Ok(None) } else { Ok(Some(RecordBatch { records, columns: vec![] })) }
    }

    fn reset(&mut self) {
        self.executed = false;
    }
}

/// FOREACH operator: FOREACH (x IN list | SET x.prop = val)
pub struct ForeachOperator {
    input: OperatorBox,
    variable: String,
    list_expr: Expression,
    set_items: Vec<(String, String, Expression)>, // (variable, property, value_expr)
    create_patterns: Vec<Pattern>,
}

impl ForeachOperator {
    pub fn new(
        input: OperatorBox,
        variable: String,
        list_expr: Expression,
        set_items: Vec<(String, String, Expression)>,
        create_patterns: Vec<Pattern>,
    ) -> Self {
        Self { input, variable, list_expr, set_items, create_patterns }
    }
}

impl PhysicalOperator for ForeachOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        Err(ExecutionError::RuntimeError(
            "ForeachOperator requires mutable store access. Use next_mut instead.".to_string()
        ))
    }

    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if let Some(record) = self.input.next_mut(store, tenant_id)? {
            // Evaluate the list expression
            let list_val = eval_expression(&self.list_expr, &record, store)?;
            let items = match list_val {
                Value::Property(PropertyValue::Array(arr)) => arr,
                _ => return Ok(Some(record)),
            };

            // Iterate over list items
            for item in &items {
                let mut inner_record = record.clone();
                inner_record.bind(self.variable.clone(), Value::Property(item.clone()));

                // Execute SET operations
                for (var, prop, expr) in &self.set_items {
                    let val = eval_expression(expr, &inner_record, store)?;
                    let prop_val = match val {
                        Value::Property(p) => p,
                        Value::Null => PropertyValue::Null,
                        _ => continue,
                    };

                    if let Some(node_val) = inner_record.get(var) {
                        match node_val {
                            Value::NodeRef(id) | Value::Node(id, _) => {
                                let _ = store.set_node_property(tenant_id, *id, prop.to_string(), prop_val.clone());
                            }
                            Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                                let _ = store.set_edge_property(*id, prop.to_string(), prop_val.clone());
                            }
                            _ => {}
                        }
                    }
                }

                // Execute CREATE operations
                for pattern in &self.create_patterns {
                    for path in &pattern.paths {
                        // A relationship pattern would need the surrounding
                        // variables joined up; creating just the start node
                        // would silently produce an orphan instead of an edge.
                        if !path.segments.is_empty() {
                            return Err(ExecutionError::RuntimeError(
                                "CREATE of a relationship pattern inside FOREACH is not supported"
                                    .to_string(),
                            ));
                        }

                        let node_id =
                            store.create_node_with_labels(path.start.labels.iter().cloned());
                        if let Some(props) = &path.start.properties {
                            for (k, v) in props {
                                let _ = store.set_node_property(tenant_id, node_id, k.to_string(), v.clone());
                            }
                        }
                        // Property values that are expressions rather than
                        // literals -- crucially including the loop variable
                        // itself. These live in `property_exprs`, and not
                        // evaluating them meant `CREATE (:T {i: i})` created
                        // the node and silently dropped `i` (#467): the right
                        // number of nodes, none of the data.
                        if let Some(prop_exprs) = &path.start.property_exprs {
                            for (k, expr) in prop_exprs {
                                let val = eval_expression(expr, &inner_record, store)?;
                                let prop_val = match val {
                                    Value::Null => PropertyValue::Null,
                                    other => match storable_property(&other) {
                                        Some(p) => p,
                                        None => {
                                            return Err(ExecutionError::TypeError(format!(
                                                "FOREACH CREATE: property `{k}` evaluated to {other:?}, \
which cannot be stored as a property value"
                                            )))
                                        }
                                    },
                                };
                                let _ = store.set_node_property(tenant_id, node_id, k.to_string(), prop_val);
                            }
                        }
                    }
                }
            }

            Ok(Some(record))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store) {
                Ok(Some(r)) => records.push(r),
                _ => break,
            }
        }
        if records.is_empty() { Ok(None) } else { Ok(Some(RecordBatch { records, columns: vec![] })) }
    }

    fn reset(&mut self) {
        self.input.reset();
    }
}

/// ShortestPathOperator - finds shortest path(s) between two nodes using BFS
pub struct ShortestPathOperator {
    input: OperatorBox,
    source_var: String,
    target_var: String,
    path_var: Option<String>,
    edge_types: Vec<String>,
    direction: Direction,
    all_paths: bool,  // false = shortestPath, true = allShortestPaths
    results: std::vec::IntoIter<Record>,
    executed: bool,
    /// `edge_types` resolved to interned ids, cached after the first use.
    /// `None` from `type_ids()` means the pattern named no types.
    type_ids: Option<Vec<u16>>,
}

impl ShortestPathOperator {
    pub fn new(
        input: OperatorBox,
        source_var: String,
        target_var: String,
        path_var: Option<String>,
        edge_types: Vec<String>,
        direction: Direction,
        all_paths: bool,
    ) -> Self {
        Self {
            input,
            source_var,
            target_var,
            path_var,
            edge_types,
            direction,
            all_paths,
            results: Vec::new().into_iter(),
            executed: false,
            type_ids: None,
        }
    }

    /// The edge-type filter as interned ids. `None` is the wildcard; an
    /// unknown type yields `Some(empty)`, which matches nothing.
    fn ensure_type_ids(&mut self, store: &GraphStore) {
        if self.edge_types.is_empty() || self.type_ids.is_some() {
            return;
        }
        let ids = self
            .edge_types
            .iter()
            .filter_map(|t| store.edge_type_id(&EdgeType::new(t.as_str())))
            .collect();
        self.type_ids = Some(ids);
    }

    fn for_each_neighbor(
        &self,
        node: NodeId,
        type_ids: Option<&[u16]>,
        store: &GraphStore,
        mut visit: impl FnMut(NodeId, crate::graph::EdgeId),
    ) {
        match self.direction {
            Direction::Outgoing => store.for_each_outgoing_neighbor(node, type_ids, &mut visit),
            Direction::Incoming => store.for_each_incoming_neighbor(node, type_ids, &mut visit),
            Direction::Both => {
                store.for_each_outgoing_neighbor(node, type_ids, &mut visit);
                store.for_each_incoming_neighbor(node, type_ids, &mut visit);
            }
        }
    }

    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut all_results = Vec::new();
        // Cloned rather than borrowed: the loop below pulls from `self.input`,
        // which needs `&mut self`. Once per query, not once per row.
        self.ensure_type_ids(store);
        let type_ids = self.type_ids.clone();
        let type_filter = type_ids.as_deref();

        while let Some(record) = self.input.next(store)? {
            let source_id = record.get(&self.source_var)
                .and_then(|v| v.node_id())
                .ok_or_else(|| ExecutionError::RuntimeError("shortestPath source not a node".to_string()))?;
            let target_id = record.get(&self.target_var)
                .and_then(|v| v.node_id())
                .ok_or_else(|| ExecutionError::RuntimeError("shortestPath target not a node".to_string()))?;

            // BFS to find shortest path(s)
            let paths = self.bfs_shortest(store, source_id, target_id, type_filter);

            if self.all_paths {
                for path in paths {
                    let mut new_record = record.clone();
                    if let Some(ref pv) = self.path_var {
                        new_record.bind(pv.clone(), Value::Path {
                            nodes: path.0,
                            edges: path.1,
                        });
                    }
                    all_results.push(new_record);
                }
            } else if let Some(path) = paths.into_iter().next() {
                let mut new_record = record.clone();
                if let Some(ref pv) = self.path_var {
                    new_record.bind(pv.clone(), Value::Path {
                        nodes: path.0,
                        edges: path.1,
                    });
                }
                all_results.push(new_record);
            }
        }

        self.results = all_results.into_iter();
        self.executed = true;
        Ok(())
    }

    /// Every shortest path from `source` to `target`, or one of them.
    ///
    /// # What this replaced, and why it timed out
    ///
    /// The previous implementation carried a full `Vec<NodeId>` and
    /// `Vec<EdgeId>` on every queue entry and — for `allShortestPaths` —
    /// **disabled the visited set entirely**:
    ///
    /// ```text
    /// if !visited.contains(&next_node) || self.all_paths {
    ///     if !self.all_paths { visited.insert(next_node); }
    /// ```
    ///
    /// So it enumerated every *walk* of length ≤ d rather than every shortest
    /// path, cloning both path vectors at each expansion, and materialised a
    /// full `Edge` per incident edge to read its type. On LDBC with the
    /// endpoints three hops apart and an average undirected degree around 41,
    /// that is on the order of 41³ ≈ 69,000 walks, each enumerating ~900
    /// incident edges as owned `Edge` objects. It did not finish inside 120 s
    /// (#516).
    ///
    /// # What this does
    ///
    /// The textbook two-phase approach:
    ///
    /// 1. **A level BFS** that records, for each node, its distance and *all*
    ///    predecessors that reach it at that distance — a shortest-path DAG.
    ///    Each node is expanded once, so this is O(V + E), and it stops at the
    ///    level where the target appears.
    /// 2. **Backtracking** from the target through that DAG to enumerate
    ///    paths. The cost is proportional to the paths actually returned
    ///    rather than to the walks that might have led anywhere.
    ///
    /// Neighbours come from the allocation-free visitor with the edge type
    /// filtered on its interned id, so an incident edge of the wrong type
    /// costs a comparison rather than an `Edge` clone (#520).
    fn bfs_shortest(
        &self,
        store: &GraphStore,
        source: NodeId,
        target: NodeId,
        type_ids: Option<&[u16]>,
    ) -> Vec<(Vec<NodeId>, Vec<crate::graph::EdgeId>)> {
        if source == target {
            return vec![(vec![source], vec![])];
        }

        // Phase 1: level BFS building the shortest-path DAG.
        let mut dist: rustc_hash::FxHashMap<NodeId, u32> = rustc_hash::FxHashMap::default();
        let mut preds: rustc_hash::FxHashMap<NodeId, Vec<(NodeId, crate::graph::EdgeId)>> =
            rustc_hash::FxHashMap::default();
        dist.insert(source, 0);

        let mut frontier = vec![source];
        let mut depth = 0u32;
        let mut reached = false;

        while !frontier.is_empty() && !reached {
            depth += 1;
            let mut next = Vec::new();
            for &current in &frontier {
                self.for_each_neighbor(current, type_ids, store, |neighbour, edge_id| {
                    match dist.get(&neighbour) {
                        None => {
                            dist.insert(neighbour, depth);
                            preds.entry(neighbour).or_default().push((current, edge_id));
                            next.push(neighbour);
                            if neighbour == target {
                                reached = true;
                            }
                        }
                        // Another predecessor at the *same* distance is another
                        // shortest way in, and `allShortestPaths` wants it. A
                        // node already seen at a shorter distance is not.
                        Some(&d) if d == depth && self.all_paths => {
                            preds.entry(neighbour).or_default().push((current, edge_id));
                        }
                        _ => {}
                    }
                });
            }
            frontier = next;
        }

        if !reached {
            return Vec::new();
        }

        // Phase 2: walk the DAG backwards from the target.
        let mut results = Vec::new();
        let mut nodes_rev = vec![target];
        let mut edges_rev = Vec::new();
        self.collect_paths(
            target,
            source,
            &preds,
            &mut nodes_rev,
            &mut edges_rev,
            &mut results,
        );
        results
    }

    /// Depth-first backtrack through the predecessor DAG, emitting one path
    /// per distinct chain. Stops after the first path when only one is wanted.
    fn collect_paths(
        &self,
        current: NodeId,
        source: NodeId,
        preds: &rustc_hash::FxHashMap<NodeId, Vec<(NodeId, crate::graph::EdgeId)>>,
        nodes_rev: &mut Vec<NodeId>,
        edges_rev: &mut Vec<crate::graph::EdgeId>,
        results: &mut Vec<(Vec<NodeId>, Vec<crate::graph::EdgeId>)>,
    ) {
        if !self.all_paths && !results.is_empty() {
            return;
        }
        if current == source {
            let mut nodes: Vec<NodeId> = nodes_rev.clone();
            nodes.reverse();
            let mut edges: Vec<crate::graph::EdgeId> = edges_rev.clone();
            edges.reverse();
            results.push((nodes, edges));
            return;
        }
        let Some(parents) = preds.get(&current) else {
            return;
        };
        for &(parent, edge_id) in parents {
            nodes_rev.push(parent);
            edges_rev.push(edge_id);
            self.collect_paths(parent, source, preds, nodes_rev, edges_rev, results);
            nodes_rev.pop();
            edges_rev.pop();
            if !self.all_paths && !results.is_empty() {
                return;
            }
        }
    }
}

impl PhysicalOperator for ShortestPathOperator {
    // A write beneath this operator refused with "requires mutable store
    // access", because the default `next_mut` delegates to `next` and `next`
    // reads its input read-only. Shared body rather than a second, mutable copy
    // of this operator's own logic -- see `drain_input_for_write` (#870).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        drain_input_for_write(&mut self.input, store, tenant_id)?;
        self.next(store)
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    /// Without this, EXPLAIN and PROFILE rendered a shortest-path plan as
    /// `Unknown` — the operator inherited the trait's default. A plan nobody
    /// can read is a plan nobody profiles, which is how #516 went unexamined.
    fn describe(&self) -> OperatorDescription {
        let kind = if self.all_paths { "AllShortestPaths" } else { "ShortestPath" };
        let types = if self.edge_types.is_empty() {
            String::new()
        } else {
            format!(":{}", self.edge_types.join("|"))
        };
        OperatorDescription {
            name: kind.to_string(),
            details: format!(
                "({})-[{}*]-({}), {:?}",
                self.source_var, types, self.target_var, self.direction
            ),
            children: vec![self.input.describe()],
        }
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.results.next() {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() { Ok(None) } else { Ok(Some(RecordBatch { records, columns: vec![] })) }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
    }
}

// ============================================================================
// WITH BARRIER OPERATOR
// ============================================================================

/// WITH projection barrier operator.
///
/// Materializes all input records, evaluates WITH items (expressions +
/// aggregations), applies DISTINCT / ORDER BY / SKIP / LIMIT, and projects
/// only the named WITH columns — forming a "barrier" that hides upstream
/// variables from downstream operators.
pub struct WithBarrierOperator {
    input: OperatorBox,
    items: Vec<(Expression, String)>, // (expr, alias)
    aggregates: Vec<AggregateFunction>,
    group_by: Vec<(Expression, String)>,
    has_aggregation: bool,
    distinct: bool,
    where_predicate: Option<Expression>,
    sort_items: Vec<(Expression, bool)>, // (expr, ascending)
    skip: Option<usize>,
    limit: Option<usize>,
    results: std::vec::IntoIter<Record>,
    executed: bool,
}

impl WithBarrierOperator {
    pub fn new(
        input: OperatorBox,
        items: Vec<(Expression, String)>,
        aggregates: Vec<AggregateFunction>,
        group_by: Vec<(Expression, String)>,
        has_aggregation: bool,
        distinct: bool,
        where_predicate: Option<Expression>,
        sort_items: Vec<(Expression, bool)>,
        skip: Option<usize>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            input,
            items,
            aggregates,
            group_by,
            has_aggregation,
            distinct,
            where_predicate,
            sort_items,
            skip,
            limit,
            results: Vec::new().into_iter(),
            executed: false,
        }
    }

    /// Bindings the ORDER BY or WHERE names that the projection did not carry.
    ///
    /// `WITH a, a.num2 % 3 AS mod ORDER BY sum` sorts by `sum`, projected by
    /// an *earlier* WITH and not by this one. Cypher allows that: after a
    /// WITH, both ORDER BY and WHERE see the projected aliases **and** the
    /// scope in front of them. Evaluating against the projected rows alone
    /// made `sum` null on every row, so the order was whatever the input order
    /// happened to be and the LIMIT then kept the wrong three (#970).
    ///
    /// The WHERE has the identical shape:
    /// `WITH a.name2 AS name WHERE name = 'B' OR a.name2 = 'C'` filters on
    /// both, and `a` is gone by then, so the second disjunct was always false
    /// and the row it should have kept was dropped.
    ///
    /// Copied under a private prefix rather than bound plainly, so a carried
    /// name cannot collide with something the projection produced or leak into
    /// the rows the next clause sees.
    const CARRY: &'static str = "__orderby_carry_";

    fn carry_sort_scope(&self, from: &Record, into: &mut Record) {
        if self.sort_items.is_empty() && self.where_predicate.is_none() {
            return;
        }
        for expr in self
            .sort_items
            .iter()
            .map(|(e, _)| e)
            .chain(self.where_predicate.iter())
        {
            let mut names = HashSet::new();
            collect_expression_names(expr, &mut names);
            for name in names {
                if into.get(&name).is_none() {
                    if let Some(v) = from.get(&name) {
                        into.bind(format!("{}{}", Self::CARRY, name), v.clone());
                    }
                }
            }
        }
    }


    /// Evaluate an ORDER BY or WHERE expression against a projected row,
    /// widened with the pre-projection bindings `carry_sort_scope` copied in.
    ///
    /// Widened **always**, not only when the projected row yields null. A
    /// predicate over a name the projection dropped does not have to evaluate
    /// to null to be wrong: `name = 'B' OR a.name2 = 'C'` with `a` gone gives
    /// a perfectly ordinary `false`, and a fallback keyed on null never fires.
    ///
    /// The projected alias still wins, because `carry_sort_scope` only copies
    /// a name the projection did not already bind.
    fn eval_sort_key(expr: &Expression, record: &Record, store: &GraphStore) -> Value {
        let mut names = HashSet::new();
        collect_expression_names(expr, &mut names);
        let mut widened: Option<Record> = None;
        for name in names {
            if let Some(v) = record.get(&format!("{}{}", Self::CARRY, name)) {
                let v = v.clone();
                widened.get_or_insert_with(|| record.clone()).bind(name, v);
            }
        }
        let target = widened.as_ref().unwrap_or(record);
        Self::evaluate_expression(expr, target, store).unwrap_or(Value::Null)
    }

    fn evaluate_expression(expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
        match expr {
            // Delegates rather than adding a sixth copy of this logic; the
            // standalone evaluator is the one implementation (#654).
            Expression::ListExpr(_) | Expression::MapExpr(_) => {
                eval_expression(expr, record, store)
            }
            Expression::Variable(var) => {
                Ok(record.get(var).cloned().unwrap_or(Value::Null))
            }
            Expression::Property { variable, property } => {
                read_property(record, variable, property, store, true)
            }
            Expression::Literal(lit) => Ok(Value::Property(lit.clone())),
            Expression::Binary { left, op, right } => {
                let left_val = Self::evaluate_expression(left, record, store)?;
                let right_val = Self::evaluate_expression(right, record, store)?;
                eval_binary_op(op, left_val, right_val)
            }
            Expression::Unary { op, expr } => {
                let val = Self::evaluate_expression(expr, record, store)?;
                eval_unary_op(op, val)
            }
            Expression::Function { name, args, .. } => {
                let arg_vals: Vec<Value> = args.iter()
                    .map(|a| Self::evaluate_expression(a, record, store))
                    .collect::<ExecutionResult<Vec<_>>>()?;
                eval_function(name, &arg_vals, Some(store))
            }
            Expression::Case { operand, when_clauses, else_result } => {
                eval_case(operand.as_deref(), when_clauses, else_result.as_deref(), |e| Self::evaluate_expression(e, record, store))
            }
            Expression::Index { expr, index } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let idx = Self::evaluate_expression(index, record, store)?;
                eval_index(collection, idx, store)
            }
            Expression::ListSlice { expr, start, end } => {
                let collection = Self::evaluate_expression(expr, record, store)?;
                let s = match start { Some(s) => Some(Self::evaluate_expression(s, record, store)?), None => None };
                let en = match end { Some(e) => Some(Self::evaluate_expression(e, record, store)?), None => None };
                eval_list_slice(collection, s, en)
            }
            Expression::ExistsSubquery { pattern, where_clause, .. } => {
                eval_exists_subquery(pattern, where_clause.as_deref(), record, store)
            }
            Expression::ListComprehension { variable, list_expr, filter, map_expr } => {
                eval_list_comprehension(variable, list_expr, filter.as_deref(), map_expr, record, store)
            }
            Expression::PredicateFunction { name, variable, list_expr, predicate } => {
                eval_predicate_function(name, variable, list_expr, predicate, record, store)
            }
            Expression::Reduce { accumulator, init, variable, list_expr, expression } => {
                eval_reduce(accumulator, init, variable, list_expr, expression, record, store)
            }
            Expression::PatternComprehension { pattern, filter, projection } => {
                eval_pattern_comprehension(pattern, filter.as_deref(), projection, record, store)
            }
            Expression::PathVariable(var) => {
                record.get(var).cloned()
                    .ok_or_else(|| ExecutionError::VariableNotFound(var.clone()))
            }
            Expression::Parameter(name) => {
                record.get(&format!("${}", name)).cloned()
                    .ok_or_else(|| ExecutionError::RuntimeError(format!("Unresolved parameter: ${}", name)))
            }
        }
    }

    fn execute_all(&mut self, store: &GraphStore) -> ExecutionResult<()> {
        let mut output_records = if self.has_aggregation {
            // Aggregation path: group by non-aggregate items
            let mut groups: HashMap<Vec<Value>, Vec<AggregatorState>> = HashMap::new();

            let batch_size = 65536;
            while let Some(batch) = self.input.next_batch(store, batch_size)? {
                for record in batch.records {
                    let mut key = Vec::new();
                    for (expr, _) in &self.group_by {
                        key.push(Self::evaluate_expression(expr, &record, store)?);
                    }

                    let states = groups.entry(key).or_insert_with(|| {
                        self.aggregates.iter().map(|agg| AggregatorState::new(&agg.func, agg.distinct)).collect()
                    });

                    for (i, agg) in self.aggregates.iter().enumerate() {
                        let val = Self::evaluate_expression(&agg.expr, &record, store)?;
                        states[i].update(&val);
                    }
                }
            }

            let mut records = Vec::new();
            for (key, states) in groups {
                let mut record = Record::new();
                for (i, (_, alias)) in self.group_by.iter().enumerate() {
                    record.bind(alias.clone(), key[i].clone());
                }
                for (i, agg) in self.aggregates.iter().enumerate() {
                    record.bind(agg.alias.clone(), states[i].result());
                }
                records.push(record);
            }

            // Post-projection: evaluate items (which may contain rewritten aggregate
            // references like Variable("__agg_0")) against the intermediate records
            let mut projected = Vec::with_capacity(records.len());
            for intermediate in records {
                let mut new_record = Record::new();
                for (expr, alias) in &self.items {
                    let value = Self::evaluate_expression(expr, &intermediate, store)?;
                    new_record.bind(alias.clone(), value);
                }
                self.carry_sort_scope(&intermediate, &mut new_record);
                projected.push(new_record);
            }
            projected
        } else {
            // Non-aggregation path: project each row.
            //
            // Early-termination: when the WITH clause is just `WITH ... LIMIT N` with no
            // ORDER BY, no WHERE, and no DISTINCT, we can stop pulling from upstream once
            // we have (skip + limit) records. This turns a full scan into a bounded one
            // for patterns like `MATCH (m:MeSHTerm) WITH m LIMIT 500 MATCH ...`.
            let can_stream_limit = self.limit.is_some()
                && self.sort_items.is_empty()
                && self.where_predicate.is_none()
                && !self.distinct;
            let cap = if can_stream_limit {
                Some(self.skip.unwrap_or(0) + self.limit.unwrap())
            } else {
                None
            };

            let mut records = Vec::new();
            let batch_size = 65536;
            'outer: while let Some(batch) = self.input.next_batch(store, batch_size)? {
                for record in batch.records {
                    let mut new_record = Record::new();
                    for (expr, alias) in &self.items {
                        let value = Self::evaluate_expression(expr, &record, store)?;
                        new_record.bind(alias.clone(), value);
                    }
                    self.carry_sort_scope(&record, &mut new_record);
                    records.push(new_record);
                    if let Some(c) = cap {
                        if records.len() >= c {
                            break 'outer;
                        }
                    }
                }
            }
            records
        };

        // Apply WHERE filter (if present in WITH ... WHERE ...)
        if let Some(ref predicate) = self.where_predicate {
            output_records.retain(|record| {
                // Through the same widening as the sort: a WITH's WHERE sees
                // the projected aliases *and* the scope in front of them.
                matches!(
                    Self::eval_sort_key(predicate, record, store),
                    Value::Property(PropertyValue::Boolean(true))
                )
            });
        }

        // Apply DISTINCT
        if self.distinct {
            let mut seen: HashSet<Vec<Value>> = HashSet::new();
            output_records.retain(|record| {
                let vals: Vec<Value> =
                    record.dedup_key().into_iter().map(|(_, v)| v).collect();
                seen.insert(vals)
            });
        }

        // Apply ORDER BY
        if !self.sort_items.is_empty() {
            let sort_items = &self.sort_items;
            output_records.sort_by(|a, b| {
                for (expr, ascending) in sort_items {
                    // The projected name wins; the carried pre-projection
                    // binding answers for anything the projection dropped.
                    let val_a = Self::eval_sort_key(expr, a, store);
                    let val_b = Self::eval_sort_key(expr, b, store);
                    // Cypher's orderability, not the property index's — see
                    // `graph::property::cypher_order`. A WITH ... ORDER BY
                    // sorts here rather than in `SortOperator`, so wiring only
                    // that one left every `WITH` sort on the old order — the
                    // same trap for the entity ranks (#917), which is why both
                    // sites now call the `Value`-level comparison.
                    let ord = crate::query::executor::record::cypher_order_value(&val_a, &val_b);
                    if ord != std::cmp::Ordering::Equal {
                        return if *ascending { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // The carried pre-projection bindings are the sort's business only.
        // Left in place they would appear as columns and, worse, re-enter
        // scope for the next clause under a name it never projected.
        if !self.sort_items.is_empty() || self.where_predicate.is_some() {
            for record in output_records.iter_mut() {
                record.retain_bindings(|k| !k.starts_with(Self::CARRY));
            }
        }

        // Apply SKIP
        if let Some(skip) = self.skip {
            if skip < output_records.len() {
                output_records = output_records.split_off(skip);
            } else {
                output_records.clear();
            }
        }

        // Apply LIMIT
        if let Some(limit) = self.limit {
            output_records.truncate(limit);
        }

        self.results = output_records.into_iter();
        self.executed = true;
        Ok(())
    }
}

impl PhysicalOperator for WithBarrierOperator {
    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    /// Pull the input **mutably** before materialising.
    ///
    /// A barrier reads its whole input before emitting anything. Doing that
    /// read-only meant a write below a `WITH` silently did not happen:
    /// `MATCH (n) SET n.x = 1 WITH n RETURN n.x` returned the *old* value, the
    /// query succeeded, and the store was unchanged. That is the real reason
    /// the grammar only ever allowed writes after the last projection — not
    /// syntax, but that mutability never reached below a barrier.
    ///
    /// The input is drained into a `MaterializedOperator` rather than
    /// threading `&mut GraphStore` through `execute_all` and its grouping
    /// helpers, which are the most intricate code in this file. A barrier
    /// materialises its whole input anyway, so nothing is buffered that would
    /// not have been.
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            let mut rows = Vec::new();
            while let Some(batch) = self.input.next_batch_mut(store, tenant_id, 65536)? {
                rows.extend(batch.records);
            }
            self.input = Box::new(MaterializedOperator::new(rows));
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if !self.executed {
            self.execute_all(store)?;
        }
        Ok(self.results.next())
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        if !self.executed {
            self.execute_all(store)?;
        }

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = self.results.next() {
                batch.push(record);
            } else {
                break;
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch { records: batch, columns: Vec::new() }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.executed = false;
        self.results = Vec::new().into_iter();
    }

    fn describe(&self) -> OperatorDescription {
        let item_strs: Vec<String> = self.items.iter().map(|(e, a)| {
            format!("{} AS {}", format_expression(e), a)
        }).collect();
        let mut details = format!("items=[{}]", item_strs.join(", "));
        if self.distinct { details.push_str(", DISTINCT"); }
        if !self.sort_items.is_empty() { details.push_str(", ORDER BY"); }
        if self.skip.is_some() { details.push_str(&format!(", SKIP {}", self.skip.unwrap())); }
        if self.limit.is_some() { details.push_str(&format!(", LIMIT {}", self.limit.unwrap())); }
        OperatorDescription {
            name: "WithBarrier".to_string(),
            details,
            children: vec![self.input.describe()],
        }
    }
}

/// ExpandInto operator: checks whether an edge exists between two already-bound endpoints.
///
/// Unlike ExpandOperator (which fans out from one bound node to discover new neighbors),
/// ExpandInto takes a record where BOTH source and target are already bound, and checks
/// whether a connecting edge exists. If it does, the record passes through (with the edge
/// optionally bound); if not, the record is filtered out.
///
/// This is semantically a filter (fan-in), not an expansion (fan-out).
pub struct ExpandIntoOperator {
    input: OperatorBox,
    source_binding: String,
    target_binding: String,
    edge_type: Option<String>,
    edge_binding: Option<String>,
}

impl ExpandIntoOperator {
    pub fn new(
        input: OperatorBox,
        source_binding: String,
        target_binding: String,
        edge_type: Option<String>,
        edge_binding: Option<String>,
    ) -> Self {
        Self {
            input,
            source_binding,
            target_binding,
            edge_type,
            edge_binding,
        }
    }
}

impl PhysicalOperator for ExpandIntoOperator {
    // A write beneath this operator refused with "requires mutable store
    // access", because the default `next_mut` delegates to `next` and `next`
    // reads its input read-only. Shared body rather than a second, mutable copy
    // of this operator's own logic -- see `drain_input_for_write` (#870).
    fn next_mut(&mut self, store: &mut GraphStore, tenant_id: &str) -> ExecutionResult<Option<Record>> {
        drain_input_for_write(&mut self.input, store, tenant_id)?;
        self.next(store)
    }

    fn children_mut(&mut self) -> Vec<&mut OperatorBox> {
        vec![&mut self.input]
    }

    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            let record = match self.input.next(store)? {
                Some(r) => r,
                None => return Ok(None),
            };

            let source_id = record.get(&self.source_binding)
                .and_then(|v| v.node_id())
                .ok_or_else(|| ExecutionError::VariableNotFound(self.source_binding.clone()))?;

            let target_id = record.get(&self.target_binding)
                .and_then(|v| v.node_id())
                .ok_or_else(|| ExecutionError::VariableNotFound(self.target_binding.clone()))?;

            let et = self.edge_type.as_ref().map(|t| EdgeType::new(t.as_str()));
            let et_ref = et.as_ref();

            if let Some(edge_id) = store.edge_between(source_id, target_id, et_ref) {
                let mut new_record = record;
                if let Some(ref edge_var) = self.edge_binding {
                    // Try full Edge first, fall back to edge_type_ids for stubs
                    if let Some(edge) = store.get_edge(edge_id) {
                        new_record.bind(
                            edge_var.clone(),
                            Value::EdgeRef(edge_id, edge.source, edge.target, edge.edge_type.clone()),
                        );
                    } else if let Some(edge_type) = store.get_edge_type(edge_id) {
                        new_record.bind(
                            edge_var.clone(),
                            Value::EdgeRef(edge_id, source_id, target_id, edge_type),
                        );
                    }
                }
                return Ok(Some(new_record));
            }
            // No edge found — skip this record, try next
        }
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: Vec::new(),
            }))
        }
    }

    fn reset(&mut self) {
        self.input.reset();
    }

    fn describe(&self) -> OperatorDescription {
        let type_str = self.edge_type.as_deref().unwrap_or("*");
        OperatorDescription {
            name: "ExpandInto".to_string(),
            details: format!("({})--[:{}]-->({})", self.source_binding, type_str, self.target_binding),
            children: vec![self.input.describe()],
        }
    }
}

/// NodeById operator: start from a specific set of node IDs.
///
/// Useful when the planner knows the exact starting nodes (e.g., from an index lookup
/// or from a previous query stage).
pub struct NodeByIdOperator {
    node_ids: Vec<NodeId>,
    position: usize,
    variable: String,
    /// Labels the pattern required, checked per id.
    ///
    /// A scan by id bypasses the label index, so `MATCH (n:Person) WHERE
    /// id(n) = 5` would otherwise match a node of any label that happens to
    /// hold that id (#538).
    labels: Vec<Label>,
}

impl NodeByIdOperator {
    pub fn new(node_ids: Vec<NodeId>, variable: String) -> Self {
        Self {
            node_ids,
            position: 0,
            variable,
            labels: Vec::new(),
        }
    }

    /// Require the node to carry every one of these labels.
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }
}

impl PhysicalOperator for NodeByIdOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        while self.position < self.node_ids.len() {
            let node_id = self.node_ids[self.position];
            self.position += 1;

            // Verify the node still exists and carries the pattern's labels.
            let matches = match store.get_node(node_id) {
                Some(node) => self.labels.iter().all(|l| node.has_label(l)),
                None => false,
            };
            if matches {
                let mut record = Record::new();
                record.bind(self.variable.clone(), Value::NodeRef(node_id));
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
        let mut records = Vec::new();
        for _ in 0..batch_size {
            match self.next(store)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        if records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(RecordBatch {
                records,
                columns: vec![self.variable.clone()],
            }))
        }
    }

    fn reset(&mut self) {
        self.position = 0;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "NodeById".to_string(),
            details: format!("var={}, ids={:?}", self.variable, self.node_ids.iter().map(|id| id.as_u64()).collect::<Vec<_>>()),
            children: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Label;

    #[test]
    fn test_node_scan_operator() {
        let mut store = GraphStore::new();
        let _alice = store.create_node("Person");
        let _bob = store.create_node("Person");

        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);

        let mut count = 0;
        while let Ok(Some(_record)) = op.next(&store) {
            count += 1;
        }

        assert_eq!(count, 2);
    }

    #[test]
    fn test_node_scan_empty_label_initializes_once() {
        // Regression: with the lazy-init refactor, a label with zero matching
        // nodes should not re-initialize on every next() call. Previously
        // initialize() guarded on `node_ids.is_empty()`, causing a re-init
        // attempt every poll.
        let mut store = GraphStore::new();
        store.create_node("Person");
        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("NoSuchLabel")]);
        for _ in 0..5 {
            assert!(op.next(&store).unwrap().is_none());
        }
    }

    #[test]
    fn test_node_scan_early_limit_terminates_streaming() {
        // With early_limit set on a single-label scan, we should stop after
        // exactly `limit` records — never iterating the rest of the label set.
        let mut store = GraphStore::new();
        for _ in 0..1000 {
            store.create_node("Person");
        }
        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")])
            .with_early_limit(7);
        let mut count = 0;
        while let Ok(Some(_)) = op.next(&store) {
            count += 1;
        }
        assert_eq!(count, 7);
    }

    #[test]
    fn test_try_push_limit_through_project_to_nodescan() {
        // Verify that try_push_limit propagates: Limit -> Project -> NodeScan.
        // The NodeScan should accept the hint and set early_limit.
        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let project = ProjectOperator::new(
            Box::new(scan),
            vec![(Expression::Variable("n".to_string()), "n".to_string())],
        );
        let mut limit = LimitOperator::new(Box::new(project), 5);

        assert!(limit.try_push_limit(5), "LimitOperator should forward to child");
        // After push, scan should have early_limit=5. We can't inspect through
        // the box, but we verify behaviorally on a 100-node store.
        let mut store = GraphStore::new();
        for _ in 0..100 {
            store.create_node("Person");
        }
        let mut produced = 0;
        while let Ok(Some(_)) = limit.next(&store) {
            produced += 1;
        }
        assert_eq!(produced, 5);
    }

    #[test]
    fn test_try_push_limit_blocked_by_filter() {
        // FilterOperator does NOT override try_push_limit — default returns
        // false. So the push doesn't reach NodeScan, and early_limit stays
        // unset. This is correct because filter selectivity is unknown.
        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let predicate = Expression::Literal(PropertyValue::Boolean(true)); // always-true
        let mut filter = FilterOperator::new(Box::new(scan), predicate);
        assert!(!filter.try_push_limit(5), "Filter should block push");
    }

    #[test]
    fn test_node_scan_multi_label_is_a_conjunction() {
        // This assertion used to read `count == 50, "multi-label union should
        // dedup"`. It encoded the defect: `(n:Person:Adult)` in Cypher is a
        // conjunction, so the answer is the 25 nodes carrying **both**, not the
        // 50 carrying either. openCypher `Match1` [3] settles it, and this test
        // is why the union survived (#944).
        let mut store = GraphStore::new();
        for _ in 0..50 {
            let id = store.create_node("Person");
            // Second label on half of them, so union and intersection differ.
            //
            // Through `add_label_to_node`, not `get_node_mut().labels.insert`.
            // The direct insert writes the node and **not** `label_index`, so
            // `node_ids_by_label("Adult")` returned nothing -- and the union
            // hid that, because `Person ∪ nothing` is still 50. The
            // intersection is what exposed the fixture, which is the same
            // failure the index itself exists to prevent: a label the node
            // carries and the index does not know about is invisible to every
            // query that looks for it.
            if id.as_u64() % 2 == 0 {
                store
                    .add_label_to_node("default", id, Label::new("Adult"))
                    .expect("label added through the store, so the index sees it");
            }
        }
        let mut op = NodeScanOperator::new(
            "n".to_string(),
            vec![Label::new("Person"), Label::new("Adult")],
        );
        let mut count = 0;
        while let Ok(Some(_)) = op.next(&store) {
            count += 1;
        }
        assert_eq!(count, 25, "both labels, not either");

        // Each label alone still scans its own set, so a fix that simply
        // narrowed everything would be caught here.
        for (label, want) in [("Person", 50), ("Adult", 25)] {
            let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new(label)]);
            let mut count = 0;
            while let Ok(Some(_)) = op.next(&store) {
                count += 1;
            }
            assert_eq!(count, want, "{label} alone");
        }

        // `early_limit` counts nodes that actually match. The union counted
        // insertions, so with a LIMIT it could stop before finding any node
        // carrying all the labels.
        let mut op = NodeScanOperator::new(
            "n".to_string(),
            vec![Label::new("Person"), Label::new("Adult")],
        )
        .with_early_limit(10);
        let mut count = 0;
        while let Ok(Some(_)) = op.next(&store) {
            count += 1;
        }
        assert_eq!(count, 10);

        // A label no node carries makes the whole conjunction empty.
        let mut op = NodeScanOperator::new(
            "n".to_string(),
            vec![Label::new("Person"), Label::new("Nonexistent")],
        );
        assert!(matches!(op.next(&store), Ok(None)));
    }

    #[test]
    fn test_filter_operator() {
        let mut store = GraphStore::new();
        let alice = store.create_node("Person");
        if let Some(node) = store.get_node_mut(alice) {
            node.set_property("age", 30i64);
        }

        let bob = store.create_node("Person");
        if let Some(node) = store.get_node_mut(bob) {
            node.set_property("age", 25i64);
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let predicate = Expression::Binary {
            left: Box::new(Expression::Property {
                variable: "n".to_string(),
                property: "age".to_string(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expression::Literal(PropertyValue::Integer(28))),
        };

        let mut filter = FilterOperator::new(Box::new(scan), predicate);

        let mut count = 0;
        while let Ok(Some(_record)) = filter.next(&store) {
            count += 1;
        }

        assert_eq!(count, 1); // Only Alice (age 30) passes the filter
    }

    #[test]
    fn test_limit_operator() {
        let mut store = GraphStore::new();
        for _ in 0..10 {
            store.create_node("Person");
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let mut limit = LimitOperator::new(Box::new(scan), 3);

        let mut count = 0;
        while let Ok(Some(_record)) = limit.next(&store) {
            count += 1;
        }

        assert_eq!(count, 3);
    }

    #[test]
    fn test_node_scan_batch() {
        let mut store = GraphStore::new();
        for i in 0..10 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "id", i as i64).unwrap();
        }

        let mut op = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        
        // Request batch size 4
        let batch1 = op.next_batch(&store, 4).unwrap().unwrap();
        assert_eq!(batch1.len(), 4);
        
        let batch2 = op.next_batch(&store, 4).unwrap().unwrap();
        assert_eq!(batch2.len(), 4);
        
        let batch3 = op.next_batch(&store, 4).unwrap().unwrap();
        assert_eq!(batch3.len(), 2); // Remaining
        
        let batch4 = op.next_batch(&store, 4).unwrap();
        assert!(batch4.is_none());
    }

    #[test]
    fn test_project_batch() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.set_node_property("default", id, "age", 30).unwrap();

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        let mut project = ProjectOperator::new(Box::new(scan), vec![
            (Expression::Property { variable: "n".to_string(), property: "age".to_string() }, "age".to_string())
        ]);

        let batch = project.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        let age = batch.records[0].get("age").unwrap().as_property().unwrap().as_integer().unwrap();
        assert_eq!(age, 30);
    }

    #[test]
    fn test_filter_batch() {
        let mut store = GraphStore::new();
        for i in 0..10 {
            let id = store.create_node("Person");
            store.set_node_property("default", id, "val", i as i64).unwrap();
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Person")]);
        // Filter val >= 5
        let predicate = Expression::Binary {
            left: Box::new(Expression::Property { variable: "n".to_string(), property: "val".to_string() }),
            op: BinaryOp::Ge,
            right: Box::new(Expression::Literal(PropertyValue::Integer(5))),
        };

        let mut filter = FilterOperator::new(Box::new(scan), predicate);

        // Pull in batches of 10 (should get all 5 matches in one go or multiple depending on implementation)
        // Implementation loops until batch filled or source exhausted.
        let batch = filter.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 5);
        for r in batch.records {
            let val = r.get("n").unwrap().resolve_property("val", &store).as_integer().unwrap();
            assert!(val >= 5);
        }
    }

    #[test]
    fn test_aggregate_batch() {
        let mut store = GraphStore::new();
        // 3 items group A, 2 items group B
        for _ in 0..3 {
            let id = store.create_node("Item");
            store.set_node_property("default", id, "group", "A").unwrap();
        }
        for _ in 0..2 {
            let id = store.create_node("Item");
            store.set_node_property("default", id, "group", "B").unwrap();
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Item")]);
        let mut agg = AggregateOperator::new(
            Box::new(scan),
            vec![(Expression::Property { variable: "n".to_string(), property: "group".to_string() }, "group".to_string())],
            vec![AggregateFunction {
                func: AggregateType::Count,
                expr: Expression::Variable("n".to_string()),
                alias: "count".to_string(),
                distinct: false,
                percentile: None,
            }]
        );

        let batch = agg.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 2); // 2 groups
        
        // Check results
        let mut counts = HashMap::new();
        for r in batch.records {
            let g = r.get("group").unwrap().as_property().unwrap().as_string().unwrap().to_string();
            let c = r.get("count").unwrap().as_property().unwrap().as_integer().unwrap();
            counts.insert(g, c);
        }
        
        assert_eq!(counts.get("A"), Some(&3));
        assert_eq!(counts.get("B"), Some(&2));
    }

    #[test]
    fn test_sort_batch() {
        let mut store = GraphStore::new();
        let values = vec![5, 1, 3, 2, 4];
        for v in values {
            let id = store.create_node("Num");
            store.set_node_property("default", id, "val", v).unwrap();
        }

        let scan = NodeScanOperator::new("n".to_string(), vec![Label::new("Num")]);
        let mut sort = SortOperator::new(
            Box::new(scan),
            vec![(Expression::Property { variable: "n".to_string(), property: "val".to_string() }, true)] // Ascending
        );

        let batch = sort.next_batch(&store, 10).unwrap().unwrap();
        assert_eq!(batch.len(), 5);

        let sorted_vals: Vec<i64> = batch.records.iter()
            .map(|r| r.get("n").unwrap().resolve_property("val", &store).as_integer().unwrap())
            .collect();

        assert_eq!(sorted_vals, vec![1, 2, 3, 4, 5]);
    }

    // ========== Batch 1: eval_function tests ==========

    // -- Date/Time functions --

    #[test]
    fn test_eval_function_date_no_args() {
        // `date()` returns a Date, not a timestamp (#689). Asserting the
        // *type* is the point: before, every temporal constructor returned the
        // same `DateTime(millis)` and this test passed for `time()` too.
        let result = eval_function("date", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::Date(days)) => assert!(days > 0),
            other => panic!("Expected Date, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_date_string() {
        let result = eval_function("date", &[Value::Property(PropertyValue::String("2024-01-15".to_string()))], None).unwrap();
        match result {
            Value::Property(p @ PropertyValue::Date(_)) => {
                assert_eq!(p.to_cypher_string(), "2024-01-15");
            }
            other => panic!("Expected Date, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_date_map() {
        let mut map = HashMap::new();
        map.insert("year".to_string(), PropertyValue::Integer(2024));
        map.insert("month".to_string(), PropertyValue::Integer(6));
        map.insert("day".to_string(), PropertyValue::Integer(15));
        let result = eval_function("date", &[Value::Property(PropertyValue::Map(map))], None).unwrap();
        match result {
            Value::Property(p @ PropertyValue::Date(_)) => {
                assert_eq!(p.to_cypher_string(), "2024-06-15");
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_date_invalid_string() {
        let result = eval_function("date", &[Value::Property(PropertyValue::String("not-a-date".to_string()))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_date_invalid_map() {
        let mut map = HashMap::new();
        map.insert("year".to_string(), PropertyValue::Integer(2024));
        map.insert("month".to_string(), PropertyValue::Integer(13)); // invalid month
        map.insert("day".to_string(), PropertyValue::Integer(1));
        let result = eval_function("date", &[Value::Property(PropertyValue::Map(map))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_date_type_error() {
        let result = eval_function("date", &[Value::Property(PropertyValue::Integer(42))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_datetime_no_args() {
        let result = eval_function("datetime", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::ZonedDateTime { secs, .. }) => assert!(secs > 0),
            other => panic!("Expected ZonedDateTime, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_datetime_rfc3339() {
        let result = eval_function("datetime", &[Value::Property(PropertyValue::String("2024-01-15T10:30:00Z".to_string()))], None).unwrap();
        match result {
            Value::Property(p @ PropertyValue::ZonedDateTime { .. }) => {
                assert_eq!(p.to_cypher_string(), "2024-01-15T10:30Z");
            }
            other => panic!("Expected ZonedDateTime, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_datetime_naive() {
        let result = eval_function("datetime", &[Value::Property(PropertyValue::String("2024-01-15T10:30:00".to_string()))], None).unwrap();
        match result {
            Value::Property(p @ PropertyValue::ZonedDateTime { .. }) => {
                assert_eq!(p.to_cypher_string(), "2024-01-15T10:30Z");
            }
            other => panic!("Expected ZonedDateTime, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_datetime_map() {
        let mut map = HashMap::new();
        map.insert("year".to_string(), PropertyValue::Integer(2024));
        map.insert("month".to_string(), PropertyValue::Integer(3));
        map.insert("day".to_string(), PropertyValue::Integer(15));
        map.insert("hour".to_string(), PropertyValue::Integer(10));
        map.insert("minute".to_string(), PropertyValue::Integer(30));
        map.insert("second".to_string(), PropertyValue::Integer(45));
        let result = eval_function("datetime", &[Value::Property(PropertyValue::Map(map))], None).unwrap();
        match result {
            Value::Property(p @ PropertyValue::ZonedDateTime { .. }) => {
                assert_eq!(p.to_cypher_string(), "2024-03-15T10:30:45Z");
                use chrono::TimeZone;
                let expected = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 45).unwrap().timestamp_millis();
                let ts = p.as_epoch_millis().unwrap();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_invalid_string() {
        let result = eval_function("datetime", &[Value::Property(PropertyValue::String("garbage".to_string()))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_datetime_type_error() {
        let result = eval_function("datetime", &[Value::Property(PropertyValue::Boolean(true))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_iso_string() {
        let result = eval_function("duration", &[Value::Property(PropertyValue::String("P1Y2M3D".to_string()))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, days, seconds, .. }) => {
                assert_eq!(months, 14); // 1Y = 12M + 2M
                assert_eq!(days, 3);
                assert_eq!(seconds, 0);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_with_time() {
        let result = eval_function("duration", &[Value::Property(PropertyValue::String("P1DT2H30M".to_string()))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, days, seconds, .. }) => {
                assert_eq!(months, 0);
                assert_eq!(days, 1);
                assert_eq!(seconds, 2 * 3600 + 30 * 60);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_map() {
        let mut map = HashMap::new();
        map.insert("months".to_string(), PropertyValue::Integer(3));
        map.insert("days".to_string(), PropertyValue::Integer(5));
        map.insert("hours".to_string(), PropertyValue::Integer(2));
        let result = eval_function("duration", &[Value::Property(PropertyValue::Map(map))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, days, seconds, .. }) => {
                assert_eq!(months, 3);
                assert_eq!(days, 5);
                assert_eq!(seconds, 7200);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_map_with_years() {
        let mut map = HashMap::new();
        map.insert("years".to_string(), PropertyValue::Integer(2));
        map.insert("months".to_string(), PropertyValue::Integer(6));
        let result = eval_function("duration", &[Value::Property(PropertyValue::Map(map))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, .. }) => {
                assert_eq!(months, 30); // 2*12 + 6
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_no_args() {
        let result = eval_function("duration", &[], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_invalid_string() {
        let result = eval_function("duration", &[Value::Property(PropertyValue::String("not-a-duration".to_string()))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_type_error() {
        let result = eval_function("duration", &[Value::Property(PropertyValue::Integer(42))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_timestamp() {
        let result = eval_function("timestamp", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::Integer(ts)) => assert!(ts > 0),
            _ => panic!("Expected Integer timestamp"),
        }
    }

    #[test]
    fn test_eval_function_duration_between() {
        let dt1 = Value::Property(PropertyValue::DateTime(1000000));
        let dt2 = Value::Property(PropertyValue::DateTime(2000000));
        let result = eval_function("duration_between", &[dt1, dt2], None).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { seconds, .. }) => {
                assert_eq!(seconds, 1000); // 1000000ms diff = 1000s
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_eval_function_duration_between_type_error() {
        let result = eval_function("duration_between", &[
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::DateTime(0)),
        ], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_duration_between_too_few_args() {
        let result = eval_function("duration_between", &[Value::Property(PropertyValue::DateTime(0))], None);
        assert!(result.is_err());
    }

    // -- Math functions --

    #[test]
    fn test_eval_function_log_float() {
        let result = eval_function("log", &[Value::Property(PropertyValue::Float(1.0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_log_integer() {
        let result = eval_function("log", &[Value::Property(PropertyValue::Integer(1))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 0.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_log_type_error() {
        let result = eval_function("log", &[Value::Property(PropertyValue::String("x".to_string()))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_exp_float() {
        let result = eval_function("exp", &[Value::Property(PropertyValue::Float(1.0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - std::f64::consts::E).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_exp_zero() {
        let result = eval_function("exp", &[Value::Property(PropertyValue::Integer(0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_exp_type_error() {
        let result = eval_function("exp", &[Value::Property(PropertyValue::Boolean(true))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_rand() {
        let result = eval_function("rand", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => {
                assert!(f >= 0.0 && f < 1.0);
            }
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_abs_int() {
        let result = eval_function("abs", &[Value::Property(PropertyValue::Integer(-42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_abs_float() {
        let result = eval_function("abs", &[Value::Property(PropertyValue::Float(-3.14))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.14).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_abs_type_error() {
        let result = eval_function("abs", &[Value::Property(PropertyValue::String("x".to_string()))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_ceil_float() {
        let result = eval_function("ceil", &[Value::Property(PropertyValue::Float(3.2))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(4)));
    }

    #[test]
    fn test_eval_function_ceil_int() {
        let result = eval_function("ceil", &[Value::Property(PropertyValue::Integer(3))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_floor_float() {
        let result = eval_function("floor", &[Value::Property(PropertyValue::Float(3.9))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_floor_int() {
        let result = eval_function("floor", &[Value::Property(PropertyValue::Integer(5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(5)));
    }

    #[test]
    fn test_eval_function_round_float() {
        let result = eval_function("round", &[Value::Property(PropertyValue::Float(3.5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(4)));
    }

    #[test]
    fn test_eval_function_round_int() {
        let result = eval_function("round", &[Value::Property(PropertyValue::Integer(7))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(7)));
    }

    #[test]
    fn test_eval_function_sqrt_float() {
        let result = eval_function("sqrt", &[Value::Property(PropertyValue::Float(16.0))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 4.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_sqrt_int() {
        let result = eval_function("sqrt", &[Value::Property(PropertyValue::Integer(9))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_sign_positive() {
        let result = eval_function("sign", &[Value::Property(PropertyValue::Integer(42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(1)));
    }

    #[test]
    fn test_eval_function_sign_negative() {
        let result = eval_function("sign", &[Value::Property(PropertyValue::Integer(-5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(-1)));
    }

    #[test]
    fn test_eval_function_sign_zero() {
        let result = eval_function("sign", &[Value::Property(PropertyValue::Integer(0))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(0)));
    }

    #[test]
    fn test_eval_function_sign_float() {
        let result = eval_function("sign", &[Value::Property(PropertyValue::Float(-2.5))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(-1)));
    }

    #[test]
    fn test_eval_function_sign_float_zero() {
        let result = eval_function("sign", &[Value::Property(PropertyValue::Float(0.0))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(0)));
    }

    // -- String edge-case functions --

    #[test]
    fn test_eval_function_ltrim() {
        let result = eval_function("ltrim", &[Value::Property(PropertyValue::String("  hello  ".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello  ".to_string())));
    }

    #[test]
    fn test_eval_function_rtrim() {
        let result = eval_function("rtrim", &[Value::Property(PropertyValue::String("  hello  ".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("  hello".to_string())));
    }

    #[test]
    fn test_eval_function_trim() {
        let result = eval_function("trim", &[Value::Property(PropertyValue::String("  hello  ".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello".to_string())));
    }

    #[test]
    fn test_eval_function_tostring_integer() {
        let result = eval_function("tostring", &[Value::Property(PropertyValue::Integer(42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("42".to_string())));
    }

    #[test]
    fn test_eval_function_tostring_boolean() {
        let result = eval_function("tostring", &[Value::Property(PropertyValue::Boolean(true))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("true".to_string())));
    }

    #[test]
    fn test_eval_function_tostring_float() {
        let result = eval_function("tostring", &[Value::Property(PropertyValue::Float(3.14))], None).unwrap();
        match result {
            Value::Property(PropertyValue::String(s)) => assert!(s.starts_with("3.14")),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_eval_function_tostring_null() {
        // null in, null out — *not* the four-character string "null", which
        // would be indistinguishable from a genuine value and would make
        // `toString(x) = 'null'` true for a missing property. The TCK has no
        // scenario pinning this one, so it was checked against Neo4j 5
        // directly: `toString(null) IS NULL` -> true.
        let result = eval_function("tostring", &[Value::Null], None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_tostring_string() {
        let result = eval_function("tostring", &[Value::Property(PropertyValue::String("hello".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello".to_string())));
    }

    #[test]
    fn test_eval_function_tointeger_string() {
        let result = eval_function("tointeger", &[Value::Property(PropertyValue::String("42".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_tointeger_bad_string() {
        // Null rather than an error, matching Cypher (#606).
        let result = eval_function("tointeger", &[Value::Property(PropertyValue::String("bad".to_string()))], None);
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_eval_function_tointeger_float() {
        let result = eval_function("tointeger", &[Value::Property(PropertyValue::Float(3.9))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_tointeger_integer() {
        let result = eval_function("tointeger", &[Value::Property(PropertyValue::Integer(7))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(7)));
    }

    #[test]
    fn test_eval_function_tointeger_type_error() {
        let result = eval_function("tointeger", &[Value::Property(PropertyValue::Boolean(true))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_tofloat_string() {
        let result = eval_function("tofloat", &[Value::Property(PropertyValue::String("3.14".to_string()))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.14).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_tofloat_bad_string() {
        // Null rather than an error, matching Cypher (#606).
        let result = eval_function("tofloat", &[Value::Property(PropertyValue::String("bad".to_string()))], None);
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_eval_function_tofloat_integer() {
        let result = eval_function("tofloat", &[Value::Property(PropertyValue::Integer(5))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 5.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_tofloat_float() {
        let result = eval_function("tofloat", &[Value::Property(PropertyValue::Float(2.5))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 2.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_eval_function_tofloat_type_error() {
        let result = eval_function("tofloat", &[Value::Property(PropertyValue::Boolean(false))], None);
        assert!(result.is_err());
    }

    // -- String manipulation --

    #[test]
    fn test_eval_function_toupper() {
        let result = eval_function("toupper", &[Value::Property(PropertyValue::String("hello".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("HELLO".to_string())));
    }

    #[test]
    fn test_eval_function_tolower() {
        let result = eval_function("tolower", &[Value::Property(PropertyValue::String("HELLO".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello".to_string())));
    }

    #[test]
    fn test_eval_function_replace() {
        let result = eval_function("replace", &[
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
            Value::Property(PropertyValue::String("rust".to_string())),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello rust".to_string())));
    }

    #[test]
    fn test_eval_function_replace_too_few_args() {
        let result = eval_function("replace", &[
            Value::Property(PropertyValue::String("hello".to_string())),
        ], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_substring() {
        let result = eval_function("substring", &[
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::Integer(6)),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("world".to_string())));
    }

    #[test]
    fn test_eval_function_substring_with_length() {
        let result = eval_function("substring", &[
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::Integer(0)),
            Value::Property(PropertyValue::Integer(5)),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello".to_string())));
    }

    #[test]
    fn test_eval_function_substring_beyond_end() {
        let result = eval_function("substring", &[
            Value::Property(PropertyValue::String("hi".to_string())),
            Value::Property(PropertyValue::Integer(100)),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("".to_string())));
    }

    #[test]
    fn test_eval_function_substring_too_few_args() {
        let result = eval_function("substring", &[
            Value::Property(PropertyValue::String("hello".to_string())),
        ], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_left() {
        let result = eval_function("left", &[
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::Integer(3)),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hel".to_string())));
    }

    #[test]
    fn test_eval_function_right() {
        let result = eval_function("right", &[
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::Integer(3)),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("llo".to_string())));
    }

    #[test]
    fn test_eval_function_reverse() {
        let result = eval_function("reverse", &[Value::Property(PropertyValue::String("abc".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("cba".to_string())));
    }

    // -- Size/length --

    #[test]
    fn test_eval_function_size_string() {
        let result = eval_function("size", &[Value::Property(PropertyValue::String("hello".to_string()))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(5)));
    }

    #[test]
    fn test_eval_function_size_array() {
        let arr = vec![PropertyValue::Integer(1), PropertyValue::Integer(2), PropertyValue::Integer(3)];
        let result = eval_function("size", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(3)));
    }

    #[test]
    fn test_eval_function_length_path() {
        use crate::graph::types::{NodeId, EdgeId};
        let path = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            edges: vec![EdgeId::new(1), EdgeId::new(2)],
        };
        let result = eval_function("length", &[path], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(2)));
    }

    #[test]
    fn test_eval_function_size_type_error() {
        let result = eval_function("size", &[Value::Property(PropertyValue::Integer(42))], None);
        assert!(result.is_err());
    }

    // -- Path functions --

    #[test]
    fn test_eval_function_nodes() {
        use crate::graph::types::{NodeId, EdgeId};
        let path = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            edges: vec![EdgeId::new(10)],
        };
        // The nodes themselves, not their ids. This asserted an
        // `Array([1, 2])` -- which is what the function used to return,
        // because a `PropertyValue` cannot hold a node. An id is a
        // plausible-looking answer that no property access can be read from
        // (#652).
        let result = eval_function("nodes", &[path], None).unwrap();
        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], Value::NodeRef(id) if id.as_u64() == 1));
                assert!(matches!(items[1], Value::NodeRef(id) if id.as_u64() == 2));
            }
            other => panic!("expected a list of nodes, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_nodes_type_error() {
        let result = eval_function("nodes", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_relationships() {
        use crate::graph::types::{NodeId, EdgeId};
        let path = Value::Path {
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            edges: vec![EdgeId::new(10)],
        };
        // With no store to resolve against, the element is `Null` rather
        // than an invented edge -- the shape is a list either way, which is
        // what changed (#652).
        let result = eval_function("relationships", &[path], None).unwrap();
        match result {
            Value::List(items) => assert_eq!(items.len(), 1),
            other => panic!("expected a list of relationships, got {other:?}"),
        }
    }

    #[test]
    fn test_eval_function_relationships_type_error() {
        let result = eval_function("relationships", &[Value::Property(PropertyValue::String("x".to_string()))], None);
        assert!(result.is_err());
    }

    // -- startNode/endNode --

    #[test]
    fn test_eval_function_startnode_edgeref() {
        use crate::graph::types::{NodeId, EdgeId, EdgeType};
        let edge = Value::EdgeRef(EdgeId::new(1), NodeId::new(10), NodeId::new(20), EdgeType::new("KNOWS"));
        let result = eval_function("startnode", &[edge], None).unwrap();
        assert_eq!(result, Value::NodeRef(NodeId::new(10)));
    }

    #[test]
    fn test_eval_function_endnode_edgeref() {
        use crate::graph::types::{NodeId, EdgeId, EdgeType};
        let edge = Value::EdgeRef(EdgeId::new(1), NodeId::new(10), NodeId::new(20), EdgeType::new("KNOWS"));
        let result = eval_function("endnode", &[edge], None).unwrap();
        assert_eq!(result, Value::NodeRef(NodeId::new(20)));
    }

    #[test]
    fn test_eval_function_startnode_type_error() {
        let result = eval_function("startnode", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_endnode_type_error() {
        let result = eval_function("endnode", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    // -- range() --

    #[test]
    fn test_eval_function_range_ascending() {
        let result = eval_function("range", &[
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(5)),
        ], None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let vals: Vec<i64> = arr.iter().map(|v| v.as_integer().unwrap()).collect();
                assert_eq!(vals, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_range_descending() {
        let result = eval_function("range", &[
            Value::Property(PropertyValue::Integer(5)),
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(-1)),
        ], None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let vals: Vec<i64> = arr.iter().map(|v| v.as_integer().unwrap()).collect();
                assert_eq!(vals, vec![5, 4, 3, 2, 1]);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_range_zero_step() {
        let result = eval_function("range", &[
            Value::Property(PropertyValue::Integer(0)),
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(0)),
        ], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_range_too_few_args() {
        let result = eval_function("range", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    // -- Predicate / meta functions --

    #[test]
    fn test_eval_function_coalesce_first_non_null() {
        let result = eval_function("coalesce", &[
            Value::Null,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::Integer(42)),
            Value::Property(PropertyValue::Integer(99)),
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_coalesce_all_null() {
        let result = eval_function("coalesce", &[Value::Null, Value::Property(PropertyValue::Null)], None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_head() {
        let arr = vec![PropertyValue::Integer(10), PropertyValue::Integer(20)];
        let result = eval_function("head", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(10)));
    }

    #[test]
    fn test_eval_function_head_empty() {
        let result = eval_function("head", &[Value::Property(PropertyValue::Array(vec![]))], None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_head_type_error() {
        let result = eval_function("head", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_last() {
        let arr = vec![PropertyValue::Integer(10), PropertyValue::Integer(20)];
        let result = eval_function("last", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(20)));
    }

    #[test]
    fn test_eval_function_last_empty() {
        let result = eval_function("last", &[Value::Property(PropertyValue::Array(vec![]))], None).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_function_tail() {
        let arr = vec![PropertyValue::Integer(1), PropertyValue::Integer(2), PropertyValue::Integer(3)];
        let result = eval_function("tail", &[Value::Property(PropertyValue::Array(arr))], None).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(2));
                assert_eq!(arr[1].as_integer(), Some(3));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_function_tail_type_error() {
        let result = eval_function("tail", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_eval_function_exists_non_null() {
        let result = eval_function("exists", &[Value::Property(PropertyValue::Integer(42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_eval_function_exists_null() {
        let result = eval_function("exists", &[Value::Null], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_eval_function_exists_property_null() {
        let result = eval_function("exists", &[Value::Property(PropertyValue::Null)], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_eval_function_unknown() {
        let result = eval_function("no_such_function", &[], None);
        assert!(result.is_err());
    }

    // ========== eval_binary_op tests ==========

    #[test]
    fn test_binary_op_mod_int() {
        let result = eval_binary_op(&BinaryOp::Mod,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(3)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(1)));
    }

    #[test]
    fn test_binary_op_mod_float() {
        let result = eval_binary_op(&BinaryOp::Mod,
            Value::Property(PropertyValue::Float(10.5)),
            Value::Property(PropertyValue::Float(3.0)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mod_int_float() {
        let result = eval_binary_op(&BinaryOp::Mod,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Float(3.0)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mod_float_int() {
        let result = eval_binary_op(&BinaryOp::Mod,
            Value::Property(PropertyValue::Float(10.0)),
            Value::Property(PropertyValue::Integer(3)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 1.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mod_zero() {
        let result = eval_binary_op(&BinaryOp::Mod,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(0)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_mod_type_error() {
        let result = eval_binary_op(&BinaryOp::Mod,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_starts_with() {
        let result = eval_binary_op(&BinaryOp::StartsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("hello".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_starts_with_false() {
        let result = eval_binary_op(&BinaryOp::StartsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_starts_with_non_string_is_null() {
        // Not an error: `1 STARTS WITH 'x'` is null. openCypher TCK String8/9/10
        // scenario 8 asks for all 36 pairings drawn from
        // `[1, 3.14, true, [], {}, null]` and expects null for every one, and
        // Neo4j 5 agrees (`(1 STARTS WITH 'x') IS NULL` -> true). This test
        // previously asserted the refusal the engine happened to implement.
        let result = eval_binary_op(&BinaryOp::StartsWith,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String("x".to_string())),
        );
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_ends_with() {
        let result = eval_binary_op(&BinaryOp::EndsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_ends_with_false() {
        let result = eval_binary_op(&BinaryOp::EndsWith,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("hello".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_ends_with_non_string_is_null() {
        // Not an error: `1 ENDS WITH 'x'` is null. openCypher TCK String8/9/10
        // scenario 8 asks for all 36 pairings drawn from
        // `[1, 3.14, true, [], {}, null]` and expects null for every one, and
        // Neo4j 5 agrees (`(1 ENDS WITH 'x') IS NULL` -> true). This test
        // previously asserted the refusal the engine happened to implement.
        let result = eval_binary_op(&BinaryOp::EndsWith,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String("x".to_string())),
        );
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_contains() {
        let result = eval_binary_op(&BinaryOp::Contains,
            Value::Property(PropertyValue::String("hello world".to_string())),
            Value::Property(PropertyValue::String("lo wo".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_contains_false() {
        let result = eval_binary_op(&BinaryOp::Contains,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::String("xyz".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_contains_non_string_is_null() {
        // Not an error: `1 CONTAINS 'x'` is null. openCypher TCK String8/9/10
        // scenario 8 asks for all 36 pairings drawn from
        // `[1, 3.14, true, [], {}, null]` and expects null for every one, and
        // Neo4j 5 agrees (`(1 CONTAINS 'x') IS NULL` -> true). This test
        // previously asserted the refusal the engine happened to implement.
        let result = eval_binary_op(&BinaryOp::Contains,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String("x".to_string())),
        );
        assert_eq!(result.unwrap(), Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_starts_with_null_left() {
        let result = eval_binary_op(&BinaryOp::StartsWith,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::String("x".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_starts_with_null_right() {
        let result = eval_binary_op(&BinaryOp::StartsWith,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::Null),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_ends_with_null() {
        let result = eval_binary_op(&BinaryOp::EndsWith,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::String("x".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_contains_null_left() {
        let result = eval_binary_op(&BinaryOp::Contains,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::String("x".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_contains_null_right() {
        let result = eval_binary_op(&BinaryOp::Contains,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::Null),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_contains_both_null() {
        let result = eval_binary_op(&BinaryOp::Contains,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::Null),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_regex_match_null() {
        let result = eval_binary_op(&BinaryOp::RegexMatch,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::String(".*".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_in_list() {
        let arr = PropertyValue::Array(vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3),
        ]);
        let result = eval_binary_op(&BinaryOp::In,
            Value::Property(PropertyValue::Integer(2)),
            Value::Property(arr),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_in_list_false() {
        let arr = PropertyValue::Array(vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
        ]);
        let result = eval_binary_op(&BinaryOp::In,
            Value::Property(PropertyValue::Integer(5)),
            Value::Property(arr),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_in_type_error() {
        let result = eval_binary_op(&BinaryOp::In,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(2)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_regex_match() {
        let result = eval_binary_op(&BinaryOp::RegexMatch,
            Value::Property(PropertyValue::String("hello123".to_string())),
            Value::Property(PropertyValue::String("^hello\\d+$".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_regex_match_false() {
        let result = eval_binary_op(&BinaryOp::RegexMatch,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::String("^\\d+$".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_regex_invalid() {
        let result = eval_binary_op(&BinaryOp::RegexMatch,
            Value::Property(PropertyValue::String("hello".to_string())),
            Value::Property(PropertyValue::String("[invalid".to_string())),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_regex_type_error() {
        let result = eval_binary_op(&BinaryOp::RegexMatch,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::String(".*".to_string())),
        );
        assert!(result.is_err());
    }

    // -- Duration arithmetic --

    #[test]
    fn test_binary_op_add_datetime_duration() {
        let dt = PropertyValue::DateTime(0); // epoch
        let dur = PropertyValue::Duration { months: 0, days: 1, seconds: 3600, nanos: 0 };
        let result = eval_binary_op(&BinaryOp::Add,
            Value::Property(dt),
            Value::Property(dur),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                // 1 day + 1 hour = 90000 seconds = 90000000 ms
                assert_eq!(ts, 90_000_000);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_binary_op_add_duration_duration() {
        let d1 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };
        let d2 = PropertyValue::Duration { months: 10, days: 20, seconds: 30, nanos: 40 };
        let result = eval_binary_op(&BinaryOp::Add,
            Value::Property(d1),
            Value::Property(d2),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, days, seconds, nanos }) => {
                assert_eq!(months, 11);
                assert_eq!(days, 22);
                assert_eq!(seconds, 33);
                assert_eq!(nanos, 44);
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_binary_op_sub_datetime_duration() {
        // Start at 1 day from epoch
        let dt = PropertyValue::DateTime(86_400_000);
        let dur = PropertyValue::Duration { months: 0, days: 1, seconds: 0, nanos: 0 };
        let result = eval_binary_op(&BinaryOp::Sub,
            Value::Property(dt),
            Value::Property(dur),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                assert_eq!(ts, 0); // back to epoch
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_binary_op_sub_datetime_datetime() {
        let dt1 = PropertyValue::DateTime(10_000_000);
        let dt2 = PropertyValue::DateTime(5_000_000);
        let result = eval_binary_op(&BinaryOp::Sub,
            Value::Property(dt1),
            Value::Property(dt2),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { seconds, .. }) => {
                assert_eq!(seconds, 5000 % 86400); // 5000s total
            }
            _ => panic!("Expected Duration"),
        }
    }

    #[test]
    fn test_binary_op_sub_duration_duration() {
        let d1 = PropertyValue::Duration { months: 10, days: 20, seconds: 30, nanos: 40 };
        let d2 = PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 };
        let result = eval_binary_op(&BinaryOp::Sub,
            Value::Property(d1),
            Value::Property(d2),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Duration { months, days, seconds, nanos }) => {
                assert_eq!(months, 9);
                assert_eq!(days, 18);
                assert_eq!(seconds, 27);
                assert_eq!(nanos, 36);
            }
            _ => panic!("Expected Duration"),
        }
    }

    // -- String concatenation --

    #[test]
    fn test_binary_op_add_strings() {
        let result = eval_binary_op(&BinaryOp::Add,
            Value::Property(PropertyValue::String("hello ".to_string())),
            Value::Property(PropertyValue::String("world".to_string())),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("hello world".to_string())));
    }

    // -- Numeric cross-type operations --

    #[test]
    fn test_binary_op_add_int_float() {
        let result = eval_binary_op(&BinaryOp::Add,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Float(2.5)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_sub_float_int() {
        let result = eval_binary_op(&BinaryOp::Sub,
            Value::Property(PropertyValue::Float(5.0)),
            Value::Property(PropertyValue::Integer(2)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 3.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_mul_int_float() {
        let result = eval_binary_op(&BinaryOp::Mul,
            Value::Property(PropertyValue::Integer(3)),
            Value::Property(PropertyValue::Float(2.0)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 6.0).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_binary_op_div_int_zero() {
        let result = eval_binary_op(&BinaryOp::Div,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Integer(0)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_div_int_float() {
        let result = eval_binary_op(&BinaryOp::Div,
            Value::Property(PropertyValue::Integer(10)),
            Value::Property(PropertyValue::Float(4.0)),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - 2.5).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    // -- Eq/Ne with Null --

    #[test]
    fn test_binary_op_eq_null() {
        let result = eval_binary_op(&BinaryOp::Eq,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::Null),
        ).unwrap();
        // Cypher: null = null is *unknown*, not true. This is why `IS NULL` exists as a
        // separate operator -- equality can never confirm nullness.
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_binary_op_ne_null_vs_int() {
        let result = eval_binary_op(&BinaryOp::Ne,
            Value::Property(PropertyValue::Null),
            Value::Property(PropertyValue::Integer(1)),
        ).unwrap();
        // Unknown, not true -- a WHERE must exclude the row rather than keep it.
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    // -- And/Or type errors --

    #[test]
    fn test_binary_op_and_type_error() {
        let result = eval_binary_op(&BinaryOp::And,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Boolean(true)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_or_type_error() {
        let result = eval_binary_op(&BinaryOp::Or,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Boolean(false)),
        );
        assert!(result.is_err());
    }

    // -- And/Or valid --

    #[test]
    fn test_binary_op_and_true() {
        let result = eval_binary_op(&BinaryOp::And,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Boolean(true)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_and_false() {
        let result = eval_binary_op(&BinaryOp::And,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Boolean(false)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_binary_op_or_true() {
        let result = eval_binary_op(&BinaryOp::Or,
            Value::Property(PropertyValue::Boolean(false)),
            Value::Property(PropertyValue::Boolean(true)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_or_false() {
        let result = eval_binary_op(&BinaryOp::Or,
            Value::Property(PropertyValue::Boolean(false)),
            Value::Property(PropertyValue::Boolean(false)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    // -- Add type errors --

    #[test]
    fn test_binary_op_add_type_error() {
        let result = eval_binary_op(&BinaryOp::Add,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_sub_type_error() {
        let result = eval_binary_op(&BinaryOp::Sub,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_mul_type_error() {
        let result = eval_binary_op(&BinaryOp::Mul,
            Value::Property(PropertyValue::String("a".to_string())),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_div_type_error() {
        let result = eval_binary_op(&BinaryOp::Div,
            Value::Property(PropertyValue::Boolean(true)),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    // -- Non-property Value type error in binary op --

    #[test]
    fn test_binary_op_non_property_left() {
        use crate::graph::types::NodeId;
        let result = eval_binary_op(&BinaryOp::Add,
            Value::NodeRef(NodeId::new(1)),
            Value::Property(PropertyValue::Integer(1)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_op_non_property_right() {
        use crate::graph::types::NodeId;
        let result = eval_binary_op(&BinaryOp::Add,
            Value::Property(PropertyValue::Integer(1)),
            Value::NodeRef(NodeId::new(1)),
        );
        assert!(result.is_err());
    }

    // -- Null handling in binary op --

    #[test]
    fn test_binary_op_null_value_left() {
        let result = eval_binary_op(&BinaryOp::Eq,
            Value::Null,
            Value::Property(PropertyValue::Integer(1)),
        ).unwrap();
        // Unknown. Filters coerce this to "exclude", so the observable WHERE behaviour is
        // unchanged from the old `false`, but NOT(unknown) is unknown -- not true.
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    // -- Comparison operators --

    #[test]
    fn test_binary_op_lt() {
        let result = eval_binary_op(&BinaryOp::Lt,
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(2)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_le_equal() {
        let result = eval_binary_op(&BinaryOp::Le,
            Value::Property(PropertyValue::Integer(2)),
            Value::Property(PropertyValue::Integer(2)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_gt() {
        let result = eval_binary_op(&BinaryOp::Gt,
            Value::Property(PropertyValue::Integer(3)),
            Value::Property(PropertyValue::Integer(2)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_ge_equal() {
        let result = eval_binary_op(&BinaryOp::Ge,
            Value::Property(PropertyValue::Integer(2)),
            Value::Property(PropertyValue::Integer(2)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_binary_op_lt_false() {
        let result = eval_binary_op(&BinaryOp::Lt,
            Value::Property(PropertyValue::Integer(5)),
            Value::Property(PropertyValue::Integer(2)),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    // ========== eval_unary_op tests ==========

    #[test]
    fn test_unary_op_is_null_true() {
        let result = eval_unary_op(&UnaryOp::IsNull, Value::Null).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_is_null_property_null() {
        let result = eval_unary_op(&UnaryOp::IsNull, Value::Property(PropertyValue::Null)).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_is_null_false() {
        let result = eval_unary_op(&UnaryOp::IsNull, Value::Property(PropertyValue::Integer(1))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_unary_op_is_not_null_true() {
        let result = eval_unary_op(&UnaryOp::IsNotNull, Value::Property(PropertyValue::Integer(1))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_is_not_null_false() {
        let result = eval_unary_op(&UnaryOp::IsNotNull, Value::Null).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_unary_op_not_true() {
        let result = eval_unary_op(&UnaryOp::Not, Value::Property(PropertyValue::Boolean(true))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_unary_op_not_false() {
        let result = eval_unary_op(&UnaryOp::Not, Value::Property(PropertyValue::Boolean(false))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_unary_op_not_type_error() {
        let result = eval_unary_op(&UnaryOp::Not, Value::Property(PropertyValue::Integer(1)));
        assert!(result.is_err());
    }

    #[test]
    fn test_unary_op_not_null() {
        let result = eval_unary_op(&UnaryOp::Not, Value::Property(PropertyValue::Null)).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_unary_op_not_value_null() {
        let result = eval_unary_op(&UnaryOp::Not, Value::Null).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Null));
    }

    #[test]
    fn test_unary_op_minus_int() {
        let result = eval_unary_op(&UnaryOp::Minus, Value::Property(PropertyValue::Integer(42))).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(-42)));
    }

    #[test]
    fn test_unary_op_minus_float() {
        let result = eval_unary_op(&UnaryOp::Minus, Value::Property(PropertyValue::Float(3.14))).unwrap();
        match result {
            Value::Property(PropertyValue::Float(f)) => assert!((f - (-3.14)).abs() < 1e-10),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_unary_op_minus_type_error() {
        let result = eval_unary_op(&UnaryOp::Minus, Value::Property(PropertyValue::String("x".to_string())));
        assert!(result.is_err());
    }

    // ========== eval_index + eval_list_slice tests ==========

    #[test]
    fn test_eval_index_array_positive() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result = eval_index(arr, Value::Property(PropertyValue::Integer(1)), &GraphStore::new()).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(20)));
    }

    #[test]
    fn test_eval_index_array_negative() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result = eval_index(arr, Value::Property(PropertyValue::Integer(-1)), &GraphStore::new()).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(30)));
    }

    #[test]
    fn test_eval_index_array_out_of_bounds() {
        let arr = Value::Property(PropertyValue::Array(vec![PropertyValue::Integer(10)]));
        let result = eval_index(arr, Value::Property(PropertyValue::Integer(5)), &GraphStore::new()).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_eval_index_map() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), PropertyValue::Integer(42));
        let result = eval_index(
            Value::Property(PropertyValue::Map(map)),
            Value::Property(PropertyValue::String("key".to_string())),
            &GraphStore::new(),
        ).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_index_map_missing_key() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), PropertyValue::Integer(42));
        let result = eval_index(
            Value::Property(PropertyValue::Map(map)),
            Value::Property(PropertyValue::String("missing".to_string())),
            &GraphStore::new(),
        ).unwrap();
        assert_eq!(result, Value::Null);
    }

    /// Indexing a non-collection is a **type error**, not null (#789).
    ///
    /// This asserted `Value::Null`, which was the behaviour and is not
    /// Cypher's: `List1 [6]` expects `TypeError: InvalidArgumentType` for
    /// `true[0]`, `123[0]`, `4.7[0]` and `'1'[0]`. The test encoded the defect,
    /// checked against the TCK rather than against the new code.
    ///
    /// The distinction it destroyed is the useful one: a list with no element
    /// 5 is a different thing from a value that was never a list. The
    /// neighbouring `test_eval_index_map_missing_key` still asserts null, and
    /// still passes, because a missing key genuinely is null.
    #[test]
    fn test_eval_index_non_collection() {
        let err = eval_index(
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(0)),
            &GraphStore::new(),
        )
        .expect_err("indexing an integer is a type error");
        assert!(
            format!("{err:?}").contains("not a list or a map"),
            "the message should name the problem: {err:?}"
        );
    }

    #[test]
    fn test_eval_list_slice_range() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
            PropertyValue::Integer(40),
            PropertyValue::Integer(50),
        ]));
        let result = eval_list_slice(
            arr,
            Some(Value::Property(PropertyValue::Integer(1))),
            Some(Value::Property(PropertyValue::Integer(3))),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(20));
                assert_eq!(arr[1].as_integer(), Some(30));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_negative_start() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result = eval_list_slice(
            arr,
            Some(Value::Property(PropertyValue::Integer(-2))),
            None,
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(20));
                assert_eq!(arr[1].as_integer(), Some(30));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_from_start() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
            PropertyValue::Integer(20),
            PropertyValue::Integer(30),
        ]));
        let result = eval_list_slice(
            arr,
            None,
            Some(Value::Property(PropertyValue::Integer(2))),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_integer(), Some(10));
                assert_eq!(arr[1].as_integer(), Some(20));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_empty_result() {
        let arr = Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(10),
        ]));
        let result = eval_list_slice(
            arr,
            Some(Value::Property(PropertyValue::Integer(3))),
            Some(Value::Property(PropertyValue::Integer(5))),
        ).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => assert!(arr.is_empty()),
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_eval_list_slice_non_array() {
        let result = eval_list_slice(
            Value::Property(PropertyValue::Integer(1)),
            None,
            None,
        ).unwrap();
        assert_eq!(result, Value::Null);
    }

    // -- id/labels/type/keys/exists meta functions --

    #[test]
    fn test_eval_function_id_noderef() {
        use crate::graph::types::NodeId;
        let result = eval_function("id", &[Value::NodeRef(NodeId::new(42))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(42)));
    }

    #[test]
    fn test_eval_function_id_edgeref() {
        use crate::graph::types::{NodeId, EdgeId, EdgeType};
        let result = eval_function("id", &[Value::EdgeRef(EdgeId::new(7), NodeId::new(1), NodeId::new(2), EdgeType::new("R"))], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::Integer(7)));
    }

    #[test]
    fn test_eval_function_id_type_error() {
        let result = eval_function("id", &[Value::Property(PropertyValue::Integer(1))], None);
        assert!(result.is_err());
    }

    #[test]
    fn test_labels_with_noderef() {
        use crate::graph::types::NodeId;

        let mut store = GraphStore::new();
        let nid = store.create_node("Person");
        store.get_node_mut(nid).unwrap().add_label(crate::graph::types::Label::new("Employee"));

        let result = eval_function("labels", &[Value::NodeRef(nid)], Some(&store)).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let labels: Vec<String> = arr.iter().map(|v| v.as_string().unwrap().to_string()).collect();
                assert!(labels.contains(&"Person".to_string()));
                assert!(labels.contains(&"Employee".to_string()));
            }
            _ => panic!("Expected array from labels()"),
        }
    }

    #[test]
    fn test_type_with_edgeref() {
        use crate::graph::types::{NodeId, EdgeId, EdgeType};

        let result = eval_function("type", &[
            Value::EdgeRef(EdgeId::new(1), NodeId::new(10), NodeId::new(20), EdgeType::new("KNOWS"))
        ], None).unwrap();
        assert_eq!(result, Value::Property(PropertyValue::String("KNOWS".to_string())));
    }

    #[test]
    fn test_keys_with_noderef() {
        use crate::graph::types::NodeId;

        let mut store = GraphStore::new();
        let nid = store.create_node("Person");
        store.get_node_mut(nid).unwrap().set_property("name", "Alice");
        store.get_node_mut(nid).unwrap().set_property("age", 30i64);

        let result = eval_function("keys", &[Value::NodeRef(nid)], Some(&store)).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let keys: Vec<String> = arr.iter().map(|v| v.as_string().unwrap().to_string()).collect();
                assert!(keys.contains(&"name".to_string()));
                assert!(keys.contains(&"age".to_string()));
            }
            _ => panic!("Expected array from keys()"),
        }
    }

    #[test]
    fn test_keys_with_edgeref() {
        use crate::graph::types::{NodeId, EdgeId, EdgeType};

        let mut store = GraphStore::new();
        let n1 = store.create_node("A");
        let n2 = store.create_node("B");
        let eid = store.create_edge(n1, n2, "REL").unwrap();
        store.set_edge_property_sparse(eid, "weight", PropertyValue::Float(1.5));

        let edge = store.get_edge(eid).unwrap();
        let result = eval_function("keys", &[
            Value::EdgeRef(eid, edge.source, edge.target, edge.edge_type.clone())
        ], Some(&store)).unwrap();
        match result {
            Value::Property(PropertyValue::Array(arr)) => {
                let keys: Vec<String> = arr.iter().map(|v| v.as_string().unwrap().to_string()).collect();
                assert!(keys.contains(&"weight".to_string()));
            }
            _ => panic!("Expected array from keys()"),
        }
    }

    // ---- ExpandIntoOperator tests (TDD) ----

    #[test]
    fn test_expand_into_basic() {
        use crate::graph::types::NodeId;

        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let _eid = store.create_edge(n1, n2, "KNOWS").unwrap();

        // Create input that provides both source and target
        let mut records = Vec::new();
        let mut r = Record::new();
        r.bind("a".to_string(), Value::NodeRef(n1));
        r.bind("b".to_string(), Value::NodeRef(n2));
        records.push(r);

        // Use CartesianProductOperator isn't suitable here, so we build a simple mock
        // by using a NodeByIdOperator for `a` and manually creating input records.
        // Instead, let's just test with a WithBarrier-like approach: produce a batch
        // Actually, simplest: use a custom input that yields our records
        let input = Box::new(StaticInputOperator { records, index: 0 });

        let mut op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            None,
        );

        let result = op.next(&store).unwrap();
        assert!(result.is_some());

        // No more records
        let result2 = op.next(&store).unwrap();
        assert!(result2.is_none());
    }

    #[test]
    fn test_expand_into_no_edge() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        // No edge between n1 and n2

        let mut records = Vec::new();
        let mut r = Record::new();
        r.bind("a".to_string(), Value::NodeRef(n1));
        r.bind("b".to_string(), Value::NodeRef(n2));
        records.push(r);

        let input = Box::new(StaticInputOperator { records, index: 0 });
        let mut op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            None,
        );

        let result = op.next(&store).unwrap();
        assert!(result.is_none()); // Record filtered out
    }

    #[test]
    fn test_expand_into_with_edge_binding() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let eid = store.create_edge(n1, n2, "KNOWS").unwrap();

        let mut records = Vec::new();
        let mut r = Record::new();
        r.bind("a".to_string(), Value::NodeRef(n1));
        r.bind("b".to_string(), Value::NodeRef(n2));
        records.push(r);

        let input = Box::new(StaticInputOperator { records, index: 0 });
        let mut op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            Some("r".to_string()),
        );

        let result = op.next(&store).unwrap().unwrap();
        // Edge should be bound
        let edge_val = result.get("r").unwrap();
        assert_eq!(edge_val.edge_id(), Some(eid));
    }

    #[test]
    fn test_expand_into_describe() {
        let input = Box::new(StaticInputOperator { records: Vec::new(), index: 0 });
        let op = ExpandIntoOperator::new(
            input,
            "a".to_string(),
            "b".to_string(),
            Some("KNOWS".to_string()),
            None,
        );
        let desc = op.describe();
        assert_eq!(desc.name, "ExpandInto");
        assert!(desc.details.contains("KNOWS"));
    }

    // ---- NodeByIdOperator tests (TDD) ----

    #[test]
    fn test_node_by_id_operator() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let n3 = store.create_node("Company");

        let mut op = NodeByIdOperator::new(vec![n1, n3], "n".to_string());

        let r1 = op.next(&store).unwrap().unwrap();
        assert_eq!(r1.get("n").unwrap().node_id(), Some(n1));

        let r2 = op.next(&store).unwrap().unwrap();
        assert_eq!(r2.get("n").unwrap().node_id(), Some(n3));

        let r3 = op.next(&store).unwrap();
        assert!(r3.is_none());
    }

    #[test]
    fn test_node_by_id_operator_deleted_node() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        store.delete_node("default", n1).unwrap();

        let mut op = NodeByIdOperator::new(vec![n1, n2], "n".to_string());

        // n1 is deleted, should skip it
        let r1 = op.next(&store).unwrap().unwrap();
        assert_eq!(r1.get("n").unwrap().node_id(), Some(n2));

        let r2 = op.next(&store).unwrap();
        assert!(r2.is_none());
    }

    #[test]
    fn test_node_by_id_operator_reset() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");

        let mut op = NodeByIdOperator::new(vec![n1], "n".to_string());
        let _ = op.next(&store).unwrap();
        assert!(op.next(&store).unwrap().is_none());

        op.reset();
        let r = op.next(&store).unwrap();
        assert!(r.is_some());
    }

    /// Helper: a simple operator that yields pre-built records (for testing downstream operators)
    struct StaticInputOperator {
        records: Vec<Record>,
        index: usize,
    }

    impl PhysicalOperator for StaticInputOperator {
        fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
            if self.index < self.records.len() {
                let r = self.records[self.index].clone();
                self.index += 1;
                Ok(Some(r))
            } else {
                Ok(None)
            }
        }

        fn next_batch(&mut self, store: &GraphStore, batch_size: usize) -> ExecutionResult<Option<RecordBatch>> {
            let mut records = Vec::new();
            for _ in 0..batch_size {
                match self.next(store)? {
                    Some(r) => records.push(r),
                    None => break,
                }
            }
            if records.is_empty() { Ok(None) } else { Ok(Some(RecordBatch { records, columns: Vec::new() })) }
        }

        fn reset(&mut self) {
            self.index = 0;
        }

        fn describe(&self) -> OperatorDescription {
            OperatorDescription {
                name: "StaticInput".to_string(),
                details: format!("{} records", self.records.len()),
                children: Vec::new(),
            }
        }
    }

    // ========== Three-valued logic tests (Cypher AND/OR with nulls) ==========

    fn prop(v: PropertyValue) -> Value { Value::Property(v) }

    #[test]
    fn test_and_false_short_circuits_null() {
        // false AND null → false (short-circuit). Previously errored with "AND requires booleans".
        let r = eval_binary_op(&BinaryOp::And, prop(PropertyValue::Boolean(false)), prop(PropertyValue::Null)).unwrap();
        assert_eq!(r, prop(PropertyValue::Boolean(false)));
        let r = eval_binary_op(&BinaryOp::And, prop(PropertyValue::Null), prop(PropertyValue::Boolean(false))).unwrap();
        assert_eq!(r, prop(PropertyValue::Boolean(false)));
    }

    #[test]
    fn test_and_true_with_null_is_null() {
        let r = eval_binary_op(&BinaryOp::And, prop(PropertyValue::Boolean(true)), prop(PropertyValue::Null)).unwrap();
        assert_eq!(r, prop(PropertyValue::Null));
        let r = eval_binary_op(&BinaryOp::And, prop(PropertyValue::Null), prop(PropertyValue::Boolean(true))).unwrap();
        assert_eq!(r, prop(PropertyValue::Null));
    }

    #[test]
    fn test_and_null_null_is_null() {
        let r = eval_binary_op(&BinaryOp::And, prop(PropertyValue::Null), prop(PropertyValue::Null)).unwrap();
        assert_eq!(r, prop(PropertyValue::Null));
    }

    #[test]
    fn test_or_true_short_circuits_null() {
        let r = eval_binary_op(&BinaryOp::Or, prop(PropertyValue::Boolean(true)), prop(PropertyValue::Null)).unwrap();
        assert_eq!(r, prop(PropertyValue::Boolean(true)));
        let r = eval_binary_op(&BinaryOp::Or, prop(PropertyValue::Null), prop(PropertyValue::Boolean(true))).unwrap();
        assert_eq!(r, prop(PropertyValue::Boolean(true)));
    }

    #[test]
    fn test_or_false_with_null_is_null() {
        let r = eval_binary_op(&BinaryOp::Or, prop(PropertyValue::Boolean(false)), prop(PropertyValue::Null)).unwrap();
        assert_eq!(r, prop(PropertyValue::Null));
        let r = eval_binary_op(&BinaryOp::Or, prop(PropertyValue::Null), prop(PropertyValue::Boolean(false))).unwrap();
        assert_eq!(r, prop(PropertyValue::Null));
    }

    #[test]
    fn test_is_not_null_and_contains_on_null_property() {
        // Regression: WHERE p.name IS NOT NULL AND p.name CONTAINS 'x'
        // When p.name is NULL: IS NOT NULL → false, CONTAINS → null.
        // false AND null must short-circuit to false, not error.
        let is_not_null = eval_unary_op(&UnaryOp::IsNotNull, Value::Null).unwrap();
        let contains_on_null = Value::Property(PropertyValue::Null); // CONTAINS on null returns null
        let r = eval_binary_op(&BinaryOp::And, is_not_null, contains_on_null).unwrap();
        assert_eq!(r, prop(PropertyValue::Boolean(false)));
    }
}