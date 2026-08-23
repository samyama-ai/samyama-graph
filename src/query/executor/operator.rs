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

/// Shared binary operator evaluation used by Project, Aggregate, and Sort operators
fn eval_binary_op(op: &BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
    // Node/edge identity comparison (Cypher: n1 = n2, n1 <> n2)
    if matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
        if let (Some(lid), Some(rid)) = (node_id_of(&left), node_id_of(&right)) {
            let eq = lid == rid;
            return Ok(Value::Property(PropertyValue::Boolean(
                if matches!(op, BinaryOp::Eq) { eq } else { !eq }
            )));
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
        BinaryOp::Eq => PropertyValue::Boolean(left_prop == right_prop),
        BinaryOp::Ne => PropertyValue::Boolean(left_prop != right_prop),
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
            // Duration + Duration
            (PropertyValue::Duration { months: m1, days: d1, seconds: s1, nanos: n1 },
             PropertyValue::Duration { months: m2, days: d2, seconds: s2, nanos: n2 }) => {
                PropertyValue::Duration { months: m1 + m2, days: d1 + d2, seconds: s1 + s2, nanos: n1 + n2 }
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
            // DateTime - DateTime = Duration
            (PropertyValue::DateTime(a), PropertyValue::DateTime(b)) => {
                let diff_ms = a - b;
                let total_seconds = diff_ms / 1000;
                PropertyValue::Duration { months: 0, days: total_seconds / 86400, seconds: total_seconds % 86400, nanos: ((diff_ms % 1000) * 1_000_000) as i32 }
            }
            // Duration - Duration
            (PropertyValue::Duration { months: m1, days: d1, seconds: s1, nanos: n1 },
             PropertyValue::Duration { months: m2, days: d2, seconds: s2, nanos: n2 }) => {
                PropertyValue::Duration { months: m1 - m2, days: d1 - d2, seconds: s1 - s2, nanos: n1 - n2 }
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Sub requires numeric operands".to_string())),
        },
        BinaryOp::Mul => match (&left_prop, &right_prop) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => PropertyValue::Integer(l * r),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => PropertyValue::Float(l * r),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => PropertyValue::Float(*l as f64 * r),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => PropertyValue::Float(l * *r as f64),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => PropertyValue::Null,
            _ => return Err(ExecutionError::TypeError("Mul requires numeric operands".to_string())),
        },
        BinaryOp::Div => match (&left_prop, &right_prop) {
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
        _ => Ok(Value::Null),
    }
}

fn eval_list_slice(collection: Value, start: Option<Value>, end: Option<Value>) -> ExecutionResult<Value> {
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
    Ok(Value::Property(val.resolve_property(property, store)))
}

/// Standalone expression evaluator usable from any operator
fn eval_expression(expr: &Expression, record: &Record, store: &GraphStore) -> ExecutionResult<Value> {
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
        Expression::ExistsSubquery { pattern, where_clause } => {
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
    let items = match list_val {
        Value::Property(ref p) if p.as_list_items().is_some() => p.as_list_items().unwrap(),
        _ => return Ok(Value::Property(PropertyValue::Array(vec![]))),
    };

    let mut result = Vec::new();
    for item in items {
        let mut inner_record = record.clone();
        inner_record.bind(variable.to_string(), Value::Property(item));

        // Apply filter
        if let Some(f) = filter {
            let cond = eval_expression(f, &inner_record, store)?;
            if !matches!(cond, Value::Property(PropertyValue::Boolean(true))) {
                continue;
            }
        }

        // Apply map expression
        let mapped = eval_expression(map_expr, &inner_record, store)?;
        match mapped {
            Value::Property(pv) => result.push(pv),
            _ => result.push(PropertyValue::Null),
        }
    }

    Ok(Value::Property(PropertyValue::Array(result)))
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

    let mut true_count = 0usize;
    for item in &items {
        let mut inner_record = record.clone_with_capacity(1);
        inner_record.bind(variable.to_string(), item.clone());
        let result = eval_expression(predicate, &inner_record, store)?;
        if matches!(result, Value::Property(PropertyValue::Boolean(true))) {
            true_count += 1;
        }
    }

    let result = match name {
        "all" => true_count == items.len(),
        "any" => true_count > 0,
        "none" => true_count == 0,
        "single" => true_count == 1,
        _ => false,
    };
    Ok(Value::Property(PropertyValue::Boolean(result)))
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
    let items = match list_val {
        Value::Property(ref p) if p.as_list_items().is_some() => p.as_list_items().unwrap(),
        _ => return Ok(init_val),
    };

    let mut acc = init_val;
    for item in items {
        let mut inner_record = record.clone();
        inner_record.bind(accumulator.to_string(), acc);
        inner_record.bind(variable.to_string(), Value::Property(item));
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
                Value::Property(PropertyValue::Duration { months, days, seconds, nanos }) => {
                    format!("P{}M{}DT{}S", months, days, seconds)
                }
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
                Value::Property(PropertyValue::String(s)) => Ok(Value::Property(
                    s.parse::<i64>()
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
            let ts = chrono::Utc::now().timestamp_millis();
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
        // subject is null, following Cypher's three-valued logic.
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
                        "a label test requires a node".to_string(),
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
        "date" => {
            if args.is_empty() {
                // date() — current date as DateTime
                let now = chrono::Utc::now().timestamp_millis();
                Ok(Value::Property(PropertyValue::DateTime(now)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        // Parse ISO date string
                        if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            let millis = dt.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Cannot parse date: {}", s)))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        let year = map.get("year").and_then(|v| v.as_integer()).unwrap_or(1970) as i32;
                        let month = map.get("month").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let day = map.get("day").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
                            let millis = dt.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Invalid date: {}-{}-{}", year, month, day)))
                        }
                    }
                    _ => Err(ExecutionError::TypeError("date() requires string or map argument".to_string())),
                }
            }
        }
        "datetime" => {
            if args.is_empty() {
                let now = chrono::Utc::now().timestamp_millis();
                Ok(Value::Property(PropertyValue::DateTime(now)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                            Ok(Value::Property(PropertyValue::DateTime(dt.timestamp_millis())))
                        } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                            Ok(Value::Property(PropertyValue::DateTime(dt.and_utc().timestamp_millis())))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Cannot parse datetime: {}", s)))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        use chrono::TimeZone;

                        // An epoch, which is what a machine caller has (Axiom 4).
                        // Handled before the calendar components because it is a
                        // complete specification on its own -- and because
                        // without it the map fell through to the defaults below
                        // and returned 1970-01-01 *silently*, which reads as a
                        // plausible date rather than a failure (#595).
                        if let Some(millis) = map.get("epochMillis").and_then(|v| v.as_integer()) {
                            return Ok(Value::Property(PropertyValue::DateTime(millis)));
                        }
                        if let Some(seconds) = map.get("epochSeconds").and_then(|v| v.as_integer()) {
                            return Ok(Value::Property(PropertyValue::DateTime(seconds * 1000)));
                        }

                        // A map naming none of the understood keys is a mistake,
                        // not a request for the epoch. Answering 1970-01-01 for
                        // it is how the missing `epochMillis` arm stayed
                        // invisible.
                        const KNOWN: [&str; 8] = [
                            "year", "month", "day", "hour", "minute", "second",
                            "epochMillis", "epochSeconds",
                        ];
                        if !map.keys().any(|k| KNOWN.contains(&k.as_str())) {
                            let mut given: Vec<String> = map.keys().cloned().collect();
                            given.sort();
                            return Err(ExecutionError::RuntimeError(format!(
                                "datetime() understands none of the keys given ({}); expected one of {}",
                                given.join(", "),
                                KNOWN.join(", ")
                            )));
                        }

                        let year = map.get("year").and_then(|v| v.as_integer()).unwrap_or(1970) as i32;
                        let month = map.get("month").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let day = map.get("day").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let hour = map.get("hour").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        let minute = map.get("minute").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        let second = map.get("second").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        if let Some(dt) = chrono::Utc.with_ymd_and_hms(year, month, day, hour, minute, second).single() {
                            Ok(Value::Property(PropertyValue::DateTime(dt.timestamp_millis())))
                        } else {
                            Err(ExecutionError::RuntimeError(format!(
                                "Invalid datetime components: year={}, month={}, day={}, hour={}, minute={}, second={}",
                                year, month, day, hour, minute, second
                            )))
                        }
                    }
                    _ => Err(ExecutionError::TypeError("datetime() requires string or map argument".to_string())),
                }
            }
        }
        // CY-28: time() — time of day, stored as millis from epoch midnight
        "time" => {
            if args.is_empty() {
                use chrono::Timelike;
                let now = chrono::Utc::now();
                let millis = (now.hour() as i64 * 3600 + now.minute() as i64 * 60 + now.second() as i64) * 1000
                    + now.timestamp_subsec_millis() as i64;
                Ok(Value::Property(PropertyValue::DateTime(millis)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        // Parse HH:MM:SS or HH:MM:SS.sss (ignore timezone for storage)
                        let time_str = s.split('+').next().unwrap_or(s).split('-').next().unwrap_or(s);
                        if let Ok(t) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S") {
                            use chrono::Timelike;
                            let millis = (t.hour() as i64 * 3600 + t.minute() as i64 * 60 + t.second() as i64) * 1000;
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else if let Ok(t) = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f") {
                            use chrono::Timelike;
                            let millis = (t.hour() as i64 * 3600 + t.minute() as i64 * 60 + t.second() as i64) * 1000
                                + (t.nanosecond() / 1_000_000) as i64;
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Cannot parse time: {}. Expected HH:MM:SS", s)))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        let hour = map.get("hour").and_then(|v| v.as_integer()).unwrap_or(0);
                        let minute = map.get("minute").and_then(|v| v.as_integer()).unwrap_or(0);
                        let second = map.get("second").and_then(|v| v.as_integer()).unwrap_or(0);
                        let millis = (hour * 3600 + minute * 60 + second) * 1000;
                        Ok(Value::Property(PropertyValue::DateTime(millis)))
                    }
                    _ => Err(ExecutionError::TypeError("time() requires string or map argument".to_string())),
                }
            }
        }
        // CY-28: localtime() — same as time() but explicitly no timezone
        "localtime" => {
            if args.is_empty() {
                use chrono::Timelike;
                let now = chrono::Utc::now();
                let millis = (now.hour() as i64 * 3600 + now.minute() as i64 * 60 + now.second() as i64) * 1000
                    + now.timestamp_subsec_millis() as i64;
                Ok(Value::Property(PropertyValue::DateTime(millis)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        if let Ok(t) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                            use chrono::Timelike;
                            let millis = (t.hour() as i64 * 3600 + t.minute() as i64 * 60 + t.second() as i64) * 1000;
                            Ok(Value::Property(PropertyValue::DateTime(millis)))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Cannot parse localtime: {}. Expected HH:MM:SS", s)))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        let hour = map.get("hour").and_then(|v| v.as_integer()).unwrap_or(0);
                        let minute = map.get("minute").and_then(|v| v.as_integer()).unwrap_or(0);
                        let second = map.get("second").and_then(|v| v.as_integer()).unwrap_or(0);
                        let millis = (hour * 3600 + minute * 60 + second) * 1000;
                        Ok(Value::Property(PropertyValue::DateTime(millis)))
                    }
                    _ => Err(ExecutionError::TypeError("localtime() requires string or map argument".to_string())),
                }
            }
        }
        // CY-28: localdatetime() — datetime without timezone
        "localdatetime" => {
            if args.is_empty() {
                let now = chrono::Utc::now().timestamp_millis();
                Ok(Value::Property(PropertyValue::DateTime(now)))
            } else {
                match &args[0] {
                    Value::Property(PropertyValue::String(s)) => {
                        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                            Ok(Value::Property(PropertyValue::DateTime(dt.and_utc().timestamp_millis())))
                        } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                            Ok(Value::Property(PropertyValue::DateTime(dt.and_utc().timestamp_millis())))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Cannot parse localdatetime: {}. Expected YYYY-MM-DDTHH:MM:SS", s)))
                        }
                    }
                    Value::Property(PropertyValue::Map(map)) => {
                        use chrono::TimeZone;
                        let year = map.get("year").and_then(|v| v.as_integer()).unwrap_or(1970) as i32;
                        let month = map.get("month").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let day = map.get("day").and_then(|v| v.as_integer()).unwrap_or(1) as u32;
                        let hour = map.get("hour").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        let minute = map.get("minute").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        let second = map.get("second").and_then(|v| v.as_integer()).unwrap_or(0) as u32;
                        if let Some(dt) = chrono::Utc.with_ymd_and_hms(year, month, day, hour, minute, second).single() {
                            Ok(Value::Property(PropertyValue::DateTime(dt.timestamp_millis())))
                        } else {
                            Err(ExecutionError::RuntimeError(format!("Invalid localdatetime components")))
                        }
                    }
                    _ => Err(ExecutionError::TypeError("localdatetime() requires string or map argument".to_string())),
                }
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
                    let months = map.get("months").and_then(|v| v.as_integer()).unwrap_or(0);
                    let days = map.get("days").and_then(|v| v.as_integer()).unwrap_or(0);
                    let hours = map.get("hours").and_then(|v| v.as_integer()).unwrap_or(0);
                    let minutes = map.get("minutes").and_then(|v| v.as_integer()).unwrap_or(0);
                    let seconds = map.get("seconds").and_then(|v| v.as_integer()).unwrap_or(0);
                    let years = map.get("years").and_then(|v| v.as_integer()).unwrap_or(0);
                    let total_months = years * 12 + months;
                    let total_seconds = hours * 3600 + minutes * 60 + seconds;
                    Ok(Value::Property(PropertyValue::Duration {
                        months: total_months,
                        days,
                        seconds: total_seconds,
                        nanos: 0,
                    }))
                }
                _ => Err(ExecutionError::TypeError("duration() requires string or map argument".to_string())),
            }
        }
        // duration component accessors
        "duration_between" | "duration.between" => {
            if args.len() < 2 { return Err(ExecutionError::RuntimeError("duration.between() requires 2 arguments".to_string())); }
            match (&args[0], &args[1]) {
                (Value::Property(PropertyValue::DateTime(a)), Value::Property(PropertyValue::DateTime(b))) => {
                    let diff_ms = b - a;
                    let total_seconds = diff_ms / 1000;
                    let remaining_days = total_seconds / 86400;
                    Ok(Value::Property(PropertyValue::Duration {
                        months: 0,
                        days: remaining_days,
                        seconds: total_seconds % 86400,
                        nanos: ((diff_ms % 1000) * 1_000_000) as i32,
                    }))
                }
                _ => Err(ExecutionError::TypeError("duration.between() requires two datetime arguments".to_string())),
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
                    _ => Err(ExecutionError::TypeError(format!("Cannot convert '{}' to boolean", s))),
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

/// Parse ISO 8601 duration string (e.g. "P1Y2M3DT4H5M6S")
fn parse_iso_duration(s: &str) -> ExecutionResult<Value> {
    let s = s.trim();
    if !s.starts_with('P') && !s.starts_with('p') {
        return Err(ExecutionError::RuntimeError(format!("Invalid duration format: {}", s)));
    }
    let rest = &s[1..];
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut seconds: i64 = 0;
    let mut nanos: i32 = 0;
    let _ = nanos; // suppress warning

    let (date_part, time_part) = if let Some(idx) = rest.find(|c: char| c == 'T' || c == 't') {
        (&rest[..idx], &rest[idx + 1..])
    } else {
        (rest, "")
    };

    // Parse date part: Y, M, D
    let mut num_buf = String::new();
    for ch in date_part.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num_buf.push(ch);
        } else {
            let val: f64 = num_buf.parse().unwrap_or(0.0);
            num_buf.clear();
            match ch {
                'Y' | 'y' => months += (val * 12.0) as i64,
                'M' | 'm' => months += val as i64,
                'W' | 'w' => days += (val * 7.0) as i64,
                'D' | 'd' => days += val as i64,
                _ => {}
            }
        }
    }

    // Parse time part: H, M, S
    num_buf.clear();
    for ch in time_part.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            num_buf.push(ch);
        } else {
            let val: f64 = num_buf.parse().unwrap_or(0.0);
            num_buf.clear();
            match ch {
                'H' | 'h' => seconds += (val * 3600.0) as i64,
                'M' | 'm' => seconds += (val * 60.0) as i64,
                'S' | 's' => {
                    seconds += val as i64;
                    nanos = ((val - val.floor()) * 1_000_000_000.0) as i32;
                }
                _ => {}
            }
        }
    }

    Ok(Value::Property(PropertyValue::Duration { months, days, seconds, nanos }))
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
        //   3. Multi-label → union with HashSet for dedup
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
            // Multi-label: union via HashSet. Stop early if early_limit is set.
            let cap = self.early_limit.unwrap_or(usize::MAX);
            let mut node_set: HashSet<NodeId> = HashSet::new();
            'outer: for label in &self.labels {
                for nid in store.node_ids_by_label(label, None) {
                    node_set.insert(nid);
                    if node_set.len() >= cap {
                        break 'outer;
                    }
                }
            }
            self.node_ids = node_set.into_iter().collect();
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
            Expression::ExistsSubquery { pattern, where_clause } => {
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

    fn evaluate_binary_op(&self, op: &BinaryOp, left: Value, right: Value) -> ExecutionResult<Value> {
        // Node/edge identity comparison (Cypher: n1 = n2, n1 <> n2)
        if matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
            if let (Some(lid), Some(rid)) = (node_id_of(&left), node_id_of(&right)) {
                let eq = lid == rid;
                return Ok(Value::Property(PropertyValue::Boolean(
                    if matches!(op, BinaryOp::Eq) { eq } else { !eq }
                )));
            }
        }

        // Extract property values
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

        // Three-valued logic, same rule as `eval_binary_op`: a comparison with a null
        // operand is unknown, and a WHERE excludes unknown. `null <> 1` evaluating to true
        // kept every row whose property was merely absent. Note this is a *second*
        // comparison implementation — the two must agree, and did not.
        if matches!(
            op,
            BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
        ) && (matches!(left_prop, PropertyValue::Null)
            || matches!(right_prop, PropertyValue::Null))
        {
            return Ok(Value::Property(PropertyValue::Null));
        }

        let result = match op {
            BinaryOp::Eq => PropertyValue::Boolean(self.coerced_eq(&left_prop, &right_prop)),
            BinaryOp::Ne => PropertyValue::Boolean(!self.coerced_eq(&left_prop, &right_prop)),
            BinaryOp::Lt => self.compare_lt(&left_prop, &right_prop)?,
            BinaryOp::Le => self.compare_le(&left_prop, &right_prop)?,
            BinaryOp::Gt => self.compare_gt(&left_prop, &right_prop)?,
            BinaryOp::Ge => self.compare_ge(&left_prop, &right_prop)?,
            BinaryOp::And => self.logical_and(&left_prop, &right_prop)?,
            BinaryOp::Or => self.logical_or(&left_prop, &right_prop)?,
            // Delegated so `^` and XOR have one definition rather than two that
            // can drift; this evaluator differs from `eval_binary_op` only in
            // its comparison coercions, which neither operator uses.
            BinaryOp::Pow | BinaryOp::Xor => {
                match eval_binary_op(op, Value::Property(left_prop.clone()), Value::Property(right_prop.clone()))? {
                    Value::Property(p) => p,
                    other => return Err(ExecutionError::TypeError(format!("unexpected {other:?}"))),
                }
            }
            BinaryOp::Add => self.arithmetic_add(&left_prop, &right_prop)?,
            BinaryOp::Sub => self.arithmetic_sub(&left_prop, &right_prop)?,
            BinaryOp::Mul => self.arithmetic_mul(&left_prop, &right_prop)?,
            BinaryOp::Div => self.arithmetic_div(&left_prop, &right_prop)?,
            BinaryOp::Mod => self.arithmetic_mod(&left_prop, &right_prop)?,
            BinaryOp::StartsWith => self.string_starts_with(&left_prop, &right_prop)?,
            BinaryOp::EndsWith => self.string_ends_with(&left_prop, &right_prop)?,
            BinaryOp::Contains => self.string_contains(&left_prop, &right_prop)?,
            BinaryOp::In => self.eval_in(&left_prop, &right_prop)?,
            BinaryOp::RegexMatch => self.regex_match(&left_prop, &right_prop)?,
        };

        Ok(Value::Property(result))
    }

    /// Equality with type coercion: Integer↔Float numeric promotion,
    /// String↔Boolean coercion ("true"/"false"), and Null handling.
    fn coerced_eq(&self, left: &PropertyValue, right: &PropertyValue) -> bool {
        match (left, right) {
            // Same-type: use derived PartialEq
            _ if std::mem::discriminant(left) == std::mem::discriminant(right) => left == right,
            // Integer ↔ Float promotion
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => (*l as f64) == *r,
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => *l == (*r as f64),
            // DateTime ↔ Integer coercion (DateTime stores epoch millis as i64)
            (PropertyValue::DateTime(l), PropertyValue::Integer(r)) |
            (PropertyValue::Integer(r), PropertyValue::DateTime(l)) => l == r,
            // String ↔ Boolean coercion (LLMs often generate `prop = 'true'`)
            (PropertyValue::Boolean(b), PropertyValue::String(s)) |
            (PropertyValue::String(s), PropertyValue::Boolean(b)) => {
                match s.to_lowercase().as_str() {
                    "true" => *b,
                    "false" => !*b,
                    _ => false,
                }
            }
            // Everything else: not equal
            _ => false,
        }
    }

    fn compare_lt(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        // Incomparable types are null, not an error: raising here aborted
        // the whole query, so rows that *did* compare never came back (#607).
        Ok(match cypher_ordering(left, right) {
            Some(std::cmp::Ordering::Less) => PropertyValue::Boolean(true),
            Some(_) => PropertyValue::Boolean(false),
            None => PropertyValue::Null,
        })
    }

    fn compare_le(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        // Incomparable types are null, not an error: raising here aborted
        // the whole query, so rows that *did* compare never came back (#607).
        Ok(match cypher_ordering(left, right) {
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => PropertyValue::Boolean(true),
            Some(_) => PropertyValue::Boolean(false),
            None => PropertyValue::Null,
        })
    }

    fn compare_gt(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        // Incomparable types are null, not an error: raising here aborted
        // the whole query, so rows that *did* compare never came back (#607).
        Ok(match cypher_ordering(left, right) {
            Some(std::cmp::Ordering::Greater) => PropertyValue::Boolean(true),
            Some(_) => PropertyValue::Boolean(false),
            None => PropertyValue::Null,
        })
    }

    fn compare_ge(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        // Incomparable types are null, not an error: raising here aborted
        // the whole query, so rows that *did* compare never came back (#607).
        Ok(match cypher_ordering(left, right) {
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal) => PropertyValue::Boolean(true),
            Some(_) => PropertyValue::Boolean(false),
            None => PropertyValue::Null,
        })
    }

    fn logical_and(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        // Cypher three-valued logic: false AND x → false, true AND null → null
        match (left, right) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => Ok(PropertyValue::Boolean(*l && *r)),
            (PropertyValue::Boolean(false), _) | (_, PropertyValue::Boolean(false)) => Ok(PropertyValue::Boolean(false)),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("AND requires boolean operands".to_string())),
        }
    }

    fn logical_or(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        // Cypher three-valued logic: true OR x → true, false OR null → null
        match (left, right) {
            (PropertyValue::Boolean(l), PropertyValue::Boolean(r)) => Ok(PropertyValue::Boolean(*l || *r)),
            (PropertyValue::Boolean(true), _) | (_, PropertyValue::Boolean(true)) => Ok(PropertyValue::Boolean(true)),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("OR requires boolean operands".to_string())),
        }
    }

    fn arithmetic_add(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Integer(l + r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l + r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(*l as f64 + r)),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Float(l + *r as f64)),
            (PropertyValue::String(l), PropertyValue::String(r)) => Ok(PropertyValue::String(format!("{}{}", l, r))),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("Addition requires numeric or string operands".to_string())),
        }
    }

    fn arithmetic_sub(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Integer(l - r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l - r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(*l as f64 - r)),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Float(l - *r as f64)),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("Subtraction requires numeric operands".to_string())),
        }
    }

    fn arithmetic_mul(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Integer(l * r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l * r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(*l as f64 * r)),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Float(l * *r as f64)),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("Multiplication requires numeric operands".to_string())),
        }
    }

    fn arithmetic_div(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => Err(ExecutionError::RuntimeError("Division by zero".to_string())),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Integer(l / r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l / r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(*l as f64 / r)),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Float(l / *r as f64)),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("Division requires numeric operands".to_string())),
        }
    }

    fn arithmetic_mod(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::Integer(_), PropertyValue::Integer(0)) => Err(ExecutionError::RuntimeError("Modulo by zero".to_string())),
            (PropertyValue::Integer(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Integer(l % r)),
            (PropertyValue::Float(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(l % r)),
            (PropertyValue::Integer(l), PropertyValue::Float(r)) => Ok(PropertyValue::Float(*l as f64 % r)),
            (PropertyValue::Float(l), PropertyValue::Integer(r)) => Ok(PropertyValue::Float(l % *r as f64)),
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("Modulo requires numeric operands".to_string())),
        }
    }

    fn string_starts_with(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        Ok(string_position_op(StringPositionOp::StartsWith, left, right))
    }

    fn string_ends_with(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        Ok(string_position_op(StringPositionOp::EndsWith, left, right))
    }

    fn string_contains(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        Ok(string_position_op(StringPositionOp::Contains, left, right))
    }

    fn eval_in(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        eval_in_list(left, right)
            .ok_or_else(|| ExecutionError::TypeError("IN requires a list on the right side".to_string()))
    }

    fn regex_match(&self, left: &PropertyValue, right: &PropertyValue) -> ExecutionResult<PropertyValue> {
        match (left, right) {
            (PropertyValue::String(text), PropertyValue::String(pattern)) => {
                let re = regex::Regex::new(pattern).map_err(|e| ExecutionError::RuntimeError(format!("Invalid regex: {}", e)))?;
                Ok(PropertyValue::Boolean(re.is_match(text)))
            }
            (PropertyValue::Null, _) | (_, PropertyValue::Null) => Ok(PropertyValue::Null),
            _ => Err(ExecutionError::TypeError("=~ requires string operands".to_string())),
        }
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
        let label_sets: Option<Vec<&std::collections::HashSet<NodeId>>> =
            if self.target_labels.is_empty() {
                None
            } else {
                Some(
                    self.target_labels
                        .iter()
                        .map(|l| store.nodes_with_label(l))
                        .collect::<Option<Vec<_>>>()
                        .unwrap_or_default(),
                )
            };
        let target_props = &self.target_props;
        let target_ids = self.target_ids.as_ref();
        // Relationship isomorphism (#684): an edge this pattern already walked
        // is not a candidate. Checked here, during the adjacency walk, so a
        // rejected edge never becomes a record.
        let used_edges: &[crate::graph::EdgeId] = if self.track_edges && !self.starts_clause {
            record.used_edge_slice()
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
                Some(sets) => sets.iter().all(|s| s.contains(&target)),
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

        match self.direction {
            Direction::Outgoing => {
                store.for_each_outgoing_neighbor(node_id, type_filter, |target, eid| {
                    if keeps(target, eid) {
                        collected.push((eid, node_id, target));
                    }
                });
            }
            Direction::Incoming => {
                store.for_each_incoming_neighbor(node_id, type_filter, |source, eid| {
                    if keeps(source, eid) {
                        collected.push((eid, source, node_id));
                    }
                });
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
            pending: std::collections::VecDeque::new(),
            type_ids: None,
            pinned_target: None,
            target_reach: None,
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
        store: &GraphStore,
        visit: &mut impl FnMut(NodeId),
    ) {
        let mut with_edge = |nb: NodeId, _e: crate::graph::EdgeId| visit(nb);
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
        match self.direction {
            Direction::Outgoing => store.for_each_outgoing_neighbor(node, type_ids, &mut visit),
            Direction::Incoming => store.for_each_incoming_neighbor(node, type_ids, &mut visit),
            Direction::Both => {
                store.for_each_outgoing_neighbor(node, type_ids, &mut visit);
                store.for_each_incoming_neighbor(node, type_ids, &mut visit);
            }
        }
    }

    /// BFS from the source bound in `record`, buffering one output record per
    /// distinct reachable target in `[min_hops, max_hops]`.
    /// Pin the target to a single node the planner resolved at plan time.
    ///
    /// Only valid when the destination really is that one node; the operator
    /// then answers "can this source reach it" rather than enumerating.
    pub fn with_pinned_target(mut self, target: NodeId) -> Self {
        self.pinned_target = Some(target);
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
                    Self::neighbors_in(cur, type_filter, &reversed, store, &mut |nb| {
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

        self.ensure_type_ids(store);
        let type_ids: Option<Vec<u16>> = self.type_ids.clone();
        let type_filter = type_ids.as_deref();

        // The path so far, as (node, edge-that-reached-it). `edges` is what
        // enforces relationship uniqueness.
        let mut path: Vec<(NodeId, crate::graph::EdgeId)> = Vec::new();
        let mut edges: Vec<crate::graph::EdgeId> = Vec::new();
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
                edges.pop();
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
                // `parent` reconstructs the path for a bound path or
                // relationship variable. Built from the current stack, so it
                // describes *this* trail rather than a shortest route.
                let mut parent: std::collections::HashMap<NodeId, (NodeId, crate::graph::EdgeId)> =
                    std::collections::HashMap::new();
                let mut prev = source_id;
                for (n, e) in &path {
                    parent.insert(*n, (prev, *e));
                    prev = *n;
                }
                self.buffer(record, nb, &parent, source_id, store);
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

    fn expand_from(&mut self, record: &Record, store: &GraphStore) -> ExecutionResult<()> {
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

        // Pinned target: one membership test instead of a BFS per row. The
        // planner only sets this when the destination resolves to a single
        // node, there is no path variable, and `min_hops <= 1`.
        if let Some(target) = self.pinned_target {
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
        if self.min_hops >= 2 {
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
        if self.path_variable.is_some() || self.rel_variable.is_some() {
            let (nodes, edges) = reconstruct_path(parent, source, target);
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
                match val {
                    Value::NodeRef(id) => {
                        let node = store.get_node(id)
                            .ok_or_else(|| ExecutionError::RuntimeError(format!("Node {:?} not found", id)))?;
                        Ok(Value::Node(id, Box::new(node.clone())))
                    }
                    Value::EdgeRef(id, ..) => {
                        let edge = store.get_edge(id)
                            .ok_or_else(|| ExecutionError::RuntimeError(format!("Edge {:?} not found", id)))?;
                        Ok(Value::Edge(id, Box::new(edge.clone())))
                    }
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
            Expression::ExistsSubquery { pattern, where_clause } => {
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
            AggregatorState::Min(curr) => {
                if let Some(prop) = value.as_property() {
                    if matches!(prop, PropertyValue::Null) {
                        return;
                    }
                    if curr.is_none() || prop < curr.as_ref().unwrap() {
                        *curr = Some(prop.clone());
                    }
                }
            }
            AggregatorState::Max(curr) => {
                if let Some(prop) = value.as_property() {
                    if matches!(prop, PropertyValue::Null) {
                        return;
                    }
                    if curr.is_none() || prop > curr.as_ref().unwrap() {
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
            (AggregatorState::Min(a), AggregatorState::Min(b)) => {
                if let Some(b) = b {
                    if a.is_none() || &b < a.as_ref().unwrap() {
                        *a = Some(b);
                    }
                }
            }
            (AggregatorState::Max(a), AggregatorState::Max(b)) => {
                if let Some(b) = b {
                    if a.is_none() || &b > a.as_ref().unwrap() {
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
            Expression::ExistsSubquery { pattern, where_clause } => {
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
    fn key_of(&self, record: &Record, store: &GraphStore) -> Vec<PropertyValue> {
        self.sort_items
            .iter()
            .map(|(expr, _)| {
                // Errors are folded to Null, which is what the comparator did
                // before and what ORDER BY over a missing property means.
                Self::evaluate_expression(expr, record, store)
                    .unwrap_or(Value::Null)
                    .as_property()
                    .cloned()
                    .unwrap_or(PropertyValue::Null)
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
    ) -> Vec<PropertyValue> {
        let mut key = Vec::with_capacity(sort_items.len());
        let mut cursor = readers.iter_mut();
        for (expr, _) in sort_items {
            match expr {
                Expression::Property { .. } => {
                    let c = cursor.next().expect("one cursor per property key");
                    key.push(c.read(record, store));
                }
                other => key.push(
                    Self::evaluate_expression(other, record, store)
                        .unwrap_or(Value::Null)
                        .as_property()
                        .cloned()
                        .unwrap_or(PropertyValue::Null),
                ),
            }
        }
        key
    }

    /// Compare two precomputed keys under the per-column sort directions.
    fn cmp_keys(a: &[PropertyValue], b: &[PropertyValue], items: &[(Expression, bool)]) -> std::cmp::Ordering {
        for (i, (_, ascending)) in items.iter().enumerate() {
            let (Some(x), Some(y)) = (a.get(i), b.get(i)) else {
                continue;
            };
            // Cypher's orderability, not the index's: `ORDER BY` puts a
            // string before a number and a list before both, where the `Ord`
            // backing the property index does the opposite. See
            // `graph::property::cypher_order` for why both orders exist.
            let ord = crate::graph::property::cypher_order(x, y);
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
    fn trim_to(keyed: &mut Vec<(Vec<PropertyValue>, Record)>, k: usize, items: &[(Expression, bool)]) {
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
            Expression::ExistsSubquery { pattern, where_clause } => {
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

        let mut keyed: Vec<(Vec<PropertyValue>, Record)> = Vec::new();
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
                        match eval_expression(expr, &empty, store) {
                            Ok(Value::Property(p)) => {
                                evaluated.insert(key.clone(), p);
                            }
                            _ => {
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
            for (source_var, target_var, edge_type, properties, edge_var, _exprs) in
                &self.edges_to_create
            {
                let source_id = self.var_to_node_id.get(source_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(source_var.clone()))?;
                let target_id = self.var_to_node_id.get(target_var)
                    .ok_or_else(|| ExecutionError::VariableNotFound(target_var.clone()))?;

                let edge_id = store.create_edge(*source_id, *target_id, edge_type.clone())
                    .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                // Set properties on edge via DS-07c sparse map
                for (key, value) in properties {
                    store.set_edge_property_sparse(edge_id, key.clone(), value.clone());
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
                                Value::Property(p) => p,
                                Value::Null => PropertyValue::Null,
                                other => {
                                    return Err(ExecutionError::TypeError(format!(
                                        "property `{key}` must be a scalar, got {other:?}"
                                    )))
                                }
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
                            if let Value::Property(pv) = eval_expression(expr, &record, store)? {
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
                            if let Value::Property(pv) = eval_expression(expr, &record, store)? {
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
    /// (source_var, target_var, edge_type, properties, edge_var)
    edges_to_merge: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
    on_create_set: Vec<(String, String, Expression)>,
    on_match_set: Vec<(String, String, Expression)>,
    done: bool,
    results: Vec<Record>,
    result_index: usize,
}

impl MatchMergeEdgeOperator {
    pub fn new(
        input: OperatorBox,
        edges_to_merge: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
        on_create_set: Vec<(String, String, Expression)>,
        on_match_set: Vec<(String, String, Expression)>,
    ) -> Self {
        Self { input, edges_to_merge, on_create_set, on_match_set, done: false, results: Vec::new(), result_index: 0 }
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
                for (source_var, target_var, edge_type, properties, edge_var) in &self.edges_to_merge {
                    let source_id = match record.get(source_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue,
                    };
                    let target_id = match record.get(target_var).and_then(|v| v.node_id()) {
                        Some(id) => id,
                        None => continue,
                    };

                    // Check if edge already exists
                    let existing = store.edge_between(source_id, target_id, Some(edge_type));

                    let mut result_record = record.clone();

                    if let Some(edge_id) = existing {
                        // Edge exists — apply ON MATCH SET
                        for (var, prop, expr) in &self.on_match_set {
                            if edge_var.as_deref() == Some(var) || var == "_edge" {
                                let val = eval_expression(expr, &result_record, store)?;
                                if let Value::Property(pv) = val {
                                    let _ = store.set_edge_property(edge_id, prop.clone(), pv);
                                }
                            }
                        }
                        if let Some(ref ev) = edge_var {
                            if let Some(edge) = store.get_edge(edge_id) {
                                result_record.bind(ev.clone(), Value::Edge(edge_id, Box::new(edge.clone())));
                            }
                        }
                    } else {
                        // Edge doesn't exist — create it + apply ON CREATE SET
                        let edge_id = store.create_edge(source_id, target_id, edge_type.clone())
                            .map_err(|e| ExecutionError::GraphError(e.to_string()))?;

                        for (key, value) in properties {
                            let _ = store.set_edge_property(edge_id, key.clone(), value.clone());
                        }

                        for (var, prop, expr) in &self.on_create_set {
                            if edge_var.as_deref() == Some(var) || var == "_edge" {
                                let val = eval_expression(expr, &result_record, store)?;
                                if let Value::Property(pv) = val {
                                    let _ = store.set_edge_property(edge_id, prop.clone(), pv);
                                }
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
    variables: Vec<String>,
    detach: bool,
}

impl DeleteOperator {
    pub fn new(input: OperatorBox, variables: Vec<String>, detach: bool) -> Self {
        Self { input, variables, detach }
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
            for var in &self.variables {
                if let Some(val) = record.get(var) {
                    match val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            let node_id = *id;
                            if self.detach {
                                let out_edges: Vec<_> = store.get_outgoing_edges(node_id).iter().map(|e| e.id).collect();
                                let in_edges: Vec<_> = store.get_incoming_edges(node_id).iter().map(|e| e.id).collect();
                                for eid in out_edges.into_iter().chain(in_edges) {
                                    let _ = store.delete_edge(eid);
                                }
                            }
                            let _ = store.delete_node(tenant_id, node_id);
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            let _ = store.delete_edge(*id);
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
        let vars = self.variables.join(", ");
        OperatorDescription {
            name: if self.detach { "DetachDelete" } else { "Delete" }.to_string(),
            details: vars,
            children: vec![self.input.describe()],
        }
    }

    fn is_mutating(&self) -> bool { true }
}

/// Set property operator: SET n.name = "Alice"
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
    fn source_properties(
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

            // Apply mutations via store methods (syncs columnar + row + index)
            for (var, prop, val) in &evaluated {

                if let Some(node_val) = record.get(var) {
                    match node_val {
                        Value::NodeRef(id) | Value::Node(id, _) => {
                            store.set_node_property(tenant_id, *id, prop.clone(), val.clone())
                                .map_err(|e| ExecutionError::GraphError(e.to_string()))?;
                        }
                        Value::EdgeRef(id, ..) | Value::Edge(id, _) => {
                            let _ = store.set_edge_property(*id, prop.clone(), val.clone());
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
                let incoming = Self::source_properties(&value, store)?;

                match target {
                    Value::NodeRef(id) | Value::Node(id, _) => {
                        if !merge {
                            // `=` replaces: every property not in the incoming
                            // map goes away. Removing them first, then writing,
                            // keeps the two spellings from differing only in
                            // leftovers.
                            for key in store.node_properties_full(id).keys().cloned().collect::<Vec<_>>() {
                                if !incoming.contains_key(&key) {
                                    let _ = store.remove_node_property(id, &key);
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
                        if !merge {
                            let existing: Vec<String> = store
                                .get_edge(id)
                                .map(|e| e.properties.keys().cloned().collect())
                                .unwrap_or_default();
                            for key in existing {
                                if !incoming.contains_key(&key) {
                                    let _ = store.remove_edge_property(id, &key);
                                }
                            }
                        }
                        for (k, v) in incoming {
                            let _ = store.set_edge_property(id, k, v);
                        }
                    }
                    _ => {}
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
    executed: bool,
}

impl MergeOperator {
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
            executed: false,
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
                Value::Property(pv) => {
                    out.insert(key.clone(), pv);
                }
                Value::Null => {
                    out.insert(key.clone(), PropertyValue::Null);
                }
                other => {
                    return Err(ExecutionError::TypeError(format!(
                        "MERGE property `{key}` must be a scalar value, got {other:?}"
                    )));
                }
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
    fn merge_path(
        &self,
        path: &crate::query::ast::PathPattern,
        base: Record,
        store: &mut GraphStore,
        tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        // Flatten the path into nodes and the relationships between them.
        let mut pattern_nodes: Vec<&crate::query::ast::NodePattern> = vec![&path.start];
        // (from_index, to_index, type, properties, variable)
        let mut pattern_rels: Vec<(usize, usize, EdgeType, HashMap<String, PropertyValue>, Option<String>)> = Vec::new();
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
            pattern_rels.push((a, b, edge_type, props, segment.edge.variable.clone()));
        }

        // Candidate node ids per pattern position. A pattern node with no label has no
        // cheap candidate set, so it cannot participate in a match and the pattern is
        // treated as absent (i.e. created).
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

        let mut candidates: Vec<Vec<NodeId>> = Vec::with_capacity(pattern_nodes.len());
        for (i, np) in pattern_nodes.iter().enumerate() {
            let mut ids = Vec::new();
            if let Some(first_label) = np.labels.first() {
                for node in store.get_nodes_by_label(first_label) {
                    if Self::node_matches(node, &np.labels, node_props[i].as_ref()) {
                        ids.push(node.id);
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
            let sets = self.on_match_set.clone();
            self.apply_sets(&sets, &record, store, tenant_id)?;
            let labels = self.on_match_labels.clone();
            Self::apply_labels(&labels, &record, store, tenant_id);
            return Ok(Some(record));
        }

        // Create the entire pattern.
        let mut created: Vec<NodeId> = Vec::with_capacity(pattern_nodes.len());
        for (i, np) in pattern_nodes.iter().enumerate() {
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
        for (from, to, edge_type, props, _var) in &pattern_rels {
            let edge_id = store
                .create_edge(created[*from], created[*to], edge_type.clone())
                .map_err(|e| ExecutionError::GraphError(e.to_string()))?;
            for (k, v) in props {
                store.set_edge_property_sparse(edge_id, k.clone(), v.clone());
            }
        }

        let sets = self.on_create_set.clone();
        self.apply_sets(&sets, &record, store, tenant_id)?;
        Ok(Some(record))
    }

    /// Assign candidate nodes position by position, keeping only assignments whose
    /// relationships all exist in the store.
    fn search(
        candidates: &[Vec<NodeId>],
        rels: &[(usize, usize, EdgeType, HashMap<String, PropertyValue>, Option<String>)],
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
            let ok = rels.iter().all(|(from, to, ty, _p, _v)| {
                if *from > i || *to > i {
                    return true;
                }
                let src = assignment[*from];
                let dst = assignment[*to];
                store
                    .get_outgoing_edge_targets(src)
                    .iter()
                    .any(|(_eid, _s, t, et)| *t == dst && et == ty)
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
            if let Value::Property(pv) = val {
                let _ = store.set_node_property(tenant_id, node_id, prop.clone(), pv);
            }
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

        // Search for existing nodes matching labels + properties
        let mut matched_node_id = None;
        if let Some(first_label) = labels.first() {
            let candidates = store.get_nodes_by_label(first_label);
            for node in candidates {
                let has_all_labels = labels.iter().all(|l| node.labels.contains(l));
                if !has_all_labels { continue; }

                if let Some(required_props) = props {
                    let props_match = required_props.iter().all(|(k, v)| {
                        node.properties.get(k).map_or(false, |pv| pv == v)
                    });
                    if !props_match { continue; }
                }

                matched_node_id = Some(node.id);
                break;
            }
        }

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
                    if let Value::Property(pv) = val {
                        let _ = store.set_node_property(tenant_id, node_id, prop.clone(), pv);
                    }
                }
            }
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
                                    Value::Property(p) => p,
                                    Value::Null => PropertyValue::Null,
                                    other => {
                                        return Err(ExecutionError::TypeError(format!(
                                            "FOREACH CREATE: property `{k}` evaluated to {other:?}, \
which cannot be stored as a property value"
                                        )))
                                    }
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
            Expression::ExistsSubquery { pattern, where_clause } => {
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
                match Self::evaluate_expression(predicate, record, store) {
                    Ok(Value::Property(PropertyValue::Boolean(b))) => b,
                    Ok(Value::Null) | Ok(Value::Property(PropertyValue::Null)) => false,
                    _ => false,
                }
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
                    let val_a = Self::evaluate_expression(expr, a, store).unwrap_or(Value::Null);
                    let val_b = Self::evaluate_expression(expr, b, store).unwrap_or(Value::Null);
                    let prop_a = val_a.as_property().unwrap_or(&PropertyValue::Null);
                    let prop_b = val_b.as_property().unwrap_or(&PropertyValue::Null);
                    // Cypher's orderability, not the property index's — see
                    // `graph::property::cypher_order`. A WITH ... ORDER BY
                    // sorts here rather than in `SortOperator`, so wiring only
                    // that one left every `WITH` sort on the old order.
                    let ord = crate::graph::property::cypher_order(prop_a, prop_b);
                    if ord != std::cmp::Ordering::Equal {
                        return if *ascending { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
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
    fn test_node_scan_multi_label_dedup_and_limit() {
        // Multi-label scan must dedup across labels (a node with both labels
        // counts once) and still respect early_limit.
        let mut store = GraphStore::new();
        for _ in 0..50 {
            let id = store.create_node("Person");
            // Add second label to half — ensures overlap
            if id.as_u64() % 2 == 0 {
                if let Some(node) = store.get_node_mut(id) {
                    node.labels.insert(Label::new("Adult"));
                }
            }
        }
        // Without limit: 50 unique nodes (the Adults are also Persons)
        let mut op = NodeScanOperator::new(
            "n".to_string(),
            vec![Label::new("Person"), Label::new("Adult")],
        );
        let mut count = 0;
        while let Ok(Some(_)) = op.next(&store) {
            count += 1;
        }
        assert_eq!(count, 50, "multi-label union should dedup");

        // With early_limit: capped at limit
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
        let result = eval_function("date", &[], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => assert!(ts > 0),
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_date_string() {
        let result = eval_function("date", &[Value::Property(PropertyValue::String("2024-01-15".to_string()))], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                // 2024-01-15 00:00:00 UTC
                let expected = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
                    .and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
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
            Value::Property(PropertyValue::DateTime(ts)) => {
                let expected = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()
                    .and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis();
                assert_eq!(ts, expected);
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
            Value::Property(PropertyValue::DateTime(ts)) => assert!(ts > 0),
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_rfc3339() {
        let result = eval_function("datetime", &[Value::Property(PropertyValue::String("2024-01-15T10:30:00Z".to_string()))], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(ts)) => {
                let expected = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z").unwrap().timestamp_millis();
                assert_eq!(ts, expected);
            }
            _ => panic!("Expected DateTime"),
        }
    }

    #[test]
    fn test_eval_function_datetime_naive() {
        let result = eval_function("datetime", &[Value::Property(PropertyValue::String("2024-01-15T10:30:00".to_string()))], None).unwrap();
        match result {
            Value::Property(PropertyValue::DateTime(_ts)) => {} // valid
            _ => panic!("Expected DateTime"),
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
            Value::Property(PropertyValue::DateTime(ts)) => {
                use chrono::TimeZone;
                let expected = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 45).unwrap().timestamp_millis();
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

    #[test]
    fn test_eval_index_non_collection() {
        let result = eval_index(
            Value::Property(PropertyValue::Integer(1)),
            Value::Property(PropertyValue::Integer(0)),
            &GraphStore::new(),
        ).unwrap();
        assert_eq!(result, Value::Null);
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