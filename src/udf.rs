//! User-defined functions and procedures, registered from Rust (LANG-10,
//! #1674). The design and what it leaves for later are in
//! `docs/ADR/ADR-044-user-defined-functions.md`.
//!
//! ```
//! use samyama::graph::{GraphStore, PropertyValue};
//! use samyama::query::QueryEngine;
//!
//! samyama::udf::register_function("shop.withTax", 1..=1, |args| match &args[0] {
//!     PropertyValue::Integer(n) => Ok(PropertyValue::Float(*n as f64 * 1.18)),
//!     PropertyValue::Float(x) => Ok(PropertyValue::Float(x * 1.18)),
//!     PropertyValue::Null => Ok(PropertyValue::Null),
//!     other => Err(format!("a price is a number, not {other}")),
//! })
//! .unwrap();
//!
//! samyama::udf::register_procedure("shop.nodeCount", 0..=0, &["nodes"], |store, _args| {
//!     Ok(vec![vec![PropertyValue::Integer(store.node_count() as i64)]])
//! })
//! .unwrap();
//!
//! let store = GraphStore::new();
//! let engine = QueryEngine::new();
//! let rows = engine.execute("RETURN shop.withTax(100) AS price", &store).unwrap();
//! assert_eq!(rows.records.len(), 1);
//! let rows = engine.execute("CALL shop.nodeCount() YIELD nodes RETURN nodes", &store).unwrap();
//! assert_eq!(rows.records.len(), 1);
//! ```
//!
//! # What a registration is
//!
//! - **A function** takes values and returns one: `RETURN shop.withTax(p.price)`.
//!   It is called once per row, wherever an expression can go.
//! - **A procedure** takes values and the graph, read-only, and returns rows
//!   under the columns it declared: `CALL shop.nodeCount() YIELD nodes`.
//!
//! Both take and return [`PropertyValue`]s, the engine's value type -- the
//! same values a query's parameters and results carry. A node, relationship or
//! path is not a value: pass `n.name` or `properties(n)`.
//!
//! # The registry is the process's
//!
//! A registration is visible to every `QueryEngine` in the process, and to a
//! server started in it. Registering a name again replaces it; until then a
//! query that names it gets the engine's own `UnknownFunction` or
//! `Procedure.NotFound`, unchanged.
//!
//! # Names
//!
//! A Cypher name, optionally dotted (`myfn`, `shop.withTax`), matched
//! regardless of case, as Cypher's own function names are. A name the engine
//! already uses -- a built-in function, an aggregate, a built-in procedure, an
//! algorithm, or a namespace of either -- is refused at registration, rather
//! than accepted and never reached.
//!
//! # Failures stay in the query
//!
//! An `Err` from user code fails the query with that message. A panic is
//! caught and reported the same way, so a bug in a registration cannot take
//! the server down with it.

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use crate::graph::{GraphStore, PropertyValue};
use crate::query::executor::{ExecutionError, ExecutionResult, Value};

/// A function body: argument values in, one value out.
pub type FunctionBody = dyn Fn(&[PropertyValue]) -> Result<PropertyValue, String> + Send + Sync;

/// A procedure body: the graph, read-only, and argument values in; rows out,
/// each as long as the procedure's columns.
pub type ProcedureBody =
    dyn Fn(&GraphStore, &[PropertyValue]) -> Result<Vec<Vec<PropertyValue>>, String> + Send + Sync;

/// A registered function.
pub struct Function {
    /// The name as it was registered.
    pub name: String,
    /// How many arguments it takes.
    pub args: RangeInclusive<usize>,
    body: Arc<FunctionBody>,
}

/// A registered procedure.
pub struct Procedure {
    /// The name as it was registered.
    pub name: String,
    /// How many arguments it takes.
    pub args: RangeInclusive<usize>,
    /// The columns each row holds, in order; what `YIELD` can name.
    pub columns: Vec<String>,
    body: Arc<ProcedureBody>,
}

/// Why a registration was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistrationError {
    #[error("`{0}` is not a name Cypher can call: use letters, digits and `_`, not starting with a digit, optionally in dotted parts (`shop.withTax`)")]
    InvalidName(String),
    #[error("`{name}` is {what}; choose a name the engine does not use, e.g. `my.{name}`")]
    Reserved { name: String, what: String },
    #[error("an argument range must not be empty; got {min}..={max}")]
    InvalidArity { min: usize, max: usize },
    #[error("`{name}`'s columns: {why}")]
    InvalidColumns { name: String, why: String },
}

#[derive(Default)]
struct Registry {
    functions: HashMap<String, Arc<Function>>,
    procedures: HashMap<String, Arc<Procedure>>,
}

fn registry() -> &'static RwLock<Registry> {
    static REGISTRY: OnceLock<RwLock<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(Registry::default()))
}

/// Bumped by every registration and removal. Part of the result cache's key,
/// so an answer computed before a function was replaced is not served after.
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// The registry's generation: it changes whenever what a name means changes.
pub fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.split('.').all(|part| {
            let mut chars = part.chars();
            matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

/// Namespaces the engine's own functions live in.
const FUNCTION_NAMESPACES: &[&str] = &[
    "date",
    "datetime",
    "duration",
    "feature",
    "localdatetime",
    "localtime",
    "point",
    "time",
    "vector",
];

/// Namespaces the engine's own procedures live in.
const PROCEDURE_NAMESPACES: &[&str] = &["ai", "algo", "db", "dbms", "eval", "gds", "samyama"];

fn first_part(lower: &str) -> &str {
    lower.split('.').next().unwrap_or(lower)
}

fn check_function_name(name: &str) -> Result<String, RegistrationError> {
    if !valid_name(name) {
        return Err(RegistrationError::InvalidName(name.to_string()));
    }
    let lower = name.to_lowercase();
    let reserved = |what: &str| RegistrationError::Reserved {
        name: name.to_string(),
        what: what.to_string(),
    };
    if crate::query::executor::operator::KNOWN_FUNCTIONS.contains(&lower.as_str()) {
        return Err(reserved("a built-in function"));
    }
    if crate::query::validate::is_aggregate_name(&lower) {
        return Err(reserved("a built-in aggregate"));
    }
    if lower.contains('.') && FUNCTION_NAMESPACES.contains(&first_part(&lower)) {
        return Err(reserved(&format!(
            "in the built-in `{}` namespace",
            first_part(&lower)
        )));
    }
    Ok(lower)
}

fn check_procedure_name(name: &str) -> Result<String, RegistrationError> {
    if !valid_name(name) {
        return Err(RegistrationError::InvalidName(name.to_string()));
    }
    let lower = name.to_lowercase();
    let reserved = |what: &str| RegistrationError::Reserved {
        name: name.to_string(),
        what: what.to_string(),
    };
    if lower.contains('.') && PROCEDURE_NAMESPACES.contains(&first_part(&lower)) {
        return Err(reserved(&format!(
            "in the built-in `{}` namespace",
            first_part(&lower)
        )));
    }
    if crate::query::executor::operator::AlgorithmOperator::is_algorithm(name) {
        return Err(reserved("a built-in algorithm"));
    }
    let builtin = crate::query::executor::planner::KNOWN_DB_PROCEDURES
        .iter()
        .chain(crate::query::executor::planner::KNOWN_OTHER_PROCEDURES)
        .chain(crate::query::executor::feature_ops::FeatureProcedure::NAMES)
        .any(|p| p.eq_ignore_ascii_case(name));
    if builtin || crate::query::executor::vector_ops::VectorProcedure::from_name(name).is_some() {
        return Err(reserved("a built-in procedure"));
    }
    Ok(lower)
}

fn check_arity(args: &RangeInclusive<usize>) -> Result<(), RegistrationError> {
    if args.is_empty() {
        return Err(RegistrationError::InvalidArity {
            min: *args.start(),
            max: *args.end(),
        });
    }
    Ok(())
}

/// Register `body` as a Cypher function called `name`, taking `args`
/// arguments. Replaces an earlier registration of the same name.
pub fn register_function<F>(
    name: &str,
    args: RangeInclusive<usize>,
    body: F,
) -> Result<(), RegistrationError>
where
    F: Fn(&[PropertyValue]) -> Result<PropertyValue, String> + Send + Sync + 'static,
{
    let key = check_function_name(name)?;
    check_arity(&args)?;
    let f = Function {
        name: name.to_string(),
        args,
        body: Arc::new(body),
    };
    registry()
        .write()
        .unwrap()
        .functions
        .insert(key, Arc::new(f));
    GENERATION.fetch_add(1, Ordering::AcqRel);
    Ok(())
}

/// Register `body` as a procedure called `name`, taking `args` arguments and
/// returning rows of `columns`. Replaces an earlier registration of the same
/// name.
pub fn register_procedure<F>(
    name: &str,
    args: RangeInclusive<usize>,
    columns: &[&str],
    body: F,
) -> Result<(), RegistrationError>
where
    F: Fn(&GraphStore, &[PropertyValue]) -> Result<Vec<Vec<PropertyValue>>, String>
        + Send
        + Sync
        + 'static,
{
    let key = check_procedure_name(name)?;
    check_arity(&args)?;
    let bad = |why: &str| RegistrationError::InvalidColumns {
        name: name.to_string(),
        why: why.to_string(),
    };
    if columns.is_empty() {
        return Err(bad(
            "a procedure needs at least one column for YIELD to name",
        ));
    }
    for (i, c) in columns.iter().enumerate() {
        if !valid_name(c) || c.contains('.') {
            return Err(bad(&format!("`{c}` is not a name YIELD can use")));
        }
        if columns[..i].contains(c) {
            return Err(bad(&format!("`{c}` is listed twice")));
        }
    }
    let p = Procedure {
        name: name.to_string(),
        args,
        columns: columns.iter().map(|c| c.to_string()).collect(),
        body: Arc::new(body),
    };
    registry()
        .write()
        .unwrap()
        .procedures
        .insert(key, Arc::new(p));
    GENERATION.fetch_add(1, Ordering::AcqRel);
    Ok(())
}

/// Remove the function called `name`. True if there was one.
pub fn unregister_function(name: &str) -> bool {
    let removed = registry()
        .write()
        .unwrap()
        .functions
        .remove(&name.to_lowercase())
        .is_some();
    if removed {
        GENERATION.fetch_add(1, Ordering::AcqRel);
    }
    removed
}

/// Remove the procedure called `name`. True if there was one.
pub fn unregister_procedure(name: &str) -> bool {
    let removed = registry()
        .write()
        .unwrap()
        .procedures
        .remove(&name.to_lowercase())
        .is_some();
    if removed {
        GENERATION.fetch_add(1, Ordering::AcqRel);
    }
    removed
}

/// The registered functions' names, sorted.
pub fn function_names() -> Vec<String> {
    let mut names: Vec<String> = registry()
        .read()
        .unwrap()
        .functions
        .values()
        .map(|f| f.name.clone())
        .collect();
    names.sort();
    names
}

/// The registered procedures' names, sorted.
pub fn procedure_names() -> Vec<String> {
    let mut names: Vec<String> = registry()
        .read()
        .unwrap()
        .procedures
        .values()
        .map(|p| p.name.clone())
        .collect();
    names.sort();
    names
}

/// The function registered as `name`, if any.
pub fn function(name: &str) -> Option<Arc<Function>> {
    let reg = registry().read().unwrap();
    if reg.functions.is_empty() {
        return None;
    }
    reg.functions.get(&name.to_lowercase()).cloned()
}

/// The procedure registered as `name`, if any.
pub fn procedure(name: &str) -> Option<Arc<Procedure>> {
    let reg = registry().read().unwrap();
    if reg.procedures.is_empty() {
        return None;
    }
    reg.procedures.get(&name.to_lowercase()).cloned()
}

fn arity_text(args: &RangeInclusive<usize>) -> String {
    let plural = |n: usize| if n == 1 { "" } else { "s" };
    match (*args.start(), *args.end()) {
        (a, b) if a == b => format!("{a} argument{}", plural(a)),
        (a, usize::MAX) => format!("at least {a} argument{}", plural(a)),
        (a, b) => format!("{a} to {b} arguments"),
    }
}

/// A query value as a user function sees it, or why it cannot be one.
fn plain(v: &Value) -> Result<PropertyValue, &'static str> {
    match v {
        Value::Property(p) => Ok(p.clone()),
        Value::Null => Ok(PropertyValue::Null),
        Value::List(items) => items
            .iter()
            .map(plain)
            .collect::<Result<Vec<_>, _>>()
            .map(PropertyValue::Array),
        Value::Map(m) => m
            .iter()
            .map(|(k, v)| plain(v).map(|v| (k.clone(), v)))
            .collect::<Result<HashMap<_, _>, _>>()
            .map(PropertyValue::Map),
        Value::Node(..) | Value::NodeRef(_) => Err("a node"),
        Value::Edge(..) | Value::EdgeRef(..) => Err("a relationship"),
        Value::Path { .. } => Err("a path"),
    }
}

fn plain_args(callee: &str, args: &[Value]) -> ExecutionResult<Vec<PropertyValue>> {
    args.iter()
        .enumerate()
        .map(|(i, v)| {
            plain(v).map_err(|what| {
                ExecutionError::bad_argument(format!(
                    "`{callee}()` takes values, and argument {} is {what}. Pass a property \
                     (`n.name`) or `properties(n)` instead.",
                    i + 1
                ))
            })
        })
        .collect()
}

fn check_count(callee: &str, args: &RangeInclusive<usize>, got: usize) -> ExecutionResult<()> {
    if args.contains(&got) {
        return Ok(());
    }
    Err(ExecutionError::bad_argument(format!(
        "`{callee}()` takes {}; got {got}.",
        arity_text(args)
    )))
}

/// Run user code, turning a panic into an error naming the callee.
fn guarded<T>(callee: &str, run: impl FnOnce() -> Result<T, String>) -> ExecutionResult<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(message)) => Err(ExecutionError::RuntimeError(format!(
            "`{callee}()` failed: {message}"
        ))),
        Err(payload) => {
            let why = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "no message".to_string());
            Err(ExecutionError::RuntimeError(format!(
                "`{callee}()` panicked: {why}"
            )))
        }
    }
}

impl Function {
    /// Call it on one row's arguments.
    pub(crate) fn call(&self, args: &[Value]) -> ExecutionResult<Value> {
        check_count(&self.name, &self.args, args.len())?;
        let args = plain_args(&self.name, args)?;
        let out = guarded(&self.name, || (self.body)(&args))?;
        Ok(match out {
            PropertyValue::Null => Value::Null,
            v => Value::Property(v),
        })
    }
}

impl Procedure {
    /// Run it once, checking every row against the declared columns.
    pub(crate) fn call(
        &self,
        store: &GraphStore,
        args: &[PropertyValue],
    ) -> ExecutionResult<Vec<Vec<PropertyValue>>> {
        check_count(&self.name, &self.args, args.len())?;
        let rows = guarded(&self.name, || (self.body)(store, args))?;
        if let Some(bad) = rows.iter().find(|r| r.len() != self.columns.len()) {
            return Err(ExecutionError::RuntimeError(format!(
                "`{}()` returned a row of {} values; it declared {} columns ({})",
                self.name,
                bad.len(),
                self.columns.len(),
                self.columns.join(", ")
            )));
        }
        Ok(rows)
    }
}
