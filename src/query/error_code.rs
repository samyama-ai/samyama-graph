//! Stable, machine-readable error codes (LANG-12).
//!
//! LANG-12 asks that every error carry "a code, the offending span, and a
//! suggested repair". Before this, none did. The scorecard's own note said why
//! the Rust variant name would not do:
//!
//! > it is unpublished, and it does not separate two faults a caller would
//! > handle differently — `TypeError("x is not a node")` and
//! > `TypeError("Add requires numeric operands")` are the same variant.
//!
//! That objection is the design constraint. A code is only worth anything if a
//! client can **branch** on it, so two faults that call for different handling
//! must not share one. Giving every error a single prefix would satisfy a
//! "carries a code" check and help nobody, which is why
//! `examples/error_quality.rs` now counts *distinct* codes per fault class
//! rather than presence alone.
//!
//! # Shape
//!
//! `Samyama.<Category>.<Subsystem>.<Fault>`, four dotted segments, following
//! the convention Neo4j established (`Neo.ClientError.Statement.SyntaxError`).
//! Matching the shape is deliberate: the people integrating against this
//! already have code that parses that form, and inventing a different one buys
//! nothing.
//!
//! `Category` is who is at fault and therefore who can act:
//!
//!   * `ClientError` — the query is wrong. Fix the query.
//!   * `DatabaseError` — the engine or the store failed. Fixing the query will
//!     not help.
//!
//! # Stability
//!
//! These strings are API. Renaming one breaks every caller branching on it, so
//! a code is added rather than changed, and a retired one is left documented
//! rather than reused for something else.

/// The query did not parse.
pub const SYNTAX: &str = "Samyama.ClientError.Statement.SyntaxError";
/// It parsed and does not mean anything — a variable nothing defines, a
/// clause in an order the language does not allow.
pub const SEMANTIC: &str = "Samyama.ClientError.Statement.SemanticError";
/// Valid Cypher this engine does not implement yet. Distinct from `SEMANTIC`
/// because the query may be correct and the answer is "not here, not yet".
pub const UNSUPPORTED: &str = "Samyama.ClientError.Statement.NotSupported";
/// A function name that does not exist.
pub const UNKNOWN_FUNCTION: &str = "Samyama.ClientError.Statement.UnknownFunction";
/// A `CALL` to a procedure that does not exist.
pub const UNKNOWN_PROCEDURE: &str = "Samyama.ClientError.Procedure.NotFound";
/// A `CALL algo.*` naming an algorithm that does not exist. Separate from
/// `UNKNOWN_PROCEDURE` because the `algo.` namespace routes to one operator,
/// so the caller's recovery differs: the procedure surface is fine and the
/// algorithm is not there.
pub const UNKNOWN_ALGORITHM: &str = "Samyama.ClientError.Procedure.UnknownAlgorithm";
/// A known function or procedure called with arguments it cannot accept — a
/// zero step, a missing required parameter, a value out of range.
pub const BAD_ARGUMENT: &str = "Samyama.ClientError.Statement.ArgumentError";
/// An operation on operands of the wrong type.
pub const TYPE_MISMATCH: &str = "Samyama.ClientError.Statement.TypeError";
/// A variable that nothing in scope binds.
pub const VARIABLE_NOT_BOUND: &str = "Samyama.ClientError.Statement.VariableNotBound";
/// A read of something this query already deleted. Its own code because the
/// caller can retry a stale read and cannot retry a type error.
pub const ENTITY_DELETED: &str = "Samyama.ClientError.Statement.EntityDeleted";
/// An aggregate used where the language does not allow one.
pub const AGGREGATE_MISUSE: &str = "Samyama.ClientError.Statement.AggregationError";
/// A write attempted through a read-only path.
pub const WRITE_IN_READ: &str = "Samyama.ClientError.Statement.WriteInReadTransaction";
/// A write refused because it would break an invariant the graph guarantees.
pub const CONSTRAINT: &str = "Samyama.ClientError.Schema.ConstraintValidationFailed";
/// The planner could not produce a plan.
pub const PLANNING: &str = "Samyama.ClientError.Statement.PlanningFailed";
/// A runtime fault not yet classified any more finely than this.
///
/// **This one is a placeholder and is meant to shrink.** `RuntimeError` is a
/// catch-all with 144 construction sites, and a single code across all of them
/// reproduces exactly the problem this module exists to fix. It is here so
/// that no error is left without a code at all; every site that a caller would
/// genuinely handle differently should get its own constant above, and the
/// count of errors still landing here is worth watching.
/// A query refused because one operator exceeded the per-operator row budget.
///
/// A client error rather than a database error: the engine is working, the
/// query asked for more intermediate rows than the budget allows -- almost
/// always an unintended cartesian product. Its own code so a caller can
/// branch on it and retry with a narrower pattern.
pub const ROW_BUDGET_EXCEEDED: &str = "Samyama.ClientError.Statement.RowBudgetExceeded";

pub const RUNTIME: &str = "Samyama.ClientError.Statement.RuntimeError";
/// The store or the engine failed. The query is not necessarily wrong.
pub const GRAPH_ACCESS: &str = "Samyama.DatabaseError.Statement.GraphAccessFailed";
/// A literal the grammar accepted and cannot mean anything -- a `\\u` escape
/// that is not four hex digits, an integer outside the range of the type.
/// Separate from `SYNTAX` because the query's *shape* is fine.
pub const INVALID_LITERAL: &str = "Samyama.ClientError.Statement.InvalidLiteral";
/// A variable used where the pattern needs a node or a relationship, and it is
/// bound to something else -- a list, a path, a scalar.
pub const VARIABLE_KIND: &str = "Samyama.ClientError.Statement.VariableTypeError";
/// Two clauses that cannot both be in the same query, or a projection that
/// does not line up -- UNION column mismatch, a duplicate output column.
pub const CLAUSE_CONFLICT: &str = "Samyama.ClientError.Statement.ClauseConflict";
/// A CREATE or MERGE pattern the language does not permit -- an untyped
/// relationship, an undirected one, a variable-length one.
pub const INVALID_WRITE_PATTERN: &str = "Samyama.ClientError.Statement.InvalidWritePattern";
