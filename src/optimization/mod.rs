//! Graph-grounded optimization primitives.
//!
//! Provides [`CypherProblem`], a `Problem`/`MultiObjectiveProblem`
//! implementation whose objective and constraints are Cypher templates
//! executed against a [`crate::graph::GraphStore`]. The decision vector is
//! substituted into the template at each evaluation and the scalar (or
//! vector) returned by the query becomes the fitness.
//!
//! This is the primitive Paper 8 builds the 7 real-world problems on top of.
//!
//! ## Memoization
//! Each evaluation is keyed by a SHA-style hash of the quantized decision
//! vector (default `1e-10` resolution). Repeated evaluation of the same
//! (or numerically very close) candidate is served from cache. Cache stats
//! are exposed via [`CypherProblem::stats`].
//!
//! ## Performance note
//! Per-evaluation cost is dominated by the Cypher path, not the cache. On
//! typical KG-grounded problems we see 1-100 ms / eval; memoization is a
//! major win when solvers revisit candidates (TLBO/Rao family commonly do
//! near elite solutions).

pub mod cypher_problem;
pub use cypher_problem::{CypherProblem, CypherMOProblem, CypherProblemStats};
