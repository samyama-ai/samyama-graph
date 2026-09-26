//! An operator that holds children must drive them through `next_mut`.
//!
//! `PhysicalOperator::next_mut` has a default:
//!
//! ```ignore
//! fn next_mut(&mut self, store: &mut GraphStore, _tenant_id: &str) -> ... {
//!     self.next(store)   // the read-only version
//! }
//! ```
//!
//! For a leaf — a scan, a `SHOW`, `SingleRowOperator` — that is exactly right.
//! For an operator holding children it silently downgrades a write pipeline to a
//! read one at that node, and every child below is driven through `next`.
//!
//! That is #1479. `JoinOperator`, `LeftOuterJoinOperator` and
//! `CartesianProductOperator` each drained their *left* through `next_mut` and
//! then called `self.next(store)`, so a mutating `CALL` joined in on the right
//! got the read executor's refusal from inside a statement routed as a write.
//! Only a bare `CALL` — which bypasses the pipeline entirely via the older
//! `call_clause` AST shape — worked.
//!
//! # Why a ratchet and not a ban
//!
//! Two operators hold children and do not override it today:
//! `CorrelatedCallOperator` and `SemiApplyOperator`. Both are reachable only
//! through `CALL { ... }`, and writes inside a `CALL {}` subquery are refused
//! before the operator tree runs — probed, not assumed:
//!
//! ```text
//! CALL { CREATE (:C) } RETURN 1
//!   -> 400 writes inside a CALL {} subquery are not supported
//! ```
//!
//! So they are latent rather than broken, and both interleave their outer and
//! body iteration rather than draining, so a correct `next_mut` for them is a
//! real change rather than a mirror of the left-hand drain. This test holds the
//! line at two and fails if a third appears — or if one is fixed and the count
//! is not lowered.
//!
//! The durable fix is to remove the default, so an operator holding children
//! cannot inherit read behaviour by saying nothing. Until then, this.

use std::path::Path;

/// Operators that hold an `OperatorBox` and do not override `next_mut`.
const ALLOWED: usize = 2;

fn source() -> String {
    std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/executor/operator.rs"),
    )
    .expect("operator.rs")
}

/// The body of `impl PhysicalOperator for <name>`, by brace matching.
fn impl_body<'a>(src: &'a str, name: &str) -> Option<&'a str> {
    let needle = format!("impl PhysicalOperator for {name} ");
    let start = src.find(&needle)?;
    let open = src[start..].find('{')? + start;
    let mut depth = 0usize;
    for (i, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&src[open..open + i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Does `pub struct <name>` declare a field of type `OperatorBox`?
fn holds_children(src: &str, name: &str) -> bool {
    let Some(start) = src.find(&format!("pub struct {name} ")) else {
        return false;
    };
    let Some(open) = src[start..].find('{').map(|i| i + start) else {
        return false;
    };
    let mut depth = 0usize;
    for (i, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return src[open..open + i].contains("OperatorBox");
                }
            }
            _ => {}
        }
    }
    false
}

#[test]
fn no_new_operator_inherits_the_read_only_next_mut() {
    let src = source();
    let mut offenders: Vec<String> = Vec::new();

    let mut cursor = 0usize;
    while let Some(rel) = src[cursor..].find("impl PhysicalOperator for ") {
        let at = cursor + rel + "impl PhysicalOperator for ".len();
        let name: String = src[at..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        cursor = at + name.len();
        if name.is_empty() {
            continue;
        }
        if !holds_children(&src, &name) {
            continue;
        }
        let Some(body) = impl_body(&src, &name) else { continue };
        if !body.contains("fn next_mut") {
            offenders.push(name);
        }
    }
    offenders.sort();
    offenders.dedup();

    assert!(
        offenders.len() <= ALLOWED,
        "{} operator(s) hold children and inherit the read-only `next_mut` \
         default, {ALLOWED} allowed: {}\n\n\
         An operator that holds an `OperatorBox` and does not override `next_mut` \
         drives its children through `next`, which silently turns a write pipeline \
         into a read one at that node. That is #1479. Override it, or remove the \
         trait default so the compiler asks.",
        offenders.len(),
        offenders.join(", ")
    );

    assert!(
        offenders.len() >= ALLOWED,
        "only {} operator(s) now inherit the default, against an allowance of \
         {ALLOWED}. Good news, and the ratchet needs tightening: set \
         `ALLOWED = {}` in this file so the ground is held.",
        offenders.len(),
        offenders.len()
    );
}
