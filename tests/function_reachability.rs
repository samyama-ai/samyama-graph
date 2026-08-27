//! Every function the evaluator implements can be *named* in Cypher (#769).
//!
//! Twice the evaluator has run ahead of the grammar:
//!
//! * #758 — the Rust side handled `ASCENDING`/`DESCENDING` and the grammar had
//!   only `ASC`/`DESC`, so `ORDER BY x ASCENDING` was a parse error. 56 TCK
//!   scenarios.
//! * #769 — `function_name` had no dot, so the entire namespaced family was
//!   unparseable. **`duration.between` was fully implemented and could never be
//!   called.** 453 TCK scenarios.
//!
//! Both looked like missing features and were missing *spellings*. A function
//! reachable only through syntax the grammar cannot produce is dead code that
//! looks live, and nothing in the suite noticed either time.
//!
//! So this asks the parser about every name the dispatcher matches. It reads
//! the dispatcher's source rather than carrying a hand-maintained list,
//! because a list that must be updated by hand is a list that silently goes
//! stale — which is the same failure mode one level up.

use std::path::Path;

/// Pull the function names out of `eval_function`'s `match lowered.as_str()`.
///
/// Deliberately conservative: it reads the arms of that one match and takes
/// the string literals. If the dispatcher is restructured this finds nothing
/// and the test says so loudly rather than passing on an empty set — a test
/// that checks nothing is the failure this file exists to prevent.
fn implemented_function_names(src: &str) -> Vec<String> {
    let Some(start) = src.find("match lowered.as_str() {") else {
        return Vec::new();
    };
    let body = &src[start..];
    let mut names = Vec::new();
    for line in body.lines() {
        let t = line.trim_start();
        // An arm looks like: "a" | "b" => {   — quoted, lowercase, then `=>`.
        let Some(arrow) = t.find("=>") else { continue };
        let head = &t[..arrow];
        if !head.starts_with('"') {
            continue;
        }
        for piece in head.split('|') {
            let p = piece.trim();
            if p.len() >= 2 && p.starts_with('"') && p.ends_with('"') {
                let n = &p[1..p.len() - 1];
                if !n.is_empty()
                    && n.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.')
                {
                    names.push(n.to_string());
                }
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

#[test]
fn every_implemented_function_can_be_named_in_cypher() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/executor/operator.rs");
    let src = std::fs::read_to_string(&path).expect("read the dispatcher");
    let names = implemented_function_names(&src);

    // Guard against the extractor silently finding nothing, which would make
    // this test pass while checking zero functions.
    assert!(
        names.len() > 50,
        "extracted only {} function names — the dispatcher was probably \
         restructured and this test is no longer reading it",
        names.len()
    );

    let unreachable: Vec<&String> = names
        .iter()
        // Zero-arg is enough: the question is whether the *name* can be
        // written, not whether the call type-checks. Arity errors belong to
        // the evaluator.
        .filter(|n| samyama::query::parser::parse_query(&format!("RETURN {n}() AS r")).is_err())
        .collect();

    assert!(
        unreachable.is_empty(),
        "{} implemented function(s) cannot be named in Cypher: {:?}\n\
         Each is dead code that looks live. This is how #758 and #769 happened.",
        unreachable.len(),
        unreachable
    );
}

/// The extractor finds the names it is supposed to find.
///
/// Without this, a change that broke the parsing above would make the real
/// test vacuous rather than red.
#[test]
fn the_extractor_actually_extracts() {
    let sample = r#"
    match lowered.as_str() {
        "abs" => { }
        "duration.between" | "duration_between" => { }
        "tostring" => { }
        other => { }
    }
    "#;
    let got = implemented_function_names(sample);
    assert!(got.contains(&"abs".to_string()), "{got:?}");
    assert!(got.contains(&"duration.between".to_string()), "{got:?}");
    assert!(got.contains(&"duration_between".to_string()), "{got:?}");
    assert!(!got.contains(&"other".to_string()), "bare identifiers are not names: {got:?}");
}
