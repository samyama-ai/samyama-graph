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
///
/// **Scoped by brace depth to that one match.** It used to take
/// `&src[start..]` — everything from the match to the end of the file — and so
/// also collected the arms of `AlgorithmOperator::is_algorithm`, which lists
/// *procedure* names reached as `CALL algo.<name>(...)` and never as scalar
/// functions. Those names are not what this test is about, and treating them
/// as functions made ML-06 (#1445) fail for adding `fastrp` and `node2vec` to
/// a procedure list, which was correct code.
fn implemented_function_names(src: &str) -> Vec<String> {
    let Some(start) = src.find("match lowered.as_str() {") else {
        return Vec::new();
    };
    // Walk from the match's opening brace to its matching close. Braces inside
    // string literals and char literals would break a naive count; the arms
    // here contain neither, and if that changes the depth goes wrong in the
    // direction of reading *less*, which shows up as the >50 guard below
    // firing rather than as a silent pass.
    let open = start + src[start..].find('{').expect("the match has a brace");
    let mut depth = 0i32;
    let mut end = src.len();
    for (i, c) in src[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = open + i;
                    break;
                }
            }
            _ => {}
        }
    }
    let body = &src[open..end];
    let mut names = Vec::new();
    // Brace depth at the start of each line, relative to the match's own
    // brace. Only depth 1 is an arm of *this* match. A nested match inside an
    // arm -- `toboolean`'s `"true" => ...` -- names a string it parses, not a
    // function, and reading it put `true` and `false` into `KNOWN_FUNCTIONS`
    // to keep this test green (#1456).
    let mut line_depth = 0i32;
    // An arm's pattern can span lines:
    //     "date.truncate" | "time.truncate"
    //     | "datetime.truncate" => {
    // so pattern lines are gathered until the `=>`.
    let mut pattern = String::new();
    for line in body.lines() {
        let depth_here = line_depth;
        line_depth += line.matches('{').count() as i32 - line.matches('}').count() as i32;
        if depth_here != 1 {
            continue;
        }
        let t = line.trim_start();
        if !(t.starts_with('"') || (t.starts_with('|') && !pattern.is_empty())) {
            pattern.clear();
            continue;
        }
        pattern.push_str(t);
        pattern.push(' ');
        // An arm looks like: "a" | "b" => {   — quoted, lowercase, then `=>`.
        let Some(arrow) = pattern.find("=>") else { continue };
        let head = std::mem::take(&mut pattern)[..arrow].to_string();
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

/// `KNOWN_FUNCTIONS` is exactly the dispatcher's arms -- no more, no fewer.
///
/// The test above catches a function the list is missing (refused at compile
/// time although it works). This one also catches the opposite: a name the
/// list accepts that no scalar call can execute. About a hundred algorithm
/// procedure names sat in the list that way, so `RETURN pagerank()` passed the
/// compile-time check and failed only at run time (#1456).
#[test]
fn known_functions_is_exactly_the_dispatcher() {
    use samyama::query::executor::operator::KNOWN_FUNCTIONS;
    use std::collections::BTreeSet;

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query/executor/operator.rs");
    let src = std::fs::read_to_string(&path).expect("read the dispatcher");
    let dispatched: BTreeSet<String> = implemented_function_names(&src).into_iter().collect();
    assert!(dispatched.len() > 50, "extracted only {} names", dispatched.len());
    let listed: BTreeSet<String> = KNOWN_FUNCTIONS.iter().map(|s| s.to_string()).collect();

    let not_dispatched: Vec<_> = listed.difference(&dispatched).collect();
    let not_listed: Vec<_> = dispatched.difference(&listed).collect();
    assert!(
        not_dispatched.is_empty(),
        "in KNOWN_FUNCTIONS but no scalar call can execute them: {not_dispatched:?}\n\
         A procedure is reached as `CALL algo.<name>(...)` and does not belong here."
    );
    assert!(
        not_listed.is_empty(),
        "dispatched but missing from KNOWN_FUNCTIONS (refused at compile time): {not_listed:?}"
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
        "date.truncate"
        | "time.truncate" => { }
        "tostring" => {
            match x {
                "nested" => { }
            }
        }
        other => { }
    }
    "#;
    let got = implemented_function_names(sample);
    assert!(got.contains(&"abs".to_string()), "{got:?}");
    assert!(got.contains(&"duration.between".to_string()), "{got:?}");
    assert!(got.contains(&"duration_between".to_string()), "{got:?}");
    assert!(!got.contains(&"other".to_string()), "bare identifiers are not names: {got:?}");
    assert!(got.contains(&"time.truncate".to_string()), "multi-line arm: {got:?}");
    assert!(!got.contains(&"nested".to_string()), "a nested match's arms are not functions: {got:?}");
}
