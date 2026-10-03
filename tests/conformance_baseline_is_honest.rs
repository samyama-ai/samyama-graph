//! The conformance baseline must describe this engine, not flatter it.
//!
//! `tools/conformance/baseline.json` is the file CI compares a run against, and the one
//! place in this repository where a change in what the engine computes for the ISO path
//! patterns is visible in a pull request. These checks are about the file itself: a
//! baseline that has drifted, or that quietly records every cell as passing, would make
//! the gate green and meaningless.

use std::collections::BTreeMap;

fn baseline() -> serde_json::Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tools/conformance/baseline.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("baseline.json is missing"))
        .expect("baseline.json is not valid JSON")
}

#[test]
fn the_baseline_records_every_construct_with_a_known_verdict() {
    let b = baseline();
    let verdicts: BTreeMap<String, String> =
        serde_json::from_value(b["verdicts"].clone()).expect("verdicts is not a map");
    assert!(
        verdicts.len() >= 80,
        "the suite has 84 constructs; a baseline with {} is measuring a subset and the \
         rest would move unnoticed",
        verdicts.len()
    );
    const KNOWN: [&str; 6] = [
        "CONFORMS", "DIVERGES", "REJECTS", "INEXPRESSIBLE", "NONDETERMINISTIC",
        "ENGINE_UNAVAILABLE",
    ];
    for (case, v) in &verdicts {
        assert!(KNOWN.contains(&v.as_str()), "{case}: unknown verdict {v}");
    }
}

#[test]
fn the_baseline_is_not_all_green() {
    // Not a requirement that the engine be wrong -- a requirement that the file be a
    // measurement. Ten cells diverge today and they are the open dialect questions in
    // #1646. If this ever fails because every cell conforms, delete the test with the
    // commit that made it true.
    let b = baseline();
    let verdicts: BTreeMap<String, String> =
        serde_json::from_value(b["verdicts"].clone()).unwrap();
    let conforming = verdicts.values().filter(|v| *v == "CONFORMS").count();
    assert!(
        conforming < verdicts.len(),
        "every cell in the baseline conforms. Either the engine is fully conforming -- \
         in which case delete this test in the same commit -- or the baseline was \
         written from something other than a run."
    );
}

#[test]
fn the_baseline_says_which_suite_and_which_build_it_came_from() {
    let b = baseline();
    let commit = b["suite_commit"].as_str().unwrap_or_default();
    assert_eq!(
        commit.len(),
        40,
        "suite_commit must be a full sha: a baseline that does not say which suite \
         produced it cannot be reproduced, and a suite change would look like an \
         engine change"
    );
    assert!(
        b["engine_version"].as_str().unwrap_or_default().contains("samyama"),
        "engine_version must name the build measured"
    );
    assert!(
        b["recorded"].as_str().unwrap_or_default().len() == 10,
        "recorded must be an ISO date"
    );
}
