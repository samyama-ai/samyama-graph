//! The shipped GAK demo writes through the governed path (AI-10, #1413).
//!
//! `tests/generated_content_is_separable.rs` asserts that `agent::enrich`
//! tags, scores, excludes and reverses model-generated state. That is a claim
//! about a *module*. AI-10's wording — LLM output is "never silently mixed with
//! source data" — is a claim about the *product*, and the product is what
//! `examples/agentic_enrichment_demo.rs` shows. Until this test existed the two
//! disagreed: the module did all of it and the demo did none of it, asking a
//! model for Cypher and executing the lines that began `CREATE` or `MATCH`.
//!
//! So this runs the demo binary and looks at the graph it leaves behind.
//!
//! # Why the demo has an `--offline` mode
//!
//! The online run shells out to the `claude` CLI, which CI does not have and
//! which would make the result depend on what a model said today. `--offline`
//! substitutes a fixed set of answers **at the point `fill` would have
//! returned** — same `Outcome` shape, same confidence an unsourced model answer
//! carries, same `quarantine → verify → retract` afterwards. What the fixture
//! replaces is the model; what it does not replace is any part of the
//! governance this test is about.
//!
//! # What this test cannot reach, and what covers it instead
//!
//! It cannot prove the *online* path is equally governed, because it does not
//! call a model. The type system does: `EnrichmentWorker::fill` returns
//! `Option<Outcome>`, a value or a list of entity names. There is no variant in
//! which the model's reply becomes a statement, so the old failure — a reply
//! parsed as Cypher and executed — is not expressible on this path rather than
//! merely untested on it.

use std::path::PathBuf;
use std::process::Command;

/// Locate the compiled example beside the test binary.
///
/// `cargo test` builds examples, so this exists whenever the tests do. It is
/// found by walking up from the test binary (`target/<profile>/deps/<name>`)
/// rather than assuming `debug`: CI runs debug, a release check runs release,
/// and a hardcoded profile silently tests a stale binary from the other one.
fn demo_binary() -> PathBuf {
    let mut dir = std::env::current_exe().expect("current exe");
    dir.pop(); // deps/
    if dir.ends_with("deps") {
        dir.pop();
    }
    let path = dir.join("examples").join("agentic_enrichment_demo");
    assert!(
        path.exists(),
        "the demo binary is not at {}. It is the subject of this test, so a \
         missing binary is a failure and not a reason to skip: a skip here is \
         how the demo goes ungoverned again without anything going red.\n\
         Build it with: cargo build --example agentic_enrichment_demo",
        path.display()
    );

    // The staleness guard, and it is not theoretical: `cargo test --test
    // gak_demo_writes_are_governed` does not rebuild examples, so while writing
    // this test I deleted the demo's `quarantine` call and all five cases
    // passed -- against the previous binary. CI runs a plain `cargo test`,
    // which does build examples, so the hazard is local; a local green that
    // means nothing is still worth refusing.
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("agentic_enrichment_demo.rs");
    let age = |p: &PathBuf| {
        std::fs::metadata(p)
            .and_then(|m| m.modified())
            .unwrap_or_else(|e| panic!("mtime of {}: {e}", p.display()))
    };
    assert!(
        age(&path) >= age(&src),
        "{} is older than {}, so this test would be checking a previous build \
         of the demo rather than the one in the working tree.\n\
         Rebuild it with: cargo build --example agentic_enrichment_demo",
        path.display(),
        src.display()
    );
    path
}

/// Run the demo offline and return its `GAK-SUMMARY` fields.
fn summary() -> std::collections::HashMap<String, i64> {
    let out = Command::new(demo_binary())
        .arg("--offline")
        .output()
        .expect("run the demo");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "the demo exited {:?}\n--- stdout ---\n{text}\n--- stderr ---\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let line = text
        .lines()
        .find(|l| l.starts_with("GAK-SUMMARY"))
        .unwrap_or_else(|| panic!("no GAK-SUMMARY line in:\n{text}"));
    line.split_whitespace()
        .skip(1)
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            Some((k.to_string(), v.parse().ok()?))
        })
        .collect()
}

fn field(s: &std::collections::HashMap<String, i64>, k: &str) -> i64 {
    *s.get(k)
        .unwrap_or_else(|| panic!("the summary has no `{k}`: {s:?}"))
}

#[test]
fn the_demo_detects_gaps_and_fills_them_without_executing_model_cypher() {
    let s = summary();
    assert_eq!(field(&s, "gaps"), 3, "two scalar gaps and one relationship gap");
    assert_eq!(
        field(&s, "filled"),
        3,
        "every gap got an answer, so the promotion counts below are decided by \
         the trust floor and not by a missing answer"
    );
}

#[test]
fn an_answer_below_the_trust_floor_is_not_written_into_the_graph() {
    // The case that distinguishes a governed write from a write. `mechanism`
    // runs with a floor above the confidence an unsourced answer carries, so it
    // is filled, quarantined, and never promoted.
    let s = summary();
    assert_eq!(field(&s, "promoted"), 2);
    assert_eq!(
        field(&s, "pending"),
        1,
        "the third answer must still be pending: quarantined, visible, unbelieved"
    );
}

#[test]
fn everything_the_model_wrote_is_marked() {
    let s = summary();
    // The drug, whose `manufacturer` was promoted, and the condition the model
    // created whole. The condition that was already in the graph is *not*
    // marked -- it is named by the model, not made by it.
    assert_eq!(field(&s, "marked"), 2);
    assert_eq!(field(&s, "edges"), 2, "both relationship targets materialized");
}

#[test]
fn a_retraction_puts_the_graph_back_and_spares_ingested_data() {
    let s = summary();
    assert_eq!(field(&s, "props_removed"), 1);
    assert_eq!(field(&s, "edges_removed"), 2);
    assert_eq!(field(&s, "nodes_removed"), 1, "only the node the model created");
    assert_eq!(
        field(&s, "nodes_after_retract"),
        field(&s, "nodes_before_retract") - 1
    );
    assert_eq!(
        field(&s, "ingested_survived"),
        1,
        "the ingested condition the model merely named must still be there; a \
         retraction that deleted it would be removing source data to undo a \
         model's claim about it"
    );
}

#[test]
fn the_demo_leaves_no_unmarked_generated_node() {
    // Stated as its own case because it is the AI-10 sentence itself. Note the
    // marker does not mean "the model made this node": the drug was ingested
    // and is marked, because one of its current property values came from a
    // model. Unmarked means nothing on the node came from one.
    let s = summary();
    let generated = field(&s, "marked");
    let total = field(&s, "nodes_before_retract");
    assert_eq!(
        total - generated,
        1,
        "exactly one node -- the ingested condition -- should be unmarked"
    );
}
