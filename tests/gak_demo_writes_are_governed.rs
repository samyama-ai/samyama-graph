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

/// Locate the compiled example, without assuming where Cargo puts it.
///
/// Cargo splits `target/` in two: an *artifact* directory for final outputs,
/// which is where a compiled example lands, and a *build* directory for
/// intermediates (`build.build-dir`, stabilised in 1.91). Both default to
/// `target`, so the split is easy to miss -- and the Cargo Book is explicit
/// that the build directory's layout "is internal to Cargo, and we can change
/// this if needed".
///
/// This used to walk up two levels from `current_exe()`, which its own comment
/// described as `target/<profile>/deps/<name>`. That is a path in the BUILD
/// directory. Rust 1.100 rearranged it, the walk landed inside a build script's
/// `out/`, and all five tests here failed on every open pull request (#2178).
/// The example had not moved; the anchor had.
///
/// So the first anchor is `CARGO_TARGET_TMPDIR`, which Cargo documents as a
/// directory inside the *target* directory and sets for every integration test
/// -- the same side of the split as the example. `current_exe()` is still tried
/// after it, because it is correct on Cargo versions before the rearrangement
/// and costs nothing to keep.
///
/// Neither anchor's depth is assumed, which is the actual lesson of #2178: a
/// hardcoded number of `pop()`s is a bet on a layout Cargo does not promise.
/// Each ancestor is tried in turn and the first holding `examples/<name>` wins,
/// so a future rearrangement changes which ancestor matches instead of breaking
/// the test. The search cannot wander into the wrong profile -- the ancestors of
/// `target/debug/tmp` do not include `target/release` -- and it cannot match the
/// sources in `<repo>/examples`, which are `.rs` files rather than a binary with
/// this name. If nothing matches, the panic lists every directory it looked in
/// rather than naming one and implying the others were impossible.
fn demo_binary() -> PathBuf {
    // `.exe` on Windows, nothing elsewhere.
    let exe = format!("agentic_enrichment_demo{}", std::env::consts::EXE_SUFFIX);

    let mut anchors = vec![PathBuf::from(env!("CARGO_TARGET_TMPDIR"))];
    if let Ok(exe_path) = std::env::current_exe() {
        anchors.push(exe_path);
    }

    // Six is enough to reach the artifact directory from either anchor
    // (`target/<profile>/tmp` is three, a build-dir path a little more) and
    // stops well short of the filesystem root, so a missing binary reports a
    // short list instead of every directory up to `/`.
    let mut tried = Vec::new();
    let mut found = None;
    'anchors: for anchor in &anchors {
        for dir in anchor.ancestors().take(6) {
            let candidate = dir.join("examples").join(&exe);
            // `is_file`, not `exists`: a directory of that name is not a binary
            // we can run, and saying "found" of one would fail later and
            // further away.
            if candidate.is_file() {
                found = Some(candidate);
                break 'anchors;
            }
            tried.push(candidate);
        }
    }

    let path = found.unwrap_or_else(|| {
        let looked: String = tried.iter().map(|p| format!("\n  {}", p.display())).collect();
        panic!(
            "the demo binary was not found. It is the subject of this test, so a \
             missing binary is a failure and not a reason to skip: a skip here is \
             how the demo goes ungoverned again without anything going red.\n\
             Build it with: cargo build --example agentic_enrichment_demo\n\
             Looked in:{looked}"
        )
    });

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
