//! `docs/DATA-HANDLING.md` says three code paths can send data to a third
//! party and that there is no telemetry. This is the check that would fail if
//! either stopped being true.
//!
//! A published statement about where data goes is worth exactly as much as the
//! thing that notices when it drifts. Without this, the page is a paragraph
//! someone wrote once, and the fourth outbound call arrives without anyone
//! editing it — which is the failure mode of every policy document.
//!
//! So the test is an inventory, not a spot check: it lists every file that
//! builds an HTTP client or names a remote endpoint, and compares that set
//! against the one the statement documents. Adding an egress path fails here
//! and names the page to update.
//!
//! It deliberately does **not** check that the page's prose is accurate — no
//! test can. It checks the one thing that makes the prose go stale.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Files allowed to construct an outbound HTTP client, each with the reason
/// `docs/DATA-HANDLING.md` gives for it.
const DOCUMENTED_EGRESS: &[(&str, &str)] = &[
    ("src/nlq/client.rs", "NLQ: question + schema summary to the configured provider"),
    ("src/embed/client.rs", "embeddings: property values to the configured provider"),
    // The agent's web-search tool builds a client and never uses it. Listed
    // rather than excused: a stub that holds a client is one edit away from
    // being an egress path, and the statement says it makes no request.
    ("src/agent/tools.rs", "web search: a stub that makes no request"),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Lines that are comments or inside a test module are not egress.
///
/// Crude on purpose: a leading `//` and a `#[cfg(test)]` boundary. A stricter
/// parser would be more precise and would also be a second thing to keep
/// right; the allowlist below is what carries the meaning.
fn code_lines(source: &str) -> impl Iterator<Item = &str> {
    let body = match source.find("#[cfg(test)]") {
        Some(i) => &source[..i],
        None => source,
    };
    body.lines().filter(|l| !l.trim_start().starts_with("//"))
}

#[test]
fn only_the_documented_files_build_an_outbound_http_client() {
    let root = repo_root();
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    assert!(files.len() > 50, "the file walk found almost nothing");

    let mut found: BTreeSet<String> = BTreeSet::new();
    for path in &files {
        let Ok(source) = std::fs::read_to_string(path) else {
            continue;
        };
        // Every non-comment line, including test modules. The first
        // `#[cfg(test)]` in a file is not reliably the start of its test module
        // — `operator.rs` carries several on production items — so truncating
        // there could hide a real client. Over-reporting is the safe direction:
        // a test that builds one gets an allowlist entry saying so.
        let hits = source
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .any(|l| l.contains("reqwest::Client"));
        if hits {
            let rel = path
                .strip_prefix(&root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            found.insert(rel);
        }
    }

    let documented: BTreeSet<String> = DOCUMENTED_EGRESS
        .iter()
        .map(|(f, _)| (*f).to_string())
        .collect();

    let undocumented: Vec<&String> = found.difference(&documented).collect();
    assert!(
        undocumented.is_empty(),
        "these files build an HTTP client and are not in docs/DATA-HANDLING.md: {undocumented:?}\n\
         If this is a new place the engine sends data, say so on that page and add it to \
         DOCUMENTED_EGRESS here. If it does not send data anywhere, say that instead — the \
         web-search stub is listed for exactly that reason."
    );

    // The other direction. An entry left behind after its egress path was
    // deleted makes the page describe a path that no longer exists, and the
    // next reader trusts the list rather than the code.
    let gone: Vec<&String> = documented.difference(&found).collect();
    assert!(
        gone.is_empty(),
        "docs/DATA-HANDLING.md documents egress from files that no longer build a client: \
         {gone:?} — remove them from the page and from DOCUMENTED_EGRESS."
    );
}

#[test]
fn there_is_no_telemetry_sdk_anywhere_in_the_tree() {
    // The README states this positively, so it needs a check that fails if
    // somebody adds one. Dependency names rather than our own identifiers:
    // `telemetry` appears in `src/agent/executor.rs`, where it writes nodes
    // into the user's own graph and sends nothing.
    let manifests = ["Cargo.toml", "crates/samyama-sdk/Cargo.toml"];
    const REPORTERS: &[&str] = &[
        "sentry", "posthog", "segment", "mixpanel", "amplitude", "datadog", "bugsnag",
        "rollbar", "honeycomb-io", "google-analytics",
    ];
    for manifest in manifests {
        let path = repo_root().join(manifest);
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for reporter in REPORTERS {
            assert!(
                !text.to_lowercase().contains(reporter),
                "{manifest} depends on {reporter}; docs/DATA-HANDLING.md and the README both \
                 state there is no telemetry, and one of the three has to change"
            );
        }
    }
}

#[test]
fn load_csv_still_refuses_to_fetch_a_url() {
    // The statement says a query cannot make the server fetch a URL. That is
    // an SSRF boundary, not a convenience, so it is checked rather than
    // described.
    let source = std::fs::read_to_string(repo_root().join("src/query/csv_source.rs"))
        .expect("csv_source.rs");
    let body: String = code_lines(&source).collect::<Vec<_>>().join("\n");
    assert!(
        !body.contains("\"http\"") && !body.contains("\"https\""),
        "an http(s) scheme appears in csv_source.rs; if LOAD CSV can fetch URLs now, \
         docs/DATA-HANDLING.md says it cannot"
    );
}
