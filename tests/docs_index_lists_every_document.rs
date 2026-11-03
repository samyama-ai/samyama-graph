//! `docs/README.md` lists every document in `docs/`, and every link in it resolves.
//!
//! The index carries its own instruction — *"Every markdown file in this directory
//! is listed below. If you add one, add a line here"* — and an instruction is not a
//! check. It was already wrong once: `#1450` found a link to a `docs/archive/` that
//! had been gone for six months and ten of eighteen documents unlisted, among them
//! `TROUBLESHOOTING.md`, `MIGRATING-FROM-NEO4J.md` and `LEAVING-SAMYAMA.md` — the
//! four a newcomer is most likely to want and least likely to guess the filename of.
//!
//! The index was rebuilt by hand. A hand-maintained list of a directory's contents
//! drifts again the next time someone adds a file in a hurry, so this test reads the
//! directory rather than a second copy of it. Adding a document without indexing it
//! now fails here instead of six months later.
//!
//! # What this does and does not check
//!
//! It checks that every document is **reachable** and that no link is **dangling**.
//! It says nothing about whether the one-line description beside a filename is
//! accurate, or whether the document itself is current — neither is mechanically
//! decidable, and claiming otherwise would make this a check that cries wolf in one
//! direction and stays silent in the other.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const INDEX: &str = "docs/README.md";

/// The repository root, from this test's own location rather than the working
/// directory: `cargo test` is not required to run from the manifest directory.
fn docs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("docs")
}

/// Relative link targets in the index, as written (`./ADR/`, `./GLOSSARY.md`).
///
/// Only relative ones. An `https://` link is a network fact this test has no
/// business asserting — a green suite must not depend on a third party being up.
fn relative_link_targets(md: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let bytes: Vec<char> = md.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        // A markdown link is `[text](target)`. A bare parenthesis in prose is not
        // one: the index writes `ADR-001 (Rust), ADR-002 (RocksDB)`, and a parser
        // that takes every `(...)` reports five dangling links that were never
        // links. Requiring the `]` is the difference between this check finding a
        // broken link and it crying wolf on every aside.
        if bytes[i] == '(' && i > 0 && bytes[i - 1] == ']' {
            if let Some(close) = bytes[i..].iter().position(|c| *c == ')') {
                let target: String = bytes[i + 1..i + close].iter().collect();
                let target = target.trim().to_string();
                if !target.is_empty()
                    && !target.contains(char::is_whitespace)
                    && !target.starts_with("http")
                    && !target.starts_with('#')
                    && !target.starts_with("mailto:")
                {
                    out.insert(target);
                }
                i += close;
            }
        }
        i += 1;
    }
    out
}

/// A link target reduced to the path it points at, relative to `docs/`.
fn resolved(target: &str) -> PathBuf {
    let t = target.split('#').next().unwrap_or(target);
    docs_dir().join(t.trim_start_matches("./"))
}

#[test]
fn every_document_in_docs_is_listed_in_the_index() {
    let index = std::fs::read_to_string(docs_dir().join("README.md"))
        .unwrap_or_else(|e| panic!("cannot read {INDEX}: {e}"));

    let mut unlisted = Vec::new();
    for entry in std::fs::read_dir(docs_dir()).expect("cannot read docs/") {
        let path = entry.expect("bad dir entry").path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if !path.is_file() || name == "README.md" {
            continue;
        }
        // Documents and the data files the index itself chooses to list.
        if !(name.ends_with(".md") || name.ends_with(".json")) {
            continue;
        }
        if !index.contains(&format!("({name}")) && !index.contains(&format!("(./{name}")) {
            unlisted.push(name);
        }
    }
    unlisted.sort();

    assert!(
        unlisted.is_empty(),
        "{} document(s) in docs/ are not linked from {INDEX}: {}\n\n\
         Add a line for each under the section it belongs to. An index that lists \
         some of a directory is worse than no index: a reader who does not find a \
         document concludes it does not exist (#1450).",
        unlisted.len(),
        unlisted.join(", ")
    );
}

#[test]
fn every_subdirectory_of_docs_is_listed_in_the_index() {
    let index = std::fs::read_to_string(docs_dir().join("README.md"))
        .unwrap_or_else(|e| panic!("cannot read {INDEX}: {e}"));

    let mut unlisted = Vec::new();
    for entry in std::fs::read_dir(docs_dir()).expect("cannot read docs/") {
        let path = entry.expect("bad dir entry").path();
        if !path.is_dir() {
            continue;
        }
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if !index.contains(&format!("({name}/")) && !index.contains(&format!("(./{name}/")) {
            unlisted.push(name);
        }
    }
    unlisted.sort();

    assert!(
        unlisted.is_empty(),
        "{} sub-directory/ies of docs/ are not listed in {INDEX}: {}",
        unlisted.len(),
        unlisted.join(", ")
    );
}

#[test]
fn every_relative_link_in_the_index_resolves() {
    let index = std::fs::read_to_string(docs_dir().join("README.md"))
        .unwrap_or_else(|e| panic!("cannot read {INDEX}: {e}"));

    let mut dangling = Vec::new();
    for target in relative_link_targets(&index) {
        if !resolved(&target).exists() {
            dangling.push(target);
        }
    }
    dangling.sort();

    assert!(
        dangling.is_empty(),
        "{} link(s) in {INDEX} point at paths that do not exist: {}\n\n\
         This is what #1450 found: `./archive/` survived in the index for six \
         months after the directory was absorbed. A broken link in the entry \
         point is the cheapest possible loss of trust in the rest of the tree.",
        dangling.len(),
        dangling.join(", ")
    );
}

#[test]
fn the_current_version_has_release_notes() {
    // Read from `Cargo.toml` rather than from `git tag`: CI checks out shallow and
    // without tags, so a git-based version of this check could not fail there --
    // and a check that cannot fail in the place it runs is not a check.
    let manifest = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .expect("cannot read Cargo.toml");
    let version = manifest
        .lines()
        .find_map(|l| l.strip_prefix("version = "))
        .map(|v| v.trim().trim_matches('"').to_string())
        .expect("Cargo.toml has no top-level version");

    let notes = docs_dir().join("release-notes").join(format!("{version}.md"));
    assert!(
        notes.exists(),
        "the engine is version {version} and docs/release-notes/{version}.md does not exist.\n\n\
         #1450 found release notes stopping at 1.7.0 while the engine was 1.9.0 -- two \
         versions with no record of what changed, which is the one thing someone \
         upgrading needs. Write the notes with the version bump, not afterwards.\n\n\
         This check deliberately covers only the current version. Back-filling every \
         historical tag is a separate job; stopping the gap from re-opening is this one."
    );
}
