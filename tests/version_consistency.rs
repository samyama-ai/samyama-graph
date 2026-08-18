//! One version of record, in every location that publishes one (API-10).
//!
//! This test exists because the drift it checks for had already happened and
//! nobody noticed: the engine was at `1.7.0` while `pip show samyama` and
//! `npm ls samyama` both reported **`0.7.0`**, and the OpenAPI document
//! advertised `0.7.0` as the server version in its own examples. A user
//! comparing the crate version against the SDK version would have concluded
//! they were a major release apart.
//!
//! The version of record is `CARGO_PKG_VERSION` — the root manifest — and
//! every other location is compared against it. There is no configuration and
//! no allowlist: a location that cannot be read is a failure, because the
//! usual way a check like this stops working is that a refactor moves a file
//! and the check quietly starts passing over nothing.
//!
//! Locations that *derive* the version at compile time (`VERSION`, the RESP
//! `INFO` payload) cannot drift and are deliberately not listed. Only the
//! places where a human typed the number are checked.

use std::path::{Path, PathBuf};

/// Repository root, from the manifest directory of this crate.
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> String {
    let path: PathBuf = root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{rel} is named in the release checklist but could not be read: {e}"))
}

/// Every `X.Y.Z` in `text`, as a list.
///
/// Tokenised rather than pattern-matched, because the first version of this
/// function matched `127.0.0` inside `127.0.0.1` and reported the loopback
/// address as a stale version claim. A token is a maximal run of digits and
/// dots; it counts only if, once trailing dots are trimmed, it has exactly
/// three numeric components. That rejects dotted quads and accepts a version
/// at the end of a sentence.
fn semvers(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in text.split(|c: char| !(c.is_ascii_digit() || c == '.')) {
        let t = token.trim_end_matches('.');
        if t.is_empty() {
            continue;
        }
        let parts: Vec<&str> = t.split('.').collect();
        if parts.len() == 3 && parts.iter().all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit())) {
            out.push(t.to_string());
        }
    }
    out
}

/// Semvers appearing as the value of a `version` key, or in the example list
/// belonging to one.
///
/// The OpenAPI document declares its own *specification* version as
/// `openapi: 3.1.0`, which is not ours to keep in sync. Scanning every semver
/// in the file therefore fails on a correct document — so only version fields
/// are read.
fn yaml_version_values(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_version_examples = false;
    for line in text.lines() {
        let l = line.trim();
        if l.starts_with("openapi:") {
            continue;
        }
        if let Some(rest) = l.strip_prefix("version:") {
            let rest = rest.trim();
            if rest.is_empty() {
                in_version_examples = true; // an examples list follows
            } else {
                in_version_examples = false;
                out.extend(semvers(rest));
            }
            continue;
        }
        if in_version_examples {
            // Stay inside the block while indented list items or `examples:`
            // continue; any other key ends it.
            if l.starts_with('-') {
                out.extend(semvers(l));
                continue;
            }
            if l.is_empty() || l.starts_with("examples:") || l.starts_with("type:") || l.starts_with("description:") {
                continue;
            }
            in_version_examples = false;
        }
    }
    out
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The `[package] version` of a Cargo manifest — not a dependency's version.
fn cargo_package_version(rel: &str) -> Option<String> {
    let text = read(rel);
    let mut section = String::new();
    for line in text.lines() {
        let l = line.trim();
        if l.starts_with('[') {
            section = l.trim_matches(|c| c == '[' || c == ']').to_string();
            continue;
        }
        if section == "package" && l.starts_with("version") && l.contains('=') {
            if l.contains("workspace") && l.contains("true") {
                return Some("workspace".into());
            }
            return l.split('"').nth(1).map(str::to_string);
        }
    }
    None
}

#[test]
fn every_cargo_manifest_carries_the_version_of_record() {
    for rel in [
        "Cargo.toml",
        "cli/Cargo.toml",
        "crates/samyama-sdk/Cargo.toml",
        "crates/samyama-optimization/Cargo.toml",
        "crates/samyama-graph-algorithms/Cargo.toml",
        "sdk/python/Cargo.toml",
    ] {
        let found = cargo_package_version(rel)
            .unwrap_or_else(|| panic!("{rel}: no [package] version"));
        assert!(
            found == VERSION || found == "workspace",
            "{rel} declares {found}, the version of record is {VERSION}"
        );
    }
}

#[test]
fn the_python_wheel_reports_the_version_of_record() {
    // PEP 440 permits `1.7.0rc1` where cargo writes `1.7.0-rc1`; the release
    // segment is what `pip show` prints, and it must match.
    let text = read("sdk/python/pyproject.toml");
    let declared = text
        .lines()
        .find(|l| l.trim_start().starts_with("version"))
        .and_then(|l| l.split('"').nth(1))
        .expect("sdk/python/pyproject.toml has no version");
    assert_eq!(
        declared.replace('-', ""),
        VERSION.replace('-', ""),
        "pip would report {declared} for an engine at {VERSION}"
    );
}

#[test]
fn the_typescript_package_and_its_lockfile_agree_with_the_version_of_record() {
    for rel in ["sdk/typescript/package.json", "sdk/typescript/package-lock.json"] {
        let text = read(rel);
        // Only the top-level "version" keys — a lockfile's dependency
        // versions are not ours to hold in sync.
        let ours: Vec<String> = text
            .lines()
            .take(12)
            .filter(|l| l.contains("\"version\""))
            .filter_map(|l| l.split('"').nth(3).map(str::to_string))
            .collect();
        assert!(!ours.is_empty(), "{rel}: no top-level version key found");
        for v in ours {
            assert_eq!(v, VERSION, "{rel} declares {v}, the version of record is {VERSION}");
        }
    }
}

#[test]
fn the_openapi_document_advertises_the_version_of_record() {
    // Including the examples: an example is what a reader copies, and a
    // status example claiming 0.7.0 is a published claim about the server.
    let text = read("api/openapi.yaml");
    let values = yaml_version_values(&text);
    assert!(!values.is_empty(), "no version fields found in api/openapi.yaml — the scan is vacuous");
    let stale: Vec<String> = values.into_iter().filter(|v| v != VERSION).collect();
    assert!(
        stale.is_empty(),
        "api/openapi.yaml mentions {stale:?}; the version of record is {VERSION}. \
         Examples count — they are what a reader copies."
    );
}

#[test]
fn the_prose_that_names_a_version_names_the_current_one() {
    // Two documents state the version in prose. They are checked for
    // *containing* the version of record rather than for containing nothing
    // else, because prose legitimately refers to history ("since 0.4").
    for rel in ["src/lib.rs", "CLAUDE.md"] {
        let text = read(rel);
        let mentions = semvers(&text);
        if mentions.is_empty() {
            continue; // no version claim to keep in sync
        }
        assert!(
            mentions.iter().any(|v| v == VERSION),
            "{rel} names {mentions:?} but never {VERSION} — the version of record"
        );
    }
}

#[test]
fn the_release_checklist_locations_all_exist() {
    // The failure mode this guards: a file is moved, the check above reads
    // nothing, and a green test means "nothing was checked".
    for rel in [
        "Cargo.toml",
        "cli/Cargo.toml",
        "crates/samyama-sdk/Cargo.toml",
        "crates/samyama-optimization/Cargo.toml",
        "crates/samyama-graph-algorithms/Cargo.toml",
        "sdk/python/Cargo.toml",
        "sdk/python/pyproject.toml",
        "sdk/typescript/package.json",
        "sdk/typescript/package-lock.json",
        "api/openapi.yaml",
        "src/lib.rs",
        "CLAUDE.md",
    ] {
        assert!(
            Path::new(&root().join(rel)).exists(),
            "{rel} is in the release checklist but does not exist"
        );
    }
}

#[test]
fn the_semver_scanner_finds_what_it_claims_to() {
    // The scanner above decides whether the OpenAPI test can fail at all, so
    // it is worth pinning. A scanner that silently found nothing would make
    // that test vacuous — the exact failure this file is about.
    assert_eq!(semvers("version: 1.7.0"), vec!["1.7.0"]);
    assert_eq!(semvers("- 0.7.0\n- 1.2.3"), vec!["0.7.0", "1.2.3"]);
    assert_eq!(semvers("v1.7.0 and 1.7.0."), vec!["1.7.0", "1.7.0"]);
    assert!(semvers("no version here").is_empty());
    assert!(semvers("1.7").is_empty(), "two components is not a semver");
    // The bug this function was rewritten for.
    assert!(semvers("bound to 127.0.0.1:6379").is_empty(), "a dotted quad is not a version");
    assert!(semvers("10.0.0.255").is_empty());

    // And the OpenAPI reader must ignore the spec version while finding ours.
    let doc = "openapi: 3.1.0\ninfo:\n  version: 1.7.0\n  x:\n    version:\n      examples:\n        - 1.7.0\n";
    assert_eq!(yaml_version_values(doc), vec!["1.7.0", "1.7.0"]);
}
