//! Deprecations follow docs/VERSIONING.md (API-09, #1750).
//!
//! The policy is four steps: mark it in `docs/deprecations.json`, announce it in
//! the release notes, keep it working for two minor releases, remove it only in
//! a major. A policy nothing checks is a paragraph, and the one deprecation that
//! predates this file shows what that costs: the bundled visualizer has said
//! "will be removed in an upcoming release" since 1.7.0 without ever naming one,
//! and the 1.7.0 release notes do not mention it at all.
//!
//! # What this checks
//!
//! - every entry is well-formed, and was not deprecated in a version later than
//!   the one of record;
//! - a removal is in a later major, with at least two minor releases between the
//!   notice and the removal, read from `docs/release-notes/`;
//! - an HTTP deprecation that has not been removed is still in the OpenAPI
//!   document and marked `deprecated: true` there, and every operation marked
//!   `deprecated: true` has an entry -- both directions, so neither list can lose
//!   one silently;
//! - once a release's notes exist, they mention every deprecation that release
//!   made (for releases the policy applies to; older entries carry a `note`).
//!
//! # What it does not check
//!
//! That a deprecated thing still *works*. The OpenAPI document saying an
//! operation exists is checked against the server by CH-API, and its responses
//! by CH-API-STABILITY; this test does not repeat them.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

type Version = (u64, u64, u64);

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

fn parse(v: &str) -> Option<Version> {
    let parts: Vec<u64> = v.split('.').map(|p| p.parse().ok()).collect::<Option<_>>()?;
    match parts[..] {
        [a, b, c] => Some((a, b, c)),
        _ => None,
    }
}

/// `1.9.0`, not `(1, 9, 0)`: these appear in failure messages.
fn show(v: Version) -> String {
    format!("{}.{}.{}", v.0, v.1, v.2)
}

fn is_later_major_release(since: Version, removed: Version) -> bool {
    removed.0 > since.0 && removed.1 == 0 && removed.2 == 0
}

#[test]
fn removal_rejects_patch_and_minor_releases_even_in_a_later_major() {
    let since = (1, 13, 0);
    for removed in [(1, 14, 0), (2, 0, 1), (2, 1, 0), (3, 2, 1)] {
        assert!(
            !is_later_major_release(since, removed),
            "{} is not a later major release", show(removed)
        );
    }
    for removed in [(2, 0, 0), (3, 0, 0)] {
        assert!(is_later_major_release(since, removed));
    }
}

fn version_of_record() -> Version {
    parse(env!("CARGO_PKG_VERSION")).expect("CARGO_PKG_VERSION is X.Y.Z")
}

fn registry() -> serde_json::Value {
    serde_json::from_str(&read("docs/deprecations.json")).expect("docs/deprecations.json is JSON")
}

fn entries() -> Vec<serde_json::Value> {
    registry()["deprecations"]
        .as_array()
        .expect("docs/deprecations.json has a `deprecations` array")
        .clone()
}

fn field<'a>(e: &'a serde_json::Value, key: &str) -> &'a str {
    e[key].as_str().unwrap_or_else(|| panic!("entry {e} has no string `{key}`"))
}

fn optional_version(e: &serde_json::Value, key: &str) -> Option<Version> {
    match &e[key] {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => {
            Some(parse(s).unwrap_or_else(|| panic!("{}: `{key}` {s:?} is not X.Y.Z", field(e, "id"))))
        }
        other => panic!("{}: `{key}` must be a version string or null, got {other}", field(e, "id")),
    }
}

/// Every version that has release notes: the releases this repository records.
fn released() -> BTreeSet<Version> {
    std::fs::read_dir(root().join("docs/release-notes"))
        .expect("docs/release-notes exists")
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            parse(name.strip_suffix(".md")?)
        })
        .collect()
}

/// `(method, path)`, the key of one OpenAPI operation.
type Operation = (String, String);

/// Every operation in the OpenAPI document, and the subset marked `deprecated: true`.
struct OpenApi {
    all: BTreeSet<Operation>,
    deprecated: BTreeSet<Operation>,
}

fn openapi_operations() -> OpenApi {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&read("openapi/openapi.yaml")).expect("openapi.yaml is YAML");
    let mut all = BTreeSet::new();
    let mut deprecated = BTreeSet::new();
    for (path, item) in doc["paths"].as_mapping().expect("openapi.yaml has `paths`") {
        let path = path.as_str().unwrap().to_string();
        for (method, op) in item.as_mapping().unwrap() {
            let method = method.as_str().unwrap().to_string();
            if !["get", "put", "post", "delete", "patch", "head", "options"].contains(&method.as_str()) {
                continue;
            }
            if op["deprecated"].as_bool() == Some(true) {
                deprecated.insert((method.clone(), path.clone()));
            }
            all.insert((method, path.clone()));
        }
    }
    OpenApi { all, deprecated }
}

fn http_operation(e: &serde_json::Value) -> Operation {
    let op = &e["http"];
    (
        op["method"].as_str().unwrap_or_else(|| panic!("{}: http.method", field(e, "id"))).to_lowercase(),
        op["path"].as_str().unwrap_or_else(|| panic!("{}: http.path", field(e, "id"))).to_string(),
    )
}

/// CONTROL: the inputs this file reasons about are present and non-empty. If one
/// moved, every assertion below would pass over nothing.
#[test]
fn the_registry_the_release_notes_and_the_openapi_document_are_all_readable() {
    assert!(!entries().is_empty(), "no deprecations listed; the checks below would be vacuous");
    assert!(released().len() >= 3, "fewer than three releases found in docs/release-notes");
    assert!(!openapi_operations().all.is_empty(), "openapi.yaml lists no operations");
    assert!(
        parse(field(&registry(), "policy_applies_from")).is_some(),
        "`policy_applies_from` is not X.Y.Z"
    );
}

#[test]
fn every_entry_is_well_formed_and_not_from_the_future() {
    let now = version_of_record();
    let mut ids = BTreeSet::new();
    for e in entries() {
        let id = field(&e, "id");
        assert!(ids.insert(id.to_string()), "two entries share the id {id:?}");
        for key in ["surface", "what", "replacement"] {
            assert!(!field(&e, key).trim().is_empty(), "{id}: empty `{key}`");
        }
        let since = optional_version(&e, "deprecated_in").unwrap_or_else(|| panic!("{id}: no `deprecated_in`"));
        assert!(since <= now, "{id}: deprecated_in {} is later than the version of record {}", show(since), show(now));
        if field(&e, "surface") == "http" {
            http_operation(&e);
        }
    }
}

#[test]
fn a_removal_waits_for_a_major_and_two_minors_of_notice() {
    let released = released();
    let now = version_of_record();
    for e in entries() {
        let id = field(&e, "id");
        let since = optional_version(&e, "deprecated_in").unwrap();
        let Some(removed) = optional_version(&e, "removed_in") else { continue };
        assert!(removed <= now, "{id}: removed_in {} is later than the version of record", show(removed));
        assert!(
            is_later_major_release(since, removed),
            "{id}: removed in {}, deprecated in {} -- removal is a breaking change \
             and only a major release may contain one",
            show(removed),
            show(since)
        );
        let minors_of_notice: BTreeSet<(u64, u64)> = released
            .iter()
            .filter(|v| **v > since && **v < removed && v.0 == since.0 && v.2 == 0)
            .map(|v| (v.0, v.1))
            .collect();
        assert!(
            minors_of_notice.len() >= 2,
            "{id}: deprecated in {} and removed in {} with {} minor release(s) of \
             notice between them; the policy needs two: {minors_of_notice:?}",
            show(since),
            show(removed),
            minors_of_notice.len()
        );
    }
}

#[test]
fn deprecated_http_operations_and_the_registry_agree() {
    let OpenApi { all, deprecated: marked } = openapi_operations();
    let mut listed = BTreeSet::new();
    for e in entries().iter().filter(|e| field(e, "surface") == "http") {
        let id = field(e, "id");
        let op = http_operation(e);
        if optional_version(e, "removed_in").is_some() {
            assert!(!all.contains(&op), "{id}: recorded as removed, but {op:?} is still in openapi.yaml");
            continue;
        }
        assert!(all.contains(&op), "{id}: {op:?} is not in openapi.yaml, but no `removed_in` says it went");
        assert!(marked.contains(&op), "{id}: {op:?} is deprecated but not marked `deprecated: true` in openapi.yaml");
        listed.insert(op);
    }
    let unlisted: Vec<_> = marked.difference(&listed).collect();
    assert!(
        unlisted.is_empty(),
        "openapi.yaml marks {unlisted:?} deprecated, but docs/deprecations.json has no entry for it"
    );
}

#[test]
fn release_notes_announce_the_deprecations_their_release_made() {
    let applies_from = parse(field(&registry(), "policy_applies_from")).unwrap();
    for e in entries() {
        let id = field(&e, "id");
        let since = optional_version(&e, "deprecated_in").unwrap();
        if since < applies_from {
            // Made before the policy: what was said then cannot be changed now,
            // so it must at least be written down.
            assert!(
                e["note"].as_str().is_some_and(|n| !n.trim().is_empty()),
                "{id}: deprecated in {}, before the policy, and has no `note` saying how it was announced", show(since)
            );
            continue;
        }
        let notes = format!("docs/release-notes/{}.{}.{}.md", since.0, since.1, since.2);
        // Notes are written at release time. Until then the entry is a
        // deprecation in progress, and the release is what must announce it.
        if !Path::new(&root().join(&notes)).exists() {
            continue;
        }
        let text = read(&notes);
        assert!(
            text.contains(id) || text.contains(field(&e, "what")),
            "{notes} does not mention {id:?} or its `what`; a deprecation is announced in the \
             release that makes it"
        );
    }
}
