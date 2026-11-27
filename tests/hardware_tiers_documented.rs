//! The hardware tiers are defined, and their test suites name tests that exist (HW-01, #1676).
//!
//! `docs/HARDWARE-TIERS.md` defines the Server, Workstation, Edge, Micro and
//! Browser tiers and, in its feature manifest, says which capabilities each
//! tier must support and which tests show it. `scripts/tier_suite.py` runs a
//! tier from that table. This test keeps the table worth running:
//!
//! * all five tiers are defined, each with hardware, resources, graph size and a
//!   feature set, not merely named;
//! * every test the manifest names exists, and every crate feature it asks for
//!   is declared, so a renamed test cannot quietly drop out of a tier;
//! * every status is one the runner understands, a required capability has at
//!   least one test, and a `gap` names the issue or requirement tracking it;
//! * the Browser tier claims nothing while there is no `wasm32` build, so a
//!   tier with nothing to build cannot read as supported;
//! * the CI platforms the page cites are still the ones `ci.yml` runs on.
//!
//! Text, not behaviour. The tests a tier names passing is what
//! `tier_suite.py` checks; that a tier's resource targets hold is what the
//! CH-HW-* suites measure, and most of them have not been measured yet.

use std::collections::BTreeSet;
use std::path::Path;

const DOC: &str = "docs/HARDWARE-TIERS.md";
const TIERS: [&str; 5] = ["Server", "Workstation", "Edge", "Micro", "Browser"];
const STATUSES: [&str; 5] = ["required", "optional", "unsupported", "gap", "—"];
const START: &str = "<!-- tier-manifest:start -->";
const END: &str = "<!-- tier-manifest:end -->";

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn doc() -> String {
    std::fs::read_to_string(root().join(DOC)).unwrap_or_else(|e| panic!("{DOC}: {e}"))
}

fn cells(line: &str) -> Vec<String> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(|c| c.trim().to_string())
        .collect()
}

fn backticked(cell: &str) -> Vec<String> {
    cell.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

struct Row {
    capability: String,
    tests: Vec<String>,
    status: Vec<String>,
    note: String,
}

fn manifest() -> Vec<Row> {
    let text = doc();
    let block = text
        .split_once(START)
        .and_then(|(_, rest)| rest.split_once(END))
        .map(|(block, _)| block)
        .unwrap_or_else(|| panic!("{DOC} has no {START} ... {END} block"));
    let lines: Vec<&str> = block
        .lines()
        .filter(|l| l.trim_start().starts_with('|'))
        .collect();
    assert!(lines.len() > 2, "{DOC}: the manifest table has no rows");
    let mut want = vec!["Capability".to_string(), "Tests".to_string()];
    want.extend(TIERS.iter().map(|t| format!("{t} tier")));
    want.push("Note".to_string());
    assert_eq!(
        cells(lines[0]),
        want,
        "{DOC}: manifest header (scripts/tier_suite.py reads it too)"
    );
    lines[2..]
        .iter()
        .map(|l| {
            let c = cells(l);
            assert_eq!(
                c.len(),
                want.len(),
                "{DOC}: row has {} cells, expected {}: {l}",
                c.len(),
                want.len()
            );
            Row {
                capability: c[0].clone(),
                tests: backticked(&c[1]),
                status: c[2..2 + TIERS.len()].to_vec(),
                note: c[c.len() - 1].clone(),
            }
        })
        .collect()
}

#[test]
fn all_five_tiers_are_defined_not_just_named() {
    let text = doc();
    let table: Vec<Vec<String>> = text
        .lines()
        .filter(|l| l.starts_with("| **"))
        .map(cells)
        .collect();
    for tier in TIERS {
        let name = format!("**{tier} tier**");
        let row = table
            .iter()
            .find(|r| r[0] == name)
            .unwrap_or_else(|| panic!("{DOC} defines no {name} row"));
        assert_eq!(
            row.len(),
            6,
            "{name}: expected hardware, resources, graph size, features, use"
        );
        // Hardware, graph size, feature set and use must say something. The
        // Browser tier's resources are "—": a tab has no core or RAM floor.
        for (i, what) in [
            (1, "reference hardware"),
            (3, "graph size"),
            (4, "feature set"),
            (5, "primary use"),
        ] {
            assert!(!row[i].is_empty() && row[i] != "—", "{name} has no {what}");
        }
        assert!(!row[2].is_empty(), "{name} has no resources cell");
    }
}

#[test]
fn every_test_a_tier_names_exists() {
    let mut missing = Vec::new();
    for row in manifest() {
        for t in &row.tests {
            match t.split_once("::") {
                None => {
                    if !root().join("tests").join(format!("{t}.rs")).is_file() {
                        missing.push(format!("{}: tests/{t}.rs", row.capability));
                    }
                }
                Some((krate, filter)) => {
                    assert!(
                        !filter.is_empty(),
                        "{}: `{t}` has an empty filter",
                        row.capability
                    );
                    let (krate, features) = krate.split_once('+').unwrap_or((krate, ""));
                    let toml = root().join("crates").join(krate).join("Cargo.toml");
                    let Ok(manifest) = std::fs::read_to_string(&toml) else {
                        missing.push(format!("{}: crate {krate}", row.capability));
                        continue;
                    };
                    let features_section = manifest.split("[features]").nth(1).unwrap_or("");
                    for f in features.split(',').filter(|f| !f.is_empty()) {
                        if !features_section
                            .lines()
                            .any(|l| l.trim_start().starts_with(&format!("{f} =")))
                        {
                            missing.push(format!("{}: feature `{f}` of {krate}", row.capability));
                        }
                    }
                }
            }
        }
    }
    assert!(
        missing.is_empty(),
        "{DOC} names tests or features that do not exist:\n  {}",
        missing.join("\n  ")
    );
}

#[test]
fn every_status_means_something() {
    let rows = manifest();
    let mut seen = BTreeSet::new();
    for row in &rows {
        assert!(
            seen.insert(row.capability.clone()),
            "{DOC}: `{}` is listed twice",
            row.capability
        );
        for (tier, s) in TIERS.iter().zip(&row.status) {
            assert!(
                STATUSES.contains(&s.as_str()),
                "{}: {tier} tier has status {s:?}, not one of {STATUSES:?}",
                row.capability
            );
            if s == "required" || s == "optional" {
                assert!(
                    !row.tests.is_empty(),
                    "{}: {s} on the {tier} tier with no test to show it",
                    row.capability
                );
            }
        }
        if row.status.iter().any(|s| s == "gap") {
            let tracked = row
                .note
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '#' || c == '-'))
                .any(|w| {
                    (w.starts_with('#')
                        && w.len() > 1
                        && w[1..].chars().all(|c| c.is_ascii_digit()))
                        || w.split_once('-').is_some_and(|(a, n)| {
                            !a.is_empty()
                                && a.chars().all(|c| c.is_ascii_uppercase())
                                && !n.is_empty()
                                && n.chars().all(|c| c.is_ascii_digit())
                        })
                });
            assert!(tracked, "{}: a gap must name the issue (#N) or requirement (XX-N) tracking it; note is {:?}", row.capability, row.note);
        }
    }
    for (i, tier) in TIERS.iter().enumerate() {
        if *tier == "Browser" {
            continue;
        }
        assert!(
            rows.iter().any(|r| r.status[i] == "required"),
            "the {tier} tier requires nothing"
        );
    }
}

#[test]
fn the_browser_tier_claims_nothing_without_a_wasm_build() {
    let mut sources = vec![root().join("Cargo.toml")];
    if let Ok(dir) = std::fs::read_dir(root().join(".github/workflows")) {
        sources.extend(dir.flatten().map(|e| e.path()));
    }
    let wasm = sources
        .iter()
        .filter_map(|p| std::fs::read_to_string(p).ok())
        .any(|t| t.contains("wasm32"));
    if wasm {
        // A wasm32 target now exists somewhere: the Browser column may start
        // claiming capabilities, and this test has nothing left to hold back.
        return;
    }
    let browser = TIERS.iter().position(|t| *t == "Browser").unwrap();
    let claimed: Vec<String> = manifest()
        .into_iter()
        .filter(|r| matches!(r.status[browser].as_str(), "required" | "optional"))
        .map(|r| r.capability)
        .collect();
    assert!(
        claimed.is_empty(),
        "no wasm32 target is built, yet the Browser tier claims: {claimed:?}"
    );
}

#[test]
fn the_ci_platforms_cited_are_the_ones_ci_runs() {
    let ci = std::fs::read_to_string(root().join(".github/workflows/ci.yml")).expect("ci.yml");
    let text = doc();
    let section = text
        .split_once("## Where each tier is tested")
        .map(|(_, s)| s)
        .expect("a `Where each tier is tested` section");
    for os in [
        "ubuntu-24.04",
        "ubuntu-24.04-arm",
        "macos-15",
        "windows-2022",
    ] {
        if section.contains(&format!("({os})")) {
            assert!(
                ci.contains(&format!("{os},")) || ci.contains(&format!("{os}]")),
                "{DOC} cites the CI platform `{os}`, which ci.yml's matrices no longer list"
            );
        }
    }
}

#[test]
fn the_tiers_page_is_listed_in_the_docs_index() {
    let index = std::fs::read_to_string(root().join("docs/README.md")).expect("docs/README.md");
    assert!(
        index.contains("(./HARDWARE-TIERS.md)"),
        "docs/README.md does not link HARDWARE-TIERS.md"
    );
}
