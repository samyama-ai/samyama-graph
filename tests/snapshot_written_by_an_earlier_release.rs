//! A snapshot written by an earlier release still loads, and loads the same
//! graph (KG-11, #1794).
//!
//! KG-11 asks that every `.sgsnap` load "across engine minor versions", tested
//! in CI. The `snapshot-portability` jobs cover the architecture half, but they
//! write and read with the same build, so they cannot notice the day this
//! checkout stops reading a file an older release wrote. This does.
//!
//! `tests/fixtures/snapshots-by-release/` holds one snapshot per earlier
//! release, each written by `examples/snapshot_portability.rs --write` built
//! from that release's tag, beside the `--json` that the release recorded. The
//! JSON is the release's own account of the graph it wrote, so this test
//! compares against what the old engine saw, not against what this one expects.
//! Nothing in the directory is edited by hand.
//!
//! The fixtures were written on x86-64. CI runs this on x86-64 and aarch64,
//! so the aarch64 run is also a cross-architecture read of an older file.
//!
//! To add the next release: build its tag, then run
//!
//! ```bash
//! cargo run --example snapshot_portability -- \
//!   --write tests/fixtures/snapshots-by-release/vX.Y.Z.sgsnap \
//!   --json  tests/fixtures/snapshots-by-release/vX.Y.Z.json
//! ```
//!
//! The example's fixture graph and fingerprint have been the same since
//! v1.10.0. If either changes, the old JSON files no longer describe what the
//! new code computes, and this test says so rather than passing.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use samyama::graph::GraphStore;

#[allow(dead_code)] // `main` and the CLI half of the example are not used here.
#[path = "../examples/snapshot_portability.rs"]
mod portability;

/// The parts of the fingerprint a query can see. `arch`, `os` and the byte
/// count describe where and how the file was written, not the graph.
const COMPARED: [&str; 6] = [
    "nodes",
    "edges",
    "node_digest",
    "edge_digest",
    "labels",
    "edge_types",
];

/// Every release since the example's fixture stopped changing. A file that
/// goes missing should fail loudly, not shrink the test.
const RELEASES: [&str; 5] = ["v1.10.0", "v1.11.0", "v1.12.0", "v1.13.0", "v1.14.0"];

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/snapshots-by-release")
}

fn recorded(release: &str) -> BTreeMap<String, String> {
    let path = fixtures().join(format!("{release}.json"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn snapshot(release: &str) -> std::fs::File {
    let path = fixtures().join(format!("{release}.sgsnap"));
    std::fs::File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// "1.10.0" -> (1, 10, 0).
fn parse(v: &str) -> (u32, u32, u32) {
    let mut it = v
        .trim_start_matches('v')
        .split(['.', '-'])
        .map(|p| p.parse::<u32>().unwrap_or_else(|_| panic!("version `{v}`")));
    (it.next().unwrap(), it.next().unwrap(), it.next().unwrap())
}

#[test]
fn every_fixture_was_written_by_the_release_it_is_named_for() {
    // A file renamed, or re-written by the current build, would turn this into
    // a round trip that passes for the wrong reason.
    for release in RELEASES {
        let header = samyama::snapshot::peek_header(snapshot(release))
            .unwrap_or_else(|e| panic!("{release}: header: {e}"));
        let named = release.trim_start_matches('v');
        assert_eq!(
            header.samyama_version, named,
            "{release}.sgsnap says it was written by {}",
            header.samyama_version
        );
        assert_eq!(
            recorded(release).get("engine_version").map(String::as_str),
            Some(named),
            "{release}.json was not recorded by {release}"
        );
        // `<=`, not `<`: between releases `dev` still carries the last
        // release's number, so the newest fixture shares it. A tagged release
        // is still older code than the branch built after it.
        assert!(
            parse(named) <= parse(env!("CARGO_PKG_VERSION")),
            "{release} is newer than this build ({})",
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(header.format, "sgsnap", "{release}");
        assert_eq!(
            header.version,
            samyama::snapshot::format::SNAPSHOT_VERSION,
            "{release} wrote format version {}; this build writes {}. A format \
             change needs its own fixture and a decision about old files.",
            header.version,
            samyama::snapshot::format::SNAPSHOT_VERSION
        );
    }
}

#[test]
fn a_snapshot_from_each_earlier_release_loads_the_graph_that_release_wrote() {
    let mut failures = Vec::new();
    for release in RELEASES {
        let want = recorded(release);
        let mut store = GraphStore::new();
        if let Err(e) = samyama::snapshot::import_tenant(&mut store, snapshot(release)) {
            failures.push(format!("{release}: does not import: {e}"));
            continue;
        }
        let got = portability::fingerprint(&store);
        for key in COMPARED {
            let w = want.get(key).map(String::as_str).unwrap_or("(absent)");
            let g = got.get(key).map(String::as_str).unwrap_or("(absent)");
            if w != g {
                failures.push(format!("{release}: {key} was {w}, now reads {g}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "this build reads a different graph from a file an earlier release wrote:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn no_fixture_is_left_out_of_the_list() {
    // A fixture added to the directory but not to RELEASES would never be read.
    let mut on_disk: Vec<String> = std::fs::read_dir(fixtures())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    on_disk.sort();
    let mut listed: Vec<String> = RELEASES
        .iter()
        .flat_map(|r| [format!("{r}.json"), format!("{r}.sgsnap")])
        .collect();
    listed.sort();
    assert_eq!(on_disk, listed);
}
