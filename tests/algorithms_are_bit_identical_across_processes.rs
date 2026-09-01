//! Every algorithm gives byte-identical output in a *fresh process* (LANG-14,
//! ALGO-11).
//!
//! `algorithm_output_is_deterministic.rs` checks determinism **within** one
//! process, which is the easy half: a `HashMap` iterates in the same order
//! twice in a row inside one run. Rust seeds its hasher **per process**, so
//! the order changes between runs — and floating-point addition is not
//! associative, so a sum over a `HashSet` gives answers differing in the last
//! bit from one process to the next.
//!
//! Three algorithms were doing exactly that, and nothing was going to catch
//! it: `adamic_adar`, `constraint` and `modularity` are all compared against a
//! recorded reference with a **1e-9 tolerance**, which is roughly seven orders
//! of magnitude wider than a last-bit difference. A check that compares
//! answers cannot find this. Only running the same input twice and diffing
//! byte for byte can.
//!
//! The differences were ~1e-16 relative and numerically meaningless. LANG-14
//! asks for *identical* output and ALGO-11 for determinism, and "almost
//! identical" is a different claim — one that makes a result set undiffable
//! and a reproduction unverifiable.

use std::process::Command;

/// Run `algo_parity_export`, which exercises every algorithm with a recorded
/// answer, and return its JSON.
///
/// A separate process each time is the entire point of the test; calling the
/// functions in a loop would re-measure what the in-process test already
/// covers and would have passed throughout the bug.
fn export(path: &str) -> String {
    let out = Command::new(env!("CARGO"))
        .args(["run", "--release", "--quiet", "--example", "algo_parity_export", "--", path])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("algo_parity_export runs");
    assert!(
        out.status.success(),
        "algo_parity_export failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    std::fs::read_to_string(path).expect("export wrote its JSON")
}

#[test]
#[ignore = "spawns cargo twice; run explicitly or in the nightly job"]
fn two_processes_produce_byte_identical_output() {
    let dir = std::env::temp_dir();
    let a = export(dir.join("samyama-det-a.json").to_str().unwrap());
    let b = export(dir.join("samyama-det-b.json").to_str().unwrap());

    if a != b {
        // Name the families that differ rather than dumping two megabytes of
        // JSON. "the output differs" sends someone hunting; "constraint
        // differs" is a place to look.
        let va: serde_json::Value = serde_json::from_str(&a).unwrap();
        let vb: serde_json::Value = serde_json::from_str(&b).unwrap();
        let mut differing: Vec<String> = Vec::new();
        for (ga, gb) in va["graphs"].as_array().unwrap().iter()
            .zip(vb["graphs"].as_array().unwrap())
        {
            let name = ga["name"].as_str().unwrap_or("?");
            for (k, v) in ga["results"].as_object().unwrap() {
                if gb["results"].get(k) != Some(v) {
                    differing.push(format!("{name}/{k}"));
                }
            }
        }
        panic!(
            "two processes disagree on {} result families: {differing:?}\n\
             Almost certainly a float sum over HashMap/HashSet iteration order. \
             Sort before summing.",
            differing.len()
        );
    }
}
