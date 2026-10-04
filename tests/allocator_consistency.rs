//! The benchmarks measure the allocator that ships (ADR-038, #1267).
//!
//! Published latencies and the CH-REGRESS gate come from the bench binaries, not
//! from the server. MVCC step 5b changed BI-14 by 1.68x under glibc and by 1.02x
//! under jemalloc, so a bench on a different allocator from the server describes a
//! build nobody runs. These checks read the sources, because what matters is which
//! binaries install `samyama::allocator::SHIPPED`, and a runtime test inside this
//! test binary cannot see another binary's allocator.

const INSTALL: &str = "static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;";

fn read(path: &str) -> String {
    std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/").to_string() + path)
        .unwrap_or_else(|e| panic!("{path}: {e}"))
}

#[test]
fn the_default_build_ships_mimalloc() {
    assert_eq!(samyama::allocator::NAME, "mimalloc");
}

#[test]
fn the_server_installs_the_shipped_allocator() {
    assert!(read("src/main.rs").contains(INSTALL), "src/main.rs does not install SHIPPED");
}

#[test]
fn every_gated_bench_installs_the_shipped_allocator() {
    for bench in ["benches/ldbc_benchmark.rs", "benches/ldbc_bi_benchmark.rs", "benches/finbench_benchmark.rs"] {
        assert!(read(bench).contains(INSTALL), "{bench} measures a different allocator from the server");
    }
    // The footprint bench counts bytes, so it wraps the shipped allocator
    // rather than installing it directly; it must not fall back to System.
    let footprint = read("benches/memory_footprint.rs");
    assert!(footprint.contains("samyama::allocator::SHIPPED.alloc(layout)"));
    assert!(!footprint.contains("System.alloc("), "memory_footprint counts through System, not SHIPPED");
}

#[test]
fn the_library_never_installs_a_global_allocator() {
    for path in ["src/lib.rs", "src/allocator.rs"] {
        let code: String = read(path).lines().filter(|l| !l.trim_start().starts_with("//")).collect();
        assert!(!code.contains("#[global_allocator]"), "{path} installs a global allocator for its host process");
    }
}

#[test]
fn the_sdk_does_not_take_the_servers_allocator() {
    let sdk = read("crates/samyama-sdk/Cargo.toml");
    let line = sdk.lines().find(|l| l.trim_start().starts_with("samyama = ")).expect("samyama dependency");
    assert!(line.contains("default-features = false"), "samyama-sdk would bring mimalloc into every embedding: {line}");
}

#[test]
fn every_gated_bench_says_which_allocator_it_ran_under() {
    // First statement of main, so no early return (a skip, a missing data
    // directory, an unknown option) can leave a run without it. The first version
    // of this line sat inside an error branch in two of the three benches and
    // never printed on a real run.
    for bench in ["benches/ldbc_benchmark.rs", "benches/ldbc_bi_benchmark.rs", "benches/finbench_benchmark.rs"] {
        let src = read(bench);
        let main = src.find("fn main()").unwrap_or_else(|| panic!("{bench}: no main"));
        let body = &src[main..];
        let open = body.find('{').expect("main body");
        let first_statement = body[open + 1..]
            .lines()
            .map(str::trim)
            .find(|l| !l.is_empty() && !l.starts_with("//"))
            .expect("a statement");
        assert!(
            first_statement.contains("samyama::allocator::NAME"),
            "{bench}: main starts with `{first_statement}`, not the allocator line"
        );
    }
}

// ---------------------------------------------------------------------------
// Examples that measure (#1818)
// ---------------------------------------------------------------------------

/// Source with `//` line comments stripped, so a pattern quoted in a doc comment
/// does not read as code. The examples' module docs quote Cypher and shell, and
/// `allocator.rs`'s own doc comment contains the install line verbatim.
fn code_of(path: &str) -> String {
    read(path)
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The rule, stated once: **an example is a measurement example if its code reads
/// a clock across a span of work** — `Instant::now()` paired with `.elapsed()`.
///
/// Why this rule and not a looser one. Of the 122 examples, 71 contain both and 51
/// contain neither; the two sets coincide exactly, and no example uses any other
/// clock (no `SystemTime::now`, `chrono`, `quanta`, `rdtsc`, `getrusage`). In every
/// one of the 71 the duration reaches the output — a latency, a throughput, a
/// per-row nanosecond figure, or an "imported in 12.3 s". None uses a clock only
/// for control flow, so there is no example that times something without
/// reporting it, and none that prints a timestamp without timing anything.
///
/// The tiers differ in stakes, not in treatment. All three report a duration a
/// reader can quote, so all three must report it under the allocator that ships:
///   * probes and benchmarks (`*_probe`, `*_bench*`, `*_scaling`, `*_cost`,
///     `bi17_*`, `ic1_*`, `is7_*`) — the number is the whole deliverable;
///   * loaders (`*_loader`, `phase1b_smoke`, `test_kg_queries`) — ingest wall time
///     and throughput, quoted in docs and run books;
///   * domain demos (`*_demo`, `uc*_*`, `banking_demo`) — per-stage latency printed
///     beside a narrative, the figures prospects read off a screen.
fn measurement_examples() -> Vec<String> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/examples");
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir).expect("examples/") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let name = path.file_stem().expect("stem").to_string_lossy().to_string();
        let code = code_of(&format!("examples/{name}.rs"));
        if code.contains("Instant::now()") && code.contains(".elapsed()") {
            found.push(name);
        }
    }
    found.sort();
    found
}

/// Examples deliberately exempt from installing `SHIPPED`, each with the reason.
/// An exemption with a stated reason is fine; a silent omission is not. Empty
/// today: every measuring example installs the shipped allocator, directly or
/// through a counting wrapper that delegates to it.
const EXEMPT: &[(&str, &str)] = &[];

#[test]
fn every_example_that_measures_installs_the_shipped_allocator() {
    // Why: #750 read 69% of IS3's cost as the allocator and +139 ns/row for a
    // string read, from a `perf` profile of an example with no
    // `#[global_allocator]` -- so a glibc profile, full of `_int_malloc` and
    // `malloc_consolidate`, of a build nobody ships. The same delta under mimalloc
    // is +5 ns/row, a factor of 28, and a 1,534-site semver-major refactor was
    // proposed on the strength of the wrong number (#1818).
    let mut wrong = Vec::new();
    for name in measurement_examples() {
        if EXEMPT.iter().any(|(e, _)| *e == name) {
            continue;
        }
        let code = code_of(&format!("examples/{name}.rs"));
        // Either installs SHIPPED directly, or -- for the examples that count
        // allocations -- wraps it, exactly as `benches/memory_footprint.rs` does.
        let installs = code.contains(INSTALL);
        let wraps = code.contains("#[global_allocator]")
            && code.contains("samyama::allocator::SHIPPED.alloc(")
            && !code.contains("System.alloc(");
        if !(installs || wraps) {
            wrong.push(name);
        }
    }
    assert!(
        wrong.is_empty(),
        "{} example(s) report a duration measured on a different allocator from the one \
         that ships. Add\n  {INSTALL}\nwith the `#[global_allocator]` attribute above it, or \
         add the example to EXEMPT with a reason:\n{}",
        wrong.len(),
        wrong.iter().map(|n| format!("  examples/{n}.rs")).collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn the_measurement_rule_still_selects_most_of_the_examples() {
    // A guard on the rule itself, not on the examples. If `measurement_examples`
    // ever returns nothing -- a renamed clock API, a refactor behind a helper, a
    // broken path -- the check above passes vacuously and stops protecting
    // anything. The counts at the time of writing: 122 examples, 71 measuring.
    let total = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/examples"))
        .expect("examples/")
        .filter(|e| {
            e.as_ref()
                .map(|e| e.path().extension().and_then(|x| x.to_str()) == Some("rs"))
                .unwrap_or(false)
        })
        .count();
    let measuring = measurement_examples().len();
    assert!(total >= 122, "examples went from 122 to {total}; did the path change?");
    assert!(
        measuring >= 71,
        "the rule selects {measuring} of {total} examples, was 71. If timing moved behind a \
         helper, the rule must follow it -- not shrink."
    );
}

#[test]
fn no_example_asserts_on_a_timing_threshold() {
    // Installing an allocator changes an example's speed, which is the point, but
    // it must not change its behaviour. Nothing in examples/ compares a measured
    // duration against a constant, so no verdict can flip. This check keeps that
    // true: a timing assertion added later is a test whose result depends on the
    // host, and belongs in a bench with a gate, not in an example.
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/examples");
    let mut offenders = Vec::new();
    for entry in std::fs::read_dir(dir).expect("examples/") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let name = path.file_stem().expect("stem").to_string_lossy().to_string();
        for line in code_of(&format!("examples/{name}.rs")).lines() {
            let has_assert = line.contains("assert!") || line.contains("assert_eq!");
            if has_assert && (line.contains("elapsed") || line.contains("_ms") || line.contains("secs_f64")) {
                offenders.push(format!("  examples/{name}.rs: {}", line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "an example asserts on a measured duration; the verdict depends on the host:\n{}",
        offenders.join("\n")
    );
}
