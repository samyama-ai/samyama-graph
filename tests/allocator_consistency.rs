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
