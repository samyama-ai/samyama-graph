//! Every kernel `samyama-gpu` exports has a CPU parity test (HW-09, #1682).
//!
//! The parity tests themselves need `--features gpu` and a device. This check
//! needs neither: it reads the source, so it runs in every test build, and a
//! kernel added without a parity test fails here on any machine.

use std::collections::BTreeSet;
use std::path::Path;

/// Each exported kernel, and the test in `gpu_parity_tests.rs` that holds it to
/// its tolerance. The graph kernels are reached through their algorithm's
/// dispatch, so one test covers all four.
const PARITY_TESTS: &[(&str, &str)] = &[
    ("gpu_page_rank", "cpu_gpu_parity_all_ops"),
    ("gpu_lcc", "cpu_gpu_parity_all_ops"),
    ("gpu_count_triangles", "cpu_gpu_parity_all_ops"),
    ("gpu_cdlp", "cpu_gpu_parity_all_ops"),
    ("gpu_sum_f64", "gpu_sum_matches_the_cpu"),
    ("gpu_argsort_f64", "gpu_argsort_matches_the_cpu"),
    ("gpu_batch_cosine", "gpu_batch_cosine_matches_the_cpu"),
    (
        "gpu_batch_inner_product",
        "gpu_batch_inner_product_matches_the_cpu",
    ),
    ("gpu_pca", "gpu_pca_matches_the_cpu"),
];

/// A kernel is a re-exported function whose name starts with this.
const KERNEL_PREFIX: &str = "gpu_";

/// The GPU crate's root, relative to this crate's manifest.
const GPU_CRATE_LIB: &str = "../samyama-gpu/src/lib.rs";
const PARITY_TEST_FILE: &str = "src/gpu_parity_tests.rs";

fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// The `gpu_*` names on the GPU crate's `pub use` lines.
fn exported_kernels(lib: &str) -> BTreeSet<String> {
    lib.lines()
        .filter(|line| line.trim_start().starts_with("pub use "))
        .flat_map(|line| line.split(|c: char| !(c.is_alphanumeric() || c == '_')))
        .filter(|word| word.starts_with(KERNEL_PREFIX))
        .map(str::to_string)
        .collect()
}

#[test]
fn every_exported_kernel_has_a_parity_test() {
    let exported = exported_kernels(&read(GPU_CRATE_LIB));
    let covered: BTreeSet<String> = PARITY_TESTS.iter().map(|(k, _)| k.to_string()).collect();
    assert_eq!(
        exported, covered,
        "the kernels samyama-gpu exports and the kernels with a parity test differ; \
         add a test to {PARITY_TEST_FILE} and list it here"
    );
}

#[test]
fn every_listed_parity_test_exists() {
    let tests = read(PARITY_TEST_FILE);
    for (kernel, test) in PARITY_TESTS {
        assert!(
            tests.contains(&format!("fn {test}()")),
            "{kernel}: no `fn {test}()` in {PARITY_TEST_FILE}"
        );
    }
}

#[test]
fn exported_kernels_reads_names_from_pub_use_lines_only() {
    let lib = "pub use a::gpu_one;\n\
               pub use b::{gpu_two, GpuThing};\n\
               pub fn gpu_available() -> bool { true }\n\
               // pub use c::gpu_commented;\n";
    let found: Vec<String> = exported_kernels(lib).into_iter().collect();
    assert_eq!(found, ["gpu_one", "gpu_two"]);
}
