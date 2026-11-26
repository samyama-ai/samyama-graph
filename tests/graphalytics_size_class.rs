//! LDBC Graphalytics size classes, as the Graphalytics runner computes them
//! (ALGO-08, #1687).
//!
//! # What was wrong
//!
//! `benches/graphalytics_benchmark.rs` picked "S" datasets from a hand-kept
//! list: `wiki-Talk`, `cit-Patents`, `datagen-7_5-fb`. By the Graphalytics
//! definition, scale = `log10(vertices + edges)`, those are 2XS, XS and S. Two
//! of the three "S" graphs were not S. There was no M at all, and no way to say
//! which class a run covered.
//!
//! The class is now computed from each dataset's own `.properties` counts, and
//! a run reports `scale_covered`: the largest class whose every dataset
//! validated every algorithm it defines. These tests pin that arithmetic. The
//! runner is a benchmark, which `cargo test` does not run, so the shared module
//! is included here.

#[allow(dead_code)]
#[path = "../benches/graphalytics_common/mod.rs"]
mod graphalytics_common;

use graphalytics_common::{class_rank, parse_properties, scale, scale_covered, size_class};

/// Vertex and edge counts from the datasets' published `.properties` files.
const PUBLISHED: &[(&str, u64, u64, Option<&str>)] = &[
    ("example-directed", 10, 17, None),
    ("wiki-Talk", 2_394_385, 5_021_410, Some("2XS")),
    ("cit-Patents", 3_774_768, 16_518_948, Some("XS")),
    ("datagen-7_5-fb", 633_432, 34_185_747, Some("S")),
    ("graph500-22", 2_396_657, 64_155_735, Some("S")),
    ("datagen-8_0-fb", 1_706_561, 107_507_376, Some("M")),
    ("graph500-23", 4_610_222, 129_333_677, Some("M")),
];

#[test]
fn each_published_dataset_lands_in_its_ldbc_class() {
    for (name, v, e, class) in PUBLISHED {
        assert_eq!(
            size_class(scale(*v, *e)),
            *class,
            "{name}: scale {}",
            scale(*v, *e)
        );
    }
}

#[test]
fn the_old_s_list_held_one_s_graph() {
    let old_s = ["wiki-Talk", "cit-Patents", "datagen-7_5-fb"];
    let classes: Vec<_> = PUBLISHED
        .iter()
        .filter(|(n, ..)| old_s.contains(n))
        .map(|(_, v, e, _)| size_class(scale(*v, *e)))
        .collect();
    assert_eq!(classes, [Some("2XS"), Some("XS"), Some("S")]);
}

#[test]
fn the_scale_is_log10_of_vertices_plus_edges_to_one_decimal() {
    assert_eq!(scale(633_432, 34_185_747), 7.5); // log10 = 7.5418
    assert_eq!(scale(1_706_561, 107_507_376), 8.0); // 8.0383
    assert_eq!(scale(4_610_222, 129_333_677), 8.1); // 8.1269
    assert_eq!(scale(0, 0), 0.0, "an empty graph does not take log10(0)");
}

#[test]
fn class_boundaries_belong_to_the_upper_class() {
    assert_eq!(size_class(6.4), None, "below 2XS");
    assert_eq!(size_class(6.5), Some("2XS"));
    assert_eq!(size_class(7.4), Some("XS"));
    assert_eq!(size_class(7.5), Some("S"));
    assert_eq!(size_class(7.9), Some("S"));
    assert_eq!(size_class(8.0), Some("M"));
    assert_eq!(size_class(8.4), Some("M"));
    assert_eq!(size_class(8.5), Some("L"));
    assert_eq!(size_class(9.9), Some("2XL"));
    assert_eq!(size_class(10.0), None, "above 2XL");
}

#[test]
fn a_class_name_is_found_in_any_case() {
    assert_eq!(class_rank("s"), class_rank("S"));
    assert!(class_rank("M") > class_rank("S"));
    assert_eq!(class_rank("XXL"), None);
}

#[test]
fn scale_covered_is_the_largest_class_whose_every_dataset_validated() {
    let (s, m) = (Some("S"), Some("M"));
    assert_eq!(
        scale_covered(&[(s, true), (s, true), (m, true), (m, true)]),
        Some("M")
    );
    // One failing M dataset uncovers M, however many others pass.
    assert_eq!(
        scale_covered(&[(s, true), (m, true), (m, false)]),
        Some("S")
    );
    assert_eq!(scale_covered(&[(s, false), (m, false)]), None);
    // The example graphs are below every class and cover none.
    assert_eq!(scale_covered(&[(None, true)]), None);
    assert_eq!(scale_covered(&[]), None);
}

#[test]
fn properties_give_the_counts_and_the_algorithms_a_dataset_defines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.properties");
    std::fs::write(
        &path,
        "# a comment\n\
         graph.g.meta.vertices = 2396657\n\
         graph.g.meta.edges = 64155735\n\
         graph.g.directed = false\n\
         graph.g.algorithms = bfs, cdlp, lcc, pr, wcc\n",
    )
    .unwrap();
    let p = parse_properties(&path);
    assert_eq!(
        (p.vertices, p.edges, p.directed),
        (Some(2_396_657), Some(64_155_735), Some(false))
    );
    // An unweighted graph defines no SSSP, so a missing SSSP reference is not
    // a failure for it.
    assert_eq!(
        p.algorithms.as_deref(),
        Some(&["BFS", "CDLP", "LCC", "PR", "WCC"].map(String::from)[..])
    );
}
