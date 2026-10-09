//! The health-systems loader must write values, not just structure (#1609, #1815).
//!
//! Both defects were published in `s3://samyama-data/snapshots/health-systems.sgsnap`
//! with the right node count (19,661), the right edge count (19,428) and the
//! right label set. A row-count test cannot see either one, so these are
//! value-distribution tests.
//!
//! **#1609 — a column shift.** `parse_csv_records` split each line on `,` with
//! no regard for quoting. WHO SPAR capacity C1 is named
//! `"C1 - Policy, legal and normative instruments"` -- the only one of the 15
//! capacity names containing a comma -- so for those rows every field right of
//! the name moved one place left: `year` landed in `score` and the name's tail
//! landed in `year`, where it failed to parse and was dropped. All 562 C01
//! nodes therefore scored 2021..=2023 with no year, against 0..=100 everywhere
//! else, and C1's real score was lost.
//!
//! **#1815 — structure with no content.** `Country.income_level` and
//! `Country.who_region` were present on all 233 nodes and non-empty on none,
//! because the upstream WHO country list carries neither. An always-present,
//! always-empty column is worse than an absent one: `exists(c.who_region)`
//! returns true and grouping by region yields one empty bucket.

use std::fs;
use std::path::Path;

use samyama_sdk::{GraphStore, PropertyValue};

#[path = "../examples/common/health_systems_common.rs"]
mod health_systems_common;

#[path = "../examples/common/value_distribution.rs"]
mod value_distribution;

/// A fixture in the shape of the real data: one capacity name with a comma in
/// it, one without, and a country list whose region/income columns are present
/// in the JSON and empty in every row -- which is what
/// `data/who_spar/countries.json` actually holds (233 entries, 0 non-empty).
fn fixture(dir: &Path) {
    let spar = dir.join("who_spar");
    fs::create_dir_all(&spar).unwrap();
    fs::write(
        spar.join("countries.json"),
        r#"[
          {"iso_code": "IND", "name": "India", "who_region": "", "income_level": ""},
          {"iso_code": "KEN", "name": "Kenya", "who_region": "", "income_level": ""},
          {"iso_code": "NOR", "name": "Norway", "who_region": "  ", "income_level": ""}
        ]"#,
    )
    .unwrap();
    fs::write(
        spar.join("spar.csv"),
        concat!(
            "country_code,capacity_code,capacity_name,year,score\n",
            // The comma-bearing capacity name, quoted, as WHO publishes it.
            "IND,C01,\"C1 - Policy, legal and normative instruments\",2021,47\n",
            "KEN,C01,\"C1 - Policy, legal and normative instruments\",2023,40\n",
            "NOR,C01,\"C1 - Policy, legal and normative instruments\",2022,100\n",
            // A capacity whose name has no comma: the control.
            "IND,C03,C3 - Financing,2021,60\n",
            "KEN,C03,C3 - Financing,2023,20\n",
            // A doubled quote inside a quoted field is one literal quote.
            "NOR,C05,\"C5 - \"\"Surveillance\"\", broadly\",2022,80\n",
            // No parsable year: the row has no identity, so it is dropped
            // rather than minted with unparsed text in its id.
            "IND,C07,C7 - Health emergency management,n/a,55\n",
        ),
    )
    .unwrap();
}

fn load() -> GraphStore {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path());
    let mut graph = GraphStore::new();
    health_systems_common::load_dataset(&mut graph, dir.path()).unwrap();
    graph
}

#[test]
fn a_quoted_comma_does_not_shift_the_columns() {
    let fields = health_systems_common::split_csv_line(
        "IND,C01,\"C1 - Policy, legal and normative instruments\",2021,47",
    );
    assert_eq!(
        fields,
        vec![
            "IND",
            "C01",
            "C1 - Policy, legal and normative instruments",
            "2021",
            "47"
        ],
        "a naive split(',') yields six fields here and moves year into score"
    );

    let doubled = health_systems_common::split_csv_line("a,\"b \"\"c\"\", d\",e");
    assert_eq!(doubled, vec!["a", "b \"c\", d", "e"]);

    // The unquoted case must be unchanged, including an empty trailing field.
    assert_eq!(
        health_systems_common::split_csv_line("a,b,,"),
        vec!["a", "b", "", ""]
    );
}

#[test]
fn no_emergency_response_score_exceeds_one_hundred() {
    let graph = load();
    let range = value_distribution::numeric_range(&graph, "EmergencyResponse", "score")
        .expect("some assessment must carry a score");
    assert!(
        range.0 >= 0.0 && range.1 <= 100.0,
        "SPAR scores are percentages; observed {:?}. A range topping out in the \
         2020s means a year is sitting in the score column (#1609)",
        range
    );
}

#[test]
fn every_capacity_scores_on_the_same_scale() {
    let graph = load();
    // C01 is the capacity the shift hit; C03's name has no comma, so it was
    // always right. Both must land in the same 0..=100 range.
    for cap in ["C01", "C03", "C05"] {
        let range = value_distribution::numeric_range_for_group(
            &graph,
            "EmergencyResponse",
            "score",
            "capacity_code",
            cap,
        )
        .unwrap_or_else(|| panic!("{cap} carries no score at all"));
        assert!(
            range.0 >= 0.0 && range.1 <= 100.0,
            "{cap} scores {:?}, off the 0..=100 scale its siblings use",
            range
        );
    }
}

#[test]
fn every_emergency_response_carries_a_year() {
    let graph = load();
    let missing = value_distribution::missing_property(&graph, "EmergencyResponse", "year");
    assert_eq!(
        missing, 0,
        "{missing} assessments have no year. The year is part of the node's \
         identity, so a row without a parsable one must be dropped, not minted \
         with unparsed text in its id (#1609)"
    );
}

#[test]
fn the_shifted_capacity_keeps_its_real_score_and_name() {
    let graph = load();
    let mut seen = Vec::new();
    for node in graph.get_nodes_by_label(&"EmergencyResponse".into()) {
        let is_c01 = matches!(node.get_property("capacity_code"), Some(PropertyValue::String(s)) if s == "C01");
        if !is_c01 {
            continue;
        }
        let score = match node.get_property("score") {
            Some(PropertyValue::Integer(i)) => *i,
            other => panic!("C01 score is {other:?}"),
        };
        let year = match node.get_property("year") {
            Some(PropertyValue::Integer(i)) => *i,
            other => panic!("C01 year is {other:?}"),
        };
        let name = match node.get_property("capacity_name") {
            Some(PropertyValue::String(s)) => s.clone(),
            other => panic!("C01 capacity_name is {other:?}"),
        };
        assert_eq!(
            name, "C1 - Policy, legal and normative instruments",
            "the name was truncated at its comma"
        );
        seen.push((year, score));
    }
    seen.sort_unstable();
    assert_eq!(seen, vec![(2021, 47), (2022, 100), (2023, 40)]);
}

#[test]
fn a_row_with_no_parsable_year_is_dropped_rather_than_half_written() {
    let graph = load();
    let c07 = value_distribution::numeric_range_for_group(
        &graph,
        "EmergencyResponse",
        "score",
        "capacity_code",
        "C07",
    );
    assert_eq!(
        c07, None,
        "the C07 fixture row has year 'n/a'; it must not reach the graph"
    );
}

#[test]
fn a_property_written_for_every_node_is_non_empty_for_at_least_one() {
    let graph = load();
    // Generalises #1815. The same measurement catches SIDER's
    // `SideEffect.name` (5,805 values, 2 distinct) and any future loader that
    // writes a column it has no values for.
    for label in ["Country", "EmergencyResponse"] {
        let violations = value_distribution::empty_on_every_node(&graph, label);
        assert!(
            violations.is_empty(),
            "a column present on every node and empty on every node reads as \
             data and is not: {violations:?}"
        );
    }
}

#[test]
fn absent_upstream_columns_are_not_written_at_all() {
    let graph = load();
    let (nodes, stats) = value_distribution::property_stats(&graph, "Country");
    assert_eq!(nodes, 3);
    for key in ["who_region", "income_level"] {
        assert!(
            !stats.contains_key(key),
            "the fixture carries no {key} value, so no Country node may carry \
             the key; exists(c.{key}) must answer false, not true-with-nothing \
             (#1815). Observed: {:?}",
            stats.get(key)
        );
    }
    // What the source does carry is still written.
    assert_eq!(stats["name"].non_empty, 3);
    assert_eq!(stats["iso_code"].non_empty, 3);
}

#[test]
fn a_text_column_is_not_one_value_repeated() {
    let graph = load();
    // SIDER's shape: 5,805 `SideEffect.name` values, 2 distinct. Here the
    // capacity name must vary with the capacity, not collapse to one string.
    let violations = value_distribution::too_few_distinct_values(&graph, "EmergencyResponse", 3, 1);
    assert!(
        violations.is_empty(),
        "a text column with one distinct value over many nodes carries no \
         information: {violations:?}"
    );
}

// ── The checks themselves must be able to fail ────────────────────────────────
//
// The Rust loader already guarded against writing an empty `who_region`, so
// `a_property_written_for_every_node_is_non_empty_for_at_least_one` and
// `a_text_column_is_not_one_value_repeated` pass against the pre-fix loader
// too. These two reconstruct the published defects directly, so the checks are
// shown to detect them rather than merely to be satisfied.

#[test]
fn the_empty_column_check_catches_the_published_shape() {
    // What `s3://samyama-data/snapshots/health-systems.sgsnap` holds today:
    // income_level and who_region on all 233 Country nodes, non-empty on 0.
    let mut graph = GraphStore::new();
    for iso in ["IND", "KEN", "NOR"] {
        let id = graph.create_node("Country");
        let n = graph.get_node_mut(id).unwrap();
        n.set_property("iso_code", PropertyValue::String(iso.to_string()));
        n.set_property("who_region", PropertyValue::String(String::new()));
        n.set_property("income_level", PropertyValue::String("  ".to_string()));
    }
    let mut violations = value_distribution::empty_on_every_node(&graph, "Country");
    violations.sort();
    assert_eq!(
        violations,
        vec![
            "Country.income_level: present on 3/3 nodes, non-empty on 0",
            "Country.who_region: present on 3/3 nodes, non-empty on 0",
        ]
    );
}

#[test]
fn the_distinct_value_check_catches_the_sider_shape() {
    // SIDER: `SideEffect.name` present on 5,805 nodes, 2 distinct values --
    // `PT` and `LLT`, the MedDRA term-type column written where the term
    // belongs (#1815).
    let mut graph = GraphStore::new();
    for i in 0..20 {
        let id = graph.create_node("SideEffect");
        let n = graph.get_node_mut(id).unwrap();
        n.set_property("umls_cui", PropertyValue::String(format!("C{i:07}")));
        let term_type = if i % 3 == 0 { "PT" } else { "LLT" };
        n.set_property("name", PropertyValue::String(term_type.to_string()));
    }
    let violations = value_distribution::too_few_distinct_values(&graph, "SideEffect", 10, 2);
    assert_eq!(
        violations,
        vec!["SideEffect.name: 20 values, 2 distinct"],
        "umls_cui is 20 distinct over 20 nodes and must not be flagged"
    );
}
