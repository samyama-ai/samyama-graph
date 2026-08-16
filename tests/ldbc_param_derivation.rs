//! Runs the LDBC parameter-derivation tests (#505).
//!
//! The derivation lives in `benches/ldbc_common/params.rs` because it is
//! benchmark scaffolding and has no business in the engine — the rule is that
//! benchmark identifiers do not appear in engine source. But every `[[bench]]`
//! in this repo sets `harness = false`, which means `cargo test --bench` runs
//! the benchmark's `main` instead of its `#[test]` functions: unit tests
//! written inside a bench never execute.
//!
//! Including the module here puts it in a normal test target, so its own
//! `#[cfg(test)] mod tests` compiles and runs under `cargo test` like anything
//! else. The end-to-end test below then checks the derivation against real
//! data when the dataset happens to be present.

#[path = "../benches/ldbc_common/params.rs"]
mod params;

use std::path::PathBuf;

/// The SF1 extract this repo ships with, when it has been downloaded.
fn sf1_dir() -> Option<PathBuf> {
    let dir = PathBuf::from("data/ldbc-sf1/social_network-sf1-CsvBasic-LongDateFormatter");
    dir.join("dynamic/person_knows_person_0_0.csv")
        .exists()
        .then_some(dir)
}

#[test]
fn derives_a_usable_parameter_set_from_the_shipped_sf1_extract() {
    // A clean host has no LDBC data. That means this test did not run, which
    // is not the same as it failing.
    let Some(dir) = sf1_dir() else {
        eprintln!("SKIP: LDBC SF1 extract not present");
        return;
    };

    let d = params::derive(&dir, 50).expect("derivation should succeed on a complete extract");

    // Every parameter must resolve against *this* extract. The failure this
    // guards is the one that has now happened three times: parameters that
    // belong to a different datagen run, producing a suite that reports
    // "passed" while measuring nothing (#449, #450, #502).
    assert!(d.person_id > 0, "anchor person must be a real id");
    assert_ne!(d.person2_id, d.person_id, "path queries need two distinct people");
    assert!(!d.first_name.is_empty());
    assert!(!d.tag_name.is_empty());
    assert!(!d.country_x.is_empty());
    assert!(!d.tag_class_name.is_empty());
    assert!(!d.organisation_name.is_empty());
    assert!(d.start_date < d.end_date, "the date window must be non-empty");
    assert!(d.end_date <= d.max_date, "maxDate must not exclude the window");
}

#[test]
fn the_median_anchor_is_not_the_busiest_person() {
    // The point of #505. Deriving by "pick the most connected person" chose a
    // 104x degree outlier at SF10; every friend-traversal query inherited it,
    // and the resulting numbers were not comparable with a competitor running
    // LDBC's own parameters.
    let Some(dir) = sf1_dir() else {
        eprintln!("SKIP: LDBC SF1 extract not present");
        return;
    };

    let d = params::derive(&dir, 50).unwrap();
    let p = &d.provenance;

    assert_eq!(p.person_degree, p.median_degree, "p50 must land on the median degree");
    assert!(
        p.person_degree < p.max_degree,
        "the median must be below the maximum ({} vs {}) or this dataset has no skew to avoid",
        p.person_degree,
        p.max_degree
    );
}

#[test]
fn a_higher_percentile_gives_a_heavier_but_still_stated_workload() {
    let Some(dir) = sf1_dir() else {
        eprintln!("SKIP: LDBC SF1 extract not present");
        return;
    };

    let median = params::derive(&dir, 50).unwrap();
    let p90 = params::derive(&dir, 90).unwrap();

    assert!(
        p90.provenance.person_degree >= median.provenance.person_degree,
        "p90 degree {} should be at least the p50 degree {}",
        p90.provenance.person_degree,
        median.provenance.person_degree
    );
    assert_eq!(p90.provenance.percentile, 90);
    assert!(p90.provenance.format().contains("p90"));
}

#[test]
fn derivation_is_deterministic() {
    // Two runs of the benchmark must ask the same question, or their timings
    // cannot be compared with each other -- let alone with another engine.
    let Some(dir) = sf1_dir() else {
        eprintln!("SKIP: LDBC SF1 extract not present");
        return;
    };

    let a = params::derive(&dir, 50).unwrap();
    let b = params::derive(&dir, 50).unwrap();

    assert_eq!(a.person_id, b.person_id);
    assert_eq!(a.person2_id, b.person2_id);
    assert_eq!(a.first_name, b.first_name);
    assert_eq!(a.tag_name, b.tag_name);
    assert_eq!(a.country_x, b.country_x);
    assert_eq!(a.country_y, b.country_y);
    assert_eq!(a.tag_class_name, b.tag_class_name);
    assert_eq!(a.organisation_name, b.organisation_name);
    assert_eq!((a.start_date, a.end_date, a.max_date), (b.start_date, b.end_date, b.max_date));
}

#[test]
fn the_written_json_round_trips_into_the_benchmark_and_carries_its_origin() {
    let Some(dir) = sf1_dir() else {
        eprintln!("SKIP: LDBC SF1 extract not present");
        return;
    };

    let d = params::derive(&dir, 50).unwrap();
    let json = d.to_json();

    // Parseable by the same reader `--params-file` uses.
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("derived JSON must parse");
    assert_eq!(parsed["personId"], d.person_id);
    assert_eq!(parsed["firstName"], d.first_name);
    assert_eq!(parsed["organisationName"], d.organisation_name);

    // A parameter file that outlives the run that made it has to say where it
    // came from, or it becomes the next set of unattributed magic numbers.
    let provenance = parsed["_provenance"].as_array().expect("_provenance block");
    assert!(
        provenance.iter().any(|l| l.as_str().is_some_and(|s| s.contains("p50"))),
        "the percentile must survive into the file: {json}"
    );
}

#[test]
fn a_directory_without_ldbc_csvs_is_an_error_not_a_panic() {
    let err = params::derive(std::path::Path::new("/nonexistent-ldbc-extract"), 50)
        .expect_err("a missing extract must be reported, not unwrapped");
    assert!(err.contains("person_knows_person"), "the error should name what it looked for: {err}");
}
