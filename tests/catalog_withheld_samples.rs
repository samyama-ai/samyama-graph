//! A sample value drawn from a row that may not be redistributed is refused
//! for release (#1159 requirement 3, TRUST-03).
//!
//! "Sample values drawn from a derived-only or local-only source must fail the
//! same check -- a sample value is a data excerpt." The check an export fails
//! is `__redistributable = false` on the row, or on a row it was derived from
//! through `DERIVED_FROM` (`samyama::provenance`).

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::provenance::{DERIVED_FROM, REDISTRIBUTABLE};
use samyama::snapshot::verify::{build_catalog, withheld_samples, CatalogEntry, QuerySpec};
use serde_json::json;

/// Two public patients, one marked withheld, and one fact derived from a
/// withheld source. Every name is distinct unless a test makes it shared.
fn ward() -> GraphStore {
    let mut s = GraphStore::new();
    let mut add = |name: &str, redist: Option<PropertyValue>| {
        let mut p = PropertyMap::new();
        p.insert("name".into(), PropertyValue::String(name.into()));
        p.insert("ward".into(), PropertyValue::String("east".into()));
        if let Some(r) = redist {
            p.insert(REDISTRIBUTABLE.into(), r);
        }
        s.create_node_with_properties("default", vec![Label::new("Patient")], p)
    };
    add("Public One", Some(PropertyValue::Boolean(true)));
    add("Unmarked Two", None);
    add("Withheld Three", Some(PropertyValue::Boolean(false)));
    // Text, as a CSV import delivers it, must read as false too.
    add("Withheld Text", Some(PropertyValue::String("false".into())));
    let derived = add("Derived Four", None);
    let mut src = PropertyMap::new();
    src.insert(REDISTRIBUTABLE.into(), PropertyValue::Boolean(false));
    let source = s.create_node_with_properties("default", vec![Label::new("Source")], src);
    s.create_edge(derived, source, DERIVED_FROM).unwrap();
    s
}

fn by_name(sample: &str, enums: Option<Vec<&str>>) -> QuerySpec {
    let mut param = json!({"name": "n", "type": "string", "sample": sample});
    if let Some(e) = enums {
        param["enum_values"] = json!(e);
    }
    serde_json::from_value(json!({
        "id": "by_name",
        "question": "Which ward is {n} in?",
        "difficulty": "easy",
        "cypher": "MATCH (p:Patient) WHERE p.name = $n RETURN p.ward",
        "params": [param]
    }))
    .unwrap()
}

fn entries(s: &GraphStore, q: QuerySpec) -> Vec<CatalogEntry> {
    build_catalog(s, &[q], &[]).unwrap().entries
}

#[test]
fn a_sample_taken_from_a_withheld_row_is_refused_and_named() {
    let s = ward();
    let found = withheld_samples(&s, &entries(&s, by_name("Withheld Three", None)));
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("by_name.params.n"), "{found:?}");
    assert!(found[0].contains("\"Withheld Three\""), "{found:?}");
    assert!(
        found[0].contains("name of node"),
        "names the property and node: {found:?}"
    );

    let found = withheld_samples(&s, &entries(&s, by_name("Withheld Text", None)));
    assert_eq!(
        found.len(),
        1,
        "a textual \"false\" is still false: {found:?}"
    );
}

#[test]
fn a_sample_from_a_row_derived_from_a_withheld_source_is_refused() {
    let s = ward();
    let found = withheld_samples(&s, &entries(&s, by_name("Derived Four", None)));
    assert_eq!(found.len(), 1, "{found:?}");
}

#[test]
fn samples_from_redistributable_or_unmarked_rows_pass() {
    let s = ward();
    for name in ["Public One", "Unmarked Two"] {
        let found = withheld_samples(&s, &entries(&s, by_name(name, None)));
        assert!(found.is_empty(), "{name}: {found:?}");
    }
}

#[test]
fn a_value_some_other_row_also_carries_is_not_an_excerpt_of_the_withheld_one() {
    // Every patient is in the east ward, the withheld ones included. "east"
    // identifies nobody, and it could have come from any of them.
    let s = ward();
    let q: QuerySpec = serde_json::from_value(json!({
        "id": "by_ward",
        "question": "Who is in ward {w}?",
        "difficulty": "easy",
        "cypher": "MATCH (p:Patient) WHERE p.ward = $w RETURN p.name",
        "params": [{"name": "w", "type": "string", "sample": "east"}]
    }))
    .unwrap();
    assert!(withheld_samples(&s, &entries(&s, q)).is_empty());
}

#[test]
fn a_declared_enum_value_is_an_excerpt_too() {
    // The sample is public, but the declared set publishes a withheld name.
    let s = ward();
    let e = entries(
        &s,
        by_name("Public One", Some(vec!["Public One", "Withheld Three"])),
    );
    let found = withheld_samples(&s, &e);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("Withheld Three"), "{found:?}");
}

#[test]
fn provenance_keys_are_not_compared_as_data() {
    // A sample that happens to equal a provenance value (here the text
    // "false" of `__redistributable`) is not an excerpt of the row.
    let s = ward();
    let mut e = entries(&s, by_name("Public One", None));
    e[0].params[0].sample = json!("false");
    assert!(withheld_samples(&s, &e).is_empty());
}
