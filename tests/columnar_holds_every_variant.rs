//! Every `PropertyValue` variant survives the columnar path alone (#545).
//!
//! This is the first item on #545's definition of done, and it is a
//! *precondition* for the rest of that issue rather than a change of its own.
//! The plan there is to stop writing the duplicate row copy — 808 MB on LDBC
//! SF1, 217 B/node of pure redundancy — and the reason that could not be done
//! before was:
//!
//! > `ColumnStore::set_property` silently drops anything that is not `Integer`,
//! > `Float`, `String` or `Boolean` — `Array`, `Map`, `DateTime`, vectors.
//! > Those *only* live in the row today, which is why the fallback exists and
//! > why removing it naively would lose them.
//!
//! A `Column::Other` spill was added since. These tests check that the claim is
//! now false, so the next stage rests on a measured fact rather than a reading
//! of the code — and so that if a future change re-narrows the column types,
//! the loss is caught here rather than as missing data after the row copy is
//! gone.
//!
//! They deliberately go through `set_column_property` / `node_columns`, not
//! through `set_node_property`, because the latter writes *both* stores and
//! would pass even if the column dropped everything.

use samyama::graph::{GraphStore, PropertyValue};

/// One of each variant, with values that would survive a lossy round trip only
/// by accident.
fn every_variant() -> Vec<(&'static str, PropertyValue)> {
    let mut map = std::collections::HashMap::new();
    map.insert("inner".to_string(), PropertyValue::Integer(7));
    map.insert("nested".to_string(), PropertyValue::String("deep".into()));

    vec![
        ("integer", PropertyValue::Integer(-42)),
        ("float", PropertyValue::Float(1.5)),
        ("string", PropertyValue::String("Ada".into())),
        ("boolean", PropertyValue::Boolean(true)),
        ("datetime", PropertyValue::DateTime(1_700_000_000_123)),
        (
            "array",
            PropertyValue::Array(vec![
                PropertyValue::Integer(1),
                PropertyValue::String("two".into()),
                PropertyValue::Boolean(false),
            ]),
        ),
        ("map", PropertyValue::Map(map)),
        ("vector", PropertyValue::Vector(vec![0.25, -0.5, 1.0])),
        (
            "duration",
            PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 },
        ),
        // The five temporal types (#689). Added here rather than left out,
        // because this file's whole claim is "every variant" -- adding
        // variants to `PropertyValue` without extending it would quietly turn
        // the name into a lie, and the loss would surface as missing data
        // after #545 removes the row copy, not here.
        //
        // Values chosen so a lossy round trip cannot pass by accident: the
        // nanoseconds do not fit in milliseconds, and the offset is a
        // half-hour one that an hours-only path truncates.
        ("date", PropertyValue::Date(16_637)),
        ("localtime", PropertyValue::LocalTime(45_074_645_876_123)),
        (
            "time",
            PropertyValue::Time { nanos: 45_074_645_876_123, offset_seconds: 19_800 },
        ),
        (
            "localdatetime",
            PropertyValue::LocalDateTime { secs: 1_437_514_832, nanos: 142_000_042 },
        ),
        (
            "zoneddatetime",
            PropertyValue::ZonedDateTime {
                secs: 1_437_514_832,
                nanos: 999_999_999,
                offset_seconds: 3600,
                zone: Some("Europe/London".to_string()),
            },
        ),
    ]
}

#[test]
fn every_variant_round_trips_through_the_column_alone() {
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let idx = id.as_u64() as usize;

    for (key, value) in every_variant() {
        store.set_column_property(id, key, value.clone());
        assert_eq!(
            store.node_columns.get_property(idx, key),
            value,
            "{key} did not survive the columnar path — #545 cannot proceed while this is true"
        );
    }
}

#[test]
fn the_exotic_variants_are_the_ones_that_matter() {
    // Stated separately because these four are the whole reason the row copy
    // is load-bearing. If this passes and the one above passes, the row copy is
    // no longer required for *correctness* of current-value reads.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let idx = id.as_u64() as usize;

    for (key, value) in every_variant() {
        if matches!(
            value,
            PropertyValue::Integer(_)
                | PropertyValue::Float(_)
                | PropertyValue::String(_)
                | PropertyValue::Boolean(_)
        ) {
            continue;
        }
        store.set_column_property(id, key, value.clone());
        let read = store.node_columns.get_property(idx, key);
        assert_eq!(read, value, "{key}");
        assert!(!read.is_null(), "{key} came back null");
    }
}

#[test]
fn a_variant_can_be_overwritten_by_another_of_the_same_type() {
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    let idx = id.as_u64() as usize;

    store.set_column_property(id, "a", PropertyValue::Array(vec![PropertyValue::Integer(1)]));
    store.set_column_property(id, "a", PropertyValue::Array(vec![PropertyValue::Integer(2)]));
    assert_eq!(
        store.node_columns.get_property(idx, "a"),
        PropertyValue::Array(vec![PropertyValue::Integer(2)])
    );
}

#[test]
fn a_typed_column_promotes_rather_than_dropping_a_foreign_value() {
    // The hazard the spill exists for: a column created as `Int` by the first
    // node, then handed an `Array` by the second. Dropping it there is exactly
    // the silent loss #545 is worried about.
    let mut store = GraphStore::new();
    let first = store.create_node("N");
    let second = store.create_node("N");

    store.set_column_property(first, "mixed", PropertyValue::Integer(1));
    store.set_column_property(
        second,
        "mixed",
        PropertyValue::Array(vec![PropertyValue::Integer(9)]),
    );

    assert_eq!(
        store.node_columns.get_property(first.as_u64() as usize, "mixed"),
        PropertyValue::Integer(1),
        "the integer written first was lost when the column promoted"
    );
    assert_eq!(
        store.node_columns.get_property(second.as_u64() as usize, "mixed"),
        PropertyValue::Array(vec![PropertyValue::Integer(9)]),
        "the array was dropped by a column typed as Int"
    );
}

#[test]
fn every_variant_survives_a_snapshot_round_trip() {
    // The other half of #545's definition of done: export reads
    // `node.properties`, so a value that lives only in the column has to reach
    // the snapshot by the merged view. Checked through the public API, since
    // that is what a real graph uses.
    let mut store = GraphStore::new();
    let id = store.create_node("N");
    for (key, value) in every_variant() {
        let _ = store.set_node_property("default", id, key.to_string(), value);
    }

    let mut bytes = Vec::new();
    samyama::snapshot::export_tenant(&store, &mut bytes).expect("export");
    let mut restored = GraphStore::new();
    samyama::snapshot::import_tenant(&mut restored, bytes.as_slice()).expect("import");

    assert_eq!(restored.node_count(), 1);
    let restored_id = restored.all_nodes().first().expect("one node").id;
    for (key, value) in every_variant() {
        assert_eq!(
            restored.node_properties_merged(restored_id).get(key).cloned(),
            Some(value),
            "{key} did not survive the snapshot round trip"
        );
    }
}
