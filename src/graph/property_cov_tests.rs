//! Coverage-focused tests for `PropertyValue`: the temporal variants' ordering,
//! hashing and rendering, and the rarely used accessors.

use super::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

fn hash_of(v: &PropertyValue) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

fn zoned(secs: i64, offset_seconds: i32, zone: Option<&str>) -> PropertyValue {
    PropertyValue::ZonedDateTime {
        secs,
        nanos: 0,
        offset_seconds,
        zone: zone.map(str::to_string),
    }
}

fn map(pairs: &[(&str, PropertyValue)]) -> PropertyValue {
    PropertyValue::Map(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

#[test]
fn cypher_order_puts_maps_before_lists_and_nan_after_numbers() {
    use PropertyValue::*;
    assert_eq!(cypher_order(&map(&[]), &Array(vec![])), Ordering::Less);
    assert_eq!(cypher_order(&Array(vec![]), &map(&[])), Ordering::Greater);
    assert_eq!(
        cypher_order(&Float(f64::NAN), &Float(1.0)),
        Ordering::Greater
    );
    assert_eq!(cypher_order(&Float(1.0), &Float(f64::NAN)), Ordering::Less);
    assert_eq!(
        cypher_order(&Float(f64::NAN), &Float(f64::NAN)),
        Ordering::Equal
    );
    assert_eq!(cypher_order(&Float(f64::NAN), &Null), Ordering::Less);
}

#[test]
fn ord_separates_every_temporal_type_into_its_own_bucket() {
    use PropertyValue::*;
    // Ascending by bucket, as documented on `Ord`.
    let ladder = vec![
        Boolean(true),
        Integer(1),
        String("s".into()),
        DateTime(0),
        Date(0),
        LocalTime(0),
        Time {
            nanos: 0,
            offset_seconds: 0,
        },
        LocalDateTime { secs: 0, nanos: 0 },
        zoned(0, 0, None),
        Array(vec![]),
        map(&[]),
        Vector(vec![]),
        Duration {
            months: 0,
            days: 0,
            seconds: 0,
            nanos: 0,
        },
        Null,
    ];
    for w in ladder.windows(2) {
        assert_eq!(w[0].cmp(&w[1]), Ordering::Less, "{:?} < {:?}", w[0], w[1]);
        assert_eq!(w[1].cmp(&w[0]), Ordering::Greater);
    }
}

#[test]
fn ord_within_each_temporal_type() {
    use PropertyValue::*;
    assert!(LocalTime(1) < LocalTime(2));
    assert!(DateTime(5) > DateTime(4));
    // 12:00+01:00 is 11:00Z, before 12:00Z.
    let noon = 12 * 3600 * 1_000_000_000i64;
    assert!(
        Time {
            nanos: noon,
            offset_seconds: 3600
        } < Time {
            nanos: noon,
            offset_seconds: 0
        }
    );
    assert!(LocalDateTime { secs: 1, nanos: 5 } < LocalDateTime { secs: 1, nanos: 6 });
    assert!(LocalDateTime { secs: 1, nanos: 9 } < LocalDateTime { secs: 2, nanos: 0 });
    // The same instant written two ways: equal on time, split by offset then zone.
    assert!(zoned(100, 0, None) < zoned(100, 3600, None));
    assert!(zoned(100, 0, None) < zoned(100, 0, Some("UTC")));
    assert_eq!(
        zoned(100, 0, Some("UTC")).cmp(&zoned(100, 0, Some("UTC"))),
        Ordering::Equal
    );
    assert!(zoned(99, 7200, None) < zoned(100, 0, None));
}

#[test]
fn ord_compares_maps_by_keys_then_values() {
    use PropertyValue::*;
    let a1 = map(&[("a", Integer(1)), ("b", Integer(1))]);
    let a2 = map(&[("a", Integer(1)), ("b", Integer(2))]);
    assert_eq!(a1.cmp(&a2), Ordering::Less);
    assert_eq!(a2.cmp(&a1), Ordering::Greater);
    assert_eq!(a1.cmp(&a1.clone()), Ordering::Equal);
    // Key sets decide before any value is looked at.
    assert_eq!(
        map(&[("a", Integer(9))]).cmp(&map(&[("b", Integer(0))])),
        Ordering::Less
    );
    assert_eq!(
        map(&[("a", Integer(9))]).cmp(&a1),
        Ordering::Less,
        "prefix key set is shorter"
    );
}

#[test]
fn hash_is_consistent_for_temporal_values_and_distinguishes_types() {
    use PropertyValue::*;
    let values = vec![
        Date(10),
        LocalTime(10),
        Time {
            nanos: 10,
            offset_seconds: 60,
        },
        LocalDateTime { secs: 10, nanos: 1 },
        zoned(10, 60, Some("Europe/Paris")),
    ];
    for v in &values {
        assert_eq!(hash_of(v), hash_of(&v.clone()), "{v:?}");
    }
    let set: HashSet<PropertyValue> = values
        .iter()
        .cloned()
        .chain(values.iter().cloned())
        .collect();
    assert_eq!(set.len(), values.len());
    assert_ne!(hash_of(&Date(10)), hash_of(&LocalTime(10)));
    assert_ne!(
        hash_of(&zoned(10, 60, None)),
        hash_of(&zoned(10, 60, Some("UTC")))
    );
}

#[test]
fn approx_heap_bytes_walks_containers() {
    use PropertyValue::*;
    assert_eq!(Integer(1).approx_heap_bytes(), 0);
    let s = std::string::String::with_capacity(16);
    assert_eq!(String(s).approx_heap_bytes(), 16);
    let v: Vec<f32> = Vec::with_capacity(4);
    assert_eq!(Vector(v).approx_heap_bytes(), 16);
    let mut items = Vec::with_capacity(2);
    items.push(String(std::string::String::with_capacity(8)));
    let arr = Array(items);
    assert_eq!(
        arr.approx_heap_bytes(),
        2 * std::mem::size_of::<PropertyValue>() + 8
    );
    let key = std::string::String::from("k");
    let expected_map = key.capacity() + std::mem::size_of::<PropertyValue>() + 16;
    let m = Map([(key, String(std::string::String::with_capacity(16)))]
        .into_iter()
        .collect());
    assert_eq!(m.approx_heap_bytes(), expected_map);
}

#[test]
fn accessors_for_less_common_variants() {
    use PropertyValue::*;
    assert_eq!(
        Vector(vec![1.5, 2.0]).as_list_items(),
        Some(vec![Float(1.5), Float(2.0)])
    );
    assert_eq!(Integer(1).as_list_items(), None);
    assert_eq!(DateTime(42).as_datetime(), Some(42));
    assert_eq!(Integer(42).as_datetime(), None);
    assert_eq!(Date(0).type_name(), "Date");
    assert_eq!(LocalTime(0).type_name(), "LocalTime");
    assert_eq!(
        Time {
            nanos: 0,
            offset_seconds: 0
        }
        .type_name(),
        "Time"
    );
    assert_eq!(
        LocalDateTime { secs: 0, nanos: 0 }.type_name(),
        "LocalDateTime"
    );
    assert_eq!(zoned(0, 0, None).type_name(), "DateTime");
}

#[test]
fn to_json_renders_temporal_values_as_iso_strings() {
    assert_eq!(
        PropertyValue::Date(1).to_json(),
        serde_json::json!("1970-01-02")
    );
    assert_eq!(
        PropertyValue::LocalDateTime {
            secs: 3600,
            nanos: 0
        }
        .to_json(),
        serde_json::json!("1970-01-01T01:00")
    );
}

#[test]
fn time_and_offset_rendering() {
    let half_past = 12 * 3600 * 1_000_000_000i64 + 500_000_000;
    assert_eq!(
        PropertyValue::LocalTime(half_past).to_cypher_string(),
        "12:00:00.5"
    );
    assert_eq!(
        PropertyValue::LocalTime(9 * 3600 * 1_000_000_000).to_cypher_string(),
        "09:00"
    );
    assert_eq!(
        PropertyValue::Time {
            nanos: 0,
            offset_seconds: -3600
        }
        .to_cypher_string(),
        "00:00-01:00"
    );
    assert_eq!(fmt_offset(3661), "+01:01:01");
    assert_eq!(fmt_offset(-5400), "-01:30");
    assert_eq!(fmt_offset(0), "Z");
}

#[test]
fn out_of_range_dates_are_reported_not_clamped() {
    assert!(fmt_date(i32::MAX).starts_with("<date out of range"));
    assert!(PropertyValue::LocalDateTime {
        secs: i64::MAX,
        nanos: 0
    }
    .to_cypher_string()
    .starts_with("<datetime out of range"));
}

#[test]
fn zoned_date_time_renders_offset_and_zone() {
    assert_eq!(
        zoned(0, 3600, Some("Europe/Paris")).to_cypher_string(),
        "1970-01-01T01:00+01:00[Europe/Paris]"
    );
    assert_eq!(zoned(0, 0, None).to_cypher_string(), "1970-01-01T00:00Z");
    // Display goes through the same rendering.
    assert_eq!(zoned(0, 0, None).to_string(), "1970-01-01T00:00Z");
}

#[test]
fn duration_rendering_through_to_cypher_string() {
    let d = |months, days, seconds, nanos| PropertyValue::Duration {
        months,
        days,
        seconds,
        nanos,
    };
    assert_eq!(d(0, 0, 5, 0).to_cypher_string(), "PT5S");
    assert_eq!(d(14, 3, 3725, 0).to_cypher_string(), "P1Y2M3DT1H2M5S");
    assert_eq!(d(0, 0, -1, -500_000_000).to_cypher_string(), "PT-1.5S");
    assert_eq!(d(0, 0, 0, 0).to_cypher_string(), "PT0S");
    // Non-temporal values fall back to Display.
    assert_eq!(PropertyValue::Integer(5).to_cypher_string(), "5");
}

#[test]
fn duration_display_names_each_component() {
    let d = PropertyValue::Duration {
        months: 14,
        days: 3,
        seconds: 3725,
        nanos: 0,
    };
    assert_eq!(d.to_string(), "P1Y2M3DT1H2M5S");
    let months_only = PropertyValue::Duration {
        months: 12,
        days: 0,
        seconds: 0,
        nanos: 0,
    };
    assert_eq!(months_only.to_string(), "P1Y");
    let frac = PropertyValue::Duration {
        months: 0,
        days: 0,
        seconds: 0,
        nanos: 5,
    };
    assert_eq!(frac.to_string(), "PT0S");
}

#[test]
fn epoch_millis_for_every_temporal_variant() {
    use PropertyValue::*;
    assert_eq!(DateTime(1234).as_epoch_millis(), Some(1234));
    assert_eq!(Date(2).as_epoch_millis(), Some(2 * 86_400_000));
    assert_eq!(LocalTime(3_000_000).as_epoch_millis(), Some(3));
    assert_eq!(
        Time {
            nanos: 3_600_000_000_000,
            offset_seconds: 3600
        }
        .as_epoch_millis(),
        Some(0)
    );
    assert_eq!(
        LocalDateTime {
            secs: 2,
            nanos: 5_000_000
        }
        .as_epoch_millis(),
        Some(2005)
    );
    assert_eq!(zoned(1, 0, None).as_epoch_millis(), Some(1000));
    assert_eq!(Integer(1).as_epoch_millis(), None);
}

#[test]
fn display_separates_map_entries() {
    let m = map(&[
        ("a", PropertyValue::Integer(1)),
        ("b", PropertyValue::Integer(2)),
    ]);
    let text = m.to_string();
    assert!(text == "{a: 1, b: 2}" || text == "{b: 2, a: 1}", "{text}");
}

#[test]
fn from_collections() {
    let arr: PropertyValue = vec![PropertyValue::Integer(1)].into();
    assert_eq!(arr, PropertyValue::Array(vec![PropertyValue::Integer(1)]));
    let mut hm = HashMap::new();
    hm.insert("k".to_string(), PropertyValue::Boolean(true));
    let m: PropertyValue = hm.clone().into();
    assert_eq!(m, PropertyValue::Map(hm));
}
