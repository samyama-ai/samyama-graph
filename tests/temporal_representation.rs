//! The five temporal types: representation, ordering, rendering, and — most
//! importantly — that they survive a snapshot round trip (#689).
//!
//! Every temporal value used to be `PropertyValue::DateTime(i64)`, a Unix
//! timestamp in milliseconds. Cypher has five distinct temporal types and
//! nanosecond precision, so `date()`, `time()`, `localtime()`,
//! `localdatetime()` and `datetime()` were indistinguishable once evaluated,
//! and `12:31:14.645876123` was destroyed at construction rather than merely
//! displayed wrongly.
//!
//! These tests come *before* any temporal function changes, deliberately. The
//! risky part of #689 is not the arithmetic, it is that `PropertyValue` is
//! serialized into the snapshot format: getting the on-disk shape wrong is the
//! one mistake that cannot be fixed by a later commit. Pinning the format
//! first means the function work cannot quietly change it.

use samyama::graph::PropertyValue as P;

/// Round-trip through **serde**, which is the in-memory/wire encoding.
///
/// This is deliberately NOT the snapshot format. The snapshot has its own
/// `__type`-tagged JSON scheme, and a serde round trip can pass while the
/// snapshot silently loses the value — so that scheme is pinned separately, in
/// `src/snapshot/mod.rs`'s own tests, where the two functions actually live.
/// I first wrote this helper's comment claiming it exercised the snapshot path
/// and it did not; the comment was wrong before the code was.
fn serde_round_trip(v: &P) -> P {
    let json = serde_json::to_string(v).expect("serialize");
    serde_json::from_str(&json).expect("deserialize")
}

#[test]
fn every_temporal_type_survives_serialization() {
    let values = vec![
        P::Date(16637),
        P::LocalTime(45_074_645_876_123),
        P::Time { nanos: 45_074_645_876_123, offset_seconds: 3600 },
        P::LocalDateTime { secs: 1_437_514_832, nanos: 142_000_000 },
        P::ZonedDateTime {
            secs: 1_437_514_832,
            nanos: 142_000_000,
            offset_seconds: 3600,
            zone: Some("Europe/London".to_string()),
        },
        P::ZonedDateTime { secs: 0, nanos: 0, offset_seconds: 0, zone: None },
    ];
    for v in values {
        assert_eq!(serde_round_trip(&v), v, "{v:?} did not survive");
    }
}

/// Nanoseconds are the whole point: this value cannot be represented in
/// milliseconds at all, which is why the old type destroyed it.
#[test]
fn nanosecond_precision_is_not_lost() {
    let t = P::LocalTime(45_074_645_876_123);
    assert_eq!(serde_round_trip(&t), t);
    assert_eq!(t.to_cypher_string(), "12:31:14.645876123");

    // The millisecond reading of the same value, for comparison: three of the
    // nine digits survive. That is what every temporal value used to become.
    assert_eq!(t.as_epoch_millis(), Some(45_074_645));
}

/// The legacy variant still deserializes, so snapshots written before this
/// change still load. This is the compatibility promise, and it is a test
/// rather than a comment because nothing else would catch breaking it.
#[test]
fn the_legacy_millisecond_datetime_still_round_trips() {
    let old = P::DateTime(1_437_514_832_142);
    assert_eq!(serde_round_trip(&old), old);
    assert_eq!(old.as_epoch_millis(), Some(1_437_514_832_142));
}

/// openCypher's rendering, which is what `toString()` returns and what the TCK
/// compares against. Trailing zero components are dropped.
#[test]
fn values_render_the_way_cypher_writes_them() {
    assert_eq!(P::Date(16637).to_cypher_string(), "2015-07-21");
    // The issue's own example: `localtime({hour: 10, minute: 35})` is '10:35',
    // not '10:35:00.000000000'.
    assert_eq!(P::LocalTime(38_100_000_000_000).to_cypher_string(), "10:35");
    assert_eq!(P::LocalTime(45_074_000_000_000).to_cypher_string(), "12:31:14");
    assert_eq!(
        P::LocalTime(45_074_645_876_123).to_cypher_string(),
        "12:31:14.645876123"
    );
    assert_eq!(
        P::Time { nanos: 45_074_645_876_123, offset_seconds: 3600 }.to_cypher_string(),
        "12:31:14.645876123+01:00"
    );
    // A half-hour offset, which a naive hours-only formatter gets wrong.
    assert_eq!(
        P::Time { nanos: 0, offset_seconds: 19_800 }.to_cypher_string(),
        "00:00+05:30"
    );
    assert_eq!(
        P::Time { nanos: 0, offset_seconds: -18_000 }.to_cypher_string(),
        "00:00-05:00"
    );
    assert_eq!(
        P::LocalDateTime { secs: 1_437_514_832, nanos: 142_000_000 }.to_cypher_string(),
        "2015-07-21T21:40:32.142"
    );
}

/// A zoned value renders in its own offset, not in UTC, and keeps the IANA
/// name when it has one.
#[test]
fn a_zoned_datetime_renders_in_its_own_offset() {
    let z = P::ZonedDateTime {
        secs: 1_437_514_832,
        nanos: 0,
        offset_seconds: 3600,
        zone: Some("Europe/London".to_string()),
    };
    assert_eq!(z.to_cypher_string(), "2015-07-21T22:40:32+01:00[Europe/London]");

    // Same instant, no named zone: offset only.
    let z2 = P::ZonedDateTime { secs: 1_437_514_832, nanos: 0, offset_seconds: 3600, zone: None };
    assert_eq!(z2.to_cypher_string(), "2015-07-21T22:40:32+01:00");

    // Offset and zone are both carried because neither implies the other; two
    // values differing only in zone must not be equal.
    assert_ne!(z, z2);
}

/// Each type is its own kind. A `Date` and a `LocalTime` holding the same
/// integer are different values, and the index must not collapse them.
#[test]
fn the_types_are_distinct_from_each_other() {
    assert_ne!(P::Date(1), P::LocalTime(1));
    assert_ne!(P::Date(0), P::DateTime(0));
    assert_ne!(
        P::LocalDateTime { secs: 0, nanos: 0 },
        P::ZonedDateTime { secs: 0, nanos: 0, offset_seconds: 0, zone: None }
    );
    assert_eq!(P::Date(1).type_name(), "Date");
    assert_eq!(P::LocalTime(1).type_name(), "LocalTime");
    assert_eq!(P::Time { nanos: 1, offset_seconds: 0 }.type_name(), "Time");
    assert_eq!(P::LocalDateTime { secs: 1, nanos: 0 }.type_name(), "LocalDateTime");
    assert_eq!(
        P::ZonedDateTime { secs: 1, nanos: 0, offset_seconds: 0, zone: None }.type_name(),
        "DateTime"
    );
}

/// Within a type, ordering follows the instant.
#[test]
fn values_of_one_type_sort_chronologically() {
    let mut dates = vec![P::Date(100), P::Date(-5), P::Date(0)];
    dates.sort();
    assert_eq!(dates, vec![P::Date(-5), P::Date(0), P::Date(100)]);

    let mut times = vec![P::LocalTime(2), P::LocalTime(0), P::LocalTime(1)];
    times.sort();
    assert_eq!(times, vec![P::LocalTime(0), P::LocalTime(1), P::LocalTime(2)]);
}

/// A zoned time sorts by the instant it denotes, not by how it is written.
///
/// `12:00+01:00` is 11:00 UTC and so comes *before* `12:00Z`. Comparing the
/// local reading would order these by their text, which looks right and is
/// not.
#[test]
fn zoned_times_sort_by_instant_not_by_local_reading() {
    let noon_plus_one = P::Time { nanos: 12 * 3_600_000_000_000, offset_seconds: 3600 };
    let noon_utc = P::Time { nanos: 12 * 3_600_000_000_000, offset_seconds: 0 };
    assert!(noon_plus_one < noon_utc, "12:00+01:00 is 11:00Z and sorts first");

    let mut v = vec![noon_utc.clone(), noon_plus_one.clone()];
    v.sort();
    assert_eq!(v, vec![noon_plus_one, noon_utc]);
}

/// Two spellings of the same instant compare equal on the instant, and are
/// then split by offset so the order stays strict.
///
/// This is not pedantry: `Ord` backs the B-tree property index, and returning
/// `Equal` for values that `PartialEq` calls different would collapse them
/// into one index key and violate the `Ord`/`Eq` contract.
#[test]
fn the_zoned_ordering_stays_strict() {
    let a = P::ZonedDateTime { secs: 100, nanos: 0, offset_seconds: 0, zone: None };
    let b = P::ZonedDateTime { secs: 100, nanos: 0, offset_seconds: 3600, zone: None };
    assert_ne!(a, b);
    assert_ne!(a.cmp(&b), std::cmp::Ordering::Equal, "must not tie: they are not Eq");
}
