//! The typed-value codec of the `.sgsnap` format (#1907).
//!
//! JSON has strings, numbers, booleans, null, lists and maps. A
//! `PropertyValue` also has dates, times, datetimes, durations and vectors,
//! and those are written as an object carrying a `__type` tag:
//! `{"__type": "Date", "days": 19723}`. The list of tags and their fields is
//! the format's contract, and it is written down in
//! `docs/ADR/ADR-022-snapshot-format.md` under "Typed values".
//!
//! Public so that a converter writing or reading `.sgsnap` outside this
//! module (`samyama convert`, #1905) calls this rather than copying it. These
//! two functions lived privately in `snapshot/mod.rs`, so any other reader or
//! writer had to reproduce the encoding, and a copy drifts.
//!
//! One ambiguity is inherent and kept: a user's map property that happens to
//! hold a `__type` key naming one of these tags, with the matching fields, is
//! read back as that typed value.

use std::collections::HashMap;

use crate::graph::property::PropertyValue;

/// Encode a property value as it is written into a `.sgsnap` record.
///
/// Plain JSON for string, number, boolean, null, list and map; a `__type`
/// object for everything JSON has no type for. The tags are listed in
/// ADR-022 under "Typed values".
pub fn property_to_json(pv: &PropertyValue) -> serde_json::Value {
    match pv {
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::Integer(i) => serde_json::json!(i),
        // JSON has no infinity or NaN, and `json!` writes them as `null`, so
        // the value was lost without a word. They are tagged instead, in
        // XML Schema's spellings.
        PropertyValue::Float(f) if !f.is_finite() => {
            serde_json::json!({"__type": "Float", "value": non_finite_name(*f)})
        }
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::Boolean(b) => serde_json::json!(b),
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::DateTime(dt) => {
            // Wrap in an object to distinguish from plain integer
            serde_json::json!({"__type": "DateTime", "value": dt})
        }
        // The five real temporal types (#689). Same `__type` scheme as
        // DateTime/Vector/Duration, and each carries its own components rather
        // than an epoch, because an epoch cannot represent a `Date` without a
        // zone or a `LocalTime` without a date. `DateTime` above stays exactly
        // as it was so snapshots written before this still read back.
        PropertyValue::Date(days) => serde_json::json!({"__type": "Date", "days": days}),
        PropertyValue::LocalTime(nanos) => {
            serde_json::json!({"__type": "LocalTime", "nanos": nanos})
        }
        PropertyValue::Time { nanos, offset_seconds } => {
            serde_json::json!({"__type": "Time", "nanos": nanos, "offset": offset_seconds})
        }
        PropertyValue::LocalDateTime { secs, nanos } => {
            serde_json::json!({"__type": "LocalDateTime", "secs": secs, "nanos": nanos})
        }
        PropertyValue::ZonedDateTime { secs, nanos, offset_seconds, zone } => {
            serde_json::json!({
                "__type": "ZonedDateTime",
                "secs": secs, "nanos": nanos,
                "offset": offset_seconds, "zone": zone
            })
        }
        PropertyValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(property_to_json).collect())
        }
        PropertyValue::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), property_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        PropertyValue::Vector(v) => {
            serde_json::json!({"__type": "Vector", "value": v})
        }
        PropertyValue::Duration {
            months,
            days,
            seconds,
            nanos,
        } => {
            serde_json::json!({
                "__type": "Duration",
                "months": months,
                "days": days,
                "seconds": seconds,
                "nanos": nanos
            })
        }
    }
}

/// `INF`, `-INF` or `NaN`, as XML Schema spells a non-finite double.
fn non_finite_name(f: f64) -> &'static str {
    if f.is_nan() {
        "NaN"
    } else if f > 0.0 {
        "INF"
    } else {
        "-INF"
    }
}

/// Decode a value written by [`property_to_json`].
///
/// Strings are trimmed, as import always has. A `__type` object whose
/// required field is missing or of the wrong type is read as a plain map
/// rather than defaulted, so a damaged record does not turn into the epoch --
/// except `Duration`, whose four fields each default to zero.
pub fn json_to_property(val: &serde_json::Value) -> PropertyValue {
    match val {
        serde_json::Value::String(s) => PropertyValue::String(s.trim().to_string()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Array(arr) => {
            PropertyValue::Array(arr.iter().map(json_to_property).collect())
        }
        serde_json::Value::Object(obj) => {
            // Check for tagged types (__type field)
            if let Some(type_tag) = obj.get("__type").and_then(|v| v.as_str()) {
                match type_tag {
                    "DateTime" => {
                        if let Some(dt) = obj.get("value").and_then(|v| v.as_i64()) {
                            return PropertyValue::DateTime(dt);
                        }
                    }
                    "Date" => {
                        if let Some(d) = obj.get("days").and_then(|v| v.as_i64()) {
                            return PropertyValue::Date(d as i32);
                        }
                    }
                    "LocalTime" => {
                        if let Some(n) = obj.get("nanos").and_then(|v| v.as_i64()) {
                            return PropertyValue::LocalTime(n);
                        }
                    }
                    "Time" => {
                        if let Some(n) = obj.get("nanos").and_then(|v| v.as_i64()) {
                            return PropertyValue::Time {
                                nanos: n,
                                offset_seconds: obj.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                            };
                        }
                    }
                    "LocalDateTime" => {
                        if let Some(sec) = obj.get("secs").and_then(|v| v.as_i64()) {
                            return PropertyValue::LocalDateTime {
                                secs: sec,
                                nanos: obj.get("nanos").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                            };
                        }
                    }
                    "ZonedDateTime" => {
                        if let Some(sec) = obj.get("secs").and_then(|v| v.as_i64()) {
                            return PropertyValue::ZonedDateTime {
                                secs: sec,
                                nanos: obj.get("nanos").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                                offset_seconds: obj.get("offset").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                                zone: obj.get("zone").and_then(|v| v.as_str()).map(|z| z.to_string()),
                            };
                        }
                    }
                    "Vector" => {
                        if let Some(arr) = obj.get("value").and_then(|v| v.as_array()) {
                            let floats: Vec<f32> = arr
                                .iter()
                                .filter_map(|v| v.as_f64().map(|f| f as f32))
                                .collect();
                            return PropertyValue::Vector(floats);
                        }
                    }
                    "Float" => {
                        let value = match obj.get("value").and_then(|v| v.as_str()) {
                            Some("INF") => Some(f64::INFINITY),
                            Some("-INF") => Some(f64::NEG_INFINITY),
                            Some("NaN") => Some(f64::NAN),
                            _ => None,
                        };
                        if let Some(f) = value {
                            return PropertyValue::Float(f);
                        }
                    }
                    "Duration" => {
                        let months = obj
                            .get("months")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        let days =
                            obj.get("days").and_then(|v| v.as_i64()).unwrap_or(0);
                        let seconds = obj
                            .get("seconds")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        let nanos = obj
                            .get("nanos")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32;
                        return PropertyValue::Duration {
                            months,
                            days,
                            seconds,
                            nanos,
                        };
                    }
                    _ => {}
                }
            }
            // Plain map (no __type tag)
            let map: HashMap<String, PropertyValue> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_property(v)))
                .collect();
            PropertyValue::Map(map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One value of every variant.
    ///
    /// The `match` below has no wildcard, so a variant added to
    /// `PropertyValue` stops this compiling until it has a value here, and
    /// with it a round trip through the codec.
    fn one_of_each() -> Vec<PropertyValue> {
        let values = vec![
            PropertyValue::String("Ada".into()),
            PropertyValue::Integer(-42),
            PropertyValue::Float(2.5),
            PropertyValue::Boolean(true),
            PropertyValue::Null,
            PropertyValue::DateTime(1_437_514_832_142),
            PropertyValue::Date(19_723),
            PropertyValue::LocalTime(45_296_000_000_123),
            PropertyValue::Time { nanos: 45_296_000_000_000, offset_seconds: 19_800 },
            PropertyValue::LocalDateTime { secs: 1_700_000_000, nanos: 7 },
            PropertyValue::ZonedDateTime {
                secs: 1_700_000_000,
                nanos: 9,
                offset_seconds: -18_000,
                zone: Some("America/New_York".into()),
            },
            PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Date(2)]),
            PropertyValue::Map(
                [
                    ("k".to_string(), PropertyValue::String("v".into())),
                    ("when".to_string(), PropertyValue::Date(3)),
                ]
                .into_iter()
                .collect(),
            ),
            PropertyValue::Vector(vec![0.25, -1.5, 3.0]),
            PropertyValue::Duration { months: 14, days: 3, seconds: 3_600, nanos: 5 },
        ];
        for v in &values {
            match v {
                PropertyValue::String(_)
                | PropertyValue::Integer(_)
                | PropertyValue::Float(_)
                | PropertyValue::Boolean(_)
                | PropertyValue::DateTime(_)
                | PropertyValue::Date(_)
                | PropertyValue::LocalTime(_)
                | PropertyValue::Time { .. }
                | PropertyValue::LocalDateTime { .. }
                | PropertyValue::ZonedDateTime { .. }
                | PropertyValue::Array(_)
                | PropertyValue::Map(_)
                | PropertyValue::Vector(_)
                | PropertyValue::Duration { .. }
                | PropertyValue::Null => {}
            }
        }
        values
    }

    #[test]
    fn every_variant_round_trips() {
        let values = one_of_each();
        assert_eq!(values.len(), 15, "one per variant");
        for v in values {
            let encoded = property_to_json(&v);
            assert_eq!(json_to_property(&encoded), v, "via {encoded}");
        }
    }

    #[test]
    fn the_round_trip_survives_serialization_to_text() {
        // The codec's output is written as text and parsed back; a value that
        // round-trips only as a `serde_json::Value` would still be lost.
        for v in one_of_each() {
            let text = serde_json::to_string(&property_to_json(&v)).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(json_to_property(&parsed), v, "via {text}");
        }
    }

    /// Infinities and NaN survive the codec. As plain JSON they were written
    /// as `null` and read back as no value at all.
    #[test]
    fn non_finite_floats_round_trip() {
        for f in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let json = property_to_json(&PropertyValue::Float(f));
            let text = serde_json::to_string(&json).unwrap();
            let back = json_to_property(&serde_json::from_str(&text).unwrap());
            match back {
                PropertyValue::Float(g) => {
                    assert!(g == f || (f.is_nan() && g.is_nan()), "{f}: {text}")
                }
                other => panic!("{f}: {text} read back as {other:?}"),
            }
        }
        assert_eq!(property_to_json(&PropertyValue::Float(2.5)), serde_json::json!(2.5));
    }
}
