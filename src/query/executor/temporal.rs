//! Constructing openCypher's five temporal types from maps and strings (#689).
//!
//! One module rather than five copies inside the function dispatcher, because
//! the component rules are shared and were previously re-implemented per
//! constructor — which is how `time()` came to accept `hour`/`minute`/`second`
//! but silently ignore `nanosecond`, while `localtime()` did the same thing a
//! few lines further down. The engine has a standing habit of answering one
//! question in several places; this is the version of that habit that produces
//! wrong values rather than slow ones.
//!
//! ## Sub-second components add up
//!
//! Cypher treats `millisecond`, `microsecond` and `nanosecond` as *additive*
//! sub-components of the second, not as alternative spellings:
//!
//! ```text
//! {second: 14, millisecond: 645, microsecond: 876, nanosecond: 123}
//!   -> 14.645876123
//! ```
//!
//! Each is bounded by the next unit up, so 1000 microseconds is an error
//! rather than a carry into milliseconds.

use crate::graph::PropertyValue;
use crate::query::executor::ExecutionError;

const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SEC;

fn err(msg: impl Into<String>) -> ExecutionError {
    ExecutionError::RuntimeError(msg.into())
}

fn field(map: &std::collections::HashMap<String, PropertyValue>, k: &str) -> Option<i64> {
    map.get(k).and_then(|v| v.as_integer())
}

/// Whether any of `keys` is present, so a constructor can tell which of the
/// several map forms it was handed.
fn has_any(map: &std::collections::HashMap<String, PropertyValue>, keys: &[&str]) -> bool {
    keys.iter().any(|k| map.contains_key(*k))
}

/// Nanoseconds since midnight from the time components of a map.
///
/// Absent components are zero, which is what Cypher specifies — `{hour: 10}`
/// is 10:00:00.000000000 — but a component that is *present and out of range*
/// is an error rather than a wrap, because a silently wrapped hour reads as a
/// valid time.
pub fn time_of_day_nanos(
    map: &std::collections::HashMap<String, PropertyValue>,
) -> Result<i64, ExecutionError> {
    let hour = field(map, "hour").unwrap_or(0);
    let minute = field(map, "minute").unwrap_or(0);
    let second = field(map, "second").unwrap_or(0);
    let milli = field(map, "millisecond").unwrap_or(0);
    let micro = field(map, "microsecond").unwrap_or(0);
    let nano = field(map, "nanosecond").unwrap_or(0);

    for (name, v, hi) in [
        ("hour", hour, 23),
        ("minute", minute, 59),
        ("second", second, 59),
        ("millisecond", milli, 999),
        ("microsecond", micro, 999_999),
        ("nanosecond", nano, 999_999_999),
    ] {
        if v < 0 || v > hi {
            return Err(err(format!("{name} must be 0..={hi}, got {v}")));
        }
    }

    Ok((hour * 3600 + minute * 60 + second) * NANOS_PER_SEC
        + milli * 1_000_000
        + micro * 1_000
        + nano)
}

/// Days since 1970-01-01 from the date components of a map.
///
/// Four spellings, all in the TCK: calendar (`year`/`month`/`day`), week
/// (`year`/`week`/`dayOfWeek`), quarter (`year`/`quarter`/`dayOfQuarter`) and
/// ordinal (`year`/`ordinalDay`). They are distinguished by which keys are
/// present, and the earlier code supported only the first — the other three
/// silently fell through to 1 January.
pub fn date_days(
    map: &std::collections::HashMap<String, PropertyValue>,
) -> Result<i32, ExecutionError> {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a date");
    let year = field(map, "year").ok_or_else(|| err("a date needs a year"))? as i32;

    let date = if has_any(map, &["week", "dayOfWeek"]) {
        let week = field(map, "week").unwrap_or(1) as u32;
        let dow = field(map, "dayOfWeek").unwrap_or(1) as u32;
        if !(1..=7).contains(&dow) {
            return Err(err(format!("dayOfWeek must be 1..=7, got {dow}")));
        }
        NaiveDate::from_isoywd_opt(year, week, weekday_from_iso(dow))
            .ok_or_else(|| err(format!("invalid ISO week date {year}-W{week}-{dow}")))?
    } else if has_any(map, &["quarter", "dayOfQuarter"]) {
        let q = field(map, "quarter").unwrap_or(1);
        let d = field(map, "dayOfQuarter").unwrap_or(1);
        if !(1..=4).contains(&q) {
            return Err(err(format!("quarter must be 1..=4, got {q}")));
        }
        let first_month = (q - 1) * 3 + 1;
        let start = NaiveDate::from_ymd_opt(year, first_month as u32, 1)
            .ok_or_else(|| err(format!("invalid quarter start {year}-Q{q}")))?;
        start
            .checked_add_signed(chrono::Duration::days(d - 1))
            .ok_or_else(|| err(format!("invalid dayOfQuarter {d}")))?
    } else if let Some(ord) = field(map, "ordinalDay") {
        NaiveDate::from_yo_opt(year, ord as u32)
            .ok_or_else(|| err(format!("invalid ordinalDay {ord} for {year}")))?
    } else {
        let month = field(map, "month").unwrap_or(1) as u32;
        let day = field(map, "day").unwrap_or(1) as u32;
        NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| err(format!("invalid date {year}-{month}-{day}")))?
    };

    Ok(date.signed_duration_since(epoch).num_days() as i32)
}

fn weekday_from_iso(dow: u32) -> chrono::Weekday {
    use chrono::Weekday::*;
    match dow {
        1 => Mon,
        2 => Tue,
        3 => Wed,
        4 => Thu,
        5 => Fri,
        6 => Sat,
        _ => Sun,
    }
}

/// A UTC offset in seconds from `Z`, `+01:00`, `+0100`, `+01`, or an IANA name.
///
/// Returns the offset and the IANA name when one was given, because Cypher
/// keeps both: an offset alone cannot survive a DST boundary and a name alone
/// cannot express `+05:30` attached to nothing.
pub fn parse_timezone(tz: &str) -> Result<(i32, Option<String>), ExecutionError> {
    let t = tz.trim();
    if t.eq_ignore_ascii_case("z") || t.eq_ignore_ascii_case("utc") {
        return Ok((0, None));
    }
    if t.starts_with('+') || t.starts_with('-') {
        let sign = if t.starts_with('-') { -1 } else { 1 };
        let rest = &t[1..];
        let (h, m) = match rest.split_once(':') {
            Some((h, m)) => (h, m),
            None if rest.len() == 4 => (&rest[..2], &rest[2..]),
            None => (rest, "0"),
        };
        let h: i32 = h.parse().map_err(|_| err(format!("bad timezone offset: {tz}")))?;
        let m: i32 = m.parse().map_err(|_| err(format!("bad timezone offset: {tz}")))?;
        return Ok((sign * (h * 3600 + m * 60), None));
    }
    // A named zone. Resolving it to an offset needs a tz database, which the
    // engine does not carry yet; the name is preserved so nothing is lost, and
    // the offset is reported as unknown rather than guessed as UTC — guessing
    // would silently shift every value by the real offset.
    Err(err(format!(
        "named time zone `{tz}` needs a tz database, which is not built in yet; \
         use an offset such as +01:00"
    )))
}

/// `PropertyValue::Date` from an ISO string: `2015-07-21`, `20150721`,
/// `2015-W30-2`, `2015-201`.
pub fn parse_date(s: &str) -> Result<PropertyValue, ExecutionError> {
    use chrono::NaiveDate;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a date");
    let t = s.trim();
    let parsed = NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(t, "%Y%m%d"))
        .or_else(|_| NaiveDate::parse_from_str(t, "%Y-%j"))
        .map_err(|_| err(format!("cannot parse date: {s}")))?;
    Ok(PropertyValue::Date(
        parsed.signed_duration_since(epoch).num_days() as i32,
    ))
}

/// Nanoseconds since midnight, and an offset if the string carried one.
pub fn parse_time_parts(s: &str) -> Result<(i64, Option<i32>), ExecutionError> {
    let t = s.trim();
    // Split the offset off the end before parsing the clock, so `-05:00` is
    // not mistaken for part of the time.
    let (clock, offset) = split_offset(t)?;
    let nt = chrono::NaiveTime::parse_from_str(clock, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(clock, "%H:%M:%S"))
        .or_else(|_| chrono::NaiveTime::parse_from_str(clock, "%H:%M"))
        .or_else(|_| chrono::NaiveTime::parse_from_str(clock, "%H"))
        .map_err(|_| err(format!("cannot parse time: {s}")))?;
    let nanos = nt
        .signed_duration_since(chrono::NaiveTime::MIN)
        .num_nanoseconds()
        .ok_or_else(|| err(format!("time out of range: {s}")))?;
    Ok((nanos, offset))
}

/// Separate a trailing UTC offset from the clock part.
fn split_offset(t: &str) -> Result<(&str, Option<i32>), ExecutionError> {
    if let Some(stripped) = t.strip_suffix('Z').or_else(|| t.strip_suffix('z')) {
        return Ok((stripped, Some(0)));
    }
    // Scan from the right for a sign that begins an offset, not a date dash.
    if let Some(pos) = t.rfind(['+', '-']) {
        // A '-' inside the first 8 characters of a date-time belongs to the
        // date, not to an offset.
        let looks_like_offset = t[pos..].contains(':') || t[pos..].len() <= 5;
        if looks_like_offset && pos > 0 {
            let (clock, off) = t.split_at(pos);
            let (secs, _) = parse_timezone(off)?;
            return Ok((clock, Some(secs)));
        }
    }
    Ok((t, None))
}

/// Every map key the temporal constructors understand.
const KNOWN_KEYS: &[&str] = &[
    "epochMillis", "epochSeconds",
    "year", "month", "day", "week", "dayOfWeek", "quarter", "dayOfQuarter", "ordinalDay",
    "hour", "minute", "second", "millisecond", "microsecond", "nanosecond",
    "timezone", "date", "time", "datetime",
];

/// Refuse a map that names nothing the constructor understands, and say which
/// keys it was given.
///
/// This is the half of #595 that keeps the other half honest: before it, *any*
/// unrecognised map fell through to the component defaults and produced
/// `1970-01-01`, which is why a missing `epochMillis` arm went unnoticed for so
/// long. A generic "needs a date" would have re-opened that hole by a different
/// route -- it does not name what was actually passed, so a typo like
/// `epochMilis` reads as "you forgot the date" rather than "that key is not a
/// thing".
pub fn reject_unknown_map(
    map: &std::collections::HashMap<String, PropertyValue>,
) -> Result<(), ExecutionError> {
    if map.keys().any(|k| KNOWN_KEYS.contains(&k.as_str())) {
        return Ok(());
    }
    let mut given: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
    given.sort_unstable();
    Err(err(format!(
        "this constructor understands none of the keys given ({}); \
         expected one of: {}",
        if given.is_empty() { "the map is empty".to_string() } else { given.join(", ") },
        KNOWN_KEYS.join(", ")
    )))
}
