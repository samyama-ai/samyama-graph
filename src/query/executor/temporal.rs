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

/// What a `timezone:` field named: a fixed offset, or an IANA zone.
///
/// They are different things and cannot be collapsed. A **fixed offset** is a
/// constant. A **named zone** has no single offset — `Europe/Stockholm` is
/// +01:00 in October and +02:00 in July — so its offset is only knowable once
/// the local date is known, which is why resolution is a separate step from
/// parsing (#767).
#[derive(Debug, Clone, PartialEq)]
pub enum TzSpec {
    Offset(i32),
    Named(chrono_tz::Tz),
}

/// Parse `Z`, `+01:00`, `+0100`, `+01`, `+02:05:59`, or `Europe/Stockholm`.
pub fn parse_timezone_spec(tz: &str) -> Result<TzSpec, ExecutionError> {
    let t = tz.trim();
    if t.eq_ignore_ascii_case("z") || t.eq_ignore_ascii_case("utc") {
        return Ok(TzSpec::Offset(0));
    }
    if t.starts_with('+') || t.starts_with('-') {
        let sign = if t.starts_with('-') { -1 } else { 1 };
        let rest = &t[1..];
        let parts: Vec<&str> = rest.split(':').collect();
        let (h, m, sec) = match parts.as_slice() {
            [h] if h.len() == 4 => (&h[..2], &h[2..], "0"),
            [h] => (*h, "0", "0"),
            [h, m] => (*h, *m, "0"),
            // `+02:05:59` — an offset with seconds, which the TCK uses.
            [h, m, sec] => (*h, *m, *sec),
            _ => return Err(err(format!("bad timezone offset: {tz}"))),
        };
        let p = |x: &str| x.parse::<i32>().map_err(|_| err(format!("bad timezone offset: {tz}")));
        return Ok(TzSpec::Offset(sign * (p(h)? * 3600 + p(m)? * 60 + p(sec)?)));
    }
    t.parse::<chrono_tz::Tz>()
        .map(TzSpec::Named)
        .map_err(|_| err(format!("unknown time zone: {tz}")))
}

/// The UTC offset this spec has at a given **local** wall-clock instant.
///
/// A local time can be ambiguous (the hour repeated when clocks go back) or
/// non-existent (the hour skipped when they go forward). Cypher resolves both
/// toward the earlier offset, which is what `LocalResult::earliest` gives;
/// picking arbitrarily would make one hour a year silently wrong.
pub fn resolve_offset(spec: &TzSpec, local_days: i64, local_nanos: i64) -> Result<i32, ExecutionError> {
    match spec {
        TzSpec::Offset(o) => Ok(*o),
        TzSpec::Named(tz) => {
            use chrono::TimeZone;
            let naive = chrono::DateTime::from_timestamp(
                local_days * 86_400 + local_nanos.div_euclid(NANOS_PER_SEC),
                local_nanos.rem_euclid(NANOS_PER_SEC) as u32,
            )
            .ok_or_else(|| err("date-time out of range"))?
            .naive_utc();
            let resolved = tz
                .from_local_datetime(&naive)
                .earliest()
                .or_else(|| tz.from_local_datetime(&naive).latest())
                .ok_or_else(|| err(format!("{tz} has no offset for {naive}")))?;
            use chrono::Offset as _;
            Ok(resolved.offset().fix().local_minus_utc())
        }
    }
}

/// The IANA name, when the spec has one.
pub fn zone_name(spec: &TzSpec) -> Option<String> {
    match spec {
        TzSpec::Named(tz) => Some(tz.name().to_string()),
        TzSpec::Offset(_) => None,
    }
}

/// Back-compatible shim for callers with no date in hand.
///
/// A named zone still needs a date to have an offset, so this resolves it
/// against 1970-01-01 — correct for a fixed offset, and an approximation for a
/// named zone that the date-bearing constructors do not use. Kept narrow on
/// purpose: `time()` is the only caller, because a time of day genuinely has
/// no date to resolve against.
pub fn parse_timezone(tz: &str) -> Result<(i32, Option<String>), ExecutionError> {
    let spec = parse_timezone_spec(tz)?;
    Ok((resolve_offset(&spec, 0, 0)?, zone_name(&spec)))
}

/// `PropertyValue::Date` from an ISO string, in any spelling.
pub fn parse_date(s: &str) -> Result<PropertyValue, ExecutionError> {
    Ok(PropertyValue::Date(parse_iso_date(s)?))
}

/// Nanoseconds since midnight, and an offset if the string carried one.
pub fn parse_time_parts(s: &str) -> Result<(i64, Option<i32>), ExecutionError> {
    let (clock, offset) = split_offset(s.trim())?;
    Ok((parse_iso_time(clock)?, offset))
}

/// Separate a trailing UTC offset from the clock part.
///
/// Care is needed with `-`: in `2015-07-21T21:40:32-04` the offset dash is the
/// last one, but in `2015-07-21` every dash belongs to the date. The rule used
/// here is that an offset dash must come after a `T` when one is present, and
/// otherwise after the time has started.
fn split_offset(t: &str) -> Result<(&str, Option<i32>), ExecutionError> {
    if let Some(stripped) = t.strip_suffix('Z').or_else(|| t.strip_suffix('z')) {
        return Ok((stripped, Some(0)));
    }
    let search_from = t.rfind('T').map(|i| i + 1).unwrap_or(0);
    if let Some(rel) = t[search_from..].rfind(['+', '-']) {
        let pos = search_from + rel;
        if pos > 0 {
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

/// `<type>.truncate(unit, value, map)` — round a temporal down to a unit, then
/// apply the map's components as overrides.
///
/// The namespace decides the *result* type, not the input type: `date.truncate`
/// over a `datetime` returns a `Date`. That is why this takes the target
/// separately rather than reading it off the value.
///
/// Units coarser than a day zero the clock and move the date to the start of
/// the period; units finer than a day keep the date and zero everything below
/// the unit. `week` goes to Monday, and `weekYear` to the first day of the ISO
/// week-year — which is not 1 January, and is the one that a "just zero the
/// smaller fields" implementation silently gets wrong.
pub fn truncate(
    target: &str,
    unit: &str,
    value: &PropertyValue,
    overrides: &std::collections::HashMap<String, PropertyValue>,
) -> Result<PropertyValue, ExecutionError> {
    use chrono::{Datelike, NaiveDate};

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a date");
    let (days, tod, offset, zone) = decompose(value)?;
    let date = epoch
        .checked_add_signed(chrono::Duration::days(days.unwrap_or(0)))
        .ok_or_else(|| err("date out of range"))?;

    // How much of the clock survives, and where the date lands.
    let (new_date, mut new_tod) = match unit.to_ascii_lowercase().as_str() {
        "millennium" => (ymd(date.year() - date.year().rem_euclid(1000), 1, 1)?, 0),
        "century" => (ymd(date.year() - date.year().rem_euclid(100), 1, 1)?, 0),
        "decade" => (ymd(date.year() - date.year().rem_euclid(10), 1, 1)?, 0),
        "year" => (ymd(date.year(), 1, 1)?, 0),
        "weekyear" => (
            NaiveDate::from_isoywd_opt(date.iso_week().year(), 1, chrono::Weekday::Mon)
                .ok_or_else(|| err("week-year out of range"))?,
            0,
        ),
        "quarter" => (ymd(date.year(), (date.month() - 1) / 3 * 3 + 1, 1)?, 0),
        "month" => (ymd(date.year(), date.month(), 1)?, 0),
        "week" => (
            date - chrono::Duration::days(date.weekday().num_days_from_monday() as i64),
            0,
        ),
        "day" => (date, 0),
        "hour" => (date, tod.unwrap_or(0) / 3_600_000_000_000 * 3_600_000_000_000),
        "minute" => (date, tod.unwrap_or(0) / 60_000_000_000 * 60_000_000_000),
        "second" => (date, tod.unwrap_or(0) / NANOS_PER_SEC * NANOS_PER_SEC),
        "millisecond" => (date, tod.unwrap_or(0) / 1_000_000 * 1_000_000),
        "microsecond" => (date, tod.unwrap_or(0) / 1_000 * 1_000),
        other => return Err(err(format!("unknown truncation unit: {other}"))),
    };

    // Overrides are applied *after* truncation, so `{day: 2}` on a millennium
    // truncation gives 2000-01-02 rather than being rounded away.
    let mut new_days = new_date.signed_duration_since(epoch).num_days();
    if !overrides.is_empty() {
        let mut y = new_date.year();
        let mut m = new_date.month();
        let mut d = new_date.day();
        if let Some(v) = field(overrides, "year") { y = v as i32; }
        if let Some(v) = field(overrides, "month") { m = v as u32; }
        if let Some(v) = field(overrides, "day") { d = v as u32; }
        if overrides.contains_key("dayOfWeek") {
            let want = field(overrides, "dayOfWeek").unwrap_or(1) as i64;
            let base = ymd(y, m, d)?;
            let cur = base.weekday().num_days_from_monday() as i64;
            new_days = (base + chrono::Duration::days(want - 1 - cur))
                .signed_duration_since(epoch)
                .num_days();
        } else {
            new_days = ymd(y, m, d)?.signed_duration_since(epoch).num_days();
        }
        // Clock overrides add on top of what truncation left.
        for (key, mult) in [
            ("hour", 3_600_000_000_000i64),
            ("minute", 60_000_000_000),
            ("second", NANOS_PER_SEC),
            ("millisecond", 1_000_000),
            ("microsecond", 1_000),
            ("nanosecond", 1),
        ] {
            if let Some(v) = field(overrides, key) {
                let unit_span = match key {
                    "hour" => 86_400 * NANOS_PER_SEC,
                    "minute" => 3_600_000_000_000,
                    "second" => 60_000_000_000,
                    _ => mult * 1_000,
                };
                // Replace that field rather than adding to it.
                let below = new_tod % mult;
                let above = new_tod / unit_span * unit_span;
                new_tod = above + v * mult + below;
            }
        }
    }

    build(target, new_days, new_tod, offset, zone)
}

fn ymd(y: i32, m: u32, d: u32) -> Result<chrono::NaiveDate, ExecutionError> {
    chrono::NaiveDate::from_ymd_opt(y, m, d).ok_or_else(|| err(format!("invalid date {y}-{m}-{d}")))
}

/// Split any temporal into (days, time-of-day, offset, zone), each optional.
#[allow(clippy::type_complexity)]
fn decompose(
    v: &PropertyValue,
) -> Result<(Option<i64>, Option<i64>, Option<i32>, Option<String>), ExecutionError> {
    Ok(match v {
        PropertyValue::Date(d) => (Some(*d as i64), None, None, None),
        PropertyValue::LocalTime(n) => (None, Some(*n), None, None),
        PropertyValue::Time { nanos, offset_seconds } => (None, Some(*nanos), Some(*offset_seconds), None),
        PropertyValue::LocalDateTime { secs, nanos } => (
            Some(secs.div_euclid(86_400)),
            Some(secs.rem_euclid(86_400) * NANOS_PER_SEC + *nanos as i64),
            None,
            None,
        ),
        PropertyValue::ZonedDateTime { secs, nanos, offset_seconds, zone } => {
            let local = secs + *offset_seconds as i64;
            (
                Some(local.div_euclid(86_400)),
                Some(local.rem_euclid(86_400) * NANOS_PER_SEC + *nanos as i64),
                Some(*offset_seconds),
                zone.clone(),
            )
        }
        other => return Err(err(format!("not a temporal value: {}", other.type_name()))),
    })
}

/// Assemble the type the namespace asked for.
fn build(
    target: &str,
    days: i64,
    tod: i64,
    offset: Option<i32>,
    zone: Option<String>,
) -> Result<PropertyValue, ExecutionError> {
    Ok(match target {
        "date" => PropertyValue::Date(days as i32),
        "localtime" => PropertyValue::LocalTime(tod.rem_euclid(NANOS_PER_DAY)),
        "time" => PropertyValue::Time {
            nanos: tod.rem_euclid(NANOS_PER_DAY),
            offset_seconds: offset.unwrap_or(0),
        },
        "localdatetime" => PropertyValue::LocalDateTime {
            secs: days * 86_400 + tod.div_euclid(NANOS_PER_SEC),
            nanos: tod.rem_euclid(NANOS_PER_SEC) as u32,
        },
        "datetime" => {
            let off = offset.unwrap_or(0);
            PropertyValue::ZonedDateTime {
                secs: days * 86_400 + tod.div_euclid(NANOS_PER_SEC) - off as i64,
                nanos: tod.rem_euclid(NANOS_PER_SEC) as u32,
                offset_seconds: off,
                zone,
            }
        }
        other => return Err(err(format!("cannot truncate to {other}"))),
    })
}


/// Split a trailing `[Area/City]` zone suffix off an ISO string.
///
/// `2015-07-21T21:40:32.142+02:00[Europe/Stockholm]` carries **both** an offset
/// and a zone, and they are not redundant: the offset is what the value had
/// when it was written, the zone is the rule it follows. Cypher keeps both, so
/// the parser must not discard either.
pub fn split_zone_suffix(s: &str) -> (&str, Option<&str>) {
    let t = s.trim();
    if let Some(open) = t.rfind('[') {
        if t.ends_with(']') {
            return (&t[..open], Some(&t[open + 1..t.len() - 1]));
        }
    }
    (t, None)
}

/// A date in any ISO-8601 spelling the TCK uses, as days since the epoch.
///
/// Six forms, and the compact ones are not decoration — `20150721`,
/// `2015W302` and `2015202` all appear:
///
/// ```text
/// 2015-07-21   20150721     calendar, extended and compact
/// 2015-W30-2   2015W302     ISO week date
/// 2015-202     2015202      ordinal day
/// ```
///
/// Years may also be signed and wider than four digits (`-999999999-01-01`),
/// which is why the year is scanned rather than taken as a fixed slice.
pub fn parse_iso_date(s: &str) -> Result<i32, ExecutionError> {
    use chrono::{Datelike, NaiveDate};
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let t = s.trim();
    if t.is_empty() {
        return Err(err("empty date"));
    }

    // Signed, variable-width year.
    let (sign, rest) = match t.strip_prefix('-') {
        Some(r) => (-1i64, r),
        None => (1i64, t.strip_prefix('+').unwrap_or(t)),
    };
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.len() < 4 {
        return Err(err(format!("cannot parse date: {s}")));
    }
    // A compact form packs everything into one digit run; an extended form
    // stops at four.
    let (year_str, tail) = if digits.len() >= 8 && !rest[digits.len()..].starts_with('-') {
        (&digits[..4], &rest[4..])
    } else {
        (&digits[..digits.len().min(if rest.len() > digits.len() { digits.len() } else { 4 })], &rest[digits.len().min(if rest.len() > digits.len() { digits.len() } else { 4 })..])
    };
    let year = sign * year_str.parse::<i64>().map_err(|_| err(format!("bad year in {s}")))?;
    let year = i32::try_from(year).map_err(|_| err(format!("year out of range in {s}")))?;
    let tail = tail.trim_start_matches('-');

    let date = if tail.is_empty() {
        NaiveDate::from_ymd_opt(year, 1, 1)
    } else if let Some(w) = tail.strip_prefix('W').or_else(|| tail.strip_prefix('w')) {
        let w = w.replace('-', "");
        let week: u32 = w[..2].parse().map_err(|_| err(format!("bad week in {s}")))?;
        let dow: u32 = if w.len() > 2 { w[2..3].parse().unwrap_or(1) } else { 1 };
        NaiveDate::from_isoywd_opt(year, week, weekday_from_iso(dow))
    } else {
        let d = tail.replace('-', "");
        match d.len() {
            // Ordinal day: three digits.
            3 => NaiveDate::from_yo_opt(year, d.parse().map_err(|_| err("bad ordinal"))?),
            // Month only.
            2 => NaiveDate::from_ymd_opt(year, d.parse().map_err(|_| err("bad month"))?, 1),
            4 => NaiveDate::from_ymd_opt(
                year,
                d[..2].parse().map_err(|_| err("bad month"))?,
                d[2..].parse().map_err(|_| err("bad day"))?,
            ),
            _ => return Err(err(format!("cannot parse date: {s}"))),
        }
    };
    let date = date.ok_or_else(|| err(format!("invalid date: {s}")))?;
    Ok(date.signed_duration_since(epoch).num_days() as i32)
}

/// A time of day in any ISO-8601 spelling, as nanoseconds since midnight.
///
/// `21:40:32.142`, `214032.142`, `2140`, `21`. The fraction may use a comma,
/// which ISO-8601 permits and the TCK uses.
pub fn parse_iso_time(s: &str) -> Result<i64, ExecutionError> {
    let t = s.trim().replace(',', ".");
    if t.is_empty() {
        return Err(err("empty time"));
    }
    let (clock, frac) = match t.split_once('.') {
        Some((c, f)) => (c.to_string(), f.to_string()),
        None => (t.clone(), String::new()),
    };
    let c = clock.replace(':', "");
    if !c.chars().all(|ch| ch.is_ascii_digit()) || c.is_empty() {
        return Err(err(format!("cannot parse time: {s}")));
    }
    let take = |a: usize, b: usize| -> i64 {
        c.get(a..b).and_then(|x| x.parse().ok()).unwrap_or(0)
    };
    let (h, m, sec) = match c.len() {
        1 | 2 => (take(0, c.len()), 0, 0),
        3 | 4 => (take(0, 2), take(2, c.len()), 0),
        5 | 6 => (take(0, 2), take(2, 4), take(4, c.len())),
        _ => return Err(err(format!("cannot parse time: {s}"))),
    };
    if h > 24 || m > 59 || sec > 59 {
        return Err(err(format!("time out of range: {s}")));
    }
    // Pad or trim the fraction to exactly nine digits.
    let nanos: i64 = if frac.is_empty() {
        0
    } else {
        let mut f = frac.chars().filter(|c| c.is_ascii_digit()).collect::<String>();
        f.truncate(9);
        while f.len() < 9 {
            f.push('0');
        }
        f.parse().unwrap_or(0)
    };
    Ok((h * 3600 + m * 60 + sec) * NANOS_PER_SEC + nanos)
}


/// Split a date-time string into its clock part and a written UTC offset.
///
/// Public because the `datetime()` string form needs the same dash rule the
/// time parser uses: in `2015-07-21T21:40:32-04` the offset dash is the last
/// one *after the `T`*, while in `2015-07-21` every dash belongs to the date.
pub fn parse_datetime_offset(t: &str) -> Result<(&str, Option<i32>), ExecutionError> {
    split_offset(t)
}
