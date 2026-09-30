//! Unit tests for the temporal constructors, parsers and `truncate`.

use super::*;
use chrono::NaiveDate;
use std::collections::HashMap;

fn map(pairs: &[(&str, i64)]) -> HashMap<String, PropertyValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), PropertyValue::Integer(*v)))
        .collect()
}

fn days(y: i32, m: u32, d: u32) -> i64 {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days()
}

fn hms(h: i64, m: i64, s: i64) -> i64 {
    (h * 3600 + m * 60 + s) * NANOS_PER_SEC
}

fn msg(e: ExecutionError) -> String {
    match e {
        ExecutionError::RuntimeError(m) => m,
        other => panic!("expected RuntimeError, got {other:?}"),
    }
}

/// 2017-11-11T12:31:14.645876123 (a Saturday).
fn sample_ldt() -> PropertyValue {
    PropertyValue::LocalDateTime {
        secs: days(2017, 11, 11) * 86_400 + 12 * 3600 + 31 * 60 + 14,
        nanos: 645_876_123,
    }
}

fn trunc_date(unit: &str) -> i64 {
    match truncate("date", unit, &sample_ldt(), &HashMap::new()).unwrap() {
        PropertyValue::Date(d) => d as i64,
        other => panic!("expected a date, got {other:?}"),
    }
}

fn trunc_tod(unit: &str) -> i64 {
    match truncate("localtime", unit, &sample_ldt(), &HashMap::new()).unwrap() {
        PropertyValue::LocalTime(n) => n,
        other => panic!("expected a localtime, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// time_of_day_nanos
// ---------------------------------------------------------------------------

#[test]
fn sub_second_components_add_up() {
    let m = map(&[
        ("hour", 10),
        ("minute", 5),
        ("second", 3),
        ("millisecond", 645),
        ("microsecond", 876),
        ("nanosecond", 123),
    ]);
    assert_eq!(time_of_day_nanos(&m).unwrap(), hms(10, 5, 3) + 645_876_123);
}

#[test]
fn absent_time_components_are_zero() {
    assert_eq!(time_of_day_nanos(&HashMap::new()).unwrap(), 0);
    assert_eq!(
        time_of_day_nanos(&map(&[("hour", 10)])).unwrap(),
        hms(10, 0, 0)
    );
}

#[test]
fn out_of_range_time_components_are_errors_not_wraps() {
    assert_eq!(
        msg(time_of_day_nanos(&map(&[("hour", 24)])).unwrap_err()),
        "hour must be 0..=23, got 24"
    );
    assert_eq!(
        msg(time_of_day_nanos(&map(&[("minute", -1)])).unwrap_err()),
        "minute must be 0..=59, got -1"
    );
    assert!(
        msg(time_of_day_nanos(&map(&[("microsecond", 1_000_000)])).unwrap_err())
            .starts_with("microsecond must be")
    );
}

// ---------------------------------------------------------------------------
// date_days
// ---------------------------------------------------------------------------

#[test]
fn calendar_date_defaults_month_and_day_to_one() {
    assert_eq!(date_days(&map(&[("year", 1970)])).unwrap(), 0);
    assert_eq!(
        date_days(&map(&[("year", 2015), ("month", 7), ("day", 21)])).unwrap() as i64,
        days(2015, 7, 21)
    );
}

#[test]
fn a_date_needs_a_year() {
    assert_eq!(
        msg(date_days(&map(&[("month", 1)])).unwrap_err()),
        "a date needs a year"
    );
}

#[test]
fn invalid_calendar_date_is_an_error() {
    assert!(
        msg(date_days(&map(&[("year", 2015), ("month", 2), ("day", 30)])).unwrap_err())
            .contains("invalid date 2015-2-30")
    );
}

#[test]
fn week_dates_map_every_iso_weekday() {
    // ISO week 1 of 1970 starts on Monday 1969-12-29.
    for dow in 1..=7 {
        let d = date_days(&map(&[("year", 1970), ("week", 1), ("dayOfWeek", dow)])).unwrap();
        assert_eq!(d as i64, days(1969, 12, 29) + dow - 1, "dayOfWeek {dow}");
    }
    // Week alone defaults to Monday.
    assert_eq!(
        date_days(&map(&[("year", 1970), ("week", 2)])).unwrap() as i64,
        days(1970, 1, 5)
    );
    // dayOfWeek alone defaults to week 1.
    assert_eq!(
        date_days(&map(&[("year", 1970), ("dayOfWeek", 3)])).unwrap() as i64,
        days(1969, 12, 31)
    );
}

#[test]
fn week_date_errors() {
    assert!(
        msg(date_days(&map(&[("year", 1970), ("dayOfWeek", 8)])).unwrap_err())
            .contains("dayOfWeek must be 1..=7")
    );
    assert!(
        msg(date_days(&map(&[("year", 1970), ("week", 60)])).unwrap_err())
            .contains("invalid ISO week date")
    );
}

#[test]
fn quarter_dates() {
    assert_eq!(
        date_days(&map(&[("year", 1970), ("quarter", 2), ("dayOfQuarter", 1)])).unwrap() as i64,
        days(1970, 4, 1)
    );
    assert_eq!(
        date_days(&map(&[
            ("year", 2015),
            ("quarter", 3),
            ("dayOfQuarter", 21)
        ]))
        .unwrap() as i64,
        days(2015, 7, 21)
    );
    // dayOfQuarter alone: first quarter.
    assert_eq!(
        date_days(&map(&[("year", 1970), ("dayOfQuarter", 32)])).unwrap() as i64,
        days(1970, 2, 1)
    );
    assert!(
        msg(date_days(&map(&[("year", 1970), ("quarter", 5)])).unwrap_err())
            .contains("quarter must be 1..=4")
    );
}

#[test]
fn ordinal_dates() {
    assert_eq!(
        date_days(&map(&[("year", 1970), ("ordinalDay", 32)])).unwrap(),
        31
    );
    assert!(
        msg(date_days(&map(&[("year", 2015), ("ordinalDay", 366)])).unwrap_err())
            .contains("invalid ordinalDay 366 for 2015")
    );
}

// ---------------------------------------------------------------------------
// Time zones
// ---------------------------------------------------------------------------

#[test]
fn timezone_spec_accepts_utc_spellings() {
    assert_eq!(parse_timezone_spec("Z").unwrap(), TzSpec::Offset(0));
    assert_eq!(parse_timezone_spec(" utc ").unwrap(), TzSpec::Offset(0));
}

#[test]
fn timezone_spec_accepts_every_offset_form() {
    assert_eq!(parse_timezone_spec("+01:00").unwrap(), TzSpec::Offset(3600));
    assert_eq!(parse_timezone_spec("+0130").unwrap(), TzSpec::Offset(5400));
    assert_eq!(parse_timezone_spec("-01").unwrap(), TzSpec::Offset(-3600));
    assert_eq!(
        parse_timezone_spec("+02:05:59").unwrap(),
        TzSpec::Offset(7559)
    );
    assert_eq!(
        parse_timezone_spec("-02:30").unwrap(),
        TzSpec::Offset(-9000)
    );
}

#[test]
fn malformed_offsets_are_errors() {
    assert!(msg(parse_timezone_spec("+01:00:00:00").unwrap_err()).contains("bad timezone offset"));
    assert!(msg(parse_timezone_spec("+ab").unwrap_err()).contains("bad timezone offset"));
    assert!(msg(parse_timezone_spec("+01:xx").unwrap_err()).contains("bad timezone offset"));
}

#[test]
fn named_zones_parse_and_unknown_ones_are_errors() {
    let spec = parse_timezone_spec("Europe/Stockholm").unwrap();
    assert_eq!(zone_name(&spec).as_deref(), Some("Europe/Stockholm"));
    assert_eq!(zone_name(&TzSpec::Offset(0)), None);
    assert_eq!(
        msg(parse_timezone_spec("Mars/Olympus").unwrap_err()),
        "unknown time zone: Mars/Olympus"
    );
}

#[test]
fn resolve_offset_for_a_fixed_offset_ignores_the_date() {
    assert_eq!(
        resolve_offset(&TzSpec::Offset(-3600), 12345, 0).unwrap(),
        -3600
    );
}

#[test]
fn a_named_zone_has_a_seasonal_offset() {
    let spec = parse_timezone_spec("Europe/Stockholm").unwrap();
    assert_eq!(
        resolve_offset(&spec, days(2017, 7, 1), hms(12, 0, 0)).unwrap(),
        7200
    );
    assert_eq!(
        resolve_offset(&spec, days(2017, 1, 1), hms(12, 0, 0)).unwrap(),
        3600
    );
}

#[test]
fn an_ambiguous_local_time_resolves_to_the_earlier_offset() {
    // 02:30 happens twice on 2017-10-29 in Stockholm; the first is +02:00.
    let spec = parse_timezone_spec("Europe/Stockholm").unwrap();
    assert_eq!(
        resolve_offset(&spec, days(2017, 10, 29), hms(2, 30, 0)).unwrap(),
        7200
    );
}

#[test]
#[ignore = "bug: resolve_offset errors on a local time in a DST gap, though its doc says such times resolve to an offset"]
fn a_skipped_local_time_still_resolves() {
    // 02:30 does not exist on 2017-03-26 in Stockholm (clocks jump 02:00 -> 03:00).
    let spec = parse_timezone_spec("Europe/Stockholm").unwrap();
    let off = resolve_offset(&spec, days(2017, 3, 26), hms(2, 30, 0)).unwrap();
    assert!(off == 3600 || off == 7200, "got {off}");
}

#[test]
fn resolve_offset_out_of_range_is_an_error() {
    let spec = parse_timezone_spec("Europe/Stockholm").unwrap();
    assert_eq!(
        msg(resolve_offset(&spec, i64::MAX / 86_400, 0).unwrap_err()),
        "date-time out of range"
    );
}

#[test]
fn parse_timezone_returns_offset_and_zone() {
    assert_eq!(parse_timezone("+05:00").unwrap(), (18000, None));
    // Stockholm had no DST in 1970, so the 1970-01-01 resolution is +01:00.
    assert_eq!(
        parse_timezone("Europe/Stockholm").unwrap(),
        (3600, Some("Europe/Stockholm".to_string()))
    );
    assert!(parse_timezone("nowhere").is_err());
}

// ---------------------------------------------------------------------------
// String parsers
// ---------------------------------------------------------------------------

#[test]
fn parse_date_wraps_a_date_value() {
    assert_eq!(
        parse_date("2015-07-21").unwrap(),
        PropertyValue::Date(days(2015, 7, 21) as i32)
    );
    assert!(parse_date("nope").is_err());
}

#[test]
fn iso_dates_in_every_spelling() {
    let want = days(2015, 7, 21) as i32;
    for s in [
        "2015-07-21",
        "20150721",
        "2015-W30-2",
        "2015W302",
        "2015-202",
        "2015202",
        "+2015-07-21",
        " 2015-07-21 ",
    ] {
        assert_eq!(parse_iso_date(s).unwrap(), want, "{s}");
    }
    assert_eq!(parse_iso_date("2015-07").unwrap() as i64, days(2015, 7, 1));
    assert_eq!(parse_iso_date("2015").unwrap() as i64, days(2015, 1, 1));
    assert_eq!(
        parse_iso_date("2015-W30").unwrap() as i64,
        days(2015, 7, 20)
    );
    assert_eq!(
        parse_iso_date("2015w30-7").unwrap() as i64,
        days(2015, 7, 26)
    );
}

#[test]
fn signed_and_wide_years() {
    assert_eq!(
        parse_iso_date("-0001-01-01").unwrap() as i64,
        days(-1, 1, 1)
    );
    assert_eq!(
        parse_iso_date("123456-01-01").unwrap() as i64,
        days(123456, 1, 1)
    );
}

#[test]
fn malformed_iso_dates_are_errors() {
    assert_eq!(msg(parse_iso_date("  ").unwrap_err()), "empty date");
    assert!(msg(parse_iso_date("15-07-21").unwrap_err()).contains("cannot parse date"));
    assert!(msg(parse_iso_date("2015-13-01").unwrap_err()).contains("invalid date"));
    assert!(msg(parse_iso_date("2015-1").unwrap_err()).contains("cannot parse date"));
    assert!(msg(parse_iso_date("2015-Wxx").unwrap_err()).contains("bad week"));
    assert_eq!(msg(parse_iso_date("2015-0a01").unwrap_err()), "bad month");
    assert_eq!(msg(parse_iso_date("2015-01a1").unwrap_err()), "bad day");
    assert_eq!(msg(parse_iso_date("2015-x1").unwrap_err()), "bad month");
    assert_eq!(msg(parse_iso_date("2015-x01").unwrap_err()), "bad ordinal");
    assert!(msg(parse_iso_date("2015-366").unwrap_err()).contains("invalid date"));
    assert!(msg(parse_iso_date("9999999999").unwrap_err()).contains("cannot parse date"));
    assert!(msg(parse_iso_date("-9999999999-01-01").unwrap_err()).contains("year out of range"));
}

#[test]
fn a_one_digit_week_is_an_error_not_a_panic() {
    // #1570. Non-ASCII tails used to reach the same byte slicing.
    for s in ["2015-W3", "2015-W", "2015-Wé1", "2015-é1", "2015-0é"] {
        let r = std::panic::catch_unwind(|| parse_iso_date(s));
        assert!(
            matches!(r, Ok(Err(_))),
            "{s}: expected an error result, got a panic or a value"
        );
    }
}

#[test]
fn iso_times_in_every_spelling() {
    assert_eq!(
        parse_iso_time("21:40:32.142").unwrap(),
        hms(21, 40, 32) + 142_000_000
    );
    assert_eq!(
        parse_iso_time("214032,142").unwrap(),
        hms(21, 40, 32) + 142_000_000
    );
    assert_eq!(parse_iso_time("2140").unwrap(), hms(21, 40, 0));
    assert_eq!(parse_iso_time("214").unwrap(), hms(21, 4, 0));
    assert_eq!(parse_iso_time("21").unwrap(), hms(21, 0, 0));
    assert_eq!(parse_iso_time("5").unwrap(), hms(5, 0, 0));
    assert_eq!(parse_iso_time("21403").unwrap(), hms(21, 40, 3));
    // 24:00 is the end-of-day spelling and is accepted.
    assert_eq!(parse_iso_time("24:00").unwrap(), hms(24, 0, 0));
    // The fraction is truncated to nine digits.
    assert_eq!(parse_iso_time("00:00:00.1234567891").unwrap(), 123_456_789);
}

#[test]
fn malformed_iso_times_are_errors() {
    assert_eq!(msg(parse_iso_time(" ").unwrap_err()), "empty time");
    assert!(msg(parse_iso_time("ab:cd").unwrap_err()).contains("cannot parse time"));
    assert!(msg(parse_iso_time(".5").unwrap_err()).contains("cannot parse time"));
    assert!(msg(parse_iso_time("1234567").unwrap_err()).contains("cannot parse time"));
    assert!(msg(parse_iso_time("25:00").unwrap_err()).contains("time out of range"));
    assert!(msg(parse_iso_time("12:60").unwrap_err()).contains("time out of range"));
    assert!(msg(parse_iso_time("12:00:60").unwrap_err()).contains("time out of range"));
}

#[test]
fn time_parts_split_a_trailing_offset() {
    assert_eq!(
        parse_time_parts("21:40:32Z").unwrap(),
        (hms(21, 40, 32), Some(0))
    );
    assert_eq!(
        parse_time_parts("21:40:32z").unwrap(),
        (hms(21, 40, 32), Some(0))
    );
    assert_eq!(
        parse_time_parts("21:40:32+01:00").unwrap(),
        (hms(21, 40, 32), Some(3600))
    );
    assert_eq!(
        parse_time_parts("21:40-04").unwrap(),
        (hms(21, 40, 0), Some(-14400))
    );
    assert_eq!(parse_time_parts(" 21:40 ").unwrap(), (hms(21, 40, 0), None));
    // A leading sign is not an offset, and the rest does not parse as a time.
    assert!(parse_time_parts("-01:00").is_err());
    // A malformed offset is reported as such.
    assert!(msg(parse_time_parts("21:40+xx").unwrap_err()).contains("bad timezone offset"));
}

#[test]
fn datetime_offset_uses_the_dash_after_the_t() {
    assert_eq!(
        parse_datetime_offset("2015-07-21T21:40:32-04").unwrap(),
        ("2015-07-21T21:40:32", Some(-14400))
    );
    assert_eq!(
        parse_datetime_offset("2015-07-21T21:40:32").unwrap(),
        ("2015-07-21T21:40:32", None)
    );
    assert_eq!(
        parse_datetime_offset("2015-07-21T21:40:32Z").unwrap(),
        ("2015-07-21T21:40:32", Some(0))
    );
}

#[test]
#[ignore = "bug: split_offset treats the last dash of a bare date ('2015-07-21') as a UTC offset, though its doc says every dash there belongs to the date"]
fn datetime_offset_leaves_a_bare_date_alone() {
    assert_eq!(
        parse_datetime_offset("2015-07-21").unwrap(),
        ("2015-07-21", None)
    );
}

#[test]
fn zone_suffix_is_split_off_only_when_bracketed() {
    assert_eq!(
        split_zone_suffix("2015-07-21T21:40:32+02:00[Europe/Stockholm]"),
        ("2015-07-21T21:40:32+02:00", Some("Europe/Stockholm"))
    );
    assert_eq!(split_zone_suffix(" 2015-07-21 "), ("2015-07-21", None));
    assert_eq!(split_zone_suffix("x[Europe"), ("x[Europe", None));
}

#[test]
fn unknown_maps_are_refused_with_the_keys_named() {
    assert!(reject_unknown_map(&map(&[("year", 1), ("bogus", 2)])).is_ok());
    let m = msg(reject_unknown_map(&map(&[("epochMilis", 1), ("abc", 2)])).unwrap_err());
    assert!(m.contains("(abc, epochMilis)"), "{m}");
    assert!(m.contains("expected one of: epochMillis"), "{m}");
    let empty = msg(reject_unknown_map(&HashMap::new()).unwrap_err());
    assert!(empty.contains("the map is empty"), "{empty}");
}

// ---------------------------------------------------------------------------
// truncate
// ---------------------------------------------------------------------------

#[test]
fn truncating_to_coarse_units_moves_the_date() {
    assert_eq!(trunc_date("millennium"), days(2000, 1, 1));
    assert_eq!(trunc_date("century"), days(2000, 1, 1));
    assert_eq!(trunc_date("decade"), days(2010, 1, 1));
    assert_eq!(trunc_date("year"), days(2017, 1, 1));
    // ISO week-year 2017 starts on Monday 2017-01-02, not 1 January.
    assert_eq!(trunc_date("weekYear"), days(2017, 1, 2));
    assert_eq!(trunc_date("quarter"), days(2017, 10, 1));
    assert_eq!(trunc_date("month"), days(2017, 11, 1));
    assert_eq!(trunc_date("week"), days(2017, 11, 6));
    assert_eq!(trunc_date("DAY"), days(2017, 11, 11));
}

#[test]
fn truncating_to_fine_units_zeroes_below_the_unit() {
    assert_eq!(trunc_tod("hour"), hms(12, 0, 0));
    assert_eq!(trunc_tod("minute"), hms(12, 31, 0));
    assert_eq!(trunc_tod("second"), hms(12, 31, 14));
    assert_eq!(trunc_tod("millisecond"), hms(12, 31, 14) + 645_000_000);
    assert_eq!(trunc_tod("microsecond"), hms(12, 31, 14) + 645_876_000);
    assert_eq!(trunc_tod("day"), 0);
}

#[test]
fn unknown_truncation_unit_is_an_error() {
    assert_eq!(
        msg(truncate("date", "fortnight", &sample_ldt(), &HashMap::new()).unwrap_err()),
        "unknown truncation unit: fortnight"
    );
}

#[test]
fn unknown_truncation_target_is_an_error() {
    assert_eq!(
        msg(truncate("duration", "day", &sample_ldt(), &HashMap::new()).unwrap_err()),
        "cannot truncate to duration"
    );
}

#[test]
fn truncating_a_non_temporal_is_an_error() {
    assert!(
        msg(truncate("date", "day", &PropertyValue::Integer(3), &HashMap::new()).unwrap_err())
            .starts_with("not a temporal value")
    );
}

#[test]
fn truncating_an_out_of_range_date_is_an_error() {
    assert_eq!(
        msg(truncate(
            "date",
            "day",
            &PropertyValue::Date(i32::MAX),
            &HashMap::new()
        )
        .unwrap_err()),
        "date out of range"
    );
}

#[test]
fn date_overrides_apply_after_truncation() {
    let got = truncate("date", "millennium", &sample_ldt(), &map(&[("day", 2)])).unwrap();
    assert_eq!(got, PropertyValue::Date(days(2000, 1, 2) as i32));
    let got = truncate(
        "date",
        "year",
        &sample_ldt(),
        &map(&[("year", 1999), ("month", 3)]),
    )
    .unwrap();
    assert_eq!(got, PropertyValue::Date(days(1999, 3, 1) as i32));
}

#[test]
fn a_day_of_week_override_moves_within_the_week() {
    let got = truncate("date", "week", &sample_ldt(), &map(&[("dayOfWeek", 2)])).unwrap();
    assert_eq!(got, PropertyValue::Date(days(2017, 11, 7) as i32));
}

#[test]
fn an_invalid_override_date_is_an_error() {
    assert!(
        msg(truncate("date", "year", &sample_ldt(), &map(&[("month", 13)])).unwrap_err())
            .contains("invalid date")
    );
}

#[test]
fn clock_overrides_replace_their_field() {
    let got = truncate(
        "localtime",
        "hour",
        &sample_ldt(),
        &map(&[("minute", 5), ("nanosecond", 7)]),
    )
    .unwrap();
    assert_eq!(got, PropertyValue::LocalTime(hms(12, 5, 0) + 7));
    let got = truncate(
        "localtime",
        "day",
        &sample_ldt(),
        &map(&[
            ("hour", 3),
            ("second", 9),
            ("millisecond", 1),
            ("microsecond", 2),
        ]),
    )
    .unwrap();
    assert_eq!(got, PropertyValue::LocalTime(hms(3, 0, 9) + 1_002_000));
}

#[test]
fn truncate_builds_the_requested_type() {
    let ldt = truncate("localdatetime", "hour", &sample_ldt(), &HashMap::new()).unwrap();
    assert_eq!(
        ldt,
        PropertyValue::LocalDateTime {
            secs: days(2017, 11, 11) * 86_400 + 12 * 3600,
            nanos: 0
        }
    );
    let t = truncate(
        "time",
        "minute",
        &PropertyValue::Time {
            nanos: hms(10, 20, 30),
            offset_seconds: 3600,
        },
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(
        t,
        PropertyValue::Time {
            nanos: hms(10, 20, 0),
            offset_seconds: 3600
        }
    );
    let lt = truncate(
        "localtime",
        "second",
        &PropertyValue::LocalTime(hms(1, 2, 3) + 5),
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(lt, PropertyValue::LocalTime(hms(1, 2, 3)));
}

#[test]
fn truncating_a_date_to_a_datetime_starts_at_midnight_utc() {
    let got = truncate(
        "datetime",
        "hour",
        &PropertyValue::Date(days(2017, 11, 11) as i32),
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(
        got,
        PropertyValue::ZonedDateTime {
            secs: days(2017, 11, 11) * 86_400,
            nanos: 0,
            offset_seconds: 0,
            zone: None
        }
    );
}

#[test]
fn a_zoned_datetime_keeps_its_zone_through_truncation() {
    // 2017-11-11T12:31:14+01:00[Europe/Stockholm] -> day -> 2017-11-11T00:00+01:00.
    let local_secs = days(2017, 11, 11) * 86_400 + 12 * 3600 + 31 * 60 + 14;
    let v = PropertyValue::ZonedDateTime {
        secs: local_secs - 3600,
        nanos: 0,
        offset_seconds: 3600,
        zone: Some("Europe/Stockholm".into()),
    };
    let got = truncate("datetime", "day", &v, &HashMap::new()).unwrap();
    assert_eq!(
        got,
        PropertyValue::ZonedDateTime {
            secs: days(2017, 11, 11) * 86_400 - 3600,
            nanos: 0,
            offset_seconds: 3600,
            zone: Some("Europe/Stockholm".into())
        }
    );
}

#[test]
fn a_timezone_override_rezones_against_the_truncated_instant() {
    let mut o = HashMap::new();
    o.insert(
        "timezone".to_string(),
        PropertyValue::String("Europe/Stockholm".into()),
    );
    // Truncating mid-summer 2017 to the year lands in winter: +01:00.
    let summer = PropertyValue::LocalDateTime {
        secs: days(2017, 7, 1) * 86_400,
        nanos: 0,
    };
    let got = truncate("datetime", "year", &summer, &o).unwrap();
    assert_eq!(
        got,
        PropertyValue::ZonedDateTime {
            secs: days(2017, 1, 1) * 86_400 - 3600,
            nanos: 0,
            offset_seconds: 3600,
            zone: Some("Europe/Stockholm".into())
        }
    );
    let mut bad = HashMap::new();
    bad.insert(
        "timezone".to_string(),
        PropertyValue::String("Nope/Nope".into()),
    );
    assert!(truncate("datetime", "year", &summer, &bad).is_err());
}
