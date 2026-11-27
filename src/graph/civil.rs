//! Proleptic Gregorian calendar arithmetic on days since 1970-01-01 (#1009).
//!
//! openCypher dates run from year -999,999,999 to +999,999,999.
//! `chrono::NaiveDate` stops at ±262,143, so a date literal such as
//! `date('-999999999-01-01')` could not be built through it at all. These are
//! Howard Hinnant's `days_from_civil` / `civil_from_days`, which are exact
//! over the whole `i64` range that matters here and agree with chrono
//! everywhere chrono has an answer: same calendar, same year zero.
//!
//! Everything works on a day number (`i64`, days since the Unix epoch), which
//! is what [`PropertyValue::Date`](crate::graph::PropertyValue::Date) holds.

/// The widest year openCypher defines a date for, in either direction.
pub const MAX_YEAR: i64 = 999_999_999;
/// The narrowest.
pub const MIN_YEAR: i64 = -999_999_999;

/// Day number of -999999999-01-01, the first date openCypher defines.
pub const MIN_DAY: i64 = -365_243_219_162;
/// Day number of +999999999-12-31, the last.
pub const MAX_DAY: i64 = 365_241_780_471;

/// Whether a day number is a date openCypher defines.
pub fn day_in_range(days: i64) -> bool {
    (MIN_DAY..=MAX_DAY).contains(&days)
}

/// Whether `year` is in the range a date may have.
pub fn year_in_range(year: i64) -> bool {
    (MIN_YEAR..=MAX_YEAR).contains(&year)
}

pub fn is_leap(year: i64) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

pub fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(year) => 29,
        2 => 28,
        _ => 0,
    }
}

pub fn days_in_year(year: i64) -> u32 {
    if is_leap(year) {
        366
    } else {
        365
    }
}

/// Days since 1970-01-01 of a calendar date. No validation: see [`from_ymd`].
pub fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let (m, d) = (month as i64, day as i64);
    let y = if m <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(year, month, day)` of a day number.
///
/// Exact for every day in [`MIN_DAY`]`..=`[`MAX_DAY`] and far beyond. Within
/// 719,468 days of `i64::MAX` the shift saturates rather than overflowing, so
/// a corrupt value renders as a wrong date instead of a panic.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days.saturating_add(719_468);
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

/// A validated calendar date, or `None` when the day does not exist or the
/// year is outside openCypher's range.
pub fn from_ymd(year: i64, month: u32, day: u32) -> Option<i64> {
    if !year_in_range(year) || !(1..=12).contains(&month) {
        return None;
    }
    if day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some(days_from_civil(year, month, day))
}

/// Day `ordinal` (1-based) of `year`.
pub fn from_yo(year: i64, ordinal: u32) -> Option<i64> {
    if !year_in_range(year) || ordinal == 0 || ordinal > days_in_year(year) {
        return None;
    }
    Some(days_from_civil(year, 1, 1) + ordinal as i64 - 1)
}

/// ISO weekday, Monday = 1 through Sunday = 7. 1970-01-01 was a Thursday.
pub fn weekday(days: i64) -> u32 {
    ((days + 3).rem_euclid(7) + 1) as u32
}

/// Day of the year, 1-based.
pub fn ordinal(days: i64) -> u32 {
    let (y, _, _) = civil_from_days(days);
    (days - days_from_civil(y, 1, 1) + 1) as u32
}

/// `(week-year, week)` of the ISO week date. The week-year is the calendar
/// year of the week's Thursday, which near a year boundary is not the date's
/// own year.
pub fn iso_week(days: i64) -> (i64, u32) {
    let thursday = days - weekday(days) as i64 + 4;
    let (wy, _, _) = civil_from_days(thursday);
    let week = (thursday - days_from_civil(wy, 1, 1)) / 7 + 1;
    (wy, week as u32)
}

/// Number of ISO weeks in `week_year`: 52 or 53.
pub fn weeks_in_year(week_year: i64) -> u32 {
    // 28 December is always in the last week of its year.
    iso_week(days_from_civil(week_year, 12, 28)).1
}

/// The day of ISO week date `week_year`-W`week`-`dow`.
pub fn from_isoywd(week_year: i64, week: u32, dow: u32) -> Option<i64> {
    if !year_in_range(week_year) || !(1..=7).contains(&dow) {
        return None;
    }
    if week == 0 || week > weeks_in_year(week_year) {
        return None;
    }
    let jan4 = days_from_civil(week_year, 1, 4);
    let monday_week1 = jan4 - weekday(jan4) as i64 + 1;
    Some(monday_week1 + (week as i64 - 1) * 7 + dow as i64 - 1)
}

/// Move by whole months, clamping the day into the target month: 31 January
/// plus one month is 28 (or 29) February.
pub fn shift_months_clamped(days: i64, months: i64) -> Option<i64> {
    let (y, m, d) = civil_from_days(days);
    let total = y
        .checked_mul(12)?
        .checked_add(m as i64 - 1)?
        .checked_add(months)?;
    let (ny, nm0) = (total.div_euclid(12), total.rem_euclid(12));
    let nm = nm0 as u32 + 1;
    if !year_in_range(ny) {
        return None;
    }
    Some(days_from_civil(ny, nm, d.min(days_in_month(ny, nm))))
}

/// A date the way openCypher writes it: `2015-07-21`, `-0001-01-01`,
/// `+999999999-12-31`.
///
/// Four-digit years are written bare; a year past 9999 takes a `+` and a
/// negative year a `-`, padded to at least four digits. That is ISO 8601's
/// expanded representation, as java.time (and so Neo4j and the TCK) renders
/// it, and also what chrono's `%Y` wrote for the years it could hold.
pub fn fmt_date(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{}-{m:02}-{d:02}", fmt_year(y))
}

fn fmt_year(y: i64) -> String {
    if y > 9_999 {
        format!("+{y}")
    } else if y < 0 {
        format!("-{:04}", y.unsigned_abs())
    } else {
        format!("{y:04}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate};

    fn chrono_days(d: NaiveDate) -> i64 {
        d.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days()
    }

    #[test]
    fn agrees_with_chrono_wherever_chrono_has_an_answer() {
        // Every day of a 2,000-year window across year zero, plus spot checks
        // near chrono's limits.
        let lo = chrono_days(NaiveDate::from_ymd_opt(-1000, 1, 1).unwrap());
        let hi = chrono_days(NaiveDate::from_ymd_opt(1000, 12, 31).unwrap());
        let mut spots: Vec<i64> = (lo..=hi).collect();
        for y in [
            -262_000, -100_001, -4_713, 1_582, 1_970, 2_000, 9_999, 10_000, 262_000,
        ] {
            spots.push(chrono_days(NaiveDate::from_ymd_opt(y, 2, 28).unwrap()));
            spots.push(chrono_days(NaiveDate::from_ymd_opt(y, 12, 31).unwrap()));
        }
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        for z in spots {
            let c = epoch + chrono::Duration::days(z);
            assert_eq!(
                civil_from_days(z),
                (c.year() as i64, c.month(), c.day()),
                "day {z}"
            );
            assert_eq!(days_from_civil(c.year() as i64, c.month(), c.day()), z);
            assert_eq!(weekday(z), c.weekday().number_from_monday(), "day {z}");
            assert_eq!(ordinal(z), c.ordinal(), "day {z}");
            let w = c.iso_week();
            assert_eq!(iso_week(z), (w.year() as i64, w.week()), "day {z}");
            assert_eq!(
                from_isoywd(w.year() as i64, w.week(), c.weekday().number_from_monday()),
                Some(z)
            );
        }
    }

    #[test]
    fn the_opencypher_extremes_round_trip() {
        let lo = from_ymd(MIN_YEAR, 1, 1).unwrap();
        let hi = from_ymd(MAX_YEAR, 12, 31).unwrap();
        assert_eq!(civil_from_days(lo), (MIN_YEAR, 1, 1));
        assert_eq!(civil_from_days(hi), (MAX_YEAR, 12, 31));
        assert!(
            lo < i32::MIN as i64 && hi > i32::MAX as i64,
            "beyond i32 days on both sides"
        );
        assert_eq!(fmt_date(lo), "-999999999-01-01");
        assert_eq!(fmt_date(hi), "+999999999-12-31");
        assert_eq!((lo, hi), (MIN_DAY, MAX_DAY));
        assert!(day_in_range(lo) && day_in_range(hi));
        assert!(!day_in_range(lo - 1) && !day_in_range(hi + 1));
        // Never a panic, even on garbage.
        let _ = fmt_date(i64::MAX);
        let _ = fmt_date(i64::MIN);
        assert_eq!(from_ymd(MAX_YEAR + 1, 1, 1), None);
        assert_eq!(from_ymd(MIN_YEAR - 1, 12, 31), None);
    }

    #[test]
    fn formats_years_as_iso_expanded() {
        assert_eq!(fmt_date(0), "1970-01-01");
        assert_eq!(fmt_date(days_from_civil(0, 1, 1)), "0000-01-01");
        assert_eq!(fmt_date(days_from_civil(-1, 12, 31)), "-0001-12-31");
        assert_eq!(fmt_date(days_from_civil(9_999, 12, 31)), "9999-12-31");
        assert_eq!(fmt_date(days_from_civil(10_000, 1, 1)), "+10000-01-01");
    }

    #[test]
    fn rejects_days_that_do_not_exist() {
        assert_eq!(from_ymd(2015, 2, 29), None);
        assert!(from_ymd(2016, 2, 29).is_some());
        assert_eq!(from_ymd(1900, 2, 29), None);
        assert_eq!(from_ymd(2015, 13, 1), None);
        assert_eq!(from_yo(2015, 366), None);
        assert!(from_yo(2016, 366).is_some());
        assert_eq!(from_isoywd(2015, 54, 1), None);
        assert_eq!(
            from_isoywd(2015, 53, 1).map(fmt_date).as_deref(),
            Some("2015-12-28")
        );
        assert_eq!(from_isoywd(2014, 53, 1), None);
    }

    #[test]
    fn month_shift_clamps_into_the_target_month() {
        let jan31 = days_from_civil(2015, 1, 31);
        assert_eq!(
            fmt_date(shift_months_clamped(jan31, 1).unwrap()),
            "2015-02-28"
        );
        assert_eq!(
            fmt_date(shift_months_clamped(jan31, -2).unwrap()),
            "2014-11-30"
        );
        let lo = from_ymd(MIN_YEAR, 1, 1).unwrap();
        assert_eq!(
            fmt_date(shift_months_clamped(lo, 1_999_999_998 * 12 + 11).unwrap()),
            "+999999999-12-01"
        );
        assert_eq!(shift_months_clamped(lo, -1), None);
    }
}
