//! Adding a duration to a `Date` drops the sub-day part (#817).
//!
//! ```text
//! date('1984-10-11') - duration({months: 1, days: -14, hours: 16, minutes: -12, seconds: 70})
//!   expected 1984-09-25, got 1984-09-24
//! ```
//!
//! A date has no clock, so a duration's hours/minutes/seconds cannot move it.
//! Two things had to be right, and getting either wrong is a one-day error:
//!
//! 1. **Drop the sub-day part**, do not apply it. Applying a +15h49m remainder
//!    to a subtraction walks backwards across midnight.
//! 2. **Drop it *before* combining with days.** `days: -14` plus a `+15h49m`
//!    remainder is −13.34 days; truncating *that* gives −13. The fractional
//!    part belongs to the clock the date does not have, so the days field must
//!    survive intact.
//!
//! Addition looked correct throughout — a positive remainder truncates back to
//! the same day — so only subtraction and mixed-sign durations exposed it.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rendered(expr: &str) -> String {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.to_cypher_string(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

const D: &str = "date({year:1984,month:10,day:11})";

/// The TCK's rows, both directions.
///
/// The **mixed-sign** duration is the one that matters: `days: -14` with a
/// positive clock remainder is where combining before truncating loses a day.
#[test]
fn a_date_ignores_the_sub_day_part_of_a_duration() {
    let mixed = "duration({months:1, days:-14, hours:16, minutes:-12, seconds:70})";
    assert_eq!(rendered(&format!("{D} + {mixed}")), "1984-10-28");
    assert_eq!(rendered(&format!("{D} - {mixed}")), "1984-09-25");

    let plain = "duration({years:12, months:5, days:14, hours:16, minutes:12, seconds:70, nanoseconds:2})";
    assert_eq!(rendered(&format!("{D} + {plain}")), "1997-03-25");
    assert_eq!(rendered(&format!("{D} - {plain}")), "1972-04-27");
}

/// A sub-day-only duration moves a date not at all, in either direction.
#[test]
fn a_sub_day_duration_does_not_move_a_date() {
    for dur in ["duration({hours: 23})", "duration({hours: 1})", "duration({seconds: 1})"] {
        assert_eq!(rendered(&format!("{D} + {dur}")), "1984-10-11", "{dur}");
        assert_eq!(rendered(&format!("{D} - {dur}")), "1984-10-11", "{dur}");
    }
}

/// **A date-time keeps its clock** — the rule is about `Date` specifically, and
/// applying it to the composite types would discard real information.
#[test]
fn a_datetime_still_applies_the_sub_day_part() {
    assert_eq!(
        rendered("localdatetime({year:1984,month:10,day:11,hour:12}) + duration({hours: 3})"),
        "1984-10-11T15:00"
    );
    assert_eq!(
        rendered("localdatetime({year:1984,month:10,day:11,hour:1}) - duration({hours: 3})"),
        "1984-10-10T22:00"
    );
}

/// Whole-day components still move a date, and months still clamp.
#[test]
fn whole_day_and_month_components_still_apply() {
    assert_eq!(rendered(&format!("{D} + duration({{days: 1}})")), "1984-10-12");
    assert_eq!(rendered(&format!("{D} - duration({{days: 1}})")), "1984-10-10");
    assert_eq!(rendered(&format!("{D} + duration({{months: 1}})")), "1984-11-11");
    // 31 January plus one month clamps to the end of February.
    assert_eq!(
        rendered("date({year:2018,month:1,day:31}) + duration({months: 1})"),
        "2018-02-28"
    );
}
