//! A named zone's offset is resolved at the *selected* date (#1006).
//!
//! ```cypher
//! WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, ...}) AS otherDate,
//!      datetime({year: 1984, month: 10, day: 11, hour: 12,
//!                timezone: 'Europe/Stockholm'}) AS otherTime
//! RETURN datetime({date: otherDate, time: otherTime, day: 28, second: 42})
//! ```
//!
//! answered `…+01:00[Europe/Stockholm]` where openCypher says `…+02:00`.
//! The date, the time, the `second` override and the zone *name* were all
//! right. Only the offset was wrong.
//!
//! A named zone has **no single offset** -- its offset is a function of the
//! date. The source is in October, when Stockholm is `+01:00`; the selection
//! moves the value to 28 March, when Stockholm is `+02:00`. The selection
//! inherited the zone and *copied its stored offset*, which is a value derived
//! from a date that is no longer the date.
//!
//! Two sites had it. The inherited-zone path, and the source offset used by
//! #809's re-zoning conversion -- where undoing `+01:00` instead of `+02:00`
//! lands the instant an hour out. Fixing one leaves the other wrong, and the
//! two failing TCK outlines are exactly one of each.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

const BINDINGS: &str = "WITH localdatetime({year: 1984, week: 10, dayOfWeek: 3, hour: 12, \
     minute: 31, second: 14, millisecond: 645}) AS otherDate, \
     datetime({year: 1984, month: 10, day: 11, hour: 12, timezone: 'Europe/Stockholm'}) AS otherTime ";

fn text(cypher: &str) -> String {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    let v = format!("{:?}", r.records[0].get(&c));
    let a = v.find('"').expect("a string result");
    v[a + 1..v.rfind('"').unwrap()].to_string()
}

fn select(expr: &str) -> String {
    text(&format!("{BINDINGS} RETURN toString(datetime({expr})) AS d"))
}

#[test]
fn an_inherited_zone_takes_the_offset_of_the_selected_date() {
    // 28 March 1984 is summer time in Stockholm; 11 October is not.
    assert_eq!(select("{date: otherDate, time: otherTime, day: 28, second: 42}"),
               "1984-03-28T12:00:42+02:00[Europe/Stockholm]");
}

#[test]
fn a_rezoning_converts_from_the_selected_dates_offset() {
    // 12:00+02:00 is 10:00 UTC, which is 00:00 in Honolulu. Undoing the
    // source's stored +01:00 instead would give 01:00.
    assert_eq!(
        select("{date: otherDate, time: otherTime, day: 28, second: 42, timezone: 'Pacific/Honolulu'}"),
        "1984-03-28T00:00:42-10:00[Pacific/Honolulu]");
}

#[test]
fn a_selection_that_stays_in_winter_is_unchanged() {
    // The control: when the selected date is on the same side of the DST
    // boundary as the source, re-resolving and copying agree.
    assert_eq!(select("{date: otherDate, time: otherTime, month: 12, day: 11}"),
               "1984-12-11T12:00+01:00[Europe/Stockholm]");
}

#[test]
fn a_zoned_source_with_no_selection_keeps_its_own_zone() {
    // #809: defaulting to UTC re-labelled the value. Still true.
    assert_eq!(text("RETURN toString(datetime({datetime: datetime({year: 1984, month: 10, \
                     day: 11, hour: 12, timezone: 'Europe/Stockholm'})})) AS d"),
               "1984-10-11T12:00+01:00[Europe/Stockholm]");
}

#[test]
fn a_bare_offset_has_no_date_to_depend_on() {
    // A `+05:00` is not a zone and must be copied unchanged whatever the date.
    assert_eq!(
        text("WITH time({hour: 12, minute: 31, second: 14, timezone: '+01:00'}) AS t, \
              date({year: 1984, month: 3, day: 28}) AS d \
              RETURN toString(datetime({date: d, time: t})) AS r"),
        "1984-03-28T12:31:14+01:00");
}
