//! Dates out to year ±999,999,999, the range openCypher defines (#1009).
//!
//! `PropertyValue::Date` held an `i32` day count and every date went through
//! `chrono::NaiveDate`, which stops at ±262,143 years. The TCK's two
//! extended-year scenarios (Temporal10 [9] and [10]) failed on parsing alone.
//!
//! The risk in widening the type is not the arithmetic, it is the bytes on
//! disk: so besides the queries, these follow an extended date through a real
//! restart and a `.sgsnap` export and import. (That an *old* i32 encoding still
//! loads is pinned beside the serde code, in `src/graph/property.rs`, where the
//! old enum can be written down.)

use samyama::graph::{GraphStore, PropertyValue};
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;

const T: &str = "default";

fn one(q: &str) -> PropertyValue {
    let store = GraphStore::new();
    let batch = QueryEngine::new()
        .execute(q, &store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    assert_eq!(batch.records.len(), 1, "{q}");
    let col = &batch.columns[0];
    batch.records[0]
        .get(col)
        .and_then(|v| v.as_property().cloned())
        .unwrap_or_else(|| panic!("{q}: no value"))
}

fn text(q: &str) -> String {
    match one(q) {
        PropertyValue::String(s) => s,
        other => panic!("{q}: expected a string, got {other:?}"),
    }
}

#[test]
fn tck_temporal10_9_duration_between_the_extreme_dates() {
    assert_eq!(
        text(
            "RETURN toString(duration.between(date('-999999999-01-01'), \
             date('+999999999-12-31'))) AS d"
        ),
        "P1999999998Y11M30D"
    );
}

#[test]
fn tck_temporal10_10_seconds_between_the_extreme_local_date_times() {
    assert_eq!(
        text(
            "RETURN toString(duration.inSeconds(localdatetime('-999999999-01-01'), \
             localdatetime('+999999999-12-31T23:59:59'))) AS d"
        ),
        "PT17531639991215H59M59S"
    );
}

#[test]
fn extended_dates_render_and_decompose() {
    assert_eq!(
        text("RETURN toString(date('+999999999-12-31')) AS d"),
        "+999999999-12-31"
    );
    assert_eq!(
        text("RETURN toString(date('-999999999-01-01')) AS d"),
        "-999999999-01-01"
    );
    assert_eq!(
        text("RETURN toString(date('+10000-01-01')) AS d"),
        "+10000-01-01"
    );
    assert_eq!(
        text("RETURN toString(date({year: -1, month: 12, day: 31})) AS d"),
        "-0001-12-31"
    );
    assert_eq!(
        one("RETURN date('+999999999-12-31').year AS y"),
        PropertyValue::Integer(999_999_999)
    );
    assert_eq!(
        one("RETURN date('-999999999-01-01').month AS m"),
        PropertyValue::Integer(1)
    );
    assert_eq!(
        one("RETURN date('+999999999-12-31') > date('2015-07-21') AS gt"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        one("RETURN date('-999999999-01-01') < date('-262144-01-01') AS lt"),
        PropertyValue::Boolean(true)
    );
}

#[test]
fn a_year_past_the_range_is_refused_not_wrapped() {
    let store = GraphStore::new();
    for q in [
        "RETURN date('+1000000000-01-01') AS d",
        "RETURN date('-1000000000-12-31') AS d",
    ] {
        assert!(
            QueryEngine::new().execute(q, &store).is_err(),
            "{q} should be an error"
        );
    }
}

#[test]
fn ordinary_dates_are_unchanged() {
    assert_eq!(
        text("RETURN toString(date('2015-07-21')) AS d"),
        "2015-07-21"
    );
    assert_eq!(
        text("RETURN toString(date('2015-W30-2')) AS d"),
        "2015-07-21"
    );
    assert_eq!(text("RETURN toString(date('2015-202')) AS d"), "2015-07-21");
    assert_eq!(
        text("RETURN toString(date('0000-01-01')) AS d"),
        "0000-01-01"
    );
    assert_eq!(
        one("RETURN date('2015-07-21').week AS w"),
        PropertyValue::Integer(30)
    );
    assert_eq!(
        text("RETURN toString(date('2015-01-31') + duration('P1M')) AS d"),
        "2015-02-28"
    );
}

fn extreme_dates() -> Vec<(String, String)> {
    [
        ("lo", "-999999999-01-01"),
        ("hi", "+999999999-12-31"),
        ("mid", "2015-07-21"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

fn read_dates(store: &GraphStore) -> Vec<(String, String)> {
    let batch = QueryEngine::new()
        .execute(
            "MATCH (n:D) RETURN n.k AS k, toString(n.d) AS d ORDER BY k",
            store,
        )
        .expect("read");
    batch
        .records
        .iter()
        .map(|r| {
            let s = |c: &str| match r.get(c).and_then(|v| v.as_property().cloned()) {
                Some(PropertyValue::String(s)) => s,
                other => panic!("{c}: {other:?}"),
            };
            (s("k"), s("d"))
        })
        .collect()
}

fn sorted(mut v: Vec<(String, String)>) -> Vec<(String, String)> {
    v.sort();
    v
}

#[test]
fn extended_dates_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let pm = PersistenceManager::new(dir.path()).unwrap();
        pm.tenants()
            .create_tenant(T.to_string(), T.to_string(), None)
            .ok();
        let mut store = GraphStore::new();
        store.enable_write_log();
        for (k, d) in extreme_dates() {
            let q = format!("CREATE (:D {{k: '{k}', d: date('{d}')}})");
            QueryEngine::new()
                .execute_mut(&q, &mut store, T)
                .unwrap_or_else(|e| panic!("{q}: {e}"));
            let muts = store.take_write_log();
            pm.apply_mutations(T, &store, &muts).expect("persist");
        }
        pm.checkpoint().unwrap();
    }
    let pm = PersistenceManager::new(dir.path()).unwrap();
    let mut store = GraphStore::new();
    pm.recover_into(T, &mut store).expect("recover");
    assert_eq!(read_dates(&store), sorted(extreme_dates()));
}

#[test]
fn extended_dates_survive_a_snapshot_export_and_import() {
    let mut g = GraphStore::new();
    for (k, d) in extreme_dates() {
        QueryEngine::new()
            .execute_mut(
                &format!("CREATE (:D {{k: '{k}', d: date('{d}')}})"),
                &mut g,
                T,
            )
            .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dates.sgsnap");
    samyama::snapshot::write_file(&g, &path, &samyama::snapshot::WriteOptions::default())
        .expect("write");
    let mut back = GraphStore::new();
    samyama::snapshot::import_tenant(&mut back, std::fs::File::open(&path).unwrap())
        .expect("import");
    assert_eq!(read_dates(&back), sorted(extreme_dates()));
}
