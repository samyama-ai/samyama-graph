//! Points, distance and bounding boxes (NDS-04).
//!
//! NDS-04 was measured at 0 of 5 probes: no `point()`, no distance, no
//! `withinBBox`, no spatial index. This adds the first four; the index is
//! still absent and `CREATE POINT INDEX` is still refused, which the
//! measurement reports rather than this hiding.
//!
//! **The distances are checked against published figures, not against the
//! implementation.** A test that asserts what the code returns confirms only
//! that the code is deterministic. London–Paris and New York–London are
//! textbook great-circle distances; if the haversine is wrong they move.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

fn eval(q: &str) -> Result<Value, String> {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let batch = engine
        .execute_mut(q, &mut store, "default")
        .map_err(|e| e.to_string())?;
    let rec = batch.records.first().ok_or("no rows")?;
    let (_, v) = rec.bindings().first().ok_or("no columns")?;
    Ok(v.clone())
}

fn float(q: &str) -> f64 {
    match eval(q).expect(q) {
        Value::Property(PropertyValue::Float(f)) => f,
        other => panic!("{q} returned {other:?}"),
    }
}

fn boolean(q: &str) -> bool {
    match eval(q).expect(q) {
        Value::Property(PropertyValue::Boolean(b)) => b,
        other => panic!("{q} returned {other:?}"),
    }
}

const LONDON: &str = "point({latitude: 51.5074, longitude: -0.1278})";
const PARIS: &str = "point({latitude: 48.8566, longitude: 2.3522})";
const NEW_YORK: &str = "point({latitude: 40.7128, longitude: -74.0060})";

#[test]
fn a_geographic_point_carries_both_spellings_and_its_crs() {
    let v = eval("RETURN point({latitude: 12.9, longitude: 77.6})").expect("point");
    let Value::Property(PropertyValue::Map(m)) = v else {
        panic!("point() did not return a map: {v:?}")
    };
    // `x`/`y` as well as `latitude`/`longitude`: a caller reading either
    // spelling gets the same point, and `x` is longitude because x is the
    // horizontal axis. Getting that pair the wrong way round is the classic
    // geospatial bug and it is silent.
    assert_eq!(m.get("x"), Some(&PropertyValue::Float(77.6)));
    assert_eq!(m.get("y"), Some(&PropertyValue::Float(12.9)));
    assert_eq!(m.get("longitude"), Some(&PropertyValue::Float(77.6)));
    assert_eq!(m.get("latitude"), Some(&PropertyValue::Float(12.9)));
    assert_eq!(m.get("srid"), Some(&PropertyValue::Integer(4326)));
    assert_eq!(m.get("crs"), Some(&PropertyValue::String("wgs-84".into())));
}

#[test]
fn a_cartesian_point_is_not_geographic() {
    let v = eval("RETURN point({x: 1.0, y: 2.0})").expect("point");
    let Value::Property(PropertyValue::Map(m)) = v else { panic!("{v:?}") };
    assert_eq!(m.get("srid"), Some(&PropertyValue::Integer(7203)));
    assert!(m.get("latitude").is_none(), "a cartesian point has no latitude");
}

#[test]
fn a_three_dimensional_point_gets_the_three_dimensional_crs() {
    let v = eval("RETURN point({latitude: 12.9, longitude: 77.6, height: 100.0})").expect("point");
    let Value::Property(PropertyValue::Map(m)) = v else { panic!("{v:?}") };
    assert_eq!(m.get("z"), Some(&PropertyValue::Float(100.0)));
    assert_eq!(m.get("srid"), Some(&PropertyValue::Integer(4979)));
}

#[test]
fn london_to_paris_is_about_343_km() {
    // Published great-circle distance: ~343.5 km. The tolerance is 1%, which
    // is wider than the haversine's spherical-earth error (~0.5%) and far
    // narrower than any plausible bug: a swapped lat/lon gives ~4,600 km, and
    // degrees-as-radians gives a number with the wrong number of digits.
    let d = float(&format!("RETURN point.distance({LONDON}, {PARIS})"));
    let km = d / 1000.0;
    assert!(
        (km - 343.5).abs() / 343.5 < 0.01,
        "London to Paris came out {km:.1} km, published ~343.5 km"
    );
}

#[test]
fn new_york_to_london_is_about_5570_km() {
    let km = float(&format!("RETURN distance({NEW_YORK}, {LONDON})")) / 1000.0;
    assert!(
        (km - 5570.0).abs() / 5570.0 < 0.01,
        "New York to London came out {km:.0} km, published ~5,570 km"
    );
}

#[test]
fn a_cartesian_distance_is_euclidean() {
    assert_eq!(float("RETURN distance(point({x: 0.0, y: 0.0}), point({x: 3.0, y: 4.0}))"), 5.0);
}

#[test]
fn distance_is_symmetric_and_zero_to_itself() {
    // Metamorphic: the same question asked two ways must give the same answer,
    // whatever the answer is. This holds even if the constant above is wrong.
    let there = float(&format!("RETURN point.distance({LONDON}, {PARIS})"));
    let back = float(&format!("RETURN point.distance({PARIS}, {LONDON})"));
    assert_eq!(there, back);
    assert_eq!(float(&format!("RETURN point.distance({LONDON}, {LONDON})")), 0.0);
}

#[test]
fn the_triangle_inequality_holds() {
    let a = float(&format!("RETURN point.distance({NEW_YORK}, {LONDON})"));
    let b = float(&format!("RETURN point.distance({LONDON}, {PARIS})"));
    let c = float(&format!("RETURN point.distance({NEW_YORK}, {PARIS})"));
    assert!(c <= a + b + 1.0, "{c} > {a} + {b}");
}

#[test]
fn a_bounding_box_includes_and_excludes() {
    let inside = "point({latitude: 12.9, longitude: 77.6})";
    let outside = "point({latitude: 20.0, longitude: 77.6})";
    let ll = "point({latitude: 12.0, longitude: 77.0})";
    let ur = "point({latitude: 14.0, longitude: 78.0})";
    assert!(boolean(&format!("RETURN point.withinBBox({inside}, {ll}, {ur})")));
    assert!(!boolean(&format!("RETURN point.withinBBox({outside}, {ll}, {ur})")));
}

#[test]
fn a_point_on_the_boundary_is_inside() {
    // Half-open or closed is a choice, and an undocumented one bites whoever
    // tiles a map. Closed, matching the usual reading of "within".
    let ll = "point({latitude: 12.0, longitude: 77.0})";
    let ur = "point({latitude: 14.0, longitude: 78.0})";
    assert!(boolean(&format!("RETURN point.withinBBox({ll}, {ll}, {ur})")));
    assert!(boolean(&format!("RETURN point.withinBBox({ur}, {ll}, {ur})")));
}

#[test]
fn an_out_of_range_coordinate_is_refused() {
    // Refused, not wrapped or clamped. A latitude of 999 is a bug in the
    // caller's data, and a point quietly moved to somewhere legal answers
    // every later query wrongly.
    let e = eval("RETURN point({latitude: 999.0, longitude: 77.6})").unwrap_err();
    assert!(e.contains("latitude"), "{e}");
    let e = eval("RETURN point({latitude: 12.0, longitude: 200.0})").unwrap_err();
    assert!(e.contains("longitude"), "{e}");
}

#[test]
fn mixing_coordinate_systems_is_an_error_not_a_number() {
    // The answer would be in no unit at all.
    let e = eval(&format!(
        "RETURN point.distance(point({{x: 0.0, y: 0.0}}), {LONDON})"
    ))
    .unwrap_err();
    assert!(e.contains("coordinate system"), "{e}");
}

#[test]
fn a_point_with_neither_pair_of_keys_is_refused() {
    let e = eval("RETURN point({a: 1.0, b: 2.0})").unwrap_err();
    assert!(e.contains("latitude") && e.contains("x"), "{e}");
}

#[test]
fn a_point_stored_on_a_node_reads_back_as_a_point() {
    // The reason a point is a map: it round-trips through property storage
    // with no new encoding. If this ever stops working, the choice was wrong.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:City {name: 'London', loc: point({latitude: 51.5074, longitude: -0.1278})})",
            &mut store,
            "default",
        )
        .expect("create");
    let batch = engine
        .execute_mut(
            &format!(
                "MATCH (c:City) WHERE point.withinBBox(c.loc, \
                 point({{latitude: 50.0, longitude: -1.0}}), \
                 point({{latitude: 52.0, longitude: 1.0}})) RETURN count(c) AS n"
            ),
            &mut store,
            "default",
        )
        .expect("query");
    let (_, v) = batch.records[0].bindings()[0].clone();
    assert_eq!(v, Value::Property(PropertyValue::Integer(1)));
}

#[test]
fn distance_filters_a_stored_point() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for (name, lat, lon) in [("London", 51.5074, -0.1278), ("New York", 40.7128, -74.0060)] {
        engine
            .execute_mut(
                &format!(
                    "CREATE (:City {{name: '{name}', loc: point({{latitude: {lat}, longitude: {lon}}})}})"
                ),
                &mut store,
                "default",
            )
            .expect("create");
    }
    let batch = engine
        .execute_mut(
            &format!(
                "MATCH (c:City) WHERE point.distance(c.loc, {PARIS}) < 500000 \
                 RETURN c.name AS name"
            ),
            &mut store,
            "default",
        )
        .expect("query");
    assert_eq!(batch.records.len(), 1, "only London is within 500 km of Paris");
}
