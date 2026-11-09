//! `CREATE / SHOW / DROP / REBUILD STRUCTURE INDEX` (SGI, ADR-041 / #2036).
//!
//! The SGI index existed before this DDL and was reachable only over HTTP, so
//! a graph's structure was something each caller asserted per request. These
//! tests pin the declaration surface: what the grammar accepts, what it
//! refuses, and that a declaration survives in the registry `GraphStore` owns.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::index::structure::ManifoldSpec;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// Runs a write statement and hands back the error instead of panicking.
fn write_err(s: &mut GraphStore, q: &str) -> String {
    let query = match parse_query(q) {
        Ok(query) => query,
        Err(e) => return format!("{e:?}"),
    };
    match MutQueryExecutor::new(s, "default".into()).execute(&query) {
        Ok(_) => panic!("{q}: expected an error, got success"),
        Err(e) => format!("{e:?}"),
    }
}

fn rows(s: &GraphStore, q: &str) -> Vec<std::collections::HashMap<String, Value>> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| {
            [
                "name", "label", "manifold", "coordinates", "contact_radius",
                "backbone", "group_by", "measure", "points",
            ]
                .iter()
                .filter_map(|k| r.get(k).map(|v| (k.to_string(), v.clone())))
                .collect()
        })
        .collect()
}

fn text_of(v: &Value) -> String {
    match v {
        Value::Property(PropertyValue::String(s)) => s.clone(),
        other => panic!("expected a string, got {other:?}"),
    }
}

/// Three residues with coordinates, which is what an SGI index is built from.
fn protein() -> GraphStore {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "UNWIND range(1,30) AS i CREATE (:Residue {x: toFloat(i), y: toFloat(i) * 0.5, \
         z: toFloat(i) * 0.25, residue_seq: i, chain_id: 'A', b_factor: toFloat(i) * 2.0})",
    );
    s
}

#[test]
fn a_structure_index_is_declared_and_then_listed() {
    let mut s = protein();
    write(
        &mut s,
        "CREATE STRUCTURE INDEX pdb ON :Residue(x, y, z) METRIC euclidean \
         BACKBONE residue_seq GROUP BY chain_id CONTACT_RADIUS 8.0 \
         MEASURE b_factor AGGREGATE sum, min, max, count",
    );

    // The declaration is in the registry the store owns, not in the operator.
    let declared = s.structure_registry.get("pdb").expect("pdb is registered");
    assert_eq!(declared.label.as_deref(), Some("Residue"));
    assert_eq!(declared.measure_prop.as_deref(), Some("b_factor"));
    match &declared.manifold {
        ManifoldSpec::Euclidean3d {
            x_prop,
            y_prop,
            z_prop,
            sequence_prop,
            group_by_prop,
            contact_radius,
            ..
        } => {
            assert_eq!((x_prop.as_str(), y_prop.as_str(), z_prop.as_str()), ("x", "y", "z"));
            assert_eq!(sequence_prop.as_deref(), Some("residue_seq"));
            assert_eq!(group_by_prop.as_deref(), Some("chain_id"));
            assert_eq!(*contact_radius, Some(8.0));
        }
        other => panic!("expected Euclidean3d, got {other:?}"),
    }

    let listed = rows(&s, "SHOW STRUCTURE INDEXES");
    let pdb = listed
        .iter()
        .find(|r| r.get("name").map(text_of).as_deref() == Some("pdb"))
        .expect("SHOW lists the declared index");
    assert_eq!(text_of(&pdb["manifold"]), "euclidean3d");
    assert_eq!(text_of(&pdb["coordinates"]), "x, y, z");
}

#[test]
fn show_reports_every_convention_the_ddl_accepts() {
    // Asserting these through the registry is not enough, and that is how the
    // gap got here: the first version of this suite read `sequence_prop` and
    // `group_by_prop` off the declaration, so BACKBONE and GROUP BY could be
    // declared and never reported. A convention a caller can set and cannot
    // read back is the shape of #1494.
    let mut s = protein();
    write(
        &mut s,
        "CREATE STRUCTURE INDEX pdb ON :Residue(x, y, z) \
         BACKBONE residue_seq GROUP BY chain_id CONTACT_RADIUS 8.0 MEASURE b_factor",
    );
    let listed = rows(&s, "SHOW STRUCTURE INDEXES");
    let pdb = listed
        .iter()
        .find(|r| r.get("name").map(text_of).as_deref() == Some("pdb"))
        .expect("SHOW lists the declared index");
    assert_eq!(text_of(&pdb["backbone"]), "residue_seq");
    assert_eq!(text_of(&pdb["group_by"]), "chain_id");
    assert_eq!(text_of(&pdb["measure"]), "b_factor");

    // A declaration that omits them reports Null, not a missing column: the row
    // shape must not depend on what was declared.
    write(&mut s, "CREATE STRUCTURE INDEX bare ON :Residue(x, y, z)");
    let listed = rows(&s, "SHOW STRUCTURE INDEXES");
    let bare = listed
        .iter()
        .find(|r| r.get("name").map(text_of).as_deref() == Some("bare"))
        .expect("SHOW lists the bare index");
    assert!(matches!(bare["backbone"], Value::Null), "{:?}", bare["backbone"]);
    assert!(matches!(bare["group_by"], Value::Null), "{:?}", bare["group_by"]);
}

#[test]
fn two_coordinates_declare_a_geographic_convention() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "UNWIND range(1,20) AS i CREATE (:Tower {lat: 12.9 + toFloat(i) * 0.01, \
         lon: 77.5 + toFloat(i) * 0.01})",
    );
    write(&mut s, "CREATE STRUCTURE INDEX towers ON :Tower(lat, lon)");
    match &s.structure_registry.get("towers").unwrap().manifold {
        ManifoldSpec::GeoSpherical {
            lat_prop,
            lon_prop,
            alt_prop,
            ..
        } => {
            assert_eq!((lat_prop.as_str(), lon_prop.as_str()), ("lat", "lon"));
            assert_eq!(*alt_prop, None);
        }
        other => panic!("expected GeoSpherical, got {other:?}"),
    }
}

#[test]
fn an_index_over_a_label_without_coordinates_is_refused() {
    // The point of building at CREATE time: a declaration that would answer
    // nothing is an error now rather than an empty result later.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:Residue {name: 'no coordinates here'})");
    let err = write_err(&mut s, "CREATE STRUCTURE INDEX pdb ON :Residue(x, y, z)");
    assert!(err.contains("would be empty"), "{err}");
    assert!(s.structure_registry.get("pdb").is_none(), "nothing registered");
}

#[test]
fn dropping_names_whether_it_was_there() {
    let mut s = protein();
    write(&mut s, "CREATE STRUCTURE INDEX pdb ON :Residue(x, y, z)");
    write(&mut s, "DROP STRUCTURE INDEX pdb");
    assert!(s.structure_registry.get("pdb").is_none());

    // A typo must not read as a successful drop.
    let err = write_err(&mut s, "DROP STRUCTURE INDEX pdbb");
    assert!(err.contains("no such structure index"), "{err}");
}

#[test]
fn rebuilding_reports_the_points_it_indexed() {
    let mut s = protein();
    write(&mut s, "CREATE STRUCTURE INDEX pdb ON :Residue(x, y, z)");
    let before = rows(&s, "SHOW STRUCTURE INDEXES").len();
    write(
        &mut s,
        "UNWIND range(31,40) AS i CREATE (:Residue {x: toFloat(i), y: toFloat(i), z: toFloat(i)})",
    );
    // REBUILD re-reads the store, so the count follows the graph rather than
    // the declaration.
    write(&mut s, "REBUILD STRUCTURE INDEX pdb");
    assert_eq!(rows(&s, "SHOW STRUCTURE INDEXES").len(), before);

    let err = write_err(&mut s, "REBUILD STRUCTURE INDEX nope");
    assert!(err.contains("no such structure index"), "{err}");
}

#[test]
fn aggregate_without_measure_is_refused() {
    assert!(
        parse_query("CREATE STRUCTURE INDEX p ON :R(x, y, z) AGGREGATE sum").is_err(),
        "AGGREGATE has nothing to roll up without MEASURE"
    );
}

#[test]
fn a_contact_radius_must_be_a_positive_distance() {
    for bad in ["0", "-1", "0.0"] {
        let q = format!("CREATE STRUCTURE INDEX p ON :R(x, y, z) CONTACT_RADIUS {bad}");
        assert!(
            parse_query(&q).is_err(),
            "CONTACT_RADIUS {bad} yields no contacts and must be refused"
        );
    }
    assert!(parse_query("CREATE STRUCTURE INDEX p ON :R(x, y, z) CONTACT_RADIUS 8.0").is_ok());
}

#[test]
fn euclidean_needs_three_coordinates() {
    assert!(
        parse_query("CREATE STRUCTURE INDEX p ON :R(lat, lon) METRIC euclidean").is_err(),
        "a 3D metric over two coordinates has no z"
    );
    assert!(parse_query("CREATE STRUCTURE INDEX p ON :R(x, y, z) METRIC euclidean").is_ok());
}

#[test]
fn the_ddl_is_a_write_and_show_is_not() {
    // `is_write` routes a statement to the mutable executor; a SHOW that
    // claimed to write would take a write lock to read a declaration.
    assert!(parse_query("CREATE STRUCTURE INDEX p ON :R(x, y, z)").unwrap().is_write());
    assert!(parse_query("DROP STRUCTURE INDEX p").unwrap().is_write());
    assert!(parse_query("REBUILD STRUCTURE INDEX p").unwrap().is_write());
    assert!(!parse_query("SHOW STRUCTURE INDEXES").unwrap().is_write());
}

#[test]
fn show_structure_index_is_accepted_singular_too() {
    assert!(parse_query("SHOW STRUCTURE INDEX").unwrap().show_structure_indexes);
    assert!(parse_query("SHOW STRUCTURE INDEXES").unwrap().show_structure_indexes);
}
