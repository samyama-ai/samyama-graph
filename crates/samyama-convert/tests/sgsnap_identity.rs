//! `.sgsnap` → `.sgsnap` through the converter is the identity (#1908).
//!
//! The plumbing proved before any foreign format lands: read, write and
//! `convert` together must give back the graph that went in -- every node with
//! its labels and properties, every edge with its type, endpoints and
//! properties -- and say it dropped nothing.

use samyama::graph::{GraphStore, PropertyValue};
use samyama_convert::{convert, ConvertError, Format, Options};

/// Two labels, an edge with a property, and the property types a snapshot has
/// to carry: string, integer, float, boolean, list.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let a = s.create_node("Person");
    s.set_node_property("default", a, "name", "Ada").unwrap();
    s.set_node_property("default", a, "born", 1815i64).unwrap();
    s.set_node_property("default", a, "score", 9.5f64).unwrap();
    let b = s.create_node("Engine");
    s.set_node_property("default", b, "working", false).unwrap();
    s.set_node_property(
        "default",
        b,
        "parts",
        PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]),
    )
    .unwrap();
    let e = s.create_edge(a, b, "DESIGNED").unwrap();
    s.set_edge_property(e, "year", 1837i64).unwrap();
    s
}

/// Everything a reader can see, in an order that does not depend on ids.
///
/// Nodes are read through `node_materialized`, as persistence reads them: most
/// node properties live in the column store, and a node's row copy alone shows
/// only the ones that never moved there.
fn contents(s: &GraphStore) -> (Vec<String>, Vec<String>) {
    let materialized = |id| s.node_materialized(id).expect("listed node exists");
    let mut nodes: Vec<String> = s
        .all_nodes()
        .iter()
        .map(|n| materialized(n.id))
        .map(|n| {
            let mut labels: Vec<_> = n.labels.iter().map(|l| l.as_str().to_string()).collect();
            labels.sort();
            let mut props: Vec<_> = n
                .properties
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect();
            props.sort();
            format!("{labels:?} {props:?}")
        })
        .collect();
    nodes.sort();
    let name = |id| {
        let n = materialized(id);
        n.properties
            .get("name")
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|| format!("{:?}", n.labels))
    };
    let mut edges: Vec<String> = s
        .all_edges()
        .iter()
        .map(|e| {
            let mut props: Vec<_> = e
                .properties
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect();
            props.sort();
            format!(
                "{} -{}-> {} {props:?}",
                name(e.source),
                e.edge_type.as_str(),
                name(e.target)
            )
        })
        .collect();
    edges.sort();
    (nodes, edges)
}

#[test]
fn sgsnap_to_sgsnap_gives_back_the_graph_and_drops_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.sgsnap");
    let output = dir.path().join("nested/out.sgsnap");
    let original = graph();
    samyama::snapshot::write_file(&original, &input, &Default::default()).unwrap();

    let report = convert(&input, &output, &Options::default()).unwrap();
    assert_eq!(
        (report.input_format, report.output_format),
        (Format::Sgsnap, Format::Sgsnap)
    );
    assert_eq!(
        (report.input.node_count(), report.input.edge_count()),
        (2, 1)
    );
    assert_eq!(
        (report.output.node_count(), report.output.edge_count()),
        (2, 1)
    );

    let mut back = GraphStore::new();
    samyama::snapshot::import_tenant(&mut back, std::fs::File::open(&output).unwrap()).unwrap();
    assert_eq!(contents(&back), contents(&original));

    // Whatever the snapshot format itself declines to carry is reported, not
    // hidden; with `require_lossless` it becomes the error.
    let strict = Options {
        require_lossless: true,
        ..Options::default()
    };
    match convert(&input, &dir.path().join("strict.sgsnap"), &strict) {
        Ok(r) => assert!(
            r.lossless(),
            "a lossy write passed require_lossless: {:?}",
            r.dropped()
        ),
        Err(ConvertError::Lossy(d)) => assert!(!d.is_empty()),
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn an_unsupported_output_is_refused_before_the_input_is_read() {
    let dir = tempfile::tempdir().unwrap();
    // The input does not exist: a refusal that names the output format proves
    // the formats were checked first.
    let err = convert(
        &dir.path().join("missing.sgsnap"),
        &dir.path().join("out.json"),
        &Options::default(),
    )
    .unwrap_err();
    assert!(
        matches!(err, ConvertError::CannotWrite(Format::Neo4jJson)),
        "{err}"
    );
    assert!(!dir.path().join("out.json").exists());

    let err = convert(
        &dir.path().join("in.xyz"),
        &dir.path().join("out.sgsnap"),
        &Options::default(),
    )
    .unwrap_err();
    assert!(matches!(err, ConvertError::UnknownFormat(_)), "{err}");
}

/// `require_lossless` refuses before the destination is replaced (review on
/// #1938). A graph with an edge always reports `edge_creation_timestamps` on
/// export, so the strict write below is lossy every time.
#[test]
fn a_refused_lossy_write_leaves_the_destination_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let strict = Options {
        require_lossless: true,
        ..Options::default()
    };
    let lossy = graph();

    // An existing destination keeps its bytes.
    let existing = dir.path().join("existing.sgsnap");
    std::fs::write(&existing, b"previous contents").unwrap();
    let err = samyama_convert::write_path(&lossy, &existing, &strict).unwrap_err();
    assert!(
        matches!(err, ConvertError::Lossy(ref d) if !d.is_empty()),
        "{err}"
    );
    assert_eq!(std::fs::read(&existing).unwrap(), b"previous contents");

    // An absent destination stays absent.
    let absent = dir.path().join("absent.sgsnap");
    let err = samyama_convert::write_path(&lossy, &absent, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert!(!absent.exists());

    // An in-place conversion does not overwrite its own input.
    let input = dir.path().join("in_place.sgsnap");
    samyama::snapshot::write_file(&lossy, &input, &Default::default()).unwrap();
    let before = std::fs::read(&input).unwrap();
    let err = convert(&input, &input, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert_eq!(std::fs::read(&input).unwrap(), before);

    // No temporary file is left behind by any of them.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}
