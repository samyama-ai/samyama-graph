//! `snapshot::write_file` writes a standalone store as a `.sgsnap` (#1907).
//!
//! The call every `samyama convert` writer makes (#1905). Pinned here: what it
//! writes imports back as the same graph, node by node and edge by edge; it
//! creates the directories it needs; and the file appears whole or not at
//! all, with no `.tmp` left behind.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::{import_tenant, write_file, WriteOptions};
use std::collections::BTreeMap;

const T: &str = "default";

/// Three labels, two edge types, typed properties, and an edge property.
fn sample() -> GraphStore {
    let mut g = GraphStore::new();
    let ada = g.create_node("Person");
    g.add_label_to_node(T, ada, "Scientist").unwrap();
    g.set_node_property(T, ada, "uid", "ada").unwrap();
    g.set_node_property(T, ada, "born", PropertyValue::Date(-56_000)).unwrap();
    g.set_node_property(T, ada, "score", 9.5).unwrap();
    g.set_node_property(T, ada, "tags", PropertyValue::Array(vec!["math".into(), "poetry".into()]))
        .unwrap();

    let alan = g.create_node("Person");
    g.set_node_property(T, alan, "uid", "alan").unwrap();
    g.set_node_property(T, alan, "active", false).unwrap();
    g.set_node_property(T, alan, "embedding", PropertyValue::Vector(vec![0.5, -0.25])).unwrap();

    let london = g.create_node("City");
    g.set_node_property(T, london, "uid", "london").unwrap();
    g.set_node_property(T, london, "population", 8_800_000i64).unwrap();

    let knows = g.create_edge(ada, alan, "KNOWS").unwrap();
    g.set_edge_property(
        knows,
        "since",
        PropertyValue::Duration { months: 12, days: 0, seconds: 0, nanos: 0 },
    )
    .unwrap();
    g.create_edge(ada, london, "LIVES_IN").unwrap();
    g.create_edge(alan, london, "LIVES_IN").unwrap();
    g
}

type Nodes = BTreeMap<String, (Vec<String>, BTreeMap<String, String>)>;
type Edges = Vec<(String, String, String, BTreeMap<String, String>)>;

/// The graph keyed by `uid`, which the import's id remapping does not touch.
fn shape(g: &GraphStore) -> (Nodes, Edges) {
    let uid = |id| match g.node_properties_merged(id).get("uid") {
        Some(PropertyValue::String(s)) => s.clone(),
        other => panic!("a node without a uid: {other:?}"),
    };
    let props = |m: std::collections::HashMap<String, PropertyValue>| {
        m.into_iter().map(|(k, v)| (k, format!("{v:?}"))).collect::<BTreeMap<_, _>>()
    };
    let nodes = g
        .all_nodes()
        .into_iter()
        .map(|n| {
            let mut labels: Vec<String> = n.labels.iter().map(|l| l.as_str().to_string()).collect();
            labels.sort();
            (uid(n.id), (labels, props(g.node_properties_merged(n.id))))
        })
        .collect();
    let mut edges: Edges = g
        .all_edges()
        .into_iter()
        .map(|e| {
            (
                uid(e.source),
                e.edge_type.as_str().to_string(),
                uid(e.target),
                props(g.edge_properties_merged(e.id)),
            )
        })
        .collect();
    edges.sort();
    (nodes, edges)
}

#[test]
fn what_it_writes_imports_back_as_the_same_graph() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nested/deeper/sample.sgsnap");
    let source = sample();

    let stats = write_file(&source, &path, &WriteOptions::default()).expect("write_file");
    assert_eq!(stats.node_count, 3);
    assert_eq!(stats.edge_count, 3);
    assert!(path.is_file(), "the parent directories were created and the file is there");
    assert!(
        !dir.path().join("nested/deeper/sample.sgsnap.tmp").exists(),
        "the temporary file was renamed, not left beside it"
    );

    let mut back = GraphStore::new();
    let imported = import_tenant(&mut back, std::fs::File::open(&path).unwrap()).expect("import");
    assert_eq!(imported.node_count, 3);
    assert_eq!(imported.edge_count, 3);

    let (want_nodes, want_edges) = shape(&source);
    let (got_nodes, got_edges) = shape(&back);
    for (uid, want) in &want_nodes {
        assert_eq!(got_nodes.get(uid), Some(want), "node {uid}");
    }
    assert_eq!(got_nodes.len(), want_nodes.len());
    assert_eq!(got_edges, want_edges);
}

#[test]
fn an_existing_file_is_replaced_whole() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.sgsnap");
    std::fs::write(&path, b"not a snapshot").unwrap();

    write_file(&sample(), &path, &WriteOptions { compression_level: 1 }).expect("write_file");

    let mut back = GraphStore::new();
    let imported = import_tenant(&mut back, std::fs::File::open(&path).unwrap()).expect("import");
    assert_eq!(imported.node_count, 3);
    assert!(!dir.path().join("g.sgsnap.tmp").exists());
}

#[test]
fn a_write_that_cannot_happen_is_an_error_and_leaves_nothing() {
    // The parent "directory" is a file, so nothing can be created under it.
    let dir = tempfile::tempdir().unwrap();
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"").unwrap();
    let path = blocker.join("g.sgsnap");

    assert!(write_file(&sample(), &path, &WriteOptions::default()).is_err());
    assert!(!path.exists());
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        1,
        "only the blocker; no stray temporary file"
    );
}
