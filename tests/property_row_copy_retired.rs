//! No production path leaves a property in the row copy, and no reader prefers
//! it (#2000, phase 1).
//!
//! Store writes stopped writing `Node::properties` and the edge row map in
//! #1188 and #545, but three paths still did: recovery from RocksDB put every
//! value of a restarted graph in the row, snapshot import put non-scalars (and
//! every value of a v1 file) there as well, and the server's demo seed wrote
//! through `get_node_mut`. Several readers also asked the row before the
//! column, so a row value a store write had replaced was still indexed and
//! exported.
//!
//! The row can only be deleted once nothing writes it and nothing reads it
//! except the store's own fallback. These tests hold both halves: a recovered or
//! imported graph has an empty row and answers through every read path, and a
//! row value the column has superseded is visible through none of them.

use samyama::algo::build_view;
use samyama::graph::{Edge, EdgeId, GraphStore, Label, Node, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::snapshot::{export_tenant, import_tenant};

fn s(v: &str) -> PropertyValue {
    PropertyValue::String(v.to_string())
}

fn column(store: &GraphStore, cypher: &str) -> Vec<PropertyValue> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(p)) => p.clone(),
            other => panic!("{cypher}: {other:?}"),
        })
        .collect()
}

fn index_finds(store: &GraphStore, label: &str, key: &str, value: PropertyValue) -> Vec<NodeId> {
    let index = store
        .property_index
        .get_index(&Label::new(label), key)
        .expect("index declared");
    let found = index.read().unwrap().get(&value);
    found
}

/// Two `:P` nodes and one `:R` edge between them, recovered the way a restart
/// recovers them: from the `Node` and `Edge` values RocksDB hands back.
fn recovered() -> GraphStore {
    let mut store = GraphStore::new();
    for (id, name, age) in [(1u64, "Ann", 30i64), (2, "Bob", 25)] {
        let mut node = Node::new(NodeId::new(id), Label::new("P"));
        node.set_property("name", name);
        node.set_property("age", age);
        node.set_property("tags", PropertyValue::Array(vec![s("a"), s("b")]));
        store.insert_recovered_node(node);
    }
    let mut edge = Edge::new(EdgeId::new(7), NodeId::new(1), NodeId::new(2), "R");
    edge.set_property("w", 2.5f64);
    store.insert_recovered_edge(edge).unwrap();
    store
}

#[test]
fn a_recovered_graph_keeps_no_row_copy() {
    let store = recovered();
    for id in [1, 2] {
        assert!(
            store
                .get_node(NodeId::new(id))
                .unwrap()
                .properties
                .is_empty(),
            "recovery left node {id}'s values in the row"
        );
    }
    assert!(
        store
            .get_edge_properties(EdgeId::new(7))
            .is_none_or(|p| p.is_empty()),
        "recovery left the edge's values in the row"
    );
    assert_eq!(
        store.node_columns.get_property(1, "age"),
        PropertyValue::Integer(30)
    );
    assert_eq!(
        store.edge_columns.get_property(7, "w"),
        PropertyValue::Float(2.5)
    );
}

#[test]
fn a_recovered_graph_answers_through_every_read_path() {
    let mut store = recovered();
    let ann = NodeId::new(1);

    // The store's accessors.
    assert_eq!(store.node_property(ann, "name"), Some(s("Ann")));
    assert_eq!(
        store
            .node_materialized(ann)
            .unwrap()
            .properties
            .get("age")
            .cloned(),
        Some(PropertyValue::Integer(30))
    );
    assert_eq!(
        store.edge_property(EdgeId::new(7), "w"),
        Some(PropertyValue::Float(2.5))
    );
    assert_eq!(
        store
            .get_edge(EdgeId::new(7))
            .unwrap()
            .properties
            .get("w")
            .cloned(),
        Some(PropertyValue::Float(2.5))
    );

    // Cypher: RETURN, WHERE, ORDER BY, a list value, a relationship property.
    assert_eq!(
        column(&store, "MATCH (n:P) RETURN n.name AS c ORDER BY n.age"),
        vec![s("Bob"), s("Ann")]
    );
    assert_eq!(
        column(&store, "MATCH (n:P) WHERE n.age > 26 RETURN n.name AS c"),
        vec![s("Ann")]
    );
    assert_eq!(
        column(&store, "MATCH (n:P {name: 'Ann'}) RETURN size(n.tags) AS c"),
        vec![PropertyValue::Integer(2)]
    );
    assert_eq!(
        column(&store, "MATCH (:P)-[r:R]->(:P) RETURN r.w AS c"),
        vec![PropertyValue::Float(2.5)]
    );

    // An index declared after recovery, as `restore_index_catalog` declares it.
    assert_eq!(store.create_property_index(&Label::new("P"), "name"), 2);
    assert_eq!(index_finds(&store, "P", "name", s("Ann")), vec![ann]);

    // The algorithm adapter's edge weights.
    let view = build_view(&store, Some("P"), Some("R"), Some("w"));
    assert_eq!(view.weights, Some(vec![2.5]));

    // A snapshot of the restarted graph carries every value.
    let mut bytes = Vec::new();
    export_tenant(&store, &mut bytes).unwrap();
    let mut back = GraphStore::new();
    import_tenant(&mut back, &bytes[..]).unwrap();
    assert_eq!(
        column(&back, "MATCH (n:P) RETURN n.name AS c ORDER BY n.name"),
        vec![s("Ann"), s("Bob")]
    );
    assert_eq!(
        column(&back, "MATCH (:P)-[r:R]->(:P) RETURN r.w AS c"),
        vec![PropertyValue::Float(2.5)]
    );
}

#[test]
fn an_import_keeps_no_row_copy_for_any_type() {
    let mut src = GraphStore::new();
    let n = src.create_node("Doc");
    src.set_node_property("default", n, "title", "t").unwrap();
    src.set_node_property(
        "default",
        n,
        "tags",
        PropertyValue::Array(vec![s("x"), s("y")]),
    )
    .unwrap();
    src.set_node_property(
        "default",
        n,
        "at",
        PropertyValue::DateTime(1_700_000_000_000),
    )
    .unwrap();
    let mut bytes = Vec::new();
    export_tenant(&src, &mut bytes).unwrap();

    let mut dst = GraphStore::new();
    import_tenant(&mut dst, &bytes[..]).unwrap();
    let id = dst.node_ids_by_label(&Label::new("Doc"), None)[0];
    assert!(
        dst.get_node(id).unwrap().properties.is_empty(),
        "the import left a non-scalar in the row"
    );
    for key in ["title", "tags", "at"] {
        assert_eq!(
            dst.node_property(id, key),
            src.node_property(n, key),
            "{key}"
        );
    }

    // A second import merging into the same node by a dedup key writes nothing
    // to the row either.
    let mut merged = GraphStore::new();
    import_tenant(&mut merged, &bytes[..]).unwrap();
    samyama::snapshot::import_tenant_with_dedup(&mut merged, &bytes[..], &["title"]).unwrap();
    for id in merged.node_ids_by_label(&Label::new("Doc"), None) {
        assert!(
            merged.get_node(id).unwrap().properties.is_empty(),
            "the merge left a row copy"
        );
        assert_eq!(
            merged.node_property(id, "tags"),
            src.node_property(n, "tags")
        );
    }
}

/// A row value a store write has replaced in the column, which only
/// `get_node_mut` can still produce. Every reader must see the column's.
#[test]
fn a_superseded_row_value_is_read_nowhere() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    store.get_node_mut(n).unwrap().set_property("k", "old");
    store.set_column_property(n, "k", s("new"));

    assert_eq!(store.node_property(n, "k"), Some(s("new")));
    assert_eq!(
        column(&store, "MATCH (n:P) WHERE n.k = 'new' RETURN n.k AS c"),
        vec![s("new")]
    );
    assert!(column(&store, "MATCH (n:P) WHERE n.k = 'old' RETURN n.k AS c").is_empty());

    // The index backfill read the row first.
    store.create_property_index(&Label::new("P"), "k");
    assert_eq!(index_finds(&store, "P", "k", s("new")), vec![n]);
    assert!(
        index_finds(&store, "P", "k", s("old")).is_empty(),
        "the index holds the row's value"
    );

    // So did the snapshot export.
    let mut bytes = Vec::new();
    export_tenant(&store, &mut bytes).unwrap();
    let mut back = GraphStore::new();
    import_tenant(&mut back, &bytes[..]).unwrap();
    let id = back.node_ids_by_label(&Label::new("P"), None)[0];
    assert_eq!(
        back.node_property(id, "k"),
        Some(s("new")),
        "the export took the row's value"
    );
}

/// The bytes the row copies hold after a restart, against a graph of the same
/// shape built through the store in this process. Before this phase the
/// recovered graph's rows held every value; now neither holds any, so the
/// recovered side must not exceed the built one.
#[test]
fn recovery_holds_no_more_row_bytes_than_a_built_graph() {
    const N: u64 = 200;
    let mut built = GraphStore::new();
    let mut restarted = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..N {
        let props = [
            ("name".to_string(), s(&format!("node-{i}"))),
            ("i".to_string(), PropertyValue::Integer(i as i64)),
        ];
        let id = built.create_node_with_properties(
            "default",
            vec![Label::new("P")],
            props.iter().cloned().collect(),
        );
        ids.push(id);
        let mut node = Node::new(id, Label::new("P"));
        for (k, v) in props {
            node.set_property(k, v);
        }
        restarted.insert_recovered_node(node);
    }
    for (i, pair) in ids.windows(2).enumerate() {
        let e = built.create_edge(pair[0], pair[1], "R").unwrap();
        built.set_edge_property(e, "w", i as f64).unwrap();
        let mut edge = Edge::new(e, pair[0], pair[1], "R");
        edge.set_property("w", i as f64);
        restarted.insert_recovered_edge(edge).unwrap();
    }

    let (b, r) = (built.memory_report(), restarted.memory_report());
    assert!(
        r.node_properties <= b.node_properties,
        "row bytes after a restart {} exceed the built graph's {}",
        r.node_properties,
        b.node_properties
    );
    assert!(
        r.edge_properties <= b.edge_properties,
        "edge row bytes after a restart {} exceed the built graph's {}",
        r.edge_properties,
        b.edge_properties
    );
    // And the values are where the built graph keeps them.
    assert_eq!(
        restarted.node_property(ids[5], "name"),
        built.node_property(ids[5], "name")
    );
}
