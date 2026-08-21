//! A snapshot round trip preserves property *values*, not just counts.
//!
//! `snapshot_persistence.rs` asserts `node_count == 2` and `edge_count == 1`
//! after a restore. A round trip that dropped every property would satisfy
//! both, so the existing coverage cannot detect the failure that would matter
//! most — and #545 proposes removing the row copy of properties, which is
//! exactly the change that would cause it.
//!
//! The double round trip is the case worth pinning. After an import,
//! `node.properties` is **empty by design**: values live in the columnar
//! store. Exporting *that* store is therefore a different situation from
//! exporting a freshly-built one, and it is the one a real deployment hits —
//! restore from a snapshot, then take a new snapshot.
//!
//! Note for anyone extending this: read values through the column or the
//! merged view. Reading `node.properties` after an import shows `None` whether
//! or not anything was lost, so an assertion on the row proves nothing.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::snapshot::{export_tenant, import_tenant};

fn prop(store: &GraphStore, idx: usize, key: &str) -> PropertyValue {
    store.node_columns.get_property(idx, key)
}

#[test]
fn properties_survive_one_round_trip() {
    let mut src = GraphStore::new();
    let n = src.create_node("Person");
    src.set_node_property("default", n, "name", "Alice").unwrap();
    src.set_node_property("default", n, "age", 30i64).unwrap();

    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    import_tenant(&mut dst, &buf[..]).expect("import");

    let idx = n.as_u64() as usize;
    assert_eq!(dst.node_count(), 1);
    assert_eq!(prop(&dst, idx, "name"), PropertyValue::String("Alice".into()));
    assert_eq!(prop(&dst, idx, "age"), PropertyValue::Integer(30));
}

#[test]
fn properties_survive_a_second_round_trip_from_an_imported_store() {
    // Restore from a snapshot, then snapshot again — the shape a deployment
    // actually performs, and the one where the row copy is already empty.
    let mut src = GraphStore::new();
    let n = src.create_node("Person");
    src.set_node_property("default", n, "name", "Alice").unwrap();

    let mut buf1 = Vec::new();
    export_tenant(&src, &mut buf1).expect("export 1");
    let mut mid = GraphStore::new();
    import_tenant(&mut mid, &buf1[..]).expect("import 1");

    let idx = n.as_u64() as usize;
    assert!(
        mid.get_node(n).map(|x| x.properties.is_empty()).unwrap_or(true),
        "precondition: after an import the row copy is empty and the column holds the value"
    );

    let mut buf2 = Vec::new();
    export_tenant(&mid, &mut buf2).expect("export 2");
    let mut end = GraphStore::new();
    import_tenant(&mut end, &buf2[..]).expect("import 2");

    assert_eq!(
        prop(&end, idx, "name"),
        PropertyValue::String("Alice".into()),
        "the value must survive an export taken from an imported store"
    );
}

#[test]
fn non_scalar_properties_survive_a_round_trip() {
    // Arrays and maps live in `Column::Other`. They are the variants #545
    // called out as row-only, and are no longer, so a round trip must carry
    // them.
    let mut src = GraphStore::new();
    let n = src.create_node("Thing");
    src.set_node_property(
        "default", n, "tags",
        PropertyValue::Array(vec![PropertyValue::String("a".into()), PropertyValue::Integer(2)]),
    ).unwrap();

    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    import_tenant(&mut dst, &buf[..]).expect("import");

    match prop(&dst, n.as_u64() as usize, "tags") {
        PropertyValue::Array(items) => assert_eq!(items.len(), 2, "both elements survive"),
        other => panic!("expected an array after the round trip, got {other:?}"),
    }
}
