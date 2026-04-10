//! Integration tests for MVCC functionality
//!
//! Verifies versioning, snapshot isolation, and historical reads.

use samyama::graph::{GraphStore, NodeId, PropertyValue};

#[test]
fn test_mvcc_snapshot_isolation() {
    let mut store = GraphStore::new();
    
    // T1: Create initial data (Version 1)
    let n1 = store.create_node("Account");
    store.set_node_property("default", n1, "balance", 100).unwrap();
    let v1 = store.current_version;

    // T2: Update data (Version 2)
    store.current_version = 2;
    store.set_node_property("default", n1, "balance", 200).unwrap();
    let v2 = store.current_version;

    // T3: Read Version 1
    let node_v1 = store.get_node_at_version(n1, v1).unwrap();
    assert_eq!(node_v1.version, 1);
    assert_eq!(node_v1.get_property("balance").unwrap().as_integer(), Some(100));

    // T4: Read Version 2
    let node_v2 = store.get_node_at_version(n1, v2).unwrap();
    assert_eq!(node_v2.version, 2);
    assert_eq!(node_v2.get_property("balance").unwrap().as_integer(), Some(200));
    
    // T5: Read latest (implicit)
    let node_latest = store.get_node(n1).unwrap();
    assert_eq!(node_latest.get_property("balance").unwrap().as_integer(), Some(200));
}

#[test]
fn test_mvcc_historical_preservation() {
    let mut store = GraphStore::new();
    let node_id = store.create_node("History");
    
    // Create 10 versions
    for i in 1..=10 {
        store.current_version = i;
        store.set_node_property("default", node_id, "v", i as i64).unwrap();
    }
    
    // Verify all 10 versions exist and are correct
    for i in 1..=10 {
        let node = store.get_node_at_version(node_id, i as u64).unwrap();
        // Since we update property "v" to i at version i
        // Version 1 has v=1, Version 2 has v=2, etc.
        let v = node.get_property("v").unwrap().as_integer().unwrap();
        assert_eq!(v, i as i64, "Mismatch at version {}", i);
        assert!(node.version <= i as u64);
    }
}

#[test]
fn test_mvcc_edge_version_history() {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("Person");
    let eid = store.create_edge(a, b, "KNOWS").unwrap();

    // Version 1: set initial property
    store.set_edge_property(eid, "weight", 1.0f64).unwrap();
    let v1 = store.current_version;

    // Version 2: update property
    store.current_version = 2;
    store.set_edge_property(eid, "weight", 2.0f64).unwrap();
    let v2 = store.current_version;

    // Version 3: update again
    store.current_version = 3;
    store.set_edge_property(eid, "weight", 3.0f64).unwrap();
    let v3 = store.current_version;

    // Read at each historical version — OSS uses edge arena MVCC
    let edge_v1 = store.get_edge_at_version(eid, v1).unwrap();
    assert_eq!(edge_v1.properties.get("weight"), Some(&PropertyValue::Float(1.0)));

    let edge_v2 = store.get_edge_at_version(eid, v2).unwrap();
    assert_eq!(edge_v2.properties.get("weight"), Some(&PropertyValue::Float(2.0)));

    let edge_v3 = store.get_edge_at_version(eid, v3).unwrap();
    assert_eq!(edge_v3.properties.get("weight"), Some(&PropertyValue::Float(3.0)));
}

#[test]
fn test_mvcc_edge_snapshot_isolation() {
    let mut store = GraphStore::new();
    let a = store.create_node("Account");
    let b = store.create_node("Account");
    let eid = store.create_edge(a, b, "TRANSFER").unwrap();

    // V1: amount = 100
    store.set_edge_property(eid, "amount", 100i64).unwrap();
    let v1 = store.current_version;

    // V2: amount = 500
    store.current_version = 2;
    store.set_edge_property(eid, "amount", 500i64).unwrap();

    // Snapshot read at V1 should see 100
    let edge_v1 = store.get_edge_at_version(eid, v1).unwrap();
    assert_eq!(edge_v1.properties.get("amount").unwrap().as_integer(), Some(100));

    // Current read should see 500
    let edge_current = store.get_edge(eid).unwrap();
    assert_eq!(edge_current.properties.get("amount").unwrap().as_integer(), Some(500));
}
