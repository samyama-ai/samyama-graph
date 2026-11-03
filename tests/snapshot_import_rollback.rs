//! A failed snapshot import must not leave part of itself behind (#199).
//!
//! A truncated snapshot returned "unexpected end of file" *and* several million nodes: the
//! caller saw an error and a partially-populated graph, with no way to tell how much had
//! landed or whether a retry would duplicate it.

use std::io::{Read, Write};

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use samyama::snapshot::{export_tenant, import_tenant};

fn good_snapshot(n: usize) -> Vec<u8> {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    for i in 0..n {
        engine
            .execute_mut(&format!("CREATE (:N {{id: {i}}})"), &mut store, "default")
            .unwrap();
    }
    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");
    buf
}

/// Cut a snapshot mid-record, so parsing fails partway through rather than at the start.
fn truncated(buf: &[u8]) -> Vec<u8> {
    let mut plain = String::new();
    flate2::read::GzDecoder::new(buf)
        .read_to_string(&mut plain)
        .expect("decode");
    let lines: Vec<&str> = plain.lines().collect();
    let cut = lines[..lines.len() / 2].join("\n")
        + "\n{\"t\":\"n\",\"id\":999,\"labels\":[\"N\"],\"pro";
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(cut.as_bytes()).expect("encode");
    enc.finish().expect("finish")
}

#[test]
fn a_failed_import_leaves_nothing_behind() {
    let snapshot = truncated(&good_snapshot(50));
    let mut store = GraphStore::new();

    let result = import_tenant(&mut store, &snapshot[..]);

    assert!(result.is_err(), "a truncated snapshot must fail");
    assert_eq!(
        store.all_nodes().len(),
        0,
        "a failed import must not leave partial state"
    );
}

#[test]
fn a_failed_import_does_not_disturb_existing_data() {
    // The rollback must remove what *this* import created and nothing else.
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    engine
        .execute_mut("CREATE (:Existing {k: 1})", &mut store, "default")
        .unwrap();
    engine
        .execute_mut("CREATE (:Existing {k: 2})", &mut store, "default")
        .unwrap();

    let snapshot = truncated(&good_snapshot(50));
    assert!(import_tenant(&mut store, &snapshot[..]).is_err());

    assert_eq!(store.all_nodes().len(), 2, "pre-existing nodes must survive");
    let batch = engine
        .execute("MATCH (x:Existing) RETURN x.k AS k", &store)
        .expect("still queryable");
    assert_eq!(batch.records.len(), 2);
}

#[test]
fn a_good_import_still_works_and_still_works_after_a_failed_one() {
    let good = good_snapshot(50);
    let bad = truncated(&good);

    let mut store = GraphStore::new();
    let stats = import_tenant(&mut store, &good[..]).expect("good import");
    assert_eq!(stats.node_count, 50);
    assert_eq!(store.all_nodes().len(), 50);

    // a failure in between must not poison the store for the next attempt
    let mut store = GraphStore::new();
    assert!(import_tenant(&mut store, &bad[..]).is_err());
    let stats = import_tenant(&mut store, &good[..]).expect("import after failure");
    assert_eq!(stats.node_count, 50);
    assert_eq!(store.all_nodes().len(), 50);
}

// ---------------------------------------------------------------------------
// Export compression (#314)
// ---------------------------------------------------------------------------

#[test]
fn a_snapshot_round_trips_at_every_compression_level() {
    // Export was fixed at gzip level 6, the slowest part of the operation and single
    // threaded — 0.77 MB/s on a billion-edge federation, projecting to 6-8 hours. The level
    // is now a choice, so what matters is that every level still produces a snapshot the
    // importer reads identically.
    use samyama::snapshot::export_tenant_with_compression;

    let engine = QueryEngine::new();
    let mut source = GraphStore::new();
    for i in 0..200 {
        engine
            .execute_mut(&format!("CREATE (:N {{id: {i}, name: \"node {i}\"}})"), &mut source, "default")
            .unwrap();
    }
    for i in 0..100 {
        engine
            .execute_mut(
                &format!("MATCH (a:N {{id: {i}}}), (b:N {{id: {}}}) CREATE (a)-[:R]->(b)", i + 1),
                &mut source, "default",
            )
            .unwrap();
    }

    for level in [0u32, 1, 3, 6, 9] {
        let mut buf = Vec::new();
        export_tenant_with_compression(&source, &mut buf, level)
            .unwrap_or_else(|e| panic!("export at level {level}: {e}"));

        let mut restored = GraphStore::new();
        let stats = import_tenant(&mut restored, &buf[..])
            .unwrap_or_else(|e| panic!("import of level {level}: {e}"));

        assert_eq!(stats.node_count, 200, "level {level}");
        assert_eq!(restored.all_nodes().len(), 200, "level {level}");

        // and the data itself survives, not just the counts
        let batch = engine
            .execute("MATCH (n:N {id: 7}) RETURN n.name AS name", &restored)
            .expect("query");
        assert_eq!(batch.records.len(), 1, "level {level}");
    }
}

#[test]
fn the_default_compression_level_is_the_documented_one() {
    // Guards against the default silently reverting to gzip's level 6, which is 2.25x
    // slower for 3% fewer bytes.
    assert_eq!(samyama::snapshot::DEFAULT_SNAPSHOT_COMPRESSION, 3);
}
