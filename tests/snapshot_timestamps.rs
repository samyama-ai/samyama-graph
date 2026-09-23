//! A snapshot round trip preserves node timestamps (#1124).
//!
//! `export_tenant` → `import_tenant` used to drop `created_at` and `updated_at`,
//! so every imported node arrived stamped 0. Nothing in Cypher reads them today,
//! which is why it sat unnoticed — and why it is worth a test rather than a
//! comment: a field that silently arrives as 0 is discovered by whoever first
//! depends on it.
//!
//! The compatibility half matters as much as the fix. The fields are additive
//! and default to 0, so a snapshot written before them must still import, and
//! must not have 0 written over the timestamps the fresh nodes were given.

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::snapshot::{export_tenant, import_tenant};

fn seeded() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..5i64 {
        let mut props = PropertyMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i));
        props.insert("name".to_string(), PropertyValue::String(format!("n{i}")));
        store.create_node_with_properties("default", vec![Label::new("Row")], props);
    }
    store
}

fn timestamps(store: &GraphStore) -> Vec<(i64, i64)> {
    let mut v: Vec<(i64, i64)> = store
        .get_nodes_by_label(&Label::new("Row"))
        .iter()
        .map(|n| (n.created_at, n.updated_at))
        .collect();
    v.sort_unstable();
    v
}

#[test]
fn a_round_trip_preserves_node_timestamps() {
    let store = seeded();
    let before = timestamps(&store);
    assert!(
        before.iter().all(|(c, u)| *c > 0 && *u > 0),
        "the fixture must have real timestamps or this proves nothing: {before:?}"
    );

    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");

    let mut restored = GraphStore::new();
    import_tenant(&mut restored, &buf[..]).expect("import");

    assert_eq!(
        timestamps(&restored),
        before,
        "the round trip changed the node timestamps"
    );
}

/// A snapshot written before the fields must still import.
///
/// Its nodes arrive with timestamps of 0, and that is correct rather than a
/// regression: the file does not carry them, and `create_node_stub` deliberately
/// leaves them at 0 to skip a clock syscall per node on import. What the guard in
/// `import_tenant` prevents is the opposite mistake — writing the snapshot's 0
/// over a value the node already had.
#[test]
fn a_snapshot_without_the_fields_still_imports() {
    use std::io::{Read, Write};

    let store = seeded();
    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");

    let raw = {
        let mut gz = flate2::read::GzDecoder::new(&buf[..]);
        let mut s = String::new();
        gz.read_to_string(&mut s).expect("read");
        s
    };
    assert!(
        raw.lines().any(|l| l.contains("\"t\":\"n\"") && l.contains("created_at")),
        "the fixture snapshot must carry the field on its node lines"
    );

    // Strip both keys from the node lines only. The header carries its own
    // required `created_at` (an ISO 8601 string), so stripping every line breaks
    // the header rather than simulating an older writer.
    let stripped: String = raw
        .lines()
        .map(|l| {
            let mut l = l.to_string();
            if !l.contains("\"t\":\"n\"") {
                return l;
            }
            for key in ["created_at", "updated_at"] {
                let pat = format!(",\"{key}\":");
                while let Some(p) = l.find(&pat) {
                    let tail = &l[p + 1..];
                    let end = tail
                        .find([',', '}'])
                        .map(|c| p + 1 + c)
                        .unwrap_or(l.len());
                    l.replace_range(p..end, "");
                }
            }
            l
        })
        .collect::<Vec<_>>()
        .join("\n");
    // Node lines only: the header keeps its own `created_at`, which is a
    // different, required field.
    assert!(
        !stripped
            .lines()
            .any(|l| l.contains("\"t\":\"n\"") && l.contains("created_at")),
        "strip failed on a node line"
    );

    let old_buf = {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(stripped.as_bytes()).expect("write");
        gz.finish().expect("finish")
    };

    let mut restored = GraphStore::new();
    import_tenant(&mut restored, &old_buf[..]).expect("an older snapshot must import");

    let after = timestamps(&restored);
    assert_eq!(after.len(), 5, "not every node imported: {after:?}");
    assert!(
        after.iter().all(|(c, u)| *c == 0 && *u == 0),
        "a snapshot that does not carry timestamps produced non-zero ones, so \
         they came from somewhere other than the file: {after:?}"
    );
}
