//! `.sgsnap` v3: the Parquet container (#1099).
//!
//! What a v3 file has to do is what a v2 file already does, so most checks
//! here compare the two: the same graph exported both ways must import to the
//! same graph, the same catalog, the same dedup decisions. What only v3 can
//! do -- be read column by column by a plain Parquet reader -- is checked with
//! the `parquet` crate and nothing of Samyama's.
//!
//! No assertion here depends on time.

use std::collections::HashMap;
use std::io::Read;

use samyama::graph::property::PropertyValue;
use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use samyama::snapshot::content_hash::content_hash;
use samyama::snapshot::{self, WriteOptions};

const T: &str = "default";

fn run(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, T)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn v2(store: &GraphStore) -> Vec<u8> {
    let mut out = Vec::new();
    snapshot::export_tenant(store, &mut out).expect("v2 export");
    out
}

fn v3(store: &GraphStore) -> Vec<u8> {
    let mut out = Vec::new();
    let opts = WriteOptions {
        version: 3,
        ..Default::default()
    };
    snapshot::export_tenant_with_options(store, &mut out, &opts).expect("v3 export");
    out
}

fn import(bytes: &[u8]) -> GraphStore {
    let mut g = GraphStore::new();
    snapshot::import_tenant(&mut g, bytes).expect("import");
    g
}

/// Every `PropertyValue` variant, with values a lossy path cannot pass by
/// accident (sub-millisecond nanos, a half-hour offset, NaN and infinities).
fn every_variant() -> Vec<(&'static str, PropertyValue)> {
    let mut map = HashMap::new();
    map.insert("inner".to_string(), PropertyValue::Integer(7));
    map.insert("nested".to_string(), PropertyValue::String("deep".into()));
    vec![
        ("integer", PropertyValue::Integer(i64::MIN)),
        ("float", PropertyValue::Float(1.5)),
        ("nan", PropertyValue::Float(f64::NAN)),
        ("inf", PropertyValue::Float(f64::INFINITY)),
        ("ninf", PropertyValue::Float(f64::NEG_INFINITY)),
        (
            "string",
            PropertyValue::String("Ada \u{1F600} \"q\"".into()),
        ),
        ("boolean", PropertyValue::Boolean(false)),
        ("datetime", PropertyValue::DateTime(1_700_000_000_123)),
        ("date", PropertyValue::Date(-16_637)),
        ("localtime", PropertyValue::LocalTime(45_074_645_876_123)),
        (
            "array",
            PropertyValue::Array(vec![
                PropertyValue::Integer(1),
                PropertyValue::String("two".into()),
                PropertyValue::Null,
                PropertyValue::Array(vec![PropertyValue::Float(0.5)]),
            ]),
        ),
        ("empty_array", PropertyValue::Array(vec![])),
        ("map", PropertyValue::Map(map)),
        (
            "vector",
            PropertyValue::Vector(vec![0.25, -0.5, 1.0, f32::MAX]),
        ),
        ("empty_vector", PropertyValue::Vector(vec![])),
        (
            "duration",
            PropertyValue::Duration {
                months: 1,
                days: -2,
                seconds: 3,
                nanos: 4,
            },
        ),
        (
            "time",
            PropertyValue::Time {
                nanos: 45_074_645_876_123,
                offset_seconds: 19_800,
            },
        ),
        (
            "localdatetime",
            PropertyValue::LocalDateTime {
                secs: 1_437_514_832,
                nanos: 142_000_042,
            },
        ),
        (
            "zoneddatetime",
            PropertyValue::ZonedDateTime {
                secs: 1_437_514_832,
                nanos: 999_999_999,
                offset_seconds: 3600,
                zone: Some("Europe/London".to_string()),
            },
        ),
    ]
}

/// NaN is not equal to itself, so values are compared by their bits where
/// that matters.
fn same(a: &PropertyValue, b: &PropertyValue) -> bool {
    match (a, b) {
        (PropertyValue::Float(x), PropertyValue::Float(y)) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

/// A graph with every value type on nodes and edges, several labels (one node
/// with two), an unlabelled node, absent properties, a key whose type differs
/// between nodes of one label, edges with and without properties, and
/// bulk-loaded (stub) nodes and edges.
fn rich() -> GraphStore {
    let mut g = GraphStore::new();
    let a = g.create_node("Person");
    let b = g.create_node("Person");
    let c = g.create_node_with_labels(["Person".into(), "Admin".into()]);
    let d = g.create_node("Place");
    let bare = g.create_node_with_labels(Vec::<samyama::graph::Label>::new());
    for (k, v) in every_variant() {
        g.set_node_property(T, a, k.to_string(), v).unwrap();
    }
    // `b` has only some keys, and `mixed` as a string where `a` and `c` have an
    // integer. `c` is also an Admin, so it is written to the Admin file; `a`
    // is what makes the key's type differ within the Person file.
    g.set_node_property(T, a, "mixed".to_string(), PropertyValue::Integer(7))
        .unwrap();
    g.set_node_property(T, b, "integer".to_string(), PropertyValue::Integer(3))
        .unwrap();
    g.set_node_property(T, b, "mixed".to_string(), PropertyValue::String("x".into()))
        .unwrap();
    g.set_node_property(T, c, "mixed".to_string(), PropertyValue::Integer(9))
        .unwrap();
    // A property named like a system column.
    g.set_node_property(
        T,
        d,
        "_id".to_string(),
        PropertyValue::String("not the id".into()),
    )
    .unwrap();
    g.set_node_property(
        T,
        bare,
        "name".to_string(),
        PropertyValue::String("nobody".into()),
    )
    .unwrap();

    let e1 = g.create_edge(a, b, "KNOWS").unwrap();
    for (k, v) in every_variant() {
        g.set_edge_property(e1, k.to_string(), v).unwrap();
    }
    let e2 = g.create_edge(c, d, "LIVES_IN").unwrap();
    g.set_edge_property(e2, "since", PropertyValue::Integer(2001))
        .unwrap();
    g.create_edge(b, a, "KNOWS").unwrap();

    // Bulk-loaded: stub nodes with column properties, stub edges of one type.
    let s1 = g.create_node_stub("Paper");
    let s2 = g.create_node_stub("Paper");
    g.set_column_property(s1, "title", PropertyValue::String("On Graphs".into()));
    g.set_column_property(s2, "year", PropertyValue::Integer(1999));
    g.create_edge_stub(s1, s2, "CITES").unwrap();
    g.create_edge_stub(s2, a, "CITES").unwrap();
    g
}

#[test]
fn default_export_is_still_v2_gzip() {
    let g = rich();
    let bytes = v2(&g);
    assert_eq!(&bytes[..2], &[0x1f, 0x8b], "the default export is gzip");
    assert_eq!(snapshot::peek_header(bytes.as_slice()).unwrap().version, 2);

    let mut via_options = Vec::new();
    snapshot::export_tenant_with_options(&g, &mut via_options, &WriteOptions::default()).unwrap();
    assert_eq!(&via_options[..2], &[0x1f, 0x8b]);
    assert_eq!(WriteOptions::default().version, 2);
}

#[test]
fn a_v3_file_is_a_tar_with_the_manifest_first() {
    let bytes = v3(&rich());
    assert_eq!(&bytes[257..262], b"ustar");
    assert_eq!(&bytes[..13], b"manifest.json");
    let header = snapshot::peek_header(bytes.as_slice()).expect("peek v3");
    assert_eq!(header.version, 3);
    assert_eq!(header.node_count, 7);
    assert_eq!(header.edge_count, 5);
    assert_eq!(header.labels, vec!["Admin", "Paper", "Person", "Place"]);
    assert_eq!(header.edge_types, vec!["CITES", "KNOWS", "LIVES_IN"]);
}

#[test]
fn every_property_type_round_trips_on_nodes_and_edges() {
    let g = rich();
    let back = import(&v3(&g));
    assert_eq!(back.node_count(), g.node_count());

    let a = back.all_nodes()[0].id;
    let props = back.node_properties_merged(a);
    for (k, v) in every_variant() {
        let got = props
            .get(k)
            .unwrap_or_else(|| panic!("node property {k} lost"));
        assert!(same(got, &v), "node property {k}: {got:?} != {v:?}");
    }

    let knows = back
        .all_edges()
        .into_iter()
        .find(|e| e.edge_type.as_str() == "KNOWS" && !e.properties.is_empty())
        .expect("the KNOWS edge with properties");
    for (k, v) in every_variant() {
        let got = knows
            .properties
            .get(k)
            .unwrap_or_else(|| panic!("edge property {k} lost"));
        assert!(same(got, &v), "edge property {k}: {got:?} != {v:?}");
    }
}

#[test]
fn v3_imports_to_the_same_graph_as_v2() {
    // Ids, labels, properties (merged view), timestamps not included, and every
    // edge with its id, endpoints, type and properties: content_hash covers
    // them all. NaN hashes by its bits, so the rich graph is fine here.
    let g = rich();
    let from_v2 = import(&v2(&g));
    let from_v3 = import(&v3(&g));
    assert_eq!(content_hash(&from_v3), content_hash(&from_v2));
}

#[test]
fn node_timestamps_survive() {
    let g = rich();
    let back = import(&v3(&g));
    let from_v2 = import(&v2(&g));
    let stamps = |s: &GraphStore| -> Vec<(i64, i64)> {
        s.all_nodes()
            .iter()
            .map(|n| (n.created_at, n.updated_at))
            .collect()
    };
    assert_eq!(stamps(&back), stamps(&from_v2));
}

#[test]
fn stub_edges_of_several_types_survive() {
    // Stub edges are written per type, so their import order -- and therefore
    // the ids a fresh import gives them -- can differ from a v2 import when
    // types interleave. The relationships themselves must not.
    let mut g = GraphStore::new();
    let n: Vec<_> = (0..6).map(|_| g.create_node_stub("N")).collect();
    for i in 0..5 {
        let t = if i % 2 == 0 { "EVEN" } else { "ODD" };
        g.create_edge_stub(n[i], n[i + 1], t).unwrap();
    }
    let rels = |s: &GraphStore| {
        let mut out = Vec::new();
        for node in s.all_nodes() {
            for &(tgt, eid) in s.get_outgoing_neighbor_slice(node.id) {
                out.push((
                    node.id.as_u64(),
                    tgt.as_u64(),
                    s.get_edge_type(eid).unwrap().as_str().to_string(),
                ));
            }
            for (tgt, eid) in s.frozen_outgoing_neighbors(node.id.as_u64() as usize) {
                out.push((
                    node.id.as_u64(),
                    tgt.as_u64(),
                    s.get_edge_type(eid).unwrap().as_str().to_string(),
                ));
            }
        }
        out.sort();
        out
    };
    let back = import(&v3(&g));
    assert_eq!(rels(&back), rels(&g));
    assert_eq!(rels(&back), rels(&import(&v2(&g))));
}

#[test]
fn the_index_catalog_survives() {
    let mut g = GraphStore::new();
    run(
        &mut g,
        "CREATE (:P {id: 1, email: 'a@x', body: 'quick brown fox', emb: [0.1, 0.2, 0.3, 0.4]}) \
         CREATE (:P {id: 2, email: 'b@x', body: 'lazy dog sleeps', emb: [0.9, 0.8, 0.7, 0.6]})",
    );
    run(&mut g, "CREATE INDEX ON :P(id)");
    run(
        &mut g,
        "CREATE CONSTRAINT ON (n:P) ASSERT n.email IS UNIQUE",
    );
    run(&mut g, "CREATE FULLTEXT INDEX ftidx FOR (d:P) ON (d.body)");
    run(
        &mut g,
        "CREATE VECTOR INDEX vidx FOR (n:P) ON (n.emb) OPTIONS {dimensions: 4, similarity: 'l2'}",
    );
    let want = g.index_catalog().definitions;
    assert!(
        want.len() >= 4,
        "the source declares four indexes: {want:?}"
    );

    let back = import(&v3(&g));
    assert_eq!(back.index_catalog().definitions, want);
    assert_eq!(
        back.index_catalog().definitions,
        import(&v2(&g)).index_catalog().definitions
    );
}

fn people(names: &[(&str, i64)]) -> GraphStore {
    let mut g = GraphStore::new();
    for (name, age) in names {
        let id = g.create_node("Person");
        g.set_node_property(
            T,
            id,
            "name".to_string(),
            PropertyValue::String(name.to_string()),
        )
        .unwrap();
        g.set_node_property(T, id, "age".to_string(), PropertyValue::Integer(*age))
            .unwrap();
    }
    g
}

#[test]
fn dedup_import_against_v3_matches_v2() {
    let base = people(&[("Ada", 36), ("Bob", 40)]);
    let incoming = people(&[("Ada", 36), ("Cy", 22)]);

    let merge = |bytes: Vec<u8>| {
        let mut target = import(&v2(&base));
        let stats =
            snapshot::import_tenant_with_dedup(&mut target, bytes.as_slice(), &["Person.name"])
                .expect("dedup import");
        (
            stats.node_count,
            stats.merged_count,
            target.node_count(),
            content_hash(&target),
        )
    };
    let via_v3 = merge(v3(&incoming));
    let via_v2 = merge(v2(&incoming));
    assert_eq!(via_v3, via_v2);
    assert_eq!(via_v3.1, 1, "Ada merges");
    assert_eq!(via_v3.2, 3, "Ada, Bob, Cy");
}

#[test]
fn v1_and_v2_files_still_import() {
    // A v1 file: a hand-written header and lines, as an old build wrote them.
    let v1_text = concat!(
        r#"{"format":"sgsnap","version":1,"tenant":"default","node_count":2,"edge_count":1,"#,
        r#""labels":["A"],"edge_types":["R"],"created_at":"2024-01-01T00:00:00Z","samyama_version":"0.5.0"}"#,
        "\n",
        r#"{"t":"n","id":0,"labels":["A"],"props":{"x":1}}"#,
        "\n",
        r#"{"t":"n","id":1,"labels":["A"],"props":{}}"#,
        "\n",
        r#"{"t":"e","id":0,"src":0,"tgt":1,"type":"R","props":{}}"#,
        "\n",
    );
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gz, v1_text.as_bytes()).unwrap();
    let v1 = gz.finish().unwrap();
    let g = import(&v1);
    assert_eq!(g.node_count(), 2);
    assert_eq!(g.all_edges().len() + count_stub_edges(&g), 1);

    let rich = rich();
    let from_v2 = import(&v2(&rich));
    assert_eq!(from_v2.node_count(), rich.node_count());
}

fn count_stub_edges(g: &GraphStore) -> usize {
    let full: std::collections::HashSet<u64> =
        g.all_edges().iter().map(|e| e.id.as_u64()).collect();
    g.all_nodes()
        .iter()
        .flat_map(|n| g.get_outgoing_neighbor_slice(n.id).to_vec())
        .filter(|(_, e)| !full.contains(&e.as_u64()))
        .count()
}

/// The members of a tar archive, by name. Written against the format, not
/// against Samyama's reader: this is what `tar xf` would give.
fn untar(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 512 <= bytes.len() {
        let h = &bytes[at..at + 512];
        if h.iter().all(|&b| b == 0) {
            break;
        }
        let name = String::from_utf8(h[..100].split(|&b| b == 0).next().unwrap().to_vec()).unwrap();
        let size_text = std::str::from_utf8(&h[124..135]).unwrap();
        let size = usize::from_str_radix(size_text.trim_matches(char::from(0)).trim(), 8).unwrap();
        at += 512;
        out.push((name, bytes[at..at + size].to_vec()));
        at += size.div_ceil(512) * 512;
    }
    out
}

#[test]
fn a_plain_parquet_reader_reads_the_columns() {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float32Type, Int64Type, UInt64Type};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let g = rich();
    let bytes = v3(&g);
    let members = untar(&bytes);
    assert_eq!(members[0].0, "manifest.json");
    let manifest: serde_json::Value = serde_json::from_slice(&members[0].1).unwrap();
    assert_eq!(manifest["layout"], "samyama.sgsnap.v3/parquet");

    let person = members
        .iter()
        .find(|(name, _)| name.starts_with("nodes/") && name.ends_with("-Person.parquet"))
        .expect("a Person file");
    // `Admin` sorts before `Person`, so the node with both is in the Admin file.
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(person.1.clone()))
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(batch.num_rows(), 2);

    let ids = batch
        .column_by_name("_id")
        .unwrap()
        .as_primitive::<UInt64Type>();
    // The ids the store gave the two Person-only nodes, whatever the
    // allocator starts at.
    let mut want: Vec<u64> = g
        .all_nodes()
        .iter()
        .filter(|n| n.labels.len() == 1 && n.labels.iter().all(|l| l.as_str() == "Person"))
        .map(|n| n.id.as_u64())
        .collect();
    want.sort();
    assert_eq!(ids.values().to_vec(), want);

    let integer = batch
        .column_by_name("integer")
        .unwrap()
        .as_primitive::<Int64Type>();
    assert_eq!(integer.value(0), i64::MIN);
    assert_eq!(integer.value(1), 3);

    let vector = batch.column_by_name("vector").unwrap().as_list::<i32>();
    let first = vector.value(0);
    assert_eq!(
        first.as_primitive::<Float32Type>().values().to_vec(),
        vec![0.25, -0.5, 1.0, f32::MAX]
    );
    assert!(vector.is_null(1), "an absent property is a Parquet null");

    // A key whose values differ in type is carried as codec JSON text.
    let mixed_field = batch.schema().field_with_name("mixed").unwrap().clone();
    assert_eq!(
        mixed_field
            .metadata()
            .get("samyama.type")
            .map(String::as_str),
        Some("json")
    );

    let edges = members
        .iter()
        .find(|(name, _)| name.starts_with("edges/") && name.ends_with("-KNOWS.parquet"))
        .expect("a KNOWS file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(edges.1.clone()))
        .unwrap()
        .build()
        .unwrap();
    let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 2);
}

#[test]
fn a_truncated_v3_file_is_refused_and_leaves_nothing() {
    let bytes = v3(&rich());
    for cut in [600, bytes.len() / 2, bytes.len() - 1024, bytes.len() - 1] {
        let mut g = GraphStore::new();
        let err = snapshot::import_tenant(&mut g, &bytes[..cut]);
        assert!(
            err.is_err(),
            "a file cut at {cut} of {} imported",
            bytes.len()
        );
        assert_eq!(
            g.node_count(),
            0,
            "a failed import at {cut} left nodes behind"
        );
    }
}

#[test]
fn a_damaged_member_is_refused() {
    let mut bytes = v3(&rich());
    // Flip a byte inside the first data member's header block.
    let manifest_size = {
        let text = std::str::from_utf8(&bytes[124..135]).unwrap();
        usize::from_str_radix(text, 8).unwrap()
    };
    let second = 512 + manifest_size.div_ceil(512) * 512;
    bytes[second + 3] ^= 0x55;
    let mut g = GraphStore::new();
    assert!(snapshot::import_tenant(&mut g, bytes.as_slice()).is_err());
    assert_eq!(g.node_count(), 0);
}

#[test]
fn relinking_a_v3_header_keeps_the_graph() {
    let g = rich();
    let bytes = v3(&g);
    let mut relinked = Vec::new();
    let header = snapshot::edit_header(bytes.as_slice(), &mut relinked, |obj| {
        obj.insert("read_only".to_string(), serde_json::Value::Bool(true));
        Ok(())
    })
    .expect("edit a v3 header");
    assert!(header.read_only);
    assert_eq!(header.version, 3);
    assert!(
        snapshot::peek_header(relinked.as_slice())
            .unwrap()
            .read_only
    );
    assert_eq!(
        content_hash(&import(&relinked)),
        content_hash(&import(&v2(&g)))
    );

    // A truncated source fails instead of becoming a shorter archive.
    let mut out = Vec::new();
    assert!(snapshot::edit_header(&bytes[..bytes.len() - 600], &mut out, |_| Ok(())).is_err());
}

#[test]
fn an_encrypted_v3_file_round_trips() {
    let g = rich();
    let key = [7u8; snapshot::encryption::KEY_BYTES];
    let mut sealed = Vec::new();
    let opts = WriteOptions {
        version: 3,
        ..Default::default()
    };
    snapshot::export_tenant_encrypted_with_options(&g, &mut sealed, &key, &opts).unwrap();
    assert!(snapshot::encryption::looks_encrypted(&sealed[..12]));

    let header = snapshot::peek_header_maybe_encrypted(sealed.as_slice(), Some(&key)).unwrap();
    assert_eq!(header.version, 3);

    let mut back = GraphStore::new();
    snapshot::import_tenant_maybe_encrypted(&mut back, sealed.as_slice(), Some(&key)).unwrap();
    assert_eq!(content_hash(&back), content_hash(&import(&v2(&g))));

    let mut nokey = GraphStore::new();
    let err = snapshot::import_tenant_maybe_encrypted(&mut nokey, sealed.as_slice(), None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("encrypted"), "{err}");
}

#[test]
fn an_unknown_version_is_refused_on_export() {
    let mut out = Vec::new();
    let opts = WriteOptions {
        version: 4,
        ..Default::default()
    };
    let err = snapshot::export_tenant_with_options(&rich(), &mut out, &opts).unwrap_err();
    assert!(
        err.to_string().contains("unsupported snapshot version 4"),
        "{err}"
    );
}

#[test]
fn write_file_and_convert_write_v3_on_request() {
    let dir = tempfile::tempdir().unwrap();
    let g = rich();
    let path = dir.path().join("g.sgsnap");
    snapshot::write_file(
        &g,
        &path,
        &WriteOptions {
            version: 3,
            ..Default::default()
        },
    )
    .unwrap();
    let mut head = [0u8; 512];
    std::fs::File::open(&path)
        .unwrap()
        .read_exact(&mut head)
        .unwrap();
    assert!(snapshot::v3::looks_like_v3(&head));
    let back = import(&std::fs::read(&path).unwrap());
    assert_eq!(content_hash(&back), content_hash(&import(&v2(&g))));

    // `samyama convert --sgsnap-version 3`: v2 in, v3 out, same graph.
    let input = dir.path().join("in.sgsnap");
    snapshot::write_file(&g, &input, &WriteOptions::default()).unwrap();
    let output = dir.path().join("out.sgsnap");
    let opts = samyama::convert::Options {
        sgsnap_version: Some(3),
        ..Default::default()
    };
    samyama::convert::convert(&input, &output, &opts).unwrap();
    let out_bytes = std::fs::read(&output).unwrap();
    assert!(snapshot::v3::looks_like_v3(&out_bytes));
    assert_eq!(
        content_hash(&import(&out_bytes)),
        content_hash(&import(&v2(&g)))
    );
}

#[test]
fn the_key_scan_and_schema_read_v3() {
    let dir = tempfile::tempdir().unwrap();
    let g = people(&[("Ada", 36), ("Bob", 40)]);
    let p2 = dir.path().join("g2.sgsnap");
    let p3 = dir.path().join("g3.sgsnap");
    snapshot::write_file(&g, &p2, &WriteOptions::default()).unwrap();
    snapshot::write_file(
        &g,
        &p3,
        &WriteOptions {
            version: 3,
            ..Default::default()
        },
    )
    .unwrap();

    let s2 = samyama::schema_doc::derive_from_path(&p2).unwrap();
    let s3 = samyama::schema_doc::derive_from_path(&p3).unwrap();
    assert_eq!(format!("{s2:?}"), format!("{s3:?}"));

    let r2 = samyama::pii::scan_snapshot_path(&p2).unwrap();
    let r3 = samyama::pii::scan_snapshot_path(&p3).unwrap();
    assert_eq!(format!("{r2:?}"), format!("{r3:?}"));
}

#[test]
fn an_empty_graph_round_trips() {
    let g = GraphStore::new();
    let bytes = v3(&g);
    let back = import(&bytes);
    assert_eq!(back.node_count(), 0);
    assert_eq!(
        snapshot::peek_header(bytes.as_slice()).unwrap().node_count,
        0
    );
}

#[test]
fn many_rows_cross_batch_boundaries() {
    // More rows than one write batch and one read batch (8K each), across two
    // labels whose ids interleave, so the reader's merge by id is exercised.
    let mut g = GraphStore::new();
    for i in 0..20_000i64 {
        let label = if i % 3 == 0 { "A" } else { "B" };
        let id = g.create_node_stub(label);
        g.set_column_property(id, "i", PropertyValue::Integer(i));
    }
    let back = import(&v3(&g));
    assert_eq!(content_hash(&back), content_hash(&import(&v2(&g))));
}
