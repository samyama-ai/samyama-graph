//! `EmbeddedClient::export` and `RemoteClient::export` (ML-01, #2267): a read
//! query's result as Arrow IPC or Parquet, in-process or over HTTP.
//!
//! The remote side runs against the engine's own HTTP router, sharing one store
//! with an embedded client, so "the two modes agree" is checked on the same
//! graph rather than on two graphs built the same way.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, ListArray, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use samyama_sdk::{EmbeddedClient, ExportFormat, RemoteClient, SamyamaClient};
use tokio::sync::RwLock;

/// An embedded client and a remote one, both on the same store.
async fn both() -> (EmbeddedClient, RemoteClient) {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store.clone(), 0).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (
        EmbeddedClient::with_store(store),
        RemoteClient::new(&format!("http://{addr}")),
    )
}

async fn people(client: &EmbeddedClient) {
    client
        .query(
            "default",
            "CREATE (:P {name: 'Ada', age: 36, score: 9.5, active: true, tags: ['a', 'b']}), \
                    (:P {name: 'Bo', age: 41, score: 7.25, active: false, tags: []}), \
                    (:P {name: 'Cy', age: 29, active: true, tags: ['c']})",
        )
        .await
        .expect("create");
}

const PEOPLE: &str = "MATCH (p:P) RETURN p.name AS name, p.age AS age, p.score AS score, \
                      p.active AS active, p.tags AS tags ORDER BY name";

fn read_ipc(bytes: &[u8]) -> RecordBatch {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .expect("an Arrow IPC stream");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.expect("a batch")).collect();
    arrow::compute::concat_batches(&schema, &batches).expect("one table")
}

fn read_parquet(bytes: Vec<u8>) -> RecordBatch {
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        bytes::Bytes::from(bytes),
    )
    .expect("a Parquet file");
    // The file's schema, not the first batch's: an empty result has no batch.
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder
        .build()
        .expect("a reader")
        .map(|b| b.expect("a batch"))
        .collect();
    arrow::compute::concat_batches(&schema, &batches).expect("one table")
}

fn col<'a, T: 'static>(t: &'a RecordBatch, name: &str) -> &'a T {
    t.column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} is not the type expected"))
}

/// The table the PEOPLE query must come back as, whichever mode and format.
fn assert_people(t: &RecordBatch) {
    let names: Vec<String> = t.schema().fields().iter().map(|f| f.name().clone()).collect();
    assert_eq!(names, ["name", "age", "score", "active", "tags"]);
    assert_eq!(t.num_rows(), 3);

    let name = col::<StringArray>(t, "name");
    assert_eq!(
        (0..3).map(|i| name.value(i)).collect::<Vec<_>>(),
        ["Ada", "Bo", "Cy"]
    );
    // Typed columns, not text: an integer column is Int64, a float column
    // Float64, a boolean column Boolean.
    let age = col::<Int64Array>(t, "age");
    assert_eq!((0..3).map(|i| age.value(i)).collect::<Vec<_>>(), [36, 41, 29]);
    let score = col::<Float64Array>(t, "score");
    assert_eq!(score.value(0), 9.5);
    assert_eq!(score.value(1), 7.25);
    assert!(score.is_null(2), "a missing property is a null, not 0.0");
    let active = col::<BooleanArray>(t, "active");
    assert_eq!(
        (0..3).map(|i| active.value(i)).collect::<Vec<_>>(),
        [true, false, true]
    );
    let tags = col::<ListArray>(t, "tags");
    assert_eq!(
        tags.value_type(),
        DataType::Utf8,
        "a list of strings is a list column"
    );
    assert_eq!(tags.value_length(0), 2);
    assert_eq!(tags.value_length(1), 0);
    assert_eq!(tags.value_length(2), 1);
}

#[tokio::test]
async fn embedded_export_is_a_typed_arrow_table() {
    let (embedded, _) = both().await;
    people(&embedded).await;
    let ipc = embedded
        .export("default", PEOPLE, ExportFormat::Arrow)
        .await
        .expect("export");
    assert_people(&read_ipc(&ipc));
}

#[tokio::test]
async fn remote_export_is_the_same_table() {
    let (embedded, remote) = both().await;
    people(&embedded).await;
    let ipc = remote
        .export("default", PEOPLE, ExportFormat::Arrow)
        .await
        .expect("export");
    assert_people(&read_ipc(&ipc));
}

#[tokio::test]
async fn the_two_modes_agree_in_both_formats() {
    let (embedded, remote) = both().await;
    people(&embedded).await;
    for q in [
        PEOPLE,
        "MATCH (p:P) RETURN p ORDER BY p.name",
        "MATCH (p:P) RETURN count(p) AS n, avg(p.age) AS mean_age",
        "MATCH (p:P) WHERE p.age > 100 RETURN p.name AS name",
    ] {
        let local = read_ipc(&embedded.export("default", q, ExportFormat::Arrow).await.expect(q));
        let wire = read_ipc(&remote.export("default", q, ExportFormat::Arrow).await.expect(q));
        assert_eq!(local, wire, "arrow differs between modes for {q}");

        let local =
            read_parquet(embedded.export("default", q, ExportFormat::Parquet).await.expect(q));
        let wire =
            read_parquet(remote.export("default", q, ExportFormat::Parquet).await.expect(q));
        assert_eq!(local, wire, "parquet differs between modes for {q}");
    }
}

#[tokio::test]
async fn parquet_carries_the_same_table_as_arrow() {
    let (embedded, _) = both().await;
    people(&embedded).await;
    let parquet = embedded
        .export("default", PEOPLE, ExportFormat::Parquet)
        .await
        .expect("export");
    assert_eq!(&parquet[..4], b"PAR1", "a Parquet file starts with its magic");
    assert_people(&read_parquet(parquet));
}

#[tokio::test]
async fn an_empty_result_keeps_its_columns() {
    let (embedded, remote) = both().await;
    people(&embedded).await;
    let q = "MATCH (p:P) WHERE p.age > 100 RETURN p.name AS name, p.age AS age";
    for bytes in [
        embedded.export("default", q, ExportFormat::Arrow).await.unwrap(),
        remote.export("default", q, ExportFormat::Arrow).await.unwrap(),
    ] {
        let t = read_ipc(&bytes);
        assert_eq!(t.num_rows(), 0);
        let names: Vec<String> = t.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, ["name", "age"]);
    }
}

#[tokio::test]
async fn a_node_is_exported_with_its_properties() {
    let (embedded, _) = both().await;
    people(&embedded).await;
    let t = read_ipc(
        &embedded
            .export("default", "MATCH (p:P {name: 'Ada'}) RETURN p", ExportFormat::Arrow)
            .await
            .unwrap(),
    );
    // Resolved to the store's view, as the endpoint does, so the node carries
    // its properties rather than a bare id.
    let p = col::<StringArray>(&t, "p").value(0);
    assert!(p.contains("Ada") && p.contains("36"), "{p}");

    // A node inside a list is left by the executor as a bare reference, which
    // renders as `{"id": n}` unless it is resolved against the store -- the
    // case `resolve_nodes` exists for. Both modes must carry its properties.
    let (embedded, remote) = both().await;
    people(&embedded).await;
    let q = "MATCH (p:P {name: 'Ada'}) RETURN collect(p) AS ps";
    for (mode, bytes) in [
        ("embedded", embedded.export("default", q, ExportFormat::Arrow).await.unwrap()),
        ("remote", remote.export("default", q, ExportFormat::Arrow).await.unwrap()),
    ] {
        let t = read_ipc(&bytes);
        let ps = col::<StringArray>(&t, "ps").value(0);
        assert!(ps.contains("Ada") && ps.contains("36"), "{mode}: {ps}");
    }
}

#[tokio::test]
async fn a_write_is_refused_in_both_modes_and_nothing_changes() {
    let (embedded, remote) = both().await;
    people(&embedded).await;
    for q in [
        "CREATE (:P {name: 'Zed'})",
        "MATCH (p:P) SET p.age = 0 RETURN p.age",
        "MATCH (p:P) DETACH DELETE p",
    ] {
        let err = embedded
            .export("default", q, ExportFormat::Arrow)
            .await
            .expect_err(q)
            .to_string();
        assert!(err.contains("read-only") && err.contains("`query`"), "{q}: {err}");
        remote
            .export("default", q, ExportFormat::Arrow)
            .await
            .expect_err(q);
    }
    let n = embedded
        .query_readonly("default", "MATCH (p:P) WHERE p.age > 0 RETURN count(p)")
        .await
        .unwrap();
    assert_eq!(n.records[0][0].as_u64(), Some(3), "a refused write ran");
}

#[tokio::test]
async fn a_foreign_graph_and_a_bad_query_are_refused_with_the_reason() {
    let (embedded, remote) = both().await;
    let err = embedded
        .export("other", "RETURN 1 AS x", ExportFormat::Arrow)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("single graph") && err.contains("other"), "{err}");
    let err = remote
        .export("other", "RETURN 1 AS x", ExportFormat::Arrow)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("400") && err.contains("other"), "{err}");

    assert!(embedded
        .export("default", "MATCH (n RETURN n", ExportFormat::Arrow)
        .await
        .is_err());
    let err = remote
        .export("default", "MATCH (n RETURN n", ExportFormat::Arrow)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("/api/query/export"), "the message names the endpoint: {err}");
}

#[test]
fn format_names_parse_and_print() {
    for name in ExportFormat::NAMES {
        assert_eq!(ExportFormat::parse(name).unwrap().as_str(), name);
    }
    assert_eq!(ExportFormat::parse("ARROW"), Some(ExportFormat::Arrow));
    assert_eq!(ExportFormat::parse("csv"), None);
    assert_eq!(ExportFormat::parse(""), None);
}
