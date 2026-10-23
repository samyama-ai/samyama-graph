//! Every value type arrives through the Rust SDK as the engine sent it (API-04,
//! #1728).
//!
//! The cases are `sdk/fixtures/value_types.json`, shared with the Python and
//! TypeScript suites so all three SDKs are held to one table. Each value is
//! read four ways, because they took different code paths:
//!
//! - as a column of its own;
//! - inside a list and inside a map built in the query. Over HTTP these were
//!   rendered with `{:?}`, so `[x]` arrived as `["Property(Integer(42))"]`;
//! - as a node property, written by `CREATE` and read back.
//!
//! Each runs through the remote client (buffered and streamed), against the
//! engine's own HTTP router, and through the embedded client, and must decode
//! to exactly the fixture's JSON. `serde_json` keeps `1.0` and `1` apart, so a
//! float that turned into an integer fails here.

use std::sync::Arc;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::http::HttpServer;
use samyama_sdk::{EmbeddedClient, RemoteClient, SamyamaClient};
use serde_json::{json, Value};
use tokio::sync::RwLock;

const GRAPH: &str = "default";

/// The shared fixture, relative to this crate's manifest.
const FIXTURE: &str = "../../sdk/fixtures/value_types.json";

/// One fixture case: a Cypher expression and the JSON it must arrive as.
struct Case {
    name: String,
    cypher: String,
    expected: Value,
    storable: bool,
}

fn cases() -> Vec<Case> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let fixture: Value = serde_json::from_str(&text).expect("the fixture is JSON");
    fixture["cases"]
        .as_array()
        .expect("`cases` is a list")
        .iter()
        .map(|c| {
            let field = |k: &str| {
                c[k].as_str()
                    .unwrap_or_else(|| panic!("case {c}: `{k}` is a string"))
                    .to_string()
            };
            let json_text = field("json");
            Case {
                expected: serde_json::from_str(&json_text)
                    .unwrap_or_else(|e| panic!("case {c}: `json` does not parse: {e}")),
                name: field("name"),
                cypher: field("cypher"),
                storable: c["storable"].as_bool().expect("`storable` is a boolean"),
            }
        })
        .collect()
}

/// A read-only query for each way a value is returned, and what it must give.
fn read_shapes(case: &Case) -> [(&'static str, String, Value); 3] {
    let (c, e) = (&case.cypher, &case.expected);
    [
        ("column", format!("RETURN {c} AS v"), e.clone()),
        ("list", format!("WITH {c} AS x RETURN [x] AS v"), json!([e])),
        (
            "map",
            format!("WITH {c} AS x RETURN {{k: x}} AS v"),
            json!({ "k": e }),
        ),
    ]
}

/// The query that stores the value on a node and reads it back. Each case gets
/// its own label, so one case's node is never read for another.
fn property_shape(index: usize, case: &Case) -> String {
    format!(
        "CREATE (n:RoundTrip{index} {{v: {}}}) RETURN n.v AS v",
        case.cypher
    )
}

async fn served(store: GraphStore) -> RemoteClient {
    let app = HttpServer::new(Arc::new(RwLock::new(store)), 0).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    RemoteClient::new(&format!("http://{addr}"))
}

/// The single value a one-row, one-column result holds.
fn only_value(records: &[Vec<Value>], query: &str) -> Value {
    assert_eq!(records.len(), 1, "{query}: one row");
    assert_eq!(records[0].len(), 1, "{query}: one column");
    records[0][0].clone()
}

/// Compare, collecting every mismatch so one run reports all of them.
fn check(
    failures: &mut Vec<String>,
    path: &str,
    case: &Case,
    shape: &str,
    got: Value,
    want: &Value,
) {
    if &got != want {
        failures.push(format!(
            "{path}, {} as {shape}: got {got}, want {want}",
            case.name
        ));
    }
}

fn assert_none(failures: Vec<String>) {
    assert!(
        failures.is_empty(),
        "{} value(s) did not round-trip:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn every_value_type_round_trips_through_the_remote_client() {
    let client = served(GraphStore::new()).await;
    let mut failures = Vec::new();
    for (i, case) in cases().iter().enumerate() {
        for (shape, query, want) in read_shapes(case) {
            let result = client.query(GRAPH, &query).await.expect(&query);
            check(
                &mut failures,
                "remote",
                case,
                shape,
                only_value(&result.records, &query),
                &want,
            );
        }
        if case.storable {
            let query = property_shape(i, case);
            let result = client.query(GRAPH, &query).await.expect(&query);
            let got = only_value(&result.records, &query);
            check(
                &mut failures,
                "remote",
                case,
                "property",
                got,
                &case.expected,
            );
        }
    }
    assert_none(failures);
}

#[tokio::test]
async fn every_value_type_round_trips_through_the_streamed_remote_client() {
    let client = served(GraphStore::new()).await;
    let mut failures = Vec::new();
    for case in cases() {
        for (shape, query, want) in read_shapes(&case) {
            let mut stream = client.query_stream(GRAPH, &query).await.expect(&query);
            let mut rows = Vec::new();
            while let Some(row) = stream.next_row().await.expect(&query) {
                rows.push(row);
            }
            check(
                &mut failures,
                "stream",
                &case,
                shape,
                only_value(&rows, &query),
                &want,
            );
        }
    }
    assert_none(failures);
}

#[tokio::test]
async fn every_value_type_round_trips_through_the_embedded_client() {
    let client = EmbeddedClient::new();
    let mut failures = Vec::new();
    for (i, case) in cases().iter().enumerate() {
        for (shape, query, want) in read_shapes(case) {
            let result = client.query(GRAPH, &query).await.expect(&query);
            check(
                &mut failures,
                "embedded",
                case,
                shape,
                only_value(&result.records, &query),
                &want,
            );
        }
        if case.storable {
            let query = property_shape(i, case);
            let result = client.query(GRAPH, &query).await.expect(&query);
            let got = only_value(&result.records, &query);
            check(
                &mut failures,
                "embedded",
                case,
                "property",
                got,
                &case.expected,
            );
        }
    }
    assert_none(failures);
}

/// Vectors cannot be written from Cypher, so they are not in the shared
/// fixture; they reach a node through the store, as an embedding does.
const VECTOR_LABEL: &str = "Embedded";
const VECTOR_PROPERTY: &str = "embedding";

/// Exact in `f32`, so they arrive unchanged ...
const EXACT_VECTOR: [f32; 3] = [0.25, -1.5, 3.0];
/// ... and one that is not: `0.1` is stored as the nearest `f32`, and arrives
/// as that number written out in full, `0.10000000149011612`.
const INEXACT_COMPONENT: f32 = 0.1;

fn store_with_a_vector(components: &[f32]) -> GraphStore {
    let mut store = GraphStore::new();
    let id = store.create_node(VECTOR_LABEL);
    store
        .set_node_property(
            GRAPH,
            id,
            VECTOR_PROPERTY,
            PropertyValue::Vector(components.to_vec()),
        )
        .expect("set a vector property");
    store
}

fn vector_query() -> String {
    format!("MATCH (n:{VECTOR_LABEL}) RETURN n.{VECTOR_PROPERTY} AS v")
}

/// The vector as JSON numbers: each `f32` widened exactly to `f64`.
fn vector_json(components: &[f32]) -> Value {
    json!(components.iter().map(|&c| c as f64).collect::<Vec<_>>())
}

#[tokio::test]
async fn a_vector_arrives_as_a_list_of_its_f32_components() {
    let mut components = EXACT_VECTOR.to_vec();
    components.push(INEXACT_COMPONENT);
    let want = vector_json(&components);
    let query = vector_query();

    let remote = served(store_with_a_vector(&components)).await;
    let r = remote.query(GRAPH, &query).await.expect("remote");
    assert_eq!(only_value(&r.records, &query), want, "remote");

    let embedded =
        EmbeddedClient::with_store(Arc::new(RwLock::new(store_with_a_vector(&components))));
    let r = embedded.query(GRAPH, &query).await.expect("embedded");
    assert_eq!(only_value(&r.records, &query), want, "embedded");

    // The widening is exact, not a rounding: the f64 converts back to the f32.
    let last = want.as_array().unwrap().last().unwrap().as_f64().unwrap();
    assert_eq!(last as f32, INEXACT_COMPONENT);
}

/// A value from a snapshot written before the temporal types existed: Unix
/// milliseconds, with no type of its own (2024-01-02T03:04:05.678Z).
const LEGACY_MILLIS: i64 = 1_704_164_645_678;
const LEGACY_PROPERTY: &str = "at";

#[tokio::test]
async fn a_legacy_datetime_arrives_as_its_milliseconds() {
    let mut store = GraphStore::new();
    let id = store.create_node(VECTOR_LABEL);
    store
        .set_node_property(
            GRAPH,
            id,
            LEGACY_PROPERTY,
            PropertyValue::DateTime(LEGACY_MILLIS),
        )
        .expect("set a legacy datetime");
    let query = format!("MATCH (n:{VECTOR_LABEL}) RETURN n.{LEGACY_PROPERTY} AS v");
    let remote = served(store).await;
    let r = remote.query(GRAPH, &query).await.expect("remote");
    assert_eq!(only_value(&r.records, &query), json!(LEGACY_MILLIS));
}

#[test]
fn the_fixture_is_well_formed() {
    let cases = cases();
    assert!(!cases.is_empty());
    let mut names: Vec<&str> = cases.iter().map(|c| c.name.as_str()).collect();
    names.sort_unstable();
    let before = names.len();
    names.dedup();
    assert_eq!(names.len(), before, "case names are unique");
}
