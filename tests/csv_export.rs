//! `POST /api/query/export` with `format: "csv"` (INT-08).
//!
//! CSV is the format Tableau, Power BI and Superset read from a URL without a
//! connector. The tests check the bytes on the wire, not the writer: a CSV
//! that a reader splits into the wrong number of fields is the failure that
//! matters, and only a parse sees it.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use samyama::export::csv::{to_csv, CsvReport};
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;
use tower::ServiceExt;

/// A minimal RFC 4180 reader, so the output is checked against the format
/// rather than against the code that wrote it.
fn parse_csv(text: &str) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match (in_quotes, c) {
            (true, '"') => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            }
            (true, c) => field.push(c),
            (false, '"') => in_quotes = true,
            (false, ',') => row.push(std::mem::take(&mut field)),
            (false, '\r') => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            (false, '\n') => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            (false, c) => field.push(c),
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows
}

fn batch_of(query: &str, store: &GraphStore) -> samyama::query::RecordBatch {
    QueryEngine::new().execute(query, store).unwrap()
}

fn people() -> GraphStore {
    let mut g = GraphStore::new();
    let a = g.create_node("Person");
    let b = g.create_node("Person");
    g.set_node_property("default", a, "name", "Alice, the first")
        .unwrap();
    g.set_node_property("default", a, "age", 34i64).unwrap();
    g.set_node_property("default", b, "name", "Bob \"Bobby\"")
        .unwrap();
    g
}

#[test]
fn a_comma_and_a_quote_in_a_value_do_not_shift_the_columns() {
    // The whole point of the format. Without quoting, "Alice, the first"
    // becomes two fields and every column after it is wrong — silently, and
    // only in the rows that happen to contain a comma.
    let store = people();
    let (text, report) = to_csv(&batch_of(
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY name",
        &store,
    ));

    let rows = parse_csv(&text);
    assert_eq!(rows[0], vec!["name", "age"]);
    assert_eq!(rows.len(), 3, "header plus two rows: {rows:?}");
    for r in &rows {
        assert_eq!(r.len(), 2, "every row has exactly two fields: {r:?}");
    }
    let names: Vec<&String> = rows[1..].iter().map(|r| &r[0]).collect();
    assert!(names.iter().any(|n| *n == "Alice, the first"));
    assert!(names.iter().any(|n| *n == "Bob \"Bobby\""));
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.columns, 2);
}

#[test]
fn a_null_is_an_empty_field_and_is_counted() {
    // CSV has no null. Bob has no `age`, so the field is empty — and an empty
    // field is also how an empty string arrives. The count is the only way a
    // user importing the file can tell how many blanks were nulls.
    let store = people();
    let (text, report) = to_csv(&batch_of(
        "MATCH (n:Person) RETURN n.name AS name, n.age AS age ORDER BY name",
        &store,
    ));
    let rows = parse_csv(&text);
    let bob = rows.iter().find(|r| r[0].starts_with("Bob")).unwrap();
    assert_eq!(bob[1], "");
    assert_eq!(report.nulls_written_as_empty, 1);
}

#[test]
fn a_list_becomes_json_in_one_field_and_is_counted() {
    let mut g = GraphStore::new();
    let a = g.create_node("N");
    g.set_node_property(
        "default",
        a,
        "tags",
        PropertyValue::Array(vec![
            PropertyValue::String("x".into()),
            PropertyValue::Integer(2),
        ]),
    )
    .unwrap();

    let (text, report) = to_csv(&batch_of("MATCH (n:N) RETURN n.tags AS tags", &g));
    let rows = parse_csv(&text);
    assert_eq!(rows[1], vec![r#"["x",2]"#.to_string()]);
    assert_eq!(report.values_written_as_json, 1);
}

#[test]
fn a_formula_field_is_reported_and_not_altered() {
    // `=1+1` in a cell is evaluated by Excel and Sheets. Prefixing or
    // stripping it would make the value that comes back different from the
    // value that went in, which is the wrong trade for a *data* export — so
    // it is counted and documented instead.
    let mut g = GraphStore::new();
    let a = g.create_node("N");
    g.set_node_property("default", a, "v", "=1+1").unwrap();

    let (text, report) = to_csv(&batch_of("MATCH (n:N) RETURN n.v AS v", &g));
    let rows = parse_csv(&text);
    assert_eq!(
        rows[1],
        vec!["=1+1".to_string()],
        "the value must survive unaltered"
    );
    assert_eq!(report.fields_a_spreadsheet_reads_as_a_formula, 1);
}

#[test]
fn an_ordinary_result_reports_nothing_lost() {
    // Counterpart to the three tests above: if these counters were always
    // non-zero they would carry no information.
    let mut g = GraphStore::new();
    let a = g.create_node("N");
    g.set_node_property("default", a, "v", 7i64).unwrap();
    let (_, report) = to_csv(&batch_of("MATCH (n:N) RETURN n.v AS v", &g));
    assert_eq!(
        report,
        CsvReport {
            rows_written: 1,
            columns: 1,
            ..Default::default()
        }
    );
}

#[test]
fn an_empty_result_still_has_its_header() {
    // A BI tool that gets no header cannot bind the columns, and an empty
    // body reads as a failed request rather than an empty answer.
    let g = GraphStore::new();
    let (text, report) = to_csv(&batch_of("MATCH (n:Nope) RETURN n.x AS x", &g));
    assert_eq!(parse_csv(&text), vec![vec!["x"]]);
    assert_eq!(report.rows_written, 0);
}

/// Just the export route, over the given store.
fn export_router(store: GraphStore) -> Router {
    use std::sync::Arc;
    use tokio::sync::RwLock;
    let state = samyama::http::server::AppState {
        store: Arc::new(RwLock::new(store)),
        engine: Arc::new(QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        persistence: None,
        transactions: Default::default(),
    };
    Router::new()
        .route(
            "/api/query/export",
            post(samyama::http::handler::export_handler),
        )
        .with_state(state)
}

#[tokio::test]
async fn the_endpoint_serves_csv_with_its_type_and_its_report() {
    let app = export_router(people());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query/export")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"query":"MATCH (n:Person) RETURN n.name AS name","format":"csv"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/csv; charset=utf-8",
        "a BI tool decides how to read the body from this header"
    );
    let report = response
        .headers()
        .get("x-samyama-export-report")
        .expect("the loss report travels with the file, not inside it")
        .to_str()
        .unwrap()
        .to_string();
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["rows_written"], 2);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let rows = parse_csv(std::str::from_utf8(&body).unwrap());
    assert_eq!(rows[0], vec!["name"]);
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn an_unknown_format_names_the_ones_that_work() {
    let app = export_router(GraphStore::new());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query/export")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"RETURN 1 AS x","format":"tsv"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("csv") && text.contains("arrow") && text.contains("parquet"),
        "the error must list every format that works, not just say no: {text}"
    );
}
