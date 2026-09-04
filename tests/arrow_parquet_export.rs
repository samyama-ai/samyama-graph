//! A query result survives the trip out through Arrow and Parquet (#1097).
//!
//! Spec 09 sets the bar for this one: the round-trip has to cover **nulls,
//! unicode, floats, nested lists and vectors** — the types that break
//! exporters — and not merely ints and strings, which any exporter gets right.
//!
//! Both directions are read back with the Arrow and Parquet readers rather than
//! inspected as bytes: a writer that produces a file nothing can open is the
//! failure this is for, and a byte-length assertion cannot see it.

use arrow::array::{Array, AsArray, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use samyama::export::{to_arrow, to_ipc, to_parquet};
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, RecordBatch};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn query(store: &GraphStore, cypher: &str) -> RecordBatch {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
}

/// Three rows, and every awkward type in one place.
///
/// The middle row omits `score`, `tags` and `embedding` entirely, which is how
/// a null reaches a column in Cypher — there is no stored null.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    run(
        &mut store,
        r#"CREATE (:Doc {ord: 1, title: "café — naïve 日本語 🌏", score: 1.5,
                         live: true, tags: ["a", "b"], counts: [1, 2, 3]})"#,
    );
    run(&mut store, r#"CREATE (:Doc {ord: 2, title: "plain", live: false})"#);
    run(
        &mut store,
        r#"CREATE (:Doc {ord: 3, title: "", score: -0.25,
                         live: true, tags: [], counts: [7]})"#,
    );
    // A vector is set through the API: a list literal stays a list in Cypher.
    //
    // Selected by `ord`, not by position in a label scan. The scan's order is
    // not defined, so "skip the second one" would have skipped whichever node
    // the scan happened to yield second and moved the null to a different row
    // between runs.
    let targets: Vec<_> = store
        .get_nodes_by_label(&samyama::graph::types::Label::new("Doc"))
        .iter()
        .filter(|n| {
            !matches!(n.get_property("ord"), Some(PropertyValue::Integer(2)))
        })
        .map(|n| n.id)
        .collect();
    assert_eq!(targets.len(), 2, "two of the three rows get an embedding");
    for id in targets {
        store
            .set_node_property(
                "default",
                id,
                "embedding".to_string(),
                PropertyValue::Vector(vec![0.5, -1.5, 2.0]),
            )
            .expect("set embedding");
    }
    store
}

const Q: &str = "MATCH (d:Doc) RETURN d.ord AS ord, d.title AS title, d.score AS score, \
                 d.live AS live, d.tags AS tags, d.counts AS counts, \
                 d.embedding AS embedding ORDER BY ord";

#[test]
fn every_column_gets_the_type_its_values_need() {
    let store = fixture();
    let batch = to_arrow(&query(&store, Q)).expect("export");

    assert_eq!(batch.num_rows(), 3);
    let f = |name: &str| batch.schema().field_with_name(name).unwrap().data_type().clone();

    assert_eq!(f("ord"), DataType::Int64, "whole numbers stay integers");
    assert_eq!(f("title"), DataType::Utf8);
    // 1.5 and -0.25 with a null between them: still a float column.
    assert_eq!(f("score"), DataType::Float64);
    assert_eq!(f("live"), DataType::Boolean);
    assert!(matches!(f("tags"), DataType::List(_)), "a list of strings is a list");
    match f("counts") {
        DataType::List(field) => assert_eq!(
            field.data_type(),
            &DataType::Int64,
            "a list of whole numbers stays a list of integers — an id list turning \
             into floats is a small wrongness a downstream join would feel"
        ),
        other => panic!("counts should be a list, got {other:?}"),
    }
    assert!(matches!(f("embedding"), DataType::List(_)), "a vector is a list of floats");

    // Every column is nullable, because a Cypher projection always can be.
    for field in batch.schema().fields() {
        assert!(field.is_nullable(), "{} should be nullable", field.name());
    }
}

#[test]
fn the_values_survive_including_the_awkward_ones() {
    let store = fixture();
    let batch = to_arrow(&query(&store, Q)).expect("export");

    let ord = batch.column_by_name("ord").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ord.values(), &[1, 2, 3]);

    let title = batch.column_by_name("title").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(title.value(0), "café — naïve 日本語 🌏", "unicode survives byte-for-byte");
    assert_eq!(title.value(2), "", "an empty string is not a null");
    assert!(!title.is_null(2));

    let score = batch.column_by_name("score").unwrap()
        .as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(score.value(0), 1.5);
    assert!(score.is_null(1), "the row that never set `score` is null, not 0.0");
    assert_eq!(score.value(2), -0.25);

    let live = batch.column_by_name("live").unwrap()
        .as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(live.value(0) && !live.value(1) && live.value(2));

    let counts = batch.column_by_name("counts").unwrap().as_list::<i32>();
    assert!(counts.is_null(1), "an absent list is null, not an empty list");
    let first: Vec<i64> = counts.value(0).as_primitive::<Int64Type>().values().to_vec();
    assert_eq!(first, vec![1, 2, 3], "a list of integers stays integers");

    let tags = batch.column_by_name("tags").unwrap().as_list::<i32>();
    assert_eq!(tags.value(0).len(), 2);
    assert_eq!(
        tags.value(2).len(),
        0,
        "an empty list is an empty list, distinct from the null on row 2"
    );
    assert!(!tags.is_null(2));

    let emb = batch.column_by_name("embedding").unwrap().as_list::<i32>();
    let v: Vec<f64> = emb.value(0).as_primitive::<Float64Type>().values().to_vec();
    assert_eq!(v, vec![0.5, -1.5, 2.0], "a vector keeps its values and its order");
    assert!(emb.is_null(1));
}

#[test]
fn parquet_reads_back_as_what_was_written() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let store = fixture();
    let source = query(&store, Q);
    let written = to_arrow(&source).expect("arrow");
    let bytes = to_parquet(&source).expect("parquet");

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .expect("the file opens")
        .build()
        .expect("reader");
    let back: Vec<_> = reader.map(|b| b.expect("batch")).collect();
    assert_eq!(back.len(), 1, "one batch for three rows");
    let back = &back[0];

    assert_eq!(back.num_rows(), written.num_rows());
    assert_eq!(back.schema().fields().len(), written.schema().fields().len());
    let back_schema = back.schema();
    for field in written.schema().fields() {
        let there = back_schema
            .field_with_name(field.name())
            .expect("column survives");
        assert_eq!(there.data_type(), field.data_type(), "{} type", field.name());
    }
    let title = back.column_by_name("title").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(title.value(0), "café — naïve 日本語 🌏");
    let score = back.column_by_name("score").unwrap()
        .as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(score.is_null(1));
}

#[test]
fn the_arrow_stream_reads_back_as_what_was_written() {
    let store = fixture();
    let source = query(&store, Q);
    let bytes = to_ipc(&source).expect("ipc");

    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .expect("the stream opens");
    let back: Vec<_> = reader.map(|b| b.expect("batch")).collect();
    assert_eq!(back.len(), 1);
    let back = &back[0];
    assert_eq!(back.num_rows(), 3);

    let emb = back.column_by_name("embedding").unwrap().as_list::<i32>();
    let v: Vec<f64> = emb.value(0).as_primitive::<Float64Type>().values().to_vec();
    assert_eq!(v, vec![0.5, -1.5, 2.0]);
}

#[test]
fn a_mixed_column_falls_back_to_json_rather_than_guessing() {
    let mut store = GraphStore::new();
    run(&mut store, r#"CREATE (:M {ord: 1, v: 7})"#);
    run(&mut store, r#"CREATE (:M {ord: 2, v: "seven"})"#);
    let batch = to_arrow(&query(
        &store,
        "MATCH (m:M) RETURN m.ord AS ord, m.v AS v ORDER BY ord",
    ))
    .expect("export");

    assert_eq!(
        batch.schema().field_with_name("v").unwrap().data_type(),
        &DataType::Utf8,
        "an integer and a string in one column cannot be Int64 or Utf8-as-text"
    );
    let v = batch.column_by_name("v").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(v.value(0), "7", "JSON, so the integer is not quoted");
    assert_eq!(v.value(1), "\"seven\"", "and the string is");
}

#[test]
fn an_entity_column_becomes_json_and_says_what_it_is() {
    let mut store = GraphStore::new();
    run(&mut store, r#"CREATE (:P {name: "Ada"})"#);
    let batch = to_arrow(&query(&store, "MATCH (p:P) RETURN p")).expect("export");

    let col = batch.column_by_name("p").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let parsed: serde_json::Value = serde_json::from_str(col.value(0)).expect("valid JSON");
    assert_eq!(parsed["labels"][0], "P");
    assert_eq!(parsed["properties"]["name"], "Ada");
}

#[test]
fn an_empty_result_keeps_its_schema() {
    let store = fixture();
    let batch = to_arrow(&query(&store, "MATCH (d:Doc) WHERE d.ord > 99 RETURN d.ord AS ord"))
        .expect("export");
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema().fields().len(), 1, "no rows is not no columns");
    // And it still writes: a reader that gets zero bytes cannot tell an empty
    // answer from a failed one.
    assert!(!to_parquet(&query(&store, "MATCH (d:Doc) WHERE d.ord > 99 RETURN d.ord AS ord"))
        .expect("parquet")
        .is_empty());
}

/// Export and import are the same shape: what goes out comes back (#1098).
///
/// This is the claim INT-05 actually makes — *round-trip* tested — and it is the
/// one an export alone cannot support. Every value is compared through a second
/// export of the re-imported nodes, so the comparison is between two things the
/// engine produced from the same data rather than between the engine and a
/// hand-written expectation that could be wrong in the same direction.
#[test]
fn parquet_round_trips_back_into_nodes() {
    use samyama::export::import::parquet_to_nodes;

    let store = fixture();
    let out = to_parquet(&query(&store, Q)).expect("export");

    let mut fresh = GraphStore::new();
    let stats = parquet_to_nodes(&mut fresh, "default", "Round", out.clone()).expect("import");
    assert_eq!(stats.nodes_created, 3);
    assert!(
        stats.skipped_columns.is_empty(),
        "nothing we wrote should be unreadable on the way back: {:?}",
        stats.skipped_columns
    );

    let back = to_arrow(&query(
        &fresh,
        "MATCH (r:Round) RETURN r.ord AS ord, r.title AS title, r.score AS score, \
         r.live AS live, r.tags AS tags, r.counts AS counts, r.embedding AS embedding \
         ORDER BY ord",
    ))
    .expect("re-export");
    let there = to_arrow(&query(&store, Q)).expect("first export");

    assert_eq!(back.num_rows(), there.num_rows());
    let back_schema = back.schema();
    for field in there.schema().fields() {
        let mirrored = back_schema
            .field_with_name(field.name())
            .unwrap_or_else(|_| panic!("{} survives the round trip", field.name()));
        assert_eq!(
            mirrored.data_type(),
            field.data_type(),
            "{} keeps its type",
            field.name()
        );
        assert_eq!(
            back.column_by_name(field.name()).unwrap().as_ref(),
            there.column_by_name(field.name()).unwrap().as_ref(),
            "{} keeps its values",
            field.name()
        );
    }
}

/// A null cell sets no property, rather than a property set to null.
///
/// Cypher has no stored null, so "the column was null" and "the node has no
/// such key" have to be the same state on the way back in — otherwise
/// `keys(n)` and `properties(n)` report a key nobody set.
#[test]
fn a_null_cell_creates_no_property() {
    use samyama::export::import::parquet_to_nodes;

    let store = fixture();
    let out = to_parquet(&query(&store, Q)).expect("export");
    let mut fresh = GraphStore::new();
    parquet_to_nodes(&mut fresh, "default", "Round", out).expect("import");

    let batch = query(
        &fresh,
        "MATCH (r:Round) WHERE r.ord = 2 RETURN size(keys(r)) AS n, r.score AS score",
    );
    let rec = &batch.records[0];
    assert!(
        matches!(
            rec.get("score"),
            None | Some(samyama::query::executor::Value::Null)
                | Some(samyama::query::executor::Value::Property(PropertyValue::Null))
        ),
        "the row that never had a score still has none"
    );
    match rec.get("n") {
        Some(samyama::query::executor::Value::Property(PropertyValue::Integer(n))) => {
            assert_eq!(*n, 3, "ord, title and live — not the three nulls beside them")
        }
        other => panic!("expected a count, got {other:?}"),
    }
}
