//! `LOAD PARQUET` (LANG-09, #1098).
//!
//! The same gate as `LOAD CSV`: off unless an import directory is configured, and
//! a source that resolves outside it is refused. The rows are checked against the
//! same data written as an `UNWIND` of map literals, so the expectation is the
//! engine's own reading of equivalent Cypher rather than a hand-written one.

use std::sync::Arc;

use arrow::array::{Array, Int64Builder};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, LargeStringArray, ListBuilder, StringArray,
    StringBuilder, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::csv_source::set_import_root;
use samyama::query::executor::Value;
use samyama::query::QueryEngine;

const T: &str = "default";

fn write_parquet(path: &std::path::Path, batch: &RecordBatch, row_group: usize) {
    let props = WriterProperties::builder()
        .set_max_row_group_size(row_group)
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

/// Three rows covering every type `/api/import/parquet` converts, each column
/// null in at least one row.
fn mixed() -> RecordBatch {
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.values().append_value("x");
    tags.values().append_value("y");
    tags.append(true);
    tags.append(false);
    tags.values().append_value("z");
    tags.append(true);
    let mut nums = ListBuilder::new(Int64Builder::new());
    nums.values().append_value(1);
    nums.values().append_value(2);
    nums.append(true);
    nums.values().append_value(3);
    nums.append(true);
    nums.append(false);

    let schema = Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("l", DataType::LargeUtf8, true),
        Field::new("tags", tags.finish_cloned().data_type().clone(), true),
        Field::new("nums", nums.finish_cloned().data_type().clone(), true),
    ]);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(1), None, Some(-7)])),
        Arc::new(Float64Array::from(vec![Some(1.5), Some(-0.25), None])),
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(LargeStringArray::from(vec![None, Some("big"), Some("")])),
        Arc::new(tags.finish()),
        Arc::new(nums.finish()),
    ];
    RecordBatch::try_new(Arc::new(schema), cols).unwrap()
}

/// The same three rows as Cypher literals.
const MIXED_UNWIND: &str = "UNWIND [\
    {i: 1, f: 1.5, s: 'a', b: true, l: null, tags: ['x', 'y'], nums: [1, 2]}, \
    {i: null, f: -0.25, s: 'b', b: false, l: 'big', tags: null, nums: [3]}, \
    {i: -7, f: null, s: 'c', b: null, l: '', tags: ['z'], nums: null}\
    ] AS row";

const COLS: [&str; 7] = ["i", "f", "s", "b", "l", "tags", "nums"];

/// A value as the thing it means: both nulls are `None`, and a list is its
/// elements, however the executor happened to box it.
fn norm(v: &Value) -> Option<PropertyValue> {
    if v.is_null() {
        return None;
    }
    match v {
        Value::Property(p) => Some(p.clone()),
        Value::List(items) => Some(PropertyValue::Array(
            items
                .iter()
                .map(|i| norm(i).unwrap_or(PropertyValue::Null))
                .collect(),
        )),
        other => panic!("unexpected value {other:?}"),
    }
}

fn projected_rows(
    engine: &QueryEngine,
    store: &GraphStore,
    source: &str,
) -> Vec<Vec<Option<PropertyValue>>> {
    let ret: Vec<String> = COLS.iter().map(|c| format!("row.{c} AS {c}")).collect();
    let q = format!(
        "{source} RETURN {}, keys(row) AS k ORDER BY s",
        ret.join(", ")
    );
    let out = engine
        .execute(&q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    out.records
        .iter()
        .map(|r| {
            let mut row: Vec<Option<PropertyValue>> =
                COLS.iter().map(|c| norm(r.get(c).unwrap())).collect();
            // keys(row), sorted: a null cell is a key bound to null, not a missing key.
            let mut keys = match norm(r.get("k").unwrap()) {
                Some(PropertyValue::Array(ks)) => ks,
                other => panic!("keys(row) = {other:?}"),
            };
            keys.sort_by_key(|k| format!("{k:?}"));
            row.push(Some(PropertyValue::Array(keys)));
            row
        })
        .collect()
}

fn created_nodes(engine: &QueryEngine, source: &str) -> Vec<Vec<Option<PropertyValue>>> {
    let props: Vec<String> = COLS.iter().map(|c| format!("{c}: row.{c}")).collect();
    let q = format!("{source} CREATE (:N {{{}}})", props.join(", "));
    let mut store = GraphStore::new();
    engine
        .execute_mut(&q, &mut store, T)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let mut nodes: Vec<Vec<Option<PropertyValue>>> = store
        .get_nodes_by_label(&"N".into())
        .iter()
        .map(|n| COLS.iter().map(|c| store.node_property(n.id, c)).collect())
        .collect();
    nodes.sort_by_key(|n| format!("{n:?}"));
    nodes
}

/// One test, because the import root is process-global and these would otherwise
/// race each other into whichever directory ran last (as in `load_csv.rs`).
#[test]
fn load_parquet_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(&dir.path().join("mixed.parquet"), &mixed(), 1024);
    let engine = QueryEngine::new();

    // ---- the gate ----
    set_import_root(None).unwrap();
    let mut store = GraphStore::new();
    let err = engine
        .execute_mut(
            "LOAD PARQUET FROM 'mixed.parquet' AS row CREATE (:N {s: row.s})",
            &mut store,
            T,
        )
        .expect_err("LOAD PARQUET ran with no import directory configured");
    assert!(
        err.to_string()
            .contains("LOAD PARQUET is disabled: no import directory"),
        "refused for the wrong reason: {err}"
    );
    assert_eq!(store.node_count(), 0);

    set_import_root(Some(dir.path())).unwrap();

    // ---- the rows are the rows an UNWIND of the same data gives ----
    let empty = GraphStore::new();
    let from_file = projected_rows(&engine, &empty, "LOAD PARQUET FROM 'mixed.parquet' AS row");
    let from_literal = projected_rows(&engine, &empty, MIXED_UNWIND);
    assert_eq!(from_file.len(), 3);
    assert_eq!(from_file, from_literal);
    assert_eq!(from_file[0][0], Some(PropertyValue::Integer(1)));
    assert_eq!(
        from_file[1][0], None,
        "a null Int64 cell did not read as null"
    );
    assert_eq!(
        from_file[0][5],
        Some(PropertyValue::Array(vec![
            PropertyValue::String("x".into()),
            PropertyValue::String("y".into())
        ]))
    );

    // ---- and build the same graph ----
    let file_graph = created_nodes(&engine, "LOAD PARQUET FROM 'mixed.parquet' AS row");
    let literal_graph = created_nodes(&engine, MIXED_UNWIND);
    assert_eq!(file_graph.len(), 3);
    assert_eq!(file_graph, literal_graph);
    assert!(
        file_graph.iter().any(|n| n[0].is_none()),
        "a null cell was stored as a property"
    );

    // ---- file:// URLs, and the clause after a WITH (the pipeline shape) ----
    let url = format!("file://{}", dir.path().join("mixed.parquet").display());
    let n = engine
        .execute(
            &format!("LOAD PARQUET FROM '{url}' AS row RETURN count(*) AS c"),
            &empty,
        )
        .unwrap();
    assert_eq!(
        n.records[0].get("c"),
        Some(&Value::Property(PropertyValue::Integer(3)))
    );
    let mut store = GraphStore::new();
    let n = engine
        .execute_mut(
            "CREATE (t:T) WITH t LOAD PARQUET FROM 'mixed.parquet' AS row RETURN count(*) AS c",
            &mut store,
            T,
        )
        .unwrap();
    assert_eq!(
        n.records[0].get("c"),
        Some(&Value::Property(PropertyValue::Integer(3)))
    );

    // ---- several row groups and several record batches are all read ----
    let rows = 2500i64;
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, false)]));
    let many = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(0..rows))],
    )
    .unwrap();
    write_parquet(&dir.path().join("many.parquet"), &many, 100);
    let meta = parquet::file::reader::SerializedFileReader::new(
        std::fs::File::open(dir.path().join("many.parquet")).unwrap(),
    )
    .unwrap();
    assert!(
        parquet::file::reader::FileReader::metadata(&meta).num_row_groups() > 1,
        "the fixture did not produce several row groups"
    );
    let out = engine
        .execute(
            "LOAD PARQUET FROM 'many.parquet' AS row RETURN count(*) AS c, sum(row.i) AS s",
            &empty,
        )
        .unwrap();
    assert_eq!(
        out.records[0].get("c"),
        Some(&Value::Property(PropertyValue::Integer(rows)))
    );
    assert_eq!(
        out.records[0].get("s"),
        Some(&Value::Property(PropertyValue::Integer(
            rows * (rows - 1) / 2
        )))
    );
    let mut store = GraphStore::new();
    engine
        .execute_mut(
            "LOAD PARQUET FROM 'many.parquet' AS row CREATE (:M {i: row.i})",
            &mut store,
            T,
        )
        .unwrap();
    assert_eq!(store.node_count(), rows as usize);

    // ---- a column with no property type is refused by name, not dropped ----
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, false),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let ts = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(TimestampMillisecondArray::from(vec![0])),
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("ts.parquet"), &ts, 1024);
    let err = engine
        .execute("LOAD PARQUET FROM 'ts.parquet' AS row RETURN row", &empty)
        .expect_err("a Timestamp column was read");
    assert!(
        err.to_string().contains("no property type for column at"),
        "{err}"
    );

    // ---- a missing file and a file that is not Parquet are errors, not panics ----
    let err = engine
        .execute(
            "LOAD PARQUET FROM 'absent.parquet' AS row RETURN row",
            &empty,
        )
        .expect_err("read a file that does not exist");
    assert!(
        err.to_string()
            .contains("LOAD PARQUET cannot read 'absent.parquet'"),
        "{err}"
    );
    std::fs::write(
        dir.path().join("corrupt.parquet"),
        b"name,city\nada,London\n",
    )
    .unwrap();
    let err = engine
        .execute(
            "LOAD PARQUET FROM 'corrupt.parquet' AS row RETURN row",
            &empty,
        )
        .expect_err("read a file that is not Parquet");
    assert!(err.to_string().contains("as Parquet"), "{err}");
    // A truncated one: a real footer cut off part-way.
    let whole = std::fs::read(dir.path().join("mixed.parquet")).unwrap();
    std::fs::write(
        dir.path().join("truncated.parquet"),
        &whole[..whole.len() / 2],
    )
    .unwrap();
    let err = engine
        .execute(
            "LOAD PARQUET FROM 'truncated.parquet' AS row RETURN row",
            &empty,
        )
        .expect_err("read a truncated file");
    assert!(err.to_string().contains("LOAD PARQUET"), "{err}");

    // ---- escaping the import directory ----
    let outside = tempfile::tempdir().unwrap();
    write_parquet(&outside.path().join("secret.parquet"), &mixed(), 1024);
    let err = engine
        .execute(
            &format!(
                "LOAD PARQUET FROM 'file://{}' AS row RETURN row",
                outside.path().join("secret.parquet").display()
            ),
            &empty,
        )
        .expect_err("read a file outside the import directory");
    assert!(err.to_string().contains("LOAD PARQUET refused"), "{err}");
    assert!(
        err.to_string().contains("outside the import directory"),
        "{err}"
    );
    let err = engine
        .execute(
            "LOAD PARQUET FROM '../secret.parquet' AS row RETURN row",
            &empty,
        )
        .expect_err("read a file through ..");
    assert!(
        err.to_string().contains("outside the import directory")
            || err.to_string().contains("cannot read"),
        "{err}"
    );
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            outside.path().join("secret.parquet"),
            dir.path().join("escape.parquet"),
        )
        .unwrap();
        let err = engine
            .execute(
                "LOAD PARQUET FROM 'escape.parquet' AS row RETURN row",
                &empty,
            )
            .expect_err("a symlink out of the import directory was followed");
        assert!(
            err.to_string().contains("outside the import directory"),
            "{err}"
        );
    }

    // ---- http is refused rather than fetched ----
    let err = engine
        .execute(
            "LOAD PARQUET FROM 'https://example.com/x.parquet' AS row RETURN row",
            &empty,
        )
        .expect_err("fetched an http source");
    assert!(
        err.to_string()
            .contains("LOAD PARQUET cannot read 'https' sources"),
        "{err}"
    );

    set_import_root(None).unwrap();
}

/// Two readings of a map-valued row that `LOAD PARQUET` made visible: an
/// aggregate over `row.x` read null for every row (so `sum` was silently 0),
/// and `keys(row)` was a type error. Neither needs a file, so neither needs the
/// import root this file's other test holds.
#[test]
fn aggregates_and_keys_read_a_map_valued_row() {
    let engine = QueryEngine::new();
    let out = engine
        .execute(
            "UNWIND [1, 2, 3] AS x WITH {i: x, s: 'k'} AS row \
             RETURN sum(row.i) AS s, min(row.i) AS lo, collect(row.i) AS c, \
             collect(keys(row)) AS k",
            &GraphStore::new(),
        )
        .unwrap();
    let r = &out.records[0];
    assert_eq!(
        r.get("s"),
        Some(&Value::Property(PropertyValue::Integer(6)))
    );
    assert_eq!(
        r.get("lo"),
        Some(&Value::Property(PropertyValue::Integer(1)))
    );
    assert_eq!(
        norm(r.get("c").unwrap()),
        Some(PropertyValue::Array(vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3)
        ]))
    );
    let keys = PropertyValue::Array(vec![
        PropertyValue::String("i".into()),
        PropertyValue::String("s".into()),
    ]);
    assert_eq!(
        norm(r.get("k").unwrap()),
        Some(PropertyValue::Array(vec![keys.clone(), keys.clone(), keys]))
    );
}
