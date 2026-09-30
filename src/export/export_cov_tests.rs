//! Column-kind inference, Arrow column building, IPC/Parquet output and the
//! Parquet node import.

use super::*;
use crate::graph::{EdgeId, EdgeType, GraphStore, NodeId, WriteAdmission};
use crate::query::executor::record::Record;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int64Type};
use std::collections::{BTreeMap, HashMap};

fn p(v: PropertyValue) -> Value {
    Value::Property(v)
}
fn int(i: i64) -> Value {
    p(PropertyValue::Integer(i))
}
fn flt(f: f64) -> Value {
    p(PropertyValue::Float(f))
}
fn st(s: &str) -> Value {
    p(PropertyValue::String(s.into()))
}

fn batch(cols: &[&str], rows: Vec<Vec<(&str, Value)>>) -> RecordBatch {
    let mut b = RecordBatch::new(cols.iter().map(|c| c.to_string()).collect());
    for row in rows {
        let mut r = Record::new();
        for (k, v) in row {
            r.bind(k, v);
        }
        b.records.push(r);
    }
    b
}

fn column(values: Vec<Value>) -> ArrowBatch {
    let rows = values.into_iter().map(|v| vec![("c", v)]).collect();
    to_arrow(&batch(&["c"], rows)).unwrap()
}

// ------------------------------------------------------------ kinds

#[test]
fn unify_table() {
    use Kind::*;
    assert_eq!(Null.unify(Int), Int);
    assert_eq!(Str.unify(Null), Str);
    assert_eq!(Int.unify(Int), Int);
    assert_eq!(Int.unify(Float), Float);
    assert_eq!(Float.unify(Int), Float);
    assert_eq!(ListEmpty.unify(ListInt), ListInt);
    assert_eq!(ListInt.unify(ListEmpty), ListInt);
    assert_eq!(ListEmpty.unify(ListFloat), ListFloat);
    assert_eq!(ListFloat.unify(ListEmpty), ListFloat);
    assert_eq!(ListEmpty.unify(ListStr), ListStr);
    assert_eq!(ListStr.unify(ListEmpty), ListStr);
    assert_eq!(ListInt.unify(ListFloat), ListFloat);
    assert_eq!(ListFloat.unify(ListInt), ListFloat);
    assert_eq!(Bool.unify(Int), Json);
    assert_eq!(ListStr.unify(ListInt), Json);
}

#[test]
fn kind_of_property_covers_every_variant() {
    use PropertyValue as P;
    assert_eq!(kind_of_property(&P::Null), Kind::Null);
    assert_eq!(kind_of_property(&P::Boolean(true)), Kind::Bool);
    assert_eq!(kind_of_property(&P::Integer(1)), Kind::Int);
    assert_eq!(kind_of_property(&P::Float(1.0)), Kind::Float);
    assert_eq!(kind_of_property(&P::String("s".into())), Kind::Str);
    for t in [
        P::Date(1),
        P::LocalTime(1),
        P::Time {
            nanos: 0,
            offset_seconds: 0,
        },
        P::LocalDateTime { secs: 0, nanos: 0 },
        P::ZonedDateTime {
            secs: 0,
            nanos: 0,
            offset_seconds: 0,
            zone: None,
        },
        P::DateTime(0),
        P::Duration {
            months: 0,
            days: 1,
            seconds: 0,
            nanos: 0,
        },
    ] {
        assert_eq!(kind_of_property(&t), Kind::Str, "{t:?}");
    }
    assert_eq!(kind_of_property(&P::Vector(vec![1.0])), Kind::ListFloat);
    assert_eq!(kind_of_property(&P::Array(vec![])), Kind::ListEmpty);
    assert_eq!(kind_of_property(&P::Array(vec![P::Null])), Kind::ListFloat);
    assert_eq!(
        kind_of_property(&P::Array(vec![P::Integer(1), P::Float(2.5)])),
        Kind::ListFloat
    );
    assert_eq!(
        kind_of_property(&P::Array(vec![P::Integer(1)])),
        Kind::ListInt
    );
    assert_eq!(
        kind_of_property(&P::Array(vec![P::String("a".into())])),
        Kind::ListStr
    );
    assert_eq!(
        kind_of_property(&P::Array(vec![P::Boolean(true)])),
        Kind::Json
    );
    assert_eq!(kind_of_property(&P::Map(HashMap::new())), Kind::Json);
}

#[test]
fn kind_of_value_lists_and_entities() {
    assert_eq!(kind_of(&Value::Null), Kind::Null);
    assert_eq!(kind_of(&Value::List(vec![])), Kind::ListEmpty);
    assert_eq!(kind_of(&Value::List(vec![Value::Null])), Kind::ListFloat);
    assert_eq!(kind_of(&Value::List(vec![int(1)])), Kind::ListInt);
    assert_eq!(kind_of(&Value::List(vec![flt(1.5)])), Kind::ListFloat);
    assert_eq!(kind_of(&Value::List(vec![st("a")])), Kind::ListStr);
    assert_eq!(
        kind_of(&Value::List(vec![Value::NodeRef(NodeId::new(1))])),
        Kind::Json
    );
    assert_eq!(kind_of(&Value::NodeRef(NodeId::new(1))), Kind::Json);
}

#[test]
fn list_extractors() {
    assert_eq!(float_list(&Value::Null), None);
    assert_eq!(float_list(&p(PropertyValue::Null)), None);
    assert_eq!(
        float_list(&p(PropertyValue::Vector(vec![0.5]))),
        Some(vec![Some(0.5)])
    );
    assert_eq!(
        float_list(&p(PropertyValue::Array(vec![
            PropertyValue::Integer(2),
            PropertyValue::String("x".into())
        ]))),
        Some(vec![Some(2.0), None])
    );
    assert_eq!(
        float_list(&Value::List(vec![int(1), st("x")])),
        Some(vec![Some(1.0), None])
    );
    assert_eq!(float_list(&st("x")), None);

    assert_eq!(int_list(&Value::Null), None);
    assert_eq!(int_list(&p(PropertyValue::Null)), None);
    assert_eq!(
        int_list(&p(PropertyValue::Array(vec![
            PropertyValue::Integer(4),
            PropertyValue::Null
        ]))),
        Some(vec![Some(4), None])
    );
    assert_eq!(
        int_list(&Value::List(vec![int(5), Value::Null])),
        Some(vec![Some(5), None])
    );
    assert_eq!(int_list(&st("x")), None);

    assert_eq!(string_list(&Value::Null), None);
    assert_eq!(string_list(&p(PropertyValue::Null)), None);
    assert_eq!(
        string_list(&p(PropertyValue::Array(vec![
            PropertyValue::String("a".into()),
            PropertyValue::Null,
            PropertyValue::Integer(3)
        ]))),
        Some(vec![Some("a".into()), None, Some("3".into())])
    );
    assert_eq!(
        string_list(&Value::List(vec![
            st("a"),
            Value::Null,
            Value::NodeRef(NodeId::new(7))
        ])),
        Some(vec![Some("a".into()), None, Some("{\"id\":7}".into())])
    );
    assert_eq!(string_list(&int(1)), None);

    assert_eq!(as_text(&Value::Null), None);
    assert_eq!(as_text(&p(PropertyValue::Null)), None);
    assert_eq!(as_text(&st("s")), Some("s".into()));
    assert_eq!(
        as_text(&p(PropertyValue::Date(0))),
        Some(PropertyValue::Date(0).to_cypher_string())
    );
    assert_eq!(as_text(&Value::NodeRef(NodeId::new(1))), None);
}

// ------------------------------------------------------------ json

#[test]
fn json_of_entities_and_containers() {
    let mut g = GraphStore::new();
    let a = g.create_node("A");
    g.set_node_property("default", a, "k", 1i64).unwrap();
    let b = g.create_node("B");
    let e = g.create_edge(a, b, "R").unwrap();
    let node = g.node_materialized(a).unwrap();
    let edge = g.get_edge(e).unwrap();

    let jn = json_of(&Value::Node(a, Box::new(node)));
    assert_eq!(jn["id"], a.as_u64());
    assert_eq!(jn["labels"], serde_json::json!(["A"]));
    assert_eq!(jn["properties"]["k"], 1);
    assert_eq!(
        json_of(&Value::Edge(e, Box::new(edge))),
        serde_json::json!({"id": e.as_u64(), "type": "R", "source": a.as_u64(), "target": b.as_u64()})
    );
    assert_eq!(
        json_of(&Value::EdgeRef(EdgeId::new(3), a, b, EdgeType::new("T"))),
        serde_json::json!({"id": 3, "type": "T", "source": a.as_u64(), "target": b.as_u64()})
    );
    assert_eq!(
        json_of(&Value::Path {
            nodes: vec![a, b],
            edges: vec![e]
        }),
        serde_json::json!({"nodes": [a.as_u64(), b.as_u64()], "edges": [e.as_u64()]})
    );
    let mut m = BTreeMap::new();
    m.insert("x".to_string(), Value::List(vec![Value::Null, int(2)]));
    assert_eq!(
        value_as_json(&Value::Map(m)),
        serde_json::json!({"x": [null, 2]})
    );
}

#[test]
fn resolve_nodes_materializes_refs_inside_lists_and_maps() {
    let mut g = GraphStore::new();
    let a = g.create_node("A");
    g.set_node_property("default", a, "name", "ann").unwrap();
    let ghost = NodeId::new(999_999);
    let mut m = BTreeMap::new();
    m.insert("n".to_string(), Value::NodeRef(a));
    let mut b = batch(
        &["n", "l", "m", "g", "x"],
        vec![vec![
            ("n", Value::NodeRef(a)),
            ("l", Value::List(vec![Value::NodeRef(a)])),
            ("m", Value::Map(m)),
            ("g", Value::NodeRef(ghost)),
            ("x", int(1)),
        ]],
    );
    resolve_nodes(&mut b, &g);
    let r = &b.records[0];
    assert!(
        matches!(r.get("n"), Some(Value::Node(id, n)) if *id == a && n.labels.iter().any(|l| l.as_str() == "A"))
    );
    assert!(matches!(r.get("l"), Some(Value::List(v)) if matches!(v[0], Value::Node(..))));
    assert!(matches!(r.get("m"), Some(Value::Map(m)) if matches!(m["n"], Value::Node(..))));
    assert!(
        matches!(r.get("g"), Some(Value::NodeRef(id)) if *id == ghost),
        "unknown ids stay refs"
    );
    assert_eq!(r.get("x"), Some(&int(1)));
    let json = json_of(r.get("n").unwrap());
    assert_eq!(json["properties"]["name"], "ann", "{json}");

    // A materialized node is refreshed from the store.
    let stale = Value::Node(
        a,
        Box::new(crate::graph::Node::new(a, crate::graph::Label::new("Old"))),
    );
    let mut b3 = batch(&["n"], vec![vec![("n", stale)]]);
    resolve_nodes(&mut b3, &g);
    assert!(
        matches!(b3.records[0].get("n"), Some(Value::Node(_, n)) if n.labels.iter().any(|l| l.as_str() == "A"))
    );

    // A materialized node whose id has gone keeps its own copy.
    let mut b2 = batch(&["n"], vec![vec![("n", r.get("n").unwrap().clone())]]);
    resolve_nodes(&mut b2, &GraphStore::new());
    assert!(matches!(b2.records[0].get("n"), Some(Value::Node(..))));
}

// ------------------------------------------------------------ arrow columns

#[test]
fn arrow_types_per_column_kind() {
    let b = column(vec![Value::Null, Value::Null]);
    assert_eq!(b.schema().field(0).data_type(), &DataType::Null);
    assert_eq!(b.num_rows(), 2);

    let b = column(vec![p(PropertyValue::Boolean(true)), Value::Null]);
    let a = b.column(0).as_boolean();
    assert!(a.value(0) && a.is_null(1));

    let b = column(vec![int(1), Value::Null]);
    let a = b.column(0).as_primitive::<Int64Type>();
    assert_eq!(a.value(0), 1);
    assert!(a.is_null(1));

    let b = column(vec![int(1), flt(2.5), Value::Null]);
    let a = b.column(0).as_primitive::<Float64Type>();
    assert_eq!((a.value(0), a.value(1)), (1.0, 2.5));
    assert!(a.is_null(2));

    let b = column(vec![st("a"), p(PropertyValue::Date(0)), Value::Null]);
    let a = b.column(0).as_string::<i32>();
    assert_eq!(a.value(0), "a");
    assert_eq!(a.value(1), PropertyValue::Date(0).to_cypher_string());
    assert!(a.is_null(2));
}

#[test]
fn arrow_list_columns() {
    // List<Float64> from vectors, arrays with a null element, and a null row.
    let b = column(vec![
        p(PropertyValue::Vector(vec![1.0, 2.0])),
        p(PropertyValue::Array(vec![
            PropertyValue::Float(0.5),
            PropertyValue::Null,
        ])),
        Value::Null,
    ]);
    let l = b.column(0).as_list::<i32>();
    assert_eq!(
        l.value(0).as_primitive::<Float64Type>().values().to_vec(),
        vec![1.0, 2.0]
    );
    assert!(l.value(1).is_null(1));
    assert!(l.is_null(2));

    // All-empty lists still produce a list column.
    let b = column(vec![Value::List(vec![]), Value::List(vec![])]);
    assert!(matches!(b.schema().field(0).data_type(), DataType::List(_)));

    // List<Int64>, with a null element and a null row.
    let b = column(vec![
        Value::List(vec![int(1), Value::Null]),
        Value::List(vec![]),
        Value::Null,
    ]);
    let l = b.column(0).as_list::<i32>();
    let first = l.value(0);
    let ints = first.as_primitive::<Int64Type>();
    assert_eq!(ints.value(0), 1);
    assert!(ints.is_null(1));
    assert_eq!(l.value(1).len(), 0);
    assert!(l.is_null(2));

    // List<Utf8>.
    let b = column(vec![Value::List(vec![st("a"), Value::Null]), Value::Null]);
    let l = b.column(0).as_list::<i32>();
    let first = l.value(0);
    let strs = first.as_string::<i32>();
    assert_eq!(strs.value(0), "a");
    assert!(strs.is_null(1));
    assert!(l.is_null(1));
}

#[test]
fn arrow_json_fallback_for_mixed_and_entity_columns() {
    let b = column(vec![int(1), st("x"), Value::Null, p(PropertyValue::Null)]);
    let a = b.column(0).as_string::<i32>();
    assert_eq!(b.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(a.value(0), "1");
    assert_eq!(a.value(1), "\"x\"");
    assert!(a.is_null(2) && a.is_null(3));

    let b = column(vec![Value::NodeRef(NodeId::new(4))]);
    assert_eq!(b.column(0).as_string::<i32>().value(0), "{\"id\":4}");
}

#[test]
fn unbound_column_is_all_null_and_empty_result_keeps_schema() {
    let b = to_arrow(&batch(&["a", "missing"], vec![vec![("a", int(1))]])).unwrap();
    assert_eq!(b.schema().field(1).data_type(), &DataType::Null);
    assert!(b.schema().fields().iter().all(|f| f.is_nullable()));

    let empty = to_arrow(&batch(&["a", "b"], vec![])).unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema().fields().len(), 2);
}

// ------------------------------------------------------------ IPC / Parquet

#[test]
fn ipc_stream_reads_back() {
    let rb = batch(&["n"], vec![vec![("n", int(7))], vec![("n", int(8))]]);
    let bytes = to_ipc(&rb).unwrap();
    let reader =
        arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec(),
        vec![7, 8]
    );
}

#[test]
fn parquet_round_trips_into_nodes_with_nulls_unset() {
    let rb = batch(
        &["name", "age", "score", "ok", "tags", "ids", "xs"],
        vec![
            vec![
                ("name", st("ann")),
                ("age", int(30)),
                ("score", flt(1.5)),
                ("ok", p(PropertyValue::Boolean(true))),
                ("tags", Value::List(vec![st("a"), Value::Null])),
                ("ids", Value::List(vec![int(1), Value::Null])),
                ("xs", Value::List(vec![flt(0.5), Value::Null])),
            ],
            vec![("name", st("bob"))],
        ],
    );
    let bytes = to_parquet(&rb).unwrap();
    let mut g = GraphStore::new();
    let stats = import::parquet_to_nodes(&mut g, "default", "P", bytes).unwrap();
    assert_eq!(stats.nodes_created, 2);
    assert_eq!(
        stats.columns,
        vec!["name", "age", "score", "ok", "tags", "ids", "xs"]
    );
    assert!(stats.skipped_columns.is_empty());

    let mut nodes: Vec<_> = g
        .get_nodes_by_label(&crate::graph::Label::new("P"))
        .into_iter()
        .map(|n| g.node_properties_merged(n.id))
        .collect();
    nodes.sort_by_key(|m| format!("{:?}", m.get("name")));
    let ann = &nodes[0];
    assert_eq!(ann.get("age"), Some(&PropertyValue::Integer(30)));
    assert_eq!(ann.get("score"), Some(&PropertyValue::Float(1.5)));
    assert_eq!(ann.get("ok"), Some(&PropertyValue::Boolean(true)));
    assert_eq!(
        ann.get("tags"),
        Some(&PropertyValue::Array(vec![
            PropertyValue::String("a".into()),
            PropertyValue::Null
        ]))
    );
    assert_eq!(
        ann.get("ids"),
        Some(&PropertyValue::Array(vec![
            PropertyValue::Integer(1),
            PropertyValue::Null
        ]))
    );
    assert_eq!(
        ann.get("xs"),
        Some(&PropertyValue::Array(vec![
            PropertyValue::Float(0.5),
            PropertyValue::Null
        ]))
    );
    let bob = &nodes[1];
    assert_eq!(bob.get("name"), Some(&PropertyValue::String("bob".into())));
    assert!(
        bob.get("age").is_none() && bob.get("tags").is_none(),
        "null cells set nothing"
    );
}

fn parquet_of(batch: ArrowBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    buf
}

#[test]
fn parquet_import_names_skipped_columns_and_reads_large_utf8() {
    use arrow::array::{Date32Array, LargeStringArray, ListArray};
    use arrow::datatypes::Int32Type;

    let large = LargeStringArray::from(vec![Some("big"), None]);
    let date = Date32Array::from(vec![Some(1), None]);
    let int32_list =
        ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)]), None]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("large", DataType::LargeUtf8, true),
        Field::new("date", DataType::Date32, true),
        Field::new("ints32", int32_list.data_type().clone(), true),
    ]));
    let rb = ArrowBatch::try_new(
        schema,
        vec![
            Arc::new(large) as ArrayRef,
            Arc::new(date),
            Arc::new(int32_list),
        ],
    )
    .unwrap();

    let mut g = GraphStore::new();
    let stats = import::parquet_to_nodes(&mut g, "default", "Q", parquet_of(rb)).unwrap();
    assert_eq!(stats.nodes_created, 2);
    assert_eq!(
        stats.skipped_columns,
        vec!["date".to_string(), "ints32".to_string()]
    );
    let values: Vec<Option<PropertyValue>> = g
        .get_nodes_by_label(&crate::graph::Label::new("Q"))
        .into_iter()
        .map(|n| g.node_properties_merged(n.id).get("large").cloned())
        .collect();
    assert!(values.contains(&Some(PropertyValue::String("big".into()))));
    assert!(values.contains(&None));
}

#[test]
fn parquet_import_reads_every_record_batch() {
    // The reader yields batches of 1024 rows; more than that spans two.
    let rows: Vec<Vec<(&str, Value)>> = (0..1500).map(|i| vec![("n", int(i))]).collect();
    let bytes = to_parquet(&batch(&["n"], rows)).unwrap();
    let mut g = GraphStore::new();
    let stats = import::parquet_to_nodes(&mut g, "default", "P", bytes).unwrap();
    assert_eq!(stats.nodes_created, 1500);
    assert_eq!(stats.columns, vec!["n".to_string()]);
    assert_eq!(g.node_count(), 1500);
}

#[test]
fn parquet_import_refuses_garbage_and_over_quota_files() {
    let mut g = GraphStore::new();
    let err =
        import::parquet_to_nodes(&mut g, "default", "P", b"not parquet".to_vec()).unwrap_err();
    assert!(matches!(err, ExportError::Parquet(_)), "{err:?}");

    let rb = batch(&["n"], vec![vec![("n", int(1))], vec![("n", int(2))]]);
    let bytes = to_parquet(&rb).unwrap();
    g.set_write_admission(Some(WriteAdmission {
        nodes_used: 0,
        max_nodes: Some(1),
        edges_used: 0,
        max_edges: None,
    }));
    let err = import::parquet_to_nodes(&mut g, "default", "P", bytes).unwrap_err();
    assert!(matches!(err, ExportError::QuotaExceeded(_)), "{err:?}");
    assert!(err.to_string().starts_with("quota exceeded: "));
    assert_eq!(g.node_count(), 0, "nothing written before the refusal");
}

#[test]
fn export_error_messages() {
    assert_eq!(ExportError::Arrow("a".into()).to_string(), "arrow: a");
    assert_eq!(ExportError::Parquet("b".into()).to_string(), "parquet: b");
}
