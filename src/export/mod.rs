//! Arrow and Parquet export for query results (INT-05, ML-01 — #1097).
//!
//! A Cypher result is a list of records; Arrow is columnar and needs one type
//! per column. The whole of this module is that reconciliation, and the
//! interesting part is what it refuses to guess.
//!
//! **A column's type is inferred from the values actually in it**, in one pass,
//! by unifying a per-value kind:
//!
//! | values in the column | Arrow type |
//! |---|---|
//! | booleans (and nulls) | `Boolean` |
//! | integers | `Int64` |
//! | integers and floats | `Float64` |
//! | strings | `Utf8` |
//! | lists of integers | `List<Int64>` |
//! | vectors, or lists with any fractional number | `List<Float64>` |
//! | lists of strings | `List<Utf8>` |
//! | anything else, or a mix of the above | `Utf8` holding JSON |
//!
//! Null is absorbed by every kind and every column is nullable, so a column
//! that is entirely null comes out as `Null`-typed rather than being guessed
//! at.
//!
//! **Temporal values are exported as ISO-8601 text, not as Arrow timestamps**,
//! and that is a decision rather than a shortcut. Arrow carries one time zone
//! per *column*; Cypher carries a zone per *value*, and a `ZonedDateTime` may
//! carry a named IANA zone that an offset cannot reconstruct across a DST
//! boundary. Mapping the five temporal types onto `Timestamp` would silently
//! drop that for exactly the values where it matters, and a lossy export is
//! worse than a text one — the text round-trips, and `pandas.to_datetime`
//! reads it.
//!
//! Nodes, relationships, paths and maps become JSON text for the same reason:
//! Arrow has no type for them, and inventing a flattening here would produce a
//! column whose meaning depends on which query wrote it.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, NullArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch as ArrowBatch;

use crate::graph::PropertyValue;
use crate::query::executor::record::{RecordBatch, Value};

/// Why an export could not be produced.
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// Arrow itself refused the batch — a schema/array length mismatch, and a
    /// bug here rather than in the caller's query.
    #[error("arrow: {0}")]
    Arrow(String),
    /// Parquet writing failed.
    #[error("parquet: {0}")]
    Parquet(String),
}

/// The Arrow type a single value would need, before unification.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// Absorbed by everything: a null fits in any column.
    Null,
    Bool,
    Int,
    Float,
    Str,
    ListInt,
    ListFloat,
    ListStr,
    /// A list with no elements, which cannot say what it is a list of. It
    /// takes the column's word for it, rather than forcing the column to JSON
    /// because one row happened to be `[]`.
    ListEmpty,
    /// JSON text — either a value Arrow has no type for, or a column whose
    /// values disagree.
    Json,
}

impl Kind {
    /// The narrowest kind that can hold both.
    fn unify(self, other: Kind) -> Kind {
        use Kind::*;
        match (self, other) {
            (Null, k) | (k, Null) => k,
            (a, b) if a == b => a,
            // The one widening worth having: a column of counts that happens to
            // contain an average is a Float64 column, not a JSON one.
            (Int, Float) | (Float, Int) => Float,
            // `[]` next to `["a"]` is a column of string lists with an empty
            // one in it, not a type conflict.
            (ListEmpty, ListInt) | (ListInt, ListEmpty) => ListInt,
            (ListEmpty, ListFloat) | (ListFloat, ListEmpty) => ListFloat,
            (ListEmpty, ListStr) | (ListStr, ListEmpty) => ListStr,
            // A list of ids stays a list of ids unless something in the column
            // is fractional. `[1, 2, 3]` arriving as `[1.0, 2.0, 3.0]` is a
            // small wrongness that a downstream join on those ids would feel.
            (ListInt, ListFloat) | (ListFloat, ListInt) => ListFloat,
            _ => Json,
        }
    }
}

fn kind_of_property(p: &PropertyValue) -> Kind {
    match p {
        PropertyValue::Null => Kind::Null,
        PropertyValue::Boolean(_) => Kind::Bool,
        PropertyValue::Integer(_) => Kind::Int,
        PropertyValue::Float(_) => Kind::Float,
        PropertyValue::String(_) => Kind::Str,
        // See the module docs: text, deliberately.
        PropertyValue::Date(_)
        | PropertyValue::LocalTime(_)
        | PropertyValue::Time { .. }
        | PropertyValue::LocalDateTime { .. }
        | PropertyValue::ZonedDateTime { .. }
        | PropertyValue::DateTime(_)
        | PropertyValue::Duration { .. } => Kind::Str,
        PropertyValue::Vector(_) => Kind::ListFloat,
        PropertyValue::Array(items) if items.is_empty() => Kind::ListEmpty,
        PropertyValue::Array(items) => {
            let mut k = Kind::Null;
            for it in items {
                k = k.unify(kind_of_property(it));
            }
            match k {
                // All-null elements have no type either; float is as good a
                // carrier as any and keeps the column a list.
                Kind::Null | Kind::Float => Kind::ListFloat,
                Kind::Int => Kind::ListInt,
                Kind::Str => Kind::ListStr,
                _ => Kind::Json,
            }
        }
        PropertyValue::Map(_) => Kind::Json,
    }
}

fn kind_of(v: &Value) -> Kind {
    match v {
        Value::Null => Kind::Null,
        Value::Property(p) => kind_of_property(p),
        Value::List(items) if items.is_empty() => Kind::ListEmpty,
        Value::List(items) => {
            let mut k = Kind::Null;
            for it in items {
                k = k.unify(kind_of(it));
            }
            match k {
                Kind::Null | Kind::Float => Kind::ListFloat,
                Kind::Int => Kind::ListInt,
                Kind::Str => Kind::ListStr,
                _ => Kind::Json,
            }
        }
        _ => Kind::Json,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Property(PropertyValue::Integer(i)) => Some(*i as f64),
        Value::Property(PropertyValue::Float(f)) => Some(*f),
        _ => None,
    }
}

fn prop_as_f64(p: &PropertyValue) -> Option<f64> {
    match p {
        PropertyValue::Integer(i) => Some(*i as f64),
        PropertyValue::Float(f) => Some(*f),
        _ => None,
    }
}

/// The float sequence behind a value that a `List<Float64>` column holds, or
/// `None` for a null.
fn float_list(v: &Value) -> Option<Vec<Option<f64>>> {
    match v {
        Value::Null | Value::Property(PropertyValue::Null) => None,
        Value::Property(PropertyValue::Vector(xs)) => {
            Some(xs.iter().map(|x| Some(*x as f64)).collect())
        }
        Value::Property(PropertyValue::Array(items)) => {
            Some(items.iter().map(prop_as_f64).collect())
        }
        Value::List(items) => Some(items.iter().map(as_f64).collect()),
        _ => None,
    }
}

fn int_list(v: &Value) -> Option<Vec<Option<i64>>> {
    let one = |p: &PropertyValue| match p {
        PropertyValue::Integer(i) => Some(*i),
        _ => None,
    };
    match v {
        Value::Null | Value::Property(PropertyValue::Null) => None,
        Value::Property(PropertyValue::Array(items)) => Some(items.iter().map(one).collect()),
        Value::List(items) => Some(
            items
                .iter()
                .map(|it| match it {
                    Value::Property(p) => one(p),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    }
}

fn string_list(v: &Value) -> Option<Vec<Option<String>>> {
    let one = |p: &PropertyValue| match p {
        PropertyValue::String(s) => Some(s.clone()),
        PropertyValue::Null => None,
        other => Some(other.to_cypher_string()),
    };
    match v {
        Value::Null | Value::Property(PropertyValue::Null) => None,
        Value::Property(PropertyValue::Array(items)) => Some(items.iter().map(one).collect()),
        Value::List(items) => Some(
            items
                .iter()
                .map(|it| match it {
                    Value::Property(p) => one(p),
                    Value::Null => None,
                    other => Some(json_of(other).to_string()),
                })
                .collect(),
        ),
        _ => None,
    }
}

/// The text an `Utf8` column holds for a value whose kind is `Str`.
fn as_text(v: &Value) -> Option<String> {
    match v {
        Value::Null | Value::Property(PropertyValue::Null) => None,
        Value::Property(PropertyValue::String(s)) => Some(s.clone()),
        Value::Property(p) => Some(p.to_cypher_string()),
        _ => None,
    }
}

/// JSON for the fallback column, and for entities inside a list.
fn json_of(v: &Value) -> serde_json::Value {
    use serde_json::json;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Property(p) => p.to_json(),
        Value::List(items) => serde_json::Value::Array(items.iter().map(json_of).collect()),
        Value::Map(entries) => serde_json::Value::Object(
            entries.iter().map(|(k, v)| (k.clone(), json_of(v))).collect(),
        ),
        Value::Node(id, node) => json!({
            "id": id.as_u64(),
            "labels": node.labels.iter().map(|l| l.as_str().to_string()).collect::<Vec<_>>(),
            "properties": node.properties.iter()
                .map(|(k, v)| (k.clone(), v.to_json()))
                .collect::<serde_json::Map<_, _>>(),
        }),
        Value::NodeRef(id) => json!({ "id": id.as_u64() }),
        Value::Edge(id, edge) => json!({
            "id": id.as_u64(),
            "type": edge.edge_type.as_str(),
            "source": edge.source.as_u64(),
            "target": edge.target.as_u64(),
        }),
        Value::EdgeRef(id, src, tgt, ty) => json!({
            "id": id.as_u64(), "type": ty.as_str(),
            "source": src.as_u64(), "target": tgt.as_u64(),
        }),
        Value::Path { nodes, edges } => json!({
            "nodes": nodes.iter().map(|n| n.as_u64()).collect::<Vec<_>>(),
            "edges": edges.iter().map(|e| e.as_u64()).collect::<Vec<_>>(),
        }),
    }
}

fn build_column(kind: Kind, values: &[Option<&Value>]) -> (DataType, ArrayRef) {
    match kind {
        Kind::Null => (
            DataType::Null,
            Arc::new(NullArray::new(values.len())) as ArrayRef,
        ),
        Kind::Bool => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(Value::Property(PropertyValue::Boolean(x))) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            (DataType::Boolean, Arc::new(b.finish()) as ArrayRef)
        }
        Kind::Int => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    Some(Value::Property(PropertyValue::Integer(x))) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            (DataType::Int64, Arc::new(b.finish()) as ArrayRef)
        }
        Kind::Float => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|v| as_f64(v)) {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
            (DataType::Float64, Arc::new(b.finish()) as ArrayRef)
        }
        Kind::Str => {
            let mut b = StringBuilder::new();
            for v in values {
                match v.and_then(as_text) {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            (DataType::Utf8, Arc::new(b.finish()) as ArrayRef)
        }
        // A column whose lists are all empty has no element type to read off the
        // data; `Float64` carries it, and every list in it is empty anyway.
        Kind::ListFloat | Kind::ListEmpty => {
            let mut b = ListBuilder::new(Float64Builder::new());
            for v in values {
                match v.and_then(float_list) {
                    Some(items) => {
                        for x in items {
                            match x {
                                Some(x) => b.values().append_value(x),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            let arr = b.finish();
            let dt = arrow::array::Array::data_type(&arr).clone();
            (dt, Arc::new(arr) as ArrayRef)
        }
        Kind::ListInt => {
            let mut b = ListBuilder::new(Int64Builder::new());
            for v in values {
                match v.and_then(int_list) {
                    Some(items) => {
                        for x in items {
                            match x {
                                Some(x) => b.values().append_value(x),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            let arr = b.finish();
            let dt = arrow::array::Array::data_type(&arr).clone();
            (dt, Arc::new(arr) as ArrayRef)
        }
        Kind::ListStr => {
            let mut b = ListBuilder::new(StringBuilder::new());
            for v in values {
                match v.and_then(string_list) {
                    Some(items) => {
                        for s in items {
                            match s {
                                Some(s) => b.values().append_value(s),
                                None => b.values().append_null(),
                            }
                        }
                        b.append(true);
                    }
                    None => b.append(false),
                }
            }
            let arr = b.finish();
            let dt = arrow::array::Array::data_type(&arr).clone();
            (dt, Arc::new(arr) as ArrayRef)
        }
        Kind::Json => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    None | Some(Value::Null) | Some(Value::Property(PropertyValue::Null)) => {
                        b.append_null()
                    }
                    Some(v) => b.append_value(json_of(v).to_string()),
                }
            }
            (DataType::Utf8, Arc::new(b.finish()) as ArrayRef)
        }
    }
}

/// A query result as an Arrow record batch.
///
/// Column order is the result's own; a column the records never bound is all
/// null, which is what a projection of a missing property means in Cypher.
pub fn to_arrow(batch: &RecordBatch) -> Result<ArrowBatch, ExportError> {
    let mut fields = Vec::with_capacity(batch.columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.columns.len());

    for name in &batch.columns {
        let values: Vec<Option<&Value>> =
            batch.records.iter().map(|r| r.get(name)).collect();
        let kind = values
            .iter()
            .map(|v| v.map(kind_of).unwrap_or(Kind::Null))
            .fold(Kind::Null, Kind::unify);
        let (dt, arr) = build_column(kind, &values);
        // Every column is nullable: a Cypher projection can always be null,
        // and a non-nullable Arrow column that later meets one is a panic
        // rather than a null.
        fields.push(Field::new(name, dt, true));
        arrays.push(arr);
    }

    let schema = Arc::new(Schema::new(fields));
    // A result with columns but no rows is a real answer and must survive as an
    // empty batch with the schema intact, which `try_new` alone cannot express.
    if batch.records.is_empty() {
        return Ok(ArrowBatch::new_empty(schema));
    }
    ArrowBatch::try_new(schema, arrays).map_err(|e| ExportError::Arrow(e.to_string()))
}

/// The Arrow IPC *stream* format — what a client reads incrementally.
pub fn to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ExportError> {
    let arrow_batch = to_arrow(batch)?;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &arrow_batch.schema())
            .map_err(|e| ExportError::Arrow(e.to_string()))?;
        writer
            .write(&arrow_batch)
            .map_err(|e| ExportError::Arrow(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| ExportError::Arrow(e.to_string()))?;
    }
    Ok(buf)
}

/// The same batch as a Parquet file, Snappy-compressed.
pub fn to_parquet(batch: &RecordBatch) -> Result<Vec<u8>, ExportError> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let arrow_batch = to_arrow(batch)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, arrow_batch.schema(), Some(props))
            .map_err(|e| ExportError::Parquet(e.to_string()))?;
        writer
            .write(&arrow_batch)
            .map_err(|e| ExportError::Parquet(e.to_string()))?;
        writer
            .close()
            .map_err(|e| ExportError::Parquet(e.to_string()))?;
    }
    Ok(buf)
}
