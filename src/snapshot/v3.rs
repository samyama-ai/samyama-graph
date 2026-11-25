//! `.sgsnap` v3: the graph as Parquet, one file per label and per edge type
//! (#1099).
//!
//! ## Layout
//!
//! A v3 snapshot is **one file**: a POSIX tar archive (ustar, with the GNU
//! base-256 size extension for members past 8 GiB) holding
//!
//! ```text
//! manifest.json                       header, index catalog, hierarchies, file list
//! nodes/00000-Person.parquet          every node whose first label (sorted) is Person
//! nodes/00001-_unlabelled_.parquet    nodes with no label, if there are any
//! edges/00000-KNOWS.parquet           every relationship of type KNOWS
//! ...
//! ```
//!
//! One file rather than a directory, because every path that moves a
//! snapshot today -- the HTTP upload, `gh release upload`, an S3 object,
//! encryption at rest -- moves one stream. `tar xf` gives the directory back,
//! and DuckDB or Polars then read each member as an ordinary Parquet file.
//!
//! The manifest comes first, so reading the header ([`super::peek_header`])
//! costs one small member and never touches the data.
//!
//! ## Columns
//!
//! The store keeps properties in typed columns (`ColumnStore`), so each file is
//! that layout written out rather than re-encoded: one column per property key
//! the file's rows carry, typed when every value of that key in the file has
//! the same simple type.
//!
//! | `PropertyValue` | Parquet / Arrow |
//! |---|---|
//! | `String` | `Utf8` |
//! | `Integer` | `Int64` |
//! | `Float` | `Float64` (NaN and the infinities included) |
//! | `Boolean` | `Boolean` |
//! | `Date` | `Date32` |
//! | `DateTime` | `Timestamp(ms, UTC)` |
//! | `LocalTime` | `Time64(ns)` |
//! | `Vector` | `List<Float32>` |
//! | anything else, or a key whose values differ in type within one file | `Utf8` holding the `.sgsnap` JSON codec ([`super::codec`]) |
//!
//! An absent property is a Parquet null. Every column carries Arrow field
//! metadata -- `samyama.key` (the property name, which the column name can
//! differ from only to avoid a system column) and `samyama.type` -- and the
//! reader goes by the metadata, never by the column name.
//!
//! System columns: nodes have `_id`, `_labels` (all labels, sorted),
//! `_created_at`, `_updated_at`; edges have `_id`, `_src`, `_tgt`. Each edge
//! file holds the relationships that had properties in the edge arena first,
//! in id order (`full_rows` in the manifest), then the bulk-loaded ones.
//!
//! ## Reading
//!
//! Import does not have a second implementation. [`LineReader`] presents a v3
//! file as the JSON-lines stream a v2 file decompresses to -- header, catalog,
//! hierarchies, nodes in id order, then edges -- and the one importer reads
//! that. Dedup, merge policies, quotas, rollback and the index catalog
//! therefore behave identically for both versions, which is what spec 02's
//! IMPORT check asks of every path.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int64Builder, ListBuilder, StringBuilder, Time64NanosecondBuilder, TimestampMillisecondBuilder,
    UInt64Builder,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, Float32Type, Float64Type, Int64Type, Schema, SchemaRef,
    Time64NanosecondType, TimeUnit, TimestampMillisecondType, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use super::codec::property_to_json;
use super::format::{
    ExportStats, SnapshotEdge, SnapshotHeader, SnapshotHierarchyIndex, SnapshotIndexCatalog,
    SnapshotNode,
};
use crate::graph::node::Node;
use crate::graph::property::PropertyValue;
use crate::graph::store::GraphStore;

/// The format version a Parquet container declares.
pub const VERSION: u32 = 3;

/// The archive member that comes first and carries everything but the rows.
pub const MANIFEST_NAME: &str = "manifest.json";

/// `layout` in the manifest: what the members are, so a later layout can be
/// told apart from this one without guessing from file names.
pub const LAYOUT: &str = "samyama.sgsnap.v3/parquet";

/// Rows per Arrow batch while writing. A Parquet row group is assembled from
/// these up to the writer's own limit.
const WRITE_BATCH_ROWS: usize = 8 * 1024;

/// Rows per Arrow batch while reading.
const READ_BATCH_ROWS: usize = 8 * 1024;

/// The largest manifest a reader accepts, the same bound a v2 line has.
const MAX_MANIFEST_BYTES: u64 = super::MAX_LINE_BYTES as u64;

// ───────────────────────────────────────────────────────────────── manifest

/// `manifest.json`: everything a v2 file says before its first node line.
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    /// Always [`LAYOUT`].
    pub layout: String,
    /// The same header a v2 file has on line 0, with `version: 3`.
    pub header: SnapshotHeader,
    /// The index catalog (#1506). Always written; `None` only for a manifest
    /// from another writer that did not say, which import treats exactly as a
    /// v2 file without a catalog line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_catalog: Option<SnapshotIndexCatalog>,
    /// Hierarchy-index declarations (ADR-035).
    #[serde(default)]
    pub hierarchies: Vec<SnapshotHierarchyIndex>,
    /// Every data member, in archive order.
    #[serde(default)]
    pub files: Vec<ManifestFile>,
}

/// Whether a member holds nodes or edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileKind {
    Nodes,
    Edges,
}

/// One Parquet member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    /// The member's path inside the archive.
    pub path: String,
    pub kind: FileKind,
    /// Nodes: the label the file is keyed on -- the first of the node's labels
    /// in sorted order. Absent for the unlabelled nodes' file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Edges: the relationship type every row has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    /// Rows in the file. A reader that finds a different number refuses it.
    pub rows: u64,
    /// Edges: how many leading rows came from the edge arena, in id order. The
    /// rest are bulk-loaded (stub) relationships.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub full_rows: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

// ───────────────────────────────────────────────────────────────── tar

/// The minimal ustar subset the container needs: regular files, written and
/// read in order. Readable by any `tar`.
mod tar {
    use std::io::{self, Read, Write};

    pub const BLOCK: usize = 512;
    /// The largest size the 11-digit octal field holds.
    const OCTAL_MAX: u64 = 0o77_777_777_777;

    fn invalid(msg: impl Into<String>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, msg.into())
    }

    pub fn header(name: &str, size: u64) -> io::Result<[u8; BLOCK]> {
        let name_bytes = name.as_bytes();
        if name_bytes.is_empty() || name_bytes.len() > 100 {
            return Err(invalid(format!(
                "archive member name {name:?} is not 1-100 bytes"
            )));
        }
        let mut h = [0u8; BLOCK];
        h[..name_bytes.len()].copy_from_slice(name_bytes);
        h[100..108].copy_from_slice(b"0000644\0");
        h[108..116].copy_from_slice(b"0000000\0");
        h[116..124].copy_from_slice(b"0000000\0");
        if size <= OCTAL_MAX {
            h[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
        } else {
            // GNU base-256: high bit of the first byte set, big-endian value.
            h[124] = 0x80;
            h[128..136].copy_from_slice(&size.to_be_bytes());
        }
        h[136..148].copy_from_slice(b"00000000000\0");
        h[148..156].copy_from_slice(b"        ");
        h[156] = b'0';
        h[257..263].copy_from_slice(b"ustar\0");
        h[263..265].copy_from_slice(b"00");
        let sum: u32 = h.iter().map(|&b| b as u32).sum();
        h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        Ok(h)
    }

    pub fn padding(size: u64) -> usize {
        (BLOCK - (size % BLOCK as u64) as usize) % BLOCK
    }

    /// Write one member whose data is `size` bytes read from `data`.
    pub fn write_member(
        out: &mut impl Write,
        name: &str,
        size: u64,
        data: &mut impl Read,
    ) -> io::Result<()> {
        out.write_all(&header(name, size)?)?;
        let copied = io::copy(&mut data.take(size), out)?;
        if copied != size {
            return Err(invalid(format!(
                "archive member {name} ended at {copied} of {size} bytes"
            )));
        }
        out.write_all(&[0u8; BLOCK][..padding(size)])
    }

    pub fn write_end(out: &mut impl Write) -> io::Result<()> {
        out.write_all(&[0u8; 2 * BLOCK])
    }

    /// Is this block the start of a ustar archive whose first member is `name`?
    pub fn starts_with_member(head: &[u8], name: &str) -> bool {
        head.len() >= 265
            && &head[257..262] == b"ustar"
            && head[..100].split(|&b| b == 0).next() == Some(name.as_bytes())
    }

    fn parse_size(field: &[u8]) -> io::Result<u64> {
        if field[0] & 0x80 != 0 {
            let mut v: u64 = (field[0] & 0x7f) as u64;
            for &b in &field[1..] {
                v = v
                    .checked_mul(256)
                    .ok_or_else(|| invalid("archive member size overflows"))?
                    + b as u64;
            }
            return Ok(v);
        }
        let text: String = field
            .iter()
            .take_while(|&&b| b != 0)
            .map(|&b| b as char)
            .collect();
        let text = text.trim();
        if text.is_empty() {
            return Ok(0);
        }
        u64::from_str_radix(text, 8)
            .map_err(|_| invalid(format!("bad archive member size {text:?}")))
    }

    /// Reads members in order.
    pub struct Reader<R> {
        inner: R,
    }

    impl<R: Read> Reader<R> {
        pub fn new(inner: R) -> Self {
            Self { inner }
        }

        /// The next member's name and size, or `None` at the end-of-archive
        /// marker. A stream that ends anywhere else is truncated, and is an
        /// error rather than a shorter archive.
        pub fn next_member(&mut self) -> io::Result<Option<(String, u64)>> {
            let mut h = [0u8; BLOCK];
            read_full(
                &mut self.inner,
                &mut h,
                "a truncated snapshot: the archive has no end marker",
            )?;
            if h.iter().all(|&b| b == 0) {
                let mut second = [0u8; BLOCK];
                read_full(
                    &mut self.inner,
                    &mut second,
                    "a truncated snapshot: the end marker is cut short",
                )?;
                if second.iter().any(|&b| b != 0) {
                    return Err(invalid(
                        "a damaged snapshot: data after an empty archive block",
                    ));
                }
                return Ok(None);
            }
            let stored: u32 = {
                let text: String = h[148..156]
                    .iter()
                    .take_while(|&&b| b != 0 && b != b' ')
                    .map(|&b| b as char)
                    .collect();
                u32::from_str_radix(text.trim(), 8)
                    .map_err(|_| invalid("a damaged snapshot: bad archive header checksum"))?
            };
            let mut sum: u32 = 0;
            for (i, &b) in h.iter().enumerate() {
                sum += if (148..156).contains(&i) {
                    b' ' as u32
                } else {
                    b as u32
                };
            }
            if sum != stored {
                return Err(invalid(
                    "a damaged snapshot: archive header checksum mismatch",
                ));
            }
            if h[156] != b'0' && h[156] != 0 {
                return Err(invalid(format!(
                    "a v3 snapshot holds only regular files; found member type {:?}",
                    h[156] as char
                )));
            }
            let name_part = |r: &[u8]| -> String {
                String::from_utf8_lossy(r.split(|&b| b == 0).next().unwrap_or(&[])).into_owned()
            };
            let mut name = name_part(&h[..100]);
            let prefix = name_part(&h[345..500]);
            if &h[257..262] == b"ustar" && !prefix.is_empty() {
                name = format!("{prefix}/{name}");
            }
            let size = parse_size(&h[124..136])?;
            Ok(Some((name, size)))
        }

        /// Copy the current member's `size` bytes to `out`, then skip its padding.
        pub fn copy_data(&mut self, size: u64, out: &mut impl Write) -> io::Result<()> {
            let copied = io::copy(&mut (&mut self.inner).take(size), out)?;
            if copied != size {
                return Err(invalid(
                    "a truncated snapshot: an archive member is cut short",
                ));
            }
            let mut pad = [0u8; BLOCK];
            let n = padding(size);
            read_full(
                &mut self.inner,
                &mut pad[..n],
                "a truncated snapshot: an archive member is cut short",
            )
        }
    }

    fn read_full(r: &mut impl Read, buf: &mut [u8], what: &str) -> io::Result<()> {
        match r.read_exact(buf) {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Err(invalid(what)),
            other => other,
        }
    }
}

/// Is `head` -- the first bytes of a file, at least 512 of them when there are
/// that many -- a v3 snapshot?
pub fn looks_like_v3(head: &[u8]) -> bool {
    tar::starts_with_member(head, MANIFEST_NAME)
}

// ───────────────────────────────────────────────────────────────── columns

/// How one property column is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColKind {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    LocalTime,
    Vector,
    /// The `.sgsnap` JSON codec, as text.
    Json,
}

impl ColKind {
    fn of(v: &PropertyValue) -> ColKind {
        match v {
            PropertyValue::String(_) => ColKind::String,
            PropertyValue::Integer(_) => ColKind::Integer,
            PropertyValue::Float(_) => ColKind::Float,
            PropertyValue::Boolean(_) => ColKind::Boolean,
            PropertyValue::Date(_) => ColKind::Date,
            PropertyValue::DateTime(_) => ColKind::DateTime,
            PropertyValue::LocalTime(_) => ColKind::LocalTime,
            PropertyValue::Vector(_) => ColKind::Vector,
            // A stored null is kept as the codec's `null`, so that a reader can
            // tell it from an absent key exactly as a v2 line can.
            _ => ColKind::Json,
        }
    }

    fn unify(self, other: ColKind) -> ColKind {
        if self == other {
            self
        } else {
            ColKind::Json
        }
    }

    fn name(self) -> &'static str {
        match self {
            ColKind::String => "String",
            ColKind::Integer => "Integer",
            ColKind::Float => "Float",
            ColKind::Boolean => "Boolean",
            ColKind::Date => "Date",
            ColKind::DateTime => "DateTime",
            ColKind::LocalTime => "LocalTime",
            ColKind::Vector => "Vector",
            ColKind::Json => "json",
        }
    }

    fn from_name(name: &str) -> Option<ColKind> {
        Some(match name {
            "String" => ColKind::String,
            "Integer" => ColKind::Integer,
            "Float" => ColKind::Float,
            "Boolean" => ColKind::Boolean,
            "Date" => ColKind::Date,
            "DateTime" => ColKind::DateTime,
            "LocalTime" => ColKind::LocalTime,
            "Vector" => ColKind::Vector,
            "json" => ColKind::Json,
            _ => return None,
        })
    }

    fn data_type(self) -> DataType {
        match self {
            ColKind::String | ColKind::Json => DataType::Utf8,
            ColKind::Integer => DataType::Int64,
            ColKind::Float => DataType::Float64,
            ColKind::Boolean => DataType::Boolean,
            ColKind::Date => DataType::Date32,
            ColKind::DateTime => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            ColKind::LocalTime => DataType::Time64(TimeUnit::Nanosecond),
            ColKind::Vector => {
                DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true)))
            }
        }
    }
}

/// Property key -> column kind, in first-seen order so the column order is
/// stable for a given graph.
#[derive(Default)]
struct Kinds {
    order: Vec<String>,
    kinds: HashMap<String, ColKind>,
}

impl Kinds {
    fn observe(&mut self, key: &str, value: &PropertyValue) {
        let k = ColKind::of(value);
        match self.kinds.get_mut(key) {
            Some(existing) => *existing = existing.unify(k),
            None => {
                self.order.push(key.to_string());
                self.kinds.insert(key.to_string(), k);
            }
        }
    }

    fn columns(&self) -> Vec<(String, ColKind)> {
        self.order
            .iter()
            .map(|k| (k.clone(), self.kinds[k]))
            .collect()
    }
}

enum ColBuilder {
    Str(StringBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Bool(BooleanBuilder),
    Date(Date32Builder),
    DateTime(TimestampMillisecondBuilder),
    LocalTime(Time64NanosecondBuilder),
    Vector(ListBuilder<Float32Builder>),
    Json(StringBuilder),
}

impl ColBuilder {
    fn new(kind: ColKind) -> Self {
        match kind {
            ColKind::String => ColBuilder::Str(StringBuilder::new()),
            ColKind::Integer => ColBuilder::Int(Int64Builder::new()),
            ColKind::Float => ColBuilder::Float(Float64Builder::new()),
            ColKind::Boolean => ColBuilder::Bool(BooleanBuilder::new()),
            ColKind::Date => ColBuilder::Date(Date32Builder::new()),
            ColKind::DateTime => {
                ColBuilder::DateTime(TimestampMillisecondBuilder::new().with_timezone("UTC"))
            }
            ColKind::LocalTime => ColBuilder::LocalTime(Time64NanosecondBuilder::new()),
            ColKind::Vector => ColBuilder::Vector(ListBuilder::new(Float32Builder::new())),
            ColKind::Json => ColBuilder::Json(StringBuilder::new()),
        }
    }

    /// Append one row. The kind was inferred from these same values, so a
    /// mismatch is a bug in this module and is reported rather than coerced.
    fn append(&mut self, v: Option<&PropertyValue>) -> Result<(), String> {
        let mismatch = || Err("v3 export: a property value does not match its column".to_string());
        match (self, v) {
            (ColBuilder::Str(b), None) => b.append_null(),
            (ColBuilder::Int(b), None) => b.append_null(),
            (ColBuilder::Float(b), None) => b.append_null(),
            (ColBuilder::Bool(b), None) => b.append_null(),
            (ColBuilder::Date(b), None) => b.append_null(),
            (ColBuilder::DateTime(b), None) => b.append_null(),
            (ColBuilder::LocalTime(b), None) => b.append_null(),
            (ColBuilder::Vector(b), None) => b.append_null(),
            (ColBuilder::Json(b), None) => b.append_null(),
            (ColBuilder::Str(b), Some(PropertyValue::String(s))) => b.append_value(s),
            (ColBuilder::Int(b), Some(PropertyValue::Integer(i))) => b.append_value(*i),
            (ColBuilder::Float(b), Some(PropertyValue::Float(f))) => b.append_value(*f),
            (ColBuilder::Bool(b), Some(PropertyValue::Boolean(x))) => b.append_value(*x),
            (ColBuilder::Date(b), Some(PropertyValue::Date(d))) => b.append_value(*d),
            (ColBuilder::DateTime(b), Some(PropertyValue::DateTime(ms))) => b.append_value(*ms),
            (ColBuilder::LocalTime(b), Some(PropertyValue::LocalTime(n))) => b.append_value(*n),
            (ColBuilder::Vector(b), Some(PropertyValue::Vector(v))) => {
                b.values().append_slice(v);
                b.append(true);
            }
            (ColBuilder::Json(b), Some(v)) => {
                let text =
                    serde_json::to_string(&property_to_json(v)).map_err(|e| e.to_string())?;
                b.append_value(text);
            }
            _ => return mismatch(),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Str(b) | ColBuilder::Json(b) => Arc::new(b.finish()),
            ColBuilder::Int(b) => Arc::new(b.finish()),
            ColBuilder::Float(b) => Arc::new(b.finish()),
            ColBuilder::Bool(b) => Arc::new(b.finish()),
            ColBuilder::Date(b) => Arc::new(b.finish()),
            ColBuilder::DateTime(b) => Arc::new(b.finish()),
            ColBuilder::LocalTime(b) => Arc::new(b.finish()),
            ColBuilder::Vector(b) => Arc::new(b.finish()),
        }
    }
}

const NODE_SYSTEM: [&str; 4] = ["_id", "_labels", "_created_at", "_updated_at"];
const EDGE_SYSTEM: [&str; 3] = ["_id", "_src", "_tgt"];

fn system_field(name: &str, role: &str, data_type: DataType, nullable: bool) -> Field {
    Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
        "samyama.column".to_string(),
        role.to_string(),
    )]))
}

/// The column name for property `key`: the key itself, unless that is a
/// system column's name or already taken, in which case a suffix makes it
/// unique. The key travels in the field metadata either way.
fn column_name(key: &str, taken: &mut std::collections::HashSet<String>) -> String {
    let mut name = key.to_string();
    let mut n = 1;
    while taken.contains(&name) {
        name = format!("{key}~{n}");
        n += 1;
    }
    taken.insert(name.clone());
    name
}

fn schema_for(kind: FileKind, props: &[(String, ColKind)]) -> SchemaRef {
    let mut fields = Vec::new();
    let system: &[&str] = match kind {
        FileKind::Nodes => {
            fields.push(system_field("_id", "id", DataType::UInt64, false));
            fields.push(system_field(
                "_labels",
                "labels",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                false,
            ));
            fields.push(system_field(
                "_created_at",
                "created_at",
                DataType::Int64,
                true,
            ));
            fields.push(system_field(
                "_updated_at",
                "updated_at",
                DataType::Int64,
                true,
            ));
            &NODE_SYSTEM
        }
        FileKind::Edges => {
            fields.push(system_field("_id", "id", DataType::UInt64, false));
            fields.push(system_field("_src", "src", DataType::UInt64, false));
            fields.push(system_field("_tgt", "tgt", DataType::UInt64, false));
            &EDGE_SYSTEM
        }
    };
    let mut taken: std::collections::HashSet<String> =
        system.iter().map(|s| s.to_string()).collect();
    for (key, k) in props {
        let name = column_name(key, &mut taken);
        fields.push(
            Field::new(name, k.data_type(), true).with_metadata(HashMap::from([
                ("samyama.key".to_string(), key.clone()),
                ("samyama.type".to_string(), k.name().to_string()),
            ])),
        );
    }
    Arc::new(Schema::new(fields))
}

// ───────────────────────────────────────────────────────────────── writing

/// One Parquet file being written to an anonymous temporary file.
struct TableWriter {
    kind: FileKind,
    schema: SchemaRef,
    prop_index: HashMap<String, usize>,
    ids: UInt64Builder,
    labels: ListBuilder<StringBuilder>,
    created: Int64Builder,
    updated: Int64Builder,
    src: UInt64Builder,
    tgt: UInt64Builder,
    props: Vec<ColBuilder>,
    pending: usize,
    rows: u64,
    writer: ArrowWriter<File>,
}

impl TableWriter {
    fn new(
        kind: FileKind,
        props: Vec<(String, ColKind)>,
        file_meta: (&str, &str),
    ) -> Result<Self, String> {
        let schema = schema_for(kind, &props);
        let wprops = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_key_value_metadata(Some(vec![
                KeyValue::new("samyama.sgsnap".to_string(), VERSION.to_string()),
                KeyValue::new(file_meta.0.to_string(), file_meta.1.to_string()),
            ]))
            .build();
        let file = tempfile::tempfile().map_err(|e| format!("v3 export: temporary file: {e}"))?;
        let writer =
            ArrowWriter::try_new(file, schema.clone(), Some(wprops)).map_err(|e| e.to_string())?;
        Ok(Self {
            kind,
            schema,
            prop_index: props
                .iter()
                .enumerate()
                .map(|(i, (k, _))| (k.clone(), i))
                .collect(),
            ids: UInt64Builder::new(),
            labels: ListBuilder::new(StringBuilder::new()),
            created: Int64Builder::new(),
            updated: Int64Builder::new(),
            src: UInt64Builder::new(),
            tgt: UInt64Builder::new(),
            props: props.iter().map(|(_, k)| ColBuilder::new(*k)).collect(),
            pending: 0,
            rows: 0,
            writer,
        })
    }

    fn append_props<'a>(
        &mut self,
        values: impl Iterator<Item = (&'a str, &'a PropertyValue)>,
    ) -> Result<(), String> {
        let mut row: Vec<Option<&PropertyValue>> = vec![None; self.props.len()];
        for (k, v) in values {
            let i = *self
                .prop_index
                .get(k)
                .ok_or_else(|| format!("v3 export: property {k:?} has no column"))?;
            row[i] = Some(v);
        }
        for (b, v) in self.props.iter_mut().zip(row) {
            b.append(v)?;
        }
        Ok(())
    }

    fn append_node(
        &mut self,
        node: &Node,
        labels: &[&str],
        props: &[(String, PropertyValue)],
    ) -> Result<(), String> {
        self.ids.append_value(node.id.as_u64());
        for l in labels {
            self.labels.values().append_value(l);
        }
        self.labels.append(true);
        // Zero is "not carried" in a v2 line too (#1124).
        self.created
            .append_option((node.created_at != 0).then_some(node.created_at));
        self.updated
            .append_option((node.updated_at != 0).then_some(node.updated_at));
        self.append_props(props.iter().map(|(k, v)| (k.as_str(), v)))?;
        self.row_done()
    }

    fn append_edge(
        &mut self,
        id: u64,
        src: u64,
        tgt: u64,
        props: Option<&crate::graph::property::PropertyMap>,
    ) -> Result<(), String> {
        self.ids.append_value(id);
        self.src.append_value(src);
        self.tgt.append_value(tgt);
        match props {
            Some(p) => self.append_props(p.iter().map(|(k, v)| (k.as_str(), v)))?,
            None => self.append_props(std::iter::empty())?,
        }
        self.row_done()
    }

    fn row_done(&mut self) -> Result<(), String> {
        self.pending += 1;
        self.rows += 1;
        if self.pending >= WRITE_BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.pending == 0 {
            return Ok(());
        }
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        cols.push(Arc::new(self.ids.finish()));
        match self.kind {
            FileKind::Nodes => {
                cols.push(Arc::new(self.labels.finish()));
                cols.push(Arc::new(self.created.finish()));
                cols.push(Arc::new(self.updated.finish()));
            }
            FileKind::Edges => {
                cols.push(Arc::new(self.src.finish()));
                cols.push(Arc::new(self.tgt.finish()));
            }
        }
        for b in &mut self.props {
            cols.push(b.finish());
        }
        let batch = RecordBatch::try_new(self.schema.clone(), cols).map_err(|e| e.to_string())?;
        self.writer.write(&batch).map_err(|e| e.to_string())?;
        self.pending = 0;
        Ok(())
    }

    /// The finished file, rewound, and its length.
    fn finish(mut self) -> Result<(File, u64, u64), String> {
        self.flush()?;
        let rows = self.rows;
        let mut file = self.writer.into_inner().map_err(|e| e.to_string())?;
        let len = file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
        file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
        Ok((file, len, rows))
    }
}

/// A node's properties as the v2 export resolves them: the row's properties,
/// then any column value the row does not have. The same rule, so the two
/// versions carry the same values.
fn node_props(store: &GraphStore, node: &Node) -> Vec<(String, PropertyValue)> {
    let mut out: Vec<(String, PropertyValue)> = node
        .properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let idx = node.id.as_u64() as usize;
    for key in store.node_columns.get_property_keys(idx) {
        if !node.properties.contains_key(&key) {
            let val = store.node_columns.get_property(idx, &key);
            if !val.is_null() {
                out.push((key, val));
            }
        }
    }
    out
}

/// A label or type as a file-name component: ASCII letters, digits, `_` and
/// `-`, at most 64 of them. The manifest carries the real name.
fn file_stem(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if s.is_empty() {
        "_".to_string()
    } else {
        s
    }
}

struct Spooled {
    meta: ManifestFile,
    file: File,
    len: u64,
}

/// A finished Parquet file: its contents, length and row count.
type Written = (File, u64, u64);

fn write_node_group(
    store: &GraphStore,
    label: Option<&str>,
    nodes: &[&Node],
) -> Result<Written, String> {
    let mut kinds = Kinds::default();
    for node in nodes {
        for (k, v) in node_props(store, node) {
            kinds.observe(&k, &v);
        }
    }
    let mut w = TableWriter::new(
        FileKind::Nodes,
        kinds.columns(),
        ("samyama.label", label.unwrap_or("")),
    )?;
    for node in nodes {
        let mut labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
        labels.sort_unstable();
        w.append_node(node, &labels, &node_props(store, node))?;
    }
    w.finish()
}

/// Write `store` as a v3 snapshot. See the module documentation for the layout.
pub fn export(
    store: &GraphStore,
    writer: impl Write,
) -> Result<ExportStats, Box<dyn std::error::Error>> {
    use rayon::prelude::*;

    let nodes = store.all_nodes();
    let full_edges = store.all_edges();
    let full_edge_ids = super::EdgeIdSet::from_ids(full_edges.iter().map(|e| e.id.as_u64()));

    // Nodes, grouped by their first label in sorted order. `all_nodes` is in id
    // order, and grouping keeps that order within each group, which is what
    // lets the reader merge the groups back into one id-ordered stream.
    let mut groups: BTreeMap<Option<&str>, Vec<&Node>> = BTreeMap::new();
    let mut label_set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for node in &nodes {
        let first = node.labels.iter().map(|l| l.as_str()).min();
        for l in &node.labels {
            if !label_set.contains(l.as_str()) {
                label_set.insert(l.as_str().to_string());
            }
        }
        groups.entry(first).or_default().push(node);
    }
    // Unlabelled nodes (`None`) sort first in the map; they are written last,
    // after every labelled file, so file numbering follows the label order.
    let mut group_list: Vec<(Option<&str>, Vec<&Node>)> = groups.into_iter().collect();
    let unlabelled_first = group_list.first().is_some_and(|(l, _)| l.is_none());
    group_list.rotate_left(usize::from(unlabelled_first));

    let written: Vec<Written> = group_list
        .par_iter()
        .map(|(label, members)| write_node_group(store, *label, members))
        .collect::<Result<_, String>>()?;

    let mut spooled: Vec<Spooled> = Vec::new();
    for (i, ((label, _), (file, len, rows))) in group_list.iter().zip(written).enumerate() {
        let stem = label
            .map(file_stem)
            .unwrap_or_else(|| "_unlabelled_".to_string());
        spooled.push(Spooled {
            meta: ManifestFile {
                path: format!("nodes/{i:05}-{stem}.parquet"),
                kind: FileKind::Nodes,
                label: label.map(str::to_string),
                edge_type: None,
                rows,
                full_rows: 0,
            },
            file,
            len,
        });
    }

    // Edges: one writer per type. The arena's edges first, in id order, with
    // their properties; then the adjacency-only (stub) edges, in the order the
    // v2 export writes them.
    let mut edge_kinds: BTreeMap<String, Kinds> = BTreeMap::new();
    for e in &full_edges {
        let kinds = edge_kinds
            .entry(e.edge_type.as_str().to_string())
            .or_default();
        for (k, v) in &e.properties {
            kinds.observe(k, v);
        }
    }
    let mut writers: BTreeMap<String, TableWriter> = BTreeMap::new();
    fn writer_for<'w>(
        writers: &'w mut BTreeMap<String, TableWriter>,
        kinds: &BTreeMap<String, Kinds>,
        edge_type: &str,
    ) -> Result<&'w mut TableWriter, String> {
        if !writers.contains_key(edge_type) {
            let cols = kinds.get(edge_type).map(Kinds::columns).unwrap_or_default();
            let w = TableWriter::new(FileKind::Edges, cols, ("samyama.edge_type", edge_type))?;
            writers.insert(edge_type.to_string(), w);
        }
        Ok(writers.get_mut(edge_type).expect("inserted above"))
    }
    let mut sorted_full: Vec<&crate::graph::edge::Edge> = full_edges.iter().collect();
    sorted_full.sort_by_key(|e| e.id.as_u64());
    for e in sorted_full {
        writer_for(&mut writers, &edge_kinds, e.edge_type.as_str())?.append_edge(
            e.id.as_u64(),
            e.source.as_u64(),
            e.target.as_u64(),
            Some(&e.properties),
        )?;
    }
    let full_rows: HashMap<String, u64> =
        writers.iter().map(|(t, w)| (t.clone(), w.rows)).collect();
    for node in &nodes {
        let src = node.id.as_u64();
        let frozen = store.frozen_outgoing_neighbors(src as usize);
        let buffered = store.get_outgoing_neighbor_slice(node.id);
        for &(tgt, eid) in frozen.iter().chain(buffered.iter()) {
            if full_edge_ids.contains(eid.as_u64()) {
                continue;
            }
            // No type means deleted after compaction; not in the graph (#1096).
            let Some(et) = store.get_edge_type(eid) else {
                continue;
            };
            writer_for(&mut writers, &edge_kinds, et.as_str())?.append_edge(
                eid.as_u64(),
                src,
                tgt.as_u64(),
                None,
            )?;
        }
    }
    let mut edge_count = 0u64;
    let mut edge_types = Vec::new();
    for (i, (edge_type, w)) in writers.into_iter().enumerate() {
        let (file, len, rows) = w.finish()?;
        edge_count += rows;
        spooled.push(Spooled {
            meta: ManifestFile {
                path: format!("edges/{i:05}-{}.parquet", file_stem(&edge_type)),
                kind: FileKind::Edges,
                label: None,
                full_rows: full_rows.get(&edge_type).copied().unwrap_or(0),
                edge_type: Some(edge_type.clone()),
                rows,
            },
            file,
            len,
        });
        edge_types.push(edge_type);
    }

    let labels: Vec<String> = label_set.into_iter().collect();
    let (dropped, sources) = super::export_losses(store, edge_count);
    let header = SnapshotHeader {
        format: "sgsnap".to_string(),
        version: VERSION,
        tenant: "default".to_string(),
        node_count: nodes.len() as u64,
        edge_count,
        labels: labels.clone(),
        edge_types: edge_types.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        samyama_version: crate::VERSION.to_string(),
        dropped: dropped.clone(),
        queries: None,
        read_only: false,
        results: None,
        sources,
        structures: store.structure_registry.list(),
    };
    let manifest = Manifest {
        layout: LAYOUT.to_string(),
        header,
        index_catalog: Some(SnapshotIndexCatalog {
            t: "i".to_string(),
            definitions: store.index_catalog().definitions,
        }),
        hierarchies: super::hierarchy_records(store),
        files: spooled.iter().map(|s| s.meta.clone()).collect(),
    };
    let manifest_json = serde_json::to_vec(&manifest)?;

    let mut out = super::CountingWriter::new(writer);
    {
        let mut buffered = io::BufWriter::with_capacity(1 << 20, &mut out);
        tar::write_member(
            &mut buffered,
            MANIFEST_NAME,
            manifest_json.len() as u64,
            &mut manifest_json.as_slice(),
        )?;
        for s in &mut spooled {
            tar::write_member(&mut buffered, &s.meta.path, s.len, &mut s.file)?;
        }
        tar::write_end(&mut buffered)?;
        // Flushed here, not on drop, so a failed last write is an error.
        buffered.flush()?;
    }
    out.flush()?;

    Ok(ExportStats {
        node_count: nodes.len() as u64,
        edge_count,
        labels,
        edge_types,
        bytes_written: out.count,
        dropped,
    })
}

// ───────────────────────────────────────────────────────────────── reading

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Read the manifest member at the head of `archive`, returning it as JSON.
fn read_manifest<R: Read>(archive: &mut tar::Reader<R>) -> io::Result<serde_json::Value> {
    let (name, size) = archive
        .next_member()?
        .ok_or_else(|| invalid("empty snapshot file: missing header"))?;
    if name != MANIFEST_NAME {
        return Err(invalid(format!(
            "a v3 snapshot starts with {MANIFEST_NAME}, not {name}"
        )));
    }
    if size > MAX_MANIFEST_BYTES {
        return Err(invalid(format!(
            "snapshot manifest longer than {MAX_MANIFEST_BYTES} bytes"
        )));
    }
    let mut bytes = Vec::with_capacity(size as usize);
    archive.copy_data(size, &mut bytes)?;
    serde_json::from_slice(&bytes).map_err(|e| invalid(format!("snapshot manifest: {e}")))
}

/// Where each column of a file is, by role.
struct Decoder {
    id: usize,
    labels: Option<usize>,
    created: Option<usize>,
    updated: Option<usize>,
    src: Option<usize>,
    tgt: Option<usize>,
    props: Vec<(usize, String, ColKind)>,
}

impl Decoder {
    fn new(schema: &Schema, kind: FileKind, path: &str) -> io::Result<Self> {
        let mut d = Decoder {
            id: usize::MAX,
            labels: None,
            created: None,
            updated: None,
            src: None,
            tgt: None,
            props: Vec::new(),
        };
        for (i, f) in schema.fields().iter().enumerate() {
            let meta = f.metadata();
            if let Some(role) = meta.get("samyama.column") {
                match role.as_str() {
                    "id" => d.id = i,
                    "labels" => d.labels = Some(i),
                    "created_at" => d.created = Some(i),
                    "updated_at" => d.updated = Some(i),
                    "src" => d.src = Some(i),
                    "tgt" => d.tgt = Some(i),
                    other => {
                        return Err(invalid(format!("{path}: unknown system column {other:?}")))
                    }
                }
            } else if let Some(key) = meta.get("samyama.key") {
                let kind = meta
                    .get("samyama.type")
                    .and_then(|t| ColKind::from_name(t))
                    .ok_or_else(|| {
                        invalid(format!(
                            "{path}: column {} has no known samyama.type",
                            f.name()
                        ))
                    })?;
                if f.data_type() != &kind.data_type() {
                    return Err(invalid(format!(
                        "{path}: column {} is {} but declares {}",
                        f.name(),
                        f.data_type(),
                        kind.name()
                    )));
                }
                d.props.push((i, key.clone(), kind));
            }
            // A column with neither is not ours; a reader that added one to
            // the file has not changed the graph, so it is ignored.
        }
        let complete = d.id != usize::MAX
            && match kind {
                FileKind::Nodes => d.labels.is_some(),
                FileKind::Edges => d.src.is_some() && d.tgt.is_some(),
            };
        if !complete {
            return Err(invalid(format!("{path}: missing a system column")));
        }
        Ok(d)
    }

    fn u64_at(batch: &RecordBatch, col: usize, row: usize) -> u64 {
        batch.column(col).as_primitive::<UInt64Type>().value(row)
    }

    fn opt_i64_at(batch: &RecordBatch, col: Option<usize>, row: usize) -> i64 {
        match col {
            Some(c) if !batch.column(c).is_null(row) => {
                batch.column(c).as_primitive::<Int64Type>().value(row)
            }
            _ => 0,
        }
    }

    fn props(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> io::Result<HashMap<String, serde_json::Value>> {
        let mut out = HashMap::with_capacity(self.props.len());
        for (col, key, kind) in &self.props {
            let a = batch.column(*col);
            if a.is_null(row) {
                continue;
            }
            let value = match kind {
                ColKind::Json => {
                    let text = a.as_string::<i32>().value(row);
                    serde_json::from_str(text)
                        .map_err(|e| invalid(format!("property {key:?}: {e}")))?
                }
                _ => property_to_json(&match kind {
                    ColKind::String => {
                        PropertyValue::String(a.as_string::<i32>().value(row).to_string())
                    }
                    ColKind::Integer => {
                        PropertyValue::Integer(a.as_primitive::<Int64Type>().value(row))
                    }
                    ColKind::Float => {
                        PropertyValue::Float(a.as_primitive::<Float64Type>().value(row))
                    }
                    ColKind::Boolean => PropertyValue::Boolean(a.as_boolean().value(row)),
                    ColKind::Date => PropertyValue::Date(a.as_primitive::<Date32Type>().value(row)),
                    ColKind::DateTime => PropertyValue::DateTime(
                        a.as_primitive::<TimestampMillisecondType>().value(row),
                    ),
                    ColKind::LocalTime => PropertyValue::LocalTime(
                        a.as_primitive::<Time64NanosecondType>().value(row),
                    ),
                    ColKind::Vector => {
                        let list = a.as_list::<i32>().value(row);
                        PropertyValue::Vector(
                            list.as_primitive::<Float32Type>()
                                .iter()
                                .map(|v| v.unwrap_or(f32::NAN))
                                .collect(),
                        )
                    }
                    ColKind::Json => unreachable!("handled above"),
                }),
            };
            out.insert(key.clone(), value);
        }
        Ok(out)
    }

    fn node_line(&self, batch: &RecordBatch, row: usize, out: &mut Vec<u8>) -> io::Result<()> {
        let labels_list = batch
            .column(self.labels.expect("checked in new"))
            .as_list::<i32>()
            .value(row);
        let labels = labels_list
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();
        let node = SnapshotNode {
            t: "n".to_string(),
            id: Self::u64_at(batch, self.id, row),
            labels,
            props: self.props(batch, row)?,
            created_at: Self::opt_i64_at(batch, self.created, row),
            updated_at: Self::opt_i64_at(batch, self.updated, row),
        };
        serde_json::to_writer(&mut *out, &node).map_err(io::Error::other)?;
        out.push(b'\n');
        Ok(())
    }

    fn edge_line(
        &self,
        batch: &RecordBatch,
        row: usize,
        edge_type: &str,
        out: &mut Vec<u8>,
    ) -> io::Result<()> {
        let edge = SnapshotEdge {
            t: "e".to_string(),
            id: Self::u64_at(batch, self.id, row),
            src: Self::u64_at(batch, self.src.expect("checked in new"), row),
            tgt: Self::u64_at(batch, self.tgt.expect("checked in new"), row),
            edge_type: edge_type.to_string(),
            props: self.props(batch, row)?,
        };
        serde_json::to_writer(&mut *out, &edge).map_err(io::Error::other)?;
        out.push(b'\n');
        Ok(())
    }
}

/// A position in one Parquet member.
struct Cursor {
    meta: ManifestFile,
    decoder: Decoder,
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
    /// Rows consumed so far.
    seen: u64,
}

impl Cursor {
    fn open(meta: ManifestFile, file: File) -> io::Result<Self> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| invalid(format!("{}: {e}", meta.path)))?;
        let decoder = Decoder::new(builder.schema(), meta.kind, &meta.path)?;
        let reader = builder
            .with_batch_size(READ_BATCH_ROWS)
            .build()
            .map_err(|e| invalid(format!("{}: {e}", meta.path)))?;
        let mut c = Cursor {
            meta,
            decoder,
            reader,
            batch: None,
            row: 0,
            seen: 0,
        };
        c.fill()?;
        Ok(c)
    }

    /// Make `batch`/`row` point at the next unread row, if there is one.
    fn fill(&mut self) -> io::Result<()> {
        loop {
            if let Some(b) = &self.batch {
                if self.row < b.num_rows() {
                    return Ok(());
                }
            }
            match self.reader.next() {
                Some(Ok(b)) => {
                    self.batch = Some(b);
                    self.row = 0;
                }
                Some(Err(e)) => return Err(invalid(format!("{}: {e}", self.meta.path))),
                None => {
                    self.batch = None;
                    if self.seen != self.meta.rows {
                        return Err(invalid(format!(
                            "{}: the manifest says {} rows, the file has {}",
                            self.meta.path, self.meta.rows, self.seen
                        )));
                    }
                    return Ok(());
                }
            }
        }
    }

    fn current_id(&self) -> Option<u64> {
        self.batch
            .as_ref()
            .map(|b| Decoder::u64_at(b, self.decoder.id, self.row))
    }

    /// Write the current row as a JSON line and move past it.
    fn emit(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        let batch = self
            .batch
            .as_ref()
            .expect("emit called on an exhausted cursor");
        match self.meta.kind {
            FileKind::Nodes => self.decoder.node_line(batch, self.row, out)?,
            FileKind::Edges => {
                let t = self.meta.edge_type.as_deref().unwrap_or_default();
                self.decoder.edge_line(batch, self.row, t, out)?
            }
        }
        self.row += 1;
        self.seen += 1;
        self.fill()
    }
}

enum Stage {
    /// The header line is in the buffer; the members have not been read.
    Header,
    /// Members spooled; nodes are merged by id.
    Nodes {
        cursors: Vec<Cursor>,
        heap: BinaryHeap<Reverse<(u64, usize)>>,
    },
    /// Arena edges, merged by id across types.
    FullEdges {
        cursors: Vec<Cursor>,
        heap: BinaryHeap<Reverse<(u64, usize)>>,
    },
    /// Bulk-loaded edges, file by file.
    StubEdges {
        cursors: std::collections::VecDeque<Cursor>,
    },
    Done,
}

/// A v3 snapshot read as the JSON-lines stream a v2 snapshot decompresses to.
pub struct LineReader<R> {
    archive: Option<tar::Reader<R>>,
    manifest: serde_json::Value,
    buf: Vec<u8>,
    pos: usize,
    stage: Stage,
    /// The edge files, held while the nodes are read.
    pending_edges: Option<Vec<Cursor>>,
}

/// The size a refill aims for before handing bytes out.
const REFILL_BYTES: usize = 256 * 1024;

impl<R: Read> LineReader<R> {
    /// Read the manifest and stage the header line. The data members are not
    /// touched until the first read past the header.
    pub fn new(reader: R) -> io::Result<Self> {
        let mut archive = tar::Reader::new(reader);
        let manifest = read_manifest(&mut archive)?;
        if manifest.get("layout").and_then(|l| l.as_str()) != Some(LAYOUT) {
            return Err(invalid(format!(
                "not a v3 snapshot layout this build reads (expected {LAYOUT:?})"
            )));
        }
        let header = manifest
            .get("header")
            .filter(|h| h.is_object())
            .ok_or_else(|| invalid("snapshot manifest has no header"))?;
        let mut buf = serde_json::to_vec(header).map_err(io::Error::other)?;
        buf.push(b'\n');
        Ok(Self {
            archive: Some(archive),
            manifest,
            buf,
            pos: 0,
            stage: Stage::Header,
            pending_edges: None,
        })
    }

    /// Spool every member to a temporary file, check the archive is whole and
    /// matches the manifest, and stage the catalog and hierarchy lines.
    fn open_members(&mut self) -> io::Result<()> {
        let parsed: Manifest = serde_json::from_value(self.manifest.clone())
            .map_err(|e| invalid(format!("snapshot manifest: {e}")))?;
        let mut by_path: HashMap<String, ManifestFile> = parsed
            .files
            .iter()
            .map(|f| (f.path.clone(), f.clone()))
            .collect();
        let mut archive = self.archive.take().expect("members are opened once");
        let mut spooled: HashMap<String, File> = HashMap::new();
        while let Some((name, size)) = archive.next_member()? {
            if by_path.remove(&name).is_none() {
                return Err(invalid(format!(
                    "snapshot member {name} is not in the manifest"
                )));
            }
            let mut f = tempfile::tempfile()?;
            archive.copy_data(size, &mut f)?;
            f.seek(SeekFrom::Start(0))?;
            spooled.insert(name, f);
        }
        if let Some(missing) = by_path.keys().next() {
            return Err(invalid(format!(
                "a truncated snapshot: member {missing} is missing"
            )));
        }

        if let Some(catalog) = &parsed.index_catalog {
            serde_json::to_writer(&mut self.buf, catalog).map_err(io::Error::other)?;
            self.buf.push(b'\n');
        }
        for h in &parsed.hierarchies {
            serde_json::to_writer(&mut self.buf, h).map_err(io::Error::other)?;
            self.buf.push(b'\n');
        }

        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        for meta in parsed.files {
            let file = spooled.remove(&meta.path).expect("checked above");
            if meta.kind == FileKind::Edges && meta.edge_type.is_none() {
                return Err(invalid(format!(
                    "{}: an edge file with no edge_type",
                    meta.path
                )));
            }
            if meta.full_rows > meta.rows {
                return Err(invalid(format!("{}: full_rows exceeds rows", meta.path)));
            }
            let c = Cursor::open(meta, file)?;
            match c.meta.kind {
                FileKind::Nodes => nodes.push(c),
                FileKind::Edges => edges.push(c),
            }
        }
        let heap = nodes
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.current_id().map(|id| Reverse((id, i))))
            .collect();
        self.stage = Stage::Nodes {
            cursors: nodes,
            heap,
        };
        // Edges are staged when the nodes run out.
        self.pending_edges = Some(edges);
        Ok(())
    }
}

impl<R: Read> LineReader<R> {
    fn refill(&mut self) -> io::Result<()> {
        self.buf.clear();
        self.pos = 0;
        while self.buf.len() < REFILL_BYTES {
            match &mut self.stage {
                Stage::Header => {
                    self.open_members()?;
                }
                Stage::Nodes { cursors, heap } => match heap.pop() {
                    Some(Reverse((_, i))) => {
                        cursors[i].emit(&mut self.buf)?;
                        if let Some(id) = cursors[i].current_id() {
                            heap.push(Reverse((id, i)));
                        }
                    }
                    None => {
                        let edges = self.pending_edges.take().unwrap_or_default();
                        let heap = edges
                            .iter()
                            .enumerate()
                            .filter(|(_, c)| c.meta.full_rows > 0)
                            .filter_map(|(i, c)| c.current_id().map(|id| Reverse((id, i))))
                            .collect();
                        self.stage = Stage::FullEdges {
                            cursors: edges,
                            heap,
                        };
                    }
                },
                Stage::FullEdges { cursors, heap } => match heap.pop() {
                    Some(Reverse((_, i))) => {
                        let c = &mut cursors[i];
                        c.emit(&mut self.buf)?;
                        if c.seen < c.meta.full_rows {
                            if let Some(id) = c.current_id() {
                                heap.push(Reverse((id, i)));
                            }
                        }
                    }
                    None => {
                        let cursors = std::mem::take(cursors).into_iter().collect();
                        self.stage = Stage::StubEdges { cursors };
                    }
                },
                Stage::StubEdges { cursors } => match cursors.front_mut() {
                    Some(c) if c.batch.is_some() => c.emit(&mut self.buf)?,
                    Some(_) => {
                        cursors.pop_front();
                    }
                    None => self.stage = Stage::Done,
                },
                Stage::Done => break,
            }
        }
        Ok(())
    }
}

impl<R: Read> Read for LineReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.buf.len() {
            if matches!(self.stage, Stage::Done) {
                return Ok(0);
            }
            self.refill()?;
        }
        let n = out.len().min(self.buf.len() - self.pos);
        out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

// ───────────────────────────────────────────────────────────────── editing

/// [`super::edit_header`] for a v3 file: the manifest's `header` object is
/// edited, and every data member is copied through byte for byte.
pub fn edit_header(
    reader: impl Read,
    writer: impl Write,
    edit: impl FnOnce(
        &mut serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error>>,
) -> Result<SnapshotHeader, Box<dyn std::error::Error>> {
    let mut archive = tar::Reader::new(reader);
    let mut manifest = read_manifest(&mut archive)?;
    let obj = manifest
        .get_mut("header")
        .and_then(|h| h.as_object_mut())
        .ok_or("snapshot manifest has no header")?;
    let format = obj.get("format").cloned().unwrap_or_default();
    if format != "sgsnap" {
        return Err(format!("invalid snapshot format: expected \"sgsnap\", got {format}").into());
    }
    edit(obj)?;
    let header: SnapshotHeader = serde_json::from_value(serde_json::Value::Object(obj.clone()))?;
    let manifest_json = serde_json::to_vec(&manifest)?;

    let mut out = io::BufWriter::new(writer);
    tar::write_member(
        &mut out,
        MANIFEST_NAME,
        manifest_json.len() as u64,
        &mut manifest_json.as_slice(),
    )?;
    // Each member is re-emitted from its own header, so an archive cut short
    // fails here instead of becoming a well-formed shorter one.
    while let Some((name, size)) = archive.next_member()? {
        out.write_all(&tar::header(&name, size)?)?;
        archive.copy_data(size, &mut out)?;
        out.write_all(&[0u8; tar::BLOCK][..tar::padding(size)])?;
    }
    tar::write_end(&mut out)?;
    out.flush()?;
    Ok(header)
}
