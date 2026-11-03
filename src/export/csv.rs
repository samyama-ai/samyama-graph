//! A query result as CSV — the format BI tools actually consume (INT-08).
//!
//! Tableau, Power BI and Superset all take CSV over HTTP. Parquet and Arrow
//! are better formats and none of the three reads them from a URL without a
//! connector, so CSV is the path that exists today.
//!
//! RFC 4180: comma-separated, `\r\n` line endings, a field quoted when it
//! holds a comma, a quote or a newline, and an embedded quote doubled.
//!
//! # Two ambiguities CSV cannot resolve, and what is done about them
//!
//! **Null and the empty string are the same field.** CSV has no null. A
//! Cypher `null` and a `""` both come out as nothing between two commas, and
//! no quoting convention distinguishes them — a quoted `""` is how some
//! writers mark the empty string, and how others mark null. Rather than pick
//! one and be wrong for half the readers, both are written as an empty field
//! and the count of nulls is reported, so a user importing the file knows how
//! many blanks were nulls.
//!
//! **A leading `=`, `+`, `-` or `@` is a formula to a spreadsheet.** Open the
//! file in Excel or Sheets and the cell is evaluated, which is a real attack
//! when the value came from somewhere untrusted. Nothing is prefixed or
//! stripped here: this is a **data** export, and silently altering a value so
//! it survives one particular reader means the number that comes back is not
//! the number that went in. The count is reported instead, and
//! `docs/BI-CONNECTIVITY.md` says to import as text.

use crate::query::executor::record::Value;
use crate::query::RecordBatch;

/// What a CSV export could not express.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CsvReport {
    pub rows_written: usize,
    pub columns: usize,
    /// Nulls written as an empty field, indistinguishable from `""`.
    pub nulls_written_as_empty: usize,
    /// Lists, maps, nodes and edges written as JSON inside one field.
    pub values_written_as_json: usize,
    /// Fields a spreadsheet would evaluate as a formula. Not altered — see
    /// the module docs.
    pub fields_a_spreadsheet_reads_as_a_formula: usize,
}

/// One cell's text, before quoting.
fn cell(v: Option<&Value>, report: &mut CsvReport) -> String {
    let Some(v) = v else {
        report.nulls_written_as_empty += 1;
        return String::new();
    };
    match v {
        Value::Null => {
            report.nulls_written_as_empty += 1;
            String::new()
        }
        Value::Property(p) => match p {
            crate::graph::PropertyValue::Null => {
                report.nulls_written_as_empty += 1;
                String::new()
            }
            crate::graph::PropertyValue::String(s) => s.clone(),
            crate::graph::PropertyValue::Array(_)
            | crate::graph::PropertyValue::Map(_)
            | crate::graph::PropertyValue::Vector(_) => {
                report.values_written_as_json += 1;
                p.to_json().to_string()
            }
            // Numbers, booleans and temporals all render as themselves. The
            // temporal rendering is the engine's own, so the text in the file
            // is the text a query result shows — a second formatter here would
            // be a second definition of what these values are.
            scalar => scalar.to_string(),
        },
        other => {
            report.values_written_as_json += 1;
            // A node or a relationship has no scalar form. JSON in one field
            // keeps it; a BI tool will treat it as text, which is honest —
            // a table is not a graph, and this is the seam where that shows.
            super::value_as_json(other).to_string()
        }
    }
}

/// RFC 4180 quoting.
fn quote(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Whether a spreadsheet would evaluate this cell rather than display it.
fn reads_as_formula(s: &str) -> bool {
    matches!(s.as_bytes().first(), Some(b'=' | b'+' | b'-' | b'@'))
        // A leading tab or CR makes the *next* character the first one a
        // spreadsheet sees, which is how the naive "does it start with =?"
        // check is bypassed.
        || matches!(s.as_bytes().first(), Some(b'\t' | b'\r'))
}

/// A query result as CSV text, with a header row.
///
/// Column order is the result's own, so it matches what the query projected
/// and what every other export of the same result produces.
pub fn to_csv(batch: &RecordBatch) -> (String, CsvReport) {
    let mut report = CsvReport {
        columns: batch.columns.len(),
        ..Default::default()
    };

    let mut out = String::new();
    let header: Vec<String> = batch.columns.iter().map(|c| quote(c)).collect();
    out.push_str(&header.join(","));
    out.push_str("\r\n");

    for record in &batch.records {
        let mut row = Vec::with_capacity(batch.columns.len());
        for name in &batch.columns {
            let text = cell(record.get(name), &mut report);
            if reads_as_formula(&text) {
                report.fields_a_spreadsheet_reads_as_a_formula += 1;
            }
            row.push(quote(&text));
        }
        out.push_str(&row.join(","));
        out.push_str("\r\n");
        report.rows_written += 1;
    }

    (out, report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting_is_rfc_4180() {
        assert_eq!(quote("plain"), "plain");
        assert_eq!(quote("a,b"), "\"a,b\"");
        assert_eq!(quote("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(quote("line\nbreak"), "\"line\nbreak\"");
        // A lone quote with no comma still needs quoting: unquoted, the
        // reader sees a field that starts mid-quote.
        assert_eq!(quote("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn formula_detection_covers_the_leading_whitespace_bypass() {
        assert!(reads_as_formula("=1+1"));
        assert!(reads_as_formula("+1"));
        assert!(reads_as_formula("-1"));
        assert!(reads_as_formula("@SUM(A1)"));
        // The bypass: a leading tab, so the first character a spreadsheet
        // acts on is the `=` behind it.
        assert!(reads_as_formula("\t=1+1"));
        assert!(!reads_as_formula("1+1"));
        assert!(!reads_as_formula("Alice"));
        assert!(!reads_as_formula(""));
        assert!(reads_as_formula("\r=1"));
    }

    use crate::graph::{NodeId, PropertyValue as P};
    use crate::query::executor::record::Record;
    use std::collections::HashMap;

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

    #[test]
    fn to_csv_writes_header_rows_and_counts_what_it_could_not_express() {
        let mut m = HashMap::new();
        m.insert("k".to_string(), P::Integer(1));
        let b = batch(
            &["a", "b,c"],
            vec![
                vec![("a", Value::Property(P::String("x, y".into()))), ("b,c", Value::Property(P::Integer(3)))],
                vec![("a", Value::Null), ("b,c", Value::Property(P::Null))],
                vec![("a", Value::Property(P::Array(vec![P::Integer(1)])))],
                vec![("a", Value::Property(P::Map(m))), ("b,c", Value::Property(P::Vector(vec![0.5])))],
                vec![("a", Value::NodeRef(NodeId::new(9))), ("b,c", Value::Property(P::String("=cmd".into())))],
                vec![("a", Value::Property(P::Boolean(true))), ("b,c", Value::Property(P::Float(1.5)))],
            ],
        );
        let (text, rep) = to_csv(&b);
        let lines: Vec<&str> = text.split("\r\n").collect();
        assert_eq!(lines[0], "a,\"b,c\"");
        assert_eq!(lines[1], "\"x, y\",3");
        assert_eq!(lines[2], ",");
        assert_eq!(lines[3], "[1],", "unbound column is empty");
        assert_eq!(lines[4], "\"{\"\"k\"\":1}\",[0.5]");
        assert_eq!(lines[5], "\"{\"\"id\"\":9}\",=cmd");
        assert_eq!(lines[6], "true,1.5");
        assert_eq!(lines[7], "", "trailing CRLF");
        assert_eq!(
            rep,
            CsvReport {
                rows_written: 6,
                columns: 2,
                nulls_written_as_empty: 3,
                values_written_as_json: 4,
                fields_a_spreadsheet_reads_as_a_formula: 1,
            }
        );
    }

    #[test]
    fn to_csv_of_empty_result_is_only_the_header() {
        let (text, rep) = to_csv(&batch(&["only"], vec![]));
        assert_eq!(text, "only\r\n");
        assert_eq!((rep.rows_written, rep.columns), (0, 1));
    }
}

/// What a whole-graph CSV export wrote, and what it could not express (#1914).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GraphCsvReport {
    pub nodes_written: usize,
    pub edges_written: usize,
    /// The files written, relative to the directory, in the order written.
    pub files: Vec<String>,
    /// Properties whose values were not all one type in a file, so the
    /// column is a `string` and numbers in it are digits, not numbers.
    /// Listed as `file:column`.
    pub columns_widened_to_string: Vec<String>,
    /// Lists, maps and vectors written as JSON text in one field.
    pub values_written_as_json: usize,
    /// Temporal values written as ISO-8601 text.
    pub temporal_values_written_as_text: usize,
    /// Labels containing `;`, the separator `neo4j-admin` reads between labels.
    pub labels_containing_a_semicolon: Vec<String>,
    /// Properties left out because their name contains `:`, which
    /// `neo4j-admin` reads as the start of the column's type (`a:b` would be
    /// `a:b:string`), and its header has no escape for it. Listed as
    /// `file:property`.
    pub properties_with_a_colon_omitted: Vec<String>,
}

impl GraphCsvReport {
    /// True when every value kept a column type that means what it meant here.
    pub fn typed_losslessly(&self) -> bool {
        self.columns_widened_to_string.is_empty()
            && self.values_written_as_json == 0
            && self.temporal_values_written_as_text == 0
            && self.labels_containing_a_semicolon.is_empty()
            && self.properties_with_a_colon_omitted.is_empty()
    }
}

/// A column's `neo4j-admin` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColType {
    Boolean,
    Long,
    Double,
    String,
}

impl ColType {
    fn of(v: &crate::graph::PropertyValue) -> ColType {
        use crate::graph::PropertyValue as P;
        match v {
            P::Boolean(_) => ColType::Boolean,
            P::Integer(_) => ColType::Long,
            P::Float(_) => ColType::Double,
            _ => ColType::String,
        }
    }

    /// The narrowest type holding both, by the rule GraphML export uses: a
    /// long and a double make a double, any other mix a string (#1575).
    fn widen(self, other: ColType) -> ColType {
        match (self, other) {
            (a, b) if a == b => a,
            (ColType::Long, ColType::Double) | (ColType::Double, ColType::Long) => ColType::Double,
            _ => ColType::String,
        }
    }

    fn name(self) -> &'static str {
        match self {
            ColType::Boolean => "boolean",
            ColType::Long => "long",
            ColType::Double => "double",
            ColType::String => "string",
        }
    }
}

/// The largest integer magnitude an `f64` holds exactly: 2^53.
const MAX_EXACT_DOUBLE_INT: u64 = 1 << 53;

/// One property value as a CSV field's text.
fn graph_cell(v: &crate::graph::PropertyValue, report: &mut GraphCsvReport) -> String {
    use crate::graph::PropertyValue as P;
    match v {
        P::Null => String::new(),
        P::String(s) => s.clone(),
        P::Integer(i) => i.to_string(),
        P::Float(f) => f.to_string(),
        P::Boolean(b) => b.to_string(),
        P::Array(_) | P::Map(_) | P::Vector(_) => {
            report.values_written_as_json += 1;
            v.to_json().to_string()
        }
        temporal => {
            report.temporal_values_written_as_text += 1;
            temporal.to_string()
        }
    }
}

/// A file-name-safe form of a label or type: anything but `[A-Za-z0-9_-]`
/// becomes `_`.
fn file_part(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect()
}

/// One file's rows before writing: the columns are only known once every row
/// has been seen.
struct Table {
    /// Leading columns (`:ID`, `:LABEL` / `:START_ID`, `:END_ID`, `:TYPE`)
    /// and their values per row.
    fixed_header: Vec<&'static str>,
    fixed: Vec<Vec<String>>,
    props: Vec<crate::graph::PropertyMap>,
}

/// Write the whole graph into `dir`, which must exist and be empty, as the
/// CSV files `neo4j-admin database import` reads (#1914).
///
/// A graph is not one table, so it is one file per label combination,
/// `nodes_<Label>[_<Label>].csv` with `:ID` and `:LABEL` (labels `;`
/// separated), and one per relationship type, `edges_<TYPE>.csv` with
/// `:START_ID`, `:END_ID` and `:TYPE`; each then has a column per property,
/// typed (`born:long`, `active:boolean`) when every value in that file has
/// one type. Ids are this graph's node ids. Property names are written as
/// they are; neo4j-admin reads `:` in a header as the type separator, so a
/// property name containing one will not import as that name.
pub fn write_graph_dir(
    store: &crate::graph::GraphStore,
    dir: &std::path::Path,
) -> std::io::Result<GraphCsvReport> {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::Write;

    let mut report = GraphCsvReport::default();
    // Keyed by what each table *is* -- a node's sorted label set, or an
    // edge's type -- not by its file name: file names are made file-safe and
    // can coincide (`A_B`, `A B` and `A`+`B` all read `nodes_A_B.csv`), and
    // two groups sharing a table would share its column types (review on
    // #1942). `false` sorts node tables before edge tables.
    let mut tables: BTreeMap<(bool, Vec<String>), Table> = BTreeMap::new();

    for node in store.all_nodes() {
        let mut labels: Vec<String> = node.labels.iter().map(|l| l.as_str().to_string()).collect();
        labels.sort();
        for l in &labels {
            if l.contains(';') && !report.labels_containing_a_semicolon.contains(l) {
                report.labels_containing_a_semicolon.push(l.clone());
            }
        }
        let t = tables.entry((false, labels.clone())).or_insert_with(|| Table {
            fixed_header: vec![":ID", ":LABEL"],
            fixed: Vec::new(),
            props: Vec::new(),
        });
        t.fixed.push(vec![node.id.as_u64().to_string(), labels.join(";")]);
        t.props.push(store.node_properties_merged(node.id));
        report.nodes_written += 1;
    }
    for edge in store.all_edges() {
        let ty = edge.edge_type.as_str().to_string();
        let t = tables
            .entry((true, vec![ty.clone()]))
            .or_insert_with(|| Table {
                fixed_header: vec![":START_ID", ":END_ID", ":TYPE"],
                fixed: Vec::new(),
                props: Vec::new(),
            });
        t.fixed.push(vec![edge.source.as_u64().to_string(), edge.target.as_u64().to_string(), ty]);
        t.props.push(store.edge_properties_merged(edge.id));
        report.edges_written += 1;
    }

    // One file per table, node files first, then edge files: the order
    // neo4j-admin is given them. A name already taken gets `_2`, `_3`, ...
    let mut used: BTreeSet<String> = BTreeSet::new();
    let mut files: Vec<(String, &Table)> = Vec::new();
    for ((is_edge, parts), t) in &tables {
        let stem = match (is_edge, parts.is_empty()) {
            (false, true) => "nodes__unlabelled".to_string(),
            (false, false) => format!(
                "nodes_{}",
                parts.iter().map(|l| file_part(l)).collect::<Vec<_>>().join("_")
            ),
            (true, _) => format!("edges_{}", file_part(&parts[0])),
        };
        let mut name = format!("{stem}.csv");
        let mut n = 2;
        while !used.insert(name.clone()) {
            name = format!("{stem}_{n}.csv");
            n += 1;
        }
        files.push((name, t));
    }
    for (name, t) in &files {
        // Every type each column held, then the one it is written as. A
        // column reported as widened held more than one type and ended as a
        // string; a long-and-double column is a double and loses nothing.
        let mut seen: BTreeMap<String, Vec<ColType>> = BTreeMap::new();
        // Columns holding an integer a double cannot represent exactly, so
        // such a column must not widen to `double` (review on #1942).
        let mut inexact_as_double: BTreeSet<String> = BTreeSet::new();
        for props in &t.props {
            for (k, v) in props {
                if matches!(v, crate::graph::PropertyValue::Null) {
                    continue;
                }
                if let crate::graph::PropertyValue::Integer(i) = v {
                    if i.unsigned_abs() > MAX_EXACT_DOUBLE_INT {
                        inexact_as_double.insert(k.clone());
                    }
                }
                let held = seen.entry(k.clone()).or_default();
                let ty = ColType::of(v);
                if !held.contains(&ty) {
                    held.push(ty);
                }
            }
        }
        // No header syntax carries a `:` in a name, so such a column is left
        // out and reported rather than written unparseable.
        for k in seen.keys().filter(|k| k.contains(':')) {
            report
                .properties_with_a_colon_omitted
                .push(format!("{name}:{k}"));
        }
        let types: BTreeMap<String, ColType> = seen
            .iter()
            .filter(|(k, _)| !k.contains(':'))
            .map(|(k, held)| {
                let mut ty = held[1..].iter().fold(held[0], |a, b| a.widen(*b));
                // A long and a double make a double only when every integer in
                // the column survives as one; past 2^53 Neo4j would round it
                // on import, so the column is text, with the digits exact.
                if ty == ColType::Double && inexact_as_double.contains(k) {
                    ty = ColType::String;
                }
                if held.len() > 1 && ty == ColType::String {
                    report.columns_widened_to_string.push(format!("{name}:{k}"));
                }
                (k.clone(), ty)
            })
            .collect();

        let mut out = std::io::BufWriter::new(std::fs::File::create(dir.join(name))?);
        let mut header: Vec<String> = t.fixed_header.iter().map(|h| h.to_string()).collect();
        header.extend(types.iter().map(|(k, ty)| format!("{k}:{}", ty.name())));
        writeln!(out, "{}", header.iter().map(|h| quote(h)).collect::<Vec<_>>().join(","))?;
        for (fixed, props) in t.fixed.iter().zip(&t.props) {
            let mut row: Vec<String> = fixed.iter().map(|f| quote(f)).collect();
            for k in types.keys() {
                // neo4j-admin reads an unquoted empty field as "no property"
                // and a quoted `""` as the empty string, so the two are kept
                // apart: absent or null is unquoted and empty, and a value
                // whose text is empty is written `""` (review on #1942).
                row.push(match props.get(k) {
                    None | Some(crate::graph::PropertyValue::Null) => String::new(),
                    Some(v) => {
                        let text = graph_cell(v, &mut report);
                        if text.is_empty() {
                            "\"\"".to_string()
                        } else {
                            quote(&text)
                        }
                    }
                });
            }
            writeln!(out, "{}", row.join(","))?;
        }
        out.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        report.files.push(name.clone());
    }
    report.columns_widened_to_string.sort();
    report.properties_with_a_colon_omitted.sort();
    Ok(report)
}
