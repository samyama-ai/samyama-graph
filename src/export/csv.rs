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
    }
}
