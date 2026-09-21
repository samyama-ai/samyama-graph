//! Export the graph as GraphML (INT-05).
//!
//! GraphML is what Gephi, yEd and Cytoscape read, and it is the format a user
//! reaches for when they want their graph somewhere this engine is not. It is
//! therefore an *exit* format first and an interchange format second — see
//! `docs/LEAVING-SAMYAMA.md`, which is the document this code has to keep
//! honest.
//!
//! # Where GraphML is narrower than a property graph
//!
//! GraphML declares each attribute once, up front, with a single type:
//!
//! ```xml
//! <key id="d0" for="node" attr.name="age" attr.type="long"/>
//! ```
//!
//! A property graph does not promise that. `age` may be an integer on one node
//! and the string `"unknown"` on another, and nothing rejected that on the way
//! in. Three things follow, and each is **counted** rather than smoothed over,
//! because a silent export that a user only discovers is wrong after they have
//! decommissioned the source is the worst outcome this code can produce:
//!
//! - **A type conflict widens to `string`.** Every value of that attribute is
//!   then written as text, so the numbers survive as digits and stop being
//!   numbers. Counted in [`GraphMlReport::attributes_widened_to_string`].
//! - **Arrays, maps and vectors have no GraphML type.** They are written as
//!   JSON inside a string attribute, which preserves the content and loses the
//!   structure to any reader that does not know to parse it. Counted in
//!   [`GraphMlReport::values_written_as_json`].
//! - **Temporal values have no GraphML type either.** They are written as ISO
//!   8601 strings, which is lossless as text and not as a type.
//!
//! # Labels and edge types
//!
//! GraphML has neither. The convention every tool in this space follows is a
//! `labels` attribute on the node and a `label` attribute on the edge, so that
//! is what is written; a node with several labels gets them space-separated,
//! which is also the convention and is ambiguous for a label containing a
//! space. Such a label is counted in
//! [`GraphMlReport::labels_containing_a_space`] rather than being quietly
//! mangled.

use crate::graph::{GraphStore, PropertyValue};
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// What an export carried, and what it could not.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GraphMlReport {
    pub nodes_written: usize,
    pub edges_written: usize,
    /// `<key>` declarations emitted, over both scopes.
    pub keys_declared: usize,
    /// Attributes whose values were not all one type, so the declaration
    /// widened to `string`. The names are listed, not just counted: a user
    /// deciding whether the export is good enough needs to know *which*
    /// attribute stopped being a number.
    pub attributes_widened_to_string: Vec<String>,
    /// Values written as JSON text because GraphML has no type for them
    /// (arrays, maps, vectors).
    pub values_written_as_json: usize,
    /// Temporal values written as ISO-8601 text.
    pub temporal_values_written_as_text: usize,
    /// Labels containing a space, which the space-separated `labels`
    /// convention cannot represent unambiguously.
    pub labels_containing_a_space: Vec<String>,
}

impl GraphMlReport {
    /// True when every value kept a GraphML type that means what it meant here.
    ///
    /// A `false` is not a failure — a GraphML export of a graph with array
    /// properties cannot be lossless, and saying so is the point.
    pub fn typed_losslessly(&self) -> bool {
        self.attributes_widened_to_string.is_empty()
            && self.values_written_as_json == 0
            && self.temporal_values_written_as_text == 0
            && self.labels_containing_a_space.is_empty()
    }
}

/// The GraphML type a value needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GmlType {
    Boolean,
    Long,
    Double,
    String,
}

impl GmlType {
    fn name(self) -> &'static str {
        match self {
            GmlType::Boolean => "boolean",
            GmlType::Long => "long",
            GmlType::Double => "double",
            GmlType::String => "string",
        }
    }

    /// The narrowest type holding both. `Ord` is declared so that this is the
    /// maximum, and the variant order above is the widening order: anything
    /// mixed with `String` is `String`, and a long mixed with a double is a
    /// double.
    fn widen(self, other: GmlType) -> GmlType {
        self.max(other)
    }
}

fn type_of(v: &PropertyValue) -> GmlType {
    match v {
        PropertyValue::Boolean(_) => GmlType::Boolean,
        PropertyValue::Integer(_) => GmlType::Long,
        PropertyValue::Float(_) => GmlType::Double,
        _ => GmlType::String,
    }
}

/// The text of a value under its declared type.
///
/// `declared` matters: an integer written under a widened `string`
/// declaration must still be written as its digits, and a float written under
/// a `double` declaration must not be rendered by `Debug`.
fn text_of(v: &PropertyValue, report: &mut GraphMlReport) -> String {
    match v {
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => f.to_string(),
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Array(_) | PropertyValue::Map(_) | PropertyValue::Vector(_) => {
            report.values_written_as_json += 1;
            json_text(v)
        }
        PropertyValue::Null => String::new(),
        temporal => {
            report.temporal_values_written_as_text += 1;
            // The engine's own rendering, so the text a user sees in a query
            // result is the text in the file. A second formatter here would be
            // a second definition of what these values are.
            temporal.to_string()
        }
    }
}

/// A complex value as JSON, so the content survives even though the structure
/// leaves GraphML's type system.
fn json_text(v: &PropertyValue) -> String {
    fn to_json(v: &PropertyValue) -> serde_json::Value {
        match v {
            PropertyValue::String(s) => serde_json::Value::String(s.clone()),
            PropertyValue::Integer(i) => serde_json::Value::from(*i),
            PropertyValue::Float(f) => serde_json::Value::from(*f),
            PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
            PropertyValue::Null => serde_json::Value::Null,
            PropertyValue::Array(items) => {
                serde_json::Value::Array(items.iter().map(to_json).collect())
            }
            PropertyValue::Vector(items) => serde_json::Value::Array(
                items.iter().map(|f| serde_json::Value::from(*f)).collect(),
            ),
            PropertyValue::Map(m) => serde_json::Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), to_json(v)))
                    .collect::<serde_json::Map<_, _>>(),
            ),
            other => serde_json::Value::String(other.to_string()),
        }
    }
    to_json(v).to_string()
}

/// XML text escaping. Applied to attribute names and values alike, because a
/// property key is user data here in exactly the way a property value is.
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // XML 1.0 forbids most control characters outright; there is no
            // escape that makes them legal, so they are dropped rather than
            // written into a file that will not parse.
            c if (c as u32) < 0x20 && c != '\t' && c != '\n' && c != '\r' => {}
            c => out.push(c),
        }
    }
    out
}

/// Property map in key order.
fn sorted(m: crate::graph::PropertyMap) -> Vec<(String, PropertyValue)> {
    let mut v: Vec<(String, PropertyValue)> = m.into_iter().collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// Write `store` as a GraphML document.
pub fn to_graphml(store: &GraphStore) -> (String, GraphMlReport) {
    let mut report = GraphMlReport::default();

    // Pass 1: decide one type per attribute per scope. GraphML declares keys
    // before any data, so the whole graph has to be seen before a single
    // element can be written. This is the cost of the format, not a choice.
    let mut node_types: BTreeMap<String, GmlType> = BTreeMap::new();
    let mut edge_types: BTreeMap<String, GmlType> = BTreeMap::new();
    let mut widened: Vec<String> = Vec::new();

    let node_ids: Vec<_> = store.all_nodes().iter().map(|n| n.id).collect();
    for id in &node_ids {
        for (key, value) in store.node_properties_merged(*id) {
            let t = type_of(&value);
            match node_types.get(&key) {
                Some(prev) if *prev != t => {
                    let w = prev.widen(t);
                    if w == GmlType::String && *prev != GmlType::String {
                        widened.push(format!("node.{key}"));
                    }
                    node_types.insert(key, w);
                }
                Some(_) => {}
                None => {
                    node_types.insert(key, t);
                }
            }
        }
    }

    let edges: Vec<_> = store.all_edges();
    for e in &edges {
        for (key, value) in store.edge_properties_merged(e.id) {
            let t = type_of(&value);
            match edge_types.get(&key) {
                Some(prev) if *prev != t => {
                    let w = prev.widen(t);
                    if w == GmlType::String && *prev != GmlType::String {
                        widened.push(format!("edge.{key}"));
                    }
                    edge_types.insert(key, w);
                }
                Some(_) => {}
                None => {
                    edge_types.insert(key, t);
                }
            }
        }
    }
    widened.sort();
    widened.dedup();
    report.attributes_widened_to_string = widened;

    // `labels` and `label` are the conventional carriers for the two things
    // GraphML has no concept of. Reserved names, so a user property called
    // `labels` would collide -- it is given a distinct key id below and the
    // conventional one keeps the plain `attr.name`, because a reader looking
    // for labels must find them where every other tool puts them.
    let mut out = String::with_capacity(node_ids.len() * 128);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(
        "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\"\n         \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n         \
         xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns \
         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">\n",
    );

    let mut key_id: BTreeMap<(bool, String), String> = BTreeMap::new();
    let mut n = 0usize;

    let _ = writeln!(
        out,
        "  <key id=\"labels\" for=\"node\" attr.name=\"labels\" attr.type=\"string\"/>"
    );
    let _ = writeln!(
        out,
        "  <key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>"
    );
    report.keys_declared += 2;

    for (key, t) in &node_types {
        let id = format!("nd{n}");
        n += 1;
        let _ = writeln!(
            out,
            "  <key id=\"{}\" for=\"node\" attr.name=\"{}\" attr.type=\"{}\"/>",
            id,
            esc(key),
            t.name()
        );
        key_id.insert((true, key.clone()), id);
        report.keys_declared += 1;
    }
    for (key, t) in &edge_types {
        let id = format!("ed{n}");
        n += 1;
        let _ = writeln!(
            out,
            "  <key id=\"{}\" for=\"edge\" attr.name=\"{}\" attr.type=\"{}\"/>",
            id,
            esc(key),
            t.name()
        );
        key_id.insert((false, key.clone()), id);
        report.keys_declared += 1;
    }

    // `edgedefault="directed"`: every edge in this engine is directed, and a
    // GraphML document must say which it means. Omitting it makes the file
    // invalid, and declaring `undirected` would assert something false.
    out.push_str("  <graph id=\"G\" edgedefault=\"directed\">\n");

    for id in &node_ids {
        let _ = writeln!(out, "    <node id=\"n{}\">", id.as_u64());
        let mut labels: Vec<String> = store
            .get_node(*id)
            .map(|n| n.labels.iter().map(|l| l.as_str().to_string()).collect())
            .unwrap_or_default();
        // The label set is a HashSet, so the export order is otherwise
        // arbitrary run to run and two exports of one graph would differ.
        labels.sort();
        if !labels.is_empty() {
            for l in &labels {
                if l.contains(' ') {
                    report.labels_containing_a_space.push(l.clone());
                }
            }
            let joined = labels.join(" ");
            let _ = writeln!(out, "      <data key=\"labels\">{}</data>", esc(&joined));
        }
        // Sorted: `node_properties_merged` returns a HashMap, so without this
        // two exports of one graph differ in `<data>` order and the file
        // cannot be diffed or checksummed. Caught by the determinism test,
        // which is the only thing that looks at order.
        for (key, value) in sorted(store.node_properties_merged(*id)) {
            if matches!(value, PropertyValue::Null) {
                continue;
            }
            if let Some(kid) = key_id.get(&(true, key.clone())) {
                let text = text_of(&value, &mut report);
                let _ = writeln!(out, "      <data key=\"{kid}\">{}</data>", esc(&text));
            }
        }
        out.push_str("    </node>\n");
        report.nodes_written += 1;
    }

    for e in &edges {
        let _ = writeln!(
            out,
            "    <edge id=\"e{}\" source=\"n{}\" target=\"n{}\">",
            e.id.as_u64(),
            e.source.as_u64(),
            e.target.as_u64()
        );
        let _ = writeln!(
            out,
            "      <data key=\"label\">{}</data>",
            esc(e.edge_type.as_str())
        );
        for (key, value) in sorted(store.edge_properties_merged(e.id)) {
            if matches!(value, PropertyValue::Null) {
                continue;
            }
            if let Some(kid) = key_id.get(&(false, key.clone())) {
                let text = text_of(&value, &mut report);
                let _ = writeln!(out, "      <data key=\"{kid}\">{}</data>", esc(&text));
            }
        }
        out.push_str("    </edge>\n");
        report.edges_written += 1;
    }

    out.push_str("  </graph>\n</graphml>\n");

    report.labels_containing_a_space.sort();
    report.labels_containing_a_space.dedup();
    (out, report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widening_order_is_the_variant_order() {
        assert_eq!(GmlType::Long.widen(GmlType::Double), GmlType::Double);
        assert_eq!(GmlType::Long.widen(GmlType::String), GmlType::String);
        assert_eq!(GmlType::Boolean.widen(GmlType::Long), GmlType::Long);
        assert_eq!(GmlType::String.widen(GmlType::Boolean), GmlType::String);
        assert_eq!(GmlType::Long.widen(GmlType::Long), GmlType::Long);
    }

    #[test]
    fn escaping_covers_the_five_and_drops_the_illegal() {
        assert_eq!(esc("a<b&c>d\"e'f"), "a&lt;b&amp;c&gt;d&quot;e&apos;f");
        // XML 1.0 has no escape for a control character; a file containing one
        // does not parse, so it cannot be written.
        assert_eq!(esc("a\u{1}b"), "ab");
        assert_eq!(esc("a\tb\nc"), "a\tb\nc");
    }
}
