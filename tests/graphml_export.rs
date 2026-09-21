//! GraphML export is a file other tools can open (INT-05).
//!
//! The file is checked with an XML parser, not with the writer that produced
//! it. A writer checked against itself agrees with itself: the failure that
//! matters is a document Gephi or yEd refuses, and only a parser sees that.

use quick_xml::events::Event;
use quick_xml::Reader;
use samyama::export::graphml::to_graphml;
use samyama::graph::{GraphStore, PropertyValue};
use std::collections::{HashMap, HashSet};

/// Every element and its attributes, in document order. Panics if the document
/// is not well-formed, which is the first thing worth knowing.
fn elements(xml: &str) -> Vec<(String, HashMap<String, String>)> {
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let mut attrs = HashMap::new();
                for a in e.attributes() {
                    let a = a.expect("attribute must parse");
                    attrs.insert(
                        String::from_utf8_lossy(a.key.as_ref()).to_string(),
                        a.unescape_value().expect("value must unescape").to_string(),
                    );
                }
                out.push((name, attrs));
            }
            Ok(_) => {}
            Err(e) => panic!("the exported document is not well-formed XML: {e}"),
        }
    }
    out
}

/// `<data key="…">text</data>` pairs, keyed by the element they sit inside.
fn data_values(xml: &str) -> Vec<(String, String)> {
    let mut reader = Reader::from_str(xml);
    let mut out = Vec::new();
    let mut pending: Option<String> = None;
    loop {
        match reader.read_event() {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) if e.name().as_ref() == b"data" => {
                pending = e
                    .attributes()
                    .flatten()
                    .find(|a| a.key.as_ref() == b"key")
                    .map(|a| a.unescape_value().unwrap().to_string());
            }
            Ok(Event::Text(t)) => {
                if let Some(k) = pending.take() {
                    out.push((k, t.unescape().unwrap().to_string()));
                }
            }
            Ok(Event::End(e)) if e.name().as_ref() == b"data" => pending = None,
            Ok(_) => {}
            Err(e) => panic!("not well-formed: {e}"),
        }
    }
    out
}

fn small_graph() -> GraphStore {
    let mut g = GraphStore::new();
    let a = g.create_node("Person");
    let b = g.create_node("Person");
    let c = g.create_node("Company");
    g.add_label_to_node("default", b, "Employee").unwrap();
    g.set_node_property("default", a, "name", "Alice").unwrap();
    g.set_node_property("default", a, "age", 34i64).unwrap();
    g.set_node_property("default", a, "score", 91.5f64).unwrap();
    g.set_node_property("default", a, "active", true).unwrap();
    g.set_node_property("default", b, "name", "Bob").unwrap();
    g.set_node_property("default", b, "age", 41i64).unwrap();
    g.set_node_property("default", c, "name", "Acme").unwrap();
    let e = g.create_edge(a, b, "KNOWS").unwrap();
    g.set_edge_property(e, "since", 2019i64).unwrap();
    g.create_edge(b, c, "WORKS_AT").unwrap();
    g
}

#[test]
fn the_document_is_well_formed_and_has_the_graphml_shape() {
    let (xml, report) = to_graphml(&small_graph());
    let els = elements(&xml);

    let root = &els[0];
    assert_eq!(root.0, "graphml");
    assert_eq!(
        root.1.get("xmlns").map(String::as_str),
        Some("http://graphml.graphdrawing.org/xmlns"),
        "without the namespace a reader cannot tell this is GraphML"
    );

    let graph = els.iter().find(|(n, _)| n == "graph").expect("a <graph>");
    assert_eq!(
        graph.1.get("edgedefault").map(String::as_str),
        Some("directed"),
        "a GraphML document must say which direction it means; every edge here is directed"
    );

    assert_eq!(els.iter().filter(|(n, _)| n == "node").count(), 3);
    assert_eq!(els.iter().filter(|(n, _)| n == "edge").count(), 2);
    assert_eq!(report.nodes_written, 3);
    assert_eq!(report.edges_written, 2);
}

#[test]
fn every_data_key_was_declared() {
    // The commonest way to write invalid GraphML: emit a `<data key="x">` with
    // no matching `<key id="x">`. The document is still well-formed XML, so a
    // parse check alone would not catch it — yEd rejects the file and the user
    // finds out at the point of leaving.
    let (xml, _) = to_graphml(&small_graph());
    let els = elements(&xml);

    let declared: HashSet<String> = els
        .iter()
        .filter(|(n, _)| n == "key")
        .filter_map(|(_, a)| a.get("id").cloned())
        .collect();
    assert!(
        !declared.is_empty(),
        "no keys declared, so this proves nothing"
    );

    for (used, _) in data_values(&xml) {
        assert!(
            declared.contains(&used),
            "<data key=\"{used}\"> has no <key id=\"{used}\">; declared: {declared:?}"
        );
    }
}

#[test]
fn labels_and_edge_types_land_where_other_tools_look() {
    // GraphML has neither concept. `labels` on the node and `label` on the
    // edge is the convention Gephi, yEd and Neo4j's own exporter follow;
    // putting them anywhere else exports a graph that opens with its structure
    // intact and its meaning gone.
    let (xml, _) = to_graphml(&small_graph());
    let data = data_values(&xml);

    let labels: Vec<&String> = data
        .iter()
        .filter(|(k, _)| k == "labels")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(labels.len(), 3);
    assert!(
        labels
            .iter()
            .any(|v| v.split(' ').collect::<HashSet<_>>() == HashSet::from(["Employee", "Person"])),
        "a node with two labels must carry both, space-separated: {labels:?}"
    );

    let edge_labels: HashSet<&str> = data
        .iter()
        .filter(|(k, _)| k == "label")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(edge_labels, HashSet::from(["KNOWS", "WORKS_AT"]));
}

#[test]
fn a_type_conflict_widens_to_string_and_says_which_attribute() {
    // `age` is a long on one node and a string on another — nothing rejected
    // that on the way in. GraphML declares one type per attribute, so one of
    // them has to give, and the user needs to know which attribute stopped
    // being a number.
    let mut g = small_graph();
    let d = g.create_node("Person");
    g.set_node_property("default", d, "age", "unknown").unwrap();

    let (xml, report) = to_graphml(&g);
    assert_eq!(
        report.attributes_widened_to_string,
        vec!["node.age".to_string()],
        "the widened attribute must be named, not merely counted"
    );
    assert!(!report.typed_losslessly());

    let els = elements(&xml);
    let age_key = els
        .iter()
        .find(|(n, a)| n == "key" && a.get("attr.name").map(String::as_str) == Some("age"))
        .expect("an `age` key");
    assert_eq!(
        age_key.1.get("attr.type").map(String::as_str),
        Some("string")
    );

    // The digits must still be there: widening the declaration must not stop
    // the numbers being written.
    let age_id = age_key.1.get("id").unwrap();
    let all = data_values(&xml);
    let ages: HashSet<&str> = all
        .iter()
        .filter(|(k, _)| k == age_id)
        .map(|(_, v)| v.as_str())
        .collect();
    assert!(
        ages.contains("34") && ages.contains("41") && ages.contains("unknown"),
        "{ages:?}"
    );
}

#[test]
fn a_clean_graph_reports_no_loss() {
    // The counterpart to the test above: if `typed_losslessly` were always
    // false it would carry no information, and the widening test would pass
    // for the wrong reason.
    let (_, report) = to_graphml(&small_graph());
    assert!(
        report.typed_losslessly(),
        "a graph of scalars only must export with its types intact: {report:?}"
    );
    assert_eq!(report.attributes_widened_to_string, Vec::<String>::new());
    assert_eq!(report.values_written_as_json, 0);
}

#[test]
fn an_array_property_keeps_its_content_as_json_and_is_counted() {
    // GraphML has no list type. Dropping the property would lose the data
    // silently; writing it as JSON keeps the content and loses the structure
    // to a reader that does not parse it, which is a trade the report has to
    // name.
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

    let (xml, report) = to_graphml(&g);
    assert_eq!(report.values_written_as_json, 1);
    assert!(!report.typed_losslessly());

    let written: Vec<String> = data_values(&xml)
        .into_iter()
        .filter(|(k, _)| k != "labels")
        .map(|(_, v)| v)
        .collect();
    assert_eq!(written, vec![r#"["x",2]"#.to_string()]);
}

#[test]
fn xml_hostile_text_survives_in_names_and_in_values() {
    // A property key is user data in exactly the way a property value is. An
    // unescaped `&` or `<` in either produces a file no parser will open, and
    // `elements()` panics rather than asserting, so this test is the check.
    let mut g = GraphStore::new();
    let a = g.create_node("N");
    g.set_node_property("default", a, "a<b&c", "x > y \"quoted\" 'single'")
        .unwrap();

    let (xml, _) = to_graphml(&g);
    let els = elements(&xml);
    assert!(
        els.iter()
            .any(|(n, at)| n == "key" && at.get("attr.name").map(String::as_str) == Some("a<b&c")),
        "the key name must survive escaping and unescape to itself"
    );
    assert!(
        data_values(&xml)
            .iter()
            .any(|(_, v)| v == "x > y \"quoted\" 'single'"),
        "the value must round-trip through XML escaping"
    );
}

#[test]
fn two_exports_of_one_graph_are_byte_identical() {
    // Labels live in a HashSet and properties in a HashMap, so nothing about
    // the store fixes an order. An export that differs run to run cannot be
    // diffed, checksummed, or used as a fixture — and the difference would
    // look like a change in the data.
    let g = small_graph();
    let (first, _) = to_graphml(&g);
    let (second, _) = to_graphml(&g);
    assert_eq!(first, second);
}

#[test]
fn an_empty_graph_is_still_a_valid_document() {
    let (xml, report) = to_graphml(&GraphStore::new());
    let els = elements(&xml);
    assert_eq!(els[0].0, "graphml");
    assert!(els.iter().any(|(n, _)| n == "graph"));
    assert_eq!(report.nodes_written, 0);
    assert_eq!(report.edges_written, 0);
    assert!(report.typed_losslessly());
}
