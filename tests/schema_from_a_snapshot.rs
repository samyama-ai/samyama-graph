//! A schema derived from the data, not from memory (KG-02).
//!
//! KG-02 asks every published KG repository for a README with a schema
//! diagram. Zero of twelve have one, and `CH-KG-CONF` gives the reason: the
//! diagram is missing because in most of them the schema is, and
//!
//! > Generating a diagram for a repository that has not written its schema
//! > down would be drawing what somebody remembers.
//!
//! These cases pin the property that answers that objection. Every edge in the
//! output is an edge that occurs in the snapshot, with the count of how many
//! times — so the diagram is a measurement and a README that disagrees with it
//! is wrong. The counts are the part nobody remembers correctly, which is why
//! they are in the diagram rather than in a caption.

use samyama::schema_doc::{derive, Schema};

/// Nodes first, then edges, which is what `export_tenant` writes.
const ORDERED: &str = r#"{"format":"sgsnap","version":2,"labels":["Person","Company"]}
{"t":"n","id":1,"labels":["Person"],"props":{"name":"Alice","age":34}}
{"t":"n","id":2,"labels":["Person"],"props":{"name":"Bob"}}
{"t":"n","id":3,"labels":["Company"],"props":{"name":"Acme"}}
{"t":"e","id":1,"src":1,"tgt":3,"type":"WORKS_AT","props":{}}
{"t":"e","id":2,"src":2,"tgt":3,"type":"WORKS_AT","props":{}}
{"t":"e","id":3,"src":1,"tgt":2,"type":"KNOWS","props":{}}
"#;

fn schema_of(text: &'static str) -> Schema {
    derive(|| Ok(text.as_bytes())).expect("well-formed snapshot")
}

#[test]
fn labels_and_their_counts_come_from_the_nodes() {
    let s = schema_of(ORDERED);
    assert_eq!(s.nodes, 3);
    assert_eq!(s.edges, 3);
    assert_eq!(s.labels.get("Person"), Some(&2));
    assert_eq!(s.labels.get("Company"), Some(&1));
}

#[test]
fn property_keys_are_the_union_of_what_nodes_carry() {
    // Alice has `age` and Bob does not. A schema that listed only the first
    // node's keys, or only the keys every node has, would be describing a
    // different graph -- and both are easy mistakes to make.
    let s = schema_of(ORDERED);
    let person: Vec<&str> = s.properties["Person"].iter().map(String::as_str).collect();
    assert_eq!(person, vec!["age", "name"]);
    let company: Vec<&str> = s.properties["Company"].iter().map(String::as_str).collect();
    assert_eq!(company, vec!["name"]);
}

#[test]
fn relationships_are_reported_as_endpoint_labels_with_counts() {
    // The part a hand-drawn diagram gets wrong, because it is the part nobody
    // remembers: which label pairs an edge type actually connects, and how
    // often.
    let s = schema_of(ORDERED);
    assert_eq!(
        s.triples.get(&(
            "Person".to_string(),
            "WORKS_AT".to_string(),
            "Company".to_string()
        )),
        Some(&2)
    );
    assert_eq!(
        s.triples.get(&("Person".to_string(), "KNOWS".to_string(), "Person".to_string())),
        Some(&1)
    );
    assert_eq!(s.triples.len(), 2, "two distinct relationships, not three edges");
    assert_eq!(s.edge_types.get("WORKS_AT"), Some(&2));
}

#[test]
fn an_edge_written_before_its_endpoints_still_resolves() {
    // The reason the file is read twice. A single streaming pass resolves an
    // edge against the nodes it has seen so far, so an edge appearing first
    // would be reported as dangling -- and the diagram would quietly lose a
    // relationship rather than fail.
    let reversed = r#"{"format":"sgsnap","version":2}
{"t":"e","id":1,"src":1,"tgt":3,"type":"WORKS_AT","props":{}}
{"t":"n","id":1,"labels":["Person"],"props":{}}
{"t":"n","id":3,"labels":["Company"],"props":{}}
"#;
    let s = schema_of(reversed);
    assert_eq!(s.dangling_edges, 0);
    assert_eq!(
        s.triples.get(&(
            "Person".to_string(),
            "WORKS_AT".to_string(),
            "Company".to_string()
        )),
        Some(&1)
    );
}

#[test]
fn an_edge_pointing_outside_the_snapshot_is_counted_not_dropped() {
    // A snapshot that is not self-contained is worth more than the diagram,
    // so the number is reported rather than the edge silently skipped.
    let dangling = r#"{"format":"sgsnap","version":2}
{"t":"n","id":1,"labels":["Person"],"props":{}}
{"t":"e","id":1,"src":1,"tgt":999,"type":"WORKS_AT","props":{}}
"#;
    let s = schema_of(dangling);
    assert_eq!(s.dangling_edges, 1);
    assert!(s.triples.is_empty());
    assert!(s.to_markdown().contains("does not contain"));
}

#[test]
fn a_node_without_labels_is_named_rather_than_dropped() {
    let unlabelled = r#"{"format":"sgsnap","version":2}
{"t":"n","id":1,"labels":[],"props":{"x":1}}
{"t":"n","id":2,"labels":["Person"],"props":{}}
{"t":"e","id":1,"src":1,"tgt":2,"type":"SAW","props":{}}
"#;
    let s = schema_of(unlabelled);
    assert_eq!(s.labels.get("(unlabelled)"), Some(&1));
    // And it appears in the diagram, because an unlabelled node that a
    // relationship starts from is a fact about the graph.
    assert!(s.to_mermaid().contains("unlabelled"));
}

#[test]
fn a_label_that_is_not_a_mermaid_identifier_still_renders() {
    // Mermaid node ids are alphanumeric-ish; labels are whatever the loader
    // wrote. A label with a space or a dash would produce a diagram that does
    // not parse, which is worse than no diagram: it renders as an error box in
    // the README it was added to.
    let awkward = r#"{"format":"sgsnap","version":2}
{"t":"n","id":1,"labels":["Clinical Trial"],"props":{}}
{"t":"n","id":2,"labels":["2ndary-Outcome"],"props":{}}
{"t":"e","id":1,"src":1,"tgt":2,"type":"HAS","props":{}}
"#;
    let s = schema_of(awkward);
    let m = s.to_mermaid();
    assert!(m.contains("Clinical_Trial["), "{m}");
    assert!(m.contains("n2ndary_Outcome["), "an id may not start with a digit: {m}");
    // The human-readable name survives inside the node box.
    assert!(m.contains("Clinical Trial<br/>"), "{m}");
    assert!(m.starts_with("```mermaid\ngraph LR\n"));
    assert!(m.trim_end().ends_with("```"));
}

#[test]
fn the_markdown_form_carries_the_same_facts() {
    let s = schema_of(ORDERED);
    let md = s.to_markdown();
    assert!(md.contains("| `Person` | 2 |"));
    assert!(md.contains("age, name"));
    assert!(md.contains("| `WORKS_AT` | `Person` | `Company` | 2 |"));
}

#[test]
fn a_file_that_is_not_a_snapshot_is_an_error() {
    // Not an empty schema. A tool that returns "this graph has no labels" for
    // a file it could not parse would put an empty diagram in a README.
    let err = derive(|| Ok("not json at all\n".as_bytes()))
        .expect_err("a file that is not the snapshot format must be an error");
    assert!(err.contains("not JSON"), "{err}");
}
