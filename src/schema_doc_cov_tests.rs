//! Schema derivation from snapshot lines, and its two renderings.

use super::*;
use std::io::{Cursor, Write};

const SNAP: &str = r#"{"t":"h","version":1}
{"t":"n","id":1,"labels":["Person"],"props":{"name":"a","age":3}}
{"t":"n","id":2,"labels":["Person","Extra"],"props":{"email":"x"}}

{"t":"n","id":3,"labels":["2 Company"]}
{"t":"n","id":4,"props":{"k":1}}
{"t":"e","src":1,"tgt":2,"type":"KNOWS"}
{"t":"e","src":1,"tgt":3,"type":"WORKS AT"}
{"t":"e","src":2,"tgt":3,"type":"WORKS AT"}
{"t":"e","src":1,"tgt":99,"type":"KNOWS"}
{"t":"e","src":4}
"#;

fn derive_str(s: &'static str) -> Result<Schema, String> {
    derive(|| Ok(Cursor::new(s.as_bytes())))
}

#[test]
fn derive_counts_labels_properties_edges_and_triples() {
    let s = derive_str(SNAP).unwrap();
    assert_eq!(s.nodes, 4);
    assert_eq!(s.edges, 5);
    let labels: Vec<(&str, u64)> = s.labels.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    assert_eq!(
        labels,
        vec![("(unlabelled)", 1), ("2 Company", 1), ("Person", 2)]
    );
    let person: Vec<&str> = s.properties["Person"].iter().map(String::as_str).collect();
    assert_eq!(person, vec!["age", "email", "name"]);
    assert!(
        !s.properties.contains_key("2 Company"),
        "a node with no props adds no key set"
    );
    assert_eq!(s.edge_types["KNOWS"], 2);
    assert_eq!(s.edge_types["WORKS AT"], 2);
    assert_eq!(s.edge_types["(untyped)"], 1);
    assert_eq!(
        s.triples[&(
            "Person".to_string(),
            "WORKS AT".to_string(),
            "2 Company".to_string()
        )],
        2
    );
    assert_eq!(
        s.triples[&(
            "Person".to_string(),
            "KNOWS".to_string(),
            "Person".to_string()
        )],
        1
    );
    assert_eq!(s.dangling_edges, 2, "unknown target and missing endpoints");
}

#[test]
fn derive_rejects_lines_that_are_not_json() {
    let err = derive_str("{\"t\":\"n\"}\nnot json\n").unwrap_err();
    assert!(err.starts_with("not JSON"), "{err}");
    // Edge pass sees the same corruption if the node pass did not.
    let mut calls = 0;
    let err = derive(|| {
        calls += 1;
        let text: &'static str = if calls == 1 { "" } else { "garbage" };
        Ok(Cursor::new(text.as_bytes()))
    })
    .unwrap_err();
    assert!(err.starts_with("not JSON"), "{err}");
}

#[test]
fn derive_propagates_open_errors() {
    let err = derive::<Cursor<&[u8]>, _>(|| Err("cannot open".to_string())).unwrap_err();
    assert_eq!(err, "cannot open");
}

#[test]
fn derive_of_empty_input_is_an_empty_schema() {
    let s = derive_str("").unwrap();
    assert_eq!((s.nodes, s.edges, s.dangling_edges), (0, 0, 0));
    assert!(s.labels.is_empty() && s.triples.is_empty());
}

#[test]
fn ident_sanitises_and_prefixes_leading_digits() {
    assert_eq!(ident("Person"), "Person");
    assert_eq!(ident("WORKS AT-x"), "WORKS_AT_x");
    assert_eq!(ident("2 Company"), "n2_Company");
    assert_eq!(ident(""), "");
}

#[test]
fn to_mermaid_lists_nodes_and_relationships() {
    let m = derive_str(SNAP).unwrap().to_mermaid();
    assert!(m.starts_with("```mermaid\ngraph LR\n"));
    assert!(m.ends_with("```\n"));
    assert!(m.contains("    Person[\"Person<br/>2\"]\n"), "{m}");
    assert!(m.contains("    n2_Company[\"2 Company<br/>1\"]\n"), "{m}");
    assert!(
        m.contains("    Person -->|\"WORKS AT (2)\"| n2_Company\n"),
        "{m}"
    );
    assert!(m.contains("    Person -->|\"KNOWS (1)\"| Person\n"), "{m}");
}

#[test]
fn to_markdown_tabulates_and_flags_dangling_edges() {
    let md = derive_str(SNAP).unwrap().to_markdown();
    assert!(md.contains("| `Person` | 2 | age, email, name |\n"), "{md}");
    assert!(md.contains("| `2 Company` | 1 |  |\n"), "{md}");
    assert!(
        md.contains("| `WORKS AT` | `Person` | `2 Company` | 2 |\n"),
        "{md}"
    );
    assert!(
        md.contains("**2 edge(s) point at a node this snapshot does not contain.**"),
        "{md}"
    );

    let clean = Schema::default().to_markdown();
    assert!(!clean.contains("edge(s) point at"));
    assert!(clean.contains("| Label | Nodes | Properties |"));
}

#[test]
fn derive_from_path_reads_plain_and_gzip_files() {
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("g.sgsnap");
    std::fs::write(&plain, SNAP).unwrap();
    let gz = dir.path().join("g.sgsnap.gz");
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(SNAP.as_bytes()).unwrap();
    std::fs::write(&gz, enc.finish().unwrap()).unwrap();

    let a = derive_from_path(&plain).unwrap();
    let b = derive_from_path(&gz).unwrap();
    assert_eq!((a.nodes, a.edges), (4, 5));
    assert_eq!(a.labels, b.labels);
    assert_eq!(a.triples, b.triples);
}

#[test]
fn derive_from_path_reports_missing_and_too_short_files() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope");
    assert!(derive_from_path(&missing).unwrap_err().contains("nope"));
    let short = dir.path().join("short");
    std::fs::write(&short, "x").unwrap();
    assert!(derive_from_path(&short).unwrap_err().contains("short"));
}
