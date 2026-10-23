//! RDF through `samyama convert`: Turtle, N-Triples and RDF/XML in and out,
//! JSON-LD out, mapped by #1910's mapping file (#1912).
//!
//! The main acceptance is the round trip: under the `generic` preset,
//! TTL → `.sgsnap` → TTL gives an isomorphic graph. `oxrdf` has no
//! canonicalisation, so graphs are compared as sorted N-Triples after
//! relabelling each blank node by the triples it takes part in.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use samyama::convert::rdf_mapping::Mapping;
use samyama::convert::{convert, ConvertError, Format, Options};
use samyama::rdf::{RdfFormat, RdfObject, RdfParser, RdfSubject, Triple};

const PREFIXES: &str = r#"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://example.org/> .
"#;

const SKOS: &str = r#"
ex:root a skos:Concept ; skos:prefLabel "Computing"@en .
ex:db a skos:Concept ; skos:prefLabel "Databases"@en ; skos:notation "DB" ; skos:broader ex:root .
ex:graph a skos:Concept ; skos:prefLabel "Graph databases"@en ;
    skos:broader ex:db ; skos:related ex:root .
"#;

const OWL: &str = r#"
ex:Animal a owl:Class ; rdfs:label "Animal" .
ex:Person a owl:Class .
ex:hasOwner a owl:ObjectProperty .
ex:Dog a owl:Class ; rdfs:subClassOf ex:Animal ,
    [ a owl:Restriction ; owl:onProperty ex:hasOwner ; owl:someValuesFrom ex:Person ] .
"#;

const LANG_DATE_MULTI: &str = r#"
ex:a ex:title "Graph"@en , "Graphe"@fr ;
     ex:published "2024-02-29"^^xsd:date ;
     ex:tag "x" , "y" , "z" ;
     ex:count 3 ;
     ex:score "2.5"^^xsd:double .
"#;

fn parse(text: &str, format: RdfFormat) -> Vec<Triple> {
    RdfParser::parse(text, format).unwrap()
}

fn term_s(s: &RdfSubject) -> (String, Option<String>) {
    match s {
        RdfSubject::NamedNode(n) => (format!("<{}>", n.as_str()), None),
        RdfSubject::BlankNode(b) => ("_".to_string(), Some(b.as_str().to_string())),
    }
}

fn term_o(o: &RdfObject) -> (String, Option<String>) {
    match o {
        RdfObject::NamedNode(n) => (format!("<{}>", n.as_str()), None),
        RdfObject::BlankNode(b) => ("_".to_string(), Some(b.as_str().to_string())),
        RdfObject::Literal(l) => (l.to_string(), None),
    }
}

/// The graph as sorted N-Triples-like lines, each blank node renamed after
/// the sorted list of triples it appears in (with every blank written `_`).
/// Two graphs that differ only in blank-node labels give the same lines.
fn canonical(triples: &[Triple]) -> Vec<String> {
    let rows: Vec<[(String, Option<String>); 3]> = triples
        .iter()
        .map(|t| {
            [
                term_s(&t.subject),
                (format!("<{}>", t.predicate.as_named_node().as_str()), None),
                term_o(&t.object),
            ]
        })
        .collect();
    let mut signature: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in &rows {
        let line = format!("{} {} {}", row[0].0, row[1].0, row[2].0);
        for (pos, (_, blank)) in row.iter().enumerate() {
            if let Some(b) = blank {
                signature
                    .entry(b.clone())
                    .or_default()
                    .push(format!("{pos}:{line}"));
            }
        }
    }
    let mut by_sig: Vec<(String, String)> = signature
        .into_iter()
        .map(|(b, mut sig)| {
            sig.sort();
            (sig.join("|"), b)
        })
        .collect();
    by_sig.sort();
    let names: BTreeMap<String, String> = by_sig
        .into_iter()
        .enumerate()
        .map(|(i, (_, b))| (b, format!("_:b{i}")))
        .collect();
    let name = |(t, blank): &(String, Option<String>)| match blank {
        Some(b) => names[b].clone(),
        None => t.clone(),
    };
    let mut lines: Vec<String> = rows
        .iter()
        .map(|r| format!("{} {} {} .", name(&r[0]), name(&r[1]), name(&r[2])))
        .collect();
    lines.sort();
    lines
}

fn write(dir: &Path, name: &str, text: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, text).unwrap();
    p
}

/// TTL → `.sgsnap` → `out`, returning the output's triples beside the input's.
fn round_trip(body: &str, out_name: &str, out_format: RdfFormat) -> (Vec<String>, Vec<String>) {
    let dir = tempfile::tempdir().unwrap();
    let text = format!("{PREFIXES}{body}");
    let input = write(dir.path(), "input.ttl", &text);
    let mid = dir.path().join("mid.sgsnap");
    let first = convert(&input, &mid, &Options::default()).unwrap();
    assert!(
        first.input.dropped().is_empty(),
        "generic should read this losslessly: {:?}",
        first.input.dropped()
    );
    let out = dir.path().join(out_name);
    convert(&mid, &out, &Options::default()).unwrap();
    let back = std::fs::read_to_string(&out).unwrap();
    (
        canonical(&parse(&text, RdfFormat::Turtle)),
        canonical(&parse(&back, out_format)),
    )
}

#[test]
fn plain_skos_round_trips_through_sgsnap_unchanged() {
    let (input, output) = round_trip(SKOS, "out.ttl", RdfFormat::Turtle);
    assert_eq!(output, input);
}

#[test]
fn owl_with_a_restriction_round_trips_through_sgsnap_unchanged() {
    let (input, output) = round_trip(OWL, "out.ttl", RdfFormat::Turtle);
    assert_eq!(output, input);
}

#[test]
fn language_tags_dates_and_multi_valued_properties_round_trip_unchanged() {
    let (input, output) = round_trip(LANG_DATE_MULTI, "out.ttl", RdfFormat::Turtle);
    assert_eq!(output, input);
}

#[test]
fn n_triples_and_rdf_xml_are_written_with_the_same_graph() {
    let (input, output) = round_trip(SKOS, "out.nt", RdfFormat::NTriples);
    assert_eq!(output, input);
    let (input, output) = round_trip(SKOS, "out.rdf", RdfFormat::RdfXml);
    assert_eq!(output, input);
}

#[test]
fn n_triples_and_rdf_xml_are_read() {
    let dir = tempfile::tempdir().unwrap();
    let triples = parse(&format!("{PREFIXES}{SKOS}"), RdfFormat::Turtle);
    for (name, format) in [
        ("in.nt", RdfFormat::NTriples),
        ("in.rdf", RdfFormat::RdfXml),
    ] {
        let text = samyama::rdf::RdfSerializer::serialize(&triples, format).unwrap();
        let input = write(dir.path(), name, &text);
        let out = dir.path().join(format!("{name}.sgsnap"));
        let report = convert(&input, &out, &Options::default()).unwrap();
        assert_eq!(report.input.node_count(), 3, "{name}");
        assert_eq!(report.input.edge_count(), 3, "{name}");
    }
}

#[test]
fn the_skos_preset_reports_exactly_what_it_drops() {
    let dir = tempfile::tempdir().unwrap();
    let body = format!(
        "{PREFIXES}{SKOS}\nex:scheme a skos:ConceptScheme .\nex:db skos:inScheme ex:scheme .\n\
         ex:graph skos:inScheme ex:scheme .\n"
    );
    let input = write(dir.path(), "skos.ttl", &body);
    let opts = Options {
        mapping: Some(Mapping::preset("skos").unwrap()),
        ..Options::default()
    };
    let report = convert(&input, &dir.path().join("skos.sgsnap"), &opts).unwrap();
    let dropped = report.input.dropped();
    let what: Vec<&str> = dropped.iter().map(|d| d.what.as_str()).collect();
    assert_eq!(what, vec!["rdf_predicates_dropped"], "{dropped:?}");
    assert_eq!(dropped[0].count, 2);
    assert!(
        dropped[0]
            .detail
            .contains("http://www.w3.org/2004/02/skos/core#inScheme (2)"),
        "{}",
        dropped[0].detail
    );

    // And refused, with nothing written, when lossless output is required.
    let strict = Options {
        require_lossless: true,
        ..opts
    };
    let out = dir.path().join("strict.sgsnap");
    let err = convert(&input, &out, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert!(!out.exists());
}

#[test]
fn the_snapshot_header_records_the_source_and_the_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let text = format!("{PREFIXES}{SKOS}");
    let input = write(dir.path(), "vocab.ttl", &text);
    let out = dir.path().join("vocab.sgsnap");
    convert(&input, &out, &Options::default()).unwrap();
    let header = samyama::snapshot::peek_header(std::fs::File::open(&out).unwrap()).unwrap();
    assert_eq!(header.sources.len(), 1);
    let source = &header.sources[0];
    assert_eq!(source.format, "turtle");
    assert_eq!(source.file, "vocab.ttl");
    assert_eq!(source.sha256.len(), 64);
    assert!(source.converter.starts_with("samyama-convert "));
    let mapping = source.mapping.as_ref().expect("a mapping is recorded");
    assert_eq!(mapping["preset"], "generic");
    assert_eq!(
        mapping["names"]["edges"]["BROADER"]["predicate"],
        "http://www.w3.org/2004/02/skos/core#broader"
    );
    let (m, names) = Mapping::from_recorded(mapping).unwrap();
    assert_eq!(m.preset, "generic");
    assert_eq!(
        names.labels.get("Concept").map(String::as_str),
        Some("http://www.w3.org/2004/02/skos/core#Concept")
    );
}

#[test]
fn a_reversed_edge_and_merged_aliases_write_back_as_they_came_in() {
    let dir = tempfile::tempdir().unwrap();
    let text = format!(
        "{PREFIXES}ex:db a skos:Concept ; skos:narrower ex:graph .\n\
         ex:graph a skos:Concept .\n\
         ex:gdb ex:preferred ex:graph .\n"
    );
    let input = write(dir.path(), "in.ttl", &text);
    let mapping = Mapping::from_yaml(
        "preset: skos\nprefixes: { ex: \"http://example.org/\" }\n\
         merge: { predicate: \"ex:preferred\", into: object, aliases_property: aliases }",
    )
    .unwrap();
    let mid = dir.path().join("mid.sgsnap");
    let opts = Options {
        mapping: Some(mapping),
        ..Options::default()
    };
    convert(&input, &mid, &opts).unwrap();
    let out = dir.path().join("out.ttl");
    convert(&mid, &out, &Options::default()).unwrap();
    let back = canonical(&parse(
        &std::fs::read_to_string(&out).unwrap(),
        RdfFormat::Turtle,
    ));
    let lines: BTreeSet<String> = back.into_iter().collect();
    // `narrower` was read as BROADER the other way round; it is written back
    // as `narrower` from the original subject.
    assert!(lines.contains(
        "<http://example.org/db> <http://www.w3.org/2004/02/skos/core#narrower> <http://example.org/graph> ."
    ), "{lines:?}");
    // The merged alias is a subject again, with the merge predicate.
    assert!(
        lines.contains(
            "<http://example.org/gdb> <http://example.org/preferred> <http://example.org/graph> ."
        ),
        "{lines:?}"
    );
}

#[test]
fn a_graph_that_never_was_rdf_is_written_in_samyamas_own_rdf_form() {
    let dir = tempfile::tempdir().unwrap();
    let mut g = samyama::graph::GraphStore::new();
    let a = g.create_node("Person");
    g.set_node_property("default", a, "name", "Ada").unwrap();
    let mid = dir.path().join("plain.sgsnap");
    samyama::snapshot::write_file(&g, &mid, &Default::default()).unwrap();
    let out = dir.path().join("plain.ttl");
    convert(&mid, &out, &Options::default()).unwrap();
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(text.contains("https://samyama.ai/rdf/prop/name"), "{text}");
    assert!(
        text.contains("https://samyama.ai/rdf/class/Person"),
        "{text}"
    );
}

#[test]
fn json_ld_is_written_but_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), "in.ttl", &format!("{PREFIXES}{SKOS}"));
    let out = dir.path().join("out.jsonld");
    convert(&input, &out, &Options::default()).unwrap();
    let text = std::fs::read_to_string(&out).unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&text).is_ok(),
        "{text}"
    );

    let err = convert(&out, &dir.path().join("back.sgsnap"), &Options::default()).unwrap_err();
    assert!(
        matches!(err, ConvertError::CannotRead(Format::JsonLd)),
        "{err}"
    );
}

#[test]
fn the_cli_takes_a_preset_or_a_mapping_file_for_rdf_input() {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), "in.ttl", &format!("{PREFIXES}{SKOS}"));
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_samyama"))
            .arg("convert")
            .args(args)
            .output()
            .unwrap()
    };
    let out = dir.path().join("p.sgsnap");
    let o = run(&[
        input.to_str().unwrap(),
        out.to_str().unwrap(),
        "--preset",
        "skos",
    ]);
    assert_eq!(
        o.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );

    let mapping = write(
        dir.path(),
        "m.yaml",
        "preset: generic\nedges: { \"skos:broader\": PARENT }\n",
    );
    let out = dir.path().join("m.sgsnap");
    let o = run(&[
        input.to_str().unwrap(),
        out.to_str().unwrap(),
        "--mapping",
        mapping.to_str().unwrap(),
    ]);
    assert_eq!(
        o.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    let mut s = samyama::graph::GraphStore::new();
    samyama::snapshot::import_tenant(&mut s, std::fs::File::open(&out).unwrap()).unwrap();
    assert!(s
        .all_edges()
        .iter()
        .any(|e| e.edge_type.as_str() == "PARENT"));

    // Both at once, or a bad mapping file, exit 2.
    let o = run(&[
        input.to_str().unwrap(),
        out.to_str().unwrap(),
        "--preset",
        "skos",
        "--mapping",
        mapping.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(2));
    let bad = write(dir.path(), "bad.yaml", "preset: nope\n");
    let o = run(&[
        input.to_str().unwrap(),
        out.to_str().unwrap(),
        "--mapping",
        bad.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(2));
}

/// The input's loss report for `body` read under `yaml`.
fn read_losses(yaml: &str, body: &str) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let input = write(dir.path(), "in.ttl", &format!("{PREFIXES}{body}"));
    let opts = Options {
        mapping: Some(Mapping::from_yaml(yaml).unwrap()),
        ..Options::default()
    };
    let report = convert(&input, &dir.path().join("out.sgsnap"), &opts).unwrap();
    report
        .input
        .dropped()
        .iter()
        .map(|d| d.what.clone())
        .collect()
}

/// What cannot be written back exactly is reported on the way in, so
/// `require_lossless` can refuse it (#1954 review): one edge type from two
/// predicates, a key holding IRIs and text, a blank node inlined under two
/// parents.
#[test]
fn what_cannot_be_written_back_exactly_is_reported_when_read() {
    let losses = read_losses(
        "preset: skos",
        "ex:a skos:broader ex:b . ex:c skos:narrower ex:d .",
    );
    assert!(
        losses.contains(&"rdf_edge_predicates_ambiguous".to_string()),
        "{losses:?}"
    );

    let losses = read_losses(
        "prefixes: { ex: \"http://example.org/\" }\nproperties: { \"ex:p\": p }",
        "ex:a ex:p ex:b ; ex:p \"http://example.org/text\" .",
    );
    assert!(
        losses.contains(&"rdf_term_kinds_mixed".to_string()),
        "{losses:?}"
    );

    let losses = read_losses(
        "blank_nodes: inline",
        "ex:a ex:part _:b . ex:c ex:part _:b . _:b ex:v \"1\" .",
    );
    assert!(
        losses.contains(&"rdf_blank_nodes_shared".to_string()),
        "{losses:?}"
    );

    // None of them where nothing is ambiguous.
    assert!(read_losses("preset: skos", "ex:a skos:broader ex:b .").is_empty());
}

/// A language map with several values in one language and untagged values
/// round-trips: TTL -> .sgsnap -> TTL gives the same graph.
#[test]
fn a_language_map_with_repeated_and_untagged_values_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let text = format!("{PREFIXES}ex:a ex:title \"A\"@en , \"B\"@en , \"plain\" , \"C\"@fr .\n");
    let input = write(dir.path(), "in.ttl", &text);
    let mid = dir.path().join("mid.sgsnap");
    let opts = Options {
        mapping: Some(Mapping::from_yaml("languages: { others: map }").unwrap()),
        ..Options::default()
    };
    let first = convert(&input, &mid, &opts).unwrap();
    assert!(
        first.input.dropped().is_empty(),
        "{:?}",
        first.input.dropped()
    );
    let out = dir.path().join("out.ttl");
    convert(&mid, &out, &Options::default()).unwrap();
    let back = std::fs::read_to_string(&out).unwrap();
    assert_eq!(
        canonical(&parse(&back, RdfFormat::Turtle)),
        canonical(&parse(&text, RdfFormat::Turtle))
    );
}
