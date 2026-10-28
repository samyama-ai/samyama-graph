//! `.sgsnap` → GraphML and → a CSV directory, through `samyama::convert`
//! (#1914).
//!
//! The GraphML is parsed back with a real XML reader: node and edge counts,
//! and each attribute declared with the type its values have. A Boolean /
//! Integer conflict widens to `string` and is reported (#1575). The CSV
//! directory has the files and column layout `neo4j-admin database import`
//! reads, one file per label combination and per relationship type.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use quick_xml::events::Event;
use samyama::convert::{convert, ConvertError, Options};
use samyama::graph::{GraphStore, PropertyValue};

/// Two people (one also a Director), a movie, two relationships; and an
/// attribute, `flag`, that is a boolean on one node and an integer on another.
fn fixture(dir: &Path) -> PathBuf {
    let mut s = GraphStore::new();
    let keanu = s.create_node("Person");
    s.set_node_property("default", keanu, "name", "Keanu")
        .unwrap();
    s.set_node_property("default", keanu, "born", 1964i64)
        .unwrap();
    s.set_node_property("default", keanu, "flag", true).unwrap();
    let lana = s.create_node("Person");
    s.add_label_to_node("default", lana, "Director").unwrap();
    s.set_node_property("default", lana, "name", "Lana, \"the\" director")
        .unwrap();
    s.set_node_property("default", lana, "born", 1965i64)
        .unwrap();
    s.set_node_property("default", lana, "flag", 7i64).unwrap();
    let matrix = s.create_node("Movie");
    s.set_node_property("default", matrix, "title", "The Matrix")
        .unwrap();
    s.set_node_property("default", matrix, "rating", 8.7f64)
        .unwrap();
    s.set_node_property(
        "default",
        matrix,
        "tags",
        PropertyValue::Array(vec!["sci-fi".into(), "action".into()]),
    )
    .unwrap();
    let e = s.create_edge(keanu, matrix, "ACTED_IN").unwrap();
    s.set_edge_property(e, "year", 1999i64).unwrap();
    s.create_edge(lana, matrix, "DIRECTED").unwrap();
    let path = dir.join("in.sgsnap");
    samyama::snapshot::write_file(&s, &path, &Default::default()).unwrap();
    path
}

/// Counts of `<node>` and `<edge>`, and the declared type of every node key.
fn parse_graphml(text: &str) -> (usize, usize, BTreeMap<String, String>) {
    let mut reader = quick_xml::Reader::from_str(text);
    let (mut nodes, mut edges) = (0, 0);
    let mut node_keys = BTreeMap::new();
    loop {
        match reader.read_event().expect("well-formed GraphML") {
            Event::Start(e) | Event::Empty(e) => {
                let attr = |name: &str| {
                    e.attributes()
                        .flatten()
                        .find(|a| a.key.as_ref() == name.as_bytes())
                        .map(|a| String::from_utf8_lossy(&a.value).into_owned())
                };
                match e.name().as_ref() {
                    b"node" => nodes += 1,
                    b"edge" => edges += 1,
                    b"key" if attr("for").as_deref() == Some("node") => {
                        node_keys.insert(attr("attr.name").unwrap(), attr("attr.type").unwrap());
                    }
                    _ => {}
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    (nodes, edges, node_keys)
}

#[test]
fn graphml_parses_back_with_its_counts_and_types_and_reports_the_widening() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture(dir.path());
    let output = dir.path().join("out/graph.graphml");

    let report = convert(&input, &output, &Options::default()).unwrap();
    assert_eq!(
        (report.output.node_count(), report.output.edge_count()),
        (3, 2)
    );

    let (nodes, edges, keys) = parse_graphml(&std::fs::read_to_string(&output).unwrap());
    assert_eq!((nodes, edges), (3, 2));
    assert_eq!(keys.get("born").map(String::as_str), Some("long"));
    assert_eq!(keys.get("rating").map(String::as_str), Some("double"));
    assert_eq!(keys.get("name").map(String::as_str), Some("string"));
    // A boolean on one node and an integer on another: `true` has no `long`
    // text, so the attribute is a string, and the report says which one.
    assert_eq!(keys.get("flag").map(String::as_str), Some("string"));
    let what: Vec<&str> = report
        .output
        .dropped()
        .iter()
        .map(|d| d.what.as_str())
        .collect();
    assert!(what.contains(&"attributes_widened_to_string"), "{what:?}");
    assert!(
        what.contains(&"values_written_as_json"),
        "the tags list: {what:?}"
    );
    let widened = report
        .output
        .dropped()
        .iter()
        .find(|d| d.what == "attributes_widened_to_string");
    assert!(widened.unwrap().detail.contains("node.flag"), "{widened:?}");
    assert!(no_staging_left(&dir.path().join("out")));
}

/// A CSV file as its header and rows, split on commas outside quotes.
fn read_csv(path: &Path) -> (Vec<String>, Vec<Vec<String>>) {
    let text = std::fs::read_to_string(path).unwrap();
    let split = |line: &str| {
        let (mut out, mut cur, mut quoted) = (Vec::new(), String::new(), false);
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            match (c, quoted) {
                ('"', true) if chars.peek() == Some(&'"') => {
                    cur.push('"');
                    chars.next();
                }
                ('"', _) => quoted = !quoted,
                (',', false) => out.push(std::mem::take(&mut cur)),
                (c, _) => cur.push(c),
            }
        }
        out.push(cur);
        out
    };
    let mut lines = text.lines();
    let header = split(lines.next().unwrap());
    (header, lines.map(split).collect())
}

#[test]
fn a_csv_directory_has_the_neo4j_admin_layout() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture(dir.path());
    let out = dir.path().join("csv_out");

    let report = convert(&input, &out, &Options::default()).unwrap();
    assert_eq!(
        (report.output.node_count(), report.output.edge_count()),
        (3, 2)
    );
    let mut files: Vec<String> = std::fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    files.sort();
    assert_eq!(
        files,
        vec![
            "edges_ACTED_IN.csv",
            "edges_DIRECTED.csv",
            "nodes_Director_Person.csv",
            "nodes_Movie.csv",
            "nodes_Person.csv",
        ]
    );

    let (header, rows) = read_csv(&out.join("nodes_Person.csv"));
    assert_eq!(
        header,
        vec![":ID", ":LABEL", "born:long", "flag:boolean", "name:string"]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], "Person");

    // Two labels in one `;`-separated field; a comma and quotes in a value
    // survive RFC 4180 quoting.
    let (_, rows) = read_csv(&out.join("nodes_Director_Person.csv"));
    assert_eq!(rows[0][1], "Director;Person");
    assert!(
        rows[0].contains(&"Lana, \"the\" director".to_string()),
        "{rows:?}"
    );

    let (header, rows) = read_csv(&out.join("edges_ACTED_IN.csv"));
    assert_eq!(header, vec![":START_ID", ":END_ID", ":TYPE", "year:long"]);
    assert_eq!(
        (rows.len(), rows[0][2].as_str(), rows[0][3].as_str()),
        (1, "ACTED_IN", "1999")
    );
    // Every :START_ID and :END_ID is an :ID in a node file.
    let mut ids = Vec::new();
    for f in files.iter().filter(|f| f.starts_with("nodes_")) {
        ids.extend(read_csv(&out.join(f)).1.into_iter().map(|r| r[0].clone()));
    }
    for f in files.iter().filter(|f| f.starts_with("edges_")) {
        for r in read_csv(&out.join(f)).1 {
            assert!(
                ids.contains(&r[0]) && ids.contains(&r[1]),
                "{f}: {r:?} names an unknown node"
            );
        }
    }

    // The list is JSON text in one field, and that is reported.
    let (header, rows) = read_csv(&out.join("nodes_Movie.csv"));
    let tags = header
        .iter()
        .position(|h| h == "tags:string")
        .expect("tags column");
    assert_eq!(rows[0][tags], r#"["sci-fi","action"]"#);
    assert!(report
        .output
        .dropped()
        .iter()
        .any(|d| d.what == "values_written_as_json"));
}

#[test]
fn a_csv_export_refuses_a_directory_that_is_not_empty() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture(dir.path());
    let out = dir.path().join("taken");
    std::fs::create_dir(&out).unwrap();
    std::fs::write(out.join("keep.txt"), b"mine").unwrap();

    let err = convert(&input, &out, &Options::default()).unwrap_err();
    assert!(
        matches!(err, ConvertError::Engine(ref m) if m.contains("not empty")),
        "{err}"
    );
    assert_eq!(std::fs::read(out.join("keep.txt")).unwrap(), b"mine");

    // An empty directory the caller made is fine.
    let empty = dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    convert(&input, &empty, &Options::default()).unwrap();
    assert!(empty.join("nodes_Movie.csv").exists());
}

/// True when `dir` holds no staging entry (`*.tmp`) left by a conversion.
fn no_staging_left(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .all(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
}

/// `require_lossless` refuses a lossy GraphML or CSV export before it is
/// published (review on #1942). The fixture's `flag` attribute is a boolean on
/// one node and an integer on another, so both exports are lossy every time.
#[test]
fn a_refused_lossy_export_leaves_the_destination_as_it_was() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture(dir.path());
    let strict = Options {
        require_lossless: true,
        ..Options::default()
    };

    // GraphML: an existing file keeps its bytes, an absent one stays absent.
    let existing = dir.path().join("existing.graphml");
    std::fs::write(&existing, b"previous contents").unwrap();
    let err = convert(&input, &existing, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert_eq!(std::fs::read(&existing).unwrap(), b"previous contents");
    let absent = dir.path().join("absent.graphml");
    let err = convert(&input, &absent, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert!(!absent.exists());

    // CSV: no directory appears, and an empty one the caller made survives.
    let csv = dir.path().join("csv_out");
    let err = convert(&input, &csv, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert!(!csv.exists());
    let empty = dir.path().join("csv_empty");
    std::fs::create_dir(&empty).unwrap();
    let err = convert(&input, &empty, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert!(empty.is_dir());

    assert!(no_staging_left(dir.path()));
}

/// A file or directory named `<output>.tmp` belongs to the user, not to the
/// conversion: it is neither deleted, truncated nor reused (review on #1942).
#[test]
fn an_existing_dot_tmp_beside_the_output_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let input = fixture(dir.path());

    let csv = dir.path().join("graph");
    let csv_tmp = dir.path().join("graph.tmp");
    std::fs::create_dir(&csv_tmp).unwrap();
    std::fs::write(csv_tmp.join("notes.txt"), b"mine").unwrap();
    convert(&input, &csv, &Options::default()).unwrap();
    assert!(csv.join("nodes_Movie.csv").exists());
    assert_eq!(std::fs::read(csv_tmp.join("notes.txt")).unwrap(), b"mine");

    let graphml = dir.path().join("g.graphml");
    let graphml_tmp = dir.path().join("g.graphml.tmp");
    std::fs::write(&graphml_tmp, b"mine").unwrap();
    convert(&input, &graphml, &Options::default()).unwrap();
    assert!(graphml.exists());
    assert_eq!(std::fs::read(&graphml_tmp).unwrap(), b"mine");
}

/// A property name with `:` cannot be written in `neo4j-admin`'s header
/// (`a:b` would read as property `a` of type `b`), so the column is left out
/// and reported, and `require_lossless` refuses (review on #1942).
#[test]
fn a_property_name_with_a_colon_is_left_out_and_reported() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = GraphStore::new();
    let n = s.create_node("Thing");
    s.set_node_property("default", n, "plain", "kept").unwrap();
    s.set_node_property("default", n, "ns:name", "omitted")
        .unwrap();
    let input = dir.path().join("colon.sgsnap");
    samyama::snapshot::write_file(&s, &input, &Default::default()).unwrap();

    let out = dir.path().join("csv");
    let report = convert(&input, &out, &Options::default()).unwrap();
    let (header, rows) = read_csv(&out.join("nodes_Thing.csv"));
    assert!(header.iter().any(|h| h == "plain:string"), "{header:?}");
    assert!(!header.iter().any(|h| h.contains("ns")), "{header:?}");
    assert!(!rows[0].iter().any(|c| c == "omitted"), "{rows:?}");
    let omitted = report
        .output
        .dropped()
        .iter()
        .find(|d| d.what == "properties_with_a_colon_omitted")
        .expect("reported");
    assert_eq!(omitted.count, 1);
    assert!(omitted.detail.contains("ns:name"), "{}", omitted.detail);

    let strict = Options {
        require_lossless: true,
        ..Options::default()
    };
    let refused = dir.path().join("csv_strict");
    let err = convert(&input, &refused, &strict).unwrap_err();
    assert!(matches!(err, ConvertError::Lossy(_)), "{err}");
    assert!(!refused.exists());
}

/// A column mixing integers and floats is `double` only when every integer is
/// exact as one. Past 2^53 an integer would round on import, so the column is
/// written as text, digits exact, and the widening is reported (review on
/// #1942).
#[test]
fn an_integer_a_double_cannot_hold_keeps_its_column_as_exact_text() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = GraphStore::new();
    let big = 9_007_199_254_740_993i64; // 2^53 + 1
    let a = s.create_node("Reading");
    s.set_node_property("default", a, "v", big).unwrap();
    let b = s.create_node("Reading");
    s.set_node_property("default", b, "v", 1.5f64).unwrap();
    // Small integers beside a float still make a `double` column.
    let c = s.create_node("Small");
    s.set_node_property("default", c, "v", 3i64).unwrap();
    let d = s.create_node("Small");
    s.set_node_property("default", d, "v", 1.5f64).unwrap();
    let input = dir.path().join("big.sgsnap");
    samyama::snapshot::write_file(&s, &input, &Default::default()).unwrap();

    let out = dir.path().join("csv");
    let report = convert(&input, &out, &Options::default()).unwrap();
    let (header, rows) = read_csv(&out.join("nodes_Reading.csv"));
    assert!(header.iter().any(|h| h == "v:string"), "{header:?}");
    assert!(
        rows.iter()
            .any(|r| r.iter().any(|c| c == "9007199254740993")),
        "the digits are written exactly: {rows:?}"
    );
    let (small_header, _) = read_csv(&out.join("nodes_Small.csv"));
    assert!(
        small_header.iter().any(|h| h == "v:double"),
        "{small_header:?}"
    );
    let widened = report
        .output
        .dropped()
        .iter()
        .find(|d| d.what == "columns_widened_to_string")
        .expect("the widening is reported");
    assert!(
        widened.detail.contains("nodes_Reading.csv:v"),
        "{}",
        widened.detail
    );

    let strict = Options {
        require_lossless: true,
        ..Options::default()
    };
    let refused = dir.path().join("csv_strict");
    assert!(matches!(
        convert(&input, &refused, &strict),
        Err(ConvertError::Lossy(_))
    ));
    assert!(!refused.exists());
}

/// An empty string and an absent property are different fields: `""` and
/// nothing at all, which is how `neo4j-admin` tells them apart (review on
/// #1942). A null cannot be stored, so absent is the only "no value".
#[test]
fn an_empty_string_is_quoted_and_an_absent_property_is_left_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = GraphStore::new();
    let full = s.create_node("Tag");
    s.set_node_property("default", full, "id", 1i64).unwrap();
    s.set_node_property("default", full, "note", "x").unwrap();
    let empty = s.create_node("Tag");
    s.set_node_property("default", empty, "id", 2i64).unwrap();
    s.set_node_property("default", empty, "note", "").unwrap();
    let absent = s.create_node("Tag");
    s.set_node_property("default", absent, "id", 3i64).unwrap();
    let input = dir.path().join("empty.sgsnap");
    samyama::snapshot::write_file(&s, &input, &Default::default()).unwrap();

    let out = dir.path().join("csv");
    convert(&input, &out, &Options::default()).unwrap();
    let text = std::fs::read_to_string(out.join("nodes_Tag.csv")).unwrap();
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().unwrap().split(',').collect();
    let (id_col, note_col) = (
        header.iter().position(|h| h.starts_with("id:")).unwrap(),
        header.iter().position(|h| h.starts_with("note:")).unwrap(),
    );
    let note_by_id: BTreeMap<String, String> = lines
        .map(|l| {
            let cells: Vec<&str> = l.split(',').collect();
            (cells[id_col].to_string(), cells[note_col].to_string())
        })
        .collect();
    assert_eq!(note_by_id["1"], "x", "{text}");
    assert_eq!(note_by_id["2"], "\"\"", "an empty string is quoted: {text}");
    assert_eq!(
        note_by_id["3"], "",
        "an absent property is an empty field: {text}"
    );
}

/// Label groups and relationship types whose file names would coincide get
/// files of their own, so each keeps its own column types (review on #1942):
/// `A_B`, `A B` and `A`+`B` all make `nodes_A_B`, and `R_S` and `R S` both
/// make `edges_R_S`.
#[test]
fn groups_whose_file_names_coincide_get_separate_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = GraphStore::new();
    let under = s.create_node("A_B");
    s.set_node_property("default", under, "x", 1i64).unwrap();
    let spaced = s.create_node("A B");
    s.set_node_property("default", spaced, "x", true).unwrap();
    let both = s.create_node("A");
    s.add_label_to_node("default", both, "B").unwrap();
    s.set_node_property("default", both, "x", 2.5f64).unwrap();
    let e1 = s.create_edge(under, spaced, "R_S").unwrap();
    s.set_edge_property(e1, "w", 1i64).unwrap();
    let e2 = s.create_edge(spaced, both, "R S").unwrap();
    s.set_edge_property(e2, "w", "heavy").unwrap();
    let input = dir.path().join("collide.sgsnap");
    samyama::snapshot::write_file(&s, &input, &Default::default()).unwrap();

    let out = dir.path().join("csv");
    let report = convert(&input, &out, &Options::default()).unwrap();
    assert!(
        !report
            .output
            .dropped()
            .iter()
            .any(|d| d.what == "columns_widened_to_string"),
        "no table mixes two groups' types: {:?}",
        report.output.dropped()
    );

    // Each file holds one group; find it by its :LABEL / :TYPE cell.
    let mut by_group: BTreeMap<String, String> = BTreeMap::new();
    for entry in std::fs::read_dir(&out).unwrap() {
        let path = entry.unwrap().path();
        let (header, rows) = read_csv(&path);
        assert_eq!(rows.len(), 1, "{path:?}: one row per group");
        let group_col = header
            .iter()
            .position(|h| h == ":LABEL" || h == ":TYPE")
            .unwrap();
        let typed: Vec<&String> = header.iter().filter(|h| !h.starts_with(':')).collect();
        by_group.insert(
            rows[0][group_col].clone(),
            typed
                .iter()
                .map(|h| h.as_str())
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    assert_eq!(by_group.get("A_B").map(String::as_str), Some("x:long"));
    assert_eq!(by_group.get("A B").map(String::as_str), Some("x:boolean"));
    assert_eq!(by_group.get("A;B").map(String::as_str), Some("x:double"));
    assert_eq!(by_group.get("R_S").map(String::as_str), Some("w:long"));
    assert_eq!(by_group.get("R S").map(String::as_str), Some("w:string"));
    assert_eq!(by_group.len(), 5);
}
