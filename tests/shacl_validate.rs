//! `CALL rdf.shacl.validate` (#2100, part 3): each supported SHACL constraint
//! finds exactly the nodes that break it, a conforming graph yields no rows,
//! and SHACL it does not check is refused rather than passed.

use std::collections::HashMap;

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

const PREFIXES: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> . \
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . \
    @prefix c: <https://samyama.ai/rdf/class/> . \
    @prefix p: <https://samyama.ai/rdf/prop/> . \
    @prefix r: <https://samyama.ai/rdf/rel/> . ";

struct Fixture {
    store: GraphStore,
    names: HashMap<String, String>,
}

fn person(store: &mut GraphStore, names: &mut HashMap<String, String>, who: &str, props: &[(&str, PropertyValue)]) -> NodeId {
    let id = store.create_node("Person");
    for (k, v) in props {
        store.get_node_mut(id).unwrap().set_property(*k, v.clone());
    }
    names.insert(format!("https://samyama.ai/rdf/node/{}", id.as_u64()), who.to_string());
    id
}

/// Alice is complete; Bob has no name and a negative age; Carol's age is text;
/// Alice knows two people and a company; nobody knows Alice.
fn fixture() -> Fixture {
    let mut store = GraphStore::new();
    let mut names = HashMap::new();
    let s = |v: &str| PropertyValue::String(v.to_string());
    let alice = person(&mut store, &mut names, "Alice", &[
        ("name", s("Alice")), ("age", PropertyValue::Integer(30)), ("email", s("alice@example.org")), ("country", s("NL")),
    ]);
    let bob = person(&mut store, &mut names, "Bob", &[
        ("age", PropertyValue::Integer(-5)), ("email", s("bob.example.org")), ("country", s("IN")),
    ]);
    let carol = person(&mut store, &mut names, "Carol", &[
        ("name", s("Carol")), ("age", s("old")), ("email", s("carol@example.org")), ("country", s("FR")),
    ]);
    let acme = store.create_node("Company");
    names.insert(format!("https://samyama.ai/rdf/node/{}", acme.as_u64()), "Acme".into());
    store.create_edge(alice, bob, "KNOWS").unwrap();
    store.create_edge(alice, carol, "KNOWS").unwrap();
    store.create_edge(alice, acme, "KNOWS").unwrap();
    store.create_edge(bob, carol, "KNOWS").unwrap();
    Fixture { store, names }
}

#[derive(Debug, PartialEq)]
struct Row {
    focus: String,
    path: Option<String>,
    severity: String,
    component: String,
    message: String,
}

fn validate(f: &Fixture, shapes: &str) -> Result<Vec<Row>, String> {
    let ttl = format!("{PREFIXES}{shapes}");
    let q = format!(
        "CALL rdf.shacl.validate('{ttl}') YIELD focus_node, result_path, severity, source_constraint, message \
         RETURN focus_node, result_path, severity, source_constraint, message"
    );
    let query = parse_query(&q).map_err(|e| e.to_string())?;
    let batch = QueryExecutor::new(&f.store).execute(&query).map_err(|e| e.to_string())?;
    let text = |v: Option<&Value>| match v {
        Some(Value::Property(PropertyValue::String(s))) => Some(s.clone()),
        _ => None,
    };
    Ok(batch
        .records
        .iter()
        .map(|r| {
            let focus = text(r.get("focus_node")).unwrap();
            Row {
                focus: f.names.get(&focus).cloned().unwrap_or(focus),
                path: text(r.get("result_path")),
                severity: text(r.get("severity")).unwrap(),
                component: text(r.get("source_constraint")).unwrap(),
                message: text(r.get("message")).unwrap(),
            }
        })
        .collect())
}

/// The names of the nodes that failed, sorted.
fn failing(f: &Fixture, shapes: &str) -> Vec<String> {
    let mut v: Vec<String> = validate(f, shapes).unwrap().into_iter().map(|r| r.focus).collect();
    v.sort();
    v
}

fn person_shape(property: &str) -> String {
    format!("c:PersonShape a sh:NodeShape ; sh:targetClass c:Person ; sh:property [ {property} ] .")
}

#[test]
fn a_graph_that_meets_every_shape_yields_no_rows() {
    let f = fixture();
    let shapes = person_shape("sh:path p:email ; sh:minCount 1 ; sh:maxCount 1 ; sh:datatype xsd:string");
    assert_eq!(validate(&f, &shapes).unwrap(), vec![]);
}

#[test]
fn each_constraint_reports_exactly_the_nodes_that_break_it() {
    let f = fixture();
    let cases: &[(&str, &[&str])] = &[
        ("sh:path p:name ; sh:minCount 1", &["Bob"]),
        ("sh:path p:age ; sh:datatype xsd:integer", &["Carol"]),
        ("sh:path p:age ; sh:minInclusive 0", &["Bob", "Carol"]),
        ("sh:path p:age ; sh:maxExclusive 30", &["Alice", "Carol"]),
        // 30 >= 4 as numbers, though "30" < "4" as text.
        ("sh:path p:age ; sh:minInclusive 4", &["Bob", "Carol"]),
        ("sh:path p:email ; sh:pattern \"^[^@]+@[^@]+$\"", &["Bob"]),
        ("sh:path p:email ; sh:minLength 16", &["Bob"]),
        ("sh:path p:name ; sh:maxLength 4", &["Alice", "Carol"]),
        ("sh:path p:country ; sh:in ( \"NL\" \"IN\" )", &["Carol"]),
        ("sh:path p:country ; sh:hasValue \"NL\"", &["Bob", "Carol"]),
        ("sh:path r:KNOWS ; sh:maxCount 2", &["Alice"]),
        ("sh:path r:KNOWS ; sh:class c:Person", &["Alice"]),
        ("sh:path r:KNOWS ; sh:nodeKind sh:Literal", &["Alice", "Alice", "Alice", "Bob"]),
        ("sh:path [ sh:inversePath r:KNOWS ] ; sh:minCount 1", &["Alice"]),
    ];
    for (property, expected) in cases {
        assert_eq!(failing(&f, &person_shape(property)), *expected, "{property}");
    }
}

#[test]
fn a_row_names_the_path_the_constraint_and_the_shapes_severity_and_message() {
    let f = fixture();
    let shapes = person_shape(
        "sh:path p:name ; sh:minCount 1 ; sh:severity sh:Warning ; sh:message \"Every person needs a name\"",
    );
    assert_eq!(
        validate(&f, &shapes).unwrap(),
        vec![Row {
            focus: "Bob".into(),
            path: Some("https://samyama.ai/rdf/prop/name".into()),
            severity: "Warning".into(),
            component: "sh:MinCountConstraintComponent".into(),
            message: "Every person needs a name".into(),
        }]
    );
    let inverse = validate(&f, &person_shape("sh:path [ sh:inversePath r:KNOWS ] ; sh:minCount 1")).unwrap();
    assert_eq!(inverse[0].path.as_deref(), Some("^https://samyama.ai/rdf/rel/KNOWS"));
    assert_eq!(inverse[0].severity, "Violation", "the default severity");
}

#[test]
fn targets_select_the_focus_nodes() {
    let f = fixture();
    // Subjects of KNOWS: Alice and Bob; only Bob has no name.
    let by_subjects = "c:S a sh:NodeShape ; sh:targetSubjectsOf r:KNOWS ; sh:property [ sh:path p:name ; sh:minCount 1 ] .";
    assert_eq!(failing(&f, by_subjects), ["Bob"]);
    // Objects of KNOWS include Acme, which is not a Person.
    let by_objects = "c:S a sh:NodeShape ; sh:targetObjectsOf r:KNOWS ; sh:class c:Person .";
    assert_eq!(failing(&f, by_objects), ["Acme"]);
    // A shape with no target validates nothing (SHACL 2.1.3).
    let untargeted = "c:S a sh:NodeShape ; sh:property [ sh:path p:name ; sh:minCount 1 ] .";
    assert_eq!(failing(&f, untargeted), Vec::<String>::new());
    let off = person_shape("sh:path p:name ; sh:minCount 1 ; sh:deactivated true");
    assert_eq!(failing(&f, &off), Vec::<String>::new());
}

#[test]
fn shacl_it_does_not_check_is_refused_not_passed() {
    let f = fixture();
    for (shapes, named) in [
        (person_shape("sh:path p:name ; sh:node c:Other"), "sh:node"),
        ("c:S a sh:NodeShape ; sh:targetClass c:Person ; sh:closed true .".to_string(), "sh:closed"),
        (person_shape("sh:path ( p:name p:x ) ; sh:minCount 1"), "inversePath"),
        ("c:S a sh:NodeShape ; sh:targetClass ".to_string(), "Turtle"),
        ("c:S c:p c:o .".to_string(), "no SHACL shapes"),
    ] {
        let err = validate(&f, &shapes).unwrap_err();
        assert!(err.contains(named), "expected an error naming {named}, got: {err}");
    }
}

#[test]
fn yield_can_rename_and_choose_columns() {
    let f = fixture();
    let q = format!(
        "CALL rdf.shacl.validate('{PREFIXES}{}') YIELD focus_node AS who RETURN who",
        person_shape("sh:path p:name ; sh:minCount 1")
    );
    let batch = QueryExecutor::new(&f.store).execute(&parse_query(&q).unwrap()).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(batch.records[0].get("who").is_some());
    let bad = format!("CALL rdf.shacl.validate('{PREFIXES}') YIELD nope RETURN nope");
    let err = QueryExecutor::new(&f.store).execute(&parse_query(&bad).unwrap()).unwrap_err().to_string();
    assert!(err.contains("focus_node"), "{err}");
}
