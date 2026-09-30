//! Unit tests for the helpers in the server binary (`src/main.rs`).
//!
//! The subcommands take `argv` as a slice, so each is driven directly with the
//! arguments an operator would type and judged by its exit status and by the
//! files it writes. The server start-up path reads the process's own arguments
//! and never returns, so it is exercised by `tests/server_binary.rs` instead.

use super::*;
use samyama::graph::{Label, PropertyMap};
use samyama::snapshot::publish_gate::Provenance;
use samyama::snapshot::verify::{build_catalog, QueryCatalog, QuerySpec};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

fn argv(args: &[&str]) -> Vec<String> {
    std::iter::once("samyama")
        .chain(args.iter().copied())
        .map(String::from)
        .collect()
}

fn s(p: &Path) -> &str {
    p.to_str().expect("utf-8 temp path")
}

/// Twelve `Thing`s in a chain, and nothing that looks like personal data.
fn things() -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..12i64 {
        let mut props = PropertyMap::new();
        props.insert("id".into(), PropertyValue::Integer(i));
        props.insert("name".into(), PropertyValue::String(format!("n{i:02}")));
        ids.push(store.create_node_with_properties("default", vec![Label::new("Thing")], props));
    }
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "LINKS").unwrap();
    }
    store
}

/// Two people with e-mail addresses, one knowing the other.
fn people() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    store
        .get_node_mut(a)
        .unwrap()
        .set_property("email", "alice.smith@example.com");
    let b = store.create_node("Person");
    store
        .get_node_mut(b)
        .unwrap()
        .set_property("email", "bob.jones@example.org");
    store.create_edge(a, b, "KNOWS").unwrap();
    store
}

fn write_snapshot(dir: &Path, name: &str, store: &GraphStore) -> PathBuf {
    let path = dir.join(name);
    let f = File::create(&path).unwrap();
    samyama::snapshot::export_tenant(store, f).unwrap();
    path
}

fn spec(id: &str, cypher: &str) -> QuerySpec {
    QuerySpec {
        id: id.into(),
        question: format!("question for {id}"),
        paraphrases: vec![],
        difficulty: "easy".into(),
        cypher: cypher.into(),
        unanswerable: false,
        params: vec![],
    }
}

fn thing_queries() -> Vec<QuerySpec> {
    vec![
        spec("q_nodes", "MATCH (t:Thing) RETURN count(t) AS n"),
        spec("q_names", "MATCH (t:Thing) RETURN t.name ORDER BY t.name"),
    ]
}

/// `QuerySpec` is read from JSON but not written to it.
fn specs_json(specs: &[QuerySpec]) -> serde_json::Value {
    specs
        .iter()
        .map(|q| {
            serde_json::json!({
                "id": q.id, "question": q.question, "difficulty": q.difficulty,
                "cypher": q.cypher, "unanswerable": q.unanswerable,
            })
        })
        .collect()
}

fn write_json<T: serde::Serialize>(dir: &Path, name: &str, v: &T) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string(v).unwrap()).unwrap();
    path
}

fn write_catalog(dir: &Path, name: &str, catalog: &QueryCatalog) -> PathBuf {
    write_json(dir, name, catalog)
}

fn authored_catalog() -> QueryCatalog {
    let mut c = build_catalog(&things(), &thing_queries(), &[]).unwrap();
    c.provenance = Provenance::Authored;
    c
}

// ───────────────────────────────────────────────────────────── pii-scan

#[test]
fn pii_scan_without_a_snapshot_is_a_usage_error() {
    assert_eq!(cmd_pii_scan(&argv(&["pii-scan"])), 2);
    // A path given only as the waiver file is not a snapshot to scan.
    assert_eq!(cmd_pii_scan(&argv(&["pii-scan", "--waivers", "w.json"])), 2);
}

#[test]
fn pii_scan_of_a_clean_snapshot_passes() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    assert_eq!(cmd_pii_scan(&argv(&["pii-scan", s(&snap)])), 0);
}

#[test]
fn pii_scan_of_a_snapshot_holding_emails_fails() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "people.sgsnap", &people());
    assert_eq!(cmd_pii_scan(&argv(&["pii-scan", s(&snap)])), 1);
}

#[test]
fn pii_scan_of_an_unreadable_snapshot_is_not_reported_clean() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent.sgsnap");
    assert_eq!(cmd_pii_scan(&argv(&["pii-scan", s(&missing)])), 2);

    // One unreadable file outranks a finding in another.
    let snap = write_snapshot(dir.path(), "people.sgsnap", &people());
    assert_eq!(cmd_pii_scan(&argv(&["pii-scan", s(&snap), s(&missing)])), 2);
}

#[test]
fn pii_scan_accepts_a_finding_its_waiver_covers() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "people.sgsnap", &people());
    let waivers = write_json(
        dir.path(),
        "waivers.json",
        &serde_json::json!([{
            "kind": "email", "location": "Person.email", "max_distinct": 2,
            "why": "synthetic fixture addresses", "decided_in": "#test"
        }]),
    );
    assert_eq!(
        cmd_pii_scan(&argv(&["pii-scan", "--waivers", s(&waivers), s(&snap)])),
        0
    );
}

#[test]
fn pii_scan_fails_when_more_values_turn_up_than_the_waiver_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "people.sgsnap", &people());
    let waivers = write_json(
        dir.path(),
        "waivers.json",
        &serde_json::json!([{
            "kind": "email", "location": "Person.email", "max_distinct": 1,
            "why": "one address was accepted", "decided_in": "#test"
        }]),
    );
    assert_eq!(
        cmd_pii_scan(&argv(&["pii-scan", "--waivers", s(&waivers), s(&snap)])),
        1
    );
}

#[test]
fn pii_scan_with_a_waiver_that_matches_nothing_still_passes_a_clean_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let waivers = write_json(
        dir.path(),
        "waivers.json",
        &serde_json::json!([{
            "kind": "email", "location": "Person.email", "max_distinct": 5,
            "why": "belongs to another artifact", "decided_in": "#test"
        }]),
    );
    assert_eq!(
        cmd_pii_scan(&argv(&["pii-scan", "--waivers", s(&waivers), s(&snap)])),
        0
    );
}

#[test]
fn pii_scan_refuses_a_waiver_file_it_cannot_use() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "people.sgsnap", &people());

    let missing = dir.path().join("absent.json");
    assert_eq!(
        cmd_pii_scan(&argv(&["pii-scan", "--waivers", s(&missing), s(&snap)])),
        2
    );

    let reasonless = write_json(
        dir.path(),
        "reasonless.json",
        &serde_json::json!([{
            "kind": "email", "location": "Person.email", "max_distinct": 2,
            "why": " ", "decided_in": "#test"
        }]),
    );
    assert_eq!(
        cmd_pii_scan(&argv(&["pii-scan", "--waivers", s(&reasonless), s(&snap)])),
        2
    );
}

// ───────────────────────────────────────────────────────────── schema

#[test]
fn schema_without_a_snapshot_is_a_usage_error() {
    assert_eq!(cmd_schema(&argv(&["schema"])), 2);
    assert_eq!(cmd_schema(&argv(&["schema", "--markdown"])), 2);
}

#[test]
fn schema_of_a_snapshot_succeeds_in_both_formats() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    assert_eq!(cmd_schema(&argv(&["schema", s(&snap)])), 0);
    assert_eq!(cmd_schema(&argv(&["schema", s(&snap), "--markdown"])), 0);
}

#[test]
fn schema_of_a_missing_snapshot_fails() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        cmd_schema(&argv(&["schema", s(&dir.path().join("absent.sgsnap"))])),
        2
    );
}

#[test]
fn schema_of_a_snapshot_with_a_dangling_edge_still_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let gz = write_snapshot(dir.path(), "people.sgsnap", &people());

    // Decompress and drop the first node record, so the KNOWS edge points at a
    // node the file does not contain.
    let mut text = String::new();
    flate2::read::GzDecoder::new(File::open(&gz).unwrap())
        .read_to_string(&mut text)
        .unwrap();
    let mut dropped = false;
    let kept: Vec<&str> = text
        .lines()
        .filter(|l| {
            let is_node = serde_json::from_str::<serde_json::Value>(l)
                .ok()
                .and_then(|v| v.get("t").and_then(|t| t.as_str()).map(|t| t == "n"))
                .unwrap_or(false);
            if is_node && !dropped {
                dropped = true;
                return false;
            }
            true
        })
        .collect();
    assert!(dropped, "the fixture has no node record to drop");
    let plain = dir.path().join("dangling.sgsnap");
    let mut f = File::create(&plain).unwrap();
    writeln!(f, "{}", kept.join("\n")).unwrap();
    drop(f);

    let schema = samyama::schema_doc::derive_from_path(&plain).unwrap();
    assert_eq!(schema.dangling_edges, 1, "the fixture must actually dangle");
    assert_eq!(cmd_schema(&argv(&["schema", s(&plain)])), 0);
}

// ───────────────────────────────────────────────────────────── keys and credentials

#[test]
fn snapshot_key_succeeds() {
    assert_eq!(cmd_snapshot_key(), 0);
}

#[test]
fn auth_token_accepts_the_default_and_a_named_credential() {
    assert_eq!(cmd_auth_token(&argv(&["auth-token"])), 0);
    assert_eq!(cmd_auth_token(&argv(&["auth-token", "ci-bot"])), 0);
}

#[test]
fn auth_token_refuses_a_name_containing_the_separator() {
    assert_eq!(cmd_auth_token(&argv(&["auth-token", "a:b"])), 2);
}

#[test]
fn auth_user_without_a_name_is_a_usage_error() {
    assert_eq!(cmd_auth_user(&argv(&["auth-user"])), 2);
}

#[test]
fn auth_user_refuses_a_name_containing_the_separator() {
    assert_eq!(cmd_auth_user(&argv(&["auth-user", "al:ice"])), 2);
}

// ───────────────────────────────────────────────────────────── verify

#[test]
fn verify_without_a_snapshot_or_catalog_is_a_usage_error() {
    assert_eq!(cmd_verify(&argv(&["verify"])), 64);
    assert_eq!(cmd_verify(&argv(&["verify", "--queries", "c.json"])), 64);
    assert_eq!(cmd_verify(&argv(&["verify", "snap.sgsnap"])), 64);
}

#[test]
fn verify_reports_an_unreadable_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let missing = dir.path().join("absent.json");
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&missing)])),
        65
    );

    let garbage = dir.path().join("garbage.json");
    std::fs::write(&garbage, "not json").unwrap();
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&garbage)])),
        65
    );
}

#[test]
fn verify_reports_a_missing_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let cat = write_catalog(dir.path(), "c.json", &authored_catalog());
    let missing = dir.path().join("absent.sgsnap");
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&missing), "--queries", s(&cat)])),
        66
    );
}

#[test]
fn verify_fails_when_the_snapshot_cannot_be_restored() {
    let dir = tempfile::tempdir().unwrap();
    let cat = write_catalog(dir.path(), "c.json", &authored_catalog());
    let corrupt = dir.path().join("corrupt.sgsnap");
    std::fs::write(&corrupt, "this is not a snapshot").unwrap();
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&corrupt), "--queries", s(&cat)])),
        1
    );
}

#[test]
fn verify_refuses_a_catalog_of_an_unknown_format() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let mut c = authored_catalog();
    c.format = "samyama.queries/99".into();
    let cat = write_catalog(dir.path(), "c.json", &c);
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&cat)])),
        65
    );
}

#[test]
fn verify_passes_a_snapshot_that_reproduces_its_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let cat = write_catalog(dir.path(), "c.json", &authored_catalog());
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&cat)])),
        0
    );
}

#[test]
fn verify_fails_a_snapshot_whose_answers_differ() {
    let dir = tempfile::tempdir().unwrap();
    // The catalog describes `things()`; the snapshot holds only people.
    let snap = write_snapshot(dir.path(), "people.sgsnap", &people());
    let cat = write_catalog(dir.path(), "c.json", &authored_catalog());
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&cat)])),
        1
    );
}

#[test]
fn verify_fails_a_run_in_which_every_entry_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let c = build_catalog(
        &things(),
        &[spec("q_none", "MATCH (x:Absent) RETURN x")],
        &["q_none".to_string()],
    )
    .unwrap();
    let cat = write_catalog(dir.path(), "c.json", &c);
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&cat)])),
        1
    );
}

// ───────────────────────────────────────────────────────────── catalog-build

#[test]
fn catalog_build_without_its_arguments_is_a_usage_error() {
    assert_eq!(cmd_catalog_build(&argv(&["catalog-build"])), 64);
    assert_eq!(
        cmd_catalog_build(&argv(&["catalog-build", "--queries", "q.json"])),
        64
    );
    assert_eq!(
        cmd_catalog_build(&argv(&["catalog-build", "s.sgsnap", "--queries", "q.json"])),
        64
    );
    assert_eq!(
        cmd_catalog_build(&argv(&["catalog-build", "s.sgsnap", "--out", "c.json"])),
        64
    );
}

#[test]
fn catalog_build_writes_a_catalog_that_verify_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let queries = write_json(dir.path(), "q.json", &specs_json(&thing_queries()));
    let out = dir.path().join("catalog.json");
    assert_eq!(
        cmd_catalog_build(&argv(&[
            "catalog-build",
            s(&snap),
            "--queries",
            s(&queries),
            "--out",
            s(&out)
        ])),
        0
    );
    let catalog: QueryCatalog =
        serde_json::from_reader(File::open(&out).unwrap()).expect("a catalog was written");
    let ids: Vec<&str> = catalog.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, ["q_nodes", "q_names"]);
    assert_eq!(catalog.entries[1].rows, 12);
    assert_eq!(
        cmd_verify(&argv(&["verify", s(&snap), "--queries", s(&out)])),
        0
    );
}

#[test]
fn catalog_build_reports_unreadable_queries() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let out = dir.path().join("catalog.json");
    let missing = dir.path().join("absent.json");
    assert_eq!(
        cmd_catalog_build(&argv(&[
            "catalog-build",
            s(&snap),
            "--queries",
            s(&missing),
            "--out",
            s(&out)
        ])),
        65
    );
    assert!(!out.exists());
}

#[test]
fn catalog_build_reports_a_missing_or_corrupt_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let queries = write_json(dir.path(), "q.json", &specs_json(&thing_queries()));
    let out = dir.path().join("catalog.json");
    let missing = dir.path().join("absent.sgsnap");
    assert_eq!(
        cmd_catalog_build(&argv(&[
            "catalog-build",
            s(&missing),
            "--queries",
            s(&queries),
            "--out",
            s(&out)
        ])),
        66
    );
    let corrupt = dir.path().join("corrupt.sgsnap");
    std::fs::write(&corrupt, "this is not a snapshot").unwrap();
    assert_eq!(
        cmd_catalog_build(&argv(&[
            "catalog-build",
            s(&corrupt),
            "--queries",
            s(&queries),
            "--out",
            s(&out)
        ])),
        1
    );
    assert!(!out.exists());
}

#[test]
fn catalog_build_refuses_a_query_that_returns_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let queries = write_json(
        dir.path(),
        "q.json",
        &specs_json(&[spec("q_none", "MATCH (x:Absent) RETURN x")]),
    );
    let out = dir.path().join("catalog.json");
    assert_eq!(
        cmd_catalog_build(&argv(&[
            "catalog-build",
            s(&snap),
            "--queries",
            s(&queries),
            "--out",
            s(&out)
        ])),
        1
    );
    assert!(!out.exists());
}

#[test]
fn catalog_build_reports_an_output_it_cannot_write() {
    let dir = tempfile::tempdir().unwrap();
    let snap = write_snapshot(dir.path(), "things.sgsnap", &things());
    let queries = write_json(dir.path(), "q.json", &specs_json(&thing_queries()));
    let out = dir.path().join("no-such-dir").join("catalog.json");
    assert_eq!(
        cmd_catalog_build(&argv(&[
            "catalog-build",
            s(&snap),
            "--queries",
            s(&queries),
            "--out",
            s(&out)
        ])),
        74
    );
}

// ───────────────────────────────────────────────────────────── catalog-gate

#[test]
fn catalog_gate_without_a_catalog_is_a_usage_error() {
    assert_eq!(cmd_catalog_gate(&argv(&["catalog-gate"])), 64);
    assert_eq!(
        cmd_catalog_gate(&argv(&["catalog-gate", "--allow-observed"])),
        64
    );
}

#[test]
fn catalog_gate_reports_an_unreadable_catalog() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        cmd_catalog_gate(&argv(&["catalog-gate", s(&dir.path().join("absent.json"))])),
        65
    );
}

#[test]
fn catalog_gate_publishes_a_clean_authored_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let cat = write_catalog(dir.path(), "c.json", &authored_catalog());
    assert_eq!(cmd_catalog_gate(&argv(&["catalog-gate", s(&cat)])), 0);
}

#[test]
fn catalog_gate_enforces_kg08_only_when_asked() {
    let dir = tempfile::tempdir().unwrap();
    // Two entries and no unanswerable one: short of KG-08 on two counts.
    let cat = write_catalog(dir.path(), "c.json", &authored_catalog());
    assert_eq!(
        cmd_catalog_gate(&argv(&["catalog-gate", s(&cat), "--kg08"])),
        1
    );
}

#[test]
fn catalog_gate_refuses_an_observed_catalog_without_the_flag() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = authored_catalog();
    c.provenance = Provenance::Observed;
    let cat = write_catalog(dir.path(), "c.json", &c);
    assert_eq!(cmd_catalog_gate(&argv(&["catalog-gate", s(&cat)])), 1);
    assert_eq!(
        cmd_catalog_gate(&argv(&["catalog-gate", s(&cat), "--allow-observed"])),
        0
    );
}

#[test]
fn catalog_gate_refuses_personal_data_in_a_parameter_sample_even_with_the_flag() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = authored_catalog();
    c.entries[0].params = vec![serde_json::from_value(serde_json::json!({
        "name": "who", "type": "string", "sample": "alice.smith@example.com"
    }))
    .unwrap()];
    let cat = write_catalog(dir.path(), "c.json", &c);
    assert_eq!(cmd_catalog_gate(&argv(&["catalog-gate", s(&cat)])), 1);
    assert_eq!(
        cmd_catalog_gate(&argv(&["catalog-gate", s(&cat), "--allow-observed"])),
        1
    );
}

// ───────────────────────────────────────────────────────────── demo data

fn count(store: &GraphStore, q: &str) -> i64 {
    let batch = QueryEngine::new()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    assert_eq!(batch.len(), 1, "{q}");
    match batch.records[0].get("n").and_then(|v| v.as_property()) {
        Some(PropertyValue::Integer(n)) => *n,
        other => panic!("{q}: expected an integer count, got {other:?}"),
    }
}

#[test]
fn the_social_network_demo_has_the_documented_shape() {
    let mut store = GraphStore::new();
    build_social_network(&mut store);

    assert_eq!(store.node_count(), 5270);
    for (label, n) in [
        ("City", 30),
        ("Company", 20),
        ("Tag", 20),
        ("Person", 200),
        ("Post", 2000),
        ("Comment", 3000),
    ] {
        assert_eq!(
            count(&store, &format!("MATCH (x:{label}) RETURN count(x) AS n")),
            n,
            "{label}"
        );
    }
    for (ty, n) in [
        ("LOCATED_IN", 20),
        ("LIVES_IN", 200),
        ("WORKS_AT", 200),
        // One per post and one per comment.
        ("WROTE", 5000),
        // 1 + i % 3 tags for each of 2000 posts.
        ("HAS_TAG", 3999),
    ] {
        assert_eq!(
            count(
                &store,
                &format!("MATCH ()-[r:{ty}]->() RETURN count(r) AS n")
            ),
            n,
            "{ty}"
        );
    }
    // Every comment either comments on a post or replies to another comment.
    assert_eq!(
        count(
            &store,
            "MATCH (c:Comment)-[r:COMMENTED|REPLIED_TO]->() RETURN count(r) AS n"
        ),
        3000
    );
    assert!(count(&store, "MATCH ()-[r:KNOWS]->() RETURN count(r) AS n") > 1000);
    assert!(count(&store, "MATCH ()-[r:LIKES]->() RETURN count(r) AS n") >= 1000);

    // Properties on nodes and on the WORKS_AT edges.
    assert_eq!(
        count(
            &store,
            "MATCH (c:City {name: 'Tokyo', country: 'JP'}) RETURN count(c) AS n"
        ),
        1
    );
    assert_eq!(
        count(
            &store,
            "MATCH (:Person)-[w:WORKS_AT {role: 'Engineer'}]->(:Company) RETURN count(w) AS n"
        ),
        40
    );
    // A repeated topic gets a numbered title.
    assert_eq!(
        count(
            &store,
            "MATCH (p:Post) WHERE p.title ENDS WITH ' #2' RETURN count(p) AS n"
        ),
        20
    );
}

#[test]
fn a_graphalytics_dataset_that_is_not_there_loads_nothing() {
    let mut store = GraphStore::new();
    assert!(!load_graphalytics_dataset(
        &mut store,
        "no-such-dataset-for-tests",
        true,
        None
    ));
    assert_eq!(store.node_count(), 0);
}

#[test]
fn a_graphalytics_vertex_limit_caps_the_vertices_and_drops_their_edges() {
    // The loader reads `data/graphalytics` relative to the working directory,
    // which is process-wide; no other test here depends on it.
    let cwd = tempfile::tempdir().unwrap();
    let ds = cwd.path().join("data/graphalytics/tiny");
    std::fs::create_dir_all(&ds).unwrap();
    std::fs::write(ds.join("tiny.v"), "1\n2\n3\n4\n").unwrap();
    std::fs::write(ds.join("tiny.e"), "1 2 2.5\n2 3\n3 4\n").unwrap();

    let before = std::env::current_dir().unwrap();
    std::env::set_current_dir(cwd.path()).unwrap();
    let mut store = GraphStore::new();
    let loaded = load_graphalytics_dataset(&mut store, "tiny", false, Some(2));
    std::env::set_current_dir(before).unwrap();

    assert!(loaded);
    assert_eq!(store.node_count(), 2, "the limit caps the vertices read");
    assert_eq!(store.edge_count(), 1, "only 1-2 has both endpoints loaded");
    let batch = QueryEngine::new()
        .execute(
            "MATCH (a:Vertex)-[r:CONNECTS]->(b:Vertex) RETURN a.vid AS a, b.vid AS b, r.weight AS w, a.dataset AS d",
            &store,
        )
        .unwrap();
    assert_eq!(batch.len(), 1);
    let rec = &batch.records[0];
    assert_eq!(
        rec.get("a").and_then(|v| v.as_property()),
        Some(&PropertyValue::Integer(1))
    );
    assert_eq!(
        rec.get("b").and_then(|v| v.as_property()),
        Some(&PropertyValue::Integer(2))
    );
    assert_eq!(
        rec.get("w").and_then(|v| v.as_property()),
        Some(&PropertyValue::Float(2.5))
    );
    assert_eq!(
        rec.get("d").and_then(|v| v.as_property()),
        Some(&PropertyValue::String("tiny".into()))
    );
}

// ───────────────────────────────────────────────────────────── quotas

#[test]
fn no_quota_flag_leaves_the_shipped_defaults_alone() {
    // The test harness is not given any `--max-*` flag.
    assert_eq!(quota_arg("--max-nodes"), None);
    assert!(quota_overrides_from_args().is_none());
}
