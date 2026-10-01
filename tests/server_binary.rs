//! The `samyama` server binary, run as a process.
//!
//! The subcommands exit with a status, and so does every start-up path that
//! refuses a configuration. The one start-up path that would serve forever is
//! made to stop by holding its RESP port: the listener cannot bind, `main`
//! returns, and the process exits normally. That lets the whole start-up
//! sequence -- demo data, recovery, quotas, the embed pipeline, the indexer --
//! run for real without leaving a server behind.
//!
//! Runs with persistence open that are not about a refusal stop that way too.
//! A refusal after persistence opens used to segfault about one run in ten,
//! because `std::process::exit` raced RocksDB's background threads (#1577);
//! `a_refusal_with_persistence_open_exits_with_its_status` covers that.
//!
//! Every run gets a fresh working directory, so the default `./samyama_data`
//! never lands in the repository, and a bounded wait: a run that does not exit
//! is killed and fails the test rather than hanging it.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Longer than any run here takes in a debug build, short enough that a server
/// which failed to stop is reported rather than waited on.
const DEADLINE: Duration = Duration::from_secs(300);

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Run {
    fn assert_code(&self, code: i32) -> &Self {
        assert_eq!(
            self.code,
            Some(code),
            "unexpected exit status\n--- stdout\n{}\n--- stderr\n{}",
            self.stdout,
            self.stderr
        );
        self
    }
    fn out_has(&self, needle: &str) -> &Self {
        assert!(
            self.stdout.contains(needle),
            "stdout lacks {needle:?}:\n{}",
            self.stdout
        );
        self
    }
    fn err_has(&self, needle: &str) -> &Self {
        assert!(
            self.stderr.contains(needle),
            "stderr lacks {needle:?}:\n{}",
            self.stderr
        );
        self
    }
}

/// Environment the binary reads, removed so the host's settings cannot leak in.
const ENV: &[&str] = &[
    "SAMYAMA_CORS_ORIGINS",
    "SAMYAMA_TLS_CERT",
    "SAMYAMA_TLS_KEY",
    "SAMYAMA_AUDIT_LOG",
    "SAMYAMA_SNAPSHOT_KEY",
    "SAMYAMA_AUTH_FILE",
    "EMBED_ENABLED",
    "EMBED_PROVIDER",
    "EMBED_MODEL",
    "EMBED_API_KEY",
    "EMBED_API_BASE_URL",
    "EMBED_DIMENSION",
    "EMBED_CHUNK_SIZE",
    "EMBED_CHUNK_OVERLAP",
];

fn run_in(cwd: &Path, args: &[&str], env: &[(&str, &str)], stdin: Option<&str>) -> Run {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_samyama"));
    cmd.args(args)
        .current_dir(cwd)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for k in ENV {
        cmd.env_remove(k);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().expect("spawn the samyama binary");

    if let Some(input) = stdin {
        let mut pipe = child.stdin.take().unwrap();
        pipe.write_all(input.as_bytes()).unwrap();
    }
    let mut out = child.stdout.take().unwrap();
    let mut err = child.stderr.take().unwrap();
    let out_t = std::thread::spawn(move || {
        let mut s = String::new();
        out.read_to_string(&mut s).ok();
        s
    });
    let err_t = std::thread::spawn(move || {
        let mut s = String::new();
        err.read_to_string(&mut s).ok();
        s
    });

    let start = Instant::now();
    let status = loop {
        match child.try_wait().expect("wait on the samyama binary") {
            Some(status) => break status,
            None if start.elapsed() > DEADLINE => {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "samyama {args:?} did not exit within {DEADLINE:?}\n--- stdout\n{}",
                    out_t.join().unwrap()
                );
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    };
    Run {
        code: status.code(),
        stdout: out_t.join().unwrap(),
        stderr: err_t.join().unwrap(),
    }
}

fn run(args: &[&str], env: &[(&str, &str)]) -> Run {
    let cwd = tempfile::tempdir().unwrap();
    run_in(cwd.path(), args, env, None)
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

/// A port nothing is listening on at the moment of asking.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

// ───────────────────────────────────────────────────────────── subcommands

#[test]
fn snapshot_key_prints_a_key_that_read_key_accepts() {
    let r = run(&["snapshot-key"], &[]);
    r.assert_code(0).out_has("--snapshot-key");
    let key = r
        .stdout
        .lines()
        .find(|l| !l.starts_with('#'))
        .expect("a key line");
    assert_eq!(key.len(), 64, "{key:?}");
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()), "{key:?}");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k");
    std::fs::write(&path, key).unwrap();
    samyama::snapshot::encryption::read_key(&path).expect("the printed key reads back");
}

#[test]
fn auth_token_prints_a_credential_line_matching_its_token() {
    use sha2::{Digest, Sha256};
    let r = run(&["auth-token", "ci-bot"], &[]);
    r.assert_code(0);
    let lines: Vec<&str> = r
        .stdout
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    assert_eq!(lines.len(), 2, "{}", r.stdout);
    let (name, digest) = lines[0].split_once(':').expect("name:digest");
    assert_eq!(name, "ci-bot");
    let token = lines[1];
    assert_eq!(token.len(), 64);
    let expected: String = Sha256::digest(token.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        digest, expected,
        "the stored digest is not the digest of the token shown"
    );

    run(&["auth-token", "a:b"], &[])
        .assert_code(2)
        .err_has("cannot contain `:`");
}

#[test]
fn auth_user_hashes_a_password_read_from_stdin() {
    let cwd = tempfile::tempdir().unwrap();
    let r = run_in(
        cwd.path(),
        &["auth-user", "alice"],
        &[],
        Some("correct horse\n"),
    );
    r.assert_code(0).out_has("alice:$argon2id$");

    // The printed line is a credential the server accepts.
    let line = r.stdout.lines().find(|l| l.starts_with("alice:")).unwrap();
    let file = cwd.path().join("creds");
    std::fs::write(&file, line).unwrap();
    assert_eq!(samyama::auth::read_credentials(&file).unwrap().len(), 1);

    run_in(cwd.path(), &["auth-user", "alice"], &[], Some("\n"))
        .assert_code(2)
        .err_has("empty password");
}

#[test]
fn schema_prints_a_diagram_of_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = samyama::GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("City");
    store.create_edge(a, b, "LIVES_IN").unwrap();
    let snap = dir.path().join("s.sgsnap");
    samyama::snapshot::export_tenant(&store, std::fs::File::create(&snap).unwrap()).unwrap();

    run(&["schema", s(&snap)], &[])
        .assert_code(0)
        .out_has("LIVES_IN")
        .err_has("2 nodes, 1 edges");
    run(&["schema", s(&snap), "--markdown"], &[])
        .assert_code(0)
        .out_has("|");
    run(&["schema"], &[]).assert_code(2).err_has("usage");
}

#[test]
fn pii_scan_names_what_it_found() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = samyama::GraphStore::new();
    let a = store.create_node("Person");
    store
        .get_node_mut(a)
        .unwrap()
        .set_property("email", "alice.smith@example.com");
    let snap = dir.path().join("p.sgsnap");
    samyama::snapshot::export_tenant(&store, std::fs::File::create(&snap).unwrap()).unwrap();

    run(&["pii-scan", s(&snap)], &[])
        .assert_code(1)
        .out_has("FINDING")
        .out_has("Person.email");

    let waivers = dir.path().join("w.json");
    std::fs::write(
        &waivers,
        r##"[{"kind":"email","location":"Person.email","max_distinct":1,
             "why":"fixture","decided_in":"#test"},
            {"kind":"email","location":"Company.email","max_distinct":1,
             "why":"another artifact","decided_in":"#test"}]"##,
    )
    .unwrap();
    run(&["pii-scan", "--waivers", s(&waivers), s(&snap)], &[])
        .assert_code(0)
        .out_has("accepted")
        .out_has("1 waiver(s) matched nothing");
}

#[test]
fn catalog_build_verify_and_gate_work_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = samyama::GraphStore::new();
    for i in 0..3i64 {
        let n = store.create_node("Thing");
        store.get_node_mut(n).unwrap().set_property("id", i);
    }
    let snap = dir.path().join("t.sgsnap");
    samyama::snapshot::export_tenant(&store, std::fs::File::create(&snap).unwrap()).unwrap();
    let queries = dir.path().join("q.json");
    std::fs::write(
        &queries,
        r#"[{"id":"q_ids","question":"which ids?","difficulty":"easy",
             "cypher":"MATCH (t:Thing) RETURN t.id ORDER BY t.id"}]"#,
    )
    .unwrap();
    let catalog = dir.path().join("c.json");

    run(
        &[
            "catalog-build",
            s(&snap),
            "--queries",
            s(&queries),
            "--out",
            s(&catalog),
        ],
        &[],
    )
    .assert_code(0)
    .out_has("1 entries, private (not built with --release)");
    run(&["verify", s(&snap), "--queries", s(&catalog)], &[])
        .assert_code(0)
        .out_has("OK  1 entries reproduced");
    // Private by default (#1159): without --release the gate refuses it.
    run(&["catalog-gate", s(&catalog)], &[])
        .assert_code(1)
        .out_has("release: private, tenant: default")
        .out_has("REFUSED the catalog was not built for release");

    run(
        &[
            "catalog-build",
            s(&snap),
            "--queries",
            s(&queries),
            "--out",
            s(&catalog),
            "--release",
        ],
        &[],
    )
    .assert_code(0)
    .out_has("publishable (--release)");
    run(&["catalog-gate", s(&catalog)], &[])
        .assert_code(0)
        .out_has("release: publishable, tenant: default")
        .out_has("KG-08")
        .out_has("OK  publishable");

    // Against a different graph the same catalog fails, naming the entry.
    let other = dir.path().join("o.sgsnap");
    let mut two = samyama::GraphStore::new();
    let n = two.create_node("Thing");
    two.get_node_mut(n).unwrap().set_property("id", 99i64);
    samyama::snapshot::export_tenant(&two, std::fs::File::create(&other).unwrap()).unwrap();
    run(&["verify", s(&other), "--queries", s(&catalog)], &[])
        .assert_code(1)
        .out_has("FAIL q_ids")
        .out_has("1 of 1 entries failed");
}

#[test]
fn a_linked_catalog_is_found_by_verify_and_a_swapped_one_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = samyama::GraphStore::new();
    for i in 0..3i64 {
        let n = store.create_node("Thing");
        store.get_node_mut(n).unwrap().set_property("id", i);
    }
    let snap = dir.path().join("t.sgsnap");
    samyama::snapshot::export_tenant(&store, std::fs::File::create(&snap).unwrap()).unwrap();
    let queries = dir.path().join("q.json");
    std::fs::write(
        &queries,
        r#"[{"id":"q_ids","cypher":"MATCH (t:Thing) RETURN t.id ORDER BY t.id"}]"#,
    )
    .unwrap();
    let catalog = dir.path().join("t.sgqueries");

    let args = [
        "catalog-build",
        s(&snap),
        "--queries",
        s(&queries),
        "--out",
        s(&catalog),
        "--link",
        "--release",
    ];
    run(&args, &[]).assert_code(0).out_has("linked");
    run(&["verify", s(&snap)], &[])
        .assert_code(0)
        .out_has("sha256 matches the snapshot header")
        .out_has("OK  1 entries reproduced");
    run(&["catalog-gate", s(&catalog), "--snapshot", s(&snap)], &[])
        .assert_code(0)
        .out_has("names this catalog");

    std::fs::write(&catalog, std::fs::read_to_string(&catalog).unwrap() + " ").unwrap();
    run(&["verify", s(&snap)], &[])
        .assert_code(1)
        .err_has("not the catalog the snapshot was published with");

    std::fs::remove_file(&catalog).unwrap();
    run(&["verify", s(&snap)], &[])
        .assert_code(66)
        .err_has("names its catalog \"t.sgqueries\"");
}

#[test]
fn verify_says_when_every_entry_came_back_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = samyama::GraphStore::new();
    let snap = dir.path().join("e.sgsnap");
    samyama::snapshot::export_tenant(&store, std::fs::File::create(&snap).unwrap()).unwrap();
    let catalog = samyama::snapshot::verify::build_catalog(
        &store,
        &[
            serde_json::from_str(r#"{"id":"q_none","cypher":"MATCH (x:Absent) RETURN x"}"#)
                .unwrap(),
        ],
        &["q_none".to_string()],
    )
    .unwrap();
    let cat = dir.path().join("c.json");
    std::fs::write(&cat, serde_json::to_string(&catalog).unwrap()).unwrap();
    run(&["verify", s(&snap), "--queries", s(&cat)], &[])
        .assert_code(1)
        .out_has("every entry returned zero rows");
}

#[test]
fn catalog_gate_prints_its_findings_and_refusals() {
    let dir = tempfile::tempdir().unwrap();
    let cat = dir.path().join("c.json");
    std::fs::write(
        &cat,
        r#"{"format":"samyama.queries/1","generated_by":"test","provenance":"observed",
            "entries":[{"id":"q1","cypher":"MATCH (p {email: 'alice.smith@example.com'}) RETURN p",
                        "rows":1,"hash":"x"}]}"#,
    )
    .unwrap();
    let r = run(&["catalog-gate", s(&cat)], &[]);
    // A format this version does not read is refused at parse time; anything
    // else must refuse on provenance and on the address.
    if r.code == Some(65) {
        panic!("fixture catalog did not parse: {}", r.stderr);
    }
    r.assert_code(1).out_has("FINDING").out_has("REFUSED");
    // It predates the release stamp, and is refused for that too.
    r.out_has("release: unstamped").out_has("predates #1159");

    run(&["catalog-gate", s(&cat), "--kg08"], &[])
        .assert_code(1)
        .out_has("KG-08 problem(s), and --kg08 was given");
}

#[test]
fn an_observed_catalog_is_published_only_with_a_recorded_signoff() {
    let dir = tempfile::tempdir().unwrap();
    let cat = dir.path().join("c.json");
    std::fs::write(
        &cat,
        r#"{"format":"samyama.queries/1","generated_by":"test","provenance":"observed",
            "publishable":true,
            "entries":[{"id":"q1","cypher":"MATCH (t:Thing) RETURN t","rows":1,"hash":"x","work":1}]}"#,
    )
    .unwrap();
    run(&["catalog-gate", s(&cat)], &[])
        .assert_code(1)
        .out_has("REFUSED the catalog is derived from observed traffic");
    run(&["catalog-gate", s(&cat), "--allow-observed"], &[])
        .assert_code(64)
        .err_has("--allow-observed needs --signoff <text>");

    let sha = samyama::snapshot::verify::queries_sha256(&std::fs::read(&cat).unwrap());
    run(
        &[
            "catalog-gate",
            s(&cat),
            "--allow-observed",
            "--signoff",
            "approved by the KG owner",
        ],
        &[],
    )
    .assert_code(0)
    .out_has(&format!(
        "SIGNOFF Observed catalog sha256 {sha}: approved by the KG owner"
    ))
    .out_has("OK  publishable");
}

// ───────────────────────────────────────────────────────────── start-up refusals

#[test]
fn a_tls_certificate_without_a_key_is_refused() {
    run(&["--ephemeral", "--tls-cert", "cert.pem"], &[])
        .assert_code(1)
        .err_has("--tls-cert and --tls-key must be given together")
        // The demos run before the server configuration is read.
        .out_has("Total nodes: 2, edges: 1")
        .out_has("Found 2 persons");
    run(&["--ephemeral"], &[("SAMYAMA_TLS_KEY", "key.pem")])
        .assert_code(1)
        .err_has("must be given together");
}

#[test]
fn an_unreadable_tls_file_is_refused() {
    run(
        &[
            "--ephemeral",
            "--tls-cert",
            "absent-cert.pem",
            "--tls-key",
            "absent-key.pem",
        ],
        &[],
    )
    .assert_code(1)
    .err_has("cannot read absent-cert.pem");
}

#[test]
fn an_audit_log_that_cannot_be_opened_is_refused() {
    run(
        &["--ephemeral", "--audit-log", "no-such-dir/audit.log"],
        &[],
    )
    .assert_code(1)
    .err_has("FATAL: --audit-log no-such-dir/audit.log");
}

#[test]
fn an_unreadable_snapshot_key_is_refused() {
    run(&["--ephemeral", "--snapshot-key", "absent.key"], &[])
        .assert_code(1)
        .err_has("FATAL: --snapshot-key absent.key");
}

#[test]
fn an_unreadable_credential_file_is_refused() {
    run(&["--ephemeral"], &[("SAMYAMA_AUTH_FILE", "absent.creds")])
        .assert_code(1)
        .err_has("FATAL: --auth-file absent.creds");
}

#[test]
fn an_import_dir_that_does_not_exist_is_refused() {
    run(&["--ephemeral", "--import-dir", "no-such-import-dir"], &[])
        .assert_code(2)
        .err_has("--import-dir");
}

#[test]
fn a_quota_flag_without_a_number_is_refused() {
    run(&["--ephemeral", "--max-nodes", "--http-port", "1"], &[])
        .assert_code(2)
        .err_has("--max-nodes requires a value");
    run(&["--ephemeral", "--max-edges"], &[])
        .assert_code(2)
        .err_has("--max-edges requires a value");
    run(&["--ephemeral", "--max-memory-bytes", "lots"], &[])
        .assert_code(2)
        .err_has("--max-memory-bytes expects a number or `unlimited`, got \"lots\"");
}

#[test]
fn an_unknown_embed_provider_is_refused_after_quotas_apply() {
    run(
        &[
            "--ephemeral",
            "--max-nodes",
            "1_000",
            "--max-edges",
            "unlimited",
            "--max-memory-bytes",
            "none",
            "--max-storage-bytes",
            "4096",
            "--demo",
            "no-such-demo",
        ],
        &[("EMBED_ENABLED", "TRUE"), ("EMBED_PROVIDER", "openia")],
    )
    .assert_code(2)
    .err_has("Unknown --demo mode 'no-such-demo'")
    .err_has("unknown EMBED_PROVIDER \"openia\"")
    .out_has("Total nodes: 0");
}

#[test]
fn every_configured_file_is_loaded_before_a_later_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    std::fs::write(p.join("cert.pem"), "not checked until the listener starts").unwrap();
    std::fs::write(p.join("key.pem"), "not checked until the listener starts").unwrap();
    std::fs::write(p.join("snap.key"), "11".repeat(32)).unwrap();
    std::fs::write(
        p.join("creds"),
        "ops:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae\n",
    )
    .unwrap();
    std::fs::create_dir(p.join("imports")).unwrap();

    run_in(
        p,
        &[
            "--ephemeral",
            "--tls-cert",
            "cert.pem",
            "--tls-key",
            "key.pem",
            "--audit-log",
            "audit.log",
            "--snapshot-key",
            "snap.key",
            "--auth-file",
            "creds",
            "--import-dir",
            "imports",
            "--cors-origin",
            "https://a.example",
            "--max-nodes",
            "x",
        ],
        &[(
            "SAMYAMA_CORS_ORIGINS",
            "https://b.example, ,https://c.example",
        )],
        None,
    )
    .assert_code(2)
    .out_has("auditing state-changing requests to audit.log")
    .out_has("snapshots exported encrypted, key from snap.key")
    .out_has("1 credential(s) loaded from creds")
    .out_has("LOAD CSV enabled, reading under imports")
    .err_has("--max-nodes expects a number");
    assert!(p.join("audit.log").exists(), "the audit log was not opened");
}

// ───────────────────────────────────────────────────────────── demo data and recovery

#[test]
fn the_social_demo_is_loaded_into_a_persistent_server() {
    let data = tempfile::tempdir().unwrap();
    serve_until_the_resp_port_is_refused(&["--data-path", s(data.path()), "--demo", "social"], &[])
        .assert_code(0)
        .out_has("No persisted tenants found.")
        .out_has("Total nodes: 5270");
}

#[test]
fn the_large_demo_loads_a_graphalytics_dataset_from_the_working_directory() {
    let cwd = tempfile::tempdir().unwrap();
    let ds = cwd.path().join("data/graphalytics/wiki-Talk");
    std::fs::create_dir_all(&ds).unwrap();
    // Comments, blank lines and junk are skipped; `|` and whitespace both separate.
    std::fs::write(
        ds.join("wiki-Talk.v"),
        "# vertices\n1\n2\n\n3\nnot-a-number\n",
    )
    .unwrap();
    std::fs::write(
        ds.join("wiki-Talk.e"),
        "# edges\n1 2\n2|3|0.5\n3 1 heavy\n\n1\nx 2\n2 y\n1 99\n",
    )
    .unwrap();

    run_in(
        cwd.path(),
        &["--ephemeral", "--demo", "large", "--max-nodes", "?"],
        &[],
        None,
    )
    .assert_code(2)
    .out_has("Loading LDBC Graphalytics: wiki-Talk...")
    .out_has("Loaded 3 vertices, 3 edges (directed)")
    .out_has("Total nodes: 3")
    .out_has("Total edges: 3");
}

#[test]
fn the_large_demo_uses_the_flat_layout_and_reports_a_missing_dataset() {
    let cwd = tempfile::tempdir().unwrap();
    run_in(
        cwd.path(),
        &["--ephemeral", "--demo", "large", "--max-nodes", "?"],
        &[],
        None,
    )
    .assert_code(2)
    .out_has("Dataset 'wiki-Talk' not found");

    let flat = cwd.path().join("data/graphalytics");
    std::fs::create_dir_all(&flat).unwrap();
    std::fs::write(flat.join("wiki-Talk.v"), "7\n8\n").unwrap();
    std::fs::write(flat.join("wiki-Talk.e"), "7 8\n").unwrap();
    run_in(
        cwd.path(),
        &["--ephemeral", "--demo", "large", "--max-nodes", "?"],
        &[],
        None,
    )
    .assert_code(2)
    .out_has("Loaded 2 vertices, 1 edges (directed)");
}

#[test]
fn a_restart_recovers_persisted_rows_and_their_indexes() {
    use samyama::persistence::PersistenceManager;
    let data = tempfile::tempdir().unwrap();
    {
        let pm = PersistenceManager::new(data.path()).unwrap();
        pm.tenants()
            .create_tenant("default".into(), "default".into(), None)
            .ok();
        let mut store = samyama::GraphStore::new();
        store.enable_write_log();
        for q in [
            "CREATE (:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Bo'})",
            "CREATE INDEX ON :Person(name)",
        ] {
            samyama::QueryEngine::new()
                .execute_mut(q, &mut store, "default")
                .unwrap_or_else(|e| panic!("{q}: {e}"));
            let muts = store.take_write_log();
            pm.apply_mutations("default", &store, &muts).unwrap();
        }
        pm.persist_index_catalog("default", &store).unwrap();
        pm.checkpoint().unwrap();
    }

    serve_until_the_resp_port_is_refused(&["--data-path", s(data.path()), "--demo", "social"], &[])
        .assert_code(0)
        .out_has("Recovering data for 1 tenant(s)")
        .out_has("Tenant 'default': 2 nodes, 1 edges")
        .out_has("Recovery complete. Total: 2 nodes, 1 edges, 1 indexes in-memory")
        // Recovered data wins over the demo.
        .out_has("Total nodes: 2\n");
}

#[test]
fn a_recovered_edge_whose_endpoint_is_gone_is_skipped_with_a_warning() {
    use samyama::persistence::PersistenceManager;
    let data = tempfile::tempdir().unwrap();
    {
        let pm = PersistenceManager::new(data.path()).unwrap();
        pm.tenants()
            .create_tenant("default".into(), "default".into(), None)
            .ok();
        let mut rows = samyama::GraphStore::new();
        rows.enable_write_log();
        let q = "CREATE (:Gone)-[:KNOWS]->(:Kept)";
        samyama::QueryEngine::new()
            .execute_mut(q, &mut rows, "default")
            .unwrap();
        let muts = rows.take_write_log();
        pm.apply_mutations("default", &rows, &muts).unwrap();
        // The source node's record is removed and its edge's is not.
        let gone = rows.get_nodes_by_label(&samyama::graph::Label::new("Gone"))[0].id;
        pm.persist_delete_node("default", gone.as_u64()).unwrap();
        pm.checkpoint().unwrap();
    }

    serve_until_the_resp_port_is_refused(&["--data-path", s(data.path())], &[])
        .assert_code(0)
        .out_has("Tenant 'default': 1 nodes, 1 edges")
        .err_has("Warning: edge recovery error")
        .out_has("Recovery complete. Total: 1 nodes, 0 edges");
}

#[test]
fn a_unique_constraint_the_recovered_rows_break_is_reported_not_restored() {
    use samyama::persistence::PersistenceManager;
    let data = tempfile::tempdir().unwrap();
    {
        let pm = PersistenceManager::new(data.path()).unwrap();
        pm.tenants()
            .create_tenant("default".into(), "default".into(), None)
            .ok();
        // Two rows with the same name, written without the constraint...
        let mut rows = samyama::GraphStore::new();
        rows.enable_write_log();
        let q = "CREATE (:Person {name: 'Ada'}), (:Person {name: 'Ada'})";
        samyama::QueryEngine::new()
            .execute_mut(q, &mut rows, "default")
            .unwrap();
        let muts = rows.take_write_log();
        pm.apply_mutations("default", &rows, &muts).unwrap();
        // ...and a catalog declaring a constraint they violate.
        let mut decl = samyama::GraphStore::new();
        samyama::QueryEngine::new()
            .execute_mut(
                "CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE",
                &mut decl,
                "default",
            )
            .unwrap();
        pm.persist_index_catalog("default", &decl).unwrap();
        pm.checkpoint().unwrap();
    }

    serve_until_the_resp_port_is_refused(&["--data-path", s(data.path())], &[])
        .assert_code(0)
        .out_has("Tenant 'default': 2 nodes, 0 edges")
        .err_has("1 index definition(s) for 'default' could not be rebuilt")
        // The constraint is refused; the property index declared with it is
        // not, and is the one index counted.
        .out_has("Recovery complete. Total: 2 nodes, 0 edges, 1 indexes in-memory");
}

#[test]
fn a_committed_snapshot_is_replayed_when_there_is_nothing_to_recover() {
    let data = tempfile::tempdir().unwrap();
    let mut store = samyama::GraphStore::new();
    let a = store.create_node("City");
    let b = store.create_node("City");
    store.create_edge(a, b, "ROAD").unwrap();
    let mut bytes = Vec::new();
    samyama::snapshot::export_tenant(&store, &mut bytes).unwrap();
    samyama::snapshot::persist::persist_snapshot(s(data.path()), &bytes).unwrap();

    serve_until_the_resp_port_is_refused(&["--data-path", s(data.path())], &[])
        .assert_code(0)
        .out_has("[snapshot-persist] Restored 2 nodes, 1 edges")
        .out_has("Total edges: 1");

    // A committed snapshot that does not parse is reported, and start-up goes on.
    let snaps = data.path().join("snapshots");
    for entry in std::fs::read_dir(&snaps).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "sgsnap") {
            std::fs::write(&path, "corrupt").unwrap();
        }
    }
    serve_until_the_resp_port_is_refused(&["--data-path", s(data.path())], &[])
        .assert_code(0)
        .err_has("[snapshot-persist] Restore error")
        .out_has("Total nodes: 0");
}

/// Before #1577 a refusal taken with RocksDB open segfaulted about one run in
/// ten. Twenty runs catch that rate most of the time; each must exit with the
/// refusal's own status, never a signal.
#[test]
fn a_refusal_with_persistence_open_exits_with_its_status() {
    for _ in 0..20 {
        let data = tempfile::tempdir().unwrap();
        run(&["--data-path", s(data.path()), "--max-nodes", "?"], &[])
            .assert_code(2)
            .err_has("--max-nodes expects a number");
    }
    let data = tempfile::tempdir().unwrap();
    run(
        &["--data-path", s(data.path())],
        &[("EMBED_ENABLED", "true"), ("EMBED_PROVIDER", "bogus")],
    )
    .assert_code(2)
    .err_has("the provider is unusable");
}

#[test]
fn an_unusable_data_path_runs_without_persistence() {
    let cwd = tempfile::tempdir().unwrap();
    // A file where the directory should be.
    std::fs::write(cwd.path().join("taken"), "").unwrap();
    run_in(
        cwd.path(),
        &["--data-path", "taken/db", "--max-nodes", "?"],
        &[],
        None,
    )
    .assert_code(2)
    .err_has("Failed to initialize persistence");
}

// ───────────────────────────────────────────────────────────── the serving path

/// Hold the RESP port so the listener cannot bind: `main` then returns and the
/// process exits by itself, after running every step of start-up.
fn serve_until_the_resp_port_is_refused(extra: &[&str], env: &[(&str, &str)]) -> Run {
    let held = TcpListener::bind("127.0.0.1:0").unwrap();
    let resp_port = held.local_addr().unwrap().port().to_string();
    let http_port = free_port().to_string();
    let mut args = vec![
        "--host",
        "127.0.0.1",
        "--port",
        &resp_port,
        "--http-port",
        &http_port,
    ];
    args.extend_from_slice(extra);
    let r = run(&args, env);
    drop(held);
    r
}

#[test]
fn an_ephemeral_server_starts_every_component_before_it_binds() {
    serve_until_the_resp_port_is_refused(&["--ephemeral"], &[])
        .assert_code(0)
        .out_has("Samyama Graph Database v")
        .out_has("Server starting on 127.0.0.1:")
        .out_has("Server ready.")
        .err_has("Server error:");
}

#[test]
fn a_persistent_server_with_credentials_starts_before_it_binds() {
    let data = tempfile::tempdir().unwrap();
    let creds = data.path().join("creds");
    std::fs::write(
        &creds,
        "ops:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae\n",
    )
    .unwrap();
    let db = data.path().join("db");
    serve_until_the_resp_port_is_refused(
        &["--data-path", s(&db), "--auth-file", s(&creds), "--max-nodes", "10"],
        &[("EMBED_ENABLED", "true"), ("EMBED_PROVIDER", "mock"), ("EMBED_DIMENSION", "8")],
    )
    .assert_code(0)
    .out_has("1 credential(s) loaded")
    .out_has("Global embed pipeline: model=text-embedding-3-small dimension=8 chunk_size=512 chunk_overlap=64")
    .out_has("Server ready.")
    .err_has("Server error:");
}

#[test]
fn an_embed_pipeline_that_cannot_be_built_is_a_warning_not_a_refusal() {
    serve_until_the_resp_port_is_refused(
        &["--ephemeral"],
        &[
            ("EMBED_ENABLED", "true"),
            ("EMBED_PROVIDER", "azure"),
            ("EMBED_CHUNK_SIZE", "256"),
            ("EMBED_CHUNK_OVERLAP", "16"),
        ],
    )
    .assert_code(0)
    .err_has("Warning: failed to build embed pipeline from EMBED_* vars")
    .out_has("Server ready.");
}

// ───────────────────────────────────────────────────────────── a real crash

/// One RESP reply, as far as these tests need to read one.
#[derive(Debug)]
// Status and error payloads are read through `Debug`, in failure messages.
#[allow(dead_code)]
enum Reply {
    Status(String),
    Error(String),
    Int(i64),
    Bulk(Option<String>),
    Array(Vec<Reply>),
    Null,
}

fn read_reply(r: &mut impl std::io::BufRead) -> std::io::Result<Reply> {
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let line = line.trim_end_matches("\r\n");
    let bad = || std::io::Error::new(std::io::ErrorKind::InvalidData, line.to_string());
    let (tag, rest) = line.split_at(1.min(line.len()));
    Ok(match tag {
        "+" => Reply::Status(rest.to_string()),
        "-" => Reply::Error(rest.to_string()),
        ":" => Reply::Int(rest.parse().map_err(|_| bad())?),
        "_" => Reply::Null,
        "$" => {
            let n: i64 = rest.parse().map_err(|_| bad())?;
            if n < 0 {
                Reply::Bulk(None)
            } else {
                let mut buf = vec![0u8; n as usize + 2];
                r.read_exact(&mut buf)?;
                buf.truncate(n as usize);
                Reply::Bulk(Some(String::from_utf8_lossy(&buf).into_owned()))
            }
        }
        "*" => {
            let n: i64 = rest.parse().map_err(|_| bad())?;
            let mut items = Vec::new();
            for _ in 0..n.max(0) {
                items.push(read_reply(r)?);
            }
            Reply::Array(items)
        }
        _ => return Err(bad()),
    })
}

/// A RESP connection that sends `GRAPH.QUERY default <cypher>`.
struct Resp {
    reader: std::io::BufReader<std::net::TcpStream>,
    writer: std::net::TcpStream,
}

impl Resp {
    fn connect(port: u16) -> std::io::Result<Self> {
        let stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(DEADLINE))?;
        Ok(Self {
            reader: std::io::BufReader::new(stream.try_clone()?),
            writer: stream,
        })
    }

    fn query(&mut self, cypher: &str) -> std::io::Result<Reply> {
        let mut frame = Vec::new();
        for arg in ["GRAPH.QUERY", "default", cypher] {
            frame.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
        let mut cmd = b"*3\r\n".to_vec();
        cmd.extend_from_slice(&frame);
        self.writer.write_all(&cmd)?;
        read_reply(&mut self.reader)
    }
}

/// A server on `data`, its output in files there, so an unread pipe cannot
/// stall it and a failure can show what it printed.
fn spawn_server(data: &Path, resp_port: u16) -> std::process::Child {
    let log = |name: &str| std::fs::File::create(data.join(name)).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_samyama"));
    cmd.args([
        "--host",
        "127.0.0.1",
        "--port",
        &resp_port.to_string(),
        "--http-port",
        &free_port().to_string(),
        "--data-path",
        s(&data.join("db")),
    ])
    .current_dir(data)
    .stdin(Stdio::null())
    .stdout(log("stdout.log"))
    .stderr(log("stderr.log"));
    for k in ENV {
        cmd.env_remove(k);
    }
    // The stock configuration: no fsync, so what survives is what the kernel
    // already had when the process died.
    cmd.env_remove("SAMYAMA_FSYNC");
    cmd.spawn().expect("spawn the samyama binary")
}

fn server_output(data: &Path) -> String {
    let read = |n: &str| std::fs::read_to_string(data.join(n)).unwrap_or_default();
    format!(
        "--- stdout\n{}\n--- stderr\n{}",
        read("stdout.log"),
        read("stderr.log")
    )
}

/// Poll until the server answers a query, for at most `DEADLINE`. Readiness is
/// the answer, not a sleep: a debug build recovering a store is slow, and how
/// slow depends on the host.
fn wait_until_serving(child: &mut std::process::Child, port: u16, data: &Path) -> Resp {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "the server exited ({status}) before serving\n{}",
                server_output(data)
            );
        }
        if let Ok(mut c) = Resp::connect(port) {
            if let Ok(Reply::Array(_)) = c.query("RETURN 1") {
                return c;
            }
        }
        if start.elapsed() > DEADLINE {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "the server did not serve within {DEADLINE:?}\n{}",
                server_output(data)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// REL-15's "process killed mid-write" and "recovery after a real crash"
/// (#1311), as a `cargo test` rather than a script run by hand.
///
/// A writer issues `CREATE (:Crash {seq: i})` over RESP, with a `SET` on every
/// fifth node, and records each write the server acknowledged. Once enough are
/// acknowledged the server gets `SIGKILL` -- no shutdown path runs -- while the
/// writer is still sending, so the signal lands wherever it lands. A restart on
/// the same directory must then hold every acknowledged `CREATE` and every
/// acknowledged `SET`, and nothing that was never issued.
///
/// Correctness does not depend on when the kill lands: every acknowledged write
/// is checked whatever the count is. The one write in flight at the kill is
/// neither acknowledged nor refused, and may or may not survive.
///
/// What recovery reads is RocksDB, which replays its own log on open. The
/// server does not replay samyama's `wal/` directory at start-up at all, so
/// this is not a test of `Wal::replay`.
#[cfg(unix)]
#[test]
fn every_acknowledged_write_survives_a_sigkill_mid_write() {
    use std::os::unix::process::ExitStatusExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// Acknowledged writes to wait for before the kill: enough that the
    /// recovered store is not trivially small, few enough for a debug build.
    const ACKED_BEFORE_KILL: usize = 150;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let port = free_port();
    let mut server = spawn_server(data, port);
    let mut conn = wait_until_serving(&mut server, port, data);

    let acked_creates = Arc::new(Mutex::new(Vec::<i64>::new()));
    let acked_sets = Arc::new(Mutex::new(Vec::<i64>::new()));
    let acked = Arc::new(AtomicUsize::new(0));
    let issued = Arc::new(AtomicUsize::new(0));
    let writer = {
        let (creates, sets) = (acked_creates.clone(), acked_sets.clone());
        let (acked, issued) = (acked.clone(), issued.clone());
        std::thread::spawn(move || -> String {
            for seq in 1i64.. {
                issued.store(seq as usize, Ordering::SeqCst);
                match conn.query(&format!("CREATE (:Crash {{seq: {seq}}})")) {
                    Ok(Reply::Array(_)) => creates.lock().unwrap().push(seq),
                    Ok(other) => return format!("CREATE {seq} was refused: {other:?}"),
                    // The connection died with the server.
                    Err(_) => return String::new(),
                }
                if seq % 5 == 0 {
                    let q = format!("MATCH (n:Crash {{seq: {seq}}}) SET n.touched = true");
                    match conn.query(&q) {
                        Ok(Reply::Array(_)) => sets.lock().unwrap().push(seq),
                        Ok(other) => return format!("SET {seq} was refused: {other:?}"),
                        Err(_) => return String::new(),
                    }
                }
                acked.fetch_add(1, Ordering::SeqCst);
            }
            unreachable!()
        })
    };

    let start = Instant::now();
    while acked.load(Ordering::SeqCst) < ACKED_BEFORE_KILL {
        if writer.is_finished() || start.elapsed() > DEADLINE {
            let _ = server.kill();
            let _ = server.wait();
            panic!(
                "the writer stopped at {} acknowledged writes: {:?}\n{}",
                acked.load(Ordering::SeqCst),
                writer.join(),
                server_output(data)
            );
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // `Child::kill` is SIGKILL on Unix. The status is checked below, so a
    // server that exited some other way cannot pass for a crash.
    server.kill().unwrap();
    let status = server.wait().unwrap();
    let refusal = writer.join().unwrap();
    assert!(refusal.is_empty(), "{refusal}\n{}", server_output(data));
    assert_eq!(
        status.signal(),
        Some(9),
        "the server was not killed: {status}"
    );

    let creates = acked_creates.lock().unwrap().clone();
    let sets = acked_sets.lock().unwrap().clone();
    let last_issued = issued.load(Ordering::SeqCst) as i64;
    assert!(creates.len() >= ACKED_BEFORE_KILL, "{}", creates.len());

    let mut restarted = spawn_server(data, port);
    let mut conn = wait_until_serving(&mut restarted, port, data);
    let rows = conn.query("MATCH (n:Crash) RETURN n.seq AS seq, n.touched AS touched ORDER BY seq");
    let _ = restarted.kill();
    let _ = restarted.wait();
    let Ok(Reply::Array(rows)) = rows else {
        panic!(
            "the recovered store did not answer: {rows:?}\n{}",
            server_output(data)
        );
    };

    let mut survived = Vec::new();
    let mut touched = Vec::new();
    for row in rows.iter().skip(1) {
        let Reply::Array(cells) = row else {
            panic!("row {row:?}")
        };
        let Reply::Int(seq) = cells[0] else {
            panic!("seq {row:?}")
        };
        survived.push(seq);
        if matches!(&cells[1], Reply::Bulk(Some(b)) if b == "true") {
            touched.push(seq);
        }
    }

    println!(
        "acknowledged {} CREATEs and {} SETs, last issued {last_issued}, recovered {} nodes",
        creates.len(),
        sets.len(),
        survived.len()
    );
    let lost: Vec<i64> = creates
        .iter()
        .filter(|s| !survived.contains(s))
        .copied()
        .collect();
    assert!(
        lost.is_empty(),
        "{} of {} acknowledged CREATEs were lost: {lost:?}",
        lost.len(),
        creates.len()
    );
    let lost_sets: Vec<i64> = sets
        .iter()
        .filter(|s| !touched.contains(s))
        .copied()
        .collect();
    assert!(
        lost_sets.is_empty(),
        "acknowledged SETs lost: {lost_sets:?}"
    );
    // Nothing that was never issued, and each node once: the in-flight write
    // may be present, anything beyond it may not.
    assert!(
        survived.iter().all(|&s| (1..=last_issued).contains(&s)),
        "a node the writer never issued: {survived:?} (last issued {last_issued})"
    );
    let mut unique = survived.clone();
    unique.dedup();
    assert_eq!(unique.len(), survived.len(), "a node recovered twice");
}
