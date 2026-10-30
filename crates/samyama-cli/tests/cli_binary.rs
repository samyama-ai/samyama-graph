//! End-to-end tests of the `samyama-cli` binary against an in-process mock of
//! the server's HTTP API.
//!
//! The CLI's behaviour lives in `main()` and the `run_*` functions it calls,
//! which print to stdout/stderr and exit with a status code. The observable
//! contract is therefore the process: what it prints and how it exits. Each
//! test starts a tiny HTTP server on 127.0.0.1 that answers `/api/query`,
//! `/api/nlq` and `/api/status` with canned JSON, runs the compiled binary with
//! `--url` pointing at it, and checks the output.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

/// How the mock answers `/api/status`.
#[derive(Clone, Copy)]
enum StatusMode {
    Healthy,
    Degraded,
    ServerError,
}

fn query_body(query: &str) -> (u16, String) {
    if query.contains("FAIL") {
        return (400, r#"{"error":"bad query"}"#.to_string());
    }
    if query.contains("GARBLED") {
        return (500, "not json at all".to_string());
    }
    if query.contains("EMPTY") {
        return (
            200,
            r#"{"nodes":[],"edges":[],"columns":[],"records":[]}"#.to_string(),
        );
    }
    let body = serde_json::json!({
        "nodes": [],
        "edges": [],
        "columns": ["n", "r", "name", "age", "ok", "tags", "missing"],
        "records": [
            [
                {"id": "1", "labels": ["Person"], "properties": {}},
                {"id": "7", "type": "KNOWS", "source": "1", "target": "2"},
                "Smith, \"Al\"",
                42,
                true,
                ["a", "b"],
                null
            ],
            [
                {"k": "v"},
                {"id": "9"},
                "Bo",
                7,
                false,
                [],
                null
            ]
        ]
    });
    (200, body.to_string())
}

fn respond(mut stream: TcpStream, mode: StatusMode) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut content_length = 0usize;
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).unwrap_or(0) == 0 || h == "\r\n" {
            break;
        }
        let lower = h.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0u8; content_length];
    let _ = reader.read_exact(&mut body);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);

    let (code, payload) = if request_line.starts_with("GET /api/status") {
        match mode {
            StatusMode::Healthy => (
                200,
                r#"{"status":"healthy","version":"0.0.1","storage":{"nodes":3,"edges":2}}"#
                    .to_string(),
            ),
            StatusMode::Degraded => (
                200,
                r#"{"status":"degraded","version":"0.0.1","storage":{"nodes":0,"edges":0}}"#
                    .to_string(),
            ),
            StatusMode::ServerError => (503, r#"{"error":"down"}"#.to_string()),
        }
    } else if request_line.starts_with("POST /api/query") {
        query_body(json["query"].as_str().unwrap_or(""))
    } else if request_line.starts_with("POST /api/nlq") {
        let q = json["question"].as_str().unwrap_or("");
        if q.contains("bad") {
            (400, r#"{"error":"no provider configured"}"#.to_string())
        } else {
            (
                200,
                r#"{"cypher":"MATCH (n) RETURN n.name AS name"}"#.to_string(),
            )
        }
    } else {
        (404, r#"{"error":"not found"}"#.to_string())
    };
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Service Unavailable",
    };
    let resp = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.flush();
}

/// Start the mock and return its base URL. The thread lives until the test
/// process exits.
fn mock_server(mode: StatusMode) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || respond(stream, mode));
        }
    });
    format!("http://{addr}")
}

/// A URL on which nothing is listening.
fn dead_url() -> String {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    drop(l);
    format!("http://{addr}")
}

fn temp_home(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("samyama_cli_test_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cli(args: &[&str]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_samyama-cli"));
    c.args(args);
    c.env_remove("SAMYAMA_URL");
    c.env_remove("SAMYAMA_DATA");
    c
}

fn run(args: &[&str]) -> Output {
    cli(args).output().expect("run samyama-cli")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

// ─────────────────────────────────────────────────────────────── query

#[test]
fn query_as_a_table_renders_entities_compactly_and_counts_rows() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "query", "MATCH (n) RETURN n"]);
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    for want in [
        "name",
        r#"("1":["Person"])"#,
        r#"["7":"KNOWS"]"#,
        "Smith, \"Al\"",
        "42",
        "true",
        r#"["a","b"]"#,
        "null",
        r#"{"k":"v"}"#,
        r#"{"id":"9"}"#,
        "2 row(s)",
    ] {
        assert!(out.contains(want), "table lacks {want:?}:\n{out}");
    }
}

#[test]
fn query_as_json_prints_the_result_document() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&[
        "--url",
        &url,
        "--format",
        "json",
        "query",
        "--readonly",
        "MATCH (n) RETURN n",
    ]);
    assert!(o.status.success(), "{}", stderr(&o));
    let v: serde_json::Value = serde_json::from_str(&stdout(&o)).expect("stdout is JSON");
    assert_eq!(v["columns"][2], "name");
    assert_eq!(v["records"].as_array().unwrap().len(), 2);
}

#[test]
fn query_as_csv_quotes_what_needs_quoting() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&[
        "--url",
        &url,
        "--format",
        "csv",
        "query",
        "MATCH (n) RETURN n",
    ]);
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines[0], "n,r,name,age,ok,tags,missing");
    // A string with a comma and quotes is quoted with doubled quotes; a
    // structured value is JSON, quoted; null is empty.
    assert!(lines[1].contains(r#""Smith, ""Al""""#), "{}", lines[1]);
    assert!(
        lines[1].contains(r#",42,true,"[""a"",""b""]","#),
        "{}",
        lines[1]
    );
    assert!(lines[1].ends_with(','), "{}", lines[1]);
    assert!(lines[2].contains(",Bo,7,false,\"[]\","), "{}", lines[2]);
}

#[test]
fn a_query_with_no_columns_prints_no_results_or_nothing() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "query", "EMPTY"]);
    assert!(o.status.success());
    assert_eq!(stdout(&o).trim(), "(no results)");
    let o = run(&["--url", &url, "--format", "csv", "query", "EMPTY"]);
    assert!(o.status.success());
    assert_eq!(stdout(&o), "");
}

#[test]
fn a_failing_query_exits_one_with_the_servers_message() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "query", "FAIL"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(
        stderr(&o).contains("Error: Query error: bad query"),
        "{}",
        stderr(&o)
    );

    // With --format json the error is JSON too.
    let o = run(&["--url", &url, "--format", "json", "query", "FAIL"]);
    assert_eq!(o.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_str(&stderr(&o)).expect("stderr is JSON");
    assert_eq!(v["error"], "Query error: bad query");

    // A non-JSON error body falls back to a generic message.
    let o = run(&["--url", &url, "query", "GARBLED"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).contains("Unknown error"), "{}", stderr(&o));
}

// ─────────────────────────────────────────────────────────────── nlq

#[test]
fn nlq_prints_the_generated_cypher() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "nlq", "who is here?"]);
    assert!(o.status.success(), "{}", stderr(&o));
    assert_eq!(stdout(&o).trim(), "MATCH (n) RETURN n.name AS name");
}

#[test]
fn nlq_execute_prints_the_cypher_to_stderr_and_results_to_stdout() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&[
        "--url",
        &url,
        "--format",
        "csv",
        "nlq",
        "--execute",
        "who is here?",
    ]);
    assert!(o.status.success(), "{}", stderr(&o));
    assert!(stderr(&o).contains("Cypher: MATCH (n) RETURN n.name AS name"));
    assert!(stdout(&o).starts_with("n,r,name,"), "{}", stdout(&o));
}

#[test]
fn nlq_as_json_is_one_document_with_or_without_results() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "--format", "json", "nlq", "who?"]);
    let v: serde_json::Value = serde_json::from_str(&stdout(&o)).expect("JSON");
    assert_eq!(v["question"], "who?");
    assert_eq!(v["cypher"], "MATCH (n) RETURN n.name AS name");
    assert!(v.get("result").is_none());

    let o = run(&[
        "--url",
        &url,
        "--format",
        "json",
        "nlq",
        "--execute",
        "who?",
    ]);
    let v: serde_json::Value = serde_json::from_str(&stdout(&o)).expect("JSON");
    assert_eq!(v["result"]["columns"][2], "name");
}

#[test]
fn nlq_reports_the_servers_refusal() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "nlq", "bad question"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(
        stderr(&o).contains("no provider configured"),
        "{}",
        stderr(&o)
    );
}

// ────────────────────────────────────────────────────── status and ping

#[test]
fn status_in_both_formats() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "status"]);
    assert!(o.status.success());
    let out = stdout(&o);
    assert!(
        out.contains("Status:  healthy") && out.contains("Version: 0.0.1"),
        "{out}"
    );
    assert!(
        out.contains("Nodes:   3") && out.contains("Edges:   2"),
        "{out}"
    );

    let o = run(&["--url", &url, "--format", "json", "status"]);
    let v: serde_json::Value = serde_json::from_str(&stdout(&o)).expect("JSON");
    assert_eq!(v["storage"]["nodes"], 3);
}

#[test]
fn status_reports_a_server_error_code() {
    let url = mock_server(StatusMode::ServerError);
    let o = run(&["--url", &url, "status"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(
        stderr(&o).contains("Status endpoint returned 503"),
        "{}",
        stderr(&o)
    );
}

#[test]
fn ping_in_both_formats_and_against_an_unhealthy_server() {
    let url = mock_server(StatusMode::Healthy);
    let o = run(&["--url", &url, "ping"]);
    assert_eq!(stdout(&o).trim(), "PONG");
    let o = run(&["--url", &url, "--format", "json", "ping"]);
    let v: serde_json::Value = serde_json::from_str(&stdout(&o)).expect("JSON");
    assert_eq!(v["ping"], "PONG");

    let sick = mock_server(StatusMode::Degraded);
    let o = run(&["--url", &sick, "ping"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(
        stderr(&o).contains("Server unhealthy: degraded"),
        "{}",
        stderr(&o)
    );
}

// ─────────────────────────────────────────────────────────────── shell

fn shell(url: &str, home: &PathBuf, input: &str) -> Output {
    let mut child = cli(&["--url", url, "shell"])
        .env("HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shell");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn shell_runs_commands_and_queries_from_stdin() {
    let url = mock_server(StatusMode::Healthy);
    let home = temp_home("shell");
    let o = shell(
        &url,
        &home,
        "\n:help\n:ping\n:status\nMATCH (n) RETURN n\nFAIL\n:quit\nnever run\n",
    );
    assert!(o.status.success(), "{}", stderr(&o));
    let out = stdout(&o);
    for want in [
        "Samyama Interactive Shell (graph: default)",
        "Commands:",
        "PONG",
        "Status:  healthy",
        "2 row(s)",
        "Bye!",
    ] {
        assert!(out.contains(want), "shell output lacks {want:?}:\n{out}");
    }
    assert!(
        stderr(&o).contains("Error: Query error: bad query"),
        "{}",
        stderr(&o)
    );
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn shell_reports_server_errors_and_ends_at_eof() {
    let url = mock_server(StatusMode::ServerError);
    let home = temp_home("shell_err");
    let o = shell(&url, &home, ":status\n:ping\n");
    assert!(o.status.success());
    let err = stderr(&o);
    assert_eq!(
        err.matches("Status endpoint returned 503").count(),
        2,
        "{err}"
    );
    assert!(stdout(&o).contains("Bye!"));
    let _ = std::fs::remove_dir_all(&home);
}

// ────────────────────────────────────────────────────────────── doctor

#[test]
fn doctor_passes_against_a_healthy_server_and_prints_a_summary() {
    let url = mock_server(StatusMode::Healthy);
    let dir = temp_home("doctor");
    let o = run(&["--url", &url, "doctor", "--data-dir", dir.to_str().unwrap()]);
    let out = stdout(&o);
    assert!(
        out.contains("server reachable") && out.contains("answered, version 0.0.1"),
        "{out}"
    );
    assert!(out.contains("exists and is writable"), "{out}");
    assert!(out.contains("check(s):"), "{out}");
    // The mock reports 0.0.1, which differs from the client's version: a
    // warning, not a failure.
    assert_eq!(o.status.code(), Some(0), "{out}");
    assert!(out.contains("differ before the patch level"), "{out}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn doctor_json_reports_an_unreachable_server_as_a_failure() {
    let url = dead_url();
    let dir = temp_home("doctor_json");
    let missing = dir.join("not-yet");
    let o = run(&[
        "--url",
        &url,
        "--format",
        "json",
        "doctor",
        "--data-dir",
        missing.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(1));
    let v: serde_json::Value = serde_json::from_str(&stdout(&o)).expect("JSON");
    assert_eq!(v["failed"], 1);
    let checks = v["checks"].as_array().unwrap();
    let find = |n: &str| checks.iter().find(|c| c["name"] == n).unwrap().clone();
    assert_eq!(find("server reachable")["verdict"], "fail");
    assert_eq!(find("version match")["verdict"], "skipped");
    assert_eq!(find("data directory")["verdict"], "pass");
    assert!(find("data directory")["detail"]
        .as_str()
        .unwrap()
        .contains("does not exist yet"));
    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────────────────────────────────────── completions

#[test]
fn completions_print_a_script_for_the_installed_binary() {
    let o = run(&["completions", "bash"]);
    assert!(o.status.success());
    let out = stdout(&o);
    assert!(out.contains("samyama-cli"), "{out}");
}

#[test]
fn an_unreachable_server_is_a_connection_error() {
    let o = run(&["--url", &dead_url(), "query", "MATCH (n) RETURN n"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(stderr(&o).starts_with("Error: "), "{}", stderr(&o));
}
