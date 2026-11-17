//! The line about trace export goes through the logger, not around it (#2077).
//!
//! `otel::from_env` ran before the subscriber was installed and reported with
//! `eprintln!`, so under `--log-format json` the server's first stderr line
//! was plain text in a stream promised to be JSON. A collector parsing stderr
//! met it on every start. Found by CH-OBS's live probe (benchmarks#254), which
//! starts the binary and reads its stderr -- so this does too.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Start the server under `--log-format <format>` with the OTLP endpoint at
/// `otlp`, wait for it to answer, ask it one query, stop it, and return what it
/// wrote to `(stdout, stderr)`. JSON logs go to stderr; text logs to stdout.
fn output_of(format: &str, otlp: Option<&str>) -> (String, String) {
    let (http, resp) = (free_port(), free_port());
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_samyama"));
    cmd.args(["--ephemeral", "--port", &resp.to_string(), "--http-port", &http.to_string(),
              "--log-format", format])
        .current_dir(tempfile::tempdir().unwrap().keep())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for k in ["SAMYAMA_LOG_FORMAT", "OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
              "RUST_LOG"] {
        cmd.env_remove(k);
    }
    if let Some(url) = otlp {
        cmd.env("OTEL_EXPORTER_OTLP_ENDPOINT", url);
    }
    let mut child: Child = cmd.spawn().expect("spawn samyama");
    let drain = |mut r: Box<dyn Read + Send>| {
        std::thread::spawn(move || {
            let mut s = String::new();
            r.read_to_string(&mut s).ok();
            s
        })
    };
    let out_reader = drain(Box::new(child.stdout.take().unwrap()));
    let err_reader = drain(Box::new(child.stderr.take().unwrap()));

    let deadline = Instant::now() + Duration::from_secs(60);
    let answered = loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", http)) {
            let body = r#"{"query":"RETURN 1 AS one"}"#;
            let req = format!(
                "POST /api/query HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            if s.write_all(req.as_bytes()).is_ok() {
                let mut r = String::new();
                let _ = s.read_to_string(&mut r);
                if r.starts_with("HTTP/1.1 200") {
                    break true;
                }
            }
        }
        if Instant::now() > deadline || child.try_wait().unwrap().is_some() {
            break false;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let _ = child.kill();
    let _ = child.wait();
    let (out, err) = (out_reader.join().unwrap(), err_reader.join().unwrap());
    assert!(answered, "the server did not answer\n--- stdout\n{out}\n--- stderr\n{err}");
    (out, err)
}

#[test]
fn with_export_on_every_stderr_line_is_json_and_the_export_is_one_of_them() {
    // Nothing listens there; building the exporter does not connect.
    let (_, text) = output_of("json", Some(&format!("http://127.0.0.1:{}", free_port())));
    let lines: Vec<serde_json::Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}\n---\n{text}")))
        .collect();
    let started = lines
        .iter()
        .find(|l| l["fields"]["message"] == "OpenTelemetry traces export started")
        .unwrap_or_else(|| panic!("no export event in:\n{text}"));
    assert_eq!(started["level"], "INFO");
    assert!(
        started["fields"]["endpoint"].as_str().is_some_and(|e| e.ends_with("/v1/traces")),
        "the endpoint is a field, not prose: {started}"
    );
}

#[test]
fn with_export_on_the_text_log_still_says_so() {
    // Text logs are written to stdout, as they were before; the line is now
    // one of them rather than a stray on stderr.
    let (text, _) = output_of("text", Some(&format!("http://127.0.0.1:{}", free_port())));
    assert!(
        text.lines().any(|l| l.contains("OpenTelemetry traces export started") && l.contains("/v1/traces")),
        "{text}"
    );
}

#[test]
fn with_export_off_nothing_is_said_about_it() {
    let (_, text) = output_of("json", None);
    assert!(!text.contains("OpenTelemetry"), "{text}");
    for l in text.lines().filter(|l| !l.trim().is_empty()) {
        serde_json::from_str::<serde_json::Value>(l).unwrap_or_else(|e| panic!("not JSON ({e}): {l}"));
    }
}
