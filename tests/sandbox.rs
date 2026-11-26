//! `samyama --sandbox` confines the server, and the server still works inside
//! it (#2218).
//!
//! Two halves, because a sandbox fails in two ways. Too loose, and an escape
//! works: so a confined process tries to read `/etc/passwd` and to start
//! `/bin/sh`, and the kernel must refuse both. Too tight, and the server
//! breaks hours later on a path nobody tested (the issue's "health=ok
//! confinement illusion"): so the server binary runs writes, an index build,
//! a restart on its own data and a snapshot export and re-import, all under
//! `--sandbox`.
//!
//! Landlock and seccomp are Linux features. Elsewhere `--sandbox` must refuse
//! to start rather than run unconfined, and that is what is checked there.

use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Child,
    time::{Duration, Instant},
};

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_samyama"));
    for k in ["NLQ_PROVIDER", "SAMYAMA_AUTH_FILE", "SAMYAMA_AUDIT_LOG", "SAMYAMA_TLS_CERT", "SAMYAMA_TLS_KEY",
              "SAMYAMA_SNAPSHOT_KEY", "OTEL_EXPORTER_OTLP_ENDPOINT"] {
        c.env_remove(k);
    }
    c.stdin(Stdio::null());
    c
}

// ── everywhere ──────────────────────────────────────────────────────────────

#[test]
fn a_provider_that_runs_a_program_is_refused_before_the_sandbox_starts() {
    let out = bin()
        .args(["--sandbox", "--ephemeral", "--port", "1", "--http-port", "1"])
        .env("NLQ_PROVIDER", "claudecode")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("claudecode") && err.contains("--sandbox"), "{err}");
}

#[cfg(not(target_os = "linux"))]
#[test]
fn off_linux_the_sandbox_refuses_to_start_rather_than_run_unconfined() {
    let out = bin().args(["--sandbox", "--ephemeral", "--port", "1", "--http-port", "1"]).output().unwrap();
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stderr).contains("not available"));
}

// ── Linux: escapes are refused ──────────────────────────────────────────────

/// Run in a child process (this test binary, re-run with an environment
/// variable), because confinement cannot be undone and would bind the test
/// runner itself.
#[cfg(target_os = "linux")]
#[test]
fn a_confined_process_cannot_read_outside_its_paths_or_start_a_program() {
    const CHILD: &str = "SAMYAMA_SANDBOX_TEST_CHILD";
    if let Ok(data) = std::env::var(CHILD) {
        let argv: Vec<String> = ["samyama", "--data-path", &data].iter().map(|s| s.to_string()).collect();
        let plan = samyama::sandbox::plan(&argv, |_| None).expect("plan");
        let applied = samyama::sandbox::apply(&plan).expect("apply");
        println!("applied landlock={} denied={}", applied.landlock, applied.denied_syscalls);
        // Inside: the data path works.
        std::fs::write(std::path::Path::new(&data).join("probe.txt"), "ok").expect("write inside the data path");
        // Outside: a file any user can normally read.
        match std::fs::read("/etc/passwd") {
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => println!("read-outside: refused"),
            other => println!("read-outside: ALLOWED {other:?}"),
        }
        match Command::new("/bin/sh").arg("-c").arg("true").status() {
            Err(e) if e.raw_os_error() == Some(libc_eperm()) || e.kind() == std::io::ErrorKind::PermissionDenied => {
                println!("exec: refused")
            }
            other => println!("exec: ALLOWED {other:?}"),
        }
        std::process::exit(0);
    }
    let data = tempfile::tempdir().unwrap();
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "a_confined_process_cannot_read_outside_its_paths_or_start_a_program", "--nocapture"])
        .env(CHILD, data.path())
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert!(text.contains("applied landlock="), "the sandbox did not apply:\n{text}");
    assert!(text.contains("read-outside: refused"), "{text}");
    assert!(text.contains("exec: refused"), "{text}");
    assert_eq!(std::fs::read_to_string(data.path().join("probe.txt")).unwrap(), "ok");
}

#[cfg(target_os = "linux")]
fn libc_eperm() -> i32 {
    1
}

// ── Linux: the server works inside it ───────────────────────────────────────

#[cfg(target_os = "linux")]
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

#[cfg(target_os = "linux")]
struct Server(Child, u16);

#[cfg(target_os = "linux")]
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(target_os = "linux")]
fn serve(data: &std::path::Path) -> Server {
    let (http, resp) = (free_port(), free_port());
    let child = bin()
        .args(["--sandbox", "--port", &resp.to_string(), "--http-port", &http.to_string(),
               "--data-path", data.to_str().unwrap()])
        .current_dir(data.parent().unwrap())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut s = Server(child, http);
    let deadline = Instant::now() + Duration::from_secs(90);
    while std::net::TcpStream::connect(("127.0.0.1", http)).is_err() {
        if let Ok(Some(status)) = s.0.try_wait() {
            let mut err = String::new();
            s.0.stderr.take().unwrap().read_to_string(&mut err).ok();
            panic!("the sandboxed server exited ({status}) before listening:\n{err}");
        }
        assert!(Instant::now() < deadline, "the sandboxed server did not start");
        std::thread::sleep(Duration::from_millis(100));
    }
    s
}

#[cfg(target_os = "linux")]
fn http(port: u16, method: &str, path: &str, ctype: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(s, "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
               Connection: close\r\n\r\n", body.len()).unwrap();
    s.write_all(body).unwrap();
    let mut r = Vec::new();
    s.read_to_end(&mut r).unwrap();
    let split = r.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&r[..split]).to_string();
    let code = head.get(9..12).and_then(|c| c.parse().ok()).unwrap_or(0);
    let mut body = r[split + 4..].to_vec();
    if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        body = dechunk(&body);
    }
    (code, body)
}

#[cfg(target_os = "linux")]
fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let nl = b.windows(2).position(|w| w == b"\r\n").unwrap();
        let n = usize::from_str_radix(std::str::from_utf8(&b[..nl]).unwrap().trim(), 16).unwrap();
        if n == 0 {
            return out;
        }
        out.extend_from_slice(&b[nl + 2..nl + 2 + n]);
        b = &b[nl + 2 + n + 2..];
    }
}

#[cfg(target_os = "linux")]
fn cypher(port: u16, q: &str) -> serde_json::Value {
    let (code, body) = http(port, "POST", "/api/query", "application/json",
                            serde_json::json!({ "query": q }).to_string().as_bytes());
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    assert_eq!(code, 200, "{q}: {v}");
    v
}

#[cfg(target_os = "linux")]
#[test]
fn a_sandboxed_server_writes_restarts_and_round_trips_a_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let count = "MATCH (n:P) RETURN count(n) AS c";

    let exported = {
        let s = serve(&data);
        cypher(s.1, "UNWIND range(1, 5000) AS i CREATE (:P {i: i, name: 'n' + toString(i)})");
        cypher(s.1, "CREATE INDEX ON :P(i)");
        cypher(s.1, "MATCH (a:P), (b:P) WHERE b.i = a.i + 1 AND a.i <= 1000 CREATE (a)-[:NEXT]->(b)");
        assert_eq!(cypher(s.1, count)["records"][0][0], 5000);
        let (code, snap) = http(s.1, "POST", "/api/snapshot/export", "application/json", b"{}");
        assert_eq!(code, 200, "snapshot export under --sandbox");
        assert!(snap.len() > 1000, "{} bytes", snap.len());
        snap
    };

    // Restart on the same data path: RocksDB reopens and replays under the sandbox.
    {
        let s = serve(&data);
        assert_eq!(cypher(s.1, count)["records"][0][0], 5000, "data survives a sandboxed restart");
    }

    // Import into a fresh sandboxed server.
    let data2 = root.path().join("data2");
    std::fs::create_dir_all(&data2).unwrap();
    let s = serve(&data2);
    let boundary = "samyamaboundary7";
    let mut body = format!("--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
                            filename=\"g.sgsnap\"\r\nContent-Type: application/octet-stream\r\n\r\n").into_bytes();
    body.extend_from_slice(&exported);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let (code, resp) = http(s.1, "POST", "/api/snapshot/import",
                            &format!("multipart/form-data; boundary={boundary}"), &body);
    assert_eq!(code, 200, "snapshot import under --sandbox: {}", String::from_utf8_lossy(&resp));
    assert_eq!(cypher(s.1, count)["records"][0][0], 5000);
    assert_eq!(cypher(s.1, "MATCH (:P)-[r:NEXT]->(:P) RETURN count(r) AS c")["records"][0][0], 1000);
}
