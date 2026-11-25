//! `samyama healthcheck` against real servers (#2202): the binary the image's
//! `HEALTHCHECK` runs, asked about the binary the image serves.

use std::net::TcpListener;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn bin() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_samyama"));
    c.env_remove("SAMYAMA_HEALTHCHECK_TOKEN")
        .env_remove("SAMYAMA_AUTH_FILE")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .stdin(Stdio::null());
    c
}

struct Server {
    child: Child,
    http: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn serve(extra: &[&str]) -> Server {
    let (http, resp) = (free_port(), free_port());
    let child = bin()
        .args(["--ephemeral", "--port", &resp.to_string(), "--http-port", &http.to_string()])
        .args(extra)
        .current_dir(tempfile::tempdir().unwrap().keep())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn samyama");
    let s = Server { child, http };
    let deadline = Instant::now() + Duration::from_secs(60);
    while std::net::TcpStream::connect(("127.0.0.1", http)).is_err() {
        assert!(Instant::now() < deadline, "the server did not open its HTTP port");
        std::thread::sleep(Duration::from_millis(100));
    }
    s
}

fn healthcheck(args: &[&str], env: &[(&str, &str)]) -> (Option<i32>, String) {
    let mut c = bin();
    c.arg("healthcheck").args(args);
    for (k, v) in env {
        c.env(k, v);
    }
    let Output { status, stdout, stderr, .. } = c.output().expect("run healthcheck");
    (status.code(), format!("{}{}", String::from_utf8_lossy(&stdout), String::from_utf8_lossy(&stderr)))
}

#[test]
fn a_running_server_is_healthy() {
    let s = serve(&[]);
    let (code, out) = healthcheck(&["--url", &format!("http://127.0.0.1:{}", s.http)], &[]);
    assert_eq!(code, Some(0), "{out}");
    assert!(out.contains("healthy"), "{out}");
}

#[test]
fn nothing_listening_is_unhealthy_and_says_why() {
    let (code, out) = healthcheck(
        &["--url", &format!("http://127.0.0.1:{}", free_port()), "--timeout", "2"],
        &[],
    );
    assert_eq!(code, Some(1), "{out}");
    assert!(out.contains("unhealthy"), "{out}");
}

#[test]
fn with_authentication_on_a_server_is_up_without_a_token_and_a_wrong_token_is_not() {
    use sha2::{Digest, Sha256};
    let digest: String = Sha256::digest(b"tok").iter().map(|b| format!("{b:02x}")).collect();
    let dir = tempfile::tempdir().unwrap();
    let creds = dir.path().join("creds.txt");
    std::fs::write(&creds, format!("probe:{digest}:roles=read\n")).unwrap();
    let s = serve(&["--auth-file", creds.to_str().unwrap()]);
    let url = format!("http://127.0.0.1:{}", s.http);

    // What `curl -sf` reported as unhealthy for as long as such a server ran.
    let (code, out) = healthcheck(&["--url", &url], &[]);
    assert_eq!(code, Some(0), "answered through the auth layer: {out}");
    assert!(out.contains("credential"), "{out}");

    let (code, out) = healthcheck(&["--url", &url], &[("SAMYAMA_HEALTHCHECK_TOKEN", "tok")]);
    assert_eq!(code, Some(0), "the right token reads the status: {out}");

    let (code, out) = healthcheck(&["--url", &url, "--token", "wrong"], &[]);
    assert_eq!(code, Some(1), "a refused token is a misconfiguration: {out}");
}

#[test]
fn arguments_it_cannot_use_are_a_usage_error() {
    for bad in [&["--timeout", "0"][..], &["--url", "no-scheme:1"], &["--what"]] {
        let (code, out) = healthcheck(bad, &[]);
        assert_eq!(code, Some(64), "{bad:?}: {out}");
        assert!(out.contains("usage"), "{out}");
    }
}
