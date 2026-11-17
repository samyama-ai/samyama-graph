//! A tenant's `max_memory_bytes` refuses writes once it is reached (#2225).
//!
//! It was configured and never enforced: nothing recorded memory use and
//! nothing compared it with the limit, so a server started with
//! `--max-memory-bytes 1024` accepted 100,000 nodes. Checked here at each
//! layer: the store's admission, the query engine, and the server binary
//! with persistence, which is the reproduction the issue reports.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use samyama::graph::{GraphStore, WriteAdmission};
use samyama::query::QueryEngine;

fn with_ceiling(max: Option<u64>) -> GraphStore {
    let mut s = GraphStore::new();
    s.set_write_admission(Some(WriteAdmission { max_memory_bytes: max, ..Default::default() }));
    s
}

fn fill(s: &mut GraphStore, n: usize) {
    let e = QueryEngine::new();
    let saved = s.write_admission();
    s.set_write_admission(None);
    e.execute_mut(&format!("UNWIND range(1, {n}) AS i CREATE (:P {{i: i, s: 'some text'}})"), s, "default")
        .unwrap();
    s.set_write_admission(saved);
}

// ── the store ───────────────────────────────────────────────────────────────

#[test]
fn a_store_at_its_ceiling_refuses_and_one_under_it_admits() {
    let mut s = with_ceiling(Some(1024));
    fill(&mut s, 200);
    let err = s.admit_memory().expect_err("over 1 KB");
    assert!(err.to_string().contains("memory ("), "{err}");

    let mut roomy = with_ceiling(Some(u64::MAX));
    fill(&mut roomy, 200);
    assert!(roomy.admit_memory().is_ok());
}

#[test]
fn no_ceiling_is_never_measured() {
    let mut s = with_ceiling(None);
    fill(&mut s, 200);
    assert!(s.admit_memory().is_ok());
    let mut none = GraphStore::new();
    fill(&mut none, 200);
    assert!(none.admit_memory().is_ok());
    assert_eq!((s.memory_readings(), none.memory_readings()), (0, 0),
               "a store with no memory quota pays nothing for one");
}

#[test]
fn a_bulk_import_is_refused_over_the_ceiling() {
    let mut s = with_ceiling(Some(1024));
    fill(&mut s, 200);
    assert!(s.admits_bulk(1, 0).is_err());
}

// ── the reading is cached, and bounded ──────────────────────────────────────

#[test]
fn unchanged_data_is_read_once() {
    let mut s = with_ceiling(Some(u64::MAX));
    fill(&mut s, 500);
    for _ in 0..100 {
        s.admit_memory().unwrap();
    }
    assert_eq!(s.memory_readings(), 1, "the data did not change, so neither did its size");
}

#[test]
fn a_change_is_read_once_the_interval_has_passed_and_not_before() {
    // Large enough that one reading takes a measurable time, so the interval
    // (ten readings' worth) is far longer than two back-to-back admissions.
    let mut s = with_ceiling(Some(u64::MAX));
    fill(&mut s, 50_000);
    let first = s.memory_in_use();
    assert_eq!(s.memory_readings(), 1);
    fill(&mut s, 10);
    s.admit_memory().unwrap();
    assert_eq!(s.memory_readings(), 1, "inside the interval the reading is reused");

    // Past the interval, the change is read.
    let deadline = Instant::now() + Duration::from_secs(30);
    while s.memory_readings() == 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
        s.admit_memory().unwrap();
    }
    assert_eq!(s.memory_readings(), 2);
    assert!(s.memory_in_use() > first, "the new reading includes the new nodes");
}

// ── the query engine ────────────────────────────────────────────────────────

#[test]
fn a_write_statement_is_refused_with_the_quota_code_and_reads_still_answer() {
    let e = QueryEngine::new();
    let mut s = with_ceiling(Some(1024));
    fill(&mut s, 200);
    let before = s.node_count();
    let err = e.execute_mut("CREATE (:Q)", &mut s, "default").expect_err("over the memory quota");
    assert!(err.to_string().contains(samyama::query::error_code::QUOTA_EXCEEDED), "{err}");
    assert_eq!(s.node_count(), before, "nothing written");
    let err = e.execute_mut("MATCH (p:P) SET p.s = 'more text'", &mut s, "default")
        .expect_err("a SET grows memory too, and is refused");
    assert!(err.to_string().contains("memory"), "{err}");
    let read = e.execute("MATCH (p:P) RETURN count(p) AS c", &s).expect("reads are not refused");
    assert_eq!(read.records.len(), 1);
}

// ── the server: the reproduction in the issue ───────────────────────────────

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn post(port: u16, body: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(s, "POST /api/query HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
               Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    let mut r = String::new();
    s.read_to_string(&mut r).unwrap();
    let code = r.get(9..12).and_then(|c| c.parse().ok()).unwrap_or(0);
    (code, r)
}

#[test]
fn a_server_started_with_a_1kb_memory_quota_stops_accepting_writes() {
    let data = tempfile::tempdir().unwrap();
    let (http, resp) = (free_port(), free_port());
    let _srv = Server(
        Command::new(env!("CARGO_BIN_EXE_samyama"))
            .args(["--port", &resp.to_string(), "--http-port", &http.to_string(),
                   "--data-path", data.path().join("d").to_str().unwrap(),
                   "--max-memory-bytes", "1024"])
            .current_dir(data.path())
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
            .spawn().unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    while std::net::TcpStream::connect(("127.0.0.1", http)).is_err() {
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(100));
    }
    let batch = r#"{"query":"UNWIND range(1, 2000) AS i CREATE (:N {i: i, s: 'xxxxxxxxxxxxxxxxxxxxxxxx'})"}"#;
    let mut refused = 0;
    let mut first_refusal = String::new();
    for _ in 0..10 {
        let (code, body) = post(http, batch);
        if code != 200 {
            refused += 1;
            if first_refusal.is_empty() {
                first_refusal = body;
            }
        }
    }
    // The first batch is admitted at ~0 bytes; once it is in, every later
    // write statement finds the store over 1 KB.
    assert!(refused >= 8, "{refused} of 10 refused; was 0 of 10 before #2225");
    assert!(first_refusal.contains("QuotaExceeded") && first_refusal.contains("memory"),
            "{first_refusal}");
    let (code, body) = post(http, r#"{"query":"MATCH (n:N) RETURN count(n) AS c"}"#);
    assert_eq!(code, 200, "reads are not refused: {body}");
}
