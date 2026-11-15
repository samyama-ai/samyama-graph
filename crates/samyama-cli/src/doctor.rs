//! `samyama-cli doctor` — DX-09.
//!
//! Checks the things that actually go wrong before a first query: no server on
//! the URL, a server on a different version to the client, no memory headroom,
//! a data directory nobody can write to.
//!
//! Two rules shape this module.
//!
//! **A check that cannot fail is not a check.** Every check returns a real
//! observation or `Skipped` with the reason it could not be made. Nothing
//! defaults to `Pass`, because a doctor that reports a clean bill of health on
//! a machine it could not inspect is worse than no doctor.
//!
//! **`Skipped` is not `Pass`.** The exit code is non-zero only for `Fail`, so a
//! check that could not run does not fail the command -- but it is printed as
//! skipped and carried into the JSON, so it can never be mistaken for a
//! successful check by anything reading the output.

use std::fmt;
use std::time::Duration;

/// Under this, RocksDB and a loaded server run out of descriptors.
const MIN_OPEN_FILES: u64 = 4096;
/// Attributed memory past this share of the host's RAM leaves no room to load.
const MEMORY_WARN_PERCENT: u64 = 80;
/// `RETURN 1` touches no data, so this much is the host or the build, not the
/// query: a debug binary, swapping, or a noisy neighbour.
const SLOW_PROBE: Duration = Duration::from_millis(25);
/// Statements in the self-check. `doctor` runs against servers in use.
pub const PROBES: usize = 20;
/// Names of the checks that need a server, for when none answered.
const SERVER_ONLY: [&str; 4] = ["persistence", "server memory", "metrics", "latency"];

/// What a `GET` came back with: the status code and body, or why there is none.
pub type Fetched = Result<(u16, String), String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    Pass,
    Warn,
    Fail,
    Skipped,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Verdict::Pass => "ok",
            Verdict::Warn => "warn",
            Verdict::Fail => "FAIL",
            Verdict::Skipped => "skip",
        })
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Check {
    pub name: &'static str,
    pub verdict: Verdict,
    /// What was observed. Never a restatement of the verdict: a reader who
    /// disagrees with the judgement needs the number it was made from.
    pub detail: String,
}

impl Check {
    fn new(name: &'static str, verdict: Verdict, detail: impl Into<String>) -> Self {
        Check { name, verdict, detail: detail.into() }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct Report {
    pub checks: Vec<Check>,
    pub failed: usize,
    pub warned: usize,
    pub skipped: usize,
}

impl Report {
    fn new(checks: Vec<Check>) -> Self {
        let failed = checks.iter().filter(|c| c.verdict == Verdict::Fail).count();
        let warned = checks.iter().filter(|c| c.verdict == Verdict::Warn).count();
        let skipped = checks.iter().filter(|c| c.verdict == Verdict::Skipped).count();
        Report { checks, failed, warned, skipped }
    }

    /// Non-zero exactly when a check failed. A warning is information, and a
    /// skipped check is an absence of information; neither is a failure, and
    /// treating them as one would make `doctor` useless in a script.
    pub fn exit_code(&self) -> i32 {
        i32::from(self.failed > 0)
    }
}

/// Total and available memory in MiB, from `/proc/meminfo`.
fn meminfo() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let field = |k: &str| -> Option<u64> {
        text.lines()
            .find(|l| l.starts_with(k))?
            .split_whitespace()
            .nth(1)?
            .parse::<u64>()
            .ok()
            .map(|kb| kb / 1024)
    };
    Some((field("MemTotal:")?, field("MemAvailable:")?))
}

fn check_url(url: &str) -> Check {
    match url::Url::parse(url) {
        Ok(u) if u.scheme() == "http" || u.scheme() == "https" => Check::new(
            "server url",
            Verdict::Pass,
            format!("{url} (host {}, port {})",
                u.host_str().unwrap_or("?"),
                u.port_or_known_default().map(|p| p.to_string()).unwrap_or_else(|| "?".into())),
        ),
        Ok(u) => Check::new("server url", Verdict::Fail,
            format!("{url} has scheme '{}'; expected http or https", u.scheme())),
        Err(e) => Check::new("server url", Verdict::Fail, format!("{url} is not a URL: {e}")),
    }
}

fn check_memory() -> Check {
    match meminfo() {
        // The engine is in-memory, so headroom is the thing that decides
        // whether a load succeeds. 1 GiB is a floor for anything non-trivial,
        // not a recommendation.
        Some((total, avail)) if avail < 1024 => Check::new(
            "memory", Verdict::Warn,
            format!("{avail} MiB available of {total} MiB; under 1 GiB free"),
        ),
        Some((total, avail)) => Check::new(
            "memory", Verdict::Pass, format!("{avail} MiB available of {total} MiB"),
        ),
        None => Check::new("memory", Verdict::Skipped, "no /proc/meminfo on this platform"),
    }
}

fn check_data_dir(dir: &std::path::Path) -> Check {
    if !dir.exists() {
        // Not existing is fine -- the server creates it. Whether it *could* be
        // created is the question, so test the parent.
        let parent = dir.parent().unwrap_or(std::path::Path::new("."));
        return match std::fs::metadata(parent) {
            Ok(m) if m.permissions().readonly() => Check::new(
                "data directory", Verdict::Fail,
                format!("{} does not exist and its parent {} is read-only",
                        dir.display(), parent.display()),
            ),
            Ok(_) => Check::new("data directory", Verdict::Pass,
                format!("{} does not exist yet; parent {} is writable",
                        dir.display(), parent.display())),
            Err(e) => Check::new("data directory", Verdict::Skipped,
                format!("cannot stat parent {}: {e}", parent.display())),
        };
    }
    // `PermissionsExt::readonly` reports the owner bit, which is not the same
    // question as "can this process write here". Answer it by writing.
    let probe = dir.join(".samyama-doctor-write-probe");
    match std::fs::write(&probe, b"") {
        Ok(()) => {
            let _ = std::fs::remove_file(&probe);
            Check::new("data directory", Verdict::Pass,
                format!("{} exists and is writable", dir.display()))
        }
        Err(e) => Check::new("data directory", Verdict::Fail,
            format!("{} exists but is not writable: {e}", dir.display())),
    }
}

/// Everything that can be decided without talking to a server.
pub fn local_checks(url: &str, data_dir: &std::path::Path) -> Vec<Check> {
    vec![
        Check::new("client version", Verdict::Pass, env!("CARGO_PKG_VERSION")),
        check_url(url),
        check_memory(),
        check_data_dir(data_dir),
        check_open_files(open_file_limit().as_deref()),
        check_nlq(|v| std::env::var(v).ok()),
    ]
}

/// This shell's descriptor limit. The server inherits what the shell that
/// started it had, so this is the server's own only when that is this machine.
fn open_file_limit() -> Option<String> {
    let out = std::process::Command::new("sh").args(["-c", "ulimit -n"]).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn check_open_files(limit: Option<&str>) -> Check {
    match limit {
        None => Check::new("open files", Verdict::Skipped, "could not run `ulimit -n`"),
        Some("unlimited") => Check::new("open files", Verdict::Pass, "unlimited"),
        Some(raw) => match raw.parse::<u64>() {
            Ok(n) if n < MIN_OPEN_FILES => Check::new("open files", Verdict::Warn,
                format!("limit is {n}, under {MIN_OPEN_FILES}; raise it with `ulimit -n` before \
                         starting the server")),
            Ok(n) => Check::new("open files", Verdict::Pass, format!("limit is {n}")),
            Err(_) => Check::new("open files", Verdict::Skipped,
                format!("`ulimit -n` said '{raw}'")),
        },
    }
}

/// The same validation the server does at start-up, on this shell's variables.
/// The server's own environment cannot be read from here.
fn check_nlq(get: impl Fn(&str) -> Option<String>) -> Check {
    if get("NLQ_PROVIDER").is_none_or(|p| p.trim().is_empty()) {
        return Check::new("nlq config", Verdict::Skipped, "NLQ_PROVIDER is not set in this shell");
    }
    match samyama::nlq::config_from(get) {
        Ok(c) => Check::new("nlq config", Verdict::Pass,
            format!("provider {:?} is configured in this shell", c.provider)),
        Err(e) => Check::new("nlq config", Verdict::Warn, e),
    }
}

/// The body of an answer worth reading, or the check that says why it is not.
fn answered<'a>(name: &'static str, fetched: &'a Fetched) -> Result<&'a str, Check> {
    match fetched {
        Ok((200..=299, body)) => Ok(body),
        // The endpoint is there and wants a credential this client lacks: an
        // absence of information, not a finding about the server.
        Ok((code @ (401 | 403), _)) => {
            Err(Check::new(name, Verdict::Skipped, format!("server answered {code}")))
        }
        Ok((code, _)) => Err(Check::new(name, Verdict::Warn, format!("server answered {code}"))),
        Err(e) => Err(Check::new(name, Verdict::Warn, format!("no answer: {e}"))),
    }
}

/// `fetched` is `GET /api/status`. A failed persist is a `Fail`, not a warning:
/// the server is refusing every write and has writes that a restart will lose.
pub fn check_persistence(fetched: &Fetched) -> Check {
    const NAME: &str = "persistence";
    let body = match answered(NAME, fetched) {
        Ok(b) => b,
        Err(c) => return c,
    };
    let v: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
    let degraded = v["storage"]["persistence_degraded"].as_bool();
    if degraded == Some(true) || v["status"] == "degraded" {
        return Check::new(NAME, Verdict::Fail,
            "degraded: a write did not reach disk and writes are being refused; \
             see the server log");
    }
    match degraded {
        Some(_) => Check::new(NAME, Verdict::Pass, "no write has failed to reach disk"),
        None => Check::new(NAME, Verdict::Skipped, "this server does not report persistence health"),
    }
}

/// RAM of this machine in MiB, if the server at `url` is on it. For a server
/// elsewhere this machine's RAM says nothing.
pub fn local_ram_mib(url: &str) -> Option<u64> {
    let u = url::Url::parse(url).ok()?;
    let local = match u.host()? {
        url::Host::Domain(d) => d.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(ip) => ip.is_loopback(),
        url::Host::Ipv6(ip) => ip.is_loopback(),
    };
    local.then(meminfo).flatten().map(|(total, _)| total)
}

/// `fetched` is `GET /api/memory`, whose figures are floors, so a warning here
/// is if anything late.
pub fn check_server_memory(fetched: &Fetched, ram_mib: Option<u64>) -> Check {
    const NAME: &str = "server memory";
    let body = match answered(NAME, fetched) {
        Ok(b) => b,
        Err(c) => return c,
    };
    let v: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
    let Some(graph) = v["graph"]["attributed_bytes"].as_u64() else {
        return Check::new(NAME, Verdict::Warn, "/api/memory has no graph.attributed_bytes");
    };
    let mib = (graph + v["index_memory_bytes"].as_u64().unwrap_or(0)) / (1024 * 1024);
    match ram_mib {
        Some(ram) if mib * 100 >= ram * MEMORY_WARN_PERCENT => Check::new(NAME, Verdict::Warn,
            format!("at least {mib} MiB of {ram} MiB host RAM is graph and indexes")),
        Some(ram) => Check::new(NAME, Verdict::Pass, format!("at least {mib} MiB of {ram} MiB host RAM")),
        None => Check::new(NAME, Verdict::Pass, format!("at least {mib} MiB; host RAM not known here")),
    }
}

/// Is `line` a Prometheus sample of a `samyama_` metric with a numeric value?
/// The text `samyama_` alone is not enough: a `# HELP` line, a label value or
/// an error page can carry it with no sample behind it.
fn is_samyama_sample(line: &str) -> bool {
    let line = line.trim();
    if line.starts_with('#') {
        return false;
    }
    // Labels can hold spaces, so the value is whatever follows the closing brace.
    let (name, rest) = match line.find('{') {
        Some(i) => (&line[..i], line[i..].rsplit_once('}').map_or("", |(_, r)| r)),
        None => line.split_once(char::is_whitespace).unwrap_or((line, "")),
    };
    name.starts_with("samyama_")
        && rest.split_whitespace().next().is_some_and(|v| v.parse::<f64>().is_ok())
}

/// `fetched` is `GET /metrics`.
pub fn check_metrics(fetched: &Fetched) -> Check {
    const NAME: &str = "metrics";
    let body = match answered(NAME, fetched) {
        Ok(b) => b,
        Err(c) => return c,
    };
    match body.lines().filter(|l| is_samyama_sample(l)).count() {
        0 => Check::new(NAME, Verdict::Warn, "/metrics answered with no samyama_ sample"),
        n => Check::new(NAME, Verdict::Pass, format!("{n} samyama_ samples")),
    }
}

/// Round trips of `RETURN 1`, or the error that stopped them.
pub fn check_latency(samples: Result<Vec<Duration>, String>) -> Check {
    const NAME: &str = "latency";
    let mut s = match samples {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => return Check::new(NAME, Verdict::Skipped, "no probes ran"),
        Err(e) => return Check::new(NAME, Verdict::Warn, format!("`RETURN 1` failed: {e}")),
    };
    s.sort();
    let p50 = s[(s.len() - 1) / 2];
    let p95 = s[(s.len() * 95).div_ceil(100) - 1];
    let seen = format!("RETURN 1 x{}: p50 {p50:.1?}, p95 {p95:.1?}", s.len());
    if p95 > SLOW_PROBE {
        Check::new(NAME, Verdict::Warn, format!("{seen}, over {SLOW_PROBE:?}; network, a debug \
            build, swapping or a busy host"))
    } else {
        Check::new(NAME, Verdict::Pass, seen)
    }
}

/// The code, when the server wants a credential this client does not have.
pub fn wants_credential(fetched: &Fetched) -> Option<u16> {
    match fetched {
        Ok((code @ (401 | 403), _)) => Some(*code),
        _ => None,
    }
}

/// The server answered, but only to ask for a credential. It is reachable, and
/// nothing else about it was observed, so every server check is skipped rather
/// than reported as broken: a locked door is not a fault in the house.
pub fn credential_required(code: u16) -> Vec<Check> {
    let why = format!("server answered {code}; doctor has no credential it accepts");
    let mut checks = vec![
        Check::new("server reachable", Verdict::Pass, format!("answered {code}")),
        Check::new("version match", Verdict::Skipped, why.clone()),
    ];
    checks.extend(SERVER_ONLY.iter().map(|n| Check::new(n, Verdict::Skipped, why.clone())));
    checks
}

/// For when no server answered: not observed is not the same as fine.
pub fn server_only_skipped() -> Vec<Check> {
    SERVER_ONLY
        .iter()
        .map(|n| Check::new(n, Verdict::Skipped, "no server answered"))
        .collect()
}

/// The RESP port is not in the HTTP URL, so it is checked only when named. If
/// it is named and nothing listens, the check fails: that is the question asked.
pub fn check_resp_port(url: &str, port: Option<u16>) -> Check {
    use std::net::ToSocketAddrs;
    const NAME: &str = "resp port";
    let Some(port) = port else {
        return Check::new(NAME, Verdict::Skipped, "pass --resp-port to check it");
    };
    let host = url::Url::parse(url).ok().and_then(|u| u.host_str().map(str::to_string));
    let Some(host) = host else {
        return Check::new(NAME, Verdict::Skipped, "the server url has no host");
    };
    let addr = (host.trim_matches(['[', ']']), port).to_socket_addrs().ok().and_then(|mut a| a.next());
    let Some(addr) = addr else {
        return Check::new(NAME, Verdict::Fail, format!("cannot resolve {host}"));
    };
    match std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
        Ok(_) => Check::new(NAME, Verdict::Pass, format!("{addr} accepts connections")),
        Err(e) => Check::new(NAME, Verdict::Fail, format!("{addr}: {e}")),
    }
}

/// The server half. `status` is the version string the server reported, or the
/// error that came back instead.
pub fn server_checks(status: Result<String, String>, client_version: &str) -> Vec<Check> {
    match status {
        Err(e) => vec![
            Check::new("server reachable", Verdict::Fail, e),
            Check::new("version match", Verdict::Skipped,
                "no server answered, so its version is unknown"),
        ],
        Ok(server_version) => {
            let reachable = Check::new("server reachable", Verdict::Pass,
                format!("answered, version {server_version}"));
            // A mismatched patch level is ordinary; a mismatched major or minor
            // is the cause of "that query works on my machine".
            fn same(v: &str) -> Vec<&str> { v.split('.').take(2).collect() }
            let m = if same(&server_version) == same(client_version) {
                Check::new("version match", Verdict::Pass,
                    format!("client {client_version}, server {server_version}"))
            } else {
                Check::new("version match", Verdict::Warn,
                    format!("client {client_version} and server {server_version} differ before \
                             the patch level; behaviour may not match the client's docs"))
            };
            vec![reachable, m]
        }
    }
}

pub fn report(checks: Vec<Check>) -> Report {
    Report::new(checks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failing_check_sets_a_non_zero_exit_code() {
        let r = report(vec![Check::new("x", Verdict::Fail, "broken")]);
        assert_eq!(r.exit_code(), 1);
        assert_eq!(r.failed, 1);
    }

    #[test]
    fn warnings_and_skips_do_not_fail_the_command() {
        // The distinction the whole module turns on: `doctor` must stay usable
        // in a script on a machine where something merely could not be checked.
        let r = report(vec![
            Check::new("a", Verdict::Warn, "low"),
            Check::new("b", Verdict::Skipped, "no /proc"),
            Check::new("c", Verdict::Pass, "fine"),
        ]);
        assert_eq!(r.exit_code(), 0);
        assert_eq!((r.failed, r.warned, r.skipped), (0, 1, 1));
    }

    #[test]
    fn an_unreachable_server_fails_and_leaves_the_version_unknown() {
        let cs = server_checks(Err("connection refused".into()), "1.7.1");
        assert_eq!(cs[0].verdict, Verdict::Fail);
        // Not Fail: we did not observe a mismatch, we observed nothing.
        assert_eq!(cs[1].verdict, Verdict::Skipped);
        assert_eq!(report(cs).exit_code(), 1);
    }

    #[test]
    fn a_patch_level_difference_is_not_a_warning() {
        let cs = server_checks(Ok("1.7.9".into()), "1.7.1");
        assert_eq!(cs[1].verdict, Verdict::Pass);
    }

    #[test]
    fn a_minor_version_difference_warns_but_does_not_fail() {
        let cs = server_checks(Ok("1.8.0".into()), "1.7.1");
        assert_eq!(cs[1].verdict, Verdict::Warn);
        assert_eq!(report(cs).exit_code(), 0);
    }

    #[test]
    fn a_bad_url_is_a_failure_and_a_good_one_is_not() {
        // Both directions: a checker stuck on Fail would pass the first alone.
        assert_eq!(check_url("http://localhost:8080").verdict, Verdict::Pass);
        assert_eq!(check_url("localhost:8080").verdict, Verdict::Fail);
        assert_eq!(check_url("ftp://localhost").verdict, Verdict::Fail);
    }

    #[test]
    fn an_unwritable_data_directory_is_a_failure() {
        let dir = std::env::temp_dir().join("samyama-doctor-test-ro");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(check_data_dir(&dir).verdict, Verdict::Pass);

        let mut p = std::fs::metadata(&dir).unwrap().permissions();
        p.set_readonly(true);
        std::fs::set_permissions(&dir, p).unwrap();
        // Running as root defeats the permission bit entirely, in which case
        // the write probe legitimately succeeds and there is nothing to assert.
        let is_root = std::fs::write(dir.join(".root-probe"), b"").is_ok();
        if !is_root {
            assert_eq!(check_data_dir(&dir).verdict, Verdict::Fail);
        }
        let mut p = std::fs::metadata(&dir).unwrap().permissions();
        p.set_readonly(false);
        let _ = std::fs::set_permissions(&dir, p);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn verdicts_print_as_their_short_labels() {
        let shown: Vec<String> = [Verdict::Pass, Verdict::Warn, Verdict::Fail, Verdict::Skipped]
            .iter()
            .map(|v| v.to_string())
            .collect();
        assert_eq!(shown, vec!["ok", "warn", "FAIL", "skip"]);
    }

    #[test]
    fn a_missing_data_directory_is_judged_by_its_parent() {
        let base =
            std::env::temp_dir().join(format!("samyama-doctor-parent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();

        // Writable parent: the directory can be created later.
        let c = check_data_dir(&base.join("data"));
        assert_eq!(c.verdict, Verdict::Pass);
        assert!(c.detail.contains("does not exist yet"), "{}", c.detail);

        // Parent that does not exist either: nothing to stat.
        let c = check_data_dir(&base.join("no").join("data"));
        assert_eq!(c.verdict, Verdict::Skipped);
        assert!(c.detail.contains("cannot stat parent"), "{}", c.detail);

        // Read-only parent (by its permission bits, which is what is checked).
        let ro = base.join("ro");
        std::fs::create_dir_all(&ro).unwrap();
        let mut p = std::fs::metadata(&ro).unwrap().permissions();
        p.set_readonly(true);
        std::fs::set_permissions(&ro, p).unwrap();
        let c = check_data_dir(&ro.join("data"));
        assert_eq!(c.verdict, Verdict::Fail);
        assert!(c.detail.contains("is read-only"), "{}", c.detail);
        let mut p = std::fs::metadata(&ro).unwrap().permissions();
        p.set_readonly(false);
        let _ = std::fs::set_permissions(&ro, p);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn a_url_check_names_the_host_and_port_it_parsed() {
        let c = check_url("https://example.test");
        assert_eq!(c.verdict, Verdict::Pass);
        assert!(
            c.detail.contains("host example.test, port 443"),
            "{}",
            c.detail
        );
        let c = check_url("ftp://example.test");
        assert!(c.detail.contains("scheme 'ftp'"), "{}", c.detail);
        let c = check_url("not a url");
        assert!(c.detail.contains("is not a URL"), "{}", c.detail);
    }

    #[test]
    fn memory_check_reads_proc_meminfo_when_present() {
        let c = check_memory();
        if std::path::Path::new("/proc/meminfo").exists() {
            assert_ne!(c.verdict, Verdict::Skipped);
            assert!(c.detail.contains("MiB available of"), "{}", c.detail);
        } else {
            assert_eq!(c.verdict, Verdict::Skipped);
        }
    }

    fn got(code: u16, body: &str) -> Fetched {
        Ok((code, body.to_string()))
    }

    #[test]
    fn persistence_is_judged_from_what_the_server_reports() {
        let v = |s: &str| check_persistence(&got(200, s)).verdict;
        assert_eq!(v(r#"{"status":"healthy","storage":{"persistence_degraded":false}}"#), Verdict::Pass);
        assert_eq!(v(r#"{"status":"degraded","storage":{"persistence_degraded":true}}"#), Verdict::Fail);
        assert_eq!(v(r#"{"status":"degraded","storage":{}}"#), Verdict::Fail);
        // An older server does not say. That is not the same as saying it is fine.
        assert_eq!(v(r#"{"status":"healthy","storage":{}}"#), Verdict::Skipped);
        assert_eq!(report(vec![check_persistence(&got(200, r#"{"status":"degraded"}"#))]).exit_code(), 1);
    }

    #[test]
    fn an_endpoint_that_wants_a_credential_is_skipped_and_a_broken_one_warns() {
        assert_eq!(check_metrics(&got(401, "")).verdict, Verdict::Skipped);
        assert_eq!(check_metrics(&got(403, "")).verdict, Verdict::Skipped);
        assert_eq!(check_metrics(&got(500, "")).verdict, Verdict::Warn);
        assert_eq!(check_metrics(&Err("refused".into())).verdict, Verdict::Warn);
        assert_eq!(check_metrics(&got(200, "<html>")).verdict, Verdict::Warn);
        let c = check_metrics(&got(200, "# HELP x\nsamyama_nodes 3\n"));
        assert_eq!((c.verdict, c.detail.as_str()), (Verdict::Pass, "1 samyama_ samples"));
    }

    #[test]
    fn metrics_needs_a_real_samyama_sample_not_just_the_text() {
        let v = |b: &str| check_metrics(&got(200, b)).verdict;
        // The text alone: in a comment, in a label, in an error page.
        assert_eq!(v("# HELP samyama_nodes Nodes\n# TYPE samyama_nodes gauge\n"), Verdict::Warn);
        assert_eq!(v("process_cpu{job=\"samyama_x\"} 1\n"), Verdict::Warn);
        assert_eq!(v("<html>samyama_ is down</html>"), Verdict::Warn);
        assert_eq!(v("samyama_nodes\n"), Verdict::Warn);
        assert_eq!(v("samyama_nodes three\n"), Verdict::Warn);
        // Real samples, with labels (spaces included) and a timestamp.
        assert_eq!(v("samyama_queries_total{kind=\"a b\"} 7 1700000000\n"), Verdict::Pass);
        assert_eq!(v("  samyama_up 1\n"), Verdict::Pass);
    }

    #[test]
    fn a_server_that_wants_a_credential_is_reachable_and_every_server_check_skips() {
        assert_eq!(wants_credential(&got(401, "")), Some(401));
        assert_eq!(wants_credential(&got(403, "")), Some(403));
        assert_eq!(wants_credential(&got(200, "{}")), None);
        assert_eq!(wants_credential(&got(500, "")), None);
        assert_eq!(wants_credential(&Err("refused".into())), None);
        let cs = credential_required(401);
        assert_eq!(cs[0].verdict, Verdict::Pass);
        assert!(cs[1..].iter().all(|c| c.verdict == Verdict::Skipped), "{cs:?}");
        let names: Vec<_> = cs.iter().map(|c| c.name).collect();
        for n in ["version match", "persistence", "server memory", "metrics", "latency"] {
            assert!(names.contains(&n), "{n} missing from {names:?}");
        }
        assert_eq!(report(cs).exit_code(), 0);
    }

    #[test]
    fn server_memory_warns_near_the_hosts_ram() {
        let body = format!(r#"{{"graph":{{"attributed_bytes":{}}},"index_memory_bytes":{}}}"#,
            700u64 << 20, 100u64 << 20);
        assert_eq!(check_server_memory(&got(200, &body), Some(1000)).verdict, Verdict::Warn);
        assert_eq!(check_server_memory(&got(200, &body), Some(1001)).verdict, Verdict::Pass);
        let c = check_server_memory(&got(200, &body), None);
        assert_eq!(c.verdict, Verdict::Pass);
        assert!(c.detail.contains("at least 800 MiB"), "{}", c.detail);
        assert_eq!(check_server_memory(&got(200, "{}"), None).verdict, Verdict::Warn);
    }

    #[test]
    fn only_a_server_on_this_machine_is_compared_with_its_ram() {
        assert_eq!(local_ram_mib("http://db.example.test:8080"), None);
        assert_eq!(local_ram_mib("http://10.0.0.1:8080"), None);
        assert_eq!(local_ram_mib("not a url"), None);
        assert_eq!(local_ram_mib("http://localhost:8080"), meminfo().map(|m| m.0));
    }

    #[test]
    fn latency_is_judged_on_p95_and_reports_both() {
        let ms = |v: &[u64]| Ok(v.iter().map(|m| Duration::from_millis(*m)).collect());
        // One outlier in twenty sits past p95 and must not warn; two reach it.
        let mut one = vec![1u64; 19];
        one.push(500);
        let c = check_latency(ms(&one));
        assert_eq!(c.verdict, Verdict::Pass, "{}", c.detail);
        assert!(c.detail.contains("p50 1.0ms, p95 1.0ms"), "{}", c.detail);
        let mut two = vec![1u64; 18];
        two.extend([500, 500]);
        assert_eq!(check_latency(ms(&two)).verdict, Verdict::Warn);
        assert_eq!(check_latency(ms(&[])).verdict, Verdict::Skipped);
        assert_eq!(check_latency(Err("400".into())).verdict, Verdict::Warn);
    }

    #[test]
    fn a_low_descriptor_limit_warns() {
        assert_eq!(check_open_files(Some("256")).verdict, Verdict::Warn);
        assert_eq!(check_open_files(Some("4095")).verdict, Verdict::Warn);
        assert_eq!(check_open_files(Some("4096")).verdict, Verdict::Pass);
        assert_eq!(check_open_files(Some("unlimited")).verdict, Verdict::Pass);
        assert_eq!(check_open_files(Some("?")).verdict, Verdict::Skipped);
        assert_eq!(check_open_files(None).verdict, Verdict::Skipped);
    }

    #[test]
    fn nlq_is_checked_only_when_a_provider_is_set() {
        assert_eq!(check_nlq(|_| None).verdict, Verdict::Skipped);
        let env = |v: &str| (v == "NLQ_PROVIDER").then(|| "openai".to_string());
        let c = check_nlq(env);
        assert_eq!(c.verdict, Verdict::Warn);
        assert!(c.detail.contains("API key"), "{}", c.detail);
        let keyed = |v: &str| match v {
            "NLQ_PROVIDER" => Some("openai".to_string()),
            "OPENAI_API_KEY" => Some("not-a-real-key".to_string()),
            _ => None,
        };
        let c = check_nlq(keyed);
        assert_eq!(c.verdict, Verdict::Pass);
        assert!(!c.detail.contains("not-a-real-key"), "{}", c.detail);
    }

    #[test]
    fn the_resp_port_is_checked_when_named() {
        let url = "http://127.0.0.1:8080";
        assert_eq!(check_resp_port(url, None).verdict, Verdict::Skipped);
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        assert_eq!(check_resp_port(url, Some(port)).verdict, Verdict::Pass);
        // A port held but not listening, rather than one just closed: a closed
        // port can be taken by a test running alongside before the check runs.
        let held = tokio::net::TcpSocket::new_v4().unwrap();
        held.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let dead = held.local_addr().unwrap().port();
        assert_eq!(check_resp_port(url, Some(dead)).verdict, Verdict::Fail);
        drop(l);
    }

    #[test]
    fn local_checks_never_return_an_empty_report() {
        let cs = local_checks("http://localhost:8080", std::path::Path::new("."));
        assert!(cs.len() >= 4, "a doctor that checks nothing reports a clean bill of health");
        assert!(cs.iter().all(|c| !c.detail.is_empty()), "every check must show what it saw");
    }
}
