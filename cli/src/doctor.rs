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
    ]
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
    fn local_checks_never_return_an_empty_report() {
        let cs = local_checks("http://localhost:8080", std::path::Path::new("."));
        assert!(cs.len() >= 4, "a doctor that checks nothing reports a clean bill of health");
        assert!(cs.iter().all(|c| !c.detail.is_empty()), "every check must show what it saw");
    }
}
