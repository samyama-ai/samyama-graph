//! `samyama healthcheck`: is the server on this URL healthy? (#2202)
//!
//! The container image checked health with `curl -sf .../api/status`, which is
//! why it carried `curl` and a shell, and which had a second defect: with
//! `--auth-file` set, `/api/status` answers 401 to a request without a
//! credential, `curl -sf` exits non-zero on it, and every server with
//! authentication on was marked unhealthy for as long as it ran.
//!
//! This asks the same question from inside the binary, so the image needs
//! neither, and answers it on what the server actually says:
//!
//! | answer                                   | exit |
//! |------------------------------------------|------|
//! | 200, `status: healthy`                   | 0    |
//! | 200, `status: degraded`                  | 1    |
//! | 401/403 and no token was given           | 0    |
//! | 401/403 with a token                     | 1    |
//! | no answer, another status, not JSON      | 1    |
//!
//! **Degraded is unhealthy.** It means a write failed to reach disk and the
//! server refuses writes until it restarts (`persistence::health`). A restart is
//! the documented way out, and restarting an unhealthy container is what an
//! orchestrator does with this answer.
//!
//! **401 without a token is healthy.** The server answered, through its auth
//! layer, which is as much as a liveness check can ask of a caller holding no
//! credential. Give one (`--token`, or `SAMYAMA_HEALTHCHECK_TOKEN` so it stays
//! out of the process list) to have the status itself read; a token the server
//! refuses is then a failure, because it is a misconfiguration.

use std::time::Duration;

/// Where the check asks, unless told otherwise: the HTTP port the image exposes.
pub const DEFAULT_URL: &str = "http://127.0.0.1:8080/api/status";
/// Shorter than the image's 5 s `HEALTHCHECK --timeout`, so the check reports
/// its own reason rather than being killed without one.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(3);
/// The environment variable a token is read from when `--token` is absent.
pub const TOKEN_ENV: &str = "SAMYAMA_HEALTHCHECK_TOKEN";

/// Exit code for arguments the command cannot use (`EX_USAGE`).
pub const EXIT_USAGE: i32 = 64;

/// What to check, parsed from `samyama healthcheck [args]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub url: String,
    pub timeout: Duration,
    pub token: Option<String>,
}

/// `args` are the words after `healthcheck`; `env` reads one variable.
pub fn parse_args(args: &[String], env: impl Fn(&str) -> Option<String>) -> Result<Args, String> {
    let mut out = Args { url: DEFAULT_URL.to_string(), timeout: DEFAULT_TIMEOUT, token: None };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        let mut value = |name: &str| {
            it.next().cloned().ok_or_else(|| format!("{name} needs a value"))
        };
        match a.as_str() {
            "--url" => {
                let u = value("--url")?;
                // A bare `http://host:port` means its status route.
                out.url = match u.split_once("://") {
                    Some((_, rest)) if !rest.contains('/') => format!("{u}/api/status"),
                    Some(_) => u,
                    None => return Err(format!("--url must start with http:// or https://, got {u:?}")),
                };
            }
            "--timeout" => {
                let t = value("--timeout")?;
                let secs: f64 = t.parse().map_err(|_| format!("--timeout takes seconds, got {t:?}"))?;
                if !(secs > 0.0 && secs.is_finite()) {
                    return Err(format!("--timeout must be positive, got {t:?}"));
                }
                out.timeout = Duration::from_secs_f64(secs);
            }
            "--token" => out.token = Some(value("--token")?),
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    if out.token.is_none() {
        out.token = env(TOKEN_ENV).filter(|t| !t.is_empty());
    }
    Ok(out)
}

/// What the server's answer means: the exit code and the reason, one line.
pub fn judge(status: Option<u16>, body: &str, had_token: bool) -> (i32, String) {
    let Some(status) = status else {
        return (1, format!("unhealthy: no answer ({body})"));
    };
    match status {
        200 => match serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("status").and_then(|s| s.as_str()).map(str::to_string))
        {
            Some(s) if s == "healthy" => (0, "healthy".to_string()),
            Some(s) if s == "degraded" => (
                1,
                "unhealthy: degraded, a write did not reach disk and writes are refused \
                 until a restart"
                    .to_string(),
            ),
            Some(other) => (1, format!("unhealthy: status {other:?}")),
            None => (1, "unhealthy: 200 without a JSON `status`".to_string()),
        },
        401 | 403 if !had_token => (
            0,
            format!(
                "healthy: the server answered ({status}) and wants a credential to show its \
                 status; set {TOKEN_ENV} to have it read"
            ),
        ),
        401 | 403 => (1, format!("unhealthy: the server refused the token ({status})")),
        other => (1, format!("unhealthy: HTTP {other}")),
    }
}

/// Ask the server once and judge the answer.
pub async fn check(args: &Args) -> (i32, String) {
    let client = match reqwest::Client::builder().timeout(args.timeout).build() {
        Ok(c) => c,
        Err(e) => return (1, format!("unhealthy: could not build a client: {e}")),
    };
    let mut req = client.get(&args.url);
    if let Some(t) = &args.token {
        req = req.bearer_auth(t);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            judge(Some(status), &body, args.token.is_some())
        }
        Err(e) => judge(None, &format!("{}: {e}", args.url), args.token.is_some()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(words: &[&str]) -> Result<Args, String> {
        parse_args(&words.iter().map(|s| s.to_string()).collect::<Vec<_>>(), |_| None)
    }

    #[test]
    fn defaults_point_at_the_image_port() {
        let a = args(&[]).unwrap();
        assert_eq!((a.url.as_str(), a.timeout, a.token), (DEFAULT_URL, DEFAULT_TIMEOUT, None));
    }

    #[test]
    fn a_bare_base_url_means_its_status_route() {
        assert_eq!(args(&["--url", "http://h:9"]).unwrap().url, "http://h:9/api/status");
        assert_eq!(args(&["--url", "https://h/x"]).unwrap().url, "https://h/x");
    }

    #[test]
    fn bad_arguments_are_refused() {
        for bad in [&["--url"][..], &["--url", "h:9"], &["--timeout", "0"], &["--timeout", "x"],
                    &["--timeout", "-1"], &["--nope"]] {
            assert!(args(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_token_comes_from_the_flag_then_the_environment() {
        let env = |k: &str| (k == TOKEN_ENV).then(|| "from-env".to_string());
        let w = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(parse_args(&w(&[]), env).unwrap().token.as_deref(), Some("from-env"));
        assert_eq!(parse_args(&w(&["--token", "flag"]), env).unwrap().token.as_deref(), Some("flag"));
        assert_eq!(parse_args(&w(&[]), |_| Some(String::new())).unwrap().token, None);
    }

    #[test]
    fn each_answer_is_judged_as_the_table_says() {
        let healthy = r#"{"status":"healthy","version":"1"}"#;
        let degraded = r#"{"status":"degraded"}"#;
        assert_eq!(judge(Some(200), healthy, false).0, 0);
        assert_eq!(judge(Some(200), degraded, false).0, 1);
        assert!(judge(Some(200), degraded, false).1.contains("restart"));
        assert_eq!(judge(Some(200), r#"{"status":"starting"}"#, false).0, 1);
        assert_eq!(judge(Some(200), "not json", false).0, 1);
        assert_eq!(judge(Some(401), "", false).0, 0, "answered through the auth layer");
        assert_eq!(judge(Some(403), "", false).0, 0);
        assert_eq!(judge(Some(401), "", true).0, 1, "a refused token is a misconfiguration");
        assert_eq!(judge(Some(500), "", false).0, 1);
        assert_eq!(judge(Some(404), "", false).0, 1);
        assert_eq!(judge(None, "connection refused", false).0, 1);
    }
}
