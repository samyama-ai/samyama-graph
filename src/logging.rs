//! How the server writes its logs: text for a terminal, JSON for a collector (#2076).
//!
//! REL-10 asks for structured logs, and the book's observability chapter shows
//! a JSON log line. The binary called bare `tracing_subscriber::fmt::init()`,
//! which writes human-oriented text only, so a log shipper (Loki, Vector,
//! Fluent Bit) had to parse prose to recover fields the events already carry
//! as structured values -- `elapsed_ms`, `rows`, `query_hash` on a slow query.
//!
//! Text stays the default: it is what a person running the server in a
//! terminal reads, and changing it would change every existing deployment's
//! output. JSON is chosen with `--log-format json` or `SAMYAMA_LOG_FORMAT=json`,
//! the flag winning when both are set. Anything else is refused at start-up
//! rather than read as text: a typo'd `jsn` that silently produced text would
//! leave a collector parsing prose until someone noticed the dashboards empty.

use tracing_subscriber::fmt::MakeWriter;

/// The two log formats the server writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// One human-readable line per event: the default.
    Text,
    /// One JSON object per line, with the current span and the span list.
    Json,
}

impl LogFormat {
    /// Read `text` or `json`, in any case.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "text" => Ok(LogFormat::Text),
            "json" => Ok(LogFormat::Json),
            other => Err(format!(
                "unknown log format `{other}`: use `text` or `json` \
                 (--log-format or SAMYAMA_LOG_FORMAT)"
            )),
        }
    }

    /// The format asked for by `--log-format <f>` / `--log-format=<f>` in
    /// `argv`, else by `SAMYAMA_LOG_FORMAT`, else text.
    pub fn from_args_and_env(argv: &[String]) -> Result<Self, String> {
        Self::resolve(argv, std::env::var("SAMYAMA_LOG_FORMAT").ok().as_deref())
    }

    /// `from_args_and_env` with the environment passed in, so it can be
    /// tested without setting process-wide state.
    pub fn resolve(argv: &[String], env: Option<&str>) -> Result<Self, String> {
        let mut args = argv.iter();
        while let Some(a) = args.next() {
            if let Some(v) = a.strip_prefix("--log-format=") {
                return Self::parse(v);
            }
            if a == "--log-format" {
                return match args.next() {
                    Some(v) => Self::parse(v),
                    None => Err("--log-format needs a value: `text` or `json`".to_string()),
                };
            }
        }
        match env {
            Some(v) if !v.trim().is_empty() => Self::parse(v),
            _ => Ok(LogFormat::Text),
        }
    }
}

/// The JSON subscriber the server installs, writing to `writer`.
///
/// Public so a test reads the same configuration the binary runs, rather than
/// a copy of it that could drift.
pub fn json_subscriber<W>(writer: W) -> impl tracing::Subscriber + Send + Sync
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    tracing_subscriber::fmt()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_writer(writer)
        .finish()
}

/// Install the process-wide subscriber. Call once, first thing in `main`.
///
/// Text goes to stdout, as it always has. JSON goes to **stderr**: the server
/// prints its banner and the start-up demo to stdout with `println!`, so a
/// stdout stream would interleave prose with the JSON and a collector reading
/// it line by line would fail on the first banner line. stderr carries the
/// log events and nothing else. Both go through `SubscriberInitExt::init`,
/// which also forwards `log`-crate records.
pub fn init(format: LogFormat) {
    use tracing_subscriber::util::SubscriberInitExt;
    match format {
        LogFormat::Text => tracing_subscriber::fmt::init(),
        LogFormat::Json => json_subscriber(std::io::stderr).init(),
    }
}

// ── Request correlation (#2076) ────────────────────────────────────────────
//
// A log line from a busy server is one of many interleaved, and a slow-query
// warning or a persistence error said nothing about which request caused it.
// Every HTTP request and every RESP command now runs inside a `request` span
// carrying an id, so each event inside it carries the same id: in JSON the
// span is written on every line (`with_current_span`), in text it prefixes
// the line. On HTTP the id goes back in `X-Request-Id`, so a caller holding a
// response can find its log lines, and a caller's own id is kept when it sends
// one, so the trace it started continues here.

/// The header a request id is read from and written to.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// The longest request id taken from a caller.
pub const MAX_REQUEST_ID_LEN: usize = 128;

/// A fresh request id: 32 hex digits, random.
pub fn new_request_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// A caller's request id, if it is one this server will log.
///
/// It is written into every log line of the request, so it is held to what an
/// id looks like: 1 to [`MAX_REQUEST_ID_LEN`] characters of letters, digits
/// and `-` `_` `.` `:`. Anything else -- a newline, a quote, a 10 KB header --
/// is not logged; the request gets a fresh id instead, rather than an error,
/// because a bad correlation header is no reason to refuse a query.
pub fn accept_request_id(supplied: &str) -> Option<&str> {
    let ok = !supplied.is_empty()
        && supplied.len() <= MAX_REQUEST_ID_LEN
        && supplied
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
    ok.then_some(supplied)
}

/// A caller-supplied name -- a graph, say -- as it may appear in a log line:
/// itself when it passes the request-id rule, else a placeholder. The graph
/// is recorded before anything checks it, and a name with a newline in it
/// would otherwise forge a line in the text log.
pub fn loggable_name(supplied: &str) -> &str {
    accept_request_id(supplied).unwrap_or("(unloggable)")
}

/// The span a request runs in. `tenant` and `command` are filled in later by
/// whoever knows them -- the HTTP query handler reads the graph from the body,
/// which no layer outside it has parsed.
pub fn request_span(request_id: &str, protocol: &'static str) -> tracing::Span {
    tracing::info_span!(
        "request",
        request_id = %request_id,
        protocol,
        method = tracing::field::Empty,
        path = tracing::field::Empty,
        command = tracing::field::Empty,
        tenant = tracing::field::Empty,
    )
}
