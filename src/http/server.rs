//! HTTP server implementation for the Visualizer

use axum::{
    extract::DefaultBodyLimit,
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use crate::embed::EmbedPipeline;
use crate::graph::GraphStore;
use crate::persistence::TenantManager;
use crate::query::QueryEngine;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::info;
use super::handler::{
    query_handler, export_handler, import_parquet_handler, status_handler, memory_handler, metrics_handler, schema_handler, sample_handler,
    import_csv_handler, import_json_handler,
    export_snapshot_handler, restore_snapshot_handler,
    set_enrich_policy_handler, enrich_handler, verify_handler,
    nlq_handler,
};
use super::vector::{list_indexes_handler, create_index_handler, search_handler};

/// HA-09: Build the tenant CRUD sub-router backed by the shared `TenantManager`.
/// Exposed at the crate level so integration tests can mount it in isolation.
pub fn build_tenant_router(tenants: Arc<TenantManager>) -> axum::Router {
    let cache = Arc::new(RwLock::new(HashMap::<String, Arc<EmbedPipeline>>::new()));
    super::tenants::router(tenants, cache)
}
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "src/http/static/"]
struct Assets;

async fn static_handler() -> impl IntoResponse {
    match Assets::get("index.html") {
        Some(content) => {
            let html = std::str::from_utf8(content.data.as_ref()).unwrap_or("Error: Invalid UTF-8 in index.html");
            Html(html.to_string())
        },
        None => Html("<h1>Error: index.html not found</h1><p>Ensure src/http/static/index.html exists and was compiled.</p>".to_string()),
    }
}

/// Answer Chrome's Private Network Access preflight.
///
/// A public HTTPS origin (the hosted Studio) may not call a loopback address unless the
/// local server opts in. Chrome sends `Access-Control-Request-Private-Network: true` on the
/// preflight and requires `Access-Control-Allow-Private-Network: true` back; without it the
/// browser blocks the request before it ever reaches the container, which is the first
/// thing a new user following the documented Docker setup hits (#342).
///
/// `CorsLayer` cannot do this -- tower-http has no PNA support -- so the header is added
/// outside it, on the response it produces. Only echoed when the request actually asks for
/// it, so ordinary same-origin traffic is unaffected.
async fn allow_private_network(
    allowed: Arc<Vec<String>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let asked = req
        .headers()
        .get("access-control-request-private-network")
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"true"));
    // Echoed only to an origin on the list. The previous version echoed to
    // whichever origin asked, which is the whole protection: Chrome sends the
    // preflight precisely so a *public* page cannot reach a private address
    // without the local service naming it. Answering "yes" to everyone
    // converts the check into a permission (#1328).
    let permitted = req
        .headers()
        .get(axum::http::header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|o| allowed.iter().any(|a| a == o));
    let mut res = next.run(req).await;
    if asked && permitted {
        res.headers_mut().insert(
            "access-control-allow-private-network",
            axum::http::HeaderValue::from_static("true"),
        );
    }
    res
}

/// A credential the server will accept, stored as a SHA-256 digest.
///
/// **Tokens, not passwords.** A fast hash is the wrong choice for a
/// human-chosen password, where the defence against an offline attack on a
/// stolen file is the cost of each guess -- that needs argon2 or similar. It is
/// the right choice for a high-entropy token, where there is nothing to guess.
/// The file format therefore takes tokens, and `samyama --new-auth-token`
/// prints one from the OS random source rather than inviting a caller to think
/// of one.
///
/// The digest is what is stored, so the file does not hold anything usable
/// against another service if it leaks, and the server never holds the token in
/// cleartext after start-up.
#[derive(Clone)]
pub struct Credential {
    /// Who this token belongs to. Not used for authorisation -- there are no
    /// roles yet (REL-08 asks for them and this is not that) -- but recorded so
    /// a future audit log has a subject, and so an operator can revoke one line.
    pub name: String,
    digest: [u8; 32],
}

impl Credential {
    /// Parse one `name:sha256-hex` line. Blank lines and `#` comments are skipped.
    fn parse(line: &str) -> Option<Result<Self, String>> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (name, hex) = match line.rsplit_once(':') {
            Some(p) => p,
            None => return Some(Err(format!("no `:` in {line:?}"))),
        };
        let hex = hex.trim();
        if hex.len() != 64 {
            return Some(Err(format!(
                "expected a 64-character sha256 digest for {:?}, got {} characters",
                name.trim(),
                hex.len()
            )));
        }
        let mut digest = [0u8; 32];
        for (i, b) in digest.iter_mut().enumerate() {
            *b = match u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                Ok(v) => v,
                Err(_) => return Some(Err(format!("{hex:?} is not hexadecimal"))),
            };
        }
        Some(Ok(Credential { name: name.trim().to_string(), digest }))
    }
}

/// Read a credential file: one `name:sha256-hex` per line.
///
/// A malformed line is an error rather than a skipped line. Skipping is how a
/// typo in a credential file becomes a server that starts cleanly and accepts
/// one fewer token than the operator believes it does.
pub fn read_credentials(path: &std::path::Path) -> Result<Vec<Credential>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        match Credential::parse(line) {
            None => continue,
            Some(Ok(c)) => out.push(c),
            Some(Err(e)) => return Err(format!("{}:{}: {e}", path.display(), n + 1)),
        }
    }
    if out.is_empty() {
        return Err(format!(
            "{} names no credentials; a file that authenticates nobody would refuse \
             every request, which is not what an operator who configured one meant",
            path.display()
        ));
    }
    Ok(out)
}

/// Compare two digests without letting the time taken depend on where they differ.
///
/// `a == b` on a slice returns as soon as a byte differs, so the time it takes
/// leaks how long a common prefix was, and a token can be recovered one byte at
/// a time. Writing the loop out keeps it constant-time without taking a
/// dependency for four lines.
fn digests_match(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Reject a request that carries no accepted credential (REL-08).
///
/// `OPTIONS` is let through unauthenticated on purpose. A CORS preflight
/// carries no `Authorization` header -- the browser sends it *before* deciding
/// whether the real request is allowed -- so authenticating it would make every
/// cross-origin call fail at the preflight, and the usual repair for that is to
/// turn authentication off. It reveals only which methods and headers the
/// endpoint accepts, which the documentation already says.
///
/// Everything else is authenticated, `/metrics` and `/` included. An exemption
/// list is the thing that quietly grows, and `/api/status` alone reports node
/// and edge counts.
async fn require_credential(
    credentials: Arc<Vec<Credential>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::{Method, StatusCode};

    if req.method() == Method::OPTIONS {
        return next.run(req).await;
    }

    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, rest) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim())
        });

    // The name of the credential that matched, resolved before the request is
    // touched. Recorded on the accepted credential rather than on the header,
    // so an audit entry says which line of the credential file was used and
    // never echoes what was presented.
    let matched: Option<String> = match presented {
        Some(token) => {
            use sha2::{Digest, Sha256};
            let got: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            // Every credential is compared even after one matches, so the time
            // taken does not depend on the position of the matching line.
            credentials.iter().fold(None, |acc, c| {
                if digests_match(&c.digest, &got) { Some(c.name.clone()) } else { acc }
            })
        }
        None => None,
    };

    if let Some(name) = matched {
        let mut req = req;
        req.extensions_mut().insert(Subject(name.clone()));
        let mut res = next.run(req).await;
        // Also on the response. The audit layer is outermost, so it sees the
        // request *before* this one has run and can only learn the subject on
        // the way back out -- a middleware sees the request going in and the
        // response coming out, and an outer layer cannot read what an inner one
        // put in the request.
        res.extensions_mut().insert(Subject(name));
        return res;
    }

    // The same answer whether the header was missing, malformed, or simply
    // wrong. Distinguishing them tells an unauthenticated caller which half to
    // work on.
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
        "unauthorized\n",
    )
        .into_response()
}

/// Build a rustls acceptor from a PEM certificate chain and private key (REL-09).
///
/// `rustls_pki_types::PemObject` rather than `rustls-pemfile`: the latter is
/// unmaintained (RUSTSEC-2025-0134) and `cargo deny` fails on it, which is how
/// every pull request got stuck earlier today. The parsing is four lines either
/// way.
fn tls_acceptor(
    cert_pem: &str,
    key_pem: &str,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(cert_pem.as_bytes()).collect::<Result<_, _>>()?;
    if certs.is_empty() {
        return Err("the certificate file contains no certificate".into());
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes())?;

    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Accept TLS connections and serve `app` over each.
///
/// A failed handshake drops that connection and nothing else: a client with
/// the wrong certificate, or one probing the port, must not be able to stop
/// the listener.
async fn serve_tls(
    listener: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    app: Router,
) -> Result<(), Box<dyn std::error::Error>> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;

    loop {
        let (stream, _peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let Ok(tls) = acceptor.accept(stream).await else {
                return; // handshake refused: their certificate, not our listener
            };
            let service = hyper::service::service_fn(move |req| {
                use tower::Service;
                app.clone().call(req)
            });
            let _ = Builder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(TokioIo::new(tls), service)
                .await;
        });
    }
}

/// The subject a request authenticated as, put in the request's extensions by
/// `require_credential` so the audit layer can name who did something.
///
/// Absent when no credential file is configured -- an unauthenticated server
/// has no subject to record, and writing "anonymous" as though it were an
/// identity would make the log look more informative than it is.
#[derive(Clone, Debug)]
pub struct Subject(pub String);

/// Append-only record of every request that can change state (REL-08).
///
/// One JSON object per line, flushed on every write. An audit log buffered in
/// memory is the one kind of log that must not lose its tail: the entries worth
/// having are the ones written just before something went wrong. That is the
/// same argument `SAMYAMA_FSYNC` makes about the WAL, at a thousandth of the
/// volume.
pub struct AuditLog {
    sink: std::sync::Mutex<std::io::BufWriter<std::fs::File>>,
    path: std::path::PathBuf,
}

impl std::fmt::Debug for AuditLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLog").field("path", &self.path).finish()
    }
}

impl AuditLog {
    /// Open `path` for appending, creating it if absent.
    pub fn open(path: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { sink: std::sync::Mutex::new(std::io::BufWriter::new(file)), path })
    }

    /// Record one entry. Failures are reported and do not fail the request:
    /// a full disk must not become a denial of service on the API, and the
    /// warning is what says the log has a hole.
    fn record(&self, subject: &str, method: &str, path: &str, status: u16) {
        use std::io::Write;
        let line = format!(
            "{{\"at\":\"{}\",\"subject\":{},\"method\":\"{}\",\"path\":{},\"status\":{}}}\n",
            chrono::Utc::now().to_rfc3339(),
            serde_json::to_string(subject).unwrap_or_else(|_| "\"?\"".into()),
            method,
            serde_json::to_string(path).unwrap_or_else(|_| "\"?\"".into()),
            status,
        );
        let mut sink = match self.sink.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        if let Err(e) = sink.write_all(line.as_bytes()).and_then(|()| sink.flush()) {
            tracing::warn!("audit log write failed, the log now has a hole: {e}");
        }
    }
}

/// Record every request that can change state.
///
/// **Selected by method, not by a list of routes.** `POST`, `PUT`, `PATCH` and
/// `DELETE` are recorded; `GET`, `HEAD` and `OPTIONS` are not. A list of write
/// routes is a list somebody forgets to extend -- the same reason the
/// credential layer exempts nothing -- and a new endpoint is audited the day it
/// is added rather than the day someone remembers.
///
/// The consequence, stated rather than hidden: a **read** submitted as
/// `POST /api/query` is recorded too. Telling it apart means parsing Cypher in
/// a middleware, and an audit log that occasionally over-records is worth more
/// than one that occasionally misses a write.
///
/// The body is never recorded. It carries the query, which carries the data.
async fn audit(
    log: Arc<AuditLog>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::http::Method;

    let method = req.method().clone();
    if matches!(method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return next.run(req).await;
    }
    let path = req.uri().path().to_string();

    let res = next.run(req).await;
    // Read off the *response*: this layer is outermost, so the credential layer
    // has not run yet when the request passes through it on the way in.
    // `unauthenticated` is a state, not a name -- it is what a request that
    // presented no accepted credential gets, including the 401s.
    let subject = res
        .extensions()
        .get::<Subject>()
        .map(|s| s.0.clone())
        .unwrap_or_else(|| "unauthenticated".to_string());
    log.record(&subject, method.as_str(), &path, res.status().as_u16());
    res
}

/// Is this address loopback-only?
///
/// Used for the warning on start-up, so it errs towards warning: anything that
/// does not parse as a loopback IP is treated as routable.
fn is_loopback(host: &str) -> bool {
    host.parse::<std::net::IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
        || host.eq_ignore_ascii_case("localhost")
}

/// CORS for exactly the configured origins.
///
/// An empty list produces a layer that allows no cross-origin request, which is
/// the default. `CorsLayer::permissive()` was the previous behaviour and it
/// accepts any origin with credentials-free requests — enough for a page on the
/// open web to drive `/api/query`, which executes arbitrary Cypher (#1328).
fn cors_layer(origins: &[String]) -> CorsLayer {
    if origins.is_empty() {
        return CorsLayer::new();
    }
    let parsed: Vec<axum::http::HeaderValue> = origins
        .iter()
        .filter_map(|o| match o.parse() {
            Ok(v) => Some(v),
            Err(_) => {
                tracing::warn!("ignoring unparseable CORS origin {o:?}");
                None
            }
        })
        .collect();
    CorsLayer::new()
        .allow_origin(parsed)
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
}

/// Shared application state for HTTP routes
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<RwLock<GraphStore>>,
    pub engine: Arc<QueryEngine>,
    /// Data directory for persisting snapshots (HA-08)
    pub data_path: Option<String>,
    /// Tenant manager for multi-tenancy support
    pub tenant_manager: Option<Arc<TenantManager>>,
    /// Global embed pipeline (fallback when tenant has no embed_config)
    pub embed_pipeline: Option<Arc<EmbedPipeline>>,
    /// Per-tenant EmbedPipeline cache; invalidated on PATCH /api/tenants/:id
    pub embed_cache: Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>>,
    /// Persistence for writes made over HTTP (#1094, #1106). Without it a write
    /// lives only in memory, and every one of them returns success.
    pub persistence: Option<Arc<crate::persistence::PersistenceManager>>,
    /// Key for encrypting snapshots at rest (REL-09). `None` exports plaintext,
    /// which is the default; import sniffs the file either way.
    pub snapshot_key: Option<Arc<[u8; crate::snapshot::encryption::KEY_BYTES]>>,
    /// Open HTTP transactions, each holding the writer's lock (#1200 step 6b).
    pub transactions: super::transactions::TxnSessions,
}

impl AppState {
    /// Take the write lock, run a mutation, and persist whatever it changed.
    ///
    /// Every HTTP path that mutates the graph goes through here. Five did not
    /// (#1106) — Parquet, CSV and JSON import, `/api/enrich` and `/api/verify` all
    /// wrote to memory and returned 200 with nothing on disk — and the shape of the
    /// miss is why this is a method and not a fourth copy of the same six lines:
    /// each handler took the lock itself, so the fix to `query_handler` was not
    /// something the others could inherit.
    ///
    /// The log is applied on the outcome of the *store*, not of `body`. A statement
    /// that fails partway does not undo the rows it already wrote — the engine has
    /// no statement rollback (LANG-07) — and REL-06 does not let disk disagree with
    /// memory about rows that are visible.
    ///
    /// `GraphStore` is passed by `&mut` rather than the guard so that a body cannot
    /// hold the lock past the persist.
    /// Has a write failed to reach disk? A handler that is about to write
    /// should ask before it does.
    ///
    /// Exposed here rather than left to each handler to remember, because the
    /// failure mode of forgetting is silent.
    pub fn writes_refused(&self) -> Option<String> {
        if self.persistence.is_some() && crate::persistence::health::is_degraded() {
            Some(crate::persistence::health::refusal())
        } else {
            None
        }
    }

    pub async fn mutate<T>(
        &self,
        graph: &str,
        body: impl FnOnce(&mut GraphStore) -> T,
    ) -> T {
        let mut store = self.store.write().await;
        if self.persistence.is_some() {
            store.enable_write_log();
        }
        let out = body(&mut store);
        if let Some(pm) = &self.persistence {
            let mutations = store.take_write_log();
            match pm.apply_mutations(graph, &store, &mutations) {
                Ok(n) => tracing::debug!("persisted {n} entities from {} mutations", mutations.len()),
                Err(e) => {
                    // `mutate` returns the body's own value and has no channel
                    // for an error, so the handler cannot be told here. What it
                    // can do is refuse the *next* write, which is what
                    // `is_degraded` below is for — and the handler that calls
                    // this checks it before running (#1274).
                    tracing::warn!("failed to persist {} mutations: {e}", mutations.len());
                    crate::persistence::health::mark_degraded(e.to_string());
                }
            }
        }
        out
    }
}

/// HTTP server managing the Visualizer API and static assets
pub struct HttpServer {
    store: Arc<RwLock<GraphStore>>,
    port: u16,
    data_path: Option<String>,
    tenants: Option<Arc<TenantManager>>,
    embed_pipeline: Option<Arc<EmbedPipeline>>,
    persistence: Option<Arc<crate::persistence::PersistenceManager>>,
    /// Address to listen on. Loopback unless asked otherwise (#1328).
    bind_host: String,
    /// Origins allowed to make cross-origin calls, and the only origins the
    /// Private Network Access opt-in is echoed to. Empty by default (#1328).
    allowed_origins: Vec<String>,
    /// Credentials the server accepts. Empty means the API is unauthenticated,
    /// which is the default and the state this server shipped in (#1328).
    credentials: Vec<Credential>,
    /// PEM certificate chain and private key. `None` serves plain HTTP, which
    /// is the default and what every existing deployment does (REL-09).
    tls: Option<(String, String)>,
    /// Where state-changing requests are recorded. `None` records nothing,
    /// which is the default (REL-08).
    audit: Option<Arc<AuditLog>>,
    /// Key for encrypting snapshots at rest (REL-09).
    snapshot_key: Option<Arc<[u8; crate::snapshot::encryption::KEY_BYTES]>>,
}

impl HttpServer {
    /// Create a new HTTP server
    pub fn new(store: Arc<RwLock<GraphStore>>, port: u16) -> Self {
        Self {
            store,
            port,
            data_path: None,
            tenants: None,
            embed_pipeline: None,
            persistence: None,
            // Loopback, not 0.0.0.0. `/api/query` executes arbitrary Cypher
            // including DELETE and nothing authenticates the caller, so the
            // default must not put that on a routable address. The Docker image
            // passes `--host 0.0.0.0` explicitly, which is the deliberate act
            // this default exists to require (#1328).
            bind_host: "127.0.0.1".to_string(),
            allowed_origins: Vec::new(),
            credentials: Vec::new(),
            tls: None,
            audit: None,
            snapshot_key: None,
        }
    }

    /// Address to listen on. Anything other than a loopback address publishes
    /// an unauthenticated write API to the network.
    pub fn with_bind_host(mut self, host: impl Into<String>) -> Self {
        self.bind_host = host.into();
        self
    }

    /// Origins permitted to call this server from a browser.
    ///
    /// Empty means no cross-origin request is allowed and the Private Network
    /// Access header is never echoed. `CorsLayer::permissive()` plus an
    /// unconditional PNA echo used to accept every origin, which turned the
    /// browser's defence against a public page reaching a private address into
    /// a permission granted to whoever asked (#1328).
    pub fn with_allowed_origins(mut self, origins: Vec<String>) -> Self {
        self.allowed_origins = origins;
        self
    }

    /// Credentials the server will accept on the request path (REL-08).
    ///
    /// Empty leaves the API unauthenticated, which is what it has always been
    /// and what the start-up warning is about. This is a change to what an
    /// operator can choose, not to what they get without asking -- the same
    /// shape as `SAMYAMA_FSYNC`. Turning it on by default would break every
    /// existing deployment on upgrade, silently for anyone who does not read
    /// the release notes until their client stops working.
    pub fn with_credentials(mut self, credentials: Vec<Credential>) -> Self {
        self.credentials = credentials;
        self
    }

    /// Serve TLS from this PEM certificate chain and private key (REL-09).
    ///
    /// `None` serves plain HTTP. Off by default for the same reason
    /// authentication is: every deployment that exists today speaks HTTP to
    /// this port, and an upgrade that started refusing them would be a worse
    /// failure than the one it fixes.
    ///
    /// There is no self-signed fallback. A server that quietly invents a
    /// certificate teaches its clients to skip verification, and a client that
    /// skips verification has no transport security at all -- it has the cost
    /// of TLS and none of the guarantee.
    pub fn with_tls(mut self, cert_pem: impl Into<String>, key_pem: impl Into<String>) -> Self {
        self.tls = Some((cert_pem.into(), key_pem.into()));
        self
    }

    /// Record every state-changing request to this log (REL-08).
    ///
    /// Off by default. An audit log is only useful if somebody reads it, and
    /// writing one nobody asked for to a path nobody chose is how a disk fills
    /// up on a machine that was working yesterday.
    pub fn with_audit_log(mut self, log: Arc<AuditLog>) -> Self {
        self.audit = Some(log);
        self
    }

    /// Encrypt exported snapshots with this key (REL-09).
    ///
    /// Import is unaffected by whether this is set: an encrypted file is
    /// recognised by its magic and a plaintext one is read as before, so
    /// turning encryption on does not strand the snapshots already taken.
    pub fn with_snapshot_key(
        mut self,
        key: Arc<[u8; crate::snapshot::encryption::KEY_BYTES]>,
    ) -> Self {
        self.snapshot_key = Some(key);
        self
    }

    /// Set the data directory for snapshot persistence (HA-08)
    pub fn with_data_path(mut self, path: Option<String>) -> Self {
        self.data_path = path;
        self
    }

    /// HA-09: share a `TenantManager` with the RESP command handler so
    /// tenants created via HTTP are immediately visible to `GRAPH.LIST`.
    pub fn with_tenant_manager(mut self, tenants: Arc<TenantManager>) -> Self {
        self.tenants = Some(tenants);
        self
    }

    /// Persist writes made through the HTTP query endpoint (#1094).
    pub fn with_persistence(mut self, pm: Arc<crate::persistence::PersistenceManager>) -> Self {
        self.persistence = Some(pm);
        self
    }

    /// Set a global embed pipeline used as fallback for all tenants that have
    /// no per-tenant embed_config configured.
    pub fn with_embed_pipeline(mut self, pipeline: Arc<EmbedPipeline>) -> Self {
        self.embed_pipeline = Some(pipeline);
        self
    }

    /// Build the router this server serves, layers and all.
    ///
    /// Separate from `start` so tests can exercise the *shipped* stack. The existing HTTP
    /// tests each assemble their own miniature router, which means anything that lives in a
    /// layer -- CORS, the Private Network Access opt-in (#342) -- was untestable and
    /// therefore untested.
    pub fn router(&self) -> Router {
        self.build_router()
    }

    /// Start the HTTP server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let app = self.build_router();

        let addr = format!("{}:{}", self.bind_host, self.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        let scheme = if self.tls.is_some() { "https" } else { "http" };
        info!("HTTP API on {scheme}://{addr}");
        if !is_loopback(&self.bind_host) {
            if self.credentials.is_empty() {
                tracing::warn!(
                    "listening on {} — /api/query executes arbitrary Cypher and no \
                     credential is read off the request; pass --auth-file (#1328)",
                    self.bind_host
                );
            }
            if self.tls.is_none() {
                tracing::warn!(
                    "listening on {} without TLS — queries, results and any \
                     bearer token cross the network in cleartext; pass \
                     --tls-cert and --tls-key (REL-09)",
                    self.bind_host
                );
            }
        }

        match &self.tls {
            None => axum::serve(listener, app).await?,
            Some((cert, key)) => {
                let acceptor = tls_acceptor(cert, key)
                    .map_err(|e| format!("TLS certificate or key is unusable: {e}"))?;
                serve_tls(listener, acceptor, app).await?;
            }
        }

        Ok(())
    }

    fn build_router(&self) -> Router {
        let embed_cache: Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let state = AppState {
            store: Arc::clone(&self.store),
            engine: Arc::new(QueryEngine::new().with_plan_hash(true)),
            data_path: self.data_path.clone(),
            tenant_manager: self.tenants.clone(),
            embed_pipeline: self.embed_pipeline.clone(),
            embed_cache: Arc::clone(&embed_cache),
            persistence: self.persistence.clone(),
            snapshot_key: self.snapshot_key.clone(),
            transactions: Default::default(),
        };

        let optimize_state = Arc::new(super::optimize::OptimizeState::default());

        let main_router = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/query/export", post(export_handler))
            .route("/api/tx/begin", post(super::transactions::begin_handler))
            .route("/api/tx/:id/commit", post(super::transactions::commit_handler))
            .route("/api/tx/:id/rollback", post(super::transactions::rollback_handler))
            .route("/api/import/parquet", post(import_parquet_handler))
            .route("/api/enrich/policy", post(set_enrich_policy_handler))
            .route("/api/enrich", post(enrich_handler))
            .route("/api/verify", post(verify_handler))
            .route("/api/nlq", post(nlq_handler))
            .route("/api/status", get(status_handler))
            .route("/api/memory", get(memory_handler))
            .route("/metrics", get(metrics_handler))
            .route("/api/schema", get(schema_handler))
            .route("/api/sample", post(sample_handler))
            .route("/api/import/csv", post(import_csv_handler))
            .route("/api/import/json", post(import_json_handler))
            .route("/api/vector/indexes", get(list_indexes_handler))
            .route("/api/vector/indexes", post(create_index_handler))
            .route("/api/vector-search", post(search_handler))
            .route("/api/snapshot/export", post(export_snapshot_handler))
            .route("/api/snapshot/import", post(restore_snapshot_handler)
                // 64 GB cap. PubMed-v2 (11 GB) and trifecta-pubmed (12 GB) need
                // headroom; 64 GB lets per-source snapshots up to ~50 GB through.
                // Body is buffered in memory by the multipart extractor — see #197
                // follow-up for streaming-to-disk to drop the RAM ceiling.
                .layer(DefaultBodyLimit::max(64 * 1024 * 1024 * 1024)))
            .with_state(state);

        let mut app = main_router
            .merge(super::optimize::router().with_state(optimize_state));

        if let Some(tm) = self.tenants.as_ref() {
            app = app.merge(super::tenants::router(Arc::clone(tm), Arc::clone(&embed_cache)));
        }

        // Authentication goes on *before* CORS, which makes it the inner layer:
        // layers apply outermost-last, so the last `.layer()` call runs first.
        // A preflight has to reach the CORS layer to be answered, and it carries
        // no credential; putting authentication outside CORS would reject it
        // before CORS could reply, and every cross-origin call would fail at the
        // preflight. `require_credential` lets `OPTIONS` through for the same
        // reason, so the ordering and the exemption agree rather than one
        // covering for the other.
        if !self.credentials.is_empty() {
            let creds = Arc::new(self.credentials.clone());
            app = app.layer(axum::middleware::from_fn(move |req, next| {
                let creds = Arc::clone(&creds);
                async move { require_credential(creds, req, next).await }
            }));
        }

        let cors = cors_layer(&self.allowed_origins);
        let origins = Arc::new(self.allowed_origins.clone());
        app = app.layer(cors).layer(axum::middleware::from_fn(
            move |req, next| {
                let origins = Arc::clone(&origins);
                async move { allow_private_network(origins, req, next).await }
            },
        ));

        // Added last, which makes it the **outermost** layer: layers apply
        // outermost-last, so the most recent `.layer()` call is the one a
        // request meets first.
        //
        // Outermost on purpose, and the test is what settled it. The first
        // version added this before the credential layer on the reasoning that
        // "first" meant "outside", which is backwards -- the credential layer
        // ended up outside and short-circuited every 401 before the audit could
        // see it, so refused writes were the one thing the log missed. A 401 on
        // `/api/query` is exactly the entry an audit log exists for.
        //
        // The subject is still available: `require_credential` puts it in the
        // request's extensions on the way *in*, and this layer reads it there.
        if let Some(log) = self.audit.clone() {
            app = app.layer(axum::middleware::from_fn(move |req, next| {
                let log = Arc::clone(&log);
                async move { audit(log, req, next).await }
            }));
        }

        app
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryEngine;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    #[test]
    fn test_http_server_new() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = HttpServer::new(Arc::clone(&store), 9090);

        assert_eq!(server.port, 9090);
        // The store Arc should have 2 strong refs (original + server)
        assert_eq!(Arc::strong_count(&store), 2);
    }

    #[test]
    fn test_http_server_new_different_ports() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let s1 = HttpServer::new(Arc::clone(&store), 8080);
        let s2 = HttpServer::new(Arc::clone(&store), 8081);

        assert_eq!(s1.port, 8080);
        assert_eq!(s2.port, 8081);
        // 3 strong refs: original + s1 + s2
        assert_eq!(Arc::strong_count(&store), 3);
    }

    #[test]
    fn test_app_state_clone() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
        };

        let cloned = state.clone();

        // Both should point to the same underlying store and engine
        assert!(Arc::ptr_eq(&state.store, &cloned.store));
        assert!(Arc::ptr_eq(&state.engine, &cloned.engine));
    }

    #[test]
    fn test_app_state_store_is_shared() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
        };

        let cloned = state.clone();

        // After clone, Arc strong_count should be 2
        assert_eq!(Arc::strong_count(&state.store), 2);
        assert_eq!(Arc::strong_count(&state.engine), 2);

        drop(cloned);

        // After dropping clone, strong_count back to 1
        assert_eq!(Arc::strong_count(&state.store), 1);
        assert_eq!(Arc::strong_count(&state.engine), 1);
    }

    #[test]
    fn test_app_state_multiple_clones() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
        };

        let c1 = state.clone();
        let c2 = state.clone();
        let c3 = c1.clone();

        assert_eq!(Arc::strong_count(&state.store), 4);
        assert_eq!(Arc::strong_count(&state.engine), 4);

        assert!(Arc::ptr_eq(&state.store, &c2.store));
        assert!(Arc::ptr_eq(&c1.store, &c3.store));
    }

    #[tokio::test]
    async fn test_app_state_store_read_write() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
        };

        // Write through the state
        {
            let mut store = state.store.write().await;
            let n = store.create_node("Test");
            store.get_node_mut(n).unwrap().set_property("key", "value");
        }

        // Read through a clone
        let cloned = state.clone();
        {
            let store = cloned.store.read().await;
            assert_eq!(store.node_count(), 1);
        }
    }

    #[test]
    fn test_static_handler_returns_html() {
        // Assets::get("index.html") should return Some for the embedded file
        let asset = Assets::get("index.html");
        assert!(asset.is_some(), "index.html should be embedded via RustEmbed");
        let content = asset.unwrap();
        let html = std::str::from_utf8(content.data.as_ref()).unwrap();
        assert!(html.contains("<html") || html.contains("<!DOCTYPE") || html.contains("<body"),
            "Embedded file should contain HTML content");
    }

    #[tokio::test]
    async fn test_router_construction() {
        // Verify that the Router can be built without panicking
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
        };

        // Deliberately no CORS layer here. This assembles a miniature router to
        // check the handlers type-check together; the shipped stack is what
        // `HttpServer::router()` returns and what `tests/http_origin_allowlist.rs`
        // drives. A permissive layer in a test router proves nothing about the
        // server and leaves the string in the source, where CH-SEC reads it and
        // reports a permissive CORS policy the server does not have (#1328).
        let _app: Router = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .with_state(state);
    }

    #[tokio::test]
    async fn test_static_handler_response() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
        };

        let app = Router::new()
            .route("/", get(static_handler))
            .with_state(state);

        let req: axum::http::Request<Body> = axum::http::Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let html = std::str::from_utf8(&bytes).unwrap();
        assert!(html.contains("<html") || html.contains("<!DOCTYPE") || html.contains("<body"),
            "Static handler should return HTML content");
    }
}
