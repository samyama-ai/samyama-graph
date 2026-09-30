//! Credential checking, audit, CORS, quota admission and the shipped router.

use super::*;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::util::ServiceExt;

/// sha256("test").
const D: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

fn cred(line: &str) -> Credential {
    Credential::parse(line).unwrap().unwrap()
}

fn b64(s: &str) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = s.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn argon2_phc(password: &str) -> String {
    use argon2::password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::encode_b64(b"samyama-http-salt").unwrap();
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn store() -> Arc<RwLock<GraphStore>> {
    Arc::new(RwLock::new(GraphStore::new()))
}

async fn send(app: Router, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, String) {
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

fn get(uri: &str, auth: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().uri(uri);
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    b.body(Body::empty()).unwrap()
}

fn post_json(uri: &str, body: &str, auth: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    b.body(Body::from(body.to_string())).unwrap()
}

// ---------- base64 / authenticate ----------

#[test]
fn base64_decodes_standard_payloads_and_stops_at_padding() {
    assert_eq!(base64_decode("").unwrap(), b"");
    assert_eq!(base64_decode("TWFu").unwrap(), b"Man");
    assert_eq!(base64_decode("TWE=").unwrap(), b"Ma");
    assert_eq!(base64_decode("TQ==").unwrap(), b"M");
    assert_eq!(
        base64_decode(&b64("alice:s3cret/+")).unwrap(),
        b"alice:s3cret/+"
    );
    assert!(
        base64_decode("TW*u").is_none(),
        "an invalid character is refused"
    );
}

#[test]
fn a_bearer_token_authenticates_by_its_digest_and_only_then() {
    let creds = vec![
        cred(&format!("svc:{D}")),
        cred(&format!("other:{}", "a".repeat(64))),
    ];
    assert_eq!(authenticate(&creds, "Bearer test").unwrap().name, "svc");
    assert_eq!(
        authenticate(&creds, "bearer   test  ").unwrap().name,
        "svc",
        "scheme is case-insensitive, token trimmed"
    );
    assert!(authenticate(&creds, "Bearer nope").is_none());
    assert!(
        authenticate(&creds, &format!("Bearer {D}")).is_none(),
        "the digest is not the token"
    );
    assert!(
        authenticate(&creds, "Bearer").is_none(),
        "no space, no credential"
    );
    assert!(
        authenticate(&creds, "Token test").is_none(),
        "an unknown scheme"
    );
}

#[test]
fn basic_auth_checks_only_the_named_users_password() {
    let phc = argon2_phc("hunter2");
    let creds = vec![cred(&format!("alice:{phc}")), cred(&format!("svc:{D}"))];
    let ok = format!("Basic {}", b64("alice:hunter2"));
    assert_eq!(authenticate(&creds, &ok).unwrap().name, "alice");
    assert!(authenticate(&creds, &format!("Basic {}", b64("alice:wrong"))).is_none());
    assert!(authenticate(&creds, &format!("Basic {}", b64("nobody:hunter2"))).is_none());
    // A token credential is never checked as a password.
    assert!(authenticate(&creds, &format!("Basic {}", b64("svc:test"))).is_none());
    assert!(authenticate(&creds, &format!("Basic {}", b64("no-colon"))).is_none());
    assert!(authenticate(&creds, "Basic !!!").is_none(), "not base64");
    assert!(authenticate(&creds, "Basic //8=").is_none(), "not UTF-8");
}

#[test]
fn a_basic_credential_with_a_corrupt_hash_never_matches() {
    let creds = vec![cred("alice:$argon2id$garbage")];
    assert!(authenticate(&creds, &format!("Basic {}", b64("alice:x"))).is_none());
}

#[test]
fn required_role_is_admin_for_management_and_write_for_imports() {
    use crate::auth::Role;
    assert_eq!(required_role(&Method::GET, "/api/tenants"), Role::Admin);
    assert_eq!(
        required_role(&Method::DELETE, "/api/tenants/x"),
        Role::Admin
    );
    assert_eq!(
        required_role(&Method::POST, "/api/snapshot/import"),
        Role::Admin
    );
    assert_eq!(
        required_role(&Method::POST, "/api/enrich/policy"),
        Role::Admin
    );
    assert_eq!(required_role(&Method::POST, "/api/import/csv"), Role::Write);
    assert_eq!(required_role(&Method::POST, "/api/enrich"), Role::Write);
    assert_eq!(
        required_role(&Method::POST, "/api/vector/indexes"),
        Role::Write
    );
    assert_eq!(
        required_role(&Method::GET, "/api/vector/indexes"),
        Role::Read
    );
    assert_eq!(required_role(&Method::POST, "/api/query"), Role::Read);
    assert_eq!(required_role(&Method::GET, "/api/status"), Role::Read);
}

#[test]
fn loopback_detection_errs_towards_warning() {
    assert!(is_loopback("127.0.0.1"));
    assert!(is_loopback("::1"));
    assert!(is_loopback("LOCALHOST"));
    assert!(!is_loopback("0.0.0.0"));
    assert!(!is_loopback("10.1.2.3"));
    assert!(!is_loopback("db.internal"));
}

// ---------- the credential layer, through the shipped router ----------

#[tokio::test]
async fn with_credentials_a_request_without_one_is_401_with_both_schemes() {
    let app = HttpServer::new(store(), 0)
        .with_credentials(vec![cred(&format!("svc:{D}"))])
        .router();
    let (status, headers, body) = send(app.clone(), get("/api/status", None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body, "unauthorized\n");
    assert_eq!(
        headers.get("www-authenticate").unwrap(),
        "Bearer, Basic realm=\"samyama\""
    );
    let (status, _, _) = send(app.clone(), get("/api/status", Some("Bearer wrong"))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _, body) = send(app, get("/api/status", Some("Bearer test"))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("\"healthy\""), "{body}");
}

#[tokio::test]
async fn a_preflight_is_let_through_without_a_credential() {
    let app = HttpServer::new(store(), 0)
        .with_credentials(vec![cred(&format!("svc:{D}"))])
        .router();
    let req = Request::builder()
        .method("OPTIONS")
        .uri("/api/query")
        .body(Body::empty())
        .unwrap();
    let (status, _, _) = send(app, req).await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_read_credential_is_forbidden_a_write_route() {
    let app = HttpServer::new(store(), 0)
        .with_credentials(vec![cred(&format!("ro:{D}:roles=read"))])
        .router();
    let req = post_json(
        "/api/import/json",
        r#"{"label":"X","nodes":[{}]}"#,
        Some("Bearer test"),
    );
    let (status, _, body) = send(app, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body.contains("missing Write role"), "{body}");
}

#[tokio::test]
async fn a_read_credential_may_run_a_read_but_not_a_write_query() {
    let s = store();
    let app = HttpServer::new(Arc::clone(&s), 0)
        .with_credentials(vec![cred(&format!("ro:{D}:roles=read"))])
        .router();
    let (status, _, body) = send(
        app.clone(),
        post_json(
            "/api/query",
            r#"{"query":"RETURN 1 AS x"}"#,
            Some("Bearer test"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _, body) = send(
        app,
        post_json(
            "/api/query",
            r#"{"query":"CREATE (:X)"}"#,
            Some("Bearer test"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(s.read().await.node_count(), 0);
}

// ---------- audit ----------

#[tokio::test]
async fn the_audit_log_records_writes_with_their_subject_and_skips_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.log");
    let log = Arc::new(AuditLog::open(&path).unwrap());
    assert!(format!("{log:?}").contains("audit.log"));
    let app = HttpServer::new(store(), 0)
        .with_credentials(vec![cred(&format!("svc:{D}"))])
        .with_audit_log(Arc::clone(&log))
        .router();

    send(app.clone(), get("/api/status", Some("Bearer test"))).await;
    send(
        app.clone(),
        post_json(
            "/api/query",
            r#"{"query":"CREATE (:A)"}"#,
            Some("Bearer test"),
        ),
    )
    .await;
    send(
        app,
        post_json("/api/query", r#"{"query":"CREATE (:B)"}"#, None),
    )
    .await;

    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<serde_json::Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2, "the GET is not audited: {text}");
    assert_eq!(lines[0]["subject"], "svc");
    assert_eq!(lines[0]["method"], "POST");
    assert_eq!(lines[0]["path"], "/api/query");
    assert_eq!(lines[0]["status"], 200);
    assert_eq!(lines[1]["subject"], "unauthenticated");
    assert_eq!(lines[1]["status"], 401);
    assert!(!text.contains("CREATE"), "the body must never be recorded");
}

#[test]
fn an_audit_log_that_cannot_be_opened_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    // A directory cannot be opened for appending.
    assert!(AuditLog::open(dir.path()).is_err());
}

// ---------- CORS and Private Network Access ----------

#[tokio::test]
async fn pna_is_echoed_only_to_an_allowed_origin_and_bad_origins_are_skipped() {
    let app = HttpServer::new(store(), 0)
        .with_allowed_origins(vec![
            "https://studio.example".to_string(),
            "bad\norigin".to_string(),
        ])
        .router();
    let preflight = |origin: &str| {
        Request::builder()
            .method("OPTIONS")
            .uri("/api/query")
            .header("origin", origin)
            .header("access-control-request-method", "POST")
            .header("access-control-request-private-network", "true")
            .body(Body::empty())
            .unwrap()
    };
    let (_, h, _) = send(app.clone(), preflight("https://studio.example")).await;
    assert_eq!(
        h.get("access-control-allow-private-network").unwrap(),
        "true"
    );
    assert_eq!(
        h.get("access-control-allow-origin").unwrap(),
        "https://studio.example"
    );
    let (_, h, _) = send(app, preflight("https://evil.example")).await;
    assert!(h.get("access-control-allow-private-network").is_none());
    assert!(h.get("access-control-allow-origin").is_none());
}

// ---------- TLS configuration ----------

#[test]
fn a_tls_acceptor_needs_a_certificate_and_a_usable_key() {
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    assert!(tls_acceptor(&cert.pem(), &key.serialize_pem()).is_ok());

    let err = tls_acceptor("", &key.serialize_pem()).err().unwrap();
    assert_eq!(
        err.to_string(),
        "the certificate file contains no certificate"
    );
    assert!(tls_acceptor(&cert.pem(), "not a key").is_err());

    // A key that does not belong to the certificate is refused too.
    let other = rcgen::KeyPair::generate().unwrap();
    assert!(tls_acceptor(&cert.pem(), &other.serialize_pem()).is_err());
}

// ---------- builder and router wiring ----------

#[tokio::test]
async fn tenant_routes_exist_only_with_a_tenant_manager() {
    let without = HttpServer::new(store(), 0).router();
    let (status, _, _) = send(without, get("/api/tenants", None)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let tm = Arc::new(TenantManager::new());
    let with = HttpServer::new(store(), 0).with_tenant_manager(tm).router();
    let (status, _, body) = send(with, get("/api/tenants", None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"default\""), "{body}");

    let direct = build_tenant_router(Arc::new(TenantManager::new()));
    let (status, _, _) = send(direct, get("/api/tenants/default", None)).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_shipped_router_serves_the_ui_and_the_optimizer() {
    let app = HttpServer::new(store(), 0)
        .with_bind_host("127.0.0.1")
        .with_data_path(None)
        .router();
    let (status, headers, body) = send(app.clone(), get("/", None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    assert!(
        body.to_lowercase().contains("<html"),
        "{}",
        &body[..body.len().min(80)]
    );
    let (status, _, body) = send(app, get("/optimize/benchmarks", None)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("sphere"));
}

#[tokio::test]
async fn a_server_with_a_snapshot_key_exports_encrypted_snapshots() {
    let key = Arc::new([7u8; crate::snapshot::encryption::KEY_BYTES]);
    let app = HttpServer::new(store(), 0).with_snapshot_key(key).router();
    let req = Request::builder()
        .method("POST")
        .uri("/api/snapshot/export")
        .body(Body::empty())
        .unwrap();
    let (status, headers, _) = send(app, req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers
        .get("content-disposition")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("snapshot.sgsnap.enc"));
}

#[tokio::test]
async fn a_server_with_persistence_writes_through_to_disk() {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(crate::persistence::PersistenceManager::new(dir.path()).unwrap());
    let app = HttpServer::new(store(), 0)
        .with_persistence(Arc::clone(&pm))
        .router();
    let (status, _, body) = send(
        app,
        post_json("/api/query", r#"{"query":"CREATE (:P), (:P)"}"#, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(pm.recover("default").unwrap().0.len(), 2);
}

#[tokio::test]
async fn a_global_embed_pipeline_serves_text_search() {
    let cfg = crate::persistence::AutoEmbedConfig {
        provider: crate::persistence::tenant::LLMProvider::Mock,
        embedding_model: "mock".into(),
        api_key: None,
        api_base_url: None,
        chunk_size: 100,
        chunk_overlap: 0,
        vector_dimension: 64,
        embedding_policies: HashMap::new(),
        embedding_property: "embedding".into(),
    };
    let pipeline = Arc::new(EmbedPipeline::new(cfg).unwrap());
    let app = HttpServer::new(store(), 0)
        .with_embed_pipeline(pipeline)
        .router();
    let (status, _, body) = send(
        app,
        post_json(
            "/api/vector-search",
            r#"{"query_text":"hello","k":3}"#,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["mode"], "text");
    assert_eq!(json["query_text"], "hello");
}

// ---------- AppState helpers ----------

fn state(persistence: Option<Arc<crate::persistence::PersistenceManager>>) -> AppState {
    AppState {
        store: store(),
        engine: Arc::new(QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(RwLock::new(HashMap::new())),
        persistence,
        snapshot_key: None,
        transactions: Default::default(),
    }
}

#[test]
fn without_persistence_nothing_is_refused_by_quota_or_health() {
    let s = state(None);
    assert_eq!(s.quota_refuses("default", u64::MAX, u64::MAX), None);
    assert_eq!(s.writes_refused(), None);
}

#[test]
fn quota_refuses_names_the_resource_that_would_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(crate::persistence::PersistenceManager::new(dir.path()).unwrap());
    pm.tenants()
        .update_quotas(
            "default",
            crate::persistence::ResourceQuotas {
                max_nodes: Some(2),
                max_edges: Some(1),
                ..crate::persistence::ResourceQuotas::unlimited()
            },
        )
        .unwrap();
    let s = state(Some(pm));
    assert_eq!(
        s.quota_refuses("default", 2, 1),
        None,
        "exactly at the limit fits"
    );
    assert_eq!(
        s.quota_refuses("default", 3, 0).as_deref(),
        Some("quota exceeded: nodes (3/2)")
    );
    assert_eq!(
        s.quota_refuses("default", 0, 2).as_deref(),
        Some("quota exceeded: edges (2/1)")
    );
    assert_eq!(s.quota_refuses("no-such-tenant", 99, 99), None);
}

#[test]
fn a_quota_on_edges_alone_leaves_nodes_unbounded() {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(crate::persistence::PersistenceManager::new(dir.path()).unwrap());
    pm.tenants()
        .update_quotas(
            "default",
            crate::persistence::ResourceQuotas {
                max_edges: Some(1),
                ..crate::persistence::ResourceQuotas::unlimited()
            },
        )
        .unwrap();
    let s = state(Some(pm));
    assert_eq!(s.quota_refuses("default", u64::MAX / 2, 1), None);
    assert_eq!(
        s.quota_refuses("default", 0, 5).as_deref(),
        Some("quota exceeded: edges (5/1)")
    );
}

#[tokio::test]
async fn an_audit_log_on_a_full_disk_does_not_fail_the_request() {
    // `/dev/full` accepts the open and fails every write with ENOSPC.
    let full = std::path::Path::new("/dev/full");
    if !full.exists() {
        return;
    }
    let log = Arc::new(AuditLog::open(full).unwrap());
    let s = store();
    let app = HttpServer::new(Arc::clone(&s), 0)
        .with_audit_log(log)
        .router();
    let (status, _, body) = send(
        app,
        post_json("/api/query", r#"{"query":"CREATE (:Kept)"}"#, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(s.read().await.node_count(), 1);
}

#[tokio::test]
async fn a_server_on_a_routable_address_still_serves_after_warning() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let server = HttpServer::new(store(), port).with_bind_host("0.0.0.0");
    let task = tokio::spawn(async move { server.start().await.map_err(|e| e.to_string()) });

    let mut stream = None;
    for _ in 0..200 {
        if let Ok(s) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let mut stream = stream.expect("the server never accepted a connection");
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("\"healthy\""), "{text}");
    task.abort();
}

#[tokio::test]
async fn a_server_that_cannot_bind_reports_the_error() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = held.local_addr().unwrap().port();
    let err = HttpServer::new(store(), port).start().await.unwrap_err();
    assert!(!err.to_string().is_empty());
    drop(held);
}

#[tokio::test]
async fn mutate_persists_what_the_body_changed_and_returns_its_value() {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(crate::persistence::PersistenceManager::new(dir.path()).unwrap());
    let s = state(Some(Arc::clone(&pm)));
    let id = s
        .mutate("default", |store| {
            let id = store.create_node("M");
            let _ = store.set_node_property(
                "default",
                id,
                "k".to_string(),
                crate::graph::PropertyValue::Integer(1),
            );
            id
        })
        .await;
    assert_eq!(s.store.read().await.node_count(), 1);
    let (nodes, _) = pm.recover("default").unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, id);
}
