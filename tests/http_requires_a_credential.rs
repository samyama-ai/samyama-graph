//! The HTTP API can require a credential (REL-08, #1328).
//!
//! `/api/query` executes arbitrary Cypher, including DELETE, and until now
//! nothing on the request path read a credential of any kind. The default is
//! unchanged — an unconfigured server is still open, which is what every
//! existing deployment expects — so what is tested here is that configuring it
//! actually closes the door, and that it closes it on *every* route rather than
//! on the one the test happened to pick.
//!
//! # Through the shipped router
//!
//! Every case goes through `HttpServer::router()`, the same chain the binary
//! serves: PNA middleware, CORS, then this. A hand-built `Router` with the
//! handler attached would prove the handler works and nothing about whether the
//! layer is reached — and the layer ordering is the part most likely to be
//! wrong, because a preflight carries no credential and must still be answered.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::server::{read_credentials, Credential, HttpServer};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// sha256("s3cret-token") — the digest a credential file would carry.
const TOKEN: &str = "s3cret-token";

fn digest_of(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// A fresh file per call. Keyed on a counter rather than on the content,
/// because the first version named it after `body.len()` and two tests writing
/// different credentials of the same length raced each other -- one truncating
/// the file the other was reading, which surfaced as "names no credentials"
/// from a file that had two.
fn creds_file(body: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!("samyama-auth-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join(format!("creds-{}.txt", N.fetch_add(1, Ordering::Relaxed)));
    std::fs::write(&path, body).expect("write");
    path
}

fn credentials(tokens: &[(&str, &str)]) -> Vec<Credential> {
    let body: String =
        tokens.iter().map(|(n, t)| format!("{n}:{}\n", digest_of(t))).collect();
    read_credentials(&creds_file(&body)).expect("parse")
}

fn app(creds: Vec<Credential>) -> axum::Router {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    HttpServer::new(store, 0)
        .with_allowed_origins(vec!["https://app.example".to_string()])
        .with_credentials(creds)
        .router()
}

async fn status_of(app: axum::Router, req: Request<Body>) -> StatusCode {
    app.oneshot(req).await.expect("response").status()
}

fn get(path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method("GET").uri(path);
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).expect("request")
}

#[tokio::test]
async fn without_a_credential_the_api_is_open_as_before() {
    // The default. Every deployment that exists today runs this way, and an
    // upgrade that started refusing their traffic would be a worse failure than
    // the one this fixes.
    assert_eq!(status_of(app(Vec::new()), get("/api/status", None)).await, StatusCode::OK);
}

#[tokio::test]
async fn a_configured_server_refuses_a_request_with_no_credential() {
    let s = status_of(app(credentials(&[("ops", TOKEN)])), get("/api/status", None)).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_right_token_is_accepted_and_a_wrong_one_is_not() {
    let creds = credentials(&[("ops", TOKEN)]);
    assert_eq!(
        status_of(app(creds.clone()), get("/api/status", Some(TOKEN))).await,
        StatusCode::OK
    );
    assert_eq!(
        status_of(app(creds.clone()), get("/api/status", Some("wrong"))).await,
        StatusCode::UNAUTHORIZED
    );
    // A prefix of the real token must not pass. This is what a comparison that
    // stopped at the first differing byte would leak, one byte at a time.
    assert_eq!(
        status_of(app(creds), get("/api/status", Some(&TOKEN[..5]))).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn any_of_several_credentials_is_accepted() {
    // More than one line, so revoking one is deleting a line rather than
    // rotating everybody at once.
    let creds = credentials(&[("ops", TOKEN), ("ci", "another-token")]);
    for t in [TOKEN, "another-token"] {
        assert_eq!(
            status_of(app(creds.clone()), get("/api/status", Some(t))).await,
            StatusCode::OK,
            "token {t} should be accepted"
        );
    }
    assert_eq!(
        status_of(app(creds), get("/api/status", Some("neither"))).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn the_scheme_must_be_bearer_and_the_header_must_be_well_formed() {
    let creds = credentials(&[("ops", TOKEN)]);
    for header in [
        format!("Basic {TOKEN}"),          // right token, wrong scheme
        TOKEN.to_string(),                 // no scheme at all
        format!("Bearer{TOKEN}"),          // no separator
        "Bearer".to_string(),              // scheme with nothing after it
    ] {
        let req = Request::builder()
            .method("GET")
            .uri("/api/status")
            .header("authorization", &header)
            .body(Body::empty())
            .expect("request");
        assert_eq!(
            status_of(app(creds.clone()), req).await,
            StatusCode::UNAUTHORIZED,
            "{header:?} should not authenticate"
        );
    }
    // `bearer` lower-case is the same scheme: RFC 7235 says the scheme is
    // case-insensitive, and a client that sends it must not be refused.
    let req = Request::builder()
        .method("GET")
        .uri("/api/status")
        .header("authorization", format!("bearer {TOKEN}"))
        .body(Body::empty())
        .expect("request");
    assert_eq!(status_of(app(creds), req).await, StatusCode::OK);
}

#[tokio::test]
async fn a_refusal_says_bearer_and_nothing_about_which_half_was_wrong() {
    let res = app(credentials(&[("ops", TOKEN)]))
        .oneshot(get("/api/status", Some("wrong")))
        .await
        .expect("response");
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        res.headers().get("www-authenticate").and_then(|v| v.to_str().ok()),
        Some("Bearer"),
        "a 401 has to say how to authenticate"
    );
}

#[tokio::test]
async fn the_preflight_is_answered_without_a_credential() {
    // The ordering case. A browser sends OPTIONS *before* deciding whether the
    // real request is allowed, and it carries no Authorization header. If
    // authentication ran outside CORS, or did not exempt OPTIONS, every
    // cross-origin call would fail at the preflight -- and the usual repair for
    // that is to turn authentication off.
    let req = Request::builder()
        .method("OPTIONS")
        .uri("/api/query")
        .header("origin", "https://app.example")
        .header("access-control-request-method", "POST")
        .body(Body::empty())
        .expect("request");
    let res = app(credentials(&[("ops", TOKEN)])).oneshot(req).await.expect("response");
    assert!(
        res.status().is_success(),
        "the preflight was refused with {}",
        res.status()
    );
    assert_eq!(
        res.headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://app.example"),
        "CORS still has to answer the preflight it was reached for"
    );
}

#[tokio::test]
async fn every_route_is_covered_and_not_just_the_one_this_test_picked() {
    // An exemption list is the thing that quietly grows, so there is none --
    // `/metrics` and `/` are authenticated too. `/api/status` alone reports node
    // and edge counts, and `/` is the page that tells a caller where the API is.
    let creds = credentials(&[("ops", TOKEN)]);
    for path in ["/", "/metrics", "/api/status", "/api/schema", "/api/memory"] {
        assert_eq!(
            status_of(app(creds.clone()), get(path, None)).await,
            StatusCode::UNAUTHORIZED,
            "GET {path} was not authenticated"
        );
    }
    // POST as well: the layer is global, but the route table is not, and a
    // write path left open would be the one that matters.
    let req = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"query":"MATCH (n) DETACH DELETE n"}"#))
        .expect("request");
    assert_eq!(
        status_of(app(creds), req).await,
        StatusCode::UNAUTHORIZED,
        "an unauthenticated DELETE reached the query handler"
    );
}

#[test]
fn a_malformed_credential_file_is_an_error_not_a_skipped_line() {
    // Skipping is how a typo becomes a server that starts cleanly and accepts
    // one fewer credential than the operator believes it does.
    let good = digest_of(TOKEN);
    for (body, why) in [
        (format!("ops{good}\n"), "no colon"),
        (format!("ops:{}\n", &good[..10]), "digest too short"),
        (format!("ops:{}zz\n", &good[..62]), "not hexadecimal"),
    ] {
        assert!(
            read_credentials(&creds_file(&body)).is_err(),
            "{why} should be refused: {body:?}"
        );
    }
    // And a file naming nobody, which would refuse every request while looking
    // like a configured server.
    assert!(read_credentials(&creds_file("# only a comment\n\n")).is_err());

    // Comments and blank lines around real entries are fine.
    let ok = read_credentials(&creds_file(&format!(
        "# operators\n\nops:{good}\n\n# ci\nci:{good}\n"
    )))
    .expect("should parse");
    assert_eq!(ok.len(), 2);
    assert_eq!(ok[0].name, "ops");
    assert_eq!(ok[1].name, "ci");
}
