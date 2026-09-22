//! The HTTP listener can serve TLS (REL-09).
//!
//! REL-09 asks for "TLS 1.3 for all protocols; at-rest encryption for storage
//! and snapshots; key rotation without downtime". Nothing served TLS at all:
//! `rustls` and `tokio-rustls` were already in the dependency tree through
//! `reqwest`, but that is the NLQ and embedding clients calling *out* to
//! someone else's API. A client speaking HTTPS is not an acceptor on our
//! socket.
//!
//! # The test verifies the certificate
//!
//! It would be much shorter to point a client at the server with certificate
//! verification turned off. That test passes against a server offering no TLS
//! worth the name — a self-signed certificate nobody checks, an expired one, a
//! certificate for a different host. So this one builds a root, issues a leaf
//! from it, hands the leaf to the server, and gives the client **only that
//! root**: the handshake succeeds because the chain verifies, and the negative
//! case below shows an unrelated root is refused.
//!
//! That is also why there is no self-signed fallback in the server. A server
//! that quietly invents a certificate teaches its clients to skip
//! verification, and a client that skips verification has the cost of TLS and
//! none of the guarantee.

use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// A CA and a leaf certificate for `localhost`, as PEM.
///
/// Returned as (ca_pem, leaf_cert_pem, leaf_key_pem). The leaf is signed by
/// the CA so the client has something to verify *against*, rather than being
/// asked to trust whatever it is shown.
fn certificates() -> (String, String, String) {
    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "samyama test root");
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let ca = ca_params.clone().self_signed(&ca_key).expect("self-signed ca");
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let leaf_params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).expect("leaf params");
    let leaf_key = rcgen::KeyPair::generate().expect("leaf key");
    let leaf = leaf_params
        .signed_by(&leaf_key, &issuer)
        .expect("leaf signed by ca");

    (ca.pem(), leaf.pem(), leaf_key.serialize_pem())
}

/// Start a server on an ephemeral port and return the port it took.
async fn serve(cert: String, key: String) -> u16 {
    // Port 0 then read it back: a fixed port makes the test fail for the wrong
    // reason on a machine already using it, and two of these run at once.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);

    let store = Arc::new(RwLock::new(GraphStore::new()));
    let server = HttpServer::new(store, port).with_tls(cert, key);
    tokio::spawn(async move {
        let _ = server.start().await;
    });

    // Wait for the port to accept rather than sleeping a fixed amount, which
    // is the flake this kind of test usually ships with.
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return port;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("the TLS server never accepted a connection on port {port}");
}

/// A rustls client that trusts exactly `roots_pem` and nothing else.
fn client_config(roots_pem: &str) -> tokio_rustls::rustls::ClientConfig {
    use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer};
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for der in CertificateDer::pem_slice_iter(roots_pem.as_bytes()) {
        roots.add(der.expect("pem cert")).expect("add root");
    }
    tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// `GET /api/status` over TLS. Returns the response text, or the handshake error.
async fn get_over_tls(port: u16, roots_pem: &str) -> Result<String, String> {
    use tokio_rustls::TlsConnector;

    let connector = TlsConnector::from(Arc::new(client_config(roots_pem)));
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let domain = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost")
        .expect("server name");
    let mut tls = connector
        .connect(domain, stream)
        .await
        .map_err(|e| format!("handshake: {e}"))?;

    tls.write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    tls.read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

#[tokio::test]
async fn a_verified_client_gets_an_answer_over_tls() {
    let (ca, cert, key) = certificates();
    let port = serve(cert, key).await;

    let body = get_over_tls(port, &ca)
        .await
        .unwrap_or_else(|e| panic!("the handshake or request failed: {e}"));

    assert!(
        body.starts_with("HTTP/1.1 200"),
        "expected a 200 over TLS, got:\n{}",
        &body[..body.len().min(200)]
    );
    assert!(
        body.contains("nodes"),
        "the response is not /api/status:\n{}",
        &body[..body.len().min(400)]
    );
}

#[tokio::test]
async fn a_client_that_does_not_trust_the_root_is_refused() {
    // The control that makes the case above mean something. If verification
    // were off -- in the client or because the server is not really doing TLS
    // -- this would succeed too.
    let (_ca, cert, key) = certificates();
    let (other_ca, _, _) = certificates();
    let port = serve(cert, key).await;

    let err = get_over_tls(port, &other_ca)
        .await
        .expect_err("a certificate from an unrelated root must not verify");
    assert!(
        err.starts_with("handshake:"),
        "expected the failure at the handshake, got: {err}"
    );
}

#[tokio::test]
async fn plain_http_is_still_the_default() {
    // Every deployment that exists today speaks HTTP to this port. Turning TLS
    // on by default would refuse all of them on upgrade.
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);
    let server = HttpServer::new(store, port);
    tokio::spawn(async move {
        let _ = server.start().await;
    });

    let mut stream = None;
    for _ in 0..200 {
        if let Ok(s) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let mut stream = stream.expect("the plain server never accepted a connection");
    stream
        .write_all(b"GET /api/status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let body = String::from_utf8_lossy(&buf);
    assert!(
        body.starts_with("HTTP/1.1 200"),
        "cleartext HTTP must still work by default, got:\n{}",
        &body[..body.len().min(200)]
    );
}

#[tokio::test]
async fn an_unusable_certificate_stops_the_server_rather_than_serving_plaintext() {
    // Falling back to HTTP here would be the worst outcome: an operator who
    // asked for TLS would get a cleartext port and a log line they did not read.
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);

    let err = HttpServer::new(store, port)
        .with_tls("not a certificate", "not a key")
        .start()
        .await
        .expect_err("a server given an unusable certificate must not start");
    assert!(
        err.to_string().contains("TLS certificate or key is unusable"),
        "the error should name the cause, got: {err}"
    );
}
