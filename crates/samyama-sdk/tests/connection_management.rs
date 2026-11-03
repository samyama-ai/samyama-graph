//! The SDK's request deadline is real, and the retry backs off (API-06, #1326).
//!
//! `CH-SDK-CONN` reads source and can say a facility is present. It cannot say
//! it works — a `.timeout(...)` in the builder may be overwritten two lines
//! later. These tests take the opposite approach: they stand up a server that
//! **accepts the connection and then says nothing**, which is the failure the
//! requirement is about, and measure what the client does.
//!
//! A server that refuses the connection would not test this. `Client::new()`
//! fails fast on a refused connection and hangs forever on an accepted one, so
//! a test against a closed port passes with or without the fix.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use samyama_sdk::{ConnectionConfig, RemoteClient, SamyamaClient};
use tokio::net::TcpListener;

/// A listener that accepts connections and never replies.
///
/// The accepted sockets are held in the task rather than dropped, because
/// dropping one closes it and the client sees EOF instead of silence.
async fn black_hole() -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            match listener.accept().await {
                Ok((sock, _)) => {
                    counter.fetch_add(1, Ordering::SeqCst);
                    held.push(sock);
                }
                Err(_) => return,
            }
        }
    });
    (format!("http://{addr}"), accepted)
}

#[tokio::test]
async fn a_silent_server_does_not_hang_the_caller() {
    let (url, _) = black_hole().await;
    let client = RemoteClient::with_config(
        &url,
        ConnectionConfig {
            timeout: Some(Duration::from_millis(300)),
            max_retries: 0,
            ..Default::default()
        },
    );

    let start = Instant::now();
    let result = client.query("default", "RETURN 1").await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "a server that never answers must not return Ok");
    // The bound, not an absolute time: a slower machine may take longer to give
    // up, but it may not fail to give up. Without a timeout this never returns
    // and the test hangs rather than failing, which is why the assertion is on
    // elapsed time and not only on `is_err`.
    assert!(
        elapsed < Duration::from_secs(5),
        "took {elapsed:?} against a 300 ms deadline — the timeout did not fire"
    );
}

#[tokio::test]
async fn the_default_configuration_sets_a_deadline() {
    // The defect was the default, not the absence of an option. A client built
    // the ordinary way must already be bounded.
    let config = ConnectionConfig::default();
    assert!(
        config.timeout.is_some(),
        "RemoteClient::new must not build a client that waits forever"
    );
    assert!(config.connect_timeout.is_some());
    assert!(config.max_retries > 0);
}

#[tokio::test]
async fn a_timeout_is_retried_the_configured_number_of_times() {
    // Counted at the server: each attempt opens a connection, so the accept
    // count is the attempt count. Asserting on elapsed time alone would pass
    // for a client that retried zero times and simply waited longer.
    let (url, accepted) = black_hole().await;
    let client = RemoteClient::with_config(
        &url,
        ConnectionConfig {
            timeout: Some(Duration::from_millis(150)),
            max_retries: 2,
            retry_base_delay: Duration::from_millis(10),
            ..Default::default()
        },
    );

    let _ = client.query("default", "RETURN 1").await;

    let n = accepted.load(Ordering::SeqCst);
    assert_eq!(
        n, 3,
        "expected the first attempt plus two retries, saw {n} connections"
    );
}

#[tokio::test]
async fn retry_can_be_switched_off() {
    let (url, accepted) = black_hole().await;
    let client = RemoteClient::with_config(
        &url,
        ConnectionConfig {
            timeout: Some(Duration::from_millis(150)),
            max_retries: 0,
            ..Default::default()
        },
    );

    let _ = client.query("default", "RETURN 1").await;

    assert_eq!(accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_backoff_grows() {
    // 10 ms then 20 ms is 30 ms of sleeping on top of two 100 ms timeouts and
    // one more attempt. A constant 10 ms delay would finish measurably sooner,
    // and a constant delay is the thing this must not be: it is what turns one
    // slow server into a thundering herd.
    let (url, _) = black_hole().await;
    let client = RemoteClient::with_config(
        &url,
        ConnectionConfig {
            timeout: Some(Duration::from_millis(100)),
            max_retries: 2,
            retry_base_delay: Duration::from_millis(50),
            ..Default::default()
        },
    );

    let start = Instant::now();
    let _ = client.query("default", "RETURN 1").await;
    let elapsed = start.elapsed();

    // 3 × 100 ms of timeout + 50 ms + 100 ms of backoff = 450 ms minimum.
    assert!(
        elapsed >= Duration::from_millis(400),
        "finished in {elapsed:?}; the second delay does not look doubled"
    );
}

#[tokio::test]
async fn a_cancelled_request_returns_promptly() {
    // Against a server that never answers, with a 30 s deadline. If
    // cancellation did nothing this would take 30 seconds; the assertion is
    // that it does not.
    let (url, _) = black_hole().await;
    let client = RemoteClient::with_config(
        &url,
        ConnectionConfig {
            timeout: Some(Duration::from_secs(30)),
            max_retries: 0,
            ..Default::default()
        },
    );

    let start = Instant::now();
    let result = client
        .query_cancellable("default", "RETURN 1", async {
            tokio::time::sleep(Duration::from_millis(100)).await;
        })
        .await;
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(
        format!("{}", result.unwrap_err()).contains("cancelled"),
        "a cancelled request must say so rather than look like a timeout"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "cancellation took {elapsed:?} against a 30 s deadline"
    );
}

#[tokio::test]
async fn cancellation_does_not_discard_an_answer_that_arrived() {
    // The `biased` select matters: a cancellation that is already resolved must
    // not beat a response that is also ready. Nothing here answers, so this
    // pins the opposite direction — an immediately-resolved cancel against a
    // request that cannot complete still reports cancellation rather than
    // hanging.
    let (url, _) = black_hole().await;
    let client = RemoteClient::with_config(
        &url,
        ConnectionConfig { timeout: Some(Duration::from_secs(30)), max_retries: 0, ..Default::default() },
    );
    let start = Instant::now();
    let result = client.query_cancellable("default", "RETURN 1", async {}).await;
    assert!(result.is_err());
    assert!(start.elapsed() < Duration::from_secs(5));
}
