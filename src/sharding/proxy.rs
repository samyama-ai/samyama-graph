//! Proxy client for forwarding requests
//!
//! Handles the networking part of routing: taking a command and sending it
//! to a remote node's RESP port.

use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::protocol::RespValue;

/// Length of the complete RESP frame at the head of `buf`, if there is one.
///
/// `None` means "not yet" -- the frame has not fully arrived. `Err` means the
/// bytes are not a frame this server would ever have produced, which is worth
/// saying rather than forwarding.
///
/// The server's own decoder answers this, deliberately. A second parser written
/// here would be a second opinion about where a reply ends, and the two would
/// drift: `RespValue::decode` already handles RESP3's types and this engine's
/// inline-command fallback, and whatever `RespValue::encode` emits on the other
/// end of the socket, this delimits by construction.
fn resp_frame_len(buf: &[u8]) -> Result<Option<usize>, String> {
    if buf.is_empty() {
        return Ok(None);
    }
    // A probe copy: `decode` advances the buffer by exactly what a complete
    // value consumed and leaves a partial frame untouched, so the difference is
    // the frame length. The proxy forwards the original bytes rather than
    // re-encoding the parsed value, so a reply it cannot represent still
    // arrives unchanged.
    let mut probe = BytesMut::from(buf);
    let before = probe.len();
    match RespValue::decode(&mut probe) {
        Ok(Some(_)) => Ok(Some(before - probe.len())),
        Ok(None) => Ok(None),
        Err(crate::protocol::RespError::Incomplete) => Ok(None),
        Err(e) => Err(format!("reply is not a RESP frame: {}", e)),
    }
}

/// How long `forward` waits for a peer to accept the connection.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How much longer than the query deadline `forward` waits for a reply. A
/// forwarded query that runs out of time on the peer is answered there with
/// `Query timed out`; the margin lets that answer arrive rather than be
/// overtaken by this side giving up first.
const REPLY_MARGIN: Duration = Duration::from_secs(10);

/// The reply limit for a server whose query deadline is `query_secs`
/// (`SAMYAMA_QUERY_TIMEOUT`, `0` for none). No query deadline means no reply
/// limit either: a peer may legitimately take as long as the query does.
fn reply_timeout_for(query_secs: u64) -> Option<Duration> {
    (query_secs > 0).then(|| Duration::from_secs(query_secs) + REPLY_MARGIN)
}

/// Proxy manager
pub struct Proxy {
    // In a real implementation, we might keep connection pools
    connect_timeout: Duration,
    reply_timeout: Option<Duration>,
}

impl Default for Proxy {
    fn default() -> Self {
        Self::new()
    }
}

impl Proxy {
    /// A proxy with the server's limits: [`DEFAULT_CONNECT_TIMEOUT`] to
    /// connect, and `SAMYAMA_QUERY_TIMEOUT` (whole seconds, default 120, the
    /// same variable and default the query engine reads) plus a margin for the
    /// reply. Peers in a cluster are assumed to run with the same setting.
    pub fn new() -> Self {
        let query_secs = std::env::var("SAMYAMA_QUERY_TIMEOUT")
            .ok().and_then(|s| s.parse().ok()).unwrap_or(120);
        Self::with_timeouts(DEFAULT_CONNECT_TIMEOUT, reply_timeout_for(query_secs))
    }

    /// A proxy with explicit limits. `reply_timeout` covers writing the
    /// command and reading the whole reply; `None` waits as long as it takes.
    pub fn with_timeouts(connect_timeout: Duration, reply_timeout: Option<Duration>) -> Self {
        Self { connect_timeout, reply_timeout }
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn reply_timeout(&self) -> Option<Duration> {
        self.reply_timeout
    }

    /// Forward a raw RESP command to a target address
    ///
    /// Note: This is a simplified implementation. Production would use
    /// connection pooling.
    ///
    /// Bounded in time (#2002). A peer that is down but whose address
    /// swallows the connection attempt, or one that accepts and then never
    /// answers, used to hold the client's connection open forever: nothing
    /// here had a deadline, and the client had no way to tell a slow peer
    /// from a dead one. Now both are errors, `routing failed` to the client,
    /// after `connect_timeout` and `reply_timeout` respectively.
    pub async fn forward(&self, target_addr: &str, command: &[u8]) -> Result<Vec<u8>, String> {
        debug!("Forwarding request to {}", target_addr);

        let mut stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(target_addr))
            .await
            .map_err(|_| format!("Failed to connect to {}: no connection after {:?}",
                                 target_addr, self.connect_timeout))?
            .map_err(|e| format!("Failed to connect to {}: {}", target_addr, e))?;

        let exchange = Self::exchange(&mut stream, target_addr, command);
        match self.reply_timeout {
            Some(limit) => tokio::time::timeout(limit, exchange).await
                .map_err(|_| format!("Failed to read from {}: no complete reply after {:?}",
                                     target_addr, limit))?,
            None => exchange.await,
        }
    }

    /// Write the command and read back exactly one RESP frame.
    async fn exchange(stream: &mut TcpStream, target_addr: &str, command: &[u8]) -> Result<Vec<u8>, String> {
        // Write command
        stream.write_all(command).await
            .map_err(|e| format!("Failed to write to {}: {}", target_addr, e))?;

        // Read until the reply is a **complete RESP frame**, not until the first
        // `read` returns.
        //
        // A single `read` returns whatever one TCP segment happened to carry, so
        // the previous version truncated every forwarded reply over 4096 bytes
        // -- and, because a segment boundary falls wherever the network puts it,
        // could truncate a short one too. The caller received a partial frame
        // with no error, which is the worst shape for this bug: the bytes parse
        // as far as they go.
        //
        // Still one connection per command. Pooling is the remaining part of
        // samyama-graph#2002 and is deliberately not done here -- it is a design
        // question (lifetime, limits, reuse after error) rather than a
        // correctness fix. The timeout is `forward`'s, around this whole call.
        let mut reply = BytesMut::new();
        let mut chunk = vec![0u8; 4096];
        loop {
            if let Some(len) = resp_frame_len(&reply)
                .map_err(|e| format!("Failed to read from {}: {}", target_addr, e))?
            {
                // Exactly the frame. Anything after it belongs to a later reply
                // and this connection is about to be dropped, so it is not ours
                // to return.
                return Ok(reply[..len].to_vec());
            }
            let n = stream.read(&mut chunk).await
                .map_err(|e| format!("Failed to read from {}: {}", target_addr, e))?;
            if n == 0 {
                if reply.is_empty() {
                    // The peer accepted the command and closed without
                    // answering. Reported as an empty reply, as before.
                    return Ok(Vec::new());
                }
                return Err(format!(
                    "Failed to read from {}: peer closed after {} byte(s) with an \
                     incomplete RESP frame", target_addr, reply.len()));
            }
            reply.extend_from_slice(&chunk[..n]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_new() {
        let _proxy = Proxy::new();
    }

    #[tokio::test]
    async fn test_proxy_forward_connection_refused() {
        let proxy = Proxy::new();
        // Connecting to an invalid address should fail
        let result = proxy.forward("127.0.0.1:19999", b"PING\r\n").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to connect"));
    }

    #[tokio::test]
    async fn forward_writes_the_command_and_returns_the_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let n = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"+PONG\r\n").await.unwrap();
            buf[..n].to_vec()
        });
        let proxy = Proxy::new();
        let reply = proxy.forward(&addr, b"PING\r\n").await.unwrap();
        assert_eq!(reply, b"+PONG\r\n");
        assert_eq!(server.await.unwrap(), b"PING\r\n");
    }

    /// The defect in samyama-graph#2002: a reply larger than one `read` was
    /// truncated and returned without an error.
    ///
    /// 4096 is the old buffer size, so a bulk string a little over it needs at
    /// least two reads. The server also sleeps mid-reply, which forces the
    /// split to land in the middle of the payload rather than leaving it to the
    /// kernel's discretion -- without that this test passes on a loopback that
    /// happens to deliver everything at once, and proves nothing.
    #[tokio::test]
    async fn a_reply_larger_than_one_read_arrives_whole() {
        let payload = "x".repeat(10_000);
        let frame = format!("${}\r\n{}\r\n", payload.len(), payload);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let expected = frame.clone();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            let bytes = expected.as_bytes();
            sock.write_all(&bytes[..1000]).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            sock.write_all(&bytes[1000..]).await.unwrap();
        });
        let reply = Proxy::new().forward(&addr, b"GET big\r\n").await.unwrap();
        server.await.unwrap();
        assert_eq!(reply.len(), frame.len(),
                   "truncated: got {} of {} bytes", reply.len(), frame.len());
        assert_eq!(reply, frame.as_bytes());
    }

    /// And the frame boundary is respected rather than "everything available":
    /// a peer that pipelines a second reply must not have it folded into the
    /// first. Reading to EOF would pass the test above and fail this one.
    #[tokio::test]
    async fn a_second_pipelined_reply_is_not_folded_into_the_first() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"+FIRST\r\n+SECOND\r\n").await.unwrap();
        });
        let reply = Proxy::new().forward(&addr, b"PING\r\n").await.unwrap();
        server.await.unwrap();
        assert_eq!(reply, b"+FIRST\r\n");
    }

    /// A peer that dies mid-frame used to yield a silent partial. It is an
    /// error now -- distinct from the empty reply below, which is a peer that
    /// closed without starting one.
    #[tokio::test]
    async fn a_peer_that_dies_mid_frame_is_an_error_not_a_partial() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"$100\r\nonly-a-few-bytes").await.unwrap();
            // dropped here, mid-frame
        });
        let err = Proxy::new().forward(&addr, b"GET k\r\n").await.unwrap_err();
        server.await.unwrap();
        assert!(err.contains("incomplete RESP frame"), "{}", err);
    }

    #[tokio::test]
    async fn forward_to_a_peer_that_closes_returns_an_empty_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            // closed without replying
        });
        let reply = Proxy::new().forward(&addr, b"PING\r\n").await.unwrap();
        assert!(reply.is_empty());
        server.await.unwrap();
    }

    // --- #2002: a peer that does not answer is an error, not a hang ---------
    //
    // Each of these runs `forward` under an outer deadline well above the
    // proxy's own, so a proxy that has no deadline fails the test by name
    // rather than hanging it.

    /// The proxy's own limits in these tests, short so the suite stays fast.
    const CONNECT: Duration = Duration::from_millis(300);
    const REPLY: Duration = Duration::from_millis(300);
    /// Far above both: reaching it means the proxy never gave up.
    const OUTER: Duration = Duration::from_secs(10);

    async fn forward_bounded(proxy: &Proxy, addr: &str, cmd: &[u8]) -> (Result<Vec<u8>, String>, Duration) {
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(OUTER, proxy.forward(addr, cmd))
            .await
            .unwrap_or_else(|_| panic!("forward to {addr} was still waiting after {OUTER:?}: no deadline"));
        (result, started.elapsed())
    }

    /// A peer that accepts the command and then says nothing, holding the
    /// connection open. This is a hung or overloaded node.
    #[tokio::test]
    async fn a_peer_that_never_answers_is_an_error_after_the_reply_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            let _ = done_rx.await; // never answers, never closes
        });
        let proxy = Proxy::with_timeouts(CONNECT, Some(REPLY));
        let (result, took) = forward_bounded(&proxy, &addr, b"PING\r\n").await;
        let _ = done_tx.send(());
        server.await.unwrap();
        let err = result.unwrap_err();
        assert!(err.contains("no complete reply after"), "{err}");
        assert!(took >= REPLY, "gave up after {took:?}, before the {REPLY:?} limit");
    }

    /// The same, but the peer stops half-way through a frame. The read loop
    /// waits for the rest of the frame, so this is the case a deadline on the
    /// first byte alone would miss.
    #[tokio::test]
    async fn a_peer_that_stalls_mid_reply_is_an_error_after_the_reply_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"$100\r\nonly-a-few-bytes").await.unwrap();
            let _ = done_rx.await; // the other 84 bytes never come
        });
        let proxy = Proxy::with_timeouts(CONNECT, Some(REPLY));
        let (result, _) = forward_bounded(&proxy, &addr, b"GET k\r\n").await;
        let _ = done_tx.send(());
        server.await.unwrap();
        let err = result.unwrap_err();
        assert!(err.contains("no complete reply after"), "{err}");
    }

    /// A peer whose address takes the connection attempt and never completes
    /// it. Made here with a listener that never accepts: once its backlog is
    /// full the kernel drops further attempts, and `connect` waits.
    #[tokio::test]
    async fn a_peer_that_never_accepts_is_an_error_after_the_connect_timeout() {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1).unwrap();
        let addr = listener.local_addr().unwrap();
        // Fill the backlog: connect until one attempt hangs.
        let mut held = Vec::new();
        let mut full = false;
        for _ in 0..64 {
            match tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(addr)).await {
                Ok(Ok(s)) => held.push(s),
                Ok(Err(e)) => panic!("could not fill the backlog: {e}"),
                Err(_) => { full = true; break; }
            }
        }
        assert!(full, "the backlog never filled after {} connections; this test cannot run here", held.len());

        let proxy = Proxy::with_timeouts(CONNECT, Some(REPLY));
        let (result, took) = forward_bounded(&proxy, &addr.to_string(), b"PING\r\n").await;
        let err = result.unwrap_err();
        assert!(err.contains("Failed to connect") && err.contains("no connection after"), "{err}");
        assert!(took >= CONNECT, "gave up after {took:?}, before the {CONNECT:?} limit");
        drop(listener);
    }

    /// The deadline is a limit, not a delay: a peer that is slow but inside it
    /// is answered normally, and a reply split across a pause still arrives.
    #[tokio::test]
    async fn a_slow_reply_inside_the_limit_still_arrives() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = sock.read(&mut buf).await.unwrap();
            sock.write_all(b"+PO").await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            sock.write_all(b"NG\r\n").await.unwrap();
        });
        let proxy = Proxy::with_timeouts(CONNECT, Some(Duration::from_secs(5)));
        let (result, _) = forward_bounded(&proxy, &addr, b"PING\r\n").await;
        server.await.unwrap();
        assert_eq!(result.unwrap(), b"+PONG\r\n");
    }

    /// The default reply limit sits above the query deadline, so a forwarded
    /// query that times out on the peer is reported with the peer's own
    /// `Query timed out` rather than overtaken by `routing failed`. And no
    /// query deadline means no reply limit.
    #[test]
    fn the_default_reply_limit_outlasts_the_query_deadline() {
        assert_eq!(reply_timeout_for(120), Some(Duration::from_secs(130)));
        assert_eq!(reply_timeout_for(1), Some(Duration::from_secs(11)));
        assert_eq!(reply_timeout_for(0), None);
        assert_eq!(Proxy::new().connect_timeout(), DEFAULT_CONNECT_TIMEOUT);
    }
}
