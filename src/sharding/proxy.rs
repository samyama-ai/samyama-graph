//! Proxy client for forwarding requests
//!
//! Handles the networking part of routing: taking a command and sending it
//! to a remote node's RESP port.

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

/// Proxy manager
pub struct Proxy {
    // In a real implementation, we might keep connection pools
}

impl Proxy {
    pub fn new() -> Self {
        Self {}
    }

    /// Forward a raw RESP command to a target address
    /// 
    /// Note: This is a simplified implementation. Production would use
    /// connection pooling and better error handling.
    pub async fn forward(&self, target_addr: &str, command: &[u8]) -> Result<Vec<u8>, String> {
        debug!("Forwarding request to {}", target_addr);

        let mut stream = TcpStream::connect(target_addr).await
            .map_err(|e| format!("Failed to connect to {}: {}", target_addr, e))?;

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
        // Still one connection per command. Pooling and a timeout are the other
        // halves of samyama-graph#2002 and are deliberately not done here --
        // pooling is a design question (lifetime, limits, reuse after error)
        // rather than a correctness fix, and bundling it would make this
        // unreviewable.
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
}
