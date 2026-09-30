//! Proxy client for forwarding requests
//!
//! Handles the networking part of routing: taking a command and sending it
//! to a remote node's RESP port.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

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

        // Read response
        // In a real RESP proxy, we would parse the response to know when it ends.
        // For MVP, we read until EOF or a reasonable buffer.
        // Since standard Redis clients keep connection open, reading "until EOF" 
        // doesn't work for single command forwarding unless we use a framed codec.
        //
        // NOTE: This simple proxy assumes one-off connections for MVP.
        // A full proxy requires a persistent connection and RESP frame parsing.
        
        let mut buf = vec![0; 4096];
        let n = stream.read(&mut buf).await
            .map_err(|e| format!("Failed to read from {}: {}", target_addr, e))?;

        Ok(buf[..n].to_vec())
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
