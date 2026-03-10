//! # Network Protocol (RESP)
//!
//! ## What is RESP?
//!
//! RESP (Redis Serialization Protocol) is a simple text/binary protocol for client-server
//! communication, originally designed for Redis. Samyama implements RESP so that **existing
//! Redis clients** — redis-cli, Python's `redis` library, Node.js `ioredis`, Go's
//! `go-redis` — can connect to Samyama without any modification. This provides instant
//! ecosystem compatibility with dozens of client libraries across every major language.
//!
//! ## Wire format
//!
//! RESP messages are prefixed with a type byte and terminated by `\r\n` (CRLF):
//! - `+OK\r\n` — Simple string (status replies)
//! - `-ERR message\r\n` — Error (with human-readable message)
//! - `:42\r\n` — Integer
//! - `$6\r\nfoobar\r\n` — Bulk string (length-prefixed, binary-safe)
//! - `*2\r\n...\r\n...\r\n` — Array (element count prefix, then elements)
//! - `_\r\n` — Null (RESP3)
//!
//! The length-prefixed bulk strings are important: they allow binary data (including
//! `\r\n` within the payload) to be transmitted without escaping.
//!
//! ## Graph commands
//!
//! Samyama extends the RESP command set with graph-specific commands:
//! - `GRAPH.QUERY <graph> <cypher>` — execute a read-write Cypher query
//! - `GRAPH.RO_QUERY <graph> <cypher>` — execute a read-only Cypher query (can be
//!   routed to replicas in a cluster)
//! - `GRAPH.DELETE <graph>` — delete an entire graph
//!
//! ## Why the Redis protocol?
//!
//! This design was inspired by FalkorDB (formerly RedisGraph). By speaking Redis's
//! protocol, Samyama avoids the "cold start" problem of needing custom client libraries.
//! Users can connect with tools they already have installed (`redis-cli`) and integrate
//! with applications using battle-tested Redis client libraries.

pub mod resp;
pub mod server;
pub mod command;

// Re-export main types
pub use resp::{RespValue, RespError, RespResult};
pub use server::{RespServer, ServerConfig};
pub use command::CommandHandler;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Ensure types are accessible
        let _ = ServerConfig::default();
    }
}
