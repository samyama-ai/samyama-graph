//! Network protocol module
//!
//! Implements RESP (Redis Serialization Protocol) support:
//! - REQ-REDIS-001: RESP protocol implementation
//! - REQ-REDIS-002: Redis client connections
//! - REQ-REDIS-003: Authentication (future)
//! - REQ-REDIS-004: Redis-compatible graph commands
//! - REQ-REDIS-006: Redis client library compatibility
//! - REQ-REDIS-007: Pipelining support
//!
//! Architecture follows ADR-003 (RESP Protocol)

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
