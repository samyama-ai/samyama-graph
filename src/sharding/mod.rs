//! Sharding and Distributed Routing module
//!
//! Implements Tenant-Level Sharding (Phase 10).

pub mod proxy;
pub mod router;

pub use proxy::Proxy;
pub use router::{RouteResult, Router};
