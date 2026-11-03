//! Sharding and Distributed Routing module
//!
//! Implements Tenant-Level Sharding (Phase 10).

pub mod router;
pub mod proxy;

pub use router::{Router, RouteResult};
pub use proxy::Proxy;