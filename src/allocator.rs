//! The memory allocator the engine ships with (ADR-038, #1267).
//!
//! This module **names** the allocator; it does not install it. A library must not
//! set `#[global_allocator]` for the process that loads it, so the server binary
//! and the benchmark binaries install it themselves:
//!
//! ```ignore
//! #[global_allocator]
//! static GLOBAL: samyama::allocator::Shipped = samyama::allocator::SHIPPED;
//! ```
//!
//! Why mimalloc: on glibc, dropping the relationship row copy (MVCC step 5b)
//! left ~800 K free chunks after an SF1 load and made BI-9/BI-12/BI-14 1.5-1.7x
//! slower. Under mimalloc those queries run at 0.36-0.47x of glibc, write churn
//! does not degrade reads, and SF10 RSS is 199.3 B/edge against glibc's 207.1.
//! The measurements are in `docs/ADR/ADR-038-memory-allocator.md`.
//!
//! Embedded users (the Python wheel, `samyama-sdk`) depend on this crate without
//! default features and keep their host's allocator. Build the server with
//! `--no-default-features` to use the system allocator.

#[cfg(feature = "mimalloc")]
pub type Shipped = mimalloc::MiMalloc;
#[cfg(feature = "mimalloc")]
pub const SHIPPED: Shipped = mimalloc::MiMalloc;

#[cfg(not(feature = "mimalloc"))]
pub type Shipped = std::alloc::System;
#[cfg(not(feature = "mimalloc"))]
pub const SHIPPED: Shipped = std::alloc::System;

/// The shipped allocator's name, for benchmark output and envelopes, so a number
/// measured under one allocator is not compared with one measured under another.
pub const NAME: &str = if cfg!(feature = "mimalloc") { "mimalloc" } else { "system" };
