//! Shared utilities for the LDBC SNB Business Intelligence benchmark.
//!
//! Re-exports the data-loading infrastructure from `ldbc_common` and provides
//! BI-specific helpers (date epoch constants, result formatting, etc.).

#![allow(dead_code)]

// Re-export the full dataset loader and formatting helpers from ldbc_common.
// The BI benchmark loads the same SNB SF1 graph as the interactive benchmark.
#[path = "../ldbc_common/mod.rs"]
pub mod ldbc_common;

pub use ldbc_common::{format_duration, format_num, load_dataset};

// ============================================================================
// LDBC BI date constants (milliseconds since epoch, LongDateFormatter)
// ============================================================================

/// 2011-07-22 — midpoint of SF1 creation window, used to split "before/after"
pub const MIDPOINT_DATE: i64 = 1_311_292_800_000;

/// 2011-01-01 — start of SF1 activity
pub const START_DATE: i64 = 1_293_840_000_000;

/// 2012-12-01 — late in the dataset
pub const END_DATE: i64 = 1_354_320_000_000;

/// 2012-07-01 — window boundary
pub const WINDOW_END: i64 = 1_341_100_800_000;

/// 2012-01-01
pub const YEAR_2012: i64 = 1_325_376_000_000;

/// 2011-06-01
pub const MID_2011: i64 = 1_306_886_400_000;
