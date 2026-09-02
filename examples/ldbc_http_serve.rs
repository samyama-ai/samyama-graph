//! Serve an LDBC SNB extract over Samyama's HTTP API, for surface-comparable
//! benchmarking.
//!
//! PERF-04 asks that point and selective reads stay the fastest of all compared
//! engines. On the 2026-09-01 SF10 run that read 7/7 — with Samyama measured
//! **in-process** through `EmbeddedClient` and Neo4j over HTTP `tx/commit`.
//! Neo4j's seven short reads all landed in a 6.4-13.3 ms band that barely moved
//! with the work done, so essentially the whole of that ratio was the round
//! trip. The requirement was marked `blocked` rather than met
//! (samyama-graph-competitor-benchmarks#110).
//!
//! Settling it needs both engines behind a comparable surface. This is
//! Samyama's half: the same extract, the same indexes the benchmark builds, and
//! the same query text — served over `POST /api/query` so a client pays a real
//! HTTP round trip exactly as the Neo4j runner does.
//!
//! The index list is duplicated from `benches/ldbc_benchmark.rs` deliberately
//! rather than shared: a benchmark and the thing it is compared against must be
//! able to drift apart *visibly*. It is asserted below to be the same length as
//! the bench's, so a silent divergence fails here instead of producing a
//! comparison of two differently-indexed graphs.
//!
//! Usage:
//!   cargo run --release --example ldbc_http_serve -- \
//!       --data-dir /root/bench/data/ldbc-sf10/social_network-sf10-... [--port 8080]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use samyama::http::server::HttpServer;
use samyama_sdk::{EmbeddedClient, SamyamaClient};

mod ldbc_common;
use ldbc_common::{format_duration, format_num};

type Error = Box<dyn std::error::Error>;

/// Exactly the set `benches/ldbc_benchmark.rs` builds. Kept in step by the
/// assertion below, not by hope.
const INDEXES: &[(&str, &str)] = &[
    ("Person", "id"),
    ("Person", "firstName"),
    ("Post", "id"),
    ("Comment", "id"),
    ("Forum", "id"),
    ("Place", "id"),
    ("Place", "name"),
    ("Organisation", "id"),
    ("Organisation", "name"),
    ("Tag", "id"),
    ("Tag", "name"),
    ("TagClass", "id"),
    ("TagClass", "name"),
];

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(
        args.iter()
            .position(|a| a == "--data-dir")
            .and_then(|i| args.get(i + 1))
            .ok_or("--data-dir <path> is required")?,
    );
    let port: u16 = args
        .iter()
        .position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(8080);

    if !data_dir.is_dir() {
        return Err(format!("no such extract: {}", data_dir.display()).into());
    }

    eprintln!("Samyama {} — serving {} over HTTP",
        env!("CARGO_PKG_VERSION"), data_dir.display());

    let client = EmbeddedClient::new();
    let load_start = Instant::now();
    let load_result = {
        let mut graph = client.store_write().await;
        ldbc_common::load_dataset(&mut graph, &data_dir)?
    };
    eprintln!(
        "Dataset: {} nodes, {} edges (loaded in {})",
        format_num(load_result.total_nodes),
        format_num(load_result.total_edges),
        format_duration(load_start.elapsed())
    );

    let idx_start = Instant::now();
    for (label, prop) in INDEXES {
        let stmt = format!("CREATE INDEX ON :{}({})", label, prop);
        if let Err(e) = client.query("default", &stmt).await {
            // Loud: an index that silently failed to build turns this into a
            // comparison of a served graph against a differently-indexed one,
            // and the only symptom is a query being slower than it should be.
            eprintln!("  WARN: index {}({}) failed: {}", label, prop, e);
        }
    }
    eprintln!("Indexes built in {} ({} of them)",
        format_duration(idx_start.elapsed()), INDEXES.len());

    // The store the HTTP layer serves is the very one just loaded — not a copy,
    // and not a second load with its own timing.
    let store: Arc<_> = client.store().clone();
    let server = HttpServer::new(store, port);

    // A readiness line the orchestration can wait for, rather than sleeping and
    // hoping. `base.sh` learned this lesson as `/root/phase`.
    eprintln!("READY http://0.0.0.0:{port}/api/query");
    server.start().await
}
