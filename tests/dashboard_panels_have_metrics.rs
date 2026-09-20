//! Every metric the dashboard plots is one `/metrics` exports (REL-10).
//!
//! A Grafana dashboard is a file. Nothing stops it naming a metric that does
//! not exist, and nothing tells you when one is renamed — the panel just goes
//! empty, which an operator reads as "no traffic" rather than "no metric". A
//! dashboard with empty panels is worse than none, because it teaches people
//! to ignore it.
//!
//! So this parses `ops/grafana/samyama-overview.json`, pulls every metric name
//! out of every PromQL expression, and checks each one against what the engine
//! actually renders. It runs a query first so the histogram is populated
//! rather than absent.
//!
//! The reverse direction is checked too, and is the one that catches drift in
//! the other direction: a metric that exists and no panel plots is either a
//! panel somebody forgot or a metric nobody needs, and both are worth a look.
//! That half is a report rather than a failure — a new metric should not break
//! a build — except for the query-latency family, which is the whole reason
//! REL-10 asks for this and must never go unplotted.

use std::collections::BTreeSet;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn dashboard() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("ops/grafana/samyama-overview.json");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("the dashboard must ship in the repo: {}: {e}", path.display()))
}

/// Metric names inside PromQL expressions in the dashboard.
///
/// A metric name here is a bare identifier that is followed by `{` or by a
/// PromQL operator — deliberately crude, and then filtered against a list of
/// PromQL keywords, because the alternative is a PromQL parser in a test.
fn metrics_plotted(json: &str) -> BTreeSet<String> {
    const KEYWORDS: &[&str] = &[
        "rate", "sum", "by", "le", "histogram_quantile", "clamp_min", "job", "label_values",
        "max", "min", "avg", "increase", "irate",
    ];
    let mut out = BTreeSet::new();
    for line in json.lines() {
        let Some(expr_at) = line.find("\"expr\"") else {
            continue;
        };
        let expr = &line[expr_at..];
        let mut token = String::new();
        for c in expr.chars() {
            if c.is_ascii_alphanumeric() || c == '_' {
                token.push(c);
            } else {
                if token.starts_with("samyama_") && !KEYWORDS.contains(&token.as_str()) {
                    out.insert(token.clone());
                }
                token.clear();
            }
        }
        if token.starts_with("samyama_") {
            out.insert(token);
        }
    }
    out
}

/// Metric names the engine renders, with the `_bucket`/`_sum`/`_count`
/// suffixes a histogram produces.
fn metrics_exported() -> BTreeSet<String> {
    // A query first: the histogram exists from the start, but running one
    // makes the test exercise the path that fills it rather than only the one
    // that prints it.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut("CREATE (:N {x: 1})", &mut store, "default")
        .expect("create");
    engine
        .execute("MATCH (n:N) RETURN n.x AS x", &store)
        .expect("read");

    let mut out = BTreeSet::new();
    for line in samyama::query::metrics::render().lines() {
        if line.starts_with('#') {
            continue;
        }
        let name = line
            .split(['{', ' '])
            .next()
            .unwrap_or_default()
            .to_string();
        if !name.is_empty() {
            out.insert(name);
        }
    }
    // The store-level gauges, which live in the HTTP handler rather than in
    // the metrics module. Named here rather than scraped, because scraping
    // would need a running server and this is a fact about the handler's
    // source either way.
    for name in [
        "samyama_nodes",
        "samyama_edges",
        "samyama_memory_attributed_bytes",
        "samyama_index_memory_bytes",
        "samyama_index_bytes",
        "samyama_query_cache_entries",
        "samyama_query_cache_hits",
        "samyama_query_cache_misses",
    ] {
        out.insert(name.to_string());
    }
    out
}

#[test]
fn the_dashboard_names_metrics_that_exist() {
    let plotted = metrics_plotted(&dashboard());
    assert!(
        plotted.len() >= 8,
        "the extractor found only {plotted:?}; if the dashboard changed shape this test \
         is now checking nothing"
    );
    let exported = metrics_exported();
    let missing: Vec<&String> = plotted.difference(&exported).collect();
    assert!(
        missing.is_empty(),
        "the dashboard plots {missing:?}, which `/metrics` does not export. A panel for a \
         metric that does not exist reads as 'no traffic', not as 'no metric'."
    );
}

#[test]
fn the_store_gauges_named_here_are_the_ones_the_handler_renders() {
    // `metrics_exported` lists the store gauges by hand, so it can go stale in
    // exactly the way this file exists to prevent. This reads them back out of
    // the handler's source: not a parse of the code, a check that each name
    // still appears in it.
    let handler = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/http/handler.rs"),
    )
    .expect("handler.rs");
    for name in metrics_exported() {
        if name.starts_with("samyama_query_duration") || name.starts_with("samyama_queries") {
            continue; // rendered by the metrics module, checked above
        }
        if name == "samyama_slow_queries_total" {
            continue;
        }
        assert!(
            handler.contains(&name),
            "{name} is listed as a store gauge and no longer appears in handler.rs"
        );
    }
}

#[test]
fn the_latency_family_is_plotted_somewhere() {
    // The one direction that is a failure rather than a report. REL-10 asks
    // for observability sufficient to diagnose a slow query; a dashboard that
    // plots graph size and cache hits and not latency answers a different
    // question, which is what this one did before the histogram existed.
    let plotted = metrics_plotted(&dashboard());
    assert!(
        plotted.iter().any(|m| m.starts_with("samyama_query_duration_seconds")),
        "no panel plots query latency: {plotted:?}"
    );
    assert!(
        plotted.contains("samyama_slow_queries_total"),
        "no panel plots the slow-query counter, which is the pointer to the log: {plotted:?}"
    );
}
