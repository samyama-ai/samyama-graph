//! OpenTelemetry trace export (#2077, REL-10).
//!
//! Metrics say a query got slower; `PROFILE` says where a query spends its
//! time when someone runs it again by hand. Neither explains a slow request on
//! live traffic: was it waiting for the store's lock, parsing, planning,
//! running, or writing to disk? The `request` span (#2076) and the stage spans
//! inside it answer that, and this module sends them to a collector (Jaeger,
//! Tempo, the OpenTelemetry Collector) over OTLP/HTTP.
//!
//! **Off unless asked for.** Export starts only when
//! `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is
//! set. Unset, nothing here runs: no exporter, no thread, no connection, and
//! the stage spans are `debug` level, so the log subscriber's `info` ceiling
//! disables them at the call site. Observability must not slow the engine.
//!
//! **The caller's trace continues here.** A W3C `traceparent` header on an
//! HTTP request makes the request span a child of the caller's span, so a
//! trace that starts in a client service does not stop at our boundary.

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::OnceLock;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::Layer;

/// The traces URL export is asked to send to, if it is asked for at all.
///
/// The OpenTelemetry convention: `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is the
/// full traces URL and wins; `OTEL_EXPORTER_OTLP_ENDPOINT` is the collector's
/// base URL, to which `/v1/traces` is appended. A blank variable is unset.
/// `get` reads a variable, so this is testable without process-wide state.
pub fn endpoint_from(get: impl Fn(&str) -> Option<String>) -> Option<String> {
    let set = |name: &str| get(name).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    set("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .or_else(|| set("OTEL_EXPORTER_OTLP_ENDPOINT").map(|b| format!("{}/v1/traces", b.trim_end_matches('/'))))
}

/// A provider exporting to `endpoint` over OTLP/HTTP in batches.
///
/// `endpoint` is the full traces URL (`http://collector:4318/v1/traces`).
/// Built on a thread of its own: the exporter's blocking HTTP client owns a
/// runtime, and creating or dropping one inside the server's tokio runtime
/// panics.
pub fn otlp_provider(endpoint: &str) -> Result<SdkTracerProvider, String> {
    let endpoint = endpoint.to_string();
    std::thread::spawn(move || {
        use opentelemetry_otlp::WithExportConfig;
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .build()
            .map_err(|e| e.to_string())?;
        let mut resource = opentelemetry_sdk::Resource::builder();
        // The variable wins when set; otherwise name the service rather than
        // leave every span under `unknown_service`.
        if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
            resource = resource.with_service_name("samyama");
        }
        Ok(SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource.build())
            .build())
    })
    .join()
    .map_err(|_| "building the OTLP exporter panicked".to_string())?
}

/// The `tracing` layer that turns spans into OpenTelemetry spans.
///
/// Our own spans down to `debug`, so the stage spans are exported; every
/// other crate at `info`, so a dependency's debug chatter is not.
pub fn layer<S>(provider: &SdkTracerProvider) -> impl Layer<S>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("samyama"))
        .with_filter(
            Targets::new()
                .with_target("samyama", LevelFilter::DEBUG)
                .with_default(LevelFilter::INFO),
        )
}

static PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// The provider for the configured endpoint, kept for `shutdown`, or `None`
/// when export is not asked for. A provider that cannot be built is reported
/// and the server runs without export: tracing is a diagnostic, not a reason
/// to refuse to serve.
pub fn from_env() -> Option<SdkTracerProvider> {
    let endpoint = endpoint_from(|v| std::env::var(v).ok())?;
    match otlp_provider(&endpoint) {
        Ok(p) => {
            let _ = PROVIDER.set(p.clone());
            eprintln!("OpenTelemetry traces export to {endpoint}");
            Some(p)
        }
        Err(e) => {
            eprintln!("OpenTelemetry export not started ({endpoint}): {e}");
            None
        }
    }
}

/// Send the spans still in the batch. Call once, on the way out; a no-op when
/// export was never started. On its own thread for the reason `otlp_provider`
/// gives.
pub fn shutdown() {
    if let Some(p) = PROVIDER.get().cloned() {
        let _ = std::thread::spawn(move || p.shutdown()).join();
    }
}

/// Make `span` a child of the W3C `traceparent` in `headers`, when there is a
/// valid one. A missing or malformed header leaves the span the root of a new
/// trace, as the W3C spec asks: a bad header is no reason to fail a request.
pub fn continue_trace(span: &tracing::Span, headers: &axum::http::HeaderMap) {
    if headers.get("traceparent").is_none() {
        return;
    }
    let cx = TraceContextPropagator::new().extract(&Headers(headers));
    let _ = span.set_parent(cx);
}

struct Headers<'a>(&'a axum::http::HeaderMap);

impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_is_off_unless_an_endpoint_is_set() {
        assert_eq!(endpoint_from(|_| None), None);
        assert_eq!(endpoint_from(|_| Some("  ".into())), None, "blank is unset");
        let only = |name: &'static str, val: &'static str| {
            move |v: &str| (v == name).then(|| val.to_string())
        };
        assert_eq!(
            endpoint_from(only("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318/")).as_deref(),
            Some("http://c:4318/v1/traces"),
            "the base URL gets the traces path"
        );
        // The traces-specific variable is preferred over the general one.
        let both = |v: &str| match v {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => Some("http://t:4318/v1/traces".into()),
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some("http://g:4318".into()),
            _ => None,
        };
        assert_eq!(endpoint_from(both).as_deref(), Some("http://t:4318/v1/traces"));
    }
}
