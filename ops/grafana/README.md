# Grafana dashboard

`samyama-overview.json` — import it into Grafana and pick a Prometheus
datasource. Point Prometheus at the engine's `/metrics`:

```yaml
scrape_configs:
  - job_name: samyama
    static_configs:
      - targets: ['localhost:8080']
```

## Diagnosing a slow query with it

1. **Query latency percentiles** — p95 rising is the signal. A mean would not
   show it: one query in twenty over budget moves a mean by nothing.
2. **Latency band occupancy** — shows the shift between bands before any
   percentile crosses a threshold, so you see it earlier and with more of the
   story.
3. **Slow queries** — the rate of queries past `SLOW_QUERY_MS`. This panel is
   the pointer, not the answer.
4. **The slow-query log** carries the query *text*, which is the part you can
   act on. Set `SLOW_QUERY_MS` to a number below your p95 and read the log.
   Each line also carries `elapsed_ms`, `rows`, `failed`, `is_write` and a
   `query_hash` (16 hex digits of the SHA-256 of the whitespace-normalised
   text), so repeats of one statement group without reading the text.

For a log collector (Loki, Vector, Fluent Bit), start the server with
`--log-format json` or `SAMYAMA_LOG_FORMAT=json`. Every log event is then one
JSON object per line on **stderr**, with `timestamp`, `level`, `target`,
`fields` and the enclosing spans; stdout keeps the start-up banner, so the log
stream holds nothing but JSON. Text on stdout remains the default, and any
other value is refused at start-up rather than read as text.

Every HTTP request and every RESP command runs inside a `request` span with a
`request_id`, `protocol` (`http` or `resp`) and, once known, `tenant`; HTTP
spans add `method` and `path`, RESP spans the `command`. Each event the request
causes, the slow-query line included, carries that span, so filtering on one
`request_id` gives everything one request did. HTTP returns the id in the
`X-Request-Id` response header, and keeps the caller's own `X-Request-Id` when
it is 1 to 128 characters of letters, digits and `-` `_` `.` `:`; anything else
is replaced with a fresh id rather than written into the log.

The panel and the log use the same threshold on purpose. A dashboard and a log
that disagree about what "slow" means give an operator two numbers and a reason
to trust neither.

## What is not here

- **Per-query-shape latency.** The histogram is process-wide, so it says the
  system got slower and not which query did. The log closes that gap; a
  per-shape metric would need a cardinality decision we have not made.
- **Rows scanned, plan choice.** Use `EXPLAIN` on the query the log named.
- **Traces on this dashboard.** Traces go to a trace store, not Prometheus;
  see below.
- **Per-operator spans and AI-call spans.** Not shipped yet: a span per
  operator per query needs a sampling decision first, or it slows the queries
  it is measuring.

## Traces

Set `OTEL_EXPORTER_OTLP_ENDPOINT` (a collector's base URL, e.g.
`http://localhost:4318`) or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (the full
traces URL) and the server exports traces over OTLP/HTTP to Jaeger, Tempo or an
OpenTelemetry Collector. The service is `samyama` unless `OTEL_SERVICE_NAME`
says otherwise. Unset, nothing is exported and nothing is built: the stage
spans below are `debug` level and the log subscriber stops at `info`, so they
are never created.

Each HTTP request and RESP command is one trace, rooted at the `request` span
(same `request_id` as the logs), with a child span per stage:

| Span | What its duration is |
|---|---|
| `samyama.lock.acquire` (`mode` = `read`/`write`) | waiting for the store's lock — contention |
| `samyama.query.parse` (`cache_hit`) | parsing, or the parse-cache lookup |
| `samyama.query.execute` | running the statement; `samyama.query.plan` inside it is planning |
| `samyama.storage.persist` (`mutations`) | writing a statement's changes to disk |

An HTTP request carrying a W3C `traceparent` header joins the caller's trace,
so a trace that starts in a client service continues through the engine. A
missing or malformed header starts a new trace; the request is not refused.
Spans are sent in batches every few seconds and flushed when the server
returns; a process that is killed loses the last batch.

## Keeping it honest

`tests/dashboard_panels_have_metrics.rs` checks that every metric named in
every PromQL expression here is one the engine exports, in both directions. A
panel for a metric that does not exist renders as *no data*, which an operator
reads as "no traffic" rather than "no metric" — so a stale dashboard is worse
than no dashboard, and this is the check that stops one shipping.
