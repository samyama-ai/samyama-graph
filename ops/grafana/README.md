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

The panel and the log use the same threshold on purpose. A dashboard and a log
that disagree about what "slow" means give an operator two numbers and a reason
to trust neither.

## What is not here

- **Per-query-shape latency.** The histogram is process-wide, so it says the
  system got slower and not which query did. The log closes that gap; a
  per-shape metric would need a cardinality decision we have not made.
- **Rows scanned, plan choice.** Use `EXPLAIN` on the query the log named.
- **Traces.** Not shipped. REL-10 asks for them and this is the metrics and
  logs half; saying so is better than a panel that is always empty.

## Keeping it honest

`tests/dashboard_panels_have_metrics.rs` checks that every metric named in
every PromQL expression here is one the engine exports, in both directions. A
panel for a metric that does not exist renders as *no data*, which an operator
reads as "no traffic" rather than "no metric" — so a stale dashboard is worse
than no dashboard, and this is the check that stops one shipping.
