# ADR-039: Streaming query results

## Status

Proposed

## Date

2026-09-22

## Context

API-07 asks for "large result sets streamed with backpressure, not buffered, on
HTTP and in all SDKs". It is measured as `query_results_streamed` and reads
`false`: a 2,000-row result comes back with a `Content-Length` and no
`Transfer-Encoding: chunked`, so the whole body is built before the first byte
leaves.

`#1393` establishes what the blocker is not. `query_handler` runs the query to a
complete `RecordBatch` and only then renders it. **Wrapping that finished batch
in a chunked body would make the measurement pass** — chunked encoding, no
`Content-Length` — while peak memory and time-to-first-byte stayed exactly what
they are. That is a check that cannot fail, and it is the first thing this ADR
rules out.

Two real blockers, and the second is the one that needs a decision.

### 1. Execution materialises

The Volcano pipeline is pull-based and could yield incrementally. `execute`
collects into a `RecordBatch` because `render_query_result` needs the batch for
`merged_node_properties`, which resolves the columnar property view for the
nodes *in the result* — it has to know which nodes those are, i.e. all of them.

This is work, but it is ordinary work: resolve properties per batch of rows
rather than per result. It raises no question about what the API promises.

### 2. The read lock would have to be held across the stream

`store.read().await` is released when the handler returns. Streaming rows means
holding it for as long as the client takes to read them, so **one stalled
consumer blocks every writer for the duration**. A client that opens a query and
stops reading is then a denial of service, and it does not have to be malicious
— a laptop that closed its lid will do.

That is the question API-07 actually poses, and no encoder change answers it.

## Decision

**Adopt an explicit cursor (option C below) as the near-term shape, and a
snapshot pin (option A) as the destination once non-blocking readers land.
Reject the bounded buffer (option B) outright.**

Concretely:

1. `POST /api/query` gains an optional `page_size`. When given, the response
   carries the first page and a `cursor`, and the read lock is released before
   the response is written.
2. `POST /api/query` with a `cursor` returns the next page. Each page is a
   *fresh consistent read*; the sequence of pages is not a single snapshot, and
   the response says so in a field rather than in documentation nobody reads.
3. `query_results_streamed` is **not** satisfied by this, and the measurement
   should not be changed to say it is. Paging is not streaming with
   backpressure; it is the honest thing available before a snapshot pin exists.
   API-07 stays red, with a shorter gap.
4. When MVCC exposes a versioned read (PERF-17's "non-blocking readers"), the
   cursor becomes a pinned version and the same endpoint gains real streaming
   with a consistent result. The client-visible shape does not change again.

## Consequences

**Easier.** Time-to-first-byte drops to the first page. Peak server memory is
bounded by `page_size` rather than by the result. No long-held read lock, so a
stalled client costs one page of memory and nothing else. The SDKs get an
iterator that is honest about what it is doing.

**Harder.** The client has to page, and a result that changes between pages can
show a row twice or not at all. That is a real cost and the reason step 2 makes
it visible: a caller who needs a consistent large read has to wait for the
snapshot pin, and should be told so rather than discovering it.

**Deferred.** True backpressure — the server slowing down because the consumer
is slow — is not delivered here. With paging, backpressure is the client not
asking for the next page, which is weaker and differently shaped. `#1393`
records backpressure as *unmeasured* rather than failing, because there is no
stream to apply it to; that stays true.

## Alternatives Considered

### A. Snapshot per stream, against MVCC

Pin a version at the start of the stream and read at it while writers proceed.

The pieces half exist: `GraphStore` keeps MVCC version chains — one `Vec<Node>`
per node id — and carries a `current_version` that "only the transaction API
advances". What is missing is a public read-at-version path; the chains are
there for rollback, not for readers, and PERF-17 ("64 concurrent clients, p99
≤ 3× single-client p50") is unmeasured.

This is the right destination: one consistent result, streamed, with no writer
blocked. It is also the most work, and doing it under an API-07 deadline is how
a versioned read gets bolted on badly.

### B. A bounded buffer that detaches from the lock after N rows

Hold the lock while filling N rows, release it, stream those, take the lock
again for the next N.

**Rejected, and it is the dangerous option.** The second acquisition sees a
different graph, so the response body contains rows from two or more states
while looking like one result. Nothing in the protocol says so and nothing in
the client can detect it. It measures as streaming, bounds memory, improves
time-to-first-byte — and quietly stops answering the question the caller asked.

Option C has the same inconsistency and is acceptable precisely because it is
*visible*: the client asked for the next page, so it knows there were two reads.
The difference between B and C is not the mechanism, it is whether the caller
can tell.

### C. An explicit cursor (chosen)

Described above.

### D. Chunk the finished batch

Wrap the complete `RecordBatch` in a chunked body.

Rejected. It changes the measured property and nothing else: the same peak
memory, the same time-to-first-byte, the same lock behaviour. `#1393` exists
partly to stop API-07 being closed this way, and this ADR records the refusal
so the next person does not have to rediscover it.

## Related Decisions

- [ADR-007](./ADR-007-volcano-iterator-execution.md) — the pull-based pipeline this
  would stream from
- `#1393` — API-07: streaming is blocked by the read lock, not the encoder
- `#1200` — MVCC undo log, which is where the version chains came from
- PERF-17 — non-blocking readers, the prerequisite for option A
