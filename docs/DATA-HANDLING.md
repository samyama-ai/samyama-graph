# Data handling

What this engine does with your data, and where any of it can go.

Written from a reading of the code, not from intent. Every claim below names
the file that carries it, so you can check it rather than believe it. If you
find a path this page does not list, that is a bug in this page and worth an
issue.

**Verified at** `main`, 2026-09-20.

## The short version

Samyama is a database you run. It stores your graph on your machine, answers
queries from it, and sends nothing anywhere unless you configure a feature that
needs an external model.

- **No telemetry.** No usage analytics, no crash reporting, no update check, no
  license check, no phone-home of any kind. There is no code that reports
  anything about your installation to us or to anyone else.
- **Three features can send data to a third party**, all off unless you
  configure them, all to a provider you choose and key yourself.
- **We operate nothing.** These requests go from your server to the provider
  you named. Samyama is not in the path and has no copy.

## What leaves the machine, and when

| Feature | What is sent | Where | On by default |
|---|---|---|---|
| NLQ (`POST /api/nlq`) | Your question, plus a **schema summary**: label names, relationship patterns, up to 5 property *key names* per label, and counts. No property values. | The provider in `NLQ_PROVIDER` | **No** |
| Embeddings | **Property values** — the text being embedded, or the `query_text` of a vector search | The provider in `EMBED_PROVIDER` | **No** |
| GAK enrichment (`POST /api/enrich`) | **Property values** — every merged property of each gap node, the label, the target property name, and your vocabulary taxonomy if configured | The provider in `NLQ_PROVIDER` | **No** |

The second and third send the contents of your graph. The first sends its
shape. That distinction is worth keeping in mind when deciding which to enable
against sensitive data.

Sources: `src/nlq/mod.rs:37`, `src/graph/store.rs:4239` (what the schema summary
contains), `src/embed/mod.rs:59`, `src/http/vector.rs:190`, `src/graph/store.rs:1374`
(auto-embed on write), `src/http/handler.rs:1368` and `src/agent/enrich.rs:184`
(what enrichment puts in the prompt).

## Choosing a provider

`NLQ_PROVIDER` and `EMBED_PROVIDER` accept `openai`, `ollama`, `gemini`,
`azure`, `anthropic`, `claudecode` and `mock`.

**`ollama` is a local endpoint** (`http://localhost:11434`) and nothing leaves
your machine on that setting unless you repoint it with `NLQ_API_BASE_URL` /
`EMBED_API_BASE_URL`. `mock` makes no call at all. `claudecode` spawns the
local `claude` CLI, so egress happens inside that binary rather than here —
note the prompt appears in your process table as an argument.

**There is no default.** An unset or unrecognised provider is refused with the
list of accepted names: the NLQ request fails, and a server started with
`EMBED_ENABLED=true` and an unusable `EMBED_PROVIDER` exits rather than
starting.

That is a change. All three sites that read a provider used to end
`_ => OpenAI`, so a typo meant OpenAI; `claudecode` was listed at neither NLQ
site and meant OpenAI; and `EMBED_PROVIDER=azure` was not on the embed list and
meant OpenAI — on the path that sends property values. Where a graph's content
goes is not a defaultable decision: the failure is silent and the data is
already gone by the time anyone looks. See `LLMProvider::parse_named` in
`src/persistence/tenant.rs` and `tests/llm_provider_is_never_defaulted.rs`.

**Your API key is a credential and is read from the environment**, never from
the graph and never logged. One caveat worth knowing: the Gemini API takes its
key as a URL query parameter (`src/nlq/client.rs:204`), so on that provider the
key reaches Google's request logs by their design, not ours.

## What stays local, that you might expect not to

- **Agent telemetry** (`src/agent/executor.rs:148`) writes `Question` and `Tool`
  nodes with latency and token counts **into your own graph**. It is a feature
  of the agent, not a report to us. Nothing is transmitted.
- **The web search tool** (`src/agent/tools.rs:147`) is a stub: it returns two
  hardcoded results and makes no request. It does print the search string to
  the server's stdout.
- **`LOAD CSV` cannot fetch a URL.** `http` and `https` are excluded from the
  allowed schemes on purpose, so a query cannot be used to make your server
  fetch something (`src/query/csv_source.rs:45`).
- **Raft and sharding** talk only to the peers you configured.

## What we would have to change for this page to stop being true

A new outbound call, a default provider, telemetry of any kind, or a feature
that sends property values without being switched on. Each of those is a change
to this page as much as to the code, and the point of naming files and line
numbers above is that the two can be checked against each other.

## Related

- [`SECURITY.md`](../SECURITY.md) — reporting a vulnerability.
- The HTTP API is **unauthenticated by default**, and can be told to require a
  credential ([#1328](https://github.com/samyama-ai/samyama-graph/issues/1328)):

  ```bash
  samyama auth-token ops >> /etc/samyama/credentials   # prints the token once
  samyama --auth-file /etc/samyama/credentials --host 0.0.0.0
  ```

  Every request then needs `Authorization: Bearer <token>` — every route, not a
  chosen subset, with `OPTIONS` exempt because a CORS preflight carries no
  credential. Without `--auth-file` nothing on the request path reads one, which
  is what every deployment before v1.9.1 does: anyone who can reach the port can
  read the graph, and `/api/query` runs arbitrary Cypher including `DELETE`.

  The file holds SHA-256 digests, not tokens. That is the right hash for a
  high-entropy token and the **wrong** one for a human-chosen password, which
  is why `samyama auth-token` generates the token from `/dev/urandom` rather
  than taking one: against a stolen file, a fast hash is safe only when there
  is nothing to guess.

  What this is not: there are no users, no roles and no per-graph grants — a
  token is all-or-nothing — and no audit log. REL-08 asks for all four.

- The HTTP API can **serve TLS**, and does not by default (REL-09):

  ```bash
  samyama --tls-cert /etc/samyama/fullchain.pem --tls-key /etc/samyama/key.pem \
          --auth-file /etc/samyama/credentials --host 0.0.0.0
  ```

  Both flags are required together; one without the other stops the server
  rather than falling back to cleartext, because a fallback would give an
  operator who asked for TLS a plain port and a log line they did not read.
  There is no self-signed fallback either: a server that invents a certificate
  teaches its clients to skip verification, and a client that skips
  verification has the cost of TLS and none of the guarantee.

  Without `--tls-cert`, queries, results and any bearer token cross the network
  in cleartext. A non-loopback bind now warns about that separately from the
  credential warning.

  At-rest encryption for storage and snapshots is the other half of REL-09 and
  does not exist.

  The "accepts any origin" half of #1328 was fixed earlier: CORS matches an
  explicit allowlist and the Private Network Access header is echoed only to an
  origin on it.
