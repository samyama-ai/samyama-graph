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

## Personal identifiers in what we publish (TRUST-10)

`samyama pii-scan <snapshot.sgsnap>...` scans a snapshot for personal
identifiers and exits non-zero if it finds any. The weekly
`.github/workflows/pii-scan.yml` runs it over every `.sgsnap` currently
attached to the `kg-snapshots-v1` release.

The gate is on the **published set**, not on the release job, because the
snapshots are uploaded by hand rather than by CI. A check wired into the
release workflow would pass every time without ever seeing an asset.

**What it looks for.** Email addresses, international-format phone numbers,
payment card numbers, Aadhaar, PAN, US Social Security numbers, and IBANs.
Every pattern that has a checksum is checked against it: a sixteen-digit number
is not a card number unless it passes Luhn, a twelve-digit number is not an
Aadhaar unless it passes Verhoeff. Values are scanned whole and then by token,
so an address inside free text is found.

**What it does not look for, and why.**

- **Names.** Several published graphs are built from public records — the
  legal-judgments graph names judges and parties, the football graph names
  players. A detector for names fires on every row of those, gets switched off,
  and leaves a control that exists and does nothing.
- **IP addresses.** Personal data under GDPR, and in an earlier draft of the
  scanner. Removed for two reasons: the cyber KGs are *about* addresses, so it
  fired on every row of the datasets it was added for; and a dotted quad is
  indistinguishable from a version string — `1.0.0.0` is both.

**What a clean run does not mean.** It does not mean a snapshot is free of
personal data. Free text carrying a home address in prose passes, and so does a
name paired with a diagnosis, which is more sensitive than anything in the list
above. The scan is a floor under the published set, not a judgement about it.

**Current state.** Pending the first full run; this line records the result
once the workflow has scanned the whole published set.

## What we would have to change for this page to stop being true

A new outbound call, a default provider, telemetry of any kind, or a feature
that sends property values without being switched on. Each of those is a change
to this page as much as to the code, and the point of naming files and line
numbers above is that the two can be checked against each other.

## Related

- [`SECURITY.md`](../SECURITY.md) — reporting a vulnerability.
- **Snapshots can be encrypted at rest** (REL-09):

  ```bash
  samyama snapshot-key > /etc/samyama/snapshot.key   # 32 bytes, as hex
  samyama --snapshot-key /etc/samyama/snapshot.key
  ```

  `/api/snapshot/export` then returns a `.sgsnap.enc`, sealed with
  ChaCha20-Poly1305 in 64 KiB frames. Import **sniffs** the file: an encrypted
  snapshot needs the key, a plaintext one is read exactly as before, so turning
  encryption on does not strand the snapshots already taken.

  What the construction protects against, and what it does not:

  - **Reading.** The frames are encrypted, not merely framed.
  - **Alteration.** Each frame is authenticated; a flipped bit fails to open.
  - **Truncation.** The stream ends with an authenticated terminator, so a file
    cut short fails instead of importing a graph missing its tail. AEAD alone
    does not give this.
  - **Key reuse across files.** The nonce is an 8-byte random per-file prefix
    plus a frame counter, so the same key may seal many snapshots.
  - **Not** key rotation without downtime, which REL-09 also asks for: changing
    the key means re-exporting.
  - **Not** the RocksDB data directory. This is snapshots only.

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

- **Users with passwords**, in the same file (REL-08, REQ-SEC-001):

  ```bash
  samyama auth-user alice >> /etc/samyama/credentials   # password read from stdin
  ```

  ```text
  ops:2b9c…                      # a machine token, SHA-256
  alice:$argon2id$v=19$m=…       # a person's password, argon2id
  ```

  Machines send `Authorization: Bearer <token>`; people send
  `Authorization: Basic <base64(user:password)>`. The two are told apart by the
  **stored form**, not by a flag, and a credential is only ever checked against
  the scheme its form belongs to — so a password is never verified with the
  fast hash, and a token is never dragged through argon2.

  Why two hashes: against a stolen file, the defence for a human-chosen
  password is the cost of each guess, so it gets argon2id. A 32-byte token from
  `samyama auth-token` has nothing to guess, so the cost buys nothing and the
  check runs on every request.

  What this is still not: there are **no roles** and **no per-graph grants**. A
  credential is all-or-nothing. Per-graph grants are not merely unimplemented —
  this build serves a single graph, so a grant could only ever say yes, and a
  permission check that cannot refuse is worse than none. That part of REL-08
  waits on multi-graph serving.

- **Every state-changing request can be recorded** (REL-08):

  ```bash
  samyama --audit-log /var/log/samyama/audit.jsonl --auth-file /etc/samyama/credentials
  ```

  One JSON object per line, flushed on every write:

  ```json
  {"at":"2026-09-22T16:19:00+00:00","subject":"ops","method":"POST","path":"/api/query","status":200}
  ```

  Selected by **method**, not by a list of routes: `POST`, `PUT`, `PATCH` and
  `DELETE` are recorded, `GET`, `HEAD` and `OPTIONS` are not. A list of write
  routes is a list somebody forgets to extend, so a new endpoint is audited the
  day it is added. The consequence, stated rather than hidden: a *read*
  submitted as `POST /api/query` is recorded too.

  Refused requests are recorded, with `subject: "unauthenticated"` — a 401 on a
  write route is exactly the entry the log exists for.

  **The request body is never recorded.** It carries the query, and the query
  carries the data. What is recorded is who, what route, which method, what
  status, and when. The bearer token never appears: the entry is built from the
  credential that matched, not from the header that was presented.

  Off by default. A log written to a path nobody chose is how a disk fills up
  on a machine that was working yesterday.

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
