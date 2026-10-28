# Versioning and compatibility

What a version number promises, which client versions a server works with, and
how something is retired. Each promise below names the CI check that keeps it;
the ones nothing checks yet are listed at the end rather than left implied.

Applies to releases from **1.13.0**. Deprecations made before then are listed in
[`deprecations.json`](./deprecations.json) with what was actually said at the
time.

---

## One version for everything a client talks to

The HTTP API, the RESP commands, the Python SDK and the TypeScript SDK share one
version number: the `version` in the root `Cargo.toml`. A Python client 1.12.0
is the client for server 1.12.0. `tests/version_consistency.rs` fails if any
place that publishes a version disagrees with it (API-10).

The server says which version it is: `GET /api/status` returns it as `version`,
every query response carries it as `engine_version`, and RESP `INFO` reports it
as `samyama_version`.

## What each part of the number means

Semantic versioning, `MAJOR.MINOR.PATCH`, with "the contract" meaning the
surfaces listed in the next section.

| Release | May contain | May not contain |
|---|---|---|
| **Patch** `1.12.x` | fixes | anything new in the contract; any removal |
| **Minor** `1.x.0` | additions; fixes; new deprecations | a breaking change |
| **Major** `x.0.0` | breaking changes, including removing what was deprecated | removing anything not deprecated for two minors first (below) |

### What is a breaking change

| Surface | Breaking | Not breaking |
|---|---|---|
| **HTTP API** ([`openapi/openapi.yaml`](../openapi/openapi.yaml)) | removing or renaming an operation or a response field; changing a field's type or meaning; refusing a request that was correctly answered before; changing the status code or error code returned for the same condition | a new operation; a new optional request field; a new response field (a client must ignore fields it does not know); a new error for input that was previously answered wrongly |
| **RESP** (`GRAPH.*`, `PING`, `ECHO`, `INFO`) | removing a command or an argument form; changing a reply's shape | a new command; a new optional argument |
| **Python SDK** (`samyama` on PyPI) and **TypeScript SDK** (`samyama-sdk` on npm) | removing or renaming a public method, argument or result attribute; changing what a call returns or raises for the same input | a new method; a new optional argument with a default that keeps the old behaviour |

### A fix is not a breaking change

When the engine answered wrongly and starts answering correctly -- or starts
refusing what it used to answer wrongly -- that is a fix, and it ships in a
patch or minor release. The contract never promised the wrong answer. What a
fix owes users is to be said: it goes under **Read this before upgrading** in
the release notes. 1.12.0 has three, for example: a vector search against a
missing index became an error instead of an empty result that looked like "no
neighbours" (#1660).

The line is whether the old behaviour was *correct*. A field renamed for
neatness is a break however small; an empty result that was silently wrong is a
fix however visible.

Not part of the contract, and free to change in any release: the text of error
messages (the error **code** is part of it), the wording of `EXPLAIN` and
`PROFILE` output, `plan_hash` values, log lines, and the bundled visualizer's
pages.

## N-1: the previous minor's SDKs work against the current server

A program using the Python or TypeScript SDK from minor release N-1 works
against a server from release N, with no change to the program. Upgrade the
server first, then the clients, in either order within a pair of adjacent
minors.

**Checked by** `sdk/compat/run.py`, the `sdk-compat` CI job. It installs the
SDKs **as published** -- from PyPI and npm, not from this tree -- for the
latest installable patch of the minor in the version of record and its
immediately preceding minor, and runs a suite through each against a server
built from the change under review: connecting, status, reads and writes, typed
values, nodes and edges, streaming, a read-only call refusing a write, and an
error raising. During a release PR that raises the workspace version, the
candidate SDKs have not been published yet, so CI uses the base version as the
ceiling until the tag publishes them. The check still requires that version and
its predecessor; it never silently skips or substitutes an older minor. The
versions come from Cargo manifests and the package indexes on every run.

Missing either required minor fails the job; an older minor cannot substitute
for it. Deprecated npm releases remain eligible because users can still install
and hold them. Yanked PyPI files are excluded; if no installable patch remains
in a required minor, the job fails. At a major boundary (`x.0.0`), an explicit
compatibility window must be defined rather than inferred from available SDKs.

Two minors rather than one because a build on `dev` can become either a new
minor or a patch: just after 1.12.0 ships, `dev` still reads 1.12.0 on its way
to 1.13.0, whose N-1 is 1.12, while a 1.12.1 must still serve 1.11. On the
1.13.0 release PR, CI therefore tests published 1.12 and 1.11 SDKs against the
1.13.0 server candidate. Later builds use the 1.13 and 1.12 SDKs once 1.13 is
published.

The Rust SDK (`crates/samyama-sdk`) is not published, so there is no previous
release of it for a user to hold; the guarantee starts when it is published.

## Deprecation: two minor releases of notice, removal only in a major

Retiring something takes four steps, in this order:

1. **Mark it.** Add an entry to [`deprecations.json`](./deprecations.json): what
   it is, the release that first ships the notice (`deprecated_in`), and what to
   use instead. An HTTP operation also gets `deprecated: true` in
   `openapi/openapi.yaml`.
2. **Announce it** under **Deprecated** in that release's notes, with the
   replacement.
3. **Keep it working** for at least the next two minor releases. Deprecated is
   not broken: it still does what it did.
4. **Remove it only in a major release**, and record `removed_in`.

So something deprecated in 1.13.0 still works in 1.14 and 1.15, and can first be
removed in 2.0.0 -- provided 1.15.0 shipped first. If 2.0.0 came after only
1.14, the removal would wait for 3.0.0.

**Checked by** `tests/deprecations_follow_the_policy.rs`:

- every entry is well-formed, and `deprecated_in` is not later than the version
  of record;
- a removal is at a later `MAJOR.0.0`, with at least two minor releases between the
  notice and the removal (the releases are read from `docs/release-notes/`);
- a deprecated HTTP operation that has not been removed is still in the OpenAPI
  document and marked `deprecated: true` there -- and every operation marked
  `deprecated: true` has an entry, so neither list can drift from the other;
- once a release's notes exist, they mention every deprecation made in that
  release (from 1.13.0; earlier entries carry a note saying what was said).

Removing a deprecated HTTP operation also changes the recorded contract, so it
fails `CH-API-STABILITY` until the corpus is re-recorded on purpose -- which is
the point: a removal should take a decision, not slip through.

## How the pieces are checked

| Promise | Check | Where |
|---|---|---|
| one version everywhere | `version_consistency` | `cargo test` |
| the HTTP contract does not change by accident | `CH-API-STABILITY`: a recorded corpus of requests and responses (`tests/fixtures/api_corpus/`) replayed against every build; re-recording needs `--i-am-changing-the-contract` (API-16) | `test` job |
| the server and the OpenAPI document agree | `CH-API` (API-01) | `test` job |
| N-1 SDKs still work | `sdk/compat/run.py` | `sdk-compat` job |
| deprecations follow the steps above | `deprecations_follow_the_policy` | `cargo test` |

## Not checked yet

Said here so that nobody reads the table above as covering more than it does:

- **Which kind of release a change belongs in.** CI fails a change to the HTTP
  contract, but it does not decide whether that change is an addition, a fix or
  a break; the person re-recording the corpus does, and the review checks it.
- **The RESP surface has no recorded corpus.** `CH-API-STABILITY` replays HTTP
  only; a change to a RESP reply shape is caught only by the tests that happen to
  read it.
- **The current SDKs' own signatures are not compared with the previous
  release's.** The N-1 job runs the old SDKs against the new server; it does not
  notice a method removed from the new SDK. That is caught in review.
- **The guarantee runs one way.** Old SDK against new server is checked; new SDK
  against old server is not promised.
- **A deprecated call does not say so at runtime.** There is no `Deprecation`
  response header and no SDK warning yet; the notice is in the documents and the
  release notes.
