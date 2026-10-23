# Samyama SDKs

Three client SDKs for the [Samyama](https://github.com/samyama-ai/samyama-graph)
graph database. All three speak Cypher and return the same result shape:
`columns`, `records`, `nodes`, `edges`.

| SDK | Directory | Package | Modes |
|---|---|---|---|
| Python | [`sdk/python/`](python/) | `samyama` on PyPI | embedded + remote |
| TypeScript | [`sdk/typescript/`](typescript/) | `samyama-sdk` on npm | remote only |
| Rust | [`crates/samyama-sdk/`](../crates/samyama-sdk/) | `samyama-sdk` (git dependency; not yet on crates.io) | embedded + remote |

The Rust SDK is **not** in this directory. It is a member of the Cargo
workspace, so it lives in [`crates/samyama-sdk/`](../crates/samyama-sdk/) with
the other crates.

**Name collision, on purpose:** the npm package and the Rust crate are both
called `samyama-sdk`. They are separate artifacts built from this repository.
The Python package is called `samyama`, not `samyama-sdk`.

## Install

```bash
pip install samyama          # Python
npm install samyama-sdk      # TypeScript
```

```toml
# Rust
samyama-sdk = { git = "https://github.com/samyama-ai/samyama-graph", tag = "v1.9.0" }
```

## Modes

- **Embedded** runs the query engine in your own process. No server, no
  network. Python and Rust only.
- **Remote** speaks HTTP to a running Samyama server. All three SDKs.

Graph algorithms and vector search are embedded-only in both the Python and
the Rust SDK.

## Read next

- Per-SDK quickstarts: [Python](python/README.md) ·
  [TypeScript](typescript/README.md) · [Rust](../crates/samyama-sdk/README.md)
- [`VALUE-TYPES.md`](VALUE-TYPES.md) — what each value type arrives as in
  each SDK, what does not survive, and the shared round-trip suite.
- [`docs/SDK-API-CLI-ARCHITECTURE.md`](../docs/SDK-API-CLI-ARCHITECTURE.md) —
  how the SDKs, the HTTP API and the CLI fit together.

## Compatibility

The SDKs share the server's version number, and the previous minor release's
Python and TypeScript SDKs work against the current server —
[`docs/VERSIONING.md`](../docs/VERSIONING.md). [`compat/`](compat/) is the check:
it installs the SDKs as published on PyPI and npm and runs them against a
server built from this tree (`python3 sdk/compat/run.py`).
