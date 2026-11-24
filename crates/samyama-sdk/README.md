# samyama-sdk — Rust SDK for the Samyama Graph Database

The client library for [Samyama](https://github.com/samyama-ai/samyama-graph),
a property-graph database with a Cypher query engine. The Python SDK is built
on this crate.

Two clients, one trait:

- `EmbeddedClient` — in-process, no network. Drives `GraphStore` and
  `QueryEngine` directly.
- `RemoteClient` — HTTP to a running Samyama server.

Both implement the `SamyamaClient` trait (`query`, `query_readonly`, `status`,
`ping`, `list_graphs`, `delete_graph`). Two extension traits are
`EmbeddedClient`-only: `AlgorithmClient` (PageRank, WCC, SCC, BFS, Dijkstra,
max-flow, MST, triangle count, PCA) and `VectorClient` (vector index, insert,
k-NN search).

> The Rust crate `samyama-sdk` and the npm package `samyama-sdk` share a name.
> They are separate artifacts from the same repository.

## Install

Not on crates.io yet. Depend on it by git:

```toml
[dependencies]
samyama-sdk = { git = "https://github.com/samyama-ai/samyama-graph", tag = "v1.17.0" }
tokio = { version = "1", features = ["full"] }
```

Inside this workspace, use a path dependency:

```toml
samyama-sdk = { path = "crates/samyama-sdk" }
```

## Connect

Embedded — the engine runs in your process. There is no server to start and
no socket: `EmbeddedClient` drives the graph store and query engine directly.

```rust
use samyama_sdk::EmbeddedClient;

let client = EmbeddedClient::new();
```

Remote — HTTP to a server. `RemoteClient::new` uses `ConnectionConfig::default()`
(30 s request timeout, 5 s connect timeout, 2 retries with 100 ms base backoff).
A refused connection is retried for any request. A timeout is retried only for
requests that cannot write (`query_readonly`, `query_stream`, `nlq`, `status`);
a write that times out returns `SamyamaError::Timeout`, because the server may
already have applied it:

```rust,no_run
use samyama_sdk::{ConnectionConfig, RemoteClient};
use std::time::Duration;

let client = RemoteClient::new("http://localhost:8080");

// or with explicit settings
let client = RemoteClient::with_config(
    "http://localhost:8080",
    ConnectionConfig { timeout: Some(Duration::from_secs(5)), ..Default::default() },
);
```

## First query

```rust
use samyama_sdk::{EmbeddedClient, SamyamaClient};

#[tokio::main]
async fn main() {
    let client = EmbeddedClient::new();

    // Create data
    client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
        .await.unwrap();

    // Query data
    let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
        .await.unwrap();
    println!("Found {} records", result.len());
}
```

The `SamyamaClient` trait must be in scope to call `query`. Note the argument
order: `graph` first, then the Cypher text. `graph` is `"default"` in this
build; both clients reject other names rather than answer from `default`.

A `QueryResult` is `{ nodes, edges, columns, records }`, with `records` as
`Vec<Vec<serde_json::Value>>` and a `len()`.

## Find a node by an external key

Data imported from another system keeps its own keys — a primary key, a UUID.
Put the key in a property under a unique constraint, and look the node up by it
on the next load instead of keeping a key-to-`NodeId` map between runs:

```rust
use samyama_sdk::{EmbeddedClient, NodeId, SamyamaClient, SamyamaResult};

#[tokio::main]
async fn main() -> SamyamaResult<()> {
    let client = EmbeddedClient::new();
    client.query("default", "CREATE CONSTRAINT FOR (a:Acct) REQUIRE a.krid IS UNIQUE").await?;
    client.query("default", "CREATE (:Acct {krid: 42})").await?;

    let id: Option<NodeId> = client.find_node_by_unique("default", "Acct", "krid", 42i64).await?;
    assert!(id.is_some());
    Ok(())
}
```

`Ok(None)` means no node holds the key. `EmbeddedClient` answers from the
constraint's index and returns an error, not a scan, when there is no unique
constraint on the label and property. `RemoteClient` sends one read-only query
with the key bound as a parameter; the server uses the index the constraint
created, but the client does not check that the constraint exists. Both return
an error if two nodes hold the key.

## Evaluate an agent on a snapshot

`SamyamaEnv` runs scored episodes against a pinned `.sgsnap`: `reset` hands
the agent a task and a fresh copy of the graph, `step` runs one Cypher
statement (500 ms budget) or takes the final answer, and the answer is scored
with a reward in `[0, 1]`.

```rust,no_run
use samyama_sdk::env::{Action, SamyamaEnv};

# fn main() -> Result<(), samyama_sdk::env::EnvError> {
let mut env = SamyamaEnv::open("cricket.sgsnap", "tasks.jsonl", 42)?;
let task = env.reset(None)?;                       // task.prompt, task.schema
env.step(Action::Query("MATCH (p:Player) RETURN p.name LIMIT 5".into()))?;
let done = env.step(Action::Answer(vec!["V Kohli".into()]))?;
println!("reward {}", done.reward);                // F1 against the answer key
# Ok(()) }
```

A read task's answer key comes from an oracle query run against a copy of the
snapshot the agent never touches, so writing to its own graph cannot change
what it is scored against. A write task is scored by postcondition queries on
the graph the agent leaves. An episode that changed the graph is undone at the
next `reset`, and the same seed draws the same tasks, so a run repeats exactly.
The tasks file format is in the `env` module documentation.

## Checked, not just written

Every Rust block in this README is compiled as a program of its own that
depends on `samyama-sdk` the way yours does, and every embedded one is run, on
each `cargo test --workspace` (`ReadmeDoctests` in `src/lib.rs`). The remote
block and the agent-evaluation block are compiled but not run, since one needs
a server and the other a snapshot file.

- `tests/embedded_opens_no_socket.rs` runs writes, reads, a key lookup,
  PageRank and a vector search through `EmbeddedClient` and checks that the
  process holds no more sockets than it started with. NLQ is the exception
  by design: it sends the question to the LLM provider you configure.
- `tests/version_consistency.rs` (at the repository root) checks that the
  `tag` in the install line above is the current version.

## More

- [`docs/SDK-API-CLI-ARCHITECTURE.md`](../../docs/SDK-API-CLI-ARCHITECTURE.md)
  — how the SDKs, the HTTP API and the CLI fit together.
- [`sdk/README.md`](../../sdk/README.md) — the Python and TypeScript SDKs.

Apache-2.0.
