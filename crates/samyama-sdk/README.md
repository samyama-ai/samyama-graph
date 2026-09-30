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
samyama-sdk = { git = "https://github.com/samyama-ai/samyama-graph", tag = "v1.9.0" }
tokio = { version = "1", features = ["full"] }
```

Inside this workspace, use a path dependency:

```toml
samyama-sdk = { path = "crates/samyama-sdk" }
```

## Connect

Embedded — the engine runs in your process:

```rust
use samyama_sdk::EmbeddedClient;

let client = EmbeddedClient::new();
```

Remote — HTTP to a server. `RemoteClient::new` uses `ConnectionConfig::default()`
(30 s request timeout, 5 s connect timeout, 2 retries with 100 ms base backoff):

```rust
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
build; the server rejects other names.

A `QueryResult` is `{ nodes, edges, columns, records }`, with `records` as
`Vec<Vec<serde_json::Value>>` and a `len()`.

## Find a node by an external key

Data imported from another system keeps its own keys — a primary key, a UUID.
Put the key in a property under a unique constraint, and look the node up by it
on the next load instead of keeping a key-to-`NodeId` map between runs:

```rust
client.query("default", "CREATE CONSTRAINT FOR (a:Acct) REQUIRE a.krid IS UNIQUE").await?;
client.query("default", "CREATE (:Acct {krid: 42})").await?;

let id: Option<NodeId> = client.find_node_by_unique("default", "Acct", "krid", 42i64).await?;
```

`Ok(None)` means no node holds the key. `EmbeddedClient` answers from the
constraint's index and returns an error, not a scan, when there is no unique
constraint on the label and property. `RemoteClient` sends one read-only query
with the key bound as a parameter; the server uses the index the constraint
created, but the client does not check that the constraint exists. Both return
an error if two nodes hold the key.

## More

- [`docs/SDK_API_CLI_ARCHITECTURE.md`](../../docs/SDK_API_CLI_ARCHITECTURE.md)
  — how the SDKs, the HTTP API and the CLI fit together.
- [`sdk/README.md`](../../sdk/README.md) — the Python and TypeScript SDKs.

Apache-2.0.
