# samyama-sdk — TypeScript SDK for the Samyama Graph Database

A typed HTTP client for [Samyama](https://github.com/samyama-ai/samyama-graph),
a property-graph database with a Cypher query engine.

This package is **remote only**: it talks to a running Samyama server over
HTTP. There is no embedded mode in TypeScript — for in-process use, see the
Python or Rust SDK.

> The npm package `samyama-sdk` and the Rust crate `samyama-sdk` share a name.
> They are separate artifacts from the same repository.

## Install

```bash
npm install samyama-sdk
```

ESM only. Needs Node 18+ (it uses the global `fetch`, `AbortSignal.timeout`
and `AbortSignal.any`).

## Connect

```ts
import { SamyamaClient } from "samyama-sdk";

const client = new SamyamaClient({
  url: "http://localhost:8080",  // default
  timeoutMs: 30000,              // whole-request deadline, default 30000
  maxRetries: 2,                 // retries on timeout/connection failure
  retryBaseDelayMs: 100,         // doubled each attempt
});
```

Or the factory, when the URL is all you need:

```ts
const client = SamyamaClient.connectHttp("http://localhost:8080");
```

## First query

```ts
import { SamyamaClient } from "samyama-sdk";

const client = SamyamaClient.connectHttp("http://localhost:8080");

await client.query('CREATE (:Person {name: "Alice"})-[:KNOWS]->(:Person {name: "Bob"})');

const result = await client.queryReadonly(
  "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
);

console.log(result.columns); // ["a.name", "b.name"]
for (const row of result.records) {
  console.log(row); // ["Alice", "Bob"]
}
```

`query` is read-write, `queryReadonly` is read-only. Both take an optional
second argument `graph`, which defaults to `"default"`.

A `QueryResult` is `{ nodes, edges, columns, records }`.

## Other methods

| Method | Returns |
|---|---|
| `explain(cypher, graph?)` | the plan, as text rows in `records` |
| `profile(cypher, graph?)` | the executed plan with timings |
| `status()` | `ServerStatus` |
| `healthy()` | `boolean` |
| `ping()` | `string` |
| `schema()` | `GraphSchema` — node types, edge types, indexes, constraints |
| `sample(options?)` | a sample subgraph |
| `listGraphs()` | `string[]` |
| `deleteGraph(graph?)` | `void` |
| `importCsv(...)` / `importJson(...)` | import results |

`HttpTransport` is exported too, for callers that want the raw request layer
with their own `AbortSignal`.

## Build from source

```bash
npm install && npm run build && npm test
```

## More

- [`docs/SDK_API_CLI_ARCHITECTURE.md`](https://github.com/samyama-ai/samyama-graph/blob/main/docs/SDK_API_CLI_ARCHITECTURE.md)
  — how the SDKs, the HTTP API and the CLI fit together.
- [`sdk/README.md`](https://github.com/samyama-ai/samyama-graph/blob/main/sdk/README.md)
  — the other two SDKs (Python, Rust).

Apache-2.0.
