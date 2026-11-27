# ADR-044: User-Defined Functions and Procedures, Registered from Rust

## Status
**Accepted** -- implemented for LANG-10 (#1674).

## Date
2026-11-26

## Context

LANG-10 asks for user-defined procedures and functions with a stable ABI,
staged across three horizons:

| | target |
|---|---|
| H1 | design + Rust registration |
| H2 | WASM UDFs, sandboxed |
| H3 | resource-limited, multi-tenant safe |

Before this, there was no way to add either. `CH-UDF` tried every route a
user might: ten Cypher statements (`CREATE FUNCTION`, `CALL
dbms.registerProcedure`, `LOAD EXTENSION`, ...), five start-up flags and three
environment variables. None was accepted, and afterwards `RETURN myfn(1)` was
still `UnknownFunction` and `CALL myfn()` still `Procedure.NotFound`. The
engine had 100-odd built-in functions and a closed set of procedures, all
dispatched by name in `eval_function` and `plan_call`.

The H1 target is about **Rust**: a program that embeds the engine -- an
application, or a server binary built around it -- adds its own names.
Loading code into a running server that was not built with it is H2, and that
is where a sandbox belongs.

## Decision

### 1. A process-wide registry, `samyama::udf`

```rust
samyama::udf::register_function("shop.withTax", 1..=1, |args| { /* ... */ })?;
samyama::udf::register_procedure("shop.nodeCount", 0..=0, &["nodes"], |store, args| { /* ... */ })?;
```

- **A function** takes argument values and returns one value. It is
  callable wherever an expression can go: `RETURN`, `WHERE`, `WITH`, `SET`.
- **A procedure** takes argument values and the graph, read-only (`&GraphStore`),
  and returns rows under the columns it declared. It is reached by `CALL ...
  YIELD`, with aliases, and composes with the rest of the query.
- There are also `unregister_function`, `unregister_procedure`,
  `function_names` and `procedure_names`.

The registry is the process's, not a `QueryEngine`'s. The reason is that
`eval_function` -- where every function call ends up -- is reached from
dozens of operators that have the store but not the engine, and threading an
engine handle through all of them would touch every operator for a feature
none of them otherwise needs. Applications register at start-up, so in
practice a registration is visible to every engine in the process anyway; a
server built around the engine serves the functions over HTTP and RESP with
no further work.

### 2. Values in, values out

Both kinds take and return `PropertyValue`, the type a query's parameters and
results already use. A list or map in a query arrives as `Array` or `Map`. A
node, relationship or path is **refused** with a message saying to pass a
property or `properties(n)` instead. Exposing `Node` and `Edge` would tie
user code to the store's internal layout -- properties live in columns, not
on the node (`Node::get_property` sees nothing for most of them) -- which is
exactly what a stable interface must not do.

A procedure gets `&GraphStore` because a procedure that cannot read the graph
is a function. It cannot write: `CALL` stays a read on every path, including
the read-only executor.

### 3. Names

A Cypher name, optionally dotted, matched regardless of case, as built-in
function names are. **A name the engine already uses is refused at
registration**:
- built-in functions and aggregates;
- the built-in function namespaces (`date.`, `vector.`, ...);
- built-in procedures and algorithms;
- the engine's procedure namespaces (`db.`, `algo.`, `gds.`, `ai.`, `eval.`,
  `dbms.`, `samyama.`).

Built-ins are dispatched first in both `eval_function` and `plan_call`, so
such a registration would be accepted and never reached. Saying so at
registration is better than finding out from a wrong answer.

Functions and procedures are separate namespaces, as in Cypher: `myfn` can be
both.

### 4. What the engine guarantees around user code

- **Unknown is unchanged.** Until a name is registered, a query using it gets
  the same compile-time `UnknownFunction` or `Procedure.NotFound` as before;
  the compile-time check (`is_known_function`) consults the registry.
- **Argument counts are checked** before user code runs: an `ArgumentError`
  naming the expected count.
- **A failure stays in the query.** An `Err(message)` from user code fails
  the query with that message. A **panic is caught** (`catch_unwind`) and
  reported the same way, so a bug in a registration does not take the server
  down.
- **A procedure's rows are checked** against its declared columns, and a
  mismatch is an error naming both.
- **The result cache cannot serve a stale answer.** The registry has a
  generation that every registration and removal bumps, and it is part of the
  result cache's key, like the store's epoch. A read cached before a function
  was replaced is not served after.

### 5. What "stable ABI" means at H1

It means a **source-level** contract: the `samyama::udf` signatures and
`PropertyValue`, kept compatible within a major version. It does **not**
mean a binary ABI, so a compiled plugin cannot be loaded into a server built
separately. A Rust closure cannot cross that boundary safely (Rust has no
stable ABI of its own), and a C ABI for `dlopen` would hand the process to
unsandboxed native code -- the opposite of H2's "sandboxed".

## Consequences

- An embedding application or a custom server binary can add functions and
  procedures today, callable from every surface the engine serves.
- The stock `samyama` server binary gains none by itself. That is H2's job.
- User code is trusted. It runs in-process, unbounded in time and memory,
  and sees every tenant's graph through the store it is handed. That is
  acceptable for code the operator compiled in, and it is why loading
  third-party code waits for H2 and H3.
- `CH-UDF` measures this through `examples/udf_registration.rs`, which
  registers the suite's own probe names (`RETURN myfn(1)`, `CALL myfn()`),
  checks that both are unknown first, and judges each call by its answer.

## Not done here, and where it goes

| | horizon |
|---|---|
| WASM functions loaded at run time, sandboxed (fuel / memory limits) | H2 |
| Per-tenant registries, resource limits, multi-tenant isolation | H3 |
| `SHOW FUNCTIONS` / `SHOW PROCEDURES` listing registrations from Cypher | later |
| Volatility flags so a non-deterministic function skips the result cache | later (see below) |

**Non-deterministic functions.** The result cache assumes a read's answer is
determined by the query, its parameters and the graph. That is the same
assumption it already makes of every built-in. A user function that reads the
clock or a random source breaks it, just as `rand()` would. Until a
volatility flag exists, such a function should not be used through the
cached read path. The registry generation covers the other half: replacing
the function.

## Alternatives considered

- **A registry per `QueryEngine`.** It would isolate engines in one process,
  but every operator that evaluates an expression would need an engine handle
  it does not have today. That cost is paid for isolation that H3 asks for
  per tenant anyway, and it will need a different shape there.
- **A registry on `GraphStore`.** This would be per tenant, but a closure in
  the store complicates snapshots, cloning and persistence. A function is
  code, not data.
- **`CREATE FUNCTION ... AS '<cypher>'`.** A Cypher-defined function is a
  macro, not an extension point: it cannot call out to a library, which is
  most of why people write UDFs.
- **`dlopen` of a C-ABI plugin.** See §5: unsandboxed native code, and the
  wrong foundation for H2.
