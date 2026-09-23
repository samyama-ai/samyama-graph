//! Capability x surface parity matrix (API-02), generated from source.
//!
//! Six surfaces ship today: the HTTP API, the RESP protocol (`GRAPH.*`
//! commands), the Rust SDK, the Python SDK, the TypeScript SDK, and the MCP
//! server. Nobody had written down which of them can do what, so a user
//! found out by trying — the Rust SDK's `AlgorithmClient` and `VectorClient`
//! are `EmbeddedClient`-only (see `crates/samyama-sdk/src/algo.rs` and
//! `vector_ext.rs`), and there is no comment anywhere saying so.
//!
//! This does not hand-write the answer. Each cell is decided by searching
//! the real source of that surface for a specific piece of literal evidence
//! (an OpenAPI path, a RESP command string, a trait method, an SDK method
//! name, an MCP tool guard) and reporting what it found. A capability added
//! to a surface tomorrow flips its cell the next time this runs; a hand-kept
//! table would just be wrong until someone remembered to edit it. This is
//! the same idea `api_contract.rs` uses for the OpenAPI/server comparison,
//! applied across six surfaces instead of two.
//!
//! It is still a **static** probe — it greps source, it does not start a
//! server and drive each surface end to end. A capability that is routed
//! but broken at runtime would show as present here and wrong in practice;
//! see the "Not generated" note in the emitted document.
//!
//!     cargo run --release --example feature_matrix [-- --out docs/API-PARITY.md]

use std::fs;
use std::path::{Path, PathBuf};

/// One (surface, evidence) pair. `needle` is literal text expected verbatim
/// in `file` at the time this was written — read out of the real source
/// while building this generator, not invented.
struct Evidence {
    file: &'static str,
    needle: &'static str,
}

/// What a cell reports when its evidence is present vs. absent. Most cells
/// are plain yes/no; some capabilities are real but scoped (embedded-only,
/// reachable only through raw Cypher, partially implemented), and that
/// distinction is exactly what API-02 asks to make explicit rather than
/// collapsing to a boolean.
struct Cell {
    evidence: Evidence,
    if_present: &'static str,
    if_absent: &'static str,
}

struct Capability {
    id: &'static str,
    title: &'static str,
    /// One line of why the pattern of yes/no/scoped looks the way it does,
    /// where the source gives a reason. Left empty rather than guessed at
    /// when it doesn't.
    note: &'static str,
    http: Cell,
    resp: Cell,
    rust_sdk: Cell,
    python_sdk: Cell,
    typescript_sdk: Cell,
    mcp: Cell,
}

const SURFACE_HEADERS: [&str; 6] = [
    "HTTP",
    "RESP",
    "Rust SDK",
    "Python SDK",
    "TypeScript SDK",
    "MCP",
];

fn capabilities() -> Vec<Capability> {
    vec![
        Capability {
            id: "cypher_read",
            title: "Cypher — read-only query",
            note: "",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "Supports both read (MATCH) and write (CREATE, SET, DELETE, MERGE) queries." },
                if_present: "Yes — `POST /api/query`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "\"GRAPH.RO_QUERY\"" },
                if_present: "Yes — `GRAPH.RO_QUERY`", if_absent: "No",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/client.rs", needle: "async fn query_readonly" },
                if_present: "Yes — `SamyamaClient::query_readonly`", if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn query_readonly" },
                if_present: "Yes — `.query_readonly()`", if_absent: "No",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "queryReadonly(" },
                if_present: "Yes — `.queryReadonly()`", if_absent: "No",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/generators/generic_tools.py", needle: "def cypher_query" },
                if_present: "Yes — `cypher_query` tool", if_absent: "No",
            },
        },
        Capability {
            id: "cypher_write",
            title: "Cypher — read-write query",
            note: "MCP has no write path anywhere: every generated and custom tool is \
                   funnelled through `is_readonly_cypher`, which rejects CREATE/SET/DELETE/\
                   MERGE/DROP/FOREACH/LOAD/CALL before the query reaches the graph \
                   (`escape.py`). That is a deliberate safety choice, documented in \
                   `sdk/python/samyama_mcp/README.md` (\"Security — read-only by \
                   construction\"), not an oversight.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "Supports both read (MATCH) and write (CREATE, SET, DELETE, MERGE) queries." },
                if_present: "Yes — `POST /api/query`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "\"GRAPH.QUERY\"" },
                if_present: "Yes — `GRAPH.QUERY`", if_absent: "No",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/client.rs", needle: "async fn query(" },
                if_present: "Yes — `SamyamaClient::query`", if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn query(" },
                if_present: "Yes — `.query()`", if_absent: "No",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "async query(" },
                if_present: "Yes — `.query()`", if_absent: "No",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/escape.py", needle: "_WRITE_KEYWORDS" },
                if_present: "**No — blocked by design** (write keywords rejected before execution)",
                if_absent: "No (unverified: the read-only guard this note depends on was not found)",
            },
        },
        Capability {
            id: "transactions",
            title: "Multi-statement transactions (begin / commit / rollback)",
            note: "No SDK exposes a transaction handle. A caller on the Rust, Python or \
                   TypeScript SDK, or over RESP, gets exactly the atomicity of one query; \
                   the only way to hold a transaction open across statements today is raw \
                   HTTP against `/api/tx/*`.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/tx/begin:" },
                if_present: "Yes — `/api/tx/begin`, `/api/tx/{id}/commit`, `/api/tx/{id}/rollback`",
                if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.TX" },
                if_present: "Yes", if_absent: "No — no `GRAPH.TX*` command is registered",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/client.rs", needle: "fn begin" },
                if_present: "Yes", if_absent: "No — not on the `SamyamaClient` trait",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn begin" },
                if_present: "Yes", if_absent: "No method exposed",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "begin(" },
                if_present: "Yes", if_absent: "No method exposed",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/server.py", needle: "begin_transaction" },
                if_present: "Yes", if_absent: "No — every tool is one query, and write tools don't exist anyway",
            },
        },
        Capability {
            id: "graph_algorithms",
            title: "Graph algorithms (PageRank, BFS, WCC, SCC, Dijkstra, MST, …)",
            note: "There is no HTTP or RESP path to these at all — `/optimize/*` is the \
                   metaheuristic solver (simulated annealing etc. for scheduling/routing \
                   problems), a different feature, not graph analytics. The Rust and Python \
                   SDKs both gate algorithm methods behind `require_embedded()` \
                   (`crates/samyama-sdk/src/algo.rs`: \"extension trait ... (EmbeddedClient \
                   only)\"; `sdk/python/src/lib.rs`: \"Algorithm methods are only available \
                   in embedded mode\"), so a client built with `SamyamaClient.connect(url)` \
                   cannot call them — only `SamyamaClient.embedded()` can. The MCP server \
                   inherits that split: `AlgorithmToolGenerator` only registers tools when \
                   `hasattr(self.client, \"page_rank\")`, i.e. only when it wraps an \
                   embedded Python client.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/algorithms/pagerank" },
                if_present: "Yes", if_absent: "No endpoint",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.PAGERANK" },
                if_present: "Yes", if_absent: "No command",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/algo.rs", needle: "impl AlgorithmClient for EmbeddedClient" },
                if_present: "**Embedded-only** — `AlgorithmClient` (pagerank/wcc/scc/bfs/dijkstra/\
                             maxflow/mst/triangles/cdlp/clustering-coeff/pca)",
                if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn page_rank" },
                if_present: "**Embedded-only** — `.page_rank()`, `.wcc()`, `.scc()`, `.bfs()`, \
                             `.dijkstra()`, `.pca()`, `.triangle_count()` all call `require_embedded()`",
                if_absent: "No",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "pageRank(" },
                if_present: "Yes", if_absent: "No method",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/generators/algorithm_tools.py", needle: "hasattr(self.client, \"page_rank\")" },
                if_present: "**Embedded-only, conditionally** — `pagerank`/`shortest_path`/\
                             `communities` tools register only if the wrapped client exposes \
                             `page_rank`",
                if_absent: "No",
            },
        },
        Capability {
            id: "vector_search",
            title: "Vector index create / add / k-NN search",
            note: "Same split as the algorithms row: the HTTP API serves it directly, the \
                   Rust and Python SDKs serve it only in embedded mode, and MCP's \
                   `VectorToolGenerator` mirrors that by checking `hasattr(self.client, \
                   \"vector_search\")` before registering `find_similar_*` tools.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/vector-search:" },
                if_present: "Yes — `/api/vector-search`, `/api/vector/indexes`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.VECTOR" },
                if_present: "Yes", if_absent: "No command",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/vector_ext.rs", needle: "impl VectorClient for EmbeddedClient" },
                if_present: "**Embedded-only** — `VectorClient`", if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn vector_search" },
                if_present: "**Embedded-only** — `.vector_search()` calls `require_embedded()`",
                if_absent: "No",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "vectorSearch(" },
                if_present: "Yes", if_absent: "No method",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/generators/vector_tools.py", needle: "hasattr(self.client, \"vector_search\")" },
                if_present: "**Embedded-only, conditionally** — `find_similar_*` tools",
                if_absent: "No",
            },
        },
        Capability {
            id: "snapshot",
            title: "Portable snapshot export / import (.sgsnap)",
            note: "",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/snapshot/export:" },
                if_present: "Yes — `/api/snapshot/export`, `/api/snapshot/import`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.SNAPSHOT" },
                if_present: "Yes", if_absent: "No command",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/embedded.rs", needle: "pub async fn export_snapshot" },
                if_present: "**Embedded-only** — `EmbeddedClient::export_snapshot`/`import_snapshot`\
                             (not on the `SamyamaClient` trait, so `RemoteClient` has neither)",
                if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "snapshot" },
                if_present: "Yes", if_absent: "No method exposed",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "exportSnapshot(" },
                if_present: "Yes", if_absent: "No method",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/server.py", needle: "snapshot" },
                if_present: "Yes", if_absent: "No tool",
            },
        },
        Capability {
            id: "bulk_import",
            title: "Bulk import (CSV / JSON / Parquet)",
            note: "The TypeScript SDK covers two of the three formats the HTTP API serves; \
                   there is no evidence of a `importParquet` method.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/import/parquet:" },
                if_present: "Yes — `/api/import/csv`, `/api/import/json`, `/api/import/parquet`",
                if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.IMPORT" },
                if_present: "Yes", if_absent: "No command",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/embedded.rs", needle: "fn import_csv" },
                if_present: "Yes", if_absent: "No method exposed",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "import_csv" },
                if_present: "Yes", if_absent: "No method exposed",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "importParquet(" },
                if_present: "Yes — csv/json/parquet",
                if_absent: "**Partial** — `.importCsv()`, `.importJson()`; no `importParquet`",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/server.py", needle: "import_csv" },
                if_present: "Yes", if_absent: "No tool",
            },
        },
        Capability {
            id: "nlq",
            title: "Natural-language-to-Cypher (NLQ)",
            note: "MCP does not need this the way the other surfaces might: instead of \
                   translating a question to Cypher, it generates a typed tool per label/\
                   edge/algorithm/vector-index so the agent calls those directly. Whether \
                   that substitutes for NLQ is exactly AI-03's open question, not this one's.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/nlq:" },
                if_present: "Yes — `/api/nlq`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.NLQ" },
                if_present: "Yes", if_absent: "No command",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/embedded.rs", needle: "fn nlq_pipeline" },
                if_present: "**Embedded-only** — `EmbeddedClient::nlq_pipeline()` returns the \
                             `NLQPipeline`; not on the shared trait",
                if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "nlq" },
                if_present: "Yes", if_absent: "No method exposed",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "nlq(" },
                if_present: "Yes", if_absent: "No method",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/server.py", needle: "nlq" },
                if_present: "Yes", if_absent: "No — tools are generated from schema instead (see note)",
            },
        },
        Capability {
            id: "schema_introspection",
            title: "Schema / index discovery",
            note: "\"Via Cypher only\" means the capability exists but not as a named method — \
                   a caller sends `SHOW INDEXES` / `SHOW CONSTRAINTS` through the same query \
                   call every other Cypher statement uses. That is a real capability, just not \
                   a discoverable one from a method list or an IDE's autocomplete.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/schema:" },
                if_present: "Yes — `/api/schema`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "GRAPH.SCHEMA" },
                if_present: "Yes", if_absent: "Via Cypher only — `SHOW INDEXES`/`SHOW CONSTRAINTS` over `GRAPH.QUERY`",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/client.rs", needle: "fn schema" },
                if_present: "Yes", if_absent: "Via Cypher only — no dedicated method on the trait",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn schema" },
                if_present: "Yes", if_absent: "Via Cypher only — no dedicated method",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "async schema(" },
                if_present: "Yes — `.schema()`", if_absent: "No",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/generators/generic_tools.py", needle: "def schema_info" },
                if_present: "Yes — `schema_info` tool", if_absent: "No",
            },
        },
        Capability {
            id: "multi_graph",
            title: "Multi-graph management (list / delete)",
            note: "MCP has no tool for this because a server is bound to one graph at \
                   construction (`SamyamaMCPServer(client, graph=\"...\")`); managing which \
                   graphs exist is an operator action taken before the server starts, not \
                   something the agent it serves does.",
            http: Cell {
                evidence: Evidence { file: "api/openapi.yaml", needle: "/api/tenants:" },
                if_present: "Yes — `/api/tenants`, `/api/tenants/{id}`", if_absent: "No",
            },
            resp: Cell {
                evidence: Evidence { file: "src/protocol/command.rs", needle: "\"GRAPH.LIST\"" },
                if_present: "Yes — `GRAPH.LIST`, `GRAPH.DELETE`", if_absent: "No",
            },
            rust_sdk: Cell {
                evidence: Evidence { file: "crates/samyama-sdk/src/client.rs", needle: "async fn list_graphs" },
                if_present: "Yes — `.list_graphs()`, `.delete_graph()`", if_absent: "No",
            },
            python_sdk: Cell {
                evidence: Evidence { file: "sdk/python/src/lib.rs", needle: "fn list_graphs" },
                if_present: "Yes — `.list_graphs()`, `.delete_graph()`", if_absent: "No",
            },
            typescript_sdk: Cell {
                evidence: Evidence { file: "sdk/typescript/src/client.ts", needle: "async listGraphs(" },
                if_present: "Yes — `.listGraphs()`, `.deleteGraph()`", if_absent: "No",
            },
            mcp: Cell {
                evidence: Evidence { file: "sdk/python/samyama_mcp/server.py", needle: "def list_graphs" },
                if_present: "Yes", if_absent: "No — the server is bound to one graph at construction",
            },
        },
    ]
}

fn check(repo: &Path, ev: &Evidence) -> bool {
    fs::read_to_string(repo.join(ev.file))
        .map(|t| t.contains(ev.needle))
        .unwrap_or(false)
}

fn resolve(repo: &Path, cell: &Cell) -> (bool, String) {
    let present = check(repo, &cell.evidence);
    let text = if present {
        cell.if_present
    } else {
        cell.if_absent
    };
    (present, text.to_string())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out_path = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "docs/API-PARITY.md".to_string());
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let caps = capabilities();

    let mut md = String::new();
    md.push_str("# Feature parity matrix (API-02)\n\n");
    md.push_str("Version: 1.0.0\n\n");
    md.push_str(
        "**Generated, not hand-maintained.** Produced by \
         `cargo run --release --example feature_matrix`, which searches the \
         real source of each surface for specific evidence (an OpenAPI path, \
         a RESP command string, an SDK method, an MCP tool guard) and reports \
         what it found — the same file this generator is defined in has the \
         exact evidence string used for every cell. Regenerate after any \
         change to a surface; a cell that goes stale means the evidence \
         string changed shape (e.g. a method was renamed) and the generator's \
         needle needs updating to match, which is a smaller and more visible \
         failure than a hand-written table silently drifting.\n\n",
    );
    md.push_str(
        "**Not generated: whether a present capability actually works.** This \
         is a static probe over source text, not a live call through each \
         surface — a route that exists but is broken at runtime reads as \
         present here. `examples/api_contract.rs` catches OpenAPI/server drift \
         the same way for the HTTP surface specifically; nothing today drives \
         a live Cypher query through all six surfaces and compares the \
         answers. That would be the natural follow-up.\n\n",
    );
    md.push_str(&format!(
        "| Capability | {} |\n",
        SURFACE_HEADERS.join(" | ")
    ));
    md.push_str(&format!("|---|{}\n", "---|".repeat(SURFACE_HEADERS.len())));

    let mut notes: Vec<(&str, &str)> = Vec::new();
    let mut summary: Vec<(String, usize, usize, usize)> = Vec::new(); // title, yes, embedded/partial, no

    for cap in &caps {
        let cells = [
            resolve(&repo, &cap.http),
            resolve(&repo, &cap.resp),
            resolve(&repo, &cap.rust_sdk),
            resolve(&repo, &cap.python_sdk),
            resolve(&repo, &cap.typescript_sdk),
            resolve(&repo, &cap.mcp),
        ];
        let row: Vec<String> = cells.iter().map(|(_, t)| t.clone()).collect();
        md.push_str(&format!(
            "| **{}**<!-- {} --> | {} |\n",
            cap.title,
            cap.id,
            row.join(" | ")
        ));
        if !cap.note.is_empty() {
            notes.push((cap.title, cap.note));
        }
        let yes = cells
            .iter()
            .filter(|(p, t)| *p && !t.contains("Embedded-only") && !t.contains("Partial"))
            .count();
        let scoped = cells
            .iter()
            .filter(|(p, t)| *p && (t.contains("Embedded-only") || t.contains("Partial")))
            .count();
        let no = cells.len() - yes - scoped;
        summary.push((cap.title.to_string(), yes, scoped, no));
    }

    md.push_str(
        "\nLegend: plain **Yes** = a first-class method/endpoint/tool exists. \
                 **Embedded-only** = real, but only when the client is built in-process \
                 (`SamyamaClient.embedded()`), not against a remote server. **Via Cypher \
                 only** = reachable by sending Cypher through the surface's normal query \
                 call, not as a named method. **Partial** = some but not all of the \
                 capability's formats/operations are covered. **No — blocked by design** = \
                 deliberately absent, not missing.\n\n",
    );

    if !notes.is_empty() {
        md.push_str("## Notes\n\n");
        for (title, note) in &notes {
            md.push_str(&format!("**{title}.** {note}\n\n"));
        }
    }

    md.push_str("## Summary\n\n");
    md.push_str("| Capability | Full | Scoped/partial | Absent |\n|---|---|---|---|\n");
    for (title, yes, scoped, no) in &summary {
        md.push_str(&format!("| {title} | {yes} | {scoped} | {no} |\n"));
    }

    md.push_str(&format!(
        "\n---\n\n_{} capabilities x {} surfaces, generated from this commit's source \
         tree (not a live probe). Regenerate: `cargo run --release --example feature_matrix`._\n",
        caps.len(),
        SURFACE_HEADERS.len(),
    ));

    let dest = repo.join(&out_path);
    if let Some(parent) = dest.parent() {
        let _ = fs::create_dir_all(parent);
    }
    fs::write(&dest, &md).unwrap_or_else(|e| panic!("failed to write {}: {e}", dest.display()));
    println!(
        "wrote {} ({} capabilities x {} surfaces)",
        dest.display(),
        caps.len(),
        SURFACE_HEADERS.len()
    );
}
