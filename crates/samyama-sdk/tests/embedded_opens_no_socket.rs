//! Embedded mode is "no server, no socket" (API-05, #1734), and this checks
//! the second half from inside the process instead of taking it on trust.
//!
//! The check counts the process's open file descriptors that are sockets,
//! before any embedded work and after each step of a workload that touches
//! every embedded surface the README documents: writes, reads, a unique-key
//! lookup, a graph algorithm, a vector index, `status` and `ping`. A step that
//! opened a listener or a connection and kept it would leave one more socket
//! behind than the process started with.
//!
//! The count is relative to a baseline, not zero, and the baseline is taken
//! once the tokio runtime is running, because neither is the engine's doing:
//! a process may inherit a socket (a test runner or terminal can hand one over
//! as stdio), and tokio's runtime opens a connected pair of local sockets to
//! receive Unix signals. Measured on macOS with `lsof`: stdin plus three Unix
//! sockets wired to one another, and no internet socket, before an
//! `EmbeddedClient` existed. That runtime is the caller's -- their
//! `#[tokio::main]` -- so it is not counted against embedded mode.
//!
//! And before trusting a count of "no new sockets",
//! the probe is shown to see one -- a TCP listener opened on purpose -- so a
//! probe that silently counts nothing cannot pass this test.
//!
//! What it does not see: a socket opened and closed between two checks. The
//! claim checked is that embedded mode leaves no listener and holds no
//! connection, which is what "no server" means to a caller.
//!
//! NLQ is left out on purpose: it sends the question to the configured LLM
//! provider, which is a network call the caller asked for.
//!
//! One test in its own file, so no other test runs in this process and opens
//! a socket of its own while the counts are taken.
#![cfg(unix)]

use std::os::unix::fs::FileTypeExt;
use std::path::Path;

use samyama_sdk::{
    AlgorithmClient, DistanceMetric, EmbeddedClient, PageRankConfig, SamyamaClient, VectorClient,
};

/// Open file descriptors of this process that are sockets.
fn open_sockets() -> usize {
    // Linux lists them under /proc/self/fd; macOS and the BSDs under /dev/fd.
    let dir = ["/proc/self/fd", "/dev/fd"]
        .into_iter()
        .find(|d| Path::new(d).is_dir())
        .expect("no /proc/self/fd or /dev/fd to read open descriptors from");
    std::fs::read_dir(dir)
        .expect("descriptor directory unreadable")
        .filter_map(|entry| std::fs::metadata(entry.ok()?.path()).ok())
        .filter(|meta| meta.file_type().is_socket())
        .count()
}

#[test]
fn embedded_mode_leaves_no_socket_open() {
    // The runtime a user's `#[tokio::main]` would give them, started before
    // the baseline: its signal sockets are the caller's, not the engine's.
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let before = open_sockets();

    // The probe sees a socket when there is one: without this, a probe that
    // matched nothing would make the assertions below vacuous.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    assert_eq!(open_sockets(), before + 1, "the probe did not see a TCP listener");
    drop(listener);
    assert_eq!(open_sockets(), before, "the probe still counts a closed listener");

    runtime.block_on(async {
        let mut steps = Vec::new();
        let mut check = |step: &str| steps.push((step.to_string(), open_sockets()));

        let client = EmbeddedClient::new();
        check("EmbeddedClient::new");

        client
            .query("default", "CREATE CONSTRAINT FOR (p:Person) REQUIRE p.name IS UNIQUE")
            .await
            .unwrap();
        client
            .query(
                "default",
                "CREATE (a:Person {name: 'Ann'})-[:KNOWS]->(b:Person {name: 'Bo'}), \
                 (b)-[:KNOWS]->(c:Person {name: 'Cy'}), (c)-[:KNOWS]->(a)",
            )
            .await
            .unwrap();
        check("query (writes)");

        let rows = client
            .query_readonly("default", "MATCH (p:Person) RETURN p.name ORDER BY p.name")
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        check("query_readonly");

        let ann = client.find_node_by_unique("default", "Person", "name", "Ann").await.unwrap();
        assert!(ann.is_some());
        check("find_node_by_unique");

        let ranks = client.page_rank(PageRankConfig::default(), Some("Person"), None).await;
        assert_eq!(ranks.len(), 3);
        check("page_rank");

        client.create_vector_index("Person", "embedding", 2, DistanceMetric::Cosine).await.unwrap();
        client.add_vector("Person", "embedding", ann.unwrap(), &[1.0, 0.0]).await.unwrap();
        let hits = client.vector_search("Person", "embedding", &[1.0, 0.1], 1).await.unwrap();
        assert_eq!(hits.len(), 1);
        check("vector index, insert, search");

        client.status().await.unwrap();
        client.ping().await.unwrap();
        check("status, ping");

        drop(client);
        check("client dropped");

        let opened: Vec<String> = steps
            .iter()
            .filter(|(_, n)| *n != before)
            .map(|(step, n)| format!("after {step}: {n} sockets open, {before} before the client"))
            .collect();
        assert!(opened.is_empty(), "embedded mode opened a socket:\n{}", opened.join("\n"));
    });
}
