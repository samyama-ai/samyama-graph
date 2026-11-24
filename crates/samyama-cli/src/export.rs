//! `samyama-cli snapshot export <file> [--include-agent-memory]` (#2242).
//!
//! Saves the server's graph as a `.sgsnap` through `POST /api/snapshot/export`.
//! Agent memory -- decisions, agent questions and tools, extraction telemetry --
//! is left out unless `--include-agent-memory` is given, because it can hold
//! personal data and an export is a file that is easy to pass on. A full backup
//! passes the flag, so its restore loses nothing.
//!
//! The file appears whole or not at all: the bytes go to `<file>.tmp`, are
//! synced, and replace `<file>` only once complete. What the export left out
//! comes back from the server in a header and is reported, so leaving memory
//! out is never silent.

use std::path::Path;
use std::time::Duration;

/// The header the server reports an export's losses in.
const DROPPED_HEADER: &str = "x-samyama-export-dropped";

/// How long to wait to connect. No overall timeout: a large graph
/// legitimately takes a while to export.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a stalled read may last.
const READ_TIMEOUT: Duration = Duration::from_secs(120);

/// One thing the export did not carry, as the server reports it.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq)]
pub struct Dropped {
    pub what: String,
    pub count: u64,
    pub detail: String,
}

/// What an export wrote.
#[derive(Debug, Clone, PartialEq)]
pub struct Exported {
    pub bytes: u64,
    pub dropped: Vec<Dropped>,
}

/// Export the server's graph to `output`.
pub async fn export_to(server: &str, output: &Path, include_agent_memory: bool) -> Result<Exported, String> {
    let http = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let url = format!(
        "{}/api/snapshot/export?include_agent_memory={include_agent_memory}",
        server.trim_end_matches('/')
    );
    let resp = http
        .post(&url)
        .send()
        .await
        .map_err(|e| format!("could not reach the server at {url}: {e}"))?;
    let status = resp.status();
    let dropped: Vec<Dropped> = resp
        .headers()
        .get(DROPPED_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| serde_json::from_str(v).ok())
        .unwrap_or_default();
    let body = resp
        .bytes()
        .await
        .map_err(|e| format!("could not read the snapshot from the server: {e}"))?;
    if !status.is_success() {
        let why = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
            .unwrap_or_else(|| String::from_utf8_lossy(&body).into_owned());
        return Err(format!("the server refused the export ({status}): {why}"));
    }
    write_whole(output, &body)?;
    Ok(Exported { bytes: body.len() as u64, dropped })
}

/// Write `bytes` to `path` through `<path>.tmp`, so a failure leaves whatever
/// was at `path` before and never a partial snapshot.
fn write_whole(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    let written = (|| -> std::io::Result<()> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    written.map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("could not write {}: {e}", path.display())
    })
}

/// The CLI command: export, then report what was written and left out.
pub async fn run_export(server: &str, output: &Path, include_agent_memory: bool, json: bool) -> Result<(), String> {
    let done = export_to(server, output, include_agent_memory).await?;
    if json {
        let body = serde_json::json!({
            "path": output.display().to_string(),
            "bytes": done.bytes,
            "include_agent_memory": include_agent_memory,
            "dropped": done.dropped,
        });
        println!("{}", serde_json::to_string_pretty(&body).map_err(|e| e.to_string())?);
        return Ok(());
    }
    println!("wrote {} bytes to {}", done.bytes, output.display());
    for d in &done.dropped {
        println!("  left out: {} {} -- {}", d.count, d.what, d.detail);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use samyama::graph::GraphStore;
    use samyama::http::HttpServer;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    /// An engine holding a data node joined to an agent decision.
    async fn engine_with_memory() -> String {
        let mut store = GraphStore::new();
        let data = store.create_node("Drug");
        let memory = store.create_node("AgentDecision");
        store.create_edge(memory, data, "ABOUT").unwrap();
        let app = HttpServer::new(Arc::new(RwLock::new(store)), 0).router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn dir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("samyama-cli-export-{}-{}", std::process::id(), rand_suffix()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rand_suffix() -> u128 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    }

    #[tokio::test]
    async fn a_default_export_leaves_memory_out_and_says_so() {
        let url = engine_with_memory().await;
        let path = dir().join("kg.sgsnap");
        let done = export_to(&url, &path, false).await.unwrap();
        assert!(done.bytes > 0 && path.exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), done.bytes);
        let row = |what: &str| done.dropped.iter().find(|d| d.what == what).map(|d| d.count);
        assert_eq!(row("agent_memory_nodes"), Some(1), "{:?}", done.dropped);
        assert_eq!(row("agent_memory_edges"), Some(1), "{:?}", done.dropped);
        assert!(!std::path::Path::new(&format!("{}.tmp", path.display())).exists(), "a temporary file was left");
    }

    #[tokio::test]
    async fn with_the_flag_memory_is_written_and_nothing_is_reported_left_out() {
        let url = engine_with_memory().await;
        let path = dir().join("full.sgsnap");
        let done = export_to(&url, &path, true).await.unwrap();
        assert!(!done.dropped.iter().any(|d| d.what.starts_with("agent_memory")), "{:?}", done.dropped);
    }

    #[tokio::test]
    async fn an_unreachable_server_is_an_error_and_writes_nothing() {
        let path = dir().join("none.sgsnap");
        // Port 9 (discard) on localhost: nothing listens there in a test.
        let err = export_to("http://127.0.0.1:9", &path, false).await.unwrap_err();
        assert!(err.contains("could not reach the server"), "{err}");
        assert!(!path.exists());
    }

    #[test]
    fn a_failed_write_leaves_the_old_file_and_no_temporary_one() {
        let d = dir();
        let path = d.join("kept.sgsnap");
        std::fs::write(&path, b"old").unwrap();
        // A directory where the temporary file should go: the write fails.
        std::fs::create_dir_all(d.join("kept.sgsnap.tmp")).unwrap();
        assert!(write_whole(&path, b"new").is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"old");
    }
}
