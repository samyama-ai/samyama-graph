//! `load <kg>`: fetch a published knowledge-graph snapshot and load it into a
//! running server in one command (KG-03, #1759).
//!
//! Before this, loading a KG meant finding the right release link in
//! `docs/KG-CATALOG.md`, downloading it, and POSTing it to the import endpoint
//! by hand. This does those three steps and checks the file on the way.
//!
//! Three things it will not do, each because getting it wrong costs more than
//! the convenience is worth:
//!
//! * It does not start a server. It loads into the one `--url` names, and says
//!   so before downloading anything if there is none (a 745 MB download that
//!   then has nowhere to go is the worst version of this failure).
//! * It does not trust the download. Every file is checked against the SHA-256
//!   and size recorded in [`KGS`], and a file that does not match is deleted and
//!   never sent to the server.
//! * It does not add to a graph that already holds data unless told to. An
//!   import appends, so running `load cricket` twice would otherwise double
//!   every node without a word.

use samyama_sdk::{RemoteClient, SamyamaClient};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

/// Where the published snapshots live: `<base>/<release tag>/<file>`.
pub const DEFAULT_SOURCE: &str = "https://github.com/samyama-ai/samyama-graph/releases/download";

/// One published snapshot.
#[derive(Debug)]
pub struct Kg {
    /// The name to type: `samyama-cli load cricket`.
    pub name: &'static str,
    /// Other names the same KG goes by (its repository name, mostly).
    pub also: &'static [&'static str],
    /// The release the file is attached to.
    pub tag: &'static str,
    /// The asset's file name.
    pub file: &'static str,
    /// Its size in bytes, to catch a truncated download before hashing.
    pub bytes: u64,
    /// Its SHA-256, as GitHub records it for the release asset.
    pub sha256: &'static str,
}

/// Every KG that `docs/KG-CATALOG.md` publishes as a snapshot. A test keeps the
/// two in step: a link added to the catalog without a row here fails.
pub const KGS: &[Kg] = &[
    Kg {
        name: "clinical-trials",
        also: &[],
        tag: "kg-snapshots-v5",
        file: "clinical-trials.sgsnap",
        bytes: 745661023,
        sha256: "6e5d80b66f78c20b278f551245922c2bd88fc87b494bdf68dde56441463b5f19",
    },
    Kg {
        name: "cricket",
        also: &[],
        tag: "kg-snapshots-v1",
        file: "cricket.sgsnap",
        bytes: 21694993,
        sha256: "b6633d55be58e1943eb9c5086ed9aa4562b55f0579b90d3ec97c5cb387786b6e",
    },
    Kg {
        name: "dbms-research",
        also: &[],
        tag: "kg-snapshots-v10",
        file: "dbms-research.sgsnap",
        bytes: 12760846,
        sha256: "82119412c26c0b0b186b0edab36d28a1c6632ec8b47905723fa7b9985880b155",
    },
    Kg {
        name: "druginteractions",
        also: &[],
        tag: "kg-snapshots-v5",
        file: "druginteractions.sgsnap",
        bytes: 8472730,
        sha256: "9f36fe1d263f2fe75cebcfcb08bf4606d6701b316f1d43ca131e5fa93b9f42bd",
    },
    Kg {
        name: "edge-ai",
        also: &[],
        tag: "kg-snapshots-v9",
        file: "edge-ai-kg.sgsnap",
        bytes: 992131,
        sha256: "de234fd0d09ba6e5c551b1df69becdc7ec93eeceba8407e1b5ba990853c9196b",
    },
    Kg {
        name: "football",
        also: &[],
        tag: "kg-snapshots-v8",
        file: "football.sgsnap",
        bytes: 451532,
        sha256: "090ebc8ec6232ceaee18834527b80b46c9e1a260601d1ff034dbd2130d1b519c",
    },
    Kg {
        name: "health-determinants",
        also: &[],
        tag: "kg-snapshots-v6",
        file: "health-determinants.sgsnap",
        bytes: 5608277,
        sha256: "b94a0eba1efd6bda5cac84c053f110a7ba79ac5359af9cce1a0b9c2aa431ce94",
    },
    Kg {
        name: "health-systems",
        also: &[],
        tag: "kg-snapshots-v6",
        file: "health-systems.sgsnap",
        bytes: 207197,
        sha256: "d99259fe66679ddaac6426c2c60c63148496133d30c930c9a7d5032d91154700",
    },
    Kg {
        name: "imdb",
        also: &[],
        tag: "kg-snapshots-v8",
        file: "imdb.sgsnap",
        bytes: 52268789,
        sha256: "13a91acd4bb1d5a4d9729a9d883d8f66cd86a9ee2bca19483e527245086445df",
    },
    Kg {
        name: "legal-judgments",
        also: &["legal-judgments-graph"],
        tag: "kg-snapshots-v1",
        file: "legal-judgments.sgsnap",
        bytes: 146179,
        sha256: "e021103aea367380ec2b14a32e42aeb17a2ed05e973d7ad85fe61639c1f58172",
    },
    Kg {
        name: "pathways",
        also: &[],
        tag: "kg-snapshots-v3",
        file: "pathways.sgsnap",
        bytes: 9569818,
        sha256: "e4511f58513727590dff7119ac1317647326b33ea56fb84b0671e1031c92d7f3",
    },
    Kg {
        name: "surveillance",
        also: &[],
        tag: "kg-snapshots-v4",
        file: "surveillance.sgsnap",
        bytes: 5939301,
        sha256: "1caf6b4a7d51d9405b01cc4a56e0b1329ee3c6a3cecfb270a4bf7762a33de005",
    },
];

/// What a name is compared as: case, `-` and `_` do not matter and a trailing
/// `kg` is dropped, so `Cricket-KG`, `cricket_kg` and `cricket` are one name.
fn key(s: &str) -> String {
    let k: String = s
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|c| *c != '-' && *c != '_')
        .collect();
    match k.strip_suffix("kg") {
        Some(rest) if !rest.is_empty() => rest.to_string(),
        _ => k,
    }
}

/// Look a KG up by the name a person would type.
pub fn find(query: &str) -> Result<&'static Kg, String> {
    let want = key(query);
    KGS.iter()
        .find(|kg| key(kg.name) == want || kg.also.iter().any(|a| key(a) == want))
        .ok_or_else(|| {
            let names: Vec<&str> = KGS.iter().map(|k| k.name).collect();
            format!(
                "no published snapshot named `{query}`. Available: {}. \
                 (Other KGs in docs/KG-CATALOG.md are built from source, not published as snapshots.)",
                names.join(", ")
            )
        })
}

/// The URL a snapshot is fetched from.
pub fn url_for(kg: &Kg, source: &str) -> String {
    format!("{}/{}/{}", source.trim_end_matches('/'), kg.tag, kg.file)
}

/// What a load did, for the report.
#[derive(Debug)]
pub struct Loaded {
    pub nodes: u64,
    pub edges: u64,
    pub bytes: u64,
    pub download: Duration,
    pub import: Duration,
}

/// Deletes the downloaded file however `load_into` ends, so a failed load
/// does not leave a large file behind.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn scratch_path(dir: &Path, kg: &Kg) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    dir.join(format!(
        "samyama-load-{}-{nanos}-{}",
        std::process::id(),
        kg.file
    ))
}

/// Fetch `url` into `dest`, then check its size and SHA-256 against `kg`.
/// On any mismatch `dest` is left for the caller's [`Scratch`] to delete.
async fn download(
    http: &reqwest::Client,
    url: &str,
    kg: &Kg,
    dest: &Path,
    say: &dyn Fn(&str),
) -> Result<(), String> {
    let mut resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| format!("could not fetch {url}: {e}"))?;
    if !resp.status().is_success() {
        let hint = if resp.status() == reqwest::StatusCode::NOT_FOUND {
            " The release asset is not there; this build's table may be older than the release."
        } else {
            ""
        };
        return Err(format!("{url} answered {}.{hint}", resp.status()));
    }

    // `create_new`: never write through a file or link that is already there.
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)
        .await
        .map_err(|e| format!("could not create {}: {e}", dest.display()))?;
    let mut hasher = Sha256::new();
    let mut got: u64 = 0;
    let mut next_mark: u64 = 25;
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| format!("download of {url} broke off after {got} bytes: {e}"))?
    {
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|e| format!("could not write {}: {e}", dest.display()))?;
        got += chunk.len() as u64;
        // A 745 MB file is minutes of silence otherwise.
        if kg.bytes >= 50_000_000 {
            while kg.bytes > 0 && got * 100 / kg.bytes >= next_mark && next_mark < 100 {
                say(&format!("  downloaded {next_mark}%"));
                next_mark += 25;
            }
        }
    }
    file.flush()
        .await
        .map_err(|e| format!("could not write {}: {e}", dest.display()))?;

    if got != kg.bytes {
        return Err(format!(
            "the download is {got} bytes but {} is {} bytes; refusing to load it",
            kg.file, kg.bytes
        ));
    }
    let digest = format!("{:x}", hasher.finalize());
    if digest != kg.sha256 {
        return Err(format!(
            "checksum mismatch for {}: expected sha256 {}, got {digest}; refusing to load it",
            kg.file, kg.sha256
        ));
    }
    Ok(())
}

/// What to do about a refusal, when there is something to do.
///
/// A server started with no flags allows 1M nodes and 10M edges, and two of the
/// published KGs are larger (clinical-trials, 7.8M nodes / 27M edges; imdb,
/// 1.9M nodes). The server's own message names the limit but not the flag that
/// raises it, so a reader who ran one command is left to find it.
fn refusal_hint(status: reqwest::StatusCode, why: &str) -> &'static str {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS && why.contains("quota exceeded") {
        " This KG is larger than the server's quota. Restart the server with higher \
         limits, e.g. `samyama --max-nodes unlimited --max-edges unlimited \
         --max-memory-bytes unlimited`, and run this again."
    } else {
        ""
    }
}

/// POST the file to the server's import and read back its counts.
async fn upload(
    http: &reqwest::Client,
    server: &str,
    path: &Path,
    name: &str,
    len: u64,
) -> Result<(u64, u64), String> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|e| format!("could not open {}: {e}", path.display()))?;
    let part = reqwest::multipart::Part::stream_with_length(file, len)
        .file_name(name.to_string())
        .mime_str("application/octet-stream")
        .map_err(|e| e.to_string())?;
    let form = reqwest::multipart::Form::new().part("file", part);
    let url = format!("{}/api/snapshot/import", server.trim_end_matches('/'));
    let resp = http
        .post(&url)
        .multipart(form)
        .send()
        .await
        .map_err(|e| format!("could not send the snapshot to {url}: {e}"))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| format!("could not read the server's reply: {e}"))?;
    let body: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    if !status.is_success() {
        let why = body
            .get("error")
            .and_then(|e| e.as_str())
            .map(str::to_string)
            .unwrap_or(text);
        return Err(format!(
            "the server refused the snapshot ({status}): {why}{}",
            refusal_hint(status, &why)
        ));
    }
    let count = |k: &str| {
        body.get(k)
            .and_then(|v| v.as_u64())
            .ok_or_else(|| format!("the server's reply has no `{k}`: {text}"))
    };
    Ok((count("nodes_imported")?, count("edges_imported")?))
}

/// The whole load. `say` receives progress lines (the CLI sends them to stderr,
/// so `--format json` output on stdout stays parseable).
pub async fn load_into(
    kg: &Kg,
    source: &str,
    server: &str,
    work_dir: &Path,
    force: bool,
    say: &dyn Fn(&str),
) -> Result<Loaded, String> {
    // Ask the server first. This is both the "is anything there" check and the
    // "is it empty" check, in one call, before a byte is downloaded.
    let status = RemoteClient::new(server).status().await.map_err(|e| {
        format!(
            "no Samyama server answered at {server}: {e}. Start one, or point --url at it; \
             nothing was downloaded."
        )
    })?;
    if status.storage.nodes > 0 && !force {
        return Err(format!(
            "the server at {server} already holds {} nodes. An import adds to what is there, \
             so loading {} now would mix the two. Use an empty server, or pass --force to add anyway; \
             nothing was downloaded.",
            status.storage.nodes, kg.name
        ));
    }

    // No overall timeout: a large file legitimately takes minutes either way.
    // A stalled connection is still caught by the read timeout.
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .read_timeout(Duration::from_secs(120))
        .build()
        .map_err(|e| e.to_string())?;

    let url = url_for(kg, source);
    let scratch = Scratch(scratch_path(work_dir, kg));
    say(&format!(
        "Downloading {} ({} bytes) from {url}",
        kg.name, kg.bytes
    ));
    let t = Instant::now();
    download(&http, &url, kg, &scratch.0, say).await?;
    let download_time = t.elapsed();
    say(&format!(
        "Checked sha256 {}…; sending it to {server}",
        &kg.sha256[..12]
    ));

    let t = Instant::now();
    let (nodes, edges) = upload(&http, server, &scratch.0, kg.file, kg.bytes).await?;
    Ok(Loaded {
        nodes,
        edges,
        bytes: kg.bytes,
        download: download_time,
        import: t.elapsed(),
    })
}

/// `samyama-cli load --list`.
pub fn run_list(json: bool) -> Result<(), Box<dyn std::error::Error>> {
    if json {
        let rows: Vec<_> = KGS
            .iter()
            .map(|k| {
                serde_json::json!({
                    "name": k.name, "release": k.tag, "file": k.file,
                    "bytes": k.bytes, "sha256": k.sha256,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        for k in KGS {
            println!("{:<22} {:>12} bytes  {}", k.name, k.bytes, k.tag);
        }
    }
    Ok(())
}

/// `samyama-cli load <kg>`.
pub async fn run_load(
    name: &str,
    server: &str,
    source: &str,
    force: bool,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let kg = find(name)?;
    let started = Instant::now();
    let say = |line: &str| eprintln!("{line}");
    let loaded = load_into(kg, source, server, &std::env::temp_dir(), force, &say).await?;
    let total = started.elapsed();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kg": kg.name,
                "nodes_imported": loaded.nodes,
                "edges_imported": loaded.edges,
                "bytes": loaded.bytes,
                "download_seconds": loaded.download.as_secs_f64(),
                "import_seconds": loaded.import.as_secs_f64(),
                "total_seconds": total.as_secs_f64(),
            }))?
        );
    } else {
        println!(
            "Loaded {}: {} nodes, {} edges in {:.1}s (download {:.1}s, import {:.1}s)",
            kg.name,
            loaded.nodes,
            loaded.edges,
            total.as_secs_f64(),
            loaded.download.as_secs_f64(),
            loaded.import.as_secs_f64()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // ---- the table ----

    #[test]
    fn the_table_is_well_formed() {
        let mut seen = std::collections::HashSet::new();
        for kg in KGS {
            assert!(
                seen.insert(key(kg.name)),
                "two rows answer to `{}`",
                kg.name
            );
            for a in kg.also {
                assert!(seen.insert(key(a)), "alias `{a}` clashes with another name");
            }
            assert_eq!(
                kg.sha256.len(),
                64,
                "{}: sha256 is not 64 characters",
                kg.name
            );
            assert!(
                kg.sha256
                    .chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "{}: sha256 must be lowercase hex",
                kg.name
            );
            assert!(kg.file.ends_with(".sgsnap"), "{}", kg.name);
            assert!(kg.tag.starts_with("kg-snapshots-v"), "{}", kg.name);
            assert!(kg.bytes > 0, "{}", kg.name);
        }
    }

    #[test]
    fn the_table_matches_the_published_catalog() {
        // docs/KG-CATALOG.md is where a person finds a KG. If it links a
        // snapshot this table lacks, `load` would say "no such KG" for
        // something the docs advertise; if the table names a file the docs
        // do not, the two have drifted.
        let catalog = include_str!("../../../docs/KG-CATALOG.md");
        let marker = "releases/download/";
        let mut linked = std::collections::BTreeSet::new();
        let mut rest = catalog;
        while let Some(i) = rest.find(marker) {
            rest = &rest[i + marker.len()..];
            let end = rest
                .find(|c: char| c == ')' || c.is_whitespace())
                .unwrap_or(rest.len());
            linked.insert(rest[..end].to_string());
        }
        let tabled: std::collections::BTreeSet<String> = KGS
            .iter()
            .map(|k| format!("{}/{}", k.tag, k.file))
            .collect();
        assert_eq!(
            linked, tabled,
            "docs/KG-CATALOG.md and the load table list different snapshots"
        );
    }

    #[test]
    fn names_resolve_the_way_a_person_types_them() {
        for q in [
            "cricket",
            "Cricket",
            "cricket-kg",
            "CRICKET_KG",
            " cricket ",
        ] {
            assert_eq!(find(q).unwrap().name, "cricket", "{q}");
        }
        assert_eq!(find("clinicaltrials-kg").unwrap().name, "clinical-trials");
        assert_eq!(find("clinical_trials").unwrap().name, "clinical-trials");
        assert_eq!(find("edge-ai-kg").unwrap().name, "edge-ai");
        assert_eq!(
            find("legal-judgments-graph-kg").unwrap().name,
            "legal-judgments"
        );
    }

    #[test]
    fn an_unknown_name_lists_what_exists() {
        let e = find("pubmed").unwrap_err();
        assert!(e.contains("no published snapshot named `pubmed`"), "{e}");
        assert!(e.contains("cricket") && e.contains("surveillance"), "{e}");
        assert!(find("").is_err());
        assert!(find("kg").is_err());
    }

    #[test]
    fn the_url_is_base_then_tag_then_file() {
        let kg = find("cricket").unwrap();
        assert_eq!(
            url_for(kg, DEFAULT_SOURCE),
            "https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v1/cricket.sgsnap"
        );
        assert_eq!(
            url_for(kg, "http://x/y/"),
            "http://x/y/kg-snapshots-v1/cricket.sgsnap"
        );
    }

    #[test]
    fn a_quota_refusal_says_how_to_raise_the_quota() {
        // The server's exact wording for clinical-trials on a default server.
        let why = "quota exceeded: nodes (7779702/1000000)";
        let hint = refusal_hint(reqwest::StatusCode::TOO_MANY_REQUESTS, why);
        assert!(hint.contains("--max-nodes unlimited"), "{hint}");
        assert!(hint.contains("--max-edges unlimited"), "{hint}");
        // Any other refusal is reported as the server worded it, with nothing added.
        assert_eq!(
            refusal_hint(reqwest::StatusCode::BAD_REQUEST, "bad magic"),
            ""
        );
        assert_eq!(
            refusal_hint(reqwest::StatusCode::TOO_MANY_REQUESTS, "slow down"),
            ""
        );
    }

    // ---- end to end, against the engine's own router ----

    use samyama::graph::GraphStore;
    use samyama::http::HttpServer;
    use tokio::sync::RwLock;

    async fn spawn_engine() -> String {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let app = HttpServer::new(store, 0).router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    /// A real snapshot: three nodes and two edges, exported by an engine.
    async fn make_snapshot() -> Vec<u8> {
        let url = spawn_engine().await;
        let c = RemoteClient::new(&url);
        c.query(
            "default",
            "CREATE (a:Person {name: 'a'})-[:KNOWS]->(b:Person {name: 'b'}), (b)-[:KNOWS]->(c:Person {name: 'c'})",
        )
        .await
        .unwrap();
        reqwest::Client::new()
            .post(format!("{url}/api/snapshot/export"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec()
    }

    /// Serves `bytes` at `/<tag>/<file>`; counts every request it gets.
    async fn spawn_source(bytes: Vec<u8>, kg: &Kg) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let path = format!("/{}/{}", kg.tag, kg.file);
        let h = hits.clone();
        let app = axum::Router::new().route(
            &path,
            axum::routing::get(move || {
                let h = h.clone();
                let bytes = bytes.clone();
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                    bytes
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), hits)
    }

    fn leak(s: String) -> &'static str {
        Box::leak(s.into_boxed_str())
    }

    /// A table row describing exactly these bytes.
    fn entry_for(bytes: &[u8]) -> Kg {
        Kg {
            name: "demo",
            also: &[],
            tag: "kg-snapshots-vtest",
            file: "demo.sgsnap",
            bytes: bytes.len() as u64,
            sha256: leak(format!("{:x}", Sha256::digest(bytes))),
        }
    }

    struct WorkDir(PathBuf);
    impl WorkDir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir()
                .join(format!("samyama-load-test-{}-{tag}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            WorkDir(p)
        }
        fn is_empty(&self) -> bool {
            std::fs::read_dir(&self.0).unwrap().next().is_none()
        }
    }
    impl Drop for WorkDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn quiet(_: &str) {}

    #[tokio::test]
    async fn loads_a_snapshot_and_the_server_then_holds_it() {
        let snap = make_snapshot().await;
        let kg = entry_for(&snap);
        let (source, hits) = spawn_source(snap, &kg).await;
        let server = spawn_engine().await;
        let work = WorkDir::new("ok");

        let r = load_into(&kg, &source, &server, &work.0, false, &quiet)
            .await
            .unwrap();

        assert_eq!((r.nodes, r.edges), (3, 2));
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let s = RemoteClient::new(&server).status().await.unwrap();
        assert_eq!(
            (s.storage.nodes, s.storage.edges),
            (3, 2),
            "the server does not hold what was reported"
        );
        assert!(work.is_empty(), "the downloaded file was left behind");
    }

    #[tokio::test]
    async fn a_file_that_fails_its_checksum_is_never_sent_and_is_deleted() {
        let snap = make_snapshot().await;
        let kg = entry_for(&snap);
        // Same length, one byte different: only the hash can tell.
        let mut tampered = snap.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0xff;
        let (source, _) = spawn_source(tampered, &kg).await;
        let server = spawn_engine().await;
        let work = WorkDir::new("tamper");

        let e = load_into(&kg, &source, &server, &work.0, false, &quiet)
            .await
            .unwrap_err();

        assert!(e.contains("checksum mismatch"), "{e}");
        let s = RemoteClient::new(&server).status().await.unwrap();
        assert_eq!(
            s.storage.nodes, 0,
            "a file that failed its checksum reached the server"
        );
        assert!(work.is_empty(), "the bad file was left behind");
    }

    #[tokio::test]
    async fn a_truncated_download_is_refused() {
        let snap = make_snapshot().await;
        let kg = entry_for(&snap);
        let (source, _) = spawn_source(snap[..snap.len() / 2].to_vec(), &kg).await;
        let server = spawn_engine().await;
        let work = WorkDir::new("short");

        let e = load_into(&kg, &source, &server, &work.0, false, &quiet)
            .await
            .unwrap_err();

        assert!(e.contains("bytes but"), "{e}");
        assert_eq!(
            RemoteClient::new(&server)
                .status()
                .await
                .unwrap()
                .storage
                .nodes,
            0
        );
        assert!(work.is_empty());
    }

    #[tokio::test]
    async fn a_missing_release_asset_says_so() {
        let snap = make_snapshot().await;
        let kg = entry_for(&snap);
        // The source serves a different path, so ours is a 404.
        let other = Kg {
            file: "other.sgsnap",
            ..entry_for(&snap)
        };
        let (source, _) = spawn_source(snap, &other).await;
        let server = spawn_engine().await;
        let work = WorkDir::new("404");

        let e = load_into(&kg, &source, &server, &work.0, false, &quiet)
            .await
            .unwrap_err();

        assert!(e.contains("404"), "{e}");
        assert!(work.is_empty());
    }

    #[tokio::test]
    async fn no_server_means_nothing_is_downloaded() {
        let snap = make_snapshot().await;
        let kg = entry_for(&snap);
        let (source, hits) = spawn_source(snap, &kg).await;
        let work = WorkDir::new("noserver");

        // Nothing listens on port 1.
        let e = load_into(&kg, &source, "http://127.0.0.1:1", &work.0, false, &quiet)
            .await
            .unwrap_err();

        assert!(e.contains("no Samyama server answered"), "{e}");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "it downloaded before checking the server"
        );
        assert!(work.is_empty());
    }

    #[tokio::test]
    async fn a_server_that_already_holds_data_is_not_added_to_unless_forced() {
        let snap = make_snapshot().await;
        let kg = entry_for(&snap);
        let (source, hits) = spawn_source(snap, &kg).await;
        let server = spawn_engine().await;
        RemoteClient::new(&server)
            .query("default", "CREATE (:Person {name: 'already here'})")
            .await
            .unwrap();
        let work = WorkDir::new("force");

        let e = load_into(&kg, &source, &server, &work.0, false, &quiet)
            .await
            .unwrap_err();
        assert!(e.contains("already holds 1 nodes"), "{e}");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "it downloaded before refusing"
        );

        let r = load_into(&kg, &source, &server, &work.0, true, &quiet)
            .await
            .unwrap();
        assert_eq!(r.nodes, 3);
        assert_eq!(
            RemoteClient::new(&server)
                .status()
                .await
                .unwrap()
                .storage
                .nodes,
            4
        );
    }

    #[tokio::test]
    async fn a_file_the_server_rejects_reports_why_and_is_deleted() {
        // The hash matches (the row is built from these bytes), so only the
        // server can tell that this is not a snapshot.
        let junk = b"this is not a snapshot".to_vec();
        let kg = entry_for(&junk);
        let (source, _) = spawn_source(junk, &kg).await;
        let server = spawn_engine().await;
        let work = WorkDir::new("junk");

        let e = load_into(&kg, &source, &server, &work.0, false, &quiet)
            .await
            .unwrap_err();

        assert!(e.contains("the server refused the snapshot"), "{e}");
        assert!(work.is_empty());
    }
}
