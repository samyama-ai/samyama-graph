//! Immutable, checksummed generations. CURRENT is the atomic publication point.
//! These files still contain vector-list dumps, not HNSW graph topology.
use super::{VectorError, VectorResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::{Component, Path},
};

#[derive(Serialize, Deserialize)]
struct Manifest {
    version: u32,
    files: BTreeMap<String, String>,
}
fn bad(message: impl Into<String>) -> VectorError {
    VectorError::IndexError(message.into())
}
fn filename(s: &str) -> VectorResult<()> {
    let mut parts = Path::new(s).components();
    if !matches!(parts.next(), Some(Component::Normal(_))) || parts.next().is_some() {
        return Err(bad("checkpoint path must be one filename"));
    }
    Ok(())
}
fn digest(path: &Path) -> VectorResult<String> {
    let mut input = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
/// Sync every dump and its manifest before the parent publishes the generation.
pub(super) fn seal(path: &Path) -> VectorResult<()> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| bad("non-UTF8 checkpoint filename"))?;
        filename(&name)?;
        files.insert(name, digest(&entry.path())?);
        std::fs::File::open(entry.path())?.sync_all()?;
    }
    let mut out = std::fs::File::create(path.join("manifest.json"))?;
    serde_json::to_writer(&mut out, &Manifest { version: 1, files })
        .map_err(|e| bad(e.to_string()))?;
    out.flush()?;
    out.sync_all()?;
    // Unix only -- same reason as the sibling fsync in `manager`, and as
    // `snapshot::persist` (#1696). The per-file fsync in the loop above is
    // a *file* and is fine on both platforms; this one is the directory.
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}
/// Validate all bytes and metadata before any live index is replaced.
pub(super) fn verify(path: &Path) -> VectorResult<()> {
    let manifest: Manifest =
        serde_json::from_reader(std::fs::File::open(path.join("manifest.json"))?)
            .map_err(|e| bad(e.to_string()))?;
    if manifest.version != 1 {
        return Err(bad("unsupported vector checkpoint version"));
    }
    if !manifest.files.contains_key("metadata.json") {
        return Err(bad("checkpoint has no metadata"));
    }
    for (name, expected) in &manifest.files {
        filename(name)?;
        if digest(&path.join(name))? != *expected {
            return Err(bad(format!("vector checkpoint checksum mismatch: {name}")));
        }
    }
    let metadata: Vec<serde_json::Value> =
        serde_json::from_reader(std::fs::File::open(path.join("metadata.json"))?)
            .map_err(|e| bad(e.to_string()))?;
    for item in metadata {
        for field in ["label", "property_key", "filename"] {
            if item[field].as_str().is_none() {
                return Err(bad(format!("missing checkpoint field {field}")));
            }
        }
        if item["dimensions"].as_u64().filter(|n| *n > 0).is_none() {
            return Err(bad("invalid checkpoint dimensions"));
        }
        let file = item["filename"]
            .as_str()
            .ok_or_else(|| bad("missing filename"))?;
        filename(file)?;
        if !manifest.files.contains_key(file) {
            return Err(bad("index file absent from manifest"));
        }
    }
    Ok(())
}
pub(super) fn generation(root: &Path) -> VectorResult<Option<std::path::PathBuf>> {
    let current = root.join("CURRENT");
    match std::fs::read_to_string(current) {
        Ok(name) => {
            filename(name.trim())?;
            Ok(Some(root.join(name.trim())))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
