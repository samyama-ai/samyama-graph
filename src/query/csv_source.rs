//! Where `LOAD CSV` is allowed to read from (LANG-09).
//!
//! `LOAD CSV FROM 'file:///etc/passwd'` on a server reachable over the network is an
//! arbitrary local file read for anyone who can send a query. So this is a gate and
//! not a warning: with no import root configured the clause is refused outright, and
//! with one configured a path that resolves outside it is refused by name.
//!
//! Resolution is done on the **canonical** path, after symlinks. A check on the
//! textual path is not a check: `<root>/../../etc/passwd` and a symlink inside the
//! root both pass it.

use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// The directory `LOAD CSV` may read under, or `None` — the default — for "refuse
/// every source".
static IMPORT_ROOT: RwLock<Option<PathBuf>> = RwLock::new(None);

/// Point `LOAD CSV` at a directory. `None` disables the clause again.
///
/// Canonicalised once here so that every later comparison is between two resolved
/// paths, and so a root that does not exist is a configuration error rather than a
/// clause that silently refuses everything.
pub fn set_import_root(root: Option<&Path>) -> Result<(), CsvSourceError> {
    let resolved = match root {
        None => None,
        Some(p) => Some(
            p.canonicalize()
                .map_err(|e| CsvSourceError::UnreadableRoot(p.display().to_string(), e.to_string()))?,
        ),
    };
    *IMPORT_ROOT.write().unwrap() = resolved;
    Ok(())
}

/// The configured root, for diagnostics.
pub fn import_root() -> Option<PathBuf> {
    IMPORT_ROOT.read().unwrap().clone()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvSourceError {
    /// No import root configured, so the clause is off.
    NoImportRoot,
    /// A scheme this build does not fetch.
    UnsupportedScheme(String),
    /// Resolved outside the import root.
    OutsideImportRoot { path: String, root: String },
    /// The file is not there, or is not readable.
    Unreadable(String, String),
    UnreadableRoot(String, String),
}

impl std::fmt::Display for CsvSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvSourceError::NoImportRoot => write!(
                f,
                "LOAD CSV is disabled: no import directory is configured. Start the \
                 server with --import-dir <path> to enable it; every source must \
                 resolve inside that directory"
            ),
            CsvSourceError::UnsupportedScheme(s) => write!(
                f,
                "LOAD CSV cannot read '{s}' sources; only file:// and plain paths \
                 under the import directory are supported"
            ),
            CsvSourceError::OutsideImportRoot { path, root } => write!(
                f,
                "LOAD CSV refused '{path}': it resolves outside the import directory \
                 '{root}'"
            ),
            CsvSourceError::Unreadable(p, e) => write!(f, "LOAD CSV cannot read '{p}': {e}"),
            CsvSourceError::UnreadableRoot(p, e) => {
                write!(f, "import directory '{p}' cannot be used: {e}")
            }
        }
    }
}

impl std::error::Error for CsvSourceError {}

/// Resolve a `LOAD CSV` source to a path that may be opened.
pub fn resolve(source: &str) -> Result<PathBuf, CsvSourceError> {
    let root = import_root().ok_or(CsvSourceError::NoImportRoot)?;

    let raw = if let Some(rest) = source.strip_prefix("file://") {
        rest.to_string()
    } else if let Some((scheme, _)) = source.split_once("://") {
        // http(s) is deliberately absent. A server that fetches arbitrary URLs on a
        // client's behalf is an SSRF primitive and deserves its own decision, not an
        // inheritance from this one.
        return Err(CsvSourceError::UnsupportedScheme(scheme.to_string()));
    } else {
        source.to_string()
    };

    // Relative paths are relative to the root, not to the process working directory,
    // which is not something a query author can see.
    let joined = if Path::new(&raw).is_absolute() {
        PathBuf::from(&raw)
    } else {
        root.join(&raw)
    };

    // Canonicalise before comparing: `<root>/../../etc/passwd` and a symlink planted
    // inside the root both pass a textual check.
    let resolved = joined
        .canonicalize()
        .map_err(|e| CsvSourceError::Unreadable(raw.clone(), e.to_string()))?;

    if !resolved.starts_with(&root) {
        return Err(CsvSourceError::OutsideImportRoot {
            path: raw,
            root: root.display().to_string(),
        });
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The global root makes these tests order-dependent, so they run as one.
    #[test]
    fn the_gate() {
        let dir = tempfile::tempdir().unwrap();
        let inside = dir.path().join("ok.csv");
        writeln!(std::fs::File::create(&inside).unwrap(), "a,b").unwrap();

        let outside_dir = tempfile::tempdir().unwrap();
        let outside = outside_dir.path().join("secret.csv");
        writeln!(std::fs::File::create(&outside).unwrap(), "a,b").unwrap();

        // Off by default: a build that has not been told where to read cannot be
        // talked into reading anywhere.
        set_import_root(None).unwrap();
        assert_eq!(resolve("ok.csv"), Err(CsvSourceError::NoImportRoot));
        assert_eq!(
            resolve(&format!("file://{}", outside.display())),
            Err(CsvSourceError::NoImportRoot)
        );

        set_import_root(Some(dir.path())).unwrap();
        assert!(resolve("ok.csv").is_ok(), "a file in the root was refused");
        assert!(resolve(&format!("file://{}", inside.display())).is_ok());

        // The three ways out of a directory.
        assert!(matches!(
            resolve(&format!("file://{}", outside.display())),
            Err(CsvSourceError::OutsideImportRoot { .. })
        ));
        assert!(matches!(
            resolve("../../etc/passwd"),
            Err(CsvSourceError::OutsideImportRoot { .. }) | Err(CsvSourceError::Unreadable(..))
        ));
        #[cfg(unix)]
        {
            let link = dir.path().join("escape.csv");
            std::os::unix::fs::symlink(&outside, &link).unwrap();
            assert!(
                matches!(
                    resolve("escape.csv"),
                    Err(CsvSourceError::OutsideImportRoot { .. })
                ),
                "a symlink out of the import directory was followed"
            );
        }

        assert!(matches!(
            resolve("https://example.com/x.csv"),
            Err(CsvSourceError::UnsupportedScheme(_))
        ));

        set_import_root(None).unwrap();
    }
}
