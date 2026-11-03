//! The data directory's format version (REL-13, #1657).
//!
//! Nothing in a data directory said which format wrote it: 0 of the 8 files a
//! fresh directory holds carried a Samyama version, so an engine reading a
//! directory written by a newer one had no way to know it should not. This is
//! the one file that says.
//!
//! `SAMYAMA_FORMAT` sits at the top of the directory, beside `data/`, `wal/`
//! and `vectors/`, as plain text so an operator can read it without tooling:
//!
//! ```text
//! format = 1
//! written_by = 1.11.0
//! ```
//!
//! **What the number means.** `FORMAT_VERSION` changes when what is on disk
//! changes in a way an older engine would misread. An engine opens a directory
//! of its own format or older and refuses a newer one, with the two numbers in
//! the error, rather than reading bytes it does not understand. `written_by` is
//! for the operator and is never compared.
//!
//! **A directory from before versioning** has data and no file. It is read as
//! format 0 -- the layout every release up to this one wrote -- and stamped,
//! so the first engine with this file adopts an existing directory instead of
//! refusing it.

use std::path::{Path, PathBuf};

/// The format this engine writes, and the newest it reads.
pub const FORMAT_VERSION: u32 = 1;

/// The file's name, at the top of the data directory.
pub const FORMAT_FILE: &str = "SAMYAMA_FORMAT";

/// Why a data directory cannot be opened by this engine.
#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    /// Written by a newer format than this engine reads.
    #[error(
        "data directory {path} is format {found}, newer than the format {supported} this \
         engine (v{engine}) can read. Start the engine that wrote it, or one at least as new; \
         this one has not opened it and will not change it"
    )]
    Newer {
        path: PathBuf,
        found: u32,
        supported: u32,
        engine: &'static str,
    },
    /// The file is there but does not say a format this engine can parse.
    #[error(
        "data directory {path} has a {file} this engine cannot read ({reason}). It has not \
         opened the directory and will not change it"
    )]
    Unreadable {
        path: PathBuf,
        file: &'static str,
        reason: String,
    },
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// What `check` found, for the caller to act on once the stores are open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Found {
    /// The file is there and names a format this engine reads.
    Versioned(u32),
    /// Data and no file: a directory from before versioning, read as format 0.
    Unversioned,
    /// Nothing written yet.
    Fresh,
}

fn io(path: &Path, source: std::io::Error) -> FormatError {
    FormatError::Io { path: path.to_path_buf(), source }
}

/// Parse `format = N` out of the file's text.
fn parse(text: &str) -> Result<u32, String> {
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else { continue };
        if key.trim() == "format" {
            return value
                .trim()
                .parse::<u32>()
                .map_err(|e| format!("`format = {}` is not a number: {e}", value.trim()));
        }
    }
    Err("no `format = N` line".into())
}

/// Read the directory's format, refusing one this engine cannot read.
///
/// Called **before** anything is opened or created, so a refused directory is
/// left exactly as it was found.
pub fn check(base: &Path) -> Result<Found, FormatError> {
    let file = base.join(FORMAT_FILE);
    match std::fs::read_to_string(&file) {
        Ok(text) => {
            let found = parse(&text).map_err(|reason| FormatError::Unreadable {
                path: base.to_path_buf(),
                file: FORMAT_FILE,
                reason,
            })?;
            if found > FORMAT_VERSION {
                return Err(FormatError::Newer {
                    path: base.to_path_buf(),
                    found,
                    supported: FORMAT_VERSION,
                    engine: env!("CARGO_PKG_VERSION"),
                });
            }
            Ok(Found::Versioned(found))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if holds_data(base).map_err(|e| io(base, e))? {
                Ok(Found::Unversioned)
            } else {
                Ok(Found::Fresh)
            }
        }
        Err(e) => Err(io(&file, e)),
    }
}

/// Whether anything has been written under `base`: any entry in it at all.
/// A directory the operator made empty for the engine is fresh; one with
/// anything in it is not ours to call fresh.
fn holds_data(base: &Path) -> std::io::Result<bool> {
    match std::fs::read_dir(base) {
        Ok(mut entries) => Ok(entries.next().is_some()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Write this engine's format, atomically: a reader sees the old file or the
/// new one, never half of either.
///
/// Called once the stores have opened, so a directory is stamped only when it
/// was actually readable. A directory already at this format is left alone.
pub fn stamp(base: &Path, found: Found) -> Result<(), FormatError> {
    if found == Found::Versioned(FORMAT_VERSION) {
        return Ok(());
    }
    let file = base.join(FORMAT_FILE);
    let tmp = base.join(format!("{FORMAT_FILE}.tmp"));
    let text = format!(
        "format = {FORMAT_VERSION}\nwritten_by = {}\n",
        env!("CARGO_PKG_VERSION")
    );
    std::fs::write(&tmp, text).map_err(|e| io(&tmp, e))?;
    std::fs::rename(&tmp, &file).map_err(|e| io(&file, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_directory_is_fresh_and_a_missing_one_too() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(check(dir.path()).unwrap(), Found::Fresh);
        assert_eq!(check(&dir.path().join("not-yet")).unwrap(), Found::Fresh);
    }

    #[test]
    fn data_without_the_file_is_the_format_before_versioning() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("data")).unwrap();
        assert_eq!(check(dir.path()).unwrap(), Found::Unversioned);
    }

    #[test]
    fn a_stamp_is_read_back_as_this_format() {
        let dir = tempfile::tempdir().unwrap();
        stamp(dir.path(), Found::Fresh).unwrap();
        assert_eq!(check(dir.path()).unwrap(), Found::Versioned(FORMAT_VERSION));
        let text = std::fs::read_to_string(dir.path().join(FORMAT_FILE)).unwrap();
        assert!(text.contains(&format!("written_by = {}", env!("CARGO_PKG_VERSION"))), "{text}");
        assert!(!dir.path().join(format!("{FORMAT_FILE}.tmp")).exists());
    }

    #[test]
    fn a_newer_format_is_refused_with_both_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let newer = FORMAT_VERSION + 1;
        std::fs::write(dir.path().join(FORMAT_FILE), format!("format = {newer}\n")).unwrap();
        let err = check(dir.path()).unwrap_err().to_string();
        assert!(err.contains(&format!("format {newer}")), "{err}");
        assert!(err.contains(&format!("format {FORMAT_VERSION}")), "{err}");
    }

    #[test]
    fn a_file_that_names_no_format_is_refused_not_guessed() {
        let dir = tempfile::tempdir().unwrap();
        for text in ["", "format = one\n", "version = 1\n"] {
            std::fs::write(dir.path().join(FORMAT_FILE), text).unwrap();
            assert!(
                matches!(check(dir.path()), Err(FormatError::Unreadable { .. })),
                "{text:?} was accepted"
            );
        }
    }
}
