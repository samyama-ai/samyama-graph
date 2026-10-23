//! Convert graph file formats to and from `.sgsnap`, offline, with a loss
//! report (#1905, #1908).
//!
//! Lives in the engine so the `samyama` binary can run it (`samyama convert`,
//! #1909): the binary is part of this package and cannot depend on a crate
//! that depends on the engine. The `samyama-convert` crate re-exports this
//! module unchanged, for programs that want only the converter.
//!
//! `.sgsnap` is the hub: each foreign format needs one reader into a
//! [`GraphStore`] and one writer out of it, not a converter to every other
//! format. A conversion is `read` into an in-memory store, then `write` from
//! it, and every step reports what it could not carry.
//!
//! ```no_run
//! use samyama::convert::{convert, Options};
//!
//! let report = convert("in.sgsnap".as_ref(), "out.sgsnap".as_ref(), &Options::default())?;
//! assert!(report.lossless());
//! # Ok::<(), samyama::convert::ConvertError>(())
//! ```
//!
//! **What this release reads and writes:** `.sgsnap` only. The other
//! [`Format`]s are named so the API and the command line do not change shape
//! as their converters land (#1912, #1913, #1914); until then they are refused
//! with [`ConvertError::CannotRead`] or [`ConvertError::CannotWrite`], never
//! read approximately. [`Format::can_read`] and [`Format::can_write`] say which
//! is which, and the README's format matrix is generated from them.
//!
//! **Nothing is dropped silently.** A reader or writer that cannot carry
//! something says so in its [`FormatReport`], and
//! [`Options::require_lossless`] turns any such drop into an error.

pub mod rdf_mapping;

use std::fmt;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

pub use crate::graph::GraphStore;
pub use crate::snapshot::format::{Dropped, ExportStats, ImportStats};

/// A file format a conversion can start or end in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Format {
    /// Samyama's portable snapshot.
    Sgsnap,
    /// RDF Turtle (`.ttl`).
    Turtle,
    /// RDF N-Triples (`.nt`).
    NTriples,
    /// RDF/XML (`.rdf`, `.owl`).
    RdfXml,
    /// JSON-LD (`.jsonld`).
    JsonLd,
    /// Neo4j `apoc.export.json` (`.json`).
    Neo4jJson,
    /// GraphML (`.graphml`).
    GraphMl,
    /// A directory of neo4j-admin style CSV files.
    Csv,
}

impl Format {
    /// Every format, in the order the README lists them.
    pub const ALL: [Format; 8] = [
        Format::Sgsnap,
        Format::Turtle,
        Format::NTriples,
        Format::RdfXml,
        Format::JsonLd,
        Format::Neo4jJson,
        Format::GraphMl,
        Format::Csv,
    ];

    /// The format a path names, from its extension (case-insensitive). A path
    /// with no extension is a CSV directory. `None` for an extension no format
    /// claims.
    pub fn from_path(path: &Path) -> Option<Format> {
        let Some(ext) = path.extension() else {
            return Some(Format::Csv);
        };
        let ext = ext.to_string_lossy().to_ascii_lowercase();
        Format::ALL
            .into_iter()
            .find(|f| f.extensions().contains(&ext.as_str()))
    }

    /// The format a name given on the command line means (`--from` / `--to`),
    /// case-insensitive: the format's own name or any of its extensions.
    pub fn from_name(name: &str) -> Option<Format> {
        let n = name.trim_start_matches('.').to_ascii_lowercase();
        let named = match n.as_str() {
            "sgsnap" => Some(Format::Sgsnap),
            "turtle" => Some(Format::Turtle),
            "ntriples" | "n-triples" => Some(Format::NTriples),
            "rdfxml" | "rdf-xml" | "rdf/xml" => Some(Format::RdfXml),
            "jsonld" | "json-ld" => Some(Format::JsonLd),
            "neo4j" | "neo4j-json" | "neo4jjson" => Some(Format::Neo4jJson),
            "graphml" => Some(Format::GraphMl),
            "csv" => Some(Format::Csv),
            _ => None,
        };
        named.or_else(|| {
            Format::ALL
                .into_iter()
                .find(|f| f.extensions().contains(&n.as_str()))
        })
    }

    /// The file extensions that name this format, without the dot.
    pub fn extensions(&self) -> &'static [&'static str] {
        match self {
            Format::Sgsnap => &["sgsnap"],
            Format::Turtle => &["ttl"],
            Format::NTriples => &["nt"],
            Format::RdfXml => &["rdf", "owl", "xml"],
            Format::JsonLd => &["jsonld"],
            Format::Neo4jJson => &["json"],
            Format::GraphMl => &["graphml"],
            Format::Csv => &[],
        }
    }

    /// Whether this crate can read the format into a graph today.
    ///
    /// JSON-LD is not among them: the engine's JSON-LD parser is a stub that
    /// errors, so reading it would only ever fail (#1912).
    pub fn can_read(&self) -> bool {
        matches!(
            self,
            Format::Sgsnap | Format::Turtle | Format::NTriples | Format::RdfXml
        )
    }

    /// Whether this crate can write a graph out in the format today.
    pub fn can_write(&self) -> bool {
        matches!(
            self,
            Format::Sgsnap | Format::Turtle | Format::NTriples | Format::RdfXml | Format::JsonLd
        )
    }

    /// The engine's RDF format for an RDF member of this enum.
    fn rdf(&self) -> Option<crate::rdf::RdfFormat> {
        use crate::rdf::RdfFormat;
        Some(match self {
            Format::Turtle => RdfFormat::Turtle,
            Format::NTriples => RdfFormat::NTriples,
            Format::RdfXml => RdfFormat::RdfXml,
            Format::JsonLd => RdfFormat::JsonLd,
            _ => return None,
        })
    }

    /// The name a snapshot header's `sources[].format` records.
    fn source_name(&self) -> &'static str {
        match self {
            Format::Sgsnap => "sgsnap",
            Format::Turtle => "turtle",
            Format::NTriples => "n-triples",
            Format::RdfXml => "rdf-xml",
            Format::JsonLd => "json-ld",
            Format::Neo4jJson => "neo4j-json",
            Format::GraphMl => "graphml",
            Format::Csv => "csv",
        }
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Format::Sgsnap => ".sgsnap",
            Format::Turtle => "Turtle",
            Format::NTriples => "N-Triples",
            Format::RdfXml => "RDF/XML",
            Format::JsonLd => "JSON-LD",
            Format::Neo4jJson => "Neo4j apoc.export.json",
            Format::GraphMl => "GraphML",
            Format::Csv => "CSV directory",
        })
    }
}

/// What a reader does with an edge whose endpoint is not in the input.
///
/// `.sgsnap` import fails the whole file on an edge to an unknown node, so a
/// converter decides before it writes. No policy drops an edge silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EndpointPolicy {
    /// Refuse the input, naming the first missing ids.
    #[default]
    Fail,
    /// Create a node labelled `Unresolved` that holds the reference.
    Placeholder,
    /// Drop the edge and count it in the report.
    Drop,
}

/// How a conversion runs.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Edges whose endpoint the input does not contain. Readers of formats
    /// with references (RDF, Neo4j) apply it; `.sgsnap` cannot hold one.
    pub on_missing_endpoint: EndpointPolicy,
    /// Treat anything a step reports dropping as an error.
    pub require_lossless: bool,
    /// How RDF input becomes a graph (#1910). `None` reads it under the
    /// `generic` preset.
    pub mapping: Option<rdf_mapping::Mapping>,
}

/// What one read or write did, in the format's own terms.
///
/// Wraps the reports the engine already produces rather than inventing a
/// second vocabulary for losses.
#[derive(Debug)]
pub enum FormatReport {
    /// A `.sgsnap` read: the engine's import report.
    SgsnapRead(ImportStats),
    /// A `.sgsnap` write: the engine's export report, its loss report included.
    SgsnapWrite(ExportStats),
    /// An RDF read under a mapping (#1912).
    RdfRead {
        report: rdf_mapping::RdfMapReport,
        dropped: Vec<Dropped>,
    },
    /// A graph written back as RDF (#1912).
    RdfWrite {
        report: rdf_mapping::RdfWriteReport,
        dropped: Vec<Dropped>,
        nodes: u64,
        edges: u64,
    },
}

impl FormatReport {
    /// What this step could not carry. Empty is lossless.
    pub fn dropped(&self) -> &[Dropped] {
        match self {
            FormatReport::SgsnapRead(_) => &[],
            FormatReport::SgsnapWrite(stats) => &stats.dropped,
            FormatReport::RdfRead { dropped, .. } | FormatReport::RdfWrite { dropped, .. } => {
                dropped
            }
        }
    }

    /// Nodes this step read or wrote.
    pub fn node_count(&self) -> u64 {
        match self {
            FormatReport::SgsnapRead(s) => s.node_count,
            FormatReport::SgsnapWrite(s) => s.node_count,
            FormatReport::RdfRead { report, .. } => report.nodes_created as u64,
            FormatReport::RdfWrite { nodes, .. } => *nodes,
        }
    }

    /// Edges this step read or wrote.
    pub fn edge_count(&self) -> u64 {
        match self {
            FormatReport::SgsnapRead(s) => s.edge_count,
            FormatReport::SgsnapWrite(s) => s.edge_count,
            FormatReport::RdfRead { report, .. } => report.edges_created as u64,
            FormatReport::RdfWrite { edges, .. } => *edges,
        }
    }
}

/// What a whole conversion did: both steps, and everything either dropped.
#[derive(Debug)]
pub struct ConvertReport {
    /// The format read.
    pub input_format: Format,
    /// The format written.
    pub output_format: Format,
    /// The read.
    pub input: FormatReport,
    /// The write.
    pub output: FormatReport,
}

impl ConvertReport {
    /// Everything either step dropped, read side first.
    pub fn dropped(&self) -> Vec<&Dropped> {
        self.input
            .dropped()
            .iter()
            .chain(self.output.dropped())
            .collect()
    }

    /// Whether nothing was dropped on either side.
    pub fn lossless(&self) -> bool {
        self.input.dropped().is_empty() && self.output.dropped().is_empty()
    }
}

/// Why a conversion did not happen, or did not complete.
#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    /// The path's extension names no format.
    #[error("cannot tell the format of {0} from its extension")]
    UnknownFormat(PathBuf),
    /// The format is named but has no reader yet.
    #[error("reading {0} is not supported yet; this build reads: {readable}", readable = readable())]
    CannotRead(Format),
    /// The format is named but has no writer yet.
    #[error("writing {0} is not supported yet; this build writes: {writable}", writable = writable())]
    CannotWrite(Format),
    /// `require_lossless` was set and a step reported a drop.
    #[error("the conversion would drop {}; refused because lossless output was required", describe(.0))]
    Lossy(Vec<Dropped>),
    /// The engine refused the input or failed to write the output.
    #[error("{0}")]
    Engine(String),
    /// A file could not be opened, read or written.
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn list(pred: fn(&Format) -> bool) -> String {
    Format::ALL
        .iter()
        .filter(|f| pred(f))
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn readable() -> String {
    list(Format::can_read)
}

fn writable() -> String {
    list(Format::can_write)
}

fn describe(dropped: &[Dropped]) -> String {
    dropped
        .iter()
        .map(|d| format!("{} {}", d.count, d.what))
        .collect::<Vec<_>>()
        .join(", ")
}

fn engine(e: Box<dyn std::error::Error>) -> ConvertError {
    ConvertError::Engine(e.to_string())
}

/// Read `input`, in `format`, into `store`.
pub fn read(
    format: Format,
    input: impl BufRead,
    store: &mut GraphStore,
    opts: &Options,
) -> Result<FormatReport, ConvertError> {
    read_named(format, input, "-", store, opts)
}

/// [`read`], naming the input for the snapshot header's `sources[].file`:
/// an RDF read records where the graph came from -- the file, its SHA-256,
/// the converter and the mapping with the names it applied -- so writing it
/// back as RDF can undo the mapping (#1911, #1912). `name` is a bare file
/// name, not a path.
pub fn read_named(
    format: Format,
    mut input: impl BufRead,
    name: &str,
    store: &mut GraphStore,
    opts: &Options,
) -> Result<FormatReport, ConvertError> {
    let report = match format {
        Format::Sgsnap => {
            FormatReport::SgsnapRead(crate::snapshot::import_tenant(store, input).map_err(engine)?)
        }
        f @ (Format::Turtle | Format::NTriples | Format::RdfXml) => {
            // Both stores are held at once: the parsed triples, then the graph
            // made from them. Streaming is a later change if a vocabulary
            // proves too large for that (#1912).
            let mut bytes = Vec::new();
            input
                .read_to_end(&mut bytes)
                .map_err(|e| ConvertError::Engine(format!("could not read the input: {e}")))?;
            let text = String::from_utf8(bytes)
                .map_err(|_| ConvertError::Engine("RDF input is not UTF-8".to_string()))?;
            let rdf_format = f.rdf().ok_or(ConvertError::CannotRead(f))?;
            let triples = crate::rdf::RdfParser::parse(&text, rdf_format).map_err(message)?;
            let mut rdf = crate::rdf::RdfStore::new();
            for t in triples {
                // A triple stated twice is one triple; RDF is a set.
                let _ = rdf.insert(t);
            }
            let mapping = match &opts.mapping {
                Some(m) => m.clone(),
                None => rdf_mapping::Mapping::preset("generic").map_err(message)?,
            };
            let report = mapping.apply(&rdf, store).map_err(message)?;
            store.add_snapshot_sources([crate::snapshot::format::SnapshotSource {
                format: f.source_name().to_string(),
                file: name.to_string(),
                sha256: sha256_hex(text.as_bytes()),
                converter: format!("samyama-convert {}", env!("CARGO_PKG_VERSION")),
                mapping: Some(mapping.recorded(&report.names)),
            }]);
            let dropped = report.dropped();
            FormatReport::RdfRead { report, dropped }
        }
        other => return Err(ConvertError::CannotRead(other)),
    };
    check_lossless(&report, opts)?;
    Ok(report)
}

/// An error that is reported by its message alone.
fn message(e: impl fmt::Display) -> ConvertError {
    ConvertError::Engine(e.to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The mapping a graph was read from RDF under, and the names it applied:
/// the last source in its header that recorded one. A graph that never was
/// RDF gets the `samyama` preset, which is Samyama's own RDF form.
fn recorded_mapping(
    store: &GraphStore,
) -> Result<(rdf_mapping::Mapping, rdf_mapping::AppliedNames), ConvertError> {
    let recorded = store
        .snapshot_sources()
        .iter()
        .rev()
        .find_map(|s| s.mapping.as_ref().and_then(rdf_mapping::Mapping::from_recorded));
    match recorded {
        Some(pair) => Ok(pair),
        None => Ok((
            rdf_mapping::Mapping::preset("samyama").map_err(message)?,
            rdf_mapping::AppliedNames::default(),
        )),
    }
}

/// The graph as RDF text in `format`, and what writing it lost. Turtle gets
/// the mapping's prefixes as `@prefix` lines.
fn rdf_text(store: &GraphStore, format: Format) -> Result<(String, FormatReport), ConvertError> {
    let rdf_format = format.rdf().ok_or(ConvertError::CannotWrite(format))?;
    let (mapping, names) = recorded_mapping(store)?;
    let (triples, report) = mapping.write_rdf(&names, store).map_err(message)?;
    let body = crate::rdf::RdfSerializer::serialize(&triples, rdf_format).map_err(message)?;
    let text = if format == Format::Turtle {
        let mut head: String = mapping
            .prefixes
            .iter()
            .map(|(p, ns)| format!("@prefix {p}: <{ns}> .\n"))
            .collect();
        head.push('\n');
        head + &body
    } else {
        body
    };
    let dropped = report.dropped();
    Ok((
        text,
        FormatReport::RdfWrite {
            report,
            dropped,
            nodes: store.all_nodes().len() as u64,
            edges: store.all_edges().len() as u64,
        },
    ))
}

/// Write `store` to `out` in `format`.
///
/// For a file, prefer [`convert`] or [`write_path`]: they write `.sgsnap`
/// atomically, so a failed conversion never leaves a truncated file behind.
pub fn write(
    format: Format,
    store: &GraphStore,
    out: impl Write,
    opts: &Options,
) -> Result<FormatReport, ConvertError> {
    let mut out = out;
    let report = match format {
        Format::Sgsnap => {
            FormatReport::SgsnapWrite(crate::snapshot::export_tenant(store, out).map_err(engine)?)
        }
        f @ (Format::Turtle | Format::NTriples | Format::RdfXml | Format::JsonLd) => {
            let (text, report) = rdf_text(store, f)?;
            // Checked before anything is written: the text is all in memory.
            check_lossless(&report, opts)?;
            out.write_all(text.as_bytes())
                .and_then(|_| out.flush())
                .map_err(|e| ConvertError::Engine(format!("could not write {f}: {e}")))?;
            report
        }
        other => return Err(ConvertError::CannotWrite(other)),
    };
    check_lossless(&report, opts)?;
    Ok(report)
}

/// Write `store` to the file at `path`, in the format its extension names.
///
/// `.sgsnap` goes through `snapshot::write_file_if`: parent directories are
/// created, and the file appears whole or not at all. With
/// [`Options::require_lossless`], a lossy export is refused before it replaces
/// `path`, so an existing file is left unchanged and an absent one stays
/// absent.
pub fn write_path(
    store: &GraphStore,
    path: &Path,
    opts: &Options,
) -> Result<FormatReport, ConvertError> {
    let format = Format::from_path(path).ok_or_else(|| ConvertError::UnknownFormat(path.into()))?;
    let report = match format {
        Format::Sgsnap => {
            let (stats, _kept) =
                crate::snapshot::write_file_if(store, path, &Default::default(), |stats| {
                    !opts.require_lossless || stats.dropped.is_empty()
                })
                .map_err(engine)?;
            FormatReport::SgsnapWrite(stats)
        }
        f @ (Format::Turtle | Format::NTriples | Format::RdfXml | Format::JsonLd) => {
            let (text, report) = rdf_text(store, f)?;
            // Refused before the file is touched, so a lossy write leaves an
            // existing output as it was.
            check_lossless(&report, opts)?;
            write_text_atomically(path, &text)?;
            report
        }
        other => return Err(ConvertError::CannotWrite(other)),
    };
    check_lossless(&report, opts)?;
    Ok(report)
}

/// Write `text` to `path` through a new, uniquely named file beside it,
/// synced and then renamed over `path`, so the file appears whole or not at
/// all and nothing already there is truncated.
fn write_text_atomically(path: &Path, text: &str) -> Result<(), ConvertError> {
    let io = |p: &Path, source: std::io::Error| ConvertError::Io {
        path: p.into(),
        source,
    };
    let parent = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut staging = tempfile::Builder::new()
        .prefix(&format!(".{name}."))
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(|e| io(parent, e))?;
    staging
        .write_all(text.as_bytes())
        .and_then(|_| staging.as_file().sync_all())
        .map_err(|e| io(staging.path(), e))?;
    staging.persist(path).map_err(|e| io(path, e.error))?;
    Ok(())
}

/// Convert the file at `input` to the file at `output`, each in the format its
/// extension names.
///
/// Both formats are checked before anything is read, so an unsupported output
/// is refused without the cost of reading the input. With
/// [`Options::require_lossless`], a lossy read stops before anything is
/// written.
pub fn convert(input: &Path, output: &Path, opts: &Options) -> Result<ConvertReport, ConvertError> {
    let input_format =
        Format::from_path(input).ok_or_else(|| ConvertError::UnknownFormat(input.into()))?;
    let output_format =
        Format::from_path(output).ok_or_else(|| ConvertError::UnknownFormat(output.into()))?;
    if !input_format.can_read() {
        return Err(ConvertError::CannotRead(input_format));
    }
    if !output_format.can_write() {
        return Err(ConvertError::CannotWrite(output_format));
    }

    let file = std::fs::File::open(input).map_err(|source| ConvertError::Io {
        path: input.into(),
        source,
    })?;
    let mut store = GraphStore::new();
    let name = input
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let read_report = read_named(
        input_format,
        std::io::BufReader::new(file),
        &name,
        &mut store,
        opts,
    )?;
    let write_report = write_path(&store, output, opts)?;
    Ok(ConvertReport {
        input_format,
        output_format,
        input: read_report,
        output: write_report,
    })
}

fn check_lossless(report: &FormatReport, opts: &Options) -> Result<(), ConvertError> {
    if opts.require_lossless && !report.dropped().is_empty() {
        return Err(ConvertError::Lossy(report.dropped().to_vec()));
    }
    Ok(())
}

/// The README's format matrix, generated from [`Format`] so it cannot drift
/// from what the converter does. `crates/samyama-convert/tests/readme.rs`
/// holds that crate's README to it.
pub fn format_matrix() -> String {
    let yes_no = |b: bool| if b { "yes" } else { "not yet" };
    let mut out = String::from("| Format | Extensions | Read | Write |\n|---|---|---|---|\n");
    for f in Format::ALL {
        let ext = if f.extensions().is_empty() {
            "(a directory)".to_string()
        } else {
            f.extensions()
                .iter()
                .map(|e| format!("`.{e}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        out.push_str(&format!(
            "| {f} | {ext} | {} | {} |\n",
            yes_no(f.can_read()),
            yes_no(f.can_write())
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_format_is_named_by_its_extension_in_any_case() {
        assert_eq!(
            Format::from_path(Path::new("a.sgsnap")),
            Some(Format::Sgsnap)
        );
        assert_eq!(Format::from_path(Path::new("a.TTL")), Some(Format::Turtle));
        assert_eq!(
            Format::from_path(Path::new("onto.owl")),
            Some(Format::RdfXml)
        );
        assert_eq!(
            Format::from_path(Path::new("dump.json")),
            Some(Format::Neo4jJson)
        );
        assert_eq!(Format::from_path(Path::new("out_dir")), Some(Format::Csv));
        assert_eq!(Format::from_path(Path::new("a.xyz")), None);
    }

    #[test]
    fn every_extension_names_exactly_one_format() {
        let mut seen = std::collections::HashSet::new();
        for f in Format::ALL {
            for e in f.extensions() {
                assert!(seen.insert(*e), ".{e} is claimed twice");
            }
        }
    }

    #[test]
    fn an_unsupported_format_is_refused_by_name_not_read_approximately() {
        let mut store = GraphStore::new();
        // JSON-LD: the engine's parser is a stub, so it is refused by name.
        let err = read(
            Format::JsonLd,
            std::io::Cursor::new(b""),
            &mut store,
            &Options::default(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, ConvertError::CannotRead(Format::JsonLd)),
            "{msg}"
        );
        assert!(msg.contains("JSON-LD") && msg.contains(".sgsnap"), "{msg}");
        let err = write(Format::GraphMl, &store, Vec::new(), &Options::default()).unwrap_err();
        assert!(
            matches!(err, ConvertError::CannotWrite(Format::GraphMl)),
            "{err}"
        );
    }
}
