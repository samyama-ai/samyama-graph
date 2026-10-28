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
    pub fn can_read(&self) -> bool {
        matches!(self, Format::Sgsnap | Format::Neo4jJson)
    }

    /// Whether this crate can write a graph out in the format today.
    pub fn can_write(&self) -> bool {
        matches!(self, Format::Sgsnap | Format::GraphMl | Format::Csv)
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
    /// A GraphML write: the exporter's report, and what it means as losses.
    GraphMlWrite {
        report: crate::export::graphml::GraphMlReport,
        dropped: Vec<Dropped>,
    },
    /// A CSV directory write: the exporter's report, and what it means as losses.
    CsvWrite {
        report: crate::export::csv::GraphCsvReport,
        dropped: Vec<Dropped>,
    },
    /// A Neo4j `apoc.export.json` read: the importer's own report, and what it
    /// means as losses.
    Neo4jRead {
        report: crate::migrate::neo4j_json::ImportReport,
        dropped: Vec<Dropped>,
    },
}

impl FormatReport {
    /// What this step could not carry. Empty is lossless.
    pub fn dropped(&self) -> &[Dropped] {
        match self {
            FormatReport::SgsnapRead(_) => &[],
            FormatReport::SgsnapWrite(stats) => &stats.dropped,
            FormatReport::GraphMlWrite { dropped, .. } | FormatReport::CsvWrite { dropped, .. } => {
                dropped
            }
            FormatReport::Neo4jRead { dropped, .. } => dropped,
        }
    }

    /// Nodes this step read or wrote.
    pub fn node_count(&self) -> u64 {
        match self {
            FormatReport::SgsnapRead(s) => s.node_count,
            FormatReport::SgsnapWrite(s) => s.node_count,
            FormatReport::GraphMlWrite { report, .. } => report.nodes_written as u64,
            FormatReport::CsvWrite { report, .. } => report.nodes_written as u64,
            FormatReport::Neo4jRead { report, .. } => {
                (report.nodes_created + report.placeholder_nodes) as u64
            }
        }
    }

    /// Edges this step read or wrote.
    pub fn edge_count(&self) -> u64 {
        match self {
            FormatReport::SgsnapRead(s) => s.edge_count,
            FormatReport::SgsnapWrite(s) => s.edge_count,
            FormatReport::GraphMlWrite { report, .. } => report.edges_written as u64,
            FormatReport::CsvWrite { report, .. } => report.edges_written as u64,
            FormatReport::Neo4jRead { report, .. } => report.edges_created as u64,
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
    /// Edges whose endpoint the input never defines, under
    /// [`EndpointPolicy::Fail`]. Names up to ten of the missing ids.
    #[error(
        "{count} relationship(s) point at nodes the input does not contain (first missing ids: {}); \
         pass --missing-endpoints drop or placeholder to convert anyway",
        first.join(", ")
    )]
    MissingEndpoints { count: usize, first: Vec<String> },
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
    let report = match format {
        Format::Sgsnap => {
            FormatReport::SgsnapRead(crate::snapshot::import_tenant(store, input).map_err(engine)?)
        }
        Format::Neo4jJson => read_neo4j(input, store, opts)?,
        other => return Err(ConvertError::CannotRead(other)),
    };
    check_lossless(&report, opts)?;
    Ok(report)
}

/// Neo4j `apoc.export.json`, either shape (#1913).
///
/// The importer takes the whole document: the single-object shape
/// (`jsonFormat:'JSON'`) cannot be split without parsing it, so the input is
/// read into memory once.
fn read_neo4j(
    mut input: impl BufRead,
    store: &mut GraphStore,
    opts: &Options,
) -> Result<FormatReport, ConvertError> {
    use crate::migrate::neo4j_json::{import_str_with, ImportOptions};
    let mut text = String::new();
    input.read_to_string(&mut text).map_err(|e| {
        ConvertError::Engine(format!("could not read the input as UTF-8 text: {e}"))
    })?;
    let placeholders = opts.on_missing_endpoint == EndpointPolicy::Placeholder;
    let report = import_str_with(&text, store, "default", ImportOptions { placeholders })
        .map_err(|e| ConvertError::Engine(e.to_string()))?;
    if report.dangling_edges > 0 && opts.on_missing_endpoint == EndpointPolicy::Fail {
        return Err(ConvertError::MissingEndpoints {
            count: report.dangling_edges,
            first: report.missing_endpoints.iter().take(10).cloned().collect(),
        });
    }
    let dropped = neo4j_losses(&report);
    Ok(FormatReport::Neo4jRead { report, dropped })
}

/// What a Neo4j import's report means as losses, one row per kind.
///
/// Values that only *look* temporal or spatial are not losses: they arrive as
/// the strings and maps the file holds, exactly, and the importer's report
/// counts them for a user who wants to convert them deliberately.
fn neo4j_losses(r: &crate::migrate::neo4j_json::ImportReport) -> Vec<Dropped> {
    let mut out = Vec::new();
    let mut push = |what: &str, count: usize, detail: String| {
        if count > 0 {
            out.push(Dropped {
                what: what.to_string(),
                count: count as u64,
                detail,
            });
        }
    };
    push(
        "dangling_relationships",
        r.dangling_edges,
        format!(
            "Relationships whose start or end node is not in the file were not created \
             (first missing ids: {}).",
            r.missing_endpoints
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    push(
        "unresolved_endpoints",
        r.placeholder_nodes,
        "Endpoints the file never defines were created as `Unresolved` nodes holding \
         only the Neo4j id (`neo4j_id`); their labels and properties are not in the input."
            .to_string(),
    );
    push(
        "null_properties",
        r.null_properties_dropped,
        "Properties whose value is JSON null were not stored: a graph cannot hold a null \
         property, so the exporter, not the graph, wrote them."
            .to_string(),
    );
    push(
        "unknown_record_types",
        r.records_skipped,
        format!(
            "Records whose type this importer does not know were skipped; their types: {}.",
            r.unknown_record_types.join(", ")
        ),
    );
    push(
        "unparseable_lines",
        r.unparseable_lines.len(),
        match r.unparseable_lines.first() {
            Some((line, err)) => {
                format!("Lines that are not JSON were skipped; the first is line {line}: {err}.")
            }
            None => String::new(),
        },
    );
    out
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
    let report =
        match format {
            Format::Sgsnap => FormatReport::SgsnapWrite(
                crate::snapshot::export_tenant(store, out).map_err(engine)?,
            ),
            Format::GraphMl => write_graphml(store, out)?,
            Format::Csv => return Err(ConvertError::Engine(
                "a CSV export is a directory of files, not one stream; use write_path or convert"
                    .to_string(),
            )),
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
        Format::Csv => write_csv_dir(store, path, opts)?,
        Format::GraphMl => write_file_atomically(path, opts, |f| write_graphml(store, f))?,
        other => return Err(ConvertError::CannotWrite(other)),
    };
    check_lossless(&report, opts)?;
    Ok(report)
}

/// GraphML into `out` (#1914). The exporter builds the document in memory:
/// GraphML declares every attribute's type before the first element, so the
/// whole graph is read before anything is written in any case.
fn write_graphml(store: &GraphStore, mut out: impl Write) -> Result<FormatReport, ConvertError> {
    let (text, report) = crate::export::graphml::to_graphml(store);
    out.write_all(text.as_bytes())
        .and_then(|_| out.flush())
        .map_err(|e| ConvertError::Engine(format!("could not write GraphML: {e}")))?;
    let mut dropped = Vec::new();
    let mut push = |what: &str, count: usize, detail: String| {
        if count > 0 {
            dropped.push(Dropped {
                what: what.to_string(),
                count: count as u64,
                detail,
            });
        }
    };
    push(
        "attributes_widened_to_string",
        report.attributes_widened_to_string.len(),
        format!(
            "GraphML declares one type per attribute; these held more than one and are \
             written as text: {}.",
            report.attributes_widened_to_string.join(", ")
        ),
    );
    push(
        "values_written_as_json",
        report.values_written_as_json,
        "Lists, maps and vectors have no GraphML type and are written as JSON text.".to_string(),
    );
    push(
        "temporal_values_written_as_text",
        report.temporal_values_written_as_text,
        "Dates and times have no GraphML type and are written as ISO-8601 text.".to_string(),
    );
    push(
        "labels_containing_a_space",
        report.labels_containing_a_space.len(),
        format!(
            "Labels are written space-separated, which these cannot be read back from: {}.",
            report.labels_containing_a_space.join(", ")
        ),
    );
    Ok(FormatReport::GraphMlWrite { report, dropped })
}

/// What a CSV directory export's report means as losses.
fn csv_losses(r: &crate::export::csv::GraphCsvReport) -> Vec<Dropped> {
    let mut out = Vec::new();
    let mut push = |what: &str, count: usize, detail: String| {
        if count > 0 {
            out.push(Dropped {
                what: what.to_string(),
                count: count as u64,
                detail,
            });
        }
    };
    push(
        "columns_widened_to_string",
        r.columns_widened_to_string.len(),
        format!(
            "A CSV column has one type; these held more than one and are written as text: {}.",
            r.columns_widened_to_string.join(", ")
        ),
    );
    push(
        "values_written_as_json",
        r.values_written_as_json,
        "Lists, maps and vectors are written as JSON text in one field.".to_string(),
    );
    push(
        "temporal_values_written_as_text",
        r.temporal_values_written_as_text,
        "Dates and times are written as ISO-8601 text in a string column.".to_string(),
    );
    push(
        "labels_containing_a_semicolon",
        r.labels_containing_a_semicolon.len(),
        format!(
            "neo4j-admin reads `;` as the separator between labels, so these split on import: {}.",
            r.labels_containing_a_semicolon.join(", ")
        ),
    );
    push(
        "properties_with_a_colon_omitted",
        r.properties_with_a_colon_omitted.len(),
        format!(
            "neo4j-admin reads `:` in a header as the start of the column type and has no \
             escape for it, so these properties are not written: {}.",
            r.properties_with_a_colon_omitted.join(", ")
        ),
    );
    out
}

/// The whole graph as a CSV directory at `dir`, whole or not at all: written
/// into `<dir>.tmp` and renamed into place. A `dir` that exists and is not
/// empty is refused rather than mixed with or overwritten.
fn write_csv_dir(
    store: &GraphStore,
    dir: &Path,
    opts: &Options,
) -> Result<FormatReport, ConvertError> {
    let io = |path: &Path, source: std::io::Error| ConvertError::Io {
        path: path.into(),
        source,
    };
    if let Ok(mut entries) = std::fs::read_dir(dir) {
        if entries.next().is_some() {
            return Err(ConvertError::Engine(format!(
                "{} exists and is not empty; a CSV export writes a new directory",
                dir.display()
            )));
        }
    }
    // Staged in a directory of its own, uniquely named beside `dir` so the
    // rename stays on one filesystem. Nothing that already exists is deleted
    // or reused, and the staging directory removes itself on any early return.
    let parent = staging_parent(dir).map_err(|e| io(dir, e))?;
    let staging = tempfile::Builder::new()
        .prefix(&staging_prefix(dir))
        .suffix(".tmp")
        .tempdir_in(&parent)
        .map_err(|e| io(&parent, e))?;
    let report = crate::export::csv::write_graph_dir(store, staging.path())
        .map_err(|e| io(staging.path(), e))?;
    let dropped = csv_losses(&report);
    let report = FormatReport::CsvWrite { report, dropped };
    // Refused before the directory appears, so a lossy export leaves no output.
    check_lossless(&report, opts)?;
    // An empty directory the caller made is replaced; a non-empty one was
    // refused above.
    let _ = std::fs::remove_dir(dir);
    let staged = staging.keep();
    std::fs::rename(&staged, dir).map_err(|e| {
        let _ = std::fs::remove_dir_all(&staged);
        io(dir, e)
    })?;
    Ok(report)
}

/// Write a single-file format through `<path>.tmp`, renamed into place only
/// once complete, so a failure never leaves a partial file at `path`.
fn write_file_atomically(
    path: &Path,
    opts: &Options,
    write: impl FnOnce(&mut std::io::BufWriter<std::fs::File>) -> Result<FormatReport, ConvertError>,
) -> Result<FormatReport, ConvertError> {
    let io = |p: &Path, source: std::io::Error| ConvertError::Io {
        path: p.into(),
        source,
    };
    let parent = staging_parent(path).map_err(|e| io(path, e))?;
    // A new, uniquely named file beside `path`: an existing `<path>.tmp` or a
    // symlink is never truncated or followed, and concurrent conversions do
    // not share a staging file. It is removed on any early return.
    let staging = tempfile::Builder::new()
        .prefix(&staging_prefix(path))
        .suffix(".tmp")
        .tempfile_in(&parent)
        .map_err(|e| io(&parent, e))?;
    let (file, staged) = staging.into_parts();
    let mut out = std::io::BufWriter::new(file);
    let report = write(&mut out)?;
    let file = out.into_inner().map_err(|e| io(&staged, e.into_error()))?;
    file.sync_all().map_err(|e| io(&staged, e))?;
    // Refused before the rename, so a lossy export leaves `path` as it was.
    check_lossless(&report, opts)?;
    staged.persist(path).map_err(|e| io(path, e.error))?;
    Ok(report)
}

/// The directory a staged output is created in: `path`'s own parent, created
/// if missing, so the final rename never crosses a filesystem.
fn staging_parent(path: &Path) -> std::io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    std::fs::create_dir_all(&parent)?;
    Ok(parent)
}

/// `.<name>.` -- a hidden prefix naming the output a staging entry belongs to.
fn staging_prefix(path: &Path) -> String {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    format!(".{name}.")
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
    let read_report = read(
        input_format,
        std::io::BufReader::new(file),
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
        let err = read(
            Format::Turtle,
            std::io::Cursor::new(b""),
            &mut store,
            &Options::default(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, ConvertError::CannotRead(Format::Turtle)),
            "{msg}"
        );
        assert!(msg.contains("Turtle") && msg.contains(".sgsnap"), "{msg}");
        // A format with no writer yet. Not GraphML: that one writes (#1914).
        let err = write(Format::Turtle, &store, Vec::new(), &Options::default()).unwrap_err();
        assert!(
            matches!(err, ConvertError::CannotWrite(Format::Turtle)),
            "{err}"
        );
    }
}
