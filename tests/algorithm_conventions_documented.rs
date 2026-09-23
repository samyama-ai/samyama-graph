//! Every algorithm a user can call has its conventions written down (ALGO-03).
//!
//! Directedness, weights, self-loops, disconnected components, tie-breaking and
//! normalisation are choices, not consequences: two correct implementations that
//! choose differently return different numbers from the same graph. An algorithm
//! that ships without them documented is one a user cannot compare against
//! anything, so shipping one fails here rather than at the next comparison.
//!
//! The list of algorithms is read from `is_algorithm`'s own match arms, which is
//! the dispatcher's list rather than a second copy of it. A copy would go stale
//! silently and this test would then pass by agreeing with itself.
//!
//! Text, not behaviour. That a row *says* self-loops are skipped is not evidence
//! that they are; `CH-ALGO-PARITY` compares our answers against a reference and
//! is where a wrong convention is caught. A missing one is caught here.

use std::collections::BTreeSet;
use std::path::Path;

const DOC: &str = "docs/ALGORITHM-CONVENTIONS.md";
const AXES: [&str; 6] = [
    "directedness", "weights", "self-loops", "disconnected", "tie-breaking", "normalisation",
];

/// `algo.pageRank`, `pagerank` and `page_rank` are one algorithm, so compare on
/// the same shape the dispatcher compares on: no namespace, no separators, no case.
fn canonical(name: &str) -> String {
    let bare = name.trim().trim_matches('`');
    let bare = match bare.rsplit_once('.') {
        Some((prefix, tail)) if prefix != "or" => tail,
        _ => bare,
    };
    bare.chars().filter(|c| c.is_alphanumeric()).flat_map(|c| c.to_lowercase()).collect()
}

/// The names `is_algorithm` will dispatch, read from its match arms.
fn dispatchable(src: &str) -> BTreeSet<String> {
    let start = src.find("pub fn is_algorithm").expect("is_algorithm not found");
    let body = &src[start..];
    let end = body.find("\n    }").expect("is_algorithm has no end");
    let mut out = BTreeSet::new();
    for chunk in body[..end].split('"').skip(1).step_by(2) {
        if !chunk.is_empty() && chunk.chars().all(|c| c.is_ascii_lowercase() || c == '.' || c.is_ascii_digit()) {
            out.insert(chunk.to_string());
        }
    }
    assert!(out.len() > 40, "read only {} names from is_algorithm", out.len());
    out
}

/// Canonical algorithm name -> the axes its row fills in.
///
/// Axes are located by their heading, not by column position: a table that grows
/// a column would otherwise shift every axis one to the left and keep scoring.
fn documented(doc: &str) -> std::collections::BTreeMap<String, BTreeSet<String>> {
    let mut rows = std::collections::BTreeMap::new();
    let mut cols: Vec<(String, usize)> = Vec::new();
    let mut name_col = None;
    let mut header_cells = 0usize;
    // Indexed, so a row can look at the line after it. A markdown header is
    // only a header because a `|---|---|` separator follows it, and that is
    // what distinguishes "the start of a different table" from "a malformed
    // row of this one".
    let pipe_lines: Vec<&str> = doc
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with('|'))
        .collect();
    for (idx, line) in pipe_lines.iter().enumerate() {
        let line = *line;
        let followed_by_separator = pipe_lines
            .get(idx + 1)
            .map(|next| {
                next.trim_matches('|')
                    .chars()
                    .all(|c| c == '-' || c == ':' || c == ' ' || c == '|')
            })
            .unwrap_or(false);
        // A header for some other table ends the algorithm table. Without
        // this, any second table in the document was read as a run of
        // malformed algorithm rows: the undirected-multiplicity table added
        // for samyama-graph#1308 has four columns against the algorithm
        // header's eight, and the shift check -- correctly, for what it
        // thought it was reading -- called it a broken row.
        if followed_by_separator && !line.to_lowercase().contains("cypher name") {
            name_col = None;
            continue;
        }
        // `\|` is a literal pipe inside a cell, not a cell boundary -- the
        // similarity rows carry `sqrt(\|A\|\|B\|)`. Splitting on every pipe
        // reads those rows with their columns shifted.
        let masked = line.replace("\\|", "\u{1}");
        let cells: Vec<String> = masked
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim().replace('\u{1}', "|"))
            .collect();
        let cells: Vec<&str> = cells.iter().map(String::as_str).collect();
        let heads: Vec<String> = cells.iter().map(|c| c.to_lowercase().replace('*', "").trim().to_string()).collect();
        if let Some(i) = heads.iter().position(|h| h.starts_with("cypher name")) {
            name_col = Some(i);
            header_cells = cells.len();
            cols = AXES.iter()
                .filter_map(|axis| heads.iter().position(|h| h.starts_with(axis)).map(|i| (axis.to_string(), i)))
                .collect();
            continue;
        }
        if cells[0].chars().all(|c| c == '-' || c == ':' || c == ' ') {
            continue; // the |---|---| separator
        }
        let Some(nc) = name_col else { continue };
        // A row with a different number of cells from its header is a row whose
        // columns have shifted, and every axis in it is then read from the wrong
        // place. Three rows did exactly that: an unescaped `|` inside
        // `sqrt(|A||B|)` ended the cell early, and this check read the shifted
        // cells as filled and passed. A gate that cannot see a broken table is
        // not checking the table.
        assert_eq!(
            cells.len(),
            header_cells,
            "row has {} cells against the header's {} -- an unescaped `|` inside a \
             formula ends the cell: {}",
            cells.len(),
            header_cells,
            cells[nc.min(cells.len() - 1)],
        );
        // A cell that is present and says nothing is not documentation. Without
        // this, a table of em dashes scores full marks.
        let filled: BTreeSet<String> = cols.iter()
            .filter(|(_, i)| {
                let v = cells[*i].to_lowercase().replace('*', "");
                let v = v.trim();
                !matches!(v, "" | "-" | "--" | "—" | "–" | "n/a" | "na" | "tbd" | "?" | "todo")
            })
            .map(|(a, _)| a.clone())
            .collect();
        for spelling in cells[nc].split(',') {
            let c = canonical(spelling);
            if !c.is_empty() {
                rows.insert(c, filled.clone());
            }
        }
    }
    assert!(name_col.is_some(), "{DOC} has no table with a 'Cypher name' column");
    rows
}

#[test]
fn every_callable_algorithm_documents_its_conventions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = std::fs::read_to_string(root.join("src/query/executor/operator.rs")).unwrap();
    let doc = std::fs::read_to_string(root.join(DOC))
        .unwrap_or_else(|e| panic!("{DOC} is missing ({e}); ALGO-03 asks for it"));

    let rows = documented(&doc);
    let mut undocumented = Vec::new();
    let mut incomplete = Vec::new();
    for name in dispatchable(&src) {
        match rows.get(&canonical(&name)) {
            None => undocumented.push(name),
            Some(filled) if filled.len() < AXES.len() => {
                let missing: Vec<&str> =
                    AXES.iter().copied().filter(|a| !filled.contains(*a)).collect();
                incomplete.push(format!("{name} (no {})", missing.join(", ")));
            }
            Some(_) => {}
        }
    }

    assert!(
        undocumented.is_empty() && incomplete.is_empty(),
        "ALGO-03: {DOC} does not cover every algorithm the dispatcher accepts.\n\
         Add a row, or a spelling to an existing row, for: {}\n\
         Fill in the missing conventions for: {}",
        if undocumented.is_empty() { "-".into() } else { undocumented.join(", ") },
        if incomplete.is_empty() { "-".into() } else { incomplete.join("; ") },
    );
}

/// The gate has to be able to fail, and the way this one would quietly stop
/// failing is by reading zero rows and finding zero gaps in them.
#[test]
fn the_gate_notices_a_missing_row_and_an_empty_cell() {
    let table = "| Algorithm | Cypher name | Directedness | Weights | Self-loops \
                 | Disconnected | Tie-breaking | Normalisation |\n\
                 |---|---|---|---|---|---|---|---|\n\
                 | PageRank | algo.pageRank | out | ignored | counted | all | none | L1 |\n\
                 | HITS | hits | out | ignored | counted | all | none | — |\n";
    let rows = documented(table);
    assert_eq!(rows["pagerank"].len(), 6, "a full row documents six conventions");
    assert_eq!(rows["hits"].len(), 5, "an em dash is not a documented convention");
    assert!(!rows.contains_key("louvain"), "a name with no row is not documented");
}

/// The check that was missing when three rows shifted: a row whose cell count
/// does not match its header is a row read from the wrong columns, and the
/// old parser scored it as complete.
#[test]
#[should_panic(expected = "cells against the header's")]
fn an_unescaped_pipe_is_caught_rather_than_read_as_a_filled_cell() {
    let table = "| Algorithm | Cypher name | Directedness | Weights | Self-loops \
                 | Disconnected | Tie-breaking | Normalisation |\n\
                 |---|---|---|---|---|---|---|---|\n\
                 | Cosine | cosine | symmetrised | ignored | excluded | refuses \
                 | none | sqrt(|A||B|) |\n";
    documented(table);
}

/// And an escaped pipe is a pipe, not a boundary — otherwise the fix for the
/// row above would be to delete the formula.
#[test]
fn an_escaped_pipe_stays_inside_its_cell() {
    let table = "| Algorithm | Cypher name | Directedness | Weights | Self-loops \
                 | Disconnected | Tie-breaking | Normalisation |\n\
                 |---|---|---|---|---|---|---|---|\n\
                 | Cosine | cosine | symmetrised | ignored | excluded | refuses \
                 | none | sqrt(\\|A\\|\\|B\\|) |\n";
    assert_eq!(documented(table)["cosine"].len(), 6);
}
