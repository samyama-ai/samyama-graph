//! PubMed — Flat file loader for parsed PubMed baseline data.
//!
//! Reads pipe-delimited files from parse_pubmed_xml.py and loads into GraphStore.
//! Two-phase loading: all nodes first (with HashMap dedup), then all edges.
//!
//! Schema: Article, Author, MeSHTerm, Chemical, Journal, Grant
//! Edges: AUTHORED_BY, ANNOTATED_WITH, MENTIONS_CHEMICAL, PUBLISHED_IN, CITES, FUNDED_BY

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

pub struct LoadResult {
    pub total_nodes: usize,
    pub total_edges: usize,
}

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut r = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { r.push(','); }
        r.push(c);
    }
    r.chars().rev().collect()
}

pub fn format_duration(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 60.0 { format!("{:.1}s", s) }
    else if s < 3600.0 { format!("{}m {:.1}s", s as u64 / 60, s % 60.0) }
    else { format!("{}h {}m", s as u64 / 3600, (s as u64 % 3600) / 60) }
}

fn set_str(g: &mut GraphStore, id: NodeId, k: &str, v: &str) {
    if !v.is_empty() {
        if let Some(n) = g.get_node_mut(id) {
            n.set_property(k, PropertyValue::String(v.to_string()));
        }
    }
}

fn set_int(g: &mut GraphStore, id: NodeId, k: &str, v: i64) {
    if let Some(n) = g.get_node_mut(id) {
        n.set_property(k, PropertyValue::Integer(v));
    }
}

/// Process a pipe-delimited file line by line, calling handler for each row.
/// Streaming — never loads entire file into memory.
fn process_pipe_file<F>(path: &Path, max_rows: usize, mut handler: F) -> usize
where F: FnMut(&[&str])
{
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => { eprintln!("  SKIP {}: {}", path.display(), e); return 0; }
    };
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut first = true;
    let mut count = 0usize;

    for line in reader.lines() {
        let line = match line { Ok(l) => l, Err(_) => continue };
        if first { first = false; continue; } // skip header
        let fields: Vec<&str> = line.split('|').collect();
        handler(&fields);
        count += 1;
        if max_rows > 0 && count >= max_rows { break; }
    }
    count
}

pub fn load_dataset(graph: &mut GraphStore, data_dir: &str, max_articles: usize) -> Result<LoadResult, Error> {
    let t0 = Instant::now();
    let mut nc: usize = 0;
    let mut ec: usize = 0;
    let dir = Path::new(data_dir);

    // ID maps for edge creation — these are the main memory consumers
    // pmid_to_node: 40M entries × ~40 bytes = ~1.6 GB (acceptable)
    let mut pmid_to_node: HashMap<String, NodeId> = HashMap::with_capacity(40_000_000);
    let mut author_to_node: HashMap<String, NodeId> = HashMap::with_capacity(10_000_000);
    let mut mesh_to_node: HashMap<String, NodeId> = HashMap::with_capacity(30_000);
    let mut chemical_to_node: HashMap<String, NodeId> = HashMap::with_capacity(500_000);
    let mut journal_to_node: HashMap<String, NodeId> = HashMap::with_capacity(50_000);
    let mut grant_to_node: HashMap<String, NodeId> = HashMap::with_capacity(1_000_000);

    // ── Phase 1: Load articles (streaming) ───────────────────────
    eprintln!("Phase 1: Loading articles (streaming 41 GB file)...");
    let t = Instant::now();
    process_pipe_file(&dir.join("articles.txt"), max_articles, |fields| {
        if fields.len() < 6 { return; }
        let pmid = fields[0];
        if pmid.is_empty() { return; }
        if pmid_to_node.contains_key(pmid) { return; }

        let id = graph.create_node("Article");
        set_str(graph, id, "pmid", pmid);
        // Only store title (skip abstract to save ~30 GB of RAM)
        // Safe truncation: find char boundary at or before 300 bytes
        let title = {
            let s = fields[1];
            if s.len() <= 300 { s }
            else {
                let mut end = 300;
                while end > 0 && !s.is_char_boundary(end) { end -= 1; }
                &s[..end]
            }
        };
        set_str(graph, id, "title", title);
        set_str(graph, id, "pub_date", fields[4]);
        if let Ok(year) = fields[5].parse::<i64>() {
            set_int(graph, id, "pub_year", year);
        }

        // Journal → dedup
        let journal = fields[3];
        if !journal.is_empty() {
            let journal_lower = journal.to_lowercase();
            let jid = *journal_to_node.entry(journal_lower).or_insert_with(|| {
                let jid = graph.create_node("Journal");
                set_str(graph, jid, "title", journal);
                nc += 1;
                jid
            });
            let _ = graph.create_edge(id, jid, "PUBLISHED_IN");
            ec += 1;
        }

        pmid_to_node.insert(pmid.to_string(), id);
        nc += 1;

        if nc % 2_000_000 == 0 {
            eprintln!("  ... {} articles ({:.0}s, {:.0}/sec)",
                format_num(nc), t.elapsed().as_secs_f64(),
                nc as f64 / t.elapsed().as_secs_f64());
        }
    });
    eprintln!("  Article: {} nodes, Journal: {} nodes ({:.1}s)",
        format_num(pmid_to_node.len()), format_num(journal_to_node.len()), t.elapsed().as_secs_f64());

    // ── Phase 2: Load authors (streaming) ────────────────────────
    eprintln!("Phase 2: Loading authors (streaming 16 GB file)...");
    let t = Instant::now();
    let mut auth_edges = 0usize;
    process_pipe_file(&dir.join("authors.txt"), 0, |fields| {
        if fields.len() < 3 { return; }
        let pmid = fields[0];
        let last_name = fields[1];
        let fore_name = fields[2];
        if last_name.is_empty() && fore_name.is_empty() { return; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => return,
        };

        let author_key = format!("{}|{}", last_name.to_lowercase(), fore_name.to_lowercase());
        let author_nid = *author_to_node.entry(author_key).or_insert_with(|| {
            let aid = graph.create_node("Author");
            set_str(graph, aid, "name", &format!("{} {}", fore_name, last_name));
            nc += 1;
            aid
        });

        let _ = graph.create_edge(article_nid, author_nid, "AUTHORED_BY");
        ec += 1; auth_edges += 1;

        if auth_edges % 10_000_000 == 0 {
            eprintln!("  ... {} AUTHORED_BY edges ({:.0}s)", format_num(auth_edges), t.elapsed().as_secs_f64());
        }
    });
    eprintln!("  Author: {} nodes, AUTHORED_BY: {} edges ({:.1}s)",
        format_num(author_to_node.len()), format_num(auth_edges), t.elapsed().as_secs_f64());

    // ── Phase 3: Load MeSH terms (streaming) ─────────────────────
    eprintln!("Phase 3: Loading MeSH terms (streaming 11 GB file)...");
    let t = Instant::now();
    let mut mesh_edges = 0usize;
    process_pipe_file(&dir.join("mesh_terms.txt"), 0, |fields| {
        if fields.len() < 3 { return; }
        let pmid = fields[0];
        let desc_id = fields[1];
        let desc_name = fields[2];
        if desc_name.is_empty() { return; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => return,
        };

        let mesh_key = desc_name.to_lowercase();
        let mesh_nid = *mesh_to_node.entry(mesh_key).or_insert_with(|| {
            let mid = graph.create_node("MeSHTerm");
            set_str(graph, mid, "descriptor_id", desc_id);
            set_str(graph, mid, "name", desc_name);
            nc += 1;
            mid
        });

        let _ = graph.create_edge(article_nid, mesh_nid, "ANNOTATED_WITH");
        ec += 1; mesh_edges += 1;

        if mesh_edges % 20_000_000 == 0 {
            eprintln!("  ... {} ANNOTATED_WITH edges ({:.0}s)", format_num(mesh_edges), t.elapsed().as_secs_f64());
        }
    });
    eprintln!("  MeSHTerm: {} nodes, ANNOTATED_WITH: {} edges ({:.1}s)",
        format_num(mesh_to_node.len()), format_num(mesh_edges), t.elapsed().as_secs_f64());

    // ── Phase 4: Load chemicals (streaming) ──────────────────────
    eprintln!("Phase 4: Loading chemicals (streaming 2 GB file)...");
    let t = Instant::now();
    let mut chem_edges = 0usize;
    process_pipe_file(&dir.join("chemicals.txt"), 0, |fields| {
        if fields.len() < 3 { return; }
        let pmid = fields[0];
        let reg_num = fields[1];
        let substance = fields[2];
        if substance.is_empty() { return; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => return,
        };

        let chem_key = substance.to_lowercase();
        let chem_nid = *chemical_to_node.entry(chem_key).or_insert_with(|| {
            let cid = graph.create_node("Chemical");
            set_str(graph, cid, "registry_number", reg_num);
            set_str(graph, cid, "name", substance);
            nc += 1;
            cid
        });

        let _ = graph.create_edge(article_nid, chem_nid, "MENTIONS_CHEMICAL");
        ec += 1; chem_edges += 1;
    });
    eprintln!("  Chemical: {} nodes, MENTIONS_CHEMICAL: {} edges ({:.1}s)",
        format_num(chemical_to_node.len()), format_num(chem_edges), t.elapsed().as_secs_f64());

    // ── Phase 5: Load citations (streaming) ──────────────────────
    eprintln!("Phase 5: Loading citations (streaming 6.6 GB file)...");
    let t = Instant::now();
    let mut cite_edges = 0usize;
    process_pipe_file(&dir.join("citations.txt"), 0, |fields| {
        if fields.len() < 2 { return; }
        let citing = fields[0];
        let cited = fields[1];

        let citing_nid = match pmid_to_node.get(citing) {
            Some(&nid) => nid,
            None => return,
        };
        let cited_nid = match pmid_to_node.get(cited) {
            Some(&nid) => nid,
            None => return,
        };

        let _ = graph.create_edge(citing_nid, cited_nid, "CITES");
        ec += 1; cite_edges += 1;

        if cite_edges % 20_000_000 == 0 {
            eprintln!("  ... {} CITES edges ({:.0}s)", format_num(cite_edges), t.elapsed().as_secs_f64());
        }
    });
    eprintln!("  CITES: {} edges ({:.1}s)", format_num(cite_edges), t.elapsed().as_secs_f64());

    // ── Phase 6: Load grants (streaming) ─────────────────────────
    eprintln!("Phase 6: Loading grants (streaming 739 MB file)...");
    let t = Instant::now();
    let mut grant_edges = 0usize;
    process_pipe_file(&dir.join("grants.txt"), 0, |fields| {
        if fields.len() < 3 { return; }
        let pmid = fields[0];
        let grant_id = fields[1];
        let agency = fields[2];
        if grant_id.is_empty() { return; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => return,
        };

        let grant_key = grant_id.to_lowercase();
        let grant_nid = *grant_to_node.entry(grant_key).or_insert_with(|| {
            let gid = graph.create_node("Grant");
            set_str(graph, gid, "grant_id", grant_id);
            set_str(graph, gid, "agency", agency);
            nc += 1;
            gid
        });

        let _ = graph.create_edge(article_nid, grant_nid, "FUNDED_BY");
        ec += 1; grant_edges += 1;
    });
    eprintln!("  Grant: {} nodes, FUNDED_BY: {} edges ({:.1}s)",
        format_num(grant_to_node.len()), format_num(grant_edges), t.elapsed().as_secs_f64());

    // ── Summary ──────────────────────────────────────────────────
    let elapsed = t0.elapsed();
    eprintln!();
    eprintln!("Load complete in {}", format_duration(elapsed));
    eprintln!("  Articles:    {}", format_num(pmid_to_node.len()));
    eprintln!("  Authors:     {}", format_num(author_to_node.len()));
    eprintln!("  MeSH terms:  {}", format_num(mesh_to_node.len()));
    eprintln!("  Chemicals:   {}", format_num(chemical_to_node.len()));
    eprintln!("  Journals:    {}", format_num(journal_to_node.len()));
    eprintln!("  Grants:      {}", format_num(grant_to_node.len()));

    Ok(LoadResult { total_nodes: nc, total_edges: ec })
}
