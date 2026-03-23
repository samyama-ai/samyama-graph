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

/// Read a pipe-delimited file, skip header, return rows as Vec of field vectors.
fn read_pipe_file(path: &Path, max_rows: usize) -> Vec<Vec<String>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => { eprintln!("  SKIP {}: {}", path.display(), e); return Vec::new(); }
    };
    let reader = BufReader::with_capacity(1024 * 1024, file);
    let mut rows = Vec::new();
    let mut first = true;

    for line in reader.lines() {
        let line = match line { Ok(l) => l, Err(_) => continue };
        if first { first = false; continue; } // skip header
        let fields: Vec<String> = line.split('|').map(|s| s.to_string()).collect();
        rows.push(fields);
        if max_rows > 0 && rows.len() >= max_rows { break; }
    }
    rows
}

pub fn load_dataset(graph: &mut GraphStore, data_dir: &str, max_articles: usize) -> Result<LoadResult, Error> {
    let t0 = Instant::now();
    let mut nc: usize = 0;
    let mut ec: usize = 0;
    let dir = Path::new(data_dir);

    // ID maps for edge creation
    let mut pmid_to_node: HashMap<String, NodeId> = HashMap::new();
    let mut author_to_node: HashMap<String, NodeId> = HashMap::new();
    let mut mesh_to_node: HashMap<String, NodeId> = HashMap::new();
    let mut chemical_to_node: HashMap<String, NodeId> = HashMap::new();
    let mut journal_to_node: HashMap<String, NodeId> = HashMap::new();
    let mut grant_to_node: HashMap<String, NodeId> = HashMap::new();

    // ── Phase 1: Create indexes ──────────────────────────────────
    eprintln!("Phase 0: Creating indexes...");
    // Indexes are implicit in GraphStore (label_index)

    // ── Phase 1: Load articles ───────────────────────────────────
    eprintln!("Phase 1: Loading articles...");
    let t = Instant::now();
    let articles = read_pipe_file(&dir.join("articles.txt"), max_articles);
    for row in &articles {
        if row.len() < 6 { continue; }
        let pmid = &row[0];
        if pmid.is_empty() || pmid_to_node.contains_key(pmid) { continue; }

        let id = graph.create_node("Article");
        set_str(graph, id, "pmid", pmid);
        set_str(graph, id, "title", &row[1]);
        // Truncate abstract to save memory at scale
        let abstract_text = if row[2].len() > 500 { &row[2][..500] } else { &row[2] };
        set_str(graph, id, "abstract", abstract_text);
        set_str(graph, id, "pub_date", &row[4]);
        if let Ok(year) = row[5].parse::<i64>() {
            set_int(graph, id, "pub_year", year);
        }

        // Journal → dedup and create edge
        let journal = &row[3];
        if !journal.is_empty() {
            let journal_lower = journal.to_lowercase();
            let jid = *journal_to_node.entry(journal_lower.clone()).or_insert_with(|| {
                let jid = graph.create_node("Journal");
                set_str(graph, jid, "title", journal);
                nc += 1;
                jid
            });
            if let Ok(_) = graph.create_edge(id, jid, "PUBLISHED_IN") { ec += 1; }
        }

        pmid_to_node.insert(pmid.clone(), id);
        nc += 1;

        if nc % 1_000_000 == 0 {
            eprintln!("  ... {} articles loaded ({:.1}s)", format_num(nc), t.elapsed().as_secs_f64());
        }
    }
    eprintln!("  Article: {} nodes, Journal: {} nodes ({:.1}s)",
        format_num(pmid_to_node.len()), format_num(journal_to_node.len()), t.elapsed().as_secs_f64());

    // ── Phase 2: Load authors + AUTHORED_BY edges ────────────────
    eprintln!("Phase 2: Loading authors...");
    let t = Instant::now();
    let authors = read_pipe_file(&dir.join("authors.txt"), 0);
    let mut auth_edges = 0usize;

    for row in &authors {
        if row.len() < 3 { continue; }
        let pmid = &row[0];
        let last_name = &row[1];
        let fore_name = &row[2];
        if last_name.is_empty() && fore_name.is_empty() { continue; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => continue,
        };

        // Dedup author by "last|fore"
        let author_key = format!("{}|{}", last_name.to_lowercase(), fore_name.to_lowercase());
        let author_nid = *author_to_node.entry(author_key.clone()).or_insert_with(|| {
            let aid = graph.create_node("Author");
            set_str(graph, aid, "last_name", last_name);
            set_str(graph, aid, "fore_name", fore_name);
            let full_name = if fore_name.is_empty() { last_name.clone() }
                else { format!("{} {}", fore_name, last_name) };
            set_str(graph, aid, "name", &full_name);
            nc += 1;
            aid
        });

        if let Ok(_) = graph.create_edge(article_nid, author_nid, "AUTHORED_BY") {
            ec += 1; auth_edges += 1;
        }

        if auth_edges % 5_000_000 == 0 && auth_edges > 0 {
            eprintln!("  ... {} AUTHORED_BY edges ({:.1}s)", format_num(auth_edges), t.elapsed().as_secs_f64());
        }
    }
    eprintln!("  Author: {} nodes, AUTHORED_BY: {} edges ({:.1}s)",
        format_num(author_to_node.len()), format_num(auth_edges), t.elapsed().as_secs_f64());

    // ── Phase 3: Load MeSH terms + ANNOTATED_WITH edges ──────────
    eprintln!("Phase 3: Loading MeSH terms...");
    let t = Instant::now();
    let mesh_rows = read_pipe_file(&dir.join("mesh_terms.txt"), 0);
    let mut mesh_edges = 0usize;

    for row in &mesh_rows {
        if row.len() < 4 { continue; }
        let pmid = &row[0];
        let desc_id = &row[1];
        let desc_name = &row[2];
        let is_major = &row[3];
        if desc_name.is_empty() { continue; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => continue,
        };

        let mesh_key = desc_name.to_lowercase();
        let mesh_nid = *mesh_to_node.entry(mesh_key.clone()).or_insert_with(|| {
            let mid = graph.create_node("MeSHTerm");
            set_str(graph, mid, "descriptor_id", desc_id);
            set_str(graph, mid, "name", desc_name);
            nc += 1;
            mid
        });

        if let Ok(_) = graph.create_edge(article_nid, mesh_nid, "ANNOTATED_WITH") {
            ec += 1; mesh_edges += 1;
        }

        if mesh_edges % 10_000_000 == 0 && mesh_edges > 0 {
            eprintln!("  ... {} ANNOTATED_WITH edges ({:.1}s)", format_num(mesh_edges), t.elapsed().as_secs_f64());
        }
    }
    eprintln!("  MeSHTerm: {} nodes, ANNOTATED_WITH: {} edges ({:.1}s)",
        format_num(mesh_to_node.len()), format_num(mesh_edges), t.elapsed().as_secs_f64());

    // ── Phase 4: Load chemicals + MENTIONS_CHEMICAL edges ────────
    eprintln!("Phase 4: Loading chemicals...");
    let t = Instant::now();
    let chem_rows = read_pipe_file(&dir.join("chemicals.txt"), 0);
    let mut chem_edges = 0usize;

    for row in &chem_rows {
        if row.len() < 3 { continue; }
        let pmid = &row[0];
        let reg_num = &row[1];
        let substance = &row[2];
        if substance.is_empty() { continue; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => continue,
        };

        let chem_key = substance.to_lowercase();
        let chem_nid = *chemical_to_node.entry(chem_key.clone()).or_insert_with(|| {
            let cid = graph.create_node("Chemical");
            set_str(graph, cid, "registry_number", reg_num);
            set_str(graph, cid, "name", substance);
            nc += 1;
            cid
        });

        if let Ok(_) = graph.create_edge(article_nid, chem_nid, "MENTIONS_CHEMICAL") {
            ec += 1; chem_edges += 1;
        }
    }
    eprintln!("  Chemical: {} nodes, MENTIONS_CHEMICAL: {} edges ({:.1}s)",
        format_num(chemical_to_node.len()), format_num(chem_edges), t.elapsed().as_secs_f64());

    // ── Phase 5: Load citations (CITES edges) ────────────────────
    eprintln!("Phase 5: Loading citations...");
    let t = Instant::now();
    let cite_rows = read_pipe_file(&dir.join("citations.txt"), 0);
    let mut cite_edges = 0usize;

    for row in &cite_rows {
        if row.len() < 2 { continue; }
        let citing = &row[0];
        let cited = &row[1];

        let citing_nid = match pmid_to_node.get(citing) {
            Some(&nid) => nid,
            None => continue,
        };
        let cited_nid = match pmid_to_node.get(cited) {
            Some(&nid) => nid,
            None => continue, // cited article not in our dataset
        };

        if let Ok(_) = graph.create_edge(citing_nid, cited_nid, "CITES") {
            ec += 1; cite_edges += 1;
        }
    }
    eprintln!("  CITES: {} edges ({:.1}s)", format_num(cite_edges), t.elapsed().as_secs_f64());

    // ── Phase 6: Load grants + FUNDED_BY edges ───────────────────
    eprintln!("Phase 6: Loading grants...");
    let t = Instant::now();
    let grant_rows = read_pipe_file(&dir.join("grants.txt"), 0);
    let mut grant_edges = 0usize;

    for row in &grant_rows {
        if row.len() < 4 { continue; }
        let pmid = &row[0];
        let grant_id = &row[1];
        let agency = &row[2];
        if grant_id.is_empty() { continue; }

        let article_nid = match pmid_to_node.get(pmid) {
            Some(&nid) => nid,
            None => continue,
        };

        let grant_key = grant_id.to_lowercase();
        let grant_nid = *grant_to_node.entry(grant_key.clone()).or_insert_with(|| {
            let gid = graph.create_node("Grant");
            set_str(graph, gid, "grant_id", grant_id);
            set_str(graph, gid, "agency", agency);
            nc += 1;
            gid
        });

        if let Ok(_) = graph.create_edge(article_nid, grant_nid, "FUNDED_BY") {
            ec += 1; grant_edges += 1;
        }
    }
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
