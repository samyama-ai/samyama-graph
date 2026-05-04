//! HGNC + Ensembl — canonical Gene/Transcript identity layer.
//!
//! HGNC slice first; Ensembl GFF transcript layer follows in a second pass.
//!
//! Schema:
//!   :Gene    (hgnc_id PK, symbol, name, locus_group, locus_type, chromosome)
//!   :SAME_AS (Gene -> existing :Protein, props: source="HGNC")
//!
//! HGNC source: hgnc_complete_set.txt (TSV, header row, ~45K rows).
//! Columns we care about: hgnc_id, symbol, name, locus_group, locus_type,
//! location, uniprot_ids (pipe-delimited within the field).

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LoadResult {
    pub gene_nodes: usize,
    pub same_as_edges: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct HgncRow {
    pub hgnc_id: String,
    pub symbol: String,
    pub name: String,
    pub locus_group: String,
    pub locus_type: String,
    pub chromosome: String,
    pub uniprot_ids: Vec<String>,
}

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut r = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            r.push(',');
        }
        r.push(c);
    }
    r.chars().rev().collect()
}

pub fn format_duration(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s < 60.0 {
        format!("{:.1}s", s)
    } else if s < 3600.0 {
        format!("{}m {:.1}s", s as u64 / 60, s % 60.0)
    } else {
        format!("{}h {}m", s as u64 / 3600, (s as u64 % 3600) / 60)
    }
}

/// Parse the HGNC `location` field down to a chromosome name.
/// Examples: "11q13.1" -> "11", "Xp22.33" -> "X", "mitochondria" -> "MT".
pub fn parse_chromosome(location: &str) -> String {
    let trimmed = location.trim();
    if trimmed.is_empty() || trimmed == "reserved" || trimmed == "unplaced" {
        return String::new();
    }
    if trimmed.eq_ignore_ascii_case("mitochondria") {
        return "MT".to_string();
    }
    let mut out = String::new();
    for c in trimmed.chars() {
        if c == 'p' || c == 'q' || c == ' ' || c == '.' || c.is_ascii_digit() && !out.is_empty() && out.ends_with(|x: char| x == 'p' || x == 'q') {
            break;
        }
        out.push(c);
    }
    // Strip trailing band digits (e.g. "11q13" -> we already broke at 'q', but
    // also handle pure-digit chromosomes that fell through).
    out.trim_end_matches(|c: char| c == 'p' || c == 'q').to_string()
}

/// Parse a single HGNC TSV header + data row pair into a typed row.
/// Returns None if required fields are missing/empty.
pub fn parse_row(headers: &HashMap<&str, usize>, fields: &[&str]) -> Option<HgncRow> {
    let get = |key: &str| -> &str {
        headers
            .get(key)
            .and_then(|i| fields.get(*i))
            .copied()
            .unwrap_or("")
    };
    let hgnc_id = get("hgnc_id").to_string();
    let symbol = get("symbol").to_string();
    if hgnc_id.is_empty() || symbol.is_empty() {
        return None;
    }
    let uniprot_field = get("uniprot_ids");
    let uniprot_ids = if uniprot_field.is_empty() {
        Vec::new()
    } else {
        uniprot_field
            .split('|')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    Some(HgncRow {
        hgnc_id,
        symbol,
        name: get("name").to_string(),
        locus_group: get("locus_group").to_string(),
        locus_type: get("locus_type").to_string(),
        chromosome: parse_chromosome(get("location")),
        uniprot_ids,
    })
}

/// Build a header-name -> column-index map from the first line of an HGNC TSV.
pub fn parse_header(line: &str) -> HashMap<&str, usize> {
    line.split('\t').enumerate().map(|(i, c)| (c, i)).collect()
}

fn set_str(g: &mut GraphStore, id: NodeId, k: &str, v: &str) {
    if !v.is_empty() {
        if let Some(n) = g.get_node_mut(id) {
            n.set_property(k, PropertyValue::String(v.to_string()));
        }
    }
}

/// Insert a single HGNC row as a :Gene node, deduped on HGNC ID.
/// Returns the NodeId (newly created or pre-existing) and whether it was new.
pub fn upsert_gene(
    graph: &mut GraphStore,
    row: &HgncRow,
    hgnc_to_node: &mut HashMap<String, NodeId>,
) -> (NodeId, bool) {
    if let Some(&id) = hgnc_to_node.get(&row.hgnc_id) {
        return (id, false);
    }
    let id = graph.create_node("Gene");
    set_str(graph, id, "hgnc_id", &row.hgnc_id);
    set_str(graph, id, "symbol", &row.symbol);
    set_str(graph, id, "name", &row.name);
    set_str(graph, id, "locus_group", &row.locus_group);
    set_str(graph, id, "locus_type", &row.locus_type);
    set_str(graph, id, "chromosome", &row.chromosome);
    hgnc_to_node.insert(row.hgnc_id.clone(), id);
    (id, true)
}

/// Stream an HGNC TSV file into the graph. `uniprot_to_node` is an optional
/// pre-populated map (UniProt accession -> :Protein NodeId) from the existing
/// v1.0 KG; when present, :SAME_AS edges are created.
pub fn load_hgnc_tsv(
    graph: &mut GraphStore,
    path: &Path,
    uniprot_to_node: Option<&HashMap<String, NodeId>>,
    max_rows: usize,
) -> Result<LoadResult, Error> {
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut hgnc_to_node: HashMap<String, NodeId> = HashMap::with_capacity(50_000);
    let mut result = LoadResult::default();

    let mut lines = reader.lines();
    let header_line = match lines.next() {
        Some(Ok(l)) => l,
        _ => return Err("HGNC TSV: missing header".into()),
    };
    let headers = parse_header(&header_line);

    let mut count = 0usize;
    for line in lines {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let fields: Vec<&str> = line.split('\t').collect();
        let row = match parse_row(&headers, &fields) {
            Some(r) => r,
            None => continue,
        };
        let (gene_id, is_new) = upsert_gene(graph, &row, &mut hgnc_to_node);
        if is_new {
            result.gene_nodes += 1;
        }
        if let Some(map) = uniprot_to_node {
            for acc in &row.uniprot_ids {
                if let Some(&pid) = map.get(acc) {
                    if graph.create_edge(gene_id, pid, "SAME_AS").is_ok() {
                        result.same_as_edges += 1;
                    }
                }
            }
        }
        count += 1;
        if max_rows > 0 && count >= max_rows {
            break;
        }
        if count % 10_000 == 0 {
            eprint!("\r  HGNC rows: {}", format_num(count));
        }
    }
    eprintln!("\r  HGNC rows: {} (done)", format_num(count));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use samyama_sdk::GraphStore;

    const FIXTURE: &str = "hgnc_id\tsymbol\tname\tlocus_group\tlocus_type\tlocation\tuniprot_ids\n\
        HGNC:1100\tBRCA1\tBRCA1 DNA repair associated\tprotein-coding gene\tgene with protein product\t17q21.31\tP38398\n\
        HGNC:1101\tBRCA2\tBRCA2 DNA repair associated\tprotein-coding gene\tgene with protein product\t13q13.1\tP51587\n\
        HGNC:11998\tTP53\ttumor protein p53\tprotein-coding gene\tgene with protein product\t17p13.1\tP04637|Q53GA5\n\
        HGNC:9999\t\tno-symbol-row\tprotein-coding gene\tgene with protein product\t1q21\tQ12345\n";

    #[test]
    fn parses_chromosome_from_location() {
        assert_eq!(parse_chromosome("17q21.31"), "17");
        assert_eq!(parse_chromosome("Xp22.33"), "X");
        assert_eq!(parse_chromosome("13q13.1"), "13");
        assert_eq!(parse_chromosome("mitochondria"), "MT");
        assert_eq!(parse_chromosome("reserved"), "");
        assert_eq!(parse_chromosome(""), "");
    }

    #[test]
    fn parses_header_into_index_map() {
        let h = parse_header("hgnc_id\tsymbol\tname");
        assert_eq!(h.get("hgnc_id"), Some(&0));
        assert_eq!(h.get("symbol"), Some(&1));
        assert_eq!(h.get("name"), Some(&2));
    }

    #[test]
    fn parses_well_formed_row() {
        let lines: Vec<&str> = FIXTURE.lines().collect();
        let headers = parse_header(lines[0]);
        let fields: Vec<&str> = lines[1].split('\t').collect();
        let row = parse_row(&headers, &fields).expect("BRCA1 row");
        assert_eq!(row.hgnc_id, "HGNC:1100");
        assert_eq!(row.symbol, "BRCA1");
        assert_eq!(row.chromosome, "17");
        assert_eq!(row.uniprot_ids, vec!["P38398"]);
    }

    #[test]
    fn parses_pipe_delimited_uniprot_ids() {
        let lines: Vec<&str> = FIXTURE.lines().collect();
        let headers = parse_header(lines[0]);
        let fields: Vec<&str> = lines[3].split('\t').collect();
        let row = parse_row(&headers, &fields).expect("TP53 row");
        assert_eq!(row.uniprot_ids, vec!["P04637", "Q53GA5"]);
    }

    #[test]
    fn skips_rows_with_empty_required_fields() {
        let lines: Vec<&str> = FIXTURE.lines().collect();
        let headers = parse_header(lines[0]);
        let fields: Vec<&str> = lines[4].split('\t').collect();
        assert!(parse_row(&headers, &fields).is_none(), "row missing symbol must be rejected");
    }

    #[test]
    fn upsert_dedupes_on_hgnc_id() {
        let mut g = GraphStore::new();
        let mut map = HashMap::new();
        let row = HgncRow {
            hgnc_id: "HGNC:1100".into(),
            symbol: "BRCA1".into(),
            name: "BRCA1 DNA repair associated".into(),
            locus_group: "protein-coding gene".into(),
            locus_type: "gene with protein product".into(),
            chromosome: "17".into(),
            uniprot_ids: vec!["P38398".into()],
        };
        let (id1, new1) = upsert_gene(&mut g, &row, &mut map);
        let (id2, new2) = upsert_gene(&mut g, &row, &mut map);
        assert!(new1);
        assert!(!new2);
        assert_eq!(id1, id2);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn load_creates_gene_nodes_and_same_as_edges() {
        let dir = tempdir();
        let path = dir.join("hgnc_complete_set.txt");
        std::fs::write(&path, FIXTURE).unwrap();

        let mut g = GraphStore::new();
        // Pre-populate one Protein so SAME_AS edge fires for BRCA1.
        let prot_id = g.create_node("Protein");
        g.get_node_mut(prot_id)
            .unwrap()
            .set_property("accession", PropertyValue::String("P38398".into()));
        let mut uniprot_map = HashMap::new();
        uniprot_map.insert("P38398".to_string(), prot_id);

        let result = load_hgnc_tsv(&mut g, &path, Some(&uniprot_map), 0).unwrap();
        // Three valid gene rows (BRCA1, BRCA2, TP53); the no-symbol row is skipped.
        assert_eq!(result.gene_nodes, 3);
        // Only BRCA1's UniProt accession is in the bridge map.
        assert_eq!(result.same_as_edges, 1);
    }

    fn tempdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("hgnc_test_{}_{}", std::process::id(), rand_suffix()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        format!("{}", nanos)
    }
}
