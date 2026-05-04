//! CIViC — Clinical Interpretations of Variants in Cancer.
//!
//! Loads CIViC nightly TSV releases into the graph. First slice covers
//! variants and their relation to genes. Evidence items, assertions,
//! drugs, and diseases follow in subsequent passes.
//!
//! Source: https://civicdb.org/downloads/nightly/  (CC0 1.0)
//!
//! Schema (this slice):
//!   :Variant      (civic_variant_id PK, name, hgvs, chromosome, start, stop,
//!                  ref_bases, variant_bases, ensembl_version,
//!                  reference_build, civic_evidence_score)
//!   :HAS_VARIANT  (Gene -> Variant) — Gene resolved by symbol against
//!                 existing :Gene nodes (loaded by the HGNC pass)
//!   :SAME_AS      (Variant -> existing :Variant) when ClinVar IDs match

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;

use samyama_sdk::{GraphStore, NodeId, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LoadResult {
    pub variant_nodes: usize,
    pub has_variant_edges: usize,
    pub same_as_edges: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CivicVariantRow {
    pub civic_variant_id: String,
    pub gene_symbol: String,
    pub variant_name: String,
    pub chromosome: String,
    pub start: Option<i64>,
    pub stop: Option<i64>,
    pub ref_bases: String,
    pub variant_bases: String,
    pub reference_build: String,
    pub ensembl_version: String,
    pub hgvs_descriptions: Vec<String>,
    pub clinvar_ids: Vec<String>,
    pub civic_evidence_score: Option<f64>,
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

pub fn parse_header(line: &str) -> HashMap<&str, usize> {
    line.split('\t').enumerate().map(|(i, c)| (c, i)).collect()
}

/// Split a comma-delimited cell into trimmed, non-empty entries.
/// CIViC uses commas inside cells for HGVS descriptions, ClinVar IDs, etc.
pub fn split_csv_cell(s: &str) -> Vec<String> {
    if s.trim().is_empty() || s.trim().eq_ignore_ascii_case("n/a") {
        return Vec::new();
    }
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty() && !x.eq_ignore_ascii_case("n/a"))
        .collect()
}

pub fn parse_variant_row(
    headers: &HashMap<&str, usize>,
    fields: &[&str],
) -> Option<CivicVariantRow> {
    let get = |key: &str| -> &str {
        headers
            .get(key)
            .and_then(|i| fields.get(*i))
            .copied()
            .unwrap_or("")
    };
    let civic_variant_id = get("variant_id").to_string();
    let gene_symbol = get("gene").to_string();
    if civic_variant_id.is_empty() || gene_symbol.is_empty() {
        return None;
    }
    let parse_pos = |s: &str| s.trim().parse::<i64>().ok();
    let parse_score = |s: &str| s.trim().parse::<f64>().ok();
    Some(CivicVariantRow {
        civic_variant_id,
        gene_symbol,
        variant_name: get("variant").to_string(),
        chromosome: get("chromosome").to_string(),
        start: parse_pos(get("start")),
        stop: parse_pos(get("stop")),
        ref_bases: get("reference_bases").to_string(),
        variant_bases: get("variant_bases").to_string(),
        reference_build: get("reference_build").to_string(),
        ensembl_version: get("ensembl_version").to_string(),
        hgvs_descriptions: split_csv_cell(get("hgvs_descriptions")),
        clinvar_ids: split_csv_cell(get("clinvar_ids")),
        civic_evidence_score: parse_score(get("civic_variant_evidence_score")),
    })
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

fn set_float(g: &mut GraphStore, id: NodeId, k: &str, v: f64) {
    if let Some(n) = g.get_node_mut(id) {
        n.set_property(k, PropertyValue::Float(v));
    }
}

/// Build a lookup of existing :Variant nodes keyed by ClinVar ID, used to
/// merge CIViC variants into the existing ClinVar-derived variant layer
/// via :SAME_AS edges.
pub fn build_clinvar_index(graph: &GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: samyama_sdk::Label = "Variant".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(cid)) = node.get_property("clinvar_id") {
            if !cid.is_empty() {
                out.insert(cid.clone(), node.id);
            }
        }
    }
    out
}

/// Build a lookup of existing :Gene nodes keyed by HGNC symbol.
pub fn build_gene_symbol_index(graph: &GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: samyama_sdk::Label = "Gene".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(sym)) = node.get_property("symbol") {
            if !sym.is_empty() {
                out.insert(sym.clone(), node.id);
            }
        }
    }
    out
}

/// Insert one CIViC variant row, deduped by civic_variant_id.
pub fn upsert_variant(
    graph: &mut GraphStore,
    row: &CivicVariantRow,
    civic_to_node: &mut HashMap<String, NodeId>,
) -> (NodeId, bool) {
    if let Some(&id) = civic_to_node.get(&row.civic_variant_id) {
        return (id, false);
    }
    let id = graph.create_node("Variant");
    set_str(graph, id, "civic_variant_id", &row.civic_variant_id);
    set_str(graph, id, "name", &row.variant_name);
    set_str(graph, id, "chromosome", &row.chromosome);
    if let Some(s) = row.start {
        set_int(graph, id, "start", s);
    }
    if let Some(e) = row.stop {
        set_int(graph, id, "stop", e);
    }
    set_str(graph, id, "ref_bases", &row.ref_bases);
    set_str(graph, id, "variant_bases", &row.variant_bases);
    set_str(graph, id, "reference_build", &row.reference_build);
    set_str(graph, id, "ensembl_version", &row.ensembl_version);
    if !row.hgvs_descriptions.is_empty() {
        set_str(graph, id, "hgvs", &row.hgvs_descriptions.join(";"));
    }
    if let Some(score) = row.civic_evidence_score {
        set_float(graph, id, "civic_evidence_score", score);
    }
    civic_to_node.insert(row.civic_variant_id.clone(), id);
    (id, true)
}

pub fn load_civic_variants_tsv(
    graph: &mut GraphStore,
    path: &Path,
    gene_index: &HashMap<String, NodeId>,
    clinvar_index: &HashMap<String, NodeId>,
    max_rows: usize,
) -> Result<LoadResult, Error> {
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut civic_to_node: HashMap<String, NodeId> = HashMap::with_capacity(10_000);
    let mut result = LoadResult::default();

    let mut lines = reader.lines();
    let header_line = match lines.next() {
        Some(Ok(l)) => l,
        _ => return Err("CIViC variants: missing header".into()),
    };
    let headers = parse_header(&header_line);

    let mut count = 0usize;
    for line in lines {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let fields: Vec<&str> = line.split('\t').collect();
        let row = match parse_variant_row(&headers, &fields) {
            Some(r) => r,
            None => continue,
        };
        let (vid, is_new) = upsert_variant(graph, &row, &mut civic_to_node);
        if is_new {
            result.variant_nodes += 1;
        }
        if let Some(&gene_id) = gene_index.get(&row.gene_symbol) {
            if graph.create_edge(gene_id, vid, "HAS_VARIANT").is_ok() {
                result.has_variant_edges += 1;
            }
        }
        for cid in &row.clinvar_ids {
            if let Some(&existing) = clinvar_index.get(cid) {
                if graph.create_edge(vid, existing, "SAME_AS").is_ok() {
                    result.same_as_edges += 1;
                }
            }
        }
        count += 1;
        if max_rows > 0 && count >= max_rows {
            break;
        }
        if count % 1_000 == 0 {
            eprint!("\r  CIViC variants: {}", format_num(count));
        }
    }
    eprintln!("\r  CIViC variants: {} (done)", format_num(count));
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use samyama_sdk::GraphStore;

    const FIXTURE: &str = "variant_id\tgene\tvariant\tchromosome\tstart\tstop\treference_bases\tvariant_bases\treference_build\tensembl_version\thgvs_descriptions\tclinvar_ids\tcivic_variant_evidence_score\n\
        12\tBRCA1\tE143K\t17\t43094464\t43094464\tC\tT\tGRCh37\t75\tNM_007294.3:c.427G>A,NP_009225.1:p.Glu143Lys\t55772\t12.5\n\
        17\tTP53\tR175H\t17\t7578406\t7578406\tC\tT\tGRCh37\t75\tNM_000546.5:c.524G>A\t12345,67890\t220.0\n\
        99\t\tno-gene\t1\t100\t100\tA\tG\tGRCh37\t75\t\t\t\n";

    #[test]
    fn splits_csv_cell_handles_na_and_empty() {
        assert_eq!(split_csv_cell(""), Vec::<String>::new());
        assert_eq!(split_csv_cell("N/A"), Vec::<String>::new());
        assert_eq!(split_csv_cell("12345"), vec!["12345"]);
        assert_eq!(
            split_csv_cell("12345, 67890 ,  N/A , 11"),
            vec!["12345", "67890", "11"]
        );
    }

    #[test]
    fn parses_variant_row_with_positions_and_score() {
        let lines: Vec<&str> = FIXTURE.lines().collect();
        let headers = parse_header(lines[0]);
        let fields: Vec<&str> = lines[1].split('\t').collect();
        let row = parse_variant_row(&headers, &fields).expect("BRCA1 E143K");
        assert_eq!(row.civic_variant_id, "12");
        assert_eq!(row.gene_symbol, "BRCA1");
        assert_eq!(row.start, Some(43094464));
        assert_eq!(row.stop, Some(43094464));
        assert_eq!(row.ref_bases, "C");
        assert_eq!(row.variant_bases, "T");
        assert_eq!(row.civic_evidence_score, Some(12.5));
        assert_eq!(row.hgvs_descriptions.len(), 2);
        assert_eq!(row.clinvar_ids, vec!["55772"]);
    }

    #[test]
    fn parses_multiple_clinvar_ids() {
        let lines: Vec<&str> = FIXTURE.lines().collect();
        let headers = parse_header(lines[0]);
        let fields: Vec<&str> = lines[2].split('\t').collect();
        let row = parse_variant_row(&headers, &fields).expect("TP53");
        assert_eq!(row.clinvar_ids, vec!["12345", "67890"]);
    }

    #[test]
    fn skips_rows_with_empty_required_fields() {
        let lines: Vec<&str> = FIXTURE.lines().collect();
        let headers = parse_header(lines[0]);
        let fields: Vec<&str> = lines[3].split('\t').collect();
        assert!(parse_variant_row(&headers, &fields).is_none());
    }

    #[test]
    fn upsert_dedupes_on_civic_variant_id() {
        let mut g = GraphStore::new();
        let mut map = HashMap::new();
        let row = CivicVariantRow {
            civic_variant_id: "12".into(),
            gene_symbol: "BRCA1".into(),
            variant_name: "E143K".into(),
            chromosome: "17".into(),
            start: Some(43094464),
            stop: Some(43094464),
            ref_bases: "C".into(),
            variant_bases: "T".into(),
            reference_build: "GRCh37".into(),
            ensembl_version: "75".into(),
            hgvs_descriptions: vec!["NM_007294.3:c.427G>A".into()],
            clinvar_ids: vec!["55772".into()],
            civic_evidence_score: Some(12.5),
        };
        let (id1, new1) = upsert_variant(&mut g, &row, &mut map);
        let (id2, new2) = upsert_variant(&mut g, &row, &mut map);
        assert!(new1);
        assert!(!new2);
        assert_eq!(id1, id2);
    }

    #[test]
    fn load_creates_variants_with_gene_and_clinvar_bridges() {
        let dir = tempdir();
        let path = dir.join("civic_variants.tsv");
        std::fs::write(&path, FIXTURE).unwrap();

        let mut g = GraphStore::new();
        let brca1 = g.create_node("Gene");
        g.get_node_mut(brca1)
            .unwrap()
            .set_property("symbol", PropertyValue::String("BRCA1".into()));
        let tp53 = g.create_node("Gene");
        g.get_node_mut(tp53)
            .unwrap()
            .set_property("symbol", PropertyValue::String("TP53".into()));
        let existing_var = g.create_node("Variant");
        g.get_node_mut(existing_var)
            .unwrap()
            .set_property("clinvar_id", PropertyValue::String("55772".into()));

        let gene_idx = build_gene_symbol_index(&g);
        let clinvar_idx = build_clinvar_index(&g);
        assert_eq!(gene_idx.len(), 2);
        assert_eq!(clinvar_idx.len(), 1);

        let res = load_civic_variants_tsv(&mut g, &path, &gene_idx, &clinvar_idx, 0).unwrap();
        assert_eq!(res.variant_nodes, 2);
        assert_eq!(res.has_variant_edges, 2);
        assert_eq!(res.same_as_edges, 1);
    }

    fn tempdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("civic_test_{}_{}", std::process::id(), rand_suffix()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        format!("{}", nanos)
    }
}
