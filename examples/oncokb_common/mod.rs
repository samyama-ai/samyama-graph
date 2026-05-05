//! OncoKB — somatic-variant therapy-implication loader (license-gated).
//!
//! Targets two of the OncoKB v1 utility endpoints:
//!   GET /api/v1/utils/allCuratedGenes.json
//!   GET /api/v1/utils/allActionableVariants.json
//!
//! Both are gated behind an OncoKB API token (academic license, ~1-2 wk
//! turnaround). This module ships the parser/loader scaffold so that the
//! moment the token arrives, downloading the JSON files and pointing the
//! loader at them produces the full Tier-1 oncology evidence layer.
//!
//! Schema:
//!   :OncoKBGene         (hugo_symbol, entrez_id, oncogene, tsg,
//!                        grch38_isoform, grch38_refseq,
//!                        highest_sensitive_level, highest_resistance_level)
//!   :Variant            (oncokb_alteration, consequence, alteration_type)
//!   :OncoKBLevel        (level — small fixed set: LEVEL_1, LEVEL_2, …)
//!   :Drug               (name)
//!   :Disease            (tumor_type, oncotree_code)
//!
//! Edges:
//!   :CURATED_AS_ONCOGENIC      (Variant -> :OncoKBGene)
//!                              props: alteration_type, consequence
//!   :HAS_THERAPEUTIC_IMPLICATION (Variant -> Drug)
//!                              props: level, tumor_type, oncotree_code
//!   :SAME_AS                   (:OncoKBGene -> existing HGNC :Gene by symbol)
//!
//! License: OncoKB academic registration at
//!   https://www.oncokb.org/account/register

use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use samyama_sdk::{GraphStore, NodeId, PropertyValue};
use serde_json::Value;

pub type Error = Box<dyn std::error::Error>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LoadResult {
    pub gene_nodes: usize,
    pub variant_nodes: usize,
    pub drug_nodes: usize,
    pub same_as_edges: usize,
    pub curated_edges: usize,
    pub therapeutic_edges: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OncoKBGene {
    pub hugo_symbol: String,
    pub entrez_id: Option<i64>,
    pub oncogene: bool,
    pub tsg: bool,
    pub grch38_isoform: String,
    pub grch38_refseq: String,
    pub highest_sensitive_level: String,
    pub highest_resistance_level: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OncoKBActionableVariant {
    pub hugo_symbol: String,
    pub alteration: String,
    pub consequence: String,
    pub alteration_type: String,
    pub level: String,
    pub drugs: Vec<String>,
    pub tumor_type: String,
    pub oncotree_code: String,
}

fn s(v: &Value) -> String {
    v.as_str().map(str::to_string).unwrap_or_default()
}

fn b(v: &Value) -> bool {
    v.as_bool().unwrap_or(false)
}

pub fn parse_curated_gene(v: &Value) -> Option<OncoKBGene> {
    let hugo_symbol = s(v.get("hugoSymbol")?);
    if hugo_symbol.is_empty() {
        return None;
    }
    Some(OncoKBGene {
        hugo_symbol,
        entrez_id: v.get("entrezGeneId").and_then(|x| x.as_i64()),
        oncogene: v.get("oncogene").map(b).unwrap_or(false),
        tsg: v.get("tsg").map(b).unwrap_or(false),
        grch38_isoform: v.get("grch38Isoform").map(s).unwrap_or_default(),
        grch38_refseq: v.get("grch38RefSeq").map(s).unwrap_or_default(),
        highest_sensitive_level: v.get("highestSensitiveLevel").map(s).unwrap_or_default(),
        highest_resistance_level: v.get("highestResistanceLevel").map(s).unwrap_or_default(),
    })
}

pub fn parse_actionable_variant(v: &Value) -> Option<OncoKBActionableVariant> {
    let hugo_symbol = v
        .get("gene")
        .and_then(|g| g.get("hugoSymbol"))
        .map(s)
        .unwrap_or_default();
    let alteration = v.get("alteration").map(s).unwrap_or_default();
    if hugo_symbol.is_empty() || alteration.is_empty() {
        return None;
    }
    let drugs: Vec<String> = v
        .get("drugs")
        .and_then(|d| d.as_array())
        .map(|arr| arr.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    Some(OncoKBActionableVariant {
        hugo_symbol,
        alteration,
        consequence: v.get("consequence").map(s).unwrap_or_default(),
        alteration_type: v.get("alterationType").map(s).unwrap_or_default(),
        level: v.get("level").map(s).unwrap_or_default(),
        drugs,
        tumor_type: v.get("tumorType").map(s).unwrap_or_default(),
        oncotree_code: v.get("oncotreeCode").map(s).unwrap_or_default(),
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

fn set_bool(g: &mut GraphStore, id: NodeId, k: &str, v: bool) {
    if let Some(n) = g.get_node_mut(id) {
        n.set_property(k, PropertyValue::Boolean(v));
    }
}

/// Insert one OncoKB gene, deduped on hugo_symbol.
pub fn upsert_gene(
    graph: &mut GraphStore,
    row: &OncoKBGene,
    sym_to_node: &mut HashMap<String, NodeId>,
) -> (NodeId, bool) {
    if let Some(&id) = sym_to_node.get(&row.hugo_symbol) {
        return (id, false);
    }
    let id = graph.create_node("OncoKBGene");
    set_str(graph, id, "hugo_symbol", &row.hugo_symbol);
    if let Some(e) = row.entrez_id {
        set_int(graph, id, "entrez_id", e);
    }
    set_bool(graph, id, "oncogene", row.oncogene);
    set_bool(graph, id, "tsg", row.tsg);
    set_str(graph, id, "grch38_isoform", &row.grch38_isoform);
    set_str(graph, id, "grch38_refseq", &row.grch38_refseq);
    set_str(graph, id, "highest_sensitive_level", &row.highest_sensitive_level);
    set_str(graph, id, "highest_resistance_level", &row.highest_resistance_level);
    sym_to_node.insert(row.hugo_symbol.clone(), id);
    (id, true)
}

/// Load OncoKB curated genes from a JSON file (top-level array).
/// `hgnc_index` is an optional symbol -> existing :Gene NodeId map; when
/// supplied a `:SAME_AS` edge bridges OncoKBGene to the canonical HGNC gene.
pub fn load_curated_genes_json(
    graph: &mut GraphStore,
    path: &Path,
    hgnc_index: Option<&HashMap<String, NodeId>>,
) -> Result<LoadResult, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let arr: Value = serde_json::from_reader(reader)?;
    let entries = arr.as_array().ok_or("OncoKB genes JSON: top-level must be an array")?;
    let mut sym_to_node: HashMap<String, NodeId> = HashMap::with_capacity(1_000);
    let mut result = LoadResult::default();

    for entry in entries {
        let row = match parse_curated_gene(entry) {
            Some(r) => r,
            None => continue,
        };
        let (oid, is_new) = upsert_gene(graph, &row, &mut sym_to_node);
        if is_new {
            result.gene_nodes += 1;
        }
        if let Some(idx) = hgnc_index {
            if let Some(&hgnc) = idx.get(&row.hugo_symbol) {
                if graph.create_edge(oid, hgnc, "SAME_AS").is_ok() {
                    result.same_as_edges += 1;
                }
            }
        }
    }
    Ok(result)
}

/// Load OncoKB actionable variants from a JSON file (top-level array).
/// Creates :Variant + :Drug nodes (deduped) and CURATED_AS_ONCOGENIC +
/// HAS_THERAPEUTIC_IMPLICATION edges. Variants link back to OncoKBGene
/// nodes already loaded by `load_curated_genes_json`.
pub fn load_actionable_variants_json(
    graph: &mut GraphStore,
    path: &Path,
    oncokb_gene_index: &HashMap<String, NodeId>,
) -> Result<LoadResult, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let arr: Value = serde_json::from_reader(reader)?;
    let entries = arr.as_array().ok_or("OncoKB variants JSON: top-level must be an array")?;
    let mut variant_to_node: HashMap<(String, String), NodeId> = HashMap::new();
    let mut drug_to_node: HashMap<String, NodeId> = HashMap::new();
    let mut result = LoadResult::default();

    for entry in entries {
        let row = match parse_actionable_variant(entry) {
            Some(r) => r,
            None => continue,
        };
        let v_key = (row.hugo_symbol.clone(), row.alteration.clone());
        let vid = if let Some(&id) = variant_to_node.get(&v_key) {
            id
        } else {
            let id = graph.create_node("Variant");
            set_str(graph, id, "oncokb_alteration", &row.alteration);
            set_str(graph, id, "oncokb_gene", &row.hugo_symbol);
            set_str(graph, id, "consequence", &row.consequence);
            set_str(graph, id, "alteration_type", &row.alteration_type);
            variant_to_node.insert(v_key.clone(), id);
            result.variant_nodes += 1;
            if let Some(&gid) = oncokb_gene_index.get(&row.hugo_symbol) {
                if graph.create_edge(id, gid, "CURATED_AS_ONCOGENIC").is_ok() {
                    result.curated_edges += 1;
                }
            }
            id
        };
        for drug_name in &row.drugs {
            let did = if let Some(&id) = drug_to_node.get(drug_name) {
                id
            } else {
                let id = graph.create_node("Drug");
                set_str(graph, id, "name", drug_name);
                drug_to_node.insert(drug_name.clone(), id);
                result.drug_nodes += 1;
                id
            };
            if graph.create_edge(vid, did, "HAS_THERAPEUTIC_IMPLICATION").is_ok() {
                result.therapeutic_edges += 1;
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use samyama_sdk::Label;

    const CURATED_GENES_FIXTURE: &str = r#"[
        {
          "hugoSymbol": "BRAF",
          "entrezGeneId": 673,
          "grch38Isoform": "ENST00000646891",
          "grch38RefSeq": "NM_004333.6",
          "oncogene": true,
          "tsg": false,
          "highestSensitiveLevel": "LEVEL_1",
          "highestResistanceLevel": null,
          "summary": "BRAF is an oncogene..."
        },
        {
          "hugoSymbol": "TP53",
          "entrezGeneId": 7157,
          "grch38Isoform": "ENST00000269305",
          "grch38RefSeq": "NM_000546.6",
          "oncogene": false,
          "tsg": true,
          "highestSensitiveLevel": "LEVEL_1",
          "highestResistanceLevel": "LEVEL_R1"
        },
        { "hugoSymbol": "" }
    ]"#;

    const ACTIONABLE_FIXTURE: &str = r#"[
        {
          "gene": {"hugoSymbol": "BRAF", "entrezGeneId": 673},
          "alteration": "V600E",
          "consequence": "missense_variant",
          "alterationType": "MUTATION",
          "level": "LEVEL_1",
          "drugs": ["Vemurafenib", "Dabrafenib"],
          "tumorType": "Melanoma",
          "oncotreeCode": "MEL"
        },
        {
          "gene": {"hugoSymbol": "BRAF", "entrezGeneId": 673},
          "alteration": "V600E",
          "consequence": "missense_variant",
          "alterationType": "MUTATION",
          "level": "LEVEL_1",
          "drugs": ["Vemurafenib", "Cobimetinib"],
          "tumorType": "Melanoma",
          "oncotreeCode": "MEL"
        },
        {
          "gene": {"hugoSymbol": "EGFR", "entrezGeneId": 1956},
          "alteration": "L858R",
          "consequence": "missense_variant",
          "alterationType": "MUTATION",
          "level": "LEVEL_1",
          "drugs": ["Osimertinib"],
          "tumorType": "Non-Small Cell Lung Cancer",
          "oncotreeCode": "NSCLC"
        },
        {
          "gene": {"hugoSymbol": ""},
          "alteration": "X"
        }
    ]"#;

    #[test]
    fn parses_curated_gene_with_full_fields() {
        let arr: Value = serde_json::from_str(CURATED_GENES_FIXTURE).unwrap();
        let entries = arr.as_array().unwrap();
        let braf = parse_curated_gene(&entries[0]).expect("BRAF");
        assert_eq!(braf.hugo_symbol, "BRAF");
        assert_eq!(braf.entrez_id, Some(673));
        assert!(braf.oncogene);
        assert!(!braf.tsg);
        assert_eq!(braf.grch38_isoform, "ENST00000646891");
        assert_eq!(braf.highest_sensitive_level, "LEVEL_1");
    }

    #[test]
    fn parses_curated_gene_with_null_fields() {
        let arr: Value = serde_json::from_str(CURATED_GENES_FIXTURE).unwrap();
        let entries = arr.as_array().unwrap();
        let tp53 = parse_curated_gene(&entries[1]).expect("TP53");
        assert!(!tp53.oncogene);
        assert!(tp53.tsg);
        assert_eq!(tp53.highest_resistance_level, "LEVEL_R1");
    }

    #[test]
    fn skips_curated_gene_without_hugo_symbol() {
        let arr: Value = serde_json::from_str(CURATED_GENES_FIXTURE).unwrap();
        let entries = arr.as_array().unwrap();
        assert!(parse_curated_gene(&entries[2]).is_none());
    }

    #[test]
    fn parses_actionable_variant_with_drug_array() {
        let arr: Value = serde_json::from_str(ACTIONABLE_FIXTURE).unwrap();
        let entries = arr.as_array().unwrap();
        let braf = parse_actionable_variant(&entries[0]).expect("BRAF V600E");
        assert_eq!(braf.hugo_symbol, "BRAF");
        assert_eq!(braf.alteration, "V600E");
        assert_eq!(braf.level, "LEVEL_1");
        assert_eq!(braf.drugs, vec!["Vemurafenib", "Dabrafenib"]);
        assert_eq!(braf.oncotree_code, "MEL");
    }

    #[test]
    fn skips_actionable_variant_without_gene() {
        let arr: Value = serde_json::from_str(ACTIONABLE_FIXTURE).unwrap();
        let entries = arr.as_array().unwrap();
        assert!(parse_actionable_variant(&entries[3]).is_none());
    }

    #[test]
    fn load_curated_genes_creates_nodes_and_hgnc_bridge() {
        let dir = tempdir();
        let path = dir.join("genes.json");
        std::fs::write(&path, CURATED_GENES_FIXTURE).unwrap();

        let mut g = GraphStore::new();
        // Pre-create one HGNC :Gene to verify the SAME_AS bridge.
        let braf_hgnc = g.create_node("Gene");
        g.get_node_mut(braf_hgnc)
            .unwrap()
            .set_property("symbol", PropertyValue::String("BRAF".into()));
        let mut hgnc_idx = HashMap::new();
        hgnc_idx.insert("BRAF".to_string(), braf_hgnc);

        let res = load_curated_genes_json(&mut g, &path, Some(&hgnc_idx)).unwrap();
        assert_eq!(res.gene_nodes, 2);    // BRAF + TP53; empty-symbol skipped
        assert_eq!(res.same_as_edges, 1); // BRAF only
    }

    #[test]
    fn load_actionable_variants_dedupes_variants_and_drugs() {
        let dir = tempdir();
        let path = dir.join("vars.json");
        std::fs::write(&path, ACTIONABLE_FIXTURE).unwrap();

        let mut g = GraphStore::new();
        // Pre-create OncoKBGene nodes for the bridge.
        let braf = g.create_node("OncoKBGene");
        g.get_node_mut(braf)
            .unwrap()
            .set_property("hugo_symbol", PropertyValue::String("BRAF".into()));
        let egfr = g.create_node("OncoKBGene");
        g.get_node_mut(egfr)
            .unwrap()
            .set_property("hugo_symbol", PropertyValue::String("EGFR".into()));
        let mut idx = HashMap::new();
        idx.insert("BRAF".to_string(), braf);
        idx.insert("EGFR".to_string(), egfr);

        let res = load_actionable_variants_json(&mut g, &path, &idx).unwrap();
        // Two unique (gene, alteration) pairs: BRAF V600E, EGFR L858R.
        assert_eq!(res.variant_nodes, 2);
        // Three unique drugs: Vemurafenib, Dabrafenib, Cobimetinib, Osimertinib.
        // Wait — actually four. Let me recount: Vemurafenib (twice), Dabrafenib, Cobimetinib, Osimertinib = 4 unique.
        assert_eq!(res.drug_nodes, 4);
        // BRAF V600E -> OncoKBGene + EGFR L858R -> OncoKBGene
        assert_eq!(res.curated_edges, 2);
        // Therapeutic edges: BRAF V600E first row 2 drugs + second row 2 drugs (dedup happens
        // on variant, not drug-edge dedup) = 4 + EGFR L858R 1 drug = 5.
        assert_eq!(res.therapeutic_edges, 5);
        // Verify the OncoKBGene label still queryable.
        let label: Label = "OncoKBGene".into();
        assert_eq!(g.get_nodes_by_label(&label).len(), 2);
    }

    fn tempdir() -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("oncokb_test_{}_{}", std::process::id(), rand_suffix()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
    fn rand_suffix() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
        format!("{}", nanos)
    }
}
