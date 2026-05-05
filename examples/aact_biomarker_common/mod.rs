//! AACT biomarker extractor — pulls gene/variant mentions out of free-text
//! eligibility criteria and links them to existing :ClinicalTrial and :Gene
//! nodes loaded by the AACT and HGNC passes.
//!
//! Schema added by this pass:
//!   :Biomarker            (canonical_form PK, gene_symbol, modifier)
//!   :REQUIRES_BIOMARKER   (ClinicalTrial -> Biomarker)
//!                         props: requirement ("inclusion"/"exclusion"/"unknown"),
//!                                confidence (f64 in [0,1]),
//!                                raw_match (original substring)
//!   :TARGETS_GENE         (Biomarker -> Gene)
//!
//! Source: AACT `eligibilities.txt` (pipe-delimited, columns include
//! `nct_id` and `criteria` — the free-text eligibility block).

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use samyama_sdk::{GraphStore, Label, NodeId, PropertyMap, PropertyValue};

pub type Error = Box<dyn std::error::Error>;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LoadResult {
    pub biomarker_nodes: usize,
    pub requires_edges: usize,
    pub targets_gene_edges: usize,
    pub trials_processed: usize,
    pub trials_with_biomarkers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    Inclusion,
    Exclusion,
    Unknown,
}

impl Requirement {
    pub fn as_str(&self) -> &'static str {
        match self {
            Requirement::Inclusion => "inclusion",
            Requirement::Exclusion => "exclusion",
            Requirement::Unknown => "unknown",
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct BiomarkerMention {
    pub gene_symbol: String,
    pub modifier: String,
    pub canonical_form: String,
    pub requirement: Requirement,
    pub confidence: f64,
    pub raw_match: String,
}

/// High-specificity modifiers — variant/event language that is unambiguous
/// when paired with a known gene symbol.
const HIGH_CONFIDENCE_MODIFIERS: &[&str] = &[
    "mutation",
    "mutations",
    "mutated",
    "amplification",
    "amplified",
    "fusion",
    "fusions",
    "rearrangement",
    "rearranged",
    "deletion",
    "wild-type",
    "wildtype",
    "wild type",
    "overexpression",
];

/// Lower-specificity modifiers — common in oncology eligibility text but
/// can also appear in non-biomarker contexts (e.g. "PD-L1 positive" is a
/// genuine biomarker; "HIV positive" is not). Confidence reflects this.
const MEDIUM_CONFIDENCE_MODIFIERS: &[&str] = &["positive", "negative", "expression"];

/// Gene symbols that are also extremely common English words. The gene-set
/// filter alone can't reject these because they ARE real HGNC symbols
/// (e.g. WAS = Wiskott-Aldrich Syndrome). Without this blocklist, AACT
/// eligibility text like "Patient was treated…" would emit a `WAS mutation`
/// biomarker. Discovered via real-data smoke v3 (top-genes report showed
/// WAS at 616 trials, an obvious false positive).
const ENGLISH_FALSE_POSITIVE_SYMBOLS: &[&str] = &[
    "WAS", "IS", "HAS", "OR", "AND", "NOT", "ALL", "ANY", "ARE", "BE", "DO",
    "GO", "HE", "IF", "IN", "IT", "ME", "MY", "NO", "OF", "ON", "SO", "THE",
    "TO", "UP", "WE", "AS", "AT", "BY", "FOR", "OUT",
];

/// Normalise a raw modifier string to a canonical form for the
/// `Biomarker.canonical_form` PK and `modifier` property.
pub fn normalise_modifier(m: &str) -> String {
    let lower = m.to_ascii_lowercase();
    match lower.as_str() {
        "mutated" | "mutations" => "mutation".into(),
        "amplified" => "amplification".into(),
        "fusions" => "fusion".into(),
        "rearranged" => "rearrangement".into(),
        "wildtype" | "wild type" | "wild-type" => "wild-type".into(),
        _ => lower,
    }
}

/// Split eligibility text into segments tagged by their requirement type.
/// AACT text typically contains "Inclusion Criteria:" and "Exclusion
/// Criteria:" section headers. Anything before the first recognised header
/// is `Unknown`.
pub fn split_inclusion_exclusion(text: &str) -> Vec<(Requirement, String)> {
    let lower = text.to_ascii_lowercase();
    // Find header positions (start byte index, requirement). We search the
    // lowercased copy but slice the original to preserve casing.
    let mut markers: Vec<(usize, Requirement)> = Vec::new();
    for (pat, req) in [
        ("inclusion criteria", Requirement::Inclusion),
        ("exclusion criteria", Requirement::Exclusion),
    ] {
        let mut start = 0usize;
        while let Some(rel) = lower[start..].find(pat) {
            markers.push((start + rel, req));
            start += rel + pat.len();
        }
    }
    if markers.is_empty() {
        return vec![(Requirement::Unknown, text.to_string())];
    }
    markers.sort_by_key(|(i, _)| *i);

    let mut segments = Vec::new();
    if markers[0].0 > 0 {
        let preamble = &text[..markers[0].0];
        if !preamble.trim().is_empty() {
            segments.push((Requirement::Unknown, preamble.to_string()));
        }
    }
    for i in 0..markers.len() {
        let (start, req) = markers[i];
        let end = markers
            .get(i + 1)
            .map(|(j, _)| *j)
            .unwrap_or(text.len());
        // Skip past the header itself — find the colon then advance.
        let body_start = text[start..end]
            .find(':')
            .map(|c| start + c + 1)
            .unwrap_or(start);
        let body = &text[body_start..end];
        if !body.trim().is_empty() {
            segments.push((req, body.to_string()));
        }
    }
    segments
}

/// Cached regex set for biomarker pattern matching. Built once per loader run.
pub struct BiomarkerPatterns {
    high: regex::Regex,
    medium: regex::Regex,
}

impl BiomarkerPatterns {
    pub fn new() -> Self {
        let high_alt = HIGH_CONFIDENCE_MODIFIERS
            .iter()
            .map(|m| regex::escape(m))
            .collect::<Vec<_>>()
            .join("|");
        let medium_alt = MEDIUM_CONFIDENCE_MODIFIERS
            .iter()
            .map(|m| regex::escape(m))
            .collect::<Vec<_>>()
            .join("|");
        // Gene symbols are 2-10 uppercase alnum (HGNC convention). We capture
        // the symbol then look ahead for a 1-3 word window before the modifier
        // to tolerate phrases like "BRCA1 germline mutation".
        // Gene token: 2-10 chars starting with letter, containing letters,
        // digits, or single hyphen (allows PD-L1, HLA-A, etc.).
        let gene = r"([A-Za-z][A-Za-z0-9\-]{1,9})";
        let high = regex::Regex::new(&format!(r"(?i)\b{}\s+({})\b", gene, high_alt))
            .expect("high-confidence regex compiles");
        let medium = regex::Regex::new(&format!(r"(?i)\b{}\s+({})\b", gene, medium_alt))
            .expect("medium-confidence regex compiles");
        BiomarkerPatterns { high, medium }
    }
}

impl Default for BiomarkerPatterns {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns true if the symbol is a real HGNC gene that's also too common
/// as an English word to be safely extracted from free-text eligibility
/// criteria. See `ENGLISH_FALSE_POSITIVE_SYMBOLS`.
pub fn is_english_false_positive(symbol: &str) -> bool {
    let upper = symbol.to_ascii_uppercase();
    ENGLISH_FALSE_POSITIVE_SYMBOLS.iter().any(|s| *s == upper)
}

/// Extract biomarker mentions from a single segment of eligibility text.
/// Mentions whose gene token isn't in `known_genes` are dropped to avoid
/// matching common English words that happen to look like uppercase tokens.
/// Mentions whose gene token is in the English-word blocklist are dropped
/// even when present in `known_genes`.
pub fn extract_from_segment(
    text: &str,
    requirement: Requirement,
    patterns: &BiomarkerPatterns,
    known_genes: &HashSet<String>,
) -> Vec<BiomarkerMention> {
    let mut out = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();

    for cap in patterns.high.captures_iter(text) {
        let gene = cap[1].to_ascii_uppercase();
        if !known_genes.contains(&gene) || is_english_false_positive(&gene) {
            continue;
        }
        let modifier = normalise_modifier(&cap[2]);
        let key = (gene.clone(), modifier.clone());
        if !seen.insert(key) {
            continue;
        }
        out.push(BiomarkerMention {
            canonical_form: format!("{} {}", gene, modifier),
            gene_symbol: gene,
            modifier,
            requirement,
            confidence: 1.0,
            raw_match: cap[0].to_string(),
        });
    }
    for cap in patterns.medium.captures_iter(text) {
        let gene = cap[1].to_ascii_uppercase();
        if !known_genes.contains(&gene) || is_english_false_positive(&gene) {
            continue;
        }
        let modifier = normalise_modifier(&cap[2]);
        let key = (gene.clone(), modifier.clone());
        if !seen.insert(key) {
            continue;
        }
        out.push(BiomarkerMention {
            canonical_form: format!("{} {}", gene, modifier),
            gene_symbol: gene,
            modifier,
            requirement,
            confidence: 0.7,
            raw_match: cap[0].to_string(),
        });
    }
    out
}

/// Run extraction across the full eligibility text, splitting by
/// inclusion/exclusion sections first.
pub fn extract_biomarkers(
    text: &str,
    patterns: &BiomarkerPatterns,
    known_genes: &HashSet<String>,
) -> Vec<BiomarkerMention> {
    split_inclusion_exclusion(text)
        .into_iter()
        .flat_map(|(req, body)| extract_from_segment(&body, req, patterns, known_genes))
        .collect()
}

fn set_str(g: &mut GraphStore, id: NodeId, k: &str, v: &str) {
    if !v.is_empty() {
        if let Some(n) = g.get_node_mut(id) {
            n.set_property(k, PropertyValue::String(v.to_string()));
        }
    }
}

/// Walk all :ClinicalTrial nodes and index by `nct_id` so the eligibility
/// pass can resolve criteria rows back to the trial node.
pub fn build_trial_index(graph: &GraphStore) -> HashMap<String, NodeId> {
    let mut out = HashMap::new();
    let label: Label = "ClinicalTrial".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(nct)) = node.get_property("nct_id") {
            out.insert(nct.clone(), node.id);
        }
    }
    out
}

/// Walk all :Gene nodes and collect upper-cased symbols into a set, plus a
/// symbol -> NodeId map for TARGETS_GENE edge creation.
pub fn build_gene_set_and_index(
    graph: &GraphStore,
) -> (HashSet<String>, HashMap<String, NodeId>) {
    let mut set = HashSet::new();
    let mut map = HashMap::new();
    let label: Label = "Gene".into();
    for node in graph.get_nodes_by_label(&label) {
        if let Some(PropertyValue::String(sym)) = node.get_property("symbol") {
            if !sym.is_empty() {
                let upper = sym.to_ascii_uppercase();
                set.insert(upper.clone());
                map.insert(upper, node.id);
            }
        }
    }
    (set, map)
}

/// Apply biomarker extraction to a single trial's eligibility text and
/// materialise the resulting mentions as graph nodes/edges. Biomarker nodes
/// are deduped across all trials by `canonical_form` via the supplied map.
pub fn apply_to_trial(
    graph: &mut GraphStore,
    trial_id: NodeId,
    mentions: &[BiomarkerMention],
    biomarker_index: &mut HashMap<String, NodeId>,
    gene_index: &HashMap<String, NodeId>,
    result: &mut LoadResult,
) {
    if mentions.is_empty() {
        return;
    }
    result.trials_with_biomarkers += 1;
    for m in mentions {
        let bid = if let Some(&id) = biomarker_index.get(&m.canonical_form) {
            id
        } else {
            let id = graph.create_node("Biomarker");
            set_str(graph, id, "canonical_form", &m.canonical_form);
            set_str(graph, id, "gene_symbol", &m.gene_symbol);
            set_str(graph, id, "modifier", &m.modifier);
            biomarker_index.insert(m.canonical_form.clone(), id);
            result.biomarker_nodes += 1;
            if let Some(&gene_node) = gene_index.get(&m.gene_symbol) {
                if graph.create_edge(id, gene_node, "TARGETS_GENE").is_ok() {
                    result.targets_gene_edges += 1;
                }
            }
            id
        };
        let mut props = PropertyMap::new();
        props.insert(
            "requirement".to_string(),
            PropertyValue::String(m.requirement.as_str().to_string()),
        );
        props.insert("confidence".to_string(), PropertyValue::Float(m.confidence));
        props.insert(
            "raw_match".to_string(),
            PropertyValue::String(m.raw_match.clone()),
        );
        if graph
            .create_edge_with_properties(trial_id, bid, "REQUIRES_BIOMARKER", props)
            .is_ok()
        {
            result.requires_edges += 1;
        }
    }
}

/// Stream `eligibilities.txt`, joining each row to a trial node and emitting
/// biomarker nodes/edges. Returns aggregate counts.
pub fn load_eligibilities(
    graph: &mut GraphStore,
    path: &Path,
    trial_index: &HashMap<String, NodeId>,
    gene_set: &HashSet<String>,
    gene_index: &HashMap<String, NodeId>,
    max_rows: usize,
) -> Result<LoadResult, Error> {
    let file = File::open(path)?;
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut lines = reader.lines();
    let header_line = match lines.next() {
        Some(Ok(l)) => l,
        _ => return Err("eligibilities.txt: missing header".into()),
    };
    let headers: HashMap<&str, usize> = header_line
        .split('|')
        .enumerate()
        .map(|(i, c)| (c, i))
        .collect();
    let nct_idx = *headers
        .get("nct_id")
        .ok_or("eligibilities.txt: missing nct_id column")?;
    let crit_idx = *headers
        .get("criteria")
        .ok_or("eligibilities.txt: missing criteria column")?;

    let patterns = BiomarkerPatterns::new();
    let mut biomarker_index: HashMap<String, NodeId> = HashMap::new();
    let mut result = LoadResult::default();
    let mut count = 0usize;

    for line in lines {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let fields: Vec<&str> = line.split('|').collect();
        let nct = fields.get(nct_idx).copied().unwrap_or("");
        let criteria = fields.get(crit_idx).copied().unwrap_or("");
        if nct.is_empty() || criteria.is_empty() {
            continue;
        }
        let trial_id = match trial_index.get(nct) {
            Some(&id) => id,
            None => {
                count += 1;
                continue;
            }
        };
        let mentions = extract_biomarkers(criteria, &patterns, gene_set);
        apply_to_trial(
            graph,
            trial_id,
            &mentions,
            &mut biomarker_index,
            gene_index,
            &mut result,
        );
        result.trials_processed += 1;
        count += 1;
        if max_rows > 0 && count >= max_rows {
            break;
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn known_oncogenes() -> HashSet<String> {
        // Includes WAS deliberately so blocklist tests have a real
        // overlap with `known_genes`.
        ["BRCA1", "BRCA2", "TP53", "EGFR", "KRAS", "HER2", "PD-L1", "PDL1", "ALK", "BRAF", "WAS"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn blocklist_drops_english_word_symbols_even_when_in_known_genes() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        // "WAS" is a real HGNC symbol (Wiskott-Aldrich Syndrome), but in
        // free-text eligibility criteria it almost always means the
        // English verb. Same for "OR", "IS", etc.
        let m = extract_from_segment(
            "Patient was treated with prior chemo. EGFR mutation required.",
            Requirement::Inclusion,
            &p,
            &genes,
        );
        let symbols: HashSet<_> = m.iter().map(|x| x.gene_symbol.as_str()).collect();
        assert!(!symbols.contains("WAS"), "WAS must be blocklisted");
        assert!(symbols.contains("EGFR"));
    }

    #[test]
    fn is_english_false_positive_recognises_common_words() {
        assert!(is_english_false_positive("WAS"));
        assert!(is_english_false_positive("was"));
        assert!(is_english_false_positive("Or"));
        assert!(!is_english_false_positive("BRCA1"));
        assert!(!is_english_false_positive("EGFR"));
    }

    #[test]
    fn normalises_modifier_variants() {
        assert_eq!(normalise_modifier("Mutated"), "mutation");
        assert_eq!(normalise_modifier("MUTATIONS"), "mutation");
        assert_eq!(normalise_modifier("Amplified"), "amplification");
        assert_eq!(normalise_modifier("Fusions"), "fusion");
        assert_eq!(normalise_modifier("Wildtype"), "wild-type");
        assert_eq!(normalise_modifier("Wild Type"), "wild-type");
        assert_eq!(normalise_modifier("Wild-Type"), "wild-type");
        assert_eq!(normalise_modifier("positive"), "positive");
    }

    #[test]
    fn splits_inclusion_and_exclusion_sections() {
        let txt = "Inclusion Criteria:\n- Age >= 18\n- BRCA1 mutation confirmed\n\nExclusion Criteria:\n- Prior chemo";
        let segs = split_inclusion_exclusion(txt);
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].0, Requirement::Inclusion);
        assert!(segs[0].1.contains("BRCA1"));
        assert_eq!(segs[1].0, Requirement::Exclusion);
        assert!(segs[1].1.contains("Prior chemo"));
    }

    #[test]
    fn split_handles_text_without_headers() {
        let segs = split_inclusion_exclusion("Patient must have EGFR mutation.");
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].0, Requirement::Unknown);
    }

    #[test]
    fn extracts_high_confidence_mentions() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        let m = extract_from_segment(
            "Patients with confirmed BRCA1 mutation or BRCA2 mutation.",
            Requirement::Inclusion,
            &p,
            &genes,
        );
        assert_eq!(m.len(), 2);
        assert!(m.iter().all(|x| x.confidence >= 0.99));
        let symbols: HashSet<_> = m.iter().map(|x| x.gene_symbol.as_str()).collect();
        assert!(symbols.contains("BRCA1"));
        assert!(symbols.contains("BRCA2"));
    }

    #[test]
    fn extracts_amplification_fusion_and_wildtype() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        let m = extract_from_segment(
            "HER2 amplification, ALK fusion, and KRAS wild-type required.",
            Requirement::Inclusion,
            &p,
            &genes,
        );
        let modifiers: HashSet<_> = m.iter().map(|x| x.modifier.as_str()).collect();
        assert!(modifiers.contains("amplification"));
        assert!(modifiers.contains("fusion"));
        assert!(modifiers.contains("wild-type"));
    }

    #[test]
    fn medium_confidence_for_positive_negative() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        let m = extract_from_segment(
            "PD-L1 positive tumour expression required.",
            Requirement::Inclusion,
            &p,
            &genes,
        );
        assert!(m.iter().any(|x| x.gene_symbol == "PD-L1" && x.modifier == "positive"));
        assert!(m.iter().all(|x| x.confidence < 0.99));
    }

    #[test]
    fn drops_uppercase_tokens_that_are_not_known_genes() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        // "HIV positive" must not produce a biomarker (HIV not in oncogene set).
        let m = extract_from_segment(
            "HIV positive patients excluded. EGFR mutation required.",
            Requirement::Unknown,
            &p,
            &genes,
        );
        let symbols: HashSet<_> = m.iter().map(|x| x.gene_symbol.as_str()).collect();
        assert!(!symbols.contains("HIV"));
        assert!(symbols.contains("EGFR"));
    }

    #[test]
    fn dedupes_within_a_single_segment() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        let m = extract_from_segment(
            "EGFR mutation present. EGFR mutation confirmed via ddPCR.",
            Requirement::Inclusion,
            &p,
            &genes,
        );
        // Both mentions are EGFR + mutation — second is dropped by the seen-set.
        let count = m.iter().filter(|x| x.gene_symbol == "EGFR" && x.modifier == "mutation").count();
        assert_eq!(count, 1);
    }

    #[test]
    fn end_to_end_extract_assigns_requirement_per_section() {
        let p = BiomarkerPatterns::new();
        let genes = known_oncogenes();
        let txt = "Inclusion Criteria:\nEGFR mutation positive.\n\nExclusion Criteria:\nPrior KRAS mutation therapy.";
        let mentions = extract_biomarkers(txt, &p, &genes);
        let egfr = mentions.iter().find(|m| m.gene_symbol == "EGFR" && m.modifier == "mutation");
        let kras = mentions.iter().find(|m| m.gene_symbol == "KRAS" && m.modifier == "mutation");
        assert_eq!(egfr.map(|m| m.requirement), Some(Requirement::Inclusion));
        assert_eq!(kras.map(|m| m.requirement), Some(Requirement::Exclusion));
    }

    #[test]
    fn apply_to_trial_creates_dedup_biomarkers_and_targets_gene_edges() {
        let mut g = GraphStore::new();
        let trial = g.create_node("ClinicalTrial");
        g.get_node_mut(trial)
            .unwrap()
            .set_property("nct_id", PropertyValue::String("NCT00000001".into()));
        let egfr_gene = g.create_node("Gene");
        g.get_node_mut(egfr_gene)
            .unwrap()
            .set_property("symbol", PropertyValue::String("EGFR".into()));

        let mut gene_index = HashMap::new();
        gene_index.insert("EGFR".to_string(), egfr_gene);
        let mentions = vec![
            BiomarkerMention {
                gene_symbol: "EGFR".into(),
                modifier: "mutation".into(),
                canonical_form: "EGFR mutation".into(),
                requirement: Requirement::Inclusion,
                confidence: 1.0,
                raw_match: "EGFR mutation".into(),
            },
            // Second mention reuses the same canonical form — must not double-create.
            BiomarkerMention {
                gene_symbol: "EGFR".into(),
                modifier: "mutation".into(),
                canonical_form: "EGFR mutation".into(),
                requirement: Requirement::Inclusion,
                confidence: 1.0,
                raw_match: "EGFR mutation".into(),
            },
        ];

        let mut biomarker_index = HashMap::new();
        let mut result = LoadResult::default();
        apply_to_trial(
            &mut g,
            trial,
            &mentions,
            &mut biomarker_index,
            &gene_index,
            &mut result,
        );
        assert_eq!(result.biomarker_nodes, 1);
        assert_eq!(result.requires_edges, 2); // both mentions become edges, but share node
        assert_eq!(result.targets_gene_edges, 1);
        assert_eq!(result.trials_with_biomarkers, 1);
    }
}
