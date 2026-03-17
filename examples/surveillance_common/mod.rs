//! Disease Surveillance KG data loading utilities.
//!
//! Loads WHO GHO data (countries, regions, diseases, disease reports,
//! vaccine coverage) into GraphStore via direct API calls.
//!
//! Source: WHO Global Health Observatory OData API
//! Data format: Pre-downloaded JSON files

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use samyama_sdk::{GraphStore, NodeId, PropertyValue};
use serde::Deserialize;

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// DATA STRUCTURES
// ============================================================================

#[derive(Deserialize)]
pub struct Country {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Title")]
    pub title: String,
}

#[derive(Deserialize)]
pub struct Region {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Title")]
    pub title: String,
}

#[derive(Deserialize)]
pub struct Disease {
    #[serde(rename = "IndicatorCode")]
    pub indicator_code: String,
    #[serde(rename = "IndicatorName")]
    pub indicator_name: String,
}

#[derive(Deserialize)]
pub struct DataRecord {
    #[serde(rename = "SpatialDim")]
    pub spatial_dim: Option<String>,
    #[serde(rename = "TimeDim", deserialize_with = "deserialize_string_or_int")]
    pub time_dim: Option<String>,
    #[serde(rename = "NumericValue")]
    pub numeric_value: Option<f64>,
    #[serde(rename = "IndicatorCode")]
    pub indicator_code: Option<String>,
    #[serde(rename = "IndicatorName")]
    pub indicator_name: Option<String>,
}

fn deserialize_string_or_int<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;
    struct StringOrInt;
    impl<'de> de::Visitor<'de> for StringOrInt {
        type Value = Option<String>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or integer")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }
        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            Ok(Some(v))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }
    deserializer.deserialize_any(StringOrInt)
}

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub country_nodes: usize,
    pub region_nodes: usize,
    pub disease_nodes: usize,
    pub disease_report_nodes: usize,
    pub vaccine_coverage_nodes: usize,
    pub health_indicator_nodes: usize,
    pub total_nodes: usize,
    pub total_edges: usize,
}

// ============================================================================
// FORMATTING
// ============================================================================

pub fn format_num(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        let mins = (secs / 60.0).floor() as u64;
        let rem = secs - (mins as f64 * 60.0);
        format!("{}m {:.1}s", mins, rem)
    }
}

fn clean_str(s: &str) -> String {
    s.replace('"', "").replace('\n', " ").replace('\r', "")
}

// ============================================================================
// MAIN LOADER
// ============================================================================

pub fn load_dataset(graph: &mut GraphStore, data_dir: &Path) -> Result<LoadResult, Error> {
    let mut country_map: HashMap<String, NodeId> = HashMap::new();
    let mut disease_map: HashMap<String, NodeId> = HashMap::new();

    let mut country_nodes = 0;
    let mut region_nodes = 0;
    let mut disease_nodes = 0;
    let mut report_nodes = 0;
    let mut vaccine_nodes = 0;
    let mut health_nodes = 0;
    let mut total_edges = 0;

    // Indexes are created via Cypher after loading

    // ── Countries ──
    let countries_path = data_dir.join("countries.json");
    if countries_path.exists() {
        let data = fs::read_to_string(&countries_path)?;
        let countries: Vec<Country> = serde_json::from_str(&data)?;
        for c in &countries {
            if c.code.is_empty() || c.title.is_empty() {
                continue;
            }
            let id = graph.create_node("Country");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("iso_code", PropertyValue::String(c.code.clone()));
                n.set_property("name", PropertyValue::String(clean_str(&c.title)));
            }
            country_map.insert(c.code.clone(), id);
            country_nodes += 1;
        }
        eprintln!("    Countries: {}", format_num(country_nodes));
    }

    // ── Regions ──
    let regions_path = data_dir.join("regions.json");
    let mut region_map: HashMap<String, NodeId> = HashMap::new();
    if regions_path.exists() {
        let data = fs::read_to_string(&regions_path)?;
        let regions: Vec<Region> = serde_json::from_str(&data)?;
        for r in &regions {
            let id = graph.create_node("Region");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("who_code", PropertyValue::String(r.code.clone()));
                n.set_property("name", PropertyValue::String(clean_str(&r.title)));
            }
            region_map.insert(r.code.clone(), id);
            region_nodes += 1;
        }
        eprintln!("    Regions: {}", format_num(region_nodes));
    }

    // ── Country → Region edges ──
    let cr_path = data_dir.join("country_regions.json");
    if cr_path.exists() {
        let data = fs::read_to_string(&cr_path)?;
        let cr_map: HashMap<String, String> = serde_json::from_str(&data)?;
        let mut cr_edges = 0;
        for (iso, who_code) in &cr_map {
            if let (Some(&country_id), Some(&region_id)) =
                (country_map.get(iso), region_map.get(who_code))
            {
                let _ = graph.create_edge(country_id, region_id, "IN_REGION");
                cr_edges += 1;
            }
        }
        total_edges += cr_edges;
        eprintln!("    IN_REGION edges: {}", format_num(cr_edges));
    }

    // ── Diseases ──
    let diseases_path = data_dir.join("diseases.json");
    if diseases_path.exists() {
        let data = fs::read_to_string(&diseases_path)?;
        let diseases: Vec<Disease> = serde_json::from_str(&data)?;
        for d in &diseases {
            if d.indicator_code.is_empty() {
                continue;
            }
            let id = graph.create_node("Disease");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property(
                    "indicator_code",
                    PropertyValue::String(d.indicator_code.clone()),
                );
                n.set_property(
                    "name",
                    PropertyValue::String(clean_str(&d.indicator_name)),
                );
            }
            disease_map.insert(d.indicator_code.clone(), id);
            disease_nodes += 1;
        }
        eprintln!("    Diseases: {}", format_num(disease_nodes));
    }

    // ── Disease Reports ──
    let disease_data_path = data_dir.join("disease_data.json");
    if disease_data_path.exists() {
        let data = fs::read_to_string(&disease_data_path)?;
        let records: Vec<DataRecord> = serde_json::from_str(&data)?;
        let mut reported_edges = 0;
        let mut report_of_edges = 0;

        for rec in &records {
            let country = match &rec.spatial_dim {
                Some(c) if !c.is_empty() => c.as_str(),
                _ => continue,
            };
            let year = match &rec.time_dim {
                Some(y) if !y.is_empty() => y.as_str(),
                _ => continue,
            };
            let value = match rec.numeric_value {
                Some(v) => v,
                None => continue,
            };
            let indicator = match &rec.indicator_code {
                Some(i) if !i.is_empty() => i.as_str(),
                _ => continue,
            };

            let rid = format!("DR-{}-{}-{}", country, indicator, year);
            let report_id = graph.create_node("DiseaseReport");
            if let Some(n) = graph.get_node_mut(report_id) {
                n.set_property("id", PropertyValue::String(rid));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                n.set_property("value", PropertyValue::Float(value));
            }
            report_nodes += 1;

            // REPORTED edge (Country → DiseaseReport)
            if let Some(&country_id) = country_map.get(country) {
                let _ = graph.create_edge(country_id, report_id, "REPORTED");
                reported_edges += 1;
            }

            // REPORT_OF edge (DiseaseReport → Disease)
            if let Some(&disease_id) = disease_map.get(indicator) {
                let _ = graph.create_edge(report_id, disease_id, "REPORT_OF");
                report_of_edges += 1;
            }
        }
        total_edges += reported_edges + report_of_edges;
        eprintln!(
            "    Disease reports: {} nodes, {} REPORTED, {} REPORT_OF",
            format_num(report_nodes),
            format_num(reported_edges),
            format_num(report_of_edges)
        );
    }

    // ── Vaccine Coverage ──
    let vaccine_path = data_dir.join("vaccine_data.json");
    if vaccine_path.exists() {
        let data = fs::read_to_string(&vaccine_path)?;
        let records: Vec<DataRecord> = serde_json::from_str(&data)?;
        let mut cov_edges = 0;

        for rec in &records {
            let country = match &rec.spatial_dim {
                Some(c) if !c.is_empty() => c.as_str(),
                _ => continue,
            };
            let year = match &rec.time_dim {
                Some(y) if !y.is_empty() => y.as_str(),
                _ => continue,
            };
            let value = match rec.numeric_value {
                Some(v) => v,
                None => continue,
            };
            let antigen = rec
                .indicator_name
                .as_deref()
                .unwrap_or("")
                .to_string();

            let vid = format!(
                "VC-{}-{}-{}",
                country,
                rec.indicator_code.as_deref().unwrap_or(""),
                year
            );
            let vc_id = graph.create_node("VaccineCoverage");
            if let Some(n) = graph.get_node_mut(vc_id) {
                n.set_property("id", PropertyValue::String(vid));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                n.set_property("coverage_pct", PropertyValue::Float(value));
                if !antigen.is_empty() {
                    n.set_property("antigen", PropertyValue::String(clean_str(&antigen)));
                }
            }
            vaccine_nodes += 1;

            if let Some(&country_id) = country_map.get(country) {
                let _ = graph.create_edge(country_id, vc_id, "HAS_COVERAGE");
                cov_edges += 1;
            }
        }
        total_edges += cov_edges;
        eprintln!(
            "    Vaccine coverage: {} nodes, {} edges",
            format_num(vaccine_nodes),
            format_num(cov_edges)
        );
    }

    // ── Health Indicators ──
    let health_path = data_dir.join("health_data.json");
    if health_path.exists() {
        let data = fs::read_to_string(&health_path)?;
        let records: Vec<DataRecord> = serde_json::from_str(&data)?;
        let mut hi_edges = 0;

        for rec in &records {
            let country = match &rec.spatial_dim {
                Some(c) if !c.is_empty() => c.as_str(),
                _ => continue,
            };
            let year = match &rec.time_dim {
                Some(y) if !y.is_empty() => y.as_str(),
                _ => continue,
            };
            let value = match rec.numeric_value {
                Some(v) => v,
                None => continue,
            };
            let indicator_code = rec.indicator_code.as_deref().unwrap_or("");
            let indicator_name = rec.indicator_name.as_deref().unwrap_or("");

            let hid = format!("HI-{}-{}-{}", country, indicator_code, year);
            let hi_id = graph.create_node("HealthIndicator");
            if let Some(n) = graph.get_node_mut(hi_id) {
                n.set_property("id", PropertyValue::String(hid));
                n.set_property(
                    "indicator_code",
                    PropertyValue::String(indicator_code.to_string()),
                );
                n.set_property(
                    "name",
                    PropertyValue::String(clean_str(indicator_name)),
                );
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                n.set_property("value", PropertyValue::Float(value));
            }
            health_nodes += 1;

            if let Some(&country_id) = country_map.get(country) {
                let _ = graph.create_edge(country_id, hi_id, "HAS_INDICATOR");
                hi_edges += 1;
            }
        }
        total_edges += hi_edges;
        eprintln!(
            "    Health indicators: {} nodes, {} edges",
            format_num(health_nodes),
            format_num(hi_edges)
        );
    }

    let total_nodes =
        country_nodes + region_nodes + disease_nodes + report_nodes + vaccine_nodes + health_nodes;

    Ok(LoadResult {
        country_nodes,
        region_nodes,
        disease_nodes,
        disease_report_nodes: report_nodes,
        vaccine_coverage_nodes: vaccine_nodes,
        health_indicator_nodes: health_nodes,
        total_nodes,
        total_edges,
    })
}
