//! Health Systems KG data loading utilities.
//!
//! Loads WHO SPAR, NHWA, GAVI, Global Fund, IHME data
//! into GraphStore via direct API calls.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Duration;

use samyama_sdk::{GraphStore, NodeId, PropertyValue};
use serde::Deserialize;

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// DATA STRUCTURES
// ============================================================================

#[derive(Deserialize)]
pub struct HSCountry {
    pub iso_code: String,
    pub name: String,
    #[serde(default)]
    pub who_region: String,
    #[serde(default)]
    pub income_level: String,
}

#[derive(Debug)]
pub struct CsvRecord {
    pub country_code: String,
    pub fields: HashMap<String, String>,
}

// ============================================================================
// LOAD RESULT
// ============================================================================

pub struct LoadResult {
    pub country_nodes: usize,
    pub emergency_response_nodes: usize,
    pub health_workforce_nodes: usize,
    pub supply_chain_nodes: usize,
    pub funding_flow_nodes: usize,
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
        format!("{}m {:.1}s", mins, secs - (mins as f64 * 60.0))
    }
}

fn clean_str(s: &str) -> String {
    s.replace('"', "").replace('\n', " ").replace('\r', "")
}

fn parse_csv_records(path: &Path) -> Result<Vec<CsvRecord>, Error> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let header_line = match lines.next() {
        Some(Ok(l)) => l,
        _ => return Ok(vec![]),
    };
    let headers: Vec<&str> = header_line.split(',').collect();
    let mut records = Vec::new();
    for line in lines {
        let line = line?;
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < headers.len() {
            continue;
        }
        let mut map = HashMap::new();
        for (i, h) in headers.iter().enumerate() {
            map.insert(h.to_string(), fields[i].to_string());
        }
        let cc = map.get("country_code").cloned().unwrap_or_default();
        if !cc.is_empty() {
            records.push(CsvRecord {
                country_code: cc,
                fields: map,
            });
        }
    }
    Ok(records)
}

// ============================================================================
// MAIN LOADER
// ============================================================================

pub fn load_dataset(graph: &mut GraphStore, data_dir: &Path) -> Result<LoadResult, Error> {
    let mut country_map: HashMap<String, NodeId> = HashMap::new();

    let mut country_nodes = 0;
    let mut er_nodes = 0;
    let mut hw_nodes = 0;
    let mut sc_nodes = 0;
    let mut ff_nodes = 0;
    let mut total_edges = 0;

    // ── Countries (from SPAR directory) ──
    let countries_path = data_dir.join("who_spar").join("countries.json");
    if countries_path.exists() {
        let data = fs::read_to_string(&countries_path)?;
        let countries: Vec<HSCountry> = serde_json::from_str(&data)?;
        for c in &countries {
            if c.iso_code.is_empty() || c.name.is_empty() {
                continue;
            }
            let id = graph.create_node("Country");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("iso_code", PropertyValue::String(c.iso_code.clone()));
                n.set_property("name", PropertyValue::String(clean_str(&c.name)));
                if !c.who_region.is_empty() {
                    n.set_property("who_region", PropertyValue::String(c.who_region.clone()));
                }
                if !c.income_level.is_empty() {
                    n.set_property(
                        "income_level",
                        PropertyValue::String(c.income_level.clone()),
                    );
                }
            }
            country_map.insert(c.iso_code.clone(), id);
            country_nodes += 1;
        }
        eprintln!("    Countries: {}", format_num(country_nodes));
    }

    // ── SPAR (Emergency Response) ──
    let spar_path = data_dir.join("who_spar").join("spar.csv");
    if spar_path.exists() {
        let records = parse_csv_records(&spar_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let cap_code = rec
                .fields
                .get("capacity_code")
                .map(|s| s.as_str())
                .unwrap_or("");
            let cap_name = rec
                .fields
                .get("capacity_name")
                .map(|s| s.as_str())
                .unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            let score = rec.fields.get("score").and_then(|s| s.parse::<f64>().ok());
            if cap_code.is_empty() || year.is_empty() {
                continue;
            }

            let nid = format!("ER-{}-{}-{}", rec.country_code, cap_code, year);
            let node_id = graph.create_node("EmergencyResponse");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid));
                n.set_property("capacity_code", PropertyValue::String(cap_code.to_string()));
                n.set_property("capacity_name", PropertyValue::String(clean_str(cap_name)));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                if let Some(s) = score {
                    n.set_property("score", PropertyValue::Integer(s as i64));
                }
                n.set_property(
                    "country_code",
                    PropertyValue::String(rec.country_code.clone()),
                );
            }
            let _ = graph.create_edge(node_id, country_id, "CAPACITY_FOR");
            er_nodes += 1;
            total_edges += 1;
        }
        eprintln!("    Emergency Response (SPAR): {}", format_num(er_nodes));
    }

    // ── NHWA (Health Workforce) ──
    let nhwa_path = data_dir.join("who_nhwa").join("nhwa.csv");
    if nhwa_path.exists() {
        let records = parse_csv_records(&nhwa_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let prof = rec
                .fields
                .get("profession")
                .map(|s| s.as_str())
                .unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            if prof.is_empty() || year.is_empty() {
                continue;
            }

            let nid = format!("HW-{}-{}-{}", rec.country_code, prof, year);
            let node_id = graph.create_node("HealthWorkforce");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid));
                n.set_property("profession", PropertyValue::String(prof.to_string()));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                let count = rec.fields.get("count").and_then(|s| s.parse::<f64>().ok());
                if let Some(c) = count {
                    n.set_property("count", PropertyValue::Integer(c as i64));
                }
                let density = rec
                    .fields
                    .get("density_per_10k")
                    .and_then(|s| s.parse::<f64>().ok());
                if let Some(d) = density {
                    n.set_property("density_per_10k", PropertyValue::Float(d));
                }
                n.set_property(
                    "country_code",
                    PropertyValue::String(rec.country_code.clone()),
                );
            }
            let _ = graph.create_edge(node_id, country_id, "SERVES");
            hw_nodes += 1;
            total_edges += 1;
        }
        eprintln!("    Health Workforce (NHWA): {}", format_num(hw_nodes));
    }

    // ── GAVI (Supply Chain) ──
    let gavi_path = data_dir.join("gavi").join("supply.csv");
    if gavi_path.exists() {
        let records = parse_csv_records(&gavi_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let vaccine = rec
                .fields
                .get("vaccine_name")
                .map(|s| s.as_str())
                .unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            if vaccine.is_empty() || year.is_empty() {
                continue;
            }

            let nid = format!(
                "SC-{}-{}-{}",
                rec.country_code,
                vaccine.replace(' ', "_"),
                year
            );
            let node_id = graph.create_node("SupplyChain");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid));
                n.set_property("vaccine_name", PropertyValue::String(vaccine.to_string()));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                for (field, key) in [
                    ("doses_shipped", "doses_shipped"),
                    ("doses_used", "doses_used"),
                    ("wastage_pct", "wastage_pct"),
                ] {
                    if let Some(v) = rec.fields.get(field).and_then(|s| s.parse::<f64>().ok()) {
                        n.set_property(key, PropertyValue::Float(v));
                    }
                }
                n.set_property(
                    "country_code",
                    PropertyValue::String(rec.country_code.clone()),
                );
            }
            let _ = graph.create_edge(node_id, country_id, "SUPPLIES");
            sc_nodes += 1;
            total_edges += 1;
        }
        eprintln!("    Supply Chain (GAVI): {}", format_num(sc_nodes));
    }

    // ── Global Fund (Funding Flows) ──
    let gf_path = data_dir.join("globalfund").join("disbursements.csv");
    if gf_path.exists() {
        let records = parse_csv_records(&gf_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let component = rec
                .fields
                .get("disease_component")
                .map(|s| s.as_str())
                .unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            if component.is_empty() || year.is_empty() {
                continue;
            }

            let nid = format!(
                "FF-{}-GF-{}-{}",
                rec.country_code,
                component.replace(' ', "_").replace('/', "_"),
                year
            );
            let node_id = graph.create_node("FundingFlow");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid));
                n.set_property(
                    "donor",
                    PropertyValue::String(
                        rec.fields
                            .get("donor")
                            .cloned()
                            .unwrap_or_else(|| "Global Fund".to_string()),
                    ),
                );
                n.set_property(
                    "disease_component",
                    PropertyValue::String(component.to_string()),
                );
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                if let Some(v) = rec
                    .fields
                    .get("amount_usd")
                    .and_then(|s| s.parse::<f64>().ok())
                {
                    n.set_property("amount_usd", PropertyValue::Float(v));
                }
                n.set_property(
                    "country_code",
                    PropertyValue::String(rec.country_code.clone()),
                );
            }
            let _ = graph.create_edge(node_id, country_id, "FUNDED_BY");
            ff_nodes += 1;
            total_edges += 1;
        }
        eprintln!("    Global Fund: {}", format_num(ff_nodes));
    }

    // ── IHME (Health Expenditure) ──
    let ihme_path = data_dir.join("ihme").join("expenditure.csv");
    if ihme_path.exists() {
        let records = parse_csv_records(&ihme_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let indicator = rec
                .fields
                .get("indicator")
                .map(|s| s.as_str())
                .unwrap_or("");
            let indicator_name = rec
                .fields
                .get("indicator_name")
                .map(|s| s.as_str())
                .unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            let value = rec.fields.get("value").and_then(|s| s.parse::<f64>().ok());
            if indicator.is_empty() || year.is_empty() || value.is_none() {
                continue;
            }

            let nid = format!("FF-{}-IHME-{}-{}", rec.country_code, indicator, year);
            let node_id = graph.create_node("FundingFlow");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid));
                n.set_property("donor", PropertyValue::String("IHME".to_string()));
                n.set_property("indicator", PropertyValue::String(indicator.to_string()));
                n.set_property(
                    "indicator_name",
                    PropertyValue::String(clean_str(indicator_name)),
                );
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                if let Some(v) = value {
                    n.set_property("value", PropertyValue::Float(v));
                }
                n.set_property(
                    "country_code",
                    PropertyValue::String(rec.country_code.clone()),
                );
            }
            let _ = graph.create_edge(node_id, country_id, "FUNDED_BY");
            ff_nodes += 1;
            total_edges += 1;
        }
        eprintln!("    IHME: {} total funding nodes", format_num(ff_nodes));
    }

    let total_nodes = country_nodes + er_nodes + hw_nodes + sc_nodes + ff_nodes;

    Ok(LoadResult {
        country_nodes,
        emergency_response_nodes: er_nodes,
        health_workforce_nodes: hw_nodes,
        supply_chain_nodes: sc_nodes,
        funding_flow_nodes: ff_nodes,
        total_nodes,
        total_edges,
    })
}
