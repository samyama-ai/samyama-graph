//! Health Determinants KG data loading utilities.
//!
//! Loads World Bank WDI, WHO Air Quality, FAO AQUASTAT, UNDP HDI data
//! into GraphStore via direct API calls.
//!
//! Data format: Pre-downloaded JSON + CSV files

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use samyama_sdk::{GraphStore, NodeId, PropertyValue};
use serde::Deserialize;

pub type Error = Box<dyn std::error::Error>;

// ============================================================================
// DATA STRUCTURES — World Bank
// ============================================================================

#[derive(Deserialize)]
pub struct WBCountry {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub region: WBRegionRef,
    #[serde(rename = "incomeLevel", default)]
    pub income_level: WBRegionRef,
}

#[derive(Deserialize, Default)]
pub struct WBRegionRef {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Deserialize)]
pub struct WBRegion {
    pub code: String,
    pub name: String,
}

#[derive(Deserialize)]
pub struct WDIRecord {
    pub country: WDICountryRef,
    #[serde(default)]
    pub countryiso3code: String,
    pub indicator: WDIIndicatorRef,
    pub date: Option<String>,
    pub value: Option<f64>,
}

#[derive(Deserialize)]
pub struct WDICountryRef {
    pub id: String,
}

#[derive(Deserialize)]
pub struct WDIIndicatorRef {
    pub id: String,
    pub value: String,
}

// CSV record for air quality, AQUASTAT, HDI
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
    pub region_nodes: usize,
    pub socioeconomic_nodes: usize,
    pub environmental_nodes: usize,
    pub nutrition_nodes: usize,
    pub demographic_nodes: usize,
    pub water_nodes: usize,
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
// CATEGORY CONFIG
// ============================================================================

struct CategoryConfig {
    label: &'static str,
    edge_type: &'static str,
    id_prefix: &'static str,
    filename: &'static str,
}

const CATEGORIES: &[CategoryConfig] = &[
    CategoryConfig {
        label: "SocioeconomicIndicator",
        edge_type: "HAS_INDICATOR",
        id_prefix: "SE",
        filename: "socioeconomic.json",
    },
    CategoryConfig {
        label: "EnvironmentalFactor",
        edge_type: "ENVIRONMENT_OF",
        id_prefix: "EF",
        filename: "environmental.json",
    },
    CategoryConfig {
        label: "NutritionIndicator",
        edge_type: "NUTRITION_STATUS",
        id_prefix: "NI",
        filename: "nutrition.json",
    },
    CategoryConfig {
        label: "DemographicProfile",
        edge_type: "DEMOGRAPHIC_OF",
        id_prefix: "DP",
        filename: "demographic.json",
    },
    CategoryConfig {
        label: "WaterResource",
        edge_type: "WATER_RESOURCE_OF",
        id_prefix: "WR",
        filename: "water.json",
    },
];

// ============================================================================
// CSV PARSER
// ============================================================================

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
    let wb_dir = data_dir.join("worldbank");
    let mut country_map: HashMap<String, NodeId> = HashMap::new();
    let mut region_map: HashMap<String, NodeId> = HashMap::new();

    let mut country_nodes = 0;
    let mut region_nodes = 0;
    let mut cat_nodes = [0usize; 5]; // SE, EF, NI, DP, WR
    let mut total_edges = 0;

    // ── Countries ──
    let countries_path = wb_dir.join("countries.json");
    if countries_path.exists() {
        let data = fs::read_to_string(&countries_path)?;
        let countries: Vec<WBCountry> = serde_json::from_str(&data)?;
        for c in &countries {
            if c.id.is_empty() || c.name.is_empty() {
                continue;
            }
            let id = graph.create_node("Country");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("iso_code", PropertyValue::String(c.id.clone()));
                n.set_property("name", PropertyValue::String(clean_str(&c.name)));
                if !c.income_level.value.is_empty() {
                    n.set_property(
                        "income_level",
                        PropertyValue::String(c.income_level.value.clone()),
                    );
                }
                if !c.region.value.is_empty() {
                    n.set_property("region_wb", PropertyValue::String(c.region.value.clone()));
                }
            }
            country_map.insert(c.id.clone(), id);
            country_nodes += 1;
        }
        eprintln!("    Countries: {}", format_num(country_nodes));
    }

    // ── Regions ──
    let regions_path = wb_dir.join("regions.json");
    if regions_path.exists() {
        let data = fs::read_to_string(&regions_path)?;
        let regions: Vec<WBRegion> = serde_json::from_str(&data)?;
        for r in &regions {
            if r.code.is_empty() || r.name.is_empty() {
                continue;
            }
            let id = graph.create_node("Region");
            if let Some(n) = graph.get_node_mut(id) {
                n.set_property("code", PropertyValue::String(r.code.clone()));
                n.set_property("name", PropertyValue::String(clean_str(&r.name)));
            }
            region_map.insert(r.code.clone(), id);
            region_nodes += 1;
        }
        eprintln!("    Regions: {}", format_num(region_nodes));
    }

    // ── Country → Region edges ──
    if countries_path.exists() {
        let data = fs::read_to_string(&countries_path)?;
        let countries: Vec<WBCountry> = serde_json::from_str(&data)?;
        let mut cr_edges = 0;
        for c in &countries {
            if let (Some(&country_id), Some(&region_id)) =
                (country_map.get(&c.id), region_map.get(&c.region.id))
            {
                let _ = graph.create_edge(country_id, region_id, "IN_REGION");
                cr_edges += 1;
            }
        }
        total_edges += cr_edges;
        eprintln!("    IN_REGION edges: {}", format_num(cr_edges));
    }

    // ── WDI Indicator Categories ──
    for (ci, cat) in CATEGORIES.iter().enumerate() {
        let cat_path = wb_dir.join(cat.filename);
        if !cat_path.exists() {
            eprintln!("    {}: (no data file)", cat.label);
            continue;
        }
        let data = fs::read_to_string(&cat_path)?;
        let records: Vec<WDIRecord> = serde_json::from_str(&data)?;
        let mut nodes = 0;
        let mut edges = 0;

        let t0 = Instant::now();
        for rec in &records {
            let cc = if rec.countryiso3code.is_empty() {
                &rec.country.id
            } else {
                &rec.countryiso3code
            };
            let country_id = match country_map.get(cc.as_str()) {
                Some(&id) => id,
                None => continue,
            };
            let date = match &rec.date {
                Some(d) if !d.is_empty() => d.as_str(),
                _ => continue,
            };
            let value = match rec.value {
                Some(v) => v,
                None => continue,
            };

            let nid_str = format!("{}-{}-{}-{}", cat.id_prefix, cc, rec.indicator.id, date);
            let node_id = graph.create_node(cat.label);
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid_str));
                n.set_property(
                    "indicator_code",
                    PropertyValue::String(rec.indicator.id.clone()),
                );
                n.set_property(
                    "indicator_name",
                    PropertyValue::String(clean_str(&rec.indicator.value)),
                );
                if let Ok(y) = date.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                n.set_property("value", PropertyValue::Float(value));
            }
            nodes += 1;

            let _ = graph.create_edge(country_id, node_id, cat.edge_type);
            edges += 1;

            if nodes % 50_000 == 0 {
                eprintln!(
                    "      ... {} nodes ({:.0}/sec)",
                    format_num(nodes),
                    nodes as f64 / t0.elapsed().as_secs_f64()
                );
            }
        }
        cat_nodes[ci] = nodes;
        total_edges += edges;
        eprintln!(
            "    {}: {} nodes, {} edges",
            cat.label,
            format_num(nodes),
            format_num(edges)
        );
    }

    // ── WHO Air Quality (CSV) ──
    let aq_path = data_dir.join("who_airquality").join("air_quality.csv");
    let mut aq_nodes = 0;
    if aq_path.exists() {
        let records = parse_csv_records(&aq_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let city = rec.fields.get("city").map(|s| s.as_str()).unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            let pm25 = rec.fields.get("pm25").and_then(|s| s.parse::<f64>().ok());

            if year.is_empty() {
                continue;
            }

            let nid_str = format!("EF-{}-AQ-{}-{}", rec.country_code, city, year);
            let node_id = graph.create_node("EnvironmentalFactor");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid_str));
                n.set_property(
                    "indicator_code",
                    PropertyValue::String("AIR_QUALITY".to_string()),
                );
                n.set_property(
                    "indicator_name",
                    PropertyValue::String("Ambient air quality".to_string()),
                );
                n.set_property("category", PropertyValue::String("air_quality".to_string()));
                n.set_property("city", PropertyValue::String(clean_str(city)));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                if let Some(v) = pm25 {
                    n.set_property("value", PropertyValue::Float(v));
                }
                let pm10 = rec.fields.get("pm10").and_then(|s| s.parse::<f64>().ok());
                if let Some(v) = pm10 {
                    n.set_property("pm10", PropertyValue::Float(v));
                }
            }
            let _ = graph.create_edge(country_id, node_id, "ENVIRONMENT_OF");
            aq_nodes += 1;
            total_edges += 1;
        }
        cat_nodes[1] += aq_nodes;
        eprintln!("    Air quality: {} nodes", format_num(aq_nodes));
    }

    // ── FAO AQUASTAT (CSV) ──
    let fao_path = data_dir.join("fao").join("aquastat.csv");
    let mut fao_nodes = 0;
    if fao_path.exists() {
        let records = parse_csv_records(&fao_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let indicator_code = rec
                .fields
                .get("indicator_code")
                .map(|s| s.as_str())
                .unwrap_or("");
            let indicator_name = rec
                .fields
                .get("indicator_name")
                .map(|s| s.as_str())
                .unwrap_or("");
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            let value = rec.fields.get("value").and_then(|s| s.parse::<f64>().ok());

            if indicator_code.is_empty() || year.is_empty() || value.is_none() {
                continue;
            }

            let nid_str = format!("WR-{}-{}-{}", rec.country_code, indicator_code, year);
            let node_id = graph.create_node("WaterResource");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid_str));
                n.set_property(
                    "indicator_code",
                    PropertyValue::String(indicator_code.to_string()),
                );
                n.set_property(
                    "indicator_name",
                    PropertyValue::String(clean_str(indicator_name)),
                );
                n.set_property("category", PropertyValue::String("water".to_string()));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                if let Some(v) = value {
                    n.set_property("value", PropertyValue::Float(v));
                }
            }
            let _ = graph.create_edge(country_id, node_id, "WATER_RESOURCE_OF");
            fao_nodes += 1;
            total_edges += 1;
        }
        cat_nodes[4] += fao_nodes;
        eprintln!("    AQUASTAT: {} nodes", format_num(fao_nodes));
    }

    // ── UNDP HDI (CSV) ──
    let hdi_path = data_dir.join("undp").join("hdi.csv");
    let mut hdi_nodes = 0;
    if hdi_path.exists() {
        let records = parse_csv_records(&hdi_path)?;
        for rec in &records {
            let country_id = match country_map.get(&rec.country_code) {
                Some(&id) => id,
                None => continue,
            };
            let year = rec.fields.get("year").map(|s| s.as_str()).unwrap_or("");
            let hdi = rec.fields.get("hdi").and_then(|s| s.parse::<f64>().ok());
            let rank = rec.fields.get("rank").and_then(|s| s.parse::<i64>().ok());

            if year.is_empty() || hdi.is_none() {
                continue;
            }

            let nid_str = format!("SE-{}-HDI-{}", rec.country_code, year);
            let node_id = graph.create_node("SocioeconomicIndicator");
            if let Some(n) = graph.get_node_mut(node_id) {
                n.set_property("id", PropertyValue::String(nid_str));
                n.set_property("indicator_code", PropertyValue::String("HDI".to_string()));
                n.set_property(
                    "indicator_name",
                    PropertyValue::String("Human Development Index".to_string()),
                );
                n.set_property("category", PropertyValue::String("development".to_string()));
                if let Ok(y) = year.parse::<i64>() {
                    n.set_property("year", PropertyValue::Integer(y));
                }
                if let Some(v) = hdi {
                    n.set_property("value", PropertyValue::Float(v));
                }
                if let Some(r) = rank {
                    n.set_property("rank", PropertyValue::Integer(r));
                }
            }
            let _ = graph.create_edge(country_id, node_id, "HAS_INDICATOR");
            hdi_nodes += 1;
            total_edges += 1;
        }
        cat_nodes[0] += hdi_nodes;
        eprintln!("    HDI: {} nodes", format_num(hdi_nodes));
    }

    let total_nodes = country_nodes + region_nodes + cat_nodes.iter().sum::<usize>();

    Ok(LoadResult {
        country_nodes,
        region_nodes,
        socioeconomic_nodes: cat_nodes[0],
        environmental_nodes: cat_nodes[1],
        nutrition_nodes: cat_nodes[2],
        demographic_nodes: cat_nodes[3],
        water_nodes: cat_nodes[4],
        total_nodes,
        total_edges,
    })
}
