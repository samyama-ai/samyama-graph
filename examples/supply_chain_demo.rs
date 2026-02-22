//! Global Pharmaceutical Supply Chain Simulation
//!
//! A comprehensive Samyama graph database demo modeling a worldwide pharmaceutical
//! supply chain with 100+ entities across 30+ countries.
//!
//! Scenarios demonstrated:
//!   A) Supply chain topology visualization and summary
//!   B) Disruption: Suez Canal blockage -- impact analysis
//!   C) Disruption: Hamburg port strike -- rerouting options
//!   D) Cold-chain monitoring for temperature-sensitive biologics
//!   E) Criticality analysis via PageRank (ports and suppliers)
//!   F) Route optimization via Jaya algorithm (container allocation)
//!   G) Alternative supplier search via vector similarity
//!
//! Run: `cargo run --example supply_chain_demo`

use samyama_sdk::{
    EmbeddedClient, SamyamaClient, AlgorithmClient, VectorClient,
    Label, PropertyValue, PropertyMap,
    AgentConfig, LLMProvider, NLQConfig,
    DistanceMetric, PageRankConfig,
    JayaSolver, SolverConfig, Problem, Array1,
};
use std::collections::HashMap;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Deterministic mock embedding from a string seed
// ---------------------------------------------------------------------------
fn mock_embedding(seed: &str, dim: usize) -> Vec<f32> {
    // Simple deterministic hash-based vector generation
    let mut hash: u64 = 5381;
    for b in seed.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(b as u64);
    }
    (0..dim)
        .map(|i| {
            let h = hash.wrapping_mul(2654435761).wrapping_add(i as u64 * 7919);
            // Map to [-1, 1] range and normalize later
            ((h % 2000) as f32 / 1000.0) - 1.0
        })
        .collect()
}

/// Cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na <= 0.0 || nb <= 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

// ---------------------------------------------------------------------------
// Data definitions
// ---------------------------------------------------------------------------

struct PortDef {
    name: &'static str,
    country: &'static str,
    region: &'static str,
    capacity_teu: i64,       // annual TEU capacity in thousands
    lat: f64,
    lon: f64,
}

struct SupplierDef {
    name: &'static str,
    country: &'static str,
    capabilities: &'static str,
    annual_revenue_m: i64,    // in millions USD
    gmp_certified: bool,
    nearest_port_idx: usize,  // index into port_data()
}

struct ProductDef {
    name: &'static str,
    category: &'static str,
    cold_chain: bool,
    unit_value: f64,          // USD per unit
    annual_demand_m: f64,     // millions of units globally
}

struct ShippingLineDef {
    name: &'static str,
    fleet_size: i64,
    headquarter: &'static str,
}

struct ShipmentDef {
    id: &'static str,
    product_idx: usize,
    supplier_idx: usize,
    origin_port_idx: usize,
    dest_port_idx: usize,
    via_port_indices: Vec<usize>,  // intermediate ports
    carrier_idx: usize,
    containers: i64,
    value_usd: f64,
    status: &'static str,
}

// ---------------------------------------------------------------------------
// Data tables
// ---------------------------------------------------------------------------

fn port_data() -> Vec<PortDef> {
    vec![
        PortDef { name: "Shanghai",          country: "China",       region: "East Asia",       capacity_teu: 47000, lat: 31.23, lon: 121.47 },
        PortDef { name: "Singapore",         country: "Singapore",   region: "Southeast Asia",  capacity_teu: 37000, lat:  1.29, lon: 103.85 },
        PortDef { name: "Rotterdam",         country: "Netherlands", region: "Europe",          capacity_teu: 14500, lat: 51.92, lon:  4.48 },
        PortDef { name: "Hamburg",            country: "Germany",     region: "Europe",          capacity_teu:  8700, lat: 53.55, lon:  9.99 },
        PortDef { name: "Los Angeles",       country: "USA",         region: "North America",   capacity_teu: 9600,  lat: 33.74, lon: -118.27 },
        PortDef { name: "Long Beach",        country: "USA",         region: "North America",   capacity_teu: 9100,  lat: 33.77, lon: -118.19 },
        PortDef { name: "Busan",             country: "South Korea", region: "East Asia",       capacity_teu: 22000, lat: 35.10, lon: 129.04 },
        PortDef { name: "Hong Kong",         country: "China",       region: "East Asia",       capacity_teu: 18000, lat: 22.29, lon: 114.15 },
        PortDef { name: "Shenzhen",          country: "China",       region: "East Asia",       capacity_teu: 27700, lat: 22.54, lon: 114.06 },
        PortDef { name: "Jebel Ali (Dubai)", country: "UAE",         region: "Middle East",     capacity_teu: 14500, lat: 25.01, lon: 55.06 },
        PortDef { name: "Antwerp",           country: "Belgium",     region: "Europe",          capacity_teu: 12000, lat: 51.26, lon:  4.40 },
        PortDef { name: "Tanjung Pelepas",   country: "Malaysia",    region: "Southeast Asia",  capacity_teu: 11000, lat:  1.37, lon: 103.55 },
        PortDef { name: "Felixstowe",        country: "UK",          region: "Europe",          capacity_teu:  3800, lat: 51.96, lon:  1.35 },
        PortDef { name: "Yokohama",          country: "Japan",       region: "East Asia",       capacity_teu:  2900, lat: 35.44, lon: 139.64 },
        PortDef { name: "Santos",            country: "Brazil",      region: "South America",   capacity_teu:  4300, lat: -23.95, lon: -46.31 },
        PortDef { name: "Colombo",           country: "Sri Lanka",   region: "South Asia",      capacity_teu:  7200, lat:  6.94, lon: 79.85 },
        PortDef { name: "Piraeus",           country: "Greece",      region: "Europe",          capacity_teu:  5400, lat: 37.94, lon: 23.65 },
        PortDef { name: "Nhava Sheva",       country: "India",       region: "South Asia",      capacity_teu:  5600, lat: 18.95, lon: 72.95 },
        PortDef { name: "Laem Chabang",      country: "Thailand",    region: "Southeast Asia",  capacity_teu:  8300, lat: 13.08, lon: 100.88 },
        PortDef { name: "Kaohsiung",         country: "Taiwan",      region: "East Asia",       capacity_teu:  9900, lat: 22.62, lon: 120.30 },
    ]
}

fn supplier_data() -> Vec<SupplierDef> {
    vec![
        // India (5)
        SupplierDef { name: "Aurobindo Pharma",       country: "India",       capabilities: "High-volume generic API manufacturing, ARV drugs, cardiovascular APIs, antibiotics, oral solid dosage forms",                             annual_revenue_m: 3200,  gmp_certified: true,  nearest_port_idx: 17 },
        SupplierDef { name: "Sun Pharmaceutical",      country: "India",       capabilities: "Specialty generics, dermatology, ophthalmology APIs, controlled substances, complex generics formulation",                                  annual_revenue_m: 5100,  gmp_certified: true,  nearest_port_idx: 17 },
        SupplierDef { name: "Dr. Reddy's Labs",        country: "India",       capabilities: "Biosimilars development, peptide synthesis, oncology APIs, injectable formulations, complex chemistry",                                     annual_revenue_m: 2800,  gmp_certified: true,  nearest_port_idx: 17 },
        SupplierDef { name: "Cipla Ltd",               country: "India",       capabilities: "Respiratory drug delivery, inhalation products, HIV antiretrovirals, pediatric formulations, pulmonary APIs",                               annual_revenue_m: 2600,  gmp_certified: true,  nearest_port_idx: 17 },
        SupplierDef { name: "Lupin Ltd",               country: "India",       capabilities: "Cardiovascular APIs, diabetes drugs, CNS medications, tuberculosis treatments, oral contraceptives",                                        annual_revenue_m: 2200,  gmp_certified: true,  nearest_port_idx: 17 },
        // Germany (4)
        SupplierDef { name: "Merck KGaA",              country: "Germany",     capabilities: "High-purity solvents, chromatography media, lab chemicals, process chemicals, semiconductor materials",                                     annual_revenue_m: 22000, gmp_certified: true,  nearest_port_idx: 3 },
        SupplierDef { name: "Bayer AG",                country: "Germany",     capabilities: "Crop science APIs, radiology contrast agents, hemophilia treatments, cardiovascular drugs, oncology pipeline",                               annual_revenue_m: 50000, gmp_certified: true,  nearest_port_idx: 3 },
        SupplierDef { name: "Boehringer Ingelheim",    country: "Germany",     capabilities: "Biopharmaceutical contract manufacturing, mammalian cell culture, viral vector production, mAb development",                                annual_revenue_m: 24000, gmp_certified: true,  nearest_port_idx: 3 },
        SupplierDef { name: "BASF Pharma",             country: "Germany",     capabilities: "Pharmaceutical excipients, vitamin APIs, omega-3 fatty acids, solubilization technology, coatings",                                         annual_revenue_m: 8500,  gmp_certified: true,  nearest_port_idx: 3 },
        // USA (5)
        SupplierDef { name: "Pfizer",                  country: "USA",         capabilities: "mRNA vaccine technology, oncology biologics, immunology drugs, rare disease treatments, gene therapy",                                       annual_revenue_m: 58000, gmp_certified: true,  nearest_port_idx: 4 },
        SupplierDef { name: "Johnson & Johnson",       country: "USA",         capabilities: "Immunology biologics, oncology, neuroscience, surgical devices, vaccine development, cell therapy",                                          annual_revenue_m: 85000, gmp_certified: true,  nearest_port_idx: 4 },
        SupplierDef { name: "Abbott Labs",             country: "USA",         capabilities: "Diagnostics reagents, nutritional products, cardiovascular devices, diabetes monitoring, neuromodulation",                                   annual_revenue_m: 40000, gmp_certified: true,  nearest_port_idx: 4 },
        SupplierDef { name: "Merck & Co",              country: "USA",         capabilities: "Immuno-oncology checkpoint inhibitors, vaccines, antiviral drugs, diabetes treatments, animal health",                                      annual_revenue_m: 60000, gmp_certified: true,  nearest_port_idx: 4 },
        SupplierDef { name: "Eli Lilly",               country: "USA",         capabilities: "GLP-1 receptor agonists, insulin manufacturing, oncology antibodies, Alzheimer disease antibodies, migraine",                               annual_revenue_m: 34000, gmp_certified: true,  nearest_port_idx: 4 },
        // Switzerland (3)
        SupplierDef { name: "Novartis",                country: "Switzerland", capabilities: "Gene therapy, CAR-T cell therapy, ophthalmology, cardiovascular, radioligand therapy, biosimilars",                                         annual_revenue_m: 51000, gmp_certified: true,  nearest_port_idx: 2 },
        SupplierDef { name: "Roche",                   country: "Switzerland", capabilities: "Oncology diagnostics, monoclonal antibodies, companion diagnostics, tissue-based cancer tests, sequencing",                                annual_revenue_m: 65000, gmp_certified: true,  nearest_port_idx: 2 },
        SupplierDef { name: "Lonza Group",             country: "Switzerland", capabilities: "Biologics CDMO, mammalian and microbial fermentation, cell and gene therapy manufacturing, ADC conjugation",                                annual_revenue_m: 6500,  gmp_certified: true,  nearest_port_idx: 2 },
        // China (3)
        SupplierDef { name: "Zhejiang Hisun",          country: "China",       capabilities: "Antibiotic APIs, antitumor drugs, cardiovascular APIs, high-volume fermentation, sterile manufacturing",                                    annual_revenue_m: 1800,  gmp_certified: true,  nearest_port_idx: 0 },
        SupplierDef { name: "Fosun Pharma",            country: "China",       capabilities: "mRNA vaccine production, oncology drugs, autoimmune biologics, medical devices, diagnostic imaging",                                        annual_revenue_m: 4200,  gmp_certified: true,  nearest_port_idx: 0 },
        SupplierDef { name: "Shanghai Pharma",         country: "China",       capabilities: "Chemical drug manufacturing, traditional Chinese medicine, pharmaceutical distribution, API intermediates",                                  annual_revenue_m: 25000, gmp_certified: true,  nearest_port_idx: 0 },
        // Japan (3)
        SupplierDef { name: "Takeda",                  country: "Japan",       capabilities: "Plasma-derived therapies, rare diseases, GI drugs, neuroscience, oncology, vaccine production",                                             annual_revenue_m: 30000, gmp_certified: true,  nearest_port_idx: 13 },
        SupplierDef { name: "Astellas",                country: "Japan",       capabilities: "Urology and transplant drugs, oncology, gene therapy, muscle disease treatments, blindness therapies",                                      annual_revenue_m: 12000, gmp_certified: true,  nearest_port_idx: 13 },
        SupplierDef { name: "Daiichi Sankyo",          country: "Japan",       capabilities: "Antibody-drug conjugates, oncology, cardiovascular, pain management, vaccine development",                                                  annual_revenue_m: 9500,  gmp_certified: true,  nearest_port_idx: 13 },
        // UK (2)
        SupplierDef { name: "GSK",                     country: "UK",          capabilities: "Vaccines, HIV treatments, respiratory medicines, oncology, immune-mediated diseases, shingles prevention",                                  annual_revenue_m: 36000, gmp_certified: true,  nearest_port_idx: 12 },
        SupplierDef { name: "AstraZeneca",             country: "UK",          capabilities: "Oncology immunotherapies, cardiovascular, renal, respiratory biologics, COVID vaccine, rare diseases",                                       annual_revenue_m: 44000, gmp_certified: true,  nearest_port_idx: 12 },
        // France (2)
        SupplierDef { name: "Sanofi",                  country: "France",      capabilities: "Insulin production, rare blood disorders, immunology biologics, vaccines, multiple sclerosis, oncology",                                    annual_revenue_m: 43000, gmp_certified: true,  nearest_port_idx: 2 },
        SupplierDef { name: "Servier",                 country: "France",      capabilities: "Cardiovascular drugs, diabetes treatments, oncology, neuroscience, immuno-inflammation therapies",                                          annual_revenue_m: 5500,  gmp_certified: true,  nearest_port_idx: 2 },
        // Ireland (2)
        SupplierDef { name: "Allergan",                country: "Ireland",     capabilities: "Botulinum toxin, eye care, dermal fillers, CNS drugs, gastroenterology, women's health",                                                    annual_revenue_m: 16000, gmp_certified: true,  nearest_port_idx: 12 },
        SupplierDef { name: "Jazz Pharmaceuticals",    country: "Ireland",     capabilities: "Sleep disorder treatments, oncology hematology, narcolepsy drugs, epilepsy treatments, GHB formulations",                                   annual_revenue_m: 3600,  gmp_certified: true,  nearest_port_idx: 12 },
        // South Korea (1)
        SupplierDef { name: "Samsung Biologics",       country: "South Korea", capabilities: "Biosimilar CDMO, large-scale mammalian cell culture, monoclonal antibody production, aseptic fill-finish",                                  annual_revenue_m: 2500,  gmp_certified: true,  nearest_port_idx: 6 },
        // Israel (1)
        SupplierDef { name: "Teva Pharmaceutical",     country: "Israel",      capabilities: "Generic drug manufacturing, CNS specialty drugs, biosimilars, respiratory generics, migraine treatments",                                   annual_revenue_m: 15000, gmp_certified: true,  nearest_port_idx: 16 },
    ]
}

fn product_data() -> Vec<ProductDef> {
    vec![
        ProductDef { name: "Metformin",      category: "Diabetes",       cold_chain: false, unit_value: 0.15,    annual_demand_m: 1500.0 },
        ProductDef { name: "Atorvastatin",   category: "Cardiovascular", cold_chain: false, unit_value: 0.30,    annual_demand_m: 1200.0 },
        ProductDef { name: "Lisinopril",     category: "Cardiovascular", cold_chain: false, unit_value: 0.12,    annual_demand_m: 900.0 },
        ProductDef { name: "Amoxicillin",    category: "Antibiotic",     cold_chain: false, unit_value: 0.25,    annual_demand_m: 2000.0 },
        ProductDef { name: "Omeprazole",     category: "GI",             cold_chain: false, unit_value: 0.20,    annual_demand_m: 800.0 },
        ProductDef { name: "Losartan",       category: "Cardiovascular", cold_chain: false, unit_value: 0.18,    annual_demand_m: 700.0 },
        ProductDef { name: "Amlodipine",     category: "Cardiovascular", cold_chain: false, unit_value: 0.10,    annual_demand_m: 1100.0 },
        ProductDef { name: "Gabapentin",     category: "Neurology",      cold_chain: false, unit_value: 0.35,    annual_demand_m: 500.0 },
        ProductDef { name: "Sertraline",     category: "Psychiatry",     cold_chain: false, unit_value: 0.22,    annual_demand_m: 600.0 },
        ProductDef { name: "Montelukast",    category: "Respiratory",    cold_chain: false, unit_value: 0.28,    annual_demand_m: 450.0 },
        ProductDef { name: "Ibuprofen",      category: "Pain",           cold_chain: false, unit_value: 0.08,    annual_demand_m: 3000.0 },
        ProductDef { name: "Acetaminophen",  category: "Pain",           cold_chain: false, unit_value: 0.06,    annual_demand_m: 4000.0 },
        ProductDef { name: "Ozempic",        category: "Diabetes",       cold_chain: true,  unit_value: 850.00,  annual_demand_m: 25.0 },   // biologic
        ProductDef { name: "Keytruda",       category: "Oncology",       cold_chain: true,  unit_value: 4800.00, annual_demand_m: 5.0 },   // biologic
        ProductDef { name: "Humira",         category: "Immunology",     cold_chain: true,  unit_value: 2700.00, annual_demand_m: 12.0 },  // biologic
        ProductDef { name: "Remdesivir",     category: "Antiviral",      cold_chain: true,  unit_value: 520.00,  annual_demand_m: 8.0 },   // biologic
        ProductDef { name: "Paxlovid",       category: "Antiviral",      cold_chain: false, unit_value: 530.00,  annual_demand_m: 15.0 },
        ProductDef { name: "Eliquis",        category: "Cardiovascular", cold_chain: false, unit_value: 7.50,    annual_demand_m: 200.0 },
        ProductDef { name: "Jardiance",      category: "Diabetes",       cold_chain: false, unit_value: 15.00,   annual_demand_m: 100.0 },
        ProductDef { name: "Dupixent",       category: "Immunology",     cold_chain: true,  unit_value: 3200.00, annual_demand_m: 6.0 },   // biologic
    ]
}

fn shipping_line_data() -> Vec<ShippingLineDef> {
    vec![
        ShippingLineDef { name: "Maersk",        fleet_size: 730, headquarter: "Denmark" },
        ShippingLineDef { name: "MSC",            fleet_size: 760, headquarter: "Switzerland" },
        ShippingLineDef { name: "CMA CGM",        fleet_size: 590, headquarter: "France" },
        ShippingLineDef { name: "COSCO",          fleet_size: 480, headquarter: "China" },
        ShippingLineDef { name: "Hapag-Lloyd",    fleet_size: 260, headquarter: "Germany" },
        ShippingLineDef { name: "ONE",             fleet_size: 210, headquarter: "Japan" },
        ShippingLineDef { name: "Evergreen",      fleet_size: 200, headquarter: "Taiwan" },
        ShippingLineDef { name: "Yang Ming",      fleet_size: 90,  headquarter: "Taiwan" },
        ShippingLineDef { name: "HMM",             fleet_size: 80,  headquarter: "South Korea" },
        ShippingLineDef { name: "ZIM",             fleet_size: 110, headquarter: "Israel" },
        ShippingLineDef { name: "PIL",             fleet_size: 95,  headquarter: "Singapore" },
        ShippingLineDef { name: "Wan Hai",        fleet_size: 75,  headquarter: "Taiwan" },
        ShippingLineDef { name: "IRISL",          fleet_size: 50,  headquarter: "Iran" },
        ShippingLineDef { name: "KMTC",            fleet_size: 65,  headquarter: "South Korea" },
        ShippingLineDef { name: "SM Line",        fleet_size: 55,  headquarter: "South Korea" },
    ]
}

fn shipment_data() -> Vec<ShipmentDef> {
    vec![
        // India -> Europe routes (via Suez)
        ShipmentDef { id: "SH-001", product_idx: 0,  supplier_idx: 0,  origin_port_idx: 17, dest_port_idx: 2,  via_port_indices: vec![9, 16], carrier_idx: 0,  containers: 45, value_usd: 2_250_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-002", product_idx: 1,  supplier_idx: 4,  origin_port_idx: 17, dest_port_idx: 3,  via_port_indices: vec![9, 16], carrier_idx: 4,  containers: 30, value_usd: 1_800_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-003", product_idx: 3,  supplier_idx: 3,  origin_port_idx: 17, dest_port_idx: 12, via_port_indices: vec![15, 9], carrier_idx: 1,  containers: 55, value_usd: 3_300_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-004", product_idx: 2,  supplier_idx: 1,  origin_port_idx: 17, dest_port_idx: 10, via_port_indices: vec![9],     carrier_idx: 2,  containers: 25, value_usd: 900_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-005", product_idx: 14, supplier_idx: 2,  origin_port_idx: 17, dest_port_idx: 4,  via_port_indices: vec![1],     carrier_idx: 0,  containers: 8,  value_usd: 21_600_000.0, status: "In Transit" },
        // China -> USA routes (trans-Pacific)
        ShipmentDef { id: "SH-006", product_idx: 11, supplier_idx: 19, origin_port_idx: 0,  dest_port_idx: 4,  via_port_indices: vec![],      carrier_idx: 3,  containers: 80, value_usd: 1_920_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-007", product_idx: 10, supplier_idx: 17, origin_port_idx: 8,  dest_port_idx: 5,  via_port_indices: vec![],      carrier_idx: 3,  containers: 70, value_usd: 1_680_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-008", product_idx: 4,  supplier_idx: 18, origin_port_idx: 0,  dest_port_idx: 5,  via_port_indices: vec![19],    carrier_idx: 6,  containers: 40, value_usd: 1_600_000.0,  status: "Loading" },
        ShipmentDef { id: "SH-009", product_idx: 15, supplier_idx: 18, origin_port_idx: 0,  dest_port_idx: 4,  via_port_indices: vec![],      carrier_idx: 3,  containers: 12, value_usd: 6_240_000.0,  status: "In Transit" },
        // China -> Europe routes (via Suez)
        ShipmentDef { id: "SH-010", product_idx: 5,  supplier_idx: 17, origin_port_idx: 0,  dest_port_idx: 2,  via_port_indices: vec![1, 9],  carrier_idx: 1,  containers: 50, value_usd: 1_800_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-011", product_idx: 6,  supplier_idx: 19, origin_port_idx: 8,  dest_port_idx: 3,  via_port_indices: vec![1, 9, 16], carrier_idx: 2, containers: 35, value_usd: 700_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-012", product_idx: 7,  supplier_idx: 17, origin_port_idx: 0,  dest_port_idx: 10, via_port_indices: vec![7, 1, 9], carrier_idx: 4, containers: 28, value_usd: 1_960_000.0, status: "In Transit" },
        // Japan -> USA
        ShipmentDef { id: "SH-013", product_idx: 12, supplier_idx: 20, origin_port_idx: 13, dest_port_idx: 4,  via_port_indices: vec![],      carrier_idx: 5,  containers: 6,  value_usd: 5_100_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-014", product_idx: 13, supplier_idx: 22, origin_port_idx: 13, dest_port_idx: 5,  via_port_indices: vec![],      carrier_idx: 5,  containers: 4,  value_usd: 19_200_000.0, status: "In Transit" },
        // Germany -> USA
        ShipmentDef { id: "SH-015", product_idx: 18, supplier_idx: 7,  origin_port_idx: 3,  dest_port_idx: 4,  via_port_indices: vec![],      carrier_idx: 4,  containers: 20, value_usd: 6_000_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-016", product_idx: 19, supplier_idx: 25, origin_port_idx: 3,  dest_port_idx: 5,  via_port_indices: vec![],      carrier_idx: 0,  containers: 5,  value_usd: 16_000_000.0, status: "In Transit" },
        // USA -> Europe
        ShipmentDef { id: "SH-017", product_idx: 13, supplier_idx: 12, origin_port_idx: 4,  dest_port_idx: 2,  via_port_indices: vec![],      carrier_idx: 0,  containers: 3,  value_usd: 14_400_000.0, status: "In Transit" },
        ShipmentDef { id: "SH-018", product_idx: 16, supplier_idx: 9,  origin_port_idx: 4,  dest_port_idx: 12, via_port_indices: vec![],      carrier_idx: 9,  containers: 15, value_usd: 7_950_000.0,  status: "In Transit" },
        // Switzerland -> USA
        ShipmentDef { id: "SH-019", product_idx: 14, supplier_idx: 14, origin_port_idx: 2,  dest_port_idx: 4,  via_port_indices: vec![],      carrier_idx: 1,  containers: 10, value_usd: 27_000_000.0, status: "In Transit" },
        // India -> USA (Pacific route)
        ShipmentDef { id: "SH-020", product_idx: 0,  supplier_idx: 0,  origin_port_idx: 17, dest_port_idx: 5,  via_port_indices: vec![1, 7],  carrier_idx: 10, containers: 60, value_usd: 2_700_000.0,  status: "In Transit" },
        // Korea -> Europe
        ShipmentDef { id: "SH-021", product_idx: 14, supplier_idx: 29, origin_port_idx: 6,  dest_port_idx: 2,  via_port_indices: vec![1, 9],  carrier_idx: 8,  containers: 7,  value_usd: 18_900_000.0, status: "In Transit" },
        // China -> South America
        ShipmentDef { id: "SH-022", product_idx: 11, supplier_idx: 19, origin_port_idx: 0,  dest_port_idx: 14, via_port_indices: vec![],      carrier_idx: 3,  containers: 40, value_usd: 960_000.0,    status: "In Transit" },
        // UK -> India
        ShipmentDef { id: "SH-023", product_idx: 19, supplier_idx: 25, origin_port_idx: 12, dest_port_idx: 17, via_port_indices: vec![9],     carrier_idx: 0,  containers: 3,  value_usd: 9_600_000.0,  status: "In Transit" },
        // France -> Japan
        ShipmentDef { id: "SH-024", product_idx: 12, supplier_idx: 25, origin_port_idx: 2,  dest_port_idx: 13, via_port_indices: vec![9, 1],  carrier_idx: 2,  containers: 5,  value_usd: 4_250_000.0,  status: "In Transit" },
        // India -> Singapore
        ShipmentDef { id: "SH-025", product_idx: 3,  supplier_idx: 0,  origin_port_idx: 17, dest_port_idx: 1,  via_port_indices: vec![15],    carrier_idx: 10, containers: 35, value_usd: 1_750_000.0,  status: "In Transit" },
        // Thailand -> Europe
        ShipmentDef { id: "SH-026", product_idx: 4,  supplier_idx: 20, origin_port_idx: 18, dest_port_idx: 2,  via_port_indices: vec![1, 9],  carrier_idx: 6,  containers: 25, value_usd: 1_000_000.0,  status: "In Transit" },
        // USA -> Brazil
        ShipmentDef { id: "SH-027", product_idx: 13, supplier_idx: 12, origin_port_idx: 5,  dest_port_idx: 14, via_port_indices: vec![],      carrier_idx: 9,  containers: 2,  value_usd: 9_600_000.0,  status: "In Transit" },
        // Germany -> Singapore
        ShipmentDef { id: "SH-028", product_idx: 18, supplier_idx: 5,  origin_port_idx: 3,  dest_port_idx: 1,  via_port_indices: vec![9],     carrier_idx: 4,  containers: 18, value_usd: 5_400_000.0,  status: "In Transit" },
        // China -> Middle East
        ShipmentDef { id: "SH-029", product_idx: 10, supplier_idx: 19, origin_port_idx: 8,  dest_port_idx: 9,  via_port_indices: vec![1],     carrier_idx: 3,  containers: 45, value_usd: 1_080_000.0,  status: "In Transit" },
        // India -> Greece
        ShipmentDef { id: "SH-030", product_idx: 0,  supplier_idx: 4,  origin_port_idx: 17, dest_port_idx: 16, via_port_indices: vec![9],     carrier_idx: 2,  containers: 30, value_usd: 1_350_000.0,  status: "In Transit" },
        // Additional high-value biologic shipments
        ShipmentDef { id: "SH-031", product_idx: 12, supplier_idx: 13, origin_port_idx: 4,  dest_port_idx: 3,  via_port_indices: vec![],      carrier_idx: 0,  containers: 4,  value_usd: 3_400_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-032", product_idx: 19, supplier_idx: 25, origin_port_idx: 2,  dest_port_idx: 13, via_port_indices: vec![9, 1],  carrier_idx: 2,  containers: 3,  value_usd: 9_600_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-033", product_idx: 14, supplier_idx: 10, origin_port_idx: 4,  dest_port_idx: 1,  via_port_indices: vec![],      carrier_idx: 1,  containers: 6,  value_usd: 16_200_000.0, status: "In Transit" },
        ShipmentDef { id: "SH-034", product_idx: 13, supplier_idx: 15, origin_port_idx: 2,  dest_port_idx: 17, via_port_indices: vec![16, 9], carrier_idx: 1,  containers: 2,  value_usd: 9_600_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-035", product_idx: 15, supplier_idx: 9,  origin_port_idx: 4,  dest_port_idx: 13, via_port_indices: vec![],      carrier_idx: 5,  containers: 8,  value_usd: 4_160_000.0,  status: "In Transit" },
        // Intra-Asia routes
        ShipmentDef { id: "SH-036", product_idx: 8,  supplier_idx: 1,  origin_port_idx: 17, dest_port_idx: 0,  via_port_indices: vec![1],     carrier_idx: 10, containers: 20, value_usd: 880_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-037", product_idx: 9,  supplier_idx: 20, origin_port_idx: 13, dest_port_idx: 1,  via_port_indices: vec![],      carrier_idx: 5,  containers: 15, value_usd: 840_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-038", product_idx: 17, supplier_idx: 9,  origin_port_idx: 4,  dest_port_idx: 6,  via_port_indices: vec![],      carrier_idx: 9,  containers: 22, value_usd: 3_300_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-039", product_idx: 7,  supplier_idx: 21, origin_port_idx: 13, dest_port_idx: 18, via_port_indices: vec![],      carrier_idx: 13, containers: 10, value_usd: 700_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-040", product_idx: 0,  supplier_idx: 17, origin_port_idx: 0,  dest_port_idx: 11, via_port_indices: vec![],      carrier_idx: 11, containers: 50, value_usd: 1_500_000.0,  status: "In Transit" },
        // Europe intra-routes
        ShipmentDef { id: "SH-041", product_idx: 18, supplier_idx: 6,  origin_port_idx: 3,  dest_port_idx: 16, via_port_indices: vec![10],    carrier_idx: 4,  containers: 12, value_usd: 3_600_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-042", product_idx: 1,  supplier_idx: 8,  origin_port_idx: 2,  dest_port_idx: 12, via_port_indices: vec![],      carrier_idx: 0,  containers: 18, value_usd: 1_080_000.0,  status: "In Transit" },
        // More trans-Pacific
        ShipmentDef { id: "SH-043", product_idx: 16, supplier_idx: 12, origin_port_idx: 5,  dest_port_idx: 19, via_port_indices: vec![],      carrier_idx: 7,  containers: 10, value_usd: 5_300_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-044", product_idx: 5,  supplier_idx: 30, origin_port_idx: 16, dest_port_idx: 4,  via_port_indices: vec![9],     carrier_idx: 9,  containers: 20, value_usd: 720_000.0,    status: "In Transit" },
        // Additional bulk generics
        ShipmentDef { id: "SH-045", product_idx: 3,  supplier_idx: 0,  origin_port_idx: 17, dest_port_idx: 14, via_port_indices: vec![15, 1], carrier_idx: 10, containers: 40, value_usd: 2_000_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-046", product_idx: 11, supplier_idx: 17, origin_port_idx: 0,  dest_port_idx: 9,  via_port_indices: vec![1],     carrier_idx: 3,  containers: 55, value_usd: 1_320_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-047", product_idx: 10, supplier_idx: 19, origin_port_idx: 0,  dest_port_idx: 15, via_port_indices: vec![1],     carrier_idx: 11, containers: 30, value_usd: 720_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-048", product_idx: 6,  supplier_idx: 3,  origin_port_idx: 17, dest_port_idx: 18, via_port_indices: vec![15],    carrier_idx: 10, containers: 25, value_usd: 500_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-049", product_idx: 2,  supplier_idx: 0,  origin_port_idx: 17, dest_port_idx: 2,  via_port_indices: vec![9, 16], carrier_idx: 1,  containers: 35, value_usd: 1_260_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-050", product_idx: 4,  supplier_idx: 4,  origin_port_idx: 17, dest_port_idx: 4,  via_port_indices: vec![1],     carrier_idx: 0,  containers: 30, value_usd: 1_200_000.0,  status: "In Transit" },
        // Hamburg-bound shipments (for port strike scenario)
        ShipmentDef { id: "SH-051", product_idx: 17, supplier_idx: 14, origin_port_idx: 2,  dest_port_idx: 3,  via_port_indices: vec![],      carrier_idx: 4,  containers: 15, value_usd: 2_250_000.0,  status: "In Transit" },
        ShipmentDef { id: "SH-052", product_idx: 9,  supplier_idx: 23, origin_port_idx: 12, dest_port_idx: 3,  via_port_indices: vec![],      carrier_idx: 0,  containers: 12, value_usd: 672_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-053", product_idx: 8,  supplier_idx: 30, origin_port_idx: 16, dest_port_idx: 3,  via_port_indices: vec![9],     carrier_idx: 9,  containers: 20, value_usd: 880_000.0,    status: "In Transit" },
        ShipmentDef { id: "SH-054", product_idx: 14, supplier_idx: 29, origin_port_idx: 6,  dest_port_idx: 3,  via_port_indices: vec![1, 9],  carrier_idx: 8,  containers: 5,  value_usd: 13_500_000.0, status: "In Transit" },
        ShipmentDef { id: "SH-055", product_idx: 0,  supplier_idx: 17, origin_port_idx: 0,  dest_port_idx: 3,  via_port_indices: vec![1, 9],  carrier_idx: 3,  containers: 60, value_usd: 1_800_000.0,  status: "In Transit" },
    ]
}

/// Shipping route connections (port index pairs representing major sea lanes)
fn route_connections() -> Vec<(usize, usize)> {
    vec![
        // Suez Canal corridor
        (9, 16),  // Dubai -> Piraeus
        (16, 2),  // Piraeus -> Rotterdam
        (16, 10), // Piraeus -> Antwerp
        (16, 3),  // Piraeus -> Hamburg
        (16, 12), // Piraeus -> Felixstowe
        (15, 9),  // Colombo -> Dubai
        (17, 15), // Nhava Sheva -> Colombo
        (17, 9),  // Nhava Sheva -> Dubai
        (1, 9),   // Singapore -> Dubai
        // Trans-Pacific
        (0, 4),   // Shanghai -> LA
        (0, 5),   // Shanghai -> Long Beach
        (8, 4),   // Shenzhen -> LA
        (8, 5),   // Shenzhen -> Long Beach
        (6, 4),   // Busan -> LA
        (13, 4),  // Yokohama -> LA
        (13, 5),  // Yokohama -> Long Beach
        (19, 4),  // Kaohsiung -> LA
        // Intra-Asia
        (0, 1),   // Shanghai -> Singapore
        (0, 7),   // Shanghai -> Hong Kong
        (0, 6),   // Shanghai -> Busan
        (0, 19),  // Shanghai -> Kaohsiung
        (8, 7),   // Shenzhen -> Hong Kong
        (8, 1),   // Shenzhen -> Singapore
        (1, 11),  // Singapore -> Tanjung Pelepas
        (1, 15),  // Singapore -> Colombo
        (1, 18),  // Singapore -> Laem Chabang
        (13, 6),  // Yokohama -> Busan
        (7, 19),  // Hong Kong -> Kaohsiung
        // Europe intra
        (2, 3),   // Rotterdam -> Hamburg
        (2, 10),  // Rotterdam -> Antwerp
        (2, 12),  // Rotterdam -> Felixstowe
        (10, 3),  // Antwerp -> Hamburg
        (10, 12), // Antwerp -> Felixstowe
        // Americas
        (4, 5),   // LA -> Long Beach
        (4, 14),  // LA -> Santos
        (5, 14),  // Long Beach -> Santos
        // Cross links
        (0, 11),  // Shanghai -> Tanjung Pelepas
        (17, 1),  // Nhava Sheva -> Singapore
    ]
}

/// SUPPLIES relationship: (supplier_idx, product_idx) pairs
fn supply_links() -> Vec<(usize, usize)> {
    vec![
        // India suppliers -> generics
        (0, 0), (0, 3), (0, 2),        // Aurobindo -> Metformin, Amoxicillin, Lisinopril
        (1, 1), (1, 8), (1, 6),        // Sun Pharma -> Atorvastatin, Sertraline, Amlodipine
        (2, 14),                        // Dr. Reddy's -> Humira (biosimilar)
        (3, 3), (3, 9),                // Cipla -> Amoxicillin, Montelukast
        (4, 0), (4, 5), (4, 1),        // Lupin -> Metformin, Losartan, Atorvastatin
        // Germany
        (5, 18),                        // Merck KGaA -> Jardiance
        (6, 1), (6, 17),               // Bayer -> Atorvastatin, Eliquis
        (7, 18), (7, 19),              // Boehringer -> Jardiance, Dupixent
        (8, 4), (8, 7),                // BASF Pharma -> Omeprazole, Gabapentin
        // USA
        (9, 16), (9, 15),              // Pfizer -> Paxlovid, Remdesivir
        (10, 14),                       // J&J -> Humira
        (11, 11),                       // Abbott -> Acetaminophen
        (12, 13), (12, 16),            // Merck & Co -> Keytruda, Paxlovid
        (13, 12), (13, 18),            // Eli Lilly -> Ozempic, Jardiance
        // Switzerland
        (14, 14), (14, 17),            // Novartis -> Humira, Eliquis
        (15, 13), (15, 19),            // Roche -> Keytruda, Dupixent
        (16, 14), (16, 13),            // Lonza -> Humira (CDMO), Keytruda (CDMO)
        // China
        (17, 0), (17, 5), (17, 10),    // Zhejiang Hisun -> Metformin, Losartan, Ibuprofen
        (18, 15),                       // Fosun Pharma -> Remdesivir
        (19, 11), (19, 10), (19, 6),   // Shanghai Pharma -> Acetaminophen, Ibuprofen, Amlodipine
        // Japan
        (20, 12), (20, 4),             // Takeda -> Ozempic, Omeprazole
        (21, 7),                        // Astellas -> Gabapentin
        (22, 13),                       // Daiichi Sankyo -> Keytruda
        // UK
        (23, 3), (23, 9),              // GSK -> Amoxicillin, Montelukast
        (24, 17), (24, 19),            // AstraZeneca -> Eliquis, Dupixent
        // France
        (25, 12), (25, 19), (25, 14),  // Sanofi -> Ozempic, Dupixent, Humira
        (26, 1), (26, 5),              // Servier -> Atorvastatin, Losartan
        // Ireland
        (27, 7), (27, 8),              // Allergan -> Gabapentin, Sertraline
        (28, 8),                        // Jazz -> Sertraline
        // Korea
        (29, 14),                       // Samsung Biologics -> Humira (CDMO)
        // Israel
        (30, 0), (30, 8), (30, 2),     // Teva -> Metformin, Sertraline, Lisinopril
    ]
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn is_claude_available() -> bool {
    std::process::Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::main]
async fn main() {
    println!("================================================================");
    println!("   GLOBAL PHARMACEUTICAL SUPPLY CHAIN SIMULATION");
    println!("   Powered by Samyama Graph Database");
    println!("================================================================");
    println!();

    let client = EmbeddedClient::new();
    let tenant = "default";

    let overall_start = Instant::now();

    // ======================================================================
    // STEP 1: BUILD THE SUPPLY CHAIN GRAPH
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | STEP 1: Building Global Supply Chain Knowledge Graph    |");
    println!("  +---------------------------------------------------------+");
    println!();

    let start = Instant::now();

    // -- Ports --
    let ports = port_data();
    let suppliers = supplier_data();
    let products = product_data();
    let lines = shipping_line_data();
    let shipments = shipment_data();

    let mut port_ids = Vec::new();
    let mut supplier_ids = Vec::new();
    let mut product_ids = Vec::new();
    let mut line_ids = Vec::new();
    let mut shipment_ids = Vec::new();
    let mut edge_count = 0usize;

    {
        let mut store = client.store_write().await;

        for p in &ports {
            let id = store.create_node("Port");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", p.name);
                node.set_property("country", p.country);
                node.set_property("region", p.region);
                node.set_property("capacity_teu_k", p.capacity_teu);
                node.set_property("lat", p.lat);
                node.set_property("lon", p.lon);
                node.set_property("status", "Operational");
            }
            port_ids.push(id);
        }
        println!("  [+] Created {} Port nodes across {} countries",
            port_ids.len(),
            ports.iter().map(|p| p.country).collect::<std::collections::HashSet<_>>().len());

        // -- Suppliers with vector embeddings --
        store.create_vector_index("Supplier", "capabilities_vec", 64, DistanceMetric::Cosine).unwrap();

        for s in &suppliers {
            let emb = mock_embedding(s.capabilities, 64);
            let mut props = PropertyMap::new();
            props.insert("name".to_string(), PropertyValue::String(s.name.to_string()));
            props.insert("country".to_string(), PropertyValue::String(s.country.to_string()));
            props.insert("capabilities".to_string(), PropertyValue::String(s.capabilities.to_string()));
            props.insert("annual_revenue_m".to_string(), PropertyValue::Integer(s.annual_revenue_m));
            props.insert("gmp_certified".to_string(), PropertyValue::Boolean(s.gmp_certified));
            props.insert("capabilities_vec".to_string(), PropertyValue::Vector(emb));
            let id = store.create_node_with_properties(
                tenant,
                vec![Label::new("Supplier")],
                props,
            );
            supplier_ids.push(id);
        }
        println!("  [+] Created {} Supplier nodes with 64-dim capability embeddings across {} countries",
            supplier_ids.len(),
            suppliers.iter().map(|s| s.country).collect::<std::collections::HashSet<_>>().len());

        // -- Products --
        for p in &products {
            let id = store.create_node("Product");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", p.name);
                node.set_property("category", p.category);
                node.set_property("cold_chain", p.cold_chain);
                node.set_property("unit_value", p.unit_value);
                node.set_property("annual_demand_m", p.annual_demand_m);
                // Classify as biologic or small molecule based on cold chain and high unit value
                let product_type = match p.name {
                    "Ozempic" | "Keytruda" | "Humira" | "Remdesivir" | "Dupixent" => "biologic",
                    _ => "small molecule",
                };
                node.set_property("product_type", product_type);
            }
            product_ids.push(id);
        }
        println!("  [+] Created {} Product nodes ({} require cold chain)",
            product_ids.len(),
            products.iter().filter(|p| p.cold_chain).count());

        // -- Shipping Lines --
        for l in &lines {
            let id = store.create_node("ShippingLine");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("name", l.name);
                node.set_property("fleet_size", l.fleet_size);
                node.set_property("headquarter", l.headquarter);
            }
            line_ids.push(id);
        }
        println!("  [+] Created {} ShippingLine nodes", line_ids.len());

        // -- Shipments --
        for s in &shipments {
            let id = store.create_node("Shipment");
            if let Some(node) = store.get_node_mut(id) {
                node.set_property("shipment_id", s.id);
                node.set_property("containers", s.containers);
                node.set_property("value_usd", s.value_usd);
                node.set_property("status", s.status);
                // Store product name for easy lookup
                node.set_property("product", products[s.product_idx].name);
                node.set_property("cold_chain", products[s.product_idx].cold_chain);
            }
            shipment_ids.push(id);
        }
        println!("  [+] Created {} Shipment nodes", shipment_ids.len());

        // -- Relationships --

        // ROUTES_THROUGH: port -> port
        for (a, b) in route_connections() {
            store.create_edge(port_ids[a], port_ids[b], "ROUTES_THROUGH").unwrap();
            store.create_edge(port_ids[b], port_ids[a], "ROUTES_THROUGH").unwrap();
            edge_count += 2;
        }

        // SUPPLIES: supplier -> product
        for (si, pi) in supply_links() {
            store.create_edge(supplier_ids[si], product_ids[pi], "SUPPLIES").unwrap();
            edge_count += 1;
        }

        // MANUFACTURES: supplier -> product (same as SUPPLIES for pharma context)
        // We mark a subset as MANUFACTURES (primary manufacturer vs distributor)
        let mfg_links = [
            (0, 0), (1, 1), (3, 3), (9, 16), (12, 13), (13, 12),
            (14, 14), (17, 10), (19, 11), (20, 4), (23, 9), (25, 19),
        ];
        for (si, pi) in mfg_links {
            store.create_edge(supplier_ids[si], product_ids[pi], "MANUFACTURES").unwrap();
            edge_count += 1;
        }

        // BASED_AT: supplier -> nearest port
        for (i, s) in suppliers.iter().enumerate() {
            store.create_edge(supplier_ids[i], port_ids[s.nearest_port_idx], "BASED_AT").unwrap();
            edge_count += 1;
        }

        // SHIPS_VIA: shipment -> origin port, via ports, destination port
        // CARRIES: shipping line -> shipment
        // CONTAINS: shipment -> product
        for (i, s) in shipments.iter().enumerate() {
            // Origin
            store.create_edge(shipment_ids[i], port_ids[s.origin_port_idx], "SHIPS_FROM").unwrap();
            edge_count += 1;
            // Destination
            store.create_edge(shipment_ids[i], port_ids[s.dest_port_idx], "SHIPS_TO").unwrap();
            edge_count += 1;
            // Via ports
            for &via in &s.via_port_indices {
                store.create_edge(shipment_ids[i], port_ids[via], "SHIPS_VIA").unwrap();
                edge_count += 1;
            }
            // Carrier
            store.create_edge(line_ids[s.carrier_idx], shipment_ids[i], "CARRIES").unwrap();
            edge_count += 1;
            // Product
            store.create_edge(shipment_ids[i], product_ids[s.product_idx], "CONTAINS").unwrap();
            edge_count += 1;
        }
    } // drop store write lock

    let build_time = start.elapsed();
    let total_nodes = port_ids.len() + supplier_ids.len() + product_ids.len()
        + line_ids.len() + shipment_ids.len();

    println!("  [+] Created {} relationships", edge_count);
    println!();
    println!("  Graph built in {:.2?}", build_time);
    println!();

    // Summary table
    println!("  +---------------------+--------+");
    println!("  | Entity              |  Count |");
    println!("  +---------------------+--------+");
    println!("  | Ports               | {:>6} |", port_ids.len());
    println!("  | Suppliers           | {:>6} |", supplier_ids.len());
    println!("  | Products            | {:>6} |", product_ids.len());
    println!("  | Shipping Lines      | {:>6} |", line_ids.len());
    println!("  | Shipments           | {:>6} |", shipment_ids.len());
    println!("  +---------------------+--------+");
    println!("  | Total Nodes         | {:>6} |", total_nodes);
    println!("  | Total Edges         | {:>6} |", edge_count);
    println!("  +---------------------+--------+");
    println!();

    // Cypher verification
    let r1 = client.query_readonly(tenant, "MATCH (p:Port) RETURN p").await.unwrap();
    let r2 = client.query_readonly(tenant, "MATCH (s:Supplier) RETURN s").await.unwrap();
    let r3 = client.query_readonly(tenant, "MATCH (sh:Shipment) RETURN sh").await.unwrap();
    println!("  Cypher Verification:");
    println!("    MATCH (p:Port) RETURN p       -> {} results", r1.records.len());
    println!("    MATCH (s:Supplier) RETURN s   -> {} results", r2.records.len());
    println!("    MATCH (sh:Shipment) RETURN sh -> {} results", r3.records.len());
    println!();

    // ======================================================================
    // SCENARIO A: Supply Chain Topology Summary
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO A: Supply Chain Topology Summary               |");
    println!("  +---------------------------------------------------------+");
    println!();

    // Region distribution
    let mut region_counts: HashMap<&str, usize> = HashMap::new();
    for p in &ports {
        *region_counts.entry(p.region).or_default() += 1;
    }
    let mut region_sorted: Vec<_> = region_counts.into_iter().collect();
    region_sorted.sort_by(|a, b| b.1.cmp(&a.1));

    println!("  Port Distribution by Region:");
    println!("  +----------------------+--------+");
    println!("  | Region               |  Ports |");
    println!("  +----------------------+--------+");
    for (region, count) in &region_sorted {
        println!("  | {:<20} | {:>6} |", region, count);
    }
    println!("  +----------------------+--------+");
    println!();

    // Supplier country distribution
    let mut country_counts: HashMap<&str, usize> = HashMap::new();
    for s in &suppliers {
        *country_counts.entry(s.country).or_default() += 1;
    }
    let mut country_sorted: Vec<_> = country_counts.into_iter().collect();
    country_sorted.sort_by(|a, b| b.1.cmp(&a.1));

    println!("  Supplier Distribution by Country:");
    println!("  +----------------------+--------+-----------------+");
    println!("  | Country              | Count  | Combined Rev $M |");
    println!("  +----------------------+--------+-----------------+");
    for (country, count) in &country_sorted {
        let total_rev: i64 = suppliers.iter()
            .filter(|s| s.country == *country)
            .map(|s| s.annual_revenue_m)
            .sum();
        println!("  | {:<20} | {:>6} | {:>15} |", country, count, total_rev);
    }
    println!("  +----------------------+--------+-----------------+");
    println!();

    // Product categories
    let mut cat_counts: HashMap<&str, usize> = HashMap::new();
    for p in &products {
        *cat_counts.entry(p.category).or_default() += 1;
    }
    let mut cat_sorted: Vec<_> = cat_counts.into_iter().collect();
    cat_sorted.sort_by(|a, b| b.1.cmp(&a.1));

    println!("  Product Categories:");
    println!("  +--------------------+--------+");
    println!("  | Category           |  Count |");
    println!("  +--------------------+--------+");
    for (cat, count) in &cat_sorted {
        println!("  | {:<18} | {:>6} |", cat, count);
    }
    println!("  +--------------------+--------+");
    println!();

    // Shipment value summary
    let total_value: f64 = shipments.iter().map(|s| s.value_usd).sum();
    let total_containers: i64 = shipments.iter().map(|s| s.containers).sum();
    let avg_value = total_value / shipments.len() as f64;

    println!("  Active Shipment Summary:");
    println!("    Total shipments:      {}", shipments.len());
    println!("    Total containers:     {}", total_containers);
    println!("    Total cargo value:    ${:.2}M", total_value / 1_000_000.0);
    println!("    Average per shipment: ${:.2}M", avg_value / 1_000_000.0);
    println!();

    // ======================================================================
    // SCENARIO B: Suez Canal Blockage
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO B: Disruption -- Suez Canal Blockage           |");
    println!("  +---------------------------------------------------------+");
    println!();
    println!("  ALERT: Suez Canal is blocked. All routes transiting Dubai");
    println!("  (Jebel Ali) <-> Piraeus are disrupted.");
    println!();

    // Identify affected routes: any route going through Dubai (idx 9) AND Piraeus (idx 16)
    // A shipment is affected if its via_port_indices contain BOTH 9 and 16,
    // or if any of its via ports includes 9 or 16 (Suez corridor)
    let suez_ports: [usize; 2] = [9, 16]; // Dubai and Piraeus
    let mut affected_shipments_b: Vec<(usize, &ShipmentDef)> = Vec::new();
    for (i, s) in shipments.iter().enumerate() {
        let uses_suez = s.via_port_indices.iter().any(|p| suez_ports.contains(p))
            || (suez_ports.contains(&s.origin_port_idx) && suez_ports.contains(&s.dest_port_idx));
        if uses_suez {
            affected_shipments_b.push((i, s));
        }
    }

    let affected_value_b: f64 = affected_shipments_b.iter().map(|(_, s)| s.value_usd).sum();
    let affected_containers_b: i64 = affected_shipments_b.iter().map(|(_, s)| s.containers).sum();

    println!("  Impact Assessment:");
    println!("    Shipments affected:   {}/{}", affected_shipments_b.len(), shipments.len());
    println!("    Containers at risk:   {}", affected_containers_b);
    println!("    Cargo value at risk:  ${:.2}M", affected_value_b / 1_000_000.0);
    println!();

    println!("  +--------+----------------------+--------+--------------+------------------+");
    println!("  | Ship   | Product              | Ctrs   | Value $      | Route            |");
    println!("  +--------+----------------------+--------+--------------+------------------+");
    for (_, s) in affected_shipments_b.iter().take(15) {
        let pname = products[s.product_idx].name;
        let origin = ports[s.origin_port_idx].name;
        let dest = ports[s.dest_port_idx].name;
        let route_str = format!("{}->{}", &origin[..6.min(origin.len())], &dest[..6.min(dest.len())]);
        println!("  | {:<6} | {:<20} | {:>6} | {:>12.0} | {:<16} |",
            s.id, pname, s.containers, s.value_usd, route_str);
    }
    if affected_shipments_b.len() > 15 {
        println!("  | ...    | ({} more shipments)   |        |              |                  |",
            affected_shipments_b.len() - 15);
    }
    println!("  +--------+----------------------+--------+--------------+------------------+");
    println!();

    // Cold chain risk
    let cold_chain_affected: Vec<_> = affected_shipments_b.iter()
        .filter(|(_, s)| products[s.product_idx].cold_chain)
        .collect();
    if !cold_chain_affected.is_empty() {
        println!("  COLD CHAIN ALERT: {} temperature-sensitive shipments affected!",
            cold_chain_affected.len());
        for (_, s) in &cold_chain_affected {
            println!("    - {} ({}) -- Value: ${:.2}M -- Extended transit may compromise integrity",
                s.id, products[s.product_idx].name, s.value_usd / 1_000_000.0);
        }
        println!();
    }

    // ======================================================================
    // SCENARIO C: Hamburg Port Strike
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO C: Disruption -- Hamburg Port Strike            |");
    println!("  +---------------------------------------------------------+");
    println!();

    // Mark Hamburg as disrupted
    let hamburg_idx = 3;
    {
        let mut store = client.store_write().await;
        store.set_node_property(
            tenant,
            port_ids[hamburg_idx],
            "status",
            PropertyValue::String("Strike - Closed".to_string()),
        ).unwrap();
    }

    println!("  Hamburg port status changed to: Strike - Closed");
    println!();

    // Find all shipments destined for Hamburg
    let mut hamburg_shipments: Vec<(usize, &ShipmentDef)> = Vec::new();
    for (i, s) in shipments.iter().enumerate() {
        if s.dest_port_idx == hamburg_idx {
            hamburg_shipments.push((i, s));
        }
    }

    let hamburg_value: f64 = hamburg_shipments.iter().map(|(_, s)| s.value_usd).sum();

    println!("  Shipments destined for Hamburg: {}", hamburg_shipments.len());
    println!("  Total value at risk: ${:.2}M", hamburg_value / 1_000_000.0);
    println!();

    println!("  +--------+----------------------+--------+--------------+-----------+");
    println!("  | Ship   | Product              | Ctrs   | Value $      | Carrier   |");
    println!("  +--------+----------------------+--------+--------------+-----------+");
    for (_, s) in &hamburg_shipments {
        let pname = products[s.product_idx].name;
        let carrier = lines[s.carrier_idx].name;
        println!("  | {:<6} | {:<20} | {:>6} | {:>12.0} | {:<9} |",
            s.id, pname, s.containers, s.value_usd, carrier);
    }
    println!("  +--------+----------------------+--------+--------------+-----------+");
    println!();

    // Rerouting options: find alternative European ports
    let alt_ports = [2, 10, 12]; // Rotterdam, Antwerp, Felixstowe
    println!("  Rerouting Options:");
    println!("  +--------------------+-----------+---------------+");
    println!("  | Alternative Port   | Country   | Capacity (kTEU)|");
    println!("  +--------------------+-----------+---------------+");
    for &pi in &alt_ports {
        println!("  | {:<18} | {:<9} | {:>13} |",
            ports[pi].name, ports[pi].country, ports[pi].capacity_teu);
    }
    println!("  +--------------------+-----------+---------------+");
    println!();

    // Check graph connectivity for routes to alternatives
    println!("  Route connectivity from Hamburg alternatives:");
    {
        let store = client.store_read().await;
        for &pi in &alt_ports {
            let edges = store.get_outgoing_edges(port_ids[pi]);
            let connections: Vec<String> = edges.iter()
                .filter(|e| e.edge_type.as_str() == "ROUTES_THROUGH")
                .filter_map(|e| {
                    store.get_node(e.target)
                        .and_then(|n| n.get_property("name"))
                        .and_then(|v| v.as_string().map(|s| s.to_string()))
                })
                .collect();
            println!("    {} -> {} connected ports: {}",
                ports[pi].name, connections.len(),
                connections.iter().take(5).cloned().collect::<Vec<_>>().join(", "));
        }
    }
    println!();

    // ======================================================================
    // SCENARIO D: Cold Chain Monitoring
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO D: Cold-Chain Monitoring                       |");
    println!("  +---------------------------------------------------------+");
    println!();
    println!("  Temperature-sensitive biologics and vaccines in transit:");
    println!();

    let mut cold_chain_shipments: Vec<&ShipmentDef> = Vec::new();
    for s in &shipments {
        if products[s.product_idx].cold_chain {
            cold_chain_shipments.push(s);
        }
    }

    let cold_value: f64 = cold_chain_shipments.iter().map(|s| s.value_usd).sum();

    println!("  +--------+--------------------+--------+--------------+---------------------+");
    println!("  | Ship   | Product            | Ctrs   | Value $      | Current Route       |");
    println!("  +--------+--------------------+--------+--------------+---------------------+");
    for s in &cold_chain_shipments {
        let pname = products[s.product_idx].name;
        let origin = ports[s.origin_port_idx].name;
        let dest = ports[s.dest_port_idx].name;
        let route = if s.via_port_indices.is_empty() {
            format!("{} -> {}", origin, dest)
        } else {
            let via_names: Vec<&str> = s.via_port_indices.iter()
                .map(|&idx| ports[idx].name)
                .collect();
            format!("{} -> {} -> {}", origin, via_names.join(" -> "), dest)
        };
        let route_display = if route.len() > 19 {
            format!("{}...", &route[..16])
        } else {
            route
        };
        println!("  | {:<6} | {:<18} | {:>6} | {:>12.0} | {:<19} |",
            s.id, pname, s.containers, s.value_usd, route_display);
    }
    println!("  +--------+--------------------+--------+--------------+---------------------+");
    println!();
    println!("  Cold-chain summary:");
    println!("    Total cold-chain shipments:  {}", cold_chain_shipments.len());
    println!("    Total cold-chain value:      ${:.2}M", cold_value / 1_000_000.0);
    println!("    Products requiring cold chain:");
    for p in &products {
        if p.cold_chain {
            println!("      - {} ({}, ${:.2}/unit)", p.name, p.category, p.unit_value);
        }
    }
    println!();

    // ======================================================================
    // SCENARIO E: Criticality Analysis (PageRank)
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO E: Criticality Analysis (PageRank)             |");
    println!("  +---------------------------------------------------------+");
    println!();

    let start = Instant::now();

    // PageRank on the full graph
    let view = client.build_view(None, None, None).await;
    let pr_scores = client.page_rank(PageRankConfig {
        damping_factor: 0.85,
        iterations: 30,
        tolerance: 0.0001,
    }, None, None).await;
    let pr_time = start.elapsed();

    println!("  PageRank computed in {:.2?} ({} nodes, {} edges in view)",
        pr_time, view.node_count, view.out_targets.len());
    println!();

    // Port criticality
    let mut port_ranks: Vec<(&str, &str, f64)> = Vec::new();
    for (i, &pid) in port_ids.iter().enumerate() {
        let score = pr_scores.get(&pid.as_u64()).copied().unwrap_or(0.0);
        port_ranks.push((ports[i].name, ports[i].country, score));
    }
    port_ranks.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());

    println!("  Top 10 Most Critical Ports:");
    println!("  +------+--------------------+--------------+----------+");
    println!("  | Rank | Port               | Country      | PageRank |");
    println!("  +------+--------------------+--------------+----------+");
    for (rank, (name, country, score)) in port_ranks.iter().take(10).enumerate() {
        println!("  | {:>4} | {:<18} | {:<12} | {:>8.6} |",
            rank + 1, name, country, score);
    }
    println!("  +------+--------------------+--------------+----------+");
    println!();

    // Supplier criticality
    let mut supplier_ranks: Vec<(&str, &str, f64)> = Vec::new();
    for (i, &sid) in supplier_ids.iter().enumerate() {
        let score = pr_scores.get(&sid.as_u64()).copied().unwrap_or(0.0);
        supplier_ranks.push((suppliers[i].name, suppliers[i].country, score));
    }
    supplier_ranks.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());

    println!("  Top 10 Most Critical Suppliers:");
    println!("  +------+------------------------+--------------+----------+");
    println!("  | Rank | Supplier               | Country      | PageRank |");
    println!("  +------+------------------------+--------------+----------+");
    for (rank, (name, country, score)) in supplier_ranks.iter().take(10).enumerate() {
        let name_disp = if name.len() > 22 {
            format!("{}...", &name[..19])
        } else {
            name.to_string()
        };
        println!("  | {:>4} | {:<22} | {:<12} | {:>8.6} |",
            rank + 1, name_disp, country, score);
    }
    println!("  +------+------------------------+--------------+----------+");
    println!();

    // WCC analysis
    let wcc = client.weakly_connected_components(None, None).await;
    println!("  Graph Connectivity (WCC):");
    println!("    Connected components: {}", wcc.components.len());
    let max_comp = wcc.components.values().map(|v| v.len()).max().unwrap_or(0);
    println!("    Largest component:    {} nodes ({:.1}% of graph)",
        max_comp, max_comp as f64 / total_nodes as f64 * 100.0);
    println!();

    // ======================================================================
    // SCENARIO F: Route Optimization (Jaya Algorithm)
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO F: Route Optimization (Jaya Algorithm)         |");
    println!("  +---------------------------------------------------------+");
    println!();
    println!("  Problem: Allocate {} containers from Hamburg-bound shipments",
        hamburg_shipments.iter().map(|(_, s)| s.containers).sum::<i64>());
    println!("  across 3 alternative European ports, minimizing total cost.");
    println!();
    println!("  Cost model: base_rate * distance_factor + congestion_penalty");
    println!("  Ports: Rotterdam (idx 0), Antwerp (idx 1), Felixstowe (idx 2)");
    println!();

    let total_hamburg_containers: f64 = hamburg_shipments.iter()
        .map(|(_, s)| s.containers as f64)
        .sum();

    // Cost coefficients per container: [base_cost, congestion_quadratic_factor]
    // Rotterdam: medium base, medium congestion (largest port)
    // Antwerp: slightly higher base, lower congestion
    // Felixstowe: higher base (smaller port), lowest congestion
    struct RerouteProblem {
        target_containers: f64,
        base_costs: [f64; 3],
        congestion_factors: [f64; 3],
        port_capacities: [f64; 3],
    }

    impl Problem for RerouteProblem {
        fn dim(&self) -> usize { 3 }

        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            let lower = Array1::from_elem(3, 0.0);
            let upper = Array1::from_vec(self.port_capacities.to_vec());
            (lower, upper)
        }

        fn objective(&self, x: &Array1<f64>) -> f64 {
            let mut cost = 0.0;
            for i in 0..3 {
                // Linear cost + quadratic congestion
                cost += self.base_costs[i] * x[i] + self.congestion_factors[i] * x[i] * x[i];
            }
            cost
        }

        fn penalty(&self, x: &Array1<f64>) -> f64 {
            let total: f64 = x.iter().sum();
            let deficit = (self.target_containers - total).abs();
            if deficit > 0.5 {
                deficit * deficit * 500.0
            } else {
                0.0
            }
        }
    }

    let reroute_problem = RerouteProblem {
        target_containers: total_hamburg_containers,
        base_costs: [45.0, 52.0, 65.0],              // $/container base
        congestion_factors: [0.15, 0.10, 0.05],       // quadratic congestion
        port_capacities: [80.0, 60.0, 40.0],          // max containers
    };

    let start = Instant::now();
    let jaya_solver = JayaSolver::new(SolverConfig {
        population_size: 30,
        max_iterations: 100,
    });
    let opt_result = jaya_solver.solve(&reroute_problem);
    let opt_time = start.elapsed();

    let alt_port_names = ["Rotterdam", "Antwerp", "Felixstowe"];
    let allocated: Vec<f64> = (0..3).map(|i| opt_result.best_variables[i]).collect();
    let total_allocated: f64 = allocated.iter().sum();

    println!("  Optimization completed in {:.2?}", opt_time);
    println!("  Minimized total cost: ${:.2}", opt_result.best_fitness);
    println!();
    println!("  +--------------------+-------------+-------------+");
    println!("  | Port               | Containers  | Cost $      |");
    println!("  +--------------------+-------------+-------------+");
    for i in 0..3 {
        let cost_i = reroute_problem.base_costs[i] * allocated[i]
            + reroute_problem.congestion_factors[i] * allocated[i] * allocated[i];
        println!("  | {:<18} | {:>11.0} | {:>11.2} |",
            alt_port_names[i], allocated[i], cost_i);
    }
    println!("  +--------------------+-------------+-------------+");
    println!("  | TOTAL              | {:>11.0} | {:>11.2} |",
        total_allocated, opt_result.best_fitness);
    println!("  +--------------------+-------------+-------------+");
    println!();

    if (total_allocated - total_hamburg_containers).abs() < 2.0 {
        println!("  All {} containers successfully rerouted.", total_hamburg_containers as i64);
    } else {
        println!("  Note: Allocation ({:.0}) vs target ({:.0}) -- constraint tolerance.",
            total_allocated, total_hamburg_containers);
    }
    println!();

    // ======================================================================
    // SCENARIO G: Alternative Supplier Search (Vector Search)
    // ======================================================================
    println!("  +---------------------------------------------------------+");
    println!("  | SCENARIO G: Alternative Supplier Search (Vector Search) |");
    println!("  +---------------------------------------------------------+");
    println!();

    // Scenario: A primary supplier is disrupted. Find alternatives with similar capabilities.
    let disrupted_suppliers = [
        (0, "Aurobindo Pharma"),    // India generic giant
        (9, "Pfizer"),              // mRNA/oncology
        (14, "Novartis"),           // Gene therapy/CAR-T
    ];

    for (si, disrupted_name) in &disrupted_suppliers {
        let query_capabilities = suppliers[*si].capabilities;
        let query_vec = mock_embedding(query_capabilities, 64);

        println!("  Disrupted supplier: {} ({})", disrupted_name, suppliers[*si].country);
        println!("  Capabilities: \"{}...\"", &query_capabilities[..60.min(query_capabilities.len())]);
        println!();

        let results = client.vector_search("Supplier", "capabilities_vec", &query_vec, 6).await.unwrap();

        println!("  Top alternative suppliers by capability similarity:");
        println!("  +------+------------------------+--------------+----------+");
        println!("  | Rank | Supplier               | Country      | Score    |");
        println!("  +------+------------------------+--------------+----------+");
        let mut rank = 0;
        {
            let store = client.store_read().await;
            for (nid, score) in &results {
                if let Some(node) = store.get_node(*nid) {
                    let name = node.get_property("name")
                        .and_then(|v| v.as_string().map(|s| s.to_string()))
                        .unwrap_or_default();
                    // Skip the disrupted supplier itself
                    if name == *disrupted_name {
                        continue;
                    }
                    rank += 1;
                    let country = node.get_property("country")
                        .and_then(|v| v.as_string().map(|s| s.to_string()))
                        .unwrap_or_default();
                    let name_disp = if name.len() > 22 {
                        format!("{}...", &name[..19])
                    } else {
                        name
                    };
                    println!("  | {:>4} | {:<22} | {:<12} | {:>8.4} |",
                        rank, name_disp, country, score);
                    if rank >= 5 {
                        break;
                    }
                }
            }
        }
        println!("  +------+------------------------+--------------+----------+");
        println!();
    }

    // Also do a manual cosine similarity comparison for validation
    println!("  Manual cosine similarity validation (Aurobindo vs top matches):");
    let aurobindo_emb = mock_embedding(suppliers[0].capabilities, 64);
    let mut all_sims: Vec<(&str, &str, f32)> = suppliers.iter()
        .filter(|s| s.name != "Aurobindo Pharma")
        .map(|s| {
            let emb = mock_embedding(s.capabilities, 64);
            (s.name, s.country, cosine_similarity(&aurobindo_emb, &emb))
        })
        .collect();
    all_sims.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());

    for (name, country, sim) in all_sims.iter().take(5) {
        println!("    {} ({}) -- cosine similarity: {:.4}", name, country, sim);
    }
    println!();

    // ======================================================================
    // NLQ Supply Chain Intelligence (ClaudeCode)
    // ======================================================================
    println!();
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ NLQ Supply Chain Intelligence (ClaudeCode)                   │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();

    if is_claude_available() {
        println!("  [ok] Claude Code CLI detected — running NLQ queries");
        println!();

        let nlq_config = NLQConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: String::new(),
            api_key: None,
            api_base_url: None,
            system_prompt: Some("You are a Cypher query expert for a pharmaceutical supply chain knowledge graph.".to_string()),
        };

        let tenant_mgr = client.tenant_manager();
        tenant_mgr.create_tenant("supply_nlq".to_string(), "Supply Chain NLQ".to_string(), None).unwrap();
        tenant_mgr.update_nlq_config("supply_nlq", Some(nlq_config.clone())).unwrap();

        let schema_summary = "Node labels: Port, Supplier, Product, ShippingLine, Shipment\n\
                              Edge types: SHIPS_FROM, SHIPS_TO, SUPPLIES, CONTAINS (Shipment->Product), CARRIES (ShippingLine->Shipment), ROUTES_THROUGH, BASED_AT\n\
                              Relationship paths: (Supplier)-[:SUPPLIES]->(Product), (Supplier)-[:BASED_AT]->(Port), (Shipment)-[:SHIPS_FROM]->(Port), (Shipment)-[:SHIPS_TO]->(Port), (Shipment)-[:CONTAINS]->(Product), (ShippingLine)-[:CARRIES]->(Shipment)\n\
                              Properties: Port(name, country, region['East Asia'/'Europe'/'North America'/'Southeast Asia'/'South Asia'/'Middle East'/'South America'], capacity_teu), \
                              Supplier(name, country[e.g. 'India', 'Germany', 'USA', 'China', 'Japan', 'Switzerland'], capabilities, annual_revenue_m, gmp_certified), \
                              Product(name, category['Diabetes'/'Cardiovascular'/'Oncology'/'Immunology'/'Antibiotic'/'Antiviral'/'GI'/'Pain'/'Neurology'/'Psychiatry'/'Respiratory'], product_type['small molecule'/'biologic'], unit_value, cold_chain), \
                              Shipment(shipment_id, status['In Transit'/'Loading'], value_usd, containers), \
                              ShippingLine(name, fleet_size, headquarter)\n\
                              Notes: Multi-type edge patterns like [:TYPE1|TYPE2] are not supported; use separate MATCH clauses. \
                              Use CONTAINS (not CARRIES) to go from Shipment to Product.";

        let nlq_pipeline = client.nlq_pipeline(nlq_config.clone()).unwrap();

        let nlq_questions = vec![
            "Which suppliers provide active pharmaceutical ingredients from India?",
            "Show all in-transit shipments of biologic products",
            "Find ports connected to both European and Asian shipping routes",
        ];

        for (i, question) in nlq_questions.iter().enumerate() {
            println!("  NLQ Query {}: \"{}\"", i + 1, question);
            match nlq_pipeline.text_to_cypher(question, schema_summary).await {
                Ok(cypher) => {
                    println!("  Generated Cypher: {}", cypher);
                    match client.query_readonly(tenant, &cypher).await {
                        Ok(result) => println!("  Results: {} records", result.records.len()),
                        Err(e) => println!("  Execution error: {}", e),
                    }
                }
                Err(e) => println!("  NLQ translation error: {}", e),
            }
            println!();
        }

        // Agent enrichment: add a new supplier
        println!("  --- Agentic Enrichment: Adding New Supplier ---");

        let mut policies = HashMap::new();
        policies.insert(
            "Supplier".to_string(),
            "When a Supplier entity is missing, enrich with capabilities and port connections.".to_string(),
        );

        let agent_config = AgentConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: String::new(),
            api_key: None,
            api_base_url: None,
            system_prompt: Some("You are a pharmaceutical supply chain knowledge graph builder.".to_string()),
            tools: vec![],
            policies,
        };

        let runtime = client.agent_runtime(agent_config);
        let enrichment_prompt = "Generate Cypher CREATE statements to add a new supplier to a pharmaceutical supply chain graph.\n\n\
                                 Create:\n\
                                 1. One Supplier node: name: 'Lonza Group', country: 'Switzerland', tier: 'Tier 1', capabilities: 'biologics manufacturing, cell therapy, API synthesis'\n\
                                 2. One Product node: name: 'mRNA Lipid Nanoparticles', category: 'Biologic', value: 85000000\n\
                                 3. Edges: (Supplier)-[:MANUFACTURES]->(Product)\n\n\
                                 RULES: Output ONLY Cypher statements, one per line. No markdown. Use single quotes for strings.\n\
                                 First CREATE nodes, then MATCH...CREATE edges.\n\
                                 MATCH format: MATCH (a:Label {prop: 'val'}), (b:Label {prop: 'val'}) CREATE (a)-[:REL]->(b)";

        match runtime.process_trigger(enrichment_prompt, "supply_nlq").await {
            Ok(response) => {
                println!("  Claude generated enrichment:");
                let stmts: Vec<String> = response.lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| {
                        let u = l.to_uppercase();
                        u.starts_with("CREATE") || u.starts_with("MATCH")
                    })
                    .collect();
                for stmt in &stmts {
                    let display = if stmt.len() > 78 { &stmt[..78] } else { stmt.as_str() };
                    println!("    | {}", display);
                }
                println!("  Parsed {} Cypher statements", stmts.len());
            }
            Err(e) => println!("  Agent enrichment error: {}", e),
        }
    } else {
        println!("  [skip] Claude Code CLI not found — skipping NLQ queries");
        println!("  Install: https://docs.anthropic.com/en/docs/claude-code");
    }
    println!();

    // ======================================================================
    // FINAL SUMMARY
    // ======================================================================
    let total_time = overall_start.elapsed();

    println!("================================================================");
    println!("   SUPPLY CHAIN SIMULATION COMPLETE");
    println!("================================================================");
    println!();
    println!("  Total execution time: {:.2?}", total_time);
    println!();
    println!("  Graph Schema:");
    println!();
    println!("    (Supplier)--[:SUPPLIES/MANUFACTURES]-->(Product)");
    println!("        |                                     ^");
    println!("    [:BASED_AT]                          [:CONTAINS]");
    println!("        |                                     |");
    println!("        v                                (Shipment)");
    println!("     (Port)<--[:ROUTES_THROUGH]-->(Port)    / | \\");
    println!("        ^                               [:SHIPS_FROM/TO/VIA]");
    println!("        |                                     |");
    println!("    (ShippingLine)--[:CARRIES]-->(Shipment)   v");
    println!("                                           (Port)");
    println!();
    println!("  Capabilities Demonstrated:");
    println!("    [A] Knowledge graph construction ({} nodes, {} edges)", total_nodes, edge_count);
    println!("    [B] Suez Canal disruption analysis ({} shipments, ${:.1}M at risk)",
        affected_shipments_b.len(), affected_value_b / 1_000_000.0);
    println!("    [C] Hamburg port strike with rerouting ({} shipments, ${:.1}M value)",
        hamburg_shipments.len(), hamburg_value / 1_000_000.0);
    println!("    [D] Cold-chain monitoring ({} temperature-sensitive shipments)",
        cold_chain_shipments.len());
    println!("    [E] PageRank criticality ({} nodes analyzed)", view.node_count);
    println!("    [F] Jaya optimization ({:.0} containers across 3 ports)", total_hamburg_containers);
    println!("    [G] Vector search for alternative suppliers (64-dim embeddings)");
    println!("    [H] NLQ supply chain intelligence via ClaudeCode pipeline");
    println!("    [I] Agentic enrichment for supplier knowledge generation");
    println!();
    println!("  Samyama Graph Database -- Global Pharmaceutical Supply Chain");
    println!("================================================================");
}
