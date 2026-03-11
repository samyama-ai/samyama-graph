//! Industrial Knowledge Graph: Asset Operations Intelligence
//!
//! Inspired by IBM AssetOpsBench, ISO 14224, and ISA-95 standards for
//! industrial asset management and maintenance optimization.
//!
//! Scenarios:
//! 1. Build asset hierarchy (Site -> Locations -> Equipment -> Sensors)
//! 2. Failure modes with vector search (semantic similarity)
//! 3. Dependency graph + cascade analysis (BFS)
//! 4. PageRank equipment criticality
//! 5. NSGA-II maintenance scheduling optimization
//! 6. NLQ integration (ClaudeCode)

use samyama_sdk::{
    EmbeddedClient, SamyamaClient, AlgorithmClient, VectorClient,
    PropertyValue, PageRankConfig, DistanceMetric,
    NLQConfig, LLMProvider,
    NSGA2Solver, SolverConfig, MultiObjectiveProblem,
    Array1,
};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Helper: deterministic mock embedding (128-dim, normalized)
// ---------------------------------------------------------------------------
fn mock_embedding(seed: usize) -> Vec<f32> {
    (0usize..128)
        .map(|j| {
            let hash = (seed.wrapping_mul(2654435761) ^ j.wrapping_mul(40503)) % 10000;
            (hash as f32 / 10000.0).max(0.001)
        })
        .collect()
}

/// Generate a query embedding similar to mock_embedding(seed) with small perturbation.
fn query_embedding(seed: usize) -> Vec<f32> {
    (0usize..128)
        .map(|j| {
            let hash = (seed.wrapping_mul(2654435761) ^ j.wrapping_mul(40503)) % 10000;
            let base = (hash as f32 / 10000.0).max(0.001);
            base + 0.02 * ((j * 7 + seed) % 5) as f32 / 5.0
        })
        .collect()
}

fn is_claude_available() -> bool {
    std::process::Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Data definitions
// ---------------------------------------------------------------------------

struct LocationDef {
    name: &'static str,
    code: &'static str,
}

struct EquipmentDef {
    name: &'static str,
    iso14224_class: &'static str,
    isa95_level: &'static str,
    criticality_score: f64,
    mtbf_hours: f64,
    install_date: &'static str,
    manufacturer: &'static str,
    location_idx: usize,
}

struct SensorDef {
    name: &'static str,
    sensor_type: &'static str,
    unit: &'static str,
    min_threshold: f64,
    max_threshold: f64,
}

struct FailureModeDef {
    name: &'static str,
    description: &'static str,
    severity: &'static str,
    equipment_indices: Vec<usize>,
}

#[tokio::main]
async fn main() {
    println!("=========================================================================");
    println!("   INDUSTRIAL KNOWLEDGE GRAPH -- Asset Operations Intelligence           ");
    println!("   Samyama Graph Database + ISO 14224 / ISA-95 Ontology                  ");
    println!("=========================================================================");

    let client = EmbeddedClient::new();
    let tenant = "industrial_plant";

    // ==================================================================================
    // STEP 1: Build Asset Hierarchy
    // ==================================================================================
    println!("\n[Step 1] Building Asset Hierarchy (ISO 14224 + ISA-95)");
    println!("---------------------------------------------------------------------------");
    let start = Instant::now();

    // --- Site ---
    let site_id;
    {
        let mut store = client.store_write().await;
        site_id = store.create_node("Site");
        store.set_node_property(tenant, site_id, "name",
            PropertyValue::String("Riverside Chemical Plant".to_string())).unwrap();
        store.set_node_property(tenant, site_id, "site_code",
            PropertyValue::String("RCP-001".to_string())).unwrap();
        store.set_node_property(tenant, site_id, "isa95_level",
            PropertyValue::String("Enterprise".to_string())).unwrap();
    }

    // --- Locations ---
    let location_defs = vec![
        LocationDef { name: "Building A - Chiller Plant",  code: "LOC-A" },
        LocationDef { name: "Building B - HVAC Center",    code: "LOC-B" },
        LocationDef { name: "Building C - Pump Station",   code: "LOC-C" },
        LocationDef { name: "Building D - Power House",    code: "LOC-D" },
    ];

    let mut location_ids = Vec::new();
    {
        let mut store = client.store_write().await;
        for loc in &location_defs {
            let id = store.create_node("Location");
            store.set_node_property(tenant, id, "name",
                PropertyValue::String(loc.name.to_string())).unwrap();
            store.set_node_property(tenant, id, "location_code",
                PropertyValue::String(loc.code.to_string())).unwrap();
            store.set_node_property(tenant, id, "isa95_level",
                PropertyValue::String("Area".to_string())).unwrap();
            store.create_edge(site_id, id, "CONTAINS_LOCATION").unwrap();
            location_ids.push(id);
        }
    }

    // --- Equipment (20 total: 4 types x 4 locations + 4 boilers in Power House) ---
    let equipment_defs: Vec<EquipmentDef> = vec![
        // Building A - Chiller Plant (index 0)
        EquipmentDef { name: "Chiller-1", iso14224_class: "Compressor/Chiller", isa95_level: "Unit", criticality_score: 9.2, mtbf_hours: 8760.0, install_date: "2019-03-15", manufacturer: "Trane Technologies", location_idx: 0 },
        EquipmentDef { name: "Chiller-2", iso14224_class: "Compressor/Chiller", isa95_level: "Unit", criticality_score: 9.0, mtbf_hours: 8500.0, install_date: "2019-03-15", manufacturer: "Trane Technologies", location_idx: 0 },
        EquipmentDef { name: "Chiller-3", iso14224_class: "Compressor/Chiller", isa95_level: "Unit", criticality_score: 7.5, mtbf_hours: 9200.0, install_date: "2021-06-01", manufacturer: "Carrier Global", location_idx: 0 },
        EquipmentDef { name: "Chiller-4", iso14224_class: "Compressor/Chiller", isa95_level: "Unit", criticality_score: 7.3, mtbf_hours: 9500.0, install_date: "2021-06-01", manufacturer: "Carrier Global", location_idx: 0 },
        // Building B - HVAC Center (index 1)
        EquipmentDef { name: "AHU-1", iso14224_class: "Air Handling Unit", isa95_level: "Unit", criticality_score: 8.1, mtbf_hours: 12000.0, install_date: "2020-01-10", manufacturer: "Daikin Industries", location_idx: 1 },
        EquipmentDef { name: "AHU-2", iso14224_class: "Air Handling Unit", isa95_level: "Unit", criticality_score: 8.0, mtbf_hours: 11500.0, install_date: "2020-01-10", manufacturer: "Daikin Industries", location_idx: 1 },
        EquipmentDef { name: "AHU-3", iso14224_class: "Air Handling Unit", isa95_level: "Unit", criticality_score: 6.8, mtbf_hours: 13000.0, install_date: "2022-04-20", manufacturer: "Johnson Controls", location_idx: 1 },
        EquipmentDef { name: "AHU-4", iso14224_class: "Air Handling Unit", isa95_level: "Unit", criticality_score: 6.5, mtbf_hours: 13500.0, install_date: "2022-04-20", manufacturer: "Johnson Controls", location_idx: 1 },
        // Building C - Pump Station (index 2)
        EquipmentDef { name: "Pump-1", iso14224_class: "Centrifugal Pump", isa95_level: "Unit", criticality_score: 8.8, mtbf_hours: 6000.0, install_date: "2018-09-01", manufacturer: "Grundfos", location_idx: 2 },
        EquipmentDef { name: "Pump-2", iso14224_class: "Centrifugal Pump", isa95_level: "Unit", criticality_score: 8.5, mtbf_hours: 6200.0, install_date: "2018-09-01", manufacturer: "Grundfos", location_idx: 2 },
        EquipmentDef { name: "Pump-3", iso14224_class: "Centrifugal Pump", isa95_level: "Unit", criticality_score: 7.0, mtbf_hours: 7500.0, install_date: "2021-11-15", manufacturer: "Flowserve", location_idx: 2 },
        EquipmentDef { name: "Pump-4", iso14224_class: "Centrifugal Pump", isa95_level: "Unit", criticality_score: 6.9, mtbf_hours: 7800.0, install_date: "2021-11-15", manufacturer: "Flowserve", location_idx: 2 },
        // Building C also has motors
        EquipmentDef { name: "Motor-1", iso14224_class: "Electric Motor", isa95_level: "Unit", criticality_score: 7.8, mtbf_hours: 20000.0, install_date: "2018-09-01", manufacturer: "ABB Ltd", location_idx: 2 },
        EquipmentDef { name: "Motor-2", iso14224_class: "Electric Motor", isa95_level: "Unit", criticality_score: 7.6, mtbf_hours: 19500.0, install_date: "2018-09-01", manufacturer: "ABB Ltd", location_idx: 2 },
        EquipmentDef { name: "Motor-3", iso14224_class: "Electric Motor", isa95_level: "Unit", criticality_score: 6.2, mtbf_hours: 22000.0, install_date: "2022-02-28", manufacturer: "Siemens AG", location_idx: 2 },
        EquipmentDef { name: "Motor-4", iso14224_class: "Electric Motor", isa95_level: "Unit", criticality_score: 6.0, mtbf_hours: 23000.0, install_date: "2022-02-28", manufacturer: "Siemens AG", location_idx: 2 },
        // Building D - Power House (index 3)
        EquipmentDef { name: "Boiler-1", iso14224_class: "Steam Boiler", isa95_level: "Unit", criticality_score: 9.5, mtbf_hours: 5000.0, install_date: "2017-05-20", manufacturer: "Babcock & Wilcox", location_idx: 3 },
        EquipmentDef { name: "Boiler-2", iso14224_class: "Steam Boiler", isa95_level: "Unit", criticality_score: 9.3, mtbf_hours: 5200.0, install_date: "2017-05-20", manufacturer: "Babcock & Wilcox", location_idx: 3 },
        EquipmentDef { name: "Boiler-3", iso14224_class: "Steam Boiler", isa95_level: "Unit", criticality_score: 8.0, mtbf_hours: 6500.0, install_date: "2020-08-10", manufacturer: "Cleaver-Brooks", location_idx: 3 },
        EquipmentDef { name: "Boiler-4", iso14224_class: "Steam Boiler", isa95_level: "Unit", criticality_score: 7.8, mtbf_hours: 6800.0, install_date: "2020-08-10", manufacturer: "Cleaver-Brooks", location_idx: 3 },
    ];

    let mut equipment_ids = Vec::new();
    {
        let mut store = client.store_write().await;
        for eq in &equipment_defs {
            let id = store.create_node("Equipment");
            store.set_node_property(tenant, id, "name",
                PropertyValue::String(eq.name.to_string())).unwrap();
            store.set_node_property(tenant, id, "iso14224_class",
                PropertyValue::String(eq.iso14224_class.to_string())).unwrap();
            store.set_node_property(tenant, id, "isa95_level",
                PropertyValue::String(eq.isa95_level.to_string())).unwrap();
            store.set_node_property(tenant, id, "criticality_score",
                PropertyValue::Float(eq.criticality_score)).unwrap();
            store.set_node_property(tenant, id, "mtbf_hours",
                PropertyValue::Float(eq.mtbf_hours)).unwrap();
            store.set_node_property(tenant, id, "install_date",
                PropertyValue::String(eq.install_date.to_string())).unwrap();
            store.set_node_property(tenant, id, "manufacturer",
                PropertyValue::String(eq.manufacturer.to_string())).unwrap();
            store.set_node_property(tenant, id, "status",
                PropertyValue::String("Operational".to_string())).unwrap();
            // Link equipment to location
            store.create_edge(location_ids[eq.location_idx], id, "CONTAINS_EQUIPMENT").unwrap();
            equipment_ids.push(id);
        }
    }

    // --- Sensors (3 per equipment = 60 total) ---
    let sensor_templates: Vec<SensorDef> = vec![
        SensorDef { name: "Temperature", sensor_type: "temperature", unit: "degC", min_threshold: -10.0, max_threshold: 120.0 },
        SensorDef { name: "Vibration",   sensor_type: "vibration",   unit: "mm/s", min_threshold: 0.0,   max_threshold: 11.2 },
        SensorDef { name: "Pressure",    sensor_type: "pressure",    unit: "bar",  min_threshold: 0.5,   max_threshold: 16.0 },
    ];

    let mut sensor_ids = Vec::new();
    {
        let mut store = client.store_write().await;
        for (eq_idx, eq) in equipment_defs.iter().enumerate() {
            for tmpl in &sensor_templates {
                let id = store.create_node("Sensor");
                let sensor_name = format!("{}-{}", eq.name, tmpl.name);
                store.set_node_property(tenant, id, "name",
                    PropertyValue::String(sensor_name)).unwrap();
                store.set_node_property(tenant, id, "type",
                    PropertyValue::String(tmpl.sensor_type.to_string())).unwrap();
                store.set_node_property(tenant, id, "unit",
                    PropertyValue::String(tmpl.unit.to_string())).unwrap();
                store.set_node_property(tenant, id, "min_threshold",
                    PropertyValue::Float(tmpl.min_threshold)).unwrap();
                store.set_node_property(tenant, id, "max_threshold",
                    PropertyValue::Float(tmpl.max_threshold)).unwrap();
                store.create_edge(equipment_ids[eq_idx], id, "HAS_SENSOR").unwrap();
                sensor_ids.push(id);
            }
        }
    }

    let build_time = start.elapsed();

    let (total_nodes, total_edges) = {
        let store = client.store_read().await;
        (store.all_nodes().len(), store.edge_count())
    };

    println!("  Asset hierarchy constructed in {:.2?}", build_time);
    println!();
    println!("  +---------------------+-------+");
    println!("  | Entity              | Count |");
    println!("  +---------------------+-------+");
    println!("  | Site                |     1 |");
    println!("  | Locations           |     {} |", location_ids.len());
    println!("  | Equipment           |    {} |", equipment_ids.len());
    println!("  | Sensors             |    {} |", sensor_ids.len());
    println!("  | Total Nodes         |    {} |", total_nodes);
    println!("  | Total Edges         |    {} |", total_edges);
    println!("  +---------------------+-------+");
    println!();
    println!("  Ontology: Site -[CONTAINS_LOCATION]-> Location -[CONTAINS_EQUIPMENT]-> Equipment -[HAS_SENSOR]-> Sensor");

    // ==================================================================================
    // STEP 2: Failure Modes + Vector Search
    // ==================================================================================
    println!("\n[Step 2] Failure Modes + Vector Search (Semantic Similarity)");
    println!("---------------------------------------------------------------------------");
    let start = Instant::now();

    // Failure modes inspired by AssetOpsBench FMSR data
    let failure_mode_defs: Vec<FailureModeDef> = vec![
        FailureModeDef {
            name: "Compressor Overheating",
            description: "Compressor failed due to normal wear causing overheating; discharge temperature exceeded 95C threshold",
            severity: "Critical",
            equipment_indices: vec![0, 1, 2, 3], // Chillers
        },
        FailureModeDef {
            name: "Bearing Wear Degradation",
            description: "Fan motor bearing worn due to normal fatigue cycles; elevated vibration levels detected at 8.5 mm/s",
            severity: "High",
            equipment_indices: vec![0, 1, 8, 9], // Chillers + Pumps
        },
        FailureModeDef {
            name: "Impeller Cavitation",
            description: "Pump impeller cavitation from low NPSH causing pitting erosion and reduced flow rate by 30%",
            severity: "High",
            equipment_indices: vec![8, 9, 10, 11], // Pumps
        },
        FailureModeDef {
            name: "Refrigerant Leak",
            description: "Refrigerant charge loss through corroded pipe joints; system capacity dropped below 70%",
            severity: "Critical",
            equipment_indices: vec![0, 1, 2, 3], // Chillers
        },
        FailureModeDef {
            name: "Motor Winding Failure",
            description: "Electric motor stator winding insulation breakdown from thermal cycling; phase-to-phase short detected",
            severity: "Critical",
            equipment_indices: vec![12, 13, 14, 15], // Motors
        },
        FailureModeDef {
            name: "Evaporator Water-Side Fouling",
            description: "Evaporator heat exchanger fouled with calcium scale deposits; approach temperature increased by 4K",
            severity: "Medium",
            equipment_indices: vec![0, 1, 2, 3], // Chillers
        },
        FailureModeDef {
            name: "Belt/Sheave Wear",
            description: "AHU drive belts worn with visible cracking; belt slippage causing 15% airflow reduction",
            severity: "Medium",
            equipment_indices: vec![4, 5, 6, 7], // AHUs
        },
        FailureModeDef {
            name: "Pressure Regulator Diaphragm Failure",
            description: "Pressure regulator diaphragm ruptured causing erratic pressure control; oscillations exceeding +/-2 bar",
            severity: "High",
            equipment_indices: vec![4, 5, 8, 9], // AHUs + Pumps
        },
        FailureModeDef {
            name: "Steam Coil Air-Side Fouling",
            description: "AHU heating coil fins blocked with dust and debris; heat transfer coefficient reduced 40%",
            severity: "Medium",
            equipment_indices: vec![4, 5, 6, 7], // AHUs
        },
        FailureModeDef {
            name: "Condenser Water Flow Imbalance",
            description: "Condenser water-side flow rate 25% below design due to partially closed valve; head pressure elevated",
            severity: "High",
            equipment_indices: vec![0, 1, 2, 3], // Chillers
        },
        FailureModeDef {
            name: "Boiler Tube Corrosion",
            description: "Fire-side boiler tube thinning from oxygen pitting corrosion; wall thickness below minimum threshold",
            severity: "Critical",
            equipment_indices: vec![16, 17, 18, 19], // Boilers
        },
        FailureModeDef {
            name: "Solenoid Valve Binding",
            description: "Solenoid valve bound due to hardened grease on plunger; valve stuck in partially open position",
            severity: "Medium",
            equipment_indices: vec![4, 5, 8, 9], // AHUs + Pumps
        },
        FailureModeDef {
            name: "Seal Mechanical Failure",
            description: "Pump mechanical seal face worn from abrasive particles in process fluid; visible leakage at 5 drops/min",
            severity: "High",
            equipment_indices: vec![8, 9, 10, 11], // Pumps
        },
        FailureModeDef {
            name: "Burner Flame Instability",
            description: "Boiler burner flame rollback and instability from fuel-air ratio drift; combustion efficiency dropped to 78%",
            severity: "Critical",
            equipment_indices: vec![16, 17, 18, 19], // Boilers
        },
        FailureModeDef {
            name: "VFD Overload Trip",
            description: "Variable frequency drive tripped on overcurrent during motor startup; drive capacitors showing ESR degradation",
            severity: "High",
            equipment_indices: vec![12, 13, 14, 15], // Motors
        },
    ];

    // Create FailureMode nodes and MONITORS edges
    let mut failure_mode_ids = Vec::new();
    let mut failure_embeddings: Vec<Vec<f32>> = Vec::new();
    {
        let mut store = client.store_write().await;
        for (fm_idx, fm) in failure_mode_defs.iter().enumerate() {
            let id = store.create_node("FailureMode");
            store.set_node_property(tenant, id, "name",
                PropertyValue::String(fm.name.to_string())).unwrap();
            store.set_node_property(tenant, id, "description",
                PropertyValue::String(fm.description.to_string())).unwrap();
            store.set_node_property(tenant, id, "severity",
                PropertyValue::String(fm.severity.to_string())).unwrap();
            // MONITORS edges to equipment
            for &eq_idx in &fm.equipment_indices {
                store.create_edge(id, equipment_ids[eq_idx], "MONITORS").unwrap();
            }
            failure_mode_ids.push(id);
            failure_embeddings.push(mock_embedding(fm_idx + 100));
        }
    }

    // Create HNSW vector index and add embeddings
    client.create_vector_index("FailureMode", "embedding", 128, DistanceMetric::Cosine)
        .await.unwrap();

    for (i, fm_id) in failure_mode_ids.iter().enumerate() {
        client.add_vector("FailureMode", "embedding", *fm_id, &failure_embeddings[i])
            .await.unwrap();
    }

    // Vector search: find failure modes similar to "compressor overheating"
    // Use the embedding of the first failure mode (Compressor Overheating) with slight perturbation
    let query_vec = query_embedding(100);

    let search_results = client.vector_search("FailureMode", "embedding", &query_vec, 5)
        .await.unwrap();

    let vs_time = start.elapsed();

    println!("  Created {} failure mode nodes with 128-dim embeddings", failure_mode_ids.len());
    println!("  Vector index built in {:.2?}", vs_time);
    println!();
    println!("  Query: \"Find failure modes similar to compressor overheating\"");
    println!();
    println!("  +------+----------------------------------+----------+----------+");
    println!("  | Rank | Failure Mode                     | Severity | Distance |");
    println!("  +------+----------------------------------+----------+----------+");

    {
        let store = client.store_read().await;
        for (rank, (node_id, distance)) in search_results.iter().enumerate() {
            if let Some(node) = store.get_node(*node_id) {
                let name = node.get_property("name")
                    .and_then(|v| v.as_string().map(|s| s.to_string()))
                    .unwrap_or_default();
                let severity = node.get_property("severity")
                    .and_then(|v| v.as_string().map(|s| s.to_string()))
                    .unwrap_or_default();
                println!("  | {:>4} | {:<32} | {:<8} | {:>8.4} |",
                    rank + 1, &name[..name.len().min(32)], severity, distance);
            }
        }
    }
    println!("  +------+----------------------------------+----------+----------+");

    // ==================================================================================
    // STEP 3: Dependency Graph + Cascade Analysis
    // ==================================================================================
    println!("\n[Step 3] Dependency Graph + Cascade Analysis");
    println!("---------------------------------------------------------------------------");
    let start = Instant::now();

    // Add DEPENDS_ON edges (downstream dependencies)
    // AHUs depend on Chillers for chilled water supply
    // Motors drive Pumps
    // Boilers provide steam/hot water to AHUs
    let dependency_edges: Vec<(usize, usize, &str)> = vec![
        // AHU depends on Chiller (chilled water)
        (4, 0, "DEPENDS_ON"),  // AHU-1 -> Chiller-1
        (5, 1, "DEPENDS_ON"),  // AHU-2 -> Chiller-2
        (6, 2, "DEPENDS_ON"),  // AHU-3 -> Chiller-3
        (7, 3, "DEPENDS_ON"),  // AHU-4 -> Chiller-4
        // Pumps depend on Motors (drive coupling)
        (8,  12, "DEPENDS_ON"),  // Pump-1 -> Motor-1
        (9,  13, "DEPENDS_ON"),  // Pump-2 -> Motor-2
        (10, 14, "DEPENDS_ON"),  // Pump-3 -> Motor-3
        (11, 15, "DEPENDS_ON"),  // Pump-4 -> Motor-4
        // Chillers depend on Pumps (condenser water circulation)
        (0, 8,  "DEPENDS_ON"),  // Chiller-1 -> Pump-1
        (1, 9,  "DEPENDS_ON"),  // Chiller-2 -> Pump-2
        (2, 10, "DEPENDS_ON"),  // Chiller-3 -> Pump-3
        (3, 11, "DEPENDS_ON"),  // Chiller-4 -> Pump-4
        // AHUs also depend on Boilers (heating coils)
        (4, 16, "DEPENDS_ON"),  // AHU-1 -> Boiler-1
        (5, 17, "DEPENDS_ON"),  // AHU-2 -> Boiler-2
        (6, 18, "DEPENDS_ON"),  // AHU-3 -> Boiler-3
        (7, 19, "DEPENDS_ON"),  // AHU-4 -> Boiler-4
    ];

    // Add SHARES_SYSTEM_WITH edges (shared infrastructure)
    let shared_system_edges: Vec<(usize, usize, &str)> = vec![
        // Chillers share cooling loop
        (0, 1, "SHARES_SYSTEM_WITH"),
        (2, 3, "SHARES_SYSTEM_WITH"),
        // Boilers share steam header
        (16, 17, "SHARES_SYSTEM_WITH"),
        (18, 19, "SHARES_SYSTEM_WITH"),
        // Motors share electrical bus
        (12, 13, "SHARES_SYSTEM_WITH"),
        (14, 15, "SHARES_SYSTEM_WITH"),
    ];

    {
        let mut store = client.store_write().await;
        for (src, tgt, edge_type) in &dependency_edges {
            store.create_edge(equipment_ids[*src], equipment_ids[*tgt], *edge_type).unwrap();
        }
        for (src, tgt, edge_type) in &shared_system_edges {
            store.create_edge(equipment_ids[*src], equipment_ids[*tgt], *edge_type).unwrap();
        }
    }

    // Build adjacency map for equipment (DEPENDS_ON, reversed: who depends on me?)
    // If Chiller-1 fails, find all equipment that transitively depends on it
    let failed_idx = 0; // Chiller-1
    let failed_id = equipment_ids[failed_idx];
    let failed_name = equipment_defs[failed_idx].name;

    println!("  ALERT: Simulating failure of {} ({})",
        failed_name, equipment_defs[failed_idx].iso14224_class);
    println!("  Tracing reverse dependencies: \"Who depends on {}?\"", failed_name);
    println!();

    // Build reverse dependency map: for each equipment, who depends on it
    let mut reverse_deps: HashMap<usize, Vec<usize>> = HashMap::new();
    for (src, tgt, etype) in &dependency_edges {
        if *etype == "DEPENDS_ON" {
            // src depends on tgt, so if tgt fails, src is affected
            reverse_deps.entry(*tgt).or_default().push(*src);
        }
    }

    // BFS from failed equipment through reverse dependency graph
    let mut visited = vec![false; equipment_defs.len()];
    let mut queue = VecDeque::new();
    let mut cascade_levels: Vec<(usize, Vec<usize>)> = Vec::new(); // (depth, equipment_indices)

    visited[failed_idx] = true;
    // Seed with equipment that directly depends on the failed one
    if let Some(dependents) = reverse_deps.get(&failed_idx) {
        let mut level_items = Vec::new();
        for &dep in dependents {
            if !visited[dep] {
                visited[dep] = true;
                queue.push_back((dep, 1));
                level_items.push(dep);
            }
        }
        if !level_items.is_empty() {
            cascade_levels.push((1, level_items));
        }
    }

    // Also check SHARES_SYSTEM_WITH for co-failure
    let mut shared_affected = Vec::new();
    for (src, tgt, _) in &shared_system_edges {
        if *src == failed_idx && !visited[*tgt] {
            visited[*tgt] = true;
            queue.push_back((*tgt, 1));
            shared_affected.push(*tgt);
        }
        if *tgt == failed_idx && !visited[*src] {
            visited[*src] = true;
            queue.push_back((*src, 1));
            shared_affected.push(*src);
        }
    }
    if !shared_affected.is_empty() {
        // Merge shared into level 1 if it exists, or create new level
        if let Some(level) = cascade_levels.iter_mut().find(|(d, _)| *d == 1) {
            level.1.extend(shared_affected);
        } else {
            cascade_levels.push((1, shared_affected));
        }
    }

    // Continue BFS
    while let Some((eq_idx, depth)) = queue.pop_front() {
        if let Some(dependents) = reverse_deps.get(&eq_idx) {
            let mut level_items = Vec::new();
            for &dep in dependents {
                if !visited[dep] {
                    visited[dep] = true;
                    queue.push_back((dep, depth + 1));
                    level_items.push(dep);
                }
            }
            if !level_items.is_empty() {
                if let Some(level) = cascade_levels.iter_mut().find(|(d, _)| *d == depth + 1) {
                    level.1.extend(level_items);
                } else {
                    cascade_levels.push((depth + 1, level_items));
                }
            }
        }
    }

    let cascade_time = start.elapsed();

    println!("  Cascade Tree (BFS from {}):", failed_name);
    println!("  +-------+-----+-------------------+----------------------+------------------+");
    println!("  | Depth | Idx | Equipment         | Class                | Mechanism        |");
    println!("  +-------+-----+-------------------+----------------------+------------------+");
    println!("  |     0 |   0 | {:<17} | {:<20} | ROOT FAILURE     |",
        failed_name, equipment_defs[failed_idx].iso14224_class);

    cascade_levels.sort_by_key(|(d, _)| *d);
    let mut total_affected = 0;
    for (depth, indices) in &cascade_levels {
        for &idx in indices {
            let mechanism = if shared_system_edges.iter().any(|(s, t, _)|
                (*s == failed_idx && *t == idx) || (*t == failed_idx && *s == idx)) {
                "SHARED SYSTEM"
            } else {
                "DEPENDENCY"
            };
            println!("  |     {} | {:>3} | {:<17} | {:<20} | {:<16} |",
                depth, idx, equipment_defs[idx].name, equipment_defs[idx].iso14224_class, mechanism);
            total_affected += 1;
        }
    }
    println!("  +-------+-----+-------------------+----------------------+------------------+");
    println!();
    println!("  Total affected equipment: {} (cascade analysis in {:.2?})", total_affected, cascade_time);

    // ==================================================================================
    // STEP 4: PageRank Equipment Criticality
    // ==================================================================================
    println!("\n[Step 4] PageRank Equipment Criticality Analysis");
    println!("---------------------------------------------------------------------------");
    println!("  Running PageRank on full plant graph to identify critical equipment...");

    let start = Instant::now();
    let _view = client.build_view(None, None, None).await;
    let scores = client.page_rank(PageRankConfig {
        damping_factor: 0.85,
        iterations: 30,
        tolerance: 0.0001,
        ..Default::default()
    }, None, None).await;
    let pr_time = start.elapsed();

    // Collect equipment scores
    let mut eq_scores: Vec<(usize, f64)> = equipment_ids.iter().enumerate()
        .map(|(i, eid)| (i, scores.get(&eid.as_u64()).copied().unwrap_or(0.0)))
        .collect();
    eq_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!("\n  PageRank computed in {:.2?} ({} nodes in graph)", pr_time, total_nodes + failure_mode_ids.len());
    println!();
    println!("  Top 10 Equipment by Operational Criticality:");
    println!("  +------+-------------------+----------------------+-----------+----------+");
    println!("  | Rank | Equipment         | Class                | PageRank  | Crit.Scr |");
    println!("  +------+-------------------+----------------------+-----------+----------+");

    for (rank, (eq_idx, score)) in eq_scores.iter().take(10).enumerate() {
        let eq = &equipment_defs[*eq_idx];
        println!("  | {:>4} | {:<17} | {:<20} | {:>9.6} | {:>8.1} |",
            rank + 1, eq.name, eq.iso14224_class, score, eq.criticality_score);
    }
    println!("  +------+-------------------+----------------------+-----------+----------+");

    // Location criticality
    println!();
    println!("  Location Criticality:");
    let mut loc_scores: Vec<(usize, f64)> = location_ids.iter().enumerate()
        .map(|(i, lid)| (i, scores.get(&lid.as_u64()).copied().unwrap_or(0.0)))
        .collect();
    loc_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!("  +------+----------------------------------+-----------+");
    println!("  | Rank | Location                         | PageRank  |");
    println!("  +------+----------------------------------+-----------+");
    for (rank, (loc_idx, score)) in loc_scores.iter().enumerate() {
        println!("  | {:>4} | {:<32} | {:>9.6} |",
            rank + 1, location_defs[*loc_idx].name, score);
    }
    println!("  +------+----------------------------------+-----------+");

    // ==================================================================================
    // STEP 5: NSGA-II Maintenance Scheduling Optimization
    // ==================================================================================
    println!("\n[Step 5] NSGA-II Maintenance Scheduling Optimization");
    println!("---------------------------------------------------------------------------");
    println!("  Objective 1: Minimize total maintenance cost");
    println!("  Objective 2: Minimize max simultaneous downtime (load leveling)");
    println!("  Variables: Maintenance week (1-52) for each of 20 equipment units");

    struct MaintenanceScheduleProblem {
        n_equipment: usize,
        mtbf_hours: Vec<f64>,
        criticality_scores: Vec<f64>,
    }

    impl MultiObjectiveProblem for MaintenanceScheduleProblem {
        fn dim(&self) -> usize { self.n_equipment }
        fn num_objectives(&self) -> usize { 2 }

        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            (Array1::from_elem(self.n_equipment, 1.0),
             Array1::from_elem(self.n_equipment, 52.0))
        }

        fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
            // Objective 1: Total maintenance cost
            // Cost increases if maintenance is scheduled later relative to MTBF
            // Equipment with lower MTBF should be maintained earlier
            let mut total_cost = 0.0;
            for i in 0..self.n_equipment {
                let week = x[i].round().max(1.0).min(52.0);
                let mtbf_weeks = self.mtbf_hours[i] / 168.0; // hours -> weeks
                let urgency = week / mtbf_weeks; // >1 means overdue
                let base_cost = 5000.0 * self.criticality_scores[i] / 10.0;
                if urgency > 0.8 {
                    // Late maintenance costs more (risk of failure)
                    total_cost += base_cost * (1.0 + (urgency - 0.8).powi(2) * 5.0);
                } else {
                    total_cost += base_cost;
                }
            }

            // Objective 2: Max simultaneous downtime (load leveling)
            // Count how many equipment are scheduled in the same week
            let mut week_counts = vec![0.0f64; 53];
            for i in 0..self.n_equipment {
                let week = x[i].round().max(1.0).min(52.0) as usize;
                week_counts[week] += self.criticality_scores[i]; // Weight by criticality
            }
            let max_concurrent = week_counts.iter().cloned().fold(0.0f64, f64::max);

            vec![total_cost, max_concurrent]
        }

        fn penalties(&self, x: &Array1<f64>) -> Vec<f64> {
            // Penalize scheduling all in the same week
            let mut week_counts = vec![0usize; 53];
            for i in 0..self.n_equipment {
                let week = x[i].round().max(1.0).min(52.0) as usize;
                week_counts[week] += 1;
            }
            let max_same_week = *week_counts.iter().max().unwrap_or(&0);
            if max_same_week > 4 {
                vec![(max_same_week as f64 - 4.0).powi(2) * 10000.0]
            } else {
                vec![0.0]
            }
        }
    }

    let mtbf_vec: Vec<f64> = equipment_defs.iter().map(|eq| eq.mtbf_hours).collect();
    let crit_vec: Vec<f64> = equipment_defs.iter().map(|eq| eq.criticality_score).collect();

    let maint_problem = MaintenanceScheduleProblem {
        n_equipment: equipment_defs.len(),
        mtbf_hours: mtbf_vec,
        criticality_scores: crit_vec,
    };

    let start = Instant::now();
    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 80,
        max_iterations: 150,
    });
    let maint_result = solver.solve(&maint_problem);
    let maint_time = start.elapsed();

    println!("\n  Optimization complete in {:.2?}", maint_time);
    println!("  Pareto front: {} non-dominated solutions", maint_result.pareto_front.len());
    println!();

    // Display top 8 Pareto solutions
    let display_count = maint_result.pareto_front.len().min(8);
    println!("  Pareto Front (top {} solutions):", display_count);
    println!("  +------+---------------+------------------+");
    println!("  | Sol# | Total Cost $  | Max Concurrency  |");
    println!("  +------+---------------+------------------+");

    for (i, sol) in maint_result.pareto_front.iter().take(display_count).enumerate() {
        println!("  | {:>4} | ${:>11.0} | {:>16.1} |",
            i + 1, sol.fitness[0], sol.fitness[1]);
    }
    println!("  +------+---------------+------------------+");

    // Show schedule for the best balanced solution (middle of Pareto front)
    let best_idx = maint_result.pareto_front.len() / 2;
    if let Some(best_sol) = maint_result.pareto_front.get(best_idx) {
        println!();
        println!("  Selected solution #{} schedule:", best_idx + 1);
        println!("  +-------------------+----------------------+----------+-----------+");
        println!("  | Equipment         | Class                | MTBF wks | Maint Wk  |");
        println!("  +-------------------+----------------------+----------+-----------+");

        for (i, eq) in equipment_defs.iter().enumerate() {
            let week = best_sol.variables[i].round().max(1.0).min(52.0);
            let mtbf_wks = eq.mtbf_hours / 168.0;
            println!("  | {:<17} | {:<20} | {:>6.0}   | Wk {:>5.0}  |",
                eq.name, eq.iso14224_class, mtbf_wks, week);
        }
        println!("  +-------------------+----------------------+----------+-----------+");
    }

    // ==================================================================================
    // STEP 6: NLQ Integration
    // ==================================================================================
    println!("\n===========================================================================");
    println!("   NLQ Industrial Intelligence (ClaudeCode)");
    println!("===========================================================================");
    println!();

    if is_claude_available() {
        println!("  [ok] Claude Code CLI detected -- running NLQ queries");
        println!();

        let nlq_config = NLQConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: String::new(),
            api_key: None,
            api_base_url: None,
            system_prompt: Some(
                "You are a Cypher query expert for an industrial asset knowledge graph.".to_string()
            ),
        };

        let tenant_mgr = client.tenant_manager();
        tenant_mgr.create_tenant(
            "industrial_nlq".to_string(),
            "Industrial KG NLQ".to_string(),
            None,
        ).unwrap();
        tenant_mgr.update_nlq_config("industrial_nlq", Some(nlq_config.clone())).unwrap();

        let schema_summary = "Node labels: Site, Location, Equipment, Sensor, FailureMode\n\
                              Edge types: CONTAINS_LOCATION, CONTAINS_EQUIPMENT, HAS_SENSOR, MONITORS, DEPENDS_ON, SHARES_SYSTEM_WITH\n\
                              Relationships: (Site)-[:CONTAINS_LOCATION]->(Location)-[:CONTAINS_EQUIPMENT]->(Equipment)-[:HAS_SENSOR]->(Sensor), \
                              (FailureMode)-[:MONITORS]->(Equipment), (Equipment)-[:DEPENDS_ON]->(Equipment), (Equipment)-[:SHARES_SYSTEM_WITH]->(Equipment)\n\
                              Properties: Equipment(name, iso14224_class, isa95_level, criticality_score[0-10], mtbf_hours, install_date, manufacturer, status), \
                              Sensor(name, type['temperature','vibration','pressure'], unit, min_threshold, max_threshold), \
                              FailureMode(name, description, severity['Critical','High','Medium']), \
                              Location(name, location_code), Site(name, site_code)";

        let nlq_pipeline = client.nlq_pipeline(nlq_config).unwrap();

        let nlq_questions = vec![
            "What equipment has the highest criticality score?",
            "Which sensors are monitoring Chiller-1?",
        ];

        for (i, question) in nlq_questions.iter().enumerate() {
            println!("  NLQ Query {}: \"{}\"", i + 1, question);
            match nlq_pipeline.text_to_cypher(question, schema_summary).await {
                Ok(cypher) => {
                    println!("  Generated Cypher: {}", cypher);
                    match client.query_readonly(tenant, &cypher).await {
                        Ok(batch) => println!("  Results: {} records", batch.records.len()),
                        Err(e) => println!("  Execution error: {}", e),
                    }
                }
                Err(e) => println!("  NLQ translation error: {}", e),
            }
            println!();
        }
    } else {
        println!("  [skip] Claude Code CLI not found -- skipping NLQ queries");
        println!("  Install: https://docs.anthropic.com/en/docs/claude-code");
    }

    // ==================================================================================
    // SUMMARY
    // ==================================================================================
    println!("\n===========================================================================");
    println!("   INDUSTRIAL KNOWLEDGE GRAPH SUMMARY");
    println!("===========================================================================");
    println!();
    println!("  Asset Hierarchy (ISO 14224 + ISA-95):");
    println!("    - 1 site, {} locations, {} equipment, {} sensors",
        location_ids.len(), equipment_ids.len(), sensor_ids.len());
    println!("    - {} failure modes with vector embeddings", failure_mode_ids.len());
    println!("    - Total graph nodes: {}, edges: {}",
        total_nodes + failure_mode_ids.len(),
        total_edges + failure_mode_defs.iter().map(|fm| fm.equipment_indices.len()).sum::<usize>()
            + dependency_edges.len() + shared_system_edges.len());
    println!();
    println!("  Vector Search:");
    println!("    - 128-dim HNSW index on failure mode embeddings");
    println!("    - Top {} similar failure modes retrieved", search_results.len());
    println!();
    println!("  Cascade Analysis:");
    println!("    - {} failure cascades to {} downstream equipment",
        failed_name, total_affected);
    println!();
    println!("  PageRank Criticality:");
    if let Some((eq_idx, score)) = eq_scores.first() {
        println!("    - Most critical: {} (score={:.6})",
            equipment_defs[*eq_idx].name, score);
    }
    println!();
    println!("  NSGA-II Maintenance Scheduling:");
    println!("    - {} Pareto-optimal schedules for {} equipment",
        maint_result.pareto_front.len(), equipment_defs.len());
    println!();
    println!("===========================================================================");
    println!("   Industrial Knowledge Graph -- Complete");
    println!("===========================================================================");
}
