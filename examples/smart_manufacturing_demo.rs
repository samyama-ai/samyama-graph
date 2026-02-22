//! Smart Manufacturing Digital Twin: Industry 4.0 Factory Floor
//!
//! Scenarios:
//! 1. Build factory digital twin graph (Machines -> ProductionLines -> Products -> Materials)
//! 2. Production scheduling optimization via Cuckoo Search
//! 3. Predictive maintenance - failure cascade analysis
//! 4. Energy cost optimization via Jaya algorithm
//! 5. Quality traceability - defect root cause analysis
//! 6. Machine criticality analysis using PageRank

use samyama_sdk::{
    EmbeddedClient, SamyamaClient, AlgorithmClient,
    Label, PropertyValue, PageRankConfig,
    NLQConfig, LLMProvider,
    CuckooSolver, JayaSolver, SolverConfig, Problem,
    Array1,
};
use std::time::Instant;
use std::collections::HashMap;

fn is_claude_available() -> bool {
    std::process::Command::new("which")
        .arg("claude")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::main]
async fn main() {
    println!("=========================================================================");
    println!("   SMART MANUFACTURING DIGITAL TWIN -- Industry 4.0 Factory Simulation   ");
    println!("   Samyama Graph Database + Optimization Engine                          ");
    println!("=========================================================================");

    let client = EmbeddedClient::new();
    let tenant = "factory_floor";

    // ==================================================================================
    // STEP 1: Build Factory Digital Twin Graph
    // ==================================================================================
    println!("\n[Step 1] Building Factory Digital Twin Graph");
    println!("---------------------------------------------------------------------------");
    let start = Instant::now();

    // --- Production Lines ---
    let line_names = [
        "Assembly Line A", "CNC Machining Center", "Robotic Welding Bay",
        "Automated Paint Shop", "Quality Inspection Lab",
    ];
    let mut line_ids = Vec::new();
    {
        let mut store = client.store_write().await;
        for name in &line_names {
            let id = store.create_node("ProductionLine");
            store.set_node_property(tenant, id, "name", PropertyValue::String(name.to_string())).unwrap();
            line_ids.push(id);
        }
    }

    // --- Machines (50+ across 5 lines) ---
    let machines_data: Vec<(&str, &str, f64, f64, f64, usize)> = vec![
        // (name, vendor, capacity_units_hr, power_kw, failure_prob, line_index)
        // Assembly Line A (index 0)
        ("S7-1500-A01", "Siemens SIMATIC S7-1500",       45.0, 12.5, 0.02, 0),
        ("S7-1500-A02", "Siemens SIMATIC S7-1500",       45.0, 12.5, 0.03, 0),
        ("IRB-6700-A03", "ABB IRB 6700",                 38.0, 18.0, 0.04, 0),
        ("IRB-6700-A04", "ABB IRB 6700",                 38.0, 18.0, 0.02, 0),
        ("UR10e-A05", "Universal Robots UR10e",           30.0,  5.5, 0.01, 0),
        ("UR10e-A06", "Universal Robots UR10e",           30.0,  5.5, 0.01, 0),
        ("KR-210-A07", "KUKA KR 210 R2700",              42.0, 22.0, 0.03, 0),
        ("MiR-250-A08", "MiR 250 AMR",                   60.0,  3.0, 0.01, 0),
        ("Keyence-A09", "Keyence IV3 Vision",             80.0,  1.2, 0.02, 0),
        ("Atlas-A10", "Atlas Copco ST Wrench",            55.0,  2.8, 0.02, 0),
        // CNC Machining Center (index 1)
        ("VF-2SS-B01", "Haas VF-2SS",                    12.0, 22.4, 0.05, 1),
        ("VF-2SS-B02", "Haas VF-2SS",                    12.0, 22.4, 0.04, 1),
        ("NLX-2500-B03", "DMG MORI NLX 2500",            10.0, 30.0, 0.03, 1),
        ("NLX-2500-B04", "DMG MORI NLX 2500",            10.0, 30.0, 0.06, 1),
        ("INTEGREX-B05", "Mazak INTEGREX i-200",           8.0, 35.0, 0.04, 1),
        ("INTEGREX-B06", "Mazak INTEGREX i-200",           8.0, 35.0, 0.03, 1),
        ("Okuma-B07", "Okuma MULTUS B300II",               9.0, 28.0, 0.05, 1),
        ("Okuma-B08", "Okuma MULTUS B300II",               9.0, 28.0, 0.04, 1),
        ("Doosan-B09", "Doosan Puma 2600SY",              11.0, 25.0, 0.03, 1),
        ("Doosan-B10", "Doosan Puma 2600SY",              11.0, 25.0, 0.05, 1),
        // Robotic Welding Bay (index 2)
        ("M-20iA-C01", "Fanuc M-20iA",                   20.0, 15.0, 0.04, 2),
        ("M-20iA-C02", "Fanuc M-20iA",                   20.0, 15.0, 0.03, 2),
        ("KR-QC-C03", "KUKA KR QUANTEC",                 18.0, 20.0, 0.05, 2),
        ("KR-QC-C04", "KUKA KR QUANTEC",                 18.0, 20.0, 0.04, 2),
        ("IRB-2600-C05", "ABB IRB 2600",                  22.0, 14.0, 0.03, 2),
        ("IRB-2600-C06", "ABB IRB 2600",                  22.0, 14.0, 0.02, 2),
        ("OTC-C07", "OTC Daihen FD-V8L",                 19.0, 16.0, 0.04, 2),
        ("Lincoln-C08", "Lincoln Electric Apex",          25.0, 18.0, 0.03, 2),
        ("Fronius-C09", "Fronius TPS/i 500",              24.0, 17.5, 0.02, 2),
        ("Miller-C10", "Miller Auto-Axcess 450",         23.0, 16.5, 0.03, 2),
        // Automated Paint Shop (index 3)
        ("Paint-D01", "Durr EcoBell3",                    35.0, 25.0, 0.02, 3),
        ("Paint-D02", "Durr EcoBell3",                    35.0, 25.0, 0.03, 3),
        ("Paint-D03", "ABB IRB 5500 FlexPainter",        30.0, 20.0, 0.02, 3),
        ("Paint-D04", "ABB IRB 5500 FlexPainter",        30.0, 20.0, 0.04, 3),
        ("Cure-D05", "Heller IR Curing Oven",            50.0, 85.0, 0.01, 3),
        ("Cure-D06", "Heller IR Curing Oven",            50.0, 85.0, 0.02, 3),
        ("Wash-D07", "Duerr EcoClean",                   40.0, 30.0, 0.01, 3),
        ("Dry-D08", "Eisenmann Drying System",           45.0, 60.0, 0.02, 3),
        ("Seal-D09", "Nordson Sealant Dispenser",        38.0, 10.0, 0.01, 3),
        ("Buff-D10", "3M Robotic Polisher",              28.0, 12.0, 0.02, 3),
        // Quality Inspection Lab (index 4)
        ("CMM-E01", "Zeiss PRISMO",                      15.0,  8.0, 0.01, 4),
        ("CMM-E02", "Hexagon Global S",                  14.0,  7.5, 0.02, 4),
        ("XRay-E03", "Nikon XT H 225",                    8.0, 12.0, 0.01, 4),
        ("CT-E04", "Waygate Phoenix V|tome|x",            6.0, 15.0, 0.02, 4),
        ("Spec-E05", "Bruker S1 TITAN XRF",              20.0,  3.0, 0.01, 4),
        ("Hard-E06", "Mitutoyo HM-200 Hardness",         25.0,  2.5, 0.01, 4),
        ("Surf-E07", "Taylor Hobson Surtronic",           30.0,  1.8, 0.01, 4),
        ("Optical-E08", "Keyence VHX-7000",              22.0,  2.0, 0.01, 4),
        ("Leak-E09", "ATEQ F620 Leak Tester",            18.0,  4.0, 0.02, 4),
        ("Force-E10", "Instron 5967 UTM",                10.0,  6.0, 0.01, 4),
    ];

    let mut machine_ids = Vec::new();
    let mut machine_line_map: HashMap<usize, Vec<usize>> = HashMap::new(); // line_idx -> vec of machine indices

    {
        let mut store = client.store_write().await;
        for (i, (name, vendor, capacity, power, fail_prob, line_idx)) in machines_data.iter().enumerate() {
            let id = store.create_node("Machine");
            store.set_node_property(tenant, id, "name", PropertyValue::String(name.to_string())).unwrap();
            store.set_node_property(tenant, id, "vendor", PropertyValue::String(vendor.to_string())).unwrap();
            store.set_node_property(tenant, id, "capacity_hr", PropertyValue::Float(*capacity)).unwrap();
            store.set_node_property(tenant, id, "power_kw", PropertyValue::Float(*power)).unwrap();
            store.set_node_property(tenant, id, "failure_prob", PropertyValue::Float(*fail_prob)).unwrap();
            store.set_node_property(tenant, id, "status", PropertyValue::String("Operational".to_string())).unwrap();
            // Set utilization (deterministic: 60-98% based on index)
            let utilization = 60.0 + ((i * 17 + 7) % 39) as f64;
            store.set_node_property(tenant, id, "utilization", PropertyValue::Float(utilization)).unwrap();
            // Link machine to production line
            store.create_edge(id, line_ids[*line_idx], "BELONGS_TO").unwrap();
            machine_ids.push(id);
            machine_line_map.entry(*line_idx).or_default().push(i);
        }
    }

    // --- Products (30 automotive parts) ---
    let products_data: Vec<(&str, &str, f64, usize)> = vec![
        // (name, part_number, demand_per_day, primary_line_index)
        ("Engine Block V8",      "EP-1001", 120.0, 1),
        ("Cylinder Head",        "EP-1002",  95.0, 1),
        ("Crankshaft",           "EP-1003",  80.0, 1),
        ("Camshaft",             "EP-1004",  85.0, 1),
        ("Connecting Rod",       "EP-1005", 200.0, 1),
        ("Piston Assembly",      "EP-1006", 180.0, 0),
        ("Transmission Housing", "EP-2001",  60.0, 1),
        ("Gear Set Primary",     "EP-2002",  70.0, 1),
        ("Gear Set Secondary",   "EP-2003",  70.0, 1),
        ("Clutch Plate",         "EP-2004", 150.0, 0),
        ("Flywheel",             "EP-2005",  90.0, 1),
        ("Drive Shaft",          "EP-3001",  75.0, 2),
        ("CV Joint Assembly",    "EP-3002", 130.0, 2),
        ("Exhaust Manifold",     "EP-3003",  65.0, 2),
        ("Turbocharger Housing", "EP-3004",  40.0, 1),
        ("Intake Manifold",      "EP-3005",  60.0, 1),
        ("Valve Cover",          "EP-4001", 110.0, 3),
        ("Oil Pan",              "EP-4002", 100.0, 3),
        ("Timing Chain Cover",   "EP-4003",  85.0, 3),
        ("Water Pump Housing",   "EP-4004",  95.0, 1),
        ("Thermostat Housing",   "EP-4005", 120.0, 1),
        ("Brake Caliper",        "EP-5001", 160.0, 1),
        ("Brake Rotor",          "EP-5002", 160.0, 1),
        ("Steering Knuckle",     "EP-5003",  70.0, 2),
        ("Control Arm",          "EP-5004",  80.0, 2),
        ("Subframe Assembly",    "EP-6001",  45.0, 2),
        ("Differential Housing", "EP-6002",  55.0, 1),
        ("Wheel Hub",            "EP-6003", 140.0, 1),
        ("Suspension Strut Cap", "EP-6004", 130.0, 0),
        ("A-Pillar Bracket",     "EP-7001", 100.0, 2),
    ];

    let mut product_ids = Vec::new();
    {
        let mut store = client.store_write().await;
        for (name, part_no, demand, line_idx) in &products_data {
            let id = store.create_node("Product");
            store.set_node_property(tenant, id, "name", PropertyValue::String(name.to_string())).unwrap();
            store.set_node_property(tenant, id, "part_number", PropertyValue::String(part_no.to_string())).unwrap();
            store.set_node_property(tenant, id, "daily_demand", PropertyValue::Float(*demand)).unwrap();
            // Link product to primary production line
            store.create_edge(id, line_ids[*line_idx], "PRODUCED_ON").unwrap();
            product_ids.push(id);
        }
    }

    // --- Raw Materials (20+) ---
    let materials_data: Vec<(&str, f64, &str)> = vec![
        // (name, cost_per_kg, supplier)
        ("Aluminum 6061-T6",      3.20, "Alcoa Corporation"),
        ("Steel 4140 Alloy",      1.85, "Nucor Steel"),
        ("Steel 1045 Carbon",     1.45, "ArcelorMittal"),
        ("Titanium Ti-6Al-4V",   28.50, "TIMET"),
        ("Carbon Fiber CF-200",   45.00, "Toray Industries"),
        ("Cast Iron GG25",        0.95, "Tupy S.A."),
        ("Stainless Steel 316L",  4.10, "Outokumpu"),
        ("Inconel 718",          52.00, "Special Metals Corp"),
        ("Copper C110",           8.50, "Freeport-McMoRan"),
        ("Magnesium AZ91D",       5.20, "Dead Sea Magnesium"),
        ("Zinc ZAMAK 3",          3.60, "Nyrstar"),
        ("Bronze C932",           7.80, "Wieland Group"),
        ("PEEK Polymer",         95.00, "Victrex"),
        ("Nylon PA66 GF30",       6.50, "DuPont"),
        ("EPDM Rubber",           4.30, "Lanxess"),
        ("Ceramic Al2O3",        12.00, "CoorsTek"),
        ("Tungsten Carbide",     65.00, "Sandvik"),
        ("Chromoly 4130",         2.10, "Timken Steel"),
        ("Beryllium Copper C17200", 35.00, "Materion"),
        ("Silicon Carbide SiC",   18.00, "Saint-Gobain"),
    ];

    let mut material_ids = Vec::new();
    {
        let mut store = client.store_write().await;
        for (name, cost, supplier) in &materials_data {
            let id = store.create_node("Material");
            store.set_node_property(tenant, id, "name", PropertyValue::String(name.to_string())).unwrap();
            store.set_node_property(tenant, id, "cost_per_kg", PropertyValue::Float(*cost)).unwrap();
            store.set_node_property(tenant, id, "supplier", PropertyValue::String(supplier.to_string())).unwrap();
            material_ids.push(id);
        }
    }

    // --- Bill of Materials edges (Product -> Material) ---
    // Map each product to 2-3 materials for realistic BOM
    let bom_links: Vec<(usize, Vec<usize>)> = vec![
        (0,  vec![5, 0]),       // Engine Block: Cast Iron, Aluminum
        (1,  vec![0, 5]),       // Cylinder Head: Aluminum, Cast Iron
        (2,  vec![1, 17]),      // Crankshaft: Steel 4140, Chromoly
        (3,  vec![1, 17]),      // Camshaft: Steel 4140, Chromoly
        (4,  vec![1, 3]),       // Connecting Rod: Steel 4140, Titanium
        (5,  vec![0, 14]),      // Piston Assembly: Aluminum, EPDM
        (6,  vec![0, 5]),       // Transmission Housing: Aluminum, Cast Iron
        (7,  vec![1, 16]),      // Gear Set Primary: Steel, Tungsten Carbide
        (8,  vec![1, 16]),      // Gear Set Secondary: Steel, Tungsten Carbide
        (9,  vec![2, 14]),      // Clutch Plate: Carbon Steel, EPDM
        (10, vec![5, 2]),       // Flywheel: Cast Iron, Carbon Steel
        (11, vec![1, 17]),      // Drive Shaft: Steel 4140, Chromoly
        (12, vec![1, 14]),      // CV Joint: Steel, EPDM
        (13, vec![6, 7]),       // Exhaust Manifold: Stainless, Inconel
        (14, vec![7, 3]),       // Turbocharger Housing: Inconel, Titanium
        (15, vec![0, 13]),      // Intake Manifold: Aluminum, Nylon
        (16, vec![0, 14]),      // Valve Cover: Aluminum, EPDM
        (17, vec![2, 0]),       // Oil Pan: Carbon Steel, Aluminum
        (18, vec![0, 14]),      // Timing Chain Cover: Aluminum, EPDM
        (19, vec![0, 8]),       // Water Pump Housing: Aluminum, Copper
        (20, vec![0, 14]),      // Thermostat Housing: Aluminum, EPDM
        (21, vec![5, 6]),       // Brake Caliper: Cast Iron, Stainless
        (22, vec![5, 15]),      // Brake Rotor: Cast Iron, Ceramic
        (23, vec![1, 0]),       // Steering Knuckle: Steel, Aluminum
        (24, vec![1, 0]),       // Control Arm: Steel, Aluminum
        (25, vec![2, 1]),       // Subframe: Carbon Steel, Alloy Steel
        (26, vec![5, 1]),       // Differential Housing: Cast Iron, Steel
        (27, vec![1, 0]),       // Wheel Hub: Steel, Aluminum
        (28, vec![2, 14]),      // Suspension Strut Cap: Steel, EPDM
        (29, vec![2, 0]),       // A-Pillar Bracket: Steel, Aluminum
    ];

    {
        let mut store = client.store_write().await;
        for (prod_idx, mat_indices) in &bom_links {
            for mat_idx in mat_indices {
                store.create_edge(product_ids[*prod_idx], material_ids[*mat_idx], "REQUIRES").unwrap();
            }
        }
    }

    // --- Machine -> Product edges (which machines produce which products) ---
    // Distribute products across machines on their line (round-robin pairs)
    {
        let mut store = client.store_write().await;
        for (prod_idx, prod_data) in products_data.iter().enumerate() {
            let line_idx = prod_data.3;
            if let Some(line_machines) = machine_line_map.get(&line_idx) {
                let nm = line_machines.len();
                // Each product assigned to 2-3 machines, cycling through line
                let start = (prod_idx * 2) % nm;
                for offset in 0..3.min(nm) {
                    let m_idx = line_machines[(start + offset) % nm];
                    store.create_edge(machine_ids[m_idx], product_ids[prod_idx], "PRODUCES").unwrap();
                }
            }
        }

        // --- Inter-line flow edges (production sequence) ---
        // CNC Machining -> Welding -> Painting -> Assembly -> Quality Inspection
        store.create_edge(line_ids[1], line_ids[2], "FEEDS_INTO").unwrap();
        store.create_edge(line_ids[2], line_ids[3], "FEEDS_INTO").unwrap();
        store.create_edge(line_ids[3], line_ids[0], "FEEDS_INTO").unwrap();
        store.create_edge(line_ids[0], line_ids[4], "FEEDS_INTO").unwrap();
    }

    let build_time = start.elapsed();

    let (total_nodes, total_machines, total_products, total_materials) = {
        let store = client.store_read().await;
        (
            store.all_nodes().len(),
            store.get_nodes_by_label(&Label::new("Machine")).len(),
            store.get_nodes_by_label(&Label::new("Product")).len(),
            store.get_nodes_by_label(&Label::new("Material")).len(),
        )
    };

    println!("  Digital twin constructed in {:.2?}", build_time);
    println!();
    println!("  +--------------------+-------+");
    println!("  | Entity             | Count |");
    println!("  +--------------------+-------+");
    println!("  | Production Lines   |     {} |", line_ids.len());
    println!("  | Machines           |    {} |", total_machines);
    println!("  | Products           |    {} |", total_products);
    println!("  | Raw Materials      |    {} |", total_materials);
    println!("  | Total Graph Nodes  |   {} |", total_nodes);
    println!("  +--------------------+-------+");

    // ==================================================================================
    // STEP 2: Production Scheduling Optimization (Cuckoo Search)
    // ==================================================================================
    println!("\n[Step 2] Production Scheduling Optimization (Cuckoo Search)");
    println!("---------------------------------------------------------------------------");
    println!("  Objective: Minimize total production cost while meeting daily demand");
    println!("  Variables: Production allocation across 10 CNC machines (line B)");
    println!("  Constraint: Total output >= 800 units/day");

    // Problem: Allocate production across 10 CNC machines
    // Each machine has different capacity and operating cost
    struct ProductionScheduleProblem {
        machine_costs: Vec<f64>,     // $/unit for each machine
        machine_capacities: Vec<f64>, // max units/hr
        target_output: f64,
    }

    impl Problem for ProductionScheduleProblem {
        fn dim(&self) -> usize { self.machine_costs.len() }

        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            let n = self.dim();
            (Array1::from_elem(n, 0.0), Array1::from_vec(self.machine_capacities.clone()))
        }

        fn objective(&self, x: &Array1<f64>) -> f64 {
            // Total cost = sum(production[i] * cost_per_unit[i])
            x.iter().zip(self.machine_costs.iter())
                .map(|(prod, cost)| prod * cost)
                .sum()
        }

        fn penalty(&self, x: &Array1<f64>) -> f64 {
            let total: f64 = x.iter().sum();
            if total < self.target_output {
                (self.target_output - total).powi(2) * 100.0
            } else {
                0.0
            }
        }
    }

    // CNC machines: cost proportional to power consumption and maintenance
    let cnc_costs = vec![4.20, 4.20, 5.80, 5.80, 7.50, 7.50, 6.10, 6.10, 4.90, 4.90];
    let cnc_caps = vec![96.0, 96.0, 80.0, 80.0, 64.0, 64.0, 72.0, 72.0, 88.0, 88.0]; // capacity * 8hr shift

    let schedule_problem = ProductionScheduleProblem {
        machine_costs: cnc_costs.clone(),
        machine_capacities: cnc_caps.clone(),
        target_output: 800.0,
    };

    let start = Instant::now();
    let solver = CuckooSolver::new(SolverConfig { population_size: 50, max_iterations: 200 });
    let result = solver.solve(&schedule_problem);
    let sched_time = start.elapsed();

    println!("\n  Optimization complete in {:.2?}", sched_time);
    println!("  Best cost: ${:.2}", result.best_fitness);
    println!();
    println!("  +------------+-------------------+----------+------------+-------------+");
    println!("  | Machine    | Vendor            | Alloc/hr | Cost/Unit  | Line Cost   |");
    println!("  +------------+-------------------+----------+------------+-------------+");

    let cnc_vendors = [
        "Haas VF-2SS",  "Haas VF-2SS",  "DMG MORI NLX",  "DMG MORI NLX",
        "Mazak INTGRX",  "Mazak INTGRX",  "Okuma MULTUS",   "Okuma MULTUS",
        "Doosan Puma",   "Doosan Puma",
    ];
    let mut total_alloc = 0.0;
    for i in 0..10 {
        let alloc = result.best_variables[i];
        let cost = alloc * cnc_costs[i];
        total_alloc += alloc;
        println!("  | {:<10} | {:<17} | {:>6.1}   | ${:>8.2}  | ${:>9.2} |",
            machines_data[10 + i].0, cnc_vendors[i], alloc, cnc_costs[i], cost);
    }
    println!("  +------------+-------------------+----------+------------+-------------+");
    println!("  | TOTAL      |                   | {:>6.1}   |            | ${:>9.2} |",
        total_alloc, result.best_fitness);
    println!("  +------------+-------------------+----------+------------+-------------+");

    if total_alloc >= 799.0 {
        println!("  Status: DEMAND MET ({:.0}/{:.0} units)", total_alloc, 800.0);
    } else {
        println!("  Status: DEMAND SHORTFALL ({:.0}/{:.0} units)", total_alloc, 800.0);
    }

    // Update graph with optimized schedule via Cypher
    let cypher_str = r#"
        CALL algo.or.solve({
            algorithm: 'Cuckoo',
            label: 'Machine',
            property: 'production',
            min: 0.0,
            max: 100.0,
            min_total: 500.0,
            cost_property: 'unit_cost',
            population_size: 50,
            max_iterations: 200
        }) YIELD fitness, algorithm, iterations
    "#;
    let _cypher_result = client.query(tenant, cypher_str).await.expect("Cypher optimization failed");

    // ==================================================================================
    // STEP 3: Failure Cascade Analysis (Predictive Maintenance)
    // ==================================================================================
    println!("\n[Step 3] Failure Cascade Analysis -- Predictive Maintenance");
    println!("---------------------------------------------------------------------------");

    // Simulate: What happens if INTEGREX-B05 (Mazak multi-axis, index 14) fails?
    let failed_machine_idx = 14; // INTEGREX-B05
    let failed_id = machine_ids[failed_machine_idx];
    let failed_name = machines_data[failed_machine_idx].0;
    let failed_vendor = machines_data[failed_machine_idx].1;

    println!("  ALERT: Simulating failure of {} ({})", failed_name, failed_vendor);
    println!();

    // Find all products this machine produces (via outgoing PRODUCES edges)
    let mut affected_products: Vec<(String, String, f64)> = Vec::new();
    {
        let store = client.store_read().await;
        let affected_edges = store.get_outgoing_edges(failed_id);

        for edge in &affected_edges {
            if let Some(target_node) = store.get_node(edge.target) {
                let labels: Vec<_> = target_node.labels.iter().map(|l| l.as_str().to_string()).collect();
                if labels.contains(&"Product".to_string()) {
                    let name = target_node.get_property("name")
                        .and_then(|v| v.as_string().map(|s| s.to_string())).unwrap_or_default();
                    let pn = target_node.get_property("part_number")
                        .and_then(|v| v.as_string().map(|s| s.to_string())).unwrap_or_default();
                    let demand = target_node.get_property("daily_demand")
                        .and_then(|v| v.as_float()).unwrap_or(0.0);
                    affected_products.push((name, pn, demand));
                }
            }
        }
    }

    // Trace downstream: affected products -> materials at risk
    println!("  Cascade Impact:");
    println!("  +------+-------------------------+-----------+------------------+");
    println!("  | Step | Entity                  | Type      | Impact           |");
    println!("  +------+-------------------------+-----------+------------------+");
    println!("  |    1 | {:<23} | Machine   | OFFLINE          |", failed_name);

    let mut impacted_material_names: Vec<String> = Vec::new();
    {
        let store = client.store_read().await;
        for (i, (name, pn, demand)) in affected_products.iter().enumerate() {
            println!("  |    {} | {:<23} | Product   | -{:.0} units/day   |", i + 2, name, demand);

            // Find materials required by this product
            let prod_id = product_ids.iter().position(|pid| {
                store.get_node(*pid)
                    .and_then(|n| n.get_property("part_number"))
                    .and_then(|v| v.as_string())
                    .map(|s| s == pn)
                    .unwrap_or(false)
            });

            if let Some(pidx) = prod_id {
                if let Some((_, mat_indices)) = bom_links.iter().find(|(pi, _)| *pi == pidx) {
                    for &mi in mat_indices {
                        let mat_name = materials_data[mi].0;
                        if !impacted_material_names.contains(&mat_name.to_string()) {
                            impacted_material_names.push(mat_name.to_string());
                        }
                    }
                }
            }
        }
    }

    let step_num = affected_products.len() + 2;
    for (i, mat) in impacted_material_names.iter().enumerate() {
        println!("  |    {} | {:<23} | Material  | Demand reduced   |", step_num + i, mat);
    }
    println!("  +------+-------------------------+-----------+------------------+");
    println!();
    println!("  Total products at risk: {}", affected_products.len());
    println!("  Total daily demand impact: {:.0} units",
        affected_products.iter().map(|(_, _, d)| d).sum::<f64>());
    println!("  Materials affected: {}", impacted_material_names.len());

    // ==================================================================================
    // STEP 4: Energy Cost Optimization (Jaya Algorithm)
    // ==================================================================================
    println!("\n[Step 4] Energy Cost Optimization (Jaya Algorithm)");
    println!("---------------------------------------------------------------------------");
    println!("  Objective: Minimize energy cost via shift scheduling");
    println!("  Shifts: Morning (6AM-2PM), Afternoon (2PM-10PM), Night (10PM-6AM)");
    println!("  Pricing: Peak $0.12/kWh (Morning+Afternoon), Off-Peak $0.06/kWh (Night)");
    println!("  Constraint: Total production >= 2400 units/day across 3 shifts");

    struct EnergyOptProblem {
        machine_powers: Vec<f64>,    // kW per machine
        shift_rates: [f64; 3],       // $/kWh per shift
        shift_hours: f64,            // hours per shift
        target_daily: f64,
        machine_throughputs: Vec<f64>, // units/hr per machine
    }

    impl Problem for EnergyOptProblem {
        fn dim(&self) -> usize { self.machine_powers.len() * 3 } // 50 machines * 3 shifts

        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            let n = self.dim();
            // 0.0 = machine off, 1.0 = machine at full capacity during shift
            (Array1::from_elem(n, 0.0), Array1::from_elem(n, 1.0))
        }

        fn objective(&self, x: &Array1<f64>) -> f64 {
            let nm = self.machine_powers.len();
            let mut total_cost = 0.0;
            for shift in 0..3 {
                for m in 0..nm {
                    let utilization = x[shift * nm + m];
                    let energy_kwh = self.machine_powers[m] * utilization * self.shift_hours;
                    total_cost += energy_kwh * self.shift_rates[shift];
                }
            }
            total_cost
        }

        fn penalty(&self, x: &Array1<f64>) -> f64 {
            let nm = self.machine_throughputs.len();
            let mut total_output = 0.0;
            for shift in 0..3 {
                for m in 0..nm {
                    let utilization = x[shift * nm + m];
                    total_output += self.machine_throughputs[m] * utilization * self.shift_hours;
                }
            }
            if total_output < self.target_daily {
                (self.target_daily - total_output).powi(2) * 10.0
            } else {
                0.0
            }
        }
    }

    let machine_powers: Vec<f64> = machines_data.iter().map(|m| m.3).collect();
    let machine_throughputs: Vec<f64> = machines_data.iter().map(|m| m.2).collect();

    let energy_problem = EnergyOptProblem {
        machine_powers: machine_powers.clone(),
        shift_rates: [0.12, 0.12, 0.06], // peak, peak, off-peak
        shift_hours: 8.0,
        target_daily: 2400.0,
        machine_throughputs: machine_throughputs.clone(),
    };

    let start = Instant::now();
    let jaya_solver = JayaSolver::new(SolverConfig { population_size: 30, max_iterations: 100 });
    let energy_result = jaya_solver.solve(&energy_problem);
    let energy_time = start.elapsed();

    // Calculate shift-level costs from the result
    let nm = machines_data.len();
    let shift_names = ["Morning 6AM-2PM", "Afternoon 2PM-10PM", "Night 10PM-6AM"];
    let shift_rate_vals = [0.12, 0.12, 0.06];

    println!("\n  Optimization complete in {:.2?}", energy_time);
    println!();
    println!("  +----------------------+----------+-----------+------------+-----------+");
    println!("  | Shift                | Rate     | Avg Util  | Energy kWh | Cost      |");
    println!("  +----------------------+----------+-----------+------------+-----------+");

    let mut total_energy = 0.0;
    let mut total_cost_energy = 0.0;
    let mut total_output_check = 0.0;

    for shift in 0..3 {
        let mut shift_energy = 0.0;
        let mut shift_util_sum = 0.0;
        for m in 0..nm {
            let util = energy_result.best_variables[shift * nm + m];
            shift_energy += machine_powers[m] * util * 8.0;
            shift_util_sum += util;
            total_output_check += machine_throughputs[m] * util * 8.0;
        }
        let shift_cost = shift_energy * shift_rate_vals[shift];
        let avg_util = shift_util_sum / nm as f64;
        total_energy += shift_energy;
        total_cost_energy += shift_cost;

        println!("  | {:<20} | ${:.4}  | {:>6.1}%   | {:>10.0} | ${:>7.2} |",
            shift_names[shift], shift_rate_vals[shift], avg_util * 100.0,
            shift_energy, shift_cost);
    }
    println!("  +----------------------+----------+-----------+------------+-----------+");
    println!("  | TOTAL                |          |           | {:>10.0} | ${:>7.2} |",
        total_energy, total_cost_energy);
    println!("  +----------------------+----------+-----------+------------+-----------+");

    // Baseline: all machines at 100% all shifts at peak rate
    let baseline_energy: f64 = machine_powers.iter().sum::<f64>() * 24.0;
    let baseline_cost = baseline_energy * 0.12;
    let savings = baseline_cost - total_cost_energy;
    let pct = if baseline_cost > 0.0 { savings / baseline_cost * 100.0 } else { 0.0 };

    println!();
    println!("  Baseline (100% all peak): ${:.2}/day ({:.0} kWh)", baseline_cost, baseline_energy);
    println!("  Optimized:                ${:.2}/day ({:.0} kWh)", total_cost_energy, total_energy);
    println!("  Daily savings:            ${:.2} ({:.1}%)", savings, pct);
    println!("  Estimated output:         {:.0} units/day (target: 2400)", total_output_check);

    // ==================================================================================
    // STEP 5: Quality Traceability -- Defect Root Cause Analysis
    // ==================================================================================
    println!("\n[Step 5] Quality Traceability -- Defect Root Cause Analysis");
    println!("---------------------------------------------------------------------------");

    // Scenario: Defective Turbocharger Housing (product index 14) detected
    let defect_product_idx = 14;
    let defect_name = products_data[defect_product_idx].0;
    let defect_pn = products_data[defect_product_idx].1;

    println!("  QUALITY ALERT: Defect detected in {} ({})", defect_name, defect_pn);
    println!();
    println!("  Tracing full provenance through graph...");
    println!();

    // 1. Find which machines produce this product (reverse lookup)
    let mut producing_machines: Vec<(String, String)> = Vec::new();
    {
        let store = client.store_read().await;
        for (m_idx, &mid) in machine_ids.iter().enumerate() {
            let edges = store.get_outgoing_edges(mid);
            for e in &edges {
                if e.target == product_ids[defect_product_idx] {
                    producing_machines.push((
                        machines_data[m_idx].0.to_string(),
                        machines_data[m_idx].1.to_string(),
                    ));
                }
            }
        }
    }

    // 2. Find materials in BOM
    let mut defect_materials: Vec<(&str, f64, &str)> = Vec::new();
    if let Some((_, mat_indices)) = bom_links.iter().find(|(pi, _)| *pi == defect_product_idx) {
        for &mi in mat_indices {
            defect_materials.push((materials_data[mi].0, materials_data[mi].1, materials_data[mi].2));
        }
    }

    // 3. Find production line
    let defect_line_idx = products_data[defect_product_idx].3;
    let defect_line_name = line_names[defect_line_idx];

    println!("  Traceability Report:");
    println!("  +-----+----------------------------+------------------+---------------------+");
    println!("  | Hop | Entity                     | Type             | Detail              |");
    println!("  +-----+----------------------------+------------------+---------------------+");
    println!("  |   0 | {:<26} | Defective Part   | {}            |", defect_name, defect_pn);
    println!("  |   1 | {:<26} | Production Line  | Primary Line      |", defect_line_name);

    for (i, (mname, mvendor)) in producing_machines.iter().enumerate() {
        println!("  |   {} | {:<26} | Machine          | {}  |",
            i + 2, mname, &mvendor[..20.min(mvendor.len())]);
    }

    let hop_base = producing_machines.len() + 2;
    for (i, (mat_name, cost, supplier)) in defect_materials.iter().enumerate() {
        println!("  |   {} | {:<26} | Raw Material     | ${:.2}/kg {}  |",
            hop_base + i, mat_name, cost, &supplier[..12.min(supplier.len())]);
    }
    println!("  +-----+----------------------------+------------------+---------------------+");
    println!();
    println!("  Root cause candidates: {} machines, {} material sources",
        producing_machines.len(), defect_materials.len());

    // ==================================================================================
    // STEP 6: Machine Criticality Analysis (PageRank)
    // ==================================================================================
    println!("\n[Step 6] Machine Criticality Analysis (PageRank)");
    println!("---------------------------------------------------------------------------");
    println!("  Running PageRank on factory graph to identify critical machines...");

    let start = Instant::now();
    let view = client.build_view(None, None, None).await;
    let scores = client.page_rank(PageRankConfig {
        damping_factor: 0.85,
        iterations: 30,
        tolerance: 0.0001,
    }, None, None).await;
    let pr_time = start.elapsed();

    // Collect machine scores and sort by criticality
    let mut machine_scores: Vec<(usize, &str, &str, f64)> = Vec::new();
    for (m_idx, &mid) in machine_ids.iter().enumerate() {
        let score = scores.get(&mid.as_u64()).copied().unwrap_or(0.0);
        machine_scores.push((m_idx, machines_data[m_idx].0, machines_data[m_idx].1, score));
    }
    machine_scores.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap());

    println!("\n  PageRank computed in {:.2?} ({} nodes analyzed)", pr_time, view.node_count);
    println!();
    println!("  Top 15 Critical Machines:");
    println!("  +------+---------------+---------------------------+------------+");
    println!("  | Rank | Machine       | Vendor                    | PageRank   |");
    println!("  +------+---------------+---------------------------+------------+");

    for (rank, (_, name, vendor, score)) in machine_scores.iter().take(15).enumerate() {
        println!("  | {:>4} | {:<13} | {:<25} | {:>10.6} |",
            rank + 1, name, vendor, score);
    }
    println!("  +------+---------------+---------------------------+------------+");

    // Find most critical production line
    let mut line_scores: Vec<(&str, f64)> = Vec::new();
    for (li, &lid) in line_ids.iter().enumerate() {
        let score = scores.get(&lid.as_u64()).copied().unwrap_or(0.0);
        line_scores.push((line_names[li], score));
    }
    line_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!();
    println!("  Production Line Criticality:");
    println!("  +------+----------------------------+------------+");
    println!("  | Rank | Production Line            | PageRank   |");
    println!("  +------+----------------------------+------------+");
    for (rank, (name, score)) in line_scores.iter().enumerate() {
        println!("  | {:>4} | {:<26} | {:>10.6} |", rank + 1, name, score);
    }
    println!("  +------+----------------------------+------------+");

    // ==================================================================================
    // NLQ Manufacturing Intelligence (ClaudeCode)
    // ==================================================================================
    println!("\n===========================================================================");
    println!("   NLQ Manufacturing Intelligence (ClaudeCode)");
    println!("===========================================================================");
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
            system_prompt: Some("You are a Cypher query expert for a smart manufacturing knowledge graph.".to_string()),
        };

        let tenant_mgr = client.tenant_manager();
        tenant_mgr.create_tenant("mfg_nlq".to_string(), "Manufacturing NLQ".to_string(), None).unwrap();
        tenant_mgr.update_nlq_config("mfg_nlq", Some(nlq_config.clone())).unwrap();

        let schema_summary = "Node labels: ProductionLine, Machine, Product, Material\n\
                              Edge types: BELONGS_TO, PRODUCES, REQUIRES, FEEDS_INTO\n\
                              Relationship paths: (Machine)-[:BELONGS_TO]->(ProductionLine), (Machine)-[:PRODUCES]->(Product)-[:REQUIRES]->(Material), (ProductionLine)-[:FEEDS_INTO]->(ProductionLine)\n\
                              Properties: Machine(name, vendor, capacity_hr, power_kw, failure_prob, status['Operational'], utilization[0.0-100.0]), \
                              Product(name, part_number, daily_demand), Material(name, cost_per_kg, supplier), \
                              ProductionLine(name[e.g. 'CNC Machining Center', 'Assembly Line A', 'Robotic Welding Bay', 'Automated Paint Shop', 'Quality Inspection Lab'])";

        let nlq_pipeline = client.nlq_pipeline(nlq_config).unwrap();

        let nlq_questions = vec![
            "Which products are affected if the CNC machining line goes offline?",
            "Show machines with utilization above 90%",
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
        println!("  [skip] Claude Code CLI not found — skipping NLQ queries");
        println!("  Install: https://docs.anthropic.com/en/docs/claude-code");
    }

    // ==================================================================================
    // SUMMARY
    // ==================================================================================
    println!("\n===========================================================================");
    println!("   DIGITAL TWIN SUMMARY");
    println!("===========================================================================");
    println!();
    println!("  Factory Graph:");
    println!("    - {} production lines, {} machines, {} products, {} materials",
        line_ids.len(), total_machines, total_products, total_materials);
    println!("    - Total graph nodes: {}", total_nodes);
    println!();
    println!("  Production Scheduling (Cuckoo Search):");
    println!("    - Optimal cost: ${:.2} for {:.0} units", result.best_fitness, total_alloc);
    println!("    - Solver: 50 nests, 200 iterations");
    println!();
    println!("  Failure Cascade:");
    println!("    - {} failure impacts {} products, {} materials",
        failed_name, affected_products.len(), impacted_material_names.len());
    println!();
    println!("  Energy Optimization (Jaya):");
    println!("    - Daily cost: ${:.2} (saved ${:.2}, {:.1}% reduction)",
        total_cost_energy, savings, pct);
    println!();
    println!("  Quality Traceability:");
    println!("    - {} traced through {} machines, {} materials",
        defect_name, producing_machines.len(), defect_materials.len());
    println!();
    println!("  Machine Criticality (PageRank):");
    if let Some((_, top_name, top_vendor, top_score)) = machine_scores.first() {
        println!("    - Most critical: {} ({}) score={:.6}",
            top_name, top_vendor, top_score);
    }
    println!();
    println!("===========================================================================");
    println!("   Smart Manufacturing Digital Twin -- Complete");
    println!("===========================================================================");
}
