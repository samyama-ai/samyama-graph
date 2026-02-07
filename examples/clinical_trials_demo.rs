//! Clinical Trials Intelligence Platform
//!
//! A comprehensive Samyama graph database demo for pharma R&D teams optimizing
//! clinical trial recruitment and site selection.
//!
//! Features demonstrated:
//! - **Knowledge Graph**: Trials, Drugs, Conditions, Sites, Patients with relationships
//! - **Vector Search**: Patient-trial matching using 128-dim embeddings
//! - **NSGA-II Optimization**: Multi-objective site selection (capacity, diversity, cost)
//! - **Graph Algorithms**: Drug interaction network (PageRank, WCC)
//! - **Competitive Landscape**: Trials competing for same patient populations
//!
//! Run: `cargo run --example clinical_trials_demo`

use samyama::{GraphStore, EdgeType, QueryEngine};
// Vector search uses cosine similarity computed inline for this demo.
// For production workloads, use: store.create_vector_index() + store.vector_search()
// with samyama::vector::DistanceMetric::Cosine for HNSW-accelerated ANN queries.
use samyama::algo::{build_view, page_rank, weakly_connected_components, PageRankConfig};
use samyama_optimization::algorithms::NSGA2Solver;
use samyama_optimization::common::{MultiObjectiveProblem, SolverConfig};
use ndarray::Array1;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helper: deterministic mock embedding
// ---------------------------------------------------------------------------
fn mock_embedding(seed: usize) -> Vec<f32> {
    (0..128).map(|j| ((seed * 7 + j * 13) % 100) as f32 / 100.0).collect()
}

/// Cosine similarity between two vectors
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    if norm_a <= 0.0 || norm_b <= 0.0 {
        return 0.0;
    }
    dot / (norm_a.sqrt() * norm_b.sqrt())
}

// ---------------------------------------------------------------------------
// Data definitions
// ---------------------------------------------------------------------------

struct TrialDef {
    nct_id: &'static str,
    title: &'static str,
    phase: &'static str,
    status: &'static str,
    sponsor: &'static str,
    target_enrollment: i64,
}

struct DrugDef {
    generic_name: &'static str,
    brand_name: &'static str,
    mechanism: &'static str,
    approval_year: i64,
}

struct ConditionDef {
    name: &'static str,
    icd10: &'static str,
    therapeutic_area: &'static str,
}

struct SiteDef {
    name: &'static str,
    city: &'static str,
    state: &'static str,
    capacity: i64,
    experience_score: f64,
    cost_per_patient: f64,
    diversity_index: f64,
}

struct PatientDef {
    id: usize,
    age: i64,
    sex: &'static str,
    primary_condition: &'static str,
    biomarker_pdl1: f64,
    biomarker_egfr: bool,
    biomarker_hba1c: f64,
    bmi: f64,
    comorbidities: &'static str,
}

fn trial_data() -> Vec<TrialDef> {
    vec![
        TrialDef { nct_id: "NCT05580562", title: "Pembrolizumab + Chemo in Advanced NSCLC", phase: "Phase III", status: "Recruiting", sponsor: "Merck", target_enrollment: 800 },
        TrialDef { nct_id: "NCT04538378", title: "Nivolumab Adjuvant Therapy in Melanoma", phase: "Phase III", status: "Recruiting", sponsor: "Bristol-Myers Squibb", target_enrollment: 600 },
        TrialDef { nct_id: "NCT03891940", title: "Atezolizumab for Triple-Negative Breast Cancer", phase: "Phase III", status: "Active", sponsor: "Roche", target_enrollment: 500 },
        TrialDef { nct_id: "NCT05275478", title: "Semaglutide for NASH", phase: "Phase III", status: "Recruiting", sponsor: "Novo Nordisk", target_enrollment: 1200 },
        TrialDef { nct_id: "NCT04379596", title: "Dupilumab in Moderate-to-Severe Atopic Dermatitis", phase: "Phase III", status: "Active", sponsor: "Regeneron", target_enrollment: 900 },
        TrialDef { nct_id: "NCT05236036", title: "Adalimumab Biosimilar in Rheumatoid Arthritis", phase: "Phase III", status: "Recruiting", sponsor: "Amgen", target_enrollment: 450 },
        TrialDef { nct_id: "NCT04564897", title: "Trastuzumab Deruxtecan in HER2+ Gastric Cancer", phase: "Phase II", status: "Recruiting", sponsor: "Daiichi Sankyo", target_enrollment: 350 },
        TrialDef { nct_id: "NCT05194995", title: "Ozempic for Cardiovascular Risk Reduction in T2D", phase: "Phase III", status: "Active", sponsor: "Novo Nordisk", target_enrollment: 1500 },
        TrialDef { nct_id: "NCT04487080", title: "Entrectinib in NTRK Fusion-Positive Solid Tumors", phase: "Phase II", status: "Recruiting", sponsor: "Roche", target_enrollment: 250 },
        TrialDef { nct_id: "NCT05063565", title: "Olaparib + Bevacizumab in Ovarian Cancer", phase: "Phase III", status: "Active", sponsor: "AstraZeneca", target_enrollment: 700 },
        TrialDef { nct_id: "NCT04516447", title: "Upadacitinib in Ulcerative Colitis", phase: "Phase III", status: "Recruiting", sponsor: "AbbVie", target_enrollment: 550 },
        TrialDef { nct_id: "NCT05007106", title: "Risankizumab in Crohn's Disease", phase: "Phase III", status: "Active", sponsor: "AbbVie", target_enrollment: 600 },
        TrialDef { nct_id: "NCT04988295", title: "Sacituzumab Govitecan in Urothelial Cancer", phase: "Phase III", status: "Recruiting", sponsor: "Gilead", target_enrollment: 400 },
        TrialDef { nct_id: "NCT05399654", title: "Tirzepatide for Obesity Management", phase: "Phase III", status: "Recruiting", sponsor: "Eli Lilly", target_enrollment: 2000 },
        TrialDef { nct_id: "NCT04136379", title: "Venetoclax + Azacitidine in AML", phase: "Phase III", status: "Active", sponsor: "AbbVie", target_enrollment: 500 },
        TrialDef { nct_id: "NCT05346796", title: "Lecanemab for Early Alzheimer's Disease", phase: "Phase III", status: "Recruiting", sponsor: "Eisai", target_enrollment: 1800 },
        TrialDef { nct_id: "NCT04622423", title: "Cabozantinib in Advanced Renal Cell Carcinoma", phase: "Phase III", status: "Active", sponsor: "Exelixis", target_enrollment: 650 },
        TrialDef { nct_id: "NCT05504044", title: "Datopotamab Deruxtecan in HR+/HER2- Breast Cancer", phase: "Phase III", status: "Recruiting", sponsor: "Daiichi Sankyo", target_enrollment: 800 },
        TrialDef { nct_id: "NCT04819100", title: "Bimekizumab in Psoriatic Arthritis", phase: "Phase III", status: "Active", sponsor: "UCB", target_enrollment: 400 },
        TrialDef { nct_id: "NCT05261399", title: "Sotrovimab Post-Exposure Prophylaxis", phase: "Phase III", status: "Completed", sponsor: "GSK", target_enrollment: 1000 },
        TrialDef { nct_id: "NCT04668300", title: "Pirtobrutinib in Relapsed CLL", phase: "Phase III", status: "Recruiting", sponsor: "Eli Lilly", target_enrollment: 550 },
        TrialDef { nct_id: "NCT05252390", title: "Elranatamab in Relapsed Multiple Myeloma", phase: "Phase II", status: "Recruiting", sponsor: "Pfizer", target_enrollment: 300 },
        TrialDef { nct_id: "NCT05183659", title: "Tucatinib + Trastuzumab in HER2+ Brain Mets", phase: "Phase III", status: "Active", sponsor: "Seagen", target_enrollment: 350 },
        TrialDef { nct_id: "NCT04720131", title: "Nemolizumab in Prurigo Nodularis", phase: "Phase III", status: "Recruiting", sponsor: "Galderma", target_enrollment: 400 },
        TrialDef { nct_id: "NCT05307705", title: "Rezafungin for Invasive Candidiasis", phase: "Phase III", status: "Active", sponsor: "Cidara", target_enrollment: 200 },
        TrialDef { nct_id: "NCT05150691", title: "Mirikizumab in Ulcerative Colitis Maintenance", phase: "Phase III", status: "Recruiting", sponsor: "Eli Lilly", target_enrollment: 700 },
        TrialDef { nct_id: "NCT04729439", title: "Ciltacabtagene Autoleucel in Multiple Myeloma", phase: "Phase III", status: "Active", sponsor: "J&J", target_enrollment: 400 },
        TrialDef { nct_id: "NCT05514054", title: "Iptacopan for Paroxysmal Nocturnal Hemoglobinuria", phase: "Phase III", status: "Recruiting", sponsor: "Novartis", target_enrollment: 300 },
        TrialDef { nct_id: "NCT05338970", title: "Capivasertib + Fulvestrant in HR+/HER2- mBC", phase: "Phase III", status: "Active", sponsor: "AstraZeneca", target_enrollment: 700 },
        TrialDef { nct_id: "NCT04872634", title: "Glofitamab in Relapsed DLBCL", phase: "Phase III", status: "Recruiting", sponsor: "Roche", target_enrollment: 350 },
        TrialDef { nct_id: "NCT05590234", title: "Donanemab for Mild Cognitive Impairment", phase: "Phase III", status: "Active", sponsor: "Eli Lilly", target_enrollment: 1500 },
    ]
}

fn drug_data() -> Vec<DrugDef> {
    vec![
        DrugDef { generic_name: "pembrolizumab", brand_name: "Keytruda", mechanism: "PD-1 inhibitor", approval_year: 2014 },
        DrugDef { generic_name: "nivolumab", brand_name: "Opdivo", mechanism: "PD-1 inhibitor", approval_year: 2014 },
        DrugDef { generic_name: "atezolizumab", brand_name: "Tecentriq", mechanism: "PD-L1 inhibitor", approval_year: 2016 },
        DrugDef { generic_name: "adalimumab", brand_name: "Humira", mechanism: "TNF-alpha inhibitor", approval_year: 2002 },
        DrugDef { generic_name: "semaglutide", brand_name: "Ozempic", mechanism: "GLP-1 receptor agonist", approval_year: 2017 },
        DrugDef { generic_name: "dupilumab", brand_name: "Dupixent", mechanism: "IL-4/IL-13 inhibitor", approval_year: 2017 },
        DrugDef { generic_name: "trastuzumab deruxtecan", brand_name: "Enhertu", mechanism: "HER2-directed ADC", approval_year: 2019 },
        DrugDef { generic_name: "entrectinib", brand_name: "Rozlytrek", mechanism: "NTRK/ROS1 inhibitor", approval_year: 2019 },
        DrugDef { generic_name: "olaparib", brand_name: "Lynparza", mechanism: "PARP inhibitor", approval_year: 2014 },
        DrugDef { generic_name: "bevacizumab", brand_name: "Avastin", mechanism: "VEGF inhibitor", approval_year: 2004 },
        DrugDef { generic_name: "upadacitinib", brand_name: "Rinvoq", mechanism: "JAK1 inhibitor", approval_year: 2019 },
        DrugDef { generic_name: "risankizumab", brand_name: "Skyrizi", mechanism: "IL-23 inhibitor", approval_year: 2019 },
        DrugDef { generic_name: "sacituzumab govitecan", brand_name: "Trodelvy", mechanism: "Trop-2-directed ADC", approval_year: 2020 },
        DrugDef { generic_name: "tirzepatide", brand_name: "Mounjaro", mechanism: "GIP/GLP-1 receptor agonist", approval_year: 2022 },
        DrugDef { generic_name: "venetoclax", brand_name: "Venclexta", mechanism: "BCL-2 inhibitor", approval_year: 2016 },
        DrugDef { generic_name: "azacitidine", brand_name: "Vidaza", mechanism: "DNA methyltransferase inhibitor", approval_year: 2004 },
        DrugDef { generic_name: "lecanemab", brand_name: "Leqembi", mechanism: "Anti-amyloid beta antibody", approval_year: 2023 },
        DrugDef { generic_name: "cabozantinib", brand_name: "Cabometyx", mechanism: "Multi-kinase inhibitor", approval_year: 2012 },
        DrugDef { generic_name: "bimekizumab", brand_name: "Bimzelx", mechanism: "IL-17A/IL-17F inhibitor", approval_year: 2023 },
        DrugDef { generic_name: "pirtobrutinib", brand_name: "Jaypirca", mechanism: "BTK inhibitor", approval_year: 2023 },
        DrugDef { generic_name: "elranatamab", brand_name: "Elrexfio", mechanism: "BCMA/CD3 bispecific", approval_year: 2023 },
        DrugDef { generic_name: "tucatinib", brand_name: "Tukysa", mechanism: "HER2 kinase inhibitor", approval_year: 2020 },
        DrugDef { generic_name: "nemolizumab", brand_name: "Nemluvio", mechanism: "IL-31 receptor antagonist", approval_year: 2024 },
        DrugDef { generic_name: "rezafungin", brand_name: "Rezzayo", mechanism: "Echinocandin antifungal", approval_year: 2023 },
        DrugDef { generic_name: "mirikizumab", brand_name: "Omvoh", mechanism: "IL-23p19 inhibitor", approval_year: 2023 },
        DrugDef { generic_name: "ciltacabtagene autoleucel", brand_name: "Carvykti", mechanism: "BCMA-directed CAR-T", approval_year: 2022 },
        DrugDef { generic_name: "iptacopan", brand_name: "Fabhalta", mechanism: "Complement factor B inhibitor", approval_year: 2023 },
        DrugDef { generic_name: "capivasertib", brand_name: "Truqap", mechanism: "AKT inhibitor", approval_year: 2023 },
        DrugDef { generic_name: "fulvestrant", brand_name: "Faslodex", mechanism: "Estrogen receptor antagonist", approval_year: 2002 },
        DrugDef { generic_name: "glofitamab", brand_name: "Columvi", mechanism: "CD20/CD3 bispecific", approval_year: 2023 },
        DrugDef { generic_name: "donanemab", brand_name: "Kisunla", mechanism: "Anti-amyloid beta antibody", approval_year: 2024 },
        DrugDef { generic_name: "datopotamab deruxtecan", brand_name: "Datroway", mechanism: "Trop-2-directed ADC", approval_year: 2024 },
        DrugDef { generic_name: "sotrovimab", brand_name: "Xevudy", mechanism: "Anti-SARS-CoV-2 mAb", approval_year: 2021 },
        DrugDef { generic_name: "trastuzumab", brand_name: "Herceptin", mechanism: "HER2 monoclonal antibody", approval_year: 1998 },
        DrugDef { generic_name: "doxorubicin", brand_name: "Adriamycin", mechanism: "Anthracycline topoisomerase inhibitor", approval_year: 1974 },
        DrugDef { generic_name: "cisplatin", brand_name: "Platinol", mechanism: "Platinum-based alkylating", approval_year: 1978 },
        DrugDef { generic_name: "carboplatin", brand_name: "Paraplatin", mechanism: "Platinum-based alkylating", approval_year: 1989 },
        DrugDef { generic_name: "paclitaxel", brand_name: "Taxol", mechanism: "Microtubule stabilizer", approval_year: 1992 },
        DrugDef { generic_name: "methotrexate", brand_name: "Trexall", mechanism: "Antifolate", approval_year: 1953 },
        DrugDef { generic_name: "rituximab", brand_name: "Rituxan", mechanism: "Anti-CD20 monoclonal antibody", approval_year: 1997 },
        DrugDef { generic_name: "ibrutinib", brand_name: "Imbruvica", mechanism: "BTK inhibitor", approval_year: 2013 },
    ]
}

fn condition_data() -> Vec<ConditionDef> {
    vec![
        ConditionDef { name: "Non-Small Cell Lung Cancer", icd10: "C34.90", therapeutic_area: "Oncology" },
        ConditionDef { name: "Melanoma", icd10: "C43.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Triple-Negative Breast Cancer", icd10: "C50.919", therapeutic_area: "Oncology" },
        ConditionDef { name: "Non-Alcoholic Steatohepatitis", icd10: "K75.81", therapeutic_area: "Hepatology" },
        ConditionDef { name: "Atopic Dermatitis", icd10: "L20.9", therapeutic_area: "Dermatology" },
        ConditionDef { name: "Rheumatoid Arthritis", icd10: "M06.9", therapeutic_area: "Rheumatology" },
        ConditionDef { name: "HER2-Positive Gastric Cancer", icd10: "C16.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Type 2 Diabetes", icd10: "E11", therapeutic_area: "Endocrinology" },
        ConditionDef { name: "NTRK Fusion-Positive Solid Tumors", icd10: "C80.1", therapeutic_area: "Oncology" },
        ConditionDef { name: "Ovarian Cancer", icd10: "C56.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Ulcerative Colitis", icd10: "K51.90", therapeutic_area: "Gastroenterology" },
        ConditionDef { name: "Crohn's Disease", icd10: "K50.90", therapeutic_area: "Gastroenterology" },
        ConditionDef { name: "Urothelial Cancer", icd10: "C67.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Obesity", icd10: "E66.01", therapeutic_area: "Endocrinology" },
        ConditionDef { name: "Acute Myeloid Leukemia", icd10: "C92.0", therapeutic_area: "Oncology" },
        ConditionDef { name: "Alzheimer's Disease", icd10: "G30.9", therapeutic_area: "Neurology" },
        ConditionDef { name: "Renal Cell Carcinoma", icd10: "C64.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "HR+/HER2- Breast Cancer", icd10: "C50.911", therapeutic_area: "Oncology" },
        ConditionDef { name: "Psoriatic Arthritis", icd10: "L40.50", therapeutic_area: "Rheumatology" },
        ConditionDef { name: "Chronic Lymphocytic Leukemia", icd10: "C91.10", therapeutic_area: "Oncology" },
        ConditionDef { name: "Multiple Myeloma", icd10: "C90.0", therapeutic_area: "Oncology" },
        ConditionDef { name: "HER2-Positive Brain Metastases", icd10: "C79.31", therapeutic_area: "Oncology" },
        ConditionDef { name: "Prurigo Nodularis", icd10: "L28.1", therapeutic_area: "Dermatology" },
        ConditionDef { name: "Invasive Candidiasis", icd10: "B37.7", therapeutic_area: "Infectious Disease" },
        ConditionDef { name: "Diffuse Large B-Cell Lymphoma", icd10: "C83.30", therapeutic_area: "Oncology" },
        ConditionDef { name: "Mild Cognitive Impairment", icd10: "G31.84", therapeutic_area: "Neurology" },
        ConditionDef { name: "Paroxysmal Nocturnal Hemoglobinuria", icd10: "D59.5", therapeutic_area: "Hematology" },
        ConditionDef { name: "Psoriasis", icd10: "L40.0", therapeutic_area: "Dermatology" },
        ConditionDef { name: "Hypertension", icd10: "I10", therapeutic_area: "Cardiology" },
        ConditionDef { name: "Hyperlipidemia", icd10: "E78.5", therapeutic_area: "Cardiology" },
        ConditionDef { name: "Asthma", icd10: "J45.909", therapeutic_area: "Pulmonology" },
        ConditionDef { name: "COPD", icd10: "J44.1", therapeutic_area: "Pulmonology" },
        ConditionDef { name: "Heart Failure", icd10: "I50.9", therapeutic_area: "Cardiology" },
        ConditionDef { name: "Chronic Kidney Disease", icd10: "N18.9", therapeutic_area: "Nephrology" },
        ConditionDef { name: "Major Depressive Disorder", icd10: "F33.1", therapeutic_area: "Psychiatry" },
        ConditionDef { name: "Systemic Lupus Erythematosus", icd10: "M32.9", therapeutic_area: "Rheumatology" },
        ConditionDef { name: "Hepatocellular Carcinoma", icd10: "C22.0", therapeutic_area: "Oncology" },
        ConditionDef { name: "Pancreatic Cancer", icd10: "C25.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Glioblastoma", icd10: "C71.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Colorectal Cancer", icd10: "C18.9", therapeutic_area: "Oncology" },
        ConditionDef { name: "Prostate Cancer", icd10: "C61", therapeutic_area: "Oncology" },
        ConditionDef { name: "Endometriosis", icd10: "N80.0", therapeutic_area: "Gynecology" },
        ConditionDef { name: "Migraine", icd10: "G43.909", therapeutic_area: "Neurology" },
        ConditionDef { name: "Osteoporosis", icd10: "M81.0", therapeutic_area: "Rheumatology" },
        ConditionDef { name: "Ankylosing Spondylitis", icd10: "M45.9", therapeutic_area: "Rheumatology" },
        ConditionDef { name: "Idiopathic Pulmonary Fibrosis", icd10: "J84.112", therapeutic_area: "Pulmonology" },
        ConditionDef { name: "Amyotrophic Lateral Sclerosis", icd10: "G12.21", therapeutic_area: "Neurology" },
        ConditionDef { name: "Sickle Cell Disease", icd10: "D57.1", therapeutic_area: "Hematology" },
        ConditionDef { name: "Hemophilia A", icd10: "D66", therapeutic_area: "Hematology" },
        ConditionDef { name: "Cystic Fibrosis", icd10: "E84.0", therapeutic_area: "Pulmonology" },
        ConditionDef { name: "HIV/AIDS", icd10: "B20", therapeutic_area: "Infectious Disease" },
    ]
}

fn site_data() -> Vec<SiteDef> {
    vec![
        SiteDef { name: "Mayo Clinic", city: "Rochester", state: "MN", capacity: 120, experience_score: 0.97, cost_per_patient: 42000.0, diversity_index: 0.62 },
        SiteDef { name: "MD Anderson Cancer Center", city: "Houston", state: "TX", capacity: 150, experience_score: 0.98, cost_per_patient: 48000.0, diversity_index: 0.78 },
        SiteDef { name: "Johns Hopkins Hospital", city: "Baltimore", state: "MD", capacity: 110, experience_score: 0.96, cost_per_patient: 45000.0, diversity_index: 0.74 },
        SiteDef { name: "Memorial Sloan Kettering", city: "New York", state: "NY", capacity: 140, experience_score: 0.97, cost_per_patient: 52000.0, diversity_index: 0.81 },
        SiteDef { name: "Dana-Farber Cancer Institute", city: "Boston", state: "MA", capacity: 100, experience_score: 0.95, cost_per_patient: 49000.0, diversity_index: 0.68 },
        SiteDef { name: "Cleveland Clinic", city: "Cleveland", state: "OH", capacity: 95, experience_score: 0.93, cost_per_patient: 38000.0, diversity_index: 0.65 },
        SiteDef { name: "UCSF Medical Center", city: "San Francisco", state: "CA", capacity: 105, experience_score: 0.94, cost_per_patient: 51000.0, diversity_index: 0.83 },
        SiteDef { name: "Massachusetts General Hospital", city: "Boston", state: "MA", capacity: 130, experience_score: 0.96, cost_per_patient: 47000.0, diversity_index: 0.72 },
        SiteDef { name: "Stanford Health Care", city: "Stanford", state: "CA", capacity: 90, experience_score: 0.94, cost_per_patient: 53000.0, diversity_index: 0.79 },
        SiteDef { name: "UCLA Medical Center", city: "Los Angeles", state: "CA", capacity: 115, experience_score: 0.93, cost_per_patient: 46000.0, diversity_index: 0.85 },
        SiteDef { name: "Duke University Hospital", city: "Durham", state: "NC", capacity: 85, experience_score: 0.92, cost_per_patient: 40000.0, diversity_index: 0.71 },
        SiteDef { name: "Cedars-Sinai Medical Center", city: "Los Angeles", state: "CA", capacity: 100, experience_score: 0.91, cost_per_patient: 50000.0, diversity_index: 0.82 },
        SiteDef { name: "Northwestern Memorial Hospital", city: "Chicago", state: "IL", capacity: 90, experience_score: 0.90, cost_per_patient: 43000.0, diversity_index: 0.76 },
        SiteDef { name: "Mount Sinai Hospital", city: "New York", state: "NY", capacity: 110, experience_score: 0.91, cost_per_patient: 48000.0, diversity_index: 0.84 },
        SiteDef { name: "Penn Medicine", city: "Philadelphia", state: "PA", capacity: 105, experience_score: 0.95, cost_per_patient: 44000.0, diversity_index: 0.73 },
        SiteDef { name: "Vanderbilt University Medical Center", city: "Nashville", state: "TN", capacity: 80, experience_score: 0.89, cost_per_patient: 37000.0, diversity_index: 0.67 },
        SiteDef { name: "University of Michigan Health", city: "Ann Arbor", state: "MI", capacity: 85, experience_score: 0.91, cost_per_patient: 39000.0, diversity_index: 0.64 },
        SiteDef { name: "NYU Langone Health", city: "New York", state: "NY", capacity: 100, experience_score: 0.92, cost_per_patient: 50000.0, diversity_index: 0.80 },
        SiteDef { name: "Emory University Hospital", city: "Atlanta", state: "GA", capacity: 75, experience_score: 0.88, cost_per_patient: 36000.0, diversity_index: 0.77 },
        SiteDef { name: "University of Colorado Hospital", city: "Aurora", state: "CO", capacity: 70, experience_score: 0.87, cost_per_patient: 35000.0, diversity_index: 0.69 },
        SiteDef { name: "Fred Hutchinson Cancer Center", city: "Seattle", state: "WA", capacity: 95, experience_score: 0.93, cost_per_patient: 47000.0, diversity_index: 0.75 },
        SiteDef { name: "University of Chicago Medicine", city: "Chicago", state: "IL", capacity: 80, experience_score: 0.90, cost_per_patient: 44000.0, diversity_index: 0.78 },
    ]
}

fn patient_data() -> Vec<PatientDef> {
    let conditions = [
        "Non-Small Cell Lung Cancer", "Melanoma", "Triple-Negative Breast Cancer",
        "Non-Alcoholic Steatohepatitis", "Atopic Dermatitis", "Rheumatoid Arthritis",
        "Type 2 Diabetes", "Ovarian Cancer", "Ulcerative Colitis", "Crohn's Disease",
        "Obesity", "Acute Myeloid Leukemia", "Alzheimer's Disease", "Renal Cell Carcinoma",
        "Chronic Lymphocytic Leukemia", "Multiple Myeloma", "Psoriatic Arthritis",
        "Diffuse Large B-Cell Lymphoma", "HR+/HER2- Breast Cancer", "Prostate Cancer",
    ];
    let sexes = ["Male", "Female"];
    let comorbidity_sets = [
        "Hypertension", "Hypertension, Hyperlipidemia", "Type 2 Diabetes",
        "Asthma", "COPD", "Heart Failure", "None", "Chronic Kidney Disease",
        "Hypertension, Type 2 Diabetes", "Hyperlipidemia, Obesity",
    ];

    (0..200).map(|i| {
        let age = 28 + ((i * 37 + 13) % 52) as i64;       // ages 28-79
        let sex = sexes[i % 2];
        let primary = conditions[i % conditions.len()];
        let pdl1 = ((i * 17 + 3) % 100) as f64;           // 0-99 %
        let egfr = (i % 7) == 0;                           // ~14% EGFR+
        let hba1c = 5.0 + ((i * 11 + 5) % 50) as f64 / 10.0; // 5.0-9.9
        let bmi = 18.5 + ((i * 23 + 7) % 250) as f64 / 10.0; // 18.5-43.4
        let comorbidity = comorbidity_sets[i % comorbidity_sets.len()];
        PatientDef {
            id: 1000 + i,
            age,
            sex,
            primary_condition: primary,
            biomarker_pdl1: pdl1,
            biomarker_egfr: egfr,
            biomarker_hba1c: hba1c,
            bmi,
            comorbidities: comorbidity,
        }
    }).collect()
}

// ---------------------------------------------------------------------------
// Trial-Drug and Trial-Condition mapping
// ---------------------------------------------------------------------------

/// Returns (trial_index, drug_indices) pairs
fn trial_drug_links() -> Vec<(usize, Vec<usize>)> {
    vec![
        (0, vec![0, 35, 36]),       // NCT05580562 -> pembrolizumab, cisplatin, carboplatin
        (1, vec![1]),               // NCT04538378 -> nivolumab
        (2, vec![2]),               // NCT03891940 -> atezolizumab
        (3, vec![4]),               // NCT05275478 -> semaglutide
        (4, vec![5]),               // NCT04379596 -> dupilumab
        (5, vec![3]),               // NCT05236036 -> adalimumab
        (6, vec![6]),               // NCT04564897 -> trastuzumab deruxtecan
        (7, vec![4]),               // NCT05194995 -> semaglutide
        (8, vec![7]),               // NCT04487080 -> entrectinib
        (9, vec![8, 9]),            // NCT05063565 -> olaparib, bevacizumab
        (10, vec![10]),             // NCT04516447 -> upadacitinib
        (11, vec![11]),             // NCT05007106 -> risankizumab
        (12, vec![12]),             // NCT04988295 -> sacituzumab govitecan
        (13, vec![13]),             // NCT05399654 -> tirzepatide
        (14, vec![14, 15]),         // NCT04136379 -> venetoclax, azacitidine
        (15, vec![16]),             // NCT05346796 -> lecanemab
        (16, vec![17]),             // NCT04622423 -> cabozantinib
        (17, vec![31]),             // NCT05504044 -> datopotamab deruxtecan
        (18, vec![18]),             // NCT04819100 -> bimekizumab
        (19, vec![32]),             // NCT05261399 -> sotrovimab
        (20, vec![19]),             // NCT04668300 -> pirtobrutinib
        (21, vec![20]),             // NCT05252390 -> elranatamab
        (22, vec![21, 33]),         // NCT05183659 -> tucatinib, trastuzumab
        (23, vec![22]),             // NCT04720131 -> nemolizumab
        (24, vec![23]),             // NCT05307705 -> rezafungin
        (25, vec![24]),             // NCT05150691 -> mirikizumab
        (26, vec![25]),             // NCT04729439 -> ciltacabtagene autoleucel
        (27, vec![26]),             // NCT05514054 -> iptacopan
        (28, vec![27, 28]),         // NCT05338970 -> capivasertib, fulvestrant
        (29, vec![29]),             // NCT04872634 -> glofitamab
        (30, vec![30]),             // NCT05590234 -> donanemab
    ]
}

/// Returns (trial_index, condition_index) pairs
fn trial_condition_links() -> Vec<(usize, usize)> {
    vec![
        (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6),
        (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12),
        (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18),
        (19, 19), (20, 20), (21, 20), (22, 21), (23, 22), (24, 23),
        (25, 10), (26, 20), (27, 26), (28, 17), (29, 24), (30, 25),
    ]
}

/// Drug interaction pairs (drugs that are commonly co-administered or have interactions)
fn drug_interaction_pairs() -> Vec<(usize, usize, &'static str)> {
    vec![
        (0, 35, "SYNERGY"),        // pembrolizumab + cisplatin
        (0, 36, "SYNERGY"),        // pembrolizumab + carboplatin
        (0, 37, "SYNERGY"),        // pembrolizumab + paclitaxel
        (1, 0, "SAME_CLASS"),      // nivolumab <-> pembrolizumab (PD-1)
        (2, 0, "SAME_CLASS"),      // atezolizumab <-> pembrolizumab (IO)
        (2, 1, "SAME_CLASS"),      // atezolizumab <-> nivolumab (IO)
        (8, 9, "COMBINATION"),     // olaparib + bevacizumab
        (14, 15, "COMBINATION"),   // venetoclax + azacitidine
        (21, 33, "COMBINATION"),   // tucatinib + trastuzumab
        (6, 33, "SAME_TARGET"),    // trastuzumab deruxtecan <-> trastuzumab (HER2)
        (27, 28, "COMBINATION"),   // capivasertib + fulvestrant
        (3, 38, "SAME_CLASS"),     // adalimumab <-> methotrexate (autoimmune)
        (10, 11, "SAME_CLASS"),    // upadacitinib <-> risankizumab (IBD)
        (19, 40, "SAME_CLASS"),    // pirtobrutinib <-> ibrutinib (BTK)
        (35, 36, "SAME_CLASS"),    // cisplatin <-> carboplatin (platinum)
        (16, 30, "SAME_TARGET"),   // lecanemab <-> donanemab (amyloid beta)
        (4, 13, "SAME_CLASS"),     // semaglutide <-> tirzepatide (GLP-1)
        (12, 31, "SAME_TARGET"),   // sacituzumab govitecan <-> datopotamab deruxtecan (Trop-2)
        (20, 25, "SAME_TARGET"),   // elranatamab <-> ciltacabtagene autoleucel (BCMA)
        (29, 39, "SAME_TARGET"),   // glofitamab <-> rituximab (CD20)
    ]
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    println!("================================================================");
    println!("   CLINICAL TRIALS INTELLIGENCE PLATFORM");
    println!("   Powered by Samyama Graph Database");
    println!("================================================================");
    println!();

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // ======================================================================
    // STEP 1: BUILD KNOWLEDGE GRAPH
    // ======================================================================
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ STEP 1: Building Clinical Trials Knowledge Graph            │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();

    // -- Trials --
    let trials = trial_data();
    let mut trial_ids = Vec::new();
    for (i, t) in trials.iter().enumerate() {
        let nid = store.create_node("Trial");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("nct_id", t.nct_id);
            node.set_property("title", t.title);
            node.set_property("phase", t.phase);
            node.set_property("status", t.status);
            node.set_property("sponsor", t.sponsor);
            node.set_property("target_enrollment", t.target_enrollment);
            node.set_property("seed", i as i64);
        }
        trial_ids.push(nid);
    }
    println!("  [+] Created {} Trial nodes", trial_ids.len());

    // -- Drugs --
    let drugs = drug_data();
    let mut drug_ids = Vec::new();
    for d in &drugs {
        let nid = store.create_node("Drug");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("generic_name", d.generic_name);
            node.set_property("brand_name", d.brand_name);
            node.set_property("mechanism", d.mechanism);
            node.set_property("approval_year", d.approval_year);
        }
        drug_ids.push(nid);
    }
    println!("  [+] Created {} Drug nodes", drug_ids.len());

    // -- Conditions --
    let conditions = condition_data();
    let mut condition_ids = Vec::new();
    for c in &conditions {
        let nid = store.create_node("Condition");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("name", c.name);
            node.set_property("icd10", c.icd10);
            node.set_property("therapeutic_area", c.therapeutic_area);
        }
        condition_ids.push(nid);
    }
    println!("  [+] Created {} Condition nodes", condition_ids.len());

    // -- Sites --
    let sites = site_data();
    let mut site_ids = Vec::new();
    for s in &sites {
        let nid = store.create_node("Site");
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("name", s.name);
            node.set_property("city", s.city);
            node.set_property("state", s.state);
            node.set_property("capacity", s.capacity);
            node.set_property("experience_score", s.experience_score);
            node.set_property("cost_per_patient", s.cost_per_patient);
            node.set_property("diversity_index", s.diversity_index);
        }
        site_ids.push(nid);
    }
    println!("  [+] Created {} Site nodes", site_ids.len());

    // -- Patients (with vector embeddings) --
    // Store embeddings as node properties for cosine similarity matching.
    // We use direct node.set_property for bulk loading and perform brute-force
    // cosine search at query time (see Step 2).
    let patients = patient_data();
    let mut patient_ids = Vec::new();
    for p in &patients {
        let nid = store.create_node("Patient");
        let emb = mock_embedding(p.id);
        if let Some(node) = store.get_node_mut(nid) {
            node.set_property("patient_id", format!("PT-{}", p.id));
            node.set_property("age", p.age);
            node.set_property("sex", p.sex);
            node.set_property("primary_condition", p.primary_condition);
            node.set_property("biomarker_pdl1", p.biomarker_pdl1);
            node.set_property("biomarker_egfr", p.biomarker_egfr);
            node.set_property("biomarker_hba1c", p.biomarker_hba1c);
            node.set_property("bmi", p.bmi);
            node.set_property("comorbidities", p.comorbidities);
            node.set_property("ehr_embedding", emb);
        }
        patient_ids.push(nid);
    }
    println!("  [+] Created {} Patient nodes with 128-dim EHR embeddings", patient_ids.len());

    // Set trial protocol embeddings
    for (i, &tid) in trial_ids.iter().enumerate() {
        let emb = mock_embedding(10000 + i);
        if let Some(node) = store.get_node_mut(tid) {
            node.set_property("protocol_embedding", emb);
        }
    }
    println!("  [+] Indexed {} Trial protocol embeddings", trial_ids.len());

    // -- Relationships --
    let mut edge_count = 0usize;

    // Trial -[TESTS]-> Drug
    for (ti, drug_indices) in trial_drug_links() {
        for &di in &drug_indices {
            if ti < trial_ids.len() && di < drug_ids.len() {
                store.create_edge(trial_ids[ti], drug_ids[di], EdgeType::new("TESTS")).unwrap();
                edge_count += 1;
            }
        }
    }

    // Trial -[STUDIES]-> Condition
    for (ti, ci) in trial_condition_links() {
        if ti < trial_ids.len() && ci < condition_ids.len() {
            store.create_edge(trial_ids[ti], condition_ids[ci], EdgeType::new("STUDIES")).unwrap();
            edge_count += 1;
        }
    }

    // Trial -[CONDUCTED_AT]-> Site (distribute trials across sites)
    for (i, &tid) in trial_ids.iter().enumerate() {
        let primary_site = i % sites.len();
        let secondary_site = (i * 3 + 7) % sites.len();
        store.create_edge(tid, site_ids[primary_site], EdgeType::new("CONDUCTED_AT")).unwrap();
        if primary_site != secondary_site {
            store.create_edge(tid, site_ids[secondary_site], EdgeType::new("CONDUCTED_AT")).unwrap();
            edge_count += 2;
        } else {
            edge_count += 1;
        }
    }

    // Patient -[HAS_CONDITION]-> Condition (map primary condition)
    let condition_names: Vec<&str> = conditions.iter().map(|c| c.name).collect();
    for (pi, p) in patients.iter().enumerate() {
        if let Some(ci) = condition_names.iter().position(|&n| n == p.primary_condition) {
            store.create_edge(patient_ids[pi], condition_ids[ci], EdgeType::new("HAS_CONDITION")).unwrap();
            edge_count += 1;
        }
    }

    // Drug -[INTERACTS_WITH]-> Drug
    for (d1, d2, interaction_type) in drug_interaction_pairs() {
        if d1 < drug_ids.len() && d2 < drug_ids.len() {
            let eid = store.create_edge(drug_ids[d1], drug_ids[d2], EdgeType::new("INTERACTS_WITH")).unwrap();
            if let Some(edge) = store.get_edge_mut(eid) {
                edge.set_property("interaction_type", interaction_type);
            }
            edge_count += 1;
        }
    }

    println!("  [+] Created {} relationships", edge_count);
    println!();

    // Summary table
    let total_nodes = trial_ids.len() + drug_ids.len() + condition_ids.len()
        + site_ids.len() + patient_ids.len();
    println!("  ┌─────────────────────┬────────┐");
    println!("  │ Entity              │  Count │");
    println!("  ├─────────────────────┼────────┤");
    println!("  │ Trials              │ {:>6} │", trial_ids.len());
    println!("  │ Drugs               │ {:>6} │", drug_ids.len());
    println!("  │ Conditions (ICD-10) │ {:>6} │", condition_ids.len());
    println!("  │ Research Sites      │ {:>6} │", site_ids.len());
    println!("  │ Patients            │ {:>6} │", patient_ids.len());
    println!("  ├─────────────────────┼────────┤");
    println!("  │ Total Nodes         │ {:>6} │", total_nodes);
    println!("  │ Total Edges         │ {:>6} │", edge_count);
    println!("  └─────────────────────┴────────┘");
    println!();

    // ======================================================================
    // STEP 2: PATIENT-TRIAL MATCHING (Vector Search)
    // ======================================================================
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ STEP 2: Patient-Trial Matching (Vector Search)              │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Scenario: Find the top 5 trials whose protocol embedding is");
    println!("  closest to a given patient's EHR embedding (cosine similarity).");
    println!();

    // Brute-force cosine similarity search across all Trial protocol embeddings.
    // In production, use store.vector_search() with an HNSW index for O(log N)
    // approximate nearest neighbor queries at scale.
    let sample_patients = [0usize, 42, 150];
    for &pi in &sample_patients {
        let p = &patients[pi];
        let patient_emb = mock_embedding(p.id);

        println!("  Patient PT-{}: {} y/o {}, {}", p.id, p.age, p.sex, p.primary_condition);
        println!("    PD-L1: {:.0}%  EGFR: {}  HbA1c: {:.1}  BMI: {:.1}",
            p.biomarker_pdl1,
            if p.biomarker_egfr { "+" } else { "-" },
            p.biomarker_hba1c,
            p.bmi);

        // Compute cosine similarity against all trial protocol embeddings
        let mut scored: Vec<(samyama::NodeId, f32)> = trial_ids.iter().filter_map(|&tid| {
            store.get_node(tid).and_then(|node| {
                node.get_property("protocol_embedding")
                    .and_then(|pv| pv.as_vector().cloned())
                    .map(|trial_emb| (tid, cosine_similarity(&patient_emb, &trial_emb)))
            })
        }).collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        scored.truncate(5);

        println!("    Top matches:");
        println!("    ┌────┬────────────────────────────────────────────────────────┬────────┐");
        println!("    │ #  │ Trial                                                  │  Score │");
        println!("    ├────┼────────────────────────────────────────────────────────┼────────┤");
        for (rank, (nid, score)) in scored.iter().enumerate() {
            if let Some(node) = store.get_node(*nid) {
                let nct = node.get_property("nct_id").map(|v| v.as_string().unwrap_or_default()).unwrap_or_default();
                let title = node.get_property("title").map(|v| v.as_string().unwrap_or_default()).unwrap_or_default();
                let display = format!("{} {}", nct, title);
                let truncated = if display.len() > 54 { format!("{}...", &display[..51]) } else { display.clone() };
                println!("    │ {:>2} │ {:<54} │ {:.4} │", rank + 1, truncated, score);
            }
        }
        println!("    └────┴────────────────────────────────────────────────────────┴────────┘");
        println!();
    }

    // ======================================================================
    // STEP 3: SITE SELECTION OPTIMIZATION (NSGA-II)
    // ======================================================================
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ STEP 3: Multi-Objective Site Selection (NSGA-II)            │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Objectives:");
    println!("    1. Maximize total enrollment capacity");
    println!("    2. Maximize average diversity index");
    println!("    3. Minimize total cost per patient");
    println!("  Constraint: Select exactly 8 out of {} sites.", sites.len());
    println!();

    // Capture site metrics for the optimizer
    let site_capacities: Vec<f64> = sites.iter().map(|s| s.capacity as f64).collect();
    let site_costs: Vec<f64> = sites.iter().map(|s| s.cost_per_patient).collect();
    let site_diversities: Vec<f64> = sites.iter().map(|s| s.diversity_index).collect();
    let n_sites = sites.len();

    struct SiteSelectionProblem {
        capacities: Vec<f64>,
        costs: Vec<f64>,
        diversities: Vec<f64>,
        n_sites: usize,
        target_selected: f64,
    }

    impl MultiObjectiveProblem for SiteSelectionProblem {
        fn dim(&self) -> usize { self.n_sites }
        fn num_objectives(&self) -> usize { 2 }

        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            (Array1::from_elem(self.n_sites, 0.0),
             Array1::from_elem(self.n_sites, 1.0))
        }

        fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
            let mut total_capacity = 0.0;
            let mut total_cost = 0.0;
            let mut total_diversity = 0.0;
            let mut selected = 0.0;

            for i in 0..self.n_sites {
                if x[i] > 0.5 {
                    total_capacity += self.capacities[i];
                    total_cost += self.costs[i];
                    total_diversity += self.diversities[i];
                    selected += 1.0;
                }
            }

            let avg_diversity = if selected > 0.0 { total_diversity / selected } else { 0.0 };

            // Minimize: -capacity (maximize capacity), cost - diversity_bonus
            // We combine capacity and diversity into one objective and cost into another
            vec![
                -total_capacity - avg_diversity * 1000.0,  // Minimize neg-capacity (maximize)
                total_cost,                                 // Minimize cost
            ]
        }

        fn penalties(&self, x: &Array1<f64>) -> Vec<f64> {
            let selected: f64 = x.iter().map(|&v| if v > 0.5 { 1.0 } else { 0.0 }).sum();
            if (selected - self.target_selected).abs() > 0.5 {
                vec![(selected - self.target_selected).powi(2) * 5000.0]
            } else {
                vec![0.0]
            }
        }
    }

    let problem = SiteSelectionProblem {
        capacities: site_capacities,
        costs: site_costs.clone(),
        diversities: site_diversities.clone(),
        n_sites,
        target_selected: 8.0,
    };

    let solver = NSGA2Solver::new(SolverConfig {
        population_size: 50,
        max_iterations: 100,
    });

    let result = solver.solve(&problem);
    println!("  Pareto front size: {} solutions", result.pareto_front.len());
    println!();

    // Pick the best solution from the Pareto front (lowest cost among feasible)
    let mut best_idx = 0;
    let mut best_score = f64::MAX;
    for (i, ind) in result.pareto_front.iter().enumerate() {
        let selected: f64 = ind.variables.iter().map(|&v| if v > 0.5 { 1.0 } else { 0.0 }).sum();
        if (selected - 8.0).abs() < 1.5 {
            // Combined score: balance capacity and cost
            let score = ind.fitness[0] + ind.fitness[1] / 100000.0;
            if score < best_score {
                best_score = score;
                best_idx = i;
            }
        }
    }

    if !result.pareto_front.is_empty() {
        let best = &result.pareto_front[best_idx];
        let selected_indices: Vec<usize> = best.variables.iter().enumerate()
            .filter(|(_, &v)| v > 0.5)
            .map(|(i, _)| i)
            .collect();

        let total_capacity: f64 = selected_indices.iter().map(|&i| sites[i].capacity as f64).sum();
        let total_cost: f64 = selected_indices.iter().map(|&i| sites[i].cost_per_patient).sum();
        let avg_diversity: f64 = if !selected_indices.is_empty() {
            selected_indices.iter().map(|&i| sites[i].diversity_index).sum::<f64>() / selected_indices.len() as f64
        } else { 0.0 };

        println!("  Optimal Site Portfolio:");
        println!("  ┌────┬──────────────────────────────────────┬──────────┬──────────┬───────────┐");
        println!("  │ #  │ Site                                 │ Capacity │ Cost/Pt  │ Diversity │");
        println!("  ├────┼──────────────────────────────────────┼──────────┼──────────┼───────────┤");
        for (rank, &si) in selected_indices.iter().enumerate() {
            let s = &sites[si];
            let name_disp = if s.name.len() > 36 { format!("{}...", &s.name[..33]) } else { s.name.to_string() };
            println!("  │ {:>2} │ {:<36} │ {:>8} │ ${:>7.0} │     {:.2} │",
                rank + 1, name_disp, s.capacity, s.cost_per_patient, s.diversity_index);
        }
        println!("  ├────┼──────────────────────────────────────┼──────────┼──────────┼───────────┤");
        println!("  │    │ TOTAL / AVERAGE                      │ {:>8.0} │ ${:>7.0} │     {:.2} │",
            total_capacity, total_cost, avg_diversity);
        println!("  └────┴──────────────────────────────────────┴──────────┴──────────┴───────────┘");
    } else {
        println!("  No feasible solution found in Pareto front.");
    }
    println!();

    // ======================================================================
    // STEP 4: DRUG INTERACTION NETWORK (Graph Algorithms)
    // ======================================================================
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ STEP 4: Drug Interaction Network Analysis                   │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();

    // 4a. PageRank on Drug nodes via INTERACTS_WITH edges
    println!("  4a. PageRank: Identifying Most Connected Drugs");
    println!("  ------------------------------------------------");
    let drug_view = build_view(&store, Some("Drug"), Some("INTERACTS_WITH"), None);
    let pr_scores = page_rank(&drug_view, PageRankConfig {
        damping_factor: 0.85,
        iterations: 30,
        tolerance: 0.0001,
    });

    // Map algo NodeId (u64) back to graph NodeId and sort
    let mut drug_ranks: Vec<(String, String, f64)> = Vec::new();
    for (&algo_id, &score) in &pr_scores {
        let graph_nid = samyama::NodeId::new(algo_id);
        if let Some(node) = store.get_node(graph_nid) {
            let generic = node.get_property("generic_name")
                .map(|v| v.as_string().unwrap_or_default()).unwrap_or_default();
            let brand = node.get_property("brand_name")
                .map(|v| v.as_string().unwrap_or_default()).unwrap_or_default();
            drug_ranks.push((generic.to_string(), brand.to_string(), score));
        }
    }
    drug_ranks.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());

    println!("  ┌────┬──────────────────────────┬──────────────────┬──────────┐");
    println!("  │ #  │ Generic Name             │ Brand Name       │ PageRank │");
    println!("  ├────┼──────────────────────────┼──────────────────┼──────────┤");
    for (i, (generic, brand, score)) in drug_ranks.iter().take(15).enumerate() {
        let gen_disp = if generic.len() > 24 { format!("{}...", &generic[..21]) } else { generic.clone() };
        let brand_disp = if brand.len() > 16 { format!("{}...", &brand[..13]) } else { brand.clone() };
        println!("  │ {:>2} │ {:<24} │ {:<16} │ {:>8.4} │", i + 1, gen_disp, brand_disp, score);
    }
    println!("  └────┴──────────────────────────┴──────────────────┴──────────┘");
    println!();

    // 4b. Weakly Connected Components on Drug interaction graph
    println!("  4b. Drug Clusters (Weakly Connected Components)");
    println!("  ------------------------------------------------");
    let wcc = weakly_connected_components(&drug_view);
    let mut cluster_sizes: Vec<(usize, Vec<String>)> = Vec::new();
    for (_comp_id, node_ids) in &wcc.components {
        let mut names: Vec<String> = Vec::new();
        for &algo_id in node_ids {
            let graph_nid = samyama::NodeId::new(algo_id);
            if let Some(node) = store.get_node(graph_nid) {
                let generic = node.get_property("generic_name")
                    .map(|v| v.as_string().unwrap_or_default()).unwrap_or_default();
                names.push(generic.to_string());
            }
        }
        names.sort();
        cluster_sizes.push((names.len(), names));
    }
    cluster_sizes.sort_by(|a, b| b.0.cmp(&a.0));

    println!("  Total drug clusters identified: {}", cluster_sizes.len());
    println!();
    for (i, (size, members)) in cluster_sizes.iter().enumerate().take(5) {
        println!("  Cluster {} ({} drugs):", i + 1, size);
        let display_members: Vec<&str> = members.iter().take(8).map(|s| s.as_str()).collect();
        let suffix = if members.len() > 8 { format!(", ... +{} more", members.len() - 8) } else { String::new() };
        println!("    {}{}", display_members.join(", "), suffix);
        println!();
    }

    // ======================================================================
    // STEP 5: COMPETITIVE LANDSCAPE ANALYSIS
    // ======================================================================
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ STEP 5: Competitive Landscape Analysis                      │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Identifying trials competing for the same patient population");
    println!("  (trials studying the same condition).");
    println!();

    // Build condition -> trials map via Cypher
    let mut condition_trials: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    let result = engine.execute("MATCH (t:Trial) RETURN t", &store).unwrap();
    for record in &result.records {
        if let Some(val) = record.get("t") {
            if let Some((nid, _node_ref)) = val.as_node() {
                let trial_nid = nid;
                let edges = store.get_outgoing_edges(trial_nid);
                for edge in &edges {
                    if edge.edge_type.as_str() == "STUDIES" {
                        if let Some(cond_node) = store.get_node(edge.target) {
                            let cond_name = cond_node.get_property("name")
                                .map(|v| v.as_string().unwrap_or_default())
                                .unwrap_or_default();
                            if let Some(trial_node) = store.get_node(trial_nid) {
                                let nct = trial_node.get_property("nct_id")
                                    .map(|v| v.as_string().unwrap_or_default())
                                    .unwrap_or_default();
                                let sponsor = trial_node.get_property("sponsor")
                                    .map(|v| v.as_string().unwrap_or_default())
                                    .unwrap_or_default();
                                let status = trial_node.get_property("status")
                                    .map(|v| v.as_string().unwrap_or_default())
                                    .unwrap_or_default();
                                condition_trials.entry(cond_name.to_string())
                                    .or_default()
                                    .push((nct.to_string(), sponsor.to_string(), status.to_string()));
                            }
                        }
                    }
                }
            }
        }
    }

    // Show conditions with multiple competing trials
    let mut competitive_conditions: Vec<(&String, &Vec<(String, String, String)>)> =
        condition_trials.iter()
            .filter(|(_, trials)| trials.len() >= 2)
            .collect();
    competitive_conditions.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    println!("  Conditions with Competing Trials:");
    println!("  ┌────────────────────────────────────────────┬─────────────────┬──────────────────────┬────────────┐");
    println!("  │ Condition                                  │ Trial ID        │ Sponsor              │ Status     │");
    println!("  ├────────────────────────────────────────────┼─────────────────┼──────────────────────┼────────────┤");
    for (cond, trials_list) in competitive_conditions.iter().take(6) {
        for (j, (nct, sponsor, status)) in trials_list.iter().enumerate() {
            let cond_disp = if j == 0 {
                if cond.len() > 42 { format!("{}...", &cond[..39]) } else { cond.to_string() }
            } else {
                String::new()
            };
            let sponsor_disp = if sponsor.len() > 20 { format!("{}...", &sponsor[..17]) } else { sponsor.clone() };
            println!("  │ {:<42} │ {:<15} │ {:<20} │ {:<10} │",
                cond_disp, nct, sponsor_disp, status);
        }
        println!("  ├────────────────────────────────────────────┼─────────────────┼──────────────────────┼────────────┤");
    }
    println!("  └────────────────────────────────────────────┴─────────────────┴──────────────────────┴────────────┘");
    println!();

    // ======================================================================
    // STEP 6: SUMMARY STATISTICS
    // ======================================================================
    println!("┌──────────────────────────────────────────────────────────────┐");
    println!("│ STEP 6: Platform Summary                                    │");
    println!("└──────────────────────────────────────────────────────────────┘");
    println!();

    // Therapeutic area distribution
    let mut ta_counts: HashMap<String, usize> = HashMap::new();
    for c in &conditions {
        *ta_counts.entry(c.therapeutic_area.to_string()).or_default() += 1;
    }
    let mut ta_sorted: Vec<_> = ta_counts.into_iter().collect();
    ta_sorted.sort_by(|a, b| b.1.cmp(&a.1));

    println!("  Condition Coverage by Therapeutic Area:");
    println!("  ┌──────────────────────────────┬────────┐");
    println!("  │ Therapeutic Area              │  Count │");
    println!("  ├──────────────────────────────┼────────┤");
    for (ta, count) in &ta_sorted {
        println!("  │ {:<28} │ {:>6} │", ta, count);
    }
    println!("  └──────────────────────────────┴────────┘");
    println!();

    // Trial phase distribution
    let mut phase_counts: HashMap<String, usize> = HashMap::new();
    for t in &trials {
        *phase_counts.entry(t.phase.to_string()).or_default() += 1;
    }
    let mut phase_sorted: Vec<_> = phase_counts.into_iter().collect();
    phase_sorted.sort_by(|a, b| a.0.cmp(&b.0));

    println!("  Trial Phase Distribution:");
    println!("  ┌────────────┬────────┐");
    println!("  │ Phase      │  Count │");
    println!("  ├────────────┼────────┤");
    for (phase, count) in &phase_sorted {
        println!("  │ {:<10} │ {:>6} │", phase, count);
    }
    println!("  └────────────┴────────┘");
    println!();

    // Sponsor portfolio
    let mut sponsor_counts: HashMap<String, usize> = HashMap::new();
    for t in &trials {
        *sponsor_counts.entry(t.sponsor.to_string()).or_default() += 1;
    }
    let mut sponsor_sorted: Vec<_> = sponsor_counts.into_iter().collect();
    sponsor_sorted.sort_by(|a, b| b.1.cmp(&a.1));

    println!("  Top Sponsors by Active Trials:");
    println!("  ┌──────────────────────────┬────────┐");
    println!("  │ Sponsor                  │ Trials │");
    println!("  ├──────────────────────────┼────────┤");
    for (sponsor, count) in sponsor_sorted.iter().take(10) {
        println!("  │ {:<24} │ {:>6} │", sponsor, count);
    }
    println!("  └──────────────────────────┴────────┘");
    println!();

    // Patient demographics summary
    let ages: Vec<i64> = patients.iter().map(|p| p.age).collect();
    let avg_age = ages.iter().sum::<i64>() as f64 / ages.len() as f64;
    let male_count = patients.iter().filter(|p| p.sex == "Male").count();
    let female_count = patients.iter().filter(|p| p.sex == "Female").count();
    let egfr_pos = patients.iter().filter(|p| p.biomarker_egfr).count();
    let pdl1_high = patients.iter().filter(|p| p.biomarker_pdl1 >= 50.0).count();
    let diabetic = patients.iter().filter(|p| p.biomarker_hba1c >= 6.5).count();
    let obese = patients.iter().filter(|p| p.bmi >= 30.0).count();

    println!("  Patient Cohort Demographics (n={}):", patients.len());
    println!("  ┌─────────────────────────────────┬──────────────┐");
    println!("  │ Metric                          │        Value │");
    println!("  ├─────────────────────────────────┼──────────────┤");
    println!("  │ Average Age                     │   {:.1} years │", avg_age);
    println!("  │ Male / Female                   │ {:>5} / {:<5} │", male_count, female_count);
    println!("  │ EGFR Mutation Positive           │ {:>5} ({:.0}%) │", egfr_pos, egfr_pos as f64 / patients.len() as f64 * 100.0);
    println!("  │ PD-L1 >= 50%                    │ {:>5} ({:.0}%) │", pdl1_high, pdl1_high as f64 / patients.len() as f64 * 100.0);
    println!("  │ HbA1c >= 6.5 (Diabetic Range)   │ {:>5} ({:.0}%) │", diabetic, diabetic as f64 / patients.len() as f64 * 100.0);
    println!("  │ BMI >= 30 (Obese)               │ {:>5} ({:.0}%) │", obese, obese as f64 / patients.len() as f64 * 100.0);
    println!("  └─────────────────────────────────┴──────────────┘");
    println!();

    // Cypher query verification
    println!("  Cypher Query Verification:");
    println!("  ┌──────────────────────────────────────────────────────────┐");

    let q1 = "MATCH (t:Trial) RETURN t";
    let r1 = engine.execute(q1, &store).unwrap();
    println!("  │ MATCH (t:Trial) RETURN t             -> {} results {:>6} │", r1.len(), "");

    let q2 = "MATCH (d:Drug) RETURN d";
    let r2 = engine.execute(q2, &store).unwrap();
    println!("  │ MATCH (d:Drug) RETURN d              -> {} results {:>6} │", r2.len(), "");

    let q3 = "MATCH (c:Condition) RETURN c";
    let r3 = engine.execute(q3, &store).unwrap();
    println!("  │ MATCH (c:Condition) RETURN c         -> {} results {:>5} │", r3.len(), "");

    let q4 = "MATCH (s:Site) RETURN s";
    let r4 = engine.execute(q4, &store).unwrap();
    println!("  │ MATCH (s:Site) RETURN s              -> {} results {:>6} │", r4.len(), "");

    let q5 = "MATCH (p:Patient) RETURN p LIMIT 5";
    let r5 = engine.execute(q5, &store).unwrap();
    println!("  │ MATCH (p:Patient) RETURN p LIMIT 5   -> {} results {:>7} │", r5.len(), "");

    println!("  └──────────────────────────────────────────────────────────┘");
    println!();

    // Final summary
    println!("================================================================");
    println!("   CLINICAL TRIALS INTELLIGENCE PLATFORM -- COMPLETE");
    println!("================================================================");
    println!();
    println!("  Knowledge Graph Schema:");
    println!();
    println!("    (Patient)-[:HAS_CONDITION]->(Condition)<-[:STUDIES]-(Trial)");
    println!("                                                  |        |");
    println!("                                                  |   [:TESTS]");
    println!("                                                  |        |");
    println!("                                                  v        v");
    println!("                                              (Site)    (Drug)");
    println!("                                                    [:INTERACTS_WITH]");
    println!("                                                     (Drug)<->(Drug)");
    println!();
    println!("  Capabilities Demonstrated:");
    println!("    [1] Knowledge graph construction (5 entity types, 4 relationship types)");
    println!("    [2] Patient-trial matching via 128-dim vector search (cosine similarity)");
    println!("    [3] Multi-objective site selection via NSGA-II optimization");
    println!("    [4] Drug network analysis via PageRank and WCC graph algorithms");
    println!("    [5] Competitive landscape analysis via graph traversal");
    println!("    [6] Enterprise reporting with summary statistics");
    println!();
}
