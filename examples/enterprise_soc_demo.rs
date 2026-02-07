//! Enterprise SOC Demo: APT Campaign Investigation
//!
//! A Security Operations Center analyst investigates an Advanced Persistent Threat
//! campaign across an enterprise network using Samyama's graph database capabilities.
//!
//! Demonstrates:
//! - Network topology modeling (50+ servers, 30+ users, firewalls)
//! - Threat intelligence ingestion with vector embeddings (20+ CVEs)
//! - APT lateral movement tracing through the graph
//! - Attack path analysis with Dijkstra shortest path
//! - Critical asset identification with PageRank
//! - Threat signature matching via vector search
//! - Network segmentation discovery with Weakly Connected Components
//!
//! Run: cargo run --example enterprise_soc_demo

use samyama::{GraphStore, Label, EdgeType, PersistenceManager, QueryEngine};
use samyama::persistence::tenant::{AgentConfig, AutoEmbedConfig, LLMProvider};
use samyama::vector::DistanceMetric;
use samyama::algo::{build_view, page_rank, weakly_connected_components, dijkstra, PageRankConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Helper: print a bordered section header
// ---------------------------------------------------------------------------
fn section(step: usize, title: &str) {
    println!();
    println!("  ┌────────────────────────────────────────────────────────────────────┐");
    println!("  │ STEP {}: {:<60}│", step, title);
    println!("  └────────────────────────────────────────────────────────────────────┘");
}

fn subsection(title: &str) {
    println!();
    println!("    --- {} ---", title);
}

fn main() {
    println!();
    println!("  ╔══════════════════════════════════════════════════════════════════════╗");
    println!("  ║   SAMYAMA GRAPH DATABASE -- Enterprise SOC Demo                      ║");
    println!("  ║   APT Campaign Investigation Across Enterprise Network               ║");
    println!("  ╚══════════════════════════════════════════════════════════════════════╝");
    println!();

    let overall_start = Instant::now();

    // ======================================================================
    // Setup: persistence, tenant, vector index
    // ======================================================================
    let temp_dir = tempfile::TempDir::new().unwrap();
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
    let tenant_id = "soc_ops";

    let agent_config = AgentConfig {
        enabled: true,
        provider: LLMProvider::Mock,
        model: "threat-analyst-v1".to_string(),
        api_key: Some("mock".to_string()),
        api_base_url: None,
        system_prompt: Some("You are a Tier 3 SOC analyst investigating APT campaigns.".to_string()),
        tools: vec![],
        policies: HashMap::from([
            ("Alert".to_string(), "Correlate with MITRE ATT&CK and recommend containment.".to_string()),
        ]),
    };

    let embed_config = AutoEmbedConfig {
        provider: LLMProvider::Mock,
        embedding_model: "threat-embed-v1".to_string(),
        api_key: Some("mock".to_string()),
        api_base_url: None,
        chunk_size: 256,
        chunk_overlap: 32,
        vector_dimension: 128,
        embedding_policies: HashMap::from([
            ("ThreatIntel".to_string(), vec!["description".to_string()]),
        ]),
    };

    persistence.tenants().create_tenant(
        tenant_id.to_string(),
        "Security Operations Center".to_string(),
        None,
    ).unwrap();
    persistence.tenants().update_agent_config(tenant_id, Some(agent_config)).unwrap();
    persistence.tenants().update_embed_config(tenant_id, Some(embed_config)).unwrap();

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    // Create vector index for threat signature matching (128-dim)
    store.create_vector_index("ThreatIntel", "signature_embedding", 128, DistanceMetric::Cosine).unwrap();

    // ======================================================================
    // STEP 1: Build Enterprise Network Topology
    // ======================================================================
    section(1, "Enterprise Network Topology Ingestion");

    // -- Servers across network zones -------
    subsection("Servers (50+ across 4 network zones)");

    struct ServerDef {
        name: &'static str,
        ip: &'static str,
        zone: &'static str,
        os: &'static str,
        role: &'static str,
        criticality: &'static str,
    }

    let servers: Vec<ServerDef> = vec![
        // DMZ (10.1.x.x) - 12 servers
        ServerDef { name: "dmz-web-01",      ip: "10.1.1.10",   zone: "DMZ",      os: "Ubuntu 22.04",   role: "Web Server",          criticality: "High" },
        ServerDef { name: "dmz-web-02",      ip: "10.1.1.11",   zone: "DMZ",      os: "Ubuntu 22.04",   role: "Web Server",          criticality: "High" },
        ServerDef { name: "dmz-web-03",      ip: "10.1.1.12",   zone: "DMZ",      os: "Ubuntu 22.04",   role: "Web Server",          criticality: "Medium" },
        ServerDef { name: "dmz-proxy-01",    ip: "10.1.2.10",   zone: "DMZ",      os: "CentOS 8",       role: "Reverse Proxy",       criticality: "High" },
        ServerDef { name: "dmz-proxy-02",    ip: "10.1.2.11",   zone: "DMZ",      os: "CentOS 8",       role: "Reverse Proxy",       criticality: "Medium" },
        ServerDef { name: "dmz-mail-01",     ip: "10.1.3.10",   zone: "DMZ",      os: "Ubuntu 22.04",   role: "Mail Gateway",        criticality: "High" },
        ServerDef { name: "dmz-dns-01",      ip: "10.1.4.10",   zone: "DMZ",      os: "Ubuntu 20.04",   role: "External DNS",        criticality: "High" },
        ServerDef { name: "dmz-vpn-01",      ip: "10.1.5.10",   zone: "DMZ",      os: "FortiOS 7.4",    role: "VPN Gateway",         criticality: "Critical" },
        ServerDef { name: "dmz-waf-01",      ip: "10.1.6.10",   zone: "DMZ",      os: "F5 BIG-IP",      role: "WAF",                 criticality: "Critical" },
        ServerDef { name: "dmz-ids-01",      ip: "10.1.7.10",   zone: "DMZ",      os: "SecurityOnion",  role: "IDS/IPS",             criticality: "Critical" },
        ServerDef { name: "dmz-lb-01",       ip: "10.1.8.10",   zone: "DMZ",      os: "HAProxy 2.8",    role: "Load Balancer",       criticality: "High" },
        ServerDef { name: "dmz-honeypot-01", ip: "10.1.9.10",   zone: "DMZ",      os: "Ubuntu 22.04",   role: "Honeypot",            criticality: "Low" },
        // Internal (172.16.x.x) - 18 servers
        ServerDef { name: "int-dc-01",       ip: "172.16.1.10", zone: "Internal", os: "Windows Server 2022", role: "Domain Controller",  criticality: "Critical" },
        ServerDef { name: "int-dc-02",       ip: "172.16.1.11", zone: "Internal", os: "Windows Server 2022", role: "Domain Controller",  criticality: "Critical" },
        ServerDef { name: "int-db-01",       ip: "172.16.2.10", zone: "Internal", os: "RHEL 9",         role: "Database (PostgreSQL)", criticality: "Critical" },
        ServerDef { name: "int-db-02",       ip: "172.16.2.11", zone: "Internal", os: "RHEL 9",         role: "Database (Oracle)",   criticality: "Critical" },
        ServerDef { name: "int-db-03",       ip: "172.16.2.12", zone: "Internal", os: "Ubuntu 22.04",   role: "Database (MongoDB)",  criticality: "High" },
        ServerDef { name: "int-app-01",      ip: "172.16.3.10", zone: "Internal", os: "RHEL 9",         role: "Application Server",  criticality: "High" },
        ServerDef { name: "int-app-02",      ip: "172.16.3.11", zone: "Internal", os: "RHEL 9",         role: "Application Server",  criticality: "High" },
        ServerDef { name: "int-app-03",      ip: "172.16.3.12", zone: "Internal", os: "RHEL 9",         role: "Application Server",  criticality: "Medium" },
        ServerDef { name: "int-file-01",     ip: "172.16.4.10", zone: "Internal", os: "Windows Server 2022", role: "File Server",        criticality: "High" },
        ServerDef { name: "int-file-02",     ip: "172.16.4.11", zone: "Internal", os: "Windows Server 2022", role: "File Server",        criticality: "Medium" },
        ServerDef { name: "int-ci-01",       ip: "172.16.5.10", zone: "Internal", os: "Ubuntu 22.04",   role: "CI/CD (Jenkins)",     criticality: "High" },
        ServerDef { name: "int-git-01",      ip: "172.16.5.11", zone: "Internal", os: "Ubuntu 22.04",   role: "Git Server",          criticality: "High" },
        ServerDef { name: "int-ldap-01",     ip: "172.16.6.10", zone: "Internal", os: "Ubuntu 22.04",   role: "LDAP Server",         criticality: "High" },
        ServerDef { name: "int-siem-01",     ip: "172.16.7.10", zone: "Internal", os: "RHEL 9",         role: "SIEM (Splunk)",       criticality: "Critical" },
        ServerDef { name: "int-ntp-01",      ip: "172.16.8.10", zone: "Internal", os: "Ubuntu 20.04",   role: "NTP Server",          criticality: "Medium" },
        ServerDef { name: "int-backup-01",   ip: "172.16.9.10", zone: "Internal", os: "RHEL 9",         role: "Backup Server",       criticality: "High" },
        ServerDef { name: "int-print-01",    ip: "172.16.10.10",zone: "Internal", os: "Windows Server 2019", role: "Print Server",       criticality: "Low" },
        ServerDef { name: "int-dhcp-01",     ip: "172.16.11.10",zone: "Internal", os: "Windows Server 2022", role: "DHCP Server",        criticality: "Medium" },
        // Cloud (10.100.x.x) - 10 servers
        ServerDef { name: "cld-k8s-master",  ip: "10.100.1.10", zone: "Cloud",    os: "Ubuntu 22.04",   role: "K8s Control Plane",   criticality: "Critical" },
        ServerDef { name: "cld-k8s-node-01", ip: "10.100.1.20", zone: "Cloud",    os: "Ubuntu 22.04",   role: "K8s Worker Node",     criticality: "High" },
        ServerDef { name: "cld-k8s-node-02", ip: "10.100.1.21", zone: "Cloud",    os: "Ubuntu 22.04",   role: "K8s Worker Node",     criticality: "High" },
        ServerDef { name: "cld-k8s-node-03", ip: "10.100.1.22", zone: "Cloud",    os: "Ubuntu 22.04",   role: "K8s Worker Node",     criticality: "Medium" },
        ServerDef { name: "cld-registry",    ip: "10.100.2.10", zone: "Cloud",    os: "Ubuntu 22.04",   role: "Container Registry",  criticality: "High" },
        ServerDef { name: "cld-api-gw",      ip: "10.100.3.10", zone: "Cloud",    os: "Amazon Linux 2", role: "API Gateway",         criticality: "High" },
        ServerDef { name: "cld-lambda-mgr",  ip: "10.100.4.10", zone: "Cloud",    os: "Amazon Linux 2", role: "Serverless Manager",  criticality: "Medium" },
        ServerDef { name: "cld-s3-proxy",    ip: "10.100.5.10", zone: "Cloud",    os: "Amazon Linux 2", role: "Storage Proxy",       criticality: "Medium" },
        ServerDef { name: "cld-rds-01",      ip: "10.100.6.10", zone: "Cloud",    os: "Amazon Linux 2", role: "Cloud Database (RDS)",criticality: "Critical" },
        ServerDef { name: "cld-elk-01",      ip: "10.100.7.10", zone: "Cloud",    os: "Ubuntu 22.04",   role: "ELK Stack",           criticality: "High" },
        // OT Network (192.168.x.x) - 10 servers
        ServerDef { name: "ot-scada-01",     ip: "192.168.1.10",zone: "OT",       os: "Windows 10 LTSC",role: "SCADA Controller",    criticality: "Critical" },
        ServerDef { name: "ot-scada-02",     ip: "192.168.1.11",zone: "OT",       os: "Windows 10 LTSC",role: "SCADA Controller",    criticality: "Critical" },
        ServerDef { name: "ot-plc-gw-01",    ip: "192.168.2.10",zone: "OT",       os: "RTOS",           role: "PLC Gateway",         criticality: "Critical" },
        ServerDef { name: "ot-hmi-01",       ip: "192.168.3.10",zone: "OT",       os: "Windows 10 LTSC",role: "HMI Server",          criticality: "High" },
        ServerDef { name: "ot-historian-01", ip: "192.168.4.10",zone: "OT",       os: "Windows Server 2019", role: "Data Historian",     criticality: "High" },
        ServerDef { name: "ot-eng-ws-01",    ip: "192.168.5.10",zone: "OT",       os: "Windows 10",     role: "Engineering WS",      criticality: "High" },
        ServerDef { name: "ot-eng-ws-02",    ip: "192.168.5.11",zone: "OT",       os: "Windows 10",     role: "Engineering WS",      criticality: "Medium" },
        ServerDef { name: "ot-fw-01",        ip: "192.168.0.1", zone: "OT",       os: "FortiOS 7.2",    role: "OT Firewall",         criticality: "Critical" },
        ServerDef { name: "ot-diode-01",     ip: "192.168.0.5", zone: "OT",       os: "Waterfall",      role: "Data Diode",          criticality: "Critical" },
        ServerDef { name: "ot-antivirus-01", ip: "192.168.6.10",zone: "OT",       os: "Windows Server 2019", role: "AV Management",      criticality: "Medium" },
    ];

    let mut server_ids: HashMap<String, samyama::NodeId> = HashMap::new();

    for s in &servers {
        let nid = store.create_node("Server");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("name", s.name);
            n.set_property("ip", s.ip);
            n.set_property("zone", s.zone);
            n.set_property("os", s.os);
            n.set_property("role", s.role);
            n.set_property("criticality", s.criticality);
        }
        let _ = store.add_label_to_node(tenant_id, nid, s.zone);
        server_ids.insert(s.name.to_string(), nid);
    }

    println!("    Servers ingested: {}", servers.len());
    println!("    ┌──────────────┬──────────┐");
    println!("    │ Network Zone │   Count  │");
    println!("    ├──────────────┼──────────┤");
    for zone in &["DMZ", "Internal", "Cloud", "OT"] {
        let count = servers.iter().filter(|s| s.zone == *zone).count();
        println!("    │ {:<12} │ {:>8} │", zone, count);
    }
    println!("    └──────────────┴──────────┘");

    // -- Firewalls & network edges -------
    subsection("Firewall Rules & Network Connectivity");

    // DMZ -> Internal gateway connections
    let fw_pairs: Vec<(&str, &str, &str)> = vec![
        ("dmz-proxy-01",  "int-app-01",    "ROUTES_TO"),
        ("dmz-proxy-01",  "int-app-02",    "ROUTES_TO"),
        ("dmz-proxy-02",  "int-app-03",    "ROUTES_TO"),
        ("dmz-mail-01",   "int-ldap-01",   "AUTHENTICATES_VIA"),
        ("dmz-vpn-01",    "int-dc-01",     "AUTHENTICATES_VIA"),
        ("dmz-waf-01",    "dmz-web-01",    "PROTECTS"),
        ("dmz-waf-01",    "dmz-web-02",    "PROTECTS"),
        ("dmz-waf-01",    "dmz-web-03",    "PROTECTS"),
        ("dmz-lb-01",     "dmz-web-01",    "BALANCES_TO"),
        ("dmz-lb-01",     "dmz-web-02",    "BALANCES_TO"),
        ("dmz-lb-01",     "dmz-web-03",    "BALANCES_TO"),
        ("dmz-ids-01",    "int-siem-01",   "REPORTS_TO"),
        // Internal connectivity
        ("int-app-01",    "int-db-01",     "CONNECTS_TO"),
        ("int-app-01",    "int-db-03",     "CONNECTS_TO"),
        ("int-app-02",    "int-db-01",     "CONNECTS_TO"),
        ("int-app-02",    "int-db-02",     "CONNECTS_TO"),
        ("int-app-03",    "int-db-02",     "CONNECTS_TO"),
        ("int-dc-01",     "int-dc-02",     "REPLICATES_TO"),
        ("int-dc-01",     "int-ldap-01",   "SYNCS_WITH"),
        ("int-ci-01",     "int-git-01",    "PULLS_FROM"),
        ("int-ci-01",     "cld-registry",  "PUSHES_TO"),
        ("int-file-01",   "int-backup-01", "BACKS_UP_TO"),
        ("int-file-02",   "int-backup-01", "BACKS_UP_TO"),
        // Cloud connectivity
        ("cld-api-gw",    "cld-k8s-node-01", "ROUTES_TO"),
        ("cld-api-gw",    "cld-k8s-node-02", "ROUTES_TO"),
        ("cld-k8s-master","cld-k8s-node-01", "MANAGES"),
        ("cld-k8s-master","cld-k8s-node-02", "MANAGES"),
        ("cld-k8s-master","cld-k8s-node-03", "MANAGES"),
        ("cld-k8s-node-01","cld-rds-01",     "CONNECTS_TO"),
        ("cld-k8s-node-02","cld-rds-01",     "CONNECTS_TO"),
        ("cld-elk-01",    "int-siem-01",     "FEEDS_TO"),
        // OT connectivity
        ("ot-fw-01",      "ot-scada-01",   "PROTECTS"),
        ("ot-fw-01",      "ot-scada-02",   "PROTECTS"),
        ("ot-scada-01",   "ot-plc-gw-01",  "CONTROLS"),
        ("ot-scada-02",   "ot-plc-gw-01",  "CONTROLS"),
        ("ot-hmi-01",     "ot-scada-01",   "DISPLAYS"),
        ("ot-historian-01","ot-scada-01",   "COLLECTS_FROM"),
        ("ot-historian-01","ot-scada-02",   "COLLECTS_FROM"),
        ("ot-diode-01",   "int-siem-01",   "ONE_WAY_TO"),
        ("ot-eng-ws-01",  "ot-scada-01",   "PROGRAMS"),
        ("ot-eng-ws-02",  "ot-scada-02",   "PROGRAMS"),
    ];

    let mut edge_count = 0;
    for (src, tgt, etype) in &fw_pairs {
        if let (Some(&s), Some(&t)) = (server_ids.get(*src), server_ids.get(*tgt)) {
            store.create_edge(s, t, EdgeType::new(*etype)).unwrap();
            edge_count += 1;
        }
    }
    println!("    Network edges created: {}", edge_count);

    // -- Users -------
    subsection("Enterprise Users (30+)");

    struct UserDef {
        name: &'static str,
        username: &'static str,
        department: &'static str,
        title: &'static str,
        access_level: &'static str,
        mfa_enabled: bool,
    }

    let users: Vec<UserDef> = vec![
        UserDef { name: "Sarah Chen",        username: "schen",       department: "IT Security",     title: "CISO",                      access_level: "Executive",  mfa_enabled: true },
        UserDef { name: "James Wilson",      username: "jwilson",     department: "IT Security",     title: "SOC Manager",               access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Priya Sharma",      username: "psharma",     department: "IT Security",     title: "Threat Analyst",            access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Mike Rodriguez",    username: "mrodriguez",  department: "IT Operations",   title: "Sysadmin Lead",             access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Emily Park",        username: "epark",       department: "IT Operations",   title: "Network Engineer",          access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "David Kim",         username: "dkim",        department: "IT Operations",   title: "Cloud Architect",           access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Lisa Zhang",        username: "lzhang",      department: "Engineering",     title: "VP Engineering",            access_level: "Privileged", mfa_enabled: true },
        UserDef { name: "Tom Anderson",      username: "tanderson",   department: "Engineering",     title: "Senior Developer",          access_level: "Developer",  mfa_enabled: true },
        UserDef { name: "Anika Patel",       username: "apatel",      department: "Engineering",     title: "DevOps Engineer",           access_level: "Developer",  mfa_enabled: true },
        UserDef { name: "Carlos Mendez",     username: "cmendez",     department: "Engineering",     title: "Backend Developer",         access_level: "Developer",  mfa_enabled: false },
        UserDef { name: "Rachel Green",      username: "rgreen",      department: "Engineering",     title: "Frontend Developer",        access_level: "Developer",  mfa_enabled: true },
        UserDef { name: "Hassan Ali",        username: "hali",        department: "Engineering",     title: "QA Engineer",               access_level: "Developer",  mfa_enabled: false },
        UserDef { name: "Jennifer Lee",      username: "jlee",        department: "Finance",         title: "CFO",                       access_level: "Executive",  mfa_enabled: true },
        UserDef { name: "Robert Brown",      username: "rbrown",      department: "Finance",         title: "Financial Analyst",         access_level: "Standard",   mfa_enabled: true },
        UserDef { name: "Maria Garcia",      username: "mgarcia",     department: "Finance",         title: "Controller",                access_level: "Privileged", mfa_enabled: true },
        UserDef { name: "Kevin O'Brien",     username: "kobrien",     department: "HR",              title: "HR Director",               access_level: "Privileged", mfa_enabled: true },
        UserDef { name: "Sophia Rossi",      username: "srossi",      department: "HR",              title: "Recruiter",                 access_level: "Standard",   mfa_enabled: false },
        UserDef { name: "Alex Thompson",     username: "athompson",   department: "Legal",           title: "General Counsel",           access_level: "Executive",  mfa_enabled: true },
        UserDef { name: "Diana Novak",       username: "dnovak",      department: "Marketing",       title: "Marketing Director",        access_level: "Standard",   mfa_enabled: true },
        UserDef { name: "Frank Miller",      username: "fmiller",     department: "Sales",           title: "VP Sales",                  access_level: "Standard",   mfa_enabled: true },
        UserDef { name: "Grace Liu",         username: "gliu",        department: "Sales",           title: "Account Executive",         access_level: "Standard",   mfa_enabled: false },
        UserDef { name: "Ivan Petrov",       username: "ipetrov",     department: "IT Operations",   title: "Database Administrator",    access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Julia Santos",      username: "jsantos",     department: "IT Operations",   title: "Help Desk Technician",      access_level: "Standard",   mfa_enabled: false },
        UserDef { name: "Nathan Cooper",     username: "ncooper",     department: "Facilities",      title: "OT Engineer",               access_level: "OT_Admin",   mfa_enabled: true },
        UserDef { name: "Olivia Wright",     username: "owright",     department: "Facilities",      title: "OT Technician",             access_level: "OT_Operator",mfa_enabled: false },
        UserDef { name: "Paul Wagner",       username: "pwagner",     department: "Research",        title: "Data Scientist",            access_level: "Developer",  mfa_enabled: true },
        UserDef { name: "Quinn Murphy",      username: "qmurphy",     department: "Research",        title: "ML Engineer",               access_level: "Developer",  mfa_enabled: true },
        UserDef { name: "Rita Yamamoto",     username: "ryamamoto",   department: "Compliance",      title: "Compliance Officer",        access_level: "Privileged", mfa_enabled: true },
        UserDef { name: "Steve Jackson",     username: "sjackson",    department: "Executive",       title: "CEO",                       access_level: "Executive",  mfa_enabled: true },
        UserDef { name: "Tanya Volkov",      username: "tvolkov",     department: "IT Security",     title: "Incident Responder",        access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Umar Farouk",       username: "ufarouk",    department: "Engineering",     title: "Site Reliability Engineer",  access_level: "Admin",      mfa_enabled: true },
        UserDef { name: "Wendy Chu",         username: "wchu",        department: "IT Security",     title: "Vulnerability Analyst",     access_level: "Admin",      mfa_enabled: true },
    ];

    let mut user_ids: HashMap<String, samyama::NodeId> = HashMap::new();

    for u in &users {
        let nid = store.create_node("User");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("name", u.name);
            n.set_property("username", u.username);
            n.set_property("department", u.department);
            n.set_property("title", u.title);
            n.set_property("access_level", u.access_level);
            n.set_property("mfa_enabled", u.mfa_enabled);
        }
        user_ids.insert(u.username.to_string(), nid);
    }

    // User -> Server access relationships
    let access_map: Vec<(&str, &str)> = vec![
        ("schen",    "int-siem-01"),   ("schen",    "int-dc-01"),
        ("jwilson",  "int-siem-01"),   ("jwilson",  "dmz-ids-01"),
        ("psharma",  "int-siem-01"),   ("psharma",  "dmz-ids-01"),
        ("mrodriguez","int-dc-01"),    ("mrodriguez","int-dc-02"),     ("mrodriguez","int-file-01"),
        ("epark",    "dmz-proxy-01"),  ("epark",    "dmz-lb-01"),     ("epark",    "dmz-waf-01"),
        ("dkim",     "cld-k8s-master"),("dkim",     "cld-api-gw"),    ("dkim",     "cld-rds-01"),
        ("tanderson","int-ci-01"),     ("tanderson","int-git-01"),
        ("apatel",   "int-ci-01"),     ("apatel",   "cld-registry"),  ("apatel",   "cld-k8s-master"),
        ("cmendez",  "int-git-01"),    ("cmendez",  "int-app-01"),
        ("ipetrov",  "int-db-01"),     ("ipetrov",  "int-db-02"),     ("ipetrov",  "int-db-03"),
        ("ncooper",  "ot-scada-01"),   ("ncooper",  "ot-scada-02"),   ("ncooper",  "ot-eng-ws-01"),
        ("owright",  "ot-hmi-01"),     ("owright",  "ot-eng-ws-02"),
        ("ufarouk",  "cld-k8s-master"),("ufarouk",  "cld-elk-01"),    ("ufarouk",  "int-siem-01"),
        ("wchu",     "dmz-honeypot-01"),("wchu",    "int-siem-01"),
    ];

    let mut access_edge_count = 0;
    for (user, server) in &access_map {
        if let (Some(&uid), Some(&sid)) = (user_ids.get(*user), server_ids.get(*server)) {
            store.create_edge(uid, sid, EdgeType::new("HAS_ACCESS")).unwrap();
            access_edge_count += 1;
        }
    }

    println!("    Users ingested: {}", users.len());
    println!("    Access relationships: {}", access_edge_count);
    println!("    ┌──────────────────┬──────────┐");
    println!("    │ Department       │   Count  │");
    println!("    ├──────────────────┼──────────┤");
    let mut dept_counts: HashMap<&str, usize> = HashMap::new();
    for u in &users { *dept_counts.entry(u.department).or_insert(0) += 1; }
    let mut dept_sorted: Vec<_> = dept_counts.iter().collect();
    dept_sorted.sort_by(|a, b| b.1.cmp(a.1));
    for (dept, count) in &dept_sorted {
        println!("    │ {:<16} │ {:>8} │", dept, count);
    }
    println!("    └──────────────────┴──────────┘");

    // ======================================================================
    // STEP 2: Threat Intelligence Ingestion
    // ======================================================================
    section(2, "Threat Intelligence Ingestion");

    struct CveDef {
        id: &'static str,
        description: &'static str,
        cvss: f64,
        family: &'static str,
        affected: &'static str,
    }

    let cves: Vec<CveDef> = vec![
        CveDef { id: "CVE-2021-44228", description: "Log4Shell RCE in Apache Log4j",                  cvss: 10.0, family: "Log4Shell",     affected: "Java Applications" },
        CveDef { id: "CVE-2021-45046", description: "Log4j incomplete fix for CVE-2021-44228",         cvss: 9.0,  family: "Log4Shell",     affected: "Java Applications" },
        CveDef { id: "CVE-2023-23397", description: "Microsoft Outlook EoP via NTLM relay",            cvss: 9.8,  family: "Outlook",       affected: "Microsoft Outlook" },
        CveDef { id: "CVE-2024-3400",  description: "PAN-OS GlobalProtect RCE pre-auth",               cvss: 10.0, family: "PAN-OS",        affected: "Palo Alto Networks" },
        CveDef { id: "CVE-2023-34362", description: "MOVEit Transfer SQL injection",                   cvss: 9.8,  family: "Cl0p",          affected: "MOVEit Transfer" },
        CveDef { id: "CVE-2023-27997", description: "FortiOS SSL-VPN heap overflow",                   cvss: 9.8,  family: "FortiGate",     affected: "FortiOS" },
        CveDef { id: "CVE-2024-21887", description: "Ivanti Connect Secure command injection",          cvss: 9.1,  family: "Ivanti",        affected: "Ivanti VPN" },
        CveDef { id: "CVE-2023-46805", description: "Ivanti Connect Secure auth bypass",                cvss: 8.2,  family: "Ivanti",        affected: "Ivanti VPN" },
        CveDef { id: "CVE-2021-34527", description: "PrintNightmare Windows Print Spooler RCE",         cvss: 8.8,  family: "PrintNightmare",affected: "Windows Print Spooler" },
        CveDef { id: "CVE-2021-27065", description: "Microsoft Exchange ProxyLogon RCE",                cvss: 9.8,  family: "ProxyLogon",    affected: "Exchange Server" },
        CveDef { id: "CVE-2021-26855", description: "Exchange Server SSRF via ProxyLogon",              cvss: 9.8,  family: "ProxyLogon",    affected: "Exchange Server" },
        CveDef { id: "CVE-2022-30190", description: "Follina MSDT code execution via Office",           cvss: 7.8,  family: "Follina",       affected: "Microsoft Office" },
        CveDef { id: "CVE-2023-36884", description: "Office HTML RCE via Storm-0978",                   cvss: 8.8,  family: "Storm-0978",    affected: "Microsoft Office" },
        CveDef { id: "CVE-2024-1709",  description: "ScreenConnect auth bypass",                        cvss: 10.0, family: "ScreenConnect",  affected: "ConnectWise" },
        CveDef { id: "CVE-2024-1708",  description: "ScreenConnect path traversal RCE",                 cvss: 8.4,  family: "ScreenConnect",  affected: "ConnectWise" },
        CveDef { id: "CVE-2023-44487", description: "HTTP/2 Rapid Reset DDoS attack",                   cvss: 7.5,  family: "HTTP2",         affected: "HTTP/2 Servers" },
        CveDef { id: "CVE-2022-41040", description: "Exchange ProxyNotShell SSRF",                      cvss: 8.8,  family: "ProxyNotShell", affected: "Exchange Server" },
        CveDef { id: "CVE-2023-20198", description: "Cisco IOS XE Web UI privilege escalation",          cvss: 10.0, family: "Cisco",         affected: "Cisco IOS XE" },
        CveDef { id: "CVE-2024-47575", description: "FortiManager missing authentication RCE",           cvss: 9.8,  family: "FortiManager",  affected: "FortiManager" },
        CveDef { id: "CVE-2023-4966",  description: "Citrix Bleed session hijack",                      cvss: 9.4,  family: "CitrixBleed",   affected: "Citrix NetScaler" },
        CveDef { id: "CVE-2024-0012",  description: "PAN-OS management interface auth bypass",           cvss: 9.8,  family: "PAN-OS",        affected: "Palo Alto Networks" },
        CveDef { id: "CVE-2021-40444", description: "MSHTML RCE via ActiveX in Office docs",             cvss: 8.8,  family: "MSHTML",        affected: "Microsoft Office" },
    ];

    let mut cve_ids: HashMap<String, samyama::NodeId> = HashMap::new();

    for (i, cve) in cves.iter().enumerate() {
        let nid = store.create_node("ThreatIntel");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("cve_id", cve.id);
            n.set_property("description", cve.description);
            n.set_property("cvss_score", cve.cvss);
            n.set_property("malware_family", cve.family);
            n.set_property("affected_product", cve.affected);
            // Deterministic 128-dim embedding for vector search
            let embedding: Vec<f32> = (0..128)
                .map(|j| ((i * 7 + j * 13) % 100) as f32 / 100.0)
                .collect();
            n.set_property("signature_embedding", embedding);
        }
        cve_ids.insert(cve.id.to_string(), nid);
    }

    println!("    CVEs ingested: {}", cves.len());
    println!("    ┌──────────────────┬────────┬───────────────────────────────────────────┐");
    println!("    │ CVE ID           │  CVSS  │ Description                               │");
    println!("    ├──────────────────┼────────┼───────────────────────────────────────────┤");
    for cve in cves.iter().take(10) {
        let desc_trunc = if cve.description.len() > 41 {
            format!("{}...", &cve.description[..38])
        } else {
            format!("{:<41}", cve.description)
        };
        println!("    │ {:<16} │ {:>5.1}  │ {} │", cve.id, cve.cvss, desc_trunc);
    }
    println!("    │ ... ({} more)     │        │                                           │", cves.len() - 10);
    println!("    └──────────────────┴────────┴───────────────────────────────────────────┘");

    // -- MITRE ATT&CK Techniques -------
    subsection("MITRE ATT&CK Techniques (15+)");

    struct MitreDef {
        technique_id: &'static str,
        name: &'static str,
        tactic: &'static str,
    }

    let techniques: Vec<MitreDef> = vec![
        MitreDef { technique_id: "T1566",     name: "Phishing",                         tactic: "Initial Access" },
        MitreDef { technique_id: "T1566.001", name: "Spearphishing Attachment",          tactic: "Initial Access" },
        MitreDef { technique_id: "T1566.002", name: "Spearphishing Link",                tactic: "Initial Access" },
        MitreDef { technique_id: "T1059",     name: "Command and Scripting Interpreter", tactic: "Execution" },
        MitreDef { technique_id: "T1059.001", name: "PowerShell",                        tactic: "Execution" },
        MitreDef { technique_id: "T1053",     name: "Scheduled Task/Job",                tactic: "Persistence" },
        MitreDef { technique_id: "T1547",     name: "Boot or Logon Autostart",           tactic: "Persistence" },
        MitreDef { technique_id: "T1078",     name: "Valid Accounts",                    tactic: "Defense Evasion" },
        MitreDef { technique_id: "T1003",     name: "OS Credential Dumping",             tactic: "Credential Access" },
        MitreDef { technique_id: "T1003.001", name: "LSASS Memory",                      tactic: "Credential Access" },
        MitreDef { technique_id: "T1021",     name: "Remote Services",                   tactic: "Lateral Movement" },
        MitreDef { technique_id: "T1021.001", name: "Remote Desktop Protocol",            tactic: "Lateral Movement" },
        MitreDef { technique_id: "T1021.002", name: "SMB/Windows Admin Shares",           tactic: "Lateral Movement" },
        MitreDef { technique_id: "T1071",     name: "Application Layer Protocol",         tactic: "Command and Control" },
        MitreDef { technique_id: "T1486",     name: "Data Encrypted for Impact",          tactic: "Impact" },
        MitreDef { technique_id: "T1048",     name: "Exfiltration Over Alternative Protocol", tactic: "Exfiltration" },
        MitreDef { technique_id: "T1570",     name: "Lateral Tool Transfer",              tactic: "Lateral Movement" },
    ];

    let mut technique_ids: HashMap<String, samyama::NodeId> = HashMap::new();

    for t in &techniques {
        let nid = store.create_node("MitreTechnique");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("technique_id", t.technique_id);
            n.set_property("name", t.name);
            n.set_property("tactic", t.tactic);
        }
        technique_ids.insert(t.technique_id.to_string(), nid);
    }

    // Link CVEs to MITRE techniques
    let cve_technique_links: Vec<(&str, &str)> = vec![
        ("CVE-2021-44228", "T1059"),
        ("CVE-2023-23397", "T1566"),
        ("CVE-2024-3400",  "T1059"),
        ("CVE-2023-34362", "T1059"),
        ("CVE-2023-27997", "T1021"),
        ("CVE-2024-21887", "T1059"),
        ("CVE-2021-34527", "T1547"),
        ("CVE-2021-27065", "T1059"),
        ("CVE-2022-30190", "T1566.001"),
        ("CVE-2023-36884", "T1566.001"),
        ("CVE-2024-1709",  "T1078"),
        ("CVE-2023-20198", "T1078"),
        ("CVE-2021-40444", "T1566.002"),
    ];

    for (cve, tech) in &cve_technique_links {
        if let (Some(&cid), Some(&tid)) = (cve_ids.get(*cve), technique_ids.get(*tech)) {
            store.create_edge(cid, tid, EdgeType::new("USES_TECHNIQUE")).unwrap();
        }
    }

    println!("    MITRE techniques ingested: {}", techniques.len());
    println!("    CVE-to-technique links: {}", cve_technique_links.len());

    // -- Malware families -------
    subsection("Known Malware Families");

    let malware_families: Vec<(&str, &str, &str)> = vec![
        ("Emotet",        "Trojan/Loader",   "Banking trojan turned malware distribution platform"),
        ("Cobalt Strike",  "C2 Framework",    "Commercial red team tool commonly abused by APT groups"),
        ("BlackCat/ALPHV", "Ransomware",      "Rust-based ransomware-as-a-service operation"),
        ("LockBit",        "Ransomware",      "Prolific ransomware gang with data leak site"),
        ("QakBot",         "Trojan/Loader",   "Banking trojan that delivers ransomware payloads"),
        ("Raspberry Robin","Worm",            "USB worm that spreads via removable drives"),
    ];

    let mut malware_ids: HashMap<String, samyama::NodeId> = HashMap::new();

    for (name, mtype, desc) in &malware_families {
        let nid = store.create_node("Malware");
        if let Some(n) = store.get_node_mut(nid) {
            n.set_property("name", *name);
            n.set_property("malware_type", *mtype);
            n.set_property("description", *desc);
        }
        malware_ids.insert(name.to_string(), nid);
    }

    // Link malware to techniques
    let malware_technique_links: Vec<(&str, &str)> = vec![
        ("Emotet",         "T1566.001"),
        ("Emotet",         "T1059.001"),
        ("Cobalt Strike",  "T1071"),
        ("Cobalt Strike",  "T1021.002"),
        ("Cobalt Strike",  "T1003.001"),
        ("BlackCat/ALPHV", "T1486"),
        ("LockBit",        "T1486"),
        ("LockBit",        "T1048"),
        ("QakBot",         "T1566.001"),
        ("Raspberry Robin","T1570"),
    ];

    for (mal, tech) in &malware_technique_links {
        if let (Some(&mid), Some(&tid)) = (malware_ids.get(*mal), technique_ids.get(*tech)) {
            store.create_edge(mid, tid, EdgeType::new("USES_TECHNIQUE")).unwrap();
        }
    }

    println!("    Malware families: {}", malware_families.len());
    for (name, mtype, _) in &malware_families {
        println!("      - {} ({})", name, mtype);
    }

    // ======================================================================
    // STEP 3: Simulate APT Lateral Movement Campaign
    // ======================================================================
    section(3, "APT Campaign Simulation (Lateral Movement)");

    println!("    SCENARIO: APT group 'PHANTOM BEAR' compromises the network via");
    println!("    spearphishing, then moves laterally toward the SCADA systems.");
    println!();

    // The attack chain:
    // 1. Spearphish cmendez (no MFA) via Emotet
    // 2. cmendez workstation -> int-git-01 (has access)
    // 3. int-git-01 -> int-ci-01 (PULLS_FROM edge)
    // 4. int-ci-01 -> cld-registry (PUSHES_TO edge) -- pivot to cloud
    // 5. cld-registry -> cld-k8s-master (supply chain implant)
    // 6. int-ci-01 -> int-app-01 (deploy backdoor)
    // 7. int-app-01 -> int-db-01 (data exfil)
    // 8. int-dc-01 (credential dump via LSASS) -- gained via int-app-01

    struct AttackStep {
        step: usize,
        source_name: &'static str,
        target_name: &'static str,
        technique: &'static str,
        description: &'static str,
    }

    let attack_chain: Vec<AttackStep> = vec![
        AttackStep { step: 1, source_name: "cmendez",      target_name: "int-git-01",     technique: "T1566.001", description: "Spearphishing email with malicious attachment (Emotet dropper)" },
        AttackStep { step: 2, source_name: "int-git-01",   target_name: "int-ci-01",      technique: "T1021",     description: "Lateral movement to CI server via stolen SSH keys" },
        AttackStep { step: 3, source_name: "int-ci-01",    target_name: "cld-registry",   technique: "T1570",     description: "Poisoned container image pushed to registry" },
        AttackStep { step: 4, source_name: "int-ci-01",    target_name: "int-app-01",     technique: "T1059.001", description: "PowerShell backdoor deployed to application server" },
        AttackStep { step: 5, source_name: "int-app-01",   target_name: "int-db-01",      technique: "T1048",     description: "Database credentials exfiltrated via DNS tunneling" },
        AttackStep { step: 6, source_name: "int-app-01",   target_name: "int-dc-01",      technique: "T1003.001", description: "LSASS memory dump on domain controller (Cobalt Strike)" },
        AttackStep { step: 7, source_name: "int-dc-01",    target_name: "int-file-01",    technique: "T1021.002", description: "SMB lateral movement to file server using DA credentials" },
        AttackStep { step: 8, source_name: "int-dc-01",    target_name: "ot-fw-01",       technique: "T1078",     description: "Attempting OT network breach using stolen admin creds" },
    ];

    // Create attack event nodes and edges
    let mut attack_event_ids: Vec<samyama::NodeId> = Vec::new();

    for a in &attack_chain {
        let event_id = store.create_node("AttackEvent");
        if let Some(n) = store.get_node_mut(event_id) {
            n.set_property("step", a.step as i64);
            n.set_property("technique", a.technique);
            n.set_property("description", a.description);
            n.set_property("timestamp", format!("2025-01-15T0{}:00:00Z", a.step));
        }
        let _ = store.add_label_to_node(tenant_id, event_id, "Alert");

        // Link event to source
        let source_id = user_ids.get(a.source_name)
            .or_else(|| server_ids.get(a.source_name));
        let target_id = server_ids.get(a.target_name);

        if let (Some(&sid), Some(&tid)) = (source_id, target_id) {
            store.create_edge(event_id, sid, EdgeType::new("ORIGINATED_FROM")).unwrap();
            store.create_edge(event_id, tid, EdgeType::new("TARGETED")).unwrap();
            // Direct lateral movement edge for path analysis
            store.create_edge(sid, tid, EdgeType::new("LATERAL_MOVEMENT")).unwrap();
        }

        // Link event to MITRE technique
        if let Some(&tech_nid) = technique_ids.get(a.technique) {
            store.create_edge(event_id, tech_nid, EdgeType::new("USES_TECHNIQUE")).unwrap();
        }

        attack_event_ids.push(event_id);
    }

    // Chain attack events sequentially
    for i in 0..attack_event_ids.len() - 1 {
        store.create_edge(attack_event_ids[i], attack_event_ids[i + 1], EdgeType::new("LEADS_TO")).unwrap();
    }

    println!("    Attack chain (8 steps):");
    println!("    ┌──────┬────────────┬──────────────────┬──────────────────────────────────────┐");
    println!("    │ Step │ Technique  │ Target           │ Description                          │");
    println!("    ├──────┼────────────┼──────────────────┼──────────────────────────────────────┤");
    for a in &attack_chain {
        let desc_trunc = if a.description.len() > 36 {
            format!("{}...", &a.description[..33])
        } else {
            format!("{:<36}", a.description)
        };
        println!("    │  {:>2}  │ {:<10} │ {:<16} │ {} │", a.step, a.technique, a.target_name, desc_trunc);
    }
    println!("    └──────┴────────────┴──────────────────┴──────────────────────────────────────┘");

    // ======================================================================
    // STEP 4: Detection & Analysis
    // ======================================================================
    section(4, "Detection & Analysis");

    // 4a. Trace lateral movement path via Cypher query
    subsection("4a. Lateral Movement Trace (Cypher)");

    let query = "MATCH (e:AttackEvent) RETURN e LIMIT 10";
    let result = engine.execute(query, &store);
    match result {
        Ok(batch) => {
            println!("    Query: {}", query);
            println!("    Attack events found: {}", batch.len());
        }
        Err(e) => {
            println!("    Query returned error (expected in demo): {}", e);
        }
    }

    // Manually trace via graph API
    println!();
    println!("    Tracing attack path via graph API:");
    let compromised_user = user_ids.get("cmendez").unwrap();
    let edges = store.get_outgoing_edges(*compromised_user);
    let accessed_servers: Vec<_> = edges.iter()
        .filter(|e| e.edge_type.as_str() == "HAS_ACCESS" || e.edge_type.as_str() == "LATERAL_MOVEMENT")
        .collect();

    println!("    Initial compromise: cmendez (Carlos Mendez - no MFA)");
    println!("    Directly accessible from compromised user: {} servers", accessed_servers.len());

    for edge in &accessed_servers {
        if let Some(target_node) = store.get_node(edge.target) {
            let sname = target_node.get_property("name").unwrap().as_string().unwrap();
            let sip = target_node.get_property("ip").unwrap().as_string().unwrap();
            println!("      -> {} ({}) via {}", sname, sip, edge.edge_type.as_str());
        }
    }

    // 4b. Threat signature vector search
    subsection("4b. Threat Signature Vector Search");

    println!("    Incoming IOC: Suspicious PowerShell beacon pattern detected.");
    println!("    Generating mock embedding and searching threat intel database...");
    println!();

    // Query vector simulating a Cobalt Strike beacon signature
    // Use a deterministic embedding close to one of our CVEs
    let query_vec: Vec<f32> = (0..128)
        .map(|j| ((5 * 7 + j * 13) % 100) as f32 / 100.0) // close to CVE index 5
        .collect();

    let search_results = store.vector_search("ThreatIntel", "signature_embedding", &query_vec, 5).unwrap();

    println!("    Top 5 matching threat signatures:");
    println!("    ┌────┬──────────────────┬─────────┬───────────────────────────────────────┐");
    println!("    │  # │ CVE ID           │  Score  │ Description                           │");
    println!("    ├────┼──────────────────┼─────────┼───────────────────────────────────────┤");
    for (rank, (nid, score)) in search_results.iter().enumerate() {
        if let Some(node) = store.get_node(*nid) {
            let cve = node.get_property("cve_id").map(|v| v.as_string().unwrap_or("?")).unwrap_or("?");
            let desc = node.get_property("description").map(|v| v.as_string().unwrap_or("?")).unwrap_or("?");
            let desc_trunc = if desc.len() > 37 {
                format!("{}...", &desc[..34])
            } else {
                format!("{:<37}", desc)
            };
            println!("    │ {:>2} │ {:<16} │ {:>6.4}  │ {} │", rank + 1, cve, score, desc_trunc);
        }
    }
    println!("    └────┴──────────────────┴─────────┴───────────────────────────────────────┘");

    // ======================================================================
    // STEP 5: Graph Algorithms for Investigation
    // ======================================================================
    section(5, "Graph Algorithms for Investigation");

    // 5a. PageRank -- Critical asset identification
    subsection("5a. Critical Asset Identification (PageRank)");
    println!("    Running PageRank across all servers to identify most-connected assets...");

    let view_all = build_view(&store, Some("Server"), None, None);
    let pr_scores = page_rank(&view_all, PageRankConfig::default());

    // Sort by score descending, map back to server names
    let mut scored_servers: Vec<(samyama::NodeId, f64)> = pr_scores.iter()
        .map(|(&node_id_u64, &score)| (samyama::NodeId::new(node_id_u64), score))
        .collect();
    scored_servers.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    println!();
    println!("    Top 10 critical servers by PageRank:");
    println!("    ┌────┬────────────────────┬──────────────┬──────────────┬───────────┐");
    println!("    │  # │ Server             │ IP Address   │ Zone         │ PageRank  │");
    println!("    ├────┼────────────────────┼──────────────┼──────────────┼───────────┤");
    for (rank, (nid, score)) in scored_servers.iter().take(10).enumerate() {
        if let Some(node) = store.get_node(*nid) {
            let sname = node.get_property("name").map(|v| v.as_string().unwrap_or("?")).unwrap_or("?");
            let sip = node.get_property("ip").map(|v| v.as_string().unwrap_or("?")).unwrap_or("?");
            let szone = node.get_property("zone").map(|v| v.as_string().unwrap_or("?")).unwrap_or("?");
            println!("    │ {:>2} │ {:<18} │ {:<12} │ {:<12} │ {:>8.4}  │", rank + 1, sname, sip, szone, score);
        }
    }
    println!("    └────┴────────────────────┴──────────────┴──────────────┴───────────┘");

    // 5b. Dijkstra -- Shortest attack path
    subsection("5b. Attack Path Analysis (Dijkstra Shortest Path)");

    // Build a view of all nodes with all edge types for path analysis
    let view_path = build_view(&store, None, None, None);

    // Find shortest path from the compromised user to the OT SCADA controller
    let entry_point = *user_ids.get("cmendez").unwrap();
    let target_asset = *server_ids.get("ot-scada-01").unwrap();

    println!("    Finding shortest path from cmendez (entry) to ot-scada-01 (target)...");

    match dijkstra(&view_path, entry_point.as_u64(), target_asset.as_u64()) {
        Some(path_result) => {
            println!("    Path found! Cost (hops): {}", path_result.cost);
            println!("    Path:");
            for (i, node_id_u64) in path_result.path.iter().enumerate() {
                let nid = samyama::NodeId::new(*node_id_u64);
                if let Some(node) = store.get_node(nid) {
                    let name = node.get_property("name")
                        .or_else(|| node.get_property("username"))
                        .map(|v| v.as_string().unwrap_or("?"))
                        .unwrap_or("?");
                    let arrow = if i < path_result.path.len() - 1 { " -->" } else { "    " };
                    println!("      [{}] {}{}", i, name, arrow);
                }
            }
        }
        None => {
            println!("    No direct path found (network segmentation is working).");
            println!("    The attacker must pivot through additional nodes.");
            println!();
            // Try alternative path: entry -> int-git-01 -> int-ci-01 -> int-app-01 -> int-dc-01
            let intermediate = *server_ids.get("int-dc-01").unwrap();
            println!("    Trying path to int-dc-01 (Domain Controller) instead...");
            match dijkstra(&view_path, entry_point.as_u64(), intermediate.as_u64()) {
                Some(path_result) => {
                    println!("    Path found! Cost (hops): {}", path_result.cost);
                    println!("    Path:");
                    for (i, node_id_u64) in path_result.path.iter().enumerate() {
                        let nid = samyama::NodeId::new(*node_id_u64);
                        if let Some(node) = store.get_node(nid) {
                            let name = node.get_property("name")
                                .or_else(|| node.get_property("username"))
                                .map(|v| v.as_string().unwrap_or("?"))
                                .unwrap_or("?");
                            let arrow = if i < path_result.path.len() - 1 { " -->" } else { "    " };
                            println!("      [{}] {}{}", i, name, arrow);
                        }
                    }
                }
                None => {
                    println!("    No path found to Domain Controller either.");
                }
            }
        }
    }

    // 5c. Weakly Connected Components -- Network segmentation
    subsection("5c. Network Segmentation Analysis (WCC)");

    println!("    Running Weakly Connected Components to verify network zones...");

    let view_servers = build_view(&store, Some("Server"), None, None);
    let wcc_result = weakly_connected_components(&view_servers);

    let num_components = wcc_result.components.len();
    println!("    Connected components found: {}", num_components);
    println!();

    // Sort components by size descending
    let mut components_sorted: Vec<_> = wcc_result.components.iter().collect();
    components_sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    println!("    ┌─────────────┬──────────┬────────────────────────────────────────────────┐");
    println!("    │ Component   │   Size   │ Sample Members                                 │");
    println!("    ├─────────────┼──────────┼────────────────────────────────────────────────┤");
    for (idx, (_, members)) in components_sorted.iter().enumerate().take(8) {
        let sample: Vec<String> = members.iter().take(4).map(|&nid_u64| {
            let nid = samyama::NodeId::new(nid_u64);
            store.get_node(nid)
                .and_then(|n| n.get_property("name"))
                .and_then(|v| v.as_string().map(|s| s.to_string()))
                .unwrap_or_else(|| format!("node-{}", nid_u64))
        }).collect();
        let sample_str = sample.join(", ");
        let sample_display = if sample_str.len() > 48 {
            format!("{}...", &sample_str[..45])
        } else {
            format!("{:<48}", sample_str)
        };
        println!("    │ Segment {:>2}  │ {:>8} │ {} │", idx + 1, members.len(), sample_display);
    }
    if components_sorted.len() > 8 {
        println!("    │ ... {} more │          │                                                │", components_sorted.len() - 8);
    }
    println!("    └─────────────┴──────────┴────────────────────────────────────────────────┘");

    // ======================================================================
    // STEP 6: Cypher Queries for Forensic Investigation
    // ======================================================================
    section(6, "Forensic Cypher Queries");

    // Query 1: Find all servers in the DMZ
    let queries: Vec<(&str, &str)> = vec![
        ("MATCH (s:DMZ) RETURN s LIMIT 5",         "DMZ Zone Servers (sample)"),
        ("MATCH (s:Internal) RETURN s LIMIT 5",    "Internal Zone Servers (sample)"),
        ("MATCH (s:Cloud) RETURN s LIMIT 5",       "Cloud Zone Servers (sample)"),
        ("MATCH (s:OT) RETURN s LIMIT 5",          "OT Zone Servers (sample)"),
        ("MATCH (u:User) RETURN u LIMIT 5",        "Enterprise Users (sample)"),
        ("MATCH (t:ThreatIntel) RETURN t LIMIT 5", "Threat Intelligence (sample)"),
        ("MATCH (m:Malware) RETURN m LIMIT 5",     "Malware Families (sample)"),
        ("MATCH (a:Alert) RETURN a LIMIT 5",       "Attack Events (sample)"),
    ];

    for (cypher, label) in &queries {
        let start = Instant::now();
        match engine.execute(cypher, &store) {
            Ok(batch) => {
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                println!("    [OK] {} => {} results ({:.2}ms)", label, batch.len(), elapsed);
            }
            Err(e) => {
                println!("    [ERR] {} => {}", label, e);
            }
        }
    }

    // ======================================================================
    // STEP 7: Summary
    // ======================================================================
    section(7, "Investigation Summary");

    let total_nodes = store.all_nodes().len();
    let server_count = store.get_nodes_by_label(&Label::new("Server")).len();
    let user_count = store.get_nodes_by_label(&Label::new("User")).len();
    let threat_count = store.get_nodes_by_label(&Label::new("ThreatIntel")).len();
    let mitre_count = store.get_nodes_by_label(&Label::new("MitreTechnique")).len();
    let malware_count = store.get_nodes_by_label(&Label::new("Malware")).len();
    let alert_count = store.get_nodes_by_label(&Label::new("Alert")).len();

    // Count edges by iterating over all nodes
    let mut total_edges = 0;
    for node in store.all_nodes() {
        total_edges += store.get_outgoing_edges(node.id).len();
    }

    let elapsed = overall_start.elapsed();

    println!();
    println!("    ┌────────────────────────────────┬───────────┐");
    println!("    │ Entity Type                    │     Count │");
    println!("    ├────────────────────────────────┼───────────┤");
    println!("    │ Servers                        │ {:>9} │", server_count);
    println!("    │ Users                          │ {:>9} │", user_count);
    println!("    │ Threat Intel (CVEs)            │ {:>9} │", threat_count);
    println!("    │ MITRE ATT&CK Techniques       │ {:>9} │", mitre_count);
    println!("    │ Malware Families               │ {:>9} │", malware_count);
    println!("    │ Attack Events                  │ {:>9} │", alert_count);
    println!("    ├────────────────────────────────┼───────────┤");
    println!("    │ Total Nodes                    │ {:>9} │", total_nodes);
    println!("    │ Total Edges                    │ {:>9} │", total_edges);
    println!("    └────────────────────────────────┴───────────┘");
    println!();
    println!("    Capabilities Demonstrated:");
    println!("      1. Network topology modeling across 4 zones (DMZ/Internal/Cloud/OT)");
    println!("      2. Threat intelligence ingestion with 128-dim vector embeddings");
    println!("      3. APT lateral movement simulation (8-step kill chain)");
    println!("      4. Cypher query engine for forensic investigation");
    println!("      5. Vector search for IOC matching against threat intel DB");
    println!("      6. PageRank for critical asset identification");
    println!("      7. Dijkstra shortest path for attack path analysis");
    println!("      8. Weakly Connected Components for network segmentation audit");
    println!("      9. MITRE ATT&CK technique mapping");
    println!("     10. Multi-tenancy with mock LLM agent configuration");
    println!();
    println!("    Total execution time: {:.2}s", elapsed.as_secs_f64());

    println!();
    println!("  ╔══════════════════════════════════════════════════════════════════════╗");
    println!("  ║   ENTERPRISE SOC DEMO COMPLETE                                       ║");
    println!("  ║   Graph Model:                                                       ║");
    println!("  ║                                                                      ║");
    println!("  ║   (User)--[:HAS_ACCESS]-->(Server)--[:CONNECTS_TO]-->(Server)        ║");
    println!("  ║      |                        |                                      ║");
    println!("  ║      v                        v                                      ║");
    println!("  ║   (AttackEvent)--[:USES_TECHNIQUE]-->(MitreTechnique)                ║");
    println!("  ║      |                                      ^                        ║");
    println!("  ║      v                                      |                        ║");
    println!("  ║   (ThreatIntel/CVE)--------[:USES_TECHNIQUE]                         ║");
    println!("  ║                                             ^                        ║");
    println!("  ║   (Malware)----------------[:USES_TECHNIQUE]                         ║");
    println!("  ╚══════════════════════════════════════════════════════════════════════╝");
    println!();
}
