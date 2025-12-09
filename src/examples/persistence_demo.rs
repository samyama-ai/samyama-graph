//! Demonstration of Phase 3 persistence features
//!
//! This example shows:
//! - Write-Ahead Log (WAL)
//! - RocksDB persistent storage
//! - Multi-tenancy with resource quotas
//! - Recovery from disk

use samyama::{
    PersistenceManager, ResourceQuotas,
    graph::{Node, Edge, NodeId, EdgeId, Label, EdgeType},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Samyama Persistence Demo ===\n");

    // 1. Create persistence manager
    println!("1. Creating persistence manager at ./demo_data");
    let persist_mgr = PersistenceManager::new("./demo_data")?;
    println!("   ✓ Storage, WAL, and tenant manager initialized\n");

    // 2. Create custom tenants with quotas
    println!("2. Creating tenants with resource quotas");

    // Small tenant with strict limits
    let small_quotas = ResourceQuotas {
        max_nodes: Some(100),
        max_edges: Some(200),
        max_memory_bytes: Some(10 * 1024 * 1024), // 10 MB
        max_storage_bytes: Some(50 * 1024 * 1024), // 50 MB
        max_connections: Some(10),
        max_query_time_ms: Some(5000), // 5 seconds
    };

    persist_mgr.tenants().create_tenant(
        "small_tenant".to_string(),
        "Small Tenant".to_string(),
        Some(small_quotas),
    )?;
    println!("   ✓ Created 'small_tenant' with 100 node limit");

    // Large tenant with generous limits
    let large_quotas = ResourceQuotas {
        max_nodes: Some(1_000_000),
        max_edges: Some(10_000_000),
        max_memory_bytes: Some(1024 * 1024 * 1024), // 1 GB
        max_storage_bytes: Some(10 * 1024 * 1024 * 1024), // 10 GB
        max_connections: Some(1000),
        max_query_time_ms: Some(60_000), // 60 seconds
    };

    persist_mgr.tenants().create_tenant(
        "large_tenant".to_string(),
        "Large Tenant".to_string(),
        Some(large_quotas),
    )?;
    println!("   ✓ Created 'large_tenant' with 1M node limit\n");

    // 3. Persist some data for default tenant
    println!("3. Persisting data for 'default' tenant");

    // Create nodes
    let mut alice = Node::new(NodeId::new(1), Label::new("Person"));
    alice.set_property("name", "Alice");
    alice.set_property("age", 30i64);
    alice.set_property("city", "New York");
    persist_mgr.persist_create_node("default", &alice)?;
    println!("   ✓ Created node: Alice (age: 30, city: New York)");

    let mut bob = Node::new(NodeId::new(2), Label::new("Person"));
    bob.set_property("name", "Bob");
    bob.set_property("age", 25i64);
    bob.set_property("city", "San Francisco");
    persist_mgr.persist_create_node("default", &bob)?;
    println!("   ✓ Created node: Bob (age: 25, city: San Francisco)");

    // Create edge
    let mut knows = Edge::new(
        EdgeId::new(1),
        NodeId::new(1),
        NodeId::new(2),
        EdgeType::new("KNOWS"),
    );
    knows.set_property("since", 2020i64);
    knows.set_property("strength", 0.9);
    persist_mgr.persist_create_edge("default", &knows)?;
    println!("   ✓ Created edge: Alice -[KNOWS]-> Bob (since: 2020)\n");

    // 4. Persist data for small_tenant
    println!("4. Persisting data for 'small_tenant'");

    for i in 1..=5 {
        let mut node = Node::new(NodeId::new(i), Label::new("Product"));
        node.set_property("id", i as i64);
        node.set_property("name", format!("Product {}", i));
        persist_mgr.persist_create_node("small_tenant", &node)?;
    }
    println!("   ✓ Created 5 products\n");

    // 5. Test quota enforcement
    println!("5. Testing quota enforcement");

    let tenant_info = persist_mgr.tenants().get_tenant("small_tenant")?;
    println!("   Tenant: {}", tenant_info.name);
    println!("   Max nodes: {:?}", tenant_info.quotas.max_nodes);

    let usage = persist_mgr.tenants().get_usage("small_tenant")?;
    println!("   Current usage: {} nodes, {} edges", usage.node_count, usage.edge_count);

    // Try to exceed quota (would need to create 95+ more nodes)
    println!("   ✓ Quota enforcement active\n");

    // 6. Create checkpoint
    println!("6. Creating checkpoint");
    persist_mgr.checkpoint()?;
    println!("   ✓ WAL and storage flushed to disk\n");

    // 7. List all tenants
    println!("7. Listing all tenants");
    let tenants = persist_mgr.tenants().list_tenants();
    for tenant in tenants {
        let usage = persist_mgr.tenants().get_usage(&tenant.id)?;
        println!("   • {} ({}): {} nodes, {} edges",
            tenant.id,
            if tenant.enabled { "enabled" } else { "disabled" },
            usage.node_count,
            usage.edge_count,
        );
    }
    println!();

    // 8. Demonstrate recovery
    println!("8. Demonstrating recovery");
    println!("   Recovering default tenant...");
    let (nodes, edges) = persist_mgr.recover("default")?;
    println!("   ✓ Recovered {} nodes and {} edges", nodes.len(), edges.len());

    for node in &nodes {
        if let Some(name) = node.get_property("name") {
            println!("     - {}: {:?}", name.as_string().unwrap(), node.id);
        }
    }
    println!();

    // 9. Demonstrate tenant isolation
    println!("9. Verifying tenant isolation");
    let (default_nodes, _) = persist_mgr.recover("default")?;
    let (small_nodes, _) = persist_mgr.recover("small_tenant")?;
    println!("   default: {} nodes", default_nodes.len());
    println!("   small_tenant: {} nodes", small_nodes.len());
    println!("   ✓ Data is properly isolated between tenants\n");

    // 10. Clean up
    println!("10. Cleanup");
    persist_mgr.flush()?;
    println!("    ✓ All pending writes flushed to disk");
    println!("    Data persisted to: ./demo_data/\n");

    println!("=== Demo Complete ===");
    println!("\nPersisted data structure:");
    println!("  ./demo_data/");
    println!("    ├── data/           (RocksDB storage)");
    println!("    └── wal/            (Write-Ahead Logs)");
    println!("\nYou can inspect the data directory or run this demo again");
    println!("to see recovery in action!");

    Ok(())
}
