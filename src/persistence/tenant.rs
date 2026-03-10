//! Multi-tenancy implementation with resource quotas
//!
//! Implements REQ-TENANT-001 through REQ-TENANT-008

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;
// warn removed - was unused import causing compiler warning
use tracing::{debug, info};

/// Tenant errors
#[derive(Error, Debug)]
pub enum TenantError {
    /// Tenant already exists
    #[error("Tenant already exists: {0}")]
    AlreadyExists(String),

    /// Tenant not found
    #[error("Tenant not found: {0}")]
    NotFound(String),

    /// Quota exceeded
    #[error("Quota exceeded for tenant {tenant}: {resource}")]
    QuotaExceeded {
        tenant: String,
        resource: String,
    },

    /// Permission denied
    #[error("Permission denied for tenant {0}")]
    PermissionDenied(String),
}

pub type TenantResult<T> = Result<T, TenantError>;

/// Resource quotas for a tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceQuotas {
    /// Maximum number of nodes
    pub max_nodes: Option<usize>,
    /// Maximum number of edges
    pub max_edges: Option<usize>,
    /// Maximum memory in bytes
    pub max_memory_bytes: Option<usize>,
    /// Maximum storage in bytes
    pub max_storage_bytes: Option<usize>,
    /// Maximum concurrent connections
    pub max_connections: Option<usize>,
    /// Maximum query execution time in milliseconds
    pub max_query_time_ms: Option<u64>,
}

impl Default for ResourceQuotas {
    fn default() -> Self {
        Self {
            max_nodes: Some(1_000_000),        // 1M nodes
            max_edges: Some(10_000_000),       // 10M edges
            max_memory_bytes: Some(1_073_741_824), // 1 GB
            max_storage_bytes: Some(10_737_418_240), // 10 GB
            max_connections: Some(100),
            max_query_time_ms: Some(60_000),   // 60 seconds
        }
    }
}

impl ResourceQuotas {
    /// Create unlimited quotas
    pub fn unlimited() -> Self {
        Self {
            max_nodes: None,
            max_edges: None,
            max_memory_bytes: None,
            max_storage_bytes: None,
            max_connections: None,
            max_query_time_ms: None,
        }
    }
}

/// Current resource usage for a tenant
#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    /// Current number of nodes
    pub node_count: usize,
    /// Current number of edges
    pub edge_count: usize,
    /// Current memory usage in bytes
    pub memory_bytes: usize,
    /// Current storage usage in bytes
    pub storage_bytes: usize,
    /// Current number of connections
    pub active_connections: usize,
}

impl ResourceUsage {
    fn check_quota(&self, quotas: &ResourceQuotas, resource: &str) -> TenantResult<()> {
        match resource {
            "nodes" => {
                if let Some(max) = quotas.max_nodes {
                    if self.node_count >= max {
                        return Err(TenantError::QuotaExceeded {
                            tenant: String::new(),
                            resource: format!("nodes ({}/{})", self.node_count, max),
                        });
                    }
                }
            }
            "edges" => {
                if let Some(max) = quotas.max_edges {
                    if self.edge_count >= max {
                        return Err(TenantError::QuotaExceeded {
                            tenant: String::new(),
                            resource: format!("edges ({}/{})", self.edge_count, max),
                        });
                    }
                }
            }
            "memory" => {
                if let Some(max) = quotas.max_memory_bytes {
                    if self.memory_bytes >= max {
                        return Err(TenantError::QuotaExceeded {
                            tenant: String::new(),
                            resource: format!("memory ({}/{})", self.memory_bytes, max),
                        });
                    }
                }
            }
            "connections" => {
                if let Some(max) = quotas.max_connections {
                    if self.active_connections >= max {
                        return Err(TenantError::QuotaExceeded {
                            tenant: String::new(),
                            resource: format!("connections ({}/{})", self.active_connections, max),
                        });
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Tenant configuration and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// Tenant ID (unique identifier)
    pub id: String,
    /// Display name
    pub name: String,
    /// Creation timestamp
    pub created_at: i64,
    /// Resource quotas
    pub quotas: ResourceQuotas,
    /// Enabled status
    pub enabled: bool,
    /// Auto-Embed configuration
    pub embed_config: Option<AutoEmbedConfig>,
    /// NLQ configuration
    pub nlq_config: Option<NLQConfig>,
    /// Agent configuration
    pub agent_config: Option<AgentConfig>,
}

impl Tenant {
    /// Create a new tenant
    pub fn new(id: String, name: String) -> Self {
        Self {
            id,
            name,
            created_at: chrono::Utc::now().timestamp(),
            quotas: ResourceQuotas::default(),
            enabled: true,
            embed_config: None,
            nlq_config: None,
            agent_config: None,
        }
    }

    /// Create a tenant with custom quotas
    pub fn with_quotas(id: String, name: String, quotas: ResourceQuotas) -> Self {
        Self {
            id,
            name,
            created_at: chrono::Utc::now().timestamp(),
            quotas,
            enabled: true,
            embed_config: None,
            nlq_config: None,
            agent_config: None,
        }
    }
}

/// LLM Provider options
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LLMProvider {
    OpenAI,
    Ollama,
    Gemini,
    AzureOpenAI,
    Anthropic,
    ClaudeCode,
    Mock,
}

/// Tool definition for agents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolConfig {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
    pub enabled: bool,
}

/// Configuration for Agentic features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    /// Enabled status
    pub enabled: bool,
    /// LLM provider for the agent
    pub provider: LLMProvider,
    /// Model name
    pub model: String,
    /// API Key
    pub api_key: Option<String>,
    /// Base URL
    pub api_base_url: Option<String>,
    /// System prompt
    pub system_prompt: Option<String>,
    /// Available tools
    pub tools: Vec<ToolConfig>,
    /// Auto-trigger policies (e.g., on node creation)
    pub policies: HashMap<String, String>, // Label -> Trigger Prompt
}

/// Configuration for NLQ features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NLQConfig {
    /// Enabled status
    pub enabled: bool,
    /// The LLM provider to use
    pub provider: LLMProvider,
    /// Model name (e.g., "gpt-4o", "llama3")
    pub model: String,
    /// API Key (optional, can be loaded from env if None)
    pub api_key: Option<String>,
    /// API Base URL (required for Ollama/Azure, optional for others)
    pub api_base_url: Option<String>,
    /// System prompt for the LLM
    pub system_prompt: Option<String>,
}

/// Configuration for Auto-Embed features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoEmbedConfig {
    /// The LLM provider to use
    pub provider: LLMProvider,
    /// Model name (e.g., "text-embedding-3-small", "llama3")
    pub embedding_model: String,
    /// API Key (optional, can be loaded from env if None)
    pub api_key: Option<String>,
    /// API Base URL (required for Ollama/Azure, optional for others)
    pub api_base_url: Option<String>,
    /// Chunk size for text splitting
    pub chunk_size: usize,
    /// Overlap between chunks
    pub chunk_overlap: usize,
    /// Vector dimension size
    pub vector_dimension: usize,
    /// Embedding policies: Label -> `Vec<PropertyKey>`
    pub embedding_policies: HashMap<String, Vec<String>>,
}

/// Tenant manager - manages all tenants and their resources
pub struct TenantManager {
    /// All tenants
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
    /// Resource usage per tenant
    usage: Arc<RwLock<HashMap<String, ResourceUsage>>>,
}

impl TenantManager {
    /// Create a new tenant manager
    pub fn new() -> Self {
        let mut tenants = HashMap::new();
        let mut usage = HashMap::new();

        // Create default tenant
        let default_tenant = Tenant::new("default".to_string(), "Default Tenant".to_string());
        tenants.insert("default".to_string(), default_tenant);
        usage.insert("default".to_string(), ResourceUsage::default());

        info!("Tenant manager initialized with default tenant");

        Self {
            tenants: Arc::new(RwLock::new(tenants)),
            usage: Arc::new(RwLock::new(usage)),
        }
    }

    /// Create a new tenant
    pub fn create_tenant(&self, id: String, name: String, quotas: Option<ResourceQuotas>) -> TenantResult<()> {
        let mut tenants = self.tenants.write().unwrap();
        let mut usage = self.usage.write().unwrap();

        if tenants.contains_key(&id) {
            return Err(TenantError::AlreadyExists(id));
        }

        let tenant = if let Some(quotas) = quotas {
            Tenant::with_quotas(id.clone(), name, quotas)
        } else {
            Tenant::new(id.clone(), name)
        };

        tenants.insert(id.clone(), tenant);
        usage.insert(id.clone(), ResourceUsage::default());

        info!("Created tenant: {}", id);

        Ok(())
    }

    /// Delete a tenant
    pub fn delete_tenant(&self, id: &str) -> TenantResult<()> {
        if id == "default" {
            return Err(TenantError::PermissionDenied("Cannot delete default tenant".to_string()));
        }

        let mut tenants = self.tenants.write().unwrap();
        let mut usage = self.usage.write().unwrap();

        if !tenants.contains_key(id) {
            return Err(TenantError::NotFound(id.to_string()));
        }

        tenants.remove(id);
        usage.remove(id);

        info!("Deleted tenant: {}", id);

        Ok(())
    }

    /// Get tenant information
    pub fn get_tenant(&self, id: &str) -> TenantResult<Tenant> {
        let tenants = self.tenants.read().unwrap();
        tenants.get(id)
            .cloned()
            .ok_or_else(|| TenantError::NotFound(id.to_string()))
    }

    /// List all tenants
    pub fn list_tenants(&self) -> Vec<Tenant> {
        let tenants = self.tenants.read().unwrap();
        tenants.values().cloned().collect()
    }

    /// Check if a tenant exists and is enabled
    pub fn is_tenant_enabled(&self, id: &str) -> bool {
        let tenants = self.tenants.read().unwrap();
        tenants.get(id)
            .map(|t| t.enabled)
            .unwrap_or(false)
    }

    /// Check and enforce resource quota
    pub fn check_quota(&self, tenant_id: &str, resource: &str) -> TenantResult<()> {
        let tenants = self.tenants.read().unwrap();
        let usage = self.usage.read().unwrap();

        let tenant = tenants.get(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        if !tenant.enabled {
            return Err(TenantError::PermissionDenied(format!("Tenant {} is disabled", tenant_id)));
        }

        let current_usage = usage.get(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        current_usage.check_quota(&tenant.quotas, resource)
            .map_err(|e| match e {
                TenantError::QuotaExceeded { tenant: _, resource } => {
                    TenantError::QuotaExceeded {
                        tenant: tenant_id.to_string(),
                        resource,
                    }
                }
                e => e,
            })
    }

    /// Increment resource usage
    pub fn increment_usage(&self, tenant_id: &str, resource: &str, amount: usize) -> TenantResult<()> {
        let mut usage = self.usage.write().unwrap();

        let tenant_usage = usage.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        match resource {
            "nodes" => tenant_usage.node_count += amount,
            "edges" => tenant_usage.edge_count += amount,
            "memory" => tenant_usage.memory_bytes += amount,
            "storage" => tenant_usage.storage_bytes += amount,
            "connections" => tenant_usage.active_connections += amount,
            _ => {}
        }

        debug!("Incremented {} for tenant {} by {}", resource, tenant_id, amount);

        Ok(())
    }

    /// Decrement resource usage
    pub fn decrement_usage(&self, tenant_id: &str, resource: &str, amount: usize) -> TenantResult<()> {
        let mut usage = self.usage.write().unwrap();

        let tenant_usage = usage.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        match resource {
            "nodes" => tenant_usage.node_count = tenant_usage.node_count.saturating_sub(amount),
            "edges" => tenant_usage.edge_count = tenant_usage.edge_count.saturating_sub(amount),
            "memory" => tenant_usage.memory_bytes = tenant_usage.memory_bytes.saturating_sub(amount),
            "storage" => tenant_usage.storage_bytes = tenant_usage.storage_bytes.saturating_sub(amount),
            "connections" => tenant_usage.active_connections = tenant_usage.active_connections.saturating_sub(amount),
            _ => {}
        }

        debug!("Decremented {} for tenant {} by {}", resource, tenant_id, amount);

        Ok(())
    }

    /// Get resource usage for a tenant
    pub fn get_usage(&self, tenant_id: &str) -> TenantResult<ResourceUsage> {
        let usage = self.usage.read().unwrap();
        usage.get(tenant_id)
            .cloned()
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))
    }

    /// Update tenant quotas
    pub fn update_quotas(&self, tenant_id: &str, quotas: ResourceQuotas) -> TenantResult<()> {
        let mut tenants = self.tenants.write().unwrap();

        let tenant = tenants.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        tenant.quotas = quotas;

        info!("Updated quotas for tenant: {}", tenant_id);

        Ok(())
    }

    /// Update Auto-Embed configuration for a tenant
    pub fn update_embed_config(&self, tenant_id: &str, config: Option<AutoEmbedConfig>) -> TenantResult<()> {
        let mut tenants = self.tenants.write().unwrap();

        let tenant = tenants.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        tenant.embed_config = config;

        info!("Updated Auto-Embed config for tenant: {}", tenant_id);

        Ok(())
    }

    /// Update NLQ configuration for a tenant
    pub fn update_nlq_config(&self, tenant_id: &str, config: Option<NLQConfig>) -> TenantResult<()> {
        let mut tenants = self.tenants.write().unwrap();

        let tenant = tenants.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        tenant.nlq_config = config;

        info!("Updated NLQ config for tenant: {}", tenant_id);

        Ok(())
    }

    /// Update Agent configuration for a tenant
    pub fn update_agent_config(&self, tenant_id: &str, config: Option<AgentConfig>) -> TenantResult<()> {
        let mut tenants = self.tenants.write().unwrap();

        let tenant = tenants.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        tenant.agent_config = config;

        info!("Updated Agent config for tenant: {}", tenant_id);

        Ok(())
    }

    /// Enable/disable a tenant
    pub fn set_enabled(&self, tenant_id: &str, enabled: bool) -> TenantResult<()> {
        if tenant_id == "default" {
            return Err(TenantError::PermissionDenied("Cannot disable default tenant".to_string()));
        }

        let mut tenants = self.tenants.write().unwrap();

        let tenant = tenants.get_mut(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        tenant.enabled = enabled;

        info!("Set tenant {} enabled status to: {}", tenant_id, enabled);

        Ok(())
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_manager_creation() {
        let manager = TenantManager::new();
        assert!(manager.is_tenant_enabled("default"));
    }

    #[test]
    fn test_create_tenant() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();

        let tenant = manager.get_tenant("tenant1").unwrap();
        assert_eq!(tenant.id, "tenant1");
        assert_eq!(tenant.name, "Tenant 1");
        assert!(tenant.enabled);
    }

    #[test]
    fn test_duplicate_tenant() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();

        let result = manager.create_tenant("tenant1".to_string(), "Tenant 1 Duplicate".to_string(), None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::AlreadyExists(_)));
    }

    #[test]
    fn test_delete_tenant() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();

        manager.delete_tenant("tenant1").unwrap();
        assert!(manager.get_tenant("tenant1").is_err());
    }

    #[test]
    fn test_cannot_delete_default() {
        let manager = TenantManager::new();
        let result = manager.delete_tenant("default");
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_enforcement() {
        let manager = TenantManager::new();

        let mut quotas = ResourceQuotas::default();
        quotas.max_nodes = Some(10);

        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), Some(quotas)).unwrap();

        // Should succeed for first 10 nodes
        for _ in 0..10 {
            manager.check_quota("tenant1", "nodes").unwrap();
            manager.increment_usage("tenant1", "nodes", 1).unwrap();
        }

        // 11th should fail
        let result = manager.check_quota("tenant1", "nodes");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::QuotaExceeded { .. }));
    }

    #[test]
    fn test_usage_tracking() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();

        manager.increment_usage("tenant1", "nodes", 5).unwrap();
        manager.increment_usage("tenant1", "edges", 10).unwrap();

        let usage = manager.get_usage("tenant1").unwrap();
        assert_eq!(usage.node_count, 5);
        assert_eq!(usage.edge_count, 10);

        manager.decrement_usage("tenant1", "nodes", 2).unwrap();
        let usage = manager.get_usage("tenant1").unwrap();
        assert_eq!(usage.node_count, 3);
    }

    #[test]
    fn test_list_tenants() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();
        manager.create_tenant("tenant2".to_string(), "Tenant 2".to_string(), None).unwrap();

        let tenants = manager.list_tenants();
        assert_eq!(tenants.len(), 3); // default + 2 new
    }

    #[test]
    fn test_disable_tenant() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();

        manager.set_enabled("tenant1", false).unwrap();
        assert!(!manager.is_tenant_enabled("tenant1"));

        let result = manager.check_quota("tenant1", "nodes");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::PermissionDenied(_)));
    }

    #[test]
    fn test_update_embed_config() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "Tenant 1".to_string(), None).unwrap();

        let embed_config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "text-embedding-3-small".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 1536,
            embedding_policies: HashMap::from([("Document".to_string(), vec!["content".to_string()])]),
        };

        manager.update_embed_config("tenant1", Some(embed_config)).unwrap();

        let tenant = manager.get_tenant("tenant1").unwrap();
        assert!(tenant.embed_config.is_some());
        let config = tenant.embed_config.unwrap();
        assert_eq!(config.provider, LLMProvider::OpenAI);
        assert_eq!(config.embedding_model, "text-embedding-3-small");
    }

    // ========== Batch 7: Additional Tenant Tests ==========

    #[test]
    fn test_update_nlq_config() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "T1".to_string(), None).unwrap();

        let nlq_config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Ollama,
            model: "llama3".to_string(),
            api_key: None,
            api_base_url: Some("http://localhost:11434".to_string()),
            system_prompt: Some("You are a Cypher expert.".to_string()),
        };

        manager.update_nlq_config("tenant1", Some(nlq_config)).unwrap();

        let tenant = manager.get_tenant("tenant1").unwrap();
        assert!(tenant.nlq_config.is_some());
        let config = tenant.nlq_config.unwrap();
        assert_eq!(config.provider, LLMProvider::Ollama);
        assert_eq!(config.model, "llama3");
    }

    #[test]
    fn test_update_agent_config() {
        let manager = TenantManager::new();
        manager.create_tenant("tenant1".to_string(), "T1".to_string(), None).unwrap();

        let agent_config = AgentConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
            tools: vec![],
            policies: HashMap::new(),
        };

        manager.update_agent_config("tenant1", Some(agent_config)).unwrap();

        let tenant = manager.get_tenant("tenant1").unwrap();
        assert!(tenant.agent_config.is_some());
        assert_eq!(tenant.agent_config.unwrap().provider, LLMProvider::Mock);
    }

    #[test]
    fn test_update_config_nonexistent_tenant() {
        let manager = TenantManager::new();

        let nlq_config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        };

        let result = manager.update_nlq_config("nonexistent", Some(nlq_config));
        assert!(result.is_err());
    }

    // ========== Additional Tenant Coverage Tests ==========

    #[test]
    fn test_resource_quotas_default() {
        let q = ResourceQuotas::default();
        assert_eq!(q.max_nodes, Some(1_000_000));
        assert_eq!(q.max_edges, Some(10_000_000));
        assert_eq!(q.max_memory_bytes, Some(1_073_741_824));
        assert_eq!(q.max_storage_bytes, Some(10_737_418_240));
        assert_eq!(q.max_connections, Some(100));
        assert_eq!(q.max_query_time_ms, Some(60_000));
    }

    #[test]
    fn test_resource_quotas_unlimited() {
        let q = ResourceQuotas::unlimited();
        assert!(q.max_nodes.is_none());
        assert!(q.max_edges.is_none());
        assert!(q.max_memory_bytes.is_none());
        assert!(q.max_storage_bytes.is_none());
        assert!(q.max_connections.is_none());
        assert!(q.max_query_time_ms.is_none());
    }

    #[test]
    fn test_tenant_new() {
        let t = Tenant::new("test_id".to_string(), "Test Name".to_string());
        assert_eq!(t.id, "test_id");
        assert_eq!(t.name, "Test Name");
        assert!(t.enabled);
        assert!(t.embed_config.is_none());
        assert!(t.nlq_config.is_none());
        assert!(t.agent_config.is_none());
        assert!(t.created_at > 0);
    }

    #[test]
    fn test_tenant_with_quotas() {
        let quotas = ResourceQuotas::unlimited();
        let t = Tenant::with_quotas("custom".to_string(), "Custom".to_string(), quotas);
        assert_eq!(t.id, "custom");
        assert!(t.quotas.max_nodes.is_none());
        assert!(t.enabled);
    }

    #[test]
    fn test_tenant_manager_default() {
        let manager = TenantManager::default();
        assert!(manager.is_tenant_enabled("default"));
    }

    #[test]
    fn test_delete_nonexistent_tenant() {
        let manager = TenantManager::new();
        let result = manager.delete_tenant("ghost");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::NotFound(_)));
    }

    #[test]
    fn test_get_nonexistent_tenant() {
        let manager = TenantManager::new();
        let result = manager.get_tenant("ghost");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::NotFound(_)));
    }

    #[test]
    fn test_is_tenant_enabled_nonexistent() {
        let manager = TenantManager::new();
        assert!(!manager.is_tenant_enabled("ghost"));
    }

    #[test]
    fn test_cannot_disable_default_tenant() {
        let manager = TenantManager::new();
        let result = manager.set_enabled("default", false);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::PermissionDenied(_)));
    }

    #[test]
    fn test_set_enabled_nonexistent() {
        let manager = TenantManager::new();
        let result = manager.set_enabled("ghost", true);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::NotFound(_)));
    }

    #[test]
    fn test_set_enabled_reenable() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        manager.set_enabled("t1", false).unwrap();
        assert!(!manager.is_tenant_enabled("t1"));

        manager.set_enabled("t1", true).unwrap();
        assert!(manager.is_tenant_enabled("t1"));
    }

    #[test]
    fn test_update_quotas() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        let new_quotas = ResourceQuotas {
            max_nodes: Some(500),
            max_edges: Some(1000),
            max_memory_bytes: None,
            max_storage_bytes: None,
            max_connections: Some(10),
            max_query_time_ms: Some(5000),
        };

        manager.update_quotas("t1", new_quotas).unwrap();

        let tenant = manager.get_tenant("t1").unwrap();
        assert_eq!(tenant.quotas.max_nodes, Some(500));
        assert_eq!(tenant.quotas.max_edges, Some(1000));
        assert!(tenant.quotas.max_memory_bytes.is_none());
    }

    #[test]
    fn test_update_quotas_nonexistent() {
        let manager = TenantManager::new();
        let result = manager.update_quotas("ghost", ResourceQuotas::unlimited());
        assert!(result.is_err());
    }

    #[test]
    fn test_check_quota_disabled_tenant() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();
        manager.set_enabled("t1", false).unwrap();

        let result = manager.check_quota("t1", "nodes");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TenantError::PermissionDenied(_)));
    }

    #[test]
    fn test_check_quota_nonexistent_tenant() {
        let manager = TenantManager::new();
        let result = manager.check_quota("ghost", "nodes");
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_enforcement_edges() {
        let manager = TenantManager::new();
        let quotas = ResourceQuotas {
            max_nodes: None,
            max_edges: Some(5),
            max_memory_bytes: None,
            max_storage_bytes: None,
            max_connections: None,
            max_query_time_ms: None,
        };
        manager.create_tenant("t1".to_string(), "T1".to_string(), Some(quotas)).unwrap();

        for _ in 0..5 {
            manager.check_quota("t1", "edges").unwrap();
            manager.increment_usage("t1", "edges", 1).unwrap();
        }

        let result = manager.check_quota("t1", "edges");
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_enforcement_memory() {
        let manager = TenantManager::new();
        let quotas = ResourceQuotas {
            max_nodes: None,
            max_edges: None,
            max_memory_bytes: Some(1024),
            max_storage_bytes: None,
            max_connections: None,
            max_query_time_ms: None,
        };
        manager.create_tenant("t1".to_string(), "T1".to_string(), Some(quotas)).unwrap();

        manager.increment_usage("t1", "memory", 1024).unwrap();
        let result = manager.check_quota("t1", "memory");
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_enforcement_connections() {
        let manager = TenantManager::new();
        let quotas = ResourceQuotas {
            max_nodes: None,
            max_edges: None,
            max_memory_bytes: None,
            max_storage_bytes: None,
            max_connections: Some(2),
            max_query_time_ms: None,
        };
        manager.create_tenant("t1".to_string(), "T1".to_string(), Some(quotas)).unwrap();

        manager.increment_usage("t1", "connections", 2).unwrap();
        let result = manager.check_quota("t1", "connections");
        assert!(result.is_err());
    }

    #[test]
    fn test_quota_unlimited_allows_everything() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), Some(ResourceQuotas::unlimited())).unwrap();

        manager.increment_usage("t1", "nodes", 999_999).unwrap();
        assert!(manager.check_quota("t1", "nodes").is_ok());

        manager.increment_usage("t1", "edges", 999_999).unwrap();
        assert!(manager.check_quota("t1", "edges").is_ok());
    }

    #[test]
    fn test_increment_usage_unknown_resource() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();
        // Unknown resource should not panic, just be a no-op
        manager.increment_usage("t1", "unknown_resource", 100).unwrap();
    }

    #[test]
    fn test_decrement_usage_unknown_resource() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();
        manager.decrement_usage("t1", "unknown_resource", 100).unwrap();
    }

    #[test]
    fn test_increment_usage_nonexistent_tenant() {
        let manager = TenantManager::new();
        let result = manager.increment_usage("ghost", "nodes", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrement_usage_nonexistent_tenant() {
        let manager = TenantManager::new();
        let result = manager.decrement_usage("ghost", "nodes", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrement_usage_saturating() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        manager.increment_usage("t1", "nodes", 3).unwrap();
        // Decrement more than the current count: should saturate to 0
        manager.decrement_usage("t1", "nodes", 100).unwrap();

        let usage = manager.get_usage("t1").unwrap();
        assert_eq!(usage.node_count, 0);
    }

    #[test]
    fn test_decrement_all_resource_types() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        manager.increment_usage("t1", "nodes", 10).unwrap();
        manager.increment_usage("t1", "edges", 20).unwrap();
        manager.increment_usage("t1", "memory", 1000).unwrap();
        manager.increment_usage("t1", "storage", 2000).unwrap();
        manager.increment_usage("t1", "connections", 5).unwrap();

        manager.decrement_usage("t1", "nodes", 3).unwrap();
        manager.decrement_usage("t1", "edges", 5).unwrap();
        manager.decrement_usage("t1", "memory", 200).unwrap();
        manager.decrement_usage("t1", "storage", 500).unwrap();
        manager.decrement_usage("t1", "connections", 2).unwrap();

        let usage = manager.get_usage("t1").unwrap();
        assert_eq!(usage.node_count, 7);
        assert_eq!(usage.edge_count, 15);
        assert_eq!(usage.memory_bytes, 800);
        assert_eq!(usage.storage_bytes, 1500);
        assert_eq!(usage.active_connections, 3);
    }

    #[test]
    fn test_get_usage_nonexistent_tenant() {
        let manager = TenantManager::new();
        let result = manager.get_usage("ghost");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_usage_default_tenant() {
        let manager = TenantManager::new();
        let usage = manager.get_usage("default").unwrap();
        assert_eq!(usage.node_count, 0);
        assert_eq!(usage.edge_count, 0);
        assert_eq!(usage.memory_bytes, 0);
        assert_eq!(usage.storage_bytes, 0);
        assert_eq!(usage.active_connections, 0);
    }

    #[test]
    fn test_update_embed_config_nonexistent() {
        let manager = TenantManager::new();
        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "text-embedding-3-small".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 256,
            chunk_overlap: 32,
            vector_dimension: 1536,
            embedding_policies: HashMap::new(),
        };
        let result = manager.update_embed_config("ghost", Some(config));
        assert!(result.is_err());
    }

    #[test]
    fn test_update_agent_config_nonexistent() {
        let manager = TenantManager::new();
        let result = manager.update_agent_config("ghost", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_clear_embed_config() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "test".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 256,
            chunk_overlap: 32,
            vector_dimension: 768,
            embedding_policies: HashMap::new(),
        };
        manager.update_embed_config("t1", Some(config)).unwrap();
        assert!(manager.get_tenant("t1").unwrap().embed_config.is_some());

        // Clear it
        manager.update_embed_config("t1", None).unwrap();
        assert!(manager.get_tenant("t1").unwrap().embed_config.is_none());
    }

    #[test]
    fn test_clear_nlq_config() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        };
        manager.update_nlq_config("t1", Some(config)).unwrap();
        assert!(manager.get_tenant("t1").unwrap().nlq_config.is_some());

        manager.update_nlq_config("t1", None).unwrap();
        assert!(manager.get_tenant("t1").unwrap().nlq_config.is_none());
    }

    #[test]
    fn test_clear_agent_config() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        let config = AgentConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
            tools: vec![],
            policies: HashMap::new(),
        };
        manager.update_agent_config("t1", Some(config)).unwrap();
        assert!(manager.get_tenant("t1").unwrap().agent_config.is_some());

        manager.update_agent_config("t1", None).unwrap();
        assert!(manager.get_tenant("t1").unwrap().agent_config.is_none());
    }

    #[test]
    fn test_llm_provider_variants() {
        // Ensure all LLMProvider variants are distinct
        let providers = vec![
            LLMProvider::OpenAI,
            LLMProvider::Ollama,
            LLMProvider::Gemini,
            LLMProvider::AzureOpenAI,
            LLMProvider::Anthropic,
            LLMProvider::ClaudeCode,
            LLMProvider::Mock,
        ];
        for (i, p1) in providers.iter().enumerate() {
            for (j, p2) in providers.iter().enumerate() {
                if i == j {
                    assert_eq!(p1, p2);
                } else {
                    assert_ne!(p1, p2);
                }
            }
        }
    }

    #[test]
    fn test_resource_quotas_serialization() {
        let quotas = ResourceQuotas {
            max_nodes: Some(500),
            max_edges: Some(1000),
            max_memory_bytes: None,
            max_storage_bytes: None,
            max_connections: Some(10),
            max_query_time_ms: Some(30_000),
        };
        let json = serde_json::to_string(&quotas).unwrap();
        let deserialized: ResourceQuotas = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.max_nodes, Some(500));
        assert_eq!(deserialized.max_edges, Some(1000));
        assert!(deserialized.max_memory_bytes.is_none());
        assert_eq!(deserialized.max_connections, Some(10));
    }

    #[test]
    fn test_nlq_config_serialization() {
        let config = NLQConfig {
            enabled: true,
            provider: LLMProvider::Gemini,
            model: "gemini-pro".to_string(),
            api_key: Some("key123".to_string()),
            api_base_url: None,
            system_prompt: Some("You are a graph expert.".to_string()),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: NLQConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.provider, LLMProvider::Gemini);
        assert_eq!(deserialized.model, "gemini-pro");
        assert_eq!(deserialized.api_key, Some("key123".to_string()));
    }

    #[test]
    fn test_auto_embed_config_serialization() {
        let config = AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "text-embedding-3-small".to_string(),
            api_key: Some("sk-test".to_string()),
            api_base_url: None,
            chunk_size: 512,
            chunk_overlap: 64,
            vector_dimension: 1536,
            embedding_policies: HashMap::from([
                ("Document".to_string(), vec!["content".to_string(), "title".to_string()]),
            ]),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AutoEmbedConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.chunk_size, 512);
        assert_eq!(deserialized.vector_dimension, 1536);
        assert_eq!(deserialized.embedding_policies.len(), 1);
    }

    #[test]
    fn test_agent_config_serialization() {
        let config = AgentConfig {
            enabled: true,
            provider: LLMProvider::Anthropic,
            model: "claude-3-opus".to_string(),
            api_key: Some("sk-ant-test".to_string()),
            api_base_url: None,
            system_prompt: Some("You are an agent.".to_string()),
            tools: vec![
                ToolConfig {
                    name: "search".to_string(),
                    description: "Search the graph".to_string(),
                    parameters: serde_json::json!({"query": "string"}),
                    enabled: true,
                },
            ],
            policies: HashMap::from([
                ("Person".to_string(), "Enrich person data".to_string()),
            ]),
        };
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AgentConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.provider, LLMProvider::Anthropic);
        assert_eq!(deserialized.tools.len(), 1);
        assert_eq!(deserialized.tools[0].name, "search");
        assert!(deserialized.tools[0].enabled);
        assert_eq!(deserialized.policies.len(), 1);
    }

    #[test]
    fn test_tool_config_serialization() {
        let tool = ToolConfig {
            name: "query".to_string(),
            description: "Execute a Cypher query".to_string(),
            parameters: serde_json::json!({"cypher": "string", "readonly": "boolean"}),
            enabled: false,
        };
        let json = serde_json::to_string(&tool).unwrap();
        let deserialized: ToolConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "query");
        assert!(!deserialized.enabled);
    }

    #[test]
    fn test_tenant_serialization() {
        let tenant = Tenant::new("t1".to_string(), "Tenant 1".to_string());
        let json = serde_json::to_string(&tenant).unwrap();
        let deserialized: Tenant = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, "t1");
        assert_eq!(deserialized.name, "Tenant 1");
        assert!(deserialized.enabled);
    }

    #[test]
    fn test_check_quota_unknown_resource_passes() {
        let manager = TenantManager::new();
        // Unknown resource type should pass quota check (no limits defined)
        let result = manager.check_quota("default", "something_random");
        assert!(result.is_ok());
    }

    #[test]
    fn test_increment_storage_usage() {
        let manager = TenantManager::new();
        manager.create_tenant("t1".to_string(), "T1".to_string(), None).unwrap();

        manager.increment_usage("t1", "storage", 5000).unwrap();
        let usage = manager.get_usage("t1").unwrap();
        assert_eq!(usage.storage_bytes, 5000);
    }

    #[test]
    fn test_resource_usage_default() {
        let usage = ResourceUsage::default();
        assert_eq!(usage.node_count, 0);
        assert_eq!(usage.edge_count, 0);
        assert_eq!(usage.memory_bytes, 0);
        assert_eq!(usage.storage_bytes, 0);
        assert_eq!(usage.active_connections, 0);
    }

    #[test]
    fn test_llm_provider_serialization_roundtrip() {
        let providers = vec![
            LLMProvider::OpenAI,
            LLMProvider::Ollama,
            LLMProvider::Gemini,
            LLMProvider::AzureOpenAI,
            LLMProvider::Anthropic,
            LLMProvider::ClaudeCode,
            LLMProvider::Mock,
        ];
        for provider in providers {
            let json = serde_json::to_string(&provider).unwrap();
            let deserialized: LLMProvider = serde_json::from_str(&json).unwrap();
            assert_eq!(provider, deserialized);
        }
    }

    // ========== Additional Tenant Coverage Tests ==========

    #[test]
    fn test_tenant_error_display_messages() {
        let e1 = TenantError::AlreadyExists("t1".to_string());
        assert!(format!("{}", e1).contains("already exists"));
        assert!(format!("{}", e1).contains("t1"));

        let e2 = TenantError::NotFound("t2".to_string());
        assert!(format!("{}", e2).contains("not found"));
        assert!(format!("{}", e2).contains("t2"));

        let e3 = TenantError::QuotaExceeded {
            tenant: "t3".to_string(),
            resource: "nodes (100/100)".to_string(),
        };
        assert!(format!("{}", e3).contains("Quota exceeded"));
        assert!(format!("{}", e3).contains("t3"));

        let e4 = TenantError::PermissionDenied("t4".to_string());
        assert!(format!("{}", e4).contains("Permission denied"));
    }

    #[test]
    fn test_auto_embed_config_construction() {
        let mut policies = HashMap::new();
        policies.insert("Article".to_string(), vec!["title".to_string(), "body".to_string()]);
        policies.insert("Comment".to_string(), vec!["text".to_string()]);

        let config = AutoEmbedConfig {
            provider: LLMProvider::Ollama,
            embedding_model: "nomic-embed-text".to_string(),
            api_key: None,
            api_base_url: Some("http://localhost:11434".to_string()),
            chunk_size: 1024,
            chunk_overlap: 128,
            vector_dimension: 768,
            embedding_policies: policies,
        };

        assert_eq!(config.provider, LLMProvider::Ollama);
        assert_eq!(config.chunk_size, 1024);
        assert_eq!(config.chunk_overlap, 128);
        assert_eq!(config.vector_dimension, 768);
        assert_eq!(config.embedding_policies.len(), 2);
        assert!(config.api_key.is_none());
        assert!(config.api_base_url.is_some());
    }

    #[test]
    fn test_nlq_config_construction() {
        let config = NLQConfig {
            enabled: false,
            provider: LLMProvider::AzureOpenAI,
            model: "gpt-4".to_string(),
            api_key: Some("azure-key".to_string()),
            api_base_url: Some("https://my-endpoint.openai.azure.com".to_string()),
            system_prompt: None,
        };

        assert!(!config.enabled);
        assert_eq!(config.provider, LLMProvider::AzureOpenAI);
        assert!(config.api_key.is_some());
        assert!(config.api_base_url.is_some());
        assert!(config.system_prompt.is_none());
    }

    #[test]
    fn test_agent_config_with_tools_and_policies() {
        let tool1 = ToolConfig {
            name: "cypher_query".to_string(),
            description: "Execute a Cypher query on the graph".to_string(),
            parameters: serde_json::json!({"query": {"type": "string"}, "readonly": {"type": "boolean"}}),
            enabled: true,
        };
        let tool2 = ToolConfig {
            name: "vector_search".to_string(),
            description: "Search vectors".to_string(),
            parameters: serde_json::json!({"query": {"type": "string"}, "k": {"type": "integer"}}),
            enabled: false,
        };

        let mut policies = HashMap::new();
        policies.insert("Person".to_string(), "Enrich person with external data".to_string());
        policies.insert("Company".to_string(), "Validate company info".to_string());

        let config = AgentConfig {
            enabled: true,
            provider: LLMProvider::ClaudeCode,
            model: "claude-3-opus".to_string(),
            api_key: Some("sk-ant-key".to_string()),
            api_base_url: None,
            system_prompt: Some("You are a graph enrichment agent.".to_string()),
            tools: vec![tool1, tool2],
            policies,
        };

        assert!(config.enabled);
        assert_eq!(config.tools.len(), 2);
        assert!(config.tools[0].enabled);
        assert!(!config.tools[1].enabled);
        assert_eq!(config.policies.len(), 2);
    }

    #[test]
    fn test_create_tenant_with_custom_quotas() {
        let manager = TenantManager::new();
        let quotas = ResourceQuotas {
            max_nodes: Some(100),
            max_edges: Some(200),
            max_memory_bytes: Some(1024 * 1024),
            max_storage_bytes: None,
            max_connections: Some(5),
            max_query_time_ms: Some(10_000),
        };

        manager.create_tenant("custom_t".to_string(), "Custom".to_string(), Some(quotas)).unwrap();

        let tenant = manager.get_tenant("custom_t").unwrap();
        assert_eq!(tenant.quotas.max_nodes, Some(100));
        assert_eq!(tenant.quotas.max_edges, Some(200));
        assert!(tenant.quotas.max_storage_bytes.is_none());
    }

    #[test]
    fn test_increment_decrement_all_resources() {
        let manager = TenantManager::new();

        // Increment all resource types
        manager.increment_usage("default", "nodes", 10).unwrap();
        manager.increment_usage("default", "edges", 20).unwrap();
        manager.increment_usage("default", "memory", 1000).unwrap();
        manager.increment_usage("default", "storage", 2000).unwrap();
        manager.increment_usage("default", "connections", 5).unwrap();

        let usage = manager.get_usage("default").unwrap();
        assert_eq!(usage.node_count, 10);
        assert_eq!(usage.edge_count, 20);
        assert_eq!(usage.memory_bytes, 1000);
        assert_eq!(usage.storage_bytes, 2000);
        assert_eq!(usage.active_connections, 5);

        // Decrement all
        manager.decrement_usage("default", "nodes", 3).unwrap();
        manager.decrement_usage("default", "edges", 5).unwrap();
        manager.decrement_usage("default", "memory", 200).unwrap();
        manager.decrement_usage("default", "storage", 500).unwrap();
        manager.decrement_usage("default", "connections", 2).unwrap();

        let usage = manager.get_usage("default").unwrap();
        assert_eq!(usage.node_count, 7);
        assert_eq!(usage.edge_count, 15);
        assert_eq!(usage.memory_bytes, 800);
        assert_eq!(usage.storage_bytes, 1500);
        assert_eq!(usage.active_connections, 3);
    }

    #[test]
    fn test_tenant_serialization_with_configs() {
        let mut tenant = Tenant::new("full".to_string(), "Full Tenant".to_string());
        tenant.embed_config = Some(AutoEmbedConfig {
            provider: LLMProvider::OpenAI,
            embedding_model: "text-embedding-3-small".to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 256,
            chunk_overlap: 32,
            vector_dimension: 1536,
            embedding_policies: HashMap::new(),
        });
        tenant.nlq_config = Some(NLQConfig {
            enabled: true,
            provider: LLMProvider::Mock,
            model: "mock".to_string(),
            api_key: None,
            api_base_url: None,
            system_prompt: None,
        });

        let json = serde_json::to_string(&tenant).unwrap();
        let deserialized: Tenant = serde_json::from_str(&json).unwrap();
        assert!(deserialized.embed_config.is_some());
        assert!(deserialized.nlq_config.is_some());
        assert_eq!(deserialized.id, "full");
    }

    #[test]
    fn test_resource_quotas_clone() {
        let quotas = ResourceQuotas {
            max_nodes: Some(42),
            max_edges: Some(84),
            max_memory_bytes: None,
            max_storage_bytes: None,
            max_connections: Some(10),
            max_query_time_ms: Some(5000),
        };

        let cloned = quotas.clone();
        assert_eq!(cloned.max_nodes, Some(42));
        assert_eq!(cloned.max_edges, Some(84));
        assert_eq!(cloned.max_connections, Some(10));
    }

    #[test]
    fn test_check_quota_storage_type_passes() {
        // Storage quota type is not explicitly enforced in check_quota match arms
        let manager = TenantManager::new();
        let result = manager.check_quota("default", "storage");
        assert!(result.is_ok());
    }
}
