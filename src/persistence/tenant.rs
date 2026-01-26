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
    /// Embedding policies: Label -> Vec<PropertyKey>
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
}
