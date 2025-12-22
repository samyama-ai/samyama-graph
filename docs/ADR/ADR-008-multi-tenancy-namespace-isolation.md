# ADR-008: Use Namespace Isolation for Multi-Tenancy

## Status
**Accepted**

## Date
2025-10-14

## Context

Samyama must support multiple isolated tenants on a single cluster:

1. **Data Isolation**: Tenants can't access each other's data
2. **Resource Quotas**: Limit memory, storage, query time per tenant
3. **Performance Isolation**: One tenant can't starve others
4. **Operational Simplicity**: Easy to manage

## Decision

**We will use namespace-based isolation with RocksDB column families and resource quotas.**

### Architecture

```mermaid
graph TB
    subgraph "Tenant 1 (4GB quota)"
        T1_NS[Namespace: tenant_1]
        T1_MEM[Memory: 3.2GB used]
        T1_CF[RocksDB CF: tenant_1]
    end

    subgraph "Tenant 2 (8GB quota)"
        T2_NS[Namespace: tenant_2]
        T2_MEM[Memory: 6.1GB used]
        T2_CF[RocksDB CF: tenant_2]
    end

    subgraph "Shared Infrastructure"
        QE[Query Engine]
        SE[Storage Engine]
        QM[Quota Manager]
    end

    T1_NS --> QM
    T2_NS --> QM
    QM --> QE
    QE --> SE
    SE --> T1_CF
    SE --> T2_CF

    style T1_NS fill:#ffe0e0
    style T2_NS fill:#e0ffe0
```

### Request Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant A as Auth
    participant Q as Quota Check
    participant E as Executor

    C->>A: GRAPH.QUERY tenant_1 "MATCH..."
    A->>A: Validate tenant credentials
    A->>Q: Check quotas

    alt Quota Available
        Q->>E: Execute in namespace
        E-->>C: Results
    else Quota Exceeded
        Q-->>C: 429 Too Many Requests
    end
```

## Rationale

### 1. RocksDB Column Families

Each tenant gets dedicated column family:
- **Logical isolation**: Separate keyspace
- **Independent compaction**: Tenant A compaction doesn't affect Tenant B
- **Easy backup/restore**: Per-tenant snapshots

### 2. Resource Quotas

```rust
struct TenantQuota {
    max_memory: usize,        // 4 GB
    max_storage: usize,       // 100 GB
    max_query_time: Duration, // 30 seconds
    max_connections: usize,   // 100
}

impl QuotaManager {
    fn check_quota(&self, tenant: &str, resource: Resource) -> Result<()> {
        let usage = self.get_usage(tenant);
        let quota = self.get_quota(tenant);

        if usage.exceeds(quota) {
            return Err(QuotaExceeded);
        }

        Ok(())
    }
}
```

## Consequences

✅ **Strong Isolation**: Tenants completely separated
✅ **Fair Resource Allocation**: Quotas prevent noisy neighbors
✅ **Simple Mental Model**: Easy to understand and operate
✅ **Scalable**: Tested with 100+ tenants per node

⚠️ **Not Full Physical Isolation**: Share CPU, network
- Mitigation: Monitor per-tenant metrics

⚠️ **Quota Enforcement Overhead**: ~0.1ms per query
- Acceptable trade-off

## Alternatives Considered

- **Separate Processes**: Too much overhead
- **Virtual Clusters**: Complex, overkill
- **No Isolation**: Unacceptable for security

**Verdict**: Namespace isolation is the sweet spot.

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
