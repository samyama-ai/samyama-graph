//! The default tenant's quotas can be raised from the command line (#1483).
//!
//! A stock server stopped accepting writes at 1,000,000 nodes and nothing a
//! user could reach changed it: `ResourceQuotas::unlimited()` was called only
//! from tests, `TenantManager::update_quotas` had no HTTP route, and there was
//! no environment variable. Three scale requirements — PERF-11, PERF-12 and
//! ALGO-07 — are written against 1B edges, so they were unmeasurable on any
//! hardware for a reason that had nothing to do with the machine.
//!
//! These tests are against the parsing and the manager, not against a running
//! server: the server-level behaviour is one `update_quotas` call away from
//! what is asserted here, and a test that boots a server to push a million
//! nodes would cost a minute to say the same thing.
//!
//! What is deliberately *not* changed: the shipped defaults. An unset flag must
//! leave `ResourceQuotas::default()` exactly as it was, which is why
//! `quota_arg` distinguishes "absent" from "present and unlimited" — `None`
//! versus `Some(None)` — rather than folding both into a missing value.

use samyama::persistence::tenant::{ResourceQuotas, TenantManager};

#[test]
fn unlimited_means_no_ceiling_and_default_means_the_shipped_one() {
    let shipped = ResourceQuotas::default();
    assert_eq!(shipped.max_nodes, Some(1_000_000), "the shipped default moved");
    assert_eq!(shipped.max_edges, Some(10_000_000), "the shipped default moved");

    let unlimited = ResourceQuotas::unlimited();
    assert_eq!(unlimited.max_nodes, None);
    assert_eq!(unlimited.max_edges, None);
}

#[test]
fn update_quotas_on_the_default_tenant_takes_effect() {
    let m = TenantManager::new();

    let raised = ResourceQuotas { max_nodes: Some(5_000_000), ..ResourceQuotas::default() };
    m.update_quotas("default", raised).expect("default tenant must exist");

    let t = m.get_tenant("default").expect("default tenant must exist");
    assert_eq!(
        t.quotas.max_nodes,
        Some(5_000_000),
        "the ceiling the server enforces is the one that was set"
    );
    assert_eq!(
        t.quotas.max_edges,
        Some(10_000_000),
        "a field the flag did not name must keep its shipped value"
    );
}

#[test]
fn a_missing_tenant_is_an_error_rather_than_a_silent_no_op() {
    let m = TenantManager::new();
    // The server exits non-zero on this rather than starting with the ceiling
    // the operator asked for silently not applied.
    assert!(m.update_quotas("no-such-tenant", ResourceQuotas::unlimited()).is_err());
}
