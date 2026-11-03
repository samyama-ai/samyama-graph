//! GPU smoke test + the require-or-skip gate (workstream Phase 1, §4).
//!
//! Today every GPU test silently returns when no adapter is found, so they pass
//! green while asserting nothing. `SAMYAMA_REQUIRE_GPU=1` turns "no GPU" into a
//! hard failure — set it on the CI GPU lane so a broken GPU path cannot pass.
//! Machines without a GPU (laptops, GitHub-hosted CI) still skip locally.

use samyama_gpu::GpuContext;

/// Return the global GPU context, or skip the test — but **panic** instead of
/// skipping when `SAMYAMA_REQUIRE_GPU=1`. This is the primitive every GPU test
/// must route through so CI cannot silently pass with no GPU.
pub fn require_or_skip() -> Option<&'static GpuContext> {
    match GpuContext::try_global() {
        Some(ctx) => Some(ctx),
        None => {
            let require = std::env::var("SAMYAMA_REQUIRE_GPU").as_deref() == Ok("1");
            assert!(
                !require,
                "SAMYAMA_REQUIRE_GPU=1 but no GPU context is available \
                 (no adapter, or SAMYAMA_GPU=off). Failing instead of skipping."
            );
            eprintln!(
                "[gpu-test] no GPU context — skipping. Set SAMYAMA_REQUIRE_GPU=1 to make this a failure."
            );
            None
        }
    }
}

#[test]
fn gpu_context_smoke() {
    let Some(ctx) = require_or_skip() else {
        return;
    };
    assert!(!ctx.adapter_name().is_empty());
    eprintln!("[gpu-test] adapter: {} ({:?})", ctx.adapter_name(), ctx.backend());
}

#[test]
fn kill_switch_contract() {
    // `is_enabled()` must honor `SAMYAMA_GPU=off|0|false`. We do not mutate the
    // process env here (it would race the other tests); we assert the contract
    // against whatever the current env is. CI covers the off-case by setting the
    // var for a dedicated run.
    let off = matches!(
        std::env::var("SAMYAMA_GPU").ok().as_deref(),
        Some("off") | Some("0") | Some("false")
    );
    assert_eq!(GpuContext::is_enabled(), !off);
}
