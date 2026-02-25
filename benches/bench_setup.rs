//! Shared benchmark setup: license loading and GPU enablement.
//!
//! Each benchmark includes this via `#[path = "bench_setup.rs"] mod bench_setup;`
//! and calls `bench_setup::init()` at the top of `main()`.
//!
//! License resolution order (same as src/main.rs in enterprise):
//!   1. `SAMYAMA_LICENSE_KEY` env var (raw token)
//!   2. `SAMYAMA_LICENSE_FILE` env var (path to file)
//!   3. `./samyama.license` default file
//!
//! No license = Community mode (CPU only, no error).

/// Initialize enterprise features for benchmarks.
///
/// In the open-source build (no `license` module, no `gpu` feature),
/// this is a no-op that prints a community-mode message.
pub fn init() {
    #[cfg(feature = "gpu")]
    {
        init_enterprise();
    }

    #[cfg(not(feature = "gpu"))]
    {
        println!("[bench] Community edition — GPU acceleration not available.");
        println!("[bench] Build with --features gpu and provide a license for GPU benchmarks.");
    }
}

#[cfg(feature = "gpu")]
fn init_enterprise() {
    // Step 1: Resolve license token
    let license_token = resolve_license_token();

    // Step 2: Validate license
    match license_token {
        Some(token) => {
            match samyama::license::License::from_token(&token) {
                Ok(lic) => {
                    if lic.is_valid() {
                        println!("[bench] License: {}", lic.status_summary());

                        // Step 3: Enable GPU if licensed
                        if lic.feature_enabled("gpu") {
                            samyama_gpu::GpuContext::enable_licensed();
                            if samyama_gpu::GpuContext::is_available() {
                                println!("[bench] GPU acceleration: ENABLED");
                            } else {
                                println!("[bench] GPU acceleration: licensed but no GPU hardware detected");
                            }
                        } else {
                            println!("[bench] GPU feature not included in license. Running CPU only.");
                        }
                    } else {
                        println!("[bench] License invalid or expired. Running in Community mode (CPU only).");
                    }
                }
                Err(e) => {
                    println!("[bench] Invalid license: {}. Running in Community mode (CPU only).", e);
                }
            }
        }
        None => {
            println!("[bench] No license found. Running in Community mode (CPU only).");
        }
    }
}

#[cfg(feature = "gpu")]
fn resolve_license_token() -> Option<String> {
    // 1. SAMYAMA_LICENSE_KEY env var (raw token)
    if let Ok(key) = std::env::var("SAMYAMA_LICENSE_KEY") {
        if !key.is_empty() {
            return Some(key);
        }
    }

    // 2. SAMYAMA_LICENSE_FILE env var (path to file)
    if let Ok(path) = std::env::var("SAMYAMA_LICENSE_FILE") {
        if let Ok(contents) = std::fs::read_to_string(&path) {
            let token = contents.trim().to_string();
            if !token.is_empty() {
                return Some(token);
            }
        }
    }

    // 3. Default file: ./samyama.license
    if let Ok(contents) = std::fs::read_to_string("samyama.license") {
        let token = contents.trim().to_string();
        if !token.is_empty() {
            return Some(token);
        }
    }

    None
}
