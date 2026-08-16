//! Shared benchmark setup: GPU status reporting.
//!
//! Each benchmark includes this via `#[path = "common/bench_setup.rs"] mod bench_setup;`
//! and calls `bench_setup::init()` at the top of `main()`.
//!
//! In OSS there is no license gate: GPU acceleration is on by default whenever a
//! GPU is present and the `gpu` feature is compiled in. Set `SAMYAMA_GPU=off` to
//! force the CPU path (baseline measurement or driver escape hatch).

/// Report GPU availability for benchmarks. No-op in a CPU-only build.
pub fn init() {
    #[cfg(feature = "gpu")]
    {
        if !samyama_gpu::GpuContext::is_enabled() {
            println!("[bench] GPU disabled via SAMYAMA_GPU=off — running CPU path.");
        } else if samyama_gpu::GpuContext::is_available() {
            println!("[bench] GPU acceleration: ENABLED (hardware detected).");
        } else {
            println!("[bench] GPU feature built, but no GPU hardware detected — CPU path.");
        }
    }

    #[cfg(not(feature = "gpu"))]
    {
        println!("[bench] Built without the `gpu` feature — CPU only.");
        println!("[bench] Rebuild with `--features gpu` for GPU-accelerated benchmarks.");
    }
}

// ---------------------------------------------------------------- host state
//
// A benchmark that reports absolute milliseconds and nothing else cannot tell
// a code change from a slower host. On this workstation the *same binary* ran
// LDBC IC9 in 2,822 ms in the morning and 4,912 ms the same evening -- 1.74x,
// no code change, nothing else on the CPU -- and two consecutive runs of one
// binary differed by 24% (#529).
//
// That is not a curiosity. `SLT-2` is a ratio against a competitor, and a
// ratio between numbers taken on a drifting host is not a ratio; a nightly
// regression gate set below that drift pages on noise, and one set above it
// misses real regressions. The fix is not to make the host stable -- it is to
// report enough that a reader can see when it was not.

use std::time::{Duration, Instant};

/// A fixed, CPU-bound unit of work, timed. Touches no memory beyond a register
/// and no I/O, so it measures core throughput and nothing else.
///
/// The point is comparability, not realism: two runs whose calibration differs
/// materially were taken on hosts of different speed, whatever their timings
/// say. Compare this figure before comparing anything else.
pub fn calibrate() -> Duration {
    // `black_box` on the accumulator stops LLVM folding the loop away, which
    // it will otherwise do -- a calibration that optimises to nothing reports
    // a stable zero and hides exactly what it exists to reveal.
    let started = Instant::now();
    let mut acc: u64 = 0x9E3779B97F4A7C15;
    for i in 0..20_000_000u64 {
        acc = acc.wrapping_mul(6364136223846793005).wrapping_add(i | 1);
        acc ^= acc >> 29;
    }
    std::hint::black_box(acc);
    started.elapsed()
}

/// What the machine was doing, as far as it will say.
///
/// Linux-only in the parts that need `/proc`; elsewhere the fields it cannot
/// answer are reported as unknown rather than guessed.
pub struct HostState {
    pub load_average: Option<f64>,
    pub cpu_mhz: Option<f64>,
}

impl HostState {
    pub fn read() -> Self {
        let load_average = std::fs::read_to_string("/proc/loadavg")
            .ok()
            .and_then(|s| s.split_whitespace().next()?.parse().ok());

        // Mean current frequency across all cores.
        //
        // Not the first core's: `/proc/cpuinfo` reports an instantaneous
        // value, and an idle core reads a few hundred MHz while the core
        // actually running the benchmark is at full speed. Reading core 0
        // alone produced 2,235 MHz at the start of a run and 400 MHz at the
        // end of the same run, which says nothing about the machine.
        //
        // Even averaged this is a weak signal next to `calibrate()`. It is
        // here because a host that has thermally capped shows it here first.
        let cpu_mhz = std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
            let readings: Vec<f64> = s
                .lines()
                .filter(|l| l.starts_with("cpu MHz"))
                .filter_map(|l| l.split(':').nth(1)?.trim().parse().ok())
                .collect();
            (!readings.is_empty()).then(|| readings.iter().sum::<f64>() / readings.len() as f64)
        });

        HostState { load_average, cpu_mhz }
    }

    pub fn format(&self) -> String {
        let load = self
            .load_average
            .map(|l| format!("{:.2}", l))
            .unwrap_or_else(|| "unknown".into());
        let mhz = self
            .cpu_mhz
            .map(|m| format!("{:.0} MHz mean", m))
            .unwrap_or_else(|| "unknown".into());
        format!("load {load}, cpu {mhz}")
    }
}

/// Print the calibration and host state that a reader needs before believing
/// any absolute figure in the run.
///
/// Called at the start of a suite; call [`report_drift`] at the end with the
/// value returned here.
pub fn report_calibration() -> Duration {
    let host = HostState::read();
    let calibration = calibrate();
    eprintln!(
        "Host calibration: {:.0} ms for a fixed CPU-bound loop ({})",
        calibration.as_secs_f64() * 1000.0,
        host.format()
    );
    eprintln!(
        "  Compare this before comparing timings: two runs whose calibration differs\n  \
         were taken on hosts of different speed, whatever their milliseconds say (#529)."
    );
    calibration
}

/// Re-measure at the end of a suite and say whether the host held still.
pub fn report_drift(before: Duration) {
    let host = HostState::read();
    let after = calibrate();
    let ratio = if before.is_zero() {
        1.0
    } else {
        after.as_secs_f64() / before.as_secs_f64()
    };
    eprintln!(
        "Host calibration after the suite: {:.0} ms ({}) — {:.2}x the opening figure",
        after.as_secs_f64() * 1000.0,
        host.format(),
        ratio
    );
    // 10% is well inside what this box does when it is behaving and well
    // outside what a stable one should do across a two-minute suite.
    if !(0.9..=1.1).contains(&ratio) {
        eprintln!(
            "  WARNING: the host changed speed by {:.0}% during the run. Timings from the\n  \
             start and the end of this suite are not comparable with each other, and none\n  \
             of them is comparable with another session (#529).",
            (ratio - 1.0).abs() * 100.0
        );
    }
}
