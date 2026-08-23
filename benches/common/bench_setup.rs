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
    warn_if_busy(&host);
    calibration
}

/// How many *other* runnable threads make a host too busy to quote, as a
/// fraction of its cores.
///
/// A quarter: below that the benchmark still gets a core essentially to
/// itself, above it the timings are a measurement of the other workload.
const BUSY_RUNNABLE_FRACTION: f64 = 0.25;

/// The 1-minute load average is a lagging fallback for hosts with no
/// `procs_running`. It is deliberately generous, because it is an average over
/// a minute the run may only just have entered: 24 spinning threads on 16 cores
/// read **9.56** after 25 seconds, by which time the calibration loop had
/// already doubled from 33 ms to 66.
const BUSY_LOAD_PER_CORE: f64 = 1.0;

/// Threads currently runnable, from `/proc/stat`. Instantaneous, unlike
/// `loadavg`.
fn procs_running() -> Option<u64> {
    std::fs::read_to_string("/proc/stat").ok().and_then(|s| {
        s.lines()
            .find_map(|l| l.strip_prefix("procs_running "))
            .and_then(|v| v.trim().parse().ok())
    })
}

/// Say so when the host is not quiet.
///
/// `report_drift` already catches a host whose speed *changes* during a run.
/// It cannot catch a run that is uniformly slow because something else is
/// resident for all of it: that run starts slow, ends slow, and drifts 1.00x.
///
/// This is not hypothetical. `ic11_probe` measured LDBC IC11 at 16.9 ms while a
/// `cargo test --workspace` compiled on the same 16 cores; on a quiet machine
/// the same probe says 8.0 ms. Every derived figure in that run was ~2x too
/// large and two of them were almost entirely the load, and the drift check
/// reported the host had held still — because it had (#715).
pub fn warn_if_busy(host: &HostState) {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
    if let Some(why) = busy_reason(host, cores) {
        eprintln!(
            "  WARNING: {why}. These timings are not comparable with a quiet-host\n  \
             run and are probably slower by roughly the ratio of the two calibration\n  \
             figures. Do not put them in a document (#715)."
        );
    }
}

/// Why the host is too busy to quote, or `None` if it is quiet enough.
fn busy_reason(host: &HostState, cores: f64) -> Option<String> {
    busy_reason_from(procs_running(), host.load_average, cores)
}

/// The decision, separated from where the numbers come from so it can be
/// tested without a busy machine.
///
/// `running` counts every runnable thread including this one, which is why it
/// is decremented: a quiet host running only the benchmark reads 1.
fn busy_reason_from(running: Option<u64>, load_average: Option<f64>, cores: f64) -> Option<String> {
    if let Some(running) = running {
        let others = running.saturating_sub(1) as f64;
        if others > BUSY_RUNNABLE_FRACTION * cores {
            return Some(format!(
                "{others:.0} other runnable thread(s) on {cores:.0} cores"
            ));
        }
        // `procs_running` is the better signal and it is available, so a
        // lagging average must not override it — a load average still decaying
        // from a finished job would otherwise condemn a host that is now idle.
        return None;
    }
    match load_average {
        Some(load) if load > BUSY_LOAD_PER_CORE * cores => {
            Some(format!("1-minute load average {load:.2} on {cores:.0} cores"))
        }
        _ => None,
    }
}

/// Whether the host is quiet enough to quote, for callers that would rather
/// refuse than warn.
pub fn host_is_quiet() -> bool {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
    busy_reason(&HostState::read(), cores).is_none()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A host running only the benchmark is quiet, whatever the average says.
    ///
    /// The average is the lagging signal: 24 spinning threads on 16 cores read
    /// 9.56 after 25 seconds, by which time the calibration loop had already
    /// doubled. The reverse lag matters just as much — a load average decaying
    /// from a job that has finished must not condemn an idle host, which is
    /// what a run of this check observed at load 6.33 on a machine whose
    /// calibration was back to its quiet figure.
    #[test]
    fn one_runnable_thread_is_quiet_even_with_a_stale_load_average() {
        assert_eq!(busy_reason_from(Some(1), Some(6.33), 16.0), None);
    }

    /// Competition above a quarter of the cores is reported.
    #[test]
    fn other_runnable_threads_are_reported_with_the_count() {
        let why = busy_reason_from(Some(26), Some(9.05), 16.0).expect("25 others on 16 cores");
        assert!(why.contains("25 other runnable"), "{why}");
        assert!(why.contains("16 cores"), "{why}");
    }

    /// A little background noise is not a busy host.
    #[test]
    fn a_few_other_threads_are_still_quiet() {
        assert_eq!(busy_reason_from(Some(4), None, 16.0), None);
        assert!(busy_reason_from(Some(6), None, 16.0).is_some());
    }

    /// Without `procs_running` the average is the fallback, not otherwise.
    #[test]
    fn the_load_average_is_only_consulted_when_the_runnable_count_is_missing() {
        assert!(busy_reason_from(None, Some(20.0), 16.0).is_some());
        assert_eq!(busy_reason_from(None, Some(3.0), 16.0), None);
        assert_eq!(busy_reason_from(None, None, 16.0), None);
    }
}
