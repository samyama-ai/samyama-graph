//! The benchmark harness reports whether the host held still (#529).
//!
//! Every `[[bench]]` sets `harness = false`, so `cargo test --bench` runs the
//! benchmark's `main` rather than its `#[test]` functions. Pulling the module
//! into a normal test target is what makes these run.
//!
//! What is worth testing here is narrow but load-bearing: the calibration must
//! not be optimised away, it must be stable enough that a real change in host
//! speed stands out from its own noise, and the drift report must fire when
//! the two figures disagree. A calibration that silently folds to zero reports
//! a perfectly stable host forever.

#[path = "../benches/common/bench_setup.rs"]
mod bench_setup;

use std::time::Duration;

#[test]
fn the_calibration_loop_is_not_optimised_away() {
    // The failure this guards: LLVM folds the loop, `calibrate()` returns
    // ~0 ns, every run looks identical, and the one thing the figure exists to
    // detect becomes invisible.
    let elapsed = bench_setup::calibrate();
    assert!(
        elapsed > Duration::from_micros(500),
        "calibration took {elapsed:?} — the loop was almost certainly folded away"
    );
    // And it must not be so slow that adding it to every benchmark is a cost
    // of its own.
    assert!(
        elapsed < Duration::from_secs(5),
        "calibration took {elapsed:?}, which is too slow to run at the start and end of every suite"
    );
}

#[test]
fn the_calibration_is_repeatable_enough_to_be_a_signal() {
    // It only has to separate a real change in host speed from its own noise.
    // The drift it exists to catch was 1.74x; the threshold that warns is 10%.
    //
    // Compared across the *faster half* of the samples, not min against max.
    // A shared CI runner preempts: one sample in five can be several times the
    // others through no fault of the loop, and an earlier version of this test
    // asserted `max/min < 2.0` and failed on exactly that. The slow tail says
    // something about the runner, not about whether this figure can detect a
    // slower host — and a test that fails on the machine's neighbours teaches
    // people to re-run CI rather than to read it.
    //
    // The faster half is the least contaminated view available without a
    // quiet machine, and it still catches the failure that matters: a loop
    // that varies wildly cannot be a calibration whatever the environment.
    let mut samples: Vec<Duration> = (0..9).map(|_| bench_setup::calibrate()).collect();
    samples.sort();

    let fastest = samples[0].as_secs_f64();
    assert!(fastest > 0.0, "calibration returned zero: {samples:?}");

    let median = samples[samples.len() / 2].as_secs_f64();
    let spread = median / fastest;
    assert!(
        spread < 3.0,
        "calibration median is {spread:.2}x its minimum ({samples:?}); it cannot \
         distinguish a slower host from its own noise"
    );
}

#[test]
fn host_state_reports_what_it_can_and_says_so_when_it_cannot() {
    let state = bench_setup::HostState::read();
    let text = state.format();

    assert!(text.contains("load"), "{text}");
    assert!(text.contains("cpu"), "{text}");

    // On Linux both should resolve. Elsewhere they must read as unknown
    // rather than as a fabricated zero.
    #[cfg(target_os = "linux")]
    {
        assert!(state.load_average.is_some(), "load average should be readable on Linux");
        assert!(state.cpu_mhz.is_some(), "cpu MHz should be readable on Linux");
        assert!(!text.contains("unknown"), "{text}");
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert!(text.contains("unknown"), "{text}");
    }
}

#[test]
fn the_mean_frequency_is_not_a_single_cores_idle_reading() {
    // Reading core 0 alone gave 2,235 MHz at the start of a run and 400 MHz at
    // the end of the same run: an idle core, not a throttled machine. The mean
    // across cores should sit in a plausible band rather than at either
    // extreme.
    #[cfg(target_os = "linux")]
    {
        let Some(mhz) = bench_setup::HostState::read().cpu_mhz else {
            eprintln!("SKIP: no cpu MHz reported");
            return;
        };
        assert!(
            (100.0..10_000.0).contains(&mhz),
            "mean cpu frequency {mhz} MHz is outside any plausible range"
        );
    }
}

#[test]
fn reporting_calibration_returns_the_figure_it_printed() {
    // `report_calibration` hands its measurement back so the end of the suite
    // can compare against it. Returning something else would make the drift
    // report meaningless while still looking right.
    let reported = bench_setup::report_calibration();
    assert!(reported > Duration::ZERO);
    // Exercises the closing path too: it must not panic when the host held
    // still, which is the overwhelmingly common case.
    bench_setup::report_drift(reported);
}

#[test]
fn drift_reporting_survives_a_zero_baseline() {
    // A pathological input rather than a realistic one: the ratio divides by
    // the opening figure, and a panic inside the *reporting* of a benchmark
    // would lose the whole run's results.
    bench_setup::report_drift(Duration::ZERO);
}
