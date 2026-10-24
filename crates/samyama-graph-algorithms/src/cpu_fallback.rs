//! Why a GPU-eligible algorithm ran on the CPU, said out loud (HW-10, #1684).
//!
//! Four algorithms can run on a GPU: PageRank, CDLP, LCC and triangle count.
//! Each falls back to its CPU path, which stays the source of truth (ADR-025),
//! and none fails because a GPU is missing. What was missing is the *reason*:
//! a user on a GPU host running the default build got CPU execution with no
//! statement that a GPU was not used, and a GPU build on a host without a
//! device fell back just as quietly.
//!
//! Every fallback now goes through [`note_cpu_fallback`], which logs the
//! reason. Compiled in every build, not only under `--features gpu`, because
//! the default build is the artifact users install and is the one that most
//! needs to say it is not using a GPU.
//!
//! # How loudly
//!
//! A reason that cannot change while the process runs is logged once per
//! algorithm, so a server answering a thousand PageRank calls says it once,
//! not a thousand times:
//!
//! | Reason | Level | How often |
//! |---|---|---|
//! | [`CpuFallbackReason::NotCompiled`] | `info` | once per algorithm |
//! | [`CpuFallbackReason::NoDevice`] | `warn` | once per algorithm |
//! | [`CpuFallbackReason::BelowThreshold`] | `debug` | every call (expected for small graphs) |
//! | [`CpuFallbackReason::UnsupportedOption`] | `debug` | every call (the caller chose the option) |
//! | [`CpuFallbackReason::KernelFailed`] | `warn` | every call (a busy or lost device can recover) |
//!
//! "Once" means once *seen*: a permanent reason is only marked as reported when
//! a subscriber will record its event. A call made before logging is set up,
//! or while the `samyama::gpu` target is filtered out, does not use it up, so
//! the first call after logging is enabled still says it.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

/// The `tracing` target every fallback is logged on.
pub const LOG_TARGET: &str = "samyama::gpu";

/// The algorithms that have a GPU path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuAlgorithm {
    PageRank,
    Cdlp,
    Lcc,
    TriangleCount,
}

impl GpuAlgorithm {
    /// Every algorithm with a GPU path, in declaration order.
    pub const ALL: &'static [GpuAlgorithm] = &[
        GpuAlgorithm::PageRank,
        GpuAlgorithm::Cdlp,
        GpuAlgorithm::Lcc,
        GpuAlgorithm::TriangleCount,
    ];

    /// The name a user calls the algorithm by.
    pub fn name(self) -> &'static str {
        match self {
            GpuAlgorithm::PageRank => "PageRank",
            GpuAlgorithm::Cdlp => "CDLP",
            GpuAlgorithm::Lcc => "LCC",
            GpuAlgorithm::TriangleCount => "triangle count",
        }
    }

    /// This algorithm's position in [`GpuAlgorithm::ALL`], for per-algorithm state.
    fn index(self) -> usize {
        self as usize
    }
}

/// Why an algorithm that could run on a GPU ran on the CPU instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CpuFallbackReason {
    /// This binary was built without the `gpu` feature, so it has no GPU path.
    NotCompiled,
    /// The binary has a GPU path, but no GPU backend (CUDA or wgpu) is available.
    NoDevice,
    /// The graph is too small for the GPU to pay off on this backend.
    BelowThreshold {
        /// Nodes in the graph.
        nodes: usize,
        /// The graph must have more nodes than this to be sent to the GPU.
        min_nodes: usize,
    },
    /// The caller asked for an option the GPU kernel does not implement.
    UnsupportedOption(&'static str),
    /// The GPU kernel returned an error: the device was busy, lost, or failed.
    KernelFailed(String),
}

impl CpuFallbackReason {
    /// Whether this reason holds for the life of the process, and so is
    /// logged once per algorithm rather than on every call.
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            CpuFallbackReason::NotCompiled | CpuFallbackReason::NoDevice
        )
    }

    /// The level this reason is logged at.
    pub fn level(&self) -> FallbackLevel {
        match self {
            CpuFallbackReason::NotCompiled => FallbackLevel::Info,
            CpuFallbackReason::NoDevice | CpuFallbackReason::KernelFailed(_) => FallbackLevel::Warn,
            CpuFallbackReason::BelowThreshold { .. } | CpuFallbackReason::UnsupportedOption(_) => {
                FallbackLevel::Debug
            }
        }
    }
}

impl fmt::Display for CpuFallbackReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CpuFallbackReason::NotCompiled => {
                f.write_str("this build has no GPU support (built without the `gpu` feature)")
            }
            CpuFallbackReason::NoDevice => f.write_str("no GPU device is available"),
            CpuFallbackReason::BelowThreshold { nodes, min_nodes } => write!(
                f,
                "the graph has {nodes} nodes; the GPU is used above {min_nodes}"
            ),
            CpuFallbackReason::UnsupportedOption(option) => {
                write!(f, "the GPU kernel does not implement {option}")
            }
            CpuFallbackReason::KernelFailed(error) => write!(f, "the GPU kernel failed: {error}"),
        }
    }
}

/// How a fallback is logged: its level, and its message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackLevel {
    Debug,
    Info,
    Warn,
}

/// Remembers which algorithms have already reported a permanent reason, so it
/// is said once per algorithm. Separate from the global instance so tests can
/// start from a clean slate.
pub struct FallbackLog {
    reported: [AtomicBool; GpuAlgorithm::ALL.len()],
}

impl FallbackLog {
    pub const fn new() -> Self {
        Self {
            reported: [const { AtomicBool::new(false) }; GpuAlgorithm::ALL.len()],
        }
    }

    /// The log line this fallback should produce, or `None` when it has
    /// already been said. Pure apart from the once-per-algorithm memory, so it
    /// is what the tests check; [`note_cpu_fallback`] only emits it.
    pub fn message(
        &self,
        algorithm: GpuAlgorithm,
        reason: &CpuFallbackReason,
    ) -> Option<(FallbackLevel, String)> {
        if reason.is_permanent() && self.reported[algorithm.index()].swap(true, Ordering::Relaxed) {
            return None;
        }
        let level = reason.level();
        let once = if reason.is_permanent() {
            " (reported once per process)"
        } else {
            ""
        };
        Some((
            level,
            format!(
                "{} ran on the CPU, not a GPU: {reason}{once}",
                algorithm.name()
            ),
        ))
    }
}

impl FallbackLog {
    /// Whether `algorithm` has already reported a permanent reason.
    #[cfg(test)]
    fn has_reported(&self, algorithm: GpuAlgorithm) -> bool {
        self.reported[algorithm.index()].load(Ordering::Relaxed)
    }
}

impl Default for FallbackLog {
    fn default() -> Self {
        Self::new()
    }
}

impl FallbackLog {
    /// Log why `algorithm` ran on the CPU through `tracing`.
    ///
    /// Asks the current subscriber first, and does nothing when it would
    /// discard the event: a permanent reason's single report is only used up
    /// by an event that is actually recorded.
    pub fn note(&self, algorithm: GpuAlgorithm, reason: &CpuFallbackReason) {
        if !level_enabled(reason.level()) {
            return;
        }
        if let Some((level, message)) = self.message(algorithm, reason) {
            match level {
                FallbackLevel::Debug => tracing::debug!(target: LOG_TARGET, "{message}"),
                FallbackLevel::Info => tracing::info!(target: LOG_TARGET, "{message}"),
                FallbackLevel::Warn => tracing::warn!(target: LOG_TARGET, "{message}"),
            }
        }
    }
}

/// Whether the current subscriber records an event at `level` on [`LOG_TARGET`].
///
/// Asks about an *event* carrying a `message` field, as the one
/// [`FallbackLog::note`] emits, not the span-or-event hint `tracing::enabled!`
/// asks about: a subscriber that filters on `Metadata::is_event`, or on the
/// event's fields, must get the same answer here as for the event itself.
fn level_enabled(level: FallbackLevel) -> bool {
    match level {
        FallbackLevel::Debug => {
            tracing::event_enabled!(target: LOG_TARGET, tracing::Level::DEBUG, message)
        }
        FallbackLevel::Info => {
            tracing::event_enabled!(target: LOG_TARGET, tracing::Level::INFO, message)
        }
        FallbackLevel::Warn => {
            tracing::event_enabled!(target: LOG_TARGET, tracing::Level::WARN, message)
        }
    }
}

/// The process-wide record of what has been reported.
static FALLBACK_LOG: FallbackLog = FallbackLog::new();

/// Log why `algorithm` ran on the CPU. Every GPU-eligible algorithm calls
/// this on every path that does not use the GPU.
pub fn note_cpu_fallback(algorithm: GpuAlgorithm, reason: &CpuFallbackReason) {
    FALLBACK_LOG.note(algorithm, reason);
}

/// Report the fallback a build without the `gpu` feature always takes.
///
/// Called from each GPU-eligible algorithm in the default build, which has no
/// dispatch gate to ask.
#[cfg(not(feature = "gpu"))]
pub(crate) fn note_not_compiled(algorithm: GpuAlgorithm) {
    note_cpu_fallback(algorithm, &CpuFallbackReason::NotCompiled);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    /// A graph size and threshold for the below-threshold reason.
    const NODES: usize = 500;
    const MIN_NODES: usize = 1_000;

    /// What a [`Recorder`] lets through.
    type Filter = fn(&tracing::Metadata<'_>) -> bool;

    /// Everything on [`LOG_TARGET`].
    const ON_TARGET: Filter = |metadata| metadata.target() == LOG_TARGET;
    /// Nothing: the target is filtered out.
    const NOTHING: Filter = |_| false;
    /// Events only, never a span or a hint: `filter_fn(|m| m.is_event())`.
    const EVENTS_ONLY: Filter = |metadata| metadata.is_event();
    /// Only events with a `message` field, as every fallback event has.
    const WITH_MESSAGE: Filter = |metadata| metadata.fields().field("message").is_some();

    /// A subscriber that records the events its `filter` lets through, and
    /// counts them.
    struct Recorder {
        filter: Filter,
        recorded: Arc<AtomicUsize>,
    }

    impl Recorder {
        fn new(filter: Filter, recorded: &Arc<AtomicUsize>) -> Self {
            Self {
                filter,
                recorded: Arc::clone(recorded),
            }
        }
    }

    impl tracing::Subscriber for Recorder {
        fn register_callsite(
            &self,
            _: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            // Ask `enabled` on every event, so recorders that disagree never
            // share a cached answer for a callsite.
            tracing::subscriber::Interest::sometimes()
        }
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            (self.filter)(metadata)
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.recorded.fetch_add(1, Ordering::Relaxed);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[test]
    fn each_reason_is_logged_at_its_documented_level() {
        let cases = [
            (CpuFallbackReason::NotCompiled, FallbackLevel::Info),
            (CpuFallbackReason::NoDevice, FallbackLevel::Warn),
            (
                CpuFallbackReason::KernelFailed("device lost".to_string()),
                FallbackLevel::Warn,
            ),
            (
                CpuFallbackReason::BelowThreshold {
                    nodes: NODES,
                    min_nodes: MIN_NODES,
                },
                FallbackLevel::Debug,
            ),
            (
                CpuFallbackReason::UnsupportedOption("dangling redistribution"),
                FallbackLevel::Debug,
            ),
        ];
        for (reason, level) in cases {
            assert_eq!(reason.level(), level, "{reason:?}");
        }
    }

    #[test]
    fn a_permanent_reason_is_kept_until_a_subscriber_records_it() {
        let log = FallbackLog::new();
        let recorded = Arc::new(AtomicUsize::new(0));
        // Before any subscriber is installed, and while one filters the target out.
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            log.note(GpuAlgorithm::PageRank, &CpuFallbackReason::NoDevice)
        });
        tracing::subscriber::with_default(Recorder::new(NOTHING, &recorded), || {
            log.note(GpuAlgorithm::PageRank, &CpuFallbackReason::NoDevice)
        });
        assert!(
            !log.has_reported(GpuAlgorithm::PageRank),
            "an event nobody recorded must not use up the only report"
        );
        // Logging is enabled: the next call says it, and only that one.
        tracing::subscriber::with_default(Recorder::new(ON_TARGET, &recorded), || {
            log.note(GpuAlgorithm::PageRank, &CpuFallbackReason::NoDevice);
            log.note(GpuAlgorithm::PageRank, &CpuFallbackReason::NoDevice);
        });
        assert_eq!(recorded.load(Ordering::Relaxed), 1);
        assert!(log.has_reported(GpuAlgorithm::PageRank));
    }

    #[test]
    fn a_filter_that_accepts_the_event_gets_every_report() {
        // #1936 review: a subscriber that filters on event metadata must not
        // be refused by the check made before the event.
        for (name, filter) in [
            ("events only", EVENTS_ONLY),
            ("with a message", WITH_MESSAGE),
        ] {
            let log = FallbackLog::new();
            let recorded = Arc::new(AtomicUsize::new(0));
            tracing::subscriber::with_default(Recorder::new(filter, &recorded), || {
                log.note(GpuAlgorithm::Lcc, &CpuFallbackReason::NoDevice);
                log.note(
                    GpuAlgorithm::Lcc,
                    &CpuFallbackReason::KernelFailed("device lost".to_string()),
                );
                log.note(
                    GpuAlgorithm::Lcc,
                    &CpuFallbackReason::BelowThreshold {
                        nodes: NODES,
                        min_nodes: MIN_NODES,
                    },
                );
            });
            assert_eq!(recorded.load(Ordering::Relaxed), 3, "{name}");
            assert!(log.has_reported(GpuAlgorithm::Lcc), "{name}");
        }
    }

    #[test]
    fn a_per_call_reason_is_recorded_on_every_call() {
        let log = FallbackLog::new();
        let recorded = Arc::new(AtomicUsize::new(0));
        let reason = CpuFallbackReason::KernelFailed("device lost".to_string());
        tracing::subscriber::with_default(Recorder::new(ON_TARGET, &recorded), || {
            log.note(GpuAlgorithm::Cdlp, &reason);
            log.note(GpuAlgorithm::Cdlp, &reason);
        });
        assert_eq!(recorded.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn every_algorithm_has_its_own_slot() {
        for (i, algorithm) in GpuAlgorithm::ALL.iter().enumerate() {
            assert_eq!(algorithm.index(), i, "{algorithm:?}");
        }
    }

    #[test]
    fn a_build_without_gpu_support_says_so_once_per_algorithm() {
        let log = FallbackLog::new();
        let (level, message) = log
            .message(GpuAlgorithm::PageRank, &CpuFallbackReason::NotCompiled)
            .expect("the first call is reported");
        assert_eq!(level, FallbackLevel::Info);
        assert!(message.contains("PageRank ran on the CPU"), "{message}");
        assert!(message.contains("`gpu` feature"), "{message}");
        assert_eq!(
            log.message(GpuAlgorithm::PageRank, &CpuFallbackReason::NotCompiled),
            None,
            "a permanent reason is not repeated"
        );
        // Another algorithm still reports its own.
        assert!(log
            .message(GpuAlgorithm::Lcc, &CpuFallbackReason::NotCompiled)
            .is_some());
    }

    #[test]
    fn a_missing_device_is_a_warning_reported_once() {
        let log = FallbackLog::new();
        let (level, message) = log
            .message(GpuAlgorithm::Cdlp, &CpuFallbackReason::NoDevice)
            .expect("reported");
        assert_eq!(level, FallbackLevel::Warn);
        assert!(message.contains("no GPU device"), "{message}");
        assert_eq!(
            log.message(GpuAlgorithm::Cdlp, &CpuFallbackReason::NoDevice),
            None
        );
    }

    #[test]
    fn a_kernel_failure_is_a_warning_on_every_call() {
        let log = FallbackLog::new();
        let reason = CpuFallbackReason::KernelFailed("device lost".to_string());
        for _ in 0..2 {
            let (level, message) = log
                .message(GpuAlgorithm::TriangleCount, &reason)
                .expect("a device can recover, so every failure is reported");
            assert_eq!(level, FallbackLevel::Warn);
            assert!(message.contains("device lost"), "{message}");
        }
    }

    #[test]
    fn a_small_graph_is_reported_at_debug_with_both_numbers() {
        let log = FallbackLog::new();
        let reason = CpuFallbackReason::BelowThreshold {
            nodes: NODES,
            min_nodes: MIN_NODES,
        };
        let (level, message) = log.message(GpuAlgorithm::PageRank, &reason).unwrap();
        assert_eq!(level, FallbackLevel::Debug);
        assert!(message.contains(&NODES.to_string()), "{message}");
        assert!(message.contains(&MIN_NODES.to_string()), "{message}");
        assert!(
            log.message(GpuAlgorithm::PageRank, &reason).is_some(),
            "not once-only"
        );
    }

    #[test]
    fn an_unsupported_option_names_the_option() {
        let log = FallbackLog::new();
        let reason = CpuFallbackReason::UnsupportedOption("dangling redistribution");
        let (level, message) = log.message(GpuAlgorithm::PageRank, &reason).unwrap();
        assert_eq!(level, FallbackLevel::Debug);
        assert!(message.contains("dangling redistribution"), "{message}");
    }

    #[test]
    fn a_per_call_reason_does_not_use_up_the_permanent_report() {
        let log = FallbackLog::new();
        let small = CpuFallbackReason::BelowThreshold {
            nodes: NODES,
            min_nodes: MIN_NODES,
        };
        log.message(GpuAlgorithm::Lcc, &small);
        assert!(
            log.message(GpuAlgorithm::Lcc, &CpuFallbackReason::NoDevice)
                .is_some(),
            "a small graph first must not silence the missing-device warning"
        );
    }

    /// Every node joined to every other: the smallest graph each of the four
    /// algorithms has something to compute on.
    #[cfg(not(feature = "gpu"))]
    fn triangle() -> crate::common::GraphView {
        let nodes: Vec<crate::common::NodeId> = (0..3).collect();
        let node_to_index = nodes.iter().enumerate().map(|(i, &n)| (n, i)).collect();
        let others = |i: usize| (0..nodes.len()).filter(|&j| j != i).collect::<Vec<_>>();
        let adjacency: Vec<Vec<usize>> = (0..nodes.len()).map(others).collect();
        crate::common::GraphView::from_adjacency_list(
            nodes.len(),
            nodes.clone(),
            node_to_index,
            adjacency.clone(),
            adjacency,
            None,
        )
    }

    /// The wiring, not the wording: each GPU-eligible algorithm, run in a
    /// build without the `gpu` feature, reports its fallback. Without this a
    /// call site could drop its `note_not_compiled` and every test above would
    /// still pass.
    #[cfg(not(feature = "gpu"))]
    #[test]
    fn every_gpu_eligible_algorithm_reports_its_fallback_in_the_default_build() {
        let view = triangle();
        // A report only counts once a subscriber records it.
        let recorded = Arc::new(AtomicUsize::new(0));
        tracing::subscriber::with_default(Recorder::new(ON_TARGET, &recorded), || {
            crate::pagerank::page_rank(&view, crate::pagerank::PageRankConfig::default());
            crate::cdlp::cdlp(&view, &crate::cdlp::CdlpConfig::default());
            crate::lcc::local_clustering_coefficient(&view);
            crate::topology::count_triangles(&view);
        });
        for &algorithm in GpuAlgorithm::ALL {
            assert!(
                FALLBACK_LOG.has_reported(algorithm),
                "{} said nothing",
                algorithm.name()
            );
        }
    }

    #[test]
    fn the_global_log_emits_without_panicking() {
        // With no subscriber installed this is a no-op; it must still not panic.
        // A per-call reason, deliberately: a permanent one would mark the
        // global log as reported and hide a call site that never reports.
        note_cpu_fallback(
            GpuAlgorithm::PageRank,
            &CpuFallbackReason::KernelFailed("test".to_string()),
        );
    }
}
