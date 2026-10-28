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
//! "Once" means once per logging setup, not once forever:
//!
//! - A call the current subscriber would not record at all (none is installed,
//!   or the `samyama::gpu` target is filtered out) does not use up the report.
//! - Whenever the logging setup changes -- a subscriber is installed, or a
//!   filter is reloaded -- every once-only report is re-armed, so the next call
//!   says it again to whoever is listening now.
//!
//! The second rule is there because the first cannot be complete. A subscriber
//! can turn an event down at the last step, from its field values
//! (`Subscriber::event_enabled`), and `tracing` does not tell the caller: the
//! check is not public, and dispatching an event returns nothing. So a report
//! can be used up by an event that was then discarded. `tracing` does announce
//! every change of setup to registered callsites (`Callsite::set_interest`),
//! and [`LoggingChanges`] listens for it, so such a report comes back as soon
//! as the setup that discarded it is replaced. A subscriber that changes its
//! answer without telling `tracing` cannot be detected by anyone.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use tracing_core::callsite::{Callsite, Identifier};
use tracing_core::field::FieldSet;
use tracing_core::metadata::Kind;
use tracing_core::subscriber::Interest;
use tracing_core::{Level, Metadata};

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

/// How many times the logging setup has changed, as `tracing` announces it.
///
/// Every once-only report is tied to the value this had when it was made, so a
/// change re-arms them all. See [`LoggingChanges`].
static LOGGING_SETUP: AtomicU64 = AtomicU64::new(0);

/// Never reported in this setup. `LOGGING_SETUP` is stored plus one, so zero
/// is free to mean this.
const NEVER_REPORTED: u64 = 0;

/// Remembers which algorithms have reported a permanent reason in the current
/// logging setup, so it is said once per algorithm per setup.
///
/// Separate from the global instance so tests can start from a clean slate
/// and count setups of their own.
pub struct FallbackLog {
    /// Per algorithm, the setup it last reported in, plus one.
    reported: [AtomicU64; GpuAlgorithm::ALL.len()],
    /// The counter of logging setups this log follows.
    setup: &'static AtomicU64,
}

impl FallbackLog {
    pub const fn new() -> Self {
        Self::following(&LOGGING_SETUP)
    }

    /// A log that counts logging setups with `setup` instead of the
    /// process-wide counter.
    const fn following(setup: &'static AtomicU64) -> Self {
        Self {
            reported: [const { AtomicU64::new(NEVER_REPORTED) }; GpuAlgorithm::ALL.len()],
            setup,
        }
    }

    /// The current setup, as stored in `reported`.
    fn this_setup(&self) -> u64 {
        self.setup.load(Ordering::Acquire) + 1
    }

    /// Whether this call may report: always, for a reason that can change;
    /// for a permanent one, only the first time in this logging setup.
    ///
    /// The swap is the claim, so two threads that race on the first call do
    /// not both report.
    fn claim(&self, algorithm: GpuAlgorithm, reason: &CpuFallbackReason) -> bool {
        if !reason.is_permanent() {
            return true;
        }
        let now = self.this_setup();
        self.reported[algorithm.index()].swap(now, Ordering::AcqRel) != now
    }

    /// The level and text this fallback is logged with. Claims nothing.
    fn render(
        &self,
        algorithm: GpuAlgorithm,
        reason: &CpuFallbackReason,
    ) -> (FallbackLevel, String) {
        let once = if reason.is_permanent() {
            " (reported once per logging setup)"
        } else {
            ""
        };
        (
            reason.level(),
            format!(
                "{} ran on the CPU, not a GPU: {reason}{once}",
                algorithm.name()
            ),
        )
    }

    /// The log line this fallback should produce, or `None` when it has
    /// already been said in this logging setup. [`FallbackLog::note`] emits it.
    pub fn message(
        &self,
        algorithm: GpuAlgorithm,
        reason: &CpuFallbackReason,
    ) -> Option<(FallbackLevel, String)> {
        self.claim(algorithm, reason)
            .then(|| self.render(algorithm, reason))
    }
}

impl FallbackLog {
    /// Whether `algorithm` has reported a permanent reason in this setup.
    #[cfg(test)]
    fn has_reported(&self, algorithm: GpuAlgorithm) -> bool {
        self.reported[algorithm.index()].load(Ordering::Acquire) == self.this_setup()
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
    /// discard the event by its metadata, so such a call does not use up a
    /// permanent reason's report. A report used up by an event the subscriber
    /// discards later, from its fields, comes back when the logging setup
    /// changes (see the module docs).
    pub fn note(&self, algorithm: GpuAlgorithm, reason: &CpuFallbackReason) {
        watch_logging_changes();
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

/// A callsite that is never logged from, registered so that `tracing` tells it
/// whenever the logging setup changes.
///
/// `tracing` calls `set_interest` on every registered callsite when one is
/// registered, when a subscriber is created (`Dispatch::new`, which installing
/// one goes through), and when interest is rebuilt
/// (`callsite::rebuild_interest_cache`, which a reloaded filter calls). Each
/// call moves [`LOGGING_SETUP`] on, which re-arms every once-only report.
struct LoggingChanges;

static LOGGING_CHANGES: LoggingChanges = LoggingChanges;

/// A hint, not an event: nothing is ever emitted from this callsite.
static LOGGING_CHANGES_META: Metadata<'static> = Metadata::new(
    "cpu fallback: logging setup",
    LOG_TARGET,
    Level::TRACE,
    Some(file!()),
    Some(line!()),
    Some(module_path!()),
    FieldSet::new(&[], Identifier(&LOGGING_CHANGES)),
    Kind::HINT,
);

impl Callsite for LoggingChanges {
    fn set_interest(&self, _: Interest) {
        LOGGING_SETUP.fetch_add(1, Ordering::AcqRel);
    }

    fn metadata(&self) -> &Metadata<'_> {
        &LOGGING_CHANGES_META
    }
}

static WATCHING: Once = Once::new();

/// Register [`LoggingChanges`], once per process.
fn watch_logging_changes() {
    WATCHING.call_once(|| tracing_core::callsite::register(&LOGGING_CHANGES));
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
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::{Arc, Mutex};

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

    /// A log with a logging-setup counter of its own, so a test's "said once"
    /// is not undone by another test installing a subscriber meanwhile.
    fn private_log() -> (FallbackLog, &'static AtomicU64) {
        let setup: &'static AtomicU64 = Box::leak(Box::new(AtomicU64::new(0)));
        (FallbackLog::following(setup), setup)
    }

    /// A subscriber that records the events its `filter` lets through, keeps
    /// their messages, and can turn events down at the last step, as
    /// `Subscriber::event_enabled` may.
    struct Recorder {
        filter: Filter,
        recorded: Arc<AtomicUsize>,
        messages: Arc<Mutex<Vec<String>>>,
        /// `event_enabled`'s answer; accepts everything when `None`.
        accept_events: Option<Arc<AtomicBool>>,
    }

    impl Recorder {
        fn new(filter: Filter, recorded: &Arc<AtomicUsize>) -> Self {
            Self {
                filter,
                recorded: Arc::clone(recorded),
                messages: Arc::default(),
                accept_events: None,
            }
        }

        /// Accepts every event's metadata, then decides from `accept`.
        fn deciding_late(recorded: &Arc<AtomicUsize>, accept: &Arc<AtomicBool>) -> Self {
            Self {
                accept_events: Some(Arc::clone(accept)),
                ..Self::new(ON_TARGET, recorded)
            }
        }
    }

    /// Collects an event's `message` field.
    struct MessageText<'a>(&'a mut String);

    impl tracing::field::Visit for MessageText<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            if field.name() == "message" {
                *self.0 = format!("{value:?}");
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
        fn event_enabled(&self, _: &tracing::Event<'_>) -> bool {
            self.accept_events
                .as_ref()
                .is_none_or(|accept| accept.load(Ordering::Relaxed))
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            self.recorded.fetch_add(1, Ordering::Relaxed);
            let mut text = String::new();
            event.record(&mut MessageText(&mut text));
            self.messages.lock().unwrap().push(text);
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
        let (log, _) = private_log();
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
            let (log, _) = private_log();
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
        let (log, _) = private_log();
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
        let (log, _) = private_log();
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
        let (log, _) = private_log();
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
        let (log, _) = private_log();
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
        let (log, _) = private_log();
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
        let (log, _) = private_log();
        let reason = CpuFallbackReason::UnsupportedOption("dangling redistribution");
        let (level, message) = log.message(GpuAlgorithm::PageRank, &reason).unwrap();
        assert_eq!(level, FallbackLevel::Debug);
        assert!(message.contains("dangling redistribution"), "{message}");
    }

    #[test]
    fn a_per_call_reason_does_not_use_up_the_permanent_report() {
        let (log, _) = private_log();
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
        // Read from what this test's own subscriber recorded, not from the
        // global log's state: other tests install subscribers concurrently,
        // and each of those starts a new logging setup.
        let recorded = Arc::new(AtomicUsize::new(0));
        let recorder = Recorder::new(ON_TARGET, &recorded);
        let messages = Arc::clone(&recorder.messages);
        tracing::subscriber::with_default(recorder, || {
            crate::pagerank::page_rank(&view, crate::pagerank::PageRankConfig::default());
            crate::cdlp::cdlp(&view, &crate::cdlp::CdlpConfig::default());
            crate::lcc::local_clustering_coefficient(&view);
            crate::topology::count_triangles(&view);
        });
        let messages = messages.lock().unwrap();
        for &algorithm in GpuAlgorithm::ALL {
            let said = format!("{} ran on the CPU", algorithm.name());
            assert!(
                messages.iter().any(|m| m.starts_with(&said)),
                "{} said nothing: {messages:?}",
                algorithm.name()
            );
        }
    }

    #[test]
    fn a_report_the_subscriber_turns_down_late_comes_back_with_the_next_setup() {
        // #1936 review: a subscriber can accept an event's metadata and still
        // turn the event down in `event_enabled`, from its fields. `tracing`
        // does not tell the caller, so that report is used up; it must come
        // back once the logging setup changes.
        let (log, setup) = private_log();
        let recorded = Arc::new(AtomicUsize::new(0));
        let accept = Arc::new(AtomicBool::new(false));
        tracing::subscriber::with_default(Recorder::deciding_late(&recorded, &accept), || {
            log.note(GpuAlgorithm::TriangleCount, &CpuFallbackReason::NoDevice);
            assert_eq!(recorded.load(Ordering::Relaxed), 0, "turned down");

            // The same setup starts accepting: nothing announced the change,
            // so there is nothing to act on, and the report stays used.
            accept.store(true, Ordering::Relaxed);
            log.note(GpuAlgorithm::TriangleCount, &CpuFallbackReason::NoDevice);
            assert_eq!(recorded.load(Ordering::Relaxed), 0);

            // The setup changes, as `set_interest` announces: reported again,
            // once.
            setup.fetch_add(1, Ordering::AcqRel);
            log.note(GpuAlgorithm::TriangleCount, &CpuFallbackReason::NoDevice);
            log.note(GpuAlgorithm::TriangleCount, &CpuFallbackReason::NoDevice);
        });
        assert_eq!(recorded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn installing_a_subscriber_or_reloading_a_filter_starts_a_new_logging_setup() {
        // What makes the rule above happen in a real process: `tracing`
        // announces both to `LoggingChanges`. Compared as "moved on", not by
        // how far, because other tests install subscribers at the same time.
        // Registered the way a real process registers it, by reporting a
        // fallback, so a `note` that stopped registering would fail here.
        note_cpu_fallback(
            GpuAlgorithm::PageRank,
            &CpuFallbackReason::KernelFailed("test".to_string()),
        );
        let before = LOGGING_SETUP.load(Ordering::Acquire);
        let _installed = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let after_install = LOGGING_SETUP.load(Ordering::Acquire);
        assert!(after_install > before, "a new subscriber was not announced");
        tracing_core::callsite::rebuild_interest_cache();
        assert!(
            LOGGING_SETUP.load(Ordering::Acquire) > after_install,
            "a reloaded filter was not announced"
        );
    }

    #[test]
    fn a_new_logging_setup_re_arms_every_once_only_report() {
        let (log, setup) = private_log();
        for &algorithm in GpuAlgorithm::ALL {
            assert!(log
                .message(algorithm, &CpuFallbackReason::NoDevice)
                .is_some());
            assert!(log
                .message(algorithm, &CpuFallbackReason::NoDevice)
                .is_none());
        }
        setup.fetch_add(1, Ordering::AcqRel);
        for &algorithm in GpuAlgorithm::ALL {
            assert!(
                log.message(algorithm, &CpuFallbackReason::NoDevice)
                    .is_some(),
                "{} was not re-armed",
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
