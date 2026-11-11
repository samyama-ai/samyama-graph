//! `SamyamaEnv`: a deterministic, verifiably scored environment over a pinned
//! `.sgsnap`, for agent evaluation and reinforcement learning with verifiable
//! rewards (#2095; AI-18, EVAL-05).
//!
//! An episode is: `reset` hands the agent a task and a fresh copy of the graph,
//! `step` runs one action (a Cypher statement, or the final answer), and the
//! answer is scored by `verify` with an exact reward in `[0, 1]`.
//!
//! ```no_run
//! use samyama_sdk::env::{Action, SamyamaEnv};
//!
//! let mut env = SamyamaEnv::open("cricket.sgsnap", "tasks.jsonl", 42).unwrap();
//! let first = env.reset(None).unwrap();
//! println!("{}", first.prompt);
//! let out = env.step(Action::Query("MATCH (p:Player) RETURN p.name LIMIT 3".into())).unwrap();
//! let done = env.step(Action::Answer(vec!["V Kohli".into()])).unwrap();
//! println!("reward {}", done.reward);
//! ```
//!
//! # What makes the reward trustworthy
//!
//! - **The answer key never reaches the agent's graph.** A read task's gold
//!   answer comes from an oracle query run once, at construction, against a
//!   separate copy of the snapshot the agent never touches. Writing to its own
//!   graph cannot change what it is scored against (KG-15).
//! - **Episodes do not leak into each other.** `reset` restores the graph from
//!   the pinned snapshot whenever the previous episode changed it, so a write
//!   in one episode is gone in the next.
//! - **Same seed, same run.** Which task `reset(None)` picks is drawn from the
//!   seed, the engine's answers do not depend on hash order (#1509), and the
//!   reward is computed from values alone. Timings are reported in `info` and
//!   never feed the reward.
//!
//! # Tasks file
//!
//! One JSON object per line. A task has an `answer`, `postconditions`, or both:
//!
//! ```text
//! {"id": "q1", "prompt": "Who does Alice know?",
//!  "answer": {"oracle": "MATCH (:Person {name: 'Alice'})-[:KNOWS]->(p) RETURN p.name"}}
//! {"id": "q2", "prompt": "Name two founders.", "answer": {"values": ["Ada", "Alan"]}}
//! {"id": "w1", "prompt": "Mark Bob inactive.",
//!  "postconditions": [{"query": "MATCH (p:Person {name: 'Bob'}) RETURN p.active",
//!                      "expect": [[false]]}]}
//! ```
//!
//! - `answer.oracle` is a query whose **first column** is the answer set; an
//!   `answer.values` list states it directly. The prediction is scored by F1
//!   against that set: partial credit, and a wrong extra value costs as much
//!   as a missing one.
//! - `postconditions` are checked against the agent's graph when the episode
//!   ends; each one's rows must equal `expect`, in any order. The reward is the
//!   fraction that hold.
//! - With both, the reward is the mean of the two.
//! - `max_steps` (optional) overrides the environment's limit for that task.
//!
//! # Not yet
//!
//! `reset` re-imports the snapshot, which costs time in proportion to the graph,
//! not to what the episode changed; MVCC `rollback_to` (#2093) is what makes it
//! proportional to the change. Episodes that only read skip the re-import.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::{Duration, Instant};

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::embedded::record_batch_to_query_result;

/// The default per-turn time budget for one statement (PERF-19).
pub const DEFAULT_TURN_TIMEOUT: Duration = Duration::from_millis(500);
/// The default number of actions an episode may take before it is truncated.
pub const DEFAULT_MAX_STEPS: usize = 20;

/// One task, as written in the tasks file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: String,
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<AnswerKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub postconditions: Vec<Postcondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_steps: Option<usize>,
}

/// Where a read task's correct answer comes from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AnswerKey {
    /// A query run against the pristine snapshot; its first column is the answer.
    Oracle(String),
    /// The answer, stated directly.
    Values(Vec<Value>),
}

/// A check on the agent's graph at the end of a write task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Postcondition {
    pub query: String,
    pub expect: Vec<Vec<Value>>,
}

/// What the agent does in one step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Run one Cypher statement against the agent's graph.
    Query(String),
    /// End the episode with this answer; it is scored at once.
    Answer(Vec<Value>),
}

/// What `reset` hands the agent.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TaskObservation {
    pub task_id: String,
    pub prompt: String,
    /// The graph's schema, as the NLQ pipeline describes it.
    pub schema: String,
}

/// What one step shows the agent.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Observation {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// The statement failed, or ran out of its time budget. The graph is as it
    /// was before the statement: writes are applied atomically or not at all.
    Error { message: String },
    /// The episode has ended.
    Done,
}

/// What `step` returns, in the shape RL rollout workers expect
/// (`obs, reward, terminated, truncated, info`).
#[derive(Debug, Clone, Serialize)]
pub struct StepResult {
    pub observation: Observation,
    /// Zero until the episode ends; then the episode's score.
    pub reward: f64,
    /// The agent answered.
    pub terminated: bool,
    /// The step limit was reached without an answer.
    pub truncated: bool,
    pub info: StepInfo,
}

/// Telemetry for one step (AI-15). Never part of the reward.
#[derive(Debug, Clone, Serialize)]
pub struct StepInfo {
    /// 1 for the first action of an episode.
    pub step: usize,
    pub latency_ms: f64,
    pub rows: usize,
    /// The statement changed the graph.
    pub wrote: bool,
    /// The full score, on the step that ends the episode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<Score>,
}

/// A scored answer.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Score {
    /// In `[0, 1]`.
    pub reward: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<AnswerScore>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub postconditions: Vec<PostconditionResult>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AnswerScore {
    pub precision: f64,
    pub recall: f64,
    pub f1: f64,
    pub expected: usize,
    pub predicted: usize,
    pub correct: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PostconditionResult {
    pub query: String,
    pub held: bool,
    /// Why it did not hold, when it did not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Everything that can go wrong outside an episode's own actions. An action
/// that fails is an [`Observation::Error`], not one of these.
#[derive(Debug, thiserror::Error)]
pub enum EnvError {
    #[error("could not read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("the snapshot does not import: {0}")]
    Snapshot(String),
    #[error("tasks file line {line}: {message}")]
    TaskLine { line: usize, message: String },
    #[error("task `{task}`: {message}")]
    Task { task: String, message: String },
    #[error("no task `{0}`")]
    UnknownTask(String),
    #[error("{0}")]
    State(&'static str),
}

/// The environment. See the module documentation.
pub struct SamyamaEnv {
    snapshot: Vec<u8>,
    snapshot_sha256: String,
    tasks: Vec<Task>,
    index: HashMap<String, usize>,
    /// Each task's gold answer set, keyed for comparison, from the pristine copy.
    gold: HashMap<String, BTreeSet<String>>,
    store: GraphStore,
    engine: QueryEngine,
    seed: u64,
    rng: u64,
    turn_timeout: Duration,
    max_steps: usize,
    /// The epoch the store had when it was last restored; a different one
    /// means the episode changed the graph and the next reset must restore it.
    clean_epoch: u64,
    restores: u64,
    episode: Option<Episode>,
}

struct Episode {
    task: usize,
    steps: usize,
    over: bool,
    actions: Vec<Action>,
}

impl SamyamaEnv {
    /// Open a snapshot file and a tasks file (JSON lines).
    pub fn open(
        snapshot_path: impl AsRef<Path>,
        tasks_path: impl AsRef<Path>,
        seed: u64,
    ) -> Result<Self, EnvError> {
        let read = |p: &Path| {
            std::fs::read(p).map_err(|source| EnvError::Io {
                path: p.display().to_string(),
                source,
            })
        };
        let snapshot = read(snapshot_path.as_ref())?;
        let text =
            String::from_utf8(read(tasks_path.as_ref())?).map_err(|e| EnvError::TaskLine {
                line: 0,
                message: format!("not UTF-8: {e}"),
            })?;
        Self::new(snapshot, parse_tasks(&text)?, seed)
    }

    /// Build from a snapshot's bytes and a list of tasks.
    ///
    /// Every task is checked here, and every oracle run, so a broken task fails
    /// before any episode starts rather than in the middle of a run.
    pub fn new(snapshot: Vec<u8>, tasks: Vec<Task>, seed: u64) -> Result<Self, EnvError> {
        let snapshot_sha256 = format!("{:x}", Sha256::digest(&snapshot));
        let engine = QueryEngine::new();
        let baseline = restore(&snapshot)?;

        let mut index = HashMap::new();
        let mut gold = HashMap::new();
        for (i, t) in tasks.iter().enumerate() {
            let fail = |message: String| EnvError::Task {
                task: t.id.clone(),
                message,
            };
            if index.insert(t.id.clone(), i).is_some() {
                return Err(fail("the id is used twice".into()));
            }
            if t.answer.is_none() && t.postconditions.is_empty() {
                return Err(fail(
                    "has neither an answer nor postconditions, so nothing can score it".into(),
                ));
            }
            if t.max_steps == Some(0) {
                return Err(fail("max_steps is 0".into()));
            }
            match &t.answer {
                Some(AnswerKey::Values(vs)) => {
                    let keys = vs.iter().map(key).collect::<Result<_, _>>().map_err(fail)?;
                    gold.insert(t.id.clone(), keys);
                }
                Some(AnswerKey::Oracle(q)) => {
                    let batch = engine
                        .execute(q, &baseline)
                        .map_err(|e| fail(format!("the oracle does not run: {e}")))?;
                    let result = record_batch_to_query_result(&batch, &baseline);
                    let keys = result
                        .records
                        .iter()
                        .map(|row| key(row.first().unwrap_or(&Value::Null)))
                        .collect::<Result<_, _>>()
                        .map_err(|e| fail(format!("the oracle's first column: {e}")))?;
                    gold.insert(t.id.clone(), keys);
                }
                None => {}
            }
            for p in &t.postconditions {
                samyama::query::parse_query(&p.query).map_err(|e| {
                    fail(format!("postcondition `{}` does not parse: {e}", p.query))
                })?;
            }
        }
        drop(baseline);

        let store = restore(&snapshot)?;
        let clean_epoch = store.epoch();
        Ok(SamyamaEnv {
            snapshot,
            snapshot_sha256,
            tasks,
            index,
            gold,
            store,
            engine,
            seed,
            rng: seed,
            turn_timeout: DEFAULT_TURN_TIMEOUT,
            max_steps: DEFAULT_MAX_STEPS,
            clean_epoch,
            restores: 0,
            episode: None,
        })
    }

    /// The per-statement time budget (default 500 ms).
    pub fn with_turn_timeout(mut self, timeout: Duration) -> Self {
        self.turn_timeout = timeout;
        self
    }

    /// The step limit for tasks that do not set their own (default 20).
    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps.max(1);
        self
    }

    /// The SHA-256 of the pinned snapshot, so a run can name what it ran on.
    pub fn snapshot_sha256(&self) -> &str {
        &self.snapshot_sha256
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }

    /// How many times `reset` has re-imported the snapshot: once after every
    /// episode that changed the graph (or tried to and was rolled back), never
    /// after one that only read.
    pub fn restores(&self) -> u64 {
        self.restores
    }

    /// The actions taken so far in the current episode, in order: enough to
    /// replay it against the same snapshot and seed.
    pub fn trajectory(&self) -> &[Action] {
        self.episode.as_ref().map_or(&[], |e| &e.actions)
    }

    /// Start an episode: restore the graph if the last episode changed it, and
    /// hand out a task, the named one or the next one the seed draws.
    pub fn reset(&mut self, task_id: Option<&str>) -> Result<TaskObservation, EnvError> {
        let task = match task_id {
            Some(id) => *self
                .index
                .get(id)
                .ok_or_else(|| EnvError::UnknownTask(id.to_string()))?,
            None => {
                if self.tasks.is_empty() {
                    return Err(EnvError::State("there are no tasks"));
                }
                (splitmix64(&mut self.rng) % self.tasks.len() as u64) as usize
            }
        };
        if self.store.epoch() != self.clean_epoch {
            self.store = restore(&self.snapshot)?;
            self.clean_epoch = self.store.epoch();
            self.restores += 1;
        }
        self.episode = Some(Episode {
            task,
            steps: 0,
            over: false,
            actions: Vec::new(),
        });
        let t = &self.tasks[task];
        Ok(TaskObservation {
            task_id: t.id.clone(),
            prompt: t.prompt.clone(),
            schema: self.store.schema_summary(),
        })
    }

    /// Take one action.
    pub fn step(&mut self, action: Action) -> Result<StepResult, EnvError> {
        let ep = self
            .episode
            .as_mut()
            .ok_or(EnvError::State("no episode has started; call reset first"))?;
        if ep.over {
            return Err(EnvError::State("the episode is over; call reset"));
        }
        ep.steps += 1;
        ep.actions.push(action.clone());
        let step = ep.steps;
        let task = ep.task;
        let limit = self.tasks[task].max_steps.unwrap_or(self.max_steps);

        let started = Instant::now();
        let (observation, rows, wrote, score) = match action {
            Action::Answer(prediction) => {
                let score = self.score(task, Some(&prediction));
                (Observation::Done, 0, false, Some(score))
            }
            Action::Query(cypher) => {
                let before = self.store.epoch();
                // Classified the way the server does it (`statement_is_write`),
                // and run on the matching path: a read sent down the write path
                // advances the store's epoch although nothing changed, which
                // would make every episode look dirty and every reset re-import.
                let outcome = match self.engine.statement_is_write(&cypher) {
                    Err(e) => Err(e),
                    Ok(true) => self.engine.execute_mut_with_params_within(
                        &cypher,
                        &mut self.store,
                        "default",
                        &Default::default(),
                        Some(self.turn_timeout),
                    ),
                    Ok(false) => self.engine.execute_with_params_within(
                        &cypher,
                        &self.store,
                        &Default::default(),
                        Some(self.turn_timeout),
                    ),
                };
                // A failed write is rolled back, but the rollback advances the
                // epoch too, so only a statement that succeeded counts as having
                // written. The next reset still restores, which costs time but
                // never correctness.
                let wrote = outcome.is_ok() && self.store.epoch() != before;
                let obs = match outcome {
                    Ok(batch) => {
                        let r = record_batch_to_query_result(&batch, &self.store);
                        Observation::Rows {
                            columns: r.columns,
                            rows: r.records,
                        }
                    }
                    Err(e) => Observation::Error {
                        message: e.to_string(),
                    },
                };
                let rows = match &obs {
                    Observation::Rows { rows, .. } => rows.len(),
                    _ => 0,
                };
                (obs, rows, wrote, None)
            }
        };
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;

        let (terminated, truncated, score) = match score {
            Some(s) => (true, false, Some(s)),
            // Out of steps without an answer: a read task scores 0, a write
            // task is still judged on the graph the agent left.
            None if step >= limit => (false, true, Some(self.score_unanswered(task))),
            None => (false, false, None),
        };
        let ep_over = terminated || truncated;
        self.episode.as_mut().expect("checked above").over = ep_over;
        Ok(StepResult {
            observation: if ep_over && !terminated {
                Observation::Done
            } else {
                observation
            },
            reward: score.as_ref().map_or(0.0, |s| s.reward),
            terminated,
            truncated,
            info: StepInfo {
                step,
                latency_ms,
                rows,
                wrote,
                score,
            },
        })
    }

    /// Score `prediction` for the current episode's task without ending it.
    pub fn verify(&self, prediction: &[Value]) -> Result<Score, EnvError> {
        let ep = self
            .episode
            .as_ref()
            .ok_or(EnvError::State("no episode has started; call reset first"))?;
        Ok(self.score(ep.task, Some(prediction)))
    }

    fn score_unanswered(&self, task: usize) -> Score {
        self.score(task, None)
    }

    fn score(&self, task: usize, prediction: Option<&[Value]>) -> Score {
        let t = &self.tasks[task];
        let mut parts = Vec::new();

        let answer = self.gold.get(&t.id).map(|gold| {
            let predicted: BTreeSet<String> = prediction
                .unwrap_or(&[])
                .iter()
                // A value that cannot be a key (a node, a map) matches nothing.
                .filter_map(|v| key(v).ok())
                .collect();
            let s = f1(gold, &predicted);
            parts.push(s.f1);
            s
        });

        let postconditions: Vec<PostconditionResult> =
            t.postconditions.iter().map(|p| self.check(p)).collect();
        if !postconditions.is_empty() {
            let held = postconditions.iter().filter(|p| p.held).count();
            parts.push(held as f64 / postconditions.len() as f64);
        }

        let reward = if parts.is_empty() {
            0.0
        } else {
            parts.iter().sum::<f64>() / parts.len() as f64
        };
        Score {
            reward,
            answer,
            postconditions,
        }
    }

    fn check(&self, p: &Postcondition) -> PostconditionResult {
        let fail = |detail: String| PostconditionResult {
            query: p.query.clone(),
            held: false,
            detail: Some(detail),
        };
        let batch = match self.engine.execute(&p.query, &self.store) {
            Ok(b) => b,
            Err(e) => return fail(format!("did not run: {e}")),
        };
        let got = record_batch_to_query_result(&batch, &self.store).records;
        match (rows_key(&got), rows_key(&p.expect)) {
            (Ok(g), Ok(w)) if g == w => PostconditionResult {
                query: p.query.clone(),
                held: true,
                detail: None,
            },
            (Ok(_), Ok(_)) => fail(format!(
                "returned {}, expected {}",
                Value::from(got),
                Value::from(p.expect.clone())
            )),
            (Err(e), _) | (_, Err(e)) => fail(e),
        }
    }
}

/// Parse a tasks file: one JSON object per line; blank lines and lines
/// starting with `#` are skipped.
pub fn parse_tasks(text: &str) -> Result<Vec<Task>, EnvError> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
        .map(|(i, l)| {
            serde_json::from_str(l).map_err(|e| EnvError::TaskLine {
                line: i + 1,
                message: e.to_string(),
            })
        })
        .collect()
}

fn restore(snapshot: &[u8]) -> Result<GraphStore, EnvError> {
    let mut store = GraphStore::new();
    samyama::snapshot::import_tenant(&mut store, snapshot)
        .map_err(|e| EnvError::Snapshot(e.to_string()))?;
    Ok(store)
}

/// A comparable key for an answer value.
///
/// A number that is a whole number keys as an integer, so `1` and `1.0` are
/// the same answer, as Cypher compares them. Nodes, edges and maps are refused:
/// an answer set is a set of identifiers, and which properties of a node to
/// compare is a choice the task should make in its oracle (`RETURN n.id`).
fn key(v: &Value) -> Result<String, String> {
    match v {
        Value::Number(n) => match n.as_f64() {
            Some(f) if f.fract() == 0.0 && f.abs() < 9.0e15 => Ok(format!("n:{}", f as i64)),
            Some(f) => Ok(format!("n:{f}")),
            None => Ok(format!("n:{n}")),
        },
        Value::String(s) => Ok(format!("s:{s}")),
        Value::Bool(b) => Ok(format!("b:{b}")),
        Value::Null => Ok("null".into()),
        Value::Array(items) => {
            let inner = items.iter().map(key).collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", inner.join(",")))
        }
        Value::Object(_) => Err(format!(
            "{v} is a node, edge or map, not a value; return a property (e.g. `n.id`)"
        )),
    }
}

/// Rows as a multiset of keys, so order does not matter but repeats do.
fn rows_key(rows: &[Vec<Value>]) -> Result<BTreeMap<String, usize>, String> {
    let mut out = BTreeMap::new();
    for row in rows {
        let k = key(&Value::Array(row.clone()))?;
        *out.entry(k).or_insert(0) += 1;
    }
    Ok(out)
}

fn f1(gold: &BTreeSet<String>, predicted: &BTreeSet<String>) -> AnswerScore {
    let correct = gold.intersection(predicted).count();
    let (precision, recall) = match (predicted.len(), gold.len()) {
        // Nothing to find and nothing claimed: exactly right.
        (0, 0) => (1.0, 1.0),
        (0, _) => (0.0, 0.0),
        (_, 0) => (0.0, 0.0),
        (p, g) => (correct as f64 / p as f64, correct as f64 / g as f64),
    };
    let f1 = if precision + recall == 0.0 {
        0.0
    } else {
        2.0 * precision * recall / (precision + recall)
    };
    AnswerScore {
        precision,
        recall,
        f1,
        expected: gold.len(),
        predicted: predicted.len(),
        correct,
    }
}

/// A fixed, written-down generator, so a seed means the same draws on every
/// platform and toolchain.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(xs: &[&str]) -> BTreeSet<String> {
        xs.iter().map(|x| key(&json!(x)).unwrap()).collect()
    }

    #[test]
    fn f1_gives_partial_credit_and_charges_for_wrong_extras() {
        let gold = set(&["a", "b"]);
        assert_eq!(f1(&gold, &set(&["a", "b"])).f1, 1.0);
        // One right, one missing, one wrong: precision 1/2, recall 1/2.
        let s = f1(&gold, &set(&["a", "c"]));
        assert_eq!((s.precision, s.recall, s.f1), (0.5, 0.5, 0.5));
        // Everything, plus as much noise: recall 1, precision 1/2.
        let s = f1(&gold, &set(&["a", "b", "c", "d"]));
        assert!((s.f1 - 2.0 / 3.0).abs() < 1e-12, "{}", s.f1);
        assert_eq!(f1(&gold, &set(&[])).f1, 0.0);
        // An empty answer is exactly right when there is nothing to find.
        assert_eq!(f1(&set(&[]), &set(&[])).f1, 1.0);
        assert_eq!(f1(&set(&[]), &set(&["a"])).f1, 0.0);
    }

    #[test]
    fn keys_treat_whole_floats_as_integers_and_refuse_nodes() {
        assert_eq!(key(&json!(1)).unwrap(), key(&json!(1.0)).unwrap());
        assert_ne!(key(&json!(1)).unwrap(), key(&json!("1")).unwrap());
        assert_ne!(key(&json!(1.5)).unwrap(), key(&json!(1)).unwrap());
        assert!(key(&json!({"id": "1", "labels": ["P"]})).is_err());
    }

    #[test]
    fn the_seeded_generator_is_fixed() {
        // Written down: a change to the generator changes every seeded run.
        let mut s = 42;
        let draws: Vec<u64> = (0..3).map(|_| splitmix64(&mut s)).collect();
        assert_eq!(
            draws,
            [
                0xbdd7_3226_2feb_6e95,
                0x28ef_e333_b266_f103,
                0x4752_6757_130f_9f52
            ]
        );
    }

    #[test]
    fn tasks_files_report_the_line_that_is_wrong() {
        let text =
            "# a comment\n\n{\"id\":\"a\",\"prompt\":\"p\",\"answer\":{\"values\":[1]}}\n{oops}\n";
        match parse_tasks(text) {
            Err(EnvError::TaskLine { line, .. }) => assert_eq!(line, 4),
            other => panic!("{other:?}"),
        }
    }
}
