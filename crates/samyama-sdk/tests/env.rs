//! `SamyamaEnv` (#2095): episodes over a pinned snapshot, scored exactly, with
//! nothing carried from one episode to the next.

use std::time::Duration;

use samyama_sdk::env::{
    parse_tasks, Action, AnswerKey, EnvError, Observation, Postcondition, SamyamaEnv, Task,
};
use samyama_sdk::{GraphStore, QueryEngine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Four people; Alice knows Bob and Carol, Bob knows Dan. Everyone is active.
fn snapshot() -> Vec<u8> {
    let mut g = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (a:Person {name: 'Alice', active: true}), \
                    (b:Person {name: 'Bob', active: true}), \
                    (c:Person {name: 'Carol', active: true}), \
                    (d:Person {name: 'Dan', active: true}), \
                    (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), (b)-[:KNOWS]->(d)",
            &mut g,
            "default",
        )
        .unwrap();
    let mut out = Vec::new();
    samyama::snapshot::export_tenant(&g, &mut out).unwrap();
    out
}

fn read_task(id: &str, oracle: &str) -> Task {
    Task {
        id: id.into(),
        prompt: format!("prompt for {id}"),
        answer: Some(AnswerKey::Oracle(oracle.into())),
        postconditions: vec![],
        max_steps: None,
    }
}

fn knows() -> Task {
    read_task(
        "knows",
        "MATCH (:Person {name: 'Alice'})-[:KNOWS]->(p) RETURN p.name",
    )
}

fn deactivate() -> Task {
    Task {
        id: "deactivate".into(),
        prompt: "Mark Bob inactive.".into(),
        answer: None,
        postconditions: vec![Postcondition {
            query: "MATCH (p:Person {name: 'Bob'}) RETURN p.active".into(),
            expect: vec![vec![json!(false)]],
        }],
        max_steps: None,
    }
}

fn env(tasks: Vec<Task>, seed: u64) -> SamyamaEnv {
    SamyamaEnv::new(snapshot(), tasks, seed).unwrap()
}

fn q(s: &str) -> Action {
    Action::Query(s.into())
}

fn answer(xs: &[&str]) -> Action {
    Action::Answer(xs.iter().map(|x| json!(x)).collect())
}

fn rows(o: &Observation) -> Vec<Vec<Value>> {
    match o {
        Observation::Rows { rows, .. } => rows.clone(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn count_nodes(e: &mut SamyamaEnv) -> Value {
    rows(&e.step(q("MATCH (n) RETURN count(n)")).unwrap().observation)[0][0].clone()
}

#[test]
fn a_right_answer_scores_one_and_a_partial_one_scores_its_f1() {
    let mut e = env(vec![knows()], 1);
    let obs = e.reset(Some("knows")).unwrap();
    assert_eq!(obs.task_id, "knows");
    assert!(obs.schema.contains("Person"), "{}", obs.schema);

    let r = e
        .step(q(
            "MATCH (:Person {name: 'Alice'})-[:KNOWS]->(p) RETURN p.name",
        ))
        .unwrap();
    assert_eq!((r.reward, r.terminated, r.truncated), (0.0, false, false));
    assert_eq!(r.info.rows, 2);

    // Scoring without ending the episode.
    let partial = e.verify(&[json!("Bob"), json!("Dan")]).unwrap();
    let a = partial.answer.unwrap();
    assert_eq!((a.precision, a.recall, a.f1), (0.5, 0.5, 0.5));

    let done = e.step(answer(&["Carol", "Bob"])).unwrap();
    assert!(done.terminated && !done.truncated);
    assert_eq!(done.reward, 1.0);
    assert_eq!(done.observation, Observation::Done);
    assert!(matches!(e.step(q("RETURN 1")), Err(EnvError::State(_))));
}

#[test]
fn the_answer_key_comes_from_the_pristine_graph_not_the_agents() {
    // The agent deletes Carol and then answers what its own graph says. The
    // gold answer was taken from the snapshot before the episode, so changing
    // the graph cannot change what it is scored against.
    let mut e = env(vec![knows()], 1);
    e.reset(Some("knows")).unwrap();
    let w = e
        .step(q("MATCH (p:Person {name: 'Carol'}) DETACH DELETE p"))
        .unwrap();
    assert!(w.info.wrote);
    let seen = rows(
        &e.step(q(
            "MATCH (:Person {name: 'Alice'})-[:KNOWS]->(p) RETURN p.name",
        ))
        .unwrap()
        .observation,
    );
    assert_eq!(seen, vec![vec![json!("Bob")]]);

    let done = e.step(answer(&["Bob"])).unwrap();
    let a = done.info.score.unwrap().answer.unwrap();
    assert_eq!((a.precision, a.recall, a.expected), (1.0, 0.5, 2));
    assert!((done.reward - 2.0 / 3.0).abs() < 1e-12, "{}", done.reward);
}

#[test]
fn a_write_task_is_judged_on_the_graph_the_agent_left() {
    let mut e = env(vec![deactivate()], 1);

    e.reset(Some("deactivate")).unwrap();
    e.step(q("MATCH (p:Person {name: 'Bob'}) SET p.active = false"))
        .unwrap();
    let done = e.step(Action::Answer(vec![])).unwrap();
    assert_eq!(done.reward, 1.0);
    assert!(done.info.score.unwrap().postconditions[0].held);

    // The next episode starts from the snapshot, where Bob is active again.
    e.reset(Some("deactivate")).unwrap();
    let done = e.step(Action::Answer(vec![])).unwrap();
    assert_eq!(done.reward, 0.0);
    let pc = &done.info.score.unwrap().postconditions[0];
    assert!(!pc.held);
    assert!(pc.detail.as_deref().unwrap().contains("[[true]]"), "{pc:?}");
}

#[test]
fn a_task_with_both_scores_the_mean_of_the_two() {
    let mut both = deactivate();
    both.id = "both".into();
    both.answer = Some(AnswerKey::Values(vec![json!("Bob")]));
    let mut e = env(vec![both], 1);
    e.reset(None).unwrap();
    // Right answer, change not made.
    let done = e.step(answer(&["Bob"])).unwrap();
    assert_eq!(done.reward, 0.5);
}

#[test]
fn a_failed_statement_is_an_observation_and_changes_nothing() {
    let mut e = env(vec![knows()], 1);
    e.reset(None).unwrap();
    let bad = e.step(q("MATCH (n RETURN n")).unwrap();
    assert!(
        matches!(bad.observation, Observation::Error { .. }),
        "{:?}",
        bad.observation
    );
    assert!(!bad.info.wrote);
    // A statement that fails part-way (division by zero after its writes)
    // leaves the graph as it was: writes are applied atomically or not at all.
    let half = e
        .step(q(
            "MATCH (p:Person) SET p.active = false WITH p RETURN 1 / 0",
        ))
        .unwrap();
    match &half.observation {
        Observation::Error { message } => {
            assert!(message.contains("Division by zero"), "{message}")
        }
        other => panic!("{other:?}"),
    }
    assert!(!half.info.wrote, "a rolled-back statement reported a write");
    let active = rows(
        &e.step(q("MATCH (p:Person) WHERE p.active = true RETURN count(p)"))
            .unwrap()
            .observation,
    );
    assert_eq!(active, vec![vec![json!(4)]]);
    assert!(!e.step(answer(&["Bob", "Carol"])).unwrap().truncated);
}

#[test]
fn an_episode_that_reaches_its_step_limit_is_truncated() {
    let mut t = knows();
    t.max_steps = Some(2);
    let mut e = env(vec![t, deactivate()], 1).with_max_steps(5);

    e.reset(Some("knows")).unwrap();
    assert!(!e.step(q("RETURN 1")).unwrap().truncated);
    let last = e.step(q("RETURN 2")).unwrap();
    assert!(last.truncated && !last.terminated);
    assert_eq!(last.reward, 0.0, "no answer was given");
    assert_eq!(last.observation, Observation::Done);
    assert!(matches!(e.step(q("RETURN 3")), Err(EnvError::State(_))));

    // A write task that runs out of steps is still judged on what it did.
    e.reset(Some("deactivate")).unwrap();
    e.step(q("MATCH (p:Person {name: 'Bob'}) SET p.active = false"))
        .unwrap();
    for _ in 0..3 {
        e.step(q("RETURN 0")).unwrap();
    }
    let last = e.step(q("RETURN 0")).unwrap();
    assert!(last.truncated);
    assert_eq!(last.reward, 1.0);
}

#[test]
fn a_hundred_mutating_episodes_leave_nothing_behind_and_repeat_exactly() {
    fn run(seed: u64) -> Vec<(String, f64)> {
        let mut e = env(vec![knows(), deactivate()], seed);
        let mut out = Vec::new();
        for i in 0..100 {
            let task = e.reset(None).unwrap().task_id;
            // Whatever the last episode did, this one starts from the snapshot.
            assert_eq!(
                count_nodes(&mut e),
                json!(4),
                "episode {i} began with leftovers"
            );
            e.step(q(&format!("CREATE (:Junk {{n: {i}}})"))).unwrap();
            e.step(q("MATCH (p:Person {name: 'Carol'}) DETACH DELETE p"))
                .unwrap();
            if i % 3 == 0 {
                e.step(q("MATCH (p:Person {name: 'Bob'}) SET p.active = false"))
                    .unwrap();
            }
            let r = e.step(answer(&["Bob", "Carol"])).unwrap();
            out.push((task, r.reward));
        }
        out
    }

    let a = run(7);
    assert_eq!(a, run(7), "the same seed gave a different run");
    assert_ne!(
        a.iter().map(|x| &x.0).collect::<Vec<_>>(),
        run(8).iter().map(|x| &x.0).collect::<Vec<_>>(),
        "a different seed drew the same 100 tasks"
    );
    // Both tasks were drawn, and each scored what its actions earned.
    for (task, reward) in &a {
        match task.as_str() {
            "knows" => assert_eq!(*reward, 1.0),
            "deactivate" => assert!(*reward == 0.0 || *reward == 1.0),
            other => panic!("{other}"),
        }
    }
    assert!(a.iter().any(|x| x.0 == "knows") && a.iter().any(|x| x.0 == "deactivate"));
}

#[test]
fn only_an_episode_that_wrote_makes_the_next_reset_reimport() {
    let mut e = env(vec![knows(), deactivate()], 1);
    for _ in 0..5 {
        e.reset(Some("knows")).unwrap();
        let r = e.step(q("MATCH (p:Person) RETURN p.name")).unwrap();
        assert!(!r.info.wrote, "a read reported a write");
        e.step(answer(&["Bob", "Carol"])).unwrap();
    }
    e.reset(Some("knows")).unwrap();
    assert_eq!(
        e.restores(),
        0,
        "read-only episodes re-imported the snapshot"
    );

    e.step(q("CREATE (:Junk)")).unwrap();
    e.reset(Some("knows")).unwrap();
    assert_eq!(e.restores(), 1);
    assert_eq!(count_nodes(&mut e), json!(4));
    e.reset(Some("knows")).unwrap();
    assert_eq!(e.restores(), 1, "the count query was a read");
}

#[test]
fn the_trajectory_is_the_actions_in_order() {
    let mut e = env(vec![knows()], 1);
    e.reset(None).unwrap();
    e.step(q("RETURN 1")).unwrap();
    e.step(answer(&["Bob"])).unwrap();
    assert_eq!(e.trajectory(), &[q("RETURN 1"), answer(&["Bob"])]);
    // Serializable, so a run can be written out and replayed.
    let line = serde_json::to_string(e.trajectory()).unwrap();
    assert_eq!(line, r#"[{"query":"RETURN 1"},{"answer":["Bob"]}]"#);
    e.reset(None).unwrap();
    assert!(e.trajectory().is_empty());
}

#[test]
fn a_statement_over_its_time_budget_is_stopped() {
    let mut e = env(vec![knows()], 1).with_turn_timeout(Duration::from_millis(1));
    e.reset(None).unwrap();
    let r = e
        .step(q(
            "UNWIND range(1, 2000) AS a UNWIND range(1, 2000) AS b RETURN count(*)",
        ))
        .unwrap();
    match r.observation {
        Observation::Error { message } => {
            assert!(message.to_lowercase().contains("time"), "{message}")
        }
        other => panic!("a 4M-row statement finished inside 1 ms: {other:?}"),
    }
}

#[test]
fn misuse_and_broken_tasks_are_reported_before_any_episode() {
    let mut e = env(vec![knows()], 1);
    assert!(matches!(e.step(q("RETURN 1")), Err(EnvError::State(_))));
    assert!(matches!(e.verify(&[]), Err(EnvError::State(_))));
    assert!(matches!(
        e.reset(Some("nope")),
        Err(EnvError::UnknownTask(_))
    ));

    let fails = |tasks: Vec<Task>| match SamyamaEnv::new(snapshot(), tasks, 1) {
        Err(EnvError::Task { message, .. }) => message,
        Err(other) => panic!("{other}"),
        Ok(_) => panic!("accepted"),
    };
    assert!(fails(vec![knows(), knows()]).contains("twice"));
    assert!(fails(vec![read_task("x", "MATCH (n RETURN n")]).contains("oracle does not run"));
    assert!(fails(vec![read_task("x", "MATCH (n:Person) RETURN n")]).contains("not a value"));
    let mut nothing = knows();
    nothing.answer = None;
    assert!(fails(vec![nothing]).contains("nothing can score it"));
    let mut bad_check = deactivate();
    bad_check.postconditions[0].query = "MATCH (".into();
    assert!(fails(vec![bad_check]).contains("does not parse"));

    assert!(matches!(
        SamyamaEnv::new(b"not a snapshot".to_vec(), vec![knows()], 1),
        Err(EnvError::Snapshot(_))
    ));
}

#[test]
fn open_reads_both_files_and_names_the_snapshot_it_ran_on() {
    let dir = std::env::temp_dir().join(format!("samyama-env-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let snap = snapshot();
    std::fs::write(dir.join("g.sgsnap"), &snap).unwrap();
    let tasks = [knows(), deactivate()]
        .iter()
        .map(|t| serde_json::to_string(t).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(dir.join("tasks.jsonl"), &tasks).unwrap();

    let e = SamyamaEnv::open(dir.join("g.sgsnap"), dir.join("tasks.jsonl"), 3).unwrap();
    assert_eq!(e.snapshot_sha256(), format!("{:x}", Sha256::digest(&snap)));
    assert_eq!(e.tasks(), parse_tasks(&tasks).unwrap().as_slice());
    assert_eq!(e.seed(), 3);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(matches!(
        SamyamaEnv::open(dir.join("missing.sgsnap"), dir.join("tasks.jsonl"), 3),
        Err(EnvError::Io { .. })
    ));
}

#[test]
fn the_documented_tasks_file_format_parses() {
    let text = r#"
{"id": "q1", "prompt": "Who does Alice know?", "answer": {"oracle": "MATCH (:Person {name: 'Alice'})-[:KNOWS]->(p) RETURN p.name"}}
{"id": "q2", "prompt": "Name two.", "answer": {"values": ["Ada", "Alan"]}}
{"id": "w1", "prompt": "Mark Bob inactive.", "postconditions": [{"query": "MATCH (p:Person {name: 'Bob'}) RETURN p.active", "expect": [[false]]}], "max_steps": 5}
"#;
    let tasks = parse_tasks(text).unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[2].max_steps, Some(5));
    SamyamaEnv::new(snapshot(), tasks, 1).unwrap();
}
