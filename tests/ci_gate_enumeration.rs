//! `ci-ok` must wait for every suite in the CI workflow (#1860).
//!
//! Branch protection requires one check, `ci-ok`, because a required context whose
//! name comes from a matrix expansion cannot be satisfied by a job an `if:` skipped:
//! GitHub never expands the matrix, so it reports `test (${{ matrix.os }})` and the
//! required `test (ubuntu-latest)` never appears. A docs-only pull request was
//! therefore MERGEABLE and permanently BLOCKED.
//!
//! Collapsing the gate onto one aggregator moves the risk somewhere new. A suite
//! added to the workflow but not to `ci-ok`'s `needs` does not fail the gate -- it
//! leaves the gate, and `ci-ok` goes green without ever waiting for it. Nothing in
//! a pull request would show that: the new job runs, reports, and is ignored.
//!
//! So the enumeration is asserted here rather than trusted, in the manner of
//! `tests/allocator_consistency.rs`: this reads the workflow and compares the two
//! lists.

fn ci_yml() -> serde_yaml::Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/.github/workflows/ci.yml");
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{path}: {e}"));
    serde_yaml::from_str(&text).unwrap_or_else(|e| panic!("{path} is not valid YAML: {e}"))
}

const AGGREGATOR: &str = "ci-ok";

/// Jobs deliberately outside the aggregator, each with the reason it is outside.
///
/// This exists because the alternative is worse. `gpml` has to be a job in this
/// workflow to reuse `test`'s release binary and to stop being a `paths:`-filtered
/// workflow whose checks never report (#1865), but putting it in `ci-ok`'s `needs`
/// would make conformance block a merge for the first time -- a policy change, and
/// not one a refactor should make on its way past.
///
/// The list fails BOTH ways, in the manner of `KNOWN_GAPS` in
/// `tests/anchor_coverage.rs`: an entry that is also in `needs` fails, and a job
/// that is in neither fails. So an entry cannot outlive the decision it records,
/// and a new suite cannot quietly escape the gate -- escaping it means adding a
/// line here, with a reason, in a diff someone reads.
const UNGATED: &[(&str, &str)] = &[
    (
        "gpml",
        "conformance has never gated a merge; requiring it is a policy decision, \
         separate from folding it into this workflow (#1865)",
    ),
    (
        "orchestration",
        "a new suite that installs Airflow and Dagster from PyPI; making it block \
         every merge is a policy decision, separate from adding it (#1715)",
    ),
];

fn jobs(doc: &serde_yaml::Value) -> Vec<String> {
    doc["jobs"]
        .as_mapping()
        .expect("ci.yml has a `jobs:` mapping")
        .keys()
        .map(|k| k.as_str().expect("a job name is a string").to_string())
        .collect()
}

fn needs_of(doc: &serde_yaml::Value, job: &str) -> Vec<String> {
    let n = &doc["jobs"][job]["needs"];
    // `needs:` is a string when there is one, a sequence when there are several.
    if let Some(s) = n.as_str() {
        return vec![s.to_string()];
    }
    n.as_sequence()
        .unwrap_or_else(|| panic!("{job} has no `needs:`"))
        .iter()
        .map(|v| v.as_str().expect("a need is a string").to_string())
        .collect()
}

/// CONTROL: the workflow parses and holds the jobs this file reasons about. If this
/// fails, every assertion below is vacuous rather than passing.
#[test]
fn the_workflow_has_an_aggregator_and_some_suites() {
    let doc = ci_yml();
    let all = jobs(&doc);
    assert!(
        all.contains(&AGGREGATOR.to_string()),
        "ci.yml has no `{AGGREGATOR}` job; branch protection requires it. Jobs: {all:?}"
    );
    assert!(
        all.len() > 2,
        "only {} job(s) in ci.yml -- this test would pass on an empty gate: {all:?}",
        all.len()
    );
}

/// Every job in the workflow is one the aggregator waits for.
///
/// Both directions fail. A suite missing from `needs` has left the gate. A `needs`
/// entry naming no job is a suite that was renamed or deleted, which GitHub reports
/// as a workflow error rather than a passing gate, but is worth catching here where
/// the message can say which name.
#[test]
fn the_aggregator_waits_for_every_suite() {
    let doc = ci_yml();
    let all = jobs(&doc);
    let needs = needs_of(&doc, AGGREGATOR);

    let exempt: Vec<&str> = UNGATED.iter().map(|(j, _)| *j).collect();

    let unguarded: Vec<&String> = all
        .iter()
        .filter(|j| {
            j.as_str() != AGGREGATOR && !needs.contains(j) && !exempt.contains(&j.as_str())
        })
        .collect();
    let phantom: Vec<&String> = needs.iter().filter(|n| !all.contains(n)).collect();

    // An exemption that is also in `needs` is a contradiction: the comment says
    // this job does not gate a merge and the workflow says it does. Whichever is
    // stale, a reader is being misled.
    let contradictory: Vec<&str> = exempt
        .iter()
        .copied()
        .filter(|j| needs.iter().any(|n| n == j))
        .collect();

    // An exemption naming no job is a decision that outlived the thing it was
    // about -- the usual way an allowlist rots into permission for anything.
    let stale: Vec<&str> = exempt
        .iter()
        .copied()
        .filter(|j| !all.iter().any(|a| a == j))
        .collect();

    // Every list in one message. Renaming a job produces two faults at once -- the
    // new name unguarded, the old one phantom -- and asserting them separately
    // reports the first, hides the second, and converges over two runs. A gate that
    // tells you half of what is wrong is a gate you run twice.
    assert!(
        unguarded.is_empty() && phantom.is_empty() && contradictory.is_empty()
            && stale.is_empty(),
        "{AGGREGATOR}'s `needs:` does not match the jobs in this workflow.\n  \
         these jobs run but {AGGREGATOR} does not wait for them, so a failure in \
         them cannot block a merge: {unguarded:?}\n  \
         these `needs:` entries name no job in this workflow: {phantom:?}\n  \
         these are listed in UNGATED yet ARE in `needs`, so the stated reason is \
         false: {contradictory:?}\n  \
         these are listed in UNGATED but name no job at all: {stale:?}\n\
         `needs:` is edited in .github/workflows/ci.yml; UNGATED in this file."
    );
}

/// `if: always()`, without which the aggregator is a gate that opens on failure.
///
/// A job whose dependency failed is SKIPPED by default. `ci-ok` has a fixed name, so
/// unlike the matrix jobs it does report when skipped -- and a skipped required check
/// counts as satisfied. The gate would pass precisely when a suite had failed.
#[test]
fn the_aggregator_runs_even_when_a_suite_fails() {
    let doc = ci_yml();
    let cond = doc["jobs"][AGGREGATOR]["if"]
        .as_str()
        .unwrap_or_else(|| panic!("{AGGREGATOR} has no `if:`; it needs `always()`"));
    assert!(
        cond.contains("always()"),
        "{AGGREGATOR} has `if: {cond}`. Without always() a failed suite skips this \
         job, and a skipped check with a fixed name satisfies branch protection -- \
         the gate would open on exactly the failure it exists to catch."
    );
}

/// The aggregator's check name must be fixed, which is the whole reason it works.
#[test]
fn the_aggregator_reports_under_one_unchanging_name() {
    let doc = ci_yml();
    let job = &doc["jobs"][AGGREGATOR];
    if let Some(name) = job["name"].as_str() {
        assert!(
            !name.contains("${{"),
            "{AGGREGATOR}'s name `{name}` is interpolated, so the check name depends \
             on context. A required context that changes name is a required context \
             that can fail to report, which is the defect this job fixes."
        );
    }
    assert!(
        job["strategy"].is_null(),
        "{AGGREGATOR} has a `strategy:`. A matrix makes its check name an expansion, \
         and an expansion is not reported when the job is skipped -- which is the \
         defect this job fixes."
    );
}
