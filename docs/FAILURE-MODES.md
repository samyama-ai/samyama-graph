# Failure modes

What happens when something goes wrong, what the data guarantee is afterwards,
and what the operator should do.

**31 of the 32 rows name the test that observed the behaviour**, at the
`path:line` of the test's signature. A row without a test is a guess about the
most important moment in a database's life, so the gaps are listed at the
bottom as gaps rather than filled in with what ought to happen.

The one exception is row **6** (power loss after COMMIT). It carries "not
tested" in place of an observation, and it is in the table rather than only in
the gap list because it is the question a reader asks at exactly that point in
the sequence — row 5 has just promised that a restart preserves everything.
Run any row for yourself:

```bash
cargo test <test name>
```

Read this next to [`ACID_GUARANTEES.md`](ACID_GUARANTEES.md), which says what is
guaranteed; this page says what is observed when the guarantee is tested. In
particular: **nothing on the write path is fsynced** (§4 there, #1309), so every
row below that says "survives a restart" means a clean restart, not a power cut.

## Durability and restart

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 1 | Process restarted after writes | Every created node, edge and property is present after recovery, by value | Committed writes survive a clean restart | None | `a_create_survives_whether_or_not_it_returns_the_node` (`tests/write_durability.rs:52`), `an_edge_and_its_properties_survive` (`tests/write_durability.rs:91`) |
| 2 | Process restarted after a delete | The deleted node stays deleted; a `DETACH DELETE` leaves 0 edges on disk | Deletions are as durable as writes | None | `a_delete_is_not_resurrected_by_the_restart` (`tests/write_durability.rs:80`), `deleting_a_node_persists_the_deletion_of_its_edges` (`tests/write_durability.rs:109`) |
| 3 | Restart after a whole-graph delete, then one write | The restart finds exactly the one new node, not the deleted three | Id reuse after a delete does not resurrect data | None | `a_deleted_graph_does_not_come_back_around_the_next_write` (`tests/graph_delete.rs:70`) |
| 4 | Restart after a snapshot import plus a later write | The recovered node carries the imported properties *and* the later write | An import and a write compose | None | `an_imported_node_keeps_the_properties_a_later_write_did_not_touch` (`tests/write_durability.rs:202`) |
| 5 | Restart, then re-run every query | A query corpus answers identically before and after; every divergence is reported | Recovery is answer-preserving, not merely count-preserving | None | `every_query_answers_the_same_after_a_persistence_restart` (`tests/query_parity_after_import.rs:188`) |
| 6 | **Power loss or host reset after COMMIT** | **Not tested — see the gaps below** | **None claimed.** The write is in the OS page cache, not on the platter (#1309) | Treat recent writes as at risk; snapshot for anything that must survive | — |

## Partial writes and corrupt files

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 7 | Killed mid-snapshot-flush: the file exists, the commit marker does not | The partial file is ignored; the store comes up empty rather than half-loaded | A snapshot is visible only once its marker is written | Re-run the snapshot | `restore_skips_partial_write_without_marker` (`tests/snapshot_persistence.rs:56`) |
| 8 | Snapshot written successfully | `.sgsnap` and `.sgsnap.committed` exist; no `.tmp` is left behind | Write-then-mark, never a half-file | None | `persist_is_atomic_no_partial_file` (`tests/snapshot_persistence.rs:73`), `persist_writes_marker_last` (`src/snapshot/persist.rs:100`) |
| 9 | Importing a truncated snapshot | The import errors and the store is left with nothing | A failed import is a no-op | Fix the file and re-import | `a_failed_import_leaves_nothing_behind` (`tests/snapshot_import_rollback.rs:41`) |
| 10 | Importing a truncated snapshot over live data | The existing rows are untouched and still queryable | A failed import cannot damage what was already there | Re-import when the file is good | `a_failed_import_does_not_disturb_existing_data` (`tests/snapshot_import_rollback.rs:56`) |
| 11 | A restore that silently loses values | `verify` fails with `ValuesDiffer` even though the row counts match | A count-only check is not the check | Do not promote the restore | `properties_lost_by_a_restore_are_caught_as_values_differing` (`tests/snapshot_verify.rs:128`) |
| 12 | A restore that comes back empty but consistent | `everything_empty` is set and the report is not OK | An all-empty run fails even when every expectation matches | Investigate the source snapshot | `an_all_empty_run_fails_even_when_expectations_match` (`tests/snapshot_verify.rs:95`) |
| 13 | A snapshot catalog in an unrecognised format | Refused, with a message containing "refusing to guess" | An unknown format is never interpreted | Supply a supported catalog | `an_unknown_catalog_format_is_refused` (`tests/snapshot_verify.rs:213`) |
| 14 | A dangling edge arriving through the recovery path | `insert_recovered_edge` refuses an edge whose target does not exist | Recovery cannot introduce corruption | Check the WAL/snapshot source | `a_dangling_edge_cannot_be_created_through_a_public_api` (`tests/db_check_integrity.rs:62`) |

| 32 | **A WAL whose last record is torn** (killed between writing a length prefix and the bytes it promised) | Replay stops at the torn record and keeps every complete record before it. A short length prefix and a short record body now behave the same way; the second used to fail the whole replay | Complete records replay; the unfinished one does not. The tests write and flush in-process, so nothing was acknowledged to a client and no process died. A record written in full and then damaged is a different case and no test damages one | None; the log is replayable | `a_record_cut_in_its_body_does_not_discard_the_records_before_it` (`tests/wal_torn_tail.rs:68`), `a_record_cut_in_its_length_prefix_also_replays_the_rest` (`tests/wal_torn_tail.rs:83`), control `an_untouched_wal_replays_everything` (`tests/wal_torn_tail.rs:114`) |

## Transactions

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 15 | Client disconnects mid-transaction | The transaction is rolled back when the connection task ends; no node, no open transaction | An abandoned transaction does not hold the writer lock or its writes | None | `a_connection_that_closes_mid_transaction_rolls_it_back` (`src/protocol/server.rs:472`) |
| 16 | COMMIT cannot be persisted | The reply is an error and memory is rolled back from the undo log. **Disk was checked on the RESP path only**: the refused SET is still `1` on disk and the refused CREATE is absent. The HTTP test asserts memory alone — `node_count() == 0` and no open transaction, no disk read | A refused commit changes nothing in memory on either path, and nothing on disk on the RESP path | Fix the cause and retry | `a_commit_that_cannot_be_persisted_is_refused_and_rolled_back` (`src/protocol/server.rs:598`, `src/http/transactions.rs:245`) |
| 17 | Two transactions write the same entity | The second commit is refused and the first one's value stands | First committer wins; the loser writes nothing | Retry the refused transaction | `the_second_of_two_conflicting_commits_is_refused` (`tests/mvcc_isolation_anomalies.rs:97`), `test_write_conflict_detection` (`src/graph/store.rs:8420`, typed `WriteConflict`) |
| 18 | A constraint refuses a commit part-way | Commit errors, the transaction's node is gone, an unrelated write in the same transaction is gone, the pre-existing value is intact, status `Aborted` | Refusal is all-or-nothing | Fix the data and retry | `a_commit_refused_by_a_constraint_changes_nothing` (`tests/mvcc_isolation_anomalies.rs:166`) |
| 19 | A query or rollback on a finished transaction | HTTP 404 for an unknown id, for a query in a finished transaction, and for rolling back a committed one | A finished transaction cannot be operated on | Begin a new transaction | `an_unknown_or_finished_transaction_is_refused` (`src/http/transactions.rs:222`) |
| 20 | **A single statement fails part-way** | The rows written before the failure stay, in memory **and on disk** — the test asserts the two agree | There is no statement rollback (LANG-07). The guarantee is that disk matches memory, not that the statement was atomic | Check what the statement wrote before retrying | `a_partial_failure_leaves_disk_agreeing_with_memory` (`tests/write_durability.rs:160`) |
| 21 | **A session transaction outlives its timeout** | Rolled back at the deadline: the write is gone, the store has no transaction open, and a later COMMIT is told *the transaction was open longer than Ns and was rolled back* rather than that none is open. RESP path only | `SAMYAMA_TX_TIMEOUT_SECS` (default 30 s) frees the writer lock, and the client learns its transaction was taken away rather than that it never had one | None; shorten the variable if 30 s is too long to hold the lock | `a_transaction_left_open_past_its_deadline_is_rolled_back` (`src/protocol/server.rs:519`) |

## Limits and hostile input

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 22 | A query that would explode | Refused with `ROW_BUDGET_EXCEEDED`, the budget, and the operator that blew it named in the message | A refusal identifies the cause, not just the fact | Add a filter, or raise `SAMYAMA_ROW_BUDGET` | `an_exploding_operator_is_refused_by_name` (`src/query/executor/budget.rs:311`) |
| 23 | A large but legitimate scan | 500 rows come back under a 100-row budget: the budget bounds explosions, not scans | The guard does not cause false refusals | None | `a_large_scan_is_not_an_explosion_and_is_not_refused` (`src/query/executor/budget.rs:425`) |
| 24 | An unparseable budget setting | Falls back to the default, not to unlimited | A typo cannot silently disable the guard | Fix the variable | `an_unparseable_budget_falls_back_to_the_default_not_to_unlimited` (`src/query/executor/budget.rs:448`) |
| 25 | A result too large for the cache | Not cached, the answer is still returned in full, and the warm entries are not flushed | One oversized answer cannot evict the working set | None | `an_answer_larger_than_the_whole_budget_is_not_cached` (`tests/result_cache_budget.rs:63`), `an_oversized_answer_does_not_flush_the_warm_entries` (`tests/result_cache_budget.rs:86`) |
| 26 | A tenant exceeding its node quota | The 4th `CREATE` under a 3-node quota is a failed statement carrying `nodes (3/3)` and `QUOTA_EXCEEDED`, and explicitly not a `DatabaseError` | A quota is admission control: the write is refused before the row is created, so a quota can never make a persist fail | Raise the quota or delete data | `a_create_at_the_ceiling_is_a_failed_statement_and_not_a_failed_persist` (`tests/quota_is_admission_control.rs:86`) |
| 27 | Creating a tenant that already exists | HTTP 409 | Tenant ids are unique | Use the existing tenant | `duplicate_tenant_creation_returns_conflict` (`tests/tenant_registry_unification.rs:117`) |
| 28 | Deleting the default tenant | HTTP 403 | The default tenant cannot be removed | None | `cannot_delete_default_tenant` (`tests/tenant_registry_unification.rs:164`) |
| 29 | Injection through a query parameter | Eight hostile strings each return 0 rows and leave `node_count()` unchanged | A bound parameter is a value and never a clause | None | `hostile_parameter_values_cannot_add_a_clause_or_write` (`tests/catalog_bound_params.rs:138`) |
| 30 | An out-of-range integer literal | Refused with "out of range" — control returns, nothing panics | Bad input is an error, not a crash | Fix the query | `an_out_of_range_literal_is_refused_rather_than_crashing` (`tests/integer_literals.rs:66`) |
| 31 | A malformed HTTP body or missing content type | 400 and 415 respectively | Malformed requests are rejected by status, not by guesswork | Fix the client | `test_query_handler_malformed_json_returns_error` (`src/http/handler.rs:2574`), `test_query_handler_missing_content_type` (`src/http/handler.rs:2591`) |

## Weak assertions

These tests exist and pass. They are not rows above, because of what they do
not assert. Listed so that a reader who finds them does not read them as
stronger evidence than they are.

| Test | What it asserts | What it is not evidence of |
|---|---|---|
| `an_altered_byte_fails` (`src/snapshot/encryption.rs:365`) | `open()` returns `is_err()` after one flipped bit | That corruption is detected and named. No error class, no message |
| `test_quota_enforcement_connections` (`src/persistence/tenant.rs:1119`) | `check_quota` returns `is_err()` at 2/2 connections | What a client sees. No error class is checked |
| `a_malformed_temporal_tag_does_not_become_a_plausible_value` (`src/snapshot/mod.rs:2335`) | One `assert_ne!`: a `Date` tag with no `days` is not `Date(0)` | That the malformed tag errors. It pins what the value is not, not that anything is refused |
| `test_raft_node_write_before_init` (`src/raft/node.rs:228`) | `node.write()` returns `is_err()` before `initialize` | Anything about the error a caller gets |
| `test_quota_enforcement_memory` (`src/persistence/tenant.rs:1096`) | `TenantError::QuotaExceeded` at 1024/1024 bytes | OOM behaviour. It measures a bookkeeping counter, not process memory |

## Gaps — failures with no test, so no row

These are listed because a failure-mode table that omits what it has not tried
is more misleading than one that admits it. Each is a test to write, tracked in
#1311.

| Failure | Why it is not written down |
|---|---|
| **Disk full, or any IO error on a write path** | Nothing injects `ENOSPC`, a read-only directory or an `io::Error`. The two commit-refused tests in row 16 call `pm.fail_next_apply_for_test()`: injection at the apply layer, not real IO. They pin the rollback, not what a filesystem error does on the way to it |
| **Process killed mid-write (SIGKILL)** | No test spawns and kills a process. Rows 7 and 20 are the nearest proxies and neither is a real crash |
| **WAL replay after a crash** | Every WAL test replays a WAL the same process just wrote and flushed. Row 32 truncates one deliberately, which is not the same as replaying one a killed process left behind |
| **A WAL record damaged after being written in full** | The three tests in `tests/wal_torn_tail.rs` cut a record's body, cut its length prefix, and leave one intact. None flips a byte inside a complete record, so nothing observes a checksum failure |
| **A query deadline exceeded** | `with_deadline` and `check_deadline` exist and nothing drives a query past one. The *transaction* timeout is row 21; the query deadline is still untested |
| **An HTTP transaction outliving its timeout** | `commit_handler` returns 409 on `Instant::now() > txn.deadline` (`src/http/transactions.rs:104`) and no test reaches it — `begin_handler` spawns a task that removes the transaction at the same deadline, so a late commit takes the 404 `not_open` path instead and the 409 is a race (#1518). Row 21 covers the RESP path only |
| **`max_query_time_ms` enforcement** | The field exists (`src/persistence/tenant.rs:55`) and four tests serialise it (`:918, :1355, :1618, :1700`). Nothing reads it to stop a query, so there is no behaviour to record |
| **Out of memory** | The memory quota checks a bookkeeping counter, not process memory, and no test exercises an allocation failure |
| **Replica lag, or a node losing leadership** | Neither exists to test: `RaftNode::write` applies locally and `initialize` makes the node leader unconditionally (#1309) |

## A note on what counts as a row here

A test that asserts a call returned an error is not evidence of a failure mode.
It says something failed, not what the operator sees. Rows above name tests that
pin an error code, an HTTP status, a typed error variant, or a value that
survived — and where both memory and disk are checked, the row says so, because
that is the pair that matters after a crash.
