# Failure modes

What happens when something goes wrong, what the data guarantee is afterwards,
and what the operator should do.

**31 of the 32 rows name the test that observed the behaviour.** A row without a
test is a guess about the most important moment in a database's life, so the
gaps are listed at the bottom as gaps rather than filled in with what ought to
happen.

The two exceptions are rows **6** (power loss after COMMIT) and **21** (a
session transaction outliving its timeout). Both carry "not tested" in place of
an observation, and both are in the table rather than only in the gap list
because they are the question a reader asks at exactly that point in the
sequence — row 5 has just promised that a restart preserves everything, and row
20 has just described what a failed statement leaves behind.
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
| 1 | Process restarted after writes | Every created node, edge and property is present after recovery, by value | Committed writes survive a clean restart | None | `a_create_survives_whether_or_not_it_returns_the_node`, `an_edge_and_its_properties_survive` (`tests/write_durability.rs`) |
| 2 | Process restarted after a delete | The deleted node stays deleted; a `DETACH DELETE` leaves 0 edges on disk | Deletions are as durable as writes | None | `a_delete_is_not_resurrected_by_the_restart`, `deleting_a_node_persists_the_deletion_of_its_edges` (`tests/write_durability.rs`) |
| 3 | Restart after a whole-graph delete, then one write | The restart finds exactly the one new node, not the deleted three | Id reuse after a delete does not resurrect data | None | `a_deleted_graph_does_not_come_back_around_the_next_write` (`tests/graph_delete.rs`) |
| 4 | Restart after a snapshot import plus a later write | The recovered node carries the imported properties *and* the later write | An import and a write compose | None | `an_imported_node_keeps_the_properties_a_later_write_did_not_touch` (`tests/write_durability.rs`) |
| 5 | Restart, then re-run every query | A query corpus answers identically before and after; every divergence is reported | Recovery is answer-preserving, not merely count-preserving | None | `every_query_answers_the_same_after_a_persistence_restart` (`tests/query_parity_after_import.rs`) |
| 6 | **Power loss or host reset after COMMIT** | **Not tested — see the gaps below** | **None claimed.** The write is in the OS page cache, not on the platter (#1309) | Treat recent writes as at risk; snapshot for anything that must survive | — |

## Partial writes and corrupt files

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 7 | Killed mid-snapshot-flush: the file exists, the commit marker does not | The partial file is ignored; the store comes up empty rather than half-loaded | A snapshot is visible only once its marker is written | Re-run the snapshot | `restore_skips_partial_write_without_marker` (`tests/snapshot_persistence.rs`) |
| 8 | Snapshot written successfully | `.sgsnap` and `.sgsnap.committed` exist; no `.tmp` is left behind | Write-then-mark, never a half-file | None | `persist_is_atomic_no_partial_file`, `persist_writes_marker_last` |
| 9 | Importing a truncated snapshot | The import errors and the store is left with nothing | A failed import is a no-op | Fix the file and re-import | `a_failed_import_leaves_nothing_behind` (`tests/snapshot_import_rollback.rs`) |
| 10 | Importing a truncated snapshot over live data | The existing rows are untouched and still queryable | A failed import cannot damage what was already there | Re-import when the file is good | `a_failed_import_does_not_disturb_existing_data` (`tests/snapshot_import_rollback.rs`) |
| 11 | A restore that silently loses values | `verify` fails with `ValuesDiffer` even though the row counts match | A count-only check is not the check | Do not promote the restore | `properties_lost_by_a_restore_are_caught_as_values_differing` (`tests/snapshot_verify.rs`) |
| 12 | A restore that comes back empty but consistent | `everything_empty` is set and the report is not OK | An all-empty run fails even when every expectation matches | Investigate the source snapshot | `an_all_empty_run_fails_even_when_expectations_match` (`tests/snapshot_verify.rs`) |
| 13 | A snapshot catalog in an unrecognised format | Refused, with a message containing "refusing to guess" | An unknown format is never interpreted | Supply a supported catalog | `an_unknown_catalog_format_is_refused` (`tests/snapshot_verify.rs`) |
| 14 | A dangling edge arriving through the recovery path | `insert_recovered_edge` refuses an edge whose target does not exist | Recovery cannot introduce corruption | Check the WAL/snapshot source | `a_dangling_edge_cannot_be_created_through_a_public_api` (`tests/db_check_integrity.rs`) |

| 32 | **A WAL whose last record is torn** (killed between writing a length prefix and the bytes it promised) | Replay stops at the torn record and keeps every complete record before it. A short length prefix and a short record body now behave the same way; the second used to fail the whole replay | Records acknowledged before the crash survive; the unfinished one does not. A failed **checksum** still errors, because a record written in full and then damaged is a different fact from a write that did not finish | None; the log is replayable | `a_record_cut_in_its_body_does_not_discard_the_records_before_it`, `a_record_cut_in_its_length_prefix_also_replays_the_rest` (`tests/wal_torn_tail.rs`) |

## Transactions

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 15 | Client disconnects mid-transaction | The transaction is rolled back when the connection task ends; no node, no open transaction | An abandoned transaction does not hold the writer lock or its writes | None | `a_connection_that_closes_mid_transaction_rolls_it_back` (`src/protocol/server.rs`) |
| 16 | COMMIT cannot be persisted | The reply is an error, memory is rolled back from the undo log, and **disk matches** | A refused commit changes nothing, in memory or on disk | Fix the cause (quota, permissions) and retry | `a_commit_that_cannot_be_persisted_is_refused_and_rolled_back` (`src/protocol/server.rs`, `src/http/transactions.rs`) |
| 17 | Two transactions write the same entity | The second commit is refused and the first one's value stands | First committer wins; the loser writes nothing | Retry the refused transaction | `the_second_of_two_conflicting_commits_is_refused` (`tests/mvcc_isolation_anomalies.rs`), `test_write_conflict_detection` (typed `WriteConflict`) |
| 18 | A constraint refuses a commit part-way | Commit errors, the transaction's node is gone, an unrelated write in the same transaction is gone, the pre-existing value is intact, status `Aborted` | Refusal is all-or-nothing | Fix the data and retry | `a_commit_refused_by_a_constraint_changes_nothing` (`tests/mvcc_isolation_anomalies.rs`) |
| 19 | A query or rollback on a finished transaction | HTTP 404 for an unknown id, for a query in a finished transaction, and for rolling back a committed one | A finished transaction cannot be operated on | Begin a new transaction | `an_unknown_or_finished_transaction_is_refused` (`src/http/transactions.rs`) |
| 20 | **A single statement fails part-way** | The rows written before the failure stay, in memory **and on disk** — the test asserts the two agree | There is no statement rollback (LANG-07). The guarantee is that disk matches memory, not that the statement was atomic | Check what the statement wrote before retrying | `a_partial_failure_leaves_disk_agreeing_with_memory` (`tests/write_durability.rs`) |
| 21 | **A session transaction outlives its timeout** | Rolled back at the deadline: the write is gone, the store has no transaction open, and a later COMMIT is told *the transaction was open longer than Ns and was rolled back* rather than that none is open | `SAMYAMA_TX_TIMEOUT_SECS` (default 30 s) frees the writer lock, and the client learns its transaction was taken away rather than that it never had one | None; shorten the variable if 30 s is too long to hold the lock | `a_transaction_left_open_past_its_deadline_is_rolled_back` (`src/protocol/server.rs`) |

## Limits and hostile input

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 22 | A query that would explode | Refused with `ROW_BUDGET_EXCEEDED`, the budget, and the operator that blew it named in the message | A refusal identifies the cause, not just the fact | Add a filter, or raise `SAMYAMA_ROW_BUDGET` | `an_exploding_operator_is_refused_by_name` (`src/query/executor/budget.rs`) |
| 23 | A large but legitimate scan | 500 rows come back under a 100-row budget: the budget bounds explosions, not scans | The guard does not cause false refusals | None | `a_large_scan_is_not_an_explosion_and_is_not_refused` |
| 24 | An unparseable budget setting | Falls back to the default, not to unlimited | A typo cannot silently disable the guard | Fix the variable | `an_unparseable_budget_falls_back_to_the_default_not_to_unlimited` |
| 25 | A result too large for the cache | Not cached, the answer is still returned in full, and the warm entries are not flushed | One oversized answer cannot evict the working set | None | `an_answer_larger_than_the_whole_budget_is_not_cached`, `an_oversized_answer_does_not_flush_the_warm_entries` (`tests/result_cache_budget.rs`) |
| 26 | A tenant exceeding its node quota | `QuotaExceeded` on the check, and the write path refuses the node past the limit | A quota is enforced where writes happen, not only where they are counted | Raise the quota or delete data | `test_quota_enforcement` (`src/persistence/tenant.rs`, `src/persistence/mod.rs`) |
| 27 | Creating a tenant that already exists | HTTP 409 | Tenant ids are unique | Use the existing tenant | `duplicate_tenant_creation_returns_conflict` (`tests/tenant_registry_unification.rs`) |
| 28 | Deleting the default tenant | HTTP 403 | The default tenant cannot be removed | None | `cannot_delete_default_tenant` |
| 29 | Injection through a query parameter | Eight hostile strings each return 0 rows and leave `node_count()` unchanged | A bound parameter is a value and never a clause | None | `hostile_parameter_values_cannot_add_a_clause_or_write` (`tests/catalog_bound_params.rs`) |
| 30 | An out-of-range integer literal | Refused with "out of range" — control returns, nothing panics | Bad input is an error, not a crash | Fix the query | `an_out_of_range_literal_is_refused_rather_than_crashing` (`tests/integer_literals.rs`) |
| 31 | A malformed HTTP body or missing content type | 400 and 415 respectively | Malformed requests are rejected by status, not by guesswork | Fix the client | `test_query_handler_malformed_json_returns_error`, `test_query_handler_missing_content_type` (`src/http/handler.rs`) |

## Gaps — failures with no test, so no row

These are listed because a failure-mode table that omits what it has not tried
is more misleading than one that admits it. Each is a test to write, tracked in
#1311.

| Failure | Why it is not written down |
|---|---|
| **Disk full, or any IO error on a write path** | Nothing injects `ENOSPC`, a read-only directory or an `io::Error`. The commit-refused tests (rows 16) reach that code through a *quota* refusal, so they cannot stand in for it |
| **Process killed mid-write (SIGKILL)** | No test spawns and kills a process. Rows 7 and 20 are the nearest proxies and neither is a real crash |
| **WAL replay after a crash** | Every WAL test replays a WAL the same process just wrote and flushed. Row 32 truncates one deliberately, which is not the same as replaying one a killed process left behind |
| **A query deadline exceeded** | `with_deadline` and `check_deadline` exist and nothing drives a query past one. The *transaction* timeout is row 21; the query deadline is still untested |
| **Out of memory** | The memory quota checks a bookkeeping counter, not process memory, and no test exercises an allocation failure |
| **Replica lag, or a node losing leadership** | Neither exists to test: `RaftNode::write` applies locally and `initialize` makes the node leader unconditionally (#1309) |

## A note on what counts as a row here

A test that asserts a call returned an error is not evidence of a failure mode.
It says something failed, not what the operator sees. Rows above name tests that
pin an error code, an HTTP status, a typed error variant, or a value that
survived — and where both memory and disk are checked, the row says so, because
that is the pair that matters after a crash.
