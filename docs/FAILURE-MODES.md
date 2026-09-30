# Failure modes

What happens when something goes wrong, what the data guarantee is afterwards,
and what the operator should do.

**41 of the 42 rows name the test that observed the behaviour**, at the
`path:line` of the test's signature. (Two rows carry the number 33, one under
snapshots and one under the WAL; both are counted.) A row without a test is a
guess about the most important moment in a database's life, so the gaps are
listed at the bottom as gaps rather than filled in with what ought to happen.

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
particular: **nothing on the write path is fsynced by default** (§4 there, #1309);
`SAMYAMA_FSYNC=1` turns the barrier on for both the WAL and RocksDB. So on a
stock server every row below that says "survives a restart" means a clean
restart, not a power cut.

## Durability and restart

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 1 | Process restarted after writes | Every created node, edge and property is present after recovery, by value | Committed writes survive a clean restart | None | `a_create_survives_whether_or_not_it_returns_the_node` (`tests/write_durability.rs:52`), `an_edge_and_its_properties_survive` (`tests/write_durability.rs:91`) |
| 2 | Process restarted after a delete | The deleted node stays deleted; a `DETACH DELETE` leaves 0 edges on disk | Deletions are as durable as writes | None | `a_delete_is_not_resurrected_by_the_restart` (`tests/write_durability.rs:80`), `deleting_a_node_persists_the_deletion_of_its_edges` (`tests/write_durability.rs:109`) |
| 3 | Restart after a whole-graph delete, then one write | The restart finds exactly the one new node, not the deleted three | Id reuse after a delete does not resurrect data | None | `a_deleted_graph_does_not_come_back_around_the_next_write` (`tests/graph_delete.rs:70`) |
| 4 | Restart after a snapshot import plus a later write | The recovered node carries the imported properties *and* the later write | An import and a write compose | None | `an_imported_node_keeps_the_properties_a_later_write_did_not_touch` (`tests/write_durability.rs:202`) |
| 5 | Restart, then re-run every query | A query corpus answers identically before and after; every divergence is reported | Recovery is answer-preserving, not merely count-preserving | None | `every_query_answers_the_same_after_a_persistence_restart` (`tests/query_parity_after_import.rs:188`) |
| 34 | **The server process is killed (`SIGKILL`) while a client is writing** | Every write the server acknowledged is there after a restart on the same directory: each `CREATE` over RESP, and each `SET` on every fifth node. Nothing that was never issued is present and no node comes back twice. The one write in flight at the kill is neither acknowledged nor refused; repeated runs of the test have recovered it in some runs and not in others. What recovery read is RocksDB, which replays its own log on open: the server does not replay samyama's `wal/` directory at start-up at all | An acknowledged auto-commit write survives the process dying, with the stock configuration (no `SAMYAMA_FSYNC`). A process kill, not a power cut: row 6 still applies | Restart on the same `--data-path`; check whether the write that had no reply landed before re-sending it | `every_acknowledged_write_survives_a_sigkill_mid_write` (`tests/server_binary.rs:1104`) |
| 6 | **Power loss or host reset after COMMIT** | **Not tested — see the gaps below** | **None claimed** by default. The write is in the OS page cache, not on the platter (#1309). With `SAMYAMA_FSYNC=1` both halves are synced before the reply, but no test cuts power to confirm it | Treat recent writes as at risk; snapshot for anything that must survive | — |

## Partial writes and corrupt files

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 7 | Killed mid-snapshot-flush: the file exists, the commit marker does not | The partial file is ignored; the store comes up empty rather than half-loaded | A snapshot is visible only once its marker is written | Re-run the snapshot | `restore_skips_partial_write_without_marker` (`tests/snapshot_persistence.rs:56`) |
| 8 | Snapshot written successfully | `.sgsnap` and `.sgsnap.committed` exist; no `.tmp` is left behind | Write-then-mark, never a half-file | None | `persist_is_atomic_no_partial_file` (`tests/snapshot_persistence.rs:73`), `persist_writes_marker_last` (`src/snapshot/persist.rs:108`) |
| 33 | A snapshot write fails (full disk, quota, `EIO`) after an earlier one committed | The persist returns the error, the tmp file is removed, and the previous `.sgsnap` and its `.committed` marker are untouched, so a restart restores it | A failed persist never costs the last committed snapshot (#1520) | Free space and re-run the snapshot | `a_failed_persist_keeps_the_last_good_snapshot` (`src/snapshot/persist.rs:121`) |
| 9 | Importing a truncated snapshot | The import errors and the store is left with nothing | A failed import is a no-op | Fix the file and re-import | `a_failed_import_leaves_nothing_behind` (`tests/snapshot_import_rollback.rs:41`) |
| 10 | Importing a truncated snapshot over live data | The existing rows are untouched and still queryable | A failed import cannot damage what was already there | Re-import when the file is good | `a_failed_import_does_not_disturb_existing_data` (`tests/snapshot_import_rollback.rs:56`) |
| 11 | A restore that silently loses values | `verify` fails with `ValuesDiffer` even though the row counts match | A count-only check is not the check | Do not promote the restore | `properties_lost_by_a_restore_are_caught_as_values_differing` (`tests/snapshot_verify.rs:130`) |
| 12 | A restore that comes back empty but consistent | `everything_empty` is set and the report is not OK | An all-empty run fails even when every expectation matches | Investigate the source snapshot | `an_all_empty_run_fails_even_when_expectations_match` (`tests/snapshot_verify.rs:95`) |
| 13 | A snapshot catalog in an unrecognised format | Refused, with a message containing "refusing to guess" | An unknown format is never interpreted | Supply a supported catalog | `an_unknown_catalog_format_is_refused` (`tests/snapshot_verify.rs:215`) |
| 38 | A catalog that is not the one the snapshot was published with (edited, swapped, or from another build) | `verify` exits 1 before any query runs, naming both SHA-256s and the file the header records; `catalog-gate --snapshot` refuses the pair | A catalog whose bytes do not match the header's `queries.sha256` is never executed against that snapshot. A snapshot whose header names no catalog verifies exactly as before | Use the catalog the header names, or rebuild the pair with `catalog-build --link` | `verify_refuses_a_catalog_whose_digest_the_header_does_not_record` (`src/main_cov_tests.rs:652`), `catalog_gate_with_a_snapshot_checks_the_pair` (`src/main_cov_tests.rs:710`), `a_linked_catalog_is_found_by_verify_and_a_swapped_one_is_refused` (`tests/server_binary.rs:355`) |
| 39 | The catalog a snapshot's header names is missing from beside it | `verify` without `--queries` exits 66, naming the file the header promised | Refused rather than skipped: a verify that ran nothing would read as a restore that passed | Fetch the catalog published with the snapshot, or pass one with `--queries` | `verify_refuses_when_the_catalog_the_header_names_is_missing` (`src/main_cov_tests.rs:675`) |
| 40 | A question catalog offered for publication that was not built for release, or predates the release stamp | `catalog-gate` exits 1 with `REFUSED`, naming `catalog-build ... --release`; no gate flag overrides it | A catalog is published only when `catalog-build --release` said so; never inferred from the tenant, and a catalog with no `publishable` key is treated as private | Rebuild with `catalog-build --release` (and `--link` again, if linked) | `catalog_gate_refuses_a_catalog_not_stamped_for_release` (`src/main_cov_tests.rs:804`), `catalog_build_stamps_the_tenant_and_is_private_without_release` (`src/main_cov_tests.rs:479`) |
| 41 | An observed catalog cleared with `--allow-observed` but no written sign-off | `catalog-gate` exits 64 before reading the catalog; with `--signoff <text>` it prints `SIGNOFF` with the catalog's SHA-256 | Observed questions are never published without a recorded, non-blank sign-off bound to the exact file | Re-run with `--signoff "<who approved it, and why>"` and keep the output with the release | `an_observed_catalog_is_published_only_with_a_recorded_signoff` (`tests/server_binary.rs:451`), `catalog_gate_refuses_an_observed_catalog_without_the_flag` (`src/main_cov_tests.rs:785`) |
| 36 | A snapshot property whose temporal tag is malformed (`{"__type": "Date"}` with no `days`, or, for `Date`, a `days` that is not a number) | Not refused. It comes back as the map it is, `__type` and all -- for all six temporal tags -- rather than as `Date(0)`, `Null` or a missing property | A corrupt temporal is never read as a plausible value. It is not reported either: the conversion flags nothing | Look for `__type` keys in map-valued properties after an import from an untrusted source | `a_malformed_temporal_tag_does_not_become_a_plausible_value` (`src/snapshot/mod.rs:2492`) |
| 14 | A dangling edge arriving through the recovery path | `insert_recovered_edge` refuses an edge whose target does not exist | Recovery cannot introduce corruption | Check the WAL/snapshot source | `a_dangling_edge_cannot_be_created_through_a_public_api` (`tests/db_check_integrity.rs:62`) |

| 32 | **A WAL whose last record is torn** (killed between writing a length prefix and the bytes it promised) | Replay stops at the torn record and keeps every complete record before it. A short length prefix and a short record body now behave the same way; the second used to fail the whole replay | Complete records replay; the unfinished one does not. The tests write and flush in-process, so nothing was acknowledged to a client and no process died. A record written in full and then damaged is a different case: row 33 | None; the log is replayable | `a_record_cut_in_its_body_does_not_discard_the_records_before_it` (`tests/wal_torn_tail.rs:68`), `a_record_cut_in_its_length_prefix_also_replays_the_rest` (`tests/wal_torn_tail.rs:83`), control `an_untouched_wal_replays_everything` (`tests/wal_torn_tail.rs:114`) |
| 33 | **A WAL record damaged after being written in full** (bytes changed inside a complete record) | Replay stops with `WalError::Corruption(offset)`, the offset being the damaged record's position in its file. The records before it have been replayed; the damaged one and everything after it have not. Observed for the same bit flipped in two payload bytes and for one bit flipped in the sequence number -- both invisible to the XOR "checksum" that CRC-32 replaced (#1311). A WAL written in the old format still replays, and a legacy record that fails its old XOR check is refused the same way | A damaged record is never applied. Nothing after it is applied either, so replay does not skip over a hole | Treat as a damaged disk: restore from a snapshot, or truncate the WAL file (named in the warning log line) at the reported offset and accept losing what follows | `the_same_bit_flipped_in_two_bytes_of_a_record_is_corruption` (`tests/wal_checksum.rs:82`), `a_flipped_bit_in_the_sequence_number_is_corruption` (`tests/wal_checksum.rs:114`), old format `a_wal_written_in_the_old_format_still_replays_and_takes_new_records` (`tests/wal_checksum.rs:171`), `a_legacy_record_that_fails_its_old_checksum_is_still_corruption` (`tests/wal_checksum.rs:193`) |

## Transactions

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 15 | Client disconnects mid-transaction | The transaction is rolled back when the connection task ends; no node, no open transaction | An abandoned transaction does not hold the writer lock or its writes | None | `a_connection_that_closes_mid_transaction_rolls_it_back` (`src/protocol/server.rs:472`) |
| 16 | COMMIT cannot be persisted | The reply is an error and memory is rolled back from the undo log. **Disk was checked on the RESP path only**: the refused SET is still `1` on disk and the refused CREATE is absent. The HTTP test asserts memory alone — `node_count() == 0` and no open transaction, no disk read | A refused commit changes nothing in memory on either path, and nothing on disk on the RESP path | Fix the cause and retry | `a_commit_that_cannot_be_persisted_is_refused_and_rolled_back` (`src/protocol/server.rs:598`, `src/http/transactions.rs:245`) |
| 17 | Two transactions write the same entity | The second commit is refused and the first one's value stands | First committer wins; the loser writes nothing | Retry the refused transaction | `the_second_of_two_conflicting_commits_is_refused` (`tests/mvcc_isolation_anomalies.rs:97`), `test_write_conflict_detection` (`src/graph/store.rs:8420`, typed `WriteConflict`) |
| 18 | A constraint refuses a commit part-way | Commit errors, the transaction's node is gone, an unrelated write in the same transaction is gone, the pre-existing value is intact, status `Aborted` | Refusal is all-or-nothing | Fix the data and retry | `a_commit_refused_by_a_constraint_changes_nothing` (`tests/mvcc_isolation_anomalies.rs:166`) |
| 19 | A query or rollback on a finished transaction | HTTP 404 for an unknown id, for a query in a finished transaction, and for rolling back a committed one | A finished transaction cannot be operated on | Begin a new transaction | `an_unknown_or_finished_transaction_is_refused` (`src/http/transactions.rs:297`) |
| 20 | **A single statement fails part-way** | The rows written before the failure stay, in memory **and on disk** — the test asserts the two agree | There is no statement rollback (LANG-07). The guarantee is that disk matches memory, not that the statement was atomic | Check what the statement wrote before retrying | `a_partial_failure_leaves_disk_agreeing_with_memory` (`tests/write_durability.rs:160`) |
| 21 | **A session transaction outlives its timeout** | Rolled back at the deadline: the write is gone, the store has no transaction open, and a later COMMIT is told *the transaction was open longer than Ns and was rolled back* rather than that none is open. Over HTTP, COMMIT, ROLLBACK and a query on the expired id all get **409** naming the timeout, while an id that never existed is still 404; the last 1024 expired ids are remembered (#1518) | `SAMYAMA_TX_TIMEOUT_SECS` (default 30 s) frees the writer lock, and the client learns its transaction was taken away rather than that it never had one | None; shorten the variable if 30 s is too long to hold the lock | `a_transaction_left_open_past_its_deadline_is_rolled_back` (`src/protocol/server.rs:519`), `a_timed_out_transaction_is_reported_as_timed_out_not_unknown` (`src/http/transactions.rs:315`), `a_commit_past_the_deadline_is_refused_as_timed_out` (`src/http/transactions.rs:340`) |

## Limits and hostile input

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 35 | **A read query runs past its deadline** | Stopped with an error containing `Query timed out`, and no rows. The same engine answers the next query: the deadline is per query. Driven through `QueryEngine`, which sets the deadline from `SAMYAMA_QUERY_TIMEOUT` (whole seconds, default 120; the test uses 1) | A read cannot hold the store past its deadline by more than the gap to the next check: the check is cooperative, between batches, not a preemption. **Write statements get no deadline** -- `execute_mut` sets none | Narrow the query, or raise `SAMYAMA_QUERY_TIMEOUT` | `a_query_past_the_engine_deadline_is_stopped_with_a_timeout_error` (`tests/query_deadline.rs:30`) |
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

## Replication

| # | Failure | Observed behaviour | Data guarantee | Operator action | Test |
|---|---|---|---|---|---|
| 37 | A write sent to a Raft node before `initialize` | Refused with `RaftError::Raft("Raft not initialized")`. Nothing is applied: the store holds no node and the log index and applied index stay at 0. The same write after `initialize` lands | An uninitialised node applies nothing. That is all it says: an initialised node applies locally and is leader unconditionally, so there is no replication to fail yet (#1309) | Initialise the node | `test_raft_node_write_before_init` (`src/raft/node.rs:232`) |

## Weak assertions

These tests exist and pass. They are not rows above, because of what they do
not assert. Listed so that a reader who finds them does not read them as
stronger evidence than they are.

| Test | What it asserts | What it is not evidence of |
|---|---|---|
| `an_altered_byte_fails` (`src/snapshot/encryption.rs:365`) | `open()` returns `is_err()` after one flipped bit | That corruption is detected and named. No error class, no message |
| `test_quota_enforcement_connections` (`src/persistence/tenant.rs:1119`) | `QuotaExceeded { tenant: "t1", resource: "connections (2/2)" }` at 2/2, allowed at 1/2 and again after a decrement | What a client sees. The API's error is pinned, but no server path counts or checks connections (see the gaps), so a client never meets it |
| `test_quota_enforcement_memory` (`src/persistence/tenant.rs:1096`) | `TenantError::QuotaExceeded` at 1024/1024 bytes | OOM behaviour. It measures a bookkeeping counter, not process memory |

## Gaps — failures with no test, so no row

These are listed because a failure-mode table that omits what it has not tried
is more misleading than one that admits it. Each is a test to write, tracked in
#1311.

| Failure | Why it is not written down |
|---|---|
| **Disk full, or any IO error on a write path** | Only row 33 injects a real `io::Error` (a directory where the snapshot's tmp file goes). Nothing injects `ENOSPC` or a read-only directory on the WAL or RocksDB paths. The two commit-refused tests in row 16 call `pm.fail_next_apply_for_test()`: injection at the apply layer, not real IO. They pin the rollback, not what a filesystem error does on the way to it |
| **Process killed mid-write, beyond one kill point** | Row 34 kills one server once per run, over RESP auto-commit, after about 150 acknowledged writes. A kill inside an HTTP transaction's COMMIT, a kill against a large store, and many kill points are the hand-run harness below (#1355), not a `cargo test` |
| **samyama's WAL replayed after a crash** | The server never replays `wal/` at start-up: `Wal::replay` is called only from tests, and recovery reads RocksDB (whose own log RocksDB replays on open -- what row 34 exercises). Every WAL test replays a WAL the same process wrote; rows 32 and 33 damage one deliberately. Until something reads the WAL on start-up, replaying one a killed process left behind is not a behaviour of the server |
| **A deadline on writes, and on the served paths** | Row 35 drives a read through `QueryEngine`. `execute_mut` sets no deadline, so a write statement has none to exceed. The RESP and HTTP handlers and the streaming read path (`execute_streaming_with_params`, which sets the same deadline) are not driven past it |
| **`max_query_time_ms` enforcement** | The field exists (`src/persistence/tenant.rs:55`) and four tests serialise it (`:918, :1382, :1645, :1727`). Nothing reads it to stop a query -- the only deadline is the process-wide `SAMYAMA_QUERY_TIMEOUT` of row 35 -- so there is no per-tenant behaviour to record |
| **The connection quota** | `check_quota(tenant, "connections")` refuses at the limit with the count in the message (see the weak assertions), but nothing outside `src/persistence/tenant.rs` increments, decrements or checks `"connections"`. A client opening connections past `max_connections` is not refused |
| **Out of memory** | The memory quota checks a bookkeeping counter, not process memory, and no test exercises an allocation failure |
| **Replica lag, or a node losing leadership** | Neither exists to test: `RaftNode::write` applies locally and `initialize` makes the node leader unconditionally (#1309) |

### Measured outside the test suite: `kill -9` mid-write (REL-04, #1355)

`scripts/crash_consistency.py` (#1352) starts the server binary with
`--data-path` on a fresh directory, runs a writer on its own thread (`CREATE`,
a `SET` on every fifth write, a read on every seventh), sends `SIGKILL` to the
server's process group after a random delay, restarts it on the same
directory and asks which writes survived. It is a Python harness run by hand,
not a `cargo test`, so it is not a row above and CI does not run it. Row 34
is its counterpart inside the suite: the same one-sided check, one kill point
per run, over RESP rather than HTTP.

The run recorded in #1355 — binary pinned at `f8c0300`, kill delay uniform in
[0.05, 0.8] s:

| mode (what counts as an acknowledgement) | kill points | acknowledged writes | lost |
|---|---|---|---|
| `auto` — the 200 on the statement | 500 | 587,488 | 0 |
| `tx` — the 200 on `POST /api/tx/:id/commit` | 500 | 318,419 | 0 |
| **total** | **1000** | **905,907** | **0** |

No recovery had a gap (a surviving write with an earlier issued write missing),
no write was present that had not been acknowledged, and no cycle failed to
run. A write in flight when the signal landed is neither acknowledged nor
refused and is excluded: that happened 908 times, and 176 of those had reached
disk anyway. Median 796 writes per cycle, maximum 2,710.

What it does not say:

- **Shallow kill points.** Nothing here crashes a server holding a large graph.
- **A process kill, not a power cut or host reset.** Row 6 is unchanged: the
  script does not set `SAMYAMA_FSYNC`, and a killed process leaves the page
  cache to the kernel.
- **Single node.** Raft is not in the loop, and #1309 is a separate question.
- REL-04's H2 and H3 — 10,000 kill points, torn-write simulation,
  fault-injected fsync failures — are not attempted.
- Survival is checked by the `seq` of each `CREATE`d node; the `SET` the
  workload also issues is not checked after restart. Row 34 checks it.

Reproduce (build first with `cargo build --release --bin samyama`; copy the
binary and pass `--binary` for a long sweep, because a rebuild replaces
`target/release/samyama` in place):

```bash
python3 scripts/crash_consistency.py --cycles 1000 --mode both \
    --min-delay 0.05 --max-delay 0.8 --binary /path/to/pinned/samyama --json out.json
```

## A note on what counts as a row here

A test that asserts a call returned an error is not evidence of a failure mode.
It says something failed, not what the operator sees. Rows above name tests that
pin an error code, an HTTP status, a typed error variant, or a value that
survived — and where both memory and disk are checked, the row says so, because
that is the pair that matters after a crash.
