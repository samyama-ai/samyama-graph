# SOC 2 control set: gap analysis

**Documented, not certified.** No SOC 2 audit has been performed on Samyama, and
nothing on this page is an auditor's opinion. It is our own reading of which
controls exist, with the evidence for each, and which do not.

This page exists so that a customer's security reviewer can proceed without a
certificate: every control below names the file, test or setting that carries
it, so it can be checked rather than believed. The criteria references (CC6.1,
A1.2, ...) are to the AICPA Trust Services Criteria; the mapping is ours.

**Verified at** `dev` `397a2ce`, 2026-10-20. The repository settings and pull
request figures were read from GitHub the same day.

## Scope

SOC 2 reports on an organisation's system. Samyama is a database you run, so it
appears in a SOC 2 audit in two ways, and this page covers both:

1. **As a component of *your* system.** What the engine gives you to build your
   own controls on: authentication, roles, TLS, audit records, backups. Sections
   1, 2 and 4.
2. **As software from a vendor.** How we change, release and fix it. Sections 3
   and 5.

Out of scope: Samyama's company-level controls (personnel, laptops, vendor
management, physical security), the hosted sandbox at `graph.samyama.cloud`, and
the Enterprise edition, which is built from a separate repository.

## Summary

| Area | What exists | Largest gap |
|---|---|---|
| [Access control](#1-access-control) | Credentials, three roles, tenant binding, on HTTP and RESP; TLS on HTTP | All of it is **off by default**, and the Docker image listens on `0.0.0.0` without it |
| [Audit](#2-audit) | Append-only JSON log of every state-changing HTTP request | No audit of RESP at all; off by default; not tamper-evident |
| [Change management](#3-change-management) | Required CI gate on `main` and `dev`; signed, attested Docker images | Reviews are not enforced: 17 of the last 100 merged PRs were approved by someone other than their author |
| [Backup and recovery](#4-backup-and-recovery) | Full snapshots, encryptable, restore tested in CI across architectures | No incremental backup or point-in-time recovery; writes are not synced to disk by default |
| [Incident response](#5-incident-response) | `SECURITY.md` with response times; a failure-mode table | The channel `SECURITY.md` calls *preferred* is switched off; no runbook; dependency alerts are not triaged |

## 1. Access control

*CC6.1, CC6.2, CC6.3, CC6.6, CC6.7*

| Control | Evidence | Default | Gap |
|---|---|---|---|
| **Authentication** on every HTTP route and every RESP connection | `--auth-file` (`src/main.rs`), `src/auth.rs`; HTTP: `tests/http_requires_a_credential.rs` (`every_route_is_covered_and_not_just_the_one_this_test_picked`); RESP: `an_unauthenticated_connection_gets_only_auth_and_ping` (`src/protocol/server.rs`) | **Off.** With no `--auth-file`, anyone who can reach a port can read and write the graph | Off by default |
| **Machine tokens and passwords** stored as hashes, not plaintext | `src/auth.rs`: tokens as SHA-256, compared in constant time (`digests_match`); passwords as argon2 PHC strings; `a_password_is_never_checked_with_the_fast_hash` | n/a | No lockout or rate limit on failed attempts |
| **Roles**: `read`, `write`, `admin`, each including the one below | `src/auth.rs` (`Role`), `required_role` (`src/http/server.rs`); `a_read_credential_reads_and_is_refused_a_write`, `every_mutating_route_needs_more_than_read` | A credential line with no `roles=` field is **admin**, for compatibility with files written before roles existed | Least privilege must be asked for |
| **Tenant isolation**: a credential bound to one graph cannot address another | `tenant=` field (`src/auth.rs`); `a_tenant_bound_credential_cannot_address_another_graph` (`tests/http_requires_a_credential.rs`) | Unbound: a credential reaches every graph | — |
| **GraphRAG answer-time policy**: what a principal's retrieved context (`ai.rag.retrieve`) may hold | `rag_hide=` field (`label/L`, `edge/T`, `prop/k`, `prop/L.k`; `src/auth.rs`, `src/query/executor/rag_policy.rs`); `tests/rag_policy.rs` (`nothing_withheld_appears_anywhere_in_the_rows`, `once_rules_are_configured_an_unscoped_retrieval_is_refused`). Withheld nodes and edges are dropped and not walked through; withheld properties are removed; `withheld` / `ai.rag.withheld` report counts and rules, not content | No rules: the context holds whatever retrieval reaches | **Retrieval only, not row-level security**: the same credential can still `MATCH` a withheld node directly. Rules are per credential; there are no per-role or per-tenant rule sets |
| **Network exposure**: loopback unless asked otherwise | `--host` defaults to `127.0.0.1` for both listeners (`src/main.rs`, `HttpServer::new` in `src/http/server.rs`) | Loopback | A routable bind without credentials only **warns**; it is not refused. The Docker image runs `--host 0.0.0.0` (`Dockerfile`) and so starts in exactly that state |
| **Encryption in transit** | `--tls-cert`/`--tls-key` (`src/main.rs`); `tests/http_serves_tls.rs`. Half a TLS configuration stops the server rather than serving plaintext | **Off**: plain HTTP | **HTTP only. RESP has no TLS**: tokens sent with `AUTH` cross the network in cleartext unless the operator adds a tunnel |
| **Credential lifecycle** | One line per credential; revoke by deleting the line | n/a | The file is read once at start-up, so rotating or revoking a credential needs a restart. No expiry |
| **Process privilege** | — | The Docker image has no `USER` line and runs as **root** | Gap |

How to turn these on is in [DATA-HANDLING.md](./DATA-HANDLING.md).

## 2. Audit

*CC7.2, CC4.1*

| Control | Evidence | Default | Gap |
|---|---|---|---|
| **Record of every state-changing request**, with who made it and the outcome | `--audit-log` (`src/main.rs`), `AuditLog` and `audit` (`src/http/server.rs`); `tests/http_audits_writes.rs` | **Off** | Off by default |
| Selected **by HTTP method**, so a new endpoint is audited the day it is added | `POST`, `PUT`, `PATCH`, `DELETE` are recorded; `a_read_submitted_as_a_post_is_recorded_too` | n/a | Reads over `GET` are not recorded, so it is not an access log |
| **Refusals are recorded**, as `unauthenticated` | `a_refused_request_is_recorded` | n/a | A refused `GET` is not |
| **Secrets and data stay out** of the log | The request body is never written; `the_token_never_appears_in_the_log` | n/a | — |
| **Durable**: flushed on every write | `AuditLog::record` | n/a | A failed write is a warning and the request still succeeds, so the log can have a hole that only the server's own log reports |
| A log that cannot be opened **stops the server** | `an_unopenable_log_is_an_error` | n/a | — |
| **RESP** writes | none | — | **Not audited.** A `GRAPH.QUERY ... DELETE` over RESP leaves no record |
| **Tamper evidence** | — | — | Append-only by open mode only; entries are not chained or signed, and retention is left to the operator |
| **Monitoring** | `/metrics` (Prometheus), behind the same credential as every other route | On | No alerting is shipped; what to alert on is the operator's to define |

## 3. Change management

*CC8.1, CC7.1*

| Control | Evidence | Gap |
|---|---|---|
| **Every change arrives as a pull request with CI** | `main` and `dev` are protected and require the `ci-ok` check, which fails if any CI job it depends on failed (`.github/workflows/ci.yml`) | Enforcement is `non_admins`: an administrator can merge past a red CI |
| **Review by a second person** | `CODEOWNERS` names one owner for every path | **Not enforced, and not the practice.** Of the last 100 merged pull requests (#1597 to #1928), 17 had an approving review from someone other than the author, 19 had any review at all, and 80 were merged by their own author. Whether the branch rule requires an approval could not be read without administrator access; the merges show it does not in effect |
| **Dependency vetting** | `cargo-deny` gates licences, sources and RustSec advisories on every PR (`deny.toml`); each ignored advisory carries its reason | Advisories outside the RustSec database are invisible to it. GitHub's dependency alerts see them, but nothing triages those: one has been open since 2026-09-04. No automated dependency updates are configured |
| **Release gate** | A release tag is refused when its regression verdict is missing, stale, red or measured on another commit (`scripts/check_release_gate.py`, `.github/workflows/tag-release.yml`) | — |
| **Build provenance** | Docker images are signed with Sigstore and carry a build-provenance attestation and an SBOM (`.github/workflows/publish-docker.yml`). npm and PyPI use OIDC trusted publishing, so no long-lived registry token exists, and both carry an SLSA provenance attestation (checked on `samyama-sdk@1.12.0` and the `samyama` 1.12.0 wheels) | The crate has no attestation. No publishing environment requires an approver; `ecr-public` alone has a branch policy |
| **Contract stability** | Recorded HTTP request/response corpus replayed on every PR (`scripts/api_stability.py`) | — |

## 4. Backup and recovery

*A1.2, A1.3, CC7.5*

| Control | Evidence | Default | Gap |
|---|---|---|---|
| **Full backup**: a snapshot of a graph as one `.sgsnap` file | `POST /api/snapshot/export`; restore with `POST /api/snapshot/import`, which needs the `admin` role (`required_role`, `src/http/server.rs`) | On request | Nothing schedules it; that is the operator's job |
| **Backups encrypted at rest** | `--snapshot-key`: ChaCha20-Poly1305, authenticated, truncation-evident; `tests/snapshot_encryption_round_trip.rs` | **Off** | Changing the key means re-exporting. The live data directory (RocksDB) is **not** encrypted; that is left to the disk |
| **Restore is tested** | A snapshot written on x86-64 is read on ARM64 and the other way round, comparing contents, on every PR that changes code (`snapshot written on` / `read on` jobs, `.github/workflows/ci.yml`) | n/a | No CI job restores a snapshot written by an **earlier release** (#1794) |
| **Crash recovery** | Acknowledged writes survive `SIGKILL` mid-write; recovery reads RocksDB alone ([FAILURE-MODES.md](./FAILURE-MODES.md), rows 34 and 43) | n/a | — |
| **Durability on power loss** | `SAMYAMA_FSYNC=1` syncs the WAL and RocksDB before replying ([ACID-GUARANTEES.md](./ACID-GUARANTEES.md) §4) | **Off**: a committed write may be lost on power loss or host reset | Nothing has cut power to confirm the synced case ([FAILURE-MODES.md](./FAILURE-MODES.md), row 6) |
| **Incremental backup, point-in-time recovery** | — | — | **Neither exists** (#1653). The recovery point is the last full snapshot |
| **Replication** | — | — | No replica to fail over to; ACID-GUARANTEES.md explains why Raft is not relied on |

## 5. Incident response

*CC7.3, CC7.4, CC7.5, CC2.3*

| Control | Evidence | Gap |
|---|---|---|
| **A way to report a vulnerability privately** | [SECURITY.md](../SECURITY.md): email, or GitHub private vulnerability reporting | **GitHub private vulnerability reporting is switched off on this repository**, though SECURITY.md calls it the preferred channel. Until a maintainer enables it, email is the only channel that works |
| **Committed response times** | SECURITY.md: acknowledgement within 3 business days, assessment within 7 | Not measured; no report has a public record to measure against |
| **Disclosure** | SECURITY.md: coordinated disclosure, credit in release notes or an advisory | No security advisory has been published yet |
| **Known failure behaviour, with operator actions** | [FAILURE-MODES.md](./FAILURE-MODES.md): each failure, what the engine does, the data guarantee, and what the operator does, each row tied to a test | Covers engine failures, not security incidents |
| **Incident runbook and post-incident review** | — | **None.** No documented roles, severity levels, escalation path or post-mortem practice |
| **Supported versions** | SECURITY.md's table | It still lists `1.1.x` as the supported line; the current release is 1.12.0 |

## What closing these gaps would take

In rough order of how much a reviewer will weigh them. None has been started by
this page.

1. **Require an approving review** on `main` and `dev`, and stop administrators
   bypassing the required check. A repository setting, for an administrator.
2. **Enable GitHub private vulnerability reporting**, so SECURITY.md's preferred
   channel exists. A repository setting, for an administrator.
3. **Make the Docker image safe by default**: run as a non-root user, and refuse
   (rather than warn about) a routable bind with no credential, with an explicit
   opt-out for a trusted network.
4. **Audit RESP** writes the way HTTP writes are audited.
5. **Triage dependency alerts** on a schedule, and turn on automated dependency
   updates.
6. **TLS for RESP.**
7. **An incident runbook**: roles, severity, escalation, and a post-mortem
   template.
8. **Point-in-time recovery** (#1653).

## Keeping this page true

Every row names its evidence so that it can be re-checked. When a row's
evidence changes, the row changes with it. The H2 target for this requirement
(REL-16) is an *evidence pipeline*: a check that collects these facts
automatically instead of a person reading them, which this page does not yet
have.

## Related

- [DATA-HANDLING.md](./DATA-HANDLING.md): what leaves the machine, and how to
  turn on authentication, TLS, auditing and snapshot encryption.
- [ACID-GUARANTEES.md](./ACID-GUARANTEES.md): the transaction and durability model.
- [FAILURE-MODES.md](./FAILURE-MODES.md): failures, guarantees and operator actions.
- [SECURITY.md](../SECURITY.md): reporting a vulnerability.
