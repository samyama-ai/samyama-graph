# ADR-037: Bolt Protocol — Feasibility and Decision

## Status
**Accepted** — Bolt is approved in principle; implementation is gated (see *Decision*).

## Date
2026-08-30

## Objective

Discharge the H1 spike required by **API-08** (12-clients-apis-sdks-crates.md) and
**INT-03** (14-interop-migration.md): determine whether Samyama should speak Neo4j's
Bolt protocol so existing Neo4j drivers connect unmodified, and record the decision.

The H1 gate item is *"Bolt feasibility spike → written decision"*. This document is
that decision. It does not implement Bolt.

## Context

Spec 14 §5 calls Bolt *"the largest single lever in the entire spec for reducing
switching cost"* and warns *"do not let it drift past H1."* The lever is real: the
five official Neo4j drivers (Java, Python, JavaScript, Go, .NET) plus the Cypher
shell, Neo4j Browser, and a long tail of BI and ETL connectors all speak Bolt and
nothing else. Anything that speaks Bolt inherits that ecosystem on day one.

The question is not whether that is valuable. It is whether Samyama can speak Bolt
*without lying*, and what it costs.

### What already exists

Samyama is not starting from a blank socket. `src/protocol/` is a complete
non-HTTP wire protocol — a RESP server (`server.rs`, 618 lines) with a tokio accept
loop, a per-connection framed codec (`resp.rs`, 1,184 lines), and a command
dispatcher that executes Cypher and serialises results (`command.rs`, 1,129 lines).
It carries persistence, the sharding router, the proxy and the cluster manager
through to each connection.

A Bolt server reuses all of that plumbing unchanged. What is genuinely new is the
handshake, the codec, the message state machine, and — the part that matters — the
type mapping.

### Cost of the new parts

| Part | Assessment |
|---|---|
| Handshake (`0x6060B017` + four 4-byte version proposals) | Trivial. Tens of lines. |
| Chunked framing (2-byte length chunks, `0x0000` terminator) | Trivial, and structurally the same problem `resp.rs` already solves. |
| PackStream v2 codec | ~600 lines. Fully specified, mechanical, symmetric with work already done for RESP. |
| Message state machine | The real engineering. `HELLO`/`LOGON`/`RUN`/`PULL`/`DISCARD`/`BEGIN`/`COMMIT`/`ROLLBACK`/`RESET`/`GOODBYE` over seven server states. The subtlety is `FAILED`: after a failure every message except `RESET` must be **ignored**, not answered. Implementations that answer them instead deadlock official drivers in ways that do not reproduce under hand-written test clients. |
| Driver conformance | The long tail. Five official drivers, each with its own feature negotiation. Not estimable from the spec; only from running them. |

None of that is prohibitive. A read path is a matter of weeks, not quarters.

### The actual blocker: the type systems do not line up

Bolt is not a transport, it is a *typed* protocol. Every value crossing it must be
one of PackStream's types or one of Bolt's structure types. Three gaps matter, and
all three run in the direction that hurts.

**1. Vectors have no Bolt representation.** `PropertyValue::Vector(Vec<f32>)` is a
first-class Samyama type and a product differentiator. Bolt has no vector. The only
available encoding is a list of floats, which means a vector written through a Bolt
driver and read back is no longer a vector — it is a list that happens to contain
numbers. The round-trip loses the type, silently, and the client cannot tell.

**2. Temporals are lossy in the direction clients care about.** Samyama has
`DateTime(i64)`, `Date(i32)`, `LocalTime(i64)`. Bolt v5 distinguishes seven temporal
structures, including offset-aware `DateTime`, zone-id-aware `DateTimeZoneId`, and
`Duration`. Samyama has **no `Duration` type and no zoned or offset-aware time**.
A driver sending a zoned datetime — the ordinary case in every JVM application —
has nowhere to land it without dropping the zone.

**3. Points do not exist on either side yet.** Bolt requires `Point2D`/`Point3D`.
NDS-04 (geospatial types and index) is 🔴 unbuilt. There is no gap *today* because
neither side has points, but it means a Bolt path cannot be called complete before
NDS-04 lands, and any client sending a point must be refused rather than coerced.

Spec 14 §4 already anticipated exactly this: CH-INTEROP's mandatory hostile cases
name *"vectors, and temporals with timezones"*. The spike's finding is that those
two cases are not hostile edge cases for Bolt — they are the two places Bolt
structurally cannot represent what Samyama holds.

### The identity problem

Official Neo4j drivers gate feature negotiation on the server agent string returned
in the `HELLO` response. A server that returns something other than `Neo4j/5.x`
must be tested against every driver to learn what it degrades or refuses; a server
that returns `Neo4j/5.x` is asserting it is Neo4j, which is both false and a
trademark exposure. There is no third option that is simultaneously honest and
guaranteed-compatible. This is a decision, not a bug, and it has to be made by a
person rather than discovered at integration time.

## Decision

**Bolt is approved.** The switching-cost argument in spec 14 §5 holds and nothing in
this spike undermines it.

**Implementation is deferred past H1 and gated on three preconditions:**

1. **NDS-04 (geospatial types)** ships, or the Bolt read path explicitly refuses
   point-valued messages with a typed error rather than coercing them.
2. **A `Duration` type and zone-aware temporals** exist in `PropertyValue`, or the
   Bolt path refuses them with a typed error. Dropping a timezone silently is not
   acceptable.
3. **The server agent string is decided by a person**, with the trademark question
   answered, before any driver-facing code is written.

**Vectors are declared out of scope for Bolt permanently.** A Bolt client cannot
carry a Samyama vector without losing its type, so the Bolt path must reject
vector-valued results with a typed error naming the HTTP or SDK route instead of
degrading them to lists. A protocol that quietly returns a different type than the
one stored is the exact failure class the H1 correctness work exists to eliminate;
adding a new instance of it in order to gain driver compatibility would be a bad
trade at any price.

**The H1 deliverable is this document.** No Bolt code ships in H1.

## Consequences

* **API-08 and INT-03 are satisfied for H1** — the requirement is a spike and a
  recorded decision, and both are now recorded. Their H2 column ("read path if
  approved") is live, with the three gates above attached.
* **The type-system gaps become tracked work in their own right.** `Duration` and
  zone-aware temporals were previously invisible; the spike surfaced them as
  blockers for a named H2 deliverable, which is a stronger reason to build them
  than "Cypher has them".
* **NDS-04 gains a second consumer.** Geospatial was justified on its own; it is now
  also on the Bolt critical path.
* **Switching cost stays high through H1.** Prospects on Neo4j drivers cannot connect
  to Samyama unmodified this cycle. INT-02 (Neo4j importer) and INT-11 (compatibility
  report) remain the H1 answer to migration, and they address moving *data* rather
  than moving *clients*. That is a real and acknowledged gap, not a solved problem.
* **The estimate is now evidence-based.** Should the decision be revisited, the cost
  breakdown above is grounded in the existing `src/protocol/` implementation rather
  than in a guess.

## Alternatives considered

**Ship a Bolt read path in H1 with lossy type coercion.** Rejected. It would return
a list where a vector was stored and a floating datetime where a zoned one was sent,
in both cases without the client being able to detect it. This is the same defect
class as the wrong-answer bugs condition 5 of the H1 gate exists to drive to zero,
and shipping it deliberately while fixing it accidentally elsewhere is incoherent.

**Ship Bolt behind an explicit "compatibility mode" flag that documents the losses.**
Rejected for H1, worth revisiting for H2. A documented loss is much better than a
silent one, but it still needs the agent-string decision, and the flag does not
reduce the implementation cost — it only makes the result honest. If the H2 gates
prove slow, this is the fallback to reconsider first.

**Never do Bolt; invest in the migration tooling instead.** Rejected. It concedes
the ecosystem permanently, and spec 14 §5's assessment of Bolt as the largest single
switching-cost lever is correct. The gates defer it; they do not cancel it.
