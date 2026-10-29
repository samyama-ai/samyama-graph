# GQL (ISO/IEC 39075) gap analysis

**Publication status — 29 October 2026.** This is a published historical
working note, first written on 21 August 2026. It is intentionally public so
readers can inspect the uncertainty and the work still required. It is **not**
a GQL conformance claim, a substitute for ISO/IEC 39075:2024, or a statement
of the engine's current capabilities.

**LANG-08's H1 deliverable is this document.**

## 0. What this document is, and is not

This is a **planning document written from working knowledge of GQL, not from
the standard text.** ISO/IEC 39075:2024 is a paid ISO deliverable and was not
consulted while writing it. Every claim below is therefore marked with a
confidence level, and §6 lists what must be checked against the actual text
before any of it is quoted outside the company.

That framing is the point. A gap analysis whose individual claims are
confidently wrong is worse than no gap analysis: it produces a roadmap of work
items that may not correspond to the standard. The purpose here is to identify
*where to look*, not to settle what the standard says.

**No conformance claim of any kind should be made on the strength of this
document.**

## 1. The headline: TCK conformance is not GQL conformance

CH-TCK measures the **openCypher** TCK. GQL is a different language. Passing
CH-TCK at 100% would leave LANG-08 at zero.

They are closely related — GQL's pattern matching descends from Cypher, and
Neo4j's own Cypher is converging on GQL — but they are not the same language
and there is no automatic transfer.

| | openCypher | GQL |
|---|---|---|
| Status | de-facto, community | ISO/IEC 39075:2024 |
| Conformance suite | public TCK, 1,615 scenarios | **none public** |
| Our measurement | 86.7% of 1,244 evaluated | unmeasured, no suite to measure with |

## 2. Where our current work may be *wrong* for GQL

This is the section worth acting on, because it affects decisions already made.

### 2.1 Path match modes — relationship isomorphism (confidence: medium)

We shipped relationship isomorphism on 2026-08-21 ([#684](https://github.com/samyama-ai/samyama-graph/pull/684)/[#685](https://github.com/samyama-ai/samyama-graph/pull/685)):
one relationship may not be traversed twice in a single MATCH. That is
openCypher's rule and is confirmed against Neo4j.

GQL is understood to make the match mode **explicit** — `WALK`, `TRAIL`,
`SIMPLE`, `ACYCLIC` — where `TRAIL` is the no-repeated-edge mode and `WALK`
permits repetition. If GQL's default is `WALK`, then our new default is
openCypher-correct and **GQL-incorrect**, and the fix is to make the mode
selectable rather than to change the default.

*Must be verified before acting.* Listed first because it is a live decision,
not a future one.

### 2.2 `CREATE` vs `INSERT` (confidence: medium-high)

GQL uses `INSERT` for creating nodes and edges. Our parser has `CREATE` only.
Likely additive — accept both — rather than a breaking change.

### 2.3 Linear composition (confidence: medium)

GQL composes statements linearly, with `NEXT` between them, and has
statement-level `FILTER` and `LET`. Our pipeline is Cypher's `WITH` chain.
These are plausibly inter-translatable, but the surface differs.

### 2.4 Graph types, catalog, sessions (confidence: medium-high)

GQL has a schema concept (graph types), a catalog, and sessions/transactions
*in the language*. We are schema-optional with no catalog and no in-language
session model. This is the largest structural gap and the one that cannot be
retrofitted cheaply — which is exactly what spec 02 predicted when it said
being early is cheap and retrofitting is expensive.

### 2.5 Quantified path patterns (confidence: low-medium)

GQL specifies quantified path patterns with `{m,n}` quantifiers. We have
Cypher's `*m..n`. Whether the semantics match beyond the syntax is unknown.

## 3. What is plausibly already close

Ordinary node and relationship pattern matching, projection and ordering,
aggregation, and the three-valued logic for comparisons — this last one
recently corrected ([#607](https://github.com/samyama-ai/samyama-graph/pull/607)). These are the parts GQL inherited most
directly from Cypher. **Plausibly** is doing real work in that sentence.

## 4. The conformance-suite problem

Spec 02's open question #2 asks whether to build and publish a GQL test suite.
This analysis does not settle it, but sharpens it:

* there is no public suite, so "GQL conformance: X%" is currently unmeasurable
  by anyone, including competitors making the claim;
* a published suite would be genuine differentiation, and expensive;
* the cheaper intermediate step is to state a **supported subset** with
  evidence per feature, which is what LANG-08's H2 column already asks for.

Building a suite is not recommended for H1. Deciding the subset is.

## 5. Recommendation

1. **Buy the standard.** Everything here is blocked on it, and the cost is
   trivial against the engineering it would misdirect.
2. **Settle §2.1 first** — it affects a default we have already shipped.
3. **Do not claim GQL conformance, in any form, in any material**, until §6 is
   closed. This document does not support such a claim.
4. Treat graph types / catalog / sessions (§2.4) as the item to design early
   even if built late.

## 6. To verify against the standard text

| # | Question | Affects |
|---|---|---|
| 1 | Default path match mode — is it `WALK`? | our shipped isomorphism default ([#684](https://github.com/samyama-ai/samyama-graph/pull/684)) |
| 2 | Is `INSERT` the only creation keyword, or is `CREATE` also valid? | parser |
| 3 | Exact `NEXT` composition semantics vs `WITH` | planner |
| 4 | Whether graph types are mandatory or optional | storage, catalog |
| 5 | Quantified path pattern semantics vs `*m..n` | var-length expansion |
| 6 | Null and three-valued-logic rules vs openCypher | comparison lattice |
| 7 | Ordering rules across types vs openCypher's orderability | `cypher_order` ([#688](https://github.com/samyama-ai/samyama-graph/pull/688)) |

Items 1 and 7 both concern behaviour changed shortly before the original
analysis was written, which is the argument for doing this analysis before
more language surface is built.
