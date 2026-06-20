# Case Study — Definition of Done

A case study takes a real, public knowledge graph, imports a published `.sgsnap`
snapshot into Samyama, and tells that domain's story through a handful of Cypher
queries and a narrated GIF. The point is **independent verification**: anyone who
clones this repo can run a case study and watch Samyama answer questions a
subject-matter expert (SME) in that domain actually cares about — no trust
required, no hidden setup.

A case study is **Done** only when every box below is checked. Several are
machine-enforced by `_lib/run_case_study.sh` (the DoD gate); the rest are
reviewed by a human before the GIF is shared.

## 1. Reproducible from a clean clone
- [ ] `cd case_studies/<domain> && ./run.sh` works on a fresh clone with only:
      a release build (`cargo build --release`) and `pip install rich requests`
      (plus `asciinema` + `agg` if regenerating the GIF).
- [ ] No manual steps, no external services, no credentials. The snapshot is
      fetched from a **pinned, immutable URL** (a GitHub release asset or an S3
      object), never a branch or "latest".
- [ ] `case.env` pins `SNAPSHOT_URL` **and** `SNAPSHOT_SHA256`; `fetch_snapshot.sh`
      fails closed on a hash mismatch.

## 2. Real data, real answers (machine-gated)
- [ ] The snapshot imports cleanly (`/api/snapshot/import` returns `status: ok`
      with a non-zero `nodes_imported`).
- [ ] **Every** showcase query returns **≥ 1 row** — `validate_queries.py` exits 0.
      An empty table or a Cypher error fails the build. This is the gate that
      stops a misleading demo from ever being recorded.
- [ ] Queries project **properties** (`RETURN n.name, count(*)`), not bare nodes —
      post-import, properties live in the columnar store and are read through the
      query engine, so `RETURN n` alone can render blank. (See
      `docs/ADR/ADR-022-snapshot-format.md`.)
- [ ] Cypher hygiene: string literals are **double-quoted**; numeric `WHERE`
      bounds match the property's type (`< 5.0` for floats) — a float/int
      mismatch silently returns no rows and would trip the gate.

## 3. Tells a domain story (SME review)
- [ ] 4–8 showcase queries, ordered as a narrative: orient → a sharp insight →
      a multi-hop / cross-entity question that relational SQL can't express
      cleanly → a closing "so what".
- [ ] Each query carries a one-line **insight** (the `// @query Title | insight`
      marker) stating what an SME learns from the answer — not what the query does.
- [ ] Scale (nodes/edges) and per-query latency are shown, so the reader sees both
      the size of the graph and that answers are interactive.
- [ ] At least one query exercises a graph-native strength: variable-length
      traversal, a graph algorithm (`CALL algo.*`), cross-KG federation, or vector
      search.

## 4. The GIF is shareable
- [ ] `demo.gif` regenerates via `RECORD=1 ./run.sh` and is committed.
- [ ] Legible at README width (~100 cols), ≤ ~2 MB, paced for reading
      (`agg --speed 1.3 --idle-time-limit 1.5`).
- [ ] Narration arc: title card (domain + data source + license) → import scale →
      each query with its result → takeaway panel.
- [ ] A domain SME (or a stand-in reviewer) confirms the questions are ones they'd
      actually ask and the answers read correctly.

## 5. Documented and attributed
- [ ] `README.md` per domain: one-paragraph narrative, schema (node labels +
      relationship types), scale, the showcase queries, the embedded GIF, and the
      **source-data license / attribution**.
- [ ] The snapshot's provenance (which release tag / S3 path, which loader built
      it) is linked, so the data lineage is auditable.
- [ ] The domain is listed in `case_studies/README.md`'s index table.

## Files every case study ships
```
case_studies/<domain>/
  case.env        # DOMAIN, SNAPSHOT_URL, SNAPSHOT_SHA256, GRAPH, [DEDUP_KEY]
  queries.cypher  # 4–8 showcase queries with // @query Title | insight markers
  demo.py         # ~10 lines: calls demo_lib.run_demo(...) with title + takeaway
  run.sh          # one line: exec ../_lib/run_case_study.sh "$@"
  README.md       # narrative, schema, scale, queries, GIF, license
  demo.gif        # committed; demo.cast is regenerable and git-ignored
```
Snapshots themselves are **not** committed — they live as release/S3 assets and
are cached under `case_studies/.snapshots/` (git-ignored).
