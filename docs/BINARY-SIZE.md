# Binary size

How big the server binary is for each build profile, what the Edge build leaves
out, and how far each is from HW-05's budgets: server ≤50 MB, Edge
feature-minimal build ≤15 MB, Micro ≤8 MB.

**The Edge build misses its budget, by more than half.** It exists, it
genuinely removes code, and it is measured below; it is not 15 MB, and the last
section says what getting there would take.

## The profiles

| Profile | Build | Leaves out |
|---|---|---|
| server | `cargo build --release --bin samyama` | nothing; default features are `mimalloc`, `otel`, `arrow` |
| edge | `cargo build --release --bin samyama --no-default-features --features edge` | `arrow` (Arrow IPC and Parquet export, `POST /api/import/parquet`, `LOAD PARQUET`) and `otel` (OpenTelemetry trace export) |

`edge` is `["mimalloc"]`: the server's allocator, and nothing else beyond the
always-on engine. What the Edge build leaves out is refused by name, never
quietly missing:

| In the Edge build | Answer |
|---|---|
| `POST /api/query/export` with `format: parquet` or `arrow` | 501, naming the `arrow` feature. CSV still exports |
| `POST /api/import/parquet` | 501, before the store is touched; nothing is created |
| `LOAD PARQUET ...` | parses and plans; running it is refused, naming the `arrow` feature |

The Rust SDK (`samyama-sdk`) asks for `arrow` itself, because
`EmbeddedClient::export` uses the engine's Arrow and Parquet encoders, so an
embedded user keeps them whatever the server is built with.

## Measured

Both with the repository's own release profile (`lto = true`,
`codegen-units = 1`), from commit `cdc84d9e` plus this change. MiB is
1024 × 1024 bytes, as the harness reports it.

| Platform | server | edge | edge saves | server budget (50) | edge budget (15) |
|---|---|---|---|---|---|
| x86_64-apple-darwin | 42.90 MiB | 34.64 MiB | 8.26 MiB (19%) | within | **over, by 19.6 MiB** |
| x86_64-apple-darwin, `strip`ped | 36.30 MiB | 29.21 MiB | 7.09 MiB | within | **over, by 14.2 MiB** |
| x86_64-unknown-linux-gnu | 37 MB, harness run | not measured | — | within | — |

The Linux server figure is CH-HW-SIZE's own measurement. No Linux edge build has
been weighed yet; `python3 scripts/binary_size.py --build` on a Linux host
records one. A macOS binary is larger than a Linux one built from the same
source, so the platforms are never compared with each other.

## Checking it

```bash
python3 scripts/binary_size.py --build            # build both profiles, weigh them against the budgets
python3 scripts/binary_size.py --build --smoke    # and start each one: the edge build must refuse what it lacks
python3 scripts/binary_size.py --build --record   # write scripts/binary_size_baseline.json for this platform
```

Two checks, kept apart on purpose:

- **Budgets** are reported for every profile. A miss fails the run only with
  `--enforce-budgets`: the edge build misses today, and a gate that is red from
  its first day gets switched off.
- **Growth** is the gate spec 05 §5 describes: a profile more than 10% larger
  than its recorded baseline fails, unless the baseline is re-recorded with
  `--reason "why"`, which is kept beside the number. Baselines are per platform.

**No CI job runs this yet.** Wiring it into a workflow, and recording the Linux
baseline the gate would compare against, is a workflow change for the DevOps
team.

## What is left in the Edge build

Code by crate in the 34.64 MiB Edge binary, from its symbol table. Approximate:
LTO inlines across crates, and generic code is counted where it was
instantiated.

| | ≈ MiB | Needed on the Edge tier? |
|---|---|---|
| symbol tables (`strip` removes them) | 5.4 | no, but backtraces lose their names |
| the engine (`samyama`) | 6.0 | yes |
| RocksDB | 3.3 | yes: durable storage is in the Edge tier's feature set |
| TLS (`aws-lc`, `rustls`) | 2.3 | for HTTPS; not if TLS ends at a proxy in front of it |
| the HTTP stack (`axum`, `h2`, `hyper`) | ~1.5 | yes: the Edge tier serves HTTP |
| optimisation solvers (`samyama_optimization`) | 0.5 | no |
| SPARQL (`spargebra` and its parser) | ~0.5 | no |

## What 15 MB would take

Removing every remaining piece the Edge tier does not need, and stripping,
comes to about 27 MiB, not 15. The rest is storage, networking and the engine
itself, which the Edge tier's own feature set requires. 15 MB needs a
different cut, and that is a design decision rather than another feature flag:

- **A RESP-only Edge build**, without the HTTP server and TLS (about 4 MiB),
  for devices that are only ever reached over RESP, as
  [CONSTRAINED-CLIENTS.md](./CONSTRAINED-CLIENTS.md) recommends.
- **Snapshot-backed storage instead of RocksDB** (about 3 MiB plus its
  compression libraries), which fits the Micro tier's read-only serving but
  drops durable writes from the Edge tier.
- **`strip` and `opt-level = "z"`** in an edge-only Cargo profile, so the server
  keeps its symbols and its speed.

Making `optimization` and `rdf`/`sparql` optional features is the next
mechanical step, about 1 MiB; it does not change the conclusion.
