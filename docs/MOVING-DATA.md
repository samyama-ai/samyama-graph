# Moving data in and out

Current converter support on this branch · `samyama convert` (#1905)

Every route in and out of Samyama goes through one file format, `.sgsnap`, in
two steps:

1. **File ↔ `.sgsnap`**, offline, with `samyama convert`. No server, no
   network, one command per file.
2. **`.sgsnap` ↔ server**, with the snapshot endpoints.

```bash
# In: someone else's format -> a snapshot -> a running server
samyama convert graph.json graph.sgsnap --report report.json
curl -X POST http://localhost:8080/api/snapshot/import -F file=@graph.sgsnap

# Out: a running server -> a snapshot -> someone else's format
curl -X POST http://localhost:8080/api/snapshot/export -o graph.sgsnap
samyama convert graph.sgsnap copy.sgsnap
```

Keeping the two apart is deliberate. A conversion can be run, checked and
re-run without touching a database, and the snapshot in the middle is a file
you can diff, checksum, keep and import into more than one server.

`/api/snapshot/import` needs the `admin` role. See
[LEAVING-SAMYAMA.md](./LEAVING-SAMYAMA.md) for what a snapshot carries and
[DATA-HANDLING.md](./DATA-HANDLING.md) for encrypted snapshots.

---

## What converts to what

`samyama convert --formats` prints this table from the converter itself. It
is the authority, and `crates/samyama-convert/tests/readme.rs` holds the crate
README's copy of it to the code.

| Format | Extensions | Read | Write |
|---|---|---|---|
| `.sgsnap` | `.sgsnap` | yes | yes |
| Turtle | `.ttl` | no | no |
| N-Triples | `.nt` | no | no |
| RDF/XML | `.rdf`, `.owl`, `.xml` | no | no |
| JSON-LD | `.jsonld` | no | no |
| Neo4j `apoc.export.json` | `.json` | yes | no |
| GraphML | `.graphml` | no | no |
| CSV directory (`neo4j-admin import` layout) | a path with no extension | no | no |

Every conversion has `.sgsnap` at one end or the other. Convert A → B as
A → `.sgsnap` → B, so each step's losses are reported separately.

A "no" is refused by name, with exit 2, before the input is opened. It never
reads approximately. JSON-LD is not read because the engine's JSON-LD parser
does not exist yet. N-Quads and TriG are not offered: a Samyama graph is one
graph, and named graphs would be dropped.

---

## The command

```bash
samyama convert <input> <output> [--from FMT] [--to FMT]
                                 [--missing-endpoints fail|placeholder|drop]
                                 [--report report.json] [--require-lossless]
samyama convert --formats
```

- The format comes from each file's extension. `--from` and `--to` override
  it, for example for an output named `graph.bin`.
- `--missing-endpoints` decides what happens to a Neo4j relationship whose
  endpoint the file never defines. `fail` (the default) exits 65 and names the
  id. `drop` leaves the relationship out and reports it. `placeholder` keeps
  it against an `Unresolved` node holding the missing id as `neo4j_id`, which
  is also reported.
- `--report` writes what each step read, wrote and dropped as JSON. It is
  refused (exit 2) when it names the input or the output, under any spelling
  of the path.
- Outputs are written whole or not at all: to a new staging file beside the
  output, then renamed into place. An existing `<output>.tmp` is never
  touched.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Converted. |
| 1 | Converted, but `--require-lossless` was given and something was dropped. The output and the report are still written. |
| 2 | Usage: an unknown or unsupported format, a bad flag, a bad mapping file, a report path that is a graph path. Nothing was read. |
| 65 | The input could not be parsed as its format, or a Neo4j relationship's endpoint is missing under `--missing-endpoints fail`. |
| 66 | The input could not be opened. |
| 73 | The output could not be written. |

---

## Pending converter extensions

RDF mapping and presets (#1952), RDF input/output (#1954), and GraphML/CSV
output (#1942) are separate pull requests. Their commands and loss-report
fields are not available at this branch's head. Use `samyama convert --formats`
to inspect the capabilities of the binary you actually run; do not assume
that a v1.12.0 version string includes these pending extensions.

For the supported Neo4j route, review the JSON report for null properties,
unknown records, malformed lines, and missing endpoints before importing the
snapshot. `--require-lossless` exits 1 when a conversion reports a loss.

---

## Library use

The command is a thin layer over `samyama_convert` (and `samyama::convert`
inside the engine):

```rust,ignore
use samyama_convert::{convert, Options};

let report = convert("graph.json".as_ref(), "graph.sgsnap".as_ref(), &Options::default())?;
for loss in report.dropped() {
    eprintln!("{} {}: {}", loss.count, loss.what, loss.detail);
}
```

`read` and `write` work on a `GraphStore` and any reader or writer, for a
conversion that is part of a larger program. See the crate's
[README](../crates/samyama-convert/README.md).

---

## Where to go next

- [MIGRATING-FROM-NEO4J.md](./MIGRATING-FROM-NEO4J.md): bringing a Neo4j
  graph across, and the queries with it.
- [LEAVING-SAMYAMA.md](./LEAVING-SAMYAMA.md): every way out, and what each
  costs.
