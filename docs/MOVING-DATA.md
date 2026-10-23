# Moving data in and out

Verified against v1.12.0 · `samyama convert` (#1905)

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
samyama convert graph.sgsnap graph.ttl
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
| Turtle | `.ttl` | yes | yes |
| N-Triples | `.nt` | yes | yes |
| RDF/XML | `.rdf`, `.owl`, `.xml` | yes | yes |
| JSON-LD | `.jsonld` | no | yes |
| Neo4j `apoc.export.json` | `.json` | yes | no |
| GraphML | `.graphml` | no | yes |
| CSV directory (`neo4j-admin import` layout) | a path with no extension | no | yes |

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
                                 [--mapping mapping.yaml | --preset generic|skos|owl|samyama]
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
- `--mapping` and `--preset` say how RDF input becomes a property graph. See
  below.
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

## Loss reports

A conversion either carries something or says it did not. Each step's
report lists what it dropped. Each entry has a `what` you can branch on, a
`count`, and a sentence on what it means for the result:

```json
{"what": "attributes_widened_to_string", "count": 1,
 "detail": "GraphML declares one type per attribute; these held more than one and are written as text: node.flag."}
```

An empty list means that step lost nothing the format can hold. The entries
you will meet most:

| Step | `what` | Means |
|---|---|---|
| any → `.sgsnap` | `edge_creation_timestamps` | Relationships carry no creation time in a snapshot; nodes do. |
| Neo4j → | `dangling_relationships`, `unresolved_endpoints` | A relationship whose endpoint the export left out, dropped or kept against a placeholder. |
| Neo4j → | `null_properties`, `unknown_record_types`, `unparseable_lines` | A JSON null (Neo4j cannot store one), a record type not read, a line that is not JSON. |
| RDF → | `rdf_predicates_dropped` | Triples the mapping drops (`drop:`), or whose object it cannot use. The predicates are named. |
| RDF → | `rdf_languages_dropped`, `rdf_decimals_as_float`, `rdf_literals_as_string`, `rdf_datatypes_normalized` | Language, number and datatype detail that does not survive as typed graph values. |
| → GraphML | `attributes_widened_to_string`, `values_written_as_json`, `temporal_values_written_as_text`, `labels_containing_a_space` | GraphML declares one type per attribute and has no list, map or temporal type. |
| → CSV | `columns_widened_to_string`, `labels_containing_a_semicolon`, `properties_with_a_colon_omitted` | The same for `neo4j-admin`'s header, plus what its header syntax cannot name. |
| → RDF | `rdf_edge_properties_not_written`, `rdf_values_as_json` | Relationship properties have no RDF form under a mapping, and values with no XSD type are written as Samyama JSON literals. |

`--require-lossless` turns any entry into exit 1, the form to use in a
script. In a library call (`samyama_convert::Options::require_lossless`) a
lossy step is refused *before* its output is published. An existing file is
left as it was.

---

## RDF: the mapping file and its presets

RDF has no single correct property-graph form, so the choices are written
down in a YAML mapping file laid over a preset:

| Preset | For | What it does |
|---|---|---|
| `generic` (default) | any RDF | `rdf:type` → label by local name, literal → property, IRI → relationship type upper-cased (`SUB_CLASS_OF`), blank node → node; a local name two namespaces share falls back to `prefix_name` and is reported |
| `skos` | SKOS vocabularies | `Concept`, `prefLabel` / `altLabel` / `definition` / `notation`, `broader` → `BROADER`, `narrower` → `BROADER` the other way round, `related`, `exactMatch`, `closeMatch`; `inScheme` dropped and reported |
| `owl` | OWL ontologies | `Class`, `Property`, `SUBCLASS_OF`, `DOMAIN`, `RANGE`, `EQUIVALENT`; restrictions inline as maps |
| `samyama` | Samyama's own RDF export | exactly the engine's mapper, so Samyama → RDF → Samyama is an identity |

A file overrides any part of its preset:

```yaml
preset: skos
prefixes: { cso: "http://cso.kmi.open.ac.uk/schema/cso#" }
labels: { "cso:Topic": Concept }
edges:
  "cso:superTopicOf": { type: BROADER, reverse: true }
merge: { predicate: "cso:preferentialEquivalent", into: object, aliases_property: altLabel }
languages: { prefer: en, others: suffix }   # others: suffix | map | drop
multi_valued: list                          # list | first | error
blank_nodes: node                           # node | inline | drop
drop: [ "skos:inScheme" ]
```

`src/convert/presets/` holds the presets, and
`src/convert/presets/examples/cso.yaml` maps the Computer Science Ontology.

**The mapping travels with the graph.** Reading RDF records the source in the
snapshot header (`sources[]`): the file name, its SHA-256, the converter, and
the resolved mapping with the names it actually applied. Writing that
snapshot back to RDF uses them to undo the mapping:
- labels and keys go back to the classes and predicates they came from, and
  reversed edges are re-reversed;
- merged aliases come back as their own subjects;
- blank nodes come back as blank nodes.

Under `generic`, Turtle → `.sgsnap` → Turtle gives back the same graph, and
the tests compare the two blank-node-aware. A snapshot of a graph that never
was RDF is written in Samyama's own RDF form.

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
