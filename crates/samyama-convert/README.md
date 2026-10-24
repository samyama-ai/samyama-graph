# samyama-convert

Convert graph file formats to and from [Samyama](https://github.com/samyama-ai/samyama-graph)'s
`.sgsnap` snapshot, offline, with a report of anything the conversion could not carry.

`.sgsnap` is the hub. Each format needs one reader into a graph and one writer out of it,
not a converter to every other format:

```text
 .ttl .nt .rdf .owl .jsonld .json (Neo4j) .graphml .csv
                    │  samyama convert (offline, loss report)
                    ▼
                 .sgsnap
                    │  snapshot import / export
                    ▼
              Samyama Graph
```

## Formats

<!-- format-matrix:start -->
| Format | Extensions | Read | Write |
|---|---|---|---|
| .sgsnap | `.sgsnap` | yes | yes |
| Turtle | `.ttl` | not yet | not yet |
| N-Triples | `.nt` | not yet | not yet |
| RDF/XML | `.rdf`, `.owl`, `.xml` | not yet | not yet |
| JSON-LD | `.jsonld` | not yet | not yet |
| Neo4j apoc.export.json | `.json` | yes | not yet |
| GraphML | `.graphml` | not yet | not yet |
| CSV directory | (a directory) | not yet | not yet |
<!-- format-matrix:end -->

A format marked "not yet" is refused by name; it is never read or written approximately.
The table is generated from the code by `tests/readme.rs`, so it cannot fall out of date.

## Use

```rust,no_run
use samyama_convert::{convert, Options};

let report = convert("in.sgsnap".as_ref(), "out.sgsnap".as_ref(), &Options::default())?;
for d in report.dropped() {
    eprintln!("dropped {} {}: {}", d.count, d.what, d.detail);
}
# Ok::<(), samyama_convert::ConvertError>(())
```

- **Nothing is dropped silently.** Every read and write returns a report, and
  `Options::require_lossless` turns any drop into an error.
- **A failed conversion leaves no partial `.sgsnap`.** Files are written to a temporary
  path and renamed into place only once complete.
- **Edges to missing nodes** fail the conversion by default; `EndpointPolicy` can instead
  create an `Unresolved` placeholder node or drop and count the edge.

The `samyama convert` command (#1909) is a thin wrapper over this crate.

## License

Apache-2.0
