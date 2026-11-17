# Vector checkpoint generations (#1973)

PersistenceManager's explicit checkpoint_vectors/recover_vectors API now writes
immutable generations with a versioned SHA-256 manifest. Every dump and metadata
file is flushed and synced before an atomic CURRENT-file replacement publishes
that generation. Recovery verifies all referenced bytes and decodes into a
staging manager before replacing live indexes. Interrupted unpublished generations
are ignored; checksum failure is an error and leaves the current manager intact.
Old vector-list dumps without CURRENT remain readable.

Named index bindings, embedding model ids, HNSW parameters and quantization are
preserved. Prior manager dumps did not preserve names or fp16 precision on load.
The caller must hold its graph mutation barrier for a consistent multi-index
checkpoint. Old generations are retained; automatic garbage collection is not
implemented. SHA-256 detects accidental corruption, not malicious modification
of both a file and its manifest.

## HNSW graph in each generation

Each generation now also carries every non-empty index's HNSW graph, written
by `hnsw_rs` under the same index read lock as its vector list:
`<index>.hnsw.topology.hnsw.graph` (layers, neighbour lists with their stored
distances, entry point), `<index>.hnsw.topology.hnsw.data` (vectors at the
index's precision, f32 or fp16) and `<index>.hnsw.topology.ids` (the node behind
each graph data id, each node's current entry, and the format, platform word
size and byte order, dimension, metric, precision, `m` and `ef_construction`
the graph was built for). All three go into the manifest.

`recover_checkpoint` reloads an index from its graph when all three files pass
their checksums and the sidecar matches both the index metadata and the loaded
graph. Reloading links the neighbour lists from the stored distances: no
distance is computed and no vector is inserted. Otherwise the index is rebuilt
from its vector list as before. A graph file that fails its checksum costs a
rebuild of that index; the vector list and metadata stay mandatory, and a
checksum failure there still fails the whole generation. `recover_checkpoint`
returns a `CheckpointRecovery` report (graphs loaded, indexes rebuilt, vectors
re-inserted), which is what the tests assert rather than a time.

The `hnsw_rs` reader asserts on malformed input and exits the process on a
short data file, so it is reached only for files the manifest has verified, and
a panic it raises is contained and treated as a rebuild. Graph files are a
same-machine cache: `hnsw_rs` writes native-endian words, so a sidecar from a
different word size or byte order is refused. Each graph reload leaks the
`hnsw_rs` reader handle (a path, a name and a counter), because the loaded
graph borrows it for a `'static` lifetime.

Disk per generation is now about twice the vector bytes: the f32 vector list
(current entries) and the graph's data file (all entries, superseded ones
included, at the index's precision).

## What remains

Still not zero-rebuild startup. Ordinary server startup does not read these
checkpoints; it rebuilds indexes from node rows. Restart integration tied to a
consistent RocksDB generation, a single in-memory vector copy (the index still
keeps its own list next to the graph's points), and 1M/10M restart/RSS
measurements remain. Do not load this cache into ordinary startup without a
freshness/generation contract: an old but checksum-valid index must not hide
newer persisted writes. No startup-speed or memory figure is claimed.
