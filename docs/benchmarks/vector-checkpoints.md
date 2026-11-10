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

This is the crash-safe checkpoint foundation, not zero-rebuild startup. The
payload is still the legacy f32 vector-list format and loads by insertion. HNSW
adjacency/entry-point persistence, shared vector storage, restart integration tied
to a consistent RocksDB generation, and 1M/10M restart/RSS measurements remain.
Do not load this cache into ordinary startup without a freshness/generation
contract: an old but checksum-valid index must not hide newer persisted writes.
No startup-speed or memory reduction is claimed by this PR.
