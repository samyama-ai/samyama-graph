//! Importers for other engines' export formats.
//!
//! A migration path is not a document. A user with a Neo4j database can already
//! find out whether their *queries* will run here — `examples/compatibility_report`
//! answers that — and until this module existed they still had no supported way
//! to bring their *data*. Knowing a migration will fail is progress over not
//! knowing, and it is not a migration.

pub mod neo4j_json;
