//! Physical operator for the named graph projection statements (ALGO-05, #1681).
//!
//! One operator runs all four, because they share the row they report:
//!
//! | Statement | Rows |
//! |---|---|
//! | `CREATE GRAPH PROJECTION` | the projection, `status` `created` or `already exists` |
//! | `REFRESH GRAPH PROJECTION` | the projection, `status` `refreshed` |
//! | `DROP GRAPH PROJECTION` | none, as `DROP INDEX` |
//! | `SHOW GRAPH PROJECTIONS` | one per projection, by name, `status` `ok` |
//!
//! Every refusal is an argument error naming the projection: a name in use, a
//! name no projection has, or an empty one.

use std::sync::Arc;

use crate::graph::projection::{Created, ProjectionError, ProjectionInfo};
use crate::graph::{GraphStore, PropertyValue};
use crate::query::ast::GraphProjectionDdl;

use super::operator::{OperatorDescription, PhysicalOperator};
use super::record::{Record, Value};
use super::{ExecutionError, ExecutionResult};

/// The `status` of a projection a `CREATE` built.
pub const STATUS_CREATED: &str = "created";
/// The `status` of a projection `CREATE ... IF NOT EXISTS` found already there.
pub const STATUS_ALREADY_EXISTS: &str = "already exists";
/// The `status` of a projection a `REFRESH` rebuilt.
pub const STATUS_REFRESHED: &str = "refreshed";
/// The `status` of a projection `SHOW` lists.
pub const STATUS_LISTED: &str = "ok";

/// The columns every projection row carries, in order.
pub fn projection_columns() -> Vec<String> {
    [
        "name",
        "node_label",
        "relationship_type",
        "weight_property",
        "nodes",
        "relationships",
        "bytes",
        "stale",
        "status",
    ]
    .iter()
    .map(|c| c.to_string())
    .collect()
}

/// The columns a statement reports: none for `DROP`.
pub fn columns_for(ddl: &GraphProjectionDdl) -> Vec<String> {
    match ddl {
        GraphProjectionDdl::Drop { .. } => Vec::new(),
        _ => projection_columns(),
    }
}

/// An optional name as a cell: the name, or null when the spec takes all.
fn name_or_null(name: Option<String>) -> Value {
    Value::Property(name.map_or(PropertyValue::Null, PropertyValue::String))
}

fn count(n: usize) -> Value {
    // A projection larger than i64::MAX entries cannot be built in memory, so
    // saturating is a formality that keeps the conversion total.
    Value::Property(PropertyValue::Integer(i64::try_from(n).unwrap_or(i64::MAX)))
}

/// One row describing a projection.
pub fn projection_record(info: ProjectionInfo, status: &str) -> Record {
    let mut record = Record::new();
    record.bind(
        "name".to_string(),
        Value::Property(PropertyValue::String(info.name)),
    );
    record.bind("node_label".to_string(), name_or_null(info.spec.node_label));
    record.bind(
        "relationship_type".to_string(),
        name_or_null(info.spec.edge_type),
    );
    record.bind(
        "weight_property".to_string(),
        name_or_null(info.spec.weight_property),
    );
    record.bind("nodes".to_string(), count(info.nodes));
    record.bind("relationships".to_string(), count(info.relationships));
    record.bind("bytes".to_string(), count(info.bytes));
    record.bind(
        "stale".to_string(),
        Value::Property(PropertyValue::Boolean(info.stale)),
    );
    record.bind(
        "status".to_string(),
        Value::Property(PropertyValue::String(status.to_string())),
    );
    record
}

fn refused(e: ProjectionError) -> ExecutionError {
    ExecutionError::bad_argument(e.to_string())
}

/// Runs one projection statement.
pub struct GraphProjectionOperator {
    ddl: GraphProjectionDdl,
    rows: Option<std::vec::IntoIter<Record>>,
}

impl GraphProjectionOperator {
    pub fn new(ddl: GraphProjectionDdl) -> Self {
        Self { ddl, rows: None }
    }

    /// The rows for `SHOW`, which needs no write access.
    fn show(store: &GraphStore) -> Vec<Record> {
        store
            .graph_projections
            .list(store.data_changes())
            .into_iter()
            .map(|info| projection_record(info, STATUS_LISTED))
            .collect()
    }

    /// The rows for the statements that change the registry.
    fn change(&self, store: &GraphStore) -> ExecutionResult<Vec<Record>> {
        // A handle of its own, so the registry can be given the store it
        // belongs to as an argument.
        let projections = Arc::clone(&store.graph_projections);
        Ok(match &self.ddl {
            GraphProjectionDdl::Create {
                name,
                spec,
                if_not_exists,
            } => {
                match projections
                    .create(store, name, spec.clone(), *if_not_exists)
                    .map_err(refused)?
                {
                    Created::Built(info) => vec![projection_record(info, STATUS_CREATED)],
                    Created::AlreadyThere(info) => {
                        vec![projection_record(info, STATUS_ALREADY_EXISTS)]
                    }
                }
            }
            GraphProjectionDdl::Refresh { name } => {
                vec![projection_record(
                    projections.refresh(store, name).map_err(refused)?,
                    STATUS_REFRESHED,
                )]
            }
            GraphProjectionDdl::Drop { name, if_exists } => {
                projections
                    .drop_projection(name, *if_exists)
                    .map_err(refused)?;
                Vec::new()
            }
            GraphProjectionDdl::Show => Self::show(store),
        })
    }

    fn emit(
        &mut self,
        rows: impl FnOnce() -> ExecutionResult<Vec<Record>>,
    ) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(rows()?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }
}

impl PhysicalOperator for GraphProjectionOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.ddl.is_write() {
            // `Query::is_write` sends these to the write executor; reaching
            // here means a read executor was built for one directly.
            return Err(ExecutionError::write_in_read(
                "a graph projection statement other than SHOW holds or frees memory for later \
                 queries, so it needs the write path",
            ));
        }
        self.emit(|| Ok(Self::show(store)))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            let rows = self.change(store)?;
            self.rows = Some(rows.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn is_mutating(&self) -> bool {
        self.ddl.is_write()
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn describe(&self) -> OperatorDescription {
        let (name, details) = match &self.ddl {
            GraphProjectionDdl::Create { name, .. } => ("CreateGraphProjection", name.clone()),
            GraphProjectionDdl::Refresh { name } => ("RefreshGraphProjection", name.clone()),
            GraphProjectionDdl::Drop { name, .. } => ("DropGraphProjection", name.clone()),
            GraphProjectionDdl::Show => ("ShowGraphProjections", String::new()),
        };
        OperatorDescription {
            name: name.to_string(),
            details,
            children: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::projection::ProjectionSpec;

    const NAME: &str = "p";

    fn store_with_two_nodes() -> GraphStore {
        let mut s = GraphStore::new();
        let a = s.create_node("N");
        let b = s.create_node("N");
        s.create_edge(a, b, "R").unwrap();
        s
    }

    fn run_mut(s: &mut GraphStore, ddl: GraphProjectionDdl) -> ExecutionResult<Vec<Record>> {
        let mut op = GraphProjectionOperator::new(ddl);
        let mut rows = Vec::new();
        while let Some(r) = op.next_mut(s, "default")? {
            rows.push(r);
        }
        Ok(rows)
    }

    fn create(if_not_exists: bool) -> GraphProjectionDdl {
        GraphProjectionDdl::Create {
            name: NAME.into(),
            spec: ProjectionSpec::default(),
            if_not_exists,
        }
    }

    fn cell<'a>(r: &'a Record, column: &str) -> &'a PropertyValue {
        r.get(column).and_then(|v| v.as_property()).expect(column)
    }

    #[test]
    fn create_reports_the_projection_with_every_column() {
        let mut s = store_with_two_nodes();
        let rows = run_mut(&mut s, create(false)).unwrap();
        assert_eq!(rows.len(), 1);
        for column in projection_columns() {
            assert!(rows[0].get(&column).is_some(), "no `{column}`");
        }
        assert_eq!(cell(&rows[0], "nodes"), &PropertyValue::Integer(2));
        assert_eq!(cell(&rows[0], "relationships"), &PropertyValue::Integer(1));
        assert_eq!(
            cell(&rows[0], "node_label"),
            &PropertyValue::Null,
            "no filter: all nodes"
        );
        assert_eq!(
            cell(&rows[0], "status"),
            &PropertyValue::String(STATUS_CREATED.into())
        );
    }

    #[test]
    fn if_not_exists_reports_the_existing_one() {
        let mut s = store_with_two_nodes();
        run_mut(&mut s, create(false)).unwrap();
        let rows = run_mut(&mut s, create(true)).unwrap();
        assert_eq!(
            cell(&rows[0], "status"),
            &PropertyValue::String(STATUS_ALREADY_EXISTS.into())
        );
        let err = run_mut(&mut s, create(false)).unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
    }

    #[test]
    fn refresh_drop_and_show_report_what_the_registry_holds() {
        let mut s = store_with_two_nodes();
        run_mut(&mut s, create(false)).unwrap();
        let refreshed = run_mut(&mut s, GraphProjectionDdl::Refresh { name: NAME.into() }).unwrap();
        assert_eq!(
            cell(&refreshed[0], "status"),
            &PropertyValue::String(STATUS_REFRESHED.into())
        );
        let shown = run_mut(&mut s, GraphProjectionDdl::Show).unwrap();
        assert_eq!(shown.len(), 1);
        let dropped = run_mut(
            &mut s,
            GraphProjectionDdl::Drop {
                name: NAME.into(),
                if_exists: false,
            },
        )
        .unwrap();
        assert!(dropped.is_empty());
        assert!(run_mut(&mut s, GraphProjectionDdl::Show)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn refusals_are_argument_errors() {
        let mut s = store_with_two_nodes();
        let err = run_mut(&mut s, GraphProjectionDdl::Refresh { name: NAME.into() }).unwrap_err();
        assert_eq!(err.code(), crate::query::error_code::BAD_ARGUMENT);
        let err = run_mut(
            &mut s,
            GraphProjectionDdl::Drop {
                name: NAME.into(),
                if_exists: false,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), crate::query::error_code::BAD_ARGUMENT);
        assert!(run_mut(
            &mut s,
            GraphProjectionDdl::Drop {
                name: NAME.into(),
                if_exists: true
            }
        )
        .is_ok());
    }

    #[test]
    fn the_read_path_shows_and_refuses_everything_else() {
        let mut s = store_with_two_nodes();
        run_mut(&mut s, create(false)).unwrap();
        let mut show = GraphProjectionOperator::new(GraphProjectionDdl::Show);
        assert!(show.next(&s).unwrap().is_some());
        assert!(!show.is_mutating());
        let mut create_op = GraphProjectionOperator::new(create(true));
        assert!(create_op.is_mutating());
        let err = create_op.next(&s).unwrap_err();
        assert_eq!(err.code(), crate::query::error_code::WRITE_IN_READ);
    }

    #[test]
    fn drop_reports_no_columns_and_the_rest_report_all() {
        assert!(columns_for(&GraphProjectionDdl::Drop {
            name: NAME.into(),
            if_exists: true
        })
        .is_empty());
        assert_eq!(columns_for(&GraphProjectionDdl::Show), projection_columns());
    }

    #[test]
    fn describe_names_the_statement_and_the_projection() {
        let d = GraphProjectionOperator::new(GraphProjectionDdl::Refresh { name: NAME.into() })
            .describe();
        assert_eq!(d.name, "RefreshGraphProjection");
        assert_eq!(d.details, NAME);
    }

    #[test]
    fn reset_lets_show_run_again() {
        let mut s = store_with_two_nodes();
        run_mut(&mut s, create(false)).unwrap();
        let mut show = GraphProjectionOperator::new(GraphProjectionDdl::Show);
        assert!(show.next(&s).unwrap().is_some());
        assert!(show.next(&s).unwrap().is_none());
        show.reset();
        assert!(show.next(&s).unwrap().is_some());
    }
}
