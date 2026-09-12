#![forbid(unsafe_code)]

use rsf_core::{
    ErrorCode, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
};
use rsf_index::IndexDescriptor;
use rsf_query::{Predicate, Projection, QueryPlan};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableStats {
    pub table: TableName,
    pub rows: u64,
    pub average_row_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlanningContext {
    pub stats: TableStats,
    pub indexes: Vec<IndexDescriptor>,
    pub stream_threshold_bytes: u64,
    pub cacheable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPlan {
    pub table: TableName,
    pub operators: Vec<PhysicalOperator>,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalOperator {
    pub access: AccessPath,
    pub estimated_rows: u64,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccessPath {
    CacheLookup,
    PrimaryKeyLookup { column: String },
    SecondaryIndexLookup { index: String, column: String },
    RangeScan,
    StreamRead { threshold_bytes: u64 },
}

#[derive(Clone, Debug, Default)]
pub struct PhysicalPlanner;

impl PhysicalPlanner {
    pub fn plan(
        &self,
        table: &TableDescriptor,
        logical: &QueryPlan,
        context: &PhysicalPlanningContext,
    ) -> RsfResult<PhysicalPlan> {
        validate_context(table, logical, context)?;

        let access = choose_access_path(table, logical, context);
        let estimated_rows = estimate_rows(context.stats.rows, logical, &access);
        let mut estimated_bytes = estimated_rows.saturating_mul(context.stats.average_row_bytes);
        let mut operators = Vec::new();

        if context.cacheable {
            operators.push(PhysicalOperator {
                access: AccessPath::CacheLookup,
                estimated_rows,
                estimated_bytes,
            });
        }

        operators.push(PhysicalOperator {
            access,
            estimated_rows,
            estimated_bytes,
        });

        if estimated_bytes > context.stream_threshold_bytes {
            operators.push(PhysicalOperator {
                access: AccessPath::StreamRead {
                    threshold_bytes: context.stream_threshold_bytes,
                },
                estimated_rows,
                estimated_bytes,
            });
        }

        if matches!(logical.projection, Projection::Columns(_)) {
            estimated_bytes = estimated_bytes
                .min(estimated_rows.saturating_mul((context.stats.average_row_bytes / 2).max(1)));
        }

        Ok(PhysicalPlan {
            table: logical.table.clone(),
            operators,
            estimated_rows,
            estimated_bytes,
        })
    }
}

fn validate_context(
    table: &TableDescriptor,
    logical: &QueryPlan,
    context: &PhysicalPlanningContext,
) -> RsfResult<()> {
    if logical.table != table.name {
        return Err(invalid(format!(
            "logical plan targets {}, not table {}",
            logical.table.0, table.name.0
        )));
    }
    if context.stats.table != table.name {
        return Err(invalid(format!(
            "stats target {}, not table {}",
            context.stats.table.0, table.name.0
        )));
    }
    if context.stats.average_row_bytes == 0 {
        return Err(invalid("average row bytes must be greater than zero"));
    }
    if context.stream_threshold_bytes == 0 {
        return Err(invalid("stream threshold bytes must be greater than zero"));
    }
    for index in &context.indexes {
        validate_index(table, index)?;
    }
    Ok(())
}

fn validate_index(table: &TableDescriptor, index: &IndexDescriptor) -> RsfResult<()> {
    if index.table != table.name {
        return Err(invalid(format!(
            "index {} targets {}, not table {}",
            index.name, index.table.0, table.name.0
        )));
    }
    if index.columns.is_empty() {
        return Err(invalid(format!("index {} has no columns", index.name)));
    }
    for column in &index.columns {
        if !table
            .columns
            .iter()
            .any(|descriptor| descriptor.name == *column)
        {
            return Err(invalid(format!(
                "index {} references unknown column {column}",
                index.name
            )));
        }
    }
    Ok(())
}

fn choose_access_path(
    table: &TableDescriptor,
    logical: &QueryPlan,
    context: &PhysicalPlanningContext,
) -> AccessPath {
    let Some(eq_column) = equality_column(logical.predicate.as_ref()) else {
        return AccessPath::RangeScan;
    };

    if table.primary_key.iter().any(|column| column == &eq_column) {
        return AccessPath::PrimaryKeyLookup { column: eq_column };
    }

    if let Some(index) = context
        .indexes
        .iter()
        .find(|index| index.columns.first() == Some(&eq_column))
    {
        return AccessPath::SecondaryIndexLookup {
            index: index.name.clone(),
            column: eq_column,
        };
    }

    AccessPath::RangeScan
}

fn equality_column(predicate: Option<&Predicate>) -> Option<String> {
    match predicate? {
        Predicate::Eq { column, .. } => Some(column.clone()),
        Predicate::And(predicates) => predicates.iter().find_map(|predicate| {
            if let Predicate::Eq { column, .. } = predicate {
                Some(column.clone())
            } else {
                equality_column(Some(predicate))
            }
        }),
    }
}

fn estimate_rows(rows: u64, logical: &QueryPlan, access: &AccessPath) -> u64 {
    let by_access = match access {
        AccessPath::PrimaryKeyLookup { .. } => 1,
        AccessPath::SecondaryIndexLookup { .. } => rows.clamp(1, 10),
        AccessPath::RangeScan | AccessPath::CacheLookup | AccessPath::StreamRead { .. } => rows,
    };
    logical
        .limit
        .map(|limit| by_access.min(limit as u64))
        .unwrap_or(by_access)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName};
    use rsf_query::{Predicate, Projection, QueryPlanner, QueryValue};

    use super::*;

    fn table() -> TableDescriptor {
        TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("users".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "name".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "active".to_string(),
                    ty: ColumnType::Bool,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn context() -> PhysicalPlanningContext {
        PhysicalPlanningContext {
            stats: TableStats {
                table: TableName("users".to_string()),
                rows: 1_000,
                average_row_bytes: 512,
            },
            indexes: vec![IndexDescriptor {
                table: TableName("users".to_string()),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: false,
            }],
            stream_threshold_bytes: 8_192,
            cacheable: false,
        }
    }

    fn logical(predicate: Option<Predicate>, limit: Option<usize>) -> QueryPlan {
        QueryPlanner
            .select(&table(), Projection::All, predicate, limit)
            .unwrap()
    }

    #[test]
    fn primary_key_equality_uses_primary_key_lookup() {
        let plan = PhysicalPlanner
            .plan(
                &table(),
                &logical(
                    Some(Predicate::Eq {
                        column: "id".to_string(),
                        value: QueryValue::Int64(1),
                    }),
                    None,
                ),
                &context(),
            )
            .unwrap();

        assert_eq!(
            plan.operators[0].access,
            AccessPath::PrimaryKeyLookup {
                column: "id".to_string()
            }
        );
        assert_eq!(plan.estimated_rows, 1);
    }

    #[test]
    fn indexed_column_equality_uses_secondary_index_lookup() {
        let plan = PhysicalPlanner
            .plan(
                &table(),
                &logical(
                    Some(Predicate::Eq {
                        column: "name".to_string(),
                        value: QueryValue::Text("ada".to_string()),
                    }),
                    Some(5),
                ),
                &context(),
            )
            .unwrap();

        assert_eq!(
            plan.operators[0].access,
            AccessPath::SecondaryIndexLookup {
                index: "users_name".to_string(),
                column: "name".to_string()
            }
        );
        assert_eq!(plan.estimated_rows, 5);
    }

    #[test]
    fn unindexed_predicate_falls_back_to_range_scan() {
        let plan = PhysicalPlanner
            .plan(
                &table(),
                &logical(
                    Some(Predicate::Eq {
                        column: "active".to_string(),
                        value: QueryValue::Bool(true),
                    }),
                    None,
                ),
                &context(),
            )
            .unwrap();

        assert_eq!(plan.operators[0].access, AccessPath::RangeScan);
        assert_eq!(plan.estimated_rows, 1_000);
    }

    #[test]
    fn large_estimate_adds_stream_read_operator() {
        let mut context = context();
        context.stream_threshold_bytes = 4_096;
        let plan = PhysicalPlanner
            .plan(&table(), &logical(None, Some(20)), &context)
            .unwrap();

        assert!(plan.operators.iter().any(|operator| {
            operator.access
                == AccessPath::StreamRead {
                    threshold_bytes: 4_096,
                }
        }));
    }

    #[test]
    fn cacheable_plan_starts_with_cache_lookup() {
        let mut context = context();
        context.cacheable = true;
        let plan = PhysicalPlanner
            .plan(
                &table(),
                &logical(
                    Some(Predicate::Eq {
                        column: "id".to_string(),
                        value: QueryValue::Int64(1),
                    }),
                    None,
                ),
                &context,
            )
            .unwrap();

        assert_eq!(plan.operators[0].access, AccessPath::CacheLookup);
        assert_eq!(
            plan.operators[1].access,
            AccessPath::PrimaryKeyLookup {
                column: "id".to_string()
            }
        );
    }

    #[test]
    fn mismatched_index_metadata_is_rejected() {
        let mut context = context();
        context.indexes[0].table = TableName("orders".to_string());
        let err = PhysicalPlanner
            .plan(&table(), &logical(None, None), &context)
            .expect_err("mismatched index should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }
}
