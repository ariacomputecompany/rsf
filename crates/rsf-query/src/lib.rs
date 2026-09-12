#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{
    ErrorCode, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryRow {
    pub table: TableName,
    pub columns: BTreeMap<String, QueryValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryValue {
    Int64(i64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlan {
    pub table: TableName,
    pub projection: Projection,
    pub predicate: Option<Predicate>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Projection {
    All,
    Columns(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    Eq { column: String, value: QueryValue },
    And(Vec<Predicate>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResult {
    pub rows: Vec<QueryRow>,
}

#[derive(Clone, Debug, Default)]
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn select(
        &self,
        table: &TableDescriptor,
        projection: Projection,
        predicate: Option<Predicate>,
        limit: Option<usize>,
    ) -> RsfResult<QueryPlan> {
        validate_projection(table, &projection)?;
        if let Some(predicate) = &predicate {
            validate_predicate(table, predicate)?;
        }
        if limit == Some(0) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "query limit must be greater than zero",
            ));
        }
        Ok(QueryPlan {
            table: table.name.clone(),
            projection,
            predicate,
            limit,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct QueryExecutor;

impl QueryExecutor {
    pub fn execute(&self, plan: &QueryPlan, rows: &[QueryRow]) -> RsfResult<QueryResult> {
        let mut output = Vec::new();
        for row in rows.iter().filter(|row| row.table == plan.table) {
            if !plan
                .predicate
                .as_ref()
                .is_none_or(|predicate| predicate_matches(predicate, row))
            {
                continue;
            }
            output.push(project_row(row, &plan.projection)?);
            if plan.limit.is_some_and(|limit| output.len() >= limit) {
                break;
            }
        }
        Ok(QueryResult { rows: output })
    }
}

fn validate_projection(table: &TableDescriptor, projection: &Projection) -> RsfResult<()> {
    match projection {
        Projection::All => Ok(()),
        Projection::Columns(columns) => {
            if columns.is_empty() {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "projection must include at least one column",
                ));
            }
            for column in columns {
                validate_column(table, column)?;
            }
            Ok(())
        }
    }
}

fn validate_predicate(table: &TableDescriptor, predicate: &Predicate) -> RsfResult<()> {
    match predicate {
        Predicate::Eq { column, .. } => validate_column(table, column),
        Predicate::And(predicates) => {
            if predicates.is_empty() {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "AND predicate must include at least one child",
                ));
            }
            for predicate in predicates {
                validate_predicate(table, predicate)?;
            }
            Ok(())
        }
    }
}

fn validate_column(table: &TableDescriptor, column: &str) -> RsfResult<()> {
    if table
        .columns
        .iter()
        .any(|descriptor| descriptor.name == column)
    {
        return Ok(());
    }
    Err(RsfError::new(
        ErrorCode::InvalidInput,
        format!("unknown query column {column}"),
    ))
}

fn predicate_matches(predicate: &Predicate, row: &QueryRow) -> bool {
    match predicate {
        Predicate::Eq { column, value } => row.columns.get(column) == Some(value),
        Predicate::And(predicates) => predicates
            .iter()
            .all(|predicate| predicate_matches(predicate, row)),
    }
}

fn project_row(row: &QueryRow, projection: &Projection) -> RsfResult<QueryRow> {
    match projection {
        Projection::All => Ok(row.clone()),
        Projection::Columns(columns) => {
            let mut projected = BTreeMap::new();
            for column in columns {
                let Some(value) = row.columns.get(column) else {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        format!("row is missing projected column {column}"),
                    ));
                };
                projected.insert(column.clone(), value.clone());
            }
            Ok(QueryRow {
                table: row.table.clone(),
                columns: projected,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName};

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

    fn row(id: i64, name: &str, active: bool) -> QueryRow {
        QueryRow {
            table: TableName("users".to_string()),
            columns: BTreeMap::from([
                ("id".to_string(), QueryValue::Int64(id)),
                ("name".to_string(), QueryValue::Text(name.to_string())),
                ("active".to_string(), QueryValue::Bool(active)),
            ]),
        }
    }

    #[test]
    fn plan_validates_projection_and_predicate_columns() {
        let plan = QueryPlanner
            .select(
                &table(),
                Projection::Columns(vec!["id".to_string(), "name".to_string()]),
                Some(Predicate::Eq {
                    column: "active".to_string(),
                    value: QueryValue::Bool(true),
                }),
                Some(10),
            )
            .unwrap();

        assert_eq!(plan.table.0, "users");
    }

    #[test]
    fn unknown_column_is_rejected() {
        let err = QueryPlanner
            .select(
                &table(),
                Projection::Columns(vec!["missing".to_string()]),
                None,
                None,
            )
            .expect_err("bad projection must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn executor_filters_projects_and_limits_rows() {
        let plan = QueryPlanner
            .select(
                &table(),
                Projection::Columns(vec!["name".to_string()]),
                Some(Predicate::Eq {
                    column: "active".to_string(),
                    value: QueryValue::Bool(true),
                }),
                Some(1),
            )
            .unwrap();
        let rows = [row(1, "ada", true), row(2, "grace", true)];

        let result = QueryExecutor.execute(&plan, &rows).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].columns.get("name"),
            Some(&QueryValue::Text("ada".to_string()))
        );
        assert!(!result.rows[0].columns.contains_key("active"));
    }

    #[test]
    fn executor_ignores_other_tables() {
        let mut other = row(1, "object", true);
        other.table = TableName("objects".to_string());
        let plan = QueryPlanner
            .select(&table(), Projection::All, None, None)
            .unwrap();

        let result = QueryExecutor.execute(&plan, &[other]).unwrap();

        assert!(result.rows.is_empty());
    }
}
