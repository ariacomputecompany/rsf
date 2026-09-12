#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    ErrorCode, RsfError, RsfResult,
    catalog::{ColumnDescriptor, ColumnType, TableDescriptor, TableName},
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
        validate_descriptor(table)?;
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

fn validate_descriptor(table: &TableDescriptor) -> RsfResult<()> {
    let mut names = BTreeSet::new();
    for column in &table.columns {
        if column.name.trim().is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("table {} contains a blank query column name", table.name.0),
            ));
        }
        if !names.insert(column.name.as_str()) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "table {} contains duplicate query column {}",
                    table.name.0, column.name
                ),
            ));
        }
    }
    Ok(())
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
            let mut seen = BTreeSet::new();
            for column in columns {
                if !seen.insert(column.as_str()) {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        format!("projection column {column} is duplicated"),
                    ));
                }
                validate_column(table, column)?;
            }
            Ok(())
        }
    }
}

fn validate_predicate(table: &TableDescriptor, predicate: &Predicate) -> RsfResult<()> {
    match predicate {
        Predicate::Eq { column, value } => {
            let descriptor = find_column(table, column)?;
            if query_value_matches_column_type(value, &descriptor.ty) {
                return Ok(());
            }
            Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "predicate column {column} expects {}, got {}",
                    column_type_name(&descriptor.ty),
                    query_value_type_name(value)
                ),
            ))
        }
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
    find_column(table, column).map(|_| ())
}

fn find_column<'a>(table: &'a TableDescriptor, column: &str) -> RsfResult<&'a ColumnDescriptor> {
    if let Some(descriptor) = table
        .columns
        .iter()
        .find(|descriptor| descriptor.name == column)
    {
        return Ok(descriptor);
    }
    Err(RsfError::new(
        ErrorCode::InvalidInput,
        format!("unknown query column {column}"),
    ))
}

fn query_value_matches_column_type(value: &QueryValue, ty: &ColumnType) -> bool {
    matches!(
        (value, ty),
        (QueryValue::Int64(_), ColumnType::Int64)
            | (QueryValue::Text(_), ColumnType::Text)
            | (QueryValue::Bool(_), ColumnType::Bool)
            | (QueryValue::Bytes(_), ColumnType::Bytes)
    )
}

fn query_value_type_name(value: &QueryValue) -> &'static str {
    match value {
        QueryValue::Int64(_) => "int64",
        QueryValue::Text(_) => "text",
        QueryValue::Bool(_) => "bool",
        QueryValue::Bytes(_) => "bytes",
    }
}

fn column_type_name(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Int64 => "int64",
        ColumnType::Bytes => "bytes",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
    }
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
    fn duplicate_projection_column_is_rejected() {
        let err = QueryPlanner
            .select(
                &table(),
                Projection::Columns(vec!["name".to_string(), "name".to_string()]),
                None,
                None,
            )
            .expect_err("duplicate projected columns must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicated"));
    }

    #[test]
    fn duplicate_descriptor_column_is_rejected() {
        let mut table = table();
        table.columns[1].name = "id".to_string();

        let err = QueryPlanner
            .select(&table, Projection::All, None, None)
            .expect_err("duplicate descriptor columns must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate query column id"));
    }

    #[test]
    fn blank_descriptor_column_is_rejected() {
        let mut table = table();
        table.columns[1].name = " ".to_string();

        let err = QueryPlanner
            .select(&table, Projection::All, None, None)
            .expect_err("blank descriptor columns must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("blank query column name"));
    }

    #[test]
    fn predicate_type_mismatch_is_rejected() {
        let err = QueryPlanner
            .select(
                &table(),
                Projection::All,
                Some(Predicate::Eq {
                    column: "id".to_string(),
                    value: QueryValue::Text("1".to_string()),
                }),
                None,
            )
            .expect_err("predicate literal must match catalog column type");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("expects int64"));
    }

    #[test]
    fn nested_predicate_type_mismatch_is_rejected() {
        let err = QueryPlanner
            .select(
                &table(),
                Projection::All,
                Some(Predicate::And(vec![
                    Predicate::Eq {
                        column: "active".to_string(),
                        value: QueryValue::Bool(true),
                    },
                    Predicate::Eq {
                        column: "name".to_string(),
                        value: QueryValue::Bytes(b"ada".to_vec()),
                    },
                ])),
                None,
            )
            .expect_err("nested predicate literal must match catalog column type");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("expects text"));
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
