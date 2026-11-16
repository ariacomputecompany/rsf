#![forbid(unsafe_code)]

use rsf_core::relational_key::RelationalKeyFormat;
use std::collections::BTreeMap;

use rsf_constraints::{ConstraintDecision, ConstraintState, ConstraintValidator, RowMutation};
use rsf_core::{
    Bytes, ErrorCode, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
    transaction::{CommitIntent, KeyRange, Mutation, TransactionId},
};
use rsf_index::{IndexDescriptor, IndexMutation, IndexPlanner, RowImage};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRequest {
    pub id: TransactionId,
    pub table: TableName,
    pub primary_key: String,
    pub values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteDecision {
    Planned(WritePlan),
    Rejected {
        checked: usize,
        violations: Vec<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WritePlan {
    pub table: TableName,
    pub row_key: Bytes,
    pub row_value: Bytes,
    pub index_mutations: Vec<IndexMutation>,
    pub intent: CommitIntent,
    pub steps: Vec<WriteStep>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteStep {
    CheckConstraints { checks: usize },
    WriteRow { key: Bytes },
    UpdateIndexes { mutations: usize },
    DeclareConflicts { ranges: usize },
    Commit,
}

#[derive(Clone, Debug)]
pub struct WritePlanner {
    key_format: RelationalKeyFormat,
    constraints: ConstraintValidator,
    indexes: Vec<IndexDescriptor>,
}

impl WritePlanner {
    pub fn new(
        table: &TableDescriptor,
        constraints: ConstraintValidator,
        indexes: Vec<IndexDescriptor>,
    ) -> RsfResult<Self> {
        for index in &indexes {
            validate_index(table, index)?;
        }
        Ok(Self {
            key_format: RelationalKeyFormat::default(),
            constraints,
            indexes,
        })
    }

    pub fn with_key_format(mut self, key_format: RelationalKeyFormat) -> Self {
        self.key_format = key_format;
        self
    }

    pub fn plan_insert(
        &self,
        table: &TableDescriptor,
        request: WriteRequest,
        constraints: &ConstraintState,
    ) -> RsfResult<WriteDecision> {
        if request.table != table.name {
            return Err(invalid(format!(
                "write request targets {}, not table {}",
                request.table.0, table.name.0
            )));
        }
        let mutation = RowMutation {
            table: request.table.clone(),
            values: request
                .values
                .iter()
                .map(|(column, value)| {
                    (
                        column.clone(),
                        rsf_constraints::ConstraintValue::Text(value.clone()),
                    )
                })
                .collect(),
        };

        let constraint_decision = self.constraints.validate(table, &mutation, constraints)?;
        let checks = match constraint_decision {
            ConstraintDecision::Accepted { checks } => checks.len(),
            ConstraintDecision::Rejected { checks, violations } => {
                return Ok(WriteDecision::Rejected {
                    checked: checks.len(),
                    violations: violations
                        .into_iter()
                        .map(|violation| violation.rule)
                        .collect(),
                });
            }
        };

        let row_key = self
            .key_format
            .row_key(&request.table.0, &request.primary_key);
        let row_value = encode_row(&request.values);
        let row = RowImage {
            primary_key: request.primary_key.clone(),
            values: request.values.clone(),
        };
        let index_plan =
            IndexPlanner::new(table, self.indexes.clone())?.plan_insert(table, &row)?;
        let mut mutations = vec![Mutation::Set {
            key: row_key.clone(),
            value: row_value.clone(),
        }];
        for mutation in &index_plan.mutations {
            match mutation {
                IndexMutation::Put {
                    key, primary_key, ..
                } => {
                    mutations.push(Mutation::Set {
                        key: key.clone(),
                        value: primary_key.as_bytes().to_vec(),
                    });
                }
                IndexMutation::Delete { key, .. } => {
                    mutations.push(Mutation::Clear { key: key.clone() });
                }
            }
        }

        let conflict_ranges = vec![
            point_range(&row_key),
            unique_range(
                self.key_format,
                &request.table,
                "primary",
                std::slice::from_ref(&request.primary_key),
            ),
        ];
        let conflict_range_count = conflict_ranges.len();
        let index_mutation_count = index_plan.mutations.len();
        let intent = CommitIntent {
            id: request.id,
            read_conflicts: conflict_ranges.clone(),
            write_conflicts: conflict_ranges,
            mutations,
        };

        Ok(WriteDecision::Planned(WritePlan {
            table: request.table,
            row_key: row_key.clone(),
            row_value,
            index_mutations: index_plan.mutations,
            intent,
            steps: vec![
                WriteStep::CheckConstraints { checks },
                WriteStep::WriteRow { key: row_key },
                WriteStep::UpdateIndexes {
                    mutations: index_mutation_count,
                },
                WriteStep::DeclareConflicts {
                    ranges: conflict_range_count,
                },
                WriteStep::Commit,
            ],
        }))
    }

    pub fn plan_update(
        &self,
        table: &TableDescriptor,
        before: RowImage,
        request: WriteRequest,
        constraints: &ConstraintState,
    ) -> RsfResult<WriteDecision> {
        if request.table != table.name {
            return Err(invalid(format!(
                "write request targets {}, not table {}",
                request.table.0, table.name.0
            )));
        }
        if before.primary_key != request.primary_key {
            return Err(invalid(
                "update before image must match request primary key",
            ));
        }
        let mutation = RowMutation {
            table: request.table.clone(),
            values: request
                .values
                .iter()
                .map(|(column, value)| {
                    (
                        column.clone(),
                        rsf_constraints::ConstraintValue::Text(value.clone()),
                    )
                })
                .collect(),
        };

        let constraint_decision = self.constraints.validate(table, &mutation, constraints)?;
        let checks = match constraint_decision {
            ConstraintDecision::Accepted { checks } => checks.len(),
            ConstraintDecision::Rejected { checks, violations } => {
                return Ok(WriteDecision::Rejected {
                    checked: checks.len(),
                    violations: violations
                        .into_iter()
                        .map(|violation| violation.rule)
                        .collect(),
                });
            }
        };

        let row_key = self
            .key_format
            .row_key(&request.table.0, &request.primary_key);
        let row_value = encode_row(&request.values);
        let after = RowImage {
            primary_key: request.primary_key.clone(),
            values: request.values.clone(),
        };
        let index_plan =
            IndexPlanner::new(table, self.indexes.clone())?.plan_update(table, &before, &after)?;
        let mut mutations = vec![Mutation::Set {
            key: row_key.clone(),
            value: row_value.clone(),
        }];
        append_index_mutations(&mut mutations, &index_plan.mutations);

        let conflict_ranges = vec![
            point_range(&row_key),
            unique_range(
                self.key_format,
                &request.table,
                "primary",
                std::slice::from_ref(&request.primary_key),
            ),
        ];
        let conflict_range_count = conflict_ranges.len();
        let index_mutation_count = index_plan.mutations.len();
        let intent = CommitIntent {
            id: request.id,
            read_conflicts: conflict_ranges.clone(),
            write_conflicts: conflict_ranges,
            mutations,
        };

        Ok(WriteDecision::Planned(WritePlan {
            table: request.table,
            row_key: row_key.clone(),
            row_value,
            index_mutations: index_plan.mutations,
            intent,
            steps: vec![
                WriteStep::CheckConstraints { checks },
                WriteStep::WriteRow { key: row_key },
                WriteStep::UpdateIndexes {
                    mutations: index_mutation_count,
                },
                WriteStep::DeclareConflicts {
                    ranges: conflict_range_count,
                },
                WriteStep::Commit,
            ],
        }))
    }

    pub fn plan_delete(
        &self,
        table: &TableDescriptor,
        row: RowImage,
        id: TransactionId,
    ) -> RsfResult<WriteDecision> {
        let row_key = self.key_format.row_key(&table.name.0, &row.primary_key);
        let index_plan =
            IndexPlanner::new(table, self.indexes.clone())?.plan_delete(table, &row)?;
        let mut mutations = vec![Mutation::Clear {
            key: row_key.clone(),
        }];
        append_index_mutations(&mut mutations, &index_plan.mutations);

        let conflict_ranges = vec![
            point_range(&row_key),
            unique_range(
                self.key_format,
                &table.name,
                "primary",
                std::slice::from_ref(&row.primary_key),
            ),
        ];
        let conflict_range_count = conflict_ranges.len();
        let index_mutation_count = index_plan.mutations.len();
        let intent = CommitIntent {
            id,
            read_conflicts: conflict_ranges.clone(),
            write_conflicts: conflict_ranges,
            mutations,
        };

        Ok(WriteDecision::Planned(WritePlan {
            table: table.name.clone(),
            row_key: row_key.clone(),
            row_value: Vec::new(),
            index_mutations: index_plan.mutations,
            intent,
            steps: vec![
                WriteStep::CheckConstraints { checks: 0 },
                WriteStep::WriteRow { key: row_key },
                WriteStep::UpdateIndexes {
                    mutations: index_mutation_count,
                },
                WriteStep::DeclareConflicts {
                    ranges: conflict_range_count,
                },
                WriteStep::Commit,
            ],
        }))
    }
}

fn append_index_mutations(mutations: &mut Vec<Mutation>, index_mutations: &[IndexMutation]) {
    for mutation in index_mutations {
        match mutation {
            IndexMutation::Put {
                key, primary_key, ..
            } => {
                mutations.push(Mutation::Set {
                    key: key.clone(),
                    value: primary_key.as_bytes().to_vec(),
                });
            }
            IndexMutation::Delete { key, .. } => {
                mutations.push(Mutation::Clear { key: key.clone() });
            }
        }
    }
}

fn validate_index(table: &TableDescriptor, index: &IndexDescriptor) -> RsfResult<()> {
    if index.table != table.name {
        return Err(invalid(format!(
            "index {} targets {}, not table {}",
            index.name, index.table.0, table.name.0
        )));
    }
    if index.columns.is_empty() {
        return Err(invalid("write planner index must include columns"));
    }
    for column in &index.columns {
        if !table.columns.iter().any(|known| known.name == *column) {
            return Err(invalid(format!(
                "write planner index column {column} is not in table {}",
                table.name.0
            )));
        }
    }
    Ok(())
}

fn unique_range(
    format: RelationalKeyFormat,
    table: &TableName,
    name: &str,
    values: &[String],
) -> KeyRange {
    point_range(&format.constraint_identity(&table.0, name, values))
}

fn point_range(key: &[u8]) -> KeyRange {
    let mut end = key.to_vec();
    end.push(0);
    KeyRange {
        start: key.to_vec(),
        end,
    }
}

fn encode_row(values: &BTreeMap<String, String>) -> Bytes {
    values
        .iter()
        .map(|(column, value)| format!("{column}={value}"))
        .collect::<Vec<_>>()
        .join(";")
        .into_bytes()
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_constraints::{ConstraintRule, ConstraintState, ConstraintValidator};
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
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn planner() -> WritePlanner {
        let table = table();
        WritePlanner::new(
            &table,
            ConstraintValidator::new(
                &table,
                vec![
                    ConstraintRule::PrimaryKey {
                        columns: vec!["id".to_string()],
                    },
                    ConstraintRule::Unique {
                        name: "users_name".to_string(),
                        columns: vec!["name".to_string()],
                    },
                ],
            )
            .unwrap(),
            vec![IndexDescriptor {
                table: table.name.clone(),
                name: "users_name".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            }],
        )
        .unwrap()
    }

    fn request(id: &str, name: &str) -> WriteRequest {
        WriteRequest {
            id: TransactionId([1; 16]),
            table: TableName("users".to_string()),
            primary_key: id.to_string(),
            values: BTreeMap::from([
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
            ]),
        }
    }

    #[test]
    fn accepted_insert_produces_row_index_and_commit_steps() {
        let decision = planner()
            .plan_insert(&table(), request("1", "ada"), &ConstraintState::default())
            .unwrap();

        let WriteDecision::Planned(plan) = decision else {
            panic!("valid write should plan");
        };
        assert_eq!(plan.steps.len(), 5);
        assert_eq!(plan.index_mutations.len(), 1);
        assert_eq!(plan.intent.mutations.len(), 2);
        assert_eq!(plan.intent.write_conflicts.len(), 2);
        assert_eq!(plan.row_key, b"\0RSFSQLR2\0\0\0\0\0\0\0\x05users1".to_vec());
    }

    #[test]
    fn accepted_update_rewrites_row_and_replaces_index_entry() {
        let before = RowImage {
            primary_key: "1".to_string(),
            values: BTreeMap::from([
                ("id".to_string(), "1".to_string()),
                ("name".to_string(), "ada".to_string()),
            ]),
        };
        let decision = planner()
            .plan_update(
                &table(),
                before,
                request("1", "grace"),
                &ConstraintState::default(),
            )
            .unwrap();

        let WriteDecision::Planned(plan) = decision else {
            panic!("valid update should plan");
        };
        assert_eq!(plan.steps.len(), 5);
        assert_eq!(plan.index_mutations.len(), 2);
        assert_eq!(plan.intent.mutations.len(), 3);
        assert_eq!(plan.intent.write_conflicts.len(), 2);
    }

    #[test]
    fn accepted_delete_clears_row_and_index_entry() {
        let row = RowImage {
            primary_key: "1".to_string(),
            values: BTreeMap::from([
                ("id".to_string(), "1".to_string()),
                ("name".to_string(), "ada".to_string()),
            ]),
        };
        let decision = planner()
            .plan_delete(&table(), row, TransactionId([9; 16]))
            .unwrap();

        let WriteDecision::Planned(plan) = decision else {
            panic!("valid delete should plan");
        };
        assert_eq!(plan.steps.len(), 5);
        assert_eq!(plan.index_mutations.len(), 1);
        assert_eq!(plan.intent.mutations.len(), 2);
        assert_eq!(plan.intent.write_conflicts.len(), 2);
    }

    #[test]
    fn rejected_constraint_stops_write_plan() {
        let mut state = ConstraintState::default();
        state.add_unique_key("users_name", ["ada"]);
        let decision = planner()
            .plan_insert(&table(), request("2", "ada"), &state)
            .unwrap();

        let WriteDecision::Rejected {
            checked,
            violations,
        } = decision
        else {
            panic!("duplicate unique key should reject");
        };
        assert_eq!(checked, 4);
        assert_eq!(violations, vec!["unique:users_name".to_string()]);
    }

    #[test]
    fn malformed_index_metadata_is_rejected() {
        let table = table();
        let err = WritePlanner::new(
            &table,
            ConstraintValidator::new(
                &table,
                vec![ConstraintRule::PrimaryKey {
                    columns: vec!["id".to_string()],
                }],
            )
            .unwrap(),
            vec![IndexDescriptor {
                table: TableName("orders".to_string()),
                name: "bad".to_string(),
                columns: vec!["name".to_string()],
                unique: true,
            }],
        )
        .expect_err("bad index should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }
}
