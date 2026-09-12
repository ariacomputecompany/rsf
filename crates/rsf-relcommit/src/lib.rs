#![forbid(unsafe_code)]

use rsf_core::transaction::Mutation;
use rsf_write::WritePlan;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalCommitGroup {
    pub table: String,
    pub row_key: Vec<u8>,
    pub row_mutations: usize,
    pub index_mutations: usize,
    pub conflict_ranges: usize,
    pub intent_mutations: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitGroupDecision {
    Accepted(RelationalCommitGroup),
    Rejected {
        reason: String,
        row_mutations: usize,
        index_mutations: usize,
        conflict_ranges: usize,
        intent_mutations: usize,
    },
}

#[derive(Clone, Debug, Default)]
pub struct RelationalCommitAuditor;

impl RelationalCommitAuditor {
    pub fn audit(&self, plan: &WritePlan) -> CommitGroupDecision {
        let row_mutations = plan
            .intent
            .mutations
            .iter()
            .filter(|mutation| mutation_key(mutation) == Some(plan.row_key.as_slice()))
            .count();
        let index_mutations = plan.index_mutations.len();
        let conflict_ranges = plan.intent.write_conflicts.len();
        let intent_mutations = plan.intent.mutations.len();

        let reject = |reason: String| CommitGroupDecision::Rejected {
            reason,
            row_mutations,
            index_mutations,
            conflict_ranges,
            intent_mutations,
        };

        let expected_row_prefix = format!("table/{}/pk/", plan.table.0);
        if !plan.row_key.starts_with(expected_row_prefix.as_bytes()) {
            return reject(format!(
                "row key {} does not belong to table {}",
                String::from_utf8_lossy(&plan.row_key),
                plan.table.0
            ));
        }
        if row_mutations != 1 {
            return reject(format!(
                "commit group must contain exactly one row mutation, got {row_mutations}"
            ));
        }
        if intent_mutations != row_mutations + index_mutations {
            return reject(format!(
                "intent mutations {intent_mutations} do not match row/index group {}",
                row_mutations + index_mutations
            ));
        }
        if let Some(missing) = missing_index_mutation(plan) {
            return reject(format!(
                "index mutation key {} is absent from commit intent",
                String::from_utf8_lossy(missing)
            ));
        }
        if conflict_ranges < 2 {
            return reject(format!(
                "commit group must declare row and identity conflict ranges, got {conflict_ranges}"
            ));
        }

        CommitGroupDecision::Accepted(RelationalCommitGroup {
            table: plan.table.0.clone(),
            row_key: plan.row_key.clone(),
            row_mutations,
            index_mutations,
            conflict_ranges,
            intent_mutations,
        })
    }
}

fn mutation_key(mutation: &Mutation) -> Option<&[u8]> {
    match mutation {
        Mutation::Set { key, .. } | Mutation::Clear { key } => Some(key.as_slice()),
    }
}

fn missing_index_mutation(plan: &WritePlan) -> Option<&[u8]> {
    plan.index_mutations
        .iter()
        .map(|mutation| match mutation {
            rsf_index::IndexMutation::Put { key, .. }
            | rsf_index::IndexMutation::Delete { key, .. } => key.as_slice(),
        })
        .find(|index_key| {
            !plan
                .intent
                .mutations
                .iter()
                .any(|intent| mutation_key(intent) == Some(*index_key))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rsf_constraints::{ConstraintRule, ConstraintState, ConstraintValidator};
    use rsf_core::{
        catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName},
        transaction::TransactionId,
    };
    use rsf_index::{IndexDescriptor, RowImage};
    use rsf_write::{WriteDecision, WritePlanner, WriteRequest};

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

    fn request(id: u8, name: &str) -> WriteRequest {
        WriteRequest {
            id: TransactionId([id; 16]),
            table: TableName("users".to_string()),
            primary_key: id.to_string(),
            values: BTreeMap::from([
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
            ]),
        }
    }

    fn planned_insert() -> WritePlan {
        let decision = planner()
            .plan_insert(&table(), request(1, "ada"), &ConstraintState::default())
            .unwrap();
        let WriteDecision::Planned(plan) = decision else {
            panic!("insert should plan");
        };
        plan
    }

    #[test]
    fn accepts_row_and_index_mutations_as_one_group() {
        let decision = RelationalCommitAuditor.audit(&planned_insert());

        let CommitGroupDecision::Accepted(group) = decision else {
            panic!("complete group should pass");
        };
        assert_eq!(group.table, "users");
        assert_eq!(group.row_mutations, 1);
        assert_eq!(group.index_mutations, 1);
        assert_eq!(group.intent_mutations, 2);
        assert_eq!(group.conflict_ranges, 2);
    }

    #[test]
    fn accepts_update_group_with_replaced_index_entry() {
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
                request(1, "grace"),
                &ConstraintState::default(),
            )
            .unwrap();
        let WriteDecision::Planned(plan) = decision else {
            panic!("update should plan");
        };

        let CommitGroupDecision::Accepted(group) = RelationalCommitAuditor.audit(&plan) else {
            panic!("complete update group should pass");
        };
        assert_eq!(group.row_mutations, 1);
        assert_eq!(group.index_mutations, 2);
        assert_eq!(group.intent_mutations, 3);
    }

    #[test]
    fn rejects_missing_index_mutation_from_intent() {
        let mut plan = planned_insert();
        plan.intent.mutations.pop();

        let CommitGroupDecision::Rejected { reason, .. } = RelationalCommitAuditor.audit(&plan)
        else {
            panic!("partial group should reject");
        };
        assert!(reason.contains("do not match row/index group"));
    }

    #[test]
    fn rejects_missing_row_mutation_from_intent() {
        let mut plan = planned_insert();
        plan.intent.mutations.remove(0);

        let CommitGroupDecision::Rejected { reason, .. } = RelationalCommitAuditor.audit(&plan)
        else {
            panic!("partial group should reject");
        };
        assert!(reason.contains("exactly one row mutation"));
    }

    #[test]
    fn accepts_row_only_group_when_table_has_no_secondary_index() {
        let table = table();
        let planner = WritePlanner::new(
            &table,
            ConstraintValidator::new(
                &table,
                vec![ConstraintRule::PrimaryKey {
                    columns: vec!["id".to_string()],
                }],
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap();
        let decision = planner
            .plan_insert(&table, request(2, "codd"), &ConstraintState::default())
            .unwrap();
        let WriteDecision::Planned(plan) = decision else {
            panic!("row-only insert should plan");
        };

        let CommitGroupDecision::Accepted(group) = RelationalCommitAuditor.audit(&plan) else {
            panic!("row-only commit group should pass");
        };
        assert_eq!(group.row_mutations, 1);
        assert_eq!(group.index_mutations, 0);
        assert_eq!(group.intent_mutations, 1);
    }
}
