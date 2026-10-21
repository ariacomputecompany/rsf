#![forbid(unsafe_code)]

use std::collections::BTreeSet;

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

        let format = match rsf_core::relational_key::RelationalKeyFormat::detect([plan
            .row_key
            .as_slice()])
        {
            Ok(format) => format,
            Err(err) => return reject(err.message),
        };
        if format.primary_key(&plan.table.0, &plan.row_key).is_err() {
            return reject(format!(
                "row key {} does not belong to table {}",
                key_description(&plan.row_key),
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
                key_description(missing)
            ));
        }
        if let Err(reason) = validate_mutation_keys(plan) {
            return reject(reason);
        }
        if let Err(reason) = validate_conflict_ranges(plan) {
            return reject(reason);
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

fn validate_mutation_keys(plan: &WritePlan) -> Result<(), String> {
    let mut expected = BTreeSet::from([plan.row_key.as_slice()]);
    for index_key in plan.index_mutations.iter().map(index_mutation_key) {
        if !expected.insert(index_key) {
            return Err(format!(
                "commit group declares duplicate index mutation key {}",
                key_description(index_key)
            ));
        }
    }

    let mut observed = BTreeSet::new();
    for mutation in &plan.intent.mutations {
        let Some(key) = mutation_key(mutation) else {
            continue;
        };
        if !observed.insert(key) {
            return Err(format!(
                "commit intent contains duplicate mutation key {}",
                key_description(key)
            ));
        }
    }

    if observed != expected {
        let unexpected = observed
            .difference(&expected)
            .next()
            .map(|key| key_description(key));
        let missing = expected
            .difference(&observed)
            .next()
            .map(|key| key_description(key));
        return Err(match (unexpected, missing) {
            (Some(unexpected), Some(missing)) => {
                format!(
                    "commit intent has unexpected mutation key {unexpected} and is missing {missing}"
                )
            }
            (Some(unexpected), None) => {
                format!("commit intent has unexpected mutation key {unexpected}")
            }
            (None, Some(missing)) => format!("commit intent is missing mutation key {missing}"),
            (None, None) => "commit intent mutation set does not match commit group".to_string(),
        });
    }

    Ok(())
}

fn validate_conflict_ranges(plan: &WritePlan) -> Result<(), String> {
    let expected = BTreeSet::from([
        range_key(&point_range(&plan.row_key)),
        range_key(&primary_identity_range(plan).ok_or_else(|| {
            format!(
                "row key {} does not encode a primary identity",
                key_description(&plan.row_key)
            )
        })?),
    ]);
    let read = conflict_set(&plan.intent.read_conflicts, "read")?;
    let write = conflict_set(&plan.intent.write_conflicts, "write")?;

    if read != expected {
        return Err(conflict_mismatch("read", &read, &expected));
    }
    if write != expected {
        return Err(conflict_mismatch("write", &write, &expected));
    }

    Ok(())
}

type ConflictSet = BTreeSet<(Vec<u8>, Vec<u8>)>;

fn conflict_set(
    ranges: &[rsf_core::transaction::KeyRange],
    label: &str,
) -> Result<ConflictSet, String> {
    let mut set = BTreeSet::new();
    for range in ranges {
        if range.start >= range.end {
            return Err(format!(
                "{label} conflict range {}..{} is empty or reversed",
                key_description(&range.start),
                key_description(&range.end)
            ));
        }
        if !set.insert(range_key(range)) {
            return Err(format!(
                "{label} conflict ranges contain duplicate range {}..{}",
                key_description(&range.start),
                key_description(&range.end)
            ));
        }
    }
    Ok(set)
}

fn conflict_mismatch(label: &str, observed: &ConflictSet, expected: &ConflictSet) -> String {
    let unexpected = observed
        .iter()
        .find(|range| !expected.contains(*range))
        .map(|range| key_description(&range.0));
    let missing = expected
        .iter()
        .find(|range| !observed.contains(*range))
        .map(|range| key_description(&range.0));
    match (unexpected, missing) {
        (Some(unexpected), Some(missing)) => {
            format!("{label} conflict ranges include unexpected {unexpected} and miss {missing}")
        }
        (Some(unexpected), None) => {
            format!("{label} conflict ranges include unexpected {unexpected}")
        }
        (None, Some(missing)) => format!("{label} conflict ranges miss {missing}"),
        (None, None) => format!("{label} conflict ranges do not match commit group"),
    }
}

fn mutation_key(mutation: &Mutation) -> Option<&[u8]> {
    match mutation {
        Mutation::Set { key, .. } | Mutation::Clear { key } => Some(key.as_slice()),
    }
}

fn index_mutation_key(mutation: &rsf_index::IndexMutation) -> &[u8] {
    match mutation {
        rsf_index::IndexMutation::Put { key, .. }
        | rsf_index::IndexMutation::Delete { key, .. } => key.as_slice(),
    }
}

fn missing_index_mutation(plan: &WritePlan) -> Option<&[u8]> {
    plan.index_mutations
        .iter()
        .map(index_mutation_key)
        .find(|index_key| {
            !plan
                .intent
                .mutations
                .iter()
                .any(|intent| mutation_key(intent) == Some(*index_key))
        })
}

fn key_description(key: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(key)
        && text.chars().all(|ch| !ch.is_control())
    {
        return text.to_string();
    }
    format!(
        "hex:{}",
        key.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

fn primary_identity_range(plan: &WritePlan) -> Option<rsf_core::transaction::KeyRange> {
    let format =
        rsf_core::relational_key::RelationalKeyFormat::detect([plan.row_key.as_slice()]).ok()?;
    let primary = format.primary_key(&plan.table.0, &plan.row_key).ok()?;
    Some(point_range(
        &format.primary_identity(&plan.table.0, &primary),
    ))
}

fn point_range(key: &[u8]) -> rsf_core::transaction::KeyRange {
    let mut end = key.to_vec();
    end.push(0);
    rsf_core::transaction::KeyRange {
        start: key.to_vec(),
        end,
    }
}

fn range_key(range: &rsf_core::transaction::KeyRange) -> (Vec<u8>, Vec<u8>) {
    (range.start.clone(), range.end.clone())
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
    fn rejects_unexpected_duplicate_mutation_key() {
        let mut plan = planned_insert();
        let duplicate_index = plan.index_mutations[0].clone();
        let duplicate_intent = plan.intent.mutations[1].clone();
        plan.index_mutations.push(duplicate_index);
        plan.intent.mutations.push(duplicate_intent);

        let CommitGroupDecision::Rejected { reason, .. } = RelationalCommitAuditor.audit(&plan)
        else {
            panic!("duplicate mutation key should reject");
        };
        assert!(reason.contains("duplicate index mutation key"));
    }

    #[test]
    fn rejects_conflict_ranges_that_do_not_cover_row_and_identity() {
        let mut plan = planned_insert();
        plan.intent.write_conflicts.pop();

        let CommitGroupDecision::Rejected { reason, .. } = RelationalCommitAuditor.audit(&plan)
        else {
            panic!("missing identity conflict range should reject");
        };
        let identity =
            rsf_core::relational_key::RelationalKeyFormat::default().primary_identity("users", "1");
        assert!(reason.contains(&format!(
            "write conflict ranges miss {}",
            key_description(&identity)
        )));
    }

    #[test]
    fn rejects_mismatched_read_conflict_ranges() {
        let mut plan = planned_insert();
        plan.intent.read_conflicts[0].start = b"table/users/pk/other".to_vec();
        plan.intent.read_conflicts[0].end = b"table/users/pk/other\0".to_vec();

        let CommitGroupDecision::Rejected { reason, .. } = RelationalCommitAuditor.audit(&plan)
        else {
            panic!("mismatched read conflict range should reject");
        };
        assert!(reason.contains("read conflict ranges include unexpected"));
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
