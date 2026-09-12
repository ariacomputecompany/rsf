#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    ErrorCode, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
};

pub type RowValues = BTreeMap<String, ConstraintValue>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowMutation {
    pub table: TableName,
    pub values: RowValues,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstraintValue {
    Null,
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstraintRule {
    NotNull {
        column: String,
    },
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        name: String,
        columns: Vec<String>,
    },
    ForeignKey {
        name: String,
        columns: Vec<String>,
        referenced_table: TableName,
        referenced_columns: Vec<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstraintDecision {
    Accepted {
        checks: Vec<ConstraintCheck>,
    },
    Rejected {
        checks: Vec<ConstraintCheck>,
        violations: Vec<ConstraintViolation>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConstraintCheck {
    pub rule: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConstraintViolation {
    pub rule: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default)]
pub struct ConstraintState {
    unique_keys: BTreeMap<String, BTreeSet<Vec<String>>>,
    referenced_keys: BTreeMap<String, BTreeSet<Vec<String>>>,
}

#[derive(Clone, Debug)]
pub struct ConstraintValidator {
    rules: Vec<ConstraintRule>,
}

impl ConstraintValidator {
    pub fn new(table: &TableDescriptor, rules: Vec<ConstraintRule>) -> RsfResult<Self> {
        for rule in &rules {
            validate_rule(table, rule)?;
        }
        Ok(Self { rules })
    }

    pub fn validate(
        &self,
        table: &TableDescriptor,
        mutation: &RowMutation,
        state: &ConstraintState,
    ) -> RsfResult<ConstraintDecision> {
        if mutation.table != table.name {
            return Err(invalid(format!(
                "mutation targets {}, not table {}",
                mutation.table.0, table.name.0
            )));
        }

        let mut checks = Vec::new();
        let mut violations = Vec::new();
        for rule in table_not_null_rules(table).iter().chain(self.rules.iter()) {
            let check = evaluate(rule, mutation, state)?;
            if !check.passed {
                violations.push(ConstraintViolation {
                    rule: check.rule.clone(),
                    detail: check.detail.clone(),
                });
            }
            checks.push(check);
        }

        if violations.is_empty() {
            Ok(ConstraintDecision::Accepted { checks })
        } else {
            Ok(ConstraintDecision::Rejected { checks, violations })
        }
    }
}

impl ConstraintState {
    pub fn add_unique_key(
        &mut self,
        constraint: impl Into<String>,
        key: impl IntoIterator<Item = impl Into<String>>,
    ) {
        self.unique_keys
            .entry(constraint.into())
            .or_default()
            .insert(key.into_iter().map(Into::into).collect());
    }

    pub fn remove_unique_key(
        &mut self,
        constraint: &str,
        key: impl IntoIterator<Item = impl Into<String>>,
    ) {
        let key = key.into_iter().map(Into::into).collect::<Vec<_>>();
        if let Some(keys) = self.unique_keys.get_mut(constraint) {
            keys.remove(&key);
            if keys.is_empty() {
                self.unique_keys.remove(constraint);
            }
        }
    }

    pub fn add_referenced_key(
        &mut self,
        table: &TableName,
        columns: &[String],
        key: impl IntoIterator<Item = impl Into<String>>,
    ) {
        self.referenced_keys
            .entry(reference_name(table, columns))
            .or_default()
            .insert(key.into_iter().map(Into::into).collect());
    }

    pub fn remove_referenced_key(
        &mut self,
        table: &TableName,
        columns: &[String],
        key: impl IntoIterator<Item = impl Into<String>>,
    ) {
        let reference = reference_name(table, columns);
        let key = key.into_iter().map(Into::into).collect::<Vec<_>>();
        if let Some(keys) = self.referenced_keys.get_mut(&reference) {
            keys.remove(&key);
            if keys.is_empty() {
                self.referenced_keys.remove(&reference);
            }
        }
    }

    pub fn unique_key_count(&self) -> usize {
        self.unique_keys.values().map(BTreeSet::len).sum()
    }

    pub fn referenced_key_count(&self) -> usize {
        self.referenced_keys.values().map(BTreeSet::len).sum()
    }

    pub fn referenced_key_entries(&self) -> Vec<(String, Vec<Vec<String>>)> {
        self.referenced_keys
            .iter()
            .map(|(reference, keys)| (reference.clone(), keys.iter().cloned().collect::<Vec<_>>()))
            .collect()
    }
}

fn validate_rule(table: &TableDescriptor, rule: &ConstraintRule) -> RsfResult<()> {
    match rule {
        ConstraintRule::NotNull { column } => validate_column(table, column),
        ConstraintRule::PrimaryKey { columns } | ConstraintRule::Unique { columns, .. } => {
            validate_columns(table, columns)
        }
        ConstraintRule::ForeignKey {
            columns,
            referenced_columns,
            ..
        } => {
            validate_columns(table, columns)?;
            if columns.len() != referenced_columns.len() {
                return Err(invalid("foreign key column count must match reference"));
            }
            if referenced_columns.is_empty() {
                return Err(invalid("foreign key must include referenced columns"));
            }
            Ok(())
        }
    }
}

fn validate_columns(table: &TableDescriptor, columns: &[String]) -> RsfResult<()> {
    if columns.is_empty() {
        return Err(invalid("constraint must include at least one column"));
    }
    for column in columns {
        validate_column(table, column)?;
    }
    Ok(())
}

fn validate_column(table: &TableDescriptor, column: &str) -> RsfResult<()> {
    if table
        .columns
        .iter()
        .any(|descriptor| descriptor.name == column)
    {
        Ok(())
    } else {
        Err(invalid(format!(
            "constraint column {column} is not in table {}",
            table.name.0
        )))
    }
}

fn evaluate(
    rule: &ConstraintRule,
    mutation: &RowMutation,
    state: &ConstraintState,
) -> RsfResult<ConstraintCheck> {
    match rule {
        ConstraintRule::NotNull { column } => {
            let passed = mutation
                .values
                .get(column)
                .is_some_and(|value| !matches!(value, ConstraintValue::Null));
            Ok(check(
                rule_name(rule),
                passed,
                if passed {
                    format!("column {column} is present")
                } else {
                    format!("column {column} is missing or null")
                },
            ))
        }
        ConstraintRule::PrimaryKey { columns } => {
            let key = key_for(columns, mutation)?;
            let passed = !key.is_empty();
            Ok(check(
                rule_name(rule),
                passed,
                format!("primary key={}", key.join("/")),
            ))
        }
        ConstraintRule::Unique { name, columns } => {
            let Some(key) = optional_key_for(columns, mutation)? else {
                return Ok(check(
                    rule_name(rule),
                    true,
                    "unique key contains null and is handled by nullability rules".to_string(),
                ));
            };
            let passed = !state
                .unique_keys
                .get(name)
                .is_some_and(|known| known.contains(&key));
            Ok(check(
                rule_name(rule),
                passed,
                if passed {
                    format!("unique key {} is available", key.join("/"))
                } else {
                    format!("unique key {} already exists", key.join("/"))
                },
            ))
        }
        ConstraintRule::ForeignKey {
            columns,
            referenced_table,
            referenced_columns,
            ..
        } => {
            let Some(key) = optional_key_for(columns, mutation)? else {
                return Ok(check(
                    rule_name(rule),
                    true,
                    "foreign key contains null and is handled by nullability rules".to_string(),
                ));
            };
            let reference = reference_name(referenced_table, referenced_columns);
            let passed = state
                .referenced_keys
                .get(&reference)
                .is_some_and(|known| known.contains(&key));
            Ok(check(
                rule_name(rule),
                passed,
                if passed {
                    format!("referenced key {} exists", key.join("/"))
                } else {
                    format!("referenced key {} is missing", key.join("/"))
                },
            ))
        }
    }
}

fn optional_key_for(columns: &[String], mutation: &RowMutation) -> RsfResult<Option<Vec<String>>> {
    let mut key = Vec::new();
    for column in columns {
        let value = mutation
            .values
            .get(column)
            .ok_or_else(|| invalid(format!("constraint key column {column} is missing")))?;
        match value {
            ConstraintValue::Null => return Ok(None),
            ConstraintValue::Text(value) => key.push(value.clone()),
        }
    }
    Ok(Some(key))
}

fn key_for(columns: &[String], mutation: &RowMutation) -> RsfResult<Vec<String>> {
    let mut key = Vec::new();
    for column in columns {
        let value = mutation
            .values
            .get(column)
            .ok_or_else(|| invalid(format!("constraint key column {column} is missing")))?;
        match value {
            ConstraintValue::Null => {
                return Err(invalid(format!(
                    "constraint key column {column} must not be null"
                )));
            }
            ConstraintValue::Text(value) => key.push(value.clone()),
        }
    }
    Ok(key)
}

fn table_not_null_rules(table: &TableDescriptor) -> Vec<ConstraintRule> {
    table
        .columns
        .iter()
        .filter(|column| !column.nullable)
        .map(|column| ConstraintRule::NotNull {
            column: column.name.clone(),
        })
        .collect()
}

fn reference_name(table: &TableName, columns: &[String]) -> String {
    format!("{}({})", table.0, columns.join(","))
}

fn rule_name(rule: &ConstraintRule) -> String {
    match rule {
        ConstraintRule::NotNull { column } => format!("not-null:{column}"),
        ConstraintRule::PrimaryKey { columns } => format!("primary-key:{}", columns.join(",")),
        ConstraintRule::Unique { name, .. } => format!("unique:{name}"),
        ConstraintRule::ForeignKey { name, .. } => format!("foreign-key:{name}"),
    }
}

fn check(rule: String, passed: bool, detail: String) -> ConstraintCheck {
    ConstraintCheck {
        rule,
        passed,
        detail,
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
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
                    name: "org_id".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn validator() -> ConstraintValidator {
        ConstraintValidator::new(
            &table(),
            vec![
                ConstraintRule::PrimaryKey {
                    columns: vec!["id".to_string()],
                },
                ConstraintRule::Unique {
                    name: "users_name".to_string(),
                    columns: vec!["name".to_string()],
                },
                ConstraintRule::ForeignKey {
                    name: "users_org".to_string(),
                    columns: vec!["org_id".to_string()],
                    referenced_table: TableName("orgs".to_string()),
                    referenced_columns: vec!["id".to_string()],
                },
            ],
        )
        .unwrap()
    }

    fn mutation(id: &str, name: &str, org: &str) -> RowMutation {
        RowMutation {
            table: TableName("users".to_string()),
            values: BTreeMap::from([
                ("id".to_string(), ConstraintValue::Text(id.to_string())),
                ("name".to_string(), ConstraintValue::Text(name.to_string())),
                ("org_id".to_string(), ConstraintValue::Text(org.to_string())),
            ]),
        }
    }

    fn state() -> ConstraintState {
        let mut state = ConstraintState::default();
        state.add_unique_key("users_name", ["grace"]);
        state.add_referenced_key(
            &TableName("orgs".to_string()),
            &["id".to_string()],
            ["research"],
        );
        state
    }

    #[test]
    fn accepts_valid_row_mutation() {
        let decision = validator()
            .validate(&table(), &mutation("1", "ada", "research"), &state())
            .unwrap();

        let ConstraintDecision::Accepted { checks } = decision else {
            panic!("valid mutation should be accepted");
        };
        assert_eq!(checks.len(), 6);
        assert!(checks.iter().all(|check| check.passed));
    }

    #[test]
    fn rejects_duplicate_unique_key() {
        let decision = validator()
            .validate(&table(), &mutation("2", "grace", "research"), &state())
            .unwrap();

        let ConstraintDecision::Rejected { violations, .. } = decision else {
            panic!("duplicate unique key should reject");
        };
        assert_eq!(violations[0].rule, "unique:users_name");
    }

    #[test]
    fn rejects_missing_foreign_key() {
        let decision = validator()
            .validate(&table(), &mutation("3", "katherine", "missing"), &state())
            .unwrap();

        let ConstraintDecision::Rejected { violations, .. } = decision else {
            panic!("missing referenced key should reject");
        };
        assert_eq!(violations[0].rule, "foreign-key:users_org");
    }

    #[test]
    fn rejects_null_not_null_column() {
        let mut mutation = mutation("4", "margaret", "research");
        mutation
            .values
            .insert("name".to_string(), ConstraintValue::Null);
        let decision = validator().validate(&table(), &mutation, &state()).unwrap();

        let ConstraintDecision::Rejected { violations, .. } = decision else {
            panic!("null required column should reject");
        };
        assert!(
            violations
                .iter()
                .any(|violation| violation.rule == "not-null:name")
        );
    }

    #[test]
    fn malformed_rule_column_is_error() {
        let err = ConstraintValidator::new(
            &table(),
            vec![ConstraintRule::Unique {
                name: "bad".to_string(),
                columns: vec!["missing".to_string()],
            }],
        )
        .expect_err("unknown constraint column should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn constraint_state_removes_unique_and_referenced_keys() {
        let mut state = ConstraintState::default();
        let org_table = TableName("orgs".to_string());
        let id_columns = vec!["id".to_string()];

        state.add_unique_key("users_name", ["ada"]);
        state.add_referenced_key(&org_table, &id_columns, ["research"]);
        assert_eq!(state.unique_key_count(), 1);
        assert_eq!(state.referenced_key_count(), 1);

        state.remove_unique_key("users_name", ["ada"]);
        state.remove_referenced_key(&org_table, &id_columns, ["research"]);
        assert_eq!(state.unique_key_count(), 0);
        assert_eq!(state.referenced_key_count(), 0);
    }
}
