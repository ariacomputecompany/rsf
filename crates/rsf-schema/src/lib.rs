#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use rsf_core::{
    CommitVersion, ErrorCode, RsfError, RsfResult,
    catalog::{ColumnDescriptor, ColumnType, TableDescriptor},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaVersion(pub CommitVersion);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaMigration {
    pub id: String,
    pub table: TableDescriptor,
    pub from: SchemaVersion,
    pub changes: Vec<SchemaChange>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchemaChange {
    AddColumn(ColumnDescriptor),
    DropColumn { name: String },
    AddIndex { name: String, columns: Vec<String> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationPolicy {
    pub allow_blocking_backfill: bool,
    pub allow_column_drop: bool,
    pub max_backfill_rows_per_batch: u32,
}

impl Default for MigrationPolicy {
    fn default() -> Self {
        Self {
            allow_blocking_backfill: false,
            allow_column_drop: false,
            max_backfill_rows_per_batch: 1_000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationPlan {
    pub id: String,
    pub table: TableDescriptor,
    pub from: SchemaVersion,
    pub to: SchemaVersion,
    pub steps: Vec<MigrationStep>,
    pub compatibility: Compatibility,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationStep {
    ValidateCurrentSchema,
    PublishDescriptor { version: SchemaVersion },
    BackfillColumn { name: String, batch_rows: u32 },
    BuildIndex { name: String, columns: Vec<String> },
    SwitchReads { version: SchemaVersion },
    RetireColumn { name: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Compatibility {
    pub old_readers: bool,
    pub old_writers: bool,
    pub requires_backfill: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SchemaPlanner;

impl SchemaPlanner {
    pub fn plan(
        &self,
        migration: SchemaMigration,
        target: SchemaVersion,
        policy: &MigrationPolicy,
    ) -> RsfResult<MigrationPlan> {
        validate_policy(policy)?;
        validate_migration(&migration)?;
        if target.0 <= migration.from.0 {
            return Err(invalid("target schema version must be newer than source"));
        }

        let mut descriptor = migration.table.clone();
        let mut steps = vec![MigrationStep::ValidateCurrentSchema];
        let mut requires_backfill = false;
        let mut old_writers = true;

        for change in &migration.changes {
            match change {
                SchemaChange::AddColumn(column) => {
                    validate_column(column)?;
                    if descriptor
                        .columns
                        .iter()
                        .any(|existing| existing.name == column.name)
                    {
                        return Err(invalid(format!("column {} already exists", column.name)));
                    }
                    if !column.nullable {
                        requires_backfill = true;
                    }
                    descriptor.columns.push(column.clone());
                    steps.push(MigrationStep::PublishDescriptor {
                        version: target.clone(),
                    });
                    if !column.nullable {
                        steps.push(MigrationStep::BackfillColumn {
                            name: column.name.clone(),
                            batch_rows: policy.max_backfill_rows_per_batch,
                        });
                    }
                }
                SchemaChange::DropColumn { name } => {
                    if !policy.allow_column_drop {
                        return Err(invalid(format!(
                            "column drop {name} requires explicit policy"
                        )));
                    }
                    if descriptor.primary_key.iter().any(|key| key == name) {
                        return Err(invalid(format!("cannot drop primary key column {name}")));
                    }
                    let before = descriptor.columns.len();
                    descriptor.columns.retain(|column| column.name != *name);
                    if descriptor.columns.len() == before {
                        return Err(invalid(format!("column {name} does not exist")));
                    }
                    old_writers = false;
                    steps.push(MigrationStep::RetireColumn { name: name.clone() });
                }
                SchemaChange::AddIndex { name, columns } => {
                    validate_name("index", name)?;
                    if columns.is_empty() {
                        return Err(invalid("index must include at least one column"));
                    }
                    for column in columns {
                        if !descriptor
                            .columns
                            .iter()
                            .any(|existing| existing.name == *column)
                        {
                            return Err(invalid(format!(
                                "index column {column} is not in table descriptor"
                            )));
                        }
                    }
                    steps.push(MigrationStep::BuildIndex {
                        name: name.clone(),
                        columns: columns.clone(),
                    });
                }
            }
        }

        steps.push(MigrationStep::SwitchReads {
            version: target.clone(),
        });

        Ok(MigrationPlan {
            id: migration.id,
            table: descriptor,
            from: migration.from,
            to: target,
            steps,
            compatibility: Compatibility {
                old_readers: true,
                old_writers,
                requires_backfill,
            },
        })
    }
}

fn validate_migration(migration: &SchemaMigration) -> RsfResult<()> {
    validate_name("migration", &migration.id)?;
    validate_name("namespace", &migration.table.namespace.0)?;
    validate_name("table", &migration.table.name.0)?;
    if migration.table.columns.is_empty() {
        return Err(invalid("migration table descriptor must include columns"));
    }
    if migration.table.primary_key.is_empty() {
        return Err(invalid(
            "migration table descriptor must include primary key",
        ));
    }
    let mut names = BTreeSet::new();
    for column in &migration.table.columns {
        validate_column(column)?;
        if !names.insert(column.name.clone()) {
            return Err(invalid(format!("duplicate column {}", column.name)));
        }
    }
    for key in &migration.table.primary_key {
        if !names.contains(key) {
            return Err(invalid(format!("primary key column {key} is missing")));
        }
    }
    if migration.changes.is_empty() {
        return Err(invalid("migration must include at least one schema change"));
    }
    Ok(())
}

fn validate_policy(policy: &MigrationPolicy) -> RsfResult<()> {
    if policy.max_backfill_rows_per_batch == 0 {
        return Err(invalid("backfill batch size must be greater than zero"));
    }
    if policy.allow_blocking_backfill && policy.max_backfill_rows_per_batch > 100_000 {
        return Err(invalid("blocking backfill batch size is too large"));
    }
    Ok(())
}

fn validate_column(column: &ColumnDescriptor) -> RsfResult<()> {
    validate_name("column", &column.name)?;
    match column.ty {
        ColumnType::Int64 | ColumnType::Bytes | ColumnType::Text | ColumnType::Bool => Ok(()),
    }
}

fn validate_name(kind: &str, value: &str) -> RsfResult<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(invalid(format!(
            "{kind} name must be non-empty ASCII alphanumeric, '_' or '-'"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::{NamespaceName, TableName};

    use super::*;

    #[test]
    fn additive_column_and_index_plan_is_online_compatible() {
        let plan = SchemaPlanner
            .plan(
                SchemaMigration {
                    id: "users_add_email".to_string(),
                    table: users(),
                    from: SchemaVersion(CommitVersion(10)),
                    changes: vec![
                        SchemaChange::AddColumn(ColumnDescriptor {
                            name: "email".to_string(),
                            ty: ColumnType::Text,
                            nullable: true,
                        }),
                        SchemaChange::AddIndex {
                            name: "users_email".to_string(),
                            columns: vec!["email".to_string()],
                        },
                    ],
                },
                SchemaVersion(CommitVersion(11)),
                &MigrationPolicy::default(),
            )
            .unwrap();

        assert_eq!(plan.table.columns.len(), 3);
        assert!(plan.compatibility.old_readers);
        assert!(plan.compatibility.old_writers);
        assert_eq!(plan.steps.len(), 4);
    }

    #[test]
    fn non_nullable_column_requires_backfill_step() {
        let plan = SchemaPlanner
            .plan(
                SchemaMigration {
                    id: "users_add_active".to_string(),
                    table: users(),
                    from: SchemaVersion(CommitVersion(10)),
                    changes: vec![SchemaChange::AddColumn(ColumnDescriptor {
                        name: "active".to_string(),
                        ty: ColumnType::Bool,
                        nullable: false,
                    })],
                },
                SchemaVersion(CommitVersion(11)),
                &MigrationPolicy::default(),
            )
            .unwrap();

        assert!(plan.compatibility.requires_backfill);
        assert!(plan.steps.iter().any(|step| {
            matches!(
                step,
                MigrationStep::BackfillColumn {
                    name,
                    batch_rows: 1_000
                } if name == "active"
            )
        }));
    }

    #[test]
    fn column_drop_requires_explicit_policy() {
        let err = SchemaPlanner
            .plan(
                SchemaMigration {
                    id: "users_drop_name".to_string(),
                    table: users(),
                    from: SchemaVersion(CommitVersion(10)),
                    changes: vec![SchemaChange::DropColumn {
                        name: "name".to_string(),
                    }],
                },
                SchemaVersion(CommitVersion(11)),
                &MigrationPolicy::default(),
            )
            .expect_err("drop without policy must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn cannot_drop_primary_key() {
        let err = SchemaPlanner
            .plan(
                SchemaMigration {
                    id: "users_drop_id".to_string(),
                    table: users(),
                    from: SchemaVersion(CommitVersion(10)),
                    changes: vec![SchemaChange::DropColumn {
                        name: "id".to_string(),
                    }],
                },
                SchemaVersion(CommitVersion(11)),
                &MigrationPolicy {
                    allow_column_drop: true,
                    ..MigrationPolicy::default()
                },
            )
            .expect_err("primary key drop must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    fn users() -> TableDescriptor {
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
}
