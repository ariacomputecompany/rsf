#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName},
    storage::WalRecordKind,
};
use rsf_schema::{Compatibility, MigrationPlan, MigrationStep, SchemaVersion};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const MIGRATION_PG: PlacementGroupId = PlacementGroupId(45);
const MIGRATION_WAL_MAGIC: &[u8; 7] = b"RSFMIG1";
const MIGRATION_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationPhase {
    Planned,
    Validated,
    DescriptorPublished,
    Backfilling,
    IndexBuilding,
    ReadsSwitched,
    Complete,
    RolledBack,
}

impl MigrationPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Validated => "validated",
            Self::DescriptorPublished => "descriptor-published",
            Self::Backfilling => "backfilling",
            Self::IndexBuilding => "index-building",
            Self::ReadsSwitched => "reads-switched",
            Self::Complete => "complete",
            Self::RolledBack => "rolled-back",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationProgress {
    pub id: String,
    pub table: String,
    pub phase: MigrationPhase,
    pub completed_steps: usize,
    pub total_steps: usize,
    pub backfilled_rows: u64,
    pub indexed_keys: u64,
    pub durable_events: usize,
    pub rollback_reason: Option<String>,
}

impl MigrationProgress {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            MigrationPhase::Complete | MigrationPhase::RolledBack
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationDecision {
    Began(MigrationProgress),
    SchemaValidated(MigrationProgress),
    DescriptorPublished(MigrationProgress),
    BackfillBatch(MigrationProgress),
    IndexCatchUp(MigrationProgress),
    ReadsSwitched(MigrationProgress),
    Completed(MigrationProgress),
    Resumed(MigrationProgress),
    RolledBack(MigrationProgress),
    Rejected { id: String, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveMigration {
    plan: MigrationPlan,
    cursor: usize,
    phase: MigrationPhase,
    backfilled_rows: u64,
    indexed_keys: u64,
    durable_events: usize,
    rollback_reason: Option<String>,
}

impl ActiveMigration {
    fn progress(&self) -> MigrationProgress {
        MigrationProgress {
            id: self.plan.id.clone(),
            table: format!("{}.{}", self.plan.table.namespace.0, self.plan.table.name.0),
            phase: self.phase,
            completed_steps: self.cursor,
            total_steps: self.plan.steps.len(),
            backfilled_rows: self.backfilled_rows,
            indexed_keys: self.indexed_keys,
            durable_events: self.durable_events,
            rollback_reason: self.rollback_reason.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MigrationLifecycle {
    migrations: BTreeMap<String, ActiveMigration>,
}

impl MigrationLifecycle {
    pub fn begin(&mut self, plan: MigrationPlan) -> RsfResult<MigrationDecision> {
        validate_plan(&plan)?;
        if self.migrations.contains_key(&plan.id) {
            return Ok(MigrationDecision::Rejected {
                id: plan.id,
                reason: "migration is already active".to_string(),
            });
        }

        let id = plan.id.clone();
        let migration = ActiveMigration {
            plan,
            cursor: 0,
            phase: MigrationPhase::Planned,
            backfilled_rows: 0,
            indexed_keys: 0,
            durable_events: 1,
            rollback_reason: None,
        };
        let progress = migration.progress();
        self.migrations.insert(id, migration);
        Ok(MigrationDecision::Began(progress))
    }

    pub fn advance(
        &mut self,
        id: &str,
        backfill_rows: u64,
        indexed_keys: u64,
    ) -> RsfResult<MigrationDecision> {
        let migration = self
            .migrations
            .get_mut(id)
            .ok_or_else(|| invalid(format!("migration {id} is not active")))?;
        if migration.phase == MigrationPhase::RolledBack {
            return Ok(MigrationDecision::Rejected {
                id: id.to_string(),
                reason: "rolled-back migration cannot advance".to_string(),
            });
        }
        if migration.phase == MigrationPhase::Complete {
            return Ok(MigrationDecision::Completed(migration.progress()));
        }

        let step = migration
            .plan
            .steps
            .get(migration.cursor)
            .ok_or_else(|| invalid(format!("migration {id} has no remaining step")))?;

        let decision = match step {
            MigrationStep::ValidateCurrentSchema => {
                migration.phase = MigrationPhase::Validated;
                migration.cursor += 1;
                migration.durable_events += 1;
                MigrationDecision::SchemaValidated(migration.progress())
            }
            MigrationStep::PublishDescriptor { .. } => {
                migration.phase = MigrationPhase::DescriptorPublished;
                migration.cursor += 1;
                migration.durable_events += 1;
                MigrationDecision::DescriptorPublished(migration.progress())
            }
            MigrationStep::BackfillColumn { .. } => {
                if backfill_rows == 0 {
                    return Ok(MigrationDecision::Rejected {
                        id: id.to_string(),
                        reason: "backfill step requires at least one row".to_string(),
                    });
                }
                migration.phase = MigrationPhase::Backfilling;
                migration.cursor += 1;
                migration.backfilled_rows = migration.backfilled_rows.saturating_add(backfill_rows);
                migration.durable_events += 1;
                MigrationDecision::BackfillBatch(migration.progress())
            }
            MigrationStep::BuildIndex { .. } => {
                if indexed_keys == 0 {
                    return Ok(MigrationDecision::Rejected {
                        id: id.to_string(),
                        reason: "index build step requires at least one indexed key".to_string(),
                    });
                }
                migration.phase = MigrationPhase::IndexBuilding;
                migration.cursor += 1;
                migration.indexed_keys = migration.indexed_keys.saturating_add(indexed_keys);
                migration.durable_events += 1;
                MigrationDecision::IndexCatchUp(migration.progress())
            }
            MigrationStep::SwitchReads { .. } => {
                migration.phase = MigrationPhase::ReadsSwitched;
                migration.cursor += 1;
                migration.durable_events += 1;
                MigrationDecision::ReadsSwitched(migration.progress())
            }
            MigrationStep::RetireColumn { .. } => {
                migration.phase = MigrationPhase::ReadsSwitched;
                migration.cursor += 1;
                migration.durable_events += 1;
                MigrationDecision::ReadsSwitched(migration.progress())
            }
        };

        if migration.cursor == migration.plan.steps.len()
            && !matches!(migration.phase, MigrationPhase::Complete)
        {
            migration.phase = MigrationPhase::Complete;
            migration.durable_events += 1;
            return Ok(MigrationDecision::Completed(migration.progress()));
        }

        Ok(decision)
    }

    pub fn resume(&self, id: &str) -> RsfResult<MigrationDecision> {
        let migration = self
            .migrations
            .get(id)
            .ok_or_else(|| invalid(format!("migration {id} is not active")))?;
        Ok(MigrationDecision::Resumed(migration.progress()))
    }

    pub fn rollback(
        &mut self,
        id: &str,
        reason: impl Into<String>,
    ) -> RsfResult<MigrationDecision> {
        let reason = reason.into();
        if reason.is_empty() {
            return Err(invalid("rollback reason must be non-empty"));
        }
        let migration = self
            .migrations
            .get_mut(id)
            .ok_or_else(|| invalid(format!("migration {id} is not active")))?;
        if migration.phase == MigrationPhase::Complete {
            return Ok(MigrationDecision::Rejected {
                id: id.to_string(),
                reason: "completed migration cannot roll back".to_string(),
            });
        }
        migration.phase = MigrationPhase::RolledBack;
        migration.rollback_reason = Some(reason);
        migration.durable_events += 1;
        Ok(MigrationDecision::RolledBack(migration.progress()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MigrationWalRecord {
    Began(MigrationPlan),
    Advanced {
        id: String,
        backfill_rows: u64,
        indexed_keys: u64,
    },
    RolledBack {
        id: String,
        reason: String,
    },
}

#[derive(Debug)]
pub struct DurableMigrationLifecycle {
    lifecycle: MigrationLifecycle,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableMigrationLifecycle {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(MIGRATION_PG),
        })?;
        let mut lifecycle = MigrationLifecycle::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Migration)
        {
            apply_migration_record(
                &mut lifecycle,
                decode_migration_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            lifecycle,
            store,
            epoch,
            next_index,
        })
    }

    pub fn begin(&mut self, plan: MigrationPlan) -> RsfResult<MigrationDecision> {
        let mut lifecycle = self.lifecycle.clone();
        let decision = lifecycle.begin(plan.clone())?;
        if matches!(decision, MigrationDecision::Began(_)) {
            self.append_record(&MigrationWalRecord::Began(plan))?;
            self.lifecycle = lifecycle;
        }
        Ok(decision)
    }

    pub fn advance(
        &mut self,
        id: &str,
        backfill_rows: u64,
        indexed_keys: u64,
    ) -> RsfResult<MigrationDecision> {
        let mut lifecycle = self.lifecycle.clone();
        let decision = lifecycle.advance(id, backfill_rows, indexed_keys)?;
        if migration_decision_is_durable_change(&decision) {
            self.append_record(&MigrationWalRecord::Advanced {
                id: id.to_string(),
                backfill_rows,
                indexed_keys,
            })?;
            self.lifecycle = lifecycle;
        }
        Ok(decision)
    }

    pub fn resume(&self, id: &str) -> RsfResult<MigrationDecision> {
        self.lifecycle.resume(id)
    }

    pub fn rollback(
        &mut self,
        id: &str,
        reason: impl Into<String>,
    ) -> RsfResult<MigrationDecision> {
        let reason = reason.into();
        let mut lifecycle = self.lifecycle.clone();
        let decision = lifecycle.rollback(id, reason.clone())?;
        if matches!(decision, MigrationDecision::RolledBack(_)) {
            self.append_record(&MigrationWalRecord::RolledBack {
                id: id.to_string(),
                reason,
            })?;
            self.lifecycle = lifecycle;
        }
        Ok(decision)
    }

    pub fn lifecycle(&self) -> &MigrationLifecycle {
        &self.lifecycle
    }

    fn append_record(&mut self, record: &MigrationWalRecord) -> RsfResult<()> {
        let payload = encode_migration_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(MIGRATION_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: MIGRATION_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Migration,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn migration_decision_is_durable_change(decision: &MigrationDecision) -> bool {
    matches!(
        decision,
        MigrationDecision::SchemaValidated(_)
            | MigrationDecision::DescriptorPublished(_)
            | MigrationDecision::BackfillBatch(_)
            | MigrationDecision::IndexCatchUp(_)
            | MigrationDecision::ReadsSwitched(_)
            | MigrationDecision::Completed(_)
    )
}

fn apply_migration_record(
    lifecycle: &mut MigrationLifecycle,
    record: MigrationWalRecord,
) -> RsfResult<()> {
    match record {
        MigrationWalRecord::Began(plan) => match lifecycle.begin(plan)? {
            MigrationDecision::Began(_) => Ok(()),
            other => Err(corrupt(format!(
                "migration WAL begin replay produced unexpected decision {other:?}"
            ))),
        },
        MigrationWalRecord::Advanced {
            id,
            backfill_rows,
            indexed_keys,
        } => {
            let decision = lifecycle.advance(&id, backfill_rows, indexed_keys)?;
            if migration_decision_is_durable_change(&decision) {
                Ok(())
            } else {
                Err(corrupt(format!(
                    "migration WAL advance replay produced unexpected decision {decision:?}"
                )))
            }
        }
        MigrationWalRecord::RolledBack { id, reason } => match lifecycle.rollback(&id, reason)? {
            MigrationDecision::RolledBack(_) => Ok(()),
            other => Err(corrupt(format!(
                "migration WAL rollback replay produced unexpected decision {other:?}"
            ))),
        },
    }
}

fn encode_migration_wal_payload(record: &MigrationWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(MIGRATION_WAL_MAGIC);
    out.push(MIGRATION_WAL_VERSION);
    match record {
        MigrationWalRecord::Began(plan) => {
            out.push(1);
            encode_plan(plan, &mut out)?;
        }
        MigrationWalRecord::Advanced {
            id,
            backfill_rows,
            indexed_keys,
        } => {
            out.push(2);
            write_string(id, &mut out)?;
            write_u64(*backfill_rows, &mut out);
            write_u64(*indexed_keys, &mut out);
        }
        MigrationWalRecord::RolledBack { id, reason } => {
            out.push(3);
            write_string(id, &mut out)?;
            write_string(reason, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_migration_wal_payload(bytes: &[u8]) -> RsfResult<MigrationWalRecord> {
    if bytes.len() < MIGRATION_WAL_MAGIC.len() + 2 {
        return Err(corrupt("migration WAL payload is too short"));
    }
    if &bytes[..MIGRATION_WAL_MAGIC.len()] != MIGRATION_WAL_MAGIC {
        return Err(corrupt("migration WAL payload has invalid magic"));
    }
    let mut offset = MIGRATION_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != MIGRATION_WAL_VERSION {
        return Err(corrupt(format!(
            "migration WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => MigrationWalRecord::Began(decode_plan(bytes, &mut offset)?),
        2 => MigrationWalRecord::Advanced {
            id: read_string(bytes, &mut offset)?,
            backfill_rows: read_u64(bytes, &mut offset)?,
            indexed_keys: read_u64(bytes, &mut offset)?,
        },
        3 => MigrationWalRecord::RolledBack {
            id: read_string(bytes, &mut offset)?,
            reason: read_string(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "migration WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("migration WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_plan(plan: &MigrationPlan, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_plan(plan)?;
    write_string(&plan.id, out)?;
    encode_table(&plan.table, out)?;
    write_u64(plan.from.0.0, out);
    write_u64(plan.to.0.0, out);
    let steps_len = u32::try_from(plan.steps.len())
        .map_err(|_| invalid("migration plan has too many steps"))?;
    out.extend_from_slice(&steps_len.to_le_bytes());
    for step in &plan.steps {
        encode_step(step, out)?;
    }
    out.push(u8::from(plan.compatibility.old_readers));
    out.push(u8::from(plan.compatibility.old_writers));
    out.push(u8::from(plan.compatibility.requires_backfill));
    Ok(())
}

fn decode_plan(bytes: &[u8], offset: &mut usize) -> RsfResult<MigrationPlan> {
    let id = read_string(bytes, offset)?;
    let table = decode_table(bytes, offset)?;
    let from = SchemaVersion(CommitVersion(read_u64(bytes, offset)?));
    let to = SchemaVersion(CommitVersion(read_u64(bytes, offset)?));
    let steps_len = read_u32(bytes, offset)?;
    let steps_len =
        usize::try_from(steps_len).map_err(|_| corrupt("migration step count is too large"))?;
    let mut steps = Vec::with_capacity(steps_len);
    for _ in 0..steps_len {
        steps.push(decode_step(bytes, offset)?);
    }
    let plan = MigrationPlan {
        id,
        table,
        from,
        to,
        steps,
        compatibility: Compatibility {
            old_readers: read_bool(bytes, offset)?,
            old_writers: read_bool(bytes, offset)?,
            requires_backfill: read_bool(bytes, offset)?,
        },
    };
    validate_plan(&plan)?;
    Ok(plan)
}

fn encode_table(table: &TableDescriptor, out: &mut Vec<u8>) -> RsfResult<()> {
    write_string(&table.namespace.0, out)?;
    write_string(&table.name.0, out)?;
    let column_len = u32::try_from(table.columns.len())
        .map_err(|_| invalid("migration table has too many columns"))?;
    out.extend_from_slice(&column_len.to_le_bytes());
    for column in &table.columns {
        write_string(&column.name, out)?;
        out.push(column_type_to_u8(&column.ty));
        out.push(u8::from(column.nullable));
    }
    let pk_len = u32::try_from(table.primary_key.len())
        .map_err(|_| invalid("migration table has too many primary-key columns"))?;
    out.extend_from_slice(&pk_len.to_le_bytes());
    for key in &table.primary_key {
        write_string(key, out)?;
    }
    Ok(())
}

fn decode_table(bytes: &[u8], offset: &mut usize) -> RsfResult<TableDescriptor> {
    let namespace = NamespaceName(read_string(bytes, offset)?);
    let name = TableName(read_string(bytes, offset)?);
    let column_len = read_u32(bytes, offset)?;
    let column_len =
        usize::try_from(column_len).map_err(|_| corrupt("migration column count is too large"))?;
    let mut columns = Vec::with_capacity(column_len);
    for _ in 0..column_len {
        columns.push(ColumnDescriptor {
            name: read_string(bytes, offset)?,
            ty: column_type_from_u8(read_u8(bytes, offset)?)?,
            nullable: read_bool(bytes, offset)?,
        });
    }
    let pk_len = read_u32(bytes, offset)?;
    let pk_len =
        usize::try_from(pk_len).map_err(|_| corrupt("migration primary-key count is too large"))?;
    let mut primary_key = Vec::with_capacity(pk_len);
    for _ in 0..pk_len {
        primary_key.push(read_string(bytes, offset)?);
    }
    Ok(TableDescriptor {
        namespace,
        name,
        columns,
        primary_key,
    })
}

fn encode_step(step: &MigrationStep, out: &mut Vec<u8>) -> RsfResult<()> {
    match step {
        MigrationStep::ValidateCurrentSchema => out.push(1),
        MigrationStep::PublishDescriptor { version } => {
            out.push(2);
            write_u64(version.0.0, out);
        }
        MigrationStep::BackfillColumn { name, batch_rows } => {
            out.push(3);
            write_string(name, out)?;
            out.extend_from_slice(&batch_rows.to_le_bytes());
        }
        MigrationStep::BuildIndex { name, columns } => {
            out.push(4);
            write_string(name, out)?;
            let column_len = u32::try_from(columns.len())
                .map_err(|_| invalid("migration index has too many columns"))?;
            out.extend_from_slice(&column_len.to_le_bytes());
            for column in columns {
                write_string(column, out)?;
            }
        }
        MigrationStep::SwitchReads { version } => {
            out.push(5);
            write_u64(version.0.0, out);
        }
        MigrationStep::RetireColumn { name } => {
            out.push(6);
            write_string(name, out)?;
        }
    }
    Ok(())
}

fn decode_step(bytes: &[u8], offset: &mut usize) -> RsfResult<MigrationStep> {
    match read_u8(bytes, offset)? {
        1 => Ok(MigrationStep::ValidateCurrentSchema),
        2 => Ok(MigrationStep::PublishDescriptor {
            version: SchemaVersion(CommitVersion(read_u64(bytes, offset)?)),
        }),
        3 => Ok(MigrationStep::BackfillColumn {
            name: read_string(bytes, offset)?,
            batch_rows: read_u32(bytes, offset)?,
        }),
        4 => {
            let name = read_string(bytes, offset)?;
            let column_len = read_u32(bytes, offset)?;
            let column_len = usize::try_from(column_len)
                .map_err(|_| corrupt("migration index column count is too large"))?;
            let mut columns = Vec::with_capacity(column_len);
            for _ in 0..column_len {
                columns.push(read_string(bytes, offset)?);
            }
            Ok(MigrationStep::BuildIndex { name, columns })
        }
        5 => Ok(MigrationStep::SwitchReads {
            version: SchemaVersion(CommitVersion(read_u64(bytes, offset)?)),
        }),
        6 => Ok(MigrationStep::RetireColumn {
            name: read_string(bytes, offset)?,
        }),
        other => Err(corrupt(format!(
            "migration step kind {other} is not supported"
        ))),
    }
}

fn column_type_to_u8(ty: &ColumnType) -> u8 {
    match ty {
        ColumnType::Int64 => 1,
        ColumnType::Bytes => 2,
        ColumnType::Text => 3,
        ColumnType::Bool => 4,
    }
}

fn column_type_from_u8(value: u8) -> RsfResult<ColumnType> {
    match value {
        1 => Ok(ColumnType::Int64),
        2 => Ok(ColumnType::Bytes),
        3 => Ok(ColumnType::Text),
        4 => Ok(ColumnType::Bool),
        _ => Err(corrupt(format!("unknown migration column type id {value}"))),
    }
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("migration string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("migration string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("migration WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("migration WAL payload has invalid UTF-8"))
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(corrupt(format!("migration bool value {other} is invalid"))),
    }
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("migration WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("migration WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("migration WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

fn validate_plan(plan: &MigrationPlan) -> RsfResult<()> {
    if plan.id.is_empty() {
        return Err(invalid("migration id must be non-empty"));
    }
    if plan.steps.is_empty() {
        return Err(invalid("migration plan must include steps"));
    }
    if !matches!(
        plan.steps.first(),
        Some(MigrationStep::ValidateCurrentSchema)
    ) {
        return Err(invalid(
            "migration plan must validate the current schema before durable changes",
        ));
    }
    if !plan
        .steps
        .iter()
        .any(|step| matches!(step, MigrationStep::SwitchReads { .. }))
    {
        return Err(invalid(
            "migration plan must include a read visibility switch",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

#[cfg(test)]
mod tests {
    use rsf_core::{
        CommitVersion,
        catalog::{ColumnDescriptor, ColumnType, NamespaceName, TableDescriptor, TableName},
    };
    use rsf_schema::{
        MigrationPolicy, SchemaChange, SchemaMigration, SchemaPlanner, SchemaVersion,
    };

    use super::*;

    #[test]
    fn migration_advances_through_barrier_backfill_index_and_switch() {
        let plan = plan();
        let mut lifecycle = MigrationLifecycle::default();
        assert!(matches!(
            lifecycle.begin(plan).expect("begin migration"),
            MigrationDecision::Began(_)
        ));
        assert!(matches!(
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("validate"),
            MigrationDecision::SchemaValidated(_)
        ));
        assert!(matches!(
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("publish"),
            MigrationDecision::DescriptorPublished(_)
        ));
        assert!(matches!(
            lifecycle
                .advance("users_add_status", 250, 0)
                .expect("backfill"),
            MigrationDecision::BackfillBatch(_)
        ));
        assert!(matches!(
            lifecycle
                .advance("users_add_status", 0, 250)
                .expect("index"),
            MigrationDecision::IndexCatchUp(_)
        ));
        let completed = lifecycle
            .advance("users_add_status", 0, 0)
            .expect("switch reads");
        match completed {
            MigrationDecision::Completed(progress) => {
                assert_eq!(progress.phase, MigrationPhase::Complete);
                assert_eq!(progress.completed_steps, 5);
                assert_eq!(progress.backfilled_rows, 250);
                assert_eq!(progress.indexed_keys, 250);
                assert_eq!(progress.durable_events, 7);
            }
            other => panic!("expected completed migration, got {other:?}"),
        }
    }

    #[test]
    fn resume_reports_durable_position_after_partial_progress() {
        let plan = plan();
        let mut lifecycle = MigrationLifecycle::default();
        lifecycle.begin(plan).expect("begin migration");
        lifecycle
            .advance("users_add_status", 0, 0)
            .expect("validate");
        lifecycle
            .advance("users_add_status", 0, 0)
            .expect("publish");
        match lifecycle
            .resume("users_add_status")
            .expect("resume migration")
        {
            MigrationDecision::Resumed(progress) => {
                assert_eq!(progress.phase, MigrationPhase::DescriptorPublished);
                assert_eq!(progress.completed_steps, 2);
                assert_eq!(progress.durable_events, 3);
            }
            other => panic!("expected resumed migration, got {other:?}"),
        }
    }

    #[test]
    fn rollback_records_terminal_reason_before_visibility_switch() {
        let plan = plan();
        let mut lifecycle = MigrationLifecycle::default();
        lifecycle.begin(plan).expect("begin migration");
        lifecycle
            .advance("users_add_status", 0, 0)
            .expect("validate");
        match lifecycle
            .rollback("users_add_status", "operator abort before read switch")
            .expect("rollback migration")
        {
            MigrationDecision::RolledBack(progress) => {
                assert_eq!(progress.phase, MigrationPhase::RolledBack);
                assert_eq!(
                    progress.rollback_reason.as_deref(),
                    Some("operator abort before read switch")
                );
            }
            other => panic!("expected rolled-back migration, got {other:?}"),
        }
        assert!(matches!(
            lifecycle
                .advance("users_add_status", 1, 1)
                .expect("advance"),
            MigrationDecision::Rejected { .. }
        ));
    }

    #[test]
    fn durable_migration_recovers_partial_progress() {
        let path = test_path("durable_migration_recovers_partial_progress");

        {
            let mut lifecycle =
                DurableMigrationLifecycle::open(&path, Epoch(8)).expect("open lifecycle");
            lifecycle.begin(plan()).expect("begin migration");
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("validate");
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("publish");
        }

        let mut recovered =
            DurableMigrationLifecycle::open(&path, Epoch(8)).expect("recover lifecycle");
        match recovered
            .resume("users_add_status")
            .expect("resume migration")
        {
            MigrationDecision::Resumed(progress) => {
                assert_eq!(progress.phase, MigrationPhase::DescriptorPublished);
                assert_eq!(progress.completed_steps, 2);
                assert_eq!(progress.durable_events, 3);
            }
            other => panic!("expected resumed migration, got {other:?}"),
        }

        assert!(matches!(
            recovered
                .advance("users_add_status", 250, 0)
                .expect("backfill"),
            MigrationDecision::BackfillBatch(_)
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_migration_recovers_rollback_terminality() {
        let path = test_path("durable_migration_recovers_rollback_terminality");

        {
            let mut lifecycle =
                DurableMigrationLifecycle::open(&path, Epoch(9)).expect("open lifecycle");
            lifecycle.begin(plan()).expect("begin migration");
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("validate");
            lifecycle
                .rollback("users_add_status", "operator abort before read switch")
                .expect("rollback");
        }

        let mut recovered =
            DurableMigrationLifecycle::open(&path, Epoch(9)).expect("recover lifecycle");
        match recovered
            .resume("users_add_status")
            .expect("resume migration")
        {
            MigrationDecision::Resumed(progress) => {
                assert_eq!(progress.phase, MigrationPhase::RolledBack);
                assert_eq!(
                    progress.rollback_reason.as_deref(),
                    Some("operator abort before read switch")
                );
            }
            other => panic!("expected resumed rolled-back migration, got {other:?}"),
        }
        assert!(matches!(
            recovered
                .advance("users_add_status", 1, 1)
                .expect("advance"),
            MigrationDecision::Rejected { .. }
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_migration_recovers_completed_state_and_rejects_rollback() {
        let path = test_path("durable_migration_recovers_completed_state_and_rejects_rollback");

        {
            let mut lifecycle =
                DurableMigrationLifecycle::open(&path, Epoch(10)).expect("open lifecycle");
            lifecycle.begin(plan()).expect("begin migration");
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("validate");
            lifecycle
                .advance("users_add_status", 0, 0)
                .expect("publish");
            lifecycle
                .advance("users_add_status", 250, 0)
                .expect("backfill");
            lifecycle
                .advance("users_add_status", 0, 250)
                .expect("index");
            assert!(matches!(
                lifecycle
                    .advance("users_add_status", 0, 0)
                    .expect("switch reads"),
                MigrationDecision::Completed(_)
            ));
        }

        let mut recovered =
            DurableMigrationLifecycle::open(&path, Epoch(10)).expect("recover lifecycle");
        match recovered
            .resume("users_add_status")
            .expect("resume migration")
        {
            MigrationDecision::Resumed(progress) => {
                assert_eq!(progress.phase, MigrationPhase::Complete);
                assert_eq!(progress.durable_events, 7);
            }
            other => panic!("expected resumed completed migration, got {other:?}"),
        }
        assert!(matches!(
            recovered
                .rollback("users_add_status", "too late")
                .expect("rollback"),
            MigrationDecision::Rejected { .. }
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn migration_wal_decode_rejects_trailing_bytes() {
        let mut payload =
            encode_migration_wal_payload(&MigrationWalRecord::Began(plan())).expect("encode");
        payload.push(99);

        let err = decode_migration_wal_payload(&payload).expect_err("trailing bytes reject");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn plan() -> MigrationPlan {
        SchemaPlanner
            .plan(
                SchemaMigration {
                    id: "users_add_status".to_string(),
                    table: users(),
                    from: SchemaVersion(CommitVersion(10)),
                    changes: vec![
                        SchemaChange::AddColumn(ColumnDescriptor {
                            name: "status".to_string(),
                            ty: ColumnType::Text,
                            nullable: false,
                        }),
                        SchemaChange::AddIndex {
                            name: "users_status".to_string(),
                            columns: vec!["status".to_string()],
                        },
                    ],
                },
                SchemaVersion(CommitVersion(11)),
                &MigrationPolicy::default(),
            )
            .expect("migration plan")
    }

    fn users() -> TableDescriptor {
        TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("users".to_string()),
            columns: vec![ColumnDescriptor {
                name: "id".to_string(),
                ty: ColumnType::Int64,
                nullable: false,
            }],
            primary_key: vec!["id".to_string()],
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-migration-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
