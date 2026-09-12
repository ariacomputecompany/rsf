use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Digest, DigestAlgorithm, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError,
    RsfResult, storage::WalRecordKind,
};
use rsf_range::{CompactionPlan, RangeId, SstableManifest};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::CompactionOutput;

const COMPACTION_PG: PlacementGroupId = PlacementGroupId(56);
const COMPACTION_WAL_MAGIC: &[u8; 7] = b"RSFCMP1";
const COMPACTION_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CompactionOperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionOperationState {
    Planned,
    Started,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionOperation {
    pub id: CompactionOperationId,
    pub plan: CompactionPlan,
    pub state: CompactionOperationState,
    pub output: Option<CompactionOutput>,
    pub failure: Option<String>,
}

#[derive(Debug)]
pub struct DurableCompactionLedger {
    operations: BTreeMap<CompactionOperationId, CompactionOperation>,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableCompactionLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(COMPACTION_PG),
        })?;
        let mut operations = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Compaction)
        {
            apply_compaction_record(
                &mut operations,
                decode_compaction_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        if let Some(last) = operations.keys().next_back() {
            next_id = last.0.saturating_add(1);
        }

        Ok(Self {
            operations,
            store,
            epoch,
            next_id,
            next_index,
        })
    }

    pub fn record_plan(&mut self, plan: CompactionPlan) -> RsfResult<CompactionOperationId> {
        validate_plan(&plan)?;
        let id = CompactionOperationId(self.next_id);
        self.append_record(&CompactionWalRecord::Planned {
            id,
            plan: plan.clone(),
        })?;
        self.operations.insert(
            id,
            CompactionOperation {
                id,
                plan,
                state: CompactionOperationState::Planned,
                output: None,
                failure: None,
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    pub fn mark_started(&mut self, id: CompactionOperationId) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != CompactionOperationState::Planned {
            return Err(invalid(format!(
                "compaction operation {} cannot start from {:?}",
                id.0, operation.state
            )));
        }
        operation.state = CompactionOperationState::Started;
        self.append_record(&CompactionWalRecord::Started { id })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_completed(
        &mut self,
        id: CompactionOperationId,
        output: CompactionOutput,
    ) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != CompactionOperationState::Started {
            return Err(invalid(format!(
                "compaction operation {} cannot complete from {:?}",
                id.0, operation.state
            )));
        }
        validate_output(&operation.plan, &output)?;
        operation.state = CompactionOperationState::Completed;
        operation.output = Some(output.clone());
        operation.failure = None;
        self.append_record(&CompactionWalRecord::Completed { id, output })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        id: CompactionOperationId,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let reason = reason.into();
        validate_failure(&reason)?;
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state == CompactionOperationState::Completed {
            return Err(invalid(format!(
                "compaction operation {} cannot fail after completion",
                id.0
            )));
        }
        operation.state = CompactionOperationState::Failed;
        operation.output = None;
        operation.failure = Some(reason.clone());
        self.append_record(&CompactionWalRecord::Failed { id, reason })?;
        self.operations = operations;
        Ok(())
    }

    pub fn operation(&self, id: CompactionOperationId) -> Option<&CompactionOperation> {
        self.operations.get(&id)
    }

    pub fn operations(&self) -> Vec<CompactionOperation> {
        self.operations.values().cloned().collect()
    }

    pub fn pending(&self) -> Vec<CompactionOperation> {
        self.operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.state,
                    CompactionOperationState::Planned | CompactionOperationState::Started
                )
            })
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &CompactionWalRecord) -> RsfResult<()> {
        let payload = encode_compaction_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(COMPACTION_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: COMPACTION_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Compaction,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CompactionWalRecord {
    Planned {
        id: CompactionOperationId,
        plan: CompactionPlan,
    },
    Started {
        id: CompactionOperationId,
    },
    Completed {
        id: CompactionOperationId,
        output: CompactionOutput,
    },
    Failed {
        id: CompactionOperationId,
        reason: String,
    },
}

fn apply_compaction_record(
    operations: &mut BTreeMap<CompactionOperationId, CompactionOperation>,
    record: CompactionWalRecord,
) -> RsfResult<()> {
    match record {
        CompactionWalRecord::Planned { id, plan } => {
            if operations.contains_key(&id) {
                return Err(corrupt(format!(
                    "duplicate compaction operation id {} in WAL",
                    id.0
                )));
            }
            validate_plan(&plan)?;
            operations.insert(
                id,
                CompactionOperation {
                    id,
                    plan,
                    state: CompactionOperationState::Planned,
                    output: None,
                    failure: None,
                },
            );
        }
        CompactionWalRecord::Started { id } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != CompactionOperationState::Planned {
                return Err(corrupt(format!(
                    "compaction operation {} cannot replay start from {:?}",
                    id.0, operation.state
                )));
            }
            operation.state = CompactionOperationState::Started;
        }
        CompactionWalRecord::Completed { id, output } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != CompactionOperationState::Started {
                return Err(corrupt(format!(
                    "compaction operation {} cannot replay completion from {:?}",
                    id.0, operation.state
                )));
            }
            validate_output(&operation.plan, &output)?;
            operation.state = CompactionOperationState::Completed;
            operation.output = Some(output);
            operation.failure = None;
        }
        CompactionWalRecord::Failed { id, reason } => {
            validate_failure(&reason)?;
            let operation = operation_mut(operations, id)?;
            if operation.state == CompactionOperationState::Completed {
                return Err(corrupt(format!(
                    "compaction operation {} cannot replay failure after completion",
                    id.0
                )));
            }
            operation.state = CompactionOperationState::Failed;
            operation.output = None;
            operation.failure = Some(reason);
        }
    }
    Ok(())
}

fn validate_plan(plan: &CompactionPlan) -> RsfResult<()> {
    if plan.inputs.len() < 2 {
        return Err(invalid(
            "durable compaction plan requires at least two inputs",
        ));
    }
    if plan.estimated_bytes == 0 {
        return Err(invalid(
            "durable compaction plan estimated bytes must be nonzero",
        ));
    }
    let mut total = 0_u64;
    for input in &plan.inputs {
        validate_manifest(input)?;
        if input.range != plan.range {
            return Err(invalid("compaction input range must match plan range"));
        }
        total = total
            .checked_add(input.bytes)
            .ok_or_else(|| invalid("compaction input bytes overflow"))?;
    }
    if total != plan.estimated_bytes {
        return Err(invalid(
            "compaction plan estimated bytes must equal input bytes",
        ));
    }
    Ok(())
}

fn validate_output(plan: &CompactionPlan, output: &CompactionOutput) -> RsfResult<()> {
    validate_plan(plan)?;
    validate_manifest(&output.manifest)?;
    if output.manifest.range != plan.range {
        return Err(invalid("compaction output range must match plan range"));
    }
    if output.manifest.level != plan.output_level {
        return Err(invalid(
            "compaction output level must match plan output level",
        ));
    }
    if output.input_bytes != plan.estimated_bytes {
        return Err(invalid("compaction output input bytes must match plan"));
    }
    if output.output_bytes != output.manifest.bytes {
        return Err(invalid(
            "compaction output bytes must match output manifest",
        ));
    }
    if output.reclaimed_bytes != output.input_bytes.saturating_sub(output.output_bytes) {
        return Err(invalid(
            "compaction reclaimed bytes must match input minus output",
        ));
    }
    if output.live_entries.is_empty() {
        return Err(invalid("compaction output must retain live entries"));
    }
    if output.manifest.first_key
        != output
            .live_entries
            .first()
            .expect("live entries checked")
            .key
    {
        return Err(invalid(
            "compaction output first key must match live entries",
        ));
    }
    if output.manifest.last_key
        != output
            .live_entries
            .last()
            .expect("live entries checked")
            .key
    {
        return Err(invalid(
            "compaction output last key must match live entries",
        ));
    }
    for entry in &output.live_entries {
        if entry.value.is_none() {
            return Err(invalid(
                "compaction output live entries cannot retain tombstones",
            ));
        }
        if entry.key < output.manifest.first_key || entry.key > output.manifest.last_key {
            return Err(invalid(
                "compaction live entry outside output manifest bounds",
            ));
        }
    }
    output.manifest.checksum.validate()?;
    Ok(())
}

fn validate_manifest(manifest: &SstableManifest) -> RsfResult<()> {
    if manifest.first_key > manifest.last_key {
        return Err(invalid(
            "SSTable manifest first key must not exceed last key",
        ));
    }
    if manifest.bytes == 0 {
        return Err(invalid("SSTable manifest bytes must be nonzero"));
    }
    manifest.checksum.validate()?;
    Ok(())
}

fn validate_failure(reason: &str) -> RsfResult<()> {
    if reason.trim().is_empty() {
        return Err(invalid("compaction failure reason must not be empty"));
    }
    Ok(())
}

fn operation_mut(
    operations: &mut BTreeMap<CompactionOperationId, CompactionOperation>,
    id: CompactionOperationId,
) -> RsfResult<&mut CompactionOperation> {
    operations
        .get_mut(&id)
        .ok_or_else(|| corrupt(format!("unknown compaction operation id {}", id.0)))
}

fn encode_compaction_wal_payload(record: &CompactionWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(COMPACTION_WAL_MAGIC);
    out.push(COMPACTION_WAL_VERSION);
    match record {
        CompactionWalRecord::Planned { id, plan } => {
            out.push(1);
            write_u64(&mut out, id.0);
            write_plan(&mut out, plan)?;
        }
        CompactionWalRecord::Started { id } => {
            out.push(2);
            write_u64(&mut out, id.0);
        }
        CompactionWalRecord::Completed { id, output } => {
            out.push(3);
            write_u64(&mut out, id.0);
            write_output(&mut out, output)?;
        }
        CompactionWalRecord::Failed { id, reason } => {
            out.push(4);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_compaction_wal_payload(bytes: &[u8]) -> RsfResult<CompactionWalRecord> {
    let mut offset = 0;
    take_magic(bytes, &mut offset)?;
    let version = read_u8(bytes, &mut offset)?;
    if version != COMPACTION_WAL_VERSION {
        return Err(corrupt(format!(
            "unsupported compaction WAL version {version}"
        )));
    }
    let record = match read_u8(bytes, &mut offset)? {
        1 => CompactionWalRecord::Planned {
            id: CompactionOperationId(read_u64(bytes, &mut offset)?),
            plan: read_plan(bytes, &mut offset)?,
        },
        2 => CompactionWalRecord::Started {
            id: CompactionOperationId(read_u64(bytes, &mut offset)?),
        },
        3 => CompactionWalRecord::Completed {
            id: CompactionOperationId(read_u64(bytes, &mut offset)?),
            output: read_output(bytes, &mut offset)?,
        },
        4 => CompactionWalRecord::Failed {
            id: CompactionOperationId(read_u64(bytes, &mut offset)?),
            reason: read_string(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "unknown compaction WAL record kind {other}"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("compaction WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn write_plan(out: &mut Vec<u8>, plan: &CompactionPlan) -> RsfResult<()> {
    write_u64(out, plan.range.0);
    write_len(out, plan.inputs.len())?;
    for input in &plan.inputs {
        write_manifest(out, input)?;
    }
    out.push(plan.output_level);
    write_u64(out, plan.estimated_bytes);
    Ok(())
}

fn read_plan(bytes: &[u8], offset: &mut usize) -> RsfResult<CompactionPlan> {
    let range = RangeId(read_u64(bytes, offset)?);
    let count = read_len(bytes, offset)?;
    let mut inputs = Vec::with_capacity(count);
    for _ in 0..count {
        inputs.push(read_manifest(bytes, offset)?);
    }
    Ok(CompactionPlan {
        range,
        inputs,
        output_level: read_u8(bytes, offset)?,
        estimated_bytes: read_u64(bytes, offset)?,
    })
}

fn write_output(out: &mut Vec<u8>, output: &CompactionOutput) -> RsfResult<()> {
    write_manifest(out, &output.manifest)?;
    write_len(out, output.live_entries.len())?;
    for entry in &output.live_entries {
        write_bytes(out, &entry.key)?;
        write_u64(out, entry.version.0);
        match &entry.value {
            Some(value) => {
                out.push(1);
                write_bytes(out, value)?;
            }
            None => out.push(0),
        }
    }
    write_u64(out, output.input_bytes);
    write_u64(out, output.output_bytes);
    write_u64(out, output.reclaimed_bytes);
    write_u64(out, output.tombstones);
    Ok(())
}

fn read_output(bytes: &[u8], offset: &mut usize) -> RsfResult<CompactionOutput> {
    let manifest = read_manifest(bytes, offset)?;
    let count = read_len(bytes, offset)?;
    let mut live_entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = read_bytes(bytes, offset)?;
        let version = CommitVersion(read_u64(bytes, offset)?);
        let value = match read_u8(bytes, offset)? {
            0 => None,
            1 => Some(read_bytes(bytes, offset)?),
            other => return Err(corrupt(format!("invalid compaction value flag {other}"))),
        };
        live_entries.push(crate::CompactionEntry {
            key,
            value,
            version,
        });
    }
    Ok(CompactionOutput {
        manifest,
        live_entries,
        input_bytes: read_u64(bytes, offset)?,
        output_bytes: read_u64(bytes, offset)?,
        reclaimed_bytes: read_u64(bytes, offset)?,
        tombstones: read_u64(bytes, offset)?,
    })
}

fn write_manifest(out: &mut Vec<u8>, manifest: &SstableManifest) -> RsfResult<()> {
    write_u64(out, manifest.range.0);
    out.push(manifest.level);
    write_bytes(out, &manifest.first_key)?;
    write_bytes(out, &manifest.last_key)?;
    write_u64(out, manifest.version.0);
    write_u64(out, manifest.bytes);
    write_digest(out, &manifest.checksum);
    Ok(())
}

fn read_manifest(bytes: &[u8], offset: &mut usize) -> RsfResult<SstableManifest> {
    Ok(SstableManifest {
        range: RangeId(read_u64(bytes, offset)?),
        level: read_u8(bytes, offset)?,
        first_key: read_bytes(bytes, offset)?,
        last_key: read_bytes(bytes, offset)?,
        version: CommitVersion(read_u64(bytes, offset)?),
        bytes: read_u64(bytes, offset)?,
        checksum: read_digest(bytes, offset)?,
    })
}

fn write_digest(out: &mut Vec<u8>, digest: &Digest) {
    out.push(match digest.algorithm() {
        DigestAlgorithm::Blake3 => 1,
        DigestAlgorithm::Sha256 => 2,
    });
    out.extend_from_slice(digest.as_bytes());
}

fn read_digest(bytes: &[u8], offset: &mut usize) -> RsfResult<Digest> {
    let algorithm = match read_u8(bytes, offset)? {
        1 => DigestAlgorithm::Blake3,
        2 => DigestAlgorithm::Sha256,
        other => return Err(corrupt(format!("unknown digest algorithm {other}"))),
    };
    let raw = take(bytes, offset, 32)?;
    let mut digest = [0; 32];
    digest.copy_from_slice(raw);
    Digest::new(algorithm, digest).map_err(|err| corrupt(err.message))
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    write_bytes(out, value.as_bytes())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    String::from_utf8(read_bytes(bytes, offset)?)
        .map_err(|_| corrupt("compaction WAL string is not utf8"))
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> RsfResult<()> {
    write_len(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<Vec<u8>> {
    let len = read_len(bytes, offset)?;
    Ok(take(bytes, offset, len)?.to_vec())
}

fn write_len(out: &mut Vec<u8>, len: usize) -> RsfResult<()> {
    let len = u32::try_from(len).map_err(|_| invalid("compaction WAL length exceeds u32"))?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    Ok(read_u32(bytes, offset)? as usize)
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    let mut buf = [0; 8];
    buf.copy_from_slice(raw);
    Ok(u64::from_le_bytes(buf))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = take(bytes, offset, 4)?;
    let mut buf = [0; 4];
    buf.copy_from_slice(raw);
    Ok(u32::from_le_bytes(buf))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(*take(bytes, offset, 1)?
        .first()
        .expect("one requested byte must exist"))
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, COMPACTION_WAL_MAGIC.len())?;
    if magic != COMPACTION_WAL_MAGIC {
        return Err(corrupt("compaction WAL payload has invalid magic"));
    }
    Ok(())
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("compaction WAL offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("compaction WAL payload is truncated"));
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests;
