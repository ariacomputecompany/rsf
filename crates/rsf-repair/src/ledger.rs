use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{
    RepairAction, ReplicaCopyResult, ReplicaQuarantineResult, UnderReplicationRepairResult,
};

const REPAIR_PG: PlacementGroupId = PlacementGroupId(52);
const REPAIR_WAL_MAGIC: &[u8; 7] = b"RSFRPR1";
const REPAIR_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RepairOperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepairOperationState {
    Planned,
    Started,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepairOperation {
    pub id: RepairOperationId,
    pub action: RepairAction,
    pub state: RepairOperationState,
    pub completion: Option<RepairCompletion>,
    pub failure: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepairCompletion {
    Copy(ReplicaCopyResult),
    Quarantine(ReplicaQuarantineResult),
    UnderReplication(UnderReplicationRepairResult),
    RecheckDigest { pg: PlacementGroupId, node: NodeId },
    DemoteSlowReplica { pg: PlacementGroupId, node: NodeId },
}

#[derive(Debug)]
pub struct DurableRepairLedger {
    operations: BTreeMap<RepairOperationId, RepairOperation>,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableRepairLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(REPAIR_PG),
        })?;
        let mut operations = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Repair)
        {
            apply_repair_record(&mut operations, decode_repair_wal_payload(&record.payload)?)?;
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

    pub fn record_plan(&mut self, action: RepairAction) -> RsfResult<RepairOperationId> {
        validate_action(&action)?;
        let id = RepairOperationId(self.next_id);
        self.append_record(&RepairWalRecord::Planned {
            id,
            action: action.clone(),
        })?;
        self.operations.insert(
            id,
            RepairOperation {
                id,
                action,
                state: RepairOperationState::Planned,
                completion: None,
                failure: None,
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    pub fn mark_started(&mut self, id: RepairOperationId) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RepairOperationState::Planned {
            return Err(invalid(format!(
                "repair operation {} cannot start from {:?}",
                id.0, operation.state
            )));
        }
        operation.state = RepairOperationState::Started;
        self.append_record(&RepairWalRecord::Started { id })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_completed(
        &mut self,
        id: RepairOperationId,
        completion: RepairCompletion,
    ) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RepairOperationState::Started {
            return Err(invalid(format!(
                "repair operation {} cannot complete from {:?}",
                id.0, operation.state
            )));
        }
        validate_completion(&operation.action, &completion)?;
        operation.state = RepairOperationState::Completed;
        operation.completion = Some(completion.clone());
        operation.failure = None;
        self.append_record(&RepairWalRecord::Completed { id, completion })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        id: RepairOperationId,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let reason = reason.into();
        validate_failure(&reason)?;
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state == RepairOperationState::Completed {
            return Err(invalid(format!(
                "repair operation {} cannot fail after completion",
                id.0
            )));
        }
        operation.state = RepairOperationState::Failed;
        operation.failure = Some(reason.clone());
        operation.completion = None;
        self.append_record(&RepairWalRecord::Failed { id, reason })?;
        self.operations = operations;
        Ok(())
    }

    pub fn operation(&self, id: RepairOperationId) -> Option<&RepairOperation> {
        self.operations.get(&id)
    }

    pub fn operations(&self) -> Vec<RepairOperation> {
        self.operations.values().cloned().collect()
    }

    pub fn pending(&self) -> Vec<RepairOperation> {
        self.operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.state,
                    RepairOperationState::Planned | RepairOperationState::Started
                )
            })
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &RepairWalRecord) -> RsfResult<()> {
        let payload = encode_repair_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(REPAIR_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: REPAIR_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Repair,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RepairWalRecord {
    Planned {
        id: RepairOperationId,
        action: RepairAction,
    },
    Started {
        id: RepairOperationId,
    },
    Completed {
        id: RepairOperationId,
        completion: RepairCompletion,
    },
    Failed {
        id: RepairOperationId,
        reason: String,
    },
}

fn apply_repair_record(
    operations: &mut BTreeMap<RepairOperationId, RepairOperation>,
    record: RepairWalRecord,
) -> RsfResult<()> {
    match record {
        RepairWalRecord::Planned { id, action } => {
            if operations.contains_key(&id) {
                return Err(corrupt(format!(
                    "duplicate repair operation id {} in WAL",
                    id.0
                )));
            }
            validate_action(&action)?;
            operations.insert(
                id,
                RepairOperation {
                    id,
                    action,
                    state: RepairOperationState::Planned,
                    completion: None,
                    failure: None,
                },
            );
        }
        RepairWalRecord::Started { id } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RepairOperationState::Planned {
                return Err(corrupt(format!(
                    "repair operation {} cannot replay start from {:?}",
                    id.0, operation.state
                )));
            }
            operation.state = RepairOperationState::Started;
        }
        RepairWalRecord::Completed { id, completion } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RepairOperationState::Started {
                return Err(corrupt(format!(
                    "repair operation {} cannot replay completion from {:?}",
                    id.0, operation.state
                )));
            }
            validate_completion(&operation.action, &completion)?;
            operation.state = RepairOperationState::Completed;
            operation.completion = Some(completion);
            operation.failure = None;
        }
        RepairWalRecord::Failed { id, reason } => {
            validate_failure(&reason)?;
            let operation = operation_mut(operations, id)?;
            if operation.state == RepairOperationState::Completed {
                return Err(corrupt(format!(
                    "repair operation {} cannot replay failure after completion",
                    id.0
                )));
            }
            operation.state = RepairOperationState::Failed;
            operation.failure = Some(reason);
            operation.completion = None;
        }
    }
    Ok(())
}

fn validate_action(action: &RepairAction) -> RsfResult<()> {
    match action {
        RepairAction::CopyReplica { pg, from, to } => {
            validate_pg(*pg)?;
            if from == to {
                return Err(invalid("repair copy source and target must differ"));
            }
        }
        RepairAction::QuarantineReplica { pg, node }
        | RepairAction::RecheckDigest { pg, node }
        | RepairAction::DemoteSlowReplica { pg, node } => {
            validate_pg(*pg)?;
            validate_node(*node)?;
        }
        RepairAction::RestoreReplication {
            pg,
            healthy,
            expected,
            deficit,
        } => {
            validate_pg(*pg)?;
            if *expected == 0 || *healthy >= *expected || *deficit == 0 {
                return Err(invalid(
                    "restore-replication repair action must carry a real deficit",
                ));
            }
            if expected.saturating_sub(*healthy) != *deficit {
                return Err(invalid(
                    "restore-replication deficit must match healthy/expected counts",
                ));
            }
        }
    }
    Ok(())
}

fn validate_completion(action: &RepairAction, completion: &RepairCompletion) -> RsfResult<()> {
    match (action, completion) {
        (RepairAction::CopyReplica { pg, from, to }, RepairCompletion::Copy(result)) => {
            if result.pg != *pg || result.from != *from || result.to != *to {
                return Err(invalid("copy repair completion does not match action"));
            }
            if result.bytes == 0 {
                return Err(invalid("copy repair completion must carry copied bytes"));
            }
            result.checksum.validate()?;
        }
        (RepairAction::QuarantineReplica { pg, node }, RepairCompletion::Quarantine(result)) => {
            if result.pg != *pg || result.node != *node {
                return Err(invalid("quarantine completion does not match action"));
            }
            result.checksum.validate()?;
        }
        (
            RepairAction::RestoreReplication {
                pg,
                healthy,
                expected,
                deficit,
            },
            RepairCompletion::UnderReplication(result),
        ) => {
            if result.pg != *pg
                || result.healthy_before != *healthy
                || result.expected != *expected
                || result.deficit != *deficit
            {
                return Err(invalid(
                    "under-replication completion does not match action",
                ));
            }
            if result.projected_healthy_replicas < result.expected {
                return Err(invalid(
                    "under-replication completion does not restore target replicas",
                ));
            }
        }
        (
            RepairAction::RecheckDigest { pg, node },
            RepairCompletion::RecheckDigest {
                pg: completed_pg,
                node: completed_node,
            },
        )
        | (
            RepairAction::DemoteSlowReplica { pg, node },
            RepairCompletion::DemoteSlowReplica {
                pg: completed_pg,
                node: completed_node,
            },
        ) => {
            if pg != completed_pg || node != completed_node {
                return Err(invalid("repair completion does not match action"));
            }
        }
        _ => {
            return Err(invalid(
                "repair completion kind does not match planned action",
            ));
        }
    }
    Ok(())
}

fn operation_mut(
    operations: &mut BTreeMap<RepairOperationId, RepairOperation>,
    id: RepairOperationId,
) -> RsfResult<&mut RepairOperation> {
    operations
        .get_mut(&id)
        .ok_or_else(|| invalid(format!("unknown repair operation {}", id.0)))
}

fn validate_pg(pg: PlacementGroupId) -> RsfResult<()> {
    if pg.0 == 0 {
        return Err(invalid("repair operation pg must be non-zero"));
    }
    Ok(())
}

fn validate_node(node: NodeId) -> RsfResult<()> {
    if node.0 == 0 {
        return Err(invalid("repair operation node must be non-zero"));
    }
    Ok(())
}

fn validate_failure(reason: &str) -> RsfResult<()> {
    if reason.trim().is_empty() {
        return Err(invalid("repair failure reason must not be empty"));
    }
    Ok(())
}

fn encode_repair_wal_payload(record: &RepairWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(REPAIR_WAL_MAGIC);
    out.push(REPAIR_WAL_VERSION);
    match record {
        RepairWalRecord::Planned { id, action } => {
            validate_action(action)?;
            out.push(1);
            write_u64(&mut out, id.0);
            encode_action(&mut out, action);
        }
        RepairWalRecord::Started { id } => {
            out.push(2);
            write_u64(&mut out, id.0);
        }
        RepairWalRecord::Completed { id, completion } => {
            out.push(3);
            write_u64(&mut out, id.0);
            encode_completion(&mut out, completion)?;
        }
        RepairWalRecord::Failed { id, reason } => {
            validate_failure(reason)?;
            out.push(4);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_repair_wal_payload(bytes: &[u8]) -> RsfResult<RepairWalRecord> {
    if bytes.len() < REPAIR_WAL_MAGIC.len() + 2 {
        return Err(corrupt("repair WAL payload is too short"));
    }
    if &bytes[..REPAIR_WAL_MAGIC.len()] != REPAIR_WAL_MAGIC {
        return Err(corrupt("repair WAL magic mismatch"));
    }
    let mut offset = REPAIR_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != REPAIR_WAL_VERSION {
        return Err(corrupt("unsupported repair WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let id = RepairOperationId(read_u64(bytes, &mut offset)?);
    let record = match tag {
        1 => RepairWalRecord::Planned {
            id,
            action: decode_action(bytes, &mut offset)?,
        },
        2 => RepairWalRecord::Started { id },
        3 => RepairWalRecord::Completed {
            id,
            completion: decode_completion(bytes, &mut offset)?,
        },
        4 => RepairWalRecord::Failed {
            id,
            reason: read_string(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown repair WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in repair WAL payload"));
    }
    Ok(record)
}

fn encode_action(out: &mut Vec<u8>, action: &RepairAction) {
    match *action {
        RepairAction::CopyReplica { pg, from, to } => {
            out.push(1);
            write_pg(out, pg);
            write_node(out, from);
            write_node(out, to);
        }
        RepairAction::QuarantineReplica { pg, node } => {
            out.push(2);
            write_pg(out, pg);
            write_node(out, node);
        }
        RepairAction::RecheckDigest { pg, node } => {
            out.push(3);
            write_pg(out, pg);
            write_node(out, node);
        }
        RepairAction::DemoteSlowReplica { pg, node } => {
            out.push(4);
            write_pg(out, pg);
            write_node(out, node);
        }
        RepairAction::RestoreReplication {
            pg,
            healthy,
            expected,
            deficit,
        } => {
            out.push(5);
            write_pg(out, pg);
            write_usize(out, healthy);
            write_usize(out, expected);
            write_usize(out, deficit);
        }
    }
}

fn decode_action(bytes: &[u8], offset: &mut usize) -> RsfResult<RepairAction> {
    match read_u8(bytes, offset)? {
        1 => Ok(RepairAction::CopyReplica {
            pg: read_pg(bytes, offset)?,
            from: read_node(bytes, offset)?,
            to: read_node(bytes, offset)?,
        }),
        2 => Ok(RepairAction::QuarantineReplica {
            pg: read_pg(bytes, offset)?,
            node: read_node(bytes, offset)?,
        }),
        3 => Ok(RepairAction::RecheckDigest {
            pg: read_pg(bytes, offset)?,
            node: read_node(bytes, offset)?,
        }),
        4 => Ok(RepairAction::DemoteSlowReplica {
            pg: read_pg(bytes, offset)?,
            node: read_node(bytes, offset)?,
        }),
        5 => Ok(RepairAction::RestoreReplication {
            pg: read_pg(bytes, offset)?,
            healthy: read_usize(bytes, offset)?,
            expected: read_usize(bytes, offset)?,
            deficit: read_usize(bytes, offset)?,
        }),
        _ => Err(corrupt("unknown repair action tag")),
    }
}

fn encode_completion(out: &mut Vec<u8>, completion: &RepairCompletion) -> RsfResult<()> {
    match completion {
        RepairCompletion::Copy(result) => {
            out.push(1);
            write_pg(out, result.pg);
            write_node(out, result.from);
            write_node(out, result.to);
            write_u64(out, result.bytes);
            write_digest(out, result.checksum);
            encode_replica(out, &result.restored);
        }
        RepairCompletion::Quarantine(result) => {
            out.push(2);
            write_pg(out, result.pg);
            write_node(out, result.node);
            write_u64(out, result.bytes);
            write_digest(out, result.checksum);
            encode_replica(out, &result.quarantined);
            write_string(out, &result.reason)?;
        }
        RepairCompletion::UnderReplication(result) => {
            out.push(3);
            write_pg(out, result.pg);
            write_usize(out, result.healthy_before);
            write_usize(out, result.expected);
            write_usize(out, result.deficit);
            write_usize(out, result.copy_repairs);
            write_usize(out, result.quarantine_repairs);
            write_usize(out, result.demote_repairs);
            write_usize(out, result.projected_healthy_replicas);
        }
        RepairCompletion::RecheckDigest { pg, node } => {
            out.push(4);
            write_pg(out, *pg);
            write_node(out, *node);
        }
        RepairCompletion::DemoteSlowReplica { pg, node } => {
            out.push(5);
            write_pg(out, *pg);
            write_node(out, *node);
        }
    }
    Ok(())
}

fn decode_completion(bytes: &[u8], offset: &mut usize) -> RsfResult<RepairCompletion> {
    match read_u8(bytes, offset)? {
        1 => Ok(RepairCompletion::Copy(ReplicaCopyResult {
            pg: read_pg(bytes, offset)?,
            from: read_node(bytes, offset)?,
            to: read_node(bytes, offset)?,
            bytes: read_u64(bytes, offset)?,
            checksum: read_digest(bytes, offset)?,
            restored: decode_replica(bytes, offset)?,
        })),
        2 => Ok(RepairCompletion::Quarantine(ReplicaQuarantineResult {
            pg: read_pg(bytes, offset)?,
            node: read_node(bytes, offset)?,
            bytes: read_u64(bytes, offset)?,
            checksum: read_digest(bytes, offset)?,
            quarantined: decode_replica(bytes, offset)?,
            reason: read_string(bytes, offset)?,
        })),
        3 => Ok(RepairCompletion::UnderReplication(
            UnderReplicationRepairResult {
                pg: read_pg(bytes, offset)?,
                healthy_before: read_usize(bytes, offset)?,
                expected: read_usize(bytes, offset)?,
                deficit: read_usize(bytes, offset)?,
                copy_repairs: read_usize(bytes, offset)?,
                quarantine_repairs: read_usize(bytes, offset)?,
                demote_repairs: read_usize(bytes, offset)?,
                projected_healthy_replicas: read_usize(bytes, offset)?,
            },
        )),
        4 => Ok(RepairCompletion::RecheckDigest {
            pg: read_pg(bytes, offset)?,
            node: read_node(bytes, offset)?,
        }),
        5 => Ok(RepairCompletion::DemoteSlowReplica {
            pg: read_pg(bytes, offset)?,
            node: read_node(bytes, offset)?,
        }),
        _ => Err(corrupt("unknown repair completion tag")),
    }
}

fn encode_replica(out: &mut Vec<u8>, replica: &crate::ReplicaState) {
    write_node(out, replica.node);
    out.push(u8::from(replica.present));
    out.push(u8::from(replica.digest_ok));
    write_u64(out, replica.lag_bytes);
    write_u64(out, replica.durable_bytes);
    write_digest(out, replica.checksum);
}

fn decode_replica(bytes: &[u8], offset: &mut usize) -> RsfResult<crate::ReplicaState> {
    Ok(crate::ReplicaState {
        node: read_node(bytes, offset)?,
        present: read_bool(bytes, offset)?,
        digest_ok: read_bool(bytes, offset)?,
        lag_bytes: read_u64(bytes, offset)?,
        durable_bytes: read_u64(bytes, offset)?,
        checksum: read_digest(bytes, offset)?,
    })
}

fn write_pg(out: &mut Vec<u8>, pg: PlacementGroupId) {
    write_u64(out, pg.0);
}

fn write_node(out: &mut Vec<u8>, node: NodeId) {
    write_u64(out, node.0);
}

fn write_usize(out: &mut Vec<u8>, value: usize) {
    write_u64(out, value as u64);
}

fn write_digest(out: &mut Vec<u8>, digest: rsf_core::Digest) {
    out.push(match digest.algorithm {
        rsf_core::DigestAlgorithm::Blake3 => 1,
        rsf_core::DigestAlgorithm::Sha256 => 2,
    });
    out.extend_from_slice(&digest.bytes);
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("repair string is too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_pg(bytes: &[u8], offset: &mut usize) -> RsfResult<PlacementGroupId> {
    Ok(PlacementGroupId(read_u64(bytes, offset)?))
}

fn read_node(bytes: &[u8], offset: &mut usize) -> RsfResult<NodeId> {
    Ok(NodeId(read_u64(bytes, offset)?))
}

fn read_usize(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    usize::try_from(read_u64(bytes, offset)?).map_err(|_| corrupt("usize field overflow"))
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(corrupt("invalid repair bool field")),
    }
}

fn read_digest(bytes: &[u8], offset: &mut usize) -> RsfResult<rsf_core::Digest> {
    let algorithm = match read_u8(bytes, offset)? {
        1 => rsf_core::DigestAlgorithm::Blake3,
        2 => rsf_core::DigestAlgorithm::Sha256,
        _ => return Err(corrupt("invalid repair digest algorithm")),
    };
    if bytes.len().saturating_sub(*offset) < 32 {
        return Err(corrupt("unexpected end of repair digest"));
    }
    let mut value = [0_u8; 32];
    value.copy_from_slice(&bytes[*offset..*offset + 32]);
    *offset += 32;
    rsf_core::Digest::new(algorithm, value)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(corrupt("unexpected end of repair string length"));
    }
    let len = u32::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    let len = usize::try_from(len).map_err(|_| corrupt("repair string length overflow"))?;
    if bytes.len().saturating_sub(*offset) < len {
        return Err(corrupt("unexpected end of repair string"));
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|_| corrupt("repair string is not utf-8"))?
        .to_owned();
    *offset += len;
    Ok(value)
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    if *offset >= bytes.len() {
        return Err(corrupt("unexpected end of repair WAL payload"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt("unexpected end of repair WAL payload"));
    }
    let value = u64::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
        bytes[*offset + 4],
        bytes[*offset + 5],
        bytes[*offset + 6],
        bytes[*offset + 7],
    ]);
    *offset += 8;
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests;
