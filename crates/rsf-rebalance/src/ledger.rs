use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RsfError, RsfResult,
    placement::{ActingSet, PlacementGroup, PlacementPolicy},
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{PlacementMoveResult, PlacementPolicyProof, RebalanceAction};

const REBALANCE_PG: PlacementGroupId = PlacementGroupId(53);
const REBALANCE_WAL_MAGIC: &[u8; 7] = b"RSFRBL1";
const REBALANCE_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RebalanceOperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebalanceOperationState {
    Planned,
    Started,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceOperation {
    pub id: RebalanceOperationId,
    pub action: RebalanceAction,
    pub proof: PlacementPolicyProof,
    pub state: RebalanceOperationState,
    pub completion: Option<RebalanceCompletion>,
    pub failure: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebalanceCompletion {
    Move(PlacementMoveResult),
}

#[derive(Debug)]
pub struct DurableRebalanceLedger {
    operations: BTreeMap<RebalanceOperationId, RebalanceOperation>,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableRebalanceLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(REBALANCE_PG),
        })?;
        let mut operations = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Rebalance)
        {
            apply_rebalance_record(
                &mut operations,
                decode_rebalance_wal_payload(&record.payload)?,
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

    pub fn record_plan(
        &mut self,
        action: RebalanceAction,
        proof: PlacementPolicyProof,
    ) -> RsfResult<RebalanceOperationId> {
        validate_action_and_proof(&action, &proof)?;
        let id = RebalanceOperationId(self.next_id);
        self.append_record(&RebalanceWalRecord::Planned {
            id,
            action: action.clone(),
            proof: proof.clone(),
        })?;
        self.operations.insert(
            id,
            RebalanceOperation {
                id,
                action,
                proof,
                state: RebalanceOperationState::Planned,
                completion: None,
                failure: None,
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    pub fn mark_started(&mut self, id: RebalanceOperationId) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RebalanceOperationState::Planned {
            return Err(invalid(format!(
                "rebalance operation {} cannot start from {:?}",
                id.0, operation.state
            )));
        }
        operation.state = RebalanceOperationState::Started;
        self.append_record(&RebalanceWalRecord::Started { id })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_completed(
        &mut self,
        id: RebalanceOperationId,
        completion: RebalanceCompletion,
    ) -> RsfResult<()> {
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state != RebalanceOperationState::Started {
            return Err(invalid(format!(
                "rebalance operation {} cannot complete from {:?}",
                id.0, operation.state
            )));
        }
        validate_completion(&operation.action, &operation.proof, &completion)?;
        operation.state = RebalanceOperationState::Completed;
        operation.completion = Some(completion.clone());
        operation.failure = None;
        self.append_record(&RebalanceWalRecord::Completed { id, completion })?;
        self.operations = operations;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        id: RebalanceOperationId,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let reason = reason.into();
        validate_failure(&reason)?;
        let mut operations = self.operations.clone();
        let operation = operation_mut(&mut operations, id)?;
        if operation.state == RebalanceOperationState::Completed {
            return Err(invalid(format!(
                "rebalance operation {} cannot fail after completion",
                id.0
            )));
        }
        operation.state = RebalanceOperationState::Failed;
        operation.failure = Some(reason.clone());
        operation.completion = None;
        self.append_record(&RebalanceWalRecord::Failed { id, reason })?;
        self.operations = operations;
        Ok(())
    }

    pub fn operation(&self, id: RebalanceOperationId) -> Option<&RebalanceOperation> {
        self.operations.get(&id)
    }

    pub fn operations(&self) -> Vec<RebalanceOperation> {
        self.operations.values().cloned().collect()
    }

    pub fn pending(&self) -> Vec<RebalanceOperation> {
        self.operations
            .values()
            .filter(|operation| {
                matches!(
                    operation.state,
                    RebalanceOperationState::Planned | RebalanceOperationState::Started
                )
            })
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &RebalanceWalRecord) -> RsfResult<()> {
        let payload = encode_rebalance_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(REBALANCE_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: REBALANCE_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Rebalance,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RebalanceWalRecord {
    Planned {
        id: RebalanceOperationId,
        action: RebalanceAction,
        proof: PlacementPolicyProof,
    },
    Started {
        id: RebalanceOperationId,
    },
    Completed {
        id: RebalanceOperationId,
        completion: RebalanceCompletion,
    },
    Failed {
        id: RebalanceOperationId,
        reason: String,
    },
}

fn apply_rebalance_record(
    operations: &mut BTreeMap<RebalanceOperationId, RebalanceOperation>,
    record: RebalanceWalRecord,
) -> RsfResult<()> {
    match record {
        RebalanceWalRecord::Planned { id, action, proof } => {
            if operations.contains_key(&id) {
                return Err(corrupt(format!(
                    "duplicate rebalance operation id {} in WAL",
                    id.0
                )));
            }
            validate_action_and_proof(&action, &proof)?;
            operations.insert(
                id,
                RebalanceOperation {
                    id,
                    action,
                    proof,
                    state: RebalanceOperationState::Planned,
                    completion: None,
                    failure: None,
                },
            );
        }
        RebalanceWalRecord::Started { id } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RebalanceOperationState::Planned {
                return Err(corrupt(format!(
                    "rebalance operation {} cannot replay start from {:?}",
                    id.0, operation.state
                )));
            }
            operation.state = RebalanceOperationState::Started;
        }
        RebalanceWalRecord::Completed { id, completion } => {
            let operation = operation_mut(operations, id)?;
            if operation.state != RebalanceOperationState::Started {
                return Err(corrupt(format!(
                    "rebalance operation {} cannot replay completion from {:?}",
                    id.0, operation.state
                )));
            }
            validate_completion(&operation.action, &operation.proof, &completion)?;
            operation.state = RebalanceOperationState::Completed;
            operation.completion = Some(completion);
            operation.failure = None;
        }
        RebalanceWalRecord::Failed { id, reason } => {
            validate_failure(&reason)?;
            let operation = operation_mut(operations, id)?;
            if operation.state == RebalanceOperationState::Completed {
                return Err(corrupt(format!(
                    "rebalance operation {} cannot replay failure after completion",
                    id.0
                )));
            }
            operation.state = RebalanceOperationState::Failed;
            operation.failure = Some(reason);
            operation.completion = None;
        }
    }
    Ok(())
}

fn validate_action_and_proof(
    action: &RebalanceAction,
    proof: &PlacementPolicyProof,
) -> RsfResult<()> {
    let RebalanceAction::MoveReplica { pg, from, to } = *action;
    if pg.0 == 0 {
        return Err(invalid("rebalance action pg must be non-zero"));
    }
    if from == to {
        return Err(invalid("rebalance source and target must differ"));
    }
    if proof.pg != pg {
        return Err(invalid("rebalance proof pg does not match action"));
    }
    if !proof.source_was_member || proof.target_was_member || !proof.target_is_storage {
        return Err(invalid(
            "rebalance proof does not describe a safe source/target move",
        ));
    }
    if !proof.no_duplicate_replicas || !proof.preserves_policy() {
        return Err(invalid(
            "rebalance proof does not preserve placement policy",
        ));
    }
    Ok(())
}

fn validate_completion(
    action: &RebalanceAction,
    proof: &PlacementPolicyProof,
    completion: &RebalanceCompletion,
) -> RsfResult<()> {
    let RebalanceAction::MoveReplica { pg, from, to } = *action;
    let RebalanceCompletion::Move(result) = completion;
    if result.pg != pg || result.from != from || result.to != to {
        return Err(invalid("rebalance move completion does not match action"));
    }
    if result.previous_replicas.len() != proof.previous_replicas
        || result.resulting_replicas.len() != proof.resulting_replicas
    {
        return Err(invalid(
            "rebalance move completion replica counts do not match proof",
        ));
    }
    if result.resulting_epoch.0 <= result.previous_epoch.0 {
        return Err(invalid("rebalance move completion must advance epoch"));
    }
    if result.resulting_group.id != pg
        || result.resulting_group.acting_set.epoch != result.resulting_epoch
        || result.resulting_group.acting_set.primary != result.resulting_primary
        || result.resulting_group.acting_set.replicas != result.resulting_replicas
    {
        return Err(invalid(
            "rebalance move completion resulting group does not match result summary",
        ));
    }
    validate_action_and_proof(action, proof)?;
    Ok(())
}

fn operation_mut(
    operations: &mut BTreeMap<RebalanceOperationId, RebalanceOperation>,
    id: RebalanceOperationId,
) -> RsfResult<&mut RebalanceOperation> {
    operations
        .get_mut(&id)
        .ok_or_else(|| invalid(format!("unknown rebalance operation {}", id.0)))
}

fn validate_failure(reason: &str) -> RsfResult<()> {
    if reason.trim().is_empty() {
        return Err(invalid("rebalance failure reason must not be empty"));
    }
    Ok(())
}

fn encode_rebalance_wal_payload(record: &RebalanceWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(REBALANCE_WAL_MAGIC);
    out.push(REBALANCE_WAL_VERSION);
    match record {
        RebalanceWalRecord::Planned { id, action, proof } => {
            validate_action_and_proof(action, proof)?;
            out.push(1);
            write_u64(&mut out, id.0);
            encode_action(&mut out, action);
            encode_proof(&mut out, proof);
        }
        RebalanceWalRecord::Started { id } => {
            out.push(2);
            write_u64(&mut out, id.0);
        }
        RebalanceWalRecord::Completed { id, completion } => {
            out.push(3);
            write_u64(&mut out, id.0);
            encode_completion(&mut out, completion);
        }
        RebalanceWalRecord::Failed { id, reason } => {
            validate_failure(reason)?;
            out.push(4);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_rebalance_wal_payload(bytes: &[u8]) -> RsfResult<RebalanceWalRecord> {
    if bytes.len() < REBALANCE_WAL_MAGIC.len() + 2 {
        return Err(corrupt("rebalance WAL payload is too short"));
    }
    if &bytes[..REBALANCE_WAL_MAGIC.len()] != REBALANCE_WAL_MAGIC {
        return Err(corrupt("rebalance WAL magic mismatch"));
    }
    let mut offset = REBALANCE_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != REBALANCE_WAL_VERSION {
        return Err(corrupt("unsupported rebalance WAL version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let id = RebalanceOperationId(read_u64(bytes, &mut offset)?);
    let record = match tag {
        1 => RebalanceWalRecord::Planned {
            id,
            action: decode_action(bytes, &mut offset)?,
            proof: decode_proof(bytes, &mut offset)?,
        },
        2 => RebalanceWalRecord::Started { id },
        3 => RebalanceWalRecord::Completed {
            id,
            completion: decode_completion(bytes, &mut offset)?,
        },
        4 => RebalanceWalRecord::Failed {
            id,
            reason: read_string(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown rebalance WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("trailing bytes in rebalance WAL payload"));
    }
    Ok(record)
}

fn encode_action(out: &mut Vec<u8>, action: &RebalanceAction) {
    let RebalanceAction::MoveReplica { pg, from, to } = *action;
    out.push(1);
    write_pg(out, pg);
    write_node(out, from);
    write_node(out, to);
}

fn decode_action(bytes: &[u8], offset: &mut usize) -> RsfResult<RebalanceAction> {
    match read_u8(bytes, offset)? {
        1 => Ok(RebalanceAction::MoveReplica {
            pg: read_pg(bytes, offset)?,
            from: read_node(bytes, offset)?,
            to: read_node(bytes, offset)?,
        }),
        _ => Err(corrupt("unknown rebalance action tag")),
    }
}

fn encode_proof(out: &mut Vec<u8>, proof: &PlacementPolicyProof) {
    write_pg(out, proof.pg);
    write_usize(out, proof.previous_replicas);
    write_usize(out, proof.resulting_replicas);
    write_usize(out, proof.expected_replicas);
    out.push(proof.min_replicas);
    write_bool(out, proof.source_was_member);
    write_bool(out, proof.target_was_member);
    write_bool(out, proof.target_is_storage);
    write_bool(out, proof.no_duplicate_replicas);
    write_usize(out, proof.previous_failure_domains);
    write_usize(out, proof.resulting_failure_domains);
    write_usize(out, proof.previous_regions);
    write_usize(out, proof.resulting_regions);
}

fn decode_proof(bytes: &[u8], offset: &mut usize) -> RsfResult<PlacementPolicyProof> {
    Ok(PlacementPolicyProof {
        pg: read_pg(bytes, offset)?,
        previous_replicas: read_usize(bytes, offset)?,
        resulting_replicas: read_usize(bytes, offset)?,
        expected_replicas: read_usize(bytes, offset)?,
        min_replicas: read_u8(bytes, offset)?,
        source_was_member: read_bool(bytes, offset)?,
        target_was_member: read_bool(bytes, offset)?,
        target_is_storage: read_bool(bytes, offset)?,
        no_duplicate_replicas: read_bool(bytes, offset)?,
        previous_failure_domains: read_usize(bytes, offset)?,
        resulting_failure_domains: read_usize(bytes, offset)?,
        previous_regions: read_usize(bytes, offset)?,
        resulting_regions: read_usize(bytes, offset)?,
    })
}

fn encode_completion(out: &mut Vec<u8>, completion: &RebalanceCompletion) {
    let RebalanceCompletion::Move(result) = completion;
    out.push(1);
    write_pg(out, result.pg);
    write_node(out, result.from);
    write_node(out, result.to);
    write_epoch(out, result.previous_epoch);
    write_epoch(out, result.resulting_epoch);
    write_node(out, result.previous_primary);
    write_node(out, result.resulting_primary);
    write_nodes(out, &result.previous_replicas);
    write_nodes(out, &result.resulting_replicas);
    encode_group(out, &result.resulting_group);
}

fn decode_completion(bytes: &[u8], offset: &mut usize) -> RsfResult<RebalanceCompletion> {
    match read_u8(bytes, offset)? {
        1 => Ok(RebalanceCompletion::Move(PlacementMoveResult {
            pg: read_pg(bytes, offset)?,
            from: read_node(bytes, offset)?,
            to: read_node(bytes, offset)?,
            previous_epoch: read_epoch(bytes, offset)?,
            resulting_epoch: read_epoch(bytes, offset)?,
            previous_primary: read_node(bytes, offset)?,
            resulting_primary: read_node(bytes, offset)?,
            previous_replicas: read_nodes(bytes, offset)?,
            resulting_replicas: read_nodes(bytes, offset)?,
            resulting_group: decode_group(bytes, offset)?,
        })),
        _ => Err(corrupt("unknown rebalance completion tag")),
    }
}

fn encode_group(out: &mut Vec<u8>, group: &PlacementGroup) {
    write_pg(out, group.id);
    write_epoch(out, group.acting_set.epoch);
    write_node(out, group.acting_set.primary);
    write_nodes(out, &group.acting_set.replicas);
    encode_policy(out, &group.policy);
}

fn decode_group(bytes: &[u8], offset: &mut usize) -> RsfResult<PlacementGroup> {
    Ok(PlacementGroup {
        id: read_pg(bytes, offset)?,
        acting_set: ActingSet {
            epoch: read_epoch(bytes, offset)?,
            primary: read_node(bytes, offset)?,
            replicas: read_nodes(bytes, offset)?,
        },
        policy: decode_policy(bytes, offset)?,
    })
}

fn encode_policy(out: &mut Vec<u8>, policy: &PlacementPolicy) {
    match policy {
        PlacementPolicy::Replicated { replicas } => {
            out.push(1);
            out.push(*replicas);
        }
        PlacementPolicy::ErasureCoded { data, parity } => {
            out.push(2);
            out.push(*data);
            out.push(*parity);
        }
    }
}

fn decode_policy(bytes: &[u8], offset: &mut usize) -> RsfResult<PlacementPolicy> {
    match read_u8(bytes, offset)? {
        1 => Ok(PlacementPolicy::Replicated {
            replicas: read_u8(bytes, offset)?,
        }),
        2 => Ok(PlacementPolicy::ErasureCoded {
            data: read_u8(bytes, offset)?,
            parity: read_u8(bytes, offset)?,
        }),
        _ => Err(corrupt("unknown placement policy tag")),
    }
}

fn write_nodes(out: &mut Vec<u8>, nodes: &[NodeId]) {
    write_usize(out, nodes.len());
    for node in nodes {
        write_node(out, *node);
    }
}

fn read_nodes(bytes: &[u8], offset: &mut usize) -> RsfResult<Vec<NodeId>> {
    let len = read_usize(bytes, offset)?;
    let mut nodes = Vec::with_capacity(len);
    for _ in 0..len {
        nodes.push(read_node(bytes, offset)?);
    }
    Ok(nodes)
}

fn write_pg(out: &mut Vec<u8>, pg: PlacementGroupId) {
    write_u64(out, pg.0);
}

fn write_epoch(out: &mut Vec<u8>, epoch: Epoch) {
    write_u64(out, epoch.0);
}

fn write_node(out: &mut Vec<u8>, node: NodeId) {
    write_u64(out, node.0);
}

fn write_usize(out: &mut Vec<u8>, value: usize) {
    write_u64(out, value as u64);
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("rebalance string is too large"))?;
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

fn read_epoch(bytes: &[u8], offset: &mut usize) -> RsfResult<Epoch> {
    Ok(Epoch(read_u64(bytes, offset)?))
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
        _ => Err(corrupt("invalid rebalance bool field")),
    }
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(corrupt("unexpected end of rebalance string length"));
    }
    let len = u32::from_le_bytes([
        bytes[*offset],
        bytes[*offset + 1],
        bytes[*offset + 2],
        bytes[*offset + 3],
    ]);
    *offset += 4;
    let len = usize::try_from(len).map_err(|_| corrupt("rebalance string length overflow"))?;
    if bytes.len().saturating_sub(*offset) < len {
        return Err(corrupt("unexpected end of rebalance string"));
    }
    let value = std::str::from_utf8(&bytes[*offset..*offset + len])
        .map_err(|_| corrupt("rebalance string is not utf-8"))?
        .to_owned();
    *offset += len;
    Ok(value)
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    if *offset >= bytes.len() {
        return Err(corrupt("unexpected end of rebalance WAL payload"));
    }
    let value = bytes[*offset];
    *offset += 1;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt("unexpected end of rebalance WAL payload"));
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
