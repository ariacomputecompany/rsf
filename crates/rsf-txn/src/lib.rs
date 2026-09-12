#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitConsistency, CommitEvidence, CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId,
    RsfError, RsfResult,
    storage::WalRecordKind,
    transaction::{CommitIntent, CommitResult, KeyRange, Mutation, TransactionId},
};
use rsf_isolation::{IsolationDecision, IsolationTracker, IsolationTransaction};
use rsf_protocol::{
    ClientRequestId, CommitCertificate, ConsistencyMode, FabricOperation, FabricRouterPort,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const TXN_PG: PlacementGroupId = PlacementGroupId(0);
const TXN_WAL_MAGIC: &[u8; 6] = b"RSFTXN";
const TXN_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadVersion {
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTransaction {
    pub intent: CommitIntent,
    pub read_version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionRecord {
    pub id: TransactionId,
    pub read_version: CommitVersion,
    pub commit_version: CommitVersion,
    pub read_conflicts: Vec<KeyRange>,
    pub write_conflicts: Vec<KeyRange>,
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionAbort {
    pub id: TransactionId,
    pub read_version: CommitVersion,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveredTransactionRecord {
    Prepared(PreparedTransaction),
    Committed {
        record: TransactionRecord,
        result: CommitResult,
    },
    Aborted(TransactionAbort),
}

#[derive(Clone, Debug, Default)]
pub struct TransactionCoordinator {
    committed: Vec<TransactionRecord>,
    isolation: IsolationTracker,
}

#[derive(Debug)]
pub struct DurableTransactionCoordinator {
    coordinator: TransactionCoordinator,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
    prepared: BTreeMap<TransactionId, PreparedTransaction>,
    committed: BTreeMap<TransactionId, CommitResult>,
    aborted: BTreeMap<TransactionId, TransactionAbort>,
}

impl TransactionCoordinator {
    pub fn begin(&self, router: &impl FabricRouterPort) -> ReadVersion {
        ReadVersion {
            version: router.latest_version(),
        }
    }

    pub fn prepare(
        &self,
        read_version: ReadVersion,
        intent: CommitIntent,
    ) -> RsfResult<PreparedTransaction> {
        validate_intent(&intent)?;
        self.ensure_no_conflicts(read_version.version, &intent.write_conflicts)?;
        Ok(PreparedTransaction {
            intent,
            read_version: read_version.version,
        })
    }

    pub fn commit(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        prepared: PreparedTransaction,
    ) -> RsfResult<CommitResult> {
        let request = transaction_request_id(&prepared.intent.id);
        if let Some(certificate) = router.request_certificate(&request) {
            return Ok(CommitResult {
                version: certificate.version,
                evidence: commit_evidence(certificate),
            });
        }

        self.ensure_no_conflicts(prepared.read_version, &prepared.intent.write_conflicts)?;
        let expected_commit_version = CommitVersion(router.latest_version().0.saturating_add(1));
        let accepted_isolation = self.check_isolation(&prepared, expected_commit_version)?;
        let operation = FabricOperation::TransactionCommit {
            transaction: prepared.intent.id.0,
        };
        ensure_operation_matches_router_route(router, pg, &operation)?;
        let commit = router.commit_request(pg, request, operation)?;
        if commit.certificate.version != expected_commit_version {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "transaction commit version changed during isolation admission",
            ));
        }
        self.isolation = accepted_isolation;
        self.committed.push(TransactionRecord {
            id: prepared.intent.id,
            read_version: prepared.read_version,
            commit_version: commit.certificate.version,
            read_conflicts: prepared.intent.read_conflicts,
            write_conflicts: prepared.intent.write_conflicts,
            mutations: prepared.intent.mutations,
        });
        Ok(CommitResult {
            version: commit.certificate.version,
            evidence: commit_evidence(commit.certificate),
        })
    }

    pub fn committed(&self) -> &[TransactionRecord] {
        &self.committed
    }

    fn ensure_no_conflicts(
        &self,
        read_version: CommitVersion,
        conflicts: &[KeyRange],
    ) -> RsfResult<()> {
        let newer = self
            .committed
            .iter()
            .filter(|record| record.commit_version > read_version)
            .find(|record| {
                record
                    .write_conflicts
                    .iter()
                    .any(|write| conflicts.iter().any(|read| ranges_overlap(write, read)))
            });

        if let Some(record) = newer {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "transaction conflict with committed version {}",
                    record.commit_version.0
                ),
            ));
        }
        Ok(())
    }

    fn accept_recovered_record(&mut self, record: TransactionRecord) -> RsfResult<()> {
        match self.isolation.commit(
            isolation_transaction(
                &record.id,
                record.read_version,
                &record.read_conflicts,
                &record.write_conflicts,
            ),
            record.commit_version,
        )? {
            IsolationDecision::Accepted(_) => {
                self.committed.push(record);
                Ok(())
            }
            IsolationDecision::Rejected { reason, .. } => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("recovered transaction violates isolation: {reason}"),
            )),
        }
    }

    fn check_isolation(
        &self,
        prepared: &PreparedTransaction,
        commit_version: CommitVersion,
    ) -> RsfResult<IsolationTracker> {
        let mut isolation = self.isolation.clone();
        match isolation.commit(
            isolation_transaction(
                &prepared.intent.id,
                prepared.read_version,
                &prepared.intent.read_conflicts,
                &prepared.intent.write_conflicts,
            ),
            commit_version,
        )? {
            IsolationDecision::Accepted(_) => Ok(isolation),
            IsolationDecision::Rejected { reason, .. } => {
                Err(RsfError::new(ErrorCode::InvalidInput, reason))
            }
        }
    }
}

impl DurableTransactionCoordinator {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(TXN_PG) })?;
        let mut coordinator = TransactionCoordinator::default();
        let mut prepared = BTreeMap::new();
        let mut committed = BTreeMap::new();
        let mut aborted = BTreeMap::new();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Transaction)
        {
            match decode_txn_wal_payload(&record.payload)? {
                RecoveredTransactionRecord::Prepared(transaction) => {
                    if !committed.contains_key(&transaction.intent.id)
                        && !aborted.contains_key(&transaction.intent.id)
                    {
                        prepared.insert(transaction.intent.id.clone(), transaction);
                    }
                }
                RecoveredTransactionRecord::Committed { record, result } => {
                    prepared.remove(&record.id);
                    aborted.remove(&record.id);
                    if !committed.contains_key(&record.id) {
                        coordinator.accept_recovered_record(record.clone())?;
                    }
                    committed.insert(record.id, result);
                }
                RecoveredTransactionRecord::Aborted(abort) => {
                    prepared.remove(&abort.id);
                    if !committed.contains_key(&abort.id) {
                        aborted.insert(abort.id.clone(), abort);
                    }
                }
            }
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            coordinator,
            store,
            epoch,
            next_index,
            prepared,
            committed,
            aborted,
        })
    }

    pub fn begin(&self, router: &impl FabricRouterPort) -> ReadVersion {
        self.coordinator.begin(router)
    }

    pub fn prepare(
        &mut self,
        read_version: ReadVersion,
        intent: CommitIntent,
    ) -> RsfResult<PreparedTransaction> {
        if let Some(result) = self.committed.get(&intent.id) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "transaction already committed at version {}",
                    result.version.0
                ),
            ));
        }
        if self.aborted.contains_key(&intent.id) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "transaction is already aborted",
            ));
        }
        if let Some(prepared) = self.prepared.get(&intent.id) {
            return Ok(prepared.clone());
        }

        let prepared = self.coordinator.prepare(read_version, intent)?;
        self.append_record(&RecoveredTransactionRecord::Prepared(prepared.clone()))?;
        self.prepared
            .insert(prepared.intent.id.clone(), prepared.clone());
        Ok(prepared)
    }

    pub fn commit(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        prepared: PreparedTransaction,
    ) -> RsfResult<CommitResult> {
        if let Some(result) = self.committed.get(&prepared.intent.id) {
            return Ok(result.clone());
        }
        if self.aborted.contains_key(&prepared.intent.id) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "aborted transaction cannot commit",
            ));
        }
        if !self.prepared.contains_key(&prepared.intent.id) {
            self.append_record(&RecoveredTransactionRecord::Prepared(prepared.clone()))?;
            self.prepared
                .insert(prepared.intent.id.clone(), prepared.clone());
        }

        let before_len = self.coordinator.committed.len();
        let result = self.coordinator.commit(router, pg, prepared.clone())?;
        let record = if self.coordinator.committed.len() > before_len {
            self.coordinator
                .committed
                .last()
                .expect("commit appended record")
                .clone()
        } else {
            TransactionRecord {
                id: prepared.intent.id.clone(),
                read_version: prepared.read_version,
                commit_version: result.version,
                read_conflicts: prepared.intent.read_conflicts.clone(),
                write_conflicts: prepared.intent.write_conflicts.clone(),
                mutations: prepared.intent.mutations.clone(),
            }
        };
        self.append_record(&RecoveredTransactionRecord::Committed {
            record,
            result: result.clone(),
        })?;
        self.prepared.remove(&prepared.intent.id);
        self.committed
            .insert(prepared.intent.id.clone(), result.clone());
        Ok(result)
    }

    pub fn abort(
        &mut self,
        id: TransactionId,
        reason: impl Into<String>,
    ) -> RsfResult<TransactionAbort> {
        if self.committed.contains_key(&id) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "committed transaction cannot abort",
            ));
        }
        if let Some(abort) = self.aborted.get(&id) {
            return Ok(abort.clone());
        }
        let read_version = self
            .prepared
            .get(&id)
            .map_or(CommitVersion(0), |prepared| prepared.read_version);
        let abort = TransactionAbort {
            id: id.clone(),
            read_version,
            reason: reason.into(),
        };
        if abort.reason.trim().is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "transaction abort reason must not be empty",
            ));
        }
        self.append_record(&RecoveredTransactionRecord::Aborted(abort.clone()))?;
        self.prepared.remove(&id);
        self.aborted.insert(id, abort.clone());
        Ok(abort)
    }

    pub fn prepared(&self) -> Vec<PreparedTransaction> {
        self.prepared.values().cloned().collect()
    }

    pub fn committed(&self) -> &[TransactionRecord] {
        self.coordinator.committed()
    }

    pub fn committed_result(&self, id: &TransactionId) -> Option<&CommitResult> {
        self.committed.get(id)
    }

    pub fn aborted(&self) -> Vec<TransactionAbort> {
        self.aborted.values().cloned().collect()
    }

    fn append_record(&mut self, record: &RecoveredTransactionRecord) -> RsfResult<()> {
        let payload = encode_txn_wal_payload(record)?;
        let previous_hash = self
            .store
            .recover_current()?
            .recovery
            .pg_tail(TXN_PG)
            .map_or([0; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: TXN_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Transaction,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn commit_evidence(certificate: CommitCertificate) -> CommitEvidence {
    CommitEvidence {
        placement_group: certificate.pg,
        log_index: certificate.index,
        version: certificate.version,
        consistency: match certificate.mode {
            ConsistencyMode::Strict => CommitConsistency::Strict,
            ConsistencyMode::AvailabilityFirst => CommitConsistency::AvailabilityFirst,
        },
        operation_id: certificate.operation_id,
    }
}

fn encode_txn_wal_payload(record: &RecoveredTransactionRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(TXN_WAL_MAGIC);
    out.push(TXN_WAL_VERSION);
    match record {
        RecoveredTransactionRecord::Prepared(prepared) => {
            out.push(1);
            write_intent(&prepared.intent, &mut out)?;
            out.extend_from_slice(&prepared.read_version.0.to_le_bytes());
        }
        RecoveredTransactionRecord::Committed { record, result } => {
            out.push(2);
            write_transaction_id(&record.id, &mut out);
            out.extend_from_slice(&record.read_version.0.to_le_bytes());
            out.extend_from_slice(&record.commit_version.0.to_le_bytes());
            write_ranges(&record.read_conflicts, &mut out)?;
            write_ranges(&record.write_conflicts, &mut out)?;
            write_mutations(&record.mutations, &mut out)?;
            write_commit_result(result, &mut out);
        }
        RecoveredTransactionRecord::Aborted(abort) => {
            out.push(3);
            write_transaction_id(&abort.id, &mut out);
            out.extend_from_slice(&abort.read_version.0.to_le_bytes());
            write_string(&abort.reason, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_txn_wal_payload(bytes: &[u8]) -> RsfResult<RecoveredTransactionRecord> {
    if !bytes.starts_with(TXN_WAL_MAGIC) {
        return Err(corrupt("transaction wal record has invalid magic"));
    }
    let mut offset = TXN_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != TXN_WAL_VERSION {
        return Err(corrupt("transaction wal record has unsupported version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => {
            let intent = read_intent(bytes, &mut offset)?;
            let read_version = CommitVersion(read_u64(bytes, &mut offset)?);
            let prepared = PreparedTransaction {
                intent,
                read_version,
            };
            validate_intent(&prepared.intent)?;
            RecoveredTransactionRecord::Prepared(prepared)
        }
        2 => {
            let id = read_transaction_id(bytes, &mut offset)?;
            let read_version = CommitVersion(read_u64(bytes, &mut offset)?);
            let commit_version = CommitVersion(read_u64(bytes, &mut offset)?);
            let read_conflicts = read_ranges(bytes, &mut offset)?;
            let write_conflicts = read_ranges(bytes, &mut offset)?;
            let mutations = read_mutations(bytes, &mut offset)?;
            let result = read_commit_result(bytes, &mut offset)?;
            if result.version != commit_version {
                return Err(corrupt(
                    "transaction committed record result version mismatch",
                ));
            }
            if write_conflicts.is_empty() || mutations.is_empty() {
                return Err(corrupt(
                    "transaction committed record must carry conflicts and mutations",
                ));
            }
            for range in read_conflicts.iter().chain(write_conflicts.iter()) {
                validate_range(range)?;
            }
            RecoveredTransactionRecord::Committed {
                record: TransactionRecord {
                    id,
                    read_version,
                    commit_version,
                    read_conflicts,
                    write_conflicts,
                    mutations,
                },
                result,
            }
        }
        3 => {
            let id = read_transaction_id(bytes, &mut offset)?;
            let read_version = CommitVersion(read_u64(bytes, &mut offset)?);
            let reason = read_string(bytes, &mut offset)?;
            if reason.trim().is_empty() {
                return Err(corrupt("transaction abort reason must not be empty"));
            }
            RecoveredTransactionRecord::Aborted(TransactionAbort {
                id,
                read_version,
                reason,
            })
        }
        _ => return Err(corrupt("transaction wal record has unknown tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("transaction wal record has trailing bytes"));
    }
    Ok(record)
}

fn write_intent(intent: &CommitIntent, out: &mut Vec<u8>) -> RsfResult<()> {
    write_transaction_id(&intent.id, out);
    write_ranges(&intent.read_conflicts, out)?;
    write_ranges(&intent.write_conflicts, out)?;
    write_mutations(&intent.mutations, out)
}

fn read_intent(bytes: &[u8], offset: &mut usize) -> RsfResult<CommitIntent> {
    Ok(CommitIntent {
        id: read_transaction_id(bytes, offset)?,
        read_conflicts: read_ranges(bytes, offset)?,
        write_conflicts: read_ranges(bytes, offset)?,
        mutations: read_mutations(bytes, offset)?,
    })
}

fn write_transaction_id(id: &TransactionId, out: &mut Vec<u8>) {
    out.extend_from_slice(&id.0);
}

fn read_transaction_id(bytes: &[u8], offset: &mut usize) -> RsfResult<TransactionId> {
    let raw = read_exact(bytes, offset, 16)?;
    let mut id = [0_u8; 16];
    id.copy_from_slice(raw);
    Ok(TransactionId(id))
}

fn write_ranges(ranges: &[KeyRange], out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(ranges.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "transaction range count does not fit in record",
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    for range in ranges {
        write_bytes(&range.start, out)?;
        write_bytes(&range.end, out)?;
    }
    Ok(())
}

fn read_ranges(bytes: &[u8], offset: &mut usize) -> RsfResult<Vec<KeyRange>> {
    let len = read_u32(bytes, offset)? as usize;
    let mut ranges = Vec::with_capacity(len);
    for _ in 0..len {
        ranges.push(KeyRange {
            start: read_bytes(bytes, offset)?,
            end: read_bytes(bytes, offset)?,
        });
    }
    Ok(ranges)
}

fn write_mutations(mutations: &[Mutation], out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(mutations.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "transaction mutation count does not fit in record",
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    for mutation in mutations {
        match mutation {
            Mutation::Set { key, value } => {
                out.push(1);
                write_bytes(key, out)?;
                write_bytes(value, out)?;
            }
            Mutation::Clear { key } => {
                out.push(2);
                write_bytes(key, out)?;
                write_bytes(&[], out)?;
            }
        }
    }
    Ok(())
}

fn read_mutations(bytes: &[u8], offset: &mut usize) -> RsfResult<Vec<Mutation>> {
    let len = read_u32(bytes, offset)? as usize;
    let mut mutations = Vec::with_capacity(len);
    for _ in 0..len {
        let tag = read_u8(bytes, offset)?;
        let key = read_bytes(bytes, offset)?;
        let value = read_bytes(bytes, offset)?;
        mutations.push(match tag {
            1 => Mutation::Set { key, value },
            2 => {
                if !value.is_empty() {
                    return Err(corrupt("transaction clear mutation carries a value"));
                }
                Mutation::Clear { key }
            }
            _ => return Err(corrupt("transaction mutation has unknown tag")),
        });
    }
    Ok(mutations)
}

fn write_commit_result(result: &CommitResult, out: &mut Vec<u8>) {
    out.extend_from_slice(&result.version.0.to_le_bytes());
    out.extend_from_slice(&result.evidence.placement_group.0.to_le_bytes());
    out.extend_from_slice(&result.evidence.log_index.0.to_le_bytes());
    out.extend_from_slice(&result.evidence.version.0.to_le_bytes());
    out.push(match result.evidence.consistency {
        CommitConsistency::Strict => 1,
        CommitConsistency::AvailabilityFirst => 2,
    });
    out.extend_from_slice(&result.evidence.operation_id.0.to_le_bytes());
}

fn read_commit_result(bytes: &[u8], offset: &mut usize) -> RsfResult<CommitResult> {
    let version = CommitVersion(read_u64(bytes, offset)?);
    let placement_group = PlacementGroupId(read_u64(bytes, offset)?);
    let log_index = LogIndex(read_u64(bytes, offset)?);
    let evidence_version = CommitVersion(read_u64(bytes, offset)?);
    if evidence_version != version {
        return Err(corrupt("transaction evidence version mismatch"));
    }
    let consistency = match read_u8(bytes, offset)? {
        1 => CommitConsistency::Strict,
        2 => CommitConsistency::AvailabilityFirst,
        _ => return Err(corrupt("transaction evidence has invalid consistency")),
    };
    let operation_id = rsf_core::OperationId(read_u64(bytes, offset)?);
    Ok(CommitResult {
        version,
        evidence: CommitEvidence {
            placement_group,
            log_index,
            version: evidence_version,
            consistency,
            operation_id,
        },
    })
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    write_bytes(value.as_bytes(), out)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    String::from_utf8(read_bytes(bytes, offset)?)
        .map_err(|_| corrupt("transaction wal string is not utf-8"))
}

fn write_bytes(bytes: &[u8], out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "transaction byte field does not fit in record",
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<Vec<u8>> {
    let len = read_u32(bytes, offset)? as usize;
    Ok(read_exact(bytes, offset, len)?.to_vec())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("transaction wal record ended early"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = read_exact(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = (*offset)
        .checked_add(len)
        .ok_or_else(|| corrupt("transaction wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("transaction wal record ended early"))?;
    *offset = end;
    Ok(raw)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

fn validate_intent(intent: &CommitIntent) -> RsfResult<()> {
    if intent.write_conflicts.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "transaction must declare write conflict ranges",
        ));
    }
    if intent.mutations.is_empty() {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "transaction must include at least one mutation",
        ));
    }
    for range in intent
        .read_conflicts
        .iter()
        .chain(intent.write_conflicts.iter())
    {
        validate_range(range)?;
    }
    Ok(())
}

fn validate_range(range: &KeyRange) -> RsfResult<()> {
    if range.start >= range.end {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "key range start must be lower than key range end",
        ));
    }
    Ok(())
}

fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    left.start < right.end && right.start < left.end
}

fn ensure_operation_matches_router_route(
    router: &impl FabricRouterPort,
    pg: PlacementGroupId,
    operation: &FabricOperation,
) -> RsfResult<()> {
    if let Some(routed_pg) = router.route_operation(operation)
        && routed_pg != pg
    {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            format!(
                "transaction placement group {} does not match current routed owner {}",
                pg.0, routed_pg.0
            ),
        ));
    }
    Ok(())
}

fn isolation_transaction(
    id: &TransactionId,
    read_version: CommitVersion,
    read_ranges: &[KeyRange],
    write_ranges: &[KeyRange],
) -> IsolationTransaction {
    IsolationTransaction {
        id: id.clone(),
        read_version,
        read_ranges: read_ranges.to_vec(),
        write_ranges: write_ranges.to_vec(),
    }
}

fn transaction_request_id(id: &TransactionId) -> ClientRequestId {
    ClientRequestId::new(format!("txn-{}", hex_id(&id.0)), 0)
}

fn hex_id(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use rsf_core::{
        CommitConsistency, Epoch, LogIndex, NodeId, OperationId,
        transaction::{CommitIntent, TransactionId},
    };
    use rsf_protocol::{FabricRouter, VsrBackedFabricRouter};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn range(start: &[u8], end: &[u8]) -> KeyRange {
        KeyRange {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }

    fn intent(id: u8, start: &[u8], end: &[u8]) -> CommitIntent {
        intent_with_id(TransactionId([id; 16]), start, end)
    }

    fn intent_with_id(id: TransactionId, start: &[u8], end: &[u8]) -> CommitIntent {
        CommitIntent {
            id,
            read_conflicts: vec![range(start, end)],
            write_conflicts: vec![range(start, end)],
            mutations: vec![Mutation::Set {
                key: start.to_vec(),
                value: b"value".to_vec(),
            }],
        }
    }

    fn transaction_id_for_pg(
        router: &impl FabricRouterPort,
        target: PlacementGroupId,
    ) -> TransactionId {
        for index in 0..10_000_u128 {
            let id = TransactionId(index.to_le_bytes());
            let operation = FabricOperation::TransactionCommit { transaction: id.0 };
            if router.route_operation(&operation) == Some(target) {
                return id;
            }
        }
        panic!(
            "no transaction id routed to target placement group {}",
            target.0
        );
    }

    fn skew_intent(
        id: u8,
        read_start: &[u8],
        read_end: &[u8],
        write_start: &[u8],
        write_end: &[u8],
    ) -> CommitIntent {
        CommitIntent {
            id: TransactionId([id; 16]),
            read_conflicts: vec![range(read_start, read_end)],
            write_conflicts: vec![range(write_start, write_end)],
            mutations: vec![Mutation::Set {
                key: write_start.to_vec(),
                value: b"value".to_vec(),
            }],
        }
    }

    #[test]
    fn commit_allocates_database_version_through_protocol() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut coordinator = TransactionCoordinator::default();

        let read = coordinator.begin(&router);
        let prepared = coordinator
            .prepare(read, intent(1, b"user/1", b"user/2"))
            .unwrap();
        let result = coordinator
            .commit(&mut router, PlacementGroupId(1), prepared)
            .unwrap();

        assert_eq!(result.version, CommitVersion(1));
        assert_eq!(result.evidence.version, CommitVersion(1));
        assert_eq!(result.evidence.placement_group, PlacementGroupId(1));
        assert_eq!(result.evidence.log_index, LogIndex(1));
        assert_eq!(result.evidence.consistency, CommitConsistency::Strict);
        assert_ne!(result.evidence.operation_id, OperationId(0));
        assert_eq!(coordinator.committed().len(), 1);
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn duplicate_transaction_commit_replays_original_version_without_recording_twice() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut coordinator = TransactionCoordinator::default();

        let read = coordinator.begin(&router);
        let prepared = coordinator
            .prepare(read, intent(9, b"user/09", b"user/10"))
            .unwrap();
        let first = coordinator
            .commit(&mut router, PlacementGroupId(1), prepared.clone())
            .unwrap();
        let replay = coordinator
            .commit(&mut router, PlacementGroupId(1), prepared)
            .unwrap();

        assert_eq!(first.version, CommitVersion(1));
        assert_eq!(replay.version, first.version);
        assert_eq!(replay.evidence, first.evidence);
        assert_eq!(coordinator.committed().len(), 1);
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn commit_allocates_version_through_vsr_backed_router_and_recovers_certificate() {
        let path = test_path("commit_allocates_version_through_vsr_backed_router");
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
            )
            .expect("router");
            let mut coordinator = TransactionCoordinator::default();

            let read = coordinator.begin(&router);
            let prepared = coordinator
                .prepare(read, intent(1, b"user/1", b"user/2"))
                .expect("prepare");
            let result = coordinator
                .commit(&mut router, PlacementGroupId(1), prepared)
                .expect("commit");

            assert_eq!(result.version, CommitVersion(1));
            assert_eq!(result.evidence.version, CommitVersion(1));
            assert_eq!(result.evidence.placement_group, PlacementGroupId(1));
            assert_eq!(result.evidence.log_index, LogIndex(1));
            assert_eq!(result.evidence.consistency, CommitConsistency::Strict);
            assert_ne!(result.evidence.operation_id, OperationId(0));
            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(coordinator.committed().len(), 1);
        }

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router");

        assert_eq!(recovered.latest_version(), CommitVersion(1));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn duplicate_transaction_commit_replays_vsr_certificate_without_new_commit() {
        let path = test_path("duplicate_transaction_commit_replays_vsr_certificate");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        {
            let mut router = VsrBackedFabricRouter::open(
                &path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut coordinator = TransactionCoordinator::default();
            let read = coordinator.begin(&router);
            let prepared = coordinator
                .prepare(read, intent(10, b"user/10", b"user/11"))
                .expect("prepare");

            let first = coordinator
                .commit(&mut router, PlacementGroupId(1), prepared.clone())
                .expect("first commit");
            let replay = coordinator
                .commit(&mut router, PlacementGroupId(1), prepared)
                .expect("replayed commit");

            assert_eq!(first.version, CommitVersion(1));
            assert_eq!(replay.version, first.version);
            assert_eq!(replay.evidence, first.evidence);
            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(coordinator.committed().len(), 1);
        }

        let recovered =
            VsrBackedFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1), NodeId(0), voters)
                .expect("recover router");
        assert_eq!(recovered.latest_version(), CommitVersion(1));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_transaction_recovers_in_flight_prepare_and_commits_once() {
        let router_path = test_path("durable_transaction_prepare_router");
        let txn_path = test_path("durable_transaction_prepare_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let prepared;

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("txn");
            prepared = coordinator
                .prepare(
                    coordinator.begin(&router),
                    intent(21, b"user/21", b"user/22"),
                )
                .expect("prepare");
            assert_eq!(coordinator.prepared().len(), 1);
            assert!(coordinator.committed().is_empty());
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("recover txn");
            assert_eq!(coordinator.prepared(), vec![prepared.clone()]);

            let first = coordinator
                .commit(&mut router, PlacementGroupId(1), prepared.clone())
                .expect("commit recovered prepare");
            let replay = coordinator
                .commit(&mut router, PlacementGroupId(1), prepared)
                .expect("replay committed prepare");
            assert_eq!(first.version, CommitVersion(1));
            assert_eq!(replay, first);
            assert_eq!(coordinator.prepared().len(), 0);
            assert_eq!(coordinator.committed().len(), 1);
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router again");
            let coordinator = DurableTransactionCoordinator::open(&txn_path, Epoch(1))
                .expect("recover txn again");
            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(coordinator.prepared().len(), 0);
            assert_eq!(coordinator.committed().len(), 1);
            assert_eq!(
                coordinator
                    .committed_result(&TransactionId([21; 16]))
                    .expect("committed result")
                    .version,
                CommitVersion(1)
            );
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn durable_transaction_recovered_commit_rejects_stale_overlap() {
        let router_path = test_path("durable_transaction_conflict_router");
        let txn_path = test_path("durable_transaction_conflict_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let stale;

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("txn");
            stale = coordinator.begin(&router);
            let prepared = coordinator
                .prepare(coordinator.begin(&router), intent(31, b"user/1", b"user/9"))
                .expect("prepare");
            coordinator
                .commit(&mut router, PlacementGroupId(1), prepared)
                .expect("commit");
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("recover txn");
            let err = coordinator
                .prepare(stale, intent(32, b"user/3", b"user/4"))
                .expect_err("recovered committed write must reject stale overlap");

            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert_eq!(coordinator.committed().len(), 1);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn durable_transaction_recovers_abort_and_fences_commit() {
        let router_path = test_path("durable_transaction_abort_router");
        let txn_path = test_path("durable_transaction_abort_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let prepared;

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("txn");
            prepared = coordinator
                .prepare(
                    coordinator.begin(&router),
                    intent(41, b"user/41", b"user/42"),
                )
                .expect("prepare");
            let abort = coordinator
                .abort(prepared.intent.id.clone(), "client rollback")
                .expect("abort");
            assert_eq!(abort.read_version, prepared.read_version);
            assert_eq!(coordinator.prepared().len(), 0);
            assert_eq!(coordinator.aborted().len(), 1);
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("recover txn");
            assert_eq!(coordinator.aborted().len(), 1);
            let err = coordinator
                .commit(&mut router, PlacementGroupId(1), prepared)
                .expect_err("aborted transaction cannot commit");
            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert_eq!(router.latest_version(), CommitVersion(0));
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn durable_transaction_allows_non_conflicting_concurrent_commits() {
        let router_path = test_path("durable_transaction_concurrent_router");
        let txn_path = test_path("durable_transaction_concurrent_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("txn");
            let snapshot = coordinator.begin(&router);
            let first = coordinator
                .prepare(snapshot.clone(), intent(51, b"user/51", b"user/52"))
                .expect("prepare first");
            let second = coordinator
                .prepare(snapshot, intent(52, b"zone/51", b"zone/52"))
                .expect("prepare second");

            let first_result = coordinator
                .commit(&mut router, PlacementGroupId(1), first)
                .expect("commit first");
            let second_result = coordinator
                .commit(&mut router, PlacementGroupId(1), second)
                .expect("commit second");

            assert_eq!(first_result.version, CommitVersion(1));
            assert_eq!(second_result.version, CommitVersion(2));
            assert_eq!(coordinator.committed().len(), 2);
        }

        {
            let router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("recover txn");

            assert_eq!(router.latest_version(), CommitVersion(2));
            assert_eq!(coordinator.committed().len(), 2);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn durable_transaction_rejects_explicit_commit_to_stale_owner_pg() {
        let router_path = test_path("durable_transaction_stale_owner_router");
        let txn_path = test_path("durable_transaction_stale_owner_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1), PlacementGroupId(2)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            router
                .update_placement_map([PlacementGroupId(3), PlacementGroupId(4)])
                .expect("policy placement update");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("txn");
            let prepared = coordinator
                .prepare(
                    coordinator.begin(&router),
                    intent_with_id(
                        transaction_id_for_pg(&router, PlacementGroupId(4)),
                        b"user/owner",
                        b"user/ownes",
                    ),
                )
                .expect("prepare");

            let err = coordinator
                .commit(&mut router, PlacementGroupId(3), prepared)
                .expect_err("stale explicit owner must be rejected");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains("current routed owner 4"));
            assert_eq!(router.latest_version(), CommitVersion(0));
            assert_eq!(coordinator.committed().len(), 0);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn durable_transaction_rejects_write_skew_through_recovered_isolation() {
        let router_path = test_path("durable_transaction_write_skew_router");
        let txn_path = test_path("durable_transaction_write_skew_store");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let stale_skew;

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("txn");
            let snapshot = coordinator.begin(&router);
            let first = coordinator
                .prepare(
                    snapshot.clone(),
                    skew_intent(61, b"account/a", b"account/b", b"account/b", b"account/c"),
                )
                .expect("prepare first");
            stale_skew = coordinator
                .prepare(
                    snapshot,
                    skew_intent(62, b"account/b", b"account/c", b"account/a", b"account/b"),
                )
                .expect("prepare stale skew");

            coordinator
                .commit(&mut router, PlacementGroupId(1), first)
                .expect("commit first");
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut coordinator =
                DurableTransactionCoordinator::open(&txn_path, Epoch(1)).expect("recover txn");

            let err = coordinator
                .commit(&mut router, PlacementGroupId(1), stale_skew)
                .expect_err("write skew must be rejected after recovery");

            assert_eq!(err.code, ErrorCode::InvalidInput);
            assert!(err.message.contains("serialization cycle"));
            assert_eq!(router.latest_version(), CommitVersion(1));
            assert_eq!(coordinator.committed().len(), 1);
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(txn_path);
    }

    #[test]
    fn overlapping_stale_transaction_is_rejected() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut coordinator = TransactionCoordinator::default();

        let stale = coordinator.begin(&router);
        let first = coordinator
            .prepare(coordinator.begin(&router), intent(1, b"user/1", b"user/9"))
            .unwrap();
        coordinator
            .commit(&mut router, PlacementGroupId(1), first)
            .unwrap();

        let err = coordinator
            .prepare(stale, intent(2, b"user/3", b"user/4"))
            .expect_err("stale overlapping transaction must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn disjoint_stale_transaction_can_commit() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let mut coordinator = TransactionCoordinator::default();

        let stale = coordinator.begin(&router);
        let first = coordinator
            .prepare(coordinator.begin(&router), intent(1, b"user/1", b"user/2"))
            .unwrap();
        coordinator
            .commit(&mut router, PlacementGroupId(1), first)
            .unwrap();
        let second = coordinator
            .prepare(stale, intent(2, b"zone/1", b"zone/2"))
            .unwrap();
        let result = coordinator
            .commit(&mut router, PlacementGroupId(1), second)
            .unwrap();

        assert_eq!(result.version, CommitVersion(2));
    }

    #[test]
    fn invalid_ranges_are_rejected() {
        let router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let coordinator = TransactionCoordinator::default();

        let err = coordinator
            .prepare(coordinator.begin(&router), intent(1, b"b", b"a"))
            .expect_err("invalid range must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-txn-{name}-{}-{nonce}", std::process::id()))
    }
}
