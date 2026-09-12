use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rsf_core::storage::{WalRecord, WalRecordKind};
use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RsfError, RsfResult,
};
use rsf_storage::{SegmentStore, SegmentStoreOptions, WalAppend};
use rsf_vsr::{
    DurableVsrCell, LocalVsrTransport, VsrClientRequestId, VsrDecision, VsrOperation, VsrPrepare,
    VsrPrepareMessage, VsrTransport,
};

use crate::{
    ClientId, ClientRequestId, CommitCertificate, ConsistencyMode, FabricCommit, FabricOperation,
    FabricRouterPort, OperationId, RequestCommitEvidence, RequestId,
};

const PROTOCOL_COMMIT_MAGIC: &[u8; 5] = b"RSFP1";
const PROTOCOL_COMMIT_BASE_LEN: usize = 5 + 8 + 8 + 8 + 1 + 8 + 1;
const PLACEMENT_MAP_MAGIC: &[u8; 5] = b"RSFM1";
const PROTOCOL_POLICY_MAGIC: &[u8; 5] = b"RSFY1";
const METADATA_PG: PlacementGroupId = PlacementGroupId(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolCommitRecord {
    pub certificate: CommitCertificate,
    pub client_request: Option<ClientRequestId>,
}

impl ProtocolCommitRecord {
    pub fn from_commit(commit: &FabricCommit) -> Self {
        Self {
            certificate: CommitCertificate::from(commit),
            client_request: None,
        }
    }

    pub fn from_client_request(commit: &FabricCommit, request: ClientRequestId) -> Self {
        Self {
            certificate: CommitCertificate::from(commit),
            client_request: Some(request),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(PROTOCOL_COMMIT_BASE_LEN);
        out.extend_from_slice(PROTOCOL_COMMIT_MAGIC);
        out.extend_from_slice(&self.certificate.pg.0.to_le_bytes());
        out.extend_from_slice(&self.certificate.index.0.to_le_bytes());
        out.extend_from_slice(&self.certificate.version.0.to_le_bytes());
        out.push(self.certificate.mode.to_u8());
        out.extend_from_slice(&self.certificate.operation_id.0.to_le_bytes());
        match &self.client_request {
            Some(request) => {
                let client = request.client.0.as_bytes();
                let client_len = u64::try_from(client.len()).expect("client id length fits u64");
                out.push(1);
                out.extend_from_slice(&request.request.0.to_le_bytes());
                out.extend_from_slice(&client_len.to_le_bytes());
                out.extend_from_slice(client);
            }
            None => out.push(0),
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < PROTOCOL_COMMIT_BASE_LEN {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "protocol commit record has length {}; expected at least {PROTOCOL_COMMIT_BASE_LEN}",
                    bytes.len()
                ),
            ));
        }
        if &bytes[0..5] != PROTOCOL_COMMIT_MAGIC {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "protocol commit record has invalid magic",
            ));
        }

        let certificate = CommitCertificate {
            pg: PlacementGroupId(read_u64(bytes, 5)?),
            index: LogIndex(read_u64(bytes, 13)?),
            version: CommitVersion(read_u64(bytes, 21)?),
            mode: ConsistencyMode::from_u8(bytes[29]).ok_or_else(|| {
                RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unknown protocol consistency mode {}", bytes[29]),
                )
            })?,
            operation_id: OperationId(read_u64(bytes, 30)?),
        };
        let client_request = match bytes[38] {
            0 => {
                if bytes.len() != PROTOCOL_COMMIT_BASE_LEN {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "protocol commit record has trailing bytes without client request",
                    ));
                }
                None
            }
            1 => {
                let request = RequestId(read_u64(bytes, 39)?);
                let client_len = usize::try_from(read_u64(bytes, 47)?).map_err(|_| {
                    RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "protocol client id length does not fit in memory",
                    )
                })?;
                let client_start = 55;
                let client_end = client_start + client_len;
                let client = bytes.get(client_start..client_end).ok_or_else(|| {
                    RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "protocol commit record ended while reading client id",
                    )
                })?;
                if client_end != bytes.len() {
                    return Err(RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "protocol commit record has trailing bytes after client id",
                    ));
                }
                let client = String::from_utf8(client.to_vec()).map_err(|_| {
                    RsfError::new(
                        ErrorCode::CorruptionDetected,
                        "protocol client id is not valid utf-8",
                    )
                })?;
                Some(ClientRequestId {
                    client: ClientId(client),
                    request,
                })
            }
            value => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unknown protocol client request flag {value}"),
                ));
            }
        };

        Ok(Self {
            certificate,
            client_request,
        })
    }

    pub fn is_protocol_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(PROTOCOL_COMMIT_MAGIC)
    }
}

#[derive(Debug)]
pub struct ProtocolWal {
    store: SegmentStore,
    pg: PlacementGroupId,
    epoch: Epoch,
}

impl ProtocolWal {
    pub fn open(path: impl AsRef<Path>, pg: PlacementGroupId, epoch: Epoch) -> RsfResult<Self> {
        Self::open_with_store_options(path, pg, epoch, SegmentStoreOptions::default())
    }

    pub fn open_with_store_options(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        epoch: Epoch,
        options: SegmentStoreOptions,
    ) -> RsfResult<Self> {
        Ok(Self {
            store: SegmentStore::open_with_options(path, options)?,
            pg,
            epoch,
        })
    }

    pub fn append_commit(&mut self, commit: &FabricCommit) -> RsfResult<DurableProtocolCommit> {
        self.append_record(ProtocolCommitRecord::from_commit(commit))
    }

    pub fn append_client_request(
        &mut self,
        commit: &FabricCommit,
        request: ClientRequestId,
    ) -> RsfResult<DurableProtocolCommit> {
        self.append_record(ProtocolCommitRecord::from_client_request(commit, request))
    }

    pub fn set_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
    }

    fn append_record(&mut self, record: ProtocolCommitRecord) -> RsfResult<DurableProtocolCommit> {
        let commit_pg = record.certificate.pg;
        if commit_pg != self.pg {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "commit for pg {} cannot be appended to protocol wal for pg {}",
                    commit_pg.0, self.pg.0
                ),
            ));
        }

        let payload = record.encode();
        let recovery = self.store.recover_current()?.recovery;
        let expected_logical_index = recovery
            .wal_records
            .iter()
            .filter(|wal| {
                wal.pg == self.pg
                    && wal.kind == WalRecordKind::Commit
                    && ProtocolCommitRecord::is_protocol_payload(&wal.payload)
            })
            .map(|wal| ProtocolCommitRecord::decode(&wal.payload))
            .collect::<RsfResult<Vec<_>>>()?
            .into_iter()
            .map(|commit| commit.certificate.index.0)
            .max()
            .unwrap_or(0)
            + 1;
        if record.certificate.index.0 != expected_logical_index {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "protocol commit index {} does not match next logical commit index {expected_logical_index} for pg {}",
                    record.certificate.index.0, self.pg.0
                ),
            ));
        }

        let tail = recovery.pg_tail(self.pg);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let storage_index = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
        let (segment_id, offset, wal_record) = self.store.append_wal(WalAppend {
            pg: self.pg,
            epoch: self.epoch,
            index: storage_index,
            kind: WalRecordKind::Commit,
            previous_hash,
            payload: &payload,
        })?;

        Ok(DurableProtocolCommit {
            segment_id,
            offset,
            certificate: record.certificate,
            client_request: record.client_request,
            storage_index,
            wal_record_hash: wal_record.record_hash,
        })
    }

    pub fn recover(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
    ) -> RsfResult<Vec<RecoveredProtocolCommit>> {
        SegmentStore::recover(path)?
            .recovery
            .wal_records
            .into_iter()
            .filter(|record| {
                record.pg == pg
                    && record.kind == WalRecordKind::Commit
                    && ProtocolCommitRecord::is_protocol_payload(&record.payload)
            })
            .map(RecoveredProtocolCommit::try_from_wal)
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableProtocolCommit {
    pub segment_id: u64,
    pub offset: u64,
    pub certificate: CommitCertificate,
    pub client_request: Option<ClientRequestId>,
    pub storage_index: LogIndex,
    pub wal_record_hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredProtocolCommit {
    pub certificate: CommitCertificate,
    pub client_request: Option<ClientRequestId>,
    pub storage_index: LogIndex,
    pub wal_record_hash: [u8; 32],
}

impl RecoveredProtocolCommit {
    fn try_from_wal(record: WalRecord) -> RsfResult<Self> {
        let protocol_record = ProtocolCommitRecord::decode(&record.payload)?;
        if protocol_record.certificate.pg != record.pg {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "protocol commit payload pg {} does not match wal pg {}",
                    protocol_record.certificate.pg.0, record.pg.0
                ),
            ));
        }
        Ok(Self {
            certificate: protocol_record.certificate,
            client_request: protocol_record.client_request,
            storage_index: record.index,
            wal_record_hash: record.record_hash,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementMap {
    pub version: u64,
    pub groups: Vec<PlacementGroupId>,
}

impl PlacementMap {
    pub fn new(
        version: u64,
        groups: impl IntoIterator<Item = PlacementGroupId>,
    ) -> RsfResult<Self> {
        let map = Self {
            version,
            groups: groups.into_iter().collect(),
        };
        map.validate()?;
        Ok(map)
    }

    pub fn route(&self, operation: &FabricOperation) -> PlacementGroupId {
        let slot = (operation.operation_id().0 as usize) % self.groups.len();
        self.groups[slot]
    }

    fn validate(&self) -> RsfResult<()> {
        if self.version == 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "placement map version must be non-zero",
            ));
        }
        if self.groups.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "placement map must contain at least one placement group",
            ));
        }
        if self.groups.contains(&METADATA_PG) {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "placement group 0 is reserved for protocol metadata",
            ));
        }
        Ok(())
    }

    fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        let group_count = u64::try_from(self.groups.len()).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "placement map group count does not fit in record",
            )
        })?;
        let mut out = Vec::with_capacity(5 + 8 + 8 + self.groups.len() * 8);
        out.extend_from_slice(PLACEMENT_MAP_MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&group_count.to_le_bytes());
        for group in &self.groups {
            out.extend_from_slice(&group.0.to_le_bytes());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if !bytes.starts_with(PLACEMENT_MAP_MAGIC) {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "placement map record has invalid magic",
            ));
        }
        if bytes.len() < 5 + 8 + 8 {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "placement map record is shorter than header",
            ));
        }
        let version = read_u64(bytes, 5)?;
        let group_count = usize::try_from(read_u64(bytes, 13)?).map_err(|_| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "placement map group count does not fit in memory",
            )
        })?;
        let expected_len = 21 + group_count * 8;
        if bytes.len() != expected_len {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "placement map record length mismatch",
            ));
        }
        let mut groups = Vec::with_capacity(group_count);
        for idx in 0..group_count {
            groups.push(PlacementGroupId(read_u64(bytes, 21 + idx * 8)?));
        }
        Self::new(version, groups)
    }

    fn is_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(PLACEMENT_MAP_MAGIC)
    }
}

fn recover_placement_map(path: impl AsRef<Path>) -> RsfResult<Option<PlacementMap>> {
    let mut latest = None;
    for record in SegmentStore::recover(path)?.recovery.wal_records {
        if record.pg == METADATA_PG
            && record.kind == WalRecordKind::Membership
            && PlacementMap::is_payload(&record.payload)
        {
            let map = PlacementMap::decode(&record.payload)?;
            if latest
                .as_ref()
                .is_none_or(|existing: &PlacementMap| map.version > existing.version)
            {
                latest = Some(map);
            }
        }
    }
    Ok(latest)
}

fn recover_or_initialize_placement_map(
    path: impl AsRef<Path>,
    groups: impl IntoIterator<Item = PlacementGroupId>,
    epoch: Epoch,
) -> RsfResult<PlacementMap> {
    let path = path.as_ref();
    if let Some(map) = recover_placement_map(path)? {
        return Ok(map);
    }
    let map = PlacementMap::new(1, groups)?;
    append_placement_map(path, epoch, &map)?;
    Ok(map)
}

fn append_placement_map(path: impl AsRef<Path>, epoch: Epoch, map: &PlacementMap) -> RsfResult<()> {
    append_placement_map_with_options(path, epoch, map, SegmentStoreOptions::default())
}

pub(crate) fn append_placement_map_with_options(
    path: impl AsRef<Path>,
    epoch: Epoch,
    map: &PlacementMap,
    options: SegmentStoreOptions,
) -> RsfResult<()> {
    let path = path.as_ref();
    let mut store = SegmentStore::open_with_options(path, options)?;
    let recovery = store.recover_current()?.recovery;
    let tail = recovery.pg_tail(METADATA_PG);
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    let index = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
    let payload = map.encode()?;
    store.append_wal(WalAppend {
        pg: METADATA_PG,
        epoch,
        index,
        kind: WalRecordKind::Membership,
        previous_hash,
        payload: &payload,
    })?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolPolicy {
    pub version: u64,
    pub allow_availability_first_immutable: bool,
    pub min_placement_groups: u64,
}

impl ProtocolPolicy {
    pub fn new(
        version: u64,
        allow_availability_first_immutable: bool,
        min_placement_groups: u64,
    ) -> RsfResult<Self> {
        let policy = Self {
            version,
            allow_availability_first_immutable,
            min_placement_groups,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn default() -> Self {
        Self {
            version: 1,
            allow_availability_first_immutable: true,
            min_placement_groups: 1,
        }
    }

    fn validate(&self) -> RsfResult<()> {
        if self.version == 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "policy version must be non-zero",
            ));
        }
        if self.min_placement_groups == 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "policy min_placement_groups must be at least one",
            ));
        }
        Ok(())
    }

    fn validate_commit(&self, mode: ConsistencyMode, operation: &FabricOperation) -> RsfResult<()> {
        if mode == ConsistencyMode::AvailabilityFirst {
            if operation.requires_strict() {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "policy rejects availability-first mode for mutable truth operations",
                ));
            }
            if !self.allow_availability_first_immutable {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "policy disables availability-first commits, including immutable payload commits",
                ));
            }
        }
        Ok(())
    }

    fn validate_placement_map(&self, map: &PlacementMap) -> RsfResult<()> {
        let groups = u64::try_from(map.groups.len()).map_err(|_| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "placement map group count does not fit policy check",
            )
        })?;
        if groups < self.min_placement_groups {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "policy requires at least {} placement groups, proposed map has {}",
                    self.min_placement_groups, groups
                ),
            ));
        }
        Ok(())
    }

    fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        let mut out = Vec::with_capacity(5 + 8 + 1 + 8);
        out.extend_from_slice(PROTOCOL_POLICY_MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.push(u8::from(self.allow_availability_first_immutable));
        out.extend_from_slice(&self.min_placement_groups.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if !bytes.starts_with(PROTOCOL_POLICY_MAGIC) {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "protocol policy record has invalid magic",
            ));
        }
        if bytes.len() != 5 + 8 + 1 + 8 {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "protocol policy record length mismatch",
            ));
        }
        let allow = match bytes[13] {
            0 => false,
            1 => true,
            value => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unknown protocol policy availability-first flag {value}"),
                ));
            }
        };
        Self::new(read_u64(bytes, 5)?, allow, read_u64(bytes, 14)?)
    }

    fn is_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(PROTOCOL_POLICY_MAGIC)
    }
}

fn recover_protocol_policy(path: impl AsRef<Path>) -> RsfResult<Option<ProtocolPolicy>> {
    let mut latest = None;
    for record in SegmentStore::recover(path)?.recovery.wal_records {
        if record.pg == METADATA_PG
            && record.kind == WalRecordKind::Membership
            && ProtocolPolicy::is_payload(&record.payload)
        {
            let policy = ProtocolPolicy::decode(&record.payload)?;
            if latest
                .as_ref()
                .is_none_or(|existing: &ProtocolPolicy| policy.version > existing.version)
            {
                latest = Some(policy);
            }
        }
    }
    Ok(latest)
}

fn recover_or_initialize_protocol_policy(
    path: impl AsRef<Path>,
    epoch: Epoch,
) -> RsfResult<ProtocolPolicy> {
    let path = path.as_ref();
    if let Some(policy) = recover_protocol_policy(path)? {
        return Ok(policy);
    }
    let policy = ProtocolPolicy::default();
    append_protocol_policy(path, epoch, &policy)?;
    Ok(policy)
}

fn append_protocol_policy(
    path: impl AsRef<Path>,
    epoch: Epoch,
    policy: &ProtocolPolicy,
) -> RsfResult<()> {
    append_protocol_policy_with_options(path, epoch, policy, SegmentStoreOptions::default())
}

pub(crate) fn append_protocol_policy_with_options(
    path: impl AsRef<Path>,
    epoch: Epoch,
    policy: &ProtocolPolicy,
    options: SegmentStoreOptions,
) -> RsfResult<()> {
    let path = path.as_ref();
    let mut store = SegmentStore::open_with_options(path, options)?;
    let recovery = store.recover_current()?.recovery;
    let tail = recovery.pg_tail(METADATA_PG);
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    let index = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
    let payload = policy.encode()?;
    store.append_wal(WalAppend {
        pg: METADATA_PG,
        epoch,
        index,
        kind: WalRecordKind::Membership,
        previous_hash,
        payload: &payload,
    })?;
    Ok(())
}

#[derive(Debug)]
pub struct DurableFabricRouter {
    path: PathBuf,
    epoch: Epoch,
    wals: BTreeMap<PlacementGroupId, ProtocolWal>,
    next_indexes: BTreeMap<PlacementGroupId, u64>,
    next_version: u64,
    request_certificates: BTreeMap<ClientRequestId, CommitCertificate>,
}

impl DurableFabricRouter {
    pub fn open(
        path: impl AsRef<Path>,
        groups: impl IntoIterator<Item = PlacementGroupId>,
        epoch: Epoch,
    ) -> RsfResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut wals = BTreeMap::new();
        let mut next_indexes = BTreeMap::new();
        let mut next_version = 1;
        let mut request_certificates = BTreeMap::new();

        for pg in groups {
            let recovered = ProtocolWal::recover(&path, pg)?;
            let mut max_index = 0;
            for commit in recovered {
                max_index = max_index.max(commit.certificate.index.0);
                next_version = next_version.max(commit.certificate.version.0 + 1);
                if let Some(request) = commit.client_request {
                    request_certificates.insert(request, commit.certificate);
                }
            }
            next_indexes.insert(pg, max_index + 1);
            wals.insert(pg, ProtocolWal::open(&path, pg, epoch)?);
        }

        Ok(Self {
            path,
            epoch,
            wals,
            next_indexes,
            next_version,
            request_certificates,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn commit(
        &mut self,
        pg: PlacementGroupId,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        self.commit_with_mode(pg, ConsistencyMode::Strict, operation)
    }

    pub fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        if mode == ConsistencyMode::AvailabilityFirst && operation.requires_strict() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "availability-first mode is only valid for immutable/verifiable payload operations",
            ));
        }
        let commit = self.next_commit(pg, mode, operation)?;
        self.append_existing_commit(commit)
    }

    pub fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<DurableRequestCommit> {
        if let Some(certificate) = self.request_certificates.get(&request).cloned() {
            return Ok(DurableRequestCommit {
                certificate,
                replayed: true,
            });
        }

        if mode == ConsistencyMode::AvailabilityFirst && operation.requires_strict() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "availability-first mode is only valid for immutable/verifiable payload operations",
            ));
        }
        let commit = self.next_commit(pg, mode, operation)?;
        self.append_existing_request(commit, request)
    }

    pub fn latest_version(&self) -> CommitVersion {
        CommitVersion(self.next_version.saturating_sub(1))
    }

    pub fn recovered_certificates(
        &self,
        pg: PlacementGroupId,
    ) -> RsfResult<Vec<CommitCertificate>> {
        Ok(ProtocolWal::recover(&self.path, pg)?
            .into_iter()
            .map(|commit| commit.certificate)
            .collect())
    }

    fn next_commit(
        &self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        let index = self.next_indexes.get(&pg).copied().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            )
        })?;
        let operation_id = operation.operation_id();
        Ok(FabricCommit {
            pg,
            index: LogIndex(index),
            version: CommitVersion(self.next_version),
            mode,
            operation_id,
            operation,
        })
    }

    fn next_commit_at(
        &self,
        pg: PlacementGroupId,
        index: LogIndex,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        let next_index = self.next_indexes.get(&pg).copied().ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            )
        })?;
        if index.0 != next_index {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!(
                    "recovered vsr request index {} does not match next protocol index {next_index} for pg {}",
                    index.0, pg.0
                ),
            ));
        }
        self.next_commit(pg, mode, operation)
    }

    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate> {
        self.request_certificates.get(request).cloned()
    }

    fn append_existing_commit(&mut self, commit: FabricCommit) -> RsfResult<FabricCommit> {
        self.wal_mut(commit.pg)?.append_commit(&commit)?;
        self.advance_after_commit(&commit);
        Ok(commit)
    }

    fn append_existing_request(
        &mut self,
        commit: FabricCommit,
        request: ClientRequestId,
    ) -> RsfResult<DurableRequestCommit> {
        self.wal_mut(commit.pg)?
            .append_client_request(&commit, request.clone())?;
        self.advance_after_commit(&commit);
        let certificate = CommitCertificate::from(&commit);
        self.request_certificates
            .insert(request, certificate.clone());

        Ok(DurableRequestCommit {
            certificate,
            replayed: false,
        })
    }

    fn ensure_group(&mut self, pg: PlacementGroupId) -> RsfResult<()> {
        if self.wals.contains_key(&pg) {
            return Ok(());
        }
        let recovered = ProtocolWal::recover(&self.path, pg)?;
        let mut max_index = 0;
        for commit in recovered {
            max_index = max_index.max(commit.certificate.index.0);
            self.next_version = self.next_version.max(commit.certificate.version.0 + 1);
            if let Some(request) = commit.client_request {
                self.request_certificates
                    .insert(request, commit.certificate);
            }
        }
        self.next_indexes.insert(pg, max_index + 1);
        self.wals
            .insert(pg, ProtocolWal::open(&self.path, pg, self.epoch)?);
        Ok(())
    }

    fn set_group_epoch(&mut self, pg: PlacementGroupId, epoch: Epoch) -> RsfResult<()> {
        self.ensure_group(pg)?;
        self.wal_mut(pg)?.set_epoch(epoch);
        Ok(())
    }

    fn wal_mut(&mut self, pg: PlacementGroupId) -> RsfResult<&mut ProtocolWal> {
        self.wals.get_mut(&pg).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            )
        })
    }

    fn advance_after_commit(&mut self, commit: &FabricCommit) {
        self.next_indexes.insert(commit.pg, commit.index.0 + 1);
        self.next_version = commit.version.0 + 1;
    }
}

impl FabricRouterPort for DurableFabricRouter {
    fn commit(
        &mut self,
        pg: PlacementGroupId,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        Self::commit(self, pg, operation)
    }

    fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        Self::commit_with_mode(self, pg, mode, operation)
    }

    fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        self.commit_request_with_mode(pg, request, ConsistencyMode::Strict, operation)
    }

    fn commit_request_with_mode(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        let result = Self::commit_request(self, pg, request, mode, operation)?;
        Ok(RequestCommitEvidence {
            certificate: result.certificate,
            replayed: result.replayed,
        })
    }

    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate> {
        Self::request_certificate(self, request)
    }

    fn latest_version(&self) -> CommitVersion {
        Self::latest_version(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRequestCommit {
    pub certificate: CommitCertificate,
    pub replayed: bool,
}

#[derive(Debug)]
pub struct VsrBackedFabricRouter<T: VsrTransport = LocalVsrTransport> {
    path: PathBuf,
    epoch: Epoch,
    router: DurableFabricRouter,
    cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
    placement: PlacementMap,
    policy: ProtocolPolicy,
    primary: NodeId,
    replicas: Vec<NodeId>,
    transport: T,
}

impl VsrBackedFabricRouter<LocalVsrTransport> {
    pub fn open(
        path: impl AsRef<Path>,
        groups: impl IntoIterator<Item = PlacementGroupId>,
        epoch: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId> + Clone,
    ) -> RsfResult<Self> {
        let transport = LocalVsrTransport::all_reachable(replicas.clone());
        Self::open_with_transport(path, groups, epoch, primary, replicas, transport)
    }
}

impl<T: VsrTransport> VsrBackedFabricRouter<T> {
    pub fn open_with_transport(
        path: impl AsRef<Path>,
        groups: impl IntoIterator<Item = PlacementGroupId>,
        epoch: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId> + Clone,
        transport: T,
    ) -> RsfResult<Self> {
        let path = path.as_ref().to_path_buf();
        let placement = recover_or_initialize_placement_map(&path, groups, epoch)?;
        let policy = recover_or_initialize_protocol_policy(&path, epoch)?;
        policy.validate_placement_map(&placement)?;
        let replicas_vec = replicas.clone().into_iter().collect::<Vec<_>>();
        let router = DurableFabricRouter::open(&path, placement.groups.iter().copied(), epoch)?;
        let mut cells = BTreeMap::new();
        for pg in placement.groups.iter().copied() {
            cells.insert(
                pg,
                DurableVsrCell::open(
                    vsr_cell_path(&path, pg),
                    pg,
                    epoch,
                    primary,
                    replicas.clone(),
                )?,
            );
        }

        Ok(Self {
            path,
            epoch,
            router,
            cells,
            placement,
            policy,
            primary,
            replicas: replicas_vec,
            transport,
        })
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn placement_map(&self) -> &PlacementMap {
        &self.placement
    }

    pub fn policy(&self) -> &ProtocolPolicy {
        &self.policy
    }

    pub fn commit(
        &mut self,
        pg: PlacementGroupId,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        self.commit_with_mode(pg, ConsistencyMode::Strict, operation)
    }

    pub fn commit_routed(&mut self, operation: FabricOperation) -> RsfResult<FabricCommit> {
        self.commit_routed_with_mode(ConsistencyMode::Strict, operation)
    }

    pub fn commit_routed_with_mode(
        &mut self,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        self.policy.validate_commit(mode, &operation)?;
        let pg = self.placement.route(&operation);
        self.commit_with_mode(pg, mode, operation)
    }

    pub fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        self.policy.validate_commit(mode, &operation)?;
        let preview = self.router.next_commit(pg, mode, operation)?;
        self.require_vsr_commit(&preview)?;
        self.sync_protocol_epoch(pg)?;
        self.router.append_existing_commit(preview)
    }

    pub fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<DurableRequestCommit> {
        if let Some(certificate) = self.router.request_certificate(&request) {
            return Ok(DurableRequestCommit {
                certificate,
                replayed: true,
            });
        }
        self.policy.validate_commit(mode, &operation)?;

        let vsr_request = VsrClientRequestId::new(request.client.0.clone(), request.request.0);
        if let Some(index) = self.vsr_request_commit(pg, &vsr_request)? {
            let commit = self.router.next_commit_at(pg, index, mode, operation)?;
            self.require_vsr_commit(&commit)?;
            self.sync_protocol_epoch(pg)?;
            return self.router.append_existing_request(commit, request);
        }

        let commit = self.router.next_commit(pg, mode, operation)?;
        self.require_vsr_commit(&commit)?;
        self.remember_vsr_request(pg, vsr_request, commit.index)?;
        self.sync_protocol_epoch(pg)?;
        self.router.append_existing_request(commit, request)
    }

    pub fn commit_request_routed(
        &mut self,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<DurableRequestCommit> {
        if let Some(certificate) = self.router.request_certificate(&request) {
            return Ok(DurableRequestCommit {
                certificate,
                replayed: true,
            });
        }
        self.policy.validate_commit(mode, &operation)?;
        let pg = self.placement.route(&operation);
        self.commit_request(pg, request, mode, operation)
    }

    pub fn update_placement_map(
        &mut self,
        groups: impl IntoIterator<Item = PlacementGroupId>,
    ) -> RsfResult<PlacementMap> {
        let next = PlacementMap::new(self.placement.version + 1, groups)?;
        self.policy.validate_placement_map(&next)?;
        append_placement_map(&self.path, self.epoch, &next)?;
        for pg in next.groups.iter().copied() {
            self.router.ensure_group(pg)?;
            self.ensure_cell(pg)?;
        }
        self.placement = next.clone();
        Ok(next)
    }

    pub fn update_policy(
        &mut self,
        allow_availability_first_immutable: bool,
        min_placement_groups: u64,
    ) -> RsfResult<ProtocolPolicy> {
        let next = ProtocolPolicy::new(
            self.policy.version + 1,
            allow_availability_first_immutable,
            min_placement_groups,
        )?;
        next.validate_placement_map(&self.placement)?;
        append_protocol_policy(&self.path, self.epoch, &next)?;
        self.policy = next.clone();
        Ok(next)
    }

    pub fn latest_version(&self) -> CommitVersion {
        self.router.latest_version()
    }

    pub fn recovered_certificates(
        &self,
        pg: PlacementGroupId,
    ) -> RsfResult<Vec<CommitCertificate>> {
        self.router.recovered_certificates(pg)
    }

    pub fn view_change(
        &mut self,
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
    ) -> VsrDecision {
        let Some(cell) = self.cells.get_mut(&pg) else {
            return VsrDecision::Rejected {
                node: primary,
                reason: format!("placement group {} is not mapped", pg.0),
            };
        };
        cell.view_change(view, primary)
    }

    pub fn timeout_election(&mut self, pg: PlacementGroupId) -> VsrDecision {
        let Some(cell) = self.cells.get_mut(&pg) else {
            return VsrDecision::Rejected {
                node: self.primary,
                reason: format!("placement group {} is not mapped", pg.0),
            };
        };
        cell.timeout_election()
    }

    pub fn reconfigure(
        &mut self,
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
    ) -> VsrDecision {
        let Some(cell) = self.cells.get_mut(&pg) else {
            return VsrDecision::Rejected {
                node: primary,
                reason: format!("placement group {} is not mapped", pg.0),
            };
        };
        let decision = cell.reconfigure(view, primary, replicas);
        if matches!(decision, VsrDecision::MembershipChanged { .. }) {
            self.primary = primary;
            self.sync_replicas_from_pg(pg);
        }
        decision
    }

    #[cfg(test)]
    pub(crate) fn remember_vsr_request_for_test(
        &mut self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        index: LogIndex,
    ) -> RsfResult<()> {
        self.remember_vsr_request(
            pg,
            VsrClientRequestId::new(request.client.0.clone(), request.request.0),
            index,
        )
    }

    #[cfg(test)]
    pub(crate) fn commit_vsr_request_without_protocol_certificate_for_test(
        &mut self,
        pg: PlacementGroupId,
        request: &ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        let commit = self.router.next_commit(pg, mode, operation)?;
        self.require_vsr_commit(&commit)?;
        self.remember_vsr_request_for_test(pg, request, commit.index)?;
        Ok(commit)
    }

    fn sync_protocol_epoch(&mut self, pg: PlacementGroupId) -> RsfResult<()> {
        let Some(cell) = self.cells.get(&pg) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            ));
        };
        self.router.set_group_epoch(pg, cell.view())
    }

    fn require_vsr_commit(&mut self, commit: &FabricCommit) -> RsfResult<()> {
        let expected = fabric_to_vsr_operation(&commit.operation, commit.operation_id);
        self.sync_replicas_from_pg(commit.pg);
        let Some(cell) = self.cells.get_mut(&commit.pg) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", commit.pg.0),
            ));
        };
        if self.primary != cell.primary() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "local node {} is not primary {} for pg {}",
                    self.primary.0,
                    cell.primary().0,
                    commit.pg.0
                ),
            ));
        }

        if cell.committed_operation(commit.index) == Some(&expected) {
            return Ok(());
        }

        let prepare = match cell.prepared(commit.index).cloned() {
            Some(prepare) if prepare.operation == expected => prepare,
            Some(_) => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "durable vsr prepare at index {} does not match protocol operation for pg {}",
                        commit.index.0, commit.pg.0
                    ),
                ));
            }
            None => prepare_vsr_operation(cell, self.primary, commit, expected.clone())?,
        };

        for replica in self
            .replicas
            .iter()
            .copied()
            .filter(|replica| *replica != self.primary)
        {
            let Ok(replica) = self.transport.acknowledge_prepare(VsrPrepareMessage {
                prepare: prepare.clone(),
                replica,
            }) else {
                continue;
            };
            let decision = cell.acknowledge(replica, cell.view(), commit.index);
            if matches!(
                decision,
                VsrDecision::Acknowledged {
                    acknowledgements,
                    ..
                } if acknowledgements >= cell.quorum()
            ) {
                break;
            }
        }

        match cell.commit(self.primary, commit.index)? {
            VsrDecision::Committed(committed) if committed.index == commit.index => Ok(()),
            VsrDecision::Rejected { reason, .. } => {
                Err(RsfError::new(ErrorCode::InvalidInput, reason))
            }
            _ => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "unexpected vsr commit decision",
            )),
        }
    }

    fn sync_replicas_from_pg(&mut self, pg: PlacementGroupId) {
        if let Some(cell) = self.cells.get(&pg) {
            self.replicas = cell.replicas();
        }
    }

    fn ensure_cell(&mut self, pg: PlacementGroupId) -> RsfResult<()> {
        if self.cells.contains_key(&pg) {
            return Ok(());
        }
        self.cells.insert(
            pg,
            DurableVsrCell::open(
                vsr_cell_path(&self.path, pg),
                pg,
                self.epoch,
                self.primary,
                self.replicas.iter().copied(),
            )?,
        );
        Ok(())
    }

    fn vsr_request_commit(
        &self,
        pg: PlacementGroupId,
        request: &VsrClientRequestId,
    ) -> RsfResult<Option<LogIndex>> {
        let Some(cell) = self.cells.get(&pg) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            ));
        };
        Ok(cell.request_commit(request))
    }

    fn remember_vsr_request(
        &mut self,
        pg: PlacementGroupId,
        request: VsrClientRequestId,
        index: LogIndex,
    ) -> RsfResult<()> {
        let Some(cell) = self.cells.get_mut(&pg) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            ));
        };
        cell.remember_client_request(request, index)
    }
}

impl<T: VsrTransport> FabricRouterPort for VsrBackedFabricRouter<T> {
    fn route_operation(&self, operation: &FabricOperation) -> Option<PlacementGroupId> {
        Some(self.placement.route(operation))
    }

    fn commit(
        &mut self,
        pg: PlacementGroupId,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        Self::commit(self, pg, operation)
    }

    fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit> {
        Self::commit_with_mode(self, pg, mode, operation)
    }

    fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        self.commit_request_with_mode(pg, request, ConsistencyMode::Strict, operation)
    }

    fn commit_request_with_mode(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence> {
        let result = Self::commit_request(self, pg, request, mode, operation)?;
        Ok(RequestCommitEvidence {
            certificate: result.certificate,
            replayed: result.replayed,
        })
    }

    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate> {
        self.router.request_certificate(request)
    }

    fn latest_version(&self) -> CommitVersion {
        Self::latest_version(self)
    }
}

fn vsr_cell_path(path: &Path, pg: PlacementGroupId) -> PathBuf {
    path.with_extension("vsr").join(format!("pg-{}", pg.0))
}

fn fabric_to_vsr_operation(operation: &FabricOperation, operation_id: OperationId) -> VsrOperation {
    match operation {
        FabricOperation::CatalogMutation { namespace, item } => VsrOperation::CatalogMutation {
            namespace: namespace.clone(),
            item: item.clone(),
        },
        FabricOperation::TransactionCommit { .. } => {
            VsrOperation::ProtocolOperation { operation_id }
        }
        FabricOperation::RelationalCatalog { table, item } => VsrOperation::CatalogMutation {
            namespace: table.clone(),
            item: item.clone(),
        },
        FabricOperation::RelationalMutation { table, key } => VsrOperation::TransactionCommit {
            key: format!("{table}/{}", hex_key(key)),
        },
        FabricOperation::KvMutation { key } => {
            VsrOperation::TransactionCommit { key: hex_key(key) }
        }
        FabricOperation::KvBatch { .. } => VsrOperation::ProtocolOperation { operation_id },
        FabricOperation::ObjectBucket { bucket } => VsrOperation::ObjectManifest {
            bucket: bucket.clone(),
            key: String::new(),
        },
        FabricOperation::ObjectMultipartStart { .. }
        | FabricOperation::ObjectMultipartPart { .. }
        | FabricOperation::ObjectHead { .. }
        | FabricOperation::ObjectManifest { .. }
        | FabricOperation::BlobPin { .. } => VsrOperation::ProtocolOperation { operation_id },
    }
}

fn prepare_vsr_operation(
    cell: &mut DurableVsrCell,
    primary: NodeId,
    commit: &FabricCommit,
    expected: VsrOperation,
) -> RsfResult<VsrPrepare> {
    match cell.prepare(primary, expected, operation_bytes(&commit.operation))? {
        VsrDecision::Prepared(prepare) if prepare.index == commit.index => Ok(prepare),
        VsrDecision::Prepared(prepare) => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!(
                "vsr prepared index {} but protocol expected index {} for pg {}",
                prepare.index.0, commit.index.0, commit.pg.0
            ),
        )),
        VsrDecision::Rejected { reason, .. } => Err(RsfError::new(ErrorCode::InvalidInput, reason)),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "unexpected vsr prepare decision",
        )),
    }
}

fn operation_bytes(operation: &FabricOperation) -> u32 {
    match operation {
        FabricOperation::CatalogMutation { namespace, item } => {
            namespace.len().saturating_add(item.len())
        }
        FabricOperation::TransactionCommit { transaction } => transaction.len(),
        FabricOperation::RelationalCatalog { table, item } => {
            table.len().saturating_add(item.len())
        }
        FabricOperation::RelationalMutation { table, key } => table.len().saturating_add(key.len()),
        FabricOperation::KvMutation { key } => key.len(),
        FabricOperation::KvBatch { keys } => keys.iter().map(Vec::len).sum(),
        FabricOperation::ObjectBucket { bucket } => bucket.len(),
        FabricOperation::ObjectMultipartStart { bucket, key, .. }
        | FabricOperation::ObjectMultipartPart { bucket, key, .. }
        | FabricOperation::ObjectHead { bucket, key }
        | FabricOperation::ObjectManifest { bucket, key } => bucket.len().saturating_add(key.len()),
        FabricOperation::BlobPin { digest } => digest.len(),
    }
    .max(1)
    .try_into()
    .unwrap_or(u32::MAX)
}

fn hex_key(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn read_u64(bytes: &[u8], start: usize) -> RsfResult<u64> {
    let end = start + 8;
    let value = bytes.get(start..end).ok_or_else(|| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "protocol commit record ended while reading u64",
        )
    })?;
    Ok(u64::from_le_bytes(value.try_into().expect("slice length")))
}
