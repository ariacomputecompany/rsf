#![forbid(unsafe_code)]

mod durable;
mod profile;

use std::collections::BTreeMap;

use rsf_core::{
    CommitVersion, ErrorCode, LogIndex, OperationId, PlacementGroupId, RsfError, RsfResult,
};

pub use durable::{
    ApplicationPayload, ApplicationSurface, DurableFabricRouter, DurableProtocolCommit,
    DurableRequestCommit, PlacementMap, ProtocolCommitRecord, ProtocolPolicy, ProtocolWal,
    RecoveredProtocolCommit, ReplicatedFabricCommand, VsrBackedFabricRouter,
};
pub use profile::{
    BorrowedProtocolPart, CustomProtocolProfile, ProtocolSpine, custom_protocol_profile,
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientId(pub String);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId(pub u64);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientRequestId {
    pub client: ClientId,
    pub request: RequestId,
}

impl ClientRequestId {
    pub fn new(client: impl Into<String>, request: u64) -> Self {
        Self {
            client: ClientId(client.into()),
            request: RequestId(request),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsistencyMode {
    Strict,
    AvailabilityFirst,
}

impl ConsistencyMode {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Strict => 1,
            Self::AvailabilityFirst => 2,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Strict),
            2 => Some(Self::AvailabilityFirst),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FabricCommit {
    pub pg: PlacementGroupId,
    pub index: LogIndex,
    pub version: CommitVersion,
    pub mode: ConsistencyMode,
    pub operation_id: OperationId,
    pub operation: FabricOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FabricOperation {
    CatalogMutation {
        namespace: String,
        item: String,
    },
    TransactionCommit {
        transaction: [u8; 16],
    },
    RelationalCatalog {
        table: String,
        item: String,
    },
    RelationalMutation {
        table: String,
        key: Vec<u8>,
    },
    KvMutation {
        key: Vec<u8>,
    },
    KvBatch {
        keys: Vec<Vec<u8>>,
    },
    ObjectBucket {
        bucket: String,
    },
    ObjectMultipartStart {
        bucket: String,
        key: String,
        upload_id: u64,
    },
    ObjectMultipartPart {
        bucket: String,
        key: String,
        upload_id: u64,
        part_number: u16,
    },
    ObjectHead {
        bucket: String,
        key: String,
    },
    ObjectManifest {
        bucket: String,
        key: String,
    },
    BlobPin {
        digest: [u8; 32],
    },
}

impl FabricOperation {
    pub fn requires_strict(&self) -> bool {
        match self {
            Self::BlobPin { .. } => false,
            Self::CatalogMutation { .. }
            | Self::TransactionCommit { .. }
            | Self::RelationalCatalog { .. }
            | Self::RelationalMutation { .. }
            | Self::KvMutation { .. }
            | Self::KvBatch { .. }
            | Self::ObjectBucket { .. }
            | Self::ObjectMultipartStart { .. }
            | Self::ObjectMultipartPart { .. }
            | Self::ObjectHead { .. }
            | Self::ObjectManifest { .. } => true,
        }
    }

    pub fn operation_id(&self) -> OperationId {
        let mut hash = FNV_OFFSET;
        match self {
            Self::CatalogMutation { namespace, item } => {
                hash_str(&mut hash, "catalog");
                hash_str(&mut hash, namespace);
                hash_str(&mut hash, item);
            }
            Self::TransactionCommit { transaction } => {
                hash_str(&mut hash, "txn");
                hash_bytes(&mut hash, transaction);
            }
            Self::RelationalCatalog { table, item } => {
                hash_str(&mut hash, "relcat");
                hash_str(&mut hash, table);
                hash_str(&mut hash, item);
            }
            Self::RelationalMutation { table, key } => {
                hash_str(&mut hash, "relmut");
                hash_str(&mut hash, table);
                hash_bytes(&mut hash, key);
            }
            Self::KvMutation { key } => {
                hash_str(&mut hash, "kvmut");
                hash_bytes(&mut hash, key);
            }
            Self::KvBatch { keys } => {
                hash_str(&mut hash, "kvbatch");
                for key in keys {
                    hash_bytes(&mut hash, key);
                }
            }
            Self::ObjectBucket { bucket } => {
                hash_str(&mut hash, "bucket");
                hash_str(&mut hash, bucket);
            }
            Self::ObjectMultipartStart {
                bucket,
                key,
                upload_id,
            } => {
                hash_str(&mut hash, "mpu-start");
                hash_str(&mut hash, bucket);
                hash_str(&mut hash, key);
                hash_u64(&mut hash, *upload_id);
            }
            Self::ObjectMultipartPart {
                bucket,
                key,
                upload_id,
                part_number,
            } => {
                hash_str(&mut hash, "mpu-part");
                hash_str(&mut hash, bucket);
                hash_str(&mut hash, key);
                hash_u64(&mut hash, *upload_id);
                hash_u64(&mut hash, u64::from(*part_number));
            }
            Self::ObjectHead { bucket, key } => {
                hash_str(&mut hash, "object-head");
                hash_str(&mut hash, bucket);
                hash_str(&mut hash, key);
            }
            Self::ObjectManifest { bucket, key } => {
                hash_str(&mut hash, "object-manifest");
                hash_str(&mut hash, bucket);
                hash_str(&mut hash, key);
            }
            Self::BlobPin { digest } => {
                hash_str(&mut hash, "blob-pin");
                hash_bytes(&mut hash, digest);
            }
        }
        OperationId(hash)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitCertificate {
    pub pg: PlacementGroupId,
    pub index: LogIndex,
    pub version: CommitVersion,
    pub mode: ConsistencyMode,
    pub operation_id: OperationId,
}

impl From<&FabricCommit> for CommitCertificate {
    fn from(commit: &FabricCommit) -> Self {
        Self {
            pg: commit.pg,
            index: commit.index,
            version: commit.version,
            mode: commit.mode,
            operation_id: commit.operation_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProtocolCell {
    pg: PlacementGroupId,
    next_index: u64,
    committed: Vec<FabricCommit>,
    requests: BTreeMap<ClientRequestId, FabricCommit>,
}

impl ProtocolCell {
    pub fn new(pg: PlacementGroupId) -> Self {
        Self {
            pg,
            next_index: 1,
            committed: Vec::new(),
            requests: BTreeMap::new(),
        }
    }

    pub fn commit(
        &mut self,
        version: CommitVersion,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> FabricCommit {
        let operation_id = operation.operation_id();
        let commit = FabricCommit {
            pg: self.pg,
            index: LogIndex(self.next_index),
            version,
            mode,
            operation_id,
            operation,
        };
        self.next_index += 1;
        self.committed.push(commit.clone());
        commit
    }

    pub fn remember_request(&mut self, request: ClientRequestId, commit: FabricCommit) {
        self.requests.insert(request, commit);
    }

    pub fn request_result(&self, request: &ClientRequestId) -> Option<&FabricCommit> {
        self.requests.get(request)
    }

    pub fn committed(&self) -> &[FabricCommit] {
        &self.committed
    }
}

pub trait FabricRouterPort {
    fn route_operation(&self, _operation: &FabricOperation) -> Option<PlacementGroupId> {
        None
    }

    fn commit(
        &mut self,
        pg: PlacementGroupId,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit>;

    fn commit_with_mode(
        &mut self,
        pg: PlacementGroupId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<FabricCommit>;

    fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence>;

    fn commit_request_with_mode(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommitEvidence>;

    /// Validate a metadata request's placement, policy and existing identity
    /// before persisting an application intent that depends on that request.
    fn validate_request(
        &self,
        _pg: PlacementGroupId,
        _request: &ClientRequestId,
        _mode: ConsistencyMode,
        _operation: &FabricOperation,
    ) -> RsfResult<()> {
        Err(RsfError::new(
            ErrorCode::InvalidInput,
            "router does not support metadata request preflight",
        ))
    }

    /// Validate a complete command before the caller persists its local intent.
    fn validate_application_request(
        &self,
        _pg: PlacementGroupId,
        _request: &ClientRequestId,
        _mode: ConsistencyMode,
        _operation: &FabricOperation,
        _application: &ApplicationPayload,
    ) -> RsfResult<()> {
        Err(RsfError::new(
            ErrorCode::InvalidInput,
            "router does not support complete application commands",
        ))
    }

    fn commit_application_request(
        &mut self,
        _pg: PlacementGroupId,
        _request: ClientRequestId,
        _mode: ConsistencyMode,
        _operation: FabricOperation,
        _application: ApplicationPayload,
    ) -> RsfResult<RequestCommitEvidence> {
        Err(RsfError::new(
            ErrorCode::InvalidInput,
            "router does not support complete application commands",
        ))
    }

    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate>;

    fn latest_version(&self) -> CommitVersion;
}

#[derive(Clone, Debug, Default)]
pub struct FabricRouter {
    cells: BTreeMap<PlacementGroupId, ProtocolCell>,
    next_version: u64,
}

impl FabricRouter {
    pub fn with_groups(groups: impl IntoIterator<Item = PlacementGroupId>) -> Self {
        let mut router = Self::default();
        for group in groups {
            router.cells.insert(group, ProtocolCell::new(group));
        }
        router.next_version = 1;
        router
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
        let Some(cell) = self.cells.get_mut(&pg) else {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("placement group {} is not mapped", pg.0),
            ));
        };
        let version = CommitVersion(self.next_version);
        self.next_version += 1;
        Ok(cell.commit(version, mode, operation))
    }

    pub fn commit_request(
        &mut self,
        pg: PlacementGroupId,
        request: ClientRequestId,
        mode: ConsistencyMode,
        operation: FabricOperation,
    ) -> RsfResult<RequestCommit> {
        let Some(existing) = self
            .cells
            .get(&pg)
            .and_then(|cell| cell.request_result(&request))
            .cloned()
        else {
            let commit = self.commit_with_mode(pg, mode, operation)?;
            let cell = self
                .cells
                .get_mut(&pg)
                .expect("cell was validated by commit");
            cell.remember_request(request, commit.clone());
            return Ok(RequestCommit {
                commit,
                replayed: false,
            });
        };

        Ok(RequestCommit {
            commit: existing,
            replayed: true,
        })
    }

    pub fn latest_version(&self) -> CommitVersion {
        CommitVersion(self.next_version.saturating_sub(1))
    }

    pub fn committed(&self) -> Vec<FabricCommit> {
        self.cells
            .values()
            .flat_map(|cell| cell.committed().iter().cloned())
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestCommit {
    pub commit: FabricCommit,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestCommitEvidence {
    pub certificate: CommitCertificate,
    pub replayed: bool,
}

impl FabricRouterPort for FabricRouter {
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
            certificate: CommitCertificate::from(&result.commit),
            replayed: result.replayed,
        })
    }

    fn request_certificate(&self, request: &ClientRequestId) -> Option<CommitCertificate> {
        self.cells
            .values()
            .find_map(|cell| cell.request_result(request).map(CommitCertificate::from))
    }

    fn latest_version(&self) -> CommitVersion {
        Self::latest_version(self)
    }
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

fn hash_str(hash: &mut u64, value: &str) {
    hash_bytes(hash, value.as_bytes());
}

fn hash_u64(hash: &mut u64, value: u64) {
    hash_bytes(hash, &value.to_le_bytes());
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    hash_u64_len(hash, bytes.len() as u64);
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn hash_u64_len(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::durable::{append_placement_map_with_options, append_protocol_policy_with_options};
    use rsf_core::Epoch;
    use rsf_core::NodeId;
    use rsf_core::storage::WalRecordKind;
    use rsf_storage::{
        SegmentLogOptions, SegmentStore, SegmentStoreOptions, SyncPolicy, WalAppend,
    };
    use rsf_vsr::LocalVsrTransport;

    #[test]
    fn commits_are_monotonic_per_cell() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(7)]);

        let first = router
            .commit(
                PlacementGroupId(7),
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("first commit");
        let second = router
            .commit(
                PlacementGroupId(7),
                FabricOperation::KvMutation { key: b"b".to_vec() },
            )
            .expect("second commit");

        assert_eq!(first.index, LogIndex(1));
        assert_eq!(second.index, LogIndex(2));
        assert_eq!(second.version, CommitVersion(2));
        assert_eq!(second.mode, ConsistencyMode::Strict);
        assert_ne!(first.operation_id, second.operation_id);
    }

    #[test]
    fn commit_versions_are_global_across_cells() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1), PlacementGroupId(2)]);

        let first = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("first commit");
        let second = router
            .commit(
                PlacementGroupId(2),
                FabricOperation::ObjectManifest {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                },
            )
            .expect("second commit");

        assert_eq!(first.index, LogIndex(1));
        assert_eq!(second.index, LogIndex(1));
        assert_eq!(first.version, CommitVersion(1));
        assert_eq!(second.version, CommitVersion(2));
        assert_eq!(router.latest_version(), CommitVersion(2));
    }

    #[test]
    fn client_request_replay_returns_same_commit_without_new_version() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let request = ClientRequestId::new("client-a", 7);

        let first = router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("first request");
        let replay = router
            .commit_request(
                PlacementGroupId(1),
                request,
                ConsistencyMode::Strict,
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("replay request");

        assert!(!first.replayed);
        assert!(replay.replayed);
        assert_eq!(first.commit, replay.commit);
        assert_eq!(router.latest_version(), CommitVersion(1));
        assert_eq!(router.committed().len(), 1);
    }

    #[test]
    fn commits_carry_explicit_consistency_mode() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let commit = router
            .commit_with_mode(
                PlacementGroupId(1),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [1; 32] },
            )
            .expect("commit");

        assert_eq!(commit.mode, ConsistencyMode::AvailabilityFirst);
    }

    #[test]
    fn mutable_truth_cannot_use_availability_first_mode() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let err = router
            .commit_with_mode(
                PlacementGroupId(1),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::ObjectHead {
                    bucket: "media".to_string(),
                    key: "hero.png".to_string(),
                },
            )
            .expect_err("object heads are mutable truth");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(router.latest_version(), CommitVersion(0));
    }

    #[test]
    fn commit_certificate_carries_stable_commit_identity() {
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let commit = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("commit");

        let certificate = CommitCertificate::from(&commit);
        assert_eq!(certificate.pg, PlacementGroupId(1));
        assert_eq!(certificate.index, LogIndex(1));
        assert_eq!(certificate.version, CommitVersion(1));
        assert_eq!(certificate.mode, ConsistencyMode::Strict);
        assert_eq!(certificate.operation_id, commit.operation_id);
    }

    #[test]
    fn protocol_wal_recovers_commit_certificates_after_reopen() {
        let path = test_path("protocol_wal_recovers_commit_certificates_after_reopen");
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let first = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("first commit");
        let second = router
            .commit_with_mode(
                PlacementGroupId(1),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [9; 32] },
            )
            .expect("second commit");

        let mut wal = ProtocolWal::open_with_store_options(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            tiny_store_options(),
        )
        .expect("open wal");
        let first_durable = wal.append_commit(&first).expect("append first");
        let second_durable = wal.append_commit(&second).expect("append second");
        drop(wal);

        assert_ne!(first_durable.segment_id, second_durable.segment_id);
        assert!(
            SegmentStore::recover(&path)
                .expect("recover segmented protocol wal")
                .segments
                .len()
                > 1
        );
        let recovered =
            ProtocolWal::recover(&path, PlacementGroupId(1)).expect("recover protocol wal");
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].certificate, CommitCertificate::from(&first));
        assert_eq!(recovered[1].certificate, CommitCertificate::from(&second));
        assert_eq!(recovered[0].wal_record_hash, first_durable.wal_record_hash);
        assert_eq!(recovered[1].wal_record_hash, second_durable.wal_record_hash);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn protocol_metadata_recovers_placement_and_policy_after_rotation() {
        let path = test_path("protocol_metadata_recovers_placement_and_policy_after_rotation");
        let options = tiny_store_options();
        let first_map = PlacementMap::new(1, [PlacementGroupId(1)]).expect("first placement map");
        let second_map = PlacementMap::new(2, [PlacementGroupId(2), PlacementGroupId(3)])
            .expect("second placement map");
        let first_policy = ProtocolPolicy::new(1, true, 1).expect("first policy");
        let second_policy = ProtocolPolicy::new(2, false, 2).expect("second policy");

        append_placement_map_with_options(&path, Epoch(1), &first_map, options)
            .expect("append first placement");
        append_protocol_policy_with_options(&path, Epoch(1), &first_policy, options)
            .expect("append first policy");
        append_placement_map_with_options(&path, Epoch(1), &second_map, options)
            .expect("append second placement");
        append_protocol_policy_with_options(&path, Epoch(1), &second_policy, options)
            .expect("append second policy");

        assert!(
            SegmentStore::recover(&path)
                .expect("recover segmented metadata")
                .segments
                .len()
                > 1
        );

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(99)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router from metadata");

        assert_eq!(recovered.placement_map(), &second_map);
        assert_eq!(recovered.policy(), &second_policy);

        let _ = std::fs::remove_dir_all(&path);
        let _ = std::fs::remove_dir_all(path.with_extension("vsr"));
    }

    #[test]
    fn protocol_truncation_keeps_certificates_and_metadata_but_drops_uncommitted_tails() {
        let path =
            test_path("protocol_truncation_keeps_certificates_and_metadata_but_drops_uncommitted");
        let options = tiny_store_options();
        let map =
            PlacementMap::new(1, [PlacementGroupId(1), PlacementGroupId(2)]).expect("placement");
        let policy = ProtocolPolicy::new(1, false, 2).expect("policy");
        append_placement_map_with_options(&path, Epoch(1), &map, options)
            .expect("append placement");
        append_protocol_policy_with_options(&path, Epoch(1), &policy, options)
            .expect("append policy");

        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let first = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("first commit");
        let second = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/b".to_vec(),
                },
            )
            .expect("second commit");
        let mut wal =
            ProtocolWal::open_with_store_options(&path, PlacementGroupId(1), Epoch(1), options)
                .expect("open protocol wal");
        wal.append_commit(&first).expect("append first");
        wal.append_commit(&second).expect("append second");
        drop(wal);

        let mut store =
            SegmentStore::open_with_options(&path, options).expect("open segmented store");
        let recovery = store.recover_current().expect("recover store").recovery;
        let protocol_tail = recovery
            .pg_tail(PlacementGroupId(1))
            .expect("protocol tail");
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(protocol_tail.index.0 + 1),
                kind: WalRecordKind::Prepare,
                previous_hash: protocol_tail.record_hash,
                payload: b"uncommitted-protocol-tail",
            })
            .expect("append uncommitted protocol tail");
        let metadata_tail = recovery
            .pg_tail(PlacementGroupId(0))
            .expect("metadata tail");
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(0),
                epoch: Epoch(1),
                index: LogIndex(metadata_tail.index.0 + 1),
                kind: WalRecordKind::Prepare,
                previous_hash: metadata_tail.record_hash,
                payload: b"uncommitted-metadata-tail",
            })
            .expect("append uncommitted metadata tail");

        let report = store
            .truncate_uncommitted_tails()
            .expect("truncate uncommitted tails");
        assert_eq!(report.dropped_wal_records, 2);
        drop(store);

        let certificates =
            ProtocolWal::recover(&path, PlacementGroupId(1)).expect("recover certificates");
        assert_eq!(certificates.len(), 2);
        assert_eq!(certificates[0].certificate, CommitCertificate::from(&first));
        assert_eq!(
            certificates[1].certificate,
            CommitCertificate::from(&second)
        );

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(99)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover router after truncation");
        assert_eq!(recovered.placement_map(), &map);
        assert_eq!(recovered.policy(), &policy);

        let recovered_store = SegmentStore::recover(&path).expect("recover compacted store");
        assert!(
            recovered_store
                .recovery
                .wal_records
                .iter()
                .all(|record| record.payload != b"uncommitted-protocol-tail"
                    && record.payload != b"uncommitted-metadata-tail")
        );

        let _ = std::fs::remove_dir_all(&path);
        let _ = std::fs::remove_dir_all(path.with_extension("vsr"));
    }

    #[test]
    fn protocol_wal_rejects_commit_for_different_placement_group() {
        let path = test_path("protocol_wal_rejects_commit_for_different_placement_group");
        let mut router = FabricRouter::with_groups([PlacementGroupId(2)]);
        let commit = router
            .commit(
                PlacementGroupId(2),
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("commit");

        let mut wal = ProtocolWal::open(&path, PlacementGroupId(1), Epoch(1)).expect("open wal");
        let err = wal
            .append_commit(&commit)
            .expect_err("wrong placement group rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn protocol_wal_rejects_out_of_order_commit_index() {
        let path = test_path("protocol_wal_rejects_out_of_order_commit_index");
        let mut router = FabricRouter::with_groups([PlacementGroupId(1)]);
        let _ = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("first commit");
        let second = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation { key: b"b".to_vec() },
            )
            .expect("second commit");

        let mut wal = ProtocolWal::open(&path, PlacementGroupId(1), Epoch(1)).expect("open wal");
        let err = wal
            .append_commit(&second)
            .expect_err("storage wal rejects skipped index");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn protocol_wal_rejects_corrupt_protocol_payload_on_recovery() {
        let path = test_path("protocol_wal_rejects_corrupt_protocol_payload_on_recovery");
        let mut store = SegmentStore::open(&path).expect("open segment store");
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Commit,
                previous_hash: [0; 32],
                payload: b"RSFP1-corrupt",
            })
            .expect("append corrupt protocol payload");
        drop(store);

        let err = ProtocolWal::recover(&path, PlacementGroupId(1))
            .expect_err("protocol decoder rejects corrupt payload");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_router_recovers_next_indexes_and_global_version() {
        let path = test_path("durable_router_recovers_next_indexes_and_global_version");
        let mut router =
            DurableFabricRouter::open(&path, [PlacementGroupId(1), PlacementGroupId(2)], Epoch(1))
                .expect("open durable router");

        let first = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation { key: b"a".to_vec() },
            )
            .expect("first commit");
        let second = router
            .commit(
                PlacementGroupId(2),
                FabricOperation::KvMutation { key: b"b".to_vec() },
            )
            .expect("second commit");
        drop(router);

        assert_eq!(first.index, LogIndex(1));
        assert_eq!(first.version, CommitVersion(1));
        assert_eq!(second.index, LogIndex(1));
        assert_eq!(second.version, CommitVersion(2));

        let mut recovered =
            DurableFabricRouter::open(&path, [PlacementGroupId(1), PlacementGroupId(2)], Epoch(1))
                .expect("recover durable router");
        assert_eq!(recovered.latest_version(), CommitVersion(2));

        let third = recovered
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation { key: b"c".to_vec() },
            )
            .expect("third commit");

        assert_eq!(third.index, LogIndex(2));
        assert_eq!(third.version, CommitVersion(3));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("pg1 certificates")
                .len(),
            2
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn durable_router_replays_client_request_after_restart() {
        let path = test_path("durable_router_replays_client_request_after_restart");
        let request = ClientRequestId::new("client-a", 42);
        let mut router =
            DurableFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1)).expect("open router");
        let first = router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("first request");
        drop(router);

        let mut recovered =
            DurableFabricRouter::open(&path, [PlacementGroupId(1)], Epoch(1)).expect("recover");
        let replay = recovered
            .commit_request(
                PlacementGroupId(1),
                request,
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"tenant/a-retry".to_vec(),
                },
            )
            .expect("replay request");

        assert!(!first.replayed);
        assert!(replay.replayed);
        assert_eq!(first.certificate, replay.certificate);
        assert_eq!(recovered.latest_version(), CommitVersion(1));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_backed_router_certifies_only_after_quorum_commit() {
        let path = test_path("vsr_backed_router_certifies_only_after_quorum_commit");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open vsr-backed router");

        let commit = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("quorum-backed commit");

        assert_eq!(commit.index, LogIndex(1));
        assert!(std::fs::metadata(&path).unwrap().is_dir());
        assert!(
            SegmentStore::recover(&path)
                .expect("recover split protocol store")
                .recovery
                .wal_records
                .len()
                >= 1
        );
        let vsr_store_path = path.with_extension("vsr").join("pg-1");
        assert!(std::fs::metadata(&vsr_store_path).unwrap().is_dir());
        assert_eq!(
            SegmentStore::recover(&vsr_store_path)
                .expect("recover split vsr store")
                .recovery
                .wal_records
                .len(),
            3 // Bootstrap membership, prepare, and commit.
        );
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );
        drop(router);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover shared wal");
        let second = recovered
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/b".to_vec(),
                },
            )
            .expect("second quorum-backed commit");

        assert_eq!(second.index, LogIndex(2));
        assert_eq!(second.version, CommitVersion(2));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            2
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_backed_router_rejects_stale_primary_after_restart_without_certificate() {
        let path =
            test_path("vsr_backed_router_rejects_stale_primary_after_restart_without_certificate");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open vsr-backed router");
        assert!(matches!(
            router.view_change(PlacementGroupId(1), Epoch(2), NodeId(1)),
            rsf_vsr::VsrDecision::ViewChanged { .. }
        ));
        drop(router);

        let mut stale = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover stale primary router");
        let err = stale
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect_err("stale primary cannot certify");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(
            stale
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            0
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_backed_router_preserves_certificate_prefix_after_new_primary_restart() {
        let path =
            test_path("vsr_backed_router_preserves_certificate_prefix_after_new_primary_restart");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open vsr-backed router");
        let first = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("first certificate");
        assert!(matches!(
            router.view_change(PlacementGroupId(1), Epoch(2), NodeId(1)),
            rsf_vsr::VsrDecision::ViewChanged { committed: 1, .. }
        ));
        drop(router);

        let mut stale = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover stale primary router");
        let err = stale
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/stale".to_vec(),
                },
            )
            .expect_err("stale primary cannot overwrite certificate prefix");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(
            stale
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates"),
            vec![CommitCertificate::from(&first)]
        );
        drop(stale);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(1),
            replicas,
        )
        .expect("recover new primary router");
        let second = recovered
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/b".to_vec(),
                },
            )
            .expect("new primary appends certificate");

        assert_eq!(second.index, LogIndex(2));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates"),
            vec![
                CommitCertificate::from(&first),
                CommitCertificate::from(&second)
            ]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_timeout_election_preserves_prefix_and_rejects_stale_local_leader() {
        let path =
            test_path("vsr_timeout_election_preserves_prefix_and_rejects_stale_local_leader");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");
        router
            .update_policy(false, 2)
            .expect("persist strict policy");
        let first = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("first certificate");
        assert!(matches!(
            router.timeout_election(PlacementGroupId(1)),
            rsf_vsr::VsrDecision::ViewChanged {
                view: Epoch(2),
                primary: NodeId(1),
                committed: 1
            }
        ));

        let err = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/stale-local".to_vec(),
                },
            )
            .expect_err("old local primary cannot lead after timeout election");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        drop(router);

        let mut stale = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover stale local node");
        let err = stale
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/stale-restart".to_vec(),
                },
            )
            .expect_err("stale local node cannot certify after restart");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(
            stale
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates"),
            vec![CommitCertificate::from(&first)]
        );
        drop(stale);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(1),
            replicas,
        )
        .expect("recover elected local node");
        let err = recovered
            .commit_with_mode(
                PlacementGroupId(1),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [9_u8; 32] },
            )
            .expect_err("policy survives timeout election recovery");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        let second = recovered
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/new-primary".to_vec(),
                },
            )
            .expect("elected local node appends after timeout");
        assert_eq!(second.index, LogIndex(2));
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates"),
            vec![
                CommitCertificate::from(&first),
                CommitCertificate::from(&second)
            ]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_transport_allows_certificate_with_one_replica_partitioned() {
        let path = test_path("vsr_transport_allows_certificate_with_one_replica_partitioned");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut transport = LocalVsrTransport::all_reachable(replicas);
        transport.partition(NodeId(2));
        let mut router = VsrBackedFabricRouter::open_with_transport(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
            transport,
        )
        .expect("open partitioned router");

        let commit = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("primary plus one replica satisfies quorum");

        assert_eq!(commit.index, LogIndex(1));
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_transport_drop_prevents_protocol_certificate_without_quorum() {
        let path = test_path("vsr_transport_drop_prevents_protocol_certificate_without_quorum");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let transport = LocalVsrTransport::with_reachable([NodeId(0)]);
        let mut router = VsrBackedFabricRouter::open_with_transport(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
            transport,
        )
        .expect("open isolated router");

        let err = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect_err("no quorum means no certificate");

        assert_eq!(err.code, ErrorCode::QuorumUnavailable);
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            0
        );

        router.transport_mut().heal(NodeId(1));
        let commit = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("healed quorum can certify");
        assert_eq!(commit.index, LogIndex(1));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_reconfiguration_updates_router_quorum_membership() {
        let path = test_path("vsr_reconfiguration_updates_router_quorum_membership");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let transport = LocalVsrTransport::with_reachable([NodeId(0), NodeId(1)]);
        let mut router = VsrBackedFabricRouter::open_with_transport(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
            transport,
        )
        .expect("open router");
        assert!(matches!(
            router.reconfigure(
                PlacementGroupId(1),
                Epoch(2),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
            ),
            rsf_vsr::VsrDecision::MembershipChanged { .. }
        ));
        assert!(matches!(
            router.reconfigure(
                PlacementGroupId(1),
                Epoch(3),
                NodeId(0),
                [NodeId(0), NodeId(2), NodeId(3)]
            ),
            rsf_vsr::VsrDecision::MembershipChanged { .. }
        ));

        let err = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/reconfigured".to_vec(),
                },
            )
            .expect_err("new voter partition prevents new quorum");
        assert_eq!(err.code, ErrorCode::QuorumUnavailable);
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            0
        );

        router.transport_mut().heal(NodeId(3));
        let commit = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/reconfigured".to_vec(),
                },
            )
            .expect("new voter restores quorum");

        assert_eq!(commit.index, LogIndex(1));
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates"),
            vec![CommitCertificate::from(&commit)]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn multi_pg_partition_does_not_leak_primary_or_quorum_state_between_groups() {
        let path =
            test_path("multi_pg_partition_does_not_leak_primary_or_quorum_state_between_groups");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let transport = LocalVsrTransport::with_reachable([NodeId(0), NodeId(1)]);
        let mut router = VsrBackedFabricRouter::open_with_transport(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
            transport,
        )
        .expect("open router");

        assert!(matches!(
            router.reconfigure(
                PlacementGroupId(1),
                Epoch(2),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
            ),
            rsf_vsr::VsrDecision::MembershipChanged { .. }
        ));
        assert!(matches!(
            router.reconfigure(
                PlacementGroupId(1),
                Epoch(3),
                NodeId(0),
                [NodeId(0), NodeId(2), NodeId(3)]
            ),
            rsf_vsr::VsrDecision::MembershipChanged { .. }
        ));

        let pg1_err = router
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/pg1-partitioned".to_vec(),
                },
            )
            .expect_err("pg1 cannot certify without its new quorum");
        assert_eq!(pg1_err.code, ErrorCode::QuorumUnavailable);

        let pg2_commit = router
            .commit(
                PlacementGroupId(2),
                FabricOperation::KvMutation {
                    key: b"tenant/pg2-still-available".to_vec(),
                },
            )
            .expect("pg2 uses its own primary and quorum state");
        assert_eq!(pg2_commit.pg, PlacementGroupId(2));
        assert_eq!(pg2_commit.index, LogIndex(1));
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(1))
                .expect("pg1 certificates")
                .len(),
            0
        );
        assert_eq!(
            router
                .recovered_certificates(PlacementGroupId(2))
                .expect("pg2 certificates"),
            vec![CommitCertificate::from(&pg2_commit)]
        );
        drop(router);

        let recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover router");
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("recovered pg1 certificates")
                .len(),
            0
        );
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(2))
                .expect("recovered pg2 certificates"),
            vec![CommitCertificate::from(&pg2_commit)]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_reconfiguration_survives_restart_and_keeps_policy() {
        let path = test_path("vsr_reconfiguration_survives_restart_and_keeps_policy");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");
        router
            .update_policy(false, 2)
            .expect("persist strict policy");
        assert!(matches!(
            router.reconfigure(
                PlacementGroupId(1),
                Epoch(2),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
            ),
            rsf_vsr::VsrDecision::MembershipChanged { .. }
        ));
        assert!(matches!(
            router.reconfigure(
                PlacementGroupId(1),
                Epoch(3),
                NodeId(1),
                [NodeId(1), NodeId(2), NodeId(3)]
            ),
            rsf_vsr::VsrDecision::MembershipChanged { .. }
        ));
        drop(router);

        let mut transport = LocalVsrTransport::all_reachable([NodeId(0), NodeId(1), NodeId(2)]);
        transport.heal(NodeId(3));
        let mut recovered = VsrBackedFabricRouter::open_with_transport(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(1),
            replicas,
            transport,
        )
        .expect("recover router");
        let err = recovered
            .commit_with_mode(
                PlacementGroupId(1),
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [7_u8; 32] },
            )
            .expect_err("policy remains strict after membership recovery");
        assert_eq!(err.code, ErrorCode::InvalidInput);

        let commit = recovered
            .commit(
                PlacementGroupId(1),
                FabricOperation::KvMutation {
                    key: b"tenant/recovered".to_vec(),
                },
            )
            .expect("recovered membership certifies strict commit");
        assert_eq!(commit.index, LogIndex(1));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_backed_request_replays_same_certificate_after_restart() {
        let path = test_path("vsr_backed_request_replays_same_certificate_after_restart");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let request = ClientRequestId::new("client-a", 99);
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");

        let first = router
            .commit_request(
                PlacementGroupId(1),
                request.clone(),
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("first request");
        assert!(!first.replayed);
        drop(router);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover router");
        let replay = recovered
            .commit_request(
                PlacementGroupId(1),
                request,
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"tenant/a-different-retry-body".to_vec(),
                },
            )
            .expect("replay request");

        assert!(replay.replayed);
        assert_eq!(first.certificate, replay.certificate);
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn vsr_backed_request_finishes_certificate_after_vsr_table_only_crash() {
        let path = test_path("vsr_backed_request_finishes_certificate_after_vsr_table_only_crash");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let request = ClientRequestId::new("client-a", 100);
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");

        let commit = router
            .commit_vsr_request_without_protocol_certificate_for_test(
                PlacementGroupId(1),
                &request,
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("vsr commit and client table without protocol certificate");
        drop(router);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover router");
        let replay = recovered
            .commit_request(
                PlacementGroupId(1),
                request,
                ConsistencyMode::Strict,
                FabricOperation::KvMutation {
                    key: b"tenant/a".to_vec(),
                },
            )
            .expect("finish missing request certificate");

        assert!(!replay.replayed);
        assert_eq!(replay.certificate.index, commit.index);
        assert_eq!(
            recovered
                .recovered_certificates(PlacementGroupId(1))
                .expect("certificates")
                .len(),
            1
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn routed_commit_uses_recovered_placement_map_after_restart() {
        let path = test_path("routed_commit_uses_recovered_placement_map_after_restart");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let operation = FabricOperation::KvMutation {
            key: b"tenant/routed".to_vec(),
        };
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");
        let expected_pg = router.placement_map().route(&operation);
        let commit = router
            .commit_routed(operation.clone())
            .expect("routed commit");
        assert_eq!(commit.pg, expected_pg);
        drop(router);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(9)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover router");
        assert_eq!(
            recovered.placement_map().groups,
            vec![PlacementGroupId(1), PlacementGroupId(2)]
        );
        let second = recovered
            .commit_routed(FabricOperation::KvMutation {
                key: b"tenant/second".to_vec(),
            })
            .expect("second routed commit");
        assert!(recovered.placement_map().groups.contains(&second.pg));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn placement_change_does_not_fork_existing_request_certificate() {
        let path = test_path("placement_change_does_not_fork_existing_request_certificate");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let request = ClientRequestId::new("client-routed", 11);
        let operation = FabricOperation::KvMutation {
            key: b"tenant/stable".to_vec(),
        };
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");

        let first = router
            .commit_request_routed(request.clone(), ConsistencyMode::Strict, operation.clone())
            .expect("first routed request");
        router
            .update_placement_map([PlacementGroupId(3), PlacementGroupId(4)])
            .expect("update placement");
        let replay = router
            .commit_request_routed(request, ConsistencyMode::Strict, operation)
            .expect("replay routed request");

        assert!(replay.replayed);
        assert_eq!(first.certificate, replay.certificate);
        assert!(matches!(
            first.certificate.pg,
            PlacementGroupId(1) | PlacementGroupId(2)
        ));
        assert_eq!(router.placement_map().version, 2);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn policy_rejects_availability_first_when_disabled_after_restart() {
        let path = test_path("policy_rejects_availability_first_when_disabled_after_restart");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");
        router
            .update_policy(false, 1)
            .expect("disable availability-first");
        drop(router);

        let mut recovered = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("recover router");
        assert!(!recovered.policy().allow_availability_first_immutable);
        let err = recovered
            .commit_routed_with_mode(
                ConsistencyMode::AvailabilityFirst,
                FabricOperation::BlobPin { digest: [7; 32] },
            )
            .expect_err("policy rejects AP");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("policy disables availability-first"));

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn policy_rejects_placement_map_below_minimum_width() {
        let path = test_path("policy_rejects_placement_map_below_minimum_width");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1), PlacementGroupId(2)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");
        router
            .update_policy(true, 2)
            .expect("require two placement groups");
        let err = router
            .update_placement_map([PlacementGroupId(3)])
            .expect_err("policy rejects narrow map");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(
            err.message
                .contains("policy requires at least 2 placement groups")
        );
        assert_eq!(
            router.placement_map().groups,
            vec![PlacementGroupId(1), PlacementGroupId(2)]
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn stricter_policy_does_not_fork_existing_request_certificate() {
        let path = test_path("stricter_policy_does_not_fork_existing_request_certificate");
        let replicas = [NodeId(0), NodeId(1), NodeId(2)];
        let request = ClientRequestId::new("client-policy", 12);
        let operation = FabricOperation::BlobPin { digest: [9; 32] };
        let mut router = VsrBackedFabricRouter::open(
            &path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            replicas,
        )
        .expect("open router");
        let first = router
            .commit_request_routed(
                request.clone(),
                ConsistencyMode::AvailabilityFirst,
                operation.clone(),
            )
            .expect("initial AP request");
        router.update_policy(false, 1).expect("disable future AP");
        let replay = router
            .commit_request_routed(request, ConsistencyMode::AvailabilityFirst, operation)
            .expect("replay existing AP request");

        assert!(replay.replayed);
        assert_eq!(first.certificate, replay.certificate);

        let _ = std::fs::remove_file(path);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-protocol-{name}-{}-{nonce}.log",
            std::process::id()
        ))
    }

    fn tiny_store_options() -> SegmentStoreOptions {
        SegmentStoreOptions {
            log_options: SegmentLogOptions {
                sync_policy: SyncPolicy::None,
            },
            max_segment_bytes: 1,
        }
    }
}
