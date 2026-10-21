mod election;
mod installation;
pub use installation::{VsrInstallationInput, VsrViewInstallation};
mod replay;
pub use election::{VsrViewChangeReport, VsrViewChangeRequest, VsrViewChangeSnapshot};
mod history;
mod learner;
mod transfer;
pub use learner::LearnerHistoryStore;
pub use transfer::{VsrHistoryDescriptor, VsrHistoryRecord, VsrHistorySnapshot};
mod migration;
mod probe;
mod replica;
pub use probe::VsrPrimaryProbe;

pub const MAX_REPLICATED_COMMAND_BYTES: usize = 1024 * 1024;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::storage::WalRecordKind;
use rsf_core::{
    Epoch, ErrorCode, LogIndex, NodeId, OperationId, PlacementGroupId, RsfError, RsfResult,
};
use rsf_storage::{SegmentStore, SegmentStoreOptions, WalAppend};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VsrOperation {
    CatalogMutation {
        namespace: String,
        item: String,
    },
    TransactionCommit {
        key: String,
    },
    ObjectManifest {
        bucket: String,
        key: String,
    },
    ProtocolOperation {
        operation_id: OperationId,
    },
    /// Complete, deterministic state-machine input, not a reference to local data.
    ReplicatedCommand {
        operation_id: OperationId,
        payload: Vec<u8>,
    },
}

impl VsrOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CatalogMutation { .. } => "catalog-mutation",
            Self::TransactionCommit { .. } => "transaction-commit",
            Self::ObjectManifest { .. } => "object-manifest",
            Self::ProtocolOperation { .. } => "protocol-operation",
            Self::ReplicatedCommand { .. } => "replicated-command",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrPrepare {
    pub pg: PlacementGroupId,
    pub view: Epoch,
    pub index: LogIndex,
    pub primary: NodeId,
    pub operation: VsrOperation,
    pub bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrCommit {
    pub pg: PlacementGroupId,
    pub view: Epoch,
    pub index: LogIndex,
    pub primary: NodeId,
    pub acknowledgements: usize,
    pub certificate: VsrCommitCertificate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrCommitCertificate {
    pub pg: PlacementGroupId,
    pub view: Epoch,
    pub index: LogIndex,
    pub primary: NodeId,
    pub operation: VsrOperation,
    pub signers: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrCommittedEntry {
    pub prepare: VsrPrepare,
    pub commit: VsrCommit,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VsrClientRequestId {
    pub client: String,
    pub request: u64,
}

impl VsrClientRequestId {
    pub fn new(client: impl Into<String>, request: u64) -> Self {
        Self {
            client: client.into(),
            request,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrClientRequestCommit {
    pub pg: PlacementGroupId,
    pub request: VsrClientRequestId,
    pub index: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrPrepareMessage {
    pub prepare: VsrPrepare,
    pub replica: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrTransportError {
    pub replica: NodeId,
    pub reason: String,
}

pub trait VsrTransport {
    /// Authenticate current leadership and the committed tip without mutating
    /// remote state. Transports without this contract must fail closed.
    fn confirm_primary(
        &mut self,
        replica: NodeId,
        _probe: &VsrPrimaryProbe,
    ) -> Result<(), VsrTransportError> {
        Err(VsrTransportError {
            replica,
            reason: "transport does not support primary probes".into(),
        })
    }

    fn acknowledge_prepare(
        &mut self,
        message: VsrPrepareMessage,
    ) -> Result<NodeId, VsrTransportError>;

    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: VsrCommit,
    ) -> Result<NodeId, VsrTransportError>;

    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, VsrTransportError>;

    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: VsrCommittedEntry,
    ) -> Result<(), VsrTransportError>;
}

/// In-process reachability model for simulations; it supplies no remote durability.
#[derive(Clone, Debug)]
pub struct LocalVsrTransport {
    reachable: BTreeSet<NodeId>,
    tails: BTreeMap<(NodeId, PlacementGroupId), LogIndex>,
}

impl LocalVsrTransport {
    pub fn all_reachable(replicas: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            reachable: replicas.into_iter().collect(),
            tails: BTreeMap::new(),
        }
    }

    pub fn with_reachable(replicas: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            reachable: replicas.into_iter().collect(),
            tails: BTreeMap::new(),
        }
    }

    pub fn partition(&mut self, replica: NodeId) {
        self.reachable.remove(&replica);
    }

    pub fn heal(&mut self, replica: NodeId) {
        self.reachable.insert(replica);
    }
}

impl VsrTransport for LocalVsrTransport {
    fn acknowledge_prepare(
        &mut self,
        message: VsrPrepareMessage,
    ) -> Result<NodeId, VsrTransportError> {
        if self.reachable.contains(&message.replica) {
            Ok(message.replica)
        } else {
            Err(VsrTransportError {
                replica: message.replica,
                reason: "replica is unreachable".to_string(),
            })
        }
    }

    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: VsrCommit,
    ) -> Result<NodeId, VsrTransportError> {
        self.require_reachable(replica)?;
        let tail = self
            .tails
            .entry((replica, commit.pg))
            .or_insert(LogIndex(0));
        if commit.index.0 > tail.0 + 1 {
            return Err(VsrTransportError {
                replica,
                reason: "simulated commit has a missing prefix".into(),
            });
        }
        *tail = (*tail).max(commit.index);
        Ok(replica)
    }

    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, VsrTransportError> {
        self.require_reachable(replica)?;
        Ok(self
            .tails
            .get(&(replica, pg))
            .copied()
            .unwrap_or(LogIndex(0)))
    }

    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: VsrCommittedEntry,
    ) -> Result<(), VsrTransportError> {
        self.acknowledge_commit(replica, entry.commit).map(|_| ())
    }
}

impl LocalVsrTransport {
    fn require_reachable(&self, replica: NodeId) -> Result<NodeId, VsrTransportError> {
        if self.reachable.contains(&replica) {
            Ok(replica)
        } else {
            Err(VsrTransportError {
                replica,
                reason: "replica is unreachable".into(),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VsrDecision {
    Prepared(VsrPrepare),
    Acknowledged {
        replica: NodeId,
        view: Epoch,
        index: LogIndex,
        acknowledgements: usize,
    },
    Committed(VsrCommit),
    ViewChanged {
        view: Epoch,
        primary: NodeId,
        committed: usize,
    },
    MembershipChanged {
        view: Epoch,
        primary: NodeId,
        replicas: usize,
        committed_tail: LogIndex,
    },
    Rejected {
        node: NodeId,
        reason: String,
    },
}

#[derive(Clone, Debug)]
struct PendingPrepare {
    prepare: VsrPrepare,
    acknowledgements: BTreeSet<NodeId>,
    committed: bool,
}

#[derive(Debug)]
pub struct VsrCell {
    pg: PlacementGroupId,
    view: Epoch,
    primary: NodeId,
    replicas: BTreeSet<NodeId>,
    next_index: u64,
    pending: BTreeMap<LogIndex, PendingPrepare>,
    committed: Vec<VsrCommit>,
    client_table: BTreeMap<VsrClientRequestId, LogIndex>,
}

#[derive(Debug)]
pub struct DurableVsrCell {
    root: std::path::PathBuf,
    options: SegmentStoreOptions,
    installed_plan: Option<[u8; 32]>,
    installation_input: Option<installation::input::Stage>,
    cell: VsrCell,
    history: history::ConfigurationHistory,
    bootstrap_persisted: bool,
    initial_bootstrap: Vec<u8>,
    store: SegmentStore,
    durability_failure: std::sync::Mutex<Option<String>>,
    view_change: Option<VsrViewChangeRequest>,
    _writer_lock: rsf_storage::ExclusiveFileLock,
}

impl VsrCell {
    pub fn new(
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, String> {
        let replicas = replicas.into_iter().collect::<BTreeSet<_>>();
        if replicas.is_empty() {
            return Err("vsr cell requires at least one replica".to_string());
        }
        if !replicas.contains(&primary) {
            return Err("vsr primary must be a replica".to_string());
        }
        Ok(Self {
            pg,
            view,
            primary,
            replicas,
            next_index: 1,
            pending: BTreeMap::new(),
            committed: Vec::new(),
            client_table: BTreeMap::new(),
        })
    }

    pub fn prepare(&mut self, primary: NodeId, operation: VsrOperation, bytes: u32) -> VsrDecision {
        if primary != self.primary {
            return VsrDecision::Rejected {
                node: primary,
                reason: "prepare rejected from non-primary".to_string(),
            };
        }
        if bytes == 0 {
            return VsrDecision::Rejected {
                node: primary,
                reason: "prepare bytes must be non-zero".to_string(),
            };
        }
        if let Err(reason) = replica::validate_payload(&operation, bytes) {
            return VsrDecision::Rejected {
                node: primary,
                reason,
            };
        }
        let index = LogIndex(self.next_index);
        let Some(next) = self.next_index.checked_add(1) else {
            return VsrDecision::Rejected {
                node: primary,
                reason: "VSR log index exhausted".into(),
            };
        };
        self.next_index = next;
        let prepare = VsrPrepare {
            pg: self.pg,
            view: self.view,
            index,
            primary,
            operation,
            bytes,
        };
        let mut acknowledgements = BTreeSet::new();
        acknowledgements.insert(primary);
        self.pending.insert(
            index,
            PendingPrepare {
                prepare: prepare.clone(),
                acknowledgements,
                committed: false,
            },
        );
        VsrDecision::Prepared(prepare)
    }

    pub fn acknowledge(&mut self, replica: NodeId, view: Epoch, index: LogIndex) -> VsrDecision {
        if view != self.view {
            return VsrDecision::Rejected {
                node: replica,
                reason: "acknowledgement view is stale".to_string(),
            };
        }
        if !self.replicas.contains(&replica) {
            return VsrDecision::Rejected {
                node: replica,
                reason: "acknowledgement from non-replica".to_string(),
            };
        }
        let Some(pending) = self.pending.get_mut(&index) else {
            return VsrDecision::Rejected {
                node: replica,
                reason: "prepare index is unknown".to_string(),
            };
        };
        if pending.prepare.view != self.view {
            return VsrDecision::Rejected {
                node: replica,
                reason: "prepare belongs to a previous view".to_string(),
            };
        }
        pending.acknowledgements.insert(replica);
        VsrDecision::Acknowledged {
            replica,
            view,
            index,
            acknowledgements: pending.acknowledgements.len(),
        }
    }

    pub fn commit(&mut self, primary: NodeId, index: LogIndex) -> VsrDecision {
        if primary != self.primary {
            return VsrDecision::Rejected {
                node: primary,
                reason: "commit rejected from non-primary".to_string(),
            };
        }
        if self.committed_tail().0.checked_add(1) != Some(index.0) {
            return VsrDecision::Rejected {
                node: primary,
                reason: "commit must extend the contiguous prefix".into(),
            };
        }
        let quorum = self.quorum();
        let Some(pending) = self.pending.get_mut(&index) else {
            return VsrDecision::Rejected {
                node: primary,
                reason: "prepare index is unknown".to_string(),
            };
        };
        if pending.prepare.view != self.view {
            return VsrDecision::Rejected {
                node: primary,
                reason: "prepare belongs to a previous view".to_string(),
            };
        }
        if pending.acknowledgements.len() < quorum {
            return VsrDecision::Rejected {
                node: primary,
                reason: "commit quorum is not satisfied".to_string(),
            };
        }
        if pending.committed {
            return VsrDecision::Rejected {
                node: primary,
                reason: "prepare already committed".to_string(),
            };
        }
        pending.committed = true;
        let certificate = VsrCommitCertificate {
            pg: pending.prepare.pg,
            view: pending.prepare.view,
            index,
            primary,
            operation: pending.prepare.operation.clone(),
            signers: pending.acknowledgements.clone(),
        };
        let commit = VsrCommit {
            pg: pending.prepare.pg,
            view: pending.prepare.view,
            index,
            primary,
            acknowledgements: certificate.signers.len(),
            certificate,
        };
        self.committed.push(commit.clone());
        VsrDecision::Committed(commit)
    }

    pub fn view_change(&mut self, view: Epoch, primary: NodeId) -> VsrDecision {
        if view <= self.view {
            return VsrDecision::Rejected {
                node: primary,
                reason: "view change must advance epoch".to_string(),
            };
        }
        if !self.replicas.contains(&primary) {
            return VsrDecision::Rejected {
                node: primary,
                reason: "new primary must be a replica".to_string(),
            };
        }
        self.view = view;
        self.primary = primary;
        VsrDecision::ViewChanged {
            view,
            primary,
            committed: self.committed.len(),
        }
    }

    pub fn timeout_election(&mut self) -> VsrDecision {
        let Some(view) = self.view.0.checked_add(1).map(Epoch) else {
            return VsrDecision::Rejected {
                node: self.primary,
                reason: "view epoch overflow during timeout election".to_string(),
            };
        };
        let Some(primary) = next_primary(&self.replicas, self.primary) else {
            return VsrDecision::Rejected {
                node: self.primary,
                reason: "timeout election requires a current primary in the replica set"
                    .to_string(),
            };
        };
        self.view_change(view, primary)
    }

    pub fn reconfigure(
        &mut self,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
    ) -> VsrDecision {
        let replicas = replicas.into_iter().collect::<BTreeSet<_>>();
        if view <= self.view {
            return VsrDecision::Rejected {
                node: primary,
                reason: "membership change must advance epoch".to_string(),
            };
        }
        if let Err(reason) = validate_membership_change(&self.replicas, &replicas, primary) {
            return VsrDecision::Rejected {
                node: primary,
                reason,
            };
        }
        self.view = view;
        self.primary = primary;
        self.replicas = replicas;
        VsrDecision::MembershipChanged {
            view,
            primary,
            replicas: self.replicas.len(),
            committed_tail: self.committed_tail(),
        }
    }

    pub fn view(&self) -> Epoch {
        self.view
    }

    pub fn primary(&self) -> NodeId {
        self.primary
    }

    pub fn committed_count(&self) -> usize {
        self.committed.len()
    }

    pub fn committed_tail(&self) -> LogIndex {
        self.committed
            .last()
            .map_or(LogIndex(0), |commit| commit.index)
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn contains_replica(&self, node: NodeId) -> bool {
        self.replicas.contains(&node)
    }

    pub fn replicas(&self) -> Vec<NodeId> {
        self.replicas.iter().copied().collect()
    }

    pub fn quorum(&self) -> usize {
        self.replicas.len() / 2 + 1
    }
}

impl DurableVsrCell {
    /// Whether this store has published a quorum-selected generation.
    pub fn has_installed_view(&self) -> bool {
        self.installed_plan.is_some()
    }

    /// Open an authoritative existing history without accepting a caller's
    /// bootstrap configuration or initializing missing placement-group state.
    pub fn open_existing(path: impl AsRef<Path>, pg: PlacementGroupId) -> RsfResult<Self> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path.join(".vsr-writer.lock"))
            .map_err(|err| invalid_input(format!("cannot open existing VSR writer lock: {err}")))?;
        let writer_lock = rsf_storage::ExclusiveFileLock::acquire(file).map_err(|err| {
            RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!("VSR store already has a writer: {err}"),
            )
        })?;
        let active = installation::publication::active(path, pg)?;
        let recovery = SegmentStore::recover_existing(&active.path)?.recovery;
        let records = recovery
            .wal_records
            .iter()
            .filter(|record| record.pg == pg)
            .collect::<Vec<_>>();
        let bootstrap = if let Some(record) = records
            .first()
            .filter(|record| record.payload.starts_with(VSR_BOOTSTRAP_MAGIC))
        {
            decode_bootstrap(&record.payload)?
        } else {
            migration::recover_bootstrap(&records, pg)?.ok_or_else(|| {
                corrupt("existing VSR history requires persisted bootstrap authority")
            })?
        };
        Self::open_locked(
            path,
            pg,
            bootstrap.view,
            bootstrap.primary,
            bootstrap.replicas,
            SegmentStoreOptions::default(),
            writer_lock,
        )
    }

    pub fn open(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
    ) -> RsfResult<Self> {
        Self::open_with_store_options(
            path,
            pg,
            view,
            primary,
            replicas,
            SegmentStoreOptions::default(),
        )
    }

    pub fn open_with_store_options(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
        options: SegmentStoreOptions,
    ) -> RsfResult<Self> {
        let path = path.as_ref();
        if options.log_options.sync_policy == rsf_storage::SyncPolicy::None {
            return Err(invalid_input("durable VSR requires sync on append"));
        }
        std::fs::create_dir_all(path)
            .map_err(|err| invalid_input(format!("cannot create VSR store: {err}")))?;
        let writer_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join(".vsr-writer.lock"))
            .map_err(|err| invalid_input(format!("cannot open VSR writer lock: {err}")))?;
        let writer_lock = rsf_storage::ExclusiveFileLock::acquire(writer_lock).map_err(|err| {
            RsfError::new(
                ErrorCode::QuorumUnavailable,
                format!("VSR store already has a writer: {err}"),
            )
        })?;
        Self::open_locked(path, pg, view, primary, replicas, options, writer_lock)
    }

    fn open_locked(
        path: &Path,
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
        options: SegmentStoreOptions,
        writer_lock: rsf_storage::ExclusiveFileLock,
    ) -> RsfResult<Self> {
        let active = installation::publication::active(path, pg)?;
        if active.plan.is_some() {
            SegmentStore::recover_existing(&active.path)?;
        }
        let (store, recovered) = SegmentStore::open_recovered(&active.path, options)?;
        let recovery = recovered.recovery;
        let records = recovery
            .wal_records
            .iter()
            .filter(|record| record.pg == pg)
            .collect::<Vec<_>>();
        installation::publication::validate_active(&active, pg, &records)?;
        let migrated_bootstrap = migration::recover_bootstrap(&records, pg)?;
        let has_migration = migrated_bootstrap.is_some();
        // Replay certificates against the membership that existed when the log
        // began, never against a caller's newer desired configuration.
        let cell = if let Some(first) = records
            .first()
            .filter(|record| record.payload.starts_with(VSR_BOOTSTRAP_MAGIC))
        {
            let bootstrap = decode_bootstrap(&first.payload)?;
            if first.kind != WalRecordKind::Membership || bootstrap.pg != pg {
                return Err(corrupt("invalid VSR bootstrap envelope"));
            }
            bootstrap
        } else if let Some(bootstrap) = migrated_bootstrap {
            bootstrap
        } else {
            VsrCell::new(pg, view, primary, replicas).map_err(invalid_input)?
        };

        let initial_bootstrap = encode_bootstrap(&cell);
        let bootstrap_persisted = has_migration
            || records.is_empty()
            || records
                .first()
                .is_some_and(|record| record.payload.starts_with(VSR_BOOTSTRAP_MAGIC));
        let replay = records
            .iter()
            .map(|record| replay::Record {
                kind: record.kind,
                epoch: record.epoch,
                payload: &record.payload,
            })
            .collect::<Vec<_>>();
        let (cell, history, view_change) = replay::recover(cell, &replay)?;

        let mut durable = Self {
            root: path.to_path_buf(),
            options,
            installed_plan: active.plan,
            installation_input: None,
            cell,
            history,
            bootstrap_persisted,
            initial_bootstrap,
            store,
            durability_failure: std::sync::Mutex::new(None),
            view_change,
            _writer_lock: writer_lock,
        };
        if records.is_empty() {
            durable.append(WalRecordKind::Membership, &encode_bootstrap(&durable.cell))?;
        }
        Ok(durable)
    }

    pub fn prepare(
        &mut self,
        primary: NodeId,
        operation: VsrOperation,
        bytes: u32,
    ) -> RsfResult<VsrDecision> {
        self.ensure_active_view()?;
        let decision = self.cell.prepare(primary, operation, bytes);
        if let VsrDecision::Prepared(prepare) = &decision {
            let result = encode_prepare(prepare)
                .and_then(|payload| self.append(WalRecordKind::Prepare, &payload));
            if let Err(err) = result {
                self.cell.pending.remove(&prepare.index);
                self.cell.next_index = prepare.index.0;
                return Err(err);
            }
        }
        Ok(decision)
    }

    pub fn acknowledge(&mut self, replica: NodeId, view: Epoch, index: LogIndex) -> VsrDecision {
        if let Err(err) = self.ensure_active_view() {
            return VsrDecision::Rejected {
                node: replica,
                reason: err.message,
            };
        }
        self.cell.acknowledge(replica, view, index)
    }

    pub fn commit(&mut self, primary: NodeId, index: LogIndex) -> RsfResult<VsrDecision> {
        self.ensure_active_view()?;
        let decision = self.cell.commit(primary, index);
        if let VsrDecision::Committed(commit) = &decision
            && let Err(err) = self.append(WalRecordKind::Commit, &encode_commit(commit))
        {
            self.cell.committed.pop();
            if let Some(pending) = self.cell.pending.get_mut(&index) {
                pending.committed = false;
            }
            return Err(err);
        }
        Ok(decision)
    }

    pub fn view_change(&mut self, view: Epoch, primary: NodeId) -> VsrDecision {
        if let Err(err) = self.ensure_active_view() {
            return VsrDecision::Rejected {
                node: primary,
                reason: err.message,
            };
        }
        if !self.bootstrap_persisted {
            return VsrDecision::Rejected {
                node: primary,
                reason: "legacy VSR bootstrap migration is required before changing views".into(),
            };
        }
        if view <= self.cell.view {
            return VsrDecision::Rejected {
                node: primary,
                reason: "view change must advance epoch".to_string(),
            };
        }
        if !self.cell.replicas.contains(&primary) {
            return VsrDecision::Rejected {
                node: primary,
                reason: "new primary must be a replica".to_string(),
            };
        }
        let change = VsrViewChange {
            pg: self.cell.pg,
            view,
            primary,
            committed_tail: self.cell.committed_tail(),
        };
        if let Err(err) = self.append(WalRecordKind::Membership, &encode_view_change(&change)) {
            return VsrDecision::Rejected {
                node: primary,
                reason: format!("view change durability failed: {}", err.message),
            };
        }
        let decision = self.cell.view_change(view, primary);
        self.history.record(&self.cell);
        decision
    }

    pub fn timeout_election(&mut self) -> VsrDecision {
        let Some(view) = self.cell.view.0.checked_add(1).map(Epoch) else {
            return VsrDecision::Rejected {
                node: self.cell.primary,
                reason: "view epoch overflow during timeout election".to_string(),
            };
        };
        let Some(primary) = next_primary(&self.cell.replicas, self.cell.primary) else {
            return VsrDecision::Rejected {
                node: self.cell.primary,
                reason: "timeout election requires a current primary in the replica set"
                    .to_string(),
            };
        };
        self.view_change(view, primary)
    }

    pub fn reconfigure(
        &mut self,
        view: Epoch,
        primary: NodeId,
        replicas: impl IntoIterator<Item = NodeId>,
    ) -> VsrDecision {
        if !self.bootstrap_persisted {
            return VsrDecision::Rejected {
                node: primary,
                reason: "legacy VSR bootstrap migration is required before changing membership"
                    .into(),
            };
        }
        if let Err(err) = self.ensure_active_view() {
            return VsrDecision::Rejected {
                node: primary,
                reason: err.message,
            };
        }
        let replicas = replicas.into_iter().collect::<BTreeSet<_>>();
        if view <= self.cell.view {
            return VsrDecision::Rejected {
                node: primary,
                reason: "membership change must advance epoch".to_string(),
            };
        }
        if let Err(reason) = validate_membership_change(&self.cell.replicas, &replicas, primary) {
            return VsrDecision::Rejected {
                node: primary,
                reason,
            };
        }
        let change = VsrMembershipChange {
            pg: self.cell.pg,
            view,
            primary,
            committed_tail: self.cell.committed_tail(),
            old_replicas: self.cell.replicas.clone(),
            new_replicas: replicas.clone(),
        };
        if let Err(err) = self.append(
            WalRecordKind::Membership,
            &encode_membership_change(&change),
        ) {
            return VsrDecision::Rejected {
                node: primary,
                reason: format!("membership change durability failed: {}", err.message),
            };
        }
        let decision = self.cell.reconfigure(view, primary, replicas);
        self.history.record(&self.cell);
        decision
    }

    pub fn view(&self) -> Epoch {
        self.cell.view()
    }

    pub fn primary(&self) -> NodeId {
        self.cell.primary()
    }

    pub fn committed_count(&self) -> usize {
        self.cell.committed_count()
    }

    pub fn committed_tail(&self) -> LogIndex {
        self.cell.committed_tail()
    }

    pub fn replica_count(&self) -> usize {
        self.cell.replica_count()
    }

    pub fn contains_replica(&self, node: NodeId) -> bool {
        self.cell.contains_replica(node)
    }

    pub fn replicas(&self) -> Vec<NodeId> {
        self.cell.replicas()
    }

    pub fn next_index(&self) -> LogIndex {
        LogIndex(self.cell.next_index)
    }

    pub fn quorum(&self) -> usize {
        self.cell.quorum()
    }

    pub fn committed_operation(&self, index: LogIndex) -> Option<&VsrOperation> {
        let pending = self.cell.pending.get(&index)?;
        pending.committed.then_some(&pending.prepare.operation)
    }

    pub fn committed_prefix(&self) -> RsfResult<Vec<VsrCommittedEntry>> {
        self.cell
            .committed
            .iter()
            .map(|commit| {
                let pending = self.cell.pending.get(&commit.index).ok_or_else(|| {
                    corrupt("vsr committed prefix is missing its prepared operation")
                })?;
                Ok(VsrCommittedEntry {
                    prepare: pending.prepare.clone(),
                    commit: commit.clone(),
                })
            })
            .collect()
    }

    pub fn catch_up_from(&mut self, entries: &[VsrCommittedEntry]) -> RsfResult<usize> {
        self.ensure_active_view()?;
        let mut applied = 0;
        for entry in entries {
            if entry.prepare.pg != self.cell.pg || entry.commit.pg != self.cell.pg {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!(
                        "catch-up entry for pg {} cannot be applied to pg {}",
                        entry.prepare.pg.0, self.cell.pg.0
                    ),
                ));
            }
            if entry.prepare.view > self.cell.view {
                return Err(invalid_input(
                    "install the certified view before transferring its committed prefix",
                ));
            }
            if entry.prepare.index != entry.commit.index {
                return Err(corrupt("catch-up prepare and commit index mismatch"));
            }
            self.history.validate(&entry.prepare, &entry.commit)?;
            let pending = self.cell.pending.get(&entry.prepare.index);
            if pending.is_some_and(|pending| pending.committed) {
                if pending.unwrap().prepare != entry.prepare {
                    return Err(corrupt(
                        "catch-up conflicts with an already committed prepare",
                    ));
                }
                continue;
            }
            if self.cell.committed_tail().0.checked_add(1) != Some(entry.prepare.index.0) {
                return Err(corrupt(
                    "catch-up must transfer a contiguous committed prefix",
                ));
            }
            let next = entry
                .prepare
                .index
                .0
                .checked_add(1)
                .ok_or_else(|| corrupt("VSR index overflow"))?;
            if pending.is_none_or(|pending| pending.prepare != entry.prepare) {
                if pending.is_some_and(|pending| pending.prepare.view >= entry.prepare.view) {
                    return Err(corrupt(
                        "catch-up conflicts with an existing prepare in the same or newer view",
                    ));
                }
                self.append(WalRecordKind::Prepare, &entry.prepare.encode()?)?;
                self.cell.next_index = self.cell.next_index.max(next);
                self.cell.pending.insert(
                    entry.prepare.index,
                    PendingPrepare {
                        prepare: entry.prepare.clone(),
                        acknowledgements: entry.commit.certificate.signers.clone(),
                        committed: false,
                    },
                );
            }
            self.append(WalRecordKind::Commit, &encode_commit(&entry.commit))?;
            let pending = self
                .cell
                .pending
                .get_mut(&entry.prepare.index)
                .ok_or_else(|| corrupt("catch-up committed prepare disappeared before commit"))?;
            pending.committed = true;
            self.cell.committed.push(entry.commit.clone());
            applied += 1;
        }
        Ok(applied)
    }

    pub fn request_commit(&self, request: &VsrClientRequestId) -> Option<LogIndex> {
        self.cell.client_table.get(request).copied()
    }

    pub fn remember_client_request(
        &mut self,
        request: VsrClientRequestId,
        index: LogIndex,
    ) -> RsfResult<()> {
        self.ensure_active_view()?;
        if let Some(existing) = self.cell.client_table.get(&request).copied() {
            if existing == index {
                return Ok(());
            }
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "client request already committed at index {}, cannot remap to {}",
                    existing.0, index.0
                ),
            ));
        }
        if !self
            .cell
            .pending
            .get(&index)
            .is_some_and(|pending| pending.committed)
        {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!(
                    "client request cannot be remembered before committed VSR index {}",
                    index.0
                ),
            ));
        }
        let record = VsrClientRequestCommit {
            pg: self.cell.pg,
            request: request.clone(),
            index,
        };
        self.append(
            WalRecordKind::Membership,
            &encode_client_request_commit(&record)?,
        )?;
        self.cell.client_table.insert(request, index);
        Ok(())
    }

    pub fn compact_committed_prefix(&mut self) -> RsfResult<()> {
        if self.cell.committed_tail().0 > 0 {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "vsr committed-prefix compaction requires a durable snapshot record before committed operations can be removed",
            ));
        }
        Ok(())
    }

    pub fn prepared(&self, index: LogIndex) -> Option<&VsrPrepare> {
        self.cell
            .pending
            .get(&index)
            .map(|pending| &pending.prepare)
    }

    fn ensure_healthy(&self) -> RsfResult<()> {
        let failure = self
            .durability_failure
            .lock()
            .map_err(|_| corrupt("VSR durability fence is poisoned"))?
            .clone();
        if let Some(reason) = failure {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("VSR durability failed; reopen before further operations: {reason}"),
            ));
        }
        let result =
            installation::publication::active(&self.root, self.cell.pg).and_then(|active| {
                if active.plan != self.installed_plan || active.path != self.store.root() {
                    Err(corrupt(
                        "active VSR generation changed while this writer owned the store",
                    ))
                } else {
                    Ok(())
                }
            });
        if let Err(err) = &result {
            *self
                .durability_failure
                .lock()
                .map_err(|_| corrupt("VSR durability fence is poisoned"))? =
                Some(err.message.clone());
        }
        result
    }

    fn append(&mut self, kind: WalRecordKind, payload: &[u8]) -> RsfResult<()> {
        self.ensure_active_view()?;
        self.append_control(kind, payload)
    }

    fn append_control(&mut self, kind: WalRecordKind, payload: &[u8]) -> RsfResult<()> {
        self.ensure_healthy()?;
        let result: RsfResult<()> = (|| {
            self.store
                .append_next_wal(self.cell.pg, self.cell.view, kind, payload)?;
            Ok(())
        })();
        if let Err(err) = &result {
            *self
                .durability_failure
                .lock()
                .map_err(|_| corrupt("VSR durability fence is poisoned"))? =
                Some(err.message.clone());
        }
        result
    }
}

const VSR_PREPARE_MAGIC: &[u8; 5] = b"VSRP1";
const VSR_COMMIT_MAGIC: &[u8; 5] = b"VSRC1";
const VSR_VIEW_CHANGE_MAGIC: &[u8; 5] = b"VSRV1";
const VSR_CLIENT_REQUEST_MAGIC: &[u8; 5] = b"VSRT1";
const VSR_MEMBERSHIP_CHANGE_MAGIC: &[u8; 5] = b"VSRM1";
const VSR_BOOTSTRAP_MAGIC: &[u8; 5] = b"VSRB1";

fn encode_bootstrap(cell: &VsrCell) -> Vec<u8> {
    let mut out = VSR_BOOTSTRAP_MAGIC.to_vec();
    out.extend_from_slice(&cell.pg.0.to_le_bytes());
    out.extend_from_slice(&cell.view.0.to_le_bytes());
    out.extend_from_slice(&cell.primary.0.to_le_bytes());
    write_nodes(&cell.replicas, &mut out);
    out
}

fn decode_bootstrap(bytes: &[u8]) -> RsfResult<VsrCell> {
    if bytes.len() < 37 || !bytes.starts_with(VSR_BOOTSTRAP_MAGIC) {
        return Err(corrupt("invalid VSR bootstrap payload"));
    }
    let (replicas, end) = read_nodes(bytes, 29)?;
    if end != bytes.len() {
        return Err(corrupt("VSR bootstrap has trailing bytes"));
    }
    VsrCell::new(
        PlacementGroupId(read_u64(bytes, 5)?),
        Epoch(read_u64(bytes, 13)?),
        NodeId(read_u64(bytes, 21)?),
        replicas,
    )
    .map_err(corrupt)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VsrViewChange {
    pg: PlacementGroupId,
    view: Epoch,
    primary: NodeId,
    committed_tail: LogIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VsrMembershipChange {
    pg: PlacementGroupId,
    view: Epoch,
    primary: NodeId,
    committed_tail: LogIndex,
    old_replicas: BTreeSet<NodeId>,
    new_replicas: BTreeSet<NodeId>,
}

fn validate_membership_change(
    old_replicas: &BTreeSet<NodeId>,
    new_replicas: &BTreeSet<NodeId>,
    primary: NodeId,
) -> Result<(), String> {
    if new_replicas.is_empty() {
        return Err("vsr membership requires at least one replica".to_string());
    }
    if !new_replicas.contains(&primary) {
        return Err("vsr membership primary must be in new replica set".to_string());
    }
    let old_quorum = old_replicas.len() / 2 + 1;
    let new_quorum = new_replicas.len() / 2 + 1;
    let union = old_replicas.union(new_replicas).count();
    if old_quorum + new_quorum <= union {
        return Err(format!(
            "vsr membership change is unsafe: old quorum {old_quorum} and new quorum {new_quorum} can be disjoint across {union} voters; add or remove one voter at a time"
        ));
    }
    Ok(())
}

fn next_primary(replicas: &BTreeSet<NodeId>, current: NodeId) -> Option<NodeId> {
    if !replicas.contains(&current) {
        return None;
    }
    replicas
        .iter()
        .copied()
        .find(|node| *node > current)
        .or_else(|| replicas.iter().next().copied())
}

fn encode_prepare(prepare: &VsrPrepare) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(VSR_PREPARE_MAGIC);
    out.extend_from_slice(&prepare.pg.0.to_le_bytes());
    out.extend_from_slice(&prepare.view.0.to_le_bytes());
    out.extend_from_slice(&prepare.index.0.to_le_bytes());
    out.extend_from_slice(&prepare.primary.0.to_le_bytes());
    out.extend_from_slice(&prepare.bytes.to_le_bytes());
    encode_operation(&prepare.operation, &mut out)?;
    Ok(out)
}

fn decode_prepare(bytes: &[u8]) -> RsfResult<VsrPrepare> {
    if bytes.len() < 5 + 8 + 8 + 8 + 8 + 4 + 1 {
        return Err(corrupt("vsr prepare payload is shorter than header"));
    }
    if &bytes[0..5] != VSR_PREPARE_MAGIC {
        return Err(corrupt("vsr prepare payload has invalid magic"));
    }
    let (operation, offset) = decode_operation(bytes, 41)?;
    if offset != bytes.len() {
        return Err(corrupt("vsr prepare payload has trailing bytes"));
    }
    Ok(VsrPrepare {
        pg: PlacementGroupId(read_u64(bytes, 5)?),
        view: Epoch(read_u64(bytes, 13)?),
        index: LogIndex(read_u64(bytes, 21)?),
        primary: NodeId(read_u64(bytes, 29)?),
        bytes: read_u32(bytes, 37)?,
        operation,
    })
}

fn encode_commit(commit: &VsrCommit) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(VSR_COMMIT_MAGIC);
    out.extend_from_slice(&commit.pg.0.to_le_bytes());
    out.extend_from_slice(&commit.view.0.to_le_bytes());
    out.extend_from_slice(&commit.index.0.to_le_bytes());
    out.extend_from_slice(&commit.primary.0.to_le_bytes());
    out.extend_from_slice(&(commit.acknowledgements as u64).to_le_bytes());
    write_nodes(&commit.certificate.signers, &mut out);
    encode_operation(&commit.certificate.operation, &mut out)
        .expect("committed operation already passed prepare encoding");
    out
}

fn decode_commit(bytes: &[u8]) -> RsfResult<VsrCommit> {
    if bytes.len() < 5 + 8 + 8 + 8 + 8 + 8 + 8 + 1 {
        return Err(corrupt(
            "vsr commit payload is shorter than certificate header",
        ));
    }
    if &bytes[0..5] != VSR_COMMIT_MAGIC {
        return Err(corrupt("vsr commit payload has invalid magic"));
    }
    let acknowledgements = usize::try_from(read_u64(bytes, 37)?)
        .map_err(|_| corrupt("vsr commit acknowledgement count does not fit in memory"))?;
    let (signers, offset) = read_nodes(bytes, 45)?;
    let (operation, offset) = decode_operation(bytes, offset)?;
    if offset != bytes.len() {
        return Err(corrupt("vsr commit certificate has trailing bytes"));
    }
    if signers.len() != acknowledgements {
        return Err(corrupt(
            "vsr commit acknowledgement count does not match certificate signer set",
        ));
    }
    let pg = PlacementGroupId(read_u64(bytes, 5)?);
    let view = Epoch(read_u64(bytes, 13)?);
    let index = LogIndex(read_u64(bytes, 21)?);
    let primary = NodeId(read_u64(bytes, 29)?);
    let certificate = VsrCommitCertificate {
        pg,
        view,
        index,
        primary,
        operation,
        signers,
    };
    Ok(VsrCommit {
        pg,
        view,
        index,
        primary,
        acknowledgements,
        certificate,
    })
}

fn is_vsr_commit_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(VSR_COMMIT_MAGIC)
}

fn validate_commit_certificate(
    commit: &VsrCommit,
    prepare: &VsrPrepare,
    replicas: &BTreeSet<NodeId>,
    quorum: usize,
) -> RsfResult<()> {
    if commit.pg != prepare.pg
        || commit.view != prepare.view
        || commit.index != prepare.index
        || commit.primary != prepare.primary
    {
        return Err(corrupt(
            "vsr commit certificate does not match prepared identity",
        ));
    }
    if commit.certificate.pg != commit.pg
        || commit.certificate.view != commit.view
        || commit.certificate.index != commit.index
        || commit.certificate.primary != commit.primary
    {
        return Err(corrupt(
            "vsr commit certificate header does not match commit",
        ));
    }
    if commit.certificate.operation != prepare.operation {
        return Err(corrupt(
            "vsr commit certificate operation does not match prepared operation",
        ));
    }
    if commit.certificate.signers.len() != commit.acknowledgements {
        return Err(corrupt(
            "vsr commit certificate signer count does not match acknowledgement count",
        ));
    }
    if commit.certificate.signers.len() < quorum {
        return Err(corrupt("vsr commit certificate does not contain quorum"));
    }
    if !commit.certificate.signers.contains(&commit.primary) {
        return Err(corrupt("vsr commit certificate is missing primary signer"));
    }
    if !commit
        .certificate
        .signers
        .iter()
        .all(|signer| replicas.contains(signer))
    {
        return Err(corrupt(
            "vsr commit certificate contains non-replica signer",
        ));
    }
    Ok(())
}

fn encode_view_change(change: &VsrViewChange) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + 8 + 8 + 8 + 8);
    out.extend_from_slice(VSR_VIEW_CHANGE_MAGIC);
    out.extend_from_slice(&change.pg.0.to_le_bytes());
    out.extend_from_slice(&change.view.0.to_le_bytes());
    out.extend_from_slice(&change.primary.0.to_le_bytes());
    out.extend_from_slice(&change.committed_tail.0.to_le_bytes());
    out
}

fn decode_view_change(bytes: &[u8]) -> RsfResult<VsrViewChange> {
    if bytes.len() != 5 + 8 + 8 + 8 + 8 {
        return Err(corrupt("vsr view-change payload length mismatch"));
    }
    if &bytes[0..5] != VSR_VIEW_CHANGE_MAGIC {
        return Err(corrupt("vsr view-change payload has invalid magic"));
    }
    Ok(VsrViewChange {
        pg: PlacementGroupId(read_u64(bytes, 5)?),
        view: Epoch(read_u64(bytes, 13)?),
        primary: NodeId(read_u64(bytes, 21)?),
        committed_tail: LogIndex(read_u64(bytes, 29)?),
    })
}

fn is_vsr_view_change_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(VSR_VIEW_CHANGE_MAGIC)
}

fn encode_membership_change(change: &VsrMembershipChange) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(VSR_MEMBERSHIP_CHANGE_MAGIC);
    out.extend_from_slice(&change.pg.0.to_le_bytes());
    out.extend_from_slice(&change.view.0.to_le_bytes());
    out.extend_from_slice(&change.primary.0.to_le_bytes());
    out.extend_from_slice(&change.committed_tail.0.to_le_bytes());
    write_nodes(&change.old_replicas, &mut out);
    write_nodes(&change.new_replicas, &mut out);
    out
}

fn decode_membership_change(bytes: &[u8]) -> RsfResult<VsrMembershipChange> {
    if bytes.len() < 5 + 8 + 8 + 8 + 8 + 8 + 8 {
        return Err(corrupt("vsr membership payload is shorter than header"));
    }
    if &bytes[0..5] != VSR_MEMBERSHIP_CHANGE_MAGIC {
        return Err(corrupt("vsr membership payload has invalid magic"));
    }
    let (old_replicas, offset) = read_nodes(bytes, 37)?;
    let (new_replicas, offset) = read_nodes(bytes, offset)?;
    if offset != bytes.len() {
        return Err(corrupt("vsr membership payload has trailing bytes"));
    }
    Ok(VsrMembershipChange {
        pg: PlacementGroupId(read_u64(bytes, 5)?),
        view: Epoch(read_u64(bytes, 13)?),
        primary: NodeId(read_u64(bytes, 21)?),
        committed_tail: LogIndex(read_u64(bytes, 29)?),
        old_replicas,
        new_replicas,
    })
}

fn is_vsr_membership_change_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(VSR_MEMBERSHIP_CHANGE_MAGIC)
}

fn encode_client_request_commit(commit: &VsrClientRequestCommit) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(VSR_CLIENT_REQUEST_MAGIC);
    out.extend_from_slice(&commit.pg.0.to_le_bytes());
    out.extend_from_slice(&commit.index.0.to_le_bytes());
    out.extend_from_slice(&commit.request.request.to_le_bytes());
    write_string(&commit.request.client, &mut out)?;
    Ok(out)
}

fn decode_client_request_commit(bytes: &[u8]) -> RsfResult<VsrClientRequestCommit> {
    if bytes.len() < 5 + 8 + 8 + 8 + 8 {
        return Err(corrupt("vsr client request payload is shorter than header"));
    }
    if &bytes[0..5] != VSR_CLIENT_REQUEST_MAGIC {
        return Err(corrupt("vsr client request payload has invalid magic"));
    }
    let (client, offset) = read_string(bytes, 29)?;
    if offset != bytes.len() {
        return Err(corrupt("vsr client request payload has trailing bytes"));
    }
    Ok(VsrClientRequestCommit {
        pg: PlacementGroupId(read_u64(bytes, 5)?),
        index: LogIndex(read_u64(bytes, 13)?),
        request: VsrClientRequestId {
            request: read_u64(bytes, 21)?,
            client,
        },
    })
}

fn is_vsr_client_request_payload(bytes: &[u8]) -> bool {
    bytes.starts_with(VSR_CLIENT_REQUEST_MAGIC)
}

fn encode_operation(operation: &VsrOperation, out: &mut Vec<u8>) -> RsfResult<()> {
    match operation {
        VsrOperation::CatalogMutation { namespace, item } => {
            out.push(1);
            write_string(namespace, out)?;
            write_string(item, out)?;
        }
        VsrOperation::TransactionCommit { key } => {
            out.push(2);
            write_string(key, out)?;
        }
        VsrOperation::ObjectManifest { bucket, key } => {
            out.push(3);
            write_string(bucket, out)?;
            write_string(key, out)?;
        }
        VsrOperation::ProtocolOperation { operation_id } => {
            out.push(4);
            out.extend_from_slice(&operation_id.0.to_le_bytes());
        }
        VsrOperation::ReplicatedCommand {
            operation_id,
            payload,
        } => {
            replica::validate_payload(operation, payload.len() as u32).map_err(invalid_input)?;
            out.push(5);
            out.extend_from_slice(&operation_id.0.to_le_bytes());
            out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
            out.extend_from_slice(payload);
        }
    }
    Ok(())
}

fn decode_operation(bytes: &[u8], offset: usize) -> RsfResult<(VsrOperation, usize)> {
    let tag = *bytes
        .get(offset)
        .ok_or_else(|| corrupt("vsr operation tag is missing"))?;
    let offset = offset + 1;
    match tag {
        1 => {
            let (namespace, offset) = read_string(bytes, offset)?;
            let (item, offset) = read_string(bytes, offset)?;
            Ok((VsrOperation::CatalogMutation { namespace, item }, offset))
        }
        2 => {
            let (key, offset) = read_string(bytes, offset)?;
            Ok((VsrOperation::TransactionCommit { key }, offset))
        }
        3 => {
            let (bucket, offset) = read_string(bytes, offset)?;
            let (key, offset) = read_string(bytes, offset)?;
            Ok((VsrOperation::ObjectManifest { bucket, key }, offset))
        }
        4 => Ok((
            VsrOperation::ProtocolOperation {
                operation_id: OperationId(read_u64(bytes, offset)?),
            },
            offset + 8,
        )),
        5 => {
            let operation_id = OperationId(read_u64(bytes, offset)?);
            let len = usize::try_from(read_u64(bytes, offset + 8)?)
                .map_err(|_| corrupt("command length overflow"))?;
            if len == 0 || len > MAX_REPLICATED_COMMAND_BYTES {
                return Err(corrupt("replicated command exceeds payload bounds"));
            }
            let start = offset + 16;
            let end = start
                .checked_add(len)
                .ok_or_else(|| corrupt("command length overflow"))?;
            let payload = bytes
                .get(start..end)
                .ok_or_else(|| corrupt("replicated command is truncated"))?
                .to_vec();
            Ok((
                VsrOperation::ReplicatedCommand {
                    operation_id,
                    payload,
                },
                end,
            ))
        }
        _ => Err(corrupt("unknown vsr operation tag")),
    }
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u64::try_from(value.len())
        .map_err(|_| invalid_input("vsr string length does not fit in record"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: usize) -> RsfResult<(String, usize)> {
    let len = usize::try_from(read_u64(bytes, offset)?)
        .map_err(|_| corrupt("vsr string length does not fit in memory"))?;
    let start = offset + 8;
    let end = start
        .checked_add(len)
        .ok_or_else(|| corrupt("vsr string length overflow"))?;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("vsr string ended early"))?;
    let value = String::from_utf8(raw.to_vec()).map_err(|_| corrupt("vsr string is not utf-8"))?;
    Ok((value, end))
}

fn write_nodes(nodes: &BTreeSet<NodeId>, out: &mut Vec<u8>) {
    out.extend_from_slice(&(nodes.len() as u64).to_le_bytes());
    for node in nodes {
        out.extend_from_slice(&node.0.to_le_bytes());
    }
}

fn read_nodes(bytes: &[u8], offset: usize) -> RsfResult<(BTreeSet<NodeId>, usize)> {
    let len = usize::try_from(read_u64(bytes, offset)?)
        .map_err(|_| corrupt("vsr node set length does not fit in memory"))?;
    let mut offset = offset + 8;
    let mut nodes = BTreeSet::new();
    for _ in 0..len {
        let node = NodeId(read_u64(bytes, offset)?);
        if !nodes.insert(node) {
            return Err(corrupt("vsr node set contains duplicate node"));
        }
        offset += 8;
    }
    Ok((nodes, offset))
}

fn read_u64(bytes: &[u8], start: usize) -> RsfResult<u64> {
    let end = start
        .checked_add(8)
        .ok_or_else(|| corrupt("vsr u64 offset overflow"))?;
    let value = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("vsr payload ended while reading u64"))?;
    Ok(u64::from_le_bytes(value.try_into().expect("slice length")))
}

fn read_u32(bytes: &[u8], start: usize) -> RsfResult<u32> {
    let end = start
        .checked_add(4)
        .ok_or_else(|| corrupt("vsr u32 offset overflow"))?;
    let value = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("vsr payload ended while reading u32"))?;
    Ok(u32::from_le_bytes(value.try_into().expect("slice length")))
}

fn invalid_input(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use rsf_storage::{SegmentLogOptions, SyncPolicy};

    fn op_id(value: u64) -> OperationId {
        OperationId::new(value).expect("test operation id must be non-zero")
    }

    fn cell() -> VsrCell {
        VsrCell::new(
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap()
    }

    #[test]
    fn commits_after_quorum_acknowledgement() {
        let mut cell = cell();
        let operation = VsrOperation::CatalogMutation {
            namespace: "app".to_string(),
            item: "users".to_string(),
        };
        assert!(matches!(
            cell.prepare(NodeId(0), operation.clone(), 128),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged {
                acknowledgements: 2,
                ..
            }
        ));
        let VsrDecision::Committed(commit) = cell.commit(NodeId(0), LogIndex(1)) else {
            panic!("commit should succeed after quorum acknowledgement");
        };
        assert_eq!(cell.committed_count(), 1);
        assert_eq!(commit.certificate.operation, operation);
        assert_eq!(
            commit.certificate.signers,
            [NodeId(0), NodeId(1)].into_iter().collect()
        );
    }

    #[test]
    fn rejects_stale_primary_after_view_change() {
        let mut cell = cell();
        assert!(matches!(
            cell.view_change(Epoch(2), NodeId(1)),
            VsrDecision::ViewChanged { .. }
        ));
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::TransactionCommit {
                    key: "app/users/1".to_string()
                },
                64
            ),
            VsrDecision::Rejected { .. }
        ));
    }

    #[test]
    fn timeout_election_advances_to_next_replica() {
        let mut cell = cell();
        assert!(matches!(
            cell.timeout_election(),
            VsrDecision::ViewChanged {
                view: Epoch(2),
                primary: NodeId(1),
                committed: 0
            }
        ));
        assert_eq!(cell.view(), Epoch(2));
        assert_eq!(cell.primary(), NodeId(1));
        assert!(matches!(
            cell.timeout_election(),
            VsrDecision::ViewChanged {
                view: Epoch(3),
                primary: NodeId(2),
                committed: 0
            }
        ));
    }

    #[test]
    fn reconfiguration_requires_joint_quorum_overlap() {
        let mut cell = cell();
        assert!(matches!(
            cell.reconfigure(
                Epoch(2),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
            ),
            VsrDecision::MembershipChanged { replicas: 4, .. }
        ));
        assert!(matches!(
            cell.reconfigure(Epoch(3), NodeId(3), [NodeId(1), NodeId(2), NodeId(3)]),
            VsrDecision::MembershipChanged {
                replicas: 3,
                committed_tail: LogIndex(0),
                ..
            }
        ));
        assert_eq!(cell.view(), Epoch(3));
        assert_eq!(cell.primary(), NodeId(3));
        assert_eq!(cell.replica_count(), 3);
        assert!(!cell.contains_replica(NodeId(0)));
        assert!(cell.contains_replica(NodeId(3)));
    }

    #[test]
    fn pending_prepare_cannot_reuse_votes_after_membership_or_view_change() {
        for membership_change in [false, true] {
            let mut cell = cell();
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(11),
                },
                1,
            );
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1));
            if membership_change {
                cell.reconfigure(
                    Epoch(2),
                    NodeId(0),
                    [NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
                );
            } else {
                cell.view_change(Epoch(2), NodeId(0));
            }
            assert!(matches!(
                cell.acknowledge(NodeId(2), Epoch(2), LogIndex(1)),
                VsrDecision::Rejected { .. }
            ));
            assert!(matches!(
                cell.commit(NodeId(0), LogIndex(1)),
                VsrDecision::Rejected { .. }
            ));
            assert_eq!(cell.committed_count(), 0);
        }
    }

    #[test]
    fn standalone_cell_commits_and_grows_one_voter_at_a_time() {
        let mut cell = VsrCell::new(PlacementGroupId(1), Epoch(1), NodeId(0), [NodeId(0)]).unwrap();
        assert_eq!(cell.quorum(), 1);
        cell.prepare(
            NodeId(0),
            VsrOperation::ObjectManifest {
                bucket: "b".into(),
                key: "k".into(),
            },
            1,
        );
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)),
            VsrDecision::Committed(_)
        ));
        assert!(matches!(
            cell.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(1)]),
            VsrDecision::MembershipChanged { .. }
        ));
        assert_eq!(cell.quorum(), 2);
        assert!(matches!(
            cell.reconfigure(Epoch(3), NodeId(0), [NodeId(0), NodeId(1), NodeId(2)]),
            VsrDecision::MembershipChanged { .. }
        ));
        assert_eq!(cell.quorum(), 2);
    }

    #[test]
    fn durable_standalone_history_recovers_after_growth_with_new_bootstrap_arguments() {
        let path = test_path("standalone_growth");
        let pg = PlacementGroupId(1);
        let mut voters = vec![NodeId(0)];
        for size in 1..=3 {
            let mut cell =
                DurableVsrCell::open(&path, pg, Epoch(size), NodeId(0), voters.clone()).unwrap();
            if size > 1 {
                assert_eq!(cell.replica_count(), size as usize - 1);
                assert!(matches!(
                    cell.reconfigure(Epoch(size), NodeId(0), voters.clone()),
                    VsrDecision::MembershipChanged { .. }
                ));
            }
            let prepare = cell
                .prepare(
                    NodeId(0),
                    VsrOperation::ProtocolOperation {
                        operation_id: op_id(size),
                    },
                    1,
                )
                .unwrap();
            let VsrDecision::Prepared(prepare) = prepare else {
                panic!("prepare");
            };
            if size > 1 {
                assert!(matches!(
                    cell.commit(NodeId(0), prepare.index).unwrap(),
                    VsrDecision::Rejected { .. }
                ));
                cell.acknowledge(NodeId(1), Epoch(size), prepare.index);
            }
            assert!(matches!(
                cell.commit(NodeId(0), prepare.index).unwrap(),
                VsrDecision::Committed(_)
            ));
            voters.push(NodeId(size));
        }
        let recovered = DurableVsrCell::open(
            &path,
            pg,
            Epoch(99),
            NodeId(2),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .unwrap();
        assert_eq!(recovered.view(), Epoch(3));
        assert_eq!(recovered.primary(), NodeId(0));
        assert_eq!(recovered.committed_count(), 3);
        assert_eq!(recovered.quorum(), 2);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn membership_validation_matches_exhaustive_majority_intersection() {
        // Exhaust all configurations and possible majorities over five voters.
        for old in 1u32..32 {
            for new in 1u32..32 {
                let nodes = |mask: u32| {
                    (0..5)
                        .filter(move |i| mask & (1 << i) != 0)
                        .map(NodeId)
                        .collect::<BTreeSet<_>>()
                };
                let old_nodes = nodes(old);
                let new_nodes = nodes(new);
                let q_old = old.count_ones() / 2 + 1;
                let q_new = new.count_ones() / 2 + 1;
                let disjoint = (1u32..32)
                    .filter(|a| a & old == *a && a.count_ones() >= q_old)
                    .any(|a| {
                        (1u32..32).any(|b| b & new == b && b.count_ones() >= q_new && a & b == 0)
                    });
                assert_eq!(
                    validate_membership_change(&old_nodes, &new_nodes, *new_nodes.first().unwrap())
                        .is_ok(),
                    !disjoint,
                    "old={old:b} new={new:b}"
                );
            }
        }
    }

    #[test]
    fn membership_rejects_overlapping_sets_with_disjoint_possible_majorities() {
        let old = [NodeId(0), NodeId(1), NodeId(2)].into_iter().collect();
        for next in [
            vec![NodeId(1), NodeId(2), NodeId(3)],
            vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        ] {
            assert!(
                validate_membership_change(&old, &next.into_iter().collect(), NodeId(1)).is_err()
            );
        }
    }

    #[test]
    fn rejects_disjoint_reconfiguration() {
        let mut cell = cell();
        assert!(matches!(
            cell.reconfigure(Epoch(2), NodeId(3), [NodeId(3), NodeId(4), NodeId(5)]),
            VsrDecision::Rejected { .. }
        ));
        assert_eq!(cell.view(), Epoch(1));
        assert!(cell.contains_replica(NodeId(0)));
        assert!(!cell.contains_replica(NodeId(3)));
    }

    #[test]
    fn rejects_commit_without_quorum() {
        let mut cell = cell();
        cell.prepare(
            NodeId(0),
            VsrOperation::ObjectManifest {
                bucket: "media".to_string(),
                key: "hero.png".to_string(),
            },
            96,
        );
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)),
            VsrDecision::Rejected { .. }
        ));
    }

    #[test]
    fn durable_cell_recovers_prepare_commit_and_next_index() {
        let path = test_path("durable_cell_recovers_prepare_commit_and_next_index");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");

        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::CatalogMutation {
                    namespace: "app".to_string(),
                    item: "users".to_string()
                },
                128
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");

        assert_eq!(recovered.committed_count(), 1);
        assert_eq!(recovered.next_index(), LogIndex(2));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_rejects_commit_certificate_for_wrong_operation() {
        let path = test_path("durable_cell_rejects_commit_certificate_for_wrong_operation");
        let prepare = VsrPrepare {
            pg: PlacementGroupId(1),
            view: Epoch(1),
            index: LogIndex(1),
            primary: NodeId(0),
            operation: VsrOperation::ProtocolOperation {
                operation_id: op_id(1),
            },
            bytes: 64,
        };
        let commit = VsrCommit {
            pg: PlacementGroupId(1),
            view: Epoch(1),
            index: LogIndex(1),
            primary: NodeId(0),
            acknowledgements: 2,
            certificate: VsrCommitCertificate {
                pg: PlacementGroupId(1),
                view: Epoch(1),
                index: LogIndex(1),
                primary: NodeId(0),
                operation: VsrOperation::ProtocolOperation {
                    operation_id: op_id(2),
                },
                signers: [NodeId(0), NodeId(1)].into_iter().collect(),
            },
        };
        let prepare_payload = encode_prepare(&prepare).expect("encode prepare");
        let commit_payload = encode_commit(&commit);
        let mut store = SegmentStore::open(&path).expect("open segment store");
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Prepare,
                previous_hash: [0_u8; 32],
                payload: &prepare_payload,
            })
            .expect("append prepare");
        let tail = store
            .recover_current()
            .expect("recover tail")
            .recovery
            .pg_tail(PlacementGroupId(1))
            .expect("tail")
            .record_hash;
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(2),
                kind: WalRecordKind::Commit,
                previous_hash: tail,
                payload: &commit_payload,
            })
            .expect("append corrupt commit certificate");
        drop(store);

        let err = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect_err("wrong operation certificate is rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_recovers_client_request_table() {
        let path = test_path("durable_cell_recovers_client_request_table");
        let request = VsrClientRequestId::new("client-a", 77);
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");

        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(9)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        cell.remember_client_request(request.clone(), LogIndex(1))
            .expect("remember request");
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");

        assert_eq!(recovered.request_commit(&request), Some(LogIndex(1)));
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: op_id(9)
            })
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_recovers_across_segment_store_rotation() {
        let path = test_path("durable_cell_recovers_across_segment_store_rotation");
        let options = tiny_store_options();
        let mut cell = DurableVsrCell::open_with_store_options(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
            options,
        )
        .expect("open durable cell");

        for index in 1..=3 {
            assert!(matches!(
                cell.prepare(
                    NodeId(0),
                    VsrOperation::ProtocolOperation {
                        operation_id: op_id(index)
                    },
                    64
                )
                .expect("prepare"),
                VsrDecision::Prepared(_)
            ));
            assert!(matches!(
                cell.acknowledge(NodeId(1), Epoch(1), LogIndex(index)),
                VsrDecision::Acknowledged { .. }
            ));
            assert!(matches!(
                cell.commit(NodeId(0), LogIndex(index)).expect("commit"),
                VsrDecision::Committed(_)
            ));
        }
        cell.remember_client_request(VsrClientRequestId::new("client-rotated", 1), LogIndex(3))
            .expect("remember client request");
        assert!(SegmentStore::recover(&path).unwrap().segments.len() > 1);
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");
        assert_eq!(recovered.committed_count(), 3);
        assert_eq!(recovered.next_index(), LogIndex(4));
        assert_eq!(
            recovered.request_commit(&VsrClientRequestId::new("client-rotated", 1)),
            Some(LogIndex(3))
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_view_change_recovers_across_segment_store_rotation() {
        let path = test_path("durable_view_change_recovers_across_segment_store_rotation");
        let mut cell = DurableVsrCell::open_with_store_options(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
            tiny_store_options(),
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(100)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        assert!(matches!(
            cell.view_change(Epoch(2), NodeId(1)),
            VsrDecision::ViewChanged { committed: 1, .. }
        ));
        assert!(SegmentStore::recover(&path).unwrap().segments.len() > 1);
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");
        assert_eq!(recovered.view(), Epoch(2));
        assert_eq!(recovered.primary(), NodeId(1));
        assert_eq!(recovered.committed_tail(), LogIndex(1));
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: op_id(100)
            })
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_membership_change_recovers_across_segment_store_rotation() {
        let path = test_path("durable_membership_change_recovers_across_segment_store_rotation");
        let mut cell = DurableVsrCell::open_with_store_options(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
            tiny_store_options(),
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(101)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        assert!(matches!(
            cell.reconfigure(
                Epoch(2),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
            ),
            VsrDecision::MembershipChanged { replicas: 4, .. }
        ));
        assert!(matches!(
            cell.reconfigure(Epoch(3), NodeId(3), [NodeId(1), NodeId(2), NodeId(3)]),
            VsrDecision::MembershipChanged {
                replicas: 3,
                committed_tail: LogIndex(1),
                ..
            }
        ));
        assert!(SegmentStore::recover(&path).unwrap().segments.len() > 1);
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");
        assert_eq!(recovered.view(), Epoch(3));
        assert_eq!(recovered.primary(), NodeId(3));
        assert!(!recovered.contains_replica(NodeId(0)));
        assert!(recovered.contains_replica(NodeId(3)));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_rejects_committed_prefix_compaction_without_snapshot() {
        let path = test_path("durable_cell_rejects_committed_prefix_compaction_without_snapshot");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(7)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));

        let err = cell
            .compact_committed_prefix()
            .expect_err("committed VSR state cannot be compacted without snapshot");
        assert_eq!(err.code, ErrorCode::InvalidInput);
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");
        assert_eq!(recovered.committed_tail(), LogIndex(1));
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: op_id(7)
            })
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_recovers_after_store_truncates_uncommitted_tail() {
        let path = test_path("durable_cell_recovers_after_store_truncates_uncommitted_tail");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(1)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(2)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        drop(cell);

        let mut store = SegmentStore::open(&path).expect("open store");
        let report = store
            .truncate_uncommitted_tails()
            .expect("truncate uncommitted tail");
        assert_eq!(report.dropped_wal_records, 1);
        drop(store);

        let mut recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");
        assert_eq!(recovered.committed_count(), 1);
        assert_eq!(recovered.next_index(), LogIndex(2));
        assert!(matches!(
            recovered
                .prepare(
                    NodeId(0),
                    VsrOperation::ProtocolOperation {
                        operation_id: op_id(3)
                    },
                    64
                )
                .expect("prepare"),
            VsrDecision::Prepared(VsrPrepare {
                index: LogIndex(2),
                ..
            })
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_rejects_stale_primary_after_recovered_view_change() {
        let path = test_path("durable_cell_rejects_stale_primary_after_recovered_view_change");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.view_change(Epoch(2), NodeId(1)),
            VsrDecision::ViewChanged { .. }
        ));
        drop(cell);

        let mut recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");
        assert_eq!(recovered.view(), Epoch(2));
        assert_eq!(recovered.primary(), NodeId(1));
        assert!(matches!(
            recovered
                .prepare(
                    NodeId(0),
                    VsrOperation::TransactionCommit {
                        key: "app/users/1".to_string()
                    },
                    64
                )
                .expect("prepare rejection"),
            VsrDecision::Rejected { .. }
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_timeout_election_preserves_committed_prefix_after_restart() {
        let path = test_path("durable_timeout_election_preserves_committed_prefix_after_restart");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(72)
                },
                64
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        assert!(matches!(
            cell.timeout_election(),
            VsrDecision::ViewChanged {
                view: Epoch(2),
                primary: NodeId(1),
                committed: 1
            }
        ));
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");

        assert_eq!(recovered.view(), Epoch(2));
        assert_eq!(recovered.primary(), NodeId(1));
        assert_eq!(recovered.committed_tail(), LogIndex(1));
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: op_id(72)
            })
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_membership_change_recovers_new_voters() {
        let path = test_path("durable_membership_change_recovers_new_voters");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.reconfigure(
                Epoch(2),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
            ),
            VsrDecision::MembershipChanged { replicas: 4, .. }
        ));
        assert!(matches!(
            cell.reconfigure(Epoch(3), NodeId(3), [NodeId(1), NodeId(2), NodeId(3)]),
            VsrDecision::MembershipChanged {
                replicas: 3,
                committed_tail: LogIndex(0),
                ..
            }
        ));
        drop(cell);

        let mut recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");

        assert_eq!(recovered.view(), Epoch(3));
        assert_eq!(recovered.primary(), NodeId(3));
        assert_eq!(recovered.replica_count(), 3);
        assert!(!recovered.contains_replica(NodeId(0)));
        assert!(recovered.contains_replica(NodeId(3)));
        assert!(matches!(
            recovered
                .prepare(
                    NodeId(3),
                    VsrOperation::ProtocolOperation {
                        operation_id: op_id(50)
                    },
                    64
                )
                .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            recovered.acknowledge(NodeId(0), Epoch(3), LogIndex(1)),
            VsrDecision::Rejected { .. }
        ));
        assert!(matches!(
            recovered.acknowledge(NodeId(1), Epoch(3), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            recovered.commit(NodeId(3), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_membership_change_rejects_corrupt_old_voters() {
        let path = test_path("durable_membership_change_rejects_corrupt_old_voters");
        let mut store = SegmentStore::open(&path).expect("open segment store");
        let change = VsrMembershipChange {
            pg: PlacementGroupId(1),
            view: Epoch(2),
            primary: NodeId(3),
            committed_tail: LogIndex(0),
            old_replicas: [NodeId(0), NodeId(2), NodeId(4)].into_iter().collect(),
            new_replicas: [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
        };
        let payload = encode_membership_change(&change);
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Membership,
                previous_hash: [0_u8; 32],
                payload: &payload,
            })
            .expect("append corrupt membership change");
        drop(store);

        let err = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect_err("corrupt membership old voters are rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_cell_catches_up_committed_prefix_before_voting() {
        let source_path = test_path("durable_cell_catches_up_source");
        let learner_path = test_path("durable_cell_catches_up_learner");
        let mut source = DurableVsrCell::open(
            &source_path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open source cell");
        assert!(matches!(
            source
                .prepare(
                    NodeId(0),
                    VsrOperation::ProtocolOperation {
                        operation_id: op_id(61)
                    },
                    64
                )
                .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            source.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            source.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        let entries = source.committed_prefix().expect("committed prefix");

        let mut learner = DurableVsrCell::open(
            &learner_path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open learner cell");
        assert_eq!(learner.catch_up_from(&entries).expect("catch up"), 1);
        assert_eq!(learner.committed_tail(), LogIndex(1));
        assert_eq!(
            learner.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: op_id(61)
            })
        );
        drop(learner);

        let recovered = DurableVsrCell::open(
            &learner_path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover learner cell");
        assert_eq!(recovered.committed_tail(), LogIndex(1));
        assert_eq!(recovered.next_index(), LogIndex(2));

        let _ = std::fs::remove_dir_all(source_path);
        let _ = std::fs::remove_dir_all(learner_path);
    }

    #[test]
    fn durable_view_change_recovers_committed_prefix_proof() {
        let path = test_path("durable_view_change_recovers_committed_prefix_proof");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(42)
                },
                96
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        assert!(matches!(
            cell.view_change(Epoch(2), NodeId(1)),
            VsrDecision::ViewChanged { committed: 1, .. }
        ));
        drop(cell);

        let recovered = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("recover durable cell");

        assert_eq!(recovered.view(), Epoch(2));
        assert_eq!(recovered.primary(), NodeId(1));
        assert_eq!(recovered.committed_count(), 1);
        assert_eq!(recovered.committed_tail(), LogIndex(1));
        assert_eq!(
            recovered.committed_operation(LogIndex(1)),
            Some(&VsrOperation::ProtocolOperation {
                operation_id: op_id(42)
            })
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_view_change_rejects_committed_prefix_regression() {
        let path = test_path("durable_view_change_rejects_committed_prefix_regression");
        let mut cell = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("open durable cell");
        assert!(matches!(
            cell.prepare(
                NodeId(0),
                VsrOperation::ProtocolOperation {
                    operation_id: op_id(43)
                },
                96
            )
            .expect("prepare"),
            VsrDecision::Prepared(_)
        ));
        assert!(matches!(
            cell.acknowledge(NodeId(1), Epoch(1), LogIndex(1)),
            VsrDecision::Acknowledged { .. }
        ));
        assert!(matches!(
            cell.commit(NodeId(0), LogIndex(1)).expect("commit"),
            VsrDecision::Committed(_)
        ));
        drop(cell);

        let mut store = SegmentStore::open(&path).expect("open segment store");
        let recovery = store
            .recover_current()
            .expect("recover segment store")
            .recovery;
        let tail = recovery.pg_tail(PlacementGroupId(1)).expect("pg tail");
        store
            .append_wal(WalAppend {
                pg: PlacementGroupId(1),
                epoch: Epoch(1),
                index: LogIndex(tail.index.0 + 1),
                kind: WalRecordKind::Membership,
                previous_hash: tail.record_hash,
                payload: &encode_view_change(&VsrViewChange {
                    pg: PlacementGroupId(1),
                    view: Epoch(2),
                    primary: NodeId(1),
                    committed_tail: LogIndex(0),
                }),
            })
            .expect("append corrupt view change");
        drop(store);

        let err = DurableVsrCell::open(
            &path,
            PlacementGroupId(1),
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect_err("corrupt view-change prefix is rejected");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        let _ = std::fs::remove_dir_all(path);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-vsr-{name}-{}-{nonce}", std::process::id()))
    }

    fn tiny_store_options() -> SegmentStoreOptions {
        SegmentStoreOptions {
            log_options: SegmentLogOptions {
                sync_policy: SyncPolicy::Data,
            },
            max_segment_bytes: 1,
        }
    }
}
