//! Namespace-bound immutable commands. Payload staging precedes the local receipt;
//! the authoritative VSR command can rebuild a receipt after any interrupted write.
use super::*;
use rsf_core::{Epoch, LogIndex, storage::WalRecordKind};
use rsf_protocol::{ApplicationPayload, ApplicationSurface, FabricCommit, ReplicatedFabricCommand};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const BINDING: &[u8] = b"RSFBNS1";
const PAYLOAD: &[u8] = b"RSFB1";
const JOURNAL_PG: PlacementGroupId = PlacementGroupId(0);

#[derive(Debug)]
pub(super) struct Journal {
    store: SegmentStore,
    pub(super) namespace: Option<String>,
    receipts: BTreeMap<ClientRequestId, ReplicatedFabricCommand>,
    next: u64,
    highest: CommitVersion,
    failure: Option<String>,
}
fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}
impl Journal {
    pub(super) fn open(root: &Path) -> RsfResult<Self> {
        let store = SegmentStore::open(root.join("replication"))?;
        let records = store.scan(SegmentScanFilter { pg: None })?.wal_records;
        let mut journal = Self {
            store,
            namespace: None,
            receipts: BTreeMap::new(),
            next: 1,
            highest: CommitVersion::GENESIS,
            failure: None,
        };
        for record in records {
            if record.kind != WalRecordKind::BlobManifest
                || record.pg != JOURNAL_PG
                || record.index.0 != journal.next
            {
                return Err(corrupt("invalid blob replication journal prefix"));
            }
            if let Some(raw) = record.payload.strip_prefix(BINDING) {
                let namespace = std::str::from_utf8(raw)
                    .map_err(|_| corrupt("invalid blob namespace encoding"))?;
                validate_namespace(namespace)?;
                if journal.next != 1 || journal.namespace.is_some() {
                    return Err(corrupt(
                        "blob namespace must be bound exactly once before receipts",
                    ));
                }
                journal.namespace = Some(namespace.into());
            } else {
                let command = ReplicatedFabricCommand::decode(&record.payload)?;
                journal.validate(&command)?;
                journal.install(command);
            }
            journal.next = journal
                .next
                .checked_add(1)
                .ok_or_else(|| corrupt("blob journal exhausted"))?;
        }
        Ok(journal)
    }
    pub(super) fn ensure_healthy(&self) -> RsfResult<()> {
        match &self.failure {
            Some(reason) => Err(corrupt(format!(
                "blob journal failed; reopen before further operations: {reason}"
            ))),
            None => Ok(()),
        }
    }
    fn append(&mut self, payload: &[u8]) -> RsfResult<()> {
        self.ensure_healthy()?;
        let result: RsfResult<()> = (|| {
            let next = self
                .next
                .checked_add(1)
                .ok_or_else(|| corrupt("blob journal exhausted"))?;
            let previous_hash = self
                .store
                .recover_current()?
                .recovery
                .pg_tail(JOURNAL_PG)
                .map_or([0; 32], |tail| tail.record_hash);
            self.store.append_wal(WalAppend {
                pg: JOURNAL_PG,
                epoch: Epoch(1),
                index: LogIndex(self.next),
                kind: WalRecordKind::BlobManifest,
                previous_hash,
                payload,
            })?;
            self.next = next;
            Ok(())
        })();
        if let Err(err) = &result {
            self.failure = Some(err.message.clone());
        }
        result
    }
    fn validate(&self, command: &ReplicatedFabricCommand) -> RsfResult<()> {
        self.ensure_healthy()?;
        command.encode()?;
        if self.namespace.as_deref() != Some(command.application.namespace.as_str())
            || command.application.surface != ApplicationSurface::Blob
        {
            return Err(invalid(
                "blob command targets a different namespace or surface",
            ));
        }
        let bytes = payload_bytes(command)?;
        let operation = FabricOperation::BlobPin {
            digest: *ContentId::for_bytes(bytes).as_bytes(),
        };
        if command.commit.operation != operation
            || command.commit.mode != ConsistencyMode::AvailabilityFirst
        {
            return Err(corrupt("blob command does not match its immutable payload"));
        }
        if self.receipts.contains_key(&command.request) || command.commit.version <= self.highest {
            return Err(corrupt("blob command repeats or reorders durable history"));
        }
        Ok(())
    }
    fn install(&mut self, command: ReplicatedFabricCommand) {
        self.highest = command.commit.version;
        self.receipts.insert(command.request.clone(), command);
    }
}
fn validate_namespace(namespace: &str) -> RsfResult<()> {
    if namespace.is_empty() || namespace.len() > 4096 || namespace.chars().any(char::is_control) {
        return Err(invalid("invalid blob replication namespace"));
    }
    Ok(())
}
fn payload_bytes(command: &ReplicatedFabricCommand) -> RsfResult<&[u8]> {
    command
        .application
        .bytes
        .strip_prefix(PAYLOAD)
        .filter(|bytes| !bytes.is_empty())
        .ok_or_else(|| corrupt("invalid or empty replicated blob payload"))
}
impl DurableBlobEngine {
    pub fn open_replicated(root: impl AsRef<Path>, namespace: &str) -> RsfResult<Self> {
        validate_namespace(namespace)?;
        let mut engine = Self::open(root)?;
        if let Some(existing) = &engine.replication.namespace {
            return if existing == namespace {
                Ok(engine)
            } else {
                Err(invalid("blob store belongs to a different namespace"))
            };
        }
        let report = engine.verify_all()?;
        if report.chunks != 0 || report.quarantined != 0 {
            return Err(invalid(
                "nonempty legacy blob store requires explicit namespace migration",
            ));
        }
        let mut binding = BINDING.to_vec();
        binding.extend_from_slice(namespace.as_bytes());
        engine.replication.append(&binding)?;
        engine.replication.namespace = Some(namespace.into());
        Ok(engine)
    }

    /// Replicate exact bytes and the caller's idempotency identity. The store must
    /// be namespace-bound; no successful response is possible from staging alone.
    pub fn put_request(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        request: ClientRequestId,
        bytes: impl Into<Bytes>,
    ) -> RsfResult<DurableBlobRecord> {
        self.replication.ensure_healthy()?;
        let namespace =
            self.replication.namespace.clone().ok_or_else(|| {
                invalid("blob request replication requires a namespace-bound store")
            })?;
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(invalid("blob payload must not be empty"));
        }
        let id = ContentId::for_bytes(&bytes);
        let operation = FabricOperation::BlobPin {
            digest: *id.as_bytes(),
        };
        let mut body = PAYLOAD.to_vec();
        body.extend_from_slice(&bytes);
        let application = ApplicationPayload {
            namespace,
            surface: ApplicationSurface::Blob,
            bytes: body,
        };
        if router.latest_version() < self.replication.highest {
            return Err(corrupt("blob store is ahead of recovered protocol history"));
        }
        if let Some(original) = self.replication.receipts.get(&request) {
            if original.commit.pg != pg || original.application != application {
                return Err(invalid(
                    "blob retry differs from its original complete command",
                ));
            }
            if router.request_certificate(&request) != Some(original.certificate()) {
                return Err(corrupt(
                    "blob retry requires matching recovered protocol history",
                ));
            }
        }
        router.validate_application_request(
            pg,
            &request,
            ConsistencyMode::AvailabilityFirst,
            &operation,
            &application,
        )?;
        let staged = self.stage(bytes)?;
        let commit = router.commit_application_request(
            pg,
            request.clone(),
            ConsistencyMode::AvailabilityFirst,
            operation.clone(),
            application.clone(),
        )?;
        let certificate = commit.certificate;
        if certificate.pg != pg
            || certificate.operation_id != operation.operation_id()
            || certificate.mode != ConsistencyMode::AvailabilityFirst
        {
            return Err(corrupt("blob certificate differs from submitted command"));
        }
        let command = ReplicatedFabricCommand {
            request,
            application,
            commit: FabricCommit {
                pg,
                index: certificate.index,
                version: certificate.version,
                mode: certificate.mode,
                operation_id: certificate.operation_id,
                operation,
            },
        };
        let mut record = self.apply_application_command(&command)?;
        record.deduped = staged.deduped;
        Ok(record)
    }

    /// Called only with an authenticated, quorum-validated command. A repeated
    /// command repairs its immutable bytes without adding pins or another receipt.
    pub fn apply_application_command(
        &mut self,
        command: &ReplicatedFabricCommand,
    ) -> RsfResult<DurableBlobRecord> {
        self.replication.ensure_healthy()?;
        let existing = self.replication.receipts.get(&command.request);
        if let Some(original) = existing {
            if original != command {
                return Err(corrupt(
                    "replicated blob receipt conflicts with original command",
                ));
            }
        } else {
            self.replication.validate(command)?;
        }
        let replayed = existing.is_some();
        let staged = self.stage(payload_bytes(command)?.to_vec())?;
        if !replayed {
            self.replication.append(&command.encode()?)?;
            self.replication.install(command.clone());
        }
        Ok(DurableBlobRecord {
            id: staged.id,
            bytes: staged.bytes,
            pin_version: command.commit.version,
            pin_evidence: commit_evidence(command.certificate()),
            deduped: staged.deduped,
            pins: staged.pins,
        })
    }
}

#[cfg(test)]
mod tests;
