use super::*;
use std::fs::OpenOptions;

const NAMESPACE: &[u8; 7] = b"RSFKNS1";

const INTENT: &[u8; 6] = b"RSFKI1";
const RECEIPT: &[u8; 6] = b"RSFKR1";
const INTERNAL_CLIENT: &str = "rsf-kv-internal:";

#[derive(Clone, Debug, Eq, PartialEq)]
struct Intent {
    request: ClientRequestId,
    pg: PlacementGroupId,
    mutations: Vec<KvMutation>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Applied {
    evidence: CommitEvidence,
    mutations: Vec<KvMutation>,
}
#[derive(Debug, Default)]
pub(super) struct Journal {
    namespace: Option<String>,
    pending: BTreeMap<ClientRequestId, Intent>,
    receipts: BTreeMap<ClientRequestId, (PlacementGroupId, LogIndex)>,
    highest_version: u64,
    applied: BTreeMap<(PlacementGroupId, LogIndex), Applied>,
    failure: Option<String>,
}

pub(super) fn operation(mutations: &[KvMutation]) -> FabricOperation {
    FabricOperation::KvBatch {
        keys: mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect(),
    }
}
fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

impl DurableKvEngine {
    pub(super) fn open_journal(path: &Path, epoch: Epoch) -> RsfResult<Self> {
        std::fs::create_dir_all(path)
            .map_err(|err| corrupt(format!("cannot create KV root: {err}")))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join(".kv-writer.lock"))
            .map_err(|err| corrupt(format!("cannot open KV ownership lock: {err}")))?;
        let lock = rsf_storage::ExclusiveFileLock::acquire(lock)
            .map_err(|err| invalid(format!("KV store already has a writer: {err}")))?;
        let (store, recovered) = SegmentStore::open_recovered(path, Default::default())?;
        let mut durable = Self {
            engine: KvEngine::default(),
            store,
            epoch,
            journal: Journal::default(),
            _writer_lock: lock,
        };
        for record in recovered.recovery.wal_records {
            match record.kind {
                WalRecordKind::KvMutation if record.payload.starts_with(NAMESPACE) => {
                    let namespace = std::str::from_utf8(&record.payload[NAMESPACE.len()..])
                        .map_err(|_| corrupt("KV namespace is not UTF-8"))?;
                    durable.validate_namespace_binding(namespace)?;
                    durable.journal.namespace = Some(namespace.to_owned());
                }
                WalRecordKind::KvMutation if record.payload.starts_with(INTENT) => {
                    let intent = decode_intent(record.pg, &record.payload)?;
                    if !durable.journal.pending.is_empty()
                        || durable.journal.receipts.contains_key(&intent.request)
                    {
                        return Err(corrupt(
                            "KV intent overlaps an unresolved or completed request",
                        ));
                    }
                    durable
                        .journal
                        .pending
                        .insert(intent.request.clone(), intent);
                }
                WalRecordKind::KvMutation if record.payload.starts_with(RECEIPT) => {
                    let (request, applied) = decode_receipt(record.pg, &record.payload)?;
                    durable.install_receipt(request, applied)?;
                }
                WalRecordKind::KvMutation => {
                    let batch = decode_kv_wal_payload(&record.payload)?;
                    let evidence = CommitEvidence {
                        placement_group: record.pg,
                        log_index: batch.protocol_log_index,
                        version: batch.version,
                        operation_id: operation(&batch.mutations).operation_id(),
                        consistency: CommitConsistency::Strict,
                    };
                    durable.install_applied(Applied {
                        evidence,
                        mutations: batch.mutations,
                    })?;
                }
                WalRecordKind::Snapshot => {
                    let snapshot = decode_kv_snapshot_payload(&record.payload)?;
                    durable.journal.highest_version = durable
                        .journal
                        .highest_version
                        .max(snapshot.highest_version.0);
                    durable.engine.replace_data(snapshot);
                }
                _ => continue,
            }
        }
        Ok(durable)
    }

    /// Resolve an interrupted durable intent before serving application reads or
    /// later writes. A previously uncertain request may commit during recovery.
    pub fn reconcile(&mut self, router: &mut impl FabricRouterPort) -> RsfResult<usize> {
        self.ensure_healthy()?;
        if router.latest_version().0 < self.journal.highest_version {
            return Err(invalid(
                "KV router is behind the materialized committed version",
            ));
        }
        let pending = self.journal.pending.values().cloned().collect::<Vec<_>>();
        for intent in &pending {
            let result = self.submit_intent(router, intent)?;
            validate_certificate(&result.certificate, intent.pg, &intent.mutations)?;
            self.persist_receipt(
                intent.request.clone(),
                Applied {
                    evidence: commit_evidence(result.certificate),
                    mutations: intent.mutations.clone(),
                },
            )?;
        }
        Ok(pending.len())
    }

    pub fn pending_request_count(&self) -> usize {
        self.journal.pending.len()
    }

    pub(super) fn commit_durable(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        batch: KvBatch,
    ) -> RsfResult<KvCommitResult> {
        self.ensure_healthy()?;
        if batch
            .client_request
            .as_ref()
            .is_some_and(|request| request.client.0.starts_with(INTERNAL_CLIENT))
        {
            return Err(invalid("reserved internal KV client identity"));
        }
        if let Some(request) = &batch.client_request {
            if let Some(pending) = self.journal.pending.get(request) {
                if pending.pg != pg || pending.mutations != batch.mutations {
                    return Err(invalid("KV retry differs from its durable intent"));
                }
            }
            if let Some(applied) = self.receipt(request) {
                self.verify_replicated_retry(router, request, pg, &batch.mutations, applied)?;
                return receipt_result(applied, pg, &batch.mutations, true);
            }
        }
        self.reconcile(router)?;
        if let Some(request) = &batch.client_request {
            if let Some(applied) = self.receipt(request) {
                self.verify_replicated_retry(router, request, pg, &batch.mutations, applied)?;
                return receipt_result(applied, pg, &batch.mutations, true);
            }
            // Upgrade legacy durable writes only when this store can prove the
            // original full mutation. A certificate containing only keys cannot
            // safely recover unknown values supplied by a retry.
            if let Some(certificate) = router.request_certificate(request) {
                validate_certificate(&certificate, pg, &batch.mutations)?;
                let evidence = commit_evidence(certificate);
                let Some(applied) = self.journal.applied.get(&(pg, evidence.log_index)).cloned()
                else {
                    return Err(corrupt(
                        "protocol request has no KV intent or durable values; recovery requires the original command",
                    ));
                };
                if applied.evidence != evidence || applied.mutations != batch.mutations {
                    return Err(invalid("KV retry differs from its original durable write"));
                }
                self.persist_receipt(request.clone(), applied.clone())?;
                return receipt_result(&applied, pg, &batch.mutations, true);
            }
        }
        validate_mutations(&batch.mutations)?;
        for mutation in &batch.mutations {
            self.engine
                .ensure_write_not_conflicted(mutation.key(), batch.snapshot.version)?;
        }
        let operation = operation(&batch.mutations);
        ensure_operation_matches_router_route(router, pg, &operation)?;
        let request = match batch.client_request {
            Some(request) => request,
            None => {
                let mut nonce = [0; 32];
                getrandom::getrandom(&mut nonce).map_err(|err| {
                    invalid(format!(
                        "cannot allocate durable KV request identity: {err}"
                    ))
                })?;
                let suffix = nonce
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                ClientRequestId::new(format!("{INTERNAL_CLIENT}{suffix}"), 0)
            }
        };
        let intent = Intent {
            request: request.clone(),
            pg,
            mutations: batch.mutations.clone(),
        };
        if let Some(application) = self.application_payload(&intent)? {
            router.validate_application_request(
                pg,
                &request,
                ConsistencyMode::Strict,
                &operation,
                &application,
            )?;
        }
        self.append_journal(pg, &encode_intent(&intent)?)?;
        self.journal.pending.insert(request.clone(), intent.clone());
        let result = self.submit_intent(router, &intent)?;
        validate_certificate(&result.certificate, pg, &batch.mutations)?;
        let applied = Applied {
            evidence: commit_evidence(result.certificate),
            mutations: batch.mutations,
        };
        self.persist_receipt(request, applied.clone())?;
        receipt_result(&applied, pg, &applied.mutations, false)
    }

    /// Materialize an already certified command. The caller must verify its
    /// quorum certificate and obtain these mutations from that command's full
    /// payload. This method atomically persists local values plus the receipt;
    /// it does not create another protocol commit.
    pub fn apply_committed_batch(
        &mut self,
        request: ClientRequestId,
        evidence: CommitEvidence,
        mutations: Vec<KvMutation>,
    ) -> RsfResult<KvCommitResult> {
        self.ensure_healthy()?;
        if let Some(applied) = self.receipt(&request) {
            if applied.evidence != evidence {
                return Err(invalid("KV replay certificate differs from its receipt"));
            }
            return receipt_result(applied, evidence.placement_group, &mutations, true);
        }
        let applied = Applied {
            evidence,
            mutations,
        };
        self.persist_receipt(request, applied.clone())?;
        receipt_result(
            &applied,
            applied.evidence.placement_group,
            &applied.mutations,
            false,
        )
    }

    /// Open an empty or already namespace-bound store for complete command replication.
    /// Legacy nonempty stores require explicit migration before this mode is enabled.
    pub fn open_replicated(
        path: impl AsRef<Path>,
        epoch: Epoch,
        namespace: &str,
    ) -> RsfResult<Self> {
        let mut store = Self::open(path, epoch)?;
        match store.journal.namespace.as_deref() {
            Some(existing) if existing == namespace => return Ok(store),
            Some(_) => return Err(invalid("KV store belongs to a different namespace")),
            None => {}
        }
        store.validate_namespace_binding(namespace)?;
        let mut payload = NAMESPACE.to_vec();
        payload.extend_from_slice(namespace.as_bytes());
        store.append_journal(PlacementGroupId(0), &payload)?;
        store.journal.namespace = Some(namespace.to_owned());
        Ok(store)
    }
    fn validate_namespace_binding(&self, namespace: &str) -> RsfResult<()> {
        if namespace.is_empty() || namespace.len() > 4096 || namespace.chars().any(char::is_control)
        {
            return Err(invalid("invalid KV replication namespace"));
        }
        if self.journal.namespace.is_some()
            || !self.journal.applied.is_empty()
            || !self.journal.pending.is_empty()
            || self.journal.highest_version != 0
        {
            return Err(invalid(
                "nonempty or already bound KV store requires explicit namespace migration",
            ));
        }
        Ok(())
    }
    fn application_payload(
        &self,
        intent: &Intent,
    ) -> RsfResult<Option<rsf_protocol::ApplicationPayload>> {
        self.journal
            .namespace
            .as_ref()
            .map(|namespace| {
                Ok(rsf_protocol::ApplicationPayload {
                    namespace: namespace.clone(),
                    surface: rsf_protocol::ApplicationSurface::Kv,
                    bytes: encode_intent(intent)?,
                })
            })
            .transpose()
    }
    fn submit_intent(
        &self,
        router: &mut impl FabricRouterPort,
        intent: &Intent,
    ) -> RsfResult<rsf_protocol::RequestCommitEvidence> {
        let operation = operation(&intent.mutations);
        match self.application_payload(intent)? {
            Some(application) => router.commit_application_request(
                intent.pg,
                intent.request.clone(),
                ConsistencyMode::Strict,
                operation,
                application,
            ),
            None => router.commit_request(intent.pg, intent.request.clone(), operation),
        }
    }
    fn verify_replicated_retry(
        &self,
        router: &mut impl FabricRouterPort,
        request: &ClientRequestId,
        pg: PlacementGroupId,
        mutations: &[KvMutation],
        applied: &Applied,
    ) -> RsfResult<()> {
        receipt_result(applied, pg, mutations, true)?;
        if self.journal.namespace.is_some() {
            if router.latest_version().0 < self.journal.highest_version
                || router
                    .request_certificate(request)
                    .map(commit_evidence)
                    .as_ref()
                    != Some(&applied.evidence)
            {
                return Err(corrupt(
                    "replicated KV retry requires the matching recovered protocol history",
                ));
            }
            let receipt = self.submit_intent(
                router,
                &Intent {
                    request: request.clone(),
                    pg,
                    mutations: mutations.to_vec(),
                },
            )?;
            if commit_evidence(receipt.certificate) != applied.evidence {
                return Err(corrupt("KV retry returned different protocol evidence"));
            }
        }
        Ok(())
    }
    /// Apply a complete command from an authenticated, quorum-validated VSR entry.
    /// Namespace and surface bindings are checked before any local mutation.
    pub fn apply_application_command(
        &mut self,
        command: &rsf_protocol::ReplicatedFabricCommand,
    ) -> RsfResult<KvCommitResult> {
        if self.journal.namespace.as_deref() != Some(command.application.namespace.as_str())
            || command.application.surface != rsf_protocol::ApplicationSurface::Kv
        {
            return Err(invalid(
                "replicated KV command targets a different namespace or surface",
            ));
        }
        command.encode()?;
        if !command.application.bytes.starts_with(INTENT) {
            return Err(corrupt("unsupported replicated KV command format"));
        }
        let intent = decode_intent(command.commit.pg, &command.application.bytes)?;
        if intent.request != command.request
            || operation(&intent.mutations) != command.commit.operation
        {
            return Err(corrupt(
                "KV command body differs from its protocol operation or request",
            ));
        }
        let certificate = command.certificate();
        validate_certificate(&certificate, intent.pg, &intent.mutations)?;
        self.apply_committed_batch(
            intent.request,
            commit_evidence(certificate),
            intent.mutations,
        )
    }

    fn persist_receipt(&mut self, request: ClientRequestId, applied: Applied) -> RsfResult<()> {
        self.validate_receipt(&request, &applied)?;
        self.append_journal(
            applied.evidence.placement_group,
            &encode_receipt(&request, &applied)?,
        )?;
        self.install_receipt(request, applied)
    }

    fn validate_receipt(&self, request: &ClientRequestId, applied: &Applied) -> RsfResult<()> {
        validate_applied(applied)?;
        if self.journal.receipts.contains_key(request) {
            return Err(corrupt("duplicate durable KV request receipt"));
        }
        if let Some(intent) = self.journal.pending.get(request) {
            if intent.pg != applied.evidence.placement_group
                || intent.mutations != applied.mutations
            {
                return Err(corrupt("KV receipt does not match its durable intent"));
            }
        } else if !self.journal.pending.is_empty() {
            return Err(corrupt("KV receipt overtakes an unresolved intent"));
        }
        self.validate_applied(applied)
    }

    fn validate_applied(&self, applied: &Applied) -> RsfResult<()> {
        validate_applied(applied)?;
        let key = (applied.evidence.placement_group, applied.evidence.log_index);
        if let Some(existing) = self.journal.applied.get(&key) {
            if existing != applied {
                return Err(corrupt(
                    "KV committed position conflicts with durable values",
                ));
            }
            return Ok(());
        }
        if self.journal.highest_version >= applied.evidence.version.0 {
            return Err(corrupt("KV apply must advance the committed version"));
        }
        Ok(())
    }
    fn install_applied(&mut self, applied: Applied) -> RsfResult<()> {
        self.validate_applied(&applied)?;
        let key = (applied.evidence.placement_group, applied.evidence.log_index);
        if !self.journal.applied.contains_key(&key) {
            self.engine
                .apply_batch_at(applied.evidence.version, applied.mutations.clone());
            self.journal.highest_version = applied.evidence.version.0;
            self.journal.applied.insert(key, applied);
        }
        Ok(())
    }
    fn receipt(&self, request: &ClientRequestId) -> Option<&Applied> {
        self.journal
            .receipts
            .get(request)
            .and_then(|key| self.journal.applied.get(key))
    }
    fn install_receipt(&mut self, request: ClientRequestId, applied: Applied) -> RsfResult<()> {
        self.validate_receipt(&request, &applied)?;
        let key = (applied.evidence.placement_group, applied.evidence.log_index);
        self.install_applied(applied)?;
        self.journal.pending.remove(&request);
        self.journal.receipts.insert(request, key);
        Ok(())
    }
    pub(super) fn ensure_healthy(&self) -> RsfResult<()> {
        if let Some(failure) = &self.journal.failure {
            return Err(corrupt(format!(
                "KV persistence failed; reopen before further writes: {failure}"
            )));
        }
        Ok(())
    }
    fn append_journal(&mut self, pg: PlacementGroupId, payload: &[u8]) -> RsfResult<()> {
        self.append_record(pg, WalRecordKind::KvMutation, payload)
            .map(|_| ())
    }

    pub(super) fn append_record(
        &mut self,
        pg: PlacementGroupId,
        kind: WalRecordKind,
        payload: &[u8],
    ) -> RsfResult<LogIndex> {
        self.ensure_healthy()?;
        let result: RsfResult<LogIndex> = (|| {
            let (_, _, record) = self.store.append_next_wal(pg, self.epoch, kind, payload)?;
            Ok(record.index)
        })();
        if let Err(err) = &result {
            self.journal.failure = Some(err.message.clone());
        }
        result
    }
}

fn validate_certificate(
    certificate: &CommitCertificate,
    pg: PlacementGroupId,
    mutations: &[KvMutation],
) -> RsfResult<()> {
    if certificate.pg != pg
        || certificate.index.0 == 0
        || certificate.version.0 == 0
        || certificate.mode != ConsistencyMode::Strict
        || certificate.operation_id != operation(mutations).operation_id()
    {
        return Err(corrupt(
            "KV protocol certificate does not match the intended strict batch",
        ));
    }
    Ok(())
}
fn validate_mutations(mutations: &[KvMutation]) -> RsfResult<()> {
    let keys = mutations
        .iter()
        .map(KvMutation::key)
        .collect::<std::collections::BTreeSet<_>>();
    if mutations.is_empty() || keys.len() != mutations.len() {
        return Err(invalid(
            "KV batch requires nonempty mutations with unique keys",
        ));
    }
    Ok(())
}
fn validate_applied(applied: &Applied) -> RsfResult<()> {
    validate_mutations(&applied.mutations).map_err(|err| corrupt(err.message))?;
    if applied.mutations.is_empty()
        || applied.evidence.log_index.0 == 0
        || applied.evidence.version.0 == 0
        || applied.evidence.consistency != CommitConsistency::Strict
        || applied.evidence.operation_id != operation(&applied.mutations).operation_id()
    {
        return Err(corrupt("invalid KV batch commit evidence"));
    }
    Ok(())
}
fn receipt_result(
    applied: &Applied,
    pg: PlacementGroupId,
    mutations: &[KvMutation],
    replayed: bool,
) -> RsfResult<KvCommitResult> {
    if applied.evidence.placement_group != pg || applied.mutations != mutations {
        return Err(invalid(
            "KV request identity was reused with a different batch",
        ));
    }
    Ok(KvCommitResult {
        version: applied.evidence.version,
        evidence: applied.evidence.clone(),
        replayed,
    })
}

fn write_request(request: &ClientRequestId, out: &mut Vec<u8>) -> RsfResult<()> {
    if request.client.0.is_empty() {
        return Err(invalid("KV client identity is empty"));
    }
    write_bytes(request.client.0.as_bytes(), out)?;
    out.extend_from_slice(&request.request.0.to_le_bytes());
    Ok(())
}
fn read_request(bytes: &[u8], offset: &mut usize) -> RsfResult<ClientRequestId> {
    let client = String::from_utf8(read_bytes(bytes, offset)?)
        .map_err(|_| corrupt("invalid KV client identity UTF-8"))?;
    if client.is_empty() {
        return Err(corrupt("KV client identity is empty"));
    }
    Ok(ClientRequestId::new(client, read_u64(bytes, offset)?))
}
fn encode_intent(intent: &Intent) -> RsfResult<Vec<u8>> {
    let mut out = INTENT.to_vec();
    write_request(&intent.request, &mut out)?;
    out.extend_from_slice(&encode_kv_wal_payload(
        CommitVersion(0),
        LogIndex(0),
        &intent.mutations,
    )?);
    Ok(out)
}
fn decode_intent(pg: PlacementGroupId, bytes: &[u8]) -> RsfResult<Intent> {
    let mut offset = INTENT.len();
    let request = read_request(bytes, &mut offset)?;
    let batch = decode_kv_wal_payload(&bytes[offset..])?;
    if batch.version.0 != 0 || batch.protocol_log_index.0 != 0 || batch.mutations.is_empty() {
        return Err(corrupt("invalid KV intent payload"));
    }
    validate_mutations(&batch.mutations).map_err(|err| corrupt(err.message))?;
    Ok(Intent {
        request,
        pg,
        mutations: batch.mutations,
    })
}
fn encode_receipt(request: &ClientRequestId, applied: &Applied) -> RsfResult<Vec<u8>> {
    let mut out = RECEIPT.to_vec();
    write_request(request, &mut out)?;
    out.extend_from_slice(&applied.evidence.operation_id.0.to_le_bytes());
    out.extend_from_slice(&encode_kv_wal_payload(
        applied.evidence.version,
        applied.evidence.log_index,
        &applied.mutations,
    )?);
    Ok(out)
}
fn decode_receipt(pg: PlacementGroupId, bytes: &[u8]) -> RsfResult<(ClientRequestId, Applied)> {
    let mut offset = RECEIPT.len();
    let request = read_request(bytes, &mut offset)?;
    let operation_id = rsf_core::OperationId(read_u64(bytes, &mut offset)?);
    let batch = decode_kv_wal_payload(&bytes[offset..])?;
    Ok((
        request,
        Applied {
            evidence: CommitEvidence {
                placement_group: pg,
                version: batch.version,
                log_index: batch.protocol_log_index,
                operation_id,
                consistency: CommitConsistency::Strict,
            },
            mutations: batch.mutations,
        },
    ))
}

#[cfg(test)]
mod tests;
