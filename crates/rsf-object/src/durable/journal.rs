use super::*;
use rsf_core::{Epoch, storage::WalRecordKind};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const LOCAL_PG: PlacementGroupId = PlacementGroupId(0);
#[derive(Clone, Debug, Eq, PartialEq)]
struct Intent {
    request: ClientRequestId,
    action: Action,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Bucket {
        pg: PlacementGroupId,
        bucket: String,
    },
    Put {
        pg: PlacementGroupId,
        blob_pg: PlacementGroupId,
        bucket: String,
        key: String,
        content: ContentId,
        bytes: u64,
        deduped: bool,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Receipt {
    Bucket(DurableObjectBucket),
    Put(DurableObjectPut),
}
#[derive(Debug)]
pub(super) struct Journal {
    store: SegmentStore,
    scope: String,
    next: u64,
    pending: Option<Intent>,
    receipts: BTreeMap<ClientRequestId, Receipt>,
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
        Ok(Self {
            store: SegmentStore::open(root.join("journal"))?,
            scope: String::new(),
            next: 1,
            pending: None,
            receipts: BTreeMap::new(),
            failure: None,
        })
    }
    pub(super) fn scope(&self) -> &str {
        &self.scope
    }
    pub(super) fn ensure_healthy(&self) -> RsfResult<()> {
        if let Some(reason) = &self.failure {
            Err(corrupt(format!(
                "object persistence failed; reopen before further operations: {reason}"
            )))
        } else {
            Ok(())
        }
    }
    fn append(&mut self, payload: &str) -> RsfResult<()> {
        self.ensure_healthy()?;
        let result: RsfResult<()> = (|| {
            let next = self
                .next
                .checked_add(1)
                .ok_or_else(|| corrupt("object journal index exhausted"))?;
            let previous_hash = self
                .store
                .recover_current()?
                .recovery
                .pg_tail(LOCAL_PG)
                .map_or([0; 32], |tail| tail.record_hash);
            self.store.append_wal(WalAppend {
                pg: LOCAL_PG,
                epoch: Epoch(1),
                index: LogIndex(self.next),
                kind: WalRecordKind::ObjectManifest,
                previous_hash,
                payload: payload.as_bytes(),
            })?;
            self.next = next;
            Ok(())
        })();
        if let Err(err) = &result {
            self.failure = Some(err.message.clone());
        }
        result
    }
}

impl DurableObjectStore {
    pub(super) fn recover_journal(&mut self) -> RsfResult<()> {
        let records = self
            .journal
            .store
            .scan(SegmentScanFilter { pg: None })?
            .wal_records;
        if records.is_empty() {
            self.load_index()?;
            let mut nonce = [0; 32];
            getrandom::getrandom(&mut nonce)
                .map_err(|err| corrupt(format!("cannot create object scope: {err}")))?;
            self.journal.scope = hex_bytes(&nonce);
            self.journal.append(&format!(
                "RSFOJ1\t{}\n{}",
                self.journal.scope,
                encode_index(&self.buckets, &self.versions)
            ))?;
            return Ok(());
        }
        for (position, record) in records.into_iter().enumerate() {
            if record.kind != WalRecordKind::ObjectManifest
                || record.pg != LOCAL_PG
                || record.index.0 != self.journal.next
            {
                return Err(corrupt(
                    "unexpected object journal record or missing prefix",
                ));
            }
            let text = std::str::from_utf8(&record.payload)
                .map_err(|_| corrupt("object journal is not UTF-8"))?;
            if position == 0 {
                let (header, index) = text
                    .split_once('\n')
                    .ok_or_else(|| corrupt("object journal bootstrap is truncated"))?;
                let scope = header
                    .strip_prefix("RSFOJ1\t")
                    .ok_or_else(|| corrupt("object journal has no bootstrap"))?;
                hex_decode_32(scope)?;
                self.journal.scope = scope.into();
                decode_index(
                    index,
                    &mut self.buckets,
                    &mut self.heads,
                    &mut self.versions,
                )?;
            } else if text.starts_with("intent\t") {
                let intent = decode_intent(text)?;
                if self.journal.pending.is_some()
                    || self.journal.receipts.contains_key(&intent.request)
                {
                    return Err(corrupt(
                        "object intent overlaps an unresolved or completed request",
                    ));
                }
                validate_action(&intent.action)?;
                self.journal.pending = Some(intent);
            } else {
                let (request, receipt) = decode_receipt(text)?;
                self.install_receipt(request, receipt)?;
            }
            self.journal.next = record
                .index
                .0
                .checked_add(1)
                .ok_or_else(|| corrupt("object journal index exhausted"))?;
        }
        Ok(())
    }

    /// Resolve the prior uncertain mutation before serving object reads.
    pub fn reconcile(&mut self, router: &mut impl FabricRouterPort) -> RsfResult<usize> {
        self.journal.ensure_healthy()?;
        let applied = self
            .buckets
            .values()
            .map(|bucket| bucket.version)
            .chain(self.heads.values().map(|head| head.version))
            .max()
            .unwrap_or(CommitVersion::GENESIS);
        if router.latest_version() < applied {
            return Err(corrupt(
                "object state is ahead of the recovered protocol; restore the matching protocol log",
            ));
        }
        let Some(intent) = self.journal.pending.clone() else {
            return Ok(0);
        };
        let receipt = match &intent.action {
            Action::Bucket { pg, bucket } => {
                let operation = FabricOperation::ObjectBucket {
                    bucket: bucket.clone(),
                };
                let commit =
                    router.commit_request(*pg, intent.request.clone(), operation.clone())?;
                validate_certificate(&commit.certificate, *pg, &operation)?;
                Receipt::Bucket(DurableObjectBucket {
                    name: bucket.clone(),
                    version: commit.certificate.version,
                    evidence: commit_evidence(commit.certificate),
                })
            }
            Action::Put {
                pg,
                blob_pg,
                bucket,
                key,
                content,
                bytes,
                deduped,
            } => {
                let payload = self.blobs.read_verified(content)?;
                if payload.len() as u64 != *bytes {
                    return Err(corrupt(
                        "object intent payload length differs from verified content",
                    ));
                }
                let blob = self.blobs.put(router, *blob_pg, payload)?;
                let operation = FabricOperation::ObjectManifest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                let commit =
                    router.commit_request(*pg, intent.request.clone(), operation.clone())?;
                validate_certificate(&commit.certificate, *pg, &operation)?;
                Receipt::Put(DurableObjectPut {
                    head: DurableObjectHead {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version: commit.certificate.version,
                        evidence: commit_evidence(commit.certificate),
                        content: content.clone(),
                        bytes: *bytes,
                        manifest_digest: manifest_digest(bucket, key, content, *bytes),
                    },
                    pin_version: blob.pin_version,
                    pin_evidence: blob.pin_evidence,
                    deduped: *deduped,
                    pins: blob.pins,
                })
            }
        };
        self.persist_receipt(intent.request, receipt)?;
        Ok(1)
    }

    pub fn create_bucket_request(
        &mut self,
        router: &mut impl FabricRouterPort,
        pg: PlacementGroupId,
        request: ClientRequestId,
        bucket: impl Into<String>,
    ) -> RsfResult<DurableObjectBucket> {
        let bucket = bucket.into();
        let action = Action::Bucket {
            pg,
            bucket: bucket.clone(),
        };
        validate_action(&action)?;
        self.check_retry(&request, &action)?;
        self.reconcile(router)?;
        if let Some(receipt) = self.journal.receipts.get(&request) {
            return match receipt {
                Receipt::Bucket(bucket) => Ok(bucket.clone()),
                _ => Err(invalid("object request was reused for another action")),
            };
        }
        if let Some(certificate) = router.request_certificate(&request) {
            validate_certificate(
                &certificate,
                pg,
                &FabricOperation::ObjectBucket {
                    bucket: bucket.clone(),
                },
            )?;
            if !self
                .buckets
                .get(&bucket)
                .is_some_and(|old| old.evidence == commit_evidence(certificate))
            {
                return Err(corrupt(
                    "object request has a certificate but no original intent or bucket",
                ));
            }
        }
        if let Some(existing) = self.buckets.get(&bucket) {
            if existing.evidence.placement_group != pg {
                return Err(invalid("bucket already belongs to another placement group"));
            }
            let receipt = Receipt::Bucket(existing.clone());
            self.persist_receipt(request, receipt.clone())?;
            let Receipt::Bucket(bucket) = receipt else {
                unreachable!()
            };
            return Ok(bucket);
        }
        self.start_intent(Intent {
            request: request.clone(),
            action,
        })?;
        self.reconcile(router)?;
        match &self.journal.receipts[&request] {
            Receipt::Bucket(bucket) => Ok(bucket.clone()),
            _ => unreachable!(),
        }
    }

    pub(super) fn put_durable(
        &mut self,
        router: &mut impl FabricRouterPort,
        blob_pg: PlacementGroupId,
        pg: PlacementGroupId,
        request: ClientRequestId,
        bucket: String,
        key: String,
        payload: Vec<u8>,
    ) -> RsfResult<DurableObjectPut> {
        let content = ContentId::for_bytes(&payload);
        let mut action = Action::Put {
            pg,
            blob_pg,
            bucket: bucket.clone(),
            key: key.clone(),
            content: content.clone(),
            bytes: payload.len() as u64,
            deduped: false,
        };
        validate_action(&action)?;
        self.check_retry(&request, &action)?;
        self.reconcile(router)?;
        self.require_bucket(&bucket)?;
        if let Some(receipt) = self.journal.receipts.get(&request) {
            return match receipt {
                Receipt::Put(put) => Ok(put.clone()),
                _ => Err(invalid("object request was reused for another action")),
            };
        }
        if let Some(certificate) = router.request_certificate(&request) {
            let operation = FabricOperation::ObjectManifest {
                bucket: bucket.clone(),
                key: key.clone(),
            };
            validate_certificate(&certificate, pg, &operation)?;
            let head=self.versions(&bucket,&key).and_then(|versions|versions.iter().find(|head|head.evidence==commit_evidence(certificate.clone()))).cloned()
                .ok_or_else(||corrupt("legacy object request has no original durable head; recovery requires its original intent"))?;
            if head.content != content || head.bytes != payload.len() as u64 {
                return Err(invalid(
                    "object retry differs from its original durable payload",
                ));
            }
            let stored = self.blobs.read_verified(&content)?;
            if stored != payload {
                return Err(corrupt("legacy object payload mismatch"));
            }
            let pin = self.blobs.put(router, blob_pg, payload)?;
            let put = DurableObjectPut {
                head,
                pin_version: pin.pin_version,
                pin_evidence: pin.pin_evidence,
                deduped: true,
                pins: pin.pins,
            };
            self.persist_receipt(request, Receipt::Put(put.clone()))?;
            return Ok(put);
        }
        let staged = self.blobs.stage(payload)?;
        if let Action::Put { deduped, .. } = &mut action {
            *deduped = staged.deduped;
        }
        self.start_intent(Intent {
            request: request.clone(),
            action,
        })?;
        self.reconcile(router)?;
        match &self.journal.receipts[&request] {
            Receipt::Put(put) => Ok(put.clone()),
            _ => unreachable!(),
        }
    }

    /// Materialize a bucket after the caller validates the authoritative quorum
    /// certificate and its complete replicated command.
    pub fn apply_committed_bucket(
        &mut self,
        request: ClientRequestId,
        bucket: DurableObjectBucket,
    ) -> RsfResult<DurableObjectBucket> {
        let receipt = Receipt::Bucket(bucket.clone());
        self.apply_receipt(request, receipt)?;
        Ok(bucket)
    }

    /// Materialize an authoritative object receipt only after its exact immutable
    /// bytes are durable locally. The caller must authenticate the command and
    /// validate its quorum certificate before invoking this method.
    pub fn apply_committed_put(
        &mut self,
        request: ClientRequestId,
        put: DurableObjectPut,
        payload: Vec<u8>,
    ) -> RsfResult<DurableObjectPut> {
        self.journal.ensure_healthy()?;
        if ContentId::for_bytes(&payload) != put.head.content
            || payload.len() as u64 != put.head.bytes
        {
            return Err(corrupt(
                "replicated object bytes do not match committed head",
            ));
        }
        let receipt = Receipt::Put(put.clone());
        self.check_retry(&request, &receipt.action())?;
        if let Some(existing) = self.journal.receipts.get(&request) {
            if existing != &receipt {
                return Err(corrupt("replicated object receipt differs from original"));
            }
            self.blobs.stage(payload)?;
            return Ok(put);
        }
        self.validate_receipt(&request, &receipt)?;
        self.blobs.stage(payload)?;
        self.persist_receipt(request, receipt)?;
        Ok(put)
    }

    fn apply_receipt(&mut self, request: ClientRequestId, receipt: Receipt) -> RsfResult<()> {
        self.check_retry(&request, &receipt.action())?;
        if let Some(existing) = self.journal.receipts.get(&request) {
            return if existing == &receipt {
                Ok(())
            } else {
                Err(corrupt("replicated object receipt differs from original"))
            };
        }
        self.persist_receipt(request, receipt)
    }

    fn check_retry(&self, request: &ClientRequestId, action: &Action) -> RsfResult<()> {
        self.journal.ensure_healthy()?;
        if request.client.0.is_empty() {
            return Err(invalid("object request client must not be empty"));
        }
        if let Some(pending) = &self.journal.pending {
            if &pending.request == request && !same_action(&pending.action, action) {
                return Err(invalid("object retry differs from its durable intent"));
            }
        }
        if let Some(receipt) = self.journal.receipts.get(request) {
            if !same_action(&receipt.action(), action) {
                return Err(invalid(
                    "object request identity was reused with a different command",
                ));
            }
        }
        Ok(())
    }
    fn start_intent(&mut self, intent: Intent) -> RsfResult<()> {
        if self.journal.pending.is_some() {
            return Err(invalid("resolve the prior object intent first"));
        }
        self.journal.append(&encode_intent(&intent))?;
        self.journal.pending = Some(intent);
        Ok(())
    }
    fn persist_receipt(&mut self, request: ClientRequestId, receipt: Receipt) -> RsfResult<()> {
        self.validate_receipt(&request, &receipt)?;
        self.journal.append(&encode_receipt(&request, &receipt))?;
        self.install_receipt(request, receipt)
    }
    fn validate_receipt(&self, request: &ClientRequestId, receipt: &Receipt) -> RsfResult<()> {
        if self.journal.receipts.contains_key(request) {
            return Err(corrupt("duplicate object request receipt"));
        }
        validate_action(&receipt.action())?;
        if let Some(pending) = &self.journal.pending {
            if &pending.request != request || !same_action(&pending.action, &receipt.action()) {
                return Err(corrupt("object receipt does not match its pending intent"));
            }
        }
        match receipt {
            Receipt::Bucket(bucket) => {
                validate_evidence(
                    &bucket.evidence,
                    &FabricOperation::ObjectBucket {
                        bucket: bucket.name.clone(),
                    },
                    CommitConsistency::Strict,
                )?;
                if bucket.version != bucket.evidence.version {
                    return Err(corrupt("bucket version differs from evidence"));
                }
                if self
                    .buckets
                    .get(&bucket.name)
                    .is_some_and(|old| old != bucket)
                {
                    return Err(corrupt("bucket receipt conflicts with durable bucket"));
                }
            }
            Receipt::Put(put) => {
                let head = &put.head;
                self.require_bucket(&head.bucket)?;
                validate_evidence(
                    &head.evidence,
                    &FabricOperation::ObjectManifest {
                        bucket: head.bucket.clone(),
                        key: head.key.clone(),
                    },
                    CommitConsistency::Strict,
                )?;
                validate_evidence(
                    &put.pin_evidence,
                    &FabricOperation::BlobPin {
                        digest: *head.content.as_bytes(),
                    },
                    CommitConsistency::AvailabilityFirst,
                )?;
                if head.version <= self.buckets[&head.bucket].version
                    || put.pin_version >= head.version
                    || head.version != head.evidence.version
                    || put.pin_version != put.pin_evidence.version
                    || put.pins == 0
                    || head.manifest_digest
                        != manifest_digest(&head.bucket, &head.key, &head.content, head.bytes)
                {
                    return Err(corrupt(
                        "object receipt metadata does not match committed payload",
                    ));
                }
                if let Some(history) = self.versions(&head.bucket, &head.key) {
                    if let Some(old) = history.iter().find(|old| old.version == head.version) {
                        if old != head {
                            return Err(corrupt("object version conflicts with durable history"));
                        }
                    } else if history
                        .last()
                        .is_some_and(|old| old.version >= head.version)
                    {
                        return Err(corrupt(
                            "object receipt would replace newer durable history",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
    fn install_receipt(&mut self, request: ClientRequestId, receipt: Receipt) -> RsfResult<()> {
        self.validate_receipt(&request, &receipt)?;
        match &receipt {
            Receipt::Bucket(bucket) => {
                self.buckets.insert(bucket.name.clone(), bucket.clone());
            }
            Receipt::Put(put) => {
                if !self
                    .versions(&put.head.bucket, &put.head.key)
                    .is_some_and(|versions| {
                        versions.iter().any(|head| head.version == put.head.version)
                    })
                {
                    self.install_head(put.head.clone());
                }
            }
        }
        self.journal.pending = None;
        self.journal.receipts.insert(request, receipt);
        Ok(())
    }
}

impl Receipt {
    fn action(&self) -> Action {
        match self {
            Self::Bucket(bucket) => Action::Bucket {
                pg: bucket.evidence.placement_group,
                bucket: bucket.name.clone(),
            },
            Self::Put(put) => Action::Put {
                pg: put.head.evidence.placement_group,
                blob_pg: put.pin_evidence.placement_group,
                bucket: put.head.bucket.clone(),
                key: put.head.key.clone(),
                content: put.head.content.clone(),
                bytes: put.head.bytes,
                deduped: put.deduped,
            },
        }
    }
}
fn same_action(left: &Action, right: &Action) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    for action in [&mut left, &mut right] {
        if let Action::Put { deduped, .. } = action {
            *deduped = false;
        }
    }
    left == right
}
fn validate_action(action: &Action) -> RsfResult<()> {
    match action {
        Action::Bucket { bucket, .. } => validate_bucket(bucket),
        Action::Put {
            bucket, key, bytes, ..
        } => {
            validate_bucket(bucket)?;
            validate_key(key)?;
            if *bytes == 0 {
                return Err(invalid("object put payload must not be empty"));
            }
            Ok(())
        }
    }
}
pub(super) fn validate_evidence(
    evidence: &CommitEvidence,
    operation: &FabricOperation,
    consistency: CommitConsistency,
) -> RsfResult<()> {
    if evidence.log_index.0 == 0
        || evidence.version.0 == 0
        || evidence.operation_id != operation.operation_id()
        || evidence.consistency != consistency
    {
        return Err(corrupt(
            "object commit evidence does not match its operation",
        ));
    }
    Ok(())
}
fn validate_certificate(
    certificate: &CommitCertificate,
    pg: PlacementGroupId,
    operation: &FabricOperation,
) -> RsfResult<()> {
    if certificate.pg != pg {
        return Err(corrupt(
            "object certificate belongs to a different placement group",
        ));
    }
    validate_evidence(
        &commit_evidence(certificate.clone()),
        operation,
        CommitConsistency::Strict,
    )
}

mod codec;
use codec::*;
#[cfg(test)]
mod tests;
