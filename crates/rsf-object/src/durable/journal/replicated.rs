//! Complete object commands preserve the original intent, immutable bytes and
//! pin evidence. Application receipts are derived atomically from those commands.
use super::*;
use rsf_protocol::{
    ApplicationPayload, ApplicationSurface, ConsistencyMode, FabricCommit, ReplicatedFabricCommand,
};

const MAGIC: &[u8] = b"RSFOC1";

impl DurableObjectStore {
    pub fn open_replicated(root: impl AsRef<Path>, namespace: &str) -> RsfResult<Self> {
        let mut store = Self::open(root)?;
        if let Some(existing) = &store.journal.namespace {
            return if existing == namespace {
                Ok(store)
            } else {
                Err(invalid("object store belongs to a different namespace"))
            };
        }
        store.validate_namespace_binding(namespace)?;
        // A legacy staged payload may belong to an interrupted metadata-only write.
        let blobs = store.blobs.verify_all()?;
        if blobs.chunks != 0 || blobs.quarantined != 0 {
            return Err(invalid(
                "nonempty legacy object payloads require explicit namespace migration",
            ));
        }
        store
            .journal
            .append(&format!("namespace\t{}", hex_bytes(namespace.as_bytes())))?;
        store.journal.namespace = Some(namespace.into());
        Ok(store)
    }

    pub(super) fn validate_namespace_binding(&self, namespace: &str) -> RsfResult<()> {
        if namespace.is_empty() || namespace.len() > 4096 || namespace.chars().any(char::is_control)
        {
            return Err(invalid("invalid object replication namespace"));
        }
        if self.journal.namespace.is_some()
            || !self.buckets.is_empty()
            || !self.versions.is_empty()
            || self.journal.pending.is_some()
            || !self.journal.receipts.is_empty()
        {
            return Err(invalid(
                "nonempty or already bound object store requires explicit namespace migration",
            ));
        }
        Ok(())
    }

    pub(super) fn preflight_replicated(
        &self,
        router: &impl FabricRouterPort,
        intent: &Intent,
        bytes: &[u8],
    ) -> RsfResult<()> {
        if router.request_certificate(&intent.request).is_some() {
            return Err(corrupt(
                "replicated object request needs its original command or pending intent",
            ));
        }
        if let Action::Put {
            blob_pg, content, ..
        } = &intent.action
        {
            self.blobs.validate_pin_request(router, *blob_pg, content)?;
        }
        // Pin fields have fixed widths, so the preflight is the exact final size.
        let pin = match &intent.action {
            Action::Put {
                blob_pg, content, ..
            } => Some((
                CommitEvidence {
                    placement_group: *blob_pg,
                    log_index: LogIndex(1),
                    version: CommitVersion(1),
                    consistency: CommitConsistency::AvailabilityFirst,
                    operation_id: FabricOperation::BlobPin {
                        digest: *content.as_bytes(),
                    }
                    .operation_id(),
                },
                1,
            )),
            Action::Bucket { .. } => None,
        };
        let application = self.application(intent, pin.as_ref(), bytes)?;
        let (pg, operation) = intent.operation();
        router.validate_application_request(
            pg,
            &intent.request,
            ConsistencyMode::Strict,
            &operation,
            &application,
        )
    }

    fn application(
        &self,
        intent: &Intent,
        pin: Option<&(CommitEvidence, u32)>,
        bytes: &[u8],
    ) -> RsfResult<ApplicationPayload> {
        let namespace = self
            .journal
            .namespace
            .clone()
            .ok_or_else(|| invalid("object replication requires a bound namespace"))?;
        let text = encode_intent(intent);
        let length = u32::try_from(text.len())
            .map_err(|_| invalid("object command header exceeds size limit"))?;
        let mut body = MAGIC.to_vec();
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(text.as_bytes());
        if let Some((pin, pins)) = pin {
            for n in [
                pin.placement_group.0,
                pin.log_index.0,
                pin.version.0,
                pin.operation_id.0,
            ] {
                body.extend_from_slice(&n.to_le_bytes());
            }
            body.extend_from_slice(&pins.to_le_bytes());
        }
        body.extend_from_slice(bytes);
        Ok(ApplicationPayload {
            namespace,
            surface: ApplicationSurface::Object,
            bytes: body,
        })
    }

    pub(super) fn reconcile_replicated(
        &mut self,
        router: &mut impl FabricRouterPort,
        intent: Intent,
    ) -> RsfResult<()> {
        let (payload, pin) = match &intent.action {
            Action::Bucket { .. } => (Vec::new(), None),
            Action::Put {
                blob_pg,
                content,
                bytes,
                ..
            } => {
                let payload = self.blobs.read_verified(content)?;
                if payload.len() as u64 != *bytes {
                    return Err(corrupt("object intent payload length mismatch"));
                }
                // This internal pin record alone does not acknowledge an object.
                // The complete object command below durably replicates the bytes.
                let pin = self
                    .blobs
                    .put_dependency(router, *blob_pg, payload.clone())?;
                (payload, Some((pin.pin_evidence, pin.pins)))
            }
        };
        let application = self.application(&intent, pin.as_ref(), &payload)?;
        let (pg, operation) = intent.operation();
        let result = router.commit_application_request(
            pg,
            intent.request.clone(),
            ConsistencyMode::Strict,
            operation.clone(),
            application.clone(),
        )?;
        validate_certificate(&result.certificate, pg, &operation)?;
        self.apply_application_command(&ReplicatedFabricCommand {
            request: intent.request,
            application,
            commit: FabricCommit {
                pg,
                index: result.certificate.index,
                version: result.certificate.version,
                mode: result.certificate.mode,
                operation_id: result.certificate.operation_id,
                operation,
            },
        })
    }

    pub(super) fn confirm_replicated_retry(
        &self,
        router: &mut impl FabricRouterPort,
        request: &ClientRequestId,
    ) -> RsfResult<()> {
        if self.journal.namespace.is_none() {
            return Ok(());
        }
        let command = self
            .journal
            .commands
            .get(request)
            .ok_or_else(|| corrupt("object receipt has no complete command"))?;
        if router.latest_version() < self.journal.highest_command
            || router.request_certificate(request) != Some(command.certificate())
        {
            return Err(corrupt(
                "object retry requires matching recovered protocol history",
            ));
        }
        let retry = router.commit_application_request(
            command.commit.pg,
            request.clone(),
            command.commit.mode,
            command.commit.operation.clone(),
            command.application.clone(),
        )?;
        if retry.certificate != command.certificate() {
            return Err(corrupt("object retry returned different evidence"));
        }
        Ok(())
    }

    /// The caller must authenticate the VSR entry and validate its quorum first.
    /// Store exact bytes before the atomic command/receipt record, then publish heads.
    pub fn apply_application_command(
        &mut self,
        command: &ReplicatedFabricCommand,
    ) -> RsfResult<()> {
        self.journal.ensure_healthy()?;
        let (intent, receipt, payload) = self.decode_application_command(command)?;
        if let Some(existing) = self.journal.commands.get(&intent.request) {
            if existing != command {
                return Err(corrupt(
                    "object replay differs from original complete command",
                ));
            }
            if !payload.is_empty() {
                self.blobs.stage(payload)?;
            }
            return Ok(());
        }
        self.validate_application_receipt(command, &receipt)?;
        if !payload.is_empty() {
            self.blobs.stage(payload)?;
        }
        self.journal
            .append(&format!("application\t{}", hex_bytes(&command.encode()?)))?;
        self.install_application_command(command.clone())
    }

    pub(super) fn install_application_command(
        &mut self,
        command: ReplicatedFabricCommand,
    ) -> RsfResult<()> {
        let (_, receipt, _) = self.decode_application_command(&command)?;
        self.validate_application_receipt(&command, &receipt)?;
        self.install_receipt(command.request.clone(), receipt)?;
        self.journal.highest_command = command.commit.version;
        self.journal
            .commands
            .insert(command.request.clone(), command);
        Ok(())
    }

    fn validate_application_receipt(
        &self,
        command: &ReplicatedFabricCommand,
        receipt: &Receipt,
    ) -> RsfResult<()> {
        if command.commit.version <= self.journal.highest_command
            || self.journal.commands.contains_key(&command.request)
        {
            return Err(corrupt(
                "object application commands repeat or reorder history",
            ));
        }
        self.validate_receipt(&command.request, receipt)
    }

    fn decode_application_command(
        &self,
        command: &ReplicatedFabricCommand,
    ) -> RsfResult<(Intent, Receipt, Vec<u8>)> {
        command.encode()?;
        if self.journal.namespace.as_deref() != Some(command.application.namespace.as_str())
            || command.application.surface != ApplicationSurface::Object
        {
            return Err(invalid(
                "object command targets a different namespace or surface",
            ));
        }
        let raw = command
            .application
            .bytes
            .strip_prefix(MAGIC)
            .ok_or_else(|| corrupt("unknown object command format"))?;
        let mut input = Input(raw);
        let length = u32::from_le_bytes(input.take(4)?.try_into().unwrap()) as usize;
        let text = std::str::from_utf8(input.take(length)?)
            .map_err(|_| corrupt("object intent is not UTF-8"))?;
        let intent = decode_intent(text)?;
        validate_action(&intent.action)?;
        let (pg, operation) = intent.operation();
        if intent.request != command.request || command.commit.operation != operation {
            return Err(corrupt(
                "object command differs from its request or operation",
            ));
        }
        validate_certificate(&command.certificate(), pg, &operation)?;
        let receipt = match &intent.action {
            Action::Bucket { bucket, .. } => {
                if !input.0.is_empty() {
                    return Err(corrupt("trailing bucket command bytes"));
                }
                let bucket = match self.buckets.get(bucket) {
                    Some(existing) if existing.evidence.placement_group == pg => existing.clone(),
                    Some(_) => {
                        return Err(corrupt("bucket belongs to a different placement group"));
                    }
                    None => DurableObjectBucket {
                        name: bucket.clone(),
                        version: command.commit.version,
                        evidence: commit_evidence(command.certificate()),
                    },
                };
                Receipt::Bucket(bucket)
            }
            Action::Put {
                blob_pg,
                bucket,
                key,
                content,
                bytes,
                deduped,
                ..
            } => {
                let evidence = CommitEvidence {
                    placement_group: PlacementGroupId(input.u64()?),
                    log_index: LogIndex(input.u64()?),
                    version: CommitVersion(input.u64()?),
                    operation_id: OperationId(input.u64()?),
                    consistency: CommitConsistency::AvailabilityFirst,
                };
                let pins = u32::from_le_bytes(input.take(4)?.try_into().unwrap());
                if evidence.placement_group != *blob_pg
                    || input.0.len() as u64 != *bytes
                    || ContentId::for_bytes(input.0) != *content
                {
                    return Err(corrupt(
                        "object command pin or payload differs from its intent",
                    ));
                }
                Receipt::Put(DurableObjectPut {
                    head: DurableObjectHead {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version: command.commit.version,
                        evidence: commit_evidence(command.certificate()),
                        content: content.clone(),
                        bytes: *bytes,
                        manifest_digest: manifest_digest(bucket, key, content, *bytes),
                    },
                    pin_version: evidence.version,
                    pin_evidence: evidence,
                    pins,
                    deduped: *deduped,
                })
            }
        };
        Ok((intent, receipt, input.0.to_vec()))
    }
}
impl Intent {
    fn operation(&self) -> (PlacementGroupId, FabricOperation) {
        match &self.action {
            Action::Bucket { pg, bucket } => (
                *pg,
                FabricOperation::ObjectBucket {
                    bucket: bucket.clone(),
                },
            ),
            Action::Put {
                pg, bucket, key, ..
            } => (
                *pg,
                FabricOperation::ObjectManifest {
                    bucket: bucket.clone(),
                    key: key.clone(),
                },
            ),
        }
    }
}
struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, length: usize) -> RsfResult<&'a [u8]> {
        if length > self.0.len() {
            return Err(corrupt("truncated object application command"));
        }
        let (value, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(value)
    }
    fn u64(&mut self) -> RsfResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests;
