//! Ownership transfer from a peer's installed cells into client coordination.
//! No log is copied or bootstrapped from an operator's stale primary setting.
use super::*;

impl<T: VsrTransport> VsrBackedFabricRouter<T> {
    /// Recover coordination around the exact installed cells owned by a peer.
    ///
    /// The caller transfers exclusive cell ownership. Every mapped group must
    /// have a published installation naming `local` as primary. This recovers
    /// inventory only: reconcile pending commands, recover application stores,
    /// and confirm the current quorum before opening client service.
    ///
    /// Complete replicated application commands are required. Metadata-only
    /// legacy logs need explicit migration; their values/receipts cannot be
    /// reconstructed from operation identities. On each restart supply these
    /// same authoritative cell roots rather than the default local bootstrap.
    pub fn open_with_installed_cells(
        path: impl AsRef<Path>,
        coordination_epoch: Epoch,
        local: NodeId,
        cells: BTreeMap<PlacementGroupId, DurableVsrCell>,
        transport: T,
    ) -> RsfResult<Self> {
        if cells.is_empty() {
            return Err(handoff_invalid("installed-state recovery requires cells"));
        }
        for (pg, cell) in &cells {
            let probe = cell.primary_probe()?;
            if !cell.has_installed_view()
                || probe.pg != *pg
                || probe.primary != local
                || !cell.contains_replica(local)
            {
                return Err(handoff_invalid(
                    "installed cell identity or primary differs from local ownership",
                ));
            }
            for index in 1..cell.next_index().0 {
                let prepare = cell
                    .prepared(LogIndex(index))
                    .ok_or_else(|| handoff_corrupt("installed history has an operation gap"))?;
                if !matches!(prepare.operation, VsrOperation::ReplicatedCommand { .. }) {
                    return Err(handoff_invalid(
                        "metadata-only installed history requires complete-command migration",
                    ));
                }
            }
        }
        let path = path.as_ref().to_path_buf();
        let groups = cells.keys().copied().collect::<Vec<_>>();
        // Reject an incomplete/different cell inventory before persisting any
        // metadata or opening writers for a new group.
        if let Some(placement) = recover_placement_map(&path)?
            && placement
                .groups
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                != groups.iter().copied().collect()
        {
            return Err(handoff_invalid(
                "installed cells differ from the durable placement map",
            ));
        }
        let placement = recover_or_initialize_placement_map(&path, groups, coordination_epoch)?;
        let policy = recover_or_initialize_protocol_policy(&path, coordination_epoch)?;
        policy.validate_placement_map(&placement)?;
        let replicas = cells.values().next().expect("nonempty cells").replicas();
        let router =
            DurableFabricRouter::open(&path, placement.groups.iter().copied(), coordination_epoch)?;
        let mut recovered = Self {
            path,
            epoch: coordination_epoch,
            router,
            cells,
            placement,
            policy,
            primary: local,
            replicas,
            transport,
            application_commands: BTreeMap::new(),
            protocol_commands: BTreeMap::new(),
            application_failure: None,
            provision_local_cells: false,
        };
        recovered.recover_application_commands()?;
        recovered.validate_installed_receipts()?;
        Ok(recovered)
    }

    /// Return the same live cells to the peer service, retaining their writer
    /// locks throughout the handoff. Derived protocol WAL writers are dropped.
    pub fn into_cells(self) -> BTreeMap<PlacementGroupId, DurableVsrCell> {
        self.cells
    }

    fn validate_installed_receipts(&self) -> RsfResult<()> {
        // A stale local receipt must not allocate versions or make an unrelated
        // selected command appear committed. Check every group, including any
        // protocol records outside the current placement inventory.
        for wal in SegmentStore::recover(&self.path)?.recovery.wal_records {
            if wal.kind != WalRecordKind::Commit
                || !ProtocolCommitRecord::is_protocol_payload(&wal.payload)
            {
                continue;
            }
            let receipt = ProtocolCommitRecord::decode(&wal.payload)?;
            let cell = self.cells.get(&wal.pg).ok_or_else(|| {
                handoff_corrupt("protocol receipt names a missing installed group")
            })?;
            let (expected, request, certificate) =
                if let Some(command) = self.protocol_commands.get(&receipt.certificate.version) {
                    (
                        command.vsr_operation()?,
                        command.request.clone(),
                        CommitCertificate::from(&command.commit),
                    )
                } else {
                    let request = receipt.client_request.as_ref().ok_or_else(|| {
                        handoff_corrupt("receipt has no installed complete command")
                    })?;
                    let command = self.application_commands.get(request).ok_or_else(|| {
                        handoff_corrupt("receipt has no installed application command")
                    })?;
                    (
                        command.vsr_operation()?,
                        Some(command.request.clone()),
                        command.certificate(),
                    )
                };
            if wal.pg != receipt.certificate.pg
                || request != receipt.client_request
                || certificate != receipt.certificate
                || cell.committed_operation(receipt.certificate.index) != Some(&expected)
            {
                return Err(handoff_corrupt(
                    "protocol receipt disagrees with installed committed history",
                ));
            }
        }
        Ok(())
    }
}
fn handoff_invalid(message: &str) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}
fn handoff_corrupt(message: &str) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests;
