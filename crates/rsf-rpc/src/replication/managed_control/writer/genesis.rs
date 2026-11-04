//! Configured Genesis planning and historical delivery identity. No transport.
use super::*;
use rsf_control_state::CatalogRoot;
use rsf_core::{Epoch, OperationId};
use sha2::{Digest, Sha256};

/// Volatile sender-side scheduling evidence from actual authenticated durable
/// replies. This is not receiver source authorization, D, Ready or admission.
/// It cannot be decoded, constructed or restored by a caller.
pub struct ObservedControlGenesisDelivery {
    key: Box<Key>,
    reporters: Vec<NodeId>,
}
struct Key {
    anchor: ControlBootstrap,
    operation: OperationId,
    prepare: [u8; 32],
    commit: [u8; 32],
    primary: NodeId,
    view: Epoch,
    lineage: rsf_vsr::VsrMembershipLineage,
    stamp: VsrStateToken,
}
impl std::fmt::Debug for ObservedControlGenesisDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Include the captured identity for diagnostics without exposing a DTO
        // constructor or a persistent completion format.
        f.debug_struct("ObservedControlGenesisDelivery")
            .field("anchor", &self.key.anchor)
            .field("operation", &self.key.operation)
            .field("prepare_digest", &self.key.prepare)
            .field("commit_digest", &self.key.commit)
            .field("historical_primary", &self.key.primary)
            .field("historical_view", &self.key.view)
            .field("lineage", &self.key.lineage)
            .field("source", &self.key.stamp)
            .field("reporters", &self.reporters)
            .finish()
    }
}
impl ObservedControlGenesisDelivery {
    pub fn pg(&self) -> PlacementGroupId {
        self.key.anchor.control_pg
    }
    pub fn genesis(&self) -> CatalogRoot {
        self.key.anchor.genesis_catalog
    }
    pub fn operation_id(&self) -> OperationId {
        self.key.operation
    }
    pub fn historical_primary(&self) -> NodeId {
        self.key.primary
    }
    pub fn historical_view(&self) -> Epoch {
        self.key.view
    }
    pub fn reporters(&self) -> &[NodeId] {
        &self.reporters
    }
}

pub(super) fn original_membership(
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
    cell: &DurableVsrCell,
) -> Result<(), String> {
    let actual = cell.membership_lineage().map_err(|e| e.message)?;
    let provisioned = genesis
        .group(anchor.control_pg)
        .ok_or("configured Genesis control group is absent")?;
    if actual.pg != anchor.control_pg
        || actual.bootstrap != provisioned.lineage.bootstrap
        || actual.plan != provisioned.lineage.plan
        || actual.voters != provisioned.voters
    {
        return Err(
            "initial Genesis delivery requires the original provisioned control membership".into(),
        );
    }
    Ok(())
}

pub(super) fn prepare(
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
    cell: &mut DurableVsrCell,
    local: NodeId,
) -> Result<VsrPrepare, String> {
    original_membership(anchor, genesis, cell)?;
    let current = cell.primary_probe().map_err(|e| e.message)?;
    let retained = cell.prepared(LogIndex(1)).cloned();
    let (operation, bytes) = match &retained {
        Some(prepare) => (prepare.operation.clone(), prepare.bytes),
        None => {
            let payload = ControlCommand::Genesis(genesis.clone())
                .encode()
                .map_err(|e| e.message)?;
            let bytes =
                u32::try_from(payload.len()).map_err(|_| "Genesis payload length overflow")?;
            (
                VsrOperation::ReplicatedCommand {
                    operation_id: progress::next_operation_id(cell)?,
                    payload,
                },
                bytes,
            )
        }
    };
    let candidate = VsrPrepare {
        pg: current.pg,
        view: current.view,
        index: LogIndex(1),
        primary: current.primary,
        operation,
        bytes,
    };
    ControlState::preflight_received_prepare(anchor, genesis, cell, &candidate)
        .map_err(|e| e.message)?;
    let actual = if let Some(retained) = retained {
        if retained == candidate {
            retained
        } else {
            cell.reprepare(local, LogIndex(1)).map_err(|e| e.message)?
        }
    } else {
        match cell
            .prepare(local, candidate.operation.clone(), candidate.bytes)
            .map_err(|e| e.message)?
        {
            VsrDecision::Prepared(prepare) => prepare,
            other => return Err(format!("control Genesis prepare rejected: {other:?}")),
        }
    };
    if actual != candidate {
        return Err("actual Genesis prepare differs from validated current envelope".into());
    }
    Ok(actual)
}

pub(super) fn observe(
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
    cell: &DurableVsrCell,
    entry: &VsrCommittedEntry,
    stamp: &VsrStateToken,
    delivered: &BTreeSet<NodeId>,
) -> Result<ObservedControlGenesisDelivery, String> {
    original_membership(anchor, genesis, cell)?;
    ControlState::preflight_committed_extension(anchor, genesis, cell, entry)
        .map_err(|e| e.message)?;
    let lineage = cell.membership_lineage().map_err(|e| e.message)?;
    if entry.prepare.index != LogIndex(1)
        || delivered.len() < cell.quorum()
        || delivered.iter().any(|node| !lineage.voters.contains(node))
        || stamp != &cell.recovery_state_token().map_err(|e| e.message)?
    {
        return Err("Genesis delivery differs from current owned quorum history".into());
    }
    let VsrOperation::ReplicatedCommand { operation_id, .. } = &entry.prepare.operation else {
        return Err("Genesis is not a replicated control command".into());
    };
    let key = Key {
        anchor: anchor.clone(),
        operation: *operation_id,
        prepare: hash(
            b"RSF Genesis delivery prepare v1",
            &entry.prepare.encode().map_err(|e| e.message)?,
        ),
        commit: hash(
            b"RSF Genesis delivery commit v1",
            &entry.commit.encode().map_err(|e| e.message)?,
        ),
        primary: entry.prepare.primary,
        view: entry.prepare.view,
        lineage,
        stamp: stamp.clone(),
    };
    // Encoding/preflight cannot renew the original source token. Recheck before
    // returning an observation, after every potentially failing proof read.
    if stamp != &cell.recovery_state_token().map_err(|e| e.message)? {
        return Err("Genesis delivery source changed while observing".into());
    }
    Ok(ObservedControlGenesisDelivery {
        key: Box::new(key),
        reporters: delivered.iter().copied().collect(),
    })
}
fn hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    digest.finalize().into()
}
