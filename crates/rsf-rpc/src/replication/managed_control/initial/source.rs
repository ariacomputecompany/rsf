//! Actual delivered Genesis -> finite historical source, without a sender worker.
use super::*;
use rsf_vsr::{VsrHistoryBatch, VsrHistoryDescriptor, VsrPrefixExportError, VsrVoterPrefixExport};

/// Frozen bounded bytes with the original opaque delivery/source identity.
/// No constructor/decoder/Clone or writable owner escape exists.
#[derive(Debug)]
pub struct ControlInitialSource {
    identity: writer::genesis::InitialGenesisSource,
    export: VsrVoterPrefixExport,
    limits: VsrPrefixLimits,
}
impl ControlInitialSource {
    pub fn placement_group(&self) -> PlacementGroupId {
        self.identity.anchor().control_pg
    }
    pub fn genesis(&self) -> rsf_control_state::CatalogRoot {
        self.identity.anchor().genesis_catalog
    }
    pub fn descriptor(&self) -> &VsrHistoryDescriptor {
        self.export.descriptor()
    }
    pub fn limits(&self) -> VsrPrefixLimits {
        self.limits
    }
}
#[derive(Debug)]
pub enum ControlInitialSourceError {
    Ownership(String),
    Prefix(VsrPrefixExportError),
    Failure(RsfError),
}
impl From<writer::genesis::InitialSourceFailure> for ControlInitialSourceError {
    fn from(error: writer::genesis::InitialSourceFailure) -> Self {
        match error {
            writer::genesis::InitialSourceFailure::Ownership(reason) => Self::Ownership(reason),
            writer::genesis::InitialSourceFailure::Physical(error) => Self::Failure(error),
        }
    }
}
impl std::fmt::Display for ControlInitialSourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ownership(reason) => f.write_str(reason),
            Self::Prefix(error) => write!(f, "{error}"),
            Self::Failure(error) => f.write_str(&error.message),
        }
    }
}
impl std::error::Error for ControlInitialSourceError {}
impl PeerReplicaServer {
    pub fn export_initial_control_source(
        &self,
        delivery: &writer::ObservedControlGenesisDelivery,
        limits: VsrPrefixLimits,
    ) -> Result<ControlInitialSource, ControlInitialSourceError> {
        let binding = self.managed_control.as_ref().ok_or_else(|| {
            ControlInitialSourceError::Ownership("control is not configured".into())
        })?;
        let cell = self.cells.get(&binding.pg()).ok_or_else(|| {
            ControlInitialSourceError::Ownership(
                "initial source requires actual control voter".into(),
            )
        })?;
        // No payload encoding occurs inside this first identity/token check.
        let identity = delivery
            .initial_source(&binding.anchor, &binding.genesis, cell, self.local)
            .map_err(ControlInitialSourceError::from)?;
        let result = cell.export_committed_prefix(LogIndex(1), limits);
        // The original delivery/source custody dominates an unsupported or
        // capacity-limited export too; an error must not skip this boundary.
        identity
            .validate(&binding.anchor, &binding.genesis, cell, self.local)
            .map_err(ControlInitialSourceError::from)?;
        let export = result.map_err(ControlInitialSourceError::Prefix)?;
        identity
            .validate_export(&binding.anchor, &binding.genesis, cell, self.local, &export)
            .map_err(ControlInitialSourceError::from)?;
        Ok(ControlInitialSource {
            identity,
            export,
            limits,
        })
    }
    pub(in crate::replication) fn validate_initial_control_source(
        &self,
        source: &ControlInitialSource,
    ) -> Result<(), ControlInitialSourceError> {
        let binding = self.managed_control.as_ref().ok_or_else(|| {
            ControlInitialSourceError::Ownership("control is not configured".into())
        })?;
        let cell = self.cells.get(&binding.pg()).ok_or_else(|| {
            ControlInitialSourceError::Ownership(
                "initial source requires actual control voter".into(),
            )
        })?;
        source
            .identity
            .validate(&binding.anchor, &binding.genesis, cell, self.local)
            .map_err(ControlInitialSourceError::from)
    }
    pub fn read_initial_control_batch(
        &self,
        source: &ControlInitialSource,
        ordinal: u64,
    ) -> Result<VsrHistoryBatch, ControlInitialSourceError> {
        let binding = self.managed_control.as_ref().ok_or_else(|| {
            ControlInitialSourceError::Ownership("control is not configured".into())
        })?;
        let cell = self.cells.get(&binding.pg()).ok_or_else(|| {
            ControlInitialSourceError::Ownership(
                "initial source requires actual control voter".into(),
            )
        })?;
        source
            .identity
            .validate(&binding.anchor, &binding.genesis, cell, self.local)
            .map_err(ControlInitialSourceError::from)?;
        let result = cell.read_committed_prefix_batch(&source.export, ordinal);
        // Original source custody dominates a failed read as well as success.
        source
            .identity
            .validate(&binding.anchor, &binding.genesis, cell, self.local)
            .map_err(ControlInitialSourceError::from)?;
        result.map_err(ControlInitialSourceError::Failure)
    }
}
