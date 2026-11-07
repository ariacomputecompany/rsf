//! Pure, finite canonical bootstrap validation before any filesystem effect.
use super::*;

/// Validated original bootstrap metadata. This is a specification, never a
/// reservation, initialized voter, publication permission or client admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrBootstrapSpec {
    pg: PlacementGroupId,
    view: Epoch,
    primary: NodeId,
    voters: Vec<NodeId>,
    pub(super) bytes: Vec<u8>,
    digest: [u8; 32],
}
impl VsrBootstrapSpec {
    pub fn new(
        pg: PlacementGroupId,
        view: Epoch,
        primary: NodeId,
        voters: impl IntoIterator<Item = NodeId>,
    ) -> RsfResult<Self> {
        if view.0 == 0 {
            return Err(invalid_input(
                "provisional voter requires a positive bootstrap view",
            ));
        }
        // Bound iteration/allocation even for an adversarial iterator. The
        // canonical bootstrap is 37 fixed bytes plus eight per unique voter.
        let maximum = (crate::transfer::MAX_HISTORY_RECORD_BYTES - 37) / 8;
        let mut unique = BTreeSet::new();
        for (index, voter) in voters.into_iter().enumerate() {
            if index >= maximum {
                return Err(invalid_input(
                    "bootstrap voter set exceeds history codec bound",
                ));
            }
            if !unique.insert(voter) {
                return Err(invalid_input("bootstrap voter set contains a duplicate"));
            }
        }
        let cell = VsrCell::new(pg, view, primary, unique).map_err(invalid_input)?;
        let bytes = encode_bootstrap(&cell);
        // Use the actual canonical domain encoder, not a parallel format.
        let digest = rsf_core::Digest::blake3_bytes(&bytes).bytes;
        Ok(Self {
            pg,
            view,
            primary,
            voters: cell.replicas.iter().copied().collect(),
            bytes,
            digest,
        })
    }
    pub fn pg(&self) -> PlacementGroupId {
        self.pg
    }
    pub fn view(&self) -> Epoch {
        self.view
    }
    pub fn primary(&self) -> NodeId {
        self.primary
    }
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }
    pub fn bootstrap_digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Pure validation for callers that must reject before durable reservation.
    /// This grants no filesystem custody or permission to initialize a voter.
    pub fn validate_limits(&self, limits: VsrPrefixLimits) -> RsfResult<()> {
        self.check_limits(limits)
    }

    pub(super) fn check_limits(&self, limits: VsrPrefixLimits) -> RsfResult<()> {
        let bytes = u64::try_from(self.bytes.len())
            .map_err(|_| invalid_input("bootstrap byte count exceeds platform range"))?
            .checked_add(18)
            .ok_or_else(|| invalid_input("bootstrap history length overflow"))?;
        limits.check(1, bytes)?;
        // Validate physical framing/manifest bounds before creating the root.
        let physical = limits.storage()?;
        if rsf_storage::wal_record_disk_bytes(self.bytes.len() as u64)? > physical.max_record_bytes
        {
            return Err(invalid_input("bootstrap exceeds physical record bound"));
        }
        Ok(())
    }
}
