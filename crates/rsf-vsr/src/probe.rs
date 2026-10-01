//! Current-view evidence for authenticated, read-only primary quorum probes.
use super::*;
use rsf_core::{Digest, DigestAlgorithm};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrPrimaryProbe {
    pub pg: PlacementGroupId,
    pub view: Epoch,
    pub primary: NodeId,
    pub voters: Vec<NodeId>,
    pub committed_tail: LogIndex,
    pub committed_tip: Digest,
}

impl VsrPrimaryProbe {
    /// RSFV1 fixes BLAKE3 for the canonical committed-prepare digest. Membership
    /// is sorted and bounded; byte equality covers the entire probe contract.
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.voters.is_empty()
            || self.voters.len() > 4096
            || self.voters.windows(2).any(|pair| pair[0] >= pair[1])
            || !self.voters.contains(&self.primary)
            || self.committed_tip.algorithm != DigestAlgorithm::Blake3
        {
            return Err(invalid_input("invalid primary probe membership or digest"));
        }
        self.committed_tip.validate()?;
        let mut bytes = b"RSFV1".to_vec();
        for value in [
            self.pg.0,
            self.view.0,
            self.primary.0,
            self.committed_tail.0,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes.extend_from_slice(self.committed_tip.as_bytes());
        bytes.extend_from_slice(&(self.voters.len() as u32).to_le_bytes());
        for voter in &self.voters {
            bytes.extend_from_slice(&voter.0.to_le_bytes());
        }
        Ok(bytes)
    }
}

impl DurableVsrCell {
    pub fn primary_probe(&self) -> RsfResult<VsrPrimaryProbe> {
        self.ensure_healthy()?;
        let tail = self.committed_tail();
        let tip = if tail.0 == 0 {
            Vec::new()
        } else {
            self.prepared(tail)
                .ok_or_else(|| corrupt("committed primary tip has no prepare"))?
                .encode()?
        };
        Ok(VsrPrimaryProbe {
            pg: self.cell.pg,
            view: self.view(),
            primary: self.primary(),
            voters: self.replicas(),
            committed_tail: tail,
            committed_tip: Digest::blake3_bytes(&tip),
        })
    }
}
