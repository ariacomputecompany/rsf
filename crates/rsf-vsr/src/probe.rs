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
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 73 || &bytes[..5] != b"RSFV1" {
            return Err(corrupt("invalid primary probe encoding"));
        }
        let count = u32::from_le_bytes(bytes[69..73].try_into().unwrap()) as usize;
        if count > 4096 || bytes.len() != 73 + 8 * count {
            return Err(corrupt("invalid primary probe voter count"));
        }
        let probe = Self {
            pg: PlacementGroupId(read_u64(bytes, 5)?),
            view: Epoch(read_u64(bytes, 13)?),
            primary: NodeId(read_u64(bytes, 21)?),
            committed_tail: LogIndex(read_u64(bytes, 29)?),
            committed_tip: Digest::blake3(bytes[37..69].try_into().unwrap())?,
            voters: (0..count)
                .map(|i| read_u64(bytes, 73 + 8 * i).map(NodeId))
                .collect::<RsfResult<_>>()?,
        };
        if probe.encode()? != bytes {
            return Err(corrupt("noncanonical primary probe"));
        }
        Ok(probe)
    }

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
        self.ensure_active_view()?;
        self.recovery_probe()
    }

    pub(super) fn recovery_probe(&self) -> RsfResult<VsrPrimaryProbe> {
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

/// Authenticated recovery/liveness inventory. A promised replica can report its
/// last normal view without granting the old primary permission to serve.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrReplicaStatus {
    pub normal: VsrPrimaryProbe,
    pub promise: Option<VsrViewChangeRequest>,
}
impl VsrReplicaStatus {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let normal = self.normal.encode()?;
        let promise = match &self.promise {
            Some(request) => {
                if request.pg != self.normal.pg
                    || request.voters != self.normal.voters
                    || request.previous_view > self.normal.view
                    || request.target_view <= self.normal.view
                {
                    return Err(invalid_input(
                        "replica status promise differs from normal configuration",
                    ));
                }
                request.encode()?
            }
            None => Vec::new(),
        };
        let mut bytes = b"RSFS1".to_vec();
        bytes.extend_from_slice(&(normal.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&normal);
        bytes.extend_from_slice(&(promise.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&promise);
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 13 || !bytes.starts_with(b"RSFS1") || bytes.len() > 128 * 1024 {
            return Err(corrupt("invalid replica status"));
        }
        let length = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let end = 9usize
            .checked_add(length)
            .ok_or_else(|| corrupt("replica status length overflow"))?;
        let normal = VsrPrimaryProbe::decode(
            bytes
                .get(9..end)
                .ok_or_else(|| corrupt("truncated replica status normal view"))?,
        )?;
        let length = bytes
            .get(end..end + 4)
            .ok_or_else(|| corrupt("truncated replica status promise length"))?;
        let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
        let payload = bytes
            .get(end + 4..)
            .ok_or_else(|| corrupt("truncated replica status promise"))?;
        if payload.len() != length {
            return Err(corrupt("replica status trailing or missing bytes"));
        }
        let status = Self {
            normal,
            promise: if payload.is_empty() {
                None
            } else {
                Some(VsrViewChangeRequest::decode(payload)?)
            },
        };
        if status.encode()? != bytes {
            return Err(corrupt("noncanonical replica status"));
        }
        Ok(status)
    }
}
impl DurableVsrCell {
    pub fn replica_status(&self) -> RsfResult<VsrReplicaStatus> {
        Ok(VsrReplicaStatus {
            normal: self.recovery_probe()?,
            promise: self.view_change.clone(),
        })
    }
}

#[cfg(test)]
mod tests;
