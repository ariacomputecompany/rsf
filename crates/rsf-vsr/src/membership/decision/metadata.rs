//! Bounded decision facts. Status is not a promise, chosen proof, or input ACK.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipDecisionState {
    pub pg: PlacementGroupId,
    pub proposal: [u8; 32],
    pub replica: NodeId,
    pub bootstrap: [u8; 32],
    pub authority: [u8; 32],
    pub promised: Option<VsrMembershipRound>,
    pub accepted: Option<(VsrMembershipRound, [u8; 32])>,
    pub chosen: Option<(VsrMembershipRound, [u8; 32])>,
}
impl VsrMembershipDecisionState {
    pub fn validate_for(&self, proposal: &VsrMembershipProposal) -> RsfResult<()> {
        self.encode()?;
        if self.pg != proposal.election.pg
            || self.proposal != proposal.id()?
            || !proposal.new_voters.contains(&self.replica)
        {
            return Err(invalid_input(
                "membership decision state has another bound identity",
            ));
        }
        for round in self
            .promised
            .into_iter()
            .chain(self.accepted.map(|v| v.0))
            .chain(self.chosen.map(|v| v.0))
        {
            round.validate(proposal)?;
        }
        Ok(())
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        if self.promised.is_some_and(|r| r.counter == 0)
            || self
                .accepted
                .is_some_and(|(r, _)| r.counter == 0 || self.promised.is_none_or(|p| r > p))
            || self.chosen.is_some_and(|(r, id)| {
                r.counter == 0 || self.accepted.is_none_or(|(a, value)| value != id || a < r)
            })
        {
            return Err(invalid_input(
                "membership decision state has impossible round relations",
            ));
        }
        let mut out = b"VSRMDR1".to_vec();
        put_u64(&mut out, self.pg.0);
        out.extend_from_slice(&self.proposal);
        put_u64(&mut out, self.replica.0);
        out.extend_from_slice(&self.bootstrap);
        out.extend_from_slice(&self.authority);
        out.push(u8::from(self.promised.is_some()));
        if let Some(round) = self.promised {
            put_round(&mut out, round);
        }
        for value in [self.accepted, self.chosen] {
            out.push(u8::from(value.is_some()));
            if let Some((round, id)) = value {
                put_round(&mut out, round);
                out.extend_from_slice(&id);
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::bounded(bytes, b"VSRMDR1", 256)?;
        let pg = PlacementGroupId(r.u64()?);
        let proposal = r.id()?;
        let replica = NodeId(r.u64()?);
        let bootstrap = r.id()?;
        let authority = r.id()?;
        let promised = match r.byte()? {
            0 => None,
            1 => Some(r.round()?),
            _ => return Err(corrupt("invalid decision promise flag")),
        };
        let mut values = [None, None];
        for value in &mut values {
            *value = match r.byte()? {
                0 => None,
                1 => Some((r.round()?, r.id()?)),
                _ => return Err(corrupt("invalid decision value flag")),
            };
        }
        r.finish()?;
        let state = Self {
            pg,
            proposal,
            replica,
            bootstrap,
            authority,
            promised,
            accepted: values[0],
            chosen: values[1],
        };
        if state.encode()? != bytes {
            return Err(corrupt("noncanonical decision state"));
        }
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VsrMembershipPromiseReply {
    Granted(VsrMembershipPromise),
    Preempted(VsrMembershipDecisionState),
}
impl VsrMembershipPromiseReply {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = b"VSRMPR1".to_vec();
        match self {
            Self::Granted(value) => {
                out.push(0);
                put_bytes(&mut out, &value.encode()?)?;
            }
            Self::Preempted(value) => {
                if value.promised.is_none() {
                    return Err(invalid_input(
                        "membership preemption has no retained promise",
                    ));
                }
                out.push(1);
                put_bytes(&mut out, &value.encode()?)?;
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::bounded(bytes, b"VSRMPR1", MAX_HISTORY_RECORD_BYTES + 16)?;
        let value = match r.byte()? {
            0 => Self::Granted(VsrMembershipPromise::decode(r.bytes()?)?),
            1 => Self::Preempted(VsrMembershipDecisionState::decode(r.bytes()?)?),
            _ => return Err(corrupt("invalid structured membership promise variant")),
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership promise reply"));
        }
        Ok(value)
    }
}

impl VsrMembershipRound {
    /// Least own round strictly above the observed floor. This does not promise
    /// it or permit use without the normal durable acceptor operations.
    pub fn after(
        proposal: &VsrMembershipProposal,
        local: NodeId,
        floor: Option<Self>,
    ) -> RsfResult<Self> {
        proposal.encode()?;
        let counter = match floor {
            None => 1,
            Some(old) => {
                old.validate(proposal)?;
                if local > old.coordinator {
                    old.counter
                } else {
                    old.counter
                        .checked_add(1)
                        .ok_or_else(|| invalid_input("membership round space exhausted"))?
                }
            }
        };
        let result = Self {
            counter,
            coordinator: local,
        };
        result.validate(proposal)?;
        Ok(result)
    }
}
