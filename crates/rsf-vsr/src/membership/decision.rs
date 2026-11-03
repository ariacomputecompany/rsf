//! Transition-only consensus. These votes choose installation input; they never
//! acknowledge application operations or grant membership/client readiness.
use super::*;
use rsf_core::Digest;
pub(super) mod disk;
mod inbox;
pub use inbox::MembershipAuthorityInbox;
mod input;
pub use input::VsrMembershipInput;
mod metadata;
pub use metadata::{VsrMembershipDecisionState, VsrMembershipPromiseReply};
mod progress;
pub use progress::{
    VsrMembershipChosenInput, VsrMembershipImportReply, VsrMembershipInputProgress,
};
mod state;
mod store;
pub use store::MembershipDecisionStore;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct VsrMembershipRound {
    pub counter: u64,
    pub coordinator: NodeId,
}
impl VsrMembershipRound {
    fn validate(&self, proposal: &VsrMembershipProposal) -> RsfResult<()> {
        if self.counter == 0 || !proposal.new_voters.contains(&self.coordinator) {
            return Err(invalid_input("invalid membership installation round"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipPromise {
    pub proposal: VsrMembershipProposal,
    pub replica: NodeId,
    pub round: VsrMembershipRound,
    pub accepted: Option<(VsrMembershipRound, [u8; 32])>,
}
impl VsrMembershipPromise {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.round.validate(&self.proposal)?;
        if !self.proposal.new_voters.contains(&self.replica) {
            return Err(invalid_input(
                "membership promise reporter is not a proposed voter",
            ));
        }
        let mut out = b"VSRMDP1".to_vec();
        put_bytes(&mut out, &self.proposal.encode()?)?;
        put_u64(&mut out, self.replica.0);
        put_round(&mut out, self.round);
        out.push(u8::from(self.accepted.is_some()));
        if let Some((round, id)) = self.accepted {
            round.validate(&self.proposal)?;
            if round > self.round {
                return Err(invalid_input("accepted membership round exceeds promise"));
            }
            put_round(&mut out, round);
            out.extend_from_slice(&id);
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMDP1")?;
        let proposal = VsrMembershipProposal::decode(r.bytes()?)?;
        let replica = NodeId(r.u64()?);
        let round = r.round()?;
        let accepted = match r.byte()? {
            0 => None,
            1 => Some((r.round()?, r.id()?)),
            _ => return Err(corrupt("invalid accepted membership flag")),
        };
        r.finish()?;
        let value = Self {
            proposal,
            replica,
            round,
            accepted,
        };
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership promise"));
        }
        Ok(value)
    }
}
/// Construct from authenticated distinct proposed-voter reports. A complete
/// package may be relayed by its authenticated coordinator under the crash-fault
/// model; the report bytes are not independent node signatures.
/// Retain the highest accepted input, including when it was a minority upload.
#[derive(Debug)]
pub struct VsrMembershipDecision {
    promises: Vec<VsrMembershipPromise>,
    input: VsrMembershipInput,
}
impl VsrMembershipDecision {
    pub fn from_promises(
        mut promises: Vec<VsrMembershipPromise>,
        input: VsrMembershipInput,
    ) -> RsfResult<Self> {
        input.encode()?;
        promises.sort_by_key(|p| p.replica);
        let first = promises
            .first()
            .ok_or_else(|| invalid_input("membership decision has no promises"))?;
        let mut accepted = None;
        let mut rounds = BTreeMap::new();
        for promise in &promises {
            promise.encode()?;
            if promise.proposal != *input.proposal() || promise.round != first.round {
                return Err(invalid_input(
                    "membership decision has mixed authority or rounds",
                ));
            }
            if let Some((round, id)) = promise.accepted {
                if rounds.insert(round, id).is_some_and(|prior| prior != id) {
                    return Err(corrupt("one membership round accepted conflicting inputs"));
                }

                match accepted {
                    Some((prior, old)) if prior == round && old != id => {
                        return Err(corrupt("one membership round accepted conflicting inputs"));
                    }
                    Some((prior, _)) if prior > round => {}
                    _ => accepted = Some((round, id)),
                }
            }
        }
        if promises.len() <= input.proposal().new_voters.len() / 2
            || promises.windows(2).any(|p| p[0].replica == p[1].replica)
        {
            return Err(invalid_input(
                "membership decision lacks a distinct new quorum",
            ));
        }
        let input_id = input.id()?;
        if accepted.is_some_and(|(_, id)| input_id != id) {
            return Err(invalid_input(
                "membership recovery must retain the highest accepted input",
            ));
        }
        Ok(Self { promises, input })
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = b"VSRMDD1".to_vec();
        put_bytes(&mut out, &self.input.encode()?)?;
        put_u64(&mut out, self.promises.len() as u64);
        for promise in &self.promises {
            put_bytes(&mut out, &promise.encode()?)?;
            if out.len() > MAX_HISTORY_RECORD_BYTES {
                return Err(invalid_input("membership decision exceeds limit"));
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMDD1")?;
        let input = VsrMembershipInput::decode(r.bytes()?)?;
        let count = r.u64()?;
        if count > 4096 {
            return Err(corrupt("too many membership decision promises"));
        }
        let mut promises = Vec::new();
        for _ in 0..count {
            promises.push(VsrMembershipPromise::decode(r.bytes()?)?);
        }
        r.finish()?;
        let value = Self::from_promises(promises, input)?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership decision"));
        }
        Ok(value)
    }
    pub fn input(&self) -> &VsrMembershipInput {
        &self.input
    }
    pub fn round(&self) -> VsrMembershipRound {
        self.promises[0].round
    }
    pub fn promises(&self) -> &[VsrMembershipPromise] {
        &self.promises
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipAccepted {
    pub proposal: VsrMembershipProposal,
    pub replica: NodeId,
    pub round: VsrMembershipRound,
    pub input: [u8; 32],
}
impl VsrMembershipAccepted {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.round.validate(&self.proposal)?;
        if !self.proposal.new_voters.contains(&self.replica) {
            return Err(invalid_input(
                "membership acceptance reporter is not a proposed voter",
            ));
        }
        let mut out = b"VSRMDA1".to_vec();
        put_bytes(&mut out, &self.proposal.encode()?)?;
        put_u64(&mut out, self.replica.0);
        put_round(&mut out, self.round);
        out.extend_from_slice(&self.input);
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMDA1")?;
        let value = Self {
            proposal: VsrMembershipProposal::decode(r.bytes()?)?,
            replica: NodeId(r.u64()?),
            round: r.round()?,
            input: r.id()?,
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership acceptance"));
        }
        Ok(value)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipChosen {
    accepted: Vec<VsrMembershipAccepted>,
}
impl VsrMembershipChosen {
    /// Authenticate every reporting node and cluster before constructing this
    /// crash-fault proof. IDs in report bytes are not independent signatures.
    pub fn from_accepted(mut accepted: Vec<VsrMembershipAccepted>) -> RsfResult<Self> {
        accepted.sort_by_key(|a| a.replica);
        let first = accepted
            .first()
            .ok_or_else(|| invalid_input("membership choice has no acceptance reports"))?;
        for report in &accepted {
            report.encode()?;
            if report.proposal != first.proposal
                || report.round != first.round
                || report.input != first.input
            {
                return Err(invalid_input("membership choice has mixed plans or rounds"));
            }
        }
        if accepted.len() <= first.proposal.new_voters.len() / 2
            || accepted.windows(2).any(|a| a[0].replica == a[1].replica)
        {
            return Err(invalid_input(
                "membership choice lacks a distinct new quorum",
            ));
        }
        Ok(Self { accepted })
    }
    pub fn proposal(&self) -> &VsrMembershipProposal {
        &self.accepted[0].proposal
    }
    pub fn round(&self) -> VsrMembershipRound {
        self.accepted[0].round
    }
    pub fn input_id(&self) -> [u8; 32] {
        self.accepted[0].input
    }
    pub fn accepted(&self) -> &[VsrMembershipAccepted] {
        &self.accepted
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = b"VSRMDC1".to_vec();
        put_u64(&mut out, self.accepted.len() as u64);
        for report in &self.accepted {
            put_bytes(&mut out, &report.encode()?)?;
            if out.len() > MAX_HISTORY_RECORD_BYTES {
                return Err(invalid_input("membership choice exceeds limit"));
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMDC1")?;
        let count = r.u64()?;
        if count > 4096 {
            return Err(corrupt("too many membership acceptance reports"));
        }
        let mut reports = Vec::new();
        for _ in 0..count {
            reports.push(VsrMembershipAccepted::decode(r.bytes()?)?);
        }
        r.finish()?;
        let value = Self::from_accepted(reports)?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership choice"));
        }
        Ok(value)
    }
}
pub(super) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}
pub(super) fn put_round(out: &mut Vec<u8>, round: VsrMembershipRound) {
    put_u64(out, round.counter);
    put_u64(out, round.coordinator.0);
}
pub(super) fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> RsfResult<()> {
    if bytes.len() > MAX_HISTORY_RECORD_BYTES {
        return Err(invalid_input("membership field exceeds limit"));
    }
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
    Ok(())
}
pub(super) struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    pub(super) fn new(bytes: &'a [u8], magic: &[u8]) -> RsfResult<Self> {
        Self::bounded(bytes, magic, MAX_HISTORY_RECORD_BYTES)
    }
    pub(super) fn bounded(bytes: &'a [u8], magic: &[u8], limit: usize) -> RsfResult<Self> {
        if bytes.len() > limit || !bytes.starts_with(magic) {
            return Err(corrupt("invalid membership decision encoding"));
        }
        Ok(Self {
            bytes,
            at: magic.len(),
        })
    }
    pub(super) fn take(&mut self, n: usize) -> RsfResult<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .ok_or_else(|| corrupt("membership field length overflow"))?;
        let result = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| corrupt("truncated membership decision"))?;
        self.at = end;
        Ok(result)
    }
    pub(super) fn byte(&mut self) -> RsfResult<u8> {
        Ok(self.take(1)?[0])
    }
    pub(super) fn u64(&mut self) -> RsfResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(super) fn id(&mut self) -> RsfResult<[u8; 32]> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    pub(super) fn round(&mut self) -> RsfResult<VsrMembershipRound> {
        Ok(VsrMembershipRound {
            counter: self.u64()?,
            coordinator: NodeId(self.u64()?),
        })
    }
    pub(super) fn bytes(&mut self) -> RsfResult<&'a [u8]> {
        let n = usize::try_from(self.u64()?)
            .map_err(|_| corrupt("membership field exceeds platform range"))?;
        self.take(n)
    }
    pub(super) fn finish(self) -> RsfResult<()> {
        if self.at != self.bytes.len() {
            return Err(corrupt("trailing membership decision bytes"));
        }
        Ok(())
    }
}
