//! Authenticated transition-only consensus. No application role is promoted.
use super::*;
use rsf_vsr::{
    MembershipAuthorityInbox, MembershipDecisionStore, VsrMembershipAccepted, VsrMembershipChosen,
    VsrMembershipDecision, VsrMembershipInput, VsrMembershipPromise, VsrMembershipRound,
};
mod client;
mod server;
mod staging;
fn context(pg: PlacementGroupId, round: VsrMembershipRound, proposal: [u8; 32]) -> Vec<u8> {
    let mut out = pg.0.to_le_bytes().to_vec();
    out.extend_from_slice(&round.counter.to_le_bytes());
    out.extend_from_slice(&round.coordinator.0.to_le_bytes());
    out.extend_from_slice(&proposal);
    out
}
fn read_context(bytes: &[u8]) -> io::Result<(PlacementGroupId, VsrMembershipRound, [u8; 32])> {
    Ok((
        PlacementGroupId(u64_at(bytes, 0)?),
        VsrMembershipRound {
            counter: u64_at(bytes, 8)?,
            coordinator: NodeId(u64_at(bytes, 16)?),
        },
        id_at(bytes, 24)?,
    ))
}
fn id_at(bytes: &[u8], offset: usize) -> io::Result<[u8; 32]> {
    Ok(bytes
        .get(offset..offset + 32)
        .ok_or_else(|| invalid("truncated membership input identity"))?
        .try_into()
        .unwrap())
}
fn input_query(pg: PlacementGroupId, id: [u8; 32]) -> Vec<u8> {
    let mut out = pg.0.to_le_bytes().to_vec();
    out.extend_from_slice(&id);
    out
}
fn acceptance_request(
    decision: &VsrMembershipDecision,
    nomination: &VsrMembershipAccepted,
) -> Result<Vec<u8>, rsf_core::RsfError> {
    let encoded = decision.encode()?;
    let mut out = (encoded.len() as u64).to_le_bytes().to_vec();
    out.extend_from_slice(&encoded);
    out.extend_from_slice(&nomination.encode()?);
    Ok(out)
}
fn decode_acceptance(bytes: &[u8]) -> io::Result<(VsrMembershipDecision, VsrMembershipAccepted)> {
    let size = usize::try_from(u64_at(bytes, 0)?)
        .map_err(|_| invalid("membership decision length exceeds platform range"))?;
    let end = 8usize
        .checked_add(size)
        .ok_or_else(|| invalid("membership decision length overflow"))?;
    let decision = VsrMembershipDecision::decode(
        bytes
            .get(8..end)
            .ok_or_else(|| invalid("truncated membership decision"))?,
    )
    .map_err(|e| invalid(e.message))?;
    let nomination = VsrMembershipAccepted::decode(
        bytes
            .get(end..)
            .ok_or_else(|| invalid("truncated membership nomination"))?,
    )
    .map_err(|e| invalid(e.message))?;
    Ok((decision, nomination))
}
