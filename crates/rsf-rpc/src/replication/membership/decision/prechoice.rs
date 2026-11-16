//! Bounded, authenticated operations for an already bound decision journal.
use super::*;
use rsf_vsr::{
    VsrMembershipChosenInput, VsrMembershipDecisionState, VsrMembershipImportReply,
    VsrMembershipInputProgress, VsrMembershipPromiseReply,
};
mod client;
mod server;

const MAGIC: &[u8] = b"RSFMD1";
const STATE: u8 = 0;
const PROMISE: u8 = 1;
const CHOSEN: u8 = 2;
const INPUT: u8 = 3;
const IMPORT_BEGIN: u8 = 4;
const IMPORT_BATCH: u8 = 5;
const IMPORT_FINISH: u8 = 6;
const READ: u8 = 7;
const PREFIX: usize = 79;

fn request(
    tag: u8,
    proposal: &VsrMembershipProposal,
    bootstrap: [u8; 32],
    body: &[u8],
) -> Result<Vec<u8>, String> {
    if PREFIX
        .checked_add(body.len())
        .is_none_or(|n| n > MAX_FRAME - HEADER)
    {
        return Err("membership recovery request exceeds frame limit".into());
    }
    let mut out = MAGIC.to_vec();
    out.push(tag);
    out.extend_from_slice(&proposal.election.pg.0.to_le_bytes());
    out.extend_from_slice(&proposal.id().map_err(|e| e.message)?);
    out.extend_from_slice(&bootstrap);
    out.extend_from_slice(body);
    Ok(out)
}
fn response(tag: u8, body: &[u8]) -> io::Result<Vec<u8>> {
    if body.len() > MAX_FRAME - HEADER - 7 {
        return Err(invalid("membership recovery reply exceeds frame limit"));
    }
    let mut out = MAGIC.to_vec();
    out.push(tag);
    out.extend_from_slice(body);
    Ok(out)
}
fn reply(bytes: &[u8], tag: u8) -> Result<&[u8], String> {
    if !bytes.starts_with(MAGIC) || bytes.get(6) != Some(&tag) {
        return Err("membership recovery reply has another operation type".into());
    }
    Ok(&bytes[7..])
}
