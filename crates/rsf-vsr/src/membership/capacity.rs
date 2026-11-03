//! Bound the complete transition proof before an old-primary initiation fences
//! any voter. Individual voter-vector codec limits do not bound nested proofs.
use super::*;
use rsf_core::Digest;

impl VsrMembershipProposal {
    /// Check that every distinct-voter proof subset, including all reporters,
    /// fits the current bounded decision and activation formats. This performs
    /// no storage mutation and does not create voting or installation evidence.
    /// Historical proposal decoding remains independent of this admission check.
    pub fn validate_publication_capacity(&self) -> RsfResult<()> {
        self.encode()?;
        let bounds = proof_bounds(self)?;
        for (name, bytes) in [
            ("input", bounds.input),
            ("decision", bounds.decision),
            ("chosen", bounds.chosen),
            ("staged quorum", bounds.quorum),
            ("activation", bounds.activation),
            ("staged metadata", bounds.metadata),
            ("publication input", bounds.publication),
            ("published report", bounds.published_report),
            ("published quorum", bounds.published_quorum),
        ] {
            if bytes > MAX_HISTORY_RECORD_BYTES {
                return Err(invalid_input(format!(
                    "membership {name} proof can require {bytes} bytes, exceeding publication record limit {MAX_HISTORY_RECORD_BYTES}"
                )));
            }
        }
        Ok(())
    }
}

struct Bounds {
    input: usize,
    decision: usize,
    chosen: usize,
    quorum: usize,
    activation: usize,
    metadata: usize,
    publication: usize,
    published_report: usize,
    published_quorum: usize,
}

fn proof_bounds(proposal: &VsrMembershipProposal) -> RsfResult<Bounds> {
    // All scalar values, IDs and digests in these codecs have fixed width.
    // Encode one representative per report kind to follow the actual codecs,
    // then count all distinct voters without constructing quadratic proof data.
    // These local values are used only for lengths and never leave this function.
    let old = proposal.election.voters.len();
    let new = proposal.new_voters.len();
    let round = VsrMembershipRound {
        counter: 1,
        coordinator: proposal.election.candidate,
    };
    let report = VsrMembershipReport {
        proposal: proposal.clone(),
        voter: VsrViewChangeReport {
            request: proposal.election.clone(),
            replica: proposal.election.voters[0],
            accepted_tail: LogIndex(0),
            history: VsrHistoryDescriptor {
                probe: VsrPrimaryProbe {
                    pg: proposal.election.pg,
                    view: proposal.election.previous_view,
                    primary: proposal.old_primary,
                    voters: proposal.election.voters.clone(),
                    committed_tail: LogIndex(0),
                    committed_tip: Digest::blake3_bytes(b"membership capacity"),
                },
                records: 1,
                digest: Digest::blake3_bytes(b"membership capacity"),
            },
        },
    };
    let promise = VsrMembershipPromise {
        proposal: proposal.clone(),
        replica: proposal.new_voters[0],
        round,
        // A recovered promise carries the largest accepted-input variant.
        accepted: Some((round, [0; 32])),
    };
    let accepted = VsrMembershipAccepted {
        proposal: proposal.clone(),
        replica: proposal.new_voters[0],
        round,
        input: [0; 32],
    };
    let staged = VsrMembershipStagedReport {
        proposal: proposal.clone(),
        replica: proposal.new_voters[0],
        input: [0; 32],
        plan: [0; 32],
        stage: [0; 32],
        committed_tail: LogIndex(0),
        accepted_tail: LogIndex(0),
    };
    let input = list_bytes(b"VSRMDI1", old, report.encode()?.len())?;
    let chosen = list_bytes(b"VSRMDC1", new, accepted.encode()?.len())?;
    let quorum = list_bytes(b"VSRMSQ1", new, staged.encode()?.len())?;
    let decision = sum(&[
        b"VSRMDD1".len(),
        8,
        input,
        8,
        product(new, sum(&[8, promise.encode()?.len()])?)?,
    ])?;
    let activation = sum(&[
        b"VSRMAI1".len(),
        8,
        input,
        8,
        chosen,
        8,
        quorum,
        8,
        staged.encode()?.len(),
        8,
    ])?;
    // META V2 adds a fixed publication digest without changing V1 decoding.
    let metadata = sum(&[
        b"VSRMST2".len(),
        8,
        8,
        8,
        input,
        8,
        chosen,
        32,
        32,
        8,
        32,
        32,
    ])?;
    // PUBLICATION binds the exact physical local receipt plus a new-majority
    // input. Its outer length fields and checksum count toward the same cap.
    let publication = sum(&[b"VSRMPU1".len(), 8, staged.encode()?.len(), 8, quorum, 32])?;
    let published = VsrMembershipPublishedReport {
        proposal: proposal.clone(),
        replica: proposal.new_voters[0],
        input: [0; 32],
        plan: [0; 32],
    };
    let published_report = published.encode()?.len();
    let published_quorum = list_bytes(b"VSRMPQ1", new, published_report)?;
    Ok(Bounds {
        input,
        decision,
        chosen,
        quorum,
        activation,
        metadata,
        publication,
        published_report,
        published_quorum,
    })
}
fn list_bytes(magic: &[u8], count: usize, item: usize) -> RsfResult<usize> {
    sum(&[magic.len(), 8, product(count, sum(&[8, item])?)?])
}
fn product(a: usize, b: usize) -> RsfResult<usize> {
    a.checked_mul(b)
        .ok_or_else(|| invalid_input("membership proof size overflow"))
}
fn sum(values: &[usize]) -> RsfResult<usize> {
    values.iter().try_fold(0usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| invalid_input("membership proof size overflow"))
    })
}

#[cfg(test)]
mod tests;
