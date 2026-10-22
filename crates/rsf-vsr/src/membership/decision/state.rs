use super::*;
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct State {
    pub(super) promised: Option<VsrMembershipRound>,
    pub(super) accepted: Option<(VsrMembershipRound, [u8; 32])>,
    pub(super) staged: Option<(VsrMembershipRound, [u8; 32])>,
    pub(super) chosen: Option<VsrMembershipChosen>,
}
pub(super) struct DecodedState {
    pub(super) local: NodeId,
    pub(super) proposal: VsrMembershipProposal,
    pub(super) authority: [u8; 32],
    pub(super) bootstrap: [u8; 32],
    pub(super) state: State,
}
pub(super) fn encode_state(
    local: NodeId,
    proposal: &VsrMembershipProposal,
    authority: [u8; 32],
    bootstrap: [u8; 32],
    state: &State,
) -> RsfResult<Vec<u8>> {
    let mut out = b"VSRMDS1".to_vec();
    put_u64(&mut out, local.0);
    put_bytes(&mut out, &proposal.encode()?)?;
    out.extend_from_slice(&authority);
    out.extend_from_slice(&bootstrap);
    out.push(u8::from(state.promised.is_some()));
    if let Some(round) = state.promised {
        put_round(&mut out, round);
    }
    for value in [state.accepted, state.staged] {
        out.push(u8::from(value.is_some()));
        if let Some((round, id)) = value {
            put_round(&mut out, round);
            out.extend_from_slice(&id);
        }
    }
    out.push(u8::from(state.chosen.is_some()));
    if let Some(chosen) = &state.chosen {
        put_bytes(&mut out, &chosen.encode()?)?;
    }
    if out.len() > MAX_HISTORY_RECORD_BYTES {
        return Err(invalid_input("membership decision state exceeds limit"));
    }
    Ok(out)
}
pub(super) fn decode_state(bytes: &[u8]) -> RsfResult<DecodedState> {
    let mut r = Reader::new(bytes, b"VSRMDS1")?;
    let local = NodeId(r.u64()?);
    let proposal = VsrMembershipProposal::decode(r.bytes()?)?;
    let authority = r.id()?;
    let bootstrap = r.id()?;
    let promised = match r.byte()? {
        0 => None,
        1 => Some(r.round()?),
        _ => return Err(corrupt("invalid membership promise state flag")),
    };
    let mut values = Vec::new();
    for _ in 0..2 {
        values.push(match r.byte()? {
            0 => None,
            1 => Some((r.round()?, r.id()?)),
            _ => return Err(corrupt("invalid membership input state flag")),
        });
    }
    let chosen = match r.byte()? {
        0 => None,
        1 => Some(VsrMembershipChosen::decode(r.bytes()?)?),
        _ => return Err(corrupt("invalid membership chosen state flag")),
    };
    r.finish()?;
    let state = State {
        promised,
        accepted: values[0],
        staged: values[1],
        chosen,
    };
    if encode_state(local, &proposal, authority, bootstrap, &state)? != bytes {
        return Err(corrupt("noncanonical membership decision state"));
    }
    Ok(DecodedState {
        local,
        proposal,
        authority,
        bootstrap,
        state,
    })
}
pub(super) fn validate_transition(
    old: Option<&State>,
    new: &State,
    proposal: &VsrMembershipProposal,
) -> RsfResult<()> {
    if let Some(round) = new.promised {
        round.validate(proposal)?;
    }
    for (round, _) in [new.accepted, new.staged].into_iter().flatten() {
        round.validate(proposal)?;
        if new.promised.is_none_or(|p| round > p) {
            return Err(corrupt("membership input state exceeds durable round"));
        }
    }
    if let Some(chosen) = &new.chosen {
        chosen.encode()?;
        if chosen.proposal() != proposal
            || new
                .accepted
                .is_none_or(|(r, id)| id != chosen.input_id() || r < chosen.round())
        {
            return Err(corrupt(
                "membership chosen state lacks matching accepted input",
            ));
        }
    }
    if let Some(old) = old
        && (old.promised > new.promised
            || old.accepted.is_some_and(|(round, id)| {
                new.accepted
                    .is_none_or(|(next, value)| next < round || (next == round && id != value))
            })
            || old.chosen.as_ref().is_some_and(|chosen| {
                new.chosen
                    .as_ref()
                    .is_none_or(|next| next.input_id() != chosen.input_id())
            }))
    {
        return Err(corrupt(
            "membership decision state regresses or changes a chosen input",
        ));
    }
    Ok(())
}
