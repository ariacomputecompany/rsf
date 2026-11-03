use super::*;

impl PeerReplicaServer {
    pub(in crate::replication) fn dispatch_membership_recovery(
        &mut self,
        sender: NodeId,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        if sender == self.local || payload.len() < PREFIX || !payload.starts_with(MAGIC) {
            return Err(invalid("invalid authenticated membership recovery request"));
        }
        let tag = payload[6];
        let pg = PlacementGroupId(u64_at(payload, 7)?);
        let proposal = id_at(payload, 15)?;
        let bootstrap = id_at(payload, 47)?;
        let body = &payload[PREFIX..];
        let store = self
            .membership_decisions
            .get_mut(&pg)
            .ok_or_else(|| invalid("membership recovery requires an existing decision journal"))?;
        let state = store.decision_state().map_err(|e| invalid(e.message))?;
        if state.proposal != proposal
            || state.bootstrap != bootstrap
            || !store.proposal().new_voters.contains(&sender)
        {
            return Err(invalid(
                "membership recovery differs from bound proposal/bootstrap or authenticated sender",
            ));
        }
        let rsf = |e: rsf_core::RsfError| invalid(e.message);
        let mut activity = None;
        let bytes = match tag {
            STATE if body.is_empty() => state.encode().map_err(rsf)?,
            PROMISE if body.len() == 16 => {
                let round = VsrMembershipRound {
                    counter: u64_at(body, 0)?,
                    coordinator: NodeId(u64_at(body, 8)?),
                };
                let value = store.try_promise(sender, round).map_err(rsf)?;
                if matches!(value, VsrMembershipPromiseReply::Granted(_)) {
                    activity = Some((round, None));
                }
                value.encode().map_err(rsf)?
            }
            CHOSEN if body.len() == 32 => {
                let id = id_at(body, 0)?;
                match store.chosen() {
                    Some(chosen) if chosen.input_id() == id => {
                        // Missing acknowledged custody must not be hidden by a
                        // cached chosen tuple/certificate response.
                        store.approval(id).map_err(rsf)?;
                        let mut out = vec![1];
                        out.extend_from_slice(&chosen.encode().map_err(rsf)?);
                        out
                    }
                    Some(_) => return Err(invalid("membership chosen query names another input")),
                    None => vec![0],
                }
            }
            INPUT if body.len() == 32 => store
                .input_progress(id_at(body, 0)?)
                .and_then(|value| value.encode())
                .map_err(rsf)?,
            IMPORT_BEGIN => {
                let input = VsrMembershipChosenInput::decode(body).map_err(rsf)?;
                store
                    .begin_chosen_input(sender, input.chosen, input.input)
                    .and_then(|value| value.encode())
                    .map_err(rsf)?
            }
            IMPORT_BATCH if body.len() > 32 => {
                let id = id_at(body, 0)?;
                let batch = rsf_vsr::VsrHistoryBatch::decode(&body[32..]).map_err(rsf)?;
                store
                    .append_chosen_input(sender, id, batch)
                    .and_then(|value| value.encode())
                    .map_err(rsf)?
            }
            IMPORT_FINISH if body.len() == 32 => store
                .finish_chosen_input(sender, id_at(body, 0)?)
                .and_then(|value| value.encode())
                .map_err(rsf)?,
            READ if body.len() == 40 || body.len() == 56 => {
                let round = if body.len() == 56 {
                    let round = VsrMembershipRound {
                        counter: u64_at(body, 40)?,
                        coordinator: NodeId(u64_at(body, 48)?),
                    };
                    if round.coordinator != sender || state.promised != Some(round) {
                        return Err(invalid("membership active read has another durable round"));
                    }
                    Some(round)
                } else {
                    None
                };
                let (_, records) = store.input(id_at(body, 0)?).map_err(rsf)?;
                let ordinal = u64_at(body, 32)?;
                let at = usize::try_from(ordinal)
                    .map_err(|_| invalid("membership input ordinal exceeds platform range"))?;
                let batch = rsf_vsr::VsrHistoryBatch::pack(
                    ordinal,
                    records.get(at..).ok_or_else(|| {
                        invalid("membership input ordinal exceeds retained prefix")
                    })?,
                )
                .map_err(rsf)?;
                if let Some(round) = round {
                    if batch.records.is_empty() {
                        return Err(invalid("membership active read has no copied records"));
                    }
                    let end = ordinal
                        .checked_add(batch.records.len() as u64)
                        .ok_or_else(|| invalid("membership read cursor overflow"))?;
                    activity = Some((round, Some((id_at(body, 0)?, end))));
                }
                batch.encode().map_err(rsf)?
            }
            _ => {
                return Err(invalid(
                    "invalid membership recovery operation length or tag",
                ));
            }
        };
        if let Some((round, copy)) = activity {
            self.note_membership_recovery_progress(pg, sender, round, copy);
        }
        response(tag, &bytes)
    }
}
