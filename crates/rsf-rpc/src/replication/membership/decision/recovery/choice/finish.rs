use super::*;

impl Group {
    pub(in super::super) fn finish_choice(
        &mut self,
        server: &mut PeerReplicaServer,
        request: PendingAction,
        kind: Kind,
        result: Result<Response, String>,
    ) -> Result<PeerMembershipRecoveryUpdate, String> {
        let PendingAction {
            pg,
            replica,
            binding,
            phase,
            ..
        } = request;
        let now = Instant::now();
        if kind == Kind::State {
            self.choice.inspected.insert(replica);
        }
        let outcome = match binding.scope {
            Scope::Plan { input, .. } => self.finish_repair(replica, input, kind, result),
            _ => {
                let store = server.membership_decisions.get_mut(&pg).unwrap();
                self.choice
                    .finish(store, replica, binding.scope, kind, result, now)
            }
        };
        let peer = self
            .peers
            .get_mut(&replica)
            .ok_or("pre-choice reply has no proposed peer")?;
        let detail = match outcome {
            Ok(()) => {
                peer.failures = 0;
                peer.next = now + self.timing.interval;
                "authenticated recovery response accepted".into()
            }
            Err(error) => {
                peer.failed(worker::Kind::Choice(kind), self.timing.interval, now);
                self.choice.failed(replica, kind);
                format!("peer recovery failed; bounded idempotent continuation scheduled: {error}")
            }
        };
        let mut update = self.update(pg, phase, detail);
        update.replica = Some(replica);
        Ok(update)
    }
    fn finish_repair(
        &mut self,
        replica: NodeId,
        id: [u8; 32],
        kind: Kind,
        result: Result<Response, String>,
    ) -> Result<(), String> {
        let peer = self.peers.get_mut(&replica).unwrap();
        let next = match (kind, result?) {
            (Kind::Source, Response::Source(VsrMembershipInputProgress::Complete { input })) => {
                if input.id().map_err(|e| e.message)? != id {
                    return Err("repair response changed input".into());
                }
                // A complete value still may fail materialization or custody;
                // never overwrite it to turn such a failure into an ACK.
                None
            }
            (Kind::Source, Response::Source(VsrMembershipInputProgress::Partial { input, .. })) => {
                if input.id().map_err(|e| e.message)? != id {
                    return Err("repair response changed input".into());
                }
                Some(Repair::Begin)
            }
            (
                Kind::Source,
                Response::Source(
                    VsrMembershipInputProgress::Missing | VsrMembershipInputProgress::Initializing,
                ),
            ) => Some(Repair::Begin),
            (
                Kind::ImportBegin { total } | Kind::ImportBatch { total, .. },
                Response::Import(reply),
            ) => match reply {
                VsrMembershipImportReply::BeginRequired => Some(Repair::Begin),
                VsrMembershipImportReply::Retained { input, records }
                    if input == id && records <= total =>
                {
                    if let Kind::ImportBatch { end, .. } = kind
                        && records < end
                    {
                        return Err("chosen copy acknowledgement shortened its exact prefix".into());
                    }
                    Some(if records == total {
                        Repair::Finish
                    } else {
                        Repair::Records(records)
                    })
                }
                VsrMembershipImportReply::Chosen { input } if input == id => None,
                _ => return Err("chosen copy returned a different identity or cursor".into()),
            },
            (Kind::ImportFinish, Response::Import(VsrMembershipImportReply::Chosen { input }))
                if input == id =>
            {
                None
            }
            (Kind::ImportFinish, Response::Import(VsrMembershipImportReply::BeginRequired)) => {
                Some(Repair::Begin)
            }
            _ => return Err("recovery worker returned another repair action's response".into()),
        };
        peer.repair = next;
        peer.require_stage = true;
        Ok(())
    }
}
impl Choice {
    fn failed(&mut self, replica: NodeId, kind: Kind) {
        if let Some(download) = &mut self.download
            && download.source == Some(replica)
        {
            download.source = None;
        }
        if let Some(ballot) = &mut self.ballot
            && matches!(
                kind,
                Kind::RoundBegin { .. } | Kind::RoundBatch { .. } | Kind::Accept
            )
        {
            ballot.uploads.remove(&replica);
        }
        if matches!(kind, Kind::Chosen) {
            // Failed evidence retrieval is not a chosen proof. Keep the actual
            // accepted/promise floor while permitting a healthy quorum to act.
            if let Some(state) = self.observations.get_mut(&replica) {
                state.chosen = None;
            }
        }
    }
    pub(super) fn finish(
        &mut self,
        store: &mut MembershipDecisionStore,
        replica: NodeId,
        scope: Scope,
        kind: Kind,
        result: Result<Response, String>,
        now: Instant,
    ) -> Result<(), String> {
        let context = context(store)?;
        let local = store.decision_state().map_err(|e| e.message)?.replica;
        let reply = result?;
        match (kind, reply) {
            (Kind::State, Response::State(state)) => {
                validate_state(&state, replica, &context)?;
                self.observe(state, now);
            }
            (Kind::Promise, Response::Promise(VsrMembershipPromiseReply::Preempted(state))) => {
                validate_state(&state, replica, &context)?;
                let Scope::Round { round, .. } = scope else {
                    return Err("promise reply has no scheduled round".into());
                };
                if state.promised.is_none_or(|p| p <= round) {
                    return Err("structured preemption did not advance its durable promise".into());
                }
                self.observe(state, now);
            }
            (Kind::Promise, Response::Promise(VsrMembershipPromiseReply::Granted(promise))) => {
                let ballot = self
                    .ballot
                    .as_mut()
                    .ok_or("promise lost scheduled ballot")?;
                promise.encode().map_err(|e| e.message)?;
                if promise.replica != replica
                    || promise.proposal != *context.proposal
                    || promise.round != ballot.round
                {
                    return Err("promise changed reporter, proposal or scheduled round".into());
                }
                // Frozen proof is immutable. Extra promises permit acceptance
                // by additional live voters without selecting a different value.
                ballot.promises.insert(replica, promise);
            }
            (Kind::Chosen, Response::Chosen(Some(chosen))) => {
                chosen.encode().map_err(|e| e.message)?;
                let expected = self
                    .observations
                    .get(&replica)
                    .and_then(|s| s.chosen)
                    .map(|v| v.1);
                if chosen.proposal() != &*context.proposal || Some(chosen.input_id()) != expected {
                    return Err("chosen query changed its authenticated canonical input".into());
                }
                self.select_chosen(chosen)?;
            }
            (Kind::Chosen, Response::Chosen(None)) => {
                if let Some(state) = self.observations.get_mut(&replica) {
                    state.chosen = None;
                }
            }
            (Kind::Source, Response::Source(progress)) => {
                let download = self
                    .download
                    .as_mut()
                    .ok_or("input source has no scheduled copy")?;
                if let VsrMembershipInputProgress::Complete { input }
                | VsrMembershipInputProgress::Partial { input, .. } = &progress
                    && (input.id().map_err(|e| e.message)? != download.id
                        || input.proposal() != &*context.proposal)
                {
                    return Err("source descriptor changed canonical membership input".into());
                }
                if let VsrMembershipInputProgress::Complete { input } = progress {
                    if download.input.as_ref().is_some_and(|old| **old != input) {
                        return Err("source descriptor differs from retained input".into());
                    }
                    if download.input.is_none() {
                        download.input = Some(Arc::new(input));
                    }
                    if download.source.is_none() {
                        download.source = Some(replica);
                    }
                }
            }
            (Kind::Read { ordinal, total }, Response::Records(batch)) => {
                let download = self
                    .download
                    .as_mut()
                    .ok_or("input records have no scheduled copy")?;
                let end = ordinal
                    .checked_add(batch.records.len() as u64)
                    .ok_or("membership download ordinal overflow")?;
                if download.source != Some(replica)
                    || download.records != ordinal
                    || batch.ordinal != ordinal
                    || batch.records.is_empty()
                    || end > total
                {
                    return Err("membership source returned another exact-prefix range".into());
                }
                let records = match scope {
                    Scope::Round {
                        round,
                        input: Some(id),
                    } if id == download.id => store
                        .append_input(local, round, id, batch)
                        .map_err(|e| e.message)?,
                    Scope::Chosen { input } if input == download.id => match store
                        .append_chosen_input(local, input, batch)
                        .map_err(|e| e.message)?
                    {
                        VsrMembershipImportReply::Retained { input: id, records }
                            if id == input =>
                        {
                            records
                        }
                        VsrMembershipImportReply::Chosen { input: id } if id == input => total,
                        VsrMembershipImportReply::BeginRequired => {
                            download.begun = false;
                            return Ok(());
                        }
                        _ => return Err("local chosen import returned another identity".into()),
                    },
                    _ => {
                        return Err(
                            "download completion lost its exact round or chosen binding".into()
                        );
                    }
                };
                if records < end || records > total {
                    return Err("local input append returned an invalid retained cursor".into());
                }
                download.records = records;
            }
            (Kind::RoundBegin { total }, Response::Offset(records))
            | (Kind::RoundBatch { total, .. }, Response::Offset(records)) => {
                if records > total {
                    return Err("round upload cursor exceeds selected input".into());
                }
                if let Kind::RoundBatch { end, .. } = kind
                    && records < end
                {
                    return Err("round upload shortened exact-prefix acknowledgement".into());
                }
                self.ballot
                    .as_mut()
                    .ok_or("round upload lost ballot")?
                    .uploads
                    .insert(replica, Some(records));
            }
            (Kind::Accept, Response::Accepted(accepted)) => {
                let ballot = self
                    .ballot
                    .as_mut()
                    .ok_or("acceptance lost scheduled ballot")?;
                accepted.encode().map_err(|e| e.message)?;
                if accepted.replica != replica
                    || accepted.round != ballot.round
                    || accepted.proposal != *context.proposal
                    || Some(accepted.input) != ballot.selected
                {
                    return Err(
                        "acceptance changed authenticated reporter or canonical ballot input"
                            .into(),
                    );
                }
                ballot.accepted.insert(replica, accepted);
            }
            _ => {
                return Err("recovery worker returned another pre-choice action's response".into());
            }
        }
        let state = store.decision_state().map_err(|e| e.message)?;
        self.discard_preempted(&state)?;
        Ok(())
    }
}
