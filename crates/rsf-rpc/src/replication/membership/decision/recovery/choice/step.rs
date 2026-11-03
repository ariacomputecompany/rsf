use super::*;

impl Group {
    pub(in super::super) fn prechoice_step(
        &mut self,
        server: &mut PeerReplicaServer,
        pg: PlacementGroupId,
    ) -> Result<(PeerMembershipRecoveryUpdate, Option<(NodeId, Action)>), String> {
        self.validate_prechoice_anchor(server, pg)?;
        let local = server.local;
        let proposal = server.membership_decisions[&pg].proposal().clone();
        let now = Instant::now();
        self.ensure_peers(&proposal, local, now);
        let store = server.membership_decisions.get_mut(&pg).unwrap();
        if let Some(detail) = self
            .choice
            .local_step(store, local, self.started, self.grace, now)?
        {
            return Ok((self.update(pg, "recovering-choice", detail), None));
        }
        let Some(replica) = self.due_peer(now) else {
            return Ok((
                self.update(
                    pg,
                    "waiting",
                    "all peer recovery attempts are in backoff".into(),
                ),
                None,
            ));
        };
        let peer = self.peers.get_mut(&replica).unwrap();
        let operation = if peer.refresh {
            peer.refresh = false;
            Operation::State
        } else {
            self.choice.operation(store, replica)?
        };
        let action = request(store, self.choice.scope(), operation)?;
        let update = self.update(
            pg,
            action.phase(),
            "bounded authenticated peer exchange dispatched".into(),
        );
        Ok((update, Some((replica, action))))
    }

    pub(in super::super) fn repair_action(
        &self,
        server: &PeerReplicaServer,
        pg: PlacementGroupId,
        repair: Repair,
    ) -> Result<Action, String> {
        let stage = server.membership_staging[&pg].plan();
        let store = &server.membership_decisions[&pg];
        let input = Arc::new(stage.input().clone());
        let operation = match repair {
            Repair::Inspect => Operation::Source(stage.chosen().input_id()),
            Repair::Begin => Operation::ImportBegin {
                chosen: Arc::new(stage.chosen().clone()),
                input,
            },
            Repair::Records(ordinal) => Operation::ImportBatch {
                batch: progress::approval_batch(&input, stage.approval(), ordinal)?,
                input,
            },
            Repair::Finish => Operation::ImportFinish(input),
        };
        request(
            store,
            Scope::Plan {
                input: stage.chosen().input_id(),
                plan: stage.id(),
            },
            operation,
        )
    }
}

impl Choice {
    pub(super) fn local_step(
        &mut self,
        store: &mut MembershipDecisionStore,
        local: NodeId,
        started: Instant,
        grace: Duration,
        now: Instant,
    ) -> Result<Option<String>, String> {
        let state = store.decision_state().map_err(|e| e.message)?;
        self.observe(state.clone(), now);
        self.discard_preempted(&state)?;
        if self.chosen.is_none()
            && let Some(chosen) = self.accepted_choice(store.proposal())?
        {
            self.select_chosen(chosen)?;
            return Ok(Some(
                "authenticated same-round accepted majority reconstructed".into(),
            ));
        }
        if self.chosen.is_some() {
            return self.download_step(store, local);
        }
        if let Some(ballot) = &mut self.ballot {
            if ballot.frozen.is_none()
                && ballot.promises.len() > store.proposal().new_voters.len() / 2
            {
                let frozen = ballot.promises.values().cloned().collect::<Vec<_>>();
                let id = highest(&frozen, state.authority)?;
                ballot.frozen = Some(frozen);
                ballot.selected = Some(id);
                self.download = Some(Download {
                    id,
                    input: None,
                    begun: false,
                    records: 0,
                    source: None,
                });
                return Ok(Some(
                    "phase-one majority frozen with its highest accepted input".into(),
                ));
            }
            if ballot.nomination.is_none() && ballot.frozen.is_some() {
                return self.download_step(store, local);
            }
            if ballot.accepted.len() > store.proposal().new_voters.len() / 2 {
                let chosen =
                    VsrMembershipChosen::from_accepted(ballot.accepted.values().cloned().collect())
                        .map_err(|e| e.message)?;
                store.learn_chosen(chosen).map_err(|e| e.message)?;
                return Ok(Some("new-round acceptance majority durably learned".into()));
            }
            return Ok(None);
        }
        // Query any advertised surviving proof before starting a new ballot.
        // A later failed query falls back to structured phase one; a tuple alone
        // never certifies a choice or guarantees complete input custody.
        if self.observations.values().any(|s| s.chosen.is_some()) {
            return Ok(None);
        }
        if store
            .proposal()
            .new_voters
            .iter()
            .any(|n| *n != local && !self.inspected.contains(n))
        {
            return Ok(None);
        }
        if self
            .detector
            .wait_for(local, store.proposal(), started, grace, now)?
        {
            return Ok(None);
        }
        // A locally damaged bound input must not cause an endless stream of
        // higher promises from a coordinator that can never finish its work.
        store.approval(state.authority).map_err(|e| e.message)?;
        if let Some((_, id)) = state.accepted {
            store.approval(id).map_err(|e| e.message)?;
        }
        let floor = self.current_floor();
        let round = match state.promised {
            Some(round) if round.coordinator == local && floor.is_none_or(|f| f <= round) => round,
            _ => {
                VsrMembershipRound::after(store.proposal(), local, floor).map_err(|e| e.message)?
            }
        };
        let promise = store.promise(local, round).map_err(|e| e.message)?;
        self.ballot = Some(Ballot {
            round,
            promises: BTreeMap::from([(local, promise)]),
            frozen: None,
            selected: None,
            decision: None,
            nomination: None,
            accepted: BTreeMap::new(),
            uploads: BTreeMap::new(),
        });
        Ok(Some(
            "own recovery round durably promised before any outbound request".into(),
        ))
    }

    fn download_step(
        &mut self,
        store: &mut MembershipDecisionStore,
        local: NodeId,
    ) -> Result<Option<String>, String> {
        let download = self
            .download
            .as_mut()
            .ok_or_else(|| "selected membership input has no copy context".to_string())?;
        if download.input.is_none() {
            match store.input_progress(download.id).map_err(|e| e.message)? {
                VsrMembershipInputProgress::Complete { input } => {
                    download.records = input.records().map_err(|e| e.message)?;
                    download.input = Some(Arc::new(input));
                }
                VsrMembershipInputProgress::Partial { input, records } => {
                    download.input = Some(Arc::new(input));
                    download.records = records;
                }
                VsrMembershipInputProgress::Missing | VsrMembershipInputProgress::Initializing => {
                    return Ok(None);
                }
            }
        }
        let input = download.input.as_ref().unwrap();
        let total = input.records().map_err(|e| e.message)?;
        if !download.begun {
            download.records = if let Some(chosen) = &self.chosen {
                match store
                    .begin_chosen_input(local, (**chosen).clone(), (**input).clone())
                    .map_err(|e| e.message)?
                {
                    VsrMembershipImportReply::Retained { records, .. } => records,
                    VsrMembershipImportReply::Chosen { .. } => total,
                    VsrMembershipImportReply::BeginRequired => {
                        return Err("local chosen BEGIN requested another BEGIN".into());
                    }
                }
            } else {
                store
                    .begin_input(
                        local,
                        self.ballot.as_ref().unwrap().round,
                        (**input).clone(),
                    )
                    .map_err(|e| e.message)?
            };
            download.begun = true;
            return Ok(Some(
                "exact local input copy context durably retained".into(),
            ));
        }
        if download.records < total {
            return Ok(None);
        }
        if self.chosen.is_some() {
            match store
                .finish_chosen_input(local, download.id)
                .map_err(|e| e.message)?
            {
                VsrMembershipImportReply::Chosen { input } if input == download.id => {}
                _ => return Err("local chosen FINISH did not retain the certified input".into()),
            }
            return Ok(Some(
                "recovered chosen input replayed and durably learned".into(),
            ));
        }
        let ballot = self.ballot.as_mut().unwrap();
        let decision = Arc::new(
            VsrMembershipDecision::from_promises(
                ballot.frozen.as_ref().unwrap().clone(),
                (**input).clone(),
            )
            .map_err(|e| e.message)?,
        );
        let nomination = store.nominate(&decision).map_err(|e| e.message)?;
        ballot.accepted.insert(local, nomination.clone());
        ballot.nomination = Some(nomination);
        ballot.decision = Some(decision);
        Ok(Some(
            "highest accepted input durably nominated before remote acceptance".into(),
        ))
    }

    pub(super) fn operation(
        &self,
        store: &MembershipDecisionStore,
        replica: NodeId,
    ) -> Result<Operation, String> {
        if self.chosen.is_none()
            && let Some((_, id)) = self.observations.get(&replica).and_then(|s| s.chosen)
        {
            return Ok(Operation::Chosen(id));
        }
        if let Some(download) = &self.download {
            let unfinished = self.chosen.is_some()
                || self.ballot.as_ref().is_some_and(|b| b.nomination.is_none());
            if unfinished {
                if download.source == Some(replica)
                    && download.begun
                    && let Some(input) = &download.input
                    && download.records < input.records().map_err(|e| e.message)?
                {
                    let round = self.ballot.as_ref().map(|b| b.round);
                    if let Some(ballot) = &self.ballot
                        && !ballot.promises.contains_key(&replica)
                    {
                        return Ok(Operation::Promise(ballot.round));
                    }
                    return Ok(Operation::Read {
                        input: Arc::clone(input),
                        ordinal: download.records,
                        round,
                    });
                }
                return Ok(Operation::Source(download.id));
            }
        }
        if let Some(ballot) = &self.ballot {
            if !ballot.promises.contains_key(&replica) {
                return Ok(Operation::Promise(ballot.round));
            }
            if let (Some(decision), Some(nomination)) = (&ballot.decision, &ballot.nomination) {
                if ballot.accepted.contains_key(&replica) {
                    return Ok(Operation::State);
                }
                let input = Arc::new(decision.input().clone());
                return match ballot.uploads.get(&replica).copied().flatten() {
                    None => Ok(Operation::RoundBegin {
                        round: ballot.round,
                        input,
                    }),
                    Some(ordinal) if ordinal < input.records().map_err(|e| e.message)? => {
                        let (_, records) = store
                            .input(input.id().map_err(|e| e.message)?)
                            .map_err(|e| e.message)?;
                        let at = usize::try_from(ordinal)
                            .map_err(|_| "membership copy offset exceeds platform range")?;
                        let batch = VsrHistoryBatch::pack(
                            ordinal,
                            records
                                .get(at..)
                                .ok_or("membership copy offset exceeds retained input")?,
                        )
                        .map_err(|e| e.message)?;
                        Ok(Operation::RoundBatch {
                            round: ballot.round,
                            input,
                            batch,
                        })
                    }
                    Some(_) => Ok(Operation::Accept {
                        decision: Arc::clone(decision),
                        nomination: nomination.clone(),
                    }),
                };
            }
        }
        Ok(Operation::State)
    }
}
