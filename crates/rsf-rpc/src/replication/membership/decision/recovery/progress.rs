use super::*;
use rsf_vsr::{HISTORY_BATCH_TARGET_BYTES, MAX_HISTORY_BATCH_RECORDS, VsrHistoryBatch};

impl Group {
    pub(super) fn step(
        &mut self,
        server: &mut PeerReplicaServer,
        pg: PlacementGroupId,
        apply: &mut impl FnMut(&VsrCommittedEntry) -> Result<(), String>,
    ) -> Result<(PeerMembershipRecoveryUpdate, Option<(NodeId, Action)>), String> {
        let store = server
            .membership_decisions
            .get(&pg)
            .ok_or_else(|| "managed membership decision disappeared".to_string())?;
        if Some(store.proposal()) != server.membership_recovery_bindings.get(&pg) {
            return Err("managed membership journal changed its bound proposal".into());
        }
        let Some(chosen) = store.chosen().cloned() else {
            return self.prechoice_step(server, pg);
        };
        if self.input.is_some_and(|input| input != chosen.input_id()) {
            self.reset_derived();
            return Err(
                "retained membership choice changed; stale recovery progress was discarded".into(),
            );
        }
        self.choice = choice::Choice::default();
        self.input = Some(chosen.input_id());
        if self.local_stage.is_none() {
            let report = server.stage_membership_locally(chosen, apply)?;
            self.plan = Some(report.plan);
            self.stages.insert(server.local, report.clone());
            self.local_stage = Some(report);
            let stage = &server.membership_staging[&pg];
            if let Some(quorum) = stage.publication_quorum().map_err(|e| e.message)? {
                self.quorum = Some(Arc::new(quorum));
            } else if let Some(cell) = server.cells.get(&pg)
                && let Some(proof) = cell
                    .membership_publication(stage.plan().id())
                    .map_err(|e| e.message)?
            {
                // This is retained historical work input, never an application ACK.
                self.quorum = Some(Arc::new(proof.staged_quorum().clone()));
            }
            let now = Instant::now();
            for replica in &stage.plan().input().proposal().new_voters {
                if *replica != server.local {
                    self.peers.entry(*replica).or_insert_with(|| Peer::new(now));
                }
            }
            return Ok((
                self.update(
                    pg,
                    "staged-locally",
                    "chosen prefix durably materialized".into(),
                ),
                None,
            ));
        }
        self.validate_local_identity(server, pg)?;
        if self.quorum.is_none() && self.stages.len() > chosen.proposal().new_voters.len() / 2 {
            self.quorum = Some(Arc::new(
                VsrMembershipStagedQuorum::from_reports(self.stages.values().cloned().collect())
                    .map_err(|e| e.message)?,
            ));
            return Ok((
                self.update(
                    pg,
                    "staging-quorum",
                    "authenticated staging majority collected".into(),
                ),
                None,
            ));
        }
        if let Some(quorum) = &self.quorum
            && self.local_publication.is_none()
        {
            let report = server.publish_membership_locally(quorum, apply)?;
            self.publications.insert(server.local, report.clone());
            self.local_publication = Some(report);
            let mut update = self.update(
                pg,
                "published-locally",
                "local publication is durable and current application prefix is materialized"
                    .into(),
            );
            self.add_majority(&mut update)?;
            return Ok((update, None));
        }
        let Some(replica) = self.due_peer(Instant::now()) else {
            return Ok((
                self.update(
                    pg,
                    "waiting",
                    "all peer recovery attempts are in backoff".into(),
                ),
                None,
            ));
        };
        let peer = &self.peers[&replica];
        let action = if let Some(repair) = peer.repair {
            self.repair_action(server, pg, repair)?
        } else if let Some(upload) = peer.upload {
            self.upload_action(server, pg, upload)?
        } else if let Some(quorum) = &self.quorum
            && self.local_publication.is_some()
            && !peer.require_stage
        {
            if peer.query_next {
                Action::Query {
                    pg,
                    plan: quorum.plan_id(),
                }
            } else {
                Action::Publish(Arc::clone(quorum))
            }
        } else {
            Action::Stage {
                chosen,
                expected: self.local_stage.as_ref().unwrap().clone(),
            }
        };
        let mut update = self.update(
            pg,
            action.phase(),
            "bounded authenticated peer exchange dispatched".into(),
        );
        update.replica = Some(replica);
        Ok((update, Some((replica, action))))
    }

    pub(super) fn validate_local_identity(
        &self,
        server: &PeerReplicaServer,
        pg: PlacementGroupId,
    ) -> Result<(), String> {
        let stage = server
            .membership_staging
            .get(&pg)
            .ok_or_else(|| "managed membership lost its owned stage".to_string())?;
        if Some(stage.plan().id()) != self.plan
            || Some(stage.plan().chosen().input_id()) != self.input
            || Some(stage.plan().input().proposal()) != server.membership_recovery_bindings.get(&pg)
        {
            return Err(
                "managed membership stage differs from scheduled canonical identity".into(),
            );
        }
        if let Some(cell) = server.cells.get(&pg) {
            // Every outbound operation starts from healthy active custody even
            // after ordinary view/promise changes. A token need not stay equal.
            cell.recovery_state_token().map_err(|e| e.message)?;
        } else {
            server
                .learners
                .get(&pg)
                .ok_or_else(|| "managed membership lost its hosted anchor".to_string())?
                .validate_membership_stage(stage)
                .map_err(|e| e.message)?;
        }
        Ok(())
    }

    fn upload_action(
        &self,
        server: &PeerReplicaServer,
        pg: PlacementGroupId,
        upload: Upload,
    ) -> Result<Action, String> {
        let plan = server.membership_staging[&pg].plan();
        let input = plan.input();
        let id = input.id().map_err(|e| e.message)?;
        let total = input.records().map_err(|e| e.message)?;
        Ok(match upload {
            Upload::Begin => Action::Upload {
                kind: worker::Kind::Begin { total },
                payload: input.encode().map_err(|e| e.message)?,
            },
            Upload::Records(ordinal) => {
                let batch = approval_batch(input, plan.approval(), ordinal)?;
                let next = ordinal
                    .checked_add(batch.records.len() as u64)
                    .ok_or_else(|| "authority recovery ordinal overflow".to_string())?;
                if next > total {
                    return Err("authority recovery batch exceeds chosen input".into());
                }
                let mut payload = input_query(pg, id);
                payload.extend_from_slice(&batch.encode().map_err(|e| e.message)?);
                Action::Upload {
                    kind: worker::Kind::Batch { next, total },
                    payload,
                }
            }
            Upload::Finish => Action::Upload {
                kind: worker::Kind::Finish,
                payload: input_query(pg, id),
            },
        })
    }

    fn add_majority(&mut self, update: &mut PeerMembershipRecoveryUpdate) -> Result<(), String> {
        if !self.reported_majority
            && let Some(quorum) = &self.quorum
            && self.publications.len() > quorum.proposal().new_voters.len() / 2
        {
            let quorum = VsrMembershipPublishedQuorum::from_reports(
                self.publications.values().cloned().collect(),
            )
            .map_err(|e| e.message)?;
            self.reported_majority = true;
            update.publication_quorum = Some(quorum);
            update.detail =
                "historical publication majority recovered; client admission remains closed".into();
        }
        Ok(())
    }

    fn reset_derived(&mut self) {
        self.input = None;
        self.plan = None;
        self.local_stage = None;
        self.stages.clear();
        self.quorum = None;
        self.local_publication = None;
        self.publications.clear();
        self.reported_majority = false;
        self.peers.clear();
        self.last_peer = None;
        self.choice = choice::Choice::default();
    }
}

impl Recovery {
    pub(super) fn finish(
        &mut self,
        server: &mut PeerReplicaServer,
        request: PendingAction,
        result: Result<Reply, String>,
    ) -> Result<PeerMembershipRecoveryUpdate, String> {
        let PendingAction {
            pg,
            binding,
            replica,
            phase,
            action,
        } = request;
        let group = self
            .groups
            .get_mut(&pg)
            .ok_or_else(|| "recovery reply lost its group".to_string())?;
        match group.matches_binding(server, pg, binding) {
            Ok(true) => {}
            Ok(false) => {
                if matches!(binding.scope, Scope::Plan { .. }) {
                    group.reset_derived();
                }
                return Ok(group.update(
                    pg,
                    "stale-reply",
                    "recovery context changed or lost healthy custody; pending response discarded"
                        .into(),
                ));
            }
            Err(error) => {
                return Ok(group.update(
                    pg,
                    "local-error",
                    format!(
                        "recovery reply discarded because local custody failed validation: {error}"
                    ),
                ));
            }
        }
        if let worker::Kind::Choice(kind) = action {
            let result = result.and_then(|reply| match reply {
                Reply::Choice(value) => Ok(value),
                _ => Err("recovery worker returned another action's response".into()),
            });
            return group.finish_choice(server, request, kind, result);
        }
        let Scope::Plan { input, plan } = binding.scope else {
            return Err("legacy recovery reply has no canonical staged plan".into());
        };
        let peer = group
            .peers
            .get_mut(&replica)
            .ok_or_else(|| "recovery reply reporter is not a proposed remote voter".to_string())?;
        let now = Instant::now();
        let result = result.and_then(|reply| {
            match (&action, &reply) {
                (worker::Kind::Begin { total }, Reply::Offset(offset))
                | (worker::Kind::Batch { total, .. }, Reply::Offset(offset))
                    if offset <= total => {}
                (worker::Kind::Finish, Reply::Finished) => {}
                (worker::Kind::Stage, Reply::Stage(report)) => {
                    if report.replica != replica
                        || report.input != input
                        || report.plan != plan
                        || Some(&report.proposal) != server.membership_recovery_bindings.get(&pg)
                    {
                        return Err(
                            "staged response changed its canonical recovery identity".into()
                        );
                    }
                }
                (worker::Kind::Publish | worker::Kind::Query, Reply::Publication(report)) => {
                    super::super::publication::check_published(
                        report,
                        replica,
                        group.quorum.as_ref().unwrap(),
                    )?;
                }
                _ => return Err("recovery worker returned another action's response".into()),
            }
            Ok(reply)
        });
        let detail = match result {
            Ok(reply) => {
                peer.failures = 0;
                peer.next = now + group.timing.interval;
                match reply {
                    Reply::Choice(_) => unreachable!(),
                    Reply::Offset(offset) => {
                        let total = match action {
                            worker::Kind::Begin { total } | worker::Kind::Batch { total, .. } => {
                                total
                            }
                            _ => unreachable!(),
                        };
                        peer.upload = Some(if offset == total {
                            Upload::Finish
                        } else {
                            Upload::Records(offset)
                        });
                    }
                    Reply::Finished => {
                        peer.upload = None;
                        peer.require_stage = true;
                        peer.provisioned = true;
                    }
                    Reply::Stage(report) => {
                        peer.upload = None;
                        peer.require_stage = false;
                        group.stages.insert(replica, report);
                    }
                    Reply::Publication(report) => {
                        peer.upload = None;
                        peer.require_stage = false;
                        peer.query_next = true;
                        group.publications.insert(replica, report);
                    }
                }
                "authenticated recovery response accepted".into()
            }
            Err(error) => {
                peer.failed(action, group.timing.interval, now);
                format!("peer recovery failed; bounded idempotent continuation scheduled: {error}")
            }
        };
        let mut update = group.update(pg, phase, detail);
        update.replica = Some(replica);
        group.add_majority(&mut update)?;
        Ok(update)
    }
}

/// Copy only one bounded batch from the retained plan. Never flatten all old
/// quorum histories into a fresh Vec for each peer or each scheduler tick.
pub(super) fn approval_batch(
    input: &VsrMembershipInput,
    approval: &VsrMembershipApproval,
    ordinal: u64,
) -> Result<VsrHistoryBatch, String> {
    let total = input.records().map_err(|e| e.message)?;
    if ordinal >= total {
        return Err("authority batch ordinal is outside chosen input".into());
    }
    let mut skip = ordinal;
    let mut records = Vec::new();
    let mut bytes = 17usize;
    'reports: for report in input.reports() {
        let snapshot = approval
            .snapshots()
            .iter()
            .find(|snapshot| snapshot.report.voter.replica == report.voter.replica)
            .ok_or_else(|| "chosen authority snapshot is missing".to_string())?;
        if skip >= snapshot.records.len() as u64 {
            skip -= snapshot.records.len() as u64;
            continue;
        }
        for record in &snapshot.records[skip as usize..] {
            let length = record.encode().map_err(|e| e.message)?.len();
            let next = bytes
                .checked_add(4)
                .and_then(|n| n.checked_add(length))
                .ok_or_else(|| "authority batch size overflow".to_string())?;
            if !records.is_empty() && next > HISTORY_BATCH_TARGET_BYTES {
                break 'reports;
            }
            records.push(record.clone());
            bytes = next;
            if records.len() == MAX_HISTORY_BATCH_RECORDS {
                break 'reports;
            }
        }
        skip = 0;
    }
    VsrHistoryBatch::pack(ordinal, &records).map_err(|e| e.message)
}
