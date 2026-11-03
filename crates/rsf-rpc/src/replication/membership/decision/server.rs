use super::*;
use rsf_vsr::MembershipStagingStore;
use std::path::{Path, PathBuf};

impl PeerReplicaServer {
    /// Configure owned transition paths only for already hosted voters/learners.
    /// Incoming RPC contents cannot select paths or create an application cell.
    pub fn with_membership_paths(
        self,
        paths: BTreeMap<PlacementGroupId, PathBuf>,
    ) -> Result<Self, String> {
        self.open_membership_paths(paths, false)
    }

    /// Reopen already provisioned transition custody without creating an inbox
    /// identity or resuming an interrupted initial decision factory.
    pub fn with_existing_membership_paths(
        self,
        paths: BTreeMap<PlacementGroupId, PathBuf>,
    ) -> Result<Self, String> {
        self.open_membership_paths(paths, true)
    }

    fn open_membership_paths(
        mut self,
        paths: BTreeMap<PlacementGroupId, PathBuf>,
        existing: bool,
    ) -> Result<Self, String> {
        if !self.membership_paths.is_empty() {
            return Err("membership paths already configured".into());
        }
        for (pg, path) in paths {
            if !self.cells.contains_key(&pg) && !self.learners.contains_key(&pg) {
                return Err("membership path requires a hosted authority anchor".into());
            }
            let inbox = if existing {
                MembershipAuthorityInbox::open_existing(path.join("inbox"), pg, self.local)
            } else {
                MembershipAuthorityInbox::open(path.join("inbox"), pg, self.local)
            }
            .map_err(|e| e.message)?;
            let staging_path = path.join("staging");
            let sealed = ["META", "INSTALLED", "PUBLICATION", ".PUBLICATION.tmp"]
                .into_iter()
                .map(|name| transition_path_present(&staging_path.join(name)))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .any(|present| present);
            let stage = if sealed {
                Some(
                    MembershipStagingStore::open_existing(&staging_path, pg, self.local)
                        .map_err(|e| e.message)?,
                )
            } else {
                None
            };
            let decision_path = path.join("decision");
            if transition_path_present(&decision_path)? {
                let store =
                    match MembershipDecisionStore::open_existing(&decision_path, pg, self.local) {
                        Ok(store) => store,
                        Err(original) if existing => return Err(original.message),
                        Err(original) => {
                            // The factory never resets a nonempty journal. A complete
                            // inbox may resume an interrupted initial authority copy.
                            // This remains the strict pre-publication path.
                            let input = inbox
                                .selected_input()
                                .ok_or_else(|| original.message.clone())?;
                            let approval = inbox
                                .finish(self.local, input.id().map_err(|e| e.message)?)
                                .map_err(|e| e.message)?;
                            self.validate_decision_authority(pg, &approval)?;
                            MembershipDecisionStore::open(&decision_path, self.local, &approval)
                                .map_err(|e| format!("{}; {}", original.message, e.message))?
                        }
                    };
                if !self.published_decision_anchor(pg, &store, stage.as_ref())? {
                    self.validate_decision_authority(
                        pg,
                        &store.authority().map_err(|e| e.message)?,
                    )?;
                    if let Some((_, id)) = store.accepted() {
                        self.validate_decision_authority(
                            pg,
                            &store.approval(id).map_err(|e| e.message)?,
                        )?;
                    }
                    if let Some(stage) = &stage {
                        if let Some(cell) = self.cells.get(&pg) {
                            stage.validate_anchor(cell).map_err(|e| e.message)?;
                        } else {
                            self.learners[&pg]
                                .validate_membership_stage(stage)
                                .map_err(|e| e.message)?;
                        }
                    }
                }
                self.membership_decisions.insert(pg, store);
            } else if existing {
                return Err("membership recovery requires an existing decision journal".into());
            } else if stage.is_some() {
                return Err("retained membership stage has no decision journal".into());
            }
            if let Some(stage) = stage {
                self.membership_staging.insert(pg, stage);
            }
            self.membership_inputs.insert(pg, inbox);
            self.membership_paths.insert(pg, path);
        }
        Ok(self)
    }

    pub fn initialize_membership_decision(
        &mut self,
        approval: &VsrMembershipApproval,
    ) -> Result<(), String> {
        let pg = approval.proposal().election.pg;
        let path = self
            .membership_paths
            .get(&pg)
            .ok_or_else(|| "membership group has no configured transition path".to_string())?;
        if let Some(existing) = self.membership_decisions.get(&pg) {
            if existing.proposal() != approval.proposal() {
                return Err("membership decision already binds another proposal".into());
            }
            if self.published_decision_anchor(pg, existing, self.membership_staging.get(&pg))? {
                let stage = &self.membership_staging[&pg];
                if approval.snapshots()[0].records.first()
                    != stage.plan().approval().snapshots()[0].records.first()
                {
                    return Err(
                        "membership authority differs from retained decision bootstrap".into(),
                    );
                }
            } else {
                self.validate_decision_authority(pg, approval)?;
            }
            return Ok(());
        }
        // An ancestral proof never creates a new acceptor or replaces a journal.
        self.validate_decision_authority(pg, approval)?;
        let store = MembershipDecisionStore::open(path.join("decision"), self.local, approval)
            .map_err(|e| e.message)?;
        self.membership_decisions.insert(pg, store);
        Ok(())
    }

    // The journal constructor validates A (initial authority), accepted input,
    // and chosen B against one immutable proposal/bootstrap. State replay also
    // requires accepted B to match chosen B. Therefore ancestry of this exact
    // retained B validates the SAME journal; A need not have been published.
    fn published_decision_anchor(
        &self,
        pg: PlacementGroupId,
        store: &MembershipDecisionStore,
        stage: Option<&MembershipStagingStore>,
    ) -> Result<bool, String> {
        self.validate_decision_nodes(store.proposal())?;
        let Some(stage) = stage else {
            return Ok(false);
        };
        let chosen = store
            .chosen()
            .ok_or_else(|| "retained membership stage has no chosen decision".to_string())?;
        if stage.plan().input().proposal() != store.proposal()
            || stage.plan().chosen().input_id() != chosen.input_id()
        {
            return Err(
                "retained membership stage differs from the decision journal's choice".into(),
            );
        }
        let Some(cell) = self.cells.get(&pg) else {
            return Ok(false);
        };
        if cell
            .membership_publication(stage.plan().id())
            .map_err(|e| e.message)?
            .is_none()
        {
            return Ok(false);
        }
        stage
            .validate_published_anchor(cell)
            .map_err(|e| e.message)?;
        Ok(true)
    }

    pub fn with_membership_decision<R>(
        &mut self,
        pg: PlacementGroupId,
        operation: impl FnOnce(&mut MembershipDecisionStore, NetworkVsrTransport) -> R,
    ) -> Result<R, String> {
        let store = self
            .membership_decisions
            .get_mut(&pg)
            .ok_or_else(|| "membership decision is not initialized".to_string())?;
        Ok(operation(store, self.control_transport.clone()))
    }
    fn validate_decision_authority(
        &self,
        pg: PlacementGroupId,
        approval: &VsrMembershipApproval,
    ) -> Result<(), String> {
        self.validate_decision_nodes(approval.proposal())?;
        if let Some(cell) = self.cells.get(&pg) {
            cell.validate_membership_authority(approval)
                .map_err(|e| e.message)
        } else {
            self.learners
                .get(&pg)
                .ok_or_else(|| "membership group has no hosted authority anchor".to_string())?
                .validate_membership_authority(approval)
                .map_err(|e| e.message)
        }
    }
    fn validate_decision_nodes(&self, proposal: &VsrMembershipProposal) -> Result<(), String> {
        if !proposal.new_voters.contains(&self.local)
            || proposal
                .election
                .voters
                .iter()
                .chain(&proposal.new_voters)
                .any(|node| !self.nodes.values().any(|known| known == node))
        {
            return Err("membership transition requires a proposed local voter and mapped identities for every old/new voter".into());
        }
        Ok(())
    }
    pub(in crate::replication) fn dispatch_membership_decision(
        &mut self,
        sender: NodeId,
        kind: u8,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        let rsf = |e: rsf_core::RsfError| invalid(e.message);
        if sender == self.local {
            return Err(invalid(
                "membership decision RPC requires a remote authenticated node",
            ));
        }
        if kind == MEMBERSHIP_AUTH_BEGIN {
            let input = VsrMembershipInput::decode(payload).map_err(rsf)?;
            let pg = input.proposal().election.pg;
            self.validate_decision_nodes(input.proposal())
                .map_err(invalid)?;
            if let Some(existing) = self.membership_decisions.get(&pg)
                && existing.proposal() != input.proposal()
            {
                return Err(invalid(
                    "membership authority upload conflicts with retained decision",
                ));
            }
            let inbox = self
                .membership_inputs
                .get_mut(&pg)
                .ok_or_else(|| invalid("membership group has no configured authority inbox"))?;
            return inbox
                .begin(sender, input)
                .map(|count| count.to_le_bytes().to_vec())
                .map_err(rsf);
        }
        if matches!(kind, MEMBERSHIP_AUTH_BATCH | MEMBERSHIP_AUTH_FINISH) {
            let pg = PlacementGroupId(u64_at(payload, 0)?);
            let id = id_at(payload, 8)?;
            let inbox = self
                .membership_inputs
                .get_mut(&pg)
                .ok_or_else(|| invalid("membership group has no configured authority inbox"))?;
            if kind == MEMBERSHIP_AUTH_BATCH {
                let batch = rsf_vsr::VsrHistoryBatch::decode(
                    payload
                        .get(40..)
                        .ok_or_else(|| invalid("missing membership authority batch"))?,
                )
                .map_err(rsf)?;
                return inbox
                    .append(sender, id, batch)
                    .map(|count| count.to_le_bytes().to_vec())
                    .map_err(rsf);
            }
            if payload.len() != 40 {
                return Err(invalid("invalid membership authority finish length"));
            }
            let approval = inbox.finish(sender, id).map_err(rsf)?;
            self.initialize_membership_decision(&approval)
                .map_err(invalid)?;
            return Ok(id.to_vec());
        }
        if kind == MEMBERSHIP_ACCEPT {
            let (decision, nomination) = decode_acceptance(payload)?;
            let pg = decision.input().proposal().election.pg;
            let store = self
                .membership_decisions
                .get_mut(&pg)
                .ok_or_else(|| invalid("membership authority is not initialized"))?;
            return store
                .accept(sender, &decision, &nomination)
                .and_then(|report| report.encode())
                .map_err(rsf);
        }
        if kind == MEMBERSHIP_CHOSEN {
            let chosen = VsrMembershipChosen::decode(payload).map_err(rsf)?;
            let pg = chosen.proposal().election.pg;
            let store = self
                .membership_decisions
                .get_mut(&pg)
                .ok_or_else(|| invalid("membership authority is not initialized"))?;
            if !store.proposal().new_voters.contains(&sender) {
                return Err(invalid(
                    "membership decision sender is not a proposed voter",
                ));
            }
            let id = chosen.input_id();
            store.learn_chosen(chosen).map_err(rsf)?;
            return Ok(id.to_vec());
        }
        if matches!(kind, MEMBERSHIP_INPUT_DESCRIPTOR | MEMBERSHIP_INPUT_READ) {
            let pg = PlacementGroupId(u64_at(payload, 0)?);
            let id = id_at(payload, 8)?;
            let store = self
                .membership_decisions
                .get(&pg)
                .ok_or_else(|| invalid("membership authority is not initialized"))?;
            if !store.proposal().new_voters.contains(&sender) {
                return Err(invalid("membership input reader is not a proposed voter"));
            }
            let (input, records) = store.input(id).map_err(rsf)?;
            if kind == MEMBERSHIP_INPUT_DESCRIPTOR {
                if payload.len() != 40 {
                    return Err(invalid("invalid membership descriptor query length"));
                }
                return input.encode().map_err(rsf);
            }
            if payload.len() != 48 {
                return Err(invalid("invalid membership history query length"));
            }
            let ordinal = u64_at(payload, 40)?;
            let start = usize::try_from(ordinal)
                .map_err(|_| invalid("membership history ordinal exceeds platform range"))?;
            return rsf_vsr::VsrHistoryBatch::pack(
                ordinal,
                records
                    .get(start..)
                    .ok_or_else(|| invalid("membership history ordinal exceeds input"))?,
            )
            .and_then(|b| b.encode())
            .map_err(rsf);
        }
        let (pg, round, proposal_id) = read_context(payload)?;
        let store = self
            .membership_decisions
            .get_mut(&pg)
            .ok_or_else(|| invalid("membership authority is not initialized"))?;
        if store.proposal().id().map_err(rsf)? != proposal_id {
            return Err(invalid(
                "membership request has a different proposal identity",
            ));
        }
        let mut copy = None;
        let result = match kind {
            MEMBERSHIP_PROMISE if payload.len() == 56 => store
                .promise(sender, round)
                .and_then(|r| r.encode())
                .map_err(rsf),
            MEMBERSHIP_INPUT_BEGIN => {
                let input = VsrMembershipInput::decode(
                    payload
                        .get(56..)
                        .ok_or_else(|| invalid("missing membership input descriptor"))?,
                )
                .map_err(rsf)?;
                let id = input.id().map_err(rsf)?;
                let count = store.begin_input(sender, round, input).map_err(rsf)?;
                copy = Some((id, count));
                Ok(count.to_le_bytes().to_vec())
            }
            MEMBERSHIP_INPUT_BATCH => {
                let id = id_at(payload, 56)?;
                let batch = rsf_vsr::VsrHistoryBatch::decode(
                    payload
                        .get(88..)
                        .ok_or_else(|| invalid("missing membership input batch"))?,
                )
                .map_err(rsf)?;
                let count = store.append_input(sender, round, id, batch).map_err(rsf)?;
                copy = Some((id, count));
                Ok(count.to_le_bytes().to_vec())
            }
            _ => Err(invalid("invalid membership decision RPC kind or length")),
        };
        if result.is_ok() {
            self.note_membership_recovery_progress(pg, sender, round, copy);
        }
        result
    }
}

pub(super) fn transition_path_present(path: &Path) -> Result<bool, String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "cannot inspect membership transition custody: {error}"
        )),
    }
}
