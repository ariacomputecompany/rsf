use super::*;
use std::path::PathBuf;
impl PeerReplicaServer {
    /// Configure owned transition paths only for already hosted voters/learners.
    /// Incoming RPC contents cannot select paths or create an application cell.
    pub fn with_membership_paths(
        mut self,
        paths: BTreeMap<PlacementGroupId, PathBuf>,
    ) -> Result<Self, String> {
        if !self.membership_paths.is_empty() {
            return Err("membership paths already configured".into());
        }
        for (pg, path) in paths {
            if !self.cells.contains_key(&pg) && !self.learners.contains_key(&pg) {
                return Err("membership path requires a hosted authority anchor".into());
            }
            let inbox = MembershipAuthorityInbox::open(path.join("inbox"), pg, self.local)
                .map_err(|e| e.message)?;
            let decision_path = path.join("decision");
            if decision_path.exists() {
                let store =
                    match MembershipDecisionStore::open_existing(&decision_path, pg, self.local) {
                        Ok(store) => store,
                        Err(original) => {
                            // The factory never resets a nonempty journal. A complete
                            // inbox may resume an interrupted initial authority copy.
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
                self.validate_decision_authority(pg, &store.authority().map_err(|e| e.message)?)?;
                if let Some((_, id)) = store.accepted() {
                    self.validate_decision_authority(
                        pg,
                        &store.approval(id).map_err(|e| e.message)?,
                    )?;
                }
                self.membership_decisions.insert(pg, store);
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
        self.validate_decision_authority(pg, approval)?;
        let path = self
            .membership_paths
            .get(&pg)
            .ok_or_else(|| "membership group has no configured transition path".to_string())?;
        if let Some(existing) = self.membership_decisions.get(&pg) {
            if existing.proposal() != approval.proposal() {
                return Err("membership decision already binds another proposal".into());
            }
            return Ok(());
        }
        let store = MembershipDecisionStore::open(path.join("decision"), self.local, approval)
            .map_err(|e| e.message)?;
        self.membership_decisions.insert(pg, store);
        Ok(())
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
        match kind {
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
                store
                    .begin_input(sender, round, input)
                    .map(|count| count.to_le_bytes().to_vec())
                    .map_err(rsf)
            }
            MEMBERSHIP_INPUT_BATCH => {
                let id = id_at(payload, 56)?;
                let batch = rsf_vsr::VsrHistoryBatch::decode(
                    payload
                        .get(88..)
                        .ok_or_else(|| invalid("missing membership input batch"))?,
                )
                .map_err(rsf)?;
                store
                    .append_input(sender, round, id, batch)
                    .map(|count| count.to_le_bytes().to_vec())
                    .map_err(rsf)
            }
            _ => Err(invalid("invalid membership decision RPC kind or length")),
        }
    }
}
