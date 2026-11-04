//! Read-only control observations from a non-voting learner's actual custody.
use super::*;
use rsf_vsr::{LearnerHistoryStore, VsrLearnerStateToken};

/// A learner observation is deliberately a distinct type. It has no voter
/// preflight, admission, commit-delivery or CommittedIntent constructor.
#[derive(Debug)]
pub struct PublishedControlState {
    state: ReplayState,
    stamp: VsrLearnerStateToken,
}
impl PublishedControlState {
    pub fn recover(anchor: &ControlBootstrap, learner: &LearnerHistoryStore) -> RsfResult<Self> {
        let view = learner.published_replay()?;
        let mut replay = read::CommittedReplay::new(
            anchor,
            &view.membership_lineage()?,
            view.committed_tail(),
            view.committed_count(),
        )?;
        view.for_each_committed(|prepare, commit| {
            let historical = view
                .membership_lineage_at(prepare.view)?
                .ok_or_else(|| corrupt("published control entry lacks historical lineage"))?;
            replay.push(prepare, commit, &historical)
        })?;
        let (state, bytes) = replay.finish()?;
        let current = view.membership_lineage()?;
        let mut accepted =
            preflight::AcceptedReplay::new(state.clone(), bytes, &current, view.next_index())?;
        view.for_each_accepted_tail(|prepare| {
            let historical = view
                .membership_lineage_at(prepare.view)?
                .ok_or_else(|| corrupt("published accepted control lacks historical lineage"))?;
            accepted.push(prepare, &historical, &current)
        })?;
        accepted.finish(view.next_index())?;
        view.validate_live()?;
        Ok(Self {
            state,
            stamp: view.state_token().clone(),
        })
    }
    pub fn validate_live(&self, learner: &LearnerHistoryStore) -> RsfResult<()> {
        if self.stamp != learner.published_state_token()? {
            return Err(invalid("published control observation is stale"));
        }
        Ok(())
    }
    pub fn catalog(&self) -> &ControlCatalog {
        self.state.catalog.as_ref().expect("recovered genesis")
    }
    pub fn root(&self) -> CatalogRoot {
        self.state.root.expect("recovered genesis")
    }
    pub fn revision(&self) -> ControlRevision {
        self.state.revision
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.state.tail
    }
    pub fn last_outcome(&self) -> &ControlOutcome {
        self.state.last.as_ref().expect("recovered genesis")
    }
    pub fn pending(&self) -> Option<&MembershipIntent> {
        self.state.pending.as_ref().map(|pending| &pending.intent)
    }
    pub fn request_outcome(&self, request: ControlRequestId) -> Option<&ControlOutcome> {
        self.state
            .receipts
            .get(&request)
            .map(|receipt| &receipt.outcome)
    }
    /// Observe the exact first committed Begin. This may support future learner
    /// retention, but never an old-control voter or application admission report.
    pub fn pending_intent(
        &self,
        learner: &LearnerHistoryStore,
        id: IntentId,
    ) -> RsfResult<PublishedIntent> {
        self.validate_live(learner)?;
        let pending = self
            .state
            .pending
            .as_ref()
            .ok_or_else(|| invalid("no published pending intent"))?;
        let receipt = self
            .state
            .receipts
            .get(&pending.intent.request)
            .ok_or_else(|| corrupt("published intent lost its request receipt"))?;
        if pending.intent.id()? != id
            || receipt.command != id.0
            || receipt.begin != Some(pending.begin)
            || receipt.outcome != (ControlOutcome::IntentAccepted { intent: id })
        {
            return Err(invalid("published pending intent/origin differs"));
        }
        let view = learner.published_replay()?;
        let prepare = view
            .committed_prepare(pending.begin)
            .ok_or_else(|| corrupt("published intent origin is not committed"))?;
        let VsrOperation::ReplicatedCommand { payload, .. } = &prepare.operation else {
            return Err(corrupt("published intent origin is not a control command"));
        };
        let historical = view
            .membership_lineage_at(prepare.view)?
            .ok_or_else(|| corrupt("published intent origin lost historical lineage"))?;
        if prepare.pg != self.state.anchor.control_pg
            || prepare.index != pending.begin
            || payload != &ControlCommand::Begin(pending.intent.clone()).encode()?
            || historical != pending.old_control
        {
            return Err(corrupt(
                "published intent changed committed operation/lineage",
            ));
        }
        let operation = super::intent::operation_digest(&prepare.operation)?;
        view.validate_live()?;
        self.validate_live(learner)?;
        Ok(PublishedIntent {
            anchor: self.state.anchor.clone(),
            intent: pending.intent.clone(),
            id,
            begin: pending.begin,
            old_control: historical,
            operation,
            stamp: self.stamp.clone(),
        })
    }
}

/// Exact committed learner observation; no voter/admission API accepts it.
#[derive(Debug)]
pub struct PublishedIntent {
    anchor: ControlBootstrap,
    intent: MembershipIntent,
    id: IntentId,
    begin: LogIndex,
    old_control: VsrMembershipLineage,
    operation: [u8; 32],
    stamp: VsrLearnerStateToken,
}
impl PublishedIntent {
    pub fn anchor(&self) -> &ControlBootstrap {
        &self.anchor
    }
    pub fn intent(&self) -> &MembershipIntent {
        &self.intent
    }
    pub fn id(&self) -> IntentId {
        self.id
    }
    pub fn begin_index(&self) -> LogIndex {
        self.begin
    }
    pub fn old_control(&self) -> &VsrMembershipLineage {
        &self.old_control
    }
    pub fn operation_digest(&self) -> [u8; 32] {
        self.operation
    }
    pub fn validate_live(&self, learner: &LearnerHistoryStore) -> RsfResult<()> {
        if self.stamp != learner.published_state_token()? {
            return Err(invalid(
                "published intent custody changed; replay before use",
            ));
        }
        Ok(())
    }
}
