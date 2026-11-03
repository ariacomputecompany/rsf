//! Immutable requests for the existing sole recovery worker. No store handles,
//! writer permissions, callbacks or full history vectors cross this boundary.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum Scope {
    Discovery,
    Round {
        round: VsrMembershipRound,
        input: Option<[u8; 32]>,
    },
    Chosen {
        input: [u8; 32],
    },
    Plan {
        input: [u8; 32],
        plan: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) struct Binding {
    pub proposal: [u8; 32],
    pub bootstrap: [u8; 32],
    pub scope: Scope,
}

#[derive(Clone, Debug)]
pub(in super::super) struct Context {
    pub proposal: Arc<VsrMembershipProposal>,
    pub bootstrap: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum Kind {
    State,
    Promise,
    Chosen,
    Source,
    Read { ordinal: u64, total: u64 },
    RoundBegin { total: u64 },
    RoundBatch { end: u64, total: u64 },
    Accept,
    ImportBegin { total: u64 },
    ImportBatch { end: u64, total: u64 },
    ImportFinish,
}

#[derive(Debug)]
pub(in super::super) enum Operation {
    State,
    Promise(VsrMembershipRound),
    Chosen([u8; 32]),
    Source([u8; 32]),
    Read {
        input: Arc<VsrMembershipInput>,
        ordinal: u64,
        round: Option<VsrMembershipRound>,
    },
    RoundBegin {
        round: VsrMembershipRound,
        input: Arc<VsrMembershipInput>,
    },
    RoundBatch {
        round: VsrMembershipRound,
        input: Arc<VsrMembershipInput>,
        batch: VsrHistoryBatch,
    },
    Accept {
        decision: Arc<VsrMembershipDecision>,
        nomination: VsrMembershipAccepted,
    },
    ImportBegin {
        chosen: Arc<VsrMembershipChosen>,
        input: Arc<VsrMembershipInput>,
    },
    ImportBatch {
        input: Arc<VsrMembershipInput>,
        batch: VsrHistoryBatch,
    },
    ImportFinish(Arc<VsrMembershipInput>),
}

#[derive(Debug)]
pub(in super::super) struct Request {
    pub context: Context,
    pub scope: Scope,
    pub operation: Operation,
}
impl Request {
    pub(in super::super) fn kind(&self) -> Result<Kind, String> {
        let total = |input: &VsrMembershipInput| input.records().map_err(|e| e.message);
        let end = |batch: &VsrHistoryBatch| {
            batch
                .ordinal
                .checked_add(batch.records.len() as u64)
                .ok_or_else(|| "recovery batch ordinal overflow".to_string())
        };
        Ok(match &self.operation {
            Operation::State => Kind::State,
            Operation::Promise(_) => Kind::Promise,
            Operation::Chosen(_) => Kind::Chosen,
            Operation::Source(_) => Kind::Source,
            Operation::Read { input, ordinal, .. } => Kind::Read {
                ordinal: *ordinal,
                total: total(input)?,
            },
            Operation::RoundBegin { input, .. } => Kind::RoundBegin {
                total: total(input)?,
            },
            Operation::RoundBatch { input, batch, .. } => Kind::RoundBatch {
                end: end(batch)?,
                total: total(input)?,
            },
            Operation::Accept { .. } => Kind::Accept,
            Operation::ImportBegin { input, .. } => Kind::ImportBegin {
                total: total(input)?,
            },
            Operation::ImportBatch { input, batch } => Kind::ImportBatch {
                end: end(batch)?,
                total: total(input)?,
            },
            Operation::ImportFinish(_) => Kind::ImportFinish,
        })
    }
    pub(in super::super) fn binding(&self) -> Result<Binding, String> {
        Ok(Binding {
            proposal: self.context.proposal.id().map_err(|e| e.message)?,
            bootstrap: self.context.bootstrap,
            scope: self.scope,
        })
    }
    pub(in super::super) fn phase(&self) -> &'static str {
        match self.operation {
            Operation::State => "discovering-choice",
            Operation::Promise(_) => "promising-input",
            Operation::Chosen(_) => "recovering-choice",
            Operation::Source(_) | Operation::Read { .. } => "fetching-input",
            Operation::RoundBegin { .. } | Operation::RoundBatch { .. } => "replicating-input",
            Operation::Accept { .. } => "accepting-input",
            Operation::ImportBegin { .. }
            | Operation::ImportBatch { .. }
            | Operation::ImportFinish(_) => "continuing-chosen-input",
        }
    }
    pub(in super::super) fn execute(
        self,
        transport: &NetworkVsrTransport,
        replica: NodeId,
    ) -> Result<Response, String> {
        let Context {
            proposal,
            bootstrap,
        } = self.context;
        let result = match self.operation {
            Operation::State => transport
                .membership_decision_state(replica, &proposal, bootstrap)
                .map(Response::State),
            Operation::Promise(round) => transport
                .promise_membership_recovery(replica, &proposal, bootstrap, round)
                .map(Response::Promise),
            Operation::Chosen(input) => transport
                .query_membership_chosen(replica, &proposal, bootstrap, input)
                .map(Response::Chosen),
            Operation::Source(input) => transport
                .membership_input_progress(replica, &proposal, bootstrap, input)
                .map(Response::Source),
            Operation::Read {
                input,
                ordinal,
                round,
            } => match round {
                Some(round) => transport
                    .membership_round_input_batch(replica, &input, bootstrap, round, ordinal),
                None => transport.membership_input_batch(replica, &input, bootstrap, ordinal),
            }
            .map(Response::Records),
            Operation::RoundBegin { round, input } => transport
                .begin_membership_input(replica, round, &input)
                .map(Response::Offset),
            Operation::RoundBatch {
                round,
                input,
                batch,
            } => transport
                .append_membership_input(replica, round, &input, batch)
                .map(Response::Offset),
            Operation::Accept {
                decision,
                nomination,
            } => transport
                .accept_membership(replica, &decision, &nomination)
                .map(Response::Accepted),
            Operation::ImportBegin { chosen, input } => transport
                .begin_membership_chosen_input(replica, &chosen, &input, bootstrap)
                .map(Response::Import),
            Operation::ImportBatch { input, batch } => transport
                .append_membership_chosen_input(replica, &input, bootstrap, batch)
                .map(Response::Import),
            Operation::ImportFinish(input) => transport
                .finish_membership_chosen_input(replica, &input, bootstrap)
                .map(Response::Import),
        };
        result.map_err(|e| e.reason)
    }
}

#[derive(Debug)]
pub(in super::super) enum Response {
    State(VsrMembershipDecisionState),
    Promise(VsrMembershipPromiseReply),
    Chosen(Option<VsrMembershipChosen>),
    Source(VsrMembershipInputProgress),
    Records(VsrHistoryBatch),
    Offset(u64),
    Accepted(VsrMembershipAccepted),
    Import(VsrMembershipImportReply),
}
