//! Shared semantic fold; only owned voter/learner entry points call it.
use super::*;
use rsf_vsr::{VsrCommit, VsrPrepare};

pub(super) struct CommittedReplay {
    state: ReplayState,
    bytes: usize,
    tail: LogIndex,
    count: usize,
    visited: usize,
}
impl CommittedReplay {
    pub(super) fn new(
        anchor: &ControlBootstrap,
        current: &VsrMembershipLineage,
        tail: LogIndex,
        count: usize,
    ) -> RsfResult<Self> {
        anchor.validate()?;
        if current.pg != anchor.control_pg || current.bootstrap != anchor.control_bootstrap {
            return Err(corrupt(
                "control owner does not extend provisioned bootstrap",
            ));
        }
        if tail.0 > MAX_CONTROL_RECORDS as u64 || count > MAX_CONTROL_RECORDS {
            return Err(invalid(
                "control history exceeds qualified replay record bound",
            ));
        }
        Ok(Self {
            state: ReplayState::new(anchor.clone()),
            bytes: 0,
            tail,
            count,
            visited: 0,
        })
    }
    pub(super) fn push(
        &mut self,
        prepare: &VsrPrepare,
        commit: &VsrCommit,
        historical: &VsrMembershipLineage,
    ) -> RsfResult<()> {
        if prepare.pg != self.state.anchor.control_pg
            || commit.pg != prepare.pg
            || prepare.index != commit.index
            || prepare.operation != commit.certificate.operation
            || prepare.index.0
                != self
                    .state
                    .tail
                    .0
                    .checked_add(1)
                    .ok_or_else(|| corrupt("control index overflow"))?
            || historical.voters.len() > MAX_NODES
        {
            return Err(corrupt("control committed entry identity/order mismatch"));
        }
        let VsrOperation::ReplicatedCommand { payload, .. } = &prepare.operation else {
            return Err(corrupt("non-control operation in control history"));
        };
        self.bytes = self
            .bytes
            .checked_add(payload.len())
            .ok_or_else(|| corrupt("control replay byte overflow"))?;
        if self.bytes > MAX_REPLAY_BYTES {
            return Err(invalid(
                "control history exceeds qualified replay byte bound",
            ));
        }
        let command = super::preflight::checked_command(prepare)?;
        self.state.apply(prepare.index, historical, command)?;
        self.visited += 1;
        Ok(())
    }
    pub(super) fn finish(self) -> RsfResult<(ReplayState, usize)> {
        if self.state.catalog.is_none()
            || self.state.tail != self.tail
            || self.visited != self.count
        {
            return Err(corrupt(
                "control history lacks complete committed genesis/prefix",
            ));
        }
        Ok((self.state, self.bytes))
    }
}
