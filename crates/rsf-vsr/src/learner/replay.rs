//! Borrowed committed history from the actual published non-voting owner.
use super::*;

/// An observation of one learner publication. This cannot expose its internal
/// voter-shaped replay cell, cast votes, probe a primary, or install a view.
/// Metadata/prepare references are historical facts; validate_live or complete
/// for_each_committed before treating a projection as a current observation.
#[derive(Debug)]
pub struct PublishedVsrReplay<'a> {
    owner: &'a LearnerHistoryStore,
    token: VsrLearnerStateToken,
}
impl LearnerHistoryStore {
    pub fn published_replay(&self) -> RsfResult<PublishedVsrReplay<'_>> {
        Ok(PublishedVsrReplay {
            owner: self,
            token: self.published_state_token()?,
        })
    }
}
impl PublishedVsrReplay<'_> {
    pub fn validate_live(&self) -> RsfResult<()> {
        if self.token != self.owner.published_state_token()? {
            return Err(corrupt("learner publication changed during observation"));
        }
        Ok(())
    }
    pub fn state_token(&self) -> &VsrLearnerStateToken {
        &self.token
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.cell().committed_tail()
    }
    pub fn committed_count(&self) -> usize {
        self.cell().committed_count()
    }
    /// Never exposes an accepted-only command from the copied history.
    pub fn committed_prepare(&self, index: LogIndex) -> Option<&VsrPrepare> {
        let pending = self.cell().cell.pending.get(&index)?;
        pending.committed.then_some(&pending.prepare)
    }
    pub fn membership_lineage(&self) -> RsfResult<VsrMembershipLineage> {
        self.validate_live()?;
        self.cell().membership_lineage()
    }
    pub fn membership_lineage_at(&self, view: Epoch) -> RsfResult<Option<VsrMembershipLineage>> {
        self.validate_live()?;
        self.cell().membership_lineage_at(view)
    }
    /// The caller may enforce command and aggregate limits before cloning any
    /// payload. Both outer learner custody and inner physical history are checked
    /// before/after traversal; callbacks do not grant application/voting ACKs.
    pub fn for_each_committed(
        &self,
        mut visit: impl FnMut(&VsrPrepare, &VsrCommit) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.validate_live()?;
        let result = (|| {
            for commit in &self.cell().cell.committed {
                let prepare = self
                    .committed_prepare(commit.index)
                    .ok_or_else(|| corrupt("published learner commit has no committed prepare"))?;
                visit(prepare, commit)?;
            }
            Ok(())
        })();
        // Revalidate the same borrowed owner even when semantic inspection
        // refuses a record. Observed physical failure takes precedence.
        self.validate_live()?;
        result
    }
    /// Accepted-only metadata remains distinct from committed observations.
    pub fn next_index(&self) -> LogIndex {
        self.cell().next_index()
    }
    pub fn for_each_accepted_tail(
        &self,
        mut visit: impl FnMut(&VsrPrepare) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.validate_live()?;
        let result = (|| {
            let first = self
                .committed_tail()
                .0
                .checked_add(1)
                .ok_or_else(|| corrupt("published accepted index overflow"))?;
            for index in first..self.next_index().0 {
                let pending = self
                    .cell()
                    .cell
                    .pending
                    .get(&LogIndex(index))
                    .filter(|pending| !pending.committed)
                    .ok_or_else(|| corrupt("published accepted suffix has a gap"))?;
                visit(&pending.prepare)?;
            }
            Ok(())
        })();
        self.validate_live()?;
        result
    }
    fn cell(&self) -> &DurableVsrCell {
        // The borrowed owner cannot finish/replace its publication while this
        // view lives; construction required a healthy existing publication.
        &self
            .owner
            .published
            .as_ref()
            .expect("validated published owner")
            .cell
    }
}
