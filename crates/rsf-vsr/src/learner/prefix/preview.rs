//! Read-only semantic inspection of a real, unpublished candidate owner.
use super::*;

/// A candidate view cannot vote, promote a learner or acknowledge application
/// state. There is no constructor from decoded records. Traversals revalidate
/// actual candidate custody; the finalizer also checks the prior publication.
#[derive(Debug)]
pub struct VsrImportPreview<'a> {
    cell: &'a DurableVsrCell,
    token: VsrStateToken,
}
impl<'a> VsrImportPreview<'a> {
    pub(in crate::learner) fn new(cell: &'a DurableVsrCell) -> RsfResult<Self> {
        Ok(Self {
            token: cell.recovery_state_token()?,
            cell,
        })
    }
    pub fn validate_live(&self) -> RsfResult<()> {
        if self.token != self.cell.recovery_state_token()? {
            return Err(corrupt(
                "learner import candidate changed during validation",
            ));
        }
        Ok(())
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.cell.committed_tail()
    }
    pub fn committed_count(&self) -> usize {
        self.cell.committed_count()
    }
    pub fn next_index(&self) -> LogIndex {
        self.cell.next_index()
    }
    pub fn membership_lineage(&self) -> RsfResult<VsrMembershipLineage> {
        self.validate_live()?;
        self.cell.membership_lineage()
    }
    pub fn membership_lineage_at(&self, view: Epoch) -> RsfResult<Option<VsrMembershipLineage>> {
        self.validate_live()?;
        self.cell.membership_lineage_at(view)
    }
    pub fn for_each_committed(
        &self,
        mut visit: impl FnMut(&VsrPrepare, &VsrCommit) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.validate_live()?;
        for commit in &self.cell.cell.committed {
            let pending = self
                .cell
                .cell
                .pending
                .get(&commit.index)
                .filter(|pending| pending.committed)
                .ok_or_else(|| corrupt("import preview commit lacks prepared operation"))?;
            visit(&pending.prepare, commit)?;
        }
        self.validate_live()
    }
    /// Visit only the retained accepted suffix. The caller must not treat these
    /// prepares as committed outcomes or omit their deterministic semantics.
    pub fn for_each_accepted_tail(
        &self,
        mut visit: impl FnMut(&VsrPrepare) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.validate_live()?;
        let start = self
            .committed_tail()
            .0
            .checked_add(1)
            .ok_or_else(|| corrupt("import preview accepted index overflow"))?;
        for index in start..self.next_index().0 {
            let pending = self
                .cell
                .cell
                .pending
                .get(&LogIndex(index))
                .filter(|pending| !pending.committed)
                .ok_or_else(|| corrupt("import preview accepted suffix has a gap"))?;
            visit(&pending.prepare)?;
        }
        self.validate_live()
    }
}
