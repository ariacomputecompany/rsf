//! Warm custody checks for an already published non-voting history. A token is
//! only a process-local validation cache; it grants no votes or client access.
use super::*;
mod owner;
pub(in crate::learner) use owner::{CandidateCustody, OwnerCustody};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrLearnerStateToken {
    published: [u8; 32],
    cell: VsrStateToken,
}

impl LearnerHistoryStore {
    /// Revalidate actual outer identity/publication/descriptor and inner cell
    /// custody. No files are created, repaired, copied or made authoritative.
    /// A legitimate `finish` of newer history changes this opaque token; callers
    /// must revalidate their transition authority before updating its cache.
    pub fn published_state_token(&self) -> RsfResult<VsrLearnerStateToken> {
        self.check_failure()?;
        let published = self
            .published
            .as_ref()
            .ok_or_else(|| invalid_input("learner has no published recovery anchor"))?;
        let result = self.validate_published_custody(published);
        if result.is_err() {
            // Losing the named outer anchor must fence the entire live learner,
            // just like losing the inner cell's stamped custody. Restoring a
            // file cannot turn this failed owner into a new authority proof.
            self.failed.store(true, Ordering::Relaxed);
        }
        result
    }

    pub(super) fn validate_published_custody(
        &self,
        published: &Published,
    ) -> RsfResult<VsrLearnerStateToken> {
        self.owner_custody.validate(&self.root, &self._lock)?;
        published.custody.validate_cell(&published.cell)?;
        if let Some(flat) = &published.bounded_flat {
            flat.validate_cell(&published.cell)?;
        }
        let token = VsrLearnerStateToken {
            published: published.id,
            cell: published.cell.recovery_state_token()?,
        };
        if let Some(flat) = &published.bounded_flat {
            flat.validate_cell(&published.cell)?;
        }
        published.custody.validate_cell(&published.cell)?;
        self.owner_custody.validate(&self.root, &self._lock)?;
        Ok(token)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod outer_tests;
