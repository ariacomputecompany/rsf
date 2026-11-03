//! Certificate authority is the configuration installed for the entry's view.
//! A later configuration seals the preceding view at its committed boundary.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Configuration {
    primary: NodeId,
    voters: BTreeSet<NodeId>,
    preceding_tail: LogIndex,
}

#[derive(Clone, Debug)]
pub(super) struct ConfigurationHistory {
    configurations: BTreeMap<Epoch, Configuration>,
    learned_commits: BTreeMap<(Epoch, LogIndex), [u8; 32]>,
    // Replay-derived evidence, not part of configuration-equivalence checks.
    // Chosen/staged proof subsets and source reporters may legitimately differ.
    publications: BTreeMap<[u8; 32], std::sync::Arc<VsrMembershipPublication>>,
}

impl ConfigurationHistory {
    pub(super) fn new(cell: &VsrCell) -> Self {
        let mut history = Self {
            configurations: BTreeMap::new(),
            learned_commits: BTreeMap::new(),
            publications: BTreeMap::new(),
        };
        history.record(cell);
        history
    }

    pub(super) fn record_membership_publication(
        &mut self,
        publication: VsrMembershipPublication,
    ) -> RsfResult<()> {
        match self.publications.entry(publication.plan_id()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(std::sync::Arc::new(publication));
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(_) => {
                Err(corrupt("membership plan activated twice in one history"))
            }
        }
    }

    pub(super) fn membership_publication(
        &self,
        plan: [u8; 32],
    ) -> Option<&VsrMembershipPublication> {
        self.publications.get(&plan).map(std::sync::Arc::as_ref)
    }

    pub(super) fn has_membership_publication(&self) -> bool {
        !self.publications.is_empty()
    }

    // Replicas can have installed different empty views or learned different
    // committed boundaries. Shared view identities must still name the same
    // primary/voters; committed data is checked separately by the installer.
    pub(super) fn compatible(&self, other: &Self) -> bool {
        other.configurations.iter().all(|(view, old)| {
            self.configurations
                .get(view)
                .is_none_or(|new| new.primary == old.primary && new.voters == old.voters)
        }) && other.learned_commits.iter().all(|(key, digest)| {
            self.learned_commits
                .get(key)
                .is_none_or(|new| new == digest)
        })
    }

    pub(super) fn contains_authority(&self, old: &Self) -> bool {
        self.compatible(old)
            && old.configurations.iter().all(|(view, configuration)| {
                self.configurations.get(view).is_some_and(|known| {
                    known.primary == configuration.primary && known.voters == configuration.voters
                })
            })
    }

    // Only a completed installation may authorize a certificate learned from
    // another reporting voter after its old view was sealed. Bind the exception
    // to the exact accepted operation; ordinary stale-view transfer cannot add it.
    pub(super) fn learn_committed(
        &mut self,
        prepare: &VsrPrepare,
        commit: &VsrCommit,
    ) -> RsfResult<()> {
        let configuration = self
            .configurations
            .get(&prepare.view)
            .ok_or_else(|| corrupt("learned commit has no historical configuration"))?;
        if prepare.primary != configuration.primary || prepare.index <= configuration.preceding_tail
        {
            return Err(corrupt(
                "learned commit conflicts with historical configuration authority",
            ));
        }
        validate_commit_certificate(
            commit,
            prepare,
            &configuration.voters,
            configuration.voters.len() / 2 + 1,
        )?;
        let digest = rsf_core::Digest::blake3_bytes(&prepare.encode()?).bytes;
        if self
            .learned_commits
            .get(&(prepare.view, prepare.index))
            .is_some_and(|old| *old != digest)
        {
            return Err(corrupt(
                "learned commit conflicts with an earlier installation",
            ));
        }
        self.learned_commits
            .insert((prepare.view, prepare.index), digest);
        Ok(())
    }

    // Called only for initial bootstrap or an accepted, strictly newer view.
    pub(super) fn record(&mut self, cell: &VsrCell) {
        self.configurations.insert(
            cell.view,
            Configuration {
                primary: cell.primary,
                voters: cell.replicas.clone(),
                preceding_tail: cell.committed_tail(),
            },
        );
    }

    pub(super) fn validate_prepare(&self, prepare: &VsrPrepare) -> RsfResult<()> {
        let configuration = self
            .configurations
            .get(&prepare.view)
            .ok_or_else(|| corrupt("committed entry has no installed historical configuration"))?;
        if prepare.primary != configuration.primary {
            return Err(corrupt(
                "committed entry primary differs from its historical configuration",
            ));
        }
        if self.learned_commits.get(&(prepare.view, prepare.index))
            == Some(&rsf_core::Digest::blake3_bytes(&prepare.encode()?).bytes)
        {
            return Ok(());
        }
        let next = self
            .configurations
            .range((
                std::ops::Bound::Excluded(prepare.view),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(_, configuration)| configuration);
        if prepare.index <= configuration.preceding_tail
            || next.is_some_and(|next| prepare.index > next.preceding_tail)
        {
            return Err(corrupt(
                "committed entry index lies outside its historical view boundary",
            ));
        }
        Ok(())
    }

    pub(super) fn validate(&self, prepare: &VsrPrepare, commit: &VsrCommit) -> RsfResult<()> {
        self.validate_prepare(prepare)?;
        let configuration = &self.configurations[&prepare.view];
        validate_commit_certificate(
            commit,
            prepare,
            &configuration.voters,
            configuration.voters.len() / 2 + 1,
        )
    }
}

#[cfg(test)]
mod tests;
