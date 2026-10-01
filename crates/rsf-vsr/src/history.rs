//! Certificate authority is the configuration installed for the entry's view.
//! A later configuration seals the preceding view at its committed boundary.
use super::*;

#[derive(Debug)]
struct Configuration {
    primary: NodeId,
    voters: BTreeSet<NodeId>,
    preceding_tail: LogIndex,
}

#[derive(Debug)]
pub(super) struct ConfigurationHistory(BTreeMap<Epoch, Configuration>);

impl ConfigurationHistory {
    pub(super) fn new(cell: &VsrCell) -> Self {
        let mut history = Self(BTreeMap::new());
        history.record(cell);
        history
    }

    // Called only for initial bootstrap or an accepted, strictly newer view.
    pub(super) fn record(&mut self, cell: &VsrCell) {
        self.0.insert(
            cell.view,
            Configuration {
                primary: cell.primary,
                voters: cell.replicas.clone(),
                preceding_tail: cell.committed_tail(),
            },
        );
    }

    pub(super) fn validate(&self, prepare: &VsrPrepare, commit: &VsrCommit) -> RsfResult<()> {
        let configuration = self
            .0
            .get(&prepare.view)
            .ok_or_else(|| corrupt("committed entry has no installed historical configuration"))?;
        if prepare.primary != configuration.primary {
            return Err(corrupt(
                "committed entry primary differs from its historical configuration",
            ));
        }
        let next = self
            .0
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
