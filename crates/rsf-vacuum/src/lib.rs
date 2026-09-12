#![forbid(unsafe_code)]

use rsf_core::{CommitVersion, ErrorCode, RsfError, RsfResult};
use rsf_tuple::{TupleChain, TupleKey, TupleVersion};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VacuumPolicy {
    pub oldest_snapshot: CommitVersion,
    pub min_versions_to_keep: usize,
}

impl VacuumPolicy {
    pub fn new(oldest_snapshot: CommitVersion, min_versions_to_keep: usize) -> RsfResult<Self> {
        if oldest_snapshot.0 == 0 {
            return Err(invalid("oldest snapshot must be non-zero"));
        }
        if min_versions_to_keep == 0 {
            return Err(invalid("vacuum must retain at least one tuple version"));
        }
        Ok(Self {
            oldest_snapshot,
            min_versions_to_keep,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VacuumPlan {
    pub key: TupleKey,
    pub oldest_snapshot: CommitVersion,
    pub pruned_versions: Vec<TupleVersion>,
    pub retained_versions: Vec<TupleVersion>,
    pub reclaimed_bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VacuumDecision {
    Planned(VacuumPlan),
    Rejected { key: TupleKey, reason: String },
}

#[derive(Clone, Debug, Default)]
pub struct VacuumPlanner;

impl VacuumPlanner {
    pub fn plan(&self, chain: &TupleChain, policy: &VacuumPolicy) -> VacuumDecision {
        if chain.versions.is_empty() {
            return rejected(chain.key.clone(), "tuple chain is empty");
        }

        let mut pruned = Vec::new();
        let mut retained = Vec::new();
        let mut remaining = chain.versions.len();

        for version in &chain.versions {
            let can_prune = version
                .header
                .delete_version
                .is_some_and(|delete_version| delete_version <= policy.oldest_snapshot)
                && remaining > policy.min_versions_to_keep;

            if can_prune {
                remaining -= 1;
                pruned.push(version.clone());
            } else {
                retained.push(version.clone());
            }
        }

        if retained.is_empty() {
            return rejected(
                chain.key.clone(),
                "vacuum plan would remove every tuple version",
            );
        }

        let reclaimed_bytes = pruned
            .iter()
            .map(|version| version.header.row_bytes)
            .sum::<u32>();

        VacuumDecision::Planned(VacuumPlan {
            key: chain.key.clone(),
            oldest_snapshot: policy.oldest_snapshot,
            pruned_versions: pruned,
            retained_versions: retained,
            reclaimed_bytes,
        })
    }
}

fn rejected(key: TupleKey, reason: impl Into<String>) -> VacuumDecision {
    VacuumDecision::Rejected {
        key,
        reason: reason.into(),
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_tuple::{TupleDecision, TupleStore};

    #[test]
    fn prunes_versions_deleted_before_oldest_snapshot() {
        let chain = sample_chain();
        let policy = VacuumPolicy::new(CommitVersion(14), 1).unwrap();

        let decision = VacuumPlanner.plan(&chain, &policy);

        assert!(matches!(
            decision,
            VacuumDecision::Planned(VacuumPlan {
                ref pruned_versions,
                ref retained_versions,
                reclaimed_bytes,
                ..
            }) if pruned_versions.len() == 2 && retained_versions.len() == 1 && reclaimed_bytes == 8
        ));
    }

    #[test]
    fn keeps_versions_needed_by_oldest_snapshot() {
        let chain = sample_chain();
        let policy = VacuumPolicy::new(CommitVersion(11), 1).unwrap();

        let decision = VacuumPlanner.plan(&chain, &policy);

        assert!(matches!(
            decision,
            VacuumDecision::Planned(VacuumPlan {
                ref pruned_versions,
                ref retained_versions,
                ..
            }) if pruned_versions.is_empty() && retained_versions.len() == 3
        ));
    }

    #[test]
    fn rejects_empty_chain() {
        let key = TupleKey::new("users", b"id=7".to_vec());
        let chain = TupleChain {
            key,
            versions: Vec::new(),
        };
        let policy = VacuumPolicy::new(CommitVersion(20), 1).unwrap();

        assert!(matches!(
            VacuumPlanner.plan(&chain, &policy),
            VacuumDecision::Rejected { .. }
        ));
    }

    #[test]
    fn rejects_invalid_policy() {
        assert!(VacuumPolicy::new(CommitVersion(0), 1).is_err());
        assert!(VacuumPolicy::new(CommitVersion(10), 0).is_err());
    }

    fn sample_chain() -> TupleChain {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"id=7".to_vec());
        assert!(matches!(
            store.insert(
                key.clone(),
                b"v001".to_vec(),
                CommitVersion(10),
                CommitVersion(3)
            ),
            Ok(TupleDecision::Inserted(_))
        ));
        assert!(matches!(
            store.update(&key, b"v002".to_vec(), CommitVersion(12), CommitVersion(3)),
            Ok(TupleDecision::Updated { .. })
        ));
        assert!(matches!(
            store.update(&key, b"v003".to_vec(), CommitVersion(14), CommitVersion(3)),
            Ok(TupleDecision::Updated { .. })
        ));
        store.chain(&key).cloned().expect("tuple chain")
    }
}
