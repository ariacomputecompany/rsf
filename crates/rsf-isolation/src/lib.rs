#![forbid(unsafe_code)]

use rsf_core::{
    CommitVersion, ErrorCode, RsfError, RsfResult,
    transaction::{KeyRange, TransactionId},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationTransaction {
    pub id: TransactionId,
    pub read_version: CommitVersion,
    pub read_ranges: Vec<KeyRange>,
    pub write_ranges: Vec<KeyRange>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyKind {
    ReadAfterWrite,
    WriteAfterRead,
    WriteAfterWrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SerializationEdge {
    pub from: TransactionId,
    pub to: TransactionId,
    pub kind: DependencyKind,
    pub range: KeyRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationCommit {
    pub id: TransactionId,
    pub read_version: CommitVersion,
    pub commit_version: CommitVersion,
    pub read_ranges: Vec<KeyRange>,
    pub write_ranges: Vec<KeyRange>,
    pub dependencies: Vec<SerializationEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IsolationDecision {
    Accepted(IsolationCommit),
    Rejected {
        transaction: TransactionId,
        reason: String,
        conflicting_version: Option<CommitVersion>,
    },
}

#[derive(Clone, Debug, Default)]
pub struct IsolationTracker {
    committed: Vec<IsolationCommit>,
}

impl IsolationTracker {
    pub fn commit(
        &mut self,
        transaction: IsolationTransaction,
        commit_version: CommitVersion,
    ) -> RsfResult<IsolationDecision> {
        validate_transaction(&transaction)?;
        if commit_version <= transaction.read_version {
            return Err(invalid("commit version must be newer than read version"));
        }

        let mut dependencies = Vec::new();
        for committed in self
            .committed
            .iter()
            .filter(|committed| committed.commit_version > transaction.read_version)
        {
            let read_after_write = overlaps_any(&transaction.read_ranges, &committed.write_ranges);
            let write_after_read = overlaps_any(&transaction.write_ranges, &committed.read_ranges);
            let write_after_write =
                overlaps_any(&transaction.write_ranges, &committed.write_ranges);

            if write_after_write.is_some() {
                return Ok(IsolationDecision::Rejected {
                    transaction: transaction.id.clone(),
                    reason: "write/write conflict with newer committed transaction".to_string(),
                    conflicting_version: Some(committed.commit_version),
                });
            }

            if let Some(range) = read_after_write {
                dependencies.push(SerializationEdge {
                    from: committed.id.clone(),
                    to: transaction.id.clone(),
                    kind: DependencyKind::ReadAfterWrite,
                    range: range.clone(),
                });
            }

            if let Some(range) = write_after_read {
                dependencies.push(SerializationEdge {
                    from: transaction.id.clone(),
                    to: committed.id.clone(),
                    kind: DependencyKind::WriteAfterRead,
                    range: range.clone(),
                });
            }

            if read_after_write.is_some() && write_after_read.is_some() {
                return Ok(IsolationDecision::Rejected {
                    transaction: transaction.id.clone(),
                    reason: "serialization cycle across stale read and write ranges".to_string(),
                    conflicting_version: Some(committed.commit_version),
                });
            }
        }

        if let Some(edge) = dependencies
            .iter()
            .find(|edge| edge.kind == DependencyKind::ReadAfterWrite)
        {
            return Ok(IsolationDecision::Rejected {
                transaction: transaction.id.clone(),
                reason: "strict serializable read observed a stale range".to_string(),
                conflicting_version: self
                    .committed
                    .iter()
                    .find(|committed| committed.id == edge.from)
                    .map(|committed| committed.commit_version),
            });
        }

        let commit = IsolationCommit {
            id: transaction.id,
            read_version: transaction.read_version,
            commit_version,
            read_ranges: transaction.read_ranges,
            write_ranges: transaction.write_ranges,
            dependencies,
        };
        self.committed.push(commit.clone());
        Ok(IsolationDecision::Accepted(commit))
    }

    pub fn committed(&self) -> &[IsolationCommit] {
        &self.committed
    }

    pub fn dependency_count(&self) -> usize {
        self.committed
            .iter()
            .map(|commit| commit.dependencies.len())
            .sum()
    }
}

impl IsolationDecision {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted(_))
    }
}

fn validate_transaction(transaction: &IsolationTransaction) -> RsfResult<()> {
    if transaction.read_ranges.is_empty() && transaction.write_ranges.is_empty() {
        return Err(invalid(
            "isolation transaction must declare read or write ranges",
        ));
    }
    for range in transaction
        .read_ranges
        .iter()
        .chain(transaction.write_ranges.iter())
    {
        validate_range(range)?;
    }
    Ok(())
}

fn validate_range(range: &KeyRange) -> RsfResult<()> {
    if range.start >= range.end {
        return Err(invalid("key range start must be lower than key range end"));
    }
    Ok(())
}

fn overlaps_any<'a>(left: &'a [KeyRange], right: &[KeyRange]) -> Option<&'a KeyRange> {
    left.iter()
        .find(|left| right.iter().any(|right| ranges_overlap(left, right)))
}

fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    left.start < right.end && right.start < left.end
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_disjoint_strict_transactions() {
        let mut tracker = IsolationTracker::default();

        let first = tracker
            .commit(
                txn(1, 10, &[range(b"user/1", b"user/2")], &[]),
                CommitVersion(11),
            )
            .expect("first commit");
        let second = tracker
            .commit(
                txn(2, 10, &[range(b"zone/1", b"zone/2")], &[]),
                CommitVersion(12),
            )
            .expect("second commit");

        assert!(first.is_accepted());
        assert!(second.is_accepted());
        assert_eq!(tracker.committed().len(), 2);
    }

    #[test]
    fn rejects_stale_serializable_reads() {
        let mut tracker = IsolationTracker::default();
        tracker
            .commit(
                txn(1, 10, &[], &[range(b"user/1", b"user/2")]),
                CommitVersion(11),
            )
            .expect("writer");

        let stale = tracker
            .commit(
                txn(2, 10, &[range(b"user/1", b"user/2")], &[]),
                CommitVersion(12),
            )
            .expect("stale read decision");

        assert_eq!(
            stale,
            IsolationDecision::Rejected {
                transaction: TransactionId([2; 16]),
                reason: "strict serializable read observed a stale range".to_string(),
                conflicting_version: Some(CommitVersion(11))
            }
        );
        assert_eq!(tracker.committed().len(), 1);
    }

    #[test]
    fn rejects_write_skew_cycles() {
        let mut tracker = IsolationTracker::default();
        tracker
            .commit(
                txn(
                    1,
                    10,
                    &[range(b"account/a", b"account/b")],
                    &[range(b"account/b", b"account/c")],
                ),
                CommitVersion(11),
            )
            .expect("first");

        let skew = tracker
            .commit(
                txn(
                    2,
                    10,
                    &[range(b"account/b", b"account/c")],
                    &[range(b"account/a", b"account/b")],
                ),
                CommitVersion(12),
            )
            .expect("skew decision");

        assert_eq!(
            skew,
            IsolationDecision::Rejected {
                transaction: TransactionId([2; 16]),
                reason: "serialization cycle across stale read and write ranges".to_string(),
                conflicting_version: Some(CommitVersion(11))
            }
        );
    }

    #[test]
    fn rejects_write_write_conflicts_before_tracking_dependencies() {
        let mut tracker = IsolationTracker::default();
        tracker
            .commit(
                txn(1, 10, &[], &[range(b"user/1", b"user/2")]),
                CommitVersion(11),
            )
            .expect("first");

        let conflict = tracker
            .commit(
                txn(2, 10, &[], &[range(b"user/1", b"user/2")]),
                CommitVersion(12),
            )
            .expect("conflict decision");

        assert_eq!(
            conflict,
            IsolationDecision::Rejected {
                transaction: TransactionId([2; 16]),
                reason: "write/write conflict with newer committed transaction".to_string(),
                conflicting_version: Some(CommitVersion(11))
            }
        );
    }

    fn txn(
        id: u8,
        read_version: u64,
        read_ranges: &[KeyRange],
        write_ranges: &[KeyRange],
    ) -> IsolationTransaction {
        IsolationTransaction {
            id: TransactionId([id; 16]),
            read_version: CommitVersion(read_version),
            read_ranges: read_ranges.to_vec(),
            write_ranges: write_ranges.to_vec(),
        }
    }

    fn range(start: &[u8], end: &[u8]) -> KeyRange {
        KeyRange {
            start: start.to_vec(),
            end: end.to_vec(),
        }
    }
}
