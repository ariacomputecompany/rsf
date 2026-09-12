use crate::{Bytes, CommitEvidence, CommitVersion};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TransactionId(pub [u8; 16]);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRange {
    pub start: Bytes,
    pub end: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitIntent {
    pub id: TransactionId,
    pub read_conflicts: Vec<KeyRange>,
    pub write_conflicts: Vec<KeyRange>,
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Set { key: Bytes, value: Bytes },
    Clear { key: Bytes },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitResult {
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
}
