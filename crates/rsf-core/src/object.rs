use crate::{Bytes, CommitEvidence, CommitVersion, blob::ContentId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketName(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectKey(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectVersion {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub kind: ObjectVersionKind,
    pub version: CommitVersion,
    pub evidence: CommitEvidence,
    pub manifest: ObjectManifest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectVersionKind {
    Live,
    DeleteMarker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectManifest {
    pub parts: Vec<ObjectPart>,
    pub metadata: Vec<(String, Bytes)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPart {
    pub offset: u64,
    pub len: u64,
    pub content: ContentId,
}
