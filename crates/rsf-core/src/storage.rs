use crate::{Bytes, Digest, Epoch, LogIndex, PlacementGroupId, RsfResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    pub pg: PlacementGroupId,
    pub epoch: Epoch,
    pub index: LogIndex,
    pub kind: WalRecordKind,
    pub previous_hash: [u8; 32],
    pub payload: Bytes,
    pub payload_hash: [u8; 32],
    pub record_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalRecordKind {
    Prepare,
    Commit,
    Snapshot,
    Membership,
    BlobManifest,
    ObjectManifest,
    KvMutation,
    RelationalMutation,
    Policy,
    Transaction,
    SessionTransaction,
    Provider,
    Quota,
    Lease,
    Job,
    Migration,
    Upgrade,
    Idempotency,
    Auth,
    Tenancy,
    Health,
    Stats,
    Retention,
    Repair,
    Rebalance,
    Backup,
    Cache,
    Compaction,
    Rpc,
    Deployment,
    RelationalWal,
    Stream,
    Watch,
    Observability,
    Superblock,
    Recovery,
    Range,
    Manifest,
    /// A durable consensus promise must never be ignored by older readers.
    ViewPromise,
    ViewInstallation,
}

impl WalRecordKind {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Prepare => 1,
            Self::Commit => 2,
            Self::Snapshot => 3,
            Self::Membership => 4,
            Self::BlobManifest => 5,
            Self::ObjectManifest => 6,
            Self::KvMutation => 7,
            Self::RelationalMutation => 8,
            Self::Policy => 9,
            Self::Transaction => 10,
            Self::SessionTransaction => 11,
            Self::Provider => 12,
            Self::Quota => 13,
            Self::Lease => 14,
            Self::Job => 15,
            Self::Migration => 16,
            Self::Upgrade => 17,
            Self::Idempotency => 18,
            Self::Auth => 19,
            Self::Tenancy => 20,
            Self::Health => 21,
            Self::Stats => 22,
            Self::Retention => 23,
            Self::Repair => 24,
            Self::Rebalance => 25,
            Self::Backup => 26,
            Self::Cache => 27,
            Self::Compaction => 28,
            Self::Rpc => 29,
            Self::Deployment => 30,
            Self::RelationalWal => 31,
            Self::Stream => 32,
            Self::Watch => 33,
            Self::Observability => 34,
            Self::Superblock => 35,
            Self::Recovery => 36,
            Self::Range => 37,
            Self::Manifest => 38,
            Self::ViewPromise => 39,
            Self::ViewInstallation => 40,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Prepare),
            2 => Some(Self::Commit),
            3 => Some(Self::Snapshot),
            4 => Some(Self::Membership),
            5 => Some(Self::BlobManifest),
            6 => Some(Self::ObjectManifest),
            7 => Some(Self::KvMutation),
            8 => Some(Self::RelationalMutation),
            9 => Some(Self::Policy),
            10 => Some(Self::Transaction),
            11 => Some(Self::SessionTransaction),
            12 => Some(Self::Provider),
            13 => Some(Self::Quota),
            14 => Some(Self::Lease),
            15 => Some(Self::Job),
            16 => Some(Self::Migration),
            17 => Some(Self::Upgrade),
            18 => Some(Self::Idempotency),
            19 => Some(Self::Auth),
            20 => Some(Self::Tenancy),
            21 => Some(Self::Health),
            22 => Some(Self::Stats),
            23 => Some(Self::Retention),
            24 => Some(Self::Repair),
            25 => Some(Self::Rebalance),
            26 => Some(Self::Backup),
            27 => Some(Self::Cache),
            28 => Some(Self::Compaction),
            29 => Some(Self::Rpc),
            30 => Some(Self::Deployment),
            31 => Some(Self::RelationalWal),
            32 => Some(Self::Stream),
            33 => Some(Self::Watch),
            34 => Some(Self::Observability),
            35 => Some(Self::Superblock),
            36 => Some(Self::Recovery),
            37 => Some(Self::Range),
            38 => Some(Self::Manifest),
            39 => Some(Self::ViewPromise),
            40 => Some(Self::ViewInstallation),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentId(pub Digest);

impl ExtentId {
    pub fn new(digest: Digest) -> Self {
        Self(digest)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentManifest {
    pub id: ExtentId,
    pub len: u64,
    pub checksum: Digest,
    pub placement_group: PlacementGroupId,
}

impl ExtentManifest {
    pub fn new(
        id: ExtentId,
        len: u64,
        checksum: Digest,
        placement_group: PlacementGroupId,
    ) -> RsfResult<Self> {
        checksum.validate()?;
        if len == 0 {
            return Err(crate::RsfError::new(
                crate::ErrorCode::InvalidInput,
                "extent length must be non-zero",
            ));
        }
        Ok(Self {
            id,
            len,
            checksum,
            placement_group,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LayoutKind {
    RelationalPage,
    RelationalWal,
    OrderedKvRange,
    ObjectPart,
    BlobChunk,
    AppendLogSegment,
    BlockStripe,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DigestAlgorithm, ErrorCode};

    #[test]
    fn extent_manifest_carries_algorithm_tagged_digest() {
        let digest = Digest::new(DigestAlgorithm::Blake3, [3_u8; 32]).expect("digest");
        let manifest =
            ExtentManifest::new(ExtentId::new(digest), 4096, digest, PlacementGroupId(7))
                .expect("manifest");

        assert_eq!(manifest.checksum.algorithm, DigestAlgorithm::Blake3);
        assert_eq!(manifest.placement_group, PlacementGroupId(7));
    }

    #[test]
    fn extent_manifest_rejects_empty_length() {
        let digest = Digest::blake3([1_u8; 32]).expect("digest");
        let err = ExtentManifest::new(ExtentId::new(digest), 0, digest, PlacementGroupId(1))
            .expect_err("zero length rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }
}
