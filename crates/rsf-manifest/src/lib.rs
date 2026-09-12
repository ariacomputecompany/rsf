#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    CommitVersion, Digest, ErrorCode, NodeId, PlacementGroupId, RegionId, RsfError, RsfResult,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ManifestKind {
    RelationalPage,
    RelationalWal,
    OrderedKvSstable,
    ObjectPart,
    BlobChunk,
    SnapshotRoot,
    BackupBlock,
}

impl ManifestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelationalPage => "relational-page",
            Self::RelationalWal => "relational-wal",
            Self::OrderedKvSstable => "ordered-kv-sstable",
            Self::ObjectPart => "object-part",
            Self::BlobChunk => "blob-chunk",
            Self::SnapshotRoot => "snapshot-root",
            Self::BackupBlock => "backup-block",
        }
    }

    pub fn surface(self) -> &'static str {
        self.surface_kind().as_str()
    }

    pub fn surface_kind(self) -> ManifestSurface {
        match self {
            Self::RelationalPage | Self::RelationalWal => ManifestSurface::Sql,
            Self::OrderedKvSstable => ManifestSurface::Kv,
            Self::ObjectPart => ManifestSurface::Object,
            Self::BlobChunk => ManifestSurface::Blob,
            Self::SnapshotRoot => ManifestSurface::Snapshot,
            Self::BackupBlock => ManifestSurface::Backup,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ManifestSurface {
    Sql,
    Kv,
    Object,
    Blob,
    Snapshot,
    Backup,
}

impl ManifestSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Kv => "kv",
            Self::Object => "object",
            Self::Blob => "blob",
            Self::Snapshot => "snapshot",
            Self::Backup => "backup",
        }
    }

    fn object_prefix(self) -> &'static str {
        match self {
            Self::Sql => "sql/",
            Self::Kv => "kv/",
            Self::Object => "object/",
            Self::Blob => "blob/",
            Self::Snapshot => "snapshot/",
            Self::Backup => "backup/",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    pub offset: u64,
    pub len: u64,
}

impl ByteRange {
    pub fn new(offset: u64, len: u64) -> Self {
        Self { offset, len }
    }

    pub fn end(self) -> u64 {
        self.offset.saturating_add(self.len)
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.offset < other.end() && other.offset < self.end()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestEntry {
    pub kind: ManifestKind,
    pub namespace: String,
    pub object: String,
    pub version: CommitVersion,
    pub extent_id: Digest,
    pub placement_group: PlacementGroupId,
    pub range: ByteRange,
    pub checksum: Digest,
    pub provider: ProviderMetadata,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProviderStorageClass {
    Hot,
    Warm,
    Cold,
    Archive,
}

impl ProviderStorageClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Warm => "warm",
            Self::Cold => "cold",
            Self::Archive => "archive",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderReplica {
    pub node: NodeId,
    pub region: RegionId,
    pub storage_class: ProviderStorageClass,
}

impl ProviderReplica {
    pub fn new(node: NodeId, region: RegionId, storage_class: ProviderStorageClass) -> Self {
        Self {
            node,
            region,
            storage_class,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderMetadata {
    pub provider_id: String,
    pub min_replicas: usize,
    pub replicas: Vec<ProviderReplica>,
}

impl ProviderMetadata {
    pub fn new(
        provider_id: impl Into<String>,
        min_replicas: usize,
        replicas: Vec<ProviderReplica>,
    ) -> Self {
        Self {
            provider_id: provider_id.into(),
            min_replicas,
            replicas,
        }
    }

    pub fn local_for_placement_group(pg: PlacementGroupId) -> Self {
        Self::new(
            "local",
            1,
            vec![ProviderReplica::new(
                NodeId(pg.0),
                RegionId::new("local").expect("static local region id must be valid"),
                ProviderStorageClass::Hot,
            )],
        )
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn region_count(&self) -> usize {
        self.replicas
            .iter()
            .map(|replica| replica.region.as_str())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestChecksumProof {
    pub extent_id: Digest,
    pub checksum: Digest,
    pub bytes: u64,
}

impl ManifestChecksumProof {
    pub fn new(extent_id: Digest, checksum: Digest, bytes: u64) -> Self {
        Self {
            extent_id,
            checksum,
            bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestChecksumReport {
    pub checked: usize,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestExtentReplicaState {
    pub extent_id: Digest,
    pub node: NodeId,
    pub present: bool,
    pub checksum_ok: bool,
    pub bytes: u64,
    pub checksum: Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestRepairFindingKind {
    MissingExtent,
    MissingReplica,
    CorruptReplica,
    UnderReplicated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestRepairFinding {
    pub kind: ManifestRepairFindingKind,
    pub extent_id: Digest,
    pub object: String,
    pub placement_group: PlacementGroupId,
    pub node: Option<NodeId>,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestRepairAction {
    CopyExtentReplica {
        extent_id: Digest,
        object: String,
        placement_group: PlacementGroupId,
        from: NodeId,
        to: NodeId,
    },
    QuarantineExtentReplica {
        extent_id: Digest,
        object: String,
        placement_group: PlacementGroupId,
        node: NodeId,
    },
    RestoreExtentReplication {
        extent_id: Digest,
        object: String,
        placement_group: PlacementGroupId,
        healthy: usize,
        expected: usize,
        deficit: usize,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManifestRepairPlan {
    pub findings: Vec<ManifestRepairFinding>,
    pub actions: Vec<ManifestRepairAction>,
}

impl ManifestEntry {
    pub fn new(
        kind: ManifestKind,
        namespace: impl Into<String>,
        object: impl Into<String>,
        version: CommitVersion,
        extent_id: Digest,
        placement_group: PlacementGroupId,
        range: ByteRange,
        checksum: Digest,
    ) -> Self {
        Self {
            kind,
            namespace: namespace.into(),
            object: object.into(),
            version,
            extent_id,
            placement_group,
            range,
            checksum,
            provider: ProviderMetadata::local_for_placement_group(placement_group),
        }
    }

    pub fn with_provider_metadata(mut self, provider: ProviderMetadata) -> Self {
        self.provider = provider;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManifestDecision {
    Published {
        entries: usize,
        total_bytes: u64,
        surfaces: usize,
    },
    EntryVerified {
        kind: ManifestKind,
        object: String,
        placement_group: PlacementGroupId,
        bytes: u64,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentManifest {
    entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedManifest {
    surface: ManifestSurface,
    manifest: ExtentManifest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlManifest(TypedManifest);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvManifest(TypedManifest);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectManifest(TypedManifest);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobManifest(TypedManifest);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifest(TypedManifest);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupManifest(TypedManifest);

impl ExtentManifest {
    pub fn publish(entries: Vec<ManifestEntry>) -> ManifestDecision {
        match validate_entries(&entries) {
            Ok(()) => ManifestDecision::Published {
                entries: entries.len(),
                total_bytes: entries.iter().map(|entry| entry.range.len).sum(),
                surfaces: surface_count(&entries),
            },
            Err(reason) => ManifestDecision::Rejected { reason },
        }
    }

    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        validate_entries(&entries).map_err(invalid)?;
        Ok(Self { entries })
    }

    pub fn verify(&self) -> Vec<ManifestDecision> {
        self.entries
            .iter()
            .map(|entry| ManifestDecision::EntryVerified {
                kind: entry.kind,
                object: entry.object.clone(),
                placement_group: entry.placement_group,
                bytes: entry.range.len,
            })
            .collect()
    }

    pub fn entries(&self) -> &[ManifestEntry] {
        &self.entries
    }

    pub fn total_bytes(&self) -> u64 {
        self.entries.iter().map(|entry| entry.range.len).sum()
    }

    pub fn surface_count(&self) -> usize {
        surface_count(&self.entries)
    }

    pub fn verify_checksums(
        &self,
        proofs: &[ManifestChecksumProof],
    ) -> RsfResult<ManifestChecksumReport> {
        verify_manifest_checksums(&self.entries, proofs).map_err(invalid)
    }

    pub fn plan_repairs(
        &self,
        states: &[ManifestExtentReplicaState],
    ) -> RsfResult<ManifestRepairPlan> {
        plan_manifest_repairs(&self.entries, states).map_err(invalid)
    }
}

impl TypedManifest {
    pub fn new(surface: ManifestSurface, entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        validate_typed_entries(surface, &entries).map_err(invalid)?;
        Ok(Self {
            surface,
            manifest: ExtentManifest { entries },
        })
    }

    pub fn surface(&self) -> ManifestSurface {
        self.surface
    }

    pub fn entries(&self) -> &[ManifestEntry] {
        self.manifest.entries()
    }

    pub fn total_bytes(&self) -> u64 {
        self.manifest.total_bytes()
    }

    pub fn verify(&self) -> Vec<ManifestDecision> {
        self.manifest.verify()
    }

    pub fn into_extent_manifest(self) -> ExtentManifest {
        self.manifest
    }

    pub fn plan_repairs(
        &self,
        states: &[ManifestExtentReplicaState],
    ) -> RsfResult<ManifestRepairPlan> {
        self.manifest.plan_repairs(states)
    }
}

impl SqlManifest {
    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        Ok(Self(TypedManifest::new(ManifestSurface::Sql, entries)?))
    }

    pub fn typed(&self) -> &TypedManifest {
        &self.0
    }
}

impl KvManifest {
    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        Ok(Self(TypedManifest::new(ManifestSurface::Kv, entries)?))
    }

    pub fn typed(&self) -> &TypedManifest {
        &self.0
    }
}

impl ObjectManifest {
    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        Ok(Self(TypedManifest::new(ManifestSurface::Object, entries)?))
    }

    pub fn typed(&self) -> &TypedManifest {
        &self.0
    }
}

impl BlobManifest {
    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        Ok(Self(TypedManifest::new(ManifestSurface::Blob, entries)?))
    }

    pub fn typed(&self) -> &TypedManifest {
        &self.0
    }
}

impl SnapshotManifest {
    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        Ok(Self(TypedManifest::new(
            ManifestSurface::Snapshot,
            entries,
        )?))
    }

    pub fn typed(&self) -> &TypedManifest {
        &self.0
    }
}

impl BackupManifest {
    pub fn new(entries: Vec<ManifestEntry>) -> RsfResult<Self> {
        Ok(Self(TypedManifest::new(ManifestSurface::Backup, entries)?))
    }

    pub fn typed(&self) -> &TypedManifest {
        &self.0
    }
}

fn validate_entries(entries: &[ManifestEntry]) -> Result<(), String> {
    if entries.is_empty() {
        return Err("manifest must include at least one extent".to_string());
    }

    let mut extent_ids = BTreeSet::new();
    let mut ranges_by_object: BTreeMap<
        (ManifestKind, String, String, CommitVersion),
        Vec<ByteRange>,
    > = BTreeMap::new();

    for entry in entries {
        if entry.namespace.is_empty() {
            return Err("manifest entry namespace must not be empty".to_string());
        }
        if entry.object.is_empty() {
            return Err("manifest entry object must not be empty".to_string());
        }
        if !entry
            .object
            .starts_with(entry.kind.surface_kind().object_prefix())
        {
            return Err(format!(
                "manifest entry {} does not match {} surface",
                entry.object,
                entry.kind.surface()
            ));
        }
        if entry.extent_id.validate().is_err() {
            return Err(format!(
                "manifest entry {} has empty extent id",
                entry.object
            ));
        }
        if !extent_ids.insert(entry.extent_id) {
            return Err(format!(
                "manifest entry {} duplicates extent id",
                entry.object
            ));
        }
        if entry.range.len == 0 {
            return Err(format!(
                "manifest entry {} has empty byte range",
                entry.object
            ));
        }
        if entry.range.end() < entry.range.offset {
            return Err(format!(
                "manifest entry {} byte range overflows",
                entry.object
            ));
        }
        if entry.checksum.validate().is_err() {
            return Err(format!(
                "manifest entry {} has empty checksum",
                entry.object
            ));
        }
        validate_provider_metadata(entry)?;

        let key = (
            entry.kind,
            entry.namespace.clone(),
            entry.object.clone(),
            entry.version,
        );
        let ranges = ranges_by_object.entry(key).or_default();
        if ranges.iter().any(|existing| existing.overlaps(entry.range)) {
            return Err(format!(
                "manifest entry {} overlaps an existing byte range",
                entry.object
            ));
        }
        ranges.push(entry.range);
    }

    Ok(())
}

fn validate_typed_entries(
    surface: ManifestSurface,
    entries: &[ManifestEntry],
) -> Result<(), String> {
    validate_entries(entries)?;
    for entry in entries {
        if entry.kind.surface_kind() != surface {
            return Err(format!(
                "{} manifest cannot contain {} extent {}",
                surface.as_str(),
                entry.kind.as_str(),
                entry.object
            ));
        }
    }
    Ok(())
}

fn validate_provider_metadata(entry: &ManifestEntry) -> Result<(), String> {
    if entry.provider.provider_id.trim().is_empty() {
        return Err(format!(
            "manifest entry {} provider id must not be empty",
            entry.object
        ));
    }
    if entry.provider.min_replicas == 0 {
        return Err(format!(
            "manifest entry {} provider min replicas must be non-zero",
            entry.object
        ));
    }
    if entry.provider.replicas.is_empty() {
        return Err(format!(
            "manifest entry {} provider metadata must include at least one replica",
            entry.object
        ));
    }
    if entry.provider.min_replicas > entry.provider.replicas.len() {
        return Err(format!(
            "manifest entry {} provider metadata requires {} replicas but only has {}",
            entry.object,
            entry.provider.min_replicas,
            entry.provider.replicas.len()
        ));
    }

    let mut nodes = BTreeSet::new();
    let mut placements = BTreeSet::new();
    for replica in &entry.provider.replicas {
        if !nodes.insert(replica.node) {
            return Err(format!(
                "manifest entry {} provider metadata duplicates node {}",
                entry.object, replica.node.0
            ));
        }
        if !placements.insert((replica.node, replica.region.clone())) {
            return Err(format!(
                "manifest entry {} provider metadata duplicates placement",
                entry.object
            ));
        }
    }
    Ok(())
}

fn verify_manifest_checksums(
    entries: &[ManifestEntry],
    proofs: &[ManifestChecksumProof],
) -> Result<ManifestChecksumReport, String> {
    let mut proofs_by_extent = BTreeMap::new();
    for proof in proofs {
        if proof.extent_id.validate().is_err() {
            return Err("checksum proof has empty extent id".to_string());
        }
        if proof.checksum.validate().is_err() {
            return Err("checksum proof has empty checksum".to_string());
        }
        if proof.bytes == 0 {
            return Err("checksum proof has empty byte count".to_string());
        }
        if proofs_by_extent.insert(proof.extent_id, proof).is_some() {
            return Err("checksum proof duplicates extent id".to_string());
        }
    }

    let mut total_bytes = 0;
    for entry in entries {
        let proof = proofs_by_extent
            .get(&entry.extent_id)
            .ok_or_else(|| format!("manifest entry {} is missing checksum proof", entry.object))?;
        if proof.checksum != entry.checksum {
            return Err(format!(
                "manifest entry {} checksum proof does not match manifest checksum",
                entry.object
            ));
        }
        if proof.bytes != entry.range.len {
            return Err(format!(
                "manifest entry {} checksum proof byte count does not match manifest range",
                entry.object
            ));
        }
        total_bytes += proof.bytes;
    }

    Ok(ManifestChecksumReport {
        checked: entries.len(),
        total_bytes,
    })
}

fn plan_manifest_repairs(
    entries: &[ManifestEntry],
    states: &[ManifestExtentReplicaState],
) -> Result<ManifestRepairPlan, String> {
    validate_entries(entries)?;
    let mut states_by_extent = BTreeMap::<Digest, Vec<&ManifestExtentReplicaState>>::new();
    let mut seen = BTreeSet::new();
    for state in states {
        if state.extent_id.validate().is_err() {
            return Err("manifest repair state has empty extent id".to_string());
        }
        state.checksum.validate().map_err(|_| {
            format!(
                "manifest repair state for extent {} has empty checksum",
                state.extent_id
            )
        })?;
        if !seen.insert((state.extent_id, state.node)) {
            return Err(format!(
                "manifest repair state duplicates extent {} replica node {}",
                state.extent_id, state.node.0
            ));
        }
        states_by_extent
            .entry(state.extent_id)
            .or_default()
            .push(state);
    }

    let mut plan = ManifestRepairPlan::default();
    for entry in entries {
        let states = states_by_extent
            .get(&entry.extent_id)
            .cloned()
            .unwrap_or_default();
        let healthy_source = healthy_manifest_repair_source(entry, &states);
        if states.is_empty() {
            plan.findings.push(ManifestRepairFinding {
                kind: ManifestRepairFindingKind::MissingExtent,
                extent_id: entry.extent_id,
                object: entry.object.clone(),
                placement_group: entry.placement_group,
                node: None,
                detail: format!("manifest extent {} has no replica evidence", entry.object),
            });
        }

        for replica in &entry.provider.replicas {
            match states.iter().find(|state| state.node == replica.node) {
                None => {
                    push_missing_manifest_replica(&mut plan, entry, replica.node, healthy_source);
                }
                Some(state) if !state.present => {
                    push_missing_manifest_replica(&mut plan, entry, replica.node, healthy_source);
                }
                Some(state)
                    if !state.checksum_ok
                        || state.bytes != entry.range.len
                        || state.checksum != entry.checksum =>
                {
                    plan.findings.push(ManifestRepairFinding {
                        kind: ManifestRepairFindingKind::CorruptReplica,
                        extent_id: entry.extent_id,
                        object: entry.object.clone(),
                        placement_group: entry.placement_group,
                        node: Some(replica.node),
                        detail: format!(
                            "manifest extent {} replica {} failed checksum or byte validation",
                            entry.object, replica.node.0
                        ),
                    });
                    plan.actions
                        .push(ManifestRepairAction::QuarantineExtentReplica {
                            extent_id: entry.extent_id,
                            object: entry.object.clone(),
                            placement_group: entry.placement_group,
                            node: replica.node,
                        });
                    if let Some(source) = healthy_source
                        && source != replica.node
                    {
                        plan.actions.push(ManifestRepairAction::CopyExtentReplica {
                            extent_id: entry.extent_id,
                            object: entry.object.clone(),
                            placement_group: entry.placement_group,
                            from: source,
                            to: replica.node,
                        });
                    }
                }
                Some(_) => {}
            }
        }

        let healthy = states
            .iter()
            .filter(|state| manifest_replica_is_healthy(entry, state))
            .count();
        if healthy < entry.provider.min_replicas {
            let deficit = entry.provider.min_replicas - healthy;
            plan.findings.push(ManifestRepairFinding {
                kind: ManifestRepairFindingKind::UnderReplicated,
                extent_id: entry.extent_id,
                object: entry.object.clone(),
                placement_group: entry.placement_group,
                node: None,
                detail: format!(
                    "manifest extent {} has {healthy}/{} healthy replicas",
                    entry.object, entry.provider.min_replicas
                ),
            });
            plan.actions
                .push(ManifestRepairAction::RestoreExtentReplication {
                    extent_id: entry.extent_id,
                    object: entry.object.clone(),
                    placement_group: entry.placement_group,
                    healthy,
                    expected: entry.provider.min_replicas,
                    deficit,
                });
        }
    }
    Ok(plan)
}

fn push_missing_manifest_replica(
    plan: &mut ManifestRepairPlan,
    entry: &ManifestEntry,
    node: NodeId,
    source: Option<NodeId>,
) {
    plan.findings.push(ManifestRepairFinding {
        kind: ManifestRepairFindingKind::MissingReplica,
        extent_id: entry.extent_id,
        object: entry.object.clone(),
        placement_group: entry.placement_group,
        node: Some(node),
        detail: format!(
            "manifest extent {} is missing replica node {}",
            entry.object, node.0
        ),
    });
    if let Some(from) = source
        && from != node
    {
        plan.actions.push(ManifestRepairAction::CopyExtentReplica {
            extent_id: entry.extent_id,
            object: entry.object.clone(),
            placement_group: entry.placement_group,
            from,
            to: node,
        });
    }
}

fn healthy_manifest_repair_source(
    entry: &ManifestEntry,
    states: &[&ManifestExtentReplicaState],
) -> Option<NodeId> {
    states
        .iter()
        .filter(|state| manifest_replica_is_healthy(entry, state))
        .map(|state| state.node)
        .min()
}

fn manifest_replica_is_healthy(entry: &ManifestEntry, state: &ManifestExtentReplicaState) -> bool {
    state.present
        && state.checksum_ok
        && state.bytes == entry.range.len
        && state.checksum == entry.checksum
        && entry
            .provider
            .replicas
            .iter()
            .any(|replica| replica.node == state.node)
}

fn surface_count(entries: &[ManifestEntry]) -> usize {
    entries
        .iter()
        .map(|entry| entry.kind.surface())
        .collect::<BTreeSet<_>>()
        .len()
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publishes_mixed_database_manifest() {
        let manifest = ExtentManifest::new(entries()).unwrap();

        assert_eq!(manifest.entries().len(), 7);
        assert_eq!(manifest.total_bytes(), 25_088);
        assert_eq!(manifest.surface_count(), 6);
        assert_eq!(manifest.verify().len(), 7);
    }

    #[test]
    fn publish_reports_summary() {
        assert_eq!(
            ExtentManifest::publish(entries()),
            ManifestDecision::Published {
                entries: 7,
                total_bytes: 25_088,
                surfaces: 6
            }
        );
    }

    #[test]
    fn typed_manifests_validate_every_surface() {
        let sql = SqlManifest::new(vec![
            entry(
                ManifestKind::RelationalPage,
                "sql/app/users/page-0",
                10,
                0,
                4096,
            ),
            entry(
                ManifestKind::RelationalWal,
                "sql/app/users/redo",
                11,
                0,
                2048,
            ),
        ])
        .unwrap();
        let kv = KvManifest::new(vec![entry(
            ManifestKind::OrderedKvSstable,
            "kv/app/config/run-7",
            12,
            0,
            8192,
        )])
        .unwrap();
        let object = ObjectManifest::new(vec![entry(
            ManifestKind::ObjectPart,
            "object/media/hero.png/part-0",
            13,
            0,
            3072,
        )])
        .unwrap();
        let blob = BlobManifest::new(vec![entry(
            ManifestKind::BlobChunk,
            "blob/cid-hero/chunk-0",
            14,
            0,
            1024,
        )])
        .unwrap();
        let snapshot = SnapshotManifest::new(vec![entry(
            ManifestKind::SnapshotRoot,
            "snapshot/app/checkpoint-22",
            15,
            0,
            2560,
        )])
        .unwrap();
        let backup = BackupManifest::new(vec![entry(
            ManifestKind::BackupBlock,
            "backup/app/checkpoint-22/block-0",
            16,
            0,
            4096,
        )])
        .unwrap();

        assert_eq!(sql.typed().surface(), ManifestSurface::Sql);
        assert_eq!(sql.typed().entries().len(), 2);
        assert_eq!(kv.typed().surface(), ManifestSurface::Kv);
        assert_eq!(object.typed().surface(), ManifestSurface::Object);
        assert_eq!(blob.typed().surface(), ManifestSurface::Blob);
        assert_eq!(snapshot.typed().surface(), ManifestSurface::Snapshot);
        assert_eq!(backup.typed().surface(), ManifestSurface::Backup);
        assert_eq!(
            [
                sql.typed().verify().len(),
                kv.typed().verify().len(),
                object.typed().verify().len(),
                blob.typed().verify().len(),
                snapshot.typed().verify().len(),
                backup.typed().verify().len()
            ],
            [2, 1, 1, 1, 1, 1]
        );
    }

    #[test]
    fn typed_manifest_rejects_cross_surface_entry() {
        let err = ObjectManifest::new(vec![entry(
            ManifestKind::BlobChunk,
            "blob/cid-hero/chunk-0",
            31,
            0,
            1024,
        )])
        .unwrap_err();

        assert!(
            err.message
                .contains("object manifest cannot contain blob-chunk")
        );
    }

    #[test]
    fn generic_manifest_rejects_surface_prefix_mismatch() {
        let entries = vec![entry(
            ManifestKind::ObjectPart,
            "blob/cid-hero/chunk-0",
            32,
            0,
            1024,
        )];

        assert!(matches!(
            ExtentManifest::publish(entries),
            ManifestDecision::Rejected { reason } if reason.contains("does not match object surface")
        ));
    }

    #[test]
    fn rejects_duplicate_extent_ids() {
        let mut entries = entries();
        entries[1].extent_id = entries[0].extent_id;

        assert!(matches!(
            ExtentManifest::publish(entries),
            ManifestDecision::Rejected { reason } if reason.contains("duplicates extent id")
        ));
    }

    #[test]
    fn rejects_overlapping_ranges_for_same_object_version() {
        let mut entries = entries();
        entries[1].kind = entries[0].kind;
        entries[1].namespace = entries[0].namespace.clone();
        entries[1].object = entries[0].object.clone();
        entries[1].version = entries[0].version;
        entries[1].range = ByteRange::new(512, 256);

        assert!(matches!(
            ExtentManifest::publish(entries),
            ManifestDecision::Rejected { reason } if reason.contains("overlaps")
        ));
    }

    #[test]
    fn accepts_adjacent_ranges_for_same_object_version() {
        let entries = vec![
            entry(
                ManifestKind::BlobChunk,
                "blob/cid-hero/chunk-0",
                40,
                0,
                1024,
            ),
            entry(
                ManifestKind::BlobChunk,
                "blob/cid-hero/chunk-0",
                41,
                1024,
                2048,
            ),
        ];

        assert!(ExtentManifest::new(entries).is_ok());
    }

    #[test]
    fn rejects_empty_checksum() {
        let mut entries = entries();
        entries[0].checksum = Digest {
            algorithm: rsf_core::DigestAlgorithm::Blake3,
            bytes: [0_u8; 32],
        };

        assert!(matches!(
            ExtentManifest::publish(entries),
            ManifestDecision::Rejected { reason } if reason.contains("empty checksum")
        ));
    }

    #[test]
    fn manifest_entries_carry_valid_provider_metadata() {
        let manifest = ExtentManifest::new(entries()).unwrap();

        assert!(
            manifest
                .entries()
                .iter()
                .all(|entry| entry.provider.replica_count() >= entry.provider.min_replicas)
        );
        assert!(
            manifest
                .entries()
                .iter()
                .all(|entry| entry.provider.region_count() >= 1)
        );
    }

    #[test]
    fn manifest_repair_plan_copies_missing_chunks_and_quarantines_corrupt_replicas() {
        let checksum = Digest::blake3_bytes(b"blob chunk bytes");
        let entry = ManifestEntry::new(
            ManifestKind::BlobChunk,
            "media",
            "blob/cid-hero/chunk-0",
            CommitVersion(22),
            Digest::blake3_bytes(b"extent-hero-0"),
            PlacementGroupId(7),
            ByteRange::new(0, 16),
            checksum,
        )
        .with_provider_metadata(ProviderMetadata::new(
            "local-replicated",
            2,
            vec![
                ProviderReplica::new(
                    NodeId(1),
                    RegionId::new("iad").expect("region"),
                    ProviderStorageClass::Hot,
                ),
                ProviderReplica::new(
                    NodeId(2),
                    RegionId::new("iad").expect("region"),
                    ProviderStorageClass::Hot,
                ),
                ProviderReplica::new(
                    NodeId(3),
                    RegionId::new("sfo").expect("region"),
                    ProviderStorageClass::Warm,
                ),
            ],
        ));
        let manifest = BlobManifest::new(vec![entry.clone()]).expect("blob manifest");

        let plan = manifest
            .typed()
            .plan_repairs(&[
                ManifestExtentReplicaState {
                    extent_id: entry.extent_id,
                    node: NodeId(1),
                    present: true,
                    checksum_ok: true,
                    bytes: entry.range.len,
                    checksum,
                },
                ManifestExtentReplicaState {
                    extent_id: entry.extent_id,
                    node: NodeId(2),
                    present: true,
                    checksum_ok: false,
                    bytes: entry.range.len,
                    checksum: Digest::blake3_bytes(b"wrong bytes"),
                },
            ])
            .expect("repair plan");

        assert_eq!(plan.findings.len(), 3);
        assert!(plan.findings.iter().any(|finding| {
            finding.kind == ManifestRepairFindingKind::CorruptReplica
                && finding.node == Some(NodeId(2))
        }));
        assert!(plan.findings.iter().any(|finding| {
            finding.kind == ManifestRepairFindingKind::MissingReplica
                && finding.node == Some(NodeId(3))
        }));
        assert!(plan.findings.iter().any(|finding| {
            finding.kind == ManifestRepairFindingKind::UnderReplicated && finding.node.is_none()
        }));
        assert!(
            plan.actions
                .contains(&ManifestRepairAction::QuarantineExtentReplica {
                    extent_id: entry.extent_id,
                    object: entry.object.clone(),
                    placement_group: entry.placement_group,
                    node: NodeId(2),
                })
        );
        assert!(
            plan.actions
                .contains(&ManifestRepairAction::CopyExtentReplica {
                    extent_id: entry.extent_id,
                    object: entry.object.clone(),
                    placement_group: entry.placement_group,
                    from: NodeId(1),
                    to: NodeId(2),
                })
        );
        assert!(
            plan.actions
                .contains(&ManifestRepairAction::CopyExtentReplica {
                    extent_id: entry.extent_id,
                    object: entry.object.clone(),
                    placement_group: entry.placement_group,
                    from: NodeId(1),
                    to: NodeId(3),
                })
        );
        assert!(
            plan.actions
                .contains(&ManifestRepairAction::RestoreExtentReplication {
                    extent_id: entry.extent_id,
                    object: entry.object.clone(),
                    placement_group: entry.placement_group,
                    healthy: 1,
                    expected: 2,
                    deficit: 1,
                })
        );
    }

    #[test]
    fn manifest_repair_plan_rejects_duplicate_replica_evidence() {
        let manifest = BlobManifest::new(vec![entry(
            ManifestKind::BlobChunk,
            "blob/cid-hero/chunk-0",
            51,
            0,
            1024,
        )])
        .expect("blob manifest");
        let entry = manifest.typed().entries().first().expect("entry");
        let state = ManifestExtentReplicaState {
            extent_id: entry.extent_id,
            node: NodeId(1),
            present: true,
            checksum_ok: true,
            bytes: entry.range.len,
            checksum: entry.checksum,
        };

        let err = manifest
            .typed()
            .plan_repairs(&[state.clone(), state])
            .expect_err("duplicate state should reject");

        assert!(err.message.contains("duplicates"));
    }

    #[test]
    fn rejects_provider_metadata_with_unsatisfied_replica_requirement() {
        let mut entries = entries();
        entries[0].provider.min_replicas = entries[0].provider.replicas.len() + 1;

        assert!(matches!(
            ExtentManifest::publish(entries),
            ManifestDecision::Rejected { reason } if reason.contains("requires")
        ));
    }

    #[test]
    fn rejects_provider_metadata_with_duplicate_nodes() {
        let mut entries = entries();
        let replica = entries[0].provider.replicas[0].clone();
        entries[0].provider.replicas.push(replica);

        assert!(matches!(
            ExtentManifest::publish(entries),
            ManifestDecision::Rejected { reason } if reason.contains("duplicates node")
        ));
    }

    #[test]
    fn verifies_manifest_checksum_proofs() {
        let manifest = ExtentManifest::new(entries()).unwrap();
        let report = manifest
            .verify_checksums(&checksum_proofs(&manifest))
            .unwrap();

        assert_eq!(report.checked, manifest.entries().len());
        assert_eq!(report.total_bytes, manifest.total_bytes());
    }

    #[test]
    fn rejects_manifest_checksum_mismatch() {
        let manifest = ExtentManifest::new(entries()).unwrap();
        let mut proofs = checksum_proofs(&manifest);
        proofs[0].checksum = Digest::blake3_bytes(b"wrong checksum");

        let err = manifest.verify_checksums(&proofs).unwrap_err();

        assert!(err.message.contains("checksum proof does not match"));
    }

    #[test]
    fn rejects_manifest_checksum_missing_proof() {
        let manifest = ExtentManifest::new(entries()).unwrap();
        let mut proofs = checksum_proofs(&manifest);
        proofs.pop();

        let err = manifest.verify_checksums(&proofs).unwrap_err();

        assert!(err.message.contains("missing checksum proof"));
    }

    fn entries() -> Vec<ManifestEntry> {
        vec![
            entry(
                ManifestKind::RelationalPage,
                "sql/app/users/page-0",
                1,
                0,
                4096,
            ),
            entry(
                ManifestKind::RelationalWal,
                "sql/app/users/redo",
                2,
                0,
                2048,
            ),
            entry(
                ManifestKind::OrderedKvSstable,
                "kv/app/config/run-7",
                3,
                0,
                8192,
            ),
            entry(
                ManifestKind::ObjectPart,
                "object/media/hero.png/part-0",
                4,
                0,
                3072,
            ),
            entry(ManifestKind::BlobChunk, "blob/cid-hero/chunk-0", 5, 0, 1024),
            entry(
                ManifestKind::SnapshotRoot,
                "snapshot/app/checkpoint-22",
                6,
                0,
                2560,
            ),
            entry(
                ManifestKind::BackupBlock,
                "backup/app/checkpoint-22/block-0",
                7,
                0,
                4096,
            ),
        ]
    }

    fn entry(kind: ManifestKind, object: &str, seed: u8, offset: u64, len: u64) -> ManifestEntry {
        ManifestEntry::new(
            kind,
            "app",
            object,
            CommitVersion(22),
            Digest::blake3_bytes(format!("extent:{object}:{seed}").as_bytes()),
            PlacementGroupId(u64::from(seed % 3) + 1),
            ByteRange::new(offset, len),
            Digest::blake3_bytes(format!("checksum:{object}:{seed}:{len}").as_bytes()),
        )
    }

    fn checksum_proofs(manifest: &ExtentManifest) -> Vec<ManifestChecksumProof> {
        manifest
            .entries()
            .iter()
            .map(|entry| {
                ManifestChecksumProof::new(entry.extent_id, entry.checksum, entry.range.len)
            })
            .collect()
    }
}
