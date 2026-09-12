use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RegionId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const PROVIDER_PG: PlacementGroupId = PlacementGroupId(41);
const PROVIDER_WAL_MAGIC: &[u8; 7] = b"RSFPRV1";
const PROVIDER_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkSurface {
    Object,
    Blob,
}

impl ChunkSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Blob => "blob",
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            Self::Object => 1,
            Self::Blob => 2,
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Object),
            2 => Some(Self::Blob),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderRecord {
    pub content_id: String,
    pub surface: ChunkSurface,
    pub node: NodeId,
    pub region: RegionId,
    pub available: bool,
    pub verified: bool,
    pub latency_ms: u16,
}

impl ProviderRecord {
    pub fn new(
        content_id: impl Into<String>,
        surface: ChunkSurface,
        node: NodeId,
        region: RegionId,
        latency_ms: u16,
    ) -> Self {
        Self {
            content_id: content_id.into(),
            surface,
            node,
            region,
            available: true,
            verified: true,
            latency_ms,
        }
    }

    pub fn try_new(
        content_id: impl Into<String>,
        surface: ChunkSurface,
        node: NodeId,
        region: impl Into<String>,
        latency_ms: u16,
    ) -> RsfResult<Self> {
        Ok(Self::new(
            content_id,
            surface,
            node,
            RegionId::new(region)?,
            latency_ms,
        ))
    }

    pub fn unavailable(mut self) -> Self {
        self.available = false;
        self
    }

    pub fn unverified(mut self) -> Self {
        self.verified = false;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderQuery {
    pub content_id: String,
    pub client_region: RegionId,
    pub min_replicas: usize,
    pub limit: usize,
}

impl ProviderQuery {
    pub fn new(
        content_id: impl Into<String>,
        client_region: RegionId,
        min_replicas: usize,
        limit: usize,
    ) -> Self {
        Self {
            content_id: content_id.into(),
            client_region,
            min_replicas,
            limit,
        }
    }

    pub fn try_new(
        content_id: impl Into<String>,
        client_region: impl Into<String>,
        min_replicas: usize,
        limit: usize,
    ) -> RsfResult<Self> {
        Ok(Self::new(
            content_id,
            RegionId::new(client_region)?,
            min_replicas,
            limit,
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderDecision {
    Selected(ProviderSelection),
    Degraded(ProviderSelection),
    Rejected { content_id: String, reason: String },
}

impl ProviderDecision {
    pub fn selected_len(&self) -> usize {
        match self {
            Self::Selected(selection) | Self::Degraded(selection) => selection.providers.len(),
            Self::Rejected { .. } => 0,
        }
    }

    pub fn is_degraded(&self) -> bool {
        matches!(self, Self::Degraded(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSelection {
    pub content_id: String,
    pub surface: ChunkSurface,
    pub providers: Vec<ProviderRecord>,
    pub requested_replicas: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ProviderDirectory {
    records: BTreeMap<String, Vec<ProviderRecord>>,
}

impl ProviderDirectory {
    pub fn advertise(&mut self, record: ProviderRecord) -> Result<(), String> {
        validate_record(&record)?;
        let records = self.records.entry(record.content_id.clone()).or_default();
        if records.iter().any(|existing| {
            existing.node == record.node
                && existing.surface == record.surface
                && existing.region == record.region
        }) {
            return Err(format!(
                "duplicate provider node={} content={}",
                record.node.0, record.content_id
            ));
        }
        records.push(record);
        Ok(())
    }

    pub fn update_health(
        &mut self,
        content_id: &str,
        surface: ChunkSurface,
        node: NodeId,
        region: &RegionId,
        available: bool,
        verified: bool,
    ) -> Result<(), String> {
        let Some(record) = self.find_record_mut(content_id, surface, node, region) else {
            return Err(format!(
                "provider node={} content={} surface={} region={} is not advertised",
                node.0,
                content_id,
                surface.as_str(),
                region.as_str()
            ));
        };
        record.available = available;
        record.verified = verified;
        Ok(())
    }

    pub fn select(&self, query: &ProviderQuery) -> ProviderDecision {
        if query.content_id.is_empty() {
            return ProviderDecision::Rejected {
                content_id: query.content_id.clone(),
                reason: "empty content id".to_string(),
            };
        }
        if query.min_replicas == 0 || query.limit == 0 {
            return ProviderDecision::Rejected {
                content_id: query.content_id.clone(),
                reason: "replica requirements must be non-zero".to_string(),
            };
        }

        let Some(records) = self.records.get(&query.content_id) else {
            return ProviderDecision::Rejected {
                content_id: query.content_id.clone(),
                reason: "content not advertised".to_string(),
            };
        };

        let mut candidates = records
            .iter()
            .filter(|record| record.available && record.verified)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|record| {
            (
                record.region != query.client_region,
                record.latency_ms,
                record.node.0,
            )
        });
        candidates.truncate(query.limit);

        let Some(surface) = records.first().map(|record| record.surface) else {
            return ProviderDecision::Rejected {
                content_id: query.content_id.clone(),
                reason: "content has no providers".to_string(),
            };
        };
        let selection = ProviderSelection {
            content_id: query.content_id.clone(),
            surface,
            providers: candidates,
            requested_replicas: query.min_replicas,
        };
        if selection.providers.is_empty() {
            ProviderDecision::Rejected {
                content_id: query.content_id.clone(),
                reason: "no healthy verified providers".to_string(),
            }
        } else if selection.providers.len() < query.min_replicas {
            ProviderDecision::Degraded(selection)
        } else {
            ProviderDecision::Selected(selection)
        }
    }

    pub fn provider_count(&self) -> usize {
        self.records.values().map(Vec::len).sum()
    }

    pub fn content_count(&self) -> usize {
        self.records.len()
    }

    pub fn region_count(&self) -> usize {
        self.records
            .values()
            .flatten()
            .map(|record| record.region.as_str())
            .collect::<BTreeSet<_>>()
            .len()
    }

    fn find_record_mut(
        &mut self,
        content_id: &str,
        surface: ChunkSurface,
        node: NodeId,
        region: &RegionId,
    ) -> Option<&mut ProviderRecord> {
        self.records.get_mut(content_id)?.iter_mut().find(|record| {
            record.surface == surface && record.node == node && record.region == *region
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProviderWalRecord {
    Advertise(ProviderRecord),
    Health {
        content_id: String,
        surface: ChunkSurface,
        node: NodeId,
        region: RegionId,
        available: bool,
        verified: bool,
    },
}

#[derive(Debug)]
pub struct DurableProviderDirectory {
    directory: ProviderDirectory,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableProviderDirectory {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(PROVIDER_PG),
        })?;
        let mut directory = ProviderDirectory::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Provider)
        {
            apply_provider_record(
                &mut directory,
                decode_provider_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            directory,
            store,
            epoch,
            next_index,
        })
    }

    pub fn advertise(&mut self, record: ProviderRecord) -> RsfResult<()> {
        let mut directory = self.directory.clone();
        directory.advertise(record.clone()).map_err(invalid)?;
        self.append_record(&ProviderWalRecord::Advertise(record))?;
        self.directory = directory;
        Ok(())
    }

    pub fn update_health(
        &mut self,
        content_id: &str,
        surface: ChunkSurface,
        node: NodeId,
        region: &RegionId,
        available: bool,
        verified: bool,
    ) -> RsfResult<()> {
        let mut directory = self.directory.clone();
        directory
            .update_health(content_id, surface, node, region, available, verified)
            .map_err(invalid)?;
        self.append_record(&ProviderWalRecord::Health {
            content_id: content_id.to_string(),
            surface,
            node,
            region: region.clone(),
            available,
            verified,
        })?;
        self.directory = directory;
        Ok(())
    }

    pub fn select(&self, query: &ProviderQuery) -> ProviderDecision {
        self.directory.select(query)
    }

    pub fn directory(&self) -> &ProviderDirectory {
        &self.directory
    }

    pub fn provider_count(&self) -> usize {
        self.directory.provider_count()
    }

    pub fn content_count(&self) -> usize {
        self.directory.content_count()
    }

    pub fn region_count(&self) -> usize {
        self.directory.region_count()
    }

    fn append_record(&mut self, record: &ProviderWalRecord) -> RsfResult<()> {
        let payload = encode_provider_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(PROVIDER_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: PROVIDER_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Provider,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn apply_provider_record(
    directory: &mut ProviderDirectory,
    record: ProviderWalRecord,
) -> RsfResult<()> {
    match record {
        ProviderWalRecord::Advertise(record) => directory.advertise(record).map_err(invalid),
        ProviderWalRecord::Health {
            content_id,
            surface,
            node,
            region,
            available,
            verified,
        } => directory
            .update_health(&content_id, surface, node, &region, available, verified)
            .map_err(invalid),
    }
}

fn encode_provider_wal_payload(record: &ProviderWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(PROVIDER_WAL_MAGIC);
    out.push(PROVIDER_WAL_VERSION);
    match record {
        ProviderWalRecord::Advertise(record) => {
            out.push(1);
            encode_provider_record(record, &mut out)?;
        }
        ProviderWalRecord::Health {
            content_id,
            surface,
            node,
            region,
            available,
            verified,
        } => {
            out.push(2);
            write_string(content_id, &mut out)?;
            out.push(surface.to_u8());
            out.extend_from_slice(&node.0.to_le_bytes());
            write_string(region.as_str(), &mut out)?;
            out.push(u8::from(*available));
            out.push(u8::from(*verified));
        }
    }
    Ok(out)
}

fn encode_provider_record(record: &ProviderRecord, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_record(record).map_err(invalid)?;
    write_string(&record.content_id, out)?;
    out.push(record.surface.to_u8());
    out.extend_from_slice(&record.node.0.to_le_bytes());
    write_string(record.region.as_str(), out)?;
    out.push(u8::from(record.available));
    out.push(u8::from(record.verified));
    out.extend_from_slice(&record.latency_ms.to_le_bytes());
    Ok(())
}

fn decode_provider_wal_payload(bytes: &[u8]) -> RsfResult<ProviderWalRecord> {
    if bytes.len() < PROVIDER_WAL_MAGIC.len() + 2 {
        return Err(corrupt("provider WAL payload is too short"));
    }
    if &bytes[..PROVIDER_WAL_MAGIC.len()] != PROVIDER_WAL_MAGIC {
        return Err(corrupt("provider WAL payload has invalid magic"));
    }
    let mut offset = PROVIDER_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != PROVIDER_WAL_VERSION {
        return Err(corrupt(format!(
            "provider WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => ProviderWalRecord::Advertise(decode_provider_record(bytes, &mut offset)?),
        2 => ProviderWalRecord::Health {
            content_id: read_string(bytes, &mut offset)?,
            surface: decode_surface(read_u8(bytes, &mut offset)?)?,
            node: NodeId(read_u64(bytes, &mut offset)?),
            region: RegionId::new(read_string(bytes, &mut offset)?)
                .map_err(|err| corrupt(err.message))?,
            available: read_bool(bytes, &mut offset)?,
            verified: read_bool(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "provider WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("provider WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn decode_provider_record(bytes: &[u8], offset: &mut usize) -> RsfResult<ProviderRecord> {
    let record = ProviderRecord {
        content_id: read_string(bytes, offset)?,
        surface: decode_surface(read_u8(bytes, offset)?)?,
        node: NodeId(read_u64(bytes, offset)?),
        region: RegionId::new(read_string(bytes, offset)?).map_err(|err| corrupt(err.message))?,
        available: read_bool(bytes, offset)?,
        verified: read_bool(bytes, offset)?,
        latency_ms: read_u16(bytes, offset)?,
    };
    validate_record(&record).map_err(invalid)?;
    Ok(record)
}

fn decode_surface(value: u8) -> RsfResult<ChunkSurface> {
    ChunkSurface::from_u8(value)
        .ok_or_else(|| corrupt(format!("unknown provider surface id {value}")))
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| invalid("provider string is too large for WAL payload"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("provider string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("provider WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec())
        .map_err(|_| corrupt("provider WAL payload contains invalid UTF-8"))
}

fn read_bool(bytes: &[u8], offset: &mut usize) -> RsfResult<bool> {
    match read_u8(bytes, offset)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(corrupt(format!("provider bool value {other} is invalid"))),
    }
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("provider WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    let raw = bytes
        .get(*offset..offset.saturating_add(2))
        .ok_or_else(|| corrupt("provider WAL payload ended while reading u16"))?;
    *offset = offset.saturating_add(2);
    Ok(u16::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("provider WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("provider WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

fn validate_record(record: &ProviderRecord) -> Result<(), String> {
    if record.content_id.is_empty() {
        return Err("provider content id must not be empty".to_string());
    }
    if record.region.as_str().is_empty() {
        return Err("provider region must not be empty".to_string());
    }
    if record.latency_ms == 0 {
        return Err("provider latency must be non-zero".to_string());
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(value: &str) -> RegionId {
        RegionId::new(value).expect("region")
    }

    #[test]
    fn selects_same_region_verified_providers_first() {
        let mut directory = ProviderDirectory::default();
        directory
            .advertise(ProviderRecord::new(
                "sha256:hero",
                ChunkSurface::Object,
                NodeId(1),
                r("sfo"),
                7,
            ))
            .unwrap();
        directory
            .advertise(ProviderRecord::new(
                "sha256:hero",
                ChunkSurface::Object,
                NodeId(2),
                r("iad"),
                2,
            ))
            .unwrap();

        let decision = directory.select(&ProviderQuery::new("sha256:hero", r("sfo"), 1, 2));

        let ProviderDecision::Selected(selection) = decision else {
            panic!("expected selected provider decision");
        };
        assert_eq!(selection.providers[0].node, NodeId(1));
    }

    #[test]
    fn degrades_when_replica_requirement_cannot_be_met() {
        let mut directory = ProviderDirectory::default();
        directory
            .advertise(ProviderRecord::new(
                "sha256:hero",
                ChunkSurface::Blob,
                NodeId(1),
                r("sfo"),
                7,
            ))
            .unwrap();
        directory
            .advertise(
                ProviderRecord::new("sha256:hero", ChunkSurface::Blob, NodeId(2), r("iad"), 9)
                    .unavailable(),
            )
            .unwrap();

        let decision = directory.select(&ProviderQuery::new("sha256:hero", r("sfo"), 2, 2));

        assert!(decision.is_degraded());
        assert_eq!(decision.selected_len(), 1);
    }

    #[test]
    fn rejects_unhealthy_or_unknown_content() {
        let mut directory = ProviderDirectory::default();
        directory
            .advertise(
                ProviderRecord::new("sha256:cold", ChunkSurface::Object, NodeId(7), r("lhr"), 22)
                    .unverified(),
            )
            .unwrap();

        assert!(matches!(
            directory.select(&ProviderQuery::new("sha256:cold", r("lhr"), 1, 1)),
            ProviderDecision::Rejected { .. }
        ));
        assert!(matches!(
            directory.select(&ProviderQuery::new("sha256:missing", r("lhr"), 1, 1)),
            ProviderDecision::Rejected { .. }
        ));
    }

    #[test]
    fn rejects_invalid_query_region_at_construction() {
        assert!(ProviderQuery::try_new("sha256:hero", "bad region", 1, 1).is_err());
    }

    #[test]
    fn durable_directory_recovers_advertisements_and_health() {
        let path = test_path("durable_directory_recovers_advertisements_and_health");
        let sfo = r("sfo");
        let iad = r("iad");

        {
            let mut directory =
                DurableProviderDirectory::open(&path, Epoch(7)).expect("open durable provider");
            directory
                .advertise(ProviderRecord::new(
                    "sha256:hero",
                    ChunkSurface::Object,
                    NodeId(1),
                    sfo.clone(),
                    7,
                ))
                .expect("advertise sfo");
            directory
                .advertise(ProviderRecord::new(
                    "sha256:hero",
                    ChunkSurface::Object,
                    NodeId(2),
                    iad.clone(),
                    3,
                ))
                .expect("advertise iad");
            directory
                .update_health(
                    "sha256:hero",
                    ChunkSurface::Object,
                    NodeId(1),
                    &sfo,
                    false,
                    true,
                )
                .expect("mark local unavailable");
        }

        let recovered =
            DurableProviderDirectory::open(&path, Epoch(7)).expect("recover durable provider");
        assert_eq!(recovered.provider_count(), 2);
        assert_eq!(recovered.content_count(), 1);
        assert_eq!(recovered.region_count(), 2);

        let decision = recovered.select(&ProviderQuery::new("sha256:hero", sfo, 1, 2));
        let ProviderDecision::Selected(selection) = decision else {
            panic!("expected recovered provider selection");
        };
        assert_eq!(selection.providers.len(), 1);
        assert_eq!(selection.providers[0].node, NodeId(2));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_directory_rejects_health_for_unknown_provider() {
        let path = test_path("durable_directory_rejects_health_for_unknown_provider");
        let mut directory =
            DurableProviderDirectory::open(&path, Epoch(7)).expect("open durable provider");
        let err = directory
            .update_health(
                "sha256:missing",
                ChunkSurface::Blob,
                NodeId(9),
                &r("lhr"),
                true,
                true,
            )
            .expect_err("unknown provider health update should reject");

        assert!(err.message.contains("is not advertised"));
        assert_eq!(directory.provider_count(), 0);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn provider_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_provider_wal_payload(&ProviderWalRecord::Advertise(
            ProviderRecord::new("sha256:hero", ChunkSurface::Blob, NodeId(1), r("sfo"), 7),
        ))
        .expect("encode");
        payload.push(99);

        let err = decode_provider_wal_payload(&payload).expect_err("trailing bytes reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-provider-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
