use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, NodeId, PlacementGroupId, RegionId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

use crate::{
    DeploymentManifest, DeploymentPlanner, DeploymentProfile, DeploymentRole, ProbeSpec,
    RoleAssignment, ServiceEndpoint, VolumeSpec,
};

const DEPLOYMENT_PG: PlacementGroupId = PlacementGroupId(58);
const DEPLOYMENT_WAL_MAGIC: &[u8; 7] = b"RSFDPL1";
const DEPLOYMENT_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DeploymentRolloutId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentRolloutState {
    Planned,
    Started,
    Applied,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentRollout {
    pub id: DeploymentRolloutId,
    pub profile: DeploymentProfile,
    pub manifest: DeploymentManifest,
    pub state: DeploymentRolloutState,
    pub failure: Option<String>,
}

#[derive(Debug)]
pub struct DurableDeploymentLedger {
    rollouts: BTreeMap<DeploymentRolloutId, DeploymentRollout>,
    planner: DeploymentPlanner,
    store: SegmentStore,
    epoch: Epoch,
    next_id: u64,
    next_index: u64,
}

impl DurableDeploymentLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(DEPLOYMENT_PG),
        })?;
        let mut rollouts = BTreeMap::new();
        let mut next_id = 1;
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Deployment)
        {
            apply_deployment_record(
                &mut rollouts,
                decode_deployment_wal_payload(&record.payload)?,
            )?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        if let Some(last) = rollouts.keys().next_back() {
            next_id = last.0.saturating_add(1);
        }

        Ok(Self {
            rollouts,
            planner: DeploymentPlanner,
            store,
            epoch,
            next_id,
            next_index,
        })
    }

    pub fn plan_rollout(&mut self, profile: DeploymentProfile) -> RsfResult<DeploymentRolloutId> {
        let manifest = self.planner.plan(profile.clone())?;
        validate_manifest(&profile, &manifest)?;
        let id = DeploymentRolloutId(self.next_id);
        self.append_record(&DeploymentWalRecord::Planned {
            id,
            profile: profile.clone(),
            manifest: manifest.clone(),
        })?;
        self.rollouts.insert(
            id,
            DeploymentRollout {
                id,
                profile,
                manifest,
                state: DeploymentRolloutState::Planned,
                failure: None,
            },
        );
        self.next_id = self.next_id.saturating_add(1);
        Ok(id)
    }

    pub fn mark_started(&mut self, id: DeploymentRolloutId) -> RsfResult<()> {
        let mut rollouts = self.rollouts.clone();
        let rollout = rollout_mut(&mut rollouts, id)?;
        if rollout.state != DeploymentRolloutState::Planned {
            return Err(invalid(format!(
                "deployment rollout {} cannot start from {:?}",
                id.0, rollout.state
            )));
        }
        rollout.state = DeploymentRolloutState::Started;
        self.append_record(&DeploymentWalRecord::Started { id })?;
        self.rollouts = rollouts;
        Ok(())
    }

    pub fn mark_applied(&mut self, id: DeploymentRolloutId) -> RsfResult<()> {
        let mut rollouts = self.rollouts.clone();
        let rollout = rollout_mut(&mut rollouts, id)?;
        if rollout.state != DeploymentRolloutState::Started {
            return Err(invalid(format!(
                "deployment rollout {} cannot apply from {:?}",
                id.0, rollout.state
            )));
        }
        rollout.state = DeploymentRolloutState::Applied;
        rollout.failure = None;
        self.append_record(&DeploymentWalRecord::Applied { id })?;
        self.rollouts = rollouts;
        Ok(())
    }

    pub fn mark_failed(
        &mut self,
        id: DeploymentRolloutId,
        reason: impl Into<String>,
    ) -> RsfResult<()> {
        let reason = reason.into();
        validate_failure(&reason)?;
        let mut rollouts = self.rollouts.clone();
        let rollout = rollout_mut(&mut rollouts, id)?;
        if rollout.state == DeploymentRolloutState::Applied {
            return Err(invalid(format!(
                "deployment rollout {} cannot fail after apply",
                id.0
            )));
        }
        rollout.state = DeploymentRolloutState::Failed;
        rollout.failure = Some(reason.clone());
        self.append_record(&DeploymentWalRecord::Failed { id, reason })?;
        self.rollouts = rollouts;
        Ok(())
    }

    pub fn rollout(&self, id: DeploymentRolloutId) -> Option<&DeploymentRollout> {
        self.rollouts.get(&id)
    }

    pub fn rollouts(&self) -> Vec<DeploymentRollout> {
        self.rollouts.values().cloned().collect()
    }

    pub fn pending(&self) -> Vec<DeploymentRollout> {
        self.rollouts
            .values()
            .filter(|rollout| {
                matches!(
                    rollout.state,
                    DeploymentRolloutState::Planned | DeploymentRolloutState::Started
                )
            })
            .cloned()
            .collect()
    }

    fn append_record(&mut self, record: &DeploymentWalRecord) -> RsfResult<()> {
        let payload = encode_deployment_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(DEPLOYMENT_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: DEPLOYMENT_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Deployment,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeploymentWalRecord {
    Planned {
        id: DeploymentRolloutId,
        profile: DeploymentProfile,
        manifest: DeploymentManifest,
    },
    Started {
        id: DeploymentRolloutId,
    },
    Applied {
        id: DeploymentRolloutId,
    },
    Failed {
        id: DeploymentRolloutId,
        reason: String,
    },
}

fn apply_deployment_record(
    rollouts: &mut BTreeMap<DeploymentRolloutId, DeploymentRollout>,
    record: DeploymentWalRecord,
) -> RsfResult<()> {
    match record {
        DeploymentWalRecord::Planned {
            id,
            profile,
            manifest,
        } => {
            if rollouts.contains_key(&id) {
                return Err(corrupt(format!(
                    "duplicate deployment rollout id {} in WAL",
                    id.0
                )));
            }
            validate_manifest(&profile, &manifest)?;
            rollouts.insert(
                id,
                DeploymentRollout {
                    id,
                    profile,
                    manifest,
                    state: DeploymentRolloutState::Planned,
                    failure: None,
                },
            );
        }
        DeploymentWalRecord::Started { id } => {
            let rollout = rollout_mut(rollouts, id)?;
            if rollout.state != DeploymentRolloutState::Planned {
                return Err(corrupt(format!(
                    "deployment rollout {} cannot replay start from {:?}",
                    id.0, rollout.state
                )));
            }
            rollout.state = DeploymentRolloutState::Started;
        }
        DeploymentWalRecord::Applied { id } => {
            let rollout = rollout_mut(rollouts, id)?;
            if rollout.state != DeploymentRolloutState::Started {
                return Err(corrupt(format!(
                    "deployment rollout {} cannot replay apply from {:?}",
                    id.0, rollout.state
                )));
            }
            rollout.state = DeploymentRolloutState::Applied;
            rollout.failure = None;
        }
        DeploymentWalRecord::Failed { id, reason } => {
            validate_failure(&reason)?;
            let rollout = rollout_mut(rollouts, id)?;
            if rollout.state == DeploymentRolloutState::Applied {
                return Err(corrupt(format!(
                    "deployment rollout {} cannot replay failure after apply",
                    id.0
                )));
            }
            rollout.state = DeploymentRolloutState::Failed;
            rollout.failure = Some(reason);
        }
    }
    Ok(())
}

fn validate_manifest(profile: &DeploymentProfile, manifest: &DeploymentManifest) -> RsfResult<()> {
    if manifest.implementation != profile.implementation {
        return Err(invalid(
            "deployment manifest implementation must match requested profile",
        ));
    }
    if manifest.roles.is_empty() {
        return Err(invalid("deployment manifest must assign roles"));
    }
    if manifest.endpoints.is_empty() {
        return Err(invalid("deployment manifest must expose endpoints"));
    }
    if manifest.volumes.len() != usize::from(profile.nodes) {
        return Err(invalid(
            "deployment manifest must provision one data volume per node",
        ));
    }
    let roles = manifest
        .roles
        .iter()
        .map(|role| role.role)
        .collect::<std::collections::BTreeSet<_>>();
    for required in [
        DeploymentRole::ControlPlane,
        DeploymentRole::Gateway,
        DeploymentRole::Metadata,
        DeploymentRole::Storage,
        DeploymentRole::Sql,
        DeploymentRole::Object,
        DeploymentRole::Blob,
        DeploymentRole::BackgroundWorker,
    ] {
        if !roles.contains(&required) {
            return Err(invalid(format!(
                "deployment manifest missing required role {}",
                required.as_str()
            )));
        }
    }
    for endpoint in &manifest.endpoints {
        if endpoint.name.trim().is_empty() || endpoint.address.trim().is_empty() {
            return Err(invalid(
                "deployment endpoint names and addresses must not be empty",
            ));
        }
        if endpoint.port == 0 {
            return Err(invalid("deployment endpoint port must be nonzero"));
        }
    }
    for volume in &manifest.volumes {
        if volume.name.trim().is_empty() || volume.purpose.trim().is_empty() || volume.bytes == 0 {
            return Err(invalid(
                "deployment volumes must name durable nonzero storage",
            ));
        }
    }
    for probe in &manifest.probes {
        if probe.name.trim().is_empty() || !probe.path.starts_with('/') {
            return Err(invalid(
                "deployment probes must have names and absolute paths",
            ));
        }
    }
    Ok(())
}

fn validate_failure(reason: &str) -> RsfResult<()> {
    if reason.trim().is_empty() {
        return Err(invalid("deployment failure reason must not be empty"));
    }
    Ok(())
}

fn rollout_mut(
    rollouts: &mut BTreeMap<DeploymentRolloutId, DeploymentRollout>,
    id: DeploymentRolloutId,
) -> RsfResult<&mut DeploymentRollout> {
    rollouts
        .get_mut(&id)
        .ok_or_else(|| corrupt(format!("unknown deployment rollout id {}", id.0)))
}

fn encode_deployment_wal_payload(record: &DeploymentWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(DEPLOYMENT_WAL_MAGIC);
    out.push(DEPLOYMENT_WAL_VERSION);
    match record {
        DeploymentWalRecord::Planned {
            id,
            profile,
            manifest,
        } => {
            out.push(1);
            write_u64(&mut out, id.0);
            write_profile(&mut out, profile)?;
            write_manifest(&mut out, manifest)?;
        }
        DeploymentWalRecord::Started { id } => {
            out.push(2);
            write_u64(&mut out, id.0);
        }
        DeploymentWalRecord::Applied { id } => {
            out.push(3);
            write_u64(&mut out, id.0);
        }
        DeploymentWalRecord::Failed { id, reason } => {
            out.push(4);
            write_u64(&mut out, id.0);
            write_string(&mut out, reason)?;
        }
    }
    Ok(out)
}

fn decode_deployment_wal_payload(bytes: &[u8]) -> RsfResult<DeploymentWalRecord> {
    let mut offset = 0;
    take_magic(bytes, &mut offset)?;
    let version = read_u8(bytes, &mut offset)?;
    if version != DEPLOYMENT_WAL_VERSION {
        return Err(corrupt(format!(
            "unsupported deployment WAL version {version}"
        )));
    }
    let record = match read_u8(bytes, &mut offset)? {
        1 => DeploymentWalRecord::Planned {
            id: DeploymentRolloutId(read_u64(bytes, &mut offset)?),
            profile: read_profile(bytes, &mut offset)?,
            manifest: read_manifest(bytes, &mut offset)?,
        },
        2 => DeploymentWalRecord::Started {
            id: DeploymentRolloutId(read_u64(bytes, &mut offset)?),
        },
        3 => DeploymentWalRecord::Applied {
            id: DeploymentRolloutId(read_u64(bytes, &mut offset)?),
        },
        4 => DeploymentWalRecord::Failed {
            id: DeploymentRolloutId(read_u64(bytes, &mut offset)?),
            reason: read_string(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "unknown deployment WAL record kind {other}"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("deployment WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn write_profile(out: &mut Vec<u8>, profile: &DeploymentProfile) -> RsfResult<()> {
    write_string(out, &profile.implementation)?;
    out.push(profile.nodes);
    write_len(out, profile.regions.len())?;
    for region in &profile.regions {
        write_string(out, region.as_str())?;
    }
    write_string(out, &profile.storage_label)?;
    write_string(out, &profile.consensus_label)
}

fn read_profile(bytes: &[u8], offset: &mut usize) -> RsfResult<DeploymentProfile> {
    let implementation = read_string(bytes, offset)?;
    let nodes = read_u8(bytes, offset)?;
    let region_count = read_len(bytes, offset)?;
    let mut regions = Vec::with_capacity(region_count);
    for _ in 0..region_count {
        regions.push(RegionId::new(read_string(bytes, offset)?)?);
    }
    Ok(DeploymentProfile {
        implementation,
        nodes,
        regions,
        storage_label: read_string(bytes, offset)?,
        consensus_label: read_string(bytes, offset)?,
    })
}

fn write_manifest(out: &mut Vec<u8>, manifest: &DeploymentManifest) -> RsfResult<()> {
    write_string(out, &manifest.implementation)?;
    write_len(out, manifest.roles.len())?;
    for role in &manifest.roles {
        write_u64(out, role.node.0);
        out.push(role_to_u8(role.role));
        write_string(out, role.region.as_str())?;
    }
    write_len(out, manifest.endpoints.len())?;
    for endpoint in &manifest.endpoints {
        write_string(out, &endpoint.name)?;
        out.push(role_to_u8(endpoint.role));
        write_string(out, &endpoint.address)?;
        write_u16(out, endpoint.port);
    }
    write_len(out, manifest.volumes.len())?;
    for volume in &manifest.volumes {
        write_u64(out, volume.node.0);
        write_string(out, &volume.name)?;
        write_string(out, &volume.purpose)?;
        write_u64(out, volume.bytes);
    }
    write_len(out, manifest.probes.len())?;
    for probe in &manifest.probes {
        write_string(out, &probe.name)?;
        out.push(role_to_u8(probe.role));
        write_string(out, &probe.path)?;
    }
    Ok(())
}

fn read_manifest(bytes: &[u8], offset: &mut usize) -> RsfResult<DeploymentManifest> {
    let implementation = read_string(bytes, offset)?;
    let role_count = read_len(bytes, offset)?;
    let mut roles = Vec::with_capacity(role_count);
    for _ in 0..role_count {
        roles.push(RoleAssignment {
            node: NodeId(read_u64(bytes, offset)?),
            role: role_from_u8(read_u8(bytes, offset)?)?,
            region: RegionId::new(read_string(bytes, offset)?)?,
        });
    }
    let endpoint_count = read_len(bytes, offset)?;
    let mut endpoints = Vec::with_capacity(endpoint_count);
    for _ in 0..endpoint_count {
        endpoints.push(ServiceEndpoint {
            name: read_string(bytes, offset)?,
            role: role_from_u8(read_u8(bytes, offset)?)?,
            address: read_string(bytes, offset)?,
            port: read_u16(bytes, offset)?,
        });
    }
    let volume_count = read_len(bytes, offset)?;
    let mut volumes = Vec::with_capacity(volume_count);
    for _ in 0..volume_count {
        volumes.push(VolumeSpec {
            node: NodeId(read_u64(bytes, offset)?),
            name: read_string(bytes, offset)?,
            purpose: read_string(bytes, offset)?,
            bytes: read_u64(bytes, offset)?,
        });
    }
    let probe_count = read_len(bytes, offset)?;
    let mut probes = Vec::with_capacity(probe_count);
    for _ in 0..probe_count {
        probes.push(ProbeSpec {
            name: read_string(bytes, offset)?,
            role: role_from_u8(read_u8(bytes, offset)?)?,
            path: read_string(bytes, offset)?,
        });
    }
    Ok(DeploymentManifest {
        implementation,
        roles,
        endpoints,
        volumes,
        probes,
    })
}

fn role_to_u8(role: DeploymentRole) -> u8 {
    match role {
        DeploymentRole::ControlPlane => 1,
        DeploymentRole::Gateway => 2,
        DeploymentRole::Metadata => 3,
        DeploymentRole::Storage => 4,
        DeploymentRole::Sql => 5,
        DeploymentRole::Object => 6,
        DeploymentRole::Blob => 7,
        DeploymentRole::BackgroundWorker => 8,
    }
}

fn role_from_u8(value: u8) -> RsfResult<DeploymentRole> {
    match value {
        1 => Ok(DeploymentRole::ControlPlane),
        2 => Ok(DeploymentRole::Gateway),
        3 => Ok(DeploymentRole::Metadata),
        4 => Ok(DeploymentRole::Storage),
        5 => Ok(DeploymentRole::Sql),
        6 => Ok(DeploymentRole::Object),
        7 => Ok(DeploymentRole::Blob),
        8 => Ok(DeploymentRole::BackgroundWorker),
        other => Err(corrupt(format!("unknown deployment role {other}"))),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    let len =
        u32::try_from(value.len()).map_err(|_| invalid("deployment WAL string exceeds u32"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)? as usize;
    let raw = take(bytes, offset, len)?;
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("deployment WAL string is not utf8"))
}

fn write_len(out: &mut Vec<u8>, len: usize) -> RsfResult<()> {
    let len = u32::try_from(len).map_err(|_| invalid("deployment WAL length exceeds u32"))?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn read_len(bytes: &[u8], offset: &mut usize) -> RsfResult<usize> {
    Ok(read_u32(bytes, offset)? as usize)
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    let mut buf = [0; 8];
    buf.copy_from_slice(raw);
    Ok(u64::from_le_bytes(buf))
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    let raw = take(bytes, offset, 2)?;
    let mut buf = [0; 2];
    buf.copy_from_slice(raw);
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = take(bytes, offset, 4)?;
    let mut buf = [0; 4];
    buf.copy_from_slice(raw);
    Ok(u32::from_le_bytes(buf))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(*take(bytes, offset, 1)?
        .first()
        .expect("one requested byte must exist"))
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, DEPLOYMENT_WAL_MAGIC.len())?;
    if magic != DEPLOYMENT_WAL_MAGIC {
        return Err(corrupt("deployment WAL payload has invalid magic"));
    }
    Ok(())
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("deployment WAL offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("deployment WAL payload is truncated"));
    }
    let out = &bytes[*offset..end];
    *offset = end;
    Ok(out)
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests;
