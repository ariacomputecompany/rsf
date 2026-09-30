#![forbid(unsafe_code)]

use std::path::Path;

use rsf_core::{
    Epoch, ErrorCode, LogIndex, PlacementGroupId, RegionId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentStore, WalAppend};

const POLICY_PG: PlacementGroupId = PlacementGroupId(0);
const POLICY_MAGIC: &[u8; 5] = b"RSFP1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredClusterPolicy {
    pub version: u64,
    pub min_replicas: u8,
    pub placement_groups: u64,
    pub strict_global_writes: bool,
    pub regions: Vec<RegionPolicy>,
}

impl DesiredClusterPolicy {
    pub fn bootstrap() -> Self {
        Self {
            version: 1,
            min_replicas: 3,
            placement_groups: 1,
            strict_global_writes: true,
            regions: Vec::new(),
        }
    }

    pub fn validate(&self) -> RsfResult<()> {
        if self.version == 0 {
            return Err(invalid("policy version must be non-zero"));
        }
        if self.min_replicas != 1 && self.min_replicas < 3 {
            return Err(invalid(
                "minimum replicas must be one for standalone or at least three for redundancy",
            ));
        }
        if self.placement_groups == 0 {
            return Err(invalid("placement group count must be non-zero"));
        }
        let mut primary_regions = 0_usize;
        let mut seen = std::collections::BTreeSet::new();
        for region in &self.regions {
            if !seen.insert(region.region.clone()) {
                return Err(invalid(format!("duplicate region {}", region.region)));
            }
            if region.role == RegionRole::Primary {
                primary_regions += 1;
            }
        }
        if !self.regions.is_empty() && primary_regions == 0 {
            return Err(invalid("at least one configured region must be primary"));
        }
        if self.strict_global_writes && primary_regions > 1 {
            return Err(invalid(
                "strict global writes require a single write-primary region",
            ));
        }
        Ok(())
    }

    pub fn explain_consistency(&self) -> PolicyExplanation {
        let write_mode = if self.strict_global_writes {
            "strict-global-write"
        } else {
            "regional-write"
        }
        .to_string();
        let latency = if self.min_replicas == 1 {
            "standalone writes sync one local durable copy; node or disk loss has no replica failover"
        } else if self.strict_global_writes {
            "writes are serialized through the primary region and replicated by quorum before mutable truth is visible"
        } else {
            "writes may be owned by regional policy, reducing locality cost while preserving explicit placement ownership"
        }
        .to_string();
        let consistency = if self.strict_global_writes {
            "strict serializable mutable-truth operations remain CP; availability-first is only allowed for immutable payloads when lower protocol policy permits it"
        } else {
            "regional ownership can reduce write latency, but cross-region ownership transfer must be reconciled before accepting conflicting mutable writes"
        }
        .to_string();
        PolicyExplanation {
            write_mode,
            latency,
            consistency,
        }
    }

    pub fn preflight(&self) -> PolicyPreflightReport {
        let explanation = self.explain_consistency();
        let primary_regions = self
            .regions
            .iter()
            .filter(|region| region.role == RegionRole::Primary)
            .count();
        let distinct_regions = self
            .regions
            .iter()
            .map(|region| region.region.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let has_read_replica = self
            .regions
            .iter()
            .any(|region| region.role == RegionRole::ReadReplica);
        let mut checks = Vec::new();
        checks.push(policy_check(
            "replica-floor",
            self.min_replicas == 1 || self.min_replicas >= 3,
            format!(
                "minimum replicas={} supports standalone (1) or redundant (at least 3) storage",
                self.min_replicas
            ),
        ));
        checks.push(policy_check(
            "placement-groups",
            self.placement_groups > 0,
            format!(
                "placement groups={} must be non-zero",
                self.placement_groups
            ),
        ));
        checks.push(policy_check(
            "region-primary",
            self.regions.is_empty() || primary_regions > 0,
            format!(
                "configured regions={} primary_regions={} mutable truth needs an owner",
                self.regions.len(),
                primary_regions
            ),
        ));
        checks.push(policy_check(
            "strict-global-primary",
            !self.strict_global_writes || primary_regions <= 1,
            format!(
                "strict_global_writes={} primary_regions={} strict mode serializes mutable writes through one region",
                self.strict_global_writes, primary_regions
            ),
        ));
        checks.push(policy_check(
            "region-duplicates",
            distinct_regions == self.regions.len(),
            format!(
                "configured regions={} distinct_regions={} region policy must be unambiguous",
                self.regions.len(),
                distinct_regions
            ),
        ));

        let mut implications = vec![
            format!("write-mode: {}", explanation.write_mode),
            format!("latency: {}", explanation.latency),
            format!("consistency: {}", explanation.consistency),
        ];
        if has_read_replica {
            implications.push(
                "read replicas can serve bounded-stale and immutable-verified reads locally, but strict reads route to the primary region"
                    .to_string(),
            );
        }
        implications.push(
            "immutable payloads may use availability-first placement only after metadata version guards prove the referenced head is current"
                .to_string(),
        );

        PolicyPreflightReport {
            policy_version: self.version,
            write_mode: explanation.write_mode,
            checks,
            implications,
        }
    }

    fn encode(&self) -> RsfResult<Vec<u8>> {
        self.validate()?;
        let mut out = Vec::new();
        out.extend_from_slice(POLICY_MAGIC);
        write_u64(self.version, &mut out);
        out.push(self.min_replicas);
        write_u64(self.placement_groups, &mut out);
        out.push(u8::from(self.strict_global_writes));
        write_u64(
            u64::try_from(self.regions.len())
                .map_err(|_| invalid("region count does not fit in policy record"))?,
            &mut out,
        );
        for region in &self.regions {
            write_string(region.region.as_str(), &mut out)?;
            out.push(region.role.to_u8());
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if !bytes.starts_with(POLICY_MAGIC) {
            return Err(corrupt("policy record has invalid magic"));
        }
        let mut offset = POLICY_MAGIC.len();
        let version = read_u64(bytes, offset)?;
        offset += 8;
        let min_replicas = *bytes
            .get(offset)
            .ok_or_else(|| corrupt("policy record missing min replicas"))?;
        offset += 1;
        let placement_groups = read_u64(bytes, offset)?;
        offset += 8;
        let strict_global_writes = match *bytes
            .get(offset)
            .ok_or_else(|| corrupt("policy record missing strict-global flag"))?
        {
            0 => false,
            1 => true,
            _ => return Err(corrupt("policy record has invalid strict-global flag")),
        };
        offset += 1;
        let region_count = usize::try_from(read_u64(bytes, offset)?)
            .map_err(|_| corrupt("policy region count does not fit in memory"))?;
        offset += 8;
        let mut regions = Vec::new();
        for _ in 0..region_count {
            let (region, next) = read_string(bytes, offset)?;
            offset = next;
            let role = RegionRole::from_u8(
                *bytes
                    .get(offset)
                    .ok_or_else(|| corrupt("policy region record missing role"))?,
            )?;
            offset += 1;
            regions.push(RegionPolicy {
                region: RegionId::new(region)?,
                role,
            });
        }
        if offset != bytes.len() {
            return Err(corrupt("policy record has trailing bytes"));
        }
        let policy = Self {
            version,
            min_replicas,
            placement_groups,
            strict_global_writes,
            regions,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn is_payload(bytes: &[u8]) -> bool {
        bytes.starts_with(POLICY_MAGIC)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionPolicy {
    pub region: RegionId,
    pub role: RegionRole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegionRole {
    Primary,
    ReadReplica,
}

impl RegionRole {
    fn to_u8(self) -> u8 {
        match self {
            Self::Primary => 1,
            Self::ReadReplica => 2,
        }
    }

    fn from_u8(value: u8) -> RsfResult<Self> {
        match value {
            1 => Ok(Self::Primary),
            2 => Ok(Self::ReadReplica),
            _ => Err(corrupt(format!("unknown region role {value}"))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyExplanation {
    pub write_mode: String,
    pub latency: String,
    pub consistency: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyPreflightReport {
    pub policy_version: u64,
    pub write_mode: String,
    pub checks: Vec<PolicyPreflightCheck>,
    pub implications: Vec<String>,
}

impl PolicyPreflightReport {
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyPreflightCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GlobalWriteDecision {
    Allowed {
        client_region: RegionId,
        target_region: RegionId,
        primary_region: Option<RegionId>,
        strict: bool,
        reason: String,
    },
    Rejected {
        client_region: RegionId,
        target_region: RegionId,
        primary_region: Option<RegionId>,
        reason: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaReadClass {
    Strict,
    BoundedStale,
    ImmutableVerified,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaReadDecision {
    ServeLocal {
        client_region: RegionId,
        target_region: RegionId,
        class: ReplicaReadClass,
        reason: String,
    },
    RoutePrimary {
        client_region: RegionId,
        target_region: RegionId,
        primary_region: RegionId,
        class: ReplicaReadClass,
        reason: String,
    },
    Rejected {
        client_region: RegionId,
        target_region: RegionId,
        class: ReplicaReadClass,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmutablePayloadPlacementRequest {
    pub client_region: RegionId,
    pub candidate_regions: Vec<RegionId>,
    pub min_replicas: usize,
    pub metadata_version: u64,
    pub required_metadata_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImmutablePayloadPlacementDecision {
    Place {
        client_region: RegionId,
        placements: Vec<RegionId>,
        metadata_version: u64,
        reason: String,
    },
    Rejected {
        client_region: RegionId,
        reason: String,
    },
}

impl ImmutablePayloadPlacementDecision {
    pub fn is_place(&self) -> bool {
        matches!(self, Self::Place { .. })
    }
}

impl ReplicaReadDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::ServeLocal { .. } | Self::RoutePrimary { .. })
    }
}

impl GlobalWriteDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }
}

impl DesiredClusterPolicy {
    pub fn primary_region(&self) -> Option<&RegionId> {
        self.regions
            .iter()
            .find(|region| region.role == RegionRole::Primary)
            .map(|region| &region.region)
    }

    pub fn region_role(&self, region: &RegionId) -> Option<RegionRole> {
        self.regions
            .iter()
            .find(|entry| &entry.region == region)
            .map(|entry| entry.role)
    }

    pub fn evaluate_replica_read(
        &self,
        client_region: RegionId,
        target_region: RegionId,
        class: ReplicaReadClass,
        mutable_write: bool,
    ) -> ReplicaReadDecision {
        if mutable_write {
            return ReplicaReadDecision::Rejected {
                client_region,
                target_region,
                class,
                reason: "read replicas never accept mutable writes without ownership transfer"
                    .to_string(),
            };
        }
        let role = self.region_role(&target_region);
        match (class, role) {
            (ReplicaReadClass::Strict, _) => {
                if let Some(primary) = self.primary_region().cloned() {
                    if target_region == primary {
                        ReplicaReadDecision::ServeLocal {
                            client_region,
                            target_region,
                            class,
                            reason: "strict read already targets the primary region".to_string(),
                        }
                    } else {
                        ReplicaReadDecision::RoutePrimary {
                            client_region,
                            target_region,
                            primary_region: primary,
                            class,
                            reason: "strict read requires primary-region linearization".to_string(),
                        }
                    }
                } else {
                    ReplicaReadDecision::Rejected {
                        client_region,
                        target_region,
                        class,
                        reason: "strict read requires an explicit primary region".to_string(),
                    }
                }
            }
            (
                ReplicaReadClass::BoundedStale | ReplicaReadClass::ImmutableVerified,
                Some(RegionRole::Primary | RegionRole::ReadReplica),
            ) => ReplicaReadDecision::ServeLocal {
                client_region,
                target_region,
                class,
                reason: "target region is eligible to serve replica-safe reads".to_string(),
            },
            (_, None) => ReplicaReadDecision::Rejected {
                client_region,
                target_region,
                class,
                reason: "target region is not configured in cluster policy".to_string(),
            },
        }
    }

    pub fn plan_immutable_payload_placement(
        &self,
        request: ImmutablePayloadPlacementRequest,
    ) -> ImmutablePayloadPlacementDecision {
        if request.min_replicas == 0 {
            return ImmutablePayloadPlacementDecision::Rejected {
                client_region: request.client_region,
                reason: "immutable payload placement requires at least one replica".to_string(),
            };
        }
        if request.metadata_version < request.required_metadata_version {
            return ImmutablePayloadPlacementDecision::Rejected {
                client_region: request.client_region,
                reason: format!(
                    "metadata version {} is older than required version {}",
                    request.metadata_version, request.required_metadata_version
                ),
            };
        }

        let mut eligible = request
            .candidate_regions
            .into_iter()
            .filter(|region| self.region_role(region).is_some())
            .collect::<Vec<_>>();
        eligible.sort();
        eligible.dedup();
        eligible.sort_by_key(|region| {
            (
                region != &request.client_region,
                self.region_role(region) != Some(RegionRole::Primary),
                region.as_str().to_string(),
            )
        });

        if eligible.len() < request.min_replicas {
            return ImmutablePayloadPlacementDecision::Rejected {
                client_region: request.client_region,
                reason: format!(
                    "immutable payload placement needs {} configured regions but only {} are eligible",
                    request.min_replicas,
                    eligible.len()
                ),
            };
        }
        eligible.truncate(request.min_replicas);
        ImmutablePayloadPlacementDecision::Place {
            client_region: request.client_region,
            placements: eligible,
            metadata_version: request.metadata_version,
            reason:
                "immutable payload may use availability-first placement after metadata version guard"
                    .to_string(),
        }
    }

    pub fn evaluate_global_write(
        &self,
        client_region: RegionId,
        target_region: RegionId,
        mutable_truth: bool,
    ) -> GlobalWriteDecision {
        if !mutable_truth {
            return GlobalWriteDecision::Allowed {
                client_region,
                target_region,
                primary_region: self.primary_region().cloned(),
                strict: self.strict_global_writes,
                reason: "immutable payload writes are outside strict mutable-truth ownership"
                    .to_string(),
            };
        }
        let primary_region = self.primary_region().cloned();
        if !self.strict_global_writes {
            return GlobalWriteDecision::Allowed {
                client_region,
                target_region,
                primary_region,
                strict: false,
                reason: "regional mutable ownership is enabled by policy".to_string(),
            };
        }
        let Some(primary) = primary_region.clone() else {
            return GlobalWriteDecision::Rejected {
                client_region,
                target_region,
                primary_region,
                reason: "strict global writes require an explicit primary region".to_string(),
            };
        };
        if target_region == primary {
            GlobalWriteDecision::Allowed {
                client_region,
                target_region,
                primary_region: Some(primary),
                strict: true,
                reason: "mutable write targets the strict global primary region".to_string(),
            }
        } else {
            GlobalWriteDecision::Rejected {
                client_region,
                target_region,
                primary_region: Some(primary),
                reason: "strict global writes must target the primary region".to_string(),
            }
        }
    }
}

#[derive(Debug)]
pub struct PolicyCatalog {
    policy: DesiredClusterPolicy,
    store: SegmentStore,
}

impl PolicyCatalog {
    pub fn open(path: impl AsRef<Path>) -> RsfResult<Self> {
        let path = path.as_ref();
        let recovery = SegmentStore::recover(path)?;
        let mut latest = None::<DesiredClusterPolicy>;
        for record in recovery.recovery.wal_records.iter().filter(|record| {
            record.pg == POLICY_PG
                && record.kind == WalRecordKind::Policy
                && DesiredClusterPolicy::is_payload(&record.payload)
        }) {
            let policy = DesiredClusterPolicy::decode(&record.payload)?;
            if latest
                .as_ref()
                .is_none_or(|current| policy.version > current.version)
            {
                latest = Some(policy);
            }
        }

        let mut catalog = Self {
            policy: latest.unwrap_or_else(DesiredClusterPolicy::bootstrap),
            store: SegmentStore::open(path)?,
        };
        if recovery.recovery.wal_records.iter().all(|record| {
            record.pg != POLICY_PG
                || record.kind != WalRecordKind::Policy
                || !DesiredClusterPolicy::is_payload(&record.payload)
        }) {
            catalog.append_current()?;
        }
        Ok(catalog)
    }

    pub fn current(&self) -> &DesiredClusterPolicy {
        &self.policy
    }

    pub fn set_replication(
        &mut self,
        min_replicas: u8,
        placement_groups: u64,
    ) -> RsfResult<DesiredClusterPolicy> {
        let mut next = self.next_policy();
        next.min_replicas = min_replicas;
        next.placement_groups = placement_groups;
        self.commit(next)
    }

    pub fn add_region(
        &mut self,
        region: RegionId,
        role: RegionRole,
    ) -> RsfResult<DesiredClusterPolicy> {
        if self
            .policy
            .regions
            .iter()
            .any(|entry| entry.region == region)
        {
            return Err(invalid(format!("region {region} is already configured")));
        }
        let mut next = self.next_policy();
        next.regions.push(RegionPolicy { region, role });
        self.commit(next)
    }

    pub fn set_strict_global_writes(&mut self, strict: bool) -> RsfResult<DesiredClusterPolicy> {
        let mut next = self.next_policy();
        next.strict_global_writes = strict;
        self.commit(next)
    }

    fn next_policy(&self) -> DesiredClusterPolicy {
        let mut next = self.policy.clone();
        next.version += 1;
        next
    }

    fn commit(&mut self, policy: DesiredClusterPolicy) -> RsfResult<DesiredClusterPolicy> {
        policy.validate()?;
        self.policy = policy;
        self.append_current()?;
        Ok(self.policy.clone())
    }

    fn append_current(&mut self) -> RsfResult<()> {
        let payload = self.policy.encode()?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(POLICY_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(tail.map_or(1, |tail| tail.index.0 + 1));
        self.store.append_wal(WalAppend {
            pg: POLICY_PG,
            epoch: Epoch(self.policy.version),
            index,
            kind: WalRecordKind::Policy,
            previous_hash,
            payload: &payload,
        })?;
        Ok(())
    }
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: usize) -> RsfResult<u64> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| corrupt("policy record ended while reading u64"))?;
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u64::try_from(value.len())
        .map_err(|_| invalid("policy string length does not fit in record"))?;
    write_u64(len, out);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: usize) -> RsfResult<(String, usize)> {
    let len = usize::try_from(read_u64(bytes, offset)?)
        .map_err(|_| corrupt("policy string length does not fit in memory"))?;
    let start = offset + 8;
    let end = start + len;
    let raw = bytes
        .get(start..end)
        .ok_or_else(|| corrupt("policy record ended while reading string"))?;
    let value =
        String::from_utf8(raw.to_vec()).map_err(|_| corrupt("policy string is not valid utf-8"))?;
    Ok((value, end))
}

fn policy_check(
    name: impl Into<String>,
    passed: bool,
    detail: impl Into<String>,
) -> PolicyPreflightCheck {
    PolicyPreflightCheck {
        name: name.into(),
        passed,
        detail: detail.into(),
    }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn catalog_initializes_and_recovers_desired_policy() {
        let path = test_path("catalog_initializes_and_recovers_desired_policy");
        {
            let mut catalog = PolicyCatalog::open(&path).expect("open catalog");
            catalog
                .add_region(RegionId::new("iad").expect("region"), RegionRole::Primary)
                .expect("add primary");
            catalog
                .add_region(
                    RegionId::new("fra").expect("region"),
                    RegionRole::ReadReplica,
                )
                .expect("add read replica");
            catalog.set_replication(5, 64).expect("set replication");
        }

        let recovered = PolicyCatalog::open(&path).expect("recover catalog");

        assert_eq!(recovered.current().version, 4);
        assert_eq!(recovered.current().min_replicas, 5);
        assert_eq!(recovered.current().placement_groups, 64);
        assert_eq!(recovered.current().regions.len(), 2);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn catalog_rejects_unsafe_replica_floor_without_advancing() {
        let path = test_path("catalog_rejects_unsafe_replica_floor_without_advancing");
        let mut catalog = PolicyCatalog::open(&path).expect("open catalog");

        let err = catalog
            .set_replication(2, 16)
            .expect_err("replica floor below quorum rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(catalog.current().version, 1);
        assert_eq!(catalog.current().min_replicas, 3);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn catalog_rejects_duplicate_regions() {
        let path = test_path("catalog_rejects_duplicate_regions");
        let mut catalog = PolicyCatalog::open(&path).expect("open catalog");
        let region = RegionId::new("iad").expect("region");
        catalog
            .add_region(region.clone(), RegionRole::Primary)
            .expect("add primary");

        let err = catalog
            .add_region(region, RegionRole::ReadReplica)
            .expect_err("duplicate region rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(catalog.current().version, 2);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn strict_global_write_explanation_names_tradeoffs() {
        let policy = DesiredClusterPolicy::bootstrap();
        let explanation = policy.explain_consistency();

        assert_eq!(explanation.write_mode, "strict-global-write");
        assert!(explanation.latency.contains("primary region"));
        assert!(explanation.consistency.contains("strict serializable"));
    }

    #[test]
    fn policy_preflight_names_latency_consistency_and_payload_tradeoffs() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let report = policy.preflight();

        assert!(report.passed());
        assert_eq!(report.write_mode, "strict-global-write");
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "strict-global-primary" && check.passed)
        );
        assert!(
            report
                .implications
                .iter()
                .any(|detail| detail.contains("strict reads route to the primary region"))
        );
        assert!(
            report
                .implications
                .iter()
                .any(|detail| detail.contains("metadata version guards"))
        );
    }

    #[test]
    fn policy_preflight_fails_unsafe_topology_without_hiding_reason() {
        let policy = DesiredClusterPolicy {
            version: 1,
            min_replicas: 2,
            placement_groups: 0,
            strict_global_writes: true,
            regions: vec![
                RegionPolicy {
                    region: region("iad"),
                    role: RegionRole::Primary,
                },
                RegionPolicy {
                    region: region("fra"),
                    role: RegionRole::Primary,
                },
            ],
        };

        let report = policy.preflight();

        assert!(!report.passed());
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "replica-floor" && !check.passed)
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "placement-groups" && !check.passed)
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "strict-global-primary" && !check.passed)
        );
    }

    #[test]
    fn strict_global_write_allows_primary_region_target() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let decision = policy.evaluate_global_write(region("fra"), region("iad"), true);

        assert!(decision.is_allowed());
        assert_eq!(
            decision,
            GlobalWriteDecision::Allowed {
                client_region: region("fra"),
                target_region: region("iad"),
                primary_region: Some(region("iad")),
                strict: true,
                reason: "mutable write targets the strict global primary region".to_string()
            }
        );
    }

    #[test]
    fn strict_global_write_rejects_read_replica_target() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let decision = policy.evaluate_global_write(region("fra"), region("fra"), true);

        assert_eq!(
            decision,
            GlobalWriteDecision::Rejected {
                client_region: region("fra"),
                target_region: region("fra"),
                primary_region: Some(region("iad")),
                reason: "strict global writes must target the primary region".to_string()
            }
        );
    }

    #[test]
    fn strict_global_write_allows_immutable_payloads_outside_primary() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let decision = policy.evaluate_global_write(region("fra"), region("fra"), false);

        assert!(decision.is_allowed());
    }

    #[test]
    fn read_replica_serves_bounded_stale_reads_locally() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let decision = policy.evaluate_replica_read(
            region("fra"),
            region("fra"),
            ReplicaReadClass::BoundedStale,
            false,
        );

        assert_eq!(
            decision,
            ReplicaReadDecision::ServeLocal {
                client_region: region("fra"),
                target_region: region("fra"),
                class: ReplicaReadClass::BoundedStale,
                reason: "target region is eligible to serve replica-safe reads".to_string()
            }
        );
    }

    #[test]
    fn read_replica_routes_strict_reads_to_primary() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let decision = policy.evaluate_replica_read(
            region("fra"),
            region("fra"),
            ReplicaReadClass::Strict,
            false,
        );

        assert_eq!(
            decision,
            ReplicaReadDecision::RoutePrimary {
                client_region: region("fra"),
                target_region: region("fra"),
                primary_region: region("iad"),
                class: ReplicaReadClass::Strict,
                reason: "strict read requires primary-region linearization".to_string()
            }
        );
    }

    #[test]
    fn read_replica_rejects_mutable_write_without_ownership() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica);

        let decision = policy.evaluate_replica_read(
            region("fra"),
            region("fra"),
            ReplicaReadClass::BoundedStale,
            true,
        );

        assert_eq!(
            decision,
            ReplicaReadDecision::Rejected {
                client_region: region("fra"),
                target_region: region("fra"),
                class: ReplicaReadClass::BoundedStale,
                reason: "read replicas never accept mutable writes without ownership transfer"
                    .to_string()
            }
        );
    }

    #[test]
    fn immutable_payload_placement_prefers_client_region_then_primary() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("fra", RegionRole::ReadReplica)
            .with_region("ord", RegionRole::ReadReplica);

        let decision = policy.plan_immutable_payload_placement(ImmutablePayloadPlacementRequest {
            client_region: region("ord"),
            candidate_regions: vec![region("fra"), region("iad"), region("ord"), region("ord")],
            min_replicas: 2,
            metadata_version: 56,
            required_metadata_version: 56,
        });

        assert_eq!(
            decision,
            ImmutablePayloadPlacementDecision::Place {
                client_region: region("ord"),
                placements: vec![region("ord"), region("iad")],
                metadata_version: 56,
                reason:
                    "immutable payload may use availability-first placement after metadata version guard"
                        .to_string()
            }
        );
    }

    #[test]
    fn immutable_payload_placement_rejects_stale_metadata() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("ord", RegionRole::ReadReplica);

        let decision = policy.plan_immutable_payload_placement(ImmutablePayloadPlacementRequest {
            client_region: region("ord"),
            candidate_regions: vec![region("iad"), region("ord")],
            min_replicas: 2,
            metadata_version: 55,
            required_metadata_version: 56,
        });

        assert_eq!(
            decision,
            ImmutablePayloadPlacementDecision::Rejected {
                client_region: region("ord"),
                reason: "metadata version 55 is older than required version 56".to_string()
            }
        );
    }

    #[test]
    fn immutable_payload_placement_rejects_insufficient_configured_regions() {
        let policy = DesiredClusterPolicy::bootstrap()
            .with_region("iad", RegionRole::Primary)
            .with_region("ord", RegionRole::ReadReplica);

        let decision = policy.plan_immutable_payload_placement(ImmutablePayloadPlacementRequest {
            client_region: region("ord"),
            candidate_regions: vec![region("ord"), region("lax")],
            min_replicas: 2,
            metadata_version: 56,
            required_metadata_version: 56,
        });

        assert_eq!(
            decision,
            ImmutablePayloadPlacementDecision::Rejected {
                client_region: region("ord"),
                reason:
                    "immutable payload placement needs 2 configured regions but only 1 are eligible"
                        .to_string()
            }
        );
    }

    #[test]
    fn immutable_payload_placement_rejects_zero_replicas() {
        let policy = DesiredClusterPolicy::bootstrap().with_region("iad", RegionRole::Primary);

        let decision = policy.plan_immutable_payload_placement(ImmutablePayloadPlacementRequest {
            client_region: region("iad"),
            candidate_regions: vec![region("iad")],
            min_replicas: 0,
            metadata_version: 56,
            required_metadata_version: 56,
        });

        assert_eq!(
            decision,
            ImmutablePayloadPlacementDecision::Rejected {
                client_region: region("iad"),
                reason: "immutable payload placement requires at least one replica".to_string()
            }
        );
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-policy-{name}-{}-{nonce}", std::process::id()))
    }

    fn region(value: &str) -> RegionId {
        RegionId::new(value).expect("region")
    }

    trait PolicyTestExt {
        fn with_region(self, region: &str, role: RegionRole) -> Self;
    }

    impl PolicyTestExt for DesiredClusterPolicy {
        fn with_region(mut self, region: &str, role: RegionRole) -> Self {
            self.regions.push(RegionPolicy {
                region: RegionId::new(region).expect("region"),
                role,
            });
            self
        }
    }
}
