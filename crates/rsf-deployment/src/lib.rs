#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use rsf_core::{ErrorCode, NodeId, RegionId, RsfError, RsfResult};

mod ledger;

pub use ledger::{
    DeploymentRollout, DeploymentRolloutId, DeploymentRolloutState, DurableDeploymentLedger,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DeploymentRole {
    ControlPlane,
    Gateway,
    Metadata,
    Storage,
    Sql,
    Object,
    Blob,
    BackgroundWorker,
}

impl DeploymentRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control-plane",
            Self::Gateway => "gateway",
            Self::Metadata => "metadata",
            Self::Storage => "storage",
            Self::Sql => "sql",
            Self::Object => "object",
            Self::Blob => "blob",
            Self::BackgroundWorker => "background-worker",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentPackageManifest {
    pub id: String,
    pub name: String,
    pub confidence: u8,
    pub nodes: u8,
    pub regions: Vec<RegionId>,
    pub placement: String,
    pub consensus: String,
    pub relational: String,
    pub object: String,
    pub blob: String,
    pub kv: String,
}

impl DeploymentPackageManifest {
    pub fn parse(input: &str) -> RsfResult<Self> {
        let mut fields = std::collections::BTreeMap::<String, String>::new();
        for line in input.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let (key, value) = trimmed
                .split_once(':')
                .ok_or_else(|| invalid(format!("manifest line missing ':' separator: {line}")))?;
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                return Err(invalid("manifest keys and values must not be empty"));
            }
            if fields.insert(key.to_string(), value.to_string()).is_some() {
                return Err(invalid(format!("duplicate manifest field '{key}'")));
            }
        }

        let id = take(&mut fields, "id")?;
        let name = take(&mut fields, "name")?;
        let confidence = take(&mut fields, "confidence")?
            .parse::<u8>()
            .map_err(|_| invalid("confidence must be an integer from 0 to 100"))?;
        if confidence > 100 {
            return Err(invalid("confidence must be an integer from 0 to 100"));
        }
        let nodes = take(&mut fields, "nodes")?
            .parse::<u8>()
            .map_err(|_| invalid("nodes must be an integer from 1 to 255"))?;
        let regions = take(&mut fields, "regions")?
            .split(',')
            .map(|region| RegionId::new(region.trim()))
            .collect::<RsfResult<Vec<_>>>()?;

        let manifest = Self {
            id,
            name,
            confidence,
            nodes,
            regions,
            placement: take(&mut fields, "placement")?,
            consensus: take(&mut fields, "consensus")?,
            relational: take(&mut fields, "relational")?,
            object: take(&mut fields, "object")?,
            blob: take(&mut fields, "blob")?,
            kv: take(&mut fields, "kv")?,
        };
        validate_package(&manifest)?;
        if !fields.is_empty() {
            return Err(invalid(format!(
                "unknown manifest fields: {}",
                fields.keys().cloned().collect::<Vec<_>>().join(",")
            )));
        }
        Ok(manifest)
    }

    pub fn deployment_profile(&self) -> DeploymentProfile {
        DeploymentProfile {
            implementation: self.id.clone(),
            nodes: self.nodes,
            regions: self.regions.clone(),
            storage_label: self.placement.clone(),
            consensus_label: self.consensus.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentProfile {
    pub implementation: String,
    pub nodes: u8,
    pub regions: Vec<RegionId>,
    pub storage_label: String,
    pub consensus_label: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleAssignment {
    pub node: NodeId,
    pub role: DeploymentRole,
    pub region: RegionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceEndpoint {
    pub name: String,
    pub role: DeploymentRole,
    pub address: String,
    pub port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeSpec {
    pub node: NodeId,
    pub name: String,
    pub purpose: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeSpec {
    pub name: String,
    pub role: DeploymentRole,
    pub path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentManifest {
    pub implementation: String,
    pub roles: Vec<RoleAssignment>,
    pub endpoints: Vec<ServiceEndpoint>,
    pub volumes: Vec<VolumeSpec>,
    pub probes: Vec<ProbeSpec>,
}

#[derive(Clone, Debug, Default)]
pub struct DeploymentPlanner;

impl DeploymentPlanner {
    pub fn plan(&self, profile: DeploymentProfile) -> RsfResult<DeploymentManifest> {
        validate_profile(&profile)?;

        let mut roles = Vec::new();
        for node in 0..profile.nodes {
            let id = NodeId(u64::from(node));
            let region = profile.regions[usize::from(node) % profile.regions.len()].clone();
            roles.push(RoleAssignment {
                node: id,
                role: DeploymentRole::ControlPlane,
                region: region.clone(),
            });
            roles.push(RoleAssignment {
                node: id,
                role: DeploymentRole::Storage,
                region: region.clone(),
            });
        }
        for (index, role) in [
            DeploymentRole::Gateway,
            DeploymentRole::Sql,
            DeploymentRole::Metadata,
            DeploymentRole::Object,
            DeploymentRole::Blob,
            DeploymentRole::BackgroundWorker,
        ]
        .into_iter()
        .enumerate()
        {
            let node = index % usize::from(profile.nodes);
            roles.push(RoleAssignment {
                node: NodeId(node as u64),
                role,
                region: profile.regions[node % profile.regions.len()].clone(),
            });
        }

        let roles_present = roles
            .iter()
            .map(|assignment| assignment.role)
            .collect::<BTreeSet<_>>();
        let endpoints = roles_present
            .iter()
            .enumerate()
            .map(|(index, role)| ServiceEndpoint {
                name: format!("{}-{}", profile.implementation, role.as_str()),
                role: *role,
                address: format!("{}.rsf.local", role.as_str()),
                port: 7000 + index as u16,
            })
            .collect::<Vec<_>>();
        let probes = roles_present
            .iter()
            .map(|role| ProbeSpec {
                name: format!("{}-ready", role.as_str()),
                role: *role,
                path: format!("/ready/{}", role.as_str()),
            })
            .collect::<Vec<_>>();
        let volumes = (0..profile.nodes)
            .map(|node| VolumeSpec {
                node: NodeId(u64::from(node)),
                name: format!("node-{node}-database"),
                purpose: format!(
                    "{} over {} durable data",
                    profile.storage_label, profile.consensus_label
                ),
                bytes: 1_099_511_627_776,
            })
            .collect::<Vec<_>>();

        Ok(DeploymentManifest {
            implementation: profile.implementation,
            roles,
            endpoints,
            volumes,
            probes,
        })
    }
}

fn validate_profile(profile: &DeploymentProfile) -> RsfResult<()> {
    if profile.implementation.trim().is_empty() {
        return Err(invalid("implementation slug must not be empty"));
    }
    if profile.nodes == 0 {
        return Err(invalid("deployment needs at least one node"));
    }
    if profile.regions.is_empty() {
        return Err(invalid("deployment needs at least one region"));
    }
    validate_unique_regions(&profile.regions)?;
    validate_label("implementation slug", &profile.implementation)?;
    validate_label("storage label", &profile.storage_label)?;
    validate_label("consensus label", &profile.consensus_label)?;
    Ok(())
}

fn validate_package(manifest: &DeploymentPackageManifest) -> RsfResult<()> {
    validate_label("manifest id", &manifest.id)?;
    validate_label("manifest name", &manifest.name)?;
    if manifest.nodes == 0 {
        return Err(invalid("manifest nodes must be greater than zero"));
    }
    if manifest.regions.is_empty() {
        return Err(invalid("manifest regions must not be empty"));
    }
    validate_unique_regions(&manifest.regions)?;
    for (label, value) in [
        ("manifest placement", &manifest.placement),
        ("manifest consensus", &manifest.consensus),
        ("manifest relational", &manifest.relational),
        ("manifest object", &manifest.object),
        ("manifest blob", &manifest.blob),
        ("manifest kv", &manifest.kv),
    ] {
        validate_label(label, value)?;
    }
    Ok(())
}

fn validate_unique_regions(regions: &[RegionId]) -> RsfResult<()> {
    let mut seen = BTreeSet::new();
    for region in regions {
        if !seen.insert(region.as_str()) {
            return Err(invalid(format!("duplicate deployment region {}", region)));
        }
    }
    Ok(())
}

fn validate_label(label: &str, value: &str) -> RsfResult<()> {
    if value.trim().is_empty() {
        Err(invalid(format!("{label} must not be empty")))
    } else if value != value.trim() {
        Err(invalid(format!("{label} must not contain edge whitespace")))
    } else {
        Ok(())
    }
}

fn take(fields: &mut std::collections::BTreeMap<String, String>, key: &str) -> RsfResult<String> {
    fields
        .remove(key)
        .ok_or_else(|| invalid(format!("missing manifest field '{key}'")))
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_replaceable_deployment_envelope() {
        let manifest = DeploymentPlanner
            .plan(profile("universal-vsr-rados", 5))
            .unwrap();

        assert_eq!(manifest.implementation, "universal-vsr-rados");
        assert_eq!(manifest.volumes.len(), 5);
        assert!(
            manifest
                .roles
                .iter()
                .any(|role| role.role == DeploymentRole::Sql)
        );
        assert!(
            manifest
                .endpoints
                .iter()
                .any(|endpoint| endpoint.role == DeploymentRole::Gateway)
        );
        assert_eq!(manifest.probes.len(), manifest.endpoints.len());
    }

    #[test]
    fn distributes_roles_across_regions() {
        let manifest = DeploymentPlanner
            .plan(profile("geo-local-wpaxos", 7))
            .unwrap();
        let regions = manifest
            .roles
            .iter()
            .map(|role| role.region.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(regions.len(), 2);
    }

    #[test]
    fn single_node_profile_still_assigns_every_required_role() {
        let manifest = DeploymentPlanner
            .plan(profile("single-node-production", 1))
            .unwrap();
        let roles = manifest
            .roles
            .iter()
            .map(|assignment| assignment.role)
            .collect::<BTreeSet<_>>();

        assert_eq!(manifest.volumes.len(), 1);
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
            assert!(
                roles.contains(&required),
                "missing role {}",
                required.as_str()
            );
        }
    }

    #[test]
    fn rejects_empty_regions() {
        let err = DeploymentPlanner
            .plan(DeploymentProfile {
                implementation: "bad".to_string(),
                nodes: 3,
                regions: Vec::new(),
                storage_label: "pgs".to_string(),
                consensus_label: "vsr".to_string(),
            })
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn rejects_duplicate_profile_regions() {
        let mut profile = profile("duplicate-region", 3);
        profile.regions = vec![
            RegionId::new("iad").expect("region"),
            RegionId::new("iad").expect("region"),
        ];

        let err = DeploymentPlanner
            .plan(profile)
            .expect_err("duplicate deployment regions must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate deployment region"));
    }

    #[test]
    fn parses_implementation_package_manifest() {
        let manifest = DeploymentPackageManifest::parse(
            "
            id: universal-vsr-rados
            name: Universal VSR-RADOS Fabric
            confidence: 91
            nodes: 5
            regions: iad,ord
            placement: RADOS-style placement groups
            consensus: Viewstamped Replication
            relational: ordered KV-backed tables
            object: strict manifests
            blob: content-addressed chunks
            kv: MVCC ordered keys
            ",
        )
        .unwrap();

        assert_eq!(manifest.id, "universal-vsr-rados");
        assert_eq!(
            manifest
                .regions
                .iter()
                .map(RegionId::as_str)
                .collect::<Vec<_>>(),
            vec!["iad", "ord"]
        );
        assert_eq!(manifest.deployment_profile().nodes, 5);
    }

    #[test]
    fn package_manifest_rejects_duplicate_fields() {
        let err = DeploymentPackageManifest::parse(
            "
            id: universal-vsr-rados
            id: overwritten
            name: Universal VSR-RADOS Fabric
            confidence: 91
            nodes: 5
            regions: iad,ord
            placement: RADOS-style placement groups
            consensus: Viewstamped Replication
            relational: ordered KV-backed tables
            object: strict manifests
            blob: content-addressed chunks
            kv: MVCC ordered keys
            ",
        )
        .expect_err("duplicate manifest fields must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate manifest field"));
    }

    #[test]
    fn package_manifest_rejects_duplicate_regions() {
        let err = DeploymentPackageManifest::parse(
            "
            id: universal-vsr-rados
            name: Universal VSR-RADOS Fabric
            confidence: 91
            nodes: 5
            regions: iad,iad
            placement: RADOS-style placement groups
            consensus: Viewstamped Replication
            relational: ordered KV-backed tables
            object: strict manifests
            blob: content-addressed chunks
            kv: MVCC ordered keys
            ",
        )
        .expect_err("duplicate manifest regions must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate deployment region"));
    }

    fn profile(implementation: &str, nodes: u8) -> DeploymentProfile {
        DeploymentProfile {
            implementation: implementation.to_string(),
            nodes,
            regions: vec![
                RegionId::new("iad").expect("region"),
                RegionId::new("sfo").expect("region"),
            ],
            storage_label: "placement groups".to_string(),
            consensus_label: "strict cells".to_string(),
        }
    }
}
