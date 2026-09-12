use std::collections::{BTreeMap, BTreeSet};

mod ledger;

pub use ledger::{
    DurableRebalanceLedger, RebalanceCompletion, RebalanceOperation, RebalanceOperationId,
    RebalanceOperationState,
};

use rsf_core::{
    Epoch, ErrorCode, NodeId, PlacementGroupId, RsfError, RsfResult,
    cluster::{ClusterMap, NodeRole},
    placement::{ActingSet, PlacementGroup, PlacementPolicy},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeLoad {
    pub node: NodeId,
    pub used_bytes: u64,
    pub capacity_bytes: u64,
}

impl NodeLoad {
    pub fn utilization_bps(&self) -> RsfResult<u64> {
        if self.capacity_bytes == 0 {
            return Err(invalid(format!("node {} has zero capacity", self.node.0)));
        }
        Ok(self.used_bytes.saturating_mul(10_000) / self.capacity_bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePolicy {
    pub high_watermark_bps: u64,
    pub low_watermark_bps: u64,
    pub min_replicas: u8,
    pub max_moves: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebalanceAction {
    MoveReplica {
        pg: PlacementGroupId,
        from: NodeId,
        to: NodeId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePlan {
    pub actions: Vec<RebalanceAction>,
    pub safety_checks: Vec<RebalanceCheck>,
    pub policy_proofs: Vec<PlacementPolicyProof>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementMoveResult {
    pub pg: PlacementGroupId,
    pub from: NodeId,
    pub to: NodeId,
    pub previous_epoch: Epoch,
    pub resulting_epoch: Epoch,
    pub previous_primary: NodeId,
    pub resulting_primary: NodeId,
    pub previous_replicas: Vec<NodeId>,
    pub resulting_replicas: Vec<NodeId>,
    pub resulting_group: PlacementGroup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceCheck {
    pub name: String,
    pub passed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementPolicyProof {
    pub pg: PlacementGroupId,
    pub previous_replicas: usize,
    pub resulting_replicas: usize,
    pub expected_replicas: usize,
    pub min_replicas: u8,
    pub source_was_member: bool,
    pub target_was_member: bool,
    pub target_is_storage: bool,
    pub no_duplicate_replicas: bool,
    pub previous_failure_domains: usize,
    pub resulting_failure_domains: usize,
    pub previous_regions: usize,
    pub resulting_regions: usize,
}

impl PlacementPolicyProof {
    pub fn preserves_policy(&self) -> bool {
        let min = usize::from(self.min_replicas);
        self.previous_replicas >= usize::from(self.min_replicas)
            && self.resulting_replicas >= min
            && self.previous_replicas == self.expected_replicas
            && self.resulting_replicas == self.expected_replicas
            && self.source_was_member
            && !self.target_was_member
            && self.target_is_storage
            && self.no_duplicate_replicas
            && self.previous_failure_domains > 0
            && self.resulting_failure_domains >= self.previous_failure_domains
    }
}

#[derive(Clone, Debug, Default)]
pub struct RebalancePlanner;

impl RebalancePlanner {
    pub fn plan(
        &self,
        cluster: &ClusterMap,
        groups: &[PlacementGroup],
        loads: &[NodeLoad],
        policy: &RebalancePolicy,
    ) -> RsfResult<RebalancePlan> {
        validate_policy(policy)?;
        let storage_nodes = cluster
            .nodes
            .iter()
            .filter(|node| node.roles.contains(&NodeRole::Storage))
            .map(|node| node.id)
            .collect::<BTreeSet<_>>();
        if storage_nodes.len() < usize::from(policy.min_replicas) {
            return Err(invalid("not enough storage nodes to satisfy min replicas"));
        }

        let loads = load_map(loads)?;
        let hot_nodes = loads
            .values()
            .filter(|load| load.utilization_bps().unwrap_or(u64::MAX) >= policy.high_watermark_bps)
            .map(|load| load.node)
            .collect::<BTreeSet<_>>();
        let cold_nodes = loads
            .values()
            .filter(|load| {
                storage_nodes.contains(&load.node)
                    && load.utilization_bps().unwrap_or(u64::MAX) <= policy.low_watermark_bps
            })
            .map(|load| load.node)
            .collect::<Vec<_>>();

        let mut actions = Vec::new();
        let mut policy_proofs = Vec::new();
        for group in groups {
            if actions.len() >= policy.max_moves {
                break;
            }
            let PlacementPolicy::Replicated { replicas } = group.policy else {
                continue;
            };
            if replicas < policy.min_replicas {
                return Err(invalid(format!(
                    "pg {} replica policy is below minimum",
                    group.id.0
                )));
            }
            let Some(from) = group
                .acting_set
                .replicas
                .iter()
                .copied()
                .find(|node| hot_nodes.contains(node))
            else {
                continue;
            };
            let Some(to) = cold_nodes
                .iter()
                .copied()
                .find(|node| !group.acting_set.replicas.contains(node))
            else {
                continue;
            };

            let resulting_replicas = group
                .acting_set
                .replicas
                .iter()
                .copied()
                .map(|node| if node == from { to } else { node })
                .collect::<Vec<_>>();
            let resulting = resulting_replicas.iter().copied().collect::<BTreeSet<_>>();
            let expected = expected_replicas(&group.policy);
            if resulting_replicas.len() != expected {
                return Err(invalid(format!(
                    "pg {} has {} replicas but policy expects {}",
                    group.id.0,
                    resulting_replicas.len(),
                    expected
                )));
            }
            if resulting.len() != resulting_replicas.len() {
                return Err(invalid(format!(
                    "pg {} movement would duplicate a replica target",
                    group.id.0
                )));
            }
            if resulting.len() < usize::from(policy.min_replicas) {
                return Err(invalid(format!(
                    "pg {} movement would reduce replica guarantees",
                    group.id.0
                )));
            }
            let proof = build_policy_proof(cluster, group, from, to, policy.min_replicas)?;
            if !proof.preserves_policy() {
                return Err(invalid(format!(
                    "pg {} movement would not preserve placement policy: {:?}",
                    group.id.0, proof
                )));
            }
            policy_proofs.push(proof);
            actions.push(RebalanceAction::MoveReplica {
                pg: group.id,
                from,
                to,
            });
        }

        Ok(RebalancePlan {
            actions,
            policy_proofs,
            safety_checks: vec![
                RebalanceCheck {
                    name: "storage-capacity".to_string(),
                    passed: true,
                },
                RebalanceCheck {
                    name: "replica-count".to_string(),
                    passed: true,
                },
                RebalanceCheck {
                    name: "target-not-existing-replica".to_string(),
                    passed: true,
                },
                RebalanceCheck {
                    name: "failure-domain-preservation".to_string(),
                    passed: true,
                },
            ],
        })
    }

    pub fn execute_move(
        &self,
        cluster: &ClusterMap,
        group: &PlacementGroup,
        action: &RebalanceAction,
        proof: &PlacementPolicyProof,
    ) -> RsfResult<PlacementMoveResult> {
        let RebalanceAction::MoveReplica { pg, from, to } = *action;
        if pg != group.id {
            return Err(invalid(format!(
                "rebalance action pg {} does not match placement group {}",
                pg.0, group.id.0
            )));
        }
        if proof.pg != group.id {
            return Err(invalid(format!(
                "rebalance proof pg {} does not match placement group {}",
                proof.pg.0, group.id.0
            )));
        }
        group.acting_set.validate()?;
        if !group.acting_set.replicas.contains(&from) {
            return Err(invalid(format!(
                "rebalance source node {} is not a replica of pg {}",
                from.0, group.id.0
            )));
        }
        if group.acting_set.replicas.contains(&to) {
            return Err(invalid(format!(
                "rebalance target node {} is already a replica of pg {}",
                to.0, group.id.0
            )));
        }
        if !is_storage_node(cluster, to) {
            return Err(invalid(format!(
                "rebalance target node {} is not an active storage node",
                to.0
            )));
        }

        let mut resulting_replicas = group.acting_set.replicas.clone();
        let source_index = resulting_replicas
            .iter()
            .position(|node| *node == from)
            .expect("source membership checked above");
        resulting_replicas[source_index] = to;
        let resulting_unique = resulting_replicas.iter().copied().collect::<BTreeSet<_>>();
        if resulting_unique.len() != resulting_replicas.len() {
            return Err(invalid(format!(
                "rebalance movement for pg {} would create duplicate replicas",
                group.id.0
            )));
        }

        let expected = expected_replicas(&group.policy);
        if resulting_replicas.len() != expected {
            return Err(invalid(format!(
                "rebalance movement for pg {} would produce {} replicas, expected {}",
                group.id.0,
                resulting_replicas.len(),
                expected
            )));
        }
        let actual_proof = build_policy_proof(cluster, group, from, to, proof.min_replicas)?;
        if &actual_proof != proof || !proof.preserves_policy() {
            return Err(invalid(format!(
                "rebalance movement proof for pg {} does not preserve placement policy",
                group.id.0
            )));
        }

        let previous_primary = group.acting_set.primary;
        let resulting_primary = if previous_primary == from {
            group
                .acting_set
                .replicas
                .iter()
                .copied()
                .find(|node| *node != from)
                .unwrap_or(to)
        } else {
            previous_primary
        };
        let resulting_epoch = Epoch(group.acting_set.epoch.0.saturating_add(1));
        let resulting_acting_set = ActingSet {
            epoch: resulting_epoch,
            primary: resulting_primary,
            replicas: resulting_replicas.clone(),
        };
        resulting_acting_set.validate()?;
        let resulting_group = PlacementGroup {
            id: group.id,
            acting_set: resulting_acting_set,
            policy: group.policy.clone(),
        };

        Ok(PlacementMoveResult {
            pg,
            from,
            to,
            previous_epoch: group.acting_set.epoch,
            resulting_epoch,
            previous_primary,
            resulting_primary,
            previous_replicas: group.acting_set.replicas.clone(),
            resulting_replicas,
            resulting_group,
        })
    }
}

fn validate_policy(policy: &RebalancePolicy) -> RsfResult<()> {
    if policy.min_replicas == 0 {
        return Err(invalid("min replicas must be greater than zero"));
    }
    if policy.high_watermark_bps <= policy.low_watermark_bps {
        return Err(invalid("high watermark must exceed low watermark"));
    }
    if policy.max_moves == 0 {
        return Err(invalid("max moves must be greater than zero"));
    }
    Ok(())
}

fn build_policy_proof(
    cluster: &ClusterMap,
    group: &PlacementGroup,
    from: NodeId,
    to: NodeId,
    min_replicas: u8,
) -> RsfResult<PlacementPolicyProof> {
    let resulting_replicas = group
        .acting_set
        .replicas
        .iter()
        .map(|node| if *node == from { to } else { *node })
        .collect::<Vec<_>>();
    let previous_topology = topology_counts(cluster, &group.acting_set.replicas)?;
    let resulting_topology = topology_counts(cluster, &resulting_replicas)?;
    Ok(PlacementPolicyProof {
        pg: group.id,
        previous_replicas: group.acting_set.replicas.len(),
        resulting_replicas: resulting_replicas.len(),
        expected_replicas: expected_replicas(&group.policy),
        min_replicas,
        source_was_member: group.acting_set.replicas.contains(&from),
        target_was_member: group.acting_set.replicas.contains(&to),
        target_is_storage: is_storage_node(cluster, to),
        no_duplicate_replicas: resulting_replicas
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            == resulting_replicas.len(),
        previous_failure_domains: previous_topology.failure_domains,
        resulting_failure_domains: resulting_topology.failure_domains,
        previous_regions: previous_topology.regions,
        resulting_regions: resulting_topology.regions,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TopologyCounts {
    failure_domains: usize,
    regions: usize,
}

fn topology_counts(cluster: &ClusterMap, replicas: &[NodeId]) -> RsfResult<TopologyCounts> {
    let mut failure_domains = BTreeSet::new();
    let mut regions = BTreeSet::new();
    for replica in replicas {
        let node = cluster
            .nodes
            .iter()
            .find(|candidate| candidate.id == *replica)
            .ok_or_else(|| {
                invalid(format!(
                    "replica node {} is missing from cluster",
                    replica.0
                ))
            })?;
        if !node.roles.contains(&NodeRole::Storage) {
            return Err(invalid(format!(
                "replica node {} is not an active storage node",
                replica.0
            )));
        }
        failure_domains.insert(node.failure_domain.clone());
        regions.insert(node.region.clone());
    }
    Ok(TopologyCounts {
        failure_domains: failure_domains.len(),
        regions: regions.len(),
    })
}

fn load_map(loads: &[NodeLoad]) -> RsfResult<BTreeMap<NodeId, NodeLoad>> {
    let mut map = BTreeMap::new();
    for load in loads {
        load.utilization_bps()?;
        if map.insert(load.node, load.clone()).is_some() {
            return Err(invalid(format!("duplicate load for node {}", load.node.0)));
        }
    }
    Ok(map)
}

fn is_storage_node(cluster: &ClusterMap, node: NodeId) -> bool {
    cluster
        .nodes
        .iter()
        .any(|candidate| candidate.id == node && candidate.roles.contains(&NodeRole::Storage))
}

fn expected_replicas(policy: &PlacementPolicy) -> usize {
    match policy {
        PlacementPolicy::Replicated { replicas } => usize::from(*replicas),
        PlacementPolicy::ErasureCoded { data, parity } => usize::from(*data + *parity),
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{
        Epoch, FailureDomainId, RegionId,
        cluster::NodeDescriptor,
        placement::{ActingSet, PlacementPolicy},
    };

    #[test]
    fn planner_moves_replica_from_hot_node_to_cold_non_member() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let groups = vec![group(1, [NodeId(0), NodeId(1), NodeId(2)])];
        let plan = planner
            .plan(
                &cluster,
                &groups,
                &[load(0, 900), load(1, 500), load(2, 500), load(3, 100)],
                &policy(),
            )
            .unwrap();

        assert_eq!(
            plan.actions,
            vec![RebalanceAction::MoveReplica {
                pg: PlacementGroupId(1),
                from: NodeId(0),
                to: NodeId(3),
            }]
        );
        assert_eq!(plan.safety_checks.len(), 4);
        assert_eq!(
            plan.policy_proofs,
            vec![PlacementPolicyProof {
                pg: PlacementGroupId(1),
                previous_replicas: 3,
                resulting_replicas: 3,
                expected_replicas: 3,
                min_replicas: 3,
                source_was_member: true,
                target_was_member: false,
                target_is_storage: true,
                no_duplicate_replicas: true,
                previous_failure_domains: 3,
                resulting_failure_domains: 3,
                previous_regions: 1,
                resulting_regions: 1,
            }]
        );
        assert!(plan.policy_proofs[0].preserves_policy());
    }

    #[test]
    fn planner_does_not_move_when_no_cold_non_member_exists() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let groups = vec![group(1, [NodeId(0), NodeId(1), NodeId(2)])];
        let plan = planner
            .plan(
                &cluster,
                &groups,
                &[load(0, 900), load(1, 500), load(2, 500), load(3, 700)],
                &policy(),
            )
            .unwrap();

        assert!(plan.actions.is_empty());
    }

    #[test]
    fn planner_rejects_move_that_reduces_failure_domain_diversity() {
        let planner = RebalancePlanner;
        let mut cluster = cluster();
        cluster.nodes[3].failure_domain = cluster.nodes[1].failure_domain.clone();
        let groups = vec![group(1, [NodeId(0), NodeId(1), NodeId(2)])];

        let err = planner
            .plan(
                &cluster,
                &groups,
                &[load(0, 900), load(1, 500), load(2, 500), load(3, 100)],
                &policy(),
            )
            .expect_err("failure-domain regression must reject rebalance move");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("would not preserve"));
    }

    #[test]
    fn move_execution_updates_placement_group_and_advances_epoch() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let group = group(1, [NodeId(0), NodeId(1), NodeId(2)]);
        let plan = planner
            .plan(
                &cluster,
                std::slice::from_ref(&group),
                &[load(0, 900), load(1, 500), load(2, 500), load(3, 100)],
                &policy(),
            )
            .unwrap();

        let result = planner
            .execute_move(&cluster, &group, &plan.actions[0], &plan.policy_proofs[0])
            .unwrap();

        assert_eq!(result.pg, PlacementGroupId(1));
        assert_eq!(result.from, NodeId(0));
        assert_eq!(result.to, NodeId(3));
        assert_eq!(result.previous_epoch, Epoch(1));
        assert_eq!(result.resulting_epoch, Epoch(2));
        assert_eq!(result.previous_primary, NodeId(0));
        assert_eq!(result.resulting_primary, NodeId(1));
        assert_eq!(
            result.previous_replicas,
            vec![NodeId(0), NodeId(1), NodeId(2)]
        );
        assert_eq!(
            result.resulting_replicas,
            vec![NodeId(3), NodeId(1), NodeId(2)]
        );
        assert_eq!(result.resulting_group.acting_set.epoch, Epoch(2));
        assert_eq!(result.resulting_group.acting_set.primary, NodeId(1));
        assert_eq!(
            result.resulting_group.acting_set.replicas,
            vec![NodeId(3), NodeId(1), NodeId(2)]
        );
    }

    #[test]
    fn move_execution_preserves_primary_when_primary_is_not_moved() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let group = group(1, [NodeId(1), NodeId(0), NodeId(2)]);
        let action = RebalanceAction::MoveReplica {
            pg: PlacementGroupId(1),
            from: NodeId(0),
            to: NodeId(3),
        };
        let proof = build_policy_proof(&cluster, &group, NodeId(0), NodeId(3), 3).unwrap();

        let result = planner
            .execute_move(&cluster, &group, &action, &proof)
            .unwrap();

        assert_eq!(result.previous_primary, NodeId(1));
        assert_eq!(result.resulting_primary, NodeId(1));
        assert_eq!(
            result.resulting_group.acting_set.replicas,
            vec![NodeId(1), NodeId(3), NodeId(2)]
        );
    }

    #[test]
    fn move_execution_rejects_existing_replica_target() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let group = group(1, [NodeId(0), NodeId(1), NodeId(2)]);
        let action = RebalanceAction::MoveReplica {
            pg: PlacementGroupId(1),
            from: NodeId(0),
            to: NodeId(1),
        };
        let proof = build_policy_proof(&cluster, &group, NodeId(0), NodeId(1), 3).unwrap();

        let err = planner
            .execute_move(&cluster, &group, &action, &proof)
            .expect_err("target already in acting set must reject");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("already a replica"));
    }

    #[test]
    fn move_execution_rejects_non_storage_target() {
        let planner = RebalancePlanner;
        let mut non_storage_cluster = cluster();
        non_storage_cluster.nodes[3].roles.clear();
        let group = group(1, [NodeId(0), NodeId(1), NodeId(2)]);
        let action = RebalanceAction::MoveReplica {
            pg: PlacementGroupId(1),
            from: NodeId(0),
            to: NodeId(3),
        };
        let proof = build_policy_proof(&cluster(), &group, NodeId(0), NodeId(3), 3).unwrap();

        let err = planner
            .execute_move(&non_storage_cluster, &group, &action, &proof)
            .expect_err("non-storage target must reject");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("not an active storage node"));
    }

    #[test]
    fn move_execution_rejects_policy_proof_mismatch() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let group = group(1, [NodeId(0), NodeId(1), NodeId(2)]);
        let action = RebalanceAction::MoveReplica {
            pg: PlacementGroupId(1),
            from: NodeId(0),
            to: NodeId(3),
        };
        let mut proof = build_policy_proof(&cluster, &group, NodeId(0), NodeId(3), 3).unwrap();
        proof.resulting_failure_domains = 2;

        let err = planner
            .execute_move(&cluster, &group, &action, &proof)
            .expect_err("invalid proof must reject");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("does not preserve"));
    }

    #[test]
    fn planner_rejects_policy_below_replica_floor() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let groups = vec![PlacementGroup {
            id: PlacementGroupId(1),
            acting_set: ActingSet {
                epoch: Epoch(1),
                primary: NodeId(0),
                replicas: vec![NodeId(0), NodeId(1)],
            },
            policy: PlacementPolicy::Replicated { replicas: 2 },
        }];

        let err = planner
            .plan(
                &cluster,
                &groups,
                &[load(0, 900), load(1, 500), load(2, 100), load(3, 100)],
                &policy(),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("below minimum"));
    }

    #[test]
    fn zero_capacity_load_is_rejected() {
        let planner = RebalancePlanner;
        let cluster = cluster();
        let groups = vec![group(1, [NodeId(0), NodeId(1), NodeId(2)])];

        let err = planner
            .plan(
                &cluster,
                &groups,
                &[NodeLoad {
                    node: NodeId(0),
                    used_bytes: 1,
                    capacity_bytes: 0,
                }],
                &policy(),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    fn cluster() -> ClusterMap {
        ClusterMap {
            epoch: Epoch(1),
            nodes: (0..4)
                .map(|id| NodeDescriptor {
                    id: NodeId(id),
                    region: RegionId::new("iad").expect("region"),
                    failure_domain: FailureDomainId::new(format!("iad-{id}")).expect("domain"),
                    capacity_bytes: 1_000,
                    roles: vec![NodeRole::Storage],
                })
                .collect(),
        }
    }

    fn group(id: u64, replicas: [NodeId; 3]) -> PlacementGroup {
        PlacementGroup {
            id: PlacementGroupId(id),
            acting_set: ActingSet {
                epoch: Epoch(1),
                primary: replicas[0],
                replicas: replicas.to_vec(),
            },
            policy: PlacementPolicy::Replicated { replicas: 3 },
        }
    }

    fn load(node: u64, used_bps: u64) -> NodeLoad {
        NodeLoad {
            node: NodeId(node),
            used_bytes: used_bps,
            capacity_bytes: 1_000,
        }
    }

    fn policy() -> RebalancePolicy {
        RebalancePolicy {
            high_watermark_bps: 8_000,
            low_watermark_bps: 2_000,
            min_replicas: 3,
            max_moves: 1,
        }
    }
}
