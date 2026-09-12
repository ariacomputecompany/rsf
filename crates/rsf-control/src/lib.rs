#![forbid(unsafe_code)]

use rsf_admin::{AdminPlane, PlacementPlan};
use rsf_core::{PlacementGroupId, RsfResult};
use rsf_observability::AuditEvent;
use rsf_policy::{DesiredClusterPolicy, PolicyPreflightReport};
use rsf_protocol::VsrBackedFabricRouter;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyReconciliationPlan {
    pub policy_version: u64,
    pub preflight: PolicyPreflightReport,
    pub desired_groups: Vec<PlacementGroupId>,
    pub steps: Vec<PolicyReconciliationStep>,
}

impl PolicyReconciliationPlan {
    pub fn changed(&self) -> bool {
        !self.steps.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PolicyReconciliationStep {
    RelaxProtocolPolicy {
        from_min_placement_groups: u64,
        to_min_placement_groups: u64,
    },
    ApplyPlacementMap {
        from_version: u64,
        from_groups: Vec<PlacementGroupId>,
        to_groups: Vec<PlacementGroupId>,
    },
    TightenProtocolPolicy {
        from_min_placement_groups: u64,
        to_min_placement_groups: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyReconciliationOutcome {
    pub plan: PolicyReconciliationPlan,
    pub placement_map_version: u64,
    pub placement_groups: usize,
    pub protocol_policy_version: u64,
    pub progress: Vec<PolicyProgressEvent>,
    pub audit: Vec<AuditEvent>,
}

impl PolicyReconciliationOutcome {
    pub fn changed(&self) -> bool {
        self.plan.changed()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyProgressEvent {
    pub sequence: u64,
    pub policy_version: u64,
    pub phase: PolicyProgressPhase,
    pub detail: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyProgressPhase {
    Started,
    Planned,
    StepStarted,
    StepApplied,
    Noop,
    Completed,
}

pub trait PolicyProgressSink {
    fn emit(&mut self, event: PolicyProgressEvent);
}

#[derive(Debug, Default)]
pub struct RecordingPolicyProgressSink {
    events: Vec<PolicyProgressEvent>,
}

impl RecordingPolicyProgressSink {
    pub fn into_events(self) -> Vec<PolicyProgressEvent> {
        self.events
    }
}

impl PolicyProgressSink for RecordingPolicyProgressSink {
    fn emit(&mut self, event: PolicyProgressEvent) {
        self.events.push(event);
    }
}

#[derive(Debug, Default)]
pub struct PolicyReconciler;

impl PolicyReconciler {
    pub fn plan(
        admin: &AdminPlane,
        router: &VsrBackedFabricRouter,
        policy: &DesiredClusterPolicy,
    ) -> RsfResult<PolicyReconciliationPlan> {
        let placement =
            admin.preview_replicated_groups(policy.placement_groups, policy.min_replicas)?;
        Ok(Self::plan_from_preview(router, policy, &placement))
    }

    pub fn apply(
        admin: &mut AdminPlane,
        router: &mut VsrBackedFabricRouter,
        policy: &DesiredClusterPolicy,
    ) -> RsfResult<PolicyReconciliationOutcome> {
        let mut sink = RecordingPolicyProgressSink::default();
        let mut outcome = Self::apply_with_progress(admin, router, policy, &mut sink)?;
        outcome.progress = sink.into_events();
        Ok(outcome)
    }

    pub fn apply_with_progress(
        admin: &mut AdminPlane,
        router: &mut VsrBackedFabricRouter,
        policy: &DesiredClusterPolicy,
        progress: &mut impl PolicyProgressSink,
    ) -> RsfResult<PolicyReconciliationOutcome> {
        let mut sequence = 1_u64;
        emit_progress(
            progress,
            &mut sequence,
            policy.version,
            PolicyProgressPhase::Started,
            format!("reconciling desired policy version {}", policy.version),
        );
        let placement =
            admin.preview_replicated_groups(policy.placement_groups, policy.min_replicas)?;
        let plan = Self::plan_from_preview(router, policy, &placement);
        emit_progress(
            progress,
            &mut sequence,
            policy.version,
            PolicyProgressPhase::Planned,
            format!(
                "planned {} reconciliation steps for {} placement groups",
                plan.steps.len(),
                plan.desired_groups.len()
            ),
        );
        let mut audit_records = vec![audit(
            "policy.reconcile.planned",
            format!(
                "policy version {} planned {} reconciliation steps",
                policy.version,
                plan.steps.len()
            ),
        )];
        if plan.steps.is_empty() {
            emit_progress(
                progress,
                &mut sequence,
                policy.version,
                PolicyProgressPhase::Noop,
                "actual policy state already matches desired policy".to_string(),
            );
            audit_records.push(audit(
                "policy.reconcile.noop",
                format!("policy version {} already converged", policy.version),
            ));
        }
        for step in &plan.steps {
            let detail = step.detail();
            emit_progress(
                progress,
                &mut sequence,
                policy.version,
                PolicyProgressPhase::StepStarted,
                detail.clone(),
            );
            match step {
                PolicyReconciliationStep::RelaxProtocolPolicy {
                    to_min_placement_groups,
                    ..
                }
                | PolicyReconciliationStep::TightenProtocolPolicy {
                    to_min_placement_groups,
                    ..
                } => {
                    admin.apply_protocol_policy(router, true, *to_min_placement_groups)?;
                }
                PolicyReconciliationStep::ApplyPlacementMap { .. } => {
                    admin.apply_placement_plan(router, &placement)?;
                }
            }
            emit_progress(
                progress,
                &mut sequence,
                policy.version,
                PolicyProgressPhase::StepApplied,
                detail.clone(),
            );
            audit_records.push(audit("policy.reconcile.step_applied", detail));
        }
        emit_progress(
            progress,
            &mut sequence,
            policy.version,
            PolicyProgressPhase::Completed,
            format!(
                "policy version {} converged at placement map version {}",
                policy.version,
                router.placement_map().version
            ),
        );
        audit_records.push(audit(
            "policy.reconcile.completed",
            format!(
                "policy version {} converged placement_groups={} protocol_policy_version={}",
                policy.version,
                router.placement_map().groups.len(),
                router.policy().version
            ),
        ));
        Ok(PolicyReconciliationOutcome {
            plan,
            placement_map_version: router.placement_map().version,
            placement_groups: router.placement_map().groups.len(),
            protocol_policy_version: router.policy().version,
            progress: Vec::new(),
            audit: audit_records,
        })
    }

    fn plan_from_preview(
        router: &VsrBackedFabricRouter,
        policy: &DesiredClusterPolicy,
        placement: &PlacementPlan,
    ) -> PolicyReconciliationPlan {
        let desired_groups = placement
            .groups
            .iter()
            .map(|group| group.id)
            .collect::<Vec<_>>();
        let mut steps = Vec::new();
        let current_min_placement_groups = router.policy().min_placement_groups;
        if current_min_placement_groups > policy.placement_groups {
            steps.push(PolicyReconciliationStep::RelaxProtocolPolicy {
                from_min_placement_groups: current_min_placement_groups,
                to_min_placement_groups: policy.placement_groups,
            });
        }
        if router.placement_map().groups != desired_groups {
            steps.push(PolicyReconciliationStep::ApplyPlacementMap {
                from_version: router.placement_map().version,
                from_groups: router.placement_map().groups.clone(),
                to_groups: desired_groups.clone(),
            });
        }
        if current_min_placement_groups < policy.placement_groups
            || !router.policy().allow_availability_first_immutable
        {
            steps.push(PolicyReconciliationStep::TightenProtocolPolicy {
                from_min_placement_groups: current_min_placement_groups,
                to_min_placement_groups: policy.placement_groups,
            });
        }
        PolicyReconciliationPlan {
            policy_version: policy.version,
            preflight: policy.preflight(),
            desired_groups,
            steps,
        }
    }
}

impl PolicyReconciliationStep {
    pub fn detail(&self) -> String {
        match self {
            Self::RelaxProtocolPolicy {
                from_min_placement_groups,
                to_min_placement_groups,
            } => format!(
                "relax protocol policy min_placement_groups {} -> {}",
                from_min_placement_groups, to_min_placement_groups
            ),
            Self::ApplyPlacementMap {
                from_version,
                from_groups,
                to_groups,
            } => format!(
                "apply placement map version {} groups {} -> {}",
                from_version,
                format_groups(from_groups),
                format_groups(to_groups)
            ),
            Self::TightenProtocolPolicy {
                from_min_placement_groups,
                to_min_placement_groups,
            } => format!(
                "tighten protocol policy min_placement_groups {} -> {}",
                from_min_placement_groups, to_min_placement_groups
            ),
        }
    }
}

fn emit_progress(
    sink: &mut impl PolicyProgressSink,
    sequence: &mut u64,
    policy_version: u64,
    phase: PolicyProgressPhase,
    detail: String,
) {
    sink.emit(PolicyProgressEvent {
        sequence: *sequence,
        policy_version,
        phase,
        detail,
    });
    *sequence += 1;
}

fn audit(action: impl Into<String>, detail: impl Into<String>) -> AuditEvent {
    AuditEvent {
        service: "control".to_string(),
        action: action.into(),
        version: None,
        node: None,
        pg: None,
        detail: detail.into(),
    }
}

fn format_groups(groups: &[PlacementGroupId]) -> String {
    groups
        .iter()
        .map(|group| group.0.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rsf_admin::{InviteToken, JoinRequest, NodeIdentity};
    use rsf_core::{Digest, Epoch, FailureDomainId, NodeId, PlacementGroupId, RegionId};
    use rsf_policy::PolicyCatalog;
    use rsf_protocol::FabricOperation;

    #[test]
    fn reconciliation_expands_placement_and_tightens_protocol_policy() {
        let admin_path = test_path("reconciliation_expands_admin");
        let policy_path = test_path("reconciliation_expands_policy");
        let protocol_path = test_path("reconciliation_expands_protocol");
        let mut admin = three_node_admin(&admin_path);
        let mut catalog = PolicyCatalog::open(&policy_path).expect("catalog");
        let policy = catalog
            .set_replication(3, 3)
            .expect("desired replication")
            .clone();
        let mut router = VsrBackedFabricRouter::open(
            &protocol_path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("router");
        let initial = router
            .commit_routed(FabricOperation::KvMutation {
                key: b"tenant/before-placement-policy".to_vec(),
            })
            .expect("initial routed commit");
        assert_eq!(initial.pg, PlacementGroupId(1));

        let planned = PolicyReconciler::plan(&admin, &router, &policy).expect("plan");
        let outcome = PolicyReconciler::apply(&mut admin, &mut router, &policy).expect("apply");
        let post_update_operation = operation_routed_outside_initial_group(&router);
        let expected_pg = router.placement_map().route(&post_update_operation);
        let post_update = router
            .commit_routed(post_update_operation)
            .expect("post-policy routed commit");

        assert_eq!(
            planned.steps,
            vec![
                PolicyReconciliationStep::ApplyPlacementMap {
                    from_version: 1,
                    from_groups: vec![PlacementGroupId(1)],
                    to_groups: vec![
                        PlacementGroupId(1),
                        PlacementGroupId(2),
                        PlacementGroupId(3)
                    ],
                },
                PolicyReconciliationStep::TightenProtocolPolicy {
                    from_min_placement_groups: 1,
                    to_min_placement_groups: 3,
                },
            ]
        );
        assert!(planned.preflight.passed());
        assert!(
            planned
                .preflight
                .implications
                .iter()
                .any(|detail| detail.contains("latency:"))
        );
        assert!(outcome.changed());
        assert_eq!(outcome.placement_groups, 3);
        assert_eq!(router.policy().min_placement_groups, 3);
        assert_ne!(expected_pg, PlacementGroupId(1));
        assert_eq!(post_update.pg, expected_pg);
        assert_eq!(
            outcome
                .progress
                .iter()
                .map(|event| event.phase)
                .collect::<Vec<_>>(),
            vec![
                PolicyProgressPhase::Started,
                PolicyProgressPhase::Planned,
                PolicyProgressPhase::StepStarted,
                PolicyProgressPhase::StepApplied,
                PolicyProgressPhase::StepStarted,
                PolicyProgressPhase::StepApplied,
                PolicyProgressPhase::Completed,
            ]
        );
        assert_eq!(
            outcome
                .audit
                .iter()
                .map(|event| event.action.as_str())
                .collect::<Vec<_>>(),
            vec![
                "policy.reconcile.planned",
                "policy.reconcile.step_applied",
                "policy.reconcile.step_applied",
                "policy.reconcile.completed",
            ]
        );

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(policy_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn reconciliation_relaxes_policy_before_shrinking_placement() {
        let admin_path = test_path("reconciliation_shrinks_admin");
        let policy_path = test_path("reconciliation_shrinks_policy");
        let protocol_path = test_path("reconciliation_shrinks_protocol");
        let mut admin = three_node_admin(&admin_path);
        let mut catalog = PolicyCatalog::open(&policy_path).expect("catalog");
        let expanded = catalog
            .set_replication(3, 3)
            .expect("expanded policy")
            .clone();
        let desired = catalog
            .set_replication(3, 1)
            .expect("shrink policy")
            .clone();
        let mut router = VsrBackedFabricRouter::open(
            &protocol_path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("router");
        PolicyReconciler::apply(&mut admin, &mut router, &expanded).expect("expand");

        let planned = PolicyReconciler::plan(&admin, &router, &desired).expect("plan shrink");
        let outcome = PolicyReconciler::apply(&mut admin, &mut router, &desired).expect("shrink");

        assert_eq!(
            planned.steps,
            vec![
                PolicyReconciliationStep::RelaxProtocolPolicy {
                    from_min_placement_groups: 3,
                    to_min_placement_groups: 1,
                },
                PolicyReconciliationStep::ApplyPlacementMap {
                    from_version: 2,
                    from_groups: vec![
                        PlacementGroupId(1),
                        PlacementGroupId(2),
                        PlacementGroupId(3)
                    ],
                    to_groups: vec![PlacementGroupId(1)],
                },
            ]
        );
        assert!(outcome.changed());
        assert_eq!(router.placement_map().groups, vec![PlacementGroupId(1)]);
        assert_eq!(router.policy().min_placement_groups, 1);

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(policy_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    #[test]
    fn reconciliation_noop_emits_progress_and_audit_without_changes() {
        let admin_path = test_path("reconciliation_noop_admin");
        let policy_path = test_path("reconciliation_noop_policy");
        let protocol_path = test_path("reconciliation_noop_protocol");
        let mut admin = three_node_admin(&admin_path);
        let mut catalog = PolicyCatalog::open(&policy_path).expect("catalog");
        let policy = catalog
            .set_replication(3, 1)
            .expect("desired policy")
            .clone();
        let mut router = VsrBackedFabricRouter::open(
            &protocol_path,
            [PlacementGroupId(1)],
            Epoch(1),
            NodeId(0),
            [NodeId(0), NodeId(1), NodeId(2)],
        )
        .expect("router");

        let outcome = PolicyReconciler::apply(&mut admin, &mut router, &policy).expect("apply");

        assert!(!outcome.changed());
        assert_eq!(
            outcome
                .progress
                .iter()
                .map(|event| event.phase)
                .collect::<Vec<_>>(),
            vec![
                PolicyProgressPhase::Started,
                PolicyProgressPhase::Planned,
                PolicyProgressPhase::Noop,
                PolicyProgressPhase::Completed,
            ]
        );
        assert!(
            outcome
                .audit
                .iter()
                .any(|event| event.action == "policy.reconcile.noop")
        );

        let _ = std::fs::remove_dir_all(admin_path);
        let _ = std::fs::remove_dir_all(policy_path);
        let _ = std::fs::remove_dir_all(protocol_path);
    }

    fn three_node_admin(path: &std::path::Path) -> AdminPlane {
        let mut admin =
            AdminPlane::open_durable(path, InviteToken("invite".to_string())).expect("admin");
        for zone in ["a", "b", "c"] {
            let record = admin.admit(request(zone)).expect("admit");
            admin
                .mark_learner_synced(record.node.id)
                .expect("sync learner");
            admin
                .promote_synced_learner(record.node.id)
                .expect("promote learner");
        }
        admin
    }

    fn operation_routed_outside_initial_group(router: &VsrBackedFabricRouter) -> FabricOperation {
        for suffix in 0..256_u16 {
            let operation = FabricOperation::KvMutation {
                key: format!("tenant/after-placement-policy/{suffix}").into_bytes(),
            };
            if router.placement_map().route(&operation) != PlacementGroupId(1) {
                return operation;
            }
        }
        panic!("expected an operation to route outside placement group 1");
    }

    fn request(zone: &str) -> JoinRequest {
        JoinRequest {
            token: InviteToken("invite".to_string()),
            identity: NodeIdentity::new(
                format!("node-public-key-{zone}"),
                format!("nonce-{zone}-000"),
                "cluster.example",
                fingerprint(zone),
            )
            .expect("identity"),
            region: RegionId::new("iad").expect("region"),
            zone: FailureDomainId::new(zone).expect("zone"),
            capacity_bytes: 1 << 30,
            supports_storage: true,
            supports_gateway: true,
        }
    }

    fn fingerprint(seed: &str) -> Digest {
        let mut bytes = [0_u8; 32];
        for (index, byte) in seed.bytes().enumerate() {
            bytes[index % 32] = bytes[index % 32].wrapping_add(byte).wrapping_add(1);
        }
        Digest::sha256(bytes).expect("sha256 fingerprint")
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-control-{name}-{}-{nonce}", std::process::id()))
    }
}
