#![forbid(unsafe_code)]

mod config;
mod daemon;

use std::time::Duration;

use config::{ServeConfigOverrides, resolve_daemon_config};
use daemon::{DaemonConfig, run_daemon};
use rsf_admin::{AdminPlane, InviteToken, JoinRequest, NodeIdentity};
use rsf_control::{PolicyProgressEvent, PolicyProgressPhase, PolicyProgressSink, PolicyReconciler};
use rsf_core::cluster::{NodeDescriptor, NodeRole};
use rsf_core::{CommitVersion, Digest, Epoch, FailureDomainId, PlacementGroupId, RegionId};
use rsf_observability::{AuditEvent, DurableAuditLog};
use rsf_policy::{PolicyCatalog, RegionRole};
use rsf_protocol::VsrBackedFabricRouter;
use rsf_upgrade::{NodeVersion, SoftwareVersion, UpgradePlanner, UpgradePolicy};

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if let Err(err) = run_cli(&args) {
        eprintln!("rsfd error: {}", err);
        std::process::exit(1);
    }
}

fn run_cli(args: &[String]) -> Result<(), String> {
    match args {
        [] => Err(usage()),
        [command, rest @ ..] if command == "status" => run_status_cli(rest),
        [command, rest @ ..] if command == "init" => run_init_cli(rest),
        [command, rest @ ..] if command == "join" => run_join_cli(rest),
        [command, rest @ ..] if command == "serve" => run_serve_cli(rest),
        [command, rest @ ..] if command == "policy" => run_policy_cli(rest),
        [command, rest @ ..] if command == "audit" => run_audit_cli(rest),
        [command, rest @ ..] if command == "upgrade" => run_upgrade_cli(rest),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: rsfd [status --state <dir> | audit --state <dir> | upgrade plan --state <dir> --from <semver> --target <semver> [--node-version <node=semver>] [--min-compatible-nodes <n>] | init --state <dir> --token <token> --node-key <key> --node-nonce <nonce> --mtls-trust-domain <domain> --tls-cert-sha256 <hex> --region <id> --zone <id> [--capacity-bytes <n>] | join --state <dir> --token <token> --node-key <key> --node-nonce <nonce> --mtls-trust-domain <domain> --tls-cert-sha256 <hex> --region <id> --zone <id> [--capacity-bytes <n>] [--gateway <true|false>] | serve [--config <path>] [--state <dir>] [--once] [--tick-ms <n>] | policy]"
        .to_string()
}

fn run_serve_cli(args: &[String]) -> Result<(), String> {
    let tick_ms = optional_flag(args, "--tick-ms")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "--tick-ms must be a positive integer".to_string())
        })
        .transpose()?;
    let config = resolve_daemon_config(
        ServeConfigOverrides {
            config: optional_flag(args, "--config").map(std::path::PathBuf::from),
            state: optional_flag(args, "--state").map(std::path::PathBuf::from),
            once: flag_present(args, "--once").then_some(true),
            tick_ms,
        },
        std::env::vars(),
    )?;
    run_daemon(config, &mut std::io::stdout())
}

fn run_init_cli(args: &[String]) -> Result<(), String> {
    let state = required_flag(args, "--state")?;
    let token = required_flag(args, "--token")?;
    let node_key = required_flag(args, "--node-key")?;
    let node_nonce = required_flag(args, "--node-nonce")?;
    let mtls_trust_domain = required_flag(args, "--mtls-trust-domain")?;
    let tls_cert_sha256 = parse_sha256_fingerprint(required_flag(args, "--tls-cert-sha256")?)?;
    let region = RegionId::new(required_flag(args, "--region")?).map_err(|err| err.message)?;
    let zone = FailureDomainId::new(required_flag(args, "--zone")?).map_err(|err| err.message)?;
    let capacity_bytes = optional_flag(args, "--capacity-bytes")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "--capacity-bytes must be a non-negative integer".to_string())
        })
        .transpose()?
        .unwrap_or(1 << 30);
    let state_path = std::path::Path::new(state);
    let mut admin =
        AdminPlane::open_or_bootstrap(admin_path(state), InviteToken(token.to_string()))
            .map_err(|err| err.message)?;
    if admin.records().next().is_some() {
        return Err("cluster is already initialized".to_string());
    }
    let mut catalog = PolicyCatalog::open(policy_path(state)).map_err(|err| err.message)?;
    if catalog.current().regions.is_empty() {
        catalog
            .add_region(region.clone(), RegionRole::Primary)
            .map_err(|err| err.message)?;
    }

    let record = admin
        .admit(JoinRequest {
            token: InviteToken(token.to_string()),
            identity: NodeIdentity::new(node_key, node_nonce, mtls_trust_domain, tls_cert_sha256)
                .map_err(|err| err.message)?,
            region: region.clone(),
            zone,
            capacity_bytes,
            supports_storage: true,
            supports_gateway: true,
        })
        .map_err(|err| err.message)?;
    admin
        .mark_learner_synced(record.node.id)
        .map_err(|err| err.message)?;
    let promoted = admin
        .promote_synced_learner(record.node.id)
        .map_err(|err| err.message)?;
    let mut audit = DurableAuditLog::open(audit_path(state)).map_err(|err| err.message)?;
    audit_operator_event(
        &mut audit,
        "cluster.init",
        Some(CommitVersion(catalog.current().version)),
        Some(promoted.node.id),
        None,
        format!(
            "initialized node={} region={} zone={} trust_domain={}",
            promoted.node.id.0,
            region,
            promoted.node.failure_domain,
            promoted.identity.transport.trust_domain
        ),
    )?;

    println!(
        "initialized cluster state={} node={} region={} policy_version={}",
        state_path.display(),
        promoted.node.id.0,
        region,
        catalog.current().version
    );
    Ok(())
}

fn run_join_cli(args: &[String]) -> Result<(), String> {
    let state = required_flag(args, "--state")?;
    let token = required_flag(args, "--token")?;
    let node_key = required_flag(args, "--node-key")?;
    let node_nonce = required_flag(args, "--node-nonce")?;
    let mtls_trust_domain = required_flag(args, "--mtls-trust-domain")?;
    let tls_cert_sha256 = parse_sha256_fingerprint(required_flag(args, "--tls-cert-sha256")?)?;
    let region = RegionId::new(required_flag(args, "--region")?).map_err(|err| err.message)?;
    let zone = FailureDomainId::new(required_flag(args, "--zone")?).map_err(|err| err.message)?;
    let capacity_bytes = optional_flag(args, "--capacity-bytes")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "--capacity-bytes must be a non-negative integer".to_string())
        })
        .transpose()?
        .unwrap_or(1 << 30);
    let supports_gateway = optional_flag(args, "--gateway")
        .map(parse_bool)
        .transpose()?
        .unwrap_or(true);

    let mut admin = AdminPlane::open_durable(admin_path(state), InviteToken(token.to_string()))
        .map_err(|err| err.message)?;
    let record = admin
        .admit(JoinRequest {
            token: InviteToken(token.to_string()),
            identity: NodeIdentity::new(node_key, node_nonce, mtls_trust_domain, tls_cert_sha256)
                .map_err(|err| err.message)?,
            region: region.clone(),
            zone,
            capacity_bytes,
            supports_storage: true,
            supports_gateway,
        })
        .map_err(|err| err.message)?;
    admin
        .mark_learner_synced(record.node.id)
        .map_err(|err| err.message)?;
    let promoted = admin
        .promote_synced_learner(record.node.id)
        .map_err(|err| err.message)?;
    let mut audit = DurableAuditLog::open(audit_path(state)).map_err(|err| err.message)?;
    audit_operator_event(
        &mut audit,
        "cluster.join",
        None,
        Some(promoted.node.id),
        None,
        format!(
            "joined node={} region={} zone={} gateway={}",
            promoted.node.id.0, region, promoted.node.failure_domain, supports_gateway
        ),
    )?;

    println!(
        "joined cluster state={} node={} region={} gateway={}",
        std::path::Path::new(state).display(),
        promoted.node.id.0,
        region,
        supports_gateway
    );
    Ok(())
}

fn run_status_cli(args: &[String]) -> Result<(), String> {
    let state = required_flag(args, "--state")?;
    let admin_dir = admin_path(state);
    let policy_dir = policy_path(state);
    if !admin_dir.exists() || !policy_dir.exists() {
        return Err("cluster is not initialized at the requested state path".to_string());
    }
    let admin = AdminPlane::recover(admin_dir).map_err(|err| err.message)?;
    let catalog = PolicyCatalog::open(policy_dir).map_err(|err| err.message)?;
    let audit_records = DurableAuditLog::recover(audit_path(state)).map_err(|err| err.message)?;
    let summary = build_operator_status(state, &admin, &catalog)?;
    let promoted = admin
        .records()
        .filter(|record| record.state == rsf_admin::AdmissionState::Promoted)
        .count();

    println!("cluster epoch: {}", admin.cluster_map().epoch.0);
    println!("nodes: {}", admin.cluster_map().nodes.len());
    println!("promoted-voters: {promoted}");
    println!("policy-version: {}", catalog.current().version);
    println!(
        "policy-placement-groups: {}",
        catalog.current().placement_groups
    );
    println!("audit-records: {}", audit_records.len());
    println!("role-storage-nodes: {}", summary.storage_nodes);
    println!("role-gateway-nodes: {}", summary.gateway_nodes);
    println!("role-metadata-nodes: {}", summary.metadata_nodes);
    println!("role-repair-nodes: {}", summary.repair_nodes);
    println!("placement-health: {}", summary.placement_health.as_str());
    println!("placement-detail: {}", summary.placement_detail);
    println!(
        "protocol-group-ids: {}",
        format_u64_list(&summary.protocol_group_ids)
    );
    println!(
        "protocol-latest-version: {}",
        summary.protocol_latest_version
    );
    println!(
        "protocol-certificate-count: {}",
        summary.protocol_certificate_count
    );
    println!("protocol-max-log-index: {}", summary.protocol_max_log_index);
    Ok(())
}

fn run_audit_cli(args: &[String]) -> Result<(), String> {
    let state = required_flag(args, "--state")?;
    let records = DurableAuditLog::recover(audit_path(state)).map_err(|err| err.message)?;
    for record in records {
        println!(
            "audit sequence={} service={} action={} version={} node={} pg={} detail={}",
            record.sequence.0,
            record.event.service,
            record.event.action,
            option_u64(record.event.version.map(|version| version.0)),
            option_u64(record.event.node.map(|node| node.0)),
            option_u64(record.event.pg.map(|pg| pg.0)),
            record.event.detail
        );
    }
    Ok(())
}

fn run_upgrade_cli(args: &[String]) -> Result<(), String> {
    match args {
        [command, rest @ ..] if command == "plan" => {
            let state = required_flag(rest, "--state")?;
            let from = parse_software_version(required_flag(rest, "--from")?)?;
            let target = parse_software_version(required_flag(rest, "--target")?)?;
            let admin = AdminPlane::recover(admin_path(state)).map_err(|err| err.message)?;
            let overrides = parse_node_version_overrides(rest)?;
            let nodes = admin
                .records()
                .filter(|record| record.state == rsf_admin::AdmissionState::Promoted)
                .map(|record| {
                    let version = overrides
                        .get(&record.node.id.0)
                        .cloned()
                        .unwrap_or_else(|| from.clone());
                    NodeVersion {
                        node: record.node.id,
                        version,
                        region: record.node.region.clone(),
                        can_drain: true,
                    }
                })
                .collect::<Vec<_>>();
            let min_compatible_nodes = optional_flag(rest, "--min-compatible-nodes")
                .map(|value| {
                    value
                        .parse::<usize>()
                        .map_err(|_| "--min-compatible-nodes must be a positive integer".to_string())
                })
                .transpose()?
                .unwrap_or_else(|| nodes.len().saturating_sub(1).max(1));
            let policy = UpgradePolicy {
                min_compatible_nodes,
                max_unavailable: 1,
                require_same_major: true,
            };
            let plan = UpgradePlanner
                .plan(&nodes, target.clone(), &policy)
                .map_err(|err| err.message)?;

            println!(
                "upgrade plan from={} target={} nodes={} min_compatible_nodes={}",
                plan.from,
                plan.to,
                nodes.len(),
                min_compatible_nodes
            );
            for check in &plan.gate_checks {
                println!(
                    "upgrade-gate {}={} {}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                );
            }
            for step in &plan.steps {
                println!(
                    "upgrade-step kind={} node={} detail={}",
                    step.kind.as_str(),
                    option_u64(step.node.map(|node| node.0)),
                    step.detail
                );
            }

            let mut audit = DurableAuditLog::open(audit_path(state)).map_err(|err| err.message)?;
            audit_operator_event(
                &mut audit,
                "upgrade.plan",
                None,
                None,
                None,
                format!(
                    "planned rolling upgrade from={} target={} nodes={} steps={}",
                    plan.from,
                    plan.to,
                    nodes.len(),
                    plan.steps.len()
                ),
            )?;
            Ok(())
        }
        _ => Err(
            "usage: rsfd upgrade plan --state <dir> --from <semver> --target <semver> [--node-version <node=semver>] [--min-compatible-nodes <n>]"
                .to_string(),
        ),
    }
}

fn run_policy_cli(args: &[String]) -> Result<(), String> {
    match args {
        [command, rest @ ..] if command == "status" => {
            let state = required_flag(rest, "--state")?;
            let catalog = PolicyCatalog::open(policy_path(state)).map_err(|err| err.message)?;
            print_policy(catalog.current());
            Ok(())
        }
        [command, rest @ ..] if command == "plan" => {
            let state = required_flag(rest, "--state")?;
            let admin = AdminPlane::recover(admin_path(state)).map_err(|err| err.message)?;
            let catalog = PolicyCatalog::open(policy_path(state)).map_err(|err| err.message)?;
            let policy = catalog.current();
            let router = open_policy_router(state, &admin)?;
            let reconciliation =
                PolicyReconciler::plan(&admin, &router, policy).map_err(|err| err.message)?;
            let plan = admin
                .preview_replicated_groups(policy.placement_groups, policy.min_replicas)
                .map_err(|err| err.message)?;
            println!(
                "policy plan version={} placement_groups={} min_replicas={}",
                policy.version, policy.placement_groups, policy.min_replicas
            );
            println!(
                "policy-preflight mode={} passed={}",
                reconciliation.preflight.write_mode,
                reconciliation.preflight.passed()
            );
            for check in &reconciliation.preflight.checks {
                println!(
                    "policy-preflight {}={} {}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                );
            }
            for implication in &reconciliation.preflight.implications {
                println!("policy-implication {implication}");
            }
            for step in &reconciliation.steps {
                println!("policy-step {}", step.detail());
            }
            for check in &plan.preflight.checks {
                println!(
                    "placement-preflight {}={} {}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                );
            }
            for group in &plan.groups {
                let replicas = group
                    .acting_set
                    .replicas
                    .iter()
                    .map(|node| node.0.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                println!(
                    "placement-group {} primary={} replicas={}",
                    group.id.0, group.acting_set.primary.0, replicas
                );
            }
            Ok(())
        }
        [command, rest @ ..] if command == "apply" => {
            let state = required_flag(rest, "--state")?;
            let mut admin = AdminPlane::recover(admin_path(state)).map_err(|err| err.message)?;
            let catalog = PolicyCatalog::open(policy_path(state)).map_err(|err| err.message)?;
            let policy = catalog.current();
            let mut router = open_policy_router(state, &admin)?;
            let mut progress = StdoutPolicyProgressSink;
            let outcome =
                PolicyReconciler::apply_with_progress(&mut admin, &mut router, policy, &mut progress)
                    .map_err(|err| err.message)?;
            let mut durable_audit =
                DurableAuditLog::open(audit_path(state)).map_err(|err| err.message)?;
            for audit in &outcome.audit {
                durable_audit
                    .append(audit.clone())
                    .map_err(|err| err.message)?;
                println!("audit action={} detail={}", audit.action, audit.detail);
            }
            println!(
                "policy applied version={} placement_map_version={} placement_groups={} changed={}",
                policy.version,
                outcome.placement_map_version,
                outcome.placement_groups,
                outcome.changed()
            );
            Ok(())
        }
        [command, rest @ ..] if command == "set" => {
            let state = required_flag(rest, "--state")?;
            let replicas = parse_u8_flag(rest, "--replicas")?;
            let groups = parse_u64_flag(rest, "--placement-groups")?;
            let mut catalog = PolicyCatalog::open(policy_path(state)).map_err(|err| err.message)?;
            let policy = catalog
                .set_replication(replicas, groups)
                .map_err(|err| err.message)?;
            if let Some(strict) = optional_flag(rest, "--strict-global-writes") {
                let strict = parse_bool(strict)?;
                catalog
                    .set_strict_global_writes(strict)
                    .map_err(|err| err.message)?;
            }
            println!(
                "policy version={} min_replicas={} placement_groups={}",
                policy.version, replicas, groups
            );
            Ok(())
        }
        [command, rest @ ..] if command == "add-region" => {
            let state = required_flag(rest, "--state")?;
            let region = RegionId::new(required_flag(rest, "--region")?).map_err(|err| err.message)?;
            let role = parse_region_role(required_flag(rest, "--role")?)?;
            let mut catalog = PolicyCatalog::open(policy_path(state)).map_err(|err| err.message)?;
            let policy = catalog.add_region(region.clone(), role).map_err(|err| err.message)?;
            println!(
                "policy version={} added_region={} role={}",
                policy.version,
                region,
                region_role_label(role)
            );
            Ok(())
        }
        _ => Err(
            "usage: rsfd policy [status --state <dir> | plan --state <dir> | apply --state <dir> | set --state <dir> --replicas <n> --placement-groups <n> [--strict-global-writes <true|false>] | add-region --state <dir> --region <id> --role <primary|read-replica>]"
                .to_string(),
        ),
    }
}

fn print_policy(policy: &rsf_policy::DesiredClusterPolicy) {
    let explanation = policy.explain_consistency();
    println!("policy version: {}", policy.version);
    println!("min-replicas: {}", policy.min_replicas);
    println!("placement-groups: {}", policy.placement_groups);
    println!("strict-global-writes: {}", policy.strict_global_writes);
    println!("write-mode: {}", explanation.write_mode);
    println!("latency: {}", explanation.latency);
    println!("consistency: {}", explanation.consistency);
    for region in &policy.regions {
        println!(
            "region: {} role={}",
            region.region,
            region_role_label(region.role)
        );
    }
}

fn policy_path(state: &str) -> std::path::PathBuf {
    std::path::Path::new(state).join("policy")
}

fn audit_path(state: &str) -> std::path::PathBuf {
    std::path::Path::new(state).join("audit")
}

fn admin_path(state: &str) -> std::path::PathBuf {
    std::path::Path::new(state).join("admin")
}

fn protocol_path(state: &str) -> std::path::PathBuf {
    std::path::Path::new(state).join("protocol")
}

fn open_policy_router(state: &str, admin: &AdminPlane) -> Result<VsrBackedFabricRouter, String> {
    let voters = admin
        .membership_view()
        .ok_or_else(|| "policy apply requires at least three promoted voters".to_string())?
        .voter_nodes();
    let primary = *voters
        .first()
        .ok_or_else(|| "policy apply requires at least one voter".to_string())?;
    VsrBackedFabricRouter::open(
        protocol_path(state),
        [PlacementGroupId(1)],
        Epoch(admin.cluster_map().epoch.0.max(1)),
        primary,
        voters,
    )
    .map_err(|err| err.message)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlacementHealth {
    Ready,
    Degraded,
}

impl PlacementHealth {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OperatorStatusSummary {
    storage_nodes: usize,
    gateway_nodes: usize,
    metadata_nodes: usize,
    repair_nodes: usize,
    placement_health: PlacementHealth,
    placement_detail: String,
    protocol_group_ids: Vec<u64>,
    protocol_latest_version: u64,
    protocol_certificate_count: usize,
    protocol_max_log_index: u64,
}

fn build_operator_status(
    state: &str,
    admin: &AdminPlane,
    catalog: &PolicyCatalog,
) -> Result<OperatorStatusSummary, String> {
    let nodes = &admin.cluster_map().nodes;
    let storage_nodes = count_role(nodes, NodeRole::Storage);
    let gateway_nodes = nodes
        .iter()
        .filter(|node| {
            node.roles.contains(&NodeRole::QueryGateway)
                || node.roles.contains(&NodeRole::ObjectGateway)
        })
        .count();
    let metadata_nodes = count_role(nodes, NodeRole::MetadataReplica);
    let repair_nodes = count_role(nodes, NodeRole::RepairWorker);
    let promoted_voters = admin
        .records()
        .filter(|record| record.state == rsf_admin::AdmissionState::Promoted)
        .count();
    let policy = catalog.current();
    let preview = admin.preview_replicated_groups(policy.placement_groups, policy.min_replicas);

    let (
        placement_health,
        placement_detail,
        protocol_group_ids,
        protocol_latest_version,
        protocol_certificate_count,
        protocol_max_log_index,
    ) = match open_policy_router(state, admin) {
        Ok(router)
            if promoted_voters >= 3
                && storage_nodes >= usize::from(policy.min_replicas)
                && router.placement_map().groups.len() as u64 >= policy.placement_groups
                && router.policy().min_placement_groups <= policy.placement_groups =>
        {
            let runtime = protocol_runtime_status(&router)?;
            (
                PlacementHealth::Ready,
                format!(
                    "ready: groups={} replicas={} protocol_groups={} policy_groups={} protocol_latest_version={} protocol_certificates={}",
                    preview
                        .as_ref()
                        .map(|preview| preview.groups.len())
                        .unwrap_or(0),
                    policy.min_replicas,
                    router.placement_map().groups.len(),
                    policy.placement_groups,
                    runtime.latest_version,
                    runtime.certificate_count
                ),
                runtime.group_ids,
                runtime.latest_version,
                runtime.certificate_count,
                runtime.max_log_index,
            )
        }
        Ok(router) => {
            let runtime = protocol_runtime_status(&router)?;
            (
                PlacementHealth::Degraded,
                format!(
                    "degraded: voters={} storage={} protocol_groups={} policy_groups={} min_replicas={} protocol_latest_version={} protocol_certificates={}",
                    promoted_voters,
                    storage_nodes,
                    router.placement_map().groups.len(),
                    policy.placement_groups,
                    policy.min_replicas,
                    runtime.latest_version,
                    runtime.certificate_count
                ),
                runtime.group_ids,
                runtime.latest_version,
                runtime.certificate_count,
                runtime.max_log_index,
            )
        }
        Err(err) => (
            PlacementHealth::Degraded,
            format!(
                "degraded: voters={} storage={} gateway={} placement_preview_groups={} protocol_router_unavailable={}",
                promoted_voters,
                storage_nodes,
                gateway_nodes,
                preview
                    .as_ref()
                    .map(|preview| preview.groups.len().to_string())
                    .unwrap_or_else(|err| format!("unavailable:{}", err.message)),
                err
            ),
            Vec::new(),
            0,
            0,
            0,
        ),
    };

    Ok(OperatorStatusSummary {
        storage_nodes,
        gateway_nodes,
        metadata_nodes,
        repair_nodes,
        placement_health,
        placement_detail,
        protocol_group_ids,
        protocol_latest_version,
        protocol_certificate_count,
        protocol_max_log_index,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProtocolRuntimeStatus {
    group_ids: Vec<u64>,
    latest_version: u64,
    certificate_count: usize,
    max_log_index: u64,
}

fn protocol_runtime_status(
    router: &VsrBackedFabricRouter,
) -> Result<ProtocolRuntimeStatus, String> {
    let mut certificate_count = 0_usize;
    let mut max_log_index = 0_u64;
    for pg in &router.placement_map().groups {
        let certificates = router
            .recovered_certificates(*pg)
            .map_err(|err| err.message)?;
        certificate_count = certificate_count.saturating_add(certificates.len());
        if let Some(max_index) = certificates
            .iter()
            .map(|certificate| certificate.index.0)
            .max()
        {
            max_log_index = max_log_index.max(max_index);
        }
    }
    Ok(ProtocolRuntimeStatus {
        group_ids: router
            .placement_map()
            .groups
            .iter()
            .map(|pg| pg.0)
            .collect(),
        latest_version: router.latest_version().0,
        certificate_count,
        max_log_index,
    })
}

fn count_role(nodes: &[NodeDescriptor], role: NodeRole) -> usize {
    nodes
        .iter()
        .filter(|node| node.roles.contains(&role))
        .count()
}

fn format_u64_list(values: &[u64]) -> String {
    if values.is_empty() {
        return "none".to_string();
    }
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug)]
struct StdoutPolicyProgressSink;

impl PolicyProgressSink for StdoutPolicyProgressSink {
    fn emit(&mut self, event: PolicyProgressEvent) {
        println!(
            "progress seq={} phase={} policy_version={} detail={}",
            event.sequence,
            progress_phase_label(event.phase),
            event.policy_version,
            event.detail
        );
    }
}

fn audit_operator_event(
    log: &mut DurableAuditLog,
    action: impl Into<String>,
    version: Option<CommitVersion>,
    node: Option<rsf_core::NodeId>,
    pg: Option<PlacementGroupId>,
    detail: impl Into<String>,
) -> Result<(), String> {
    log.append(AuditEvent {
        service: "rsfd".to_string(),
        action: action.into(),
        version,
        node,
        pg,
        detail: detail.into(),
    })
    .map_err(|err| err.message)?;
    Ok(())
}

fn option_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn progress_phase_label(phase: PolicyProgressPhase) -> &'static str {
    match phase {
        PolicyProgressPhase::Started => "started",
        PolicyProgressPhase::Planned => "planned",
        PolicyProgressPhase::StepStarted => "step-started",
        PolicyProgressPhase::StepApplied => "step-applied",
        PolicyProgressPhase::Noop => "noop",
        PolicyProgressPhase::Completed => "completed",
    }
}

fn required_flag<'a>(args: &'a [String], flag: &str) -> Result<&'a str, String> {
    optional_flag(args, flag).ok_or_else(|| format!("missing required flag {flag}"))
}

fn optional_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == flag)
        .map(|window| window[1].as_str())
}

fn flag_present(args: &[String], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn parse_u8_flag(args: &[String], flag: &str) -> Result<u8, String> {
    required_flag(args, flag)?
        .parse::<u8>()
        .map_err(|_| format!("{flag} must be an integer from 0 to 255"))
}

fn parse_u64_flag(args: &[String], flag: &str) -> Result<u64, String> {
    required_flag(args, flag)?
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err("boolean flags must be true or false".to_string()),
    }
}

fn parse_software_version(value: &str) -> Result<SoftwareVersion, String> {
    value.parse::<SoftwareVersion>().map_err(|err| err.message)
}

fn parse_node_version_overrides(
    args: &[String],
) -> Result<std::collections::BTreeMap<u64, SoftwareVersion>, String> {
    let mut overrides = std::collections::BTreeMap::new();
    for value in flag_values(args, "--node-version") {
        let (node, version) = value
            .split_once('=')
            .ok_or_else(|| "--node-version must be formatted as <node=semver>".to_string())?;
        let node = node
            .parse::<u64>()
            .map_err(|_| "--node-version node must be an unsigned integer".to_string())?;
        let version = parse_software_version(version)?;
        if overrides.insert(node, version).is_some() {
            return Err(format!("duplicate --node-version override for node {node}"));
        }
    }
    Ok(overrides)
}

fn flag_values<'a>(args: &'a [String], flag: &str) -> Vec<&'a str> {
    args.windows(2)
        .filter(|window| window[0] == flag)
        .map(|window| window[1].as_str())
        .collect()
}

fn parse_sha256_fingerprint(value: &str) -> Result<Digest, String> {
    let hex = value.strip_prefix("sha256:").unwrap_or(value);
    if hex.len() != 64 {
        return Err("--tls-cert-sha256 must be exactly 64 hex characters".to_string());
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }
    Digest::sha256(bytes).map_err(|err| err.message)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("--tls-cert-sha256 must contain only hex characters".to_string()),
    }
}

fn parse_region_role(value: &str) -> Result<RegionRole, String> {
    match value {
        "primary" => Ok(RegionRole::Primary),
        "read-replica" => Ok(RegionRole::ReadReplica),
        _ => Err("region role must be primary or read-replica".to_string()),
    }
}

fn region_role_label(role: RegionRole) -> &'static str {
    match role {
        RegionRole::Primary => "primary",
        RegionRole::ReadReplica => "read-replica",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn cli_without_args_returns_production_usage() {
        let err = run_cli(&[]).expect_err("missing command should return usage");

        assert!(err.contains("usage: rsfd"));
        assert!(!err.contains("dev scaffold"));
        assert!(!err.contains("demo"));
    }

    #[test]
    fn production_cli_rejects_demo_commands() {
        let err = run_cli(&["demo".to_string(), "list".to_string()])
            .expect_err("production daemon must not expose demo commands");

        assert!(err.contains("usage: rsfd"));
        assert!(!err.contains("demo"));
    }

    #[test]
    fn init_cli_bootstraps_one_node_cluster_state() {
        let state = test_path("init_cli_bootstraps_one_node_cluster_state");
        let state_arg = state.display().to_string();

        run_cli(&[
            "init".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-bootstrap".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-bootstrap".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("bootstrap"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-a".to_string(),
        ])
        .expect("init cluster");
        run_cli(&[
            "status".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("status reads initialized state");

        let admin = AdminPlane::open_durable(
            state.join("admin"),
            InviteToken("cluster-token".to_string()),
        )
        .expect("recover admin");
        let catalog = PolicyCatalog::open(state.join("policy")).expect("recover policy");
        let audit = DurableAuditLog::recover(state.join("audit")).expect("recover audit");

        assert_eq!(admin.cluster_map().nodes.len(), 1);
        assert_eq!(
            admin.records().next().expect("record").state,
            rsf_admin::AdmissionState::Promoted
        );
        let identity = &admin.records().next().expect("record").identity;
        assert_eq!(
            identity.transport.spiffe_id,
            "spiffe://cluster.example/rsf/node/node-public-key-bootstrap"
        );
        assert_eq!(
            identity.transport.certificate_fingerprint,
            parse_sha256_fingerprint(&fingerprint_hex("bootstrap")).expect("fingerprint")
        );
        assert_eq!(catalog.current().regions.len(), 1);
        assert_eq!(catalog.current().regions[0].role, RegionRole::Primary);
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].event.service, "rsfd");
        assert_eq!(audit[0].event.action, "cluster.init");
        assert_eq!(audit[0].event.node, Some(admin.cluster_map().nodes[0].id));

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn init_cli_rejects_invalid_tls_certificate_fingerprint() {
        let state = test_path("init_cli_rejects_invalid_tls_certificate_fingerprint");
        let state_arg = state.display().to_string();

        let err = run_cli(&[
            "init".to_string(),
            "--state".to_string(),
            state_arg,
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-bootstrap".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-bootstrap".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            "not-hex".to_string(),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-a".to_string(),
        ])
        .expect_err("bad fingerprint rejected");

        assert!(err.contains("--tls-cert-sha256"));
        assert!(!state.join("admin").exists());

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn init_cli_rejects_existing_cluster_state() {
        let state = test_path("init_cli_rejects_existing_cluster_state");
        let state_arg = state.display().to_string();
        let init = || {
            run_cli(&[
                "init".to_string(),
                "--state".to_string(),
                state_arg.clone(),
                "--token".to_string(),
                "cluster-token".to_string(),
                "--node-key".to_string(),
                "node-public-key-bootstrap".to_string(),
                "--node-nonce".to_string(),
                "node-nonce-bootstrap".to_string(),
                "--mtls-trust-domain".to_string(),
                "cluster.example".to_string(),
                "--tls-cert-sha256".to_string(),
                fingerprint_hex("bootstrap"),
                "--region".to_string(),
                "iad".to_string(),
                "--zone".to_string(),
                "iad-a".to_string(),
            ])
        };

        init().expect("first init");
        let err = init().expect_err("second init rejected");

        assert!(err.contains("already initialized"));

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn join_cli_promotes_three_node_cluster_and_rejects_wrong_token() {
        let state = test_path("join_cli_promotes_three_node_cluster_and_rejects_wrong_token");
        let state_arg = state.display().to_string();

        run_cli(&[
            "init".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-bootstrap".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-bootstrap".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("bootstrap"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-a".to_string(),
        ])
        .expect("init cluster");
        run_cli(&[
            "join".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-second".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-second".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("second"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-b".to_string(),
        ])
        .expect("join second node");
        run_cli(&[
            "join".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-third".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-third".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("third"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-c".to_string(),
        ])
        .expect("join third node");
        let wrong = run_cli(&[
            "join".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--token".to_string(),
            "wrong-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-bad".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-bad".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("bad"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-d".to_string(),
        ])
        .expect_err("wrong token rejected");

        let admin = AdminPlane::open_durable(
            state.join("admin"),
            InviteToken("cluster-token".to_string()),
        )
        .expect("recover admin");
        let audit = DurableAuditLog::recover(state.join("audit")).expect("recover audit");

        assert!(wrong.contains("join token"));
        assert_eq!(admin.cluster_map().nodes.len(), 3);
        assert_eq!(admin.membership_view().expect("membership").voters(), 3);
        assert_eq!(
            admin
                .records()
                .filter(|record| record.state == rsf_admin::AdmissionState::Promoted)
                .count(),
            3
        );
        assert_eq!(audit.len(), 3);
        assert_eq!(audit[0].event.action, "cluster.init");
        assert_eq!(audit[1].event.action, "cluster.join");
        assert_eq!(audit[2].event.action, "cluster.join");

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn policy_plan_cli_dry_runs_replication_placement_without_audit_write() {
        let state = test_path("policy_plan_cli_dry_runs_replication_placement_without_audit_write");
        let state_arg = state.display().to_string();

        bootstrap_three_nodes(&state_arg);
        run_cli(&[
            "policy".to_string(),
            "set".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--replicas".to_string(),
            "3".to_string(),
            "--placement-groups".to_string(),
            "2".to_string(),
        ])
        .expect("set replication policy");
        let before = AdminPlane::recover(state.join("admin")).expect("recover before plan");
        let before_audit = before.audit().len();

        run_cli(&[
            "policy".to_string(),
            "plan".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("dry-run policy plan");

        let after = AdminPlane::recover(state.join("admin")).expect("recover after plan");
        let catalog = PolicyCatalog::open(state.join("policy")).expect("recover policy");
        let plan = after
            .preview_replicated_groups(
                catalog.current().placement_groups,
                catalog.current().min_replicas,
            )
            .expect("preview placement");

        assert_eq!(after.audit().len(), before_audit);
        assert_eq!(plan.groups.len(), 2);
        assert_eq!(plan.groups[0].acting_set.replicas.len(), 3);

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn policy_apply_cli_updates_durable_router_and_is_idempotent() {
        let state = test_path("policy_apply_cli_updates_durable_router_and_is_idempotent");
        let state_arg = state.display().to_string();

        bootstrap_three_nodes(&state_arg);
        run_cli(&[
            "policy".to_string(),
            "set".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--replicas".to_string(),
            "3".to_string(),
            "--placement-groups".to_string(),
            "3".to_string(),
        ])
        .expect("set replication policy");
        run_cli(&[
            "policy".to_string(),
            "apply".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("apply policy");

        let admin = AdminPlane::recover(state.join("admin")).expect("recover admin");
        let audit_after_first = admin.audit().len();
        let durable_audit_after_first =
            DurableAuditLog::recover(state.join("audit")).expect("recover audit");
        let router = open_policy_router(&state_arg, &admin).expect("recover router");

        assert_eq!(router.placement_map().groups.len(), 3);
        assert_eq!(router.policy().min_placement_groups, 3);
        assert!(
            admin
                .audit()
                .iter()
                .any(|event| { event.action == rsf_admin::AdmissionAuditAction::PlacementApplied })
        );
        assert!(
            admin
                .audit()
                .iter()
                .any(|event| event.action == rsf_admin::AdmissionAuditAction::PolicyApplied)
        );
        assert!(durable_audit_after_first.iter().any(|record| {
            record.event.service == "control" && record.event.action == "policy.reconcile.completed"
        }));

        run_cli(&[
            "policy".to_string(),
            "apply".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("second apply is idempotent");

        let recovered_admin = AdminPlane::recover(state.join("admin")).expect("recover admin");
        let recovered_router = open_policy_router(&state_arg, &recovered_admin).expect("router");
        let durable_audit_after_second =
            DurableAuditLog::recover(state.join("audit")).expect("recover audit");

        assert_eq!(recovered_admin.audit().len(), audit_after_first);
        assert_eq!(recovered_router.placement_map().groups.len(), 3);
        assert_eq!(recovered_router.policy().min_placement_groups, 3);
        assert!(durable_audit_after_second.len() > durable_audit_after_first.len());
        assert!(
            durable_audit_after_second
                .iter()
                .any(|record| record.event.action == "policy.reconcile.noop")
        );

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn audit_cli_reads_durable_operator_log() {
        let state = test_path("audit_cli_reads_durable_operator_log");
        let state_arg = state.display().to_string();

        bootstrap_three_nodes(&state_arg);
        run_cli(&[
            "audit".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("audit command reads durable log");

        let audit = DurableAuditLog::recover(state.join("audit")).expect("recover audit");

        assert_eq!(audit.len(), 3);
        assert_eq!(audit[0].sequence.0, 1);
        assert_eq!(audit[0].event.action, "cluster.init");
        assert_eq!(audit[1].event.action, "cluster.join");
        assert_eq!(audit[2].event.action, "cluster.join");

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn upgrade_plan_cli_gates_rollout_and_audits_accepted_plan() {
        let state = test_path("upgrade_plan_cli_gates_rollout_and_audits_accepted_plan");
        let state_arg = state.display().to_string();

        bootstrap_three_nodes(&state_arg);
        run_cli(&[
            "upgrade".to_string(),
            "plan".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--from".to_string(),
            "1.2.0".to_string(),
            "--target".to_string(),
            "1.3.0".to_string(),
            "--node-version".to_string(),
            "1=1.2.1".to_string(),
        ])
        .expect("upgrade plan passes gates");

        let audit = DurableAuditLog::recover(state.join("audit")).expect("recover audit");

        assert!(
            audit
                .iter()
                .any(|record| record.event.action == "upgrade.plan")
        );

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn upgrade_plan_cli_rejects_unsafe_compatibility_floor() {
        let state = test_path("upgrade_plan_cli_rejects_unsafe_compatibility_floor");
        let state_arg = state.display().to_string();

        bootstrap_three_nodes(&state_arg);
        let err = run_cli(&[
            "upgrade".to_string(),
            "plan".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--from".to_string(),
            "1.2.0".to_string(),
            "--target".to_string(),
            "1.3.0".to_string(),
            "--min-compatible-nodes".to_string(),
            "3".to_string(),
        ])
        .expect_err("unsafe rollout rejected");
        let audit = DurableAuditLog::recover(state.join("audit")).expect("recover audit");

        assert!(err.contains("upgrade gate"));
        assert!(
            !audit
                .iter()
                .any(|record| record.event.action == "upgrade.plan")
        );

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn operator_status_summarizes_degraded_bootstrap_cluster() {
        let state = test_path("operator_status_summarizes_degraded_bootstrap_cluster");
        let state_arg = state.display().to_string();

        run_cli(&[
            "init".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-bootstrap".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-bootstrap".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("bootstrap"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-a".to_string(),
        ])
        .expect("init cluster");
        run_cli(&[
            "status".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("status prints degraded summary");

        let admin = AdminPlane::recover(state.join("admin")).expect("recover admin");
        let catalog = PolicyCatalog::open(state.join("policy")).expect("recover policy");
        let summary =
            build_operator_status(&state_arg, &admin, &catalog).expect("operator summary");

        assert_eq!(summary.storage_nodes, 1);
        assert_eq!(summary.gateway_nodes, 1);
        assert_eq!(summary.placement_health, PlacementHealth::Degraded);
        assert!(
            summary
                .placement_detail
                .contains("protocol_router_unavailable")
        );

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn operator_status_reports_ready_roles_and_placement_after_policy_apply() {
        let state =
            test_path("operator_status_reports_ready_roles_and_placement_after_policy_apply");
        let state_arg = state.display().to_string();

        bootstrap_three_nodes(&state_arg);
        run_cli(&[
            "policy".to_string(),
            "set".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--replicas".to_string(),
            "3".to_string(),
            "--placement-groups".to_string(),
            "3".to_string(),
        ])
        .expect("set policy");
        run_cli(&[
            "policy".to_string(),
            "apply".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("apply policy");
        {
            let admin = AdminPlane::recover(state.join("admin")).expect("recover admin");
            let voters = admin.membership_view().expect("membership").voter_nodes();
            let mut router = VsrBackedFabricRouter::open(
                protocol_path(&state_arg),
                [PlacementGroupId(1)],
                Epoch(admin.cluster_map().epoch.0.max(1)),
                voters[0],
                voters,
            )
            .expect("router");
            router
                .commit(
                    PlacementGroupId(2),
                    rsf_protocol::FabricOperation::CatalogMutation {
                        namespace: "system".to_string(),
                        item: "status-live-protocol".to_string(),
                    },
                )
                .expect("protocol commit");
        }
        run_cli(&[
            "status".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("status prints ready summary");

        let admin = AdminPlane::recover(state.join("admin")).expect("recover admin");
        let catalog = PolicyCatalog::open(state.join("policy")).expect("recover policy");
        let summary =
            build_operator_status(&state_arg, &admin, &catalog).expect("operator summary");

        assert_eq!(summary.storage_nodes, 3);
        assert_eq!(summary.gateway_nodes, 3);
        assert_eq!(summary.metadata_nodes, 3);
        assert_eq!(summary.repair_nodes, 3);
        assert_eq!(summary.placement_health, PlacementHealth::Ready);
        assert!(summary.placement_detail.contains("protocol_groups=3"));
        assert_eq!(summary.protocol_group_ids, vec![1, 2, 3]);
        assert_eq!(summary.protocol_latest_version, 1);
        assert_eq!(summary.protocol_certificate_count, 1);
        assert_eq!(summary.protocol_max_log_index, 1);
        assert!(summary.placement_detail.contains("protocol_certificates=1"));

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn policy_cli_sets_and_recovers_durable_catalog() {
        let state = test_path("policy_cli_sets_and_recovers_durable_catalog");
        let state_arg = state.display().to_string();

        run_cli(&[
            "policy".to_string(),
            "add-region".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--region".to_string(),
            "iad".to_string(),
            "--role".to_string(),
            "primary".to_string(),
        ])
        .expect("add primary region");
        run_cli(&[
            "policy".to_string(),
            "set".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--replicas".to_string(),
            "5".to_string(),
            "--placement-groups".to_string(),
            "64".to_string(),
            "--strict-global-writes".to_string(),
            "true".to_string(),
        ])
        .expect("set replication policy");
        run_cli(&[
            "policy".to_string(),
            "status".to_string(),
            "--state".to_string(),
            state_arg.clone(),
        ])
        .expect("status");

        let catalog = PolicyCatalog::open(state.join("policy")).expect("recover catalog");

        assert_eq!(catalog.current().min_replicas, 5);
        assert_eq!(catalog.current().placement_groups, 64);
        assert_eq!(catalog.current().regions.len(), 1);

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn policy_cli_rejects_unsafe_replica_floor() {
        let state = test_path("policy_cli_rejects_unsafe_replica_floor");
        let state_arg = state.display().to_string();

        let err = run_cli(&[
            "policy".to_string(),
            "set".to_string(),
            "--state".to_string(),
            state_arg,
            "--replicas".to_string(),
            "2".to_string(),
            "--placement-groups".to_string(),
            "16".to_string(),
        ])
        .expect_err("unsafe replica floor rejected");

        assert!(err.contains("minimum replicas"));

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn serve_cli_starts_production_daemon_once() {
        let state = test_path("serve_cli_starts_production_daemon_once");
        let state_arg = state.display().to_string();
        bootstrap_three_nodes(&state_arg);
        {
            let admin = AdminPlane::recover(state.join("admin")).expect("recover admin");
            let voters = admin.membership_view().expect("membership").voter_nodes();
            let mut router = VsrBackedFabricRouter::open(
                protocol_path(&state_arg),
                [PlacementGroupId(1)],
                Epoch(admin.cluster_map().epoch.0.max(1)),
                voters[0],
                voters,
            )
            .expect("router");
            router
                .commit(
                    PlacementGroupId(1),
                    rsf_protocol::FabricOperation::CatalogMutation {
                        namespace: "system".to_string(),
                        item: "daemon-live-status".to_string(),
                    },
                )
                .expect("protocol commit");
        }

        run_cli(&[
            "serve".to_string(),
            "--state".to_string(),
            state_arg.clone(),
            "--once".to_string(),
        ])
        .expect("serve once");
        let snapshot = daemon::load_snapshot(&state).expect("daemon snapshot");

        assert!(snapshot.ready);
        assert_eq!(snapshot.nodes, 3);
        assert_eq!(snapshot.promoted_voters, 3);
        assert_eq!(snapshot.storage_nodes, 3);
        assert_eq!(snapshot.gateway_nodes, 3);
        assert_eq!(snapshot.protocol_group_ids, vec![1]);
        assert_eq!(snapshot.protocol_latest_version, 1);
        assert_eq!(snapshot.protocol_certificate_count, 1);
        assert_eq!(snapshot.protocol_max_log_index, 1);

        let health =
            daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Healthz).expect("healthz");
        assert!(health.body.contains("\"protocol_group_ids\":[1]"));
        assert!(health.body.contains("\"protocol_latest_version\":1"));
        assert!(health.body.contains("\"protocol_certificate_count\":1"));
        let metrics =
            daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Metrics).expect("metrics");
        assert!(
            metrics
                .body
                .contains("rsf_daemon_protocol_certificate_count 1\n")
        );
        assert!(
            metrics
                .body
                .contains("rsf_daemon_protocol_pg_1_present 1\n")
        );

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn serve_cli_uses_config_file_for_daemon_startup() {
        let state = test_path("serve_cli_uses_config_file_for_daemon_startup");
        let state_arg = state.display().to_string();
        let config_path = test_path("serve_cli_uses_config_file_for_daemon_startup_config");
        bootstrap_three_nodes(&state_arg);
        std::fs::write(
            &config_path,
            format!(
                "state = \"{}\"\nonce = true\ntick_ms = 5\n",
                state.display()
            ),
        )
        .expect("write config");

        run_cli(&[
            "serve".to_string(),
            "--config".to_string(),
            config_path.display().to_string(),
        ])
        .expect("serve from config");

        let snapshot = daemon::load_snapshot(&state).expect("daemon snapshot");
        assert!(snapshot.ready);

        let _ = std::fs::remove_file(config_path);
        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn serve_cli_rejects_invalid_config_before_opening_state() {
        let config_path = test_path("serve_cli_rejects_invalid_config_before_opening_state");
        std::fs::write(&config_path, "state = \"/tmp/rsf\"\nunknown = true\n")
            .expect("write config");

        let err = run_cli(&[
            "serve".to_string(),
            "--config".to_string(),
            config_path.display().to_string(),
            "--once".to_string(),
        ])
        .expect_err("invalid config rejected");

        assert!(err.contains("unknown key unknown"));

        let _ = std::fs::remove_file(config_path);
    }

    #[test]
    fn daemon_runtime_emits_startup_readiness_to_stdout() {
        let state = test_path("daemon_runtime_emits_startup_readiness_to_stdout");
        let state_arg = state.display().to_string();
        bootstrap_three_nodes(&state_arg);

        let mut out = Vec::new();
        daemon::run_daemon(
            DaemonConfig {
                state: state.clone(),
                once: true,
                tick: Duration::from_millis(1),
            },
            &mut out,
        )
        .expect("daemon once");
        let output = String::from_utf8(out).expect("stdout is utf8");

        assert!(output.contains("daemon tick=0"));
        assert!(output.contains("readiness=ready"));
        assert!(output.contains("daemon cluster"));
        assert!(output.contains("daemon policy"));
        assert!(output.contains("daemon protocol"));
        assert!(output.contains("daemon endpoint path=/livez status=200"));
        assert!(output.contains("daemon endpoint path=/readyz status=200"));
        assert!(output.contains("daemon endpoint path=/healthz status=200"));
        assert!(output.contains("daemon endpoint path=/metrics status=200"));

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn daemon_health_endpoints_report_ready_and_degraded_states() {
        let state = test_path("daemon_health_endpoints_report_ready_and_degraded_states");
        let state_arg = state.display().to_string();
        bootstrap_three_nodes(&state_arg);
        let mut snapshot = daemon::load_snapshot(&state).expect("daemon snapshot");

        let live =
            daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Livez).expect("livez");
        let ready =
            daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Readyz).expect("readyz");
        let health =
            daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Healthz).expect("healthz");
        let metrics =
            daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Metrics).expect("metrics");

        assert_eq!(live.status, 200);
        assert_eq!(live.body, "alive\n");
        assert_eq!(ready.status, 200);
        assert!(ready.body.contains("ready:"));
        assert_eq!(health.status, 200);
        assert_eq!(health.content_type, "application/json");
        assert!(health.body.contains("\"ready\":true"));
        assert!(health.body.contains("\"promoted_voters\":3"));
        assert_eq!(metrics.status, 200);
        assert_eq!(metrics.content_type, "text/plain; version=0.0.4");
        assert!(metrics.body.contains("# TYPE rsf_daemon_nodes gauge"));
        assert!(metrics.body.contains("rsf_daemon_nodes 3\n"));
        assert!(metrics.body.contains("rsf_daemon_ready 1\n"));

        snapshot.ready = false;
        snapshot.readiness = "degraded: no protocol groups".to_string();
        let degraded_ready = daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Readyz)
            .expect("degraded readyz");
        let degraded_health = daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Healthz)
            .expect("degraded healthz");
        let degraded_metrics = daemon::render_endpoint(&snapshot, daemon::HealthEndpoint::Metrics)
            .expect("degraded metrics");

        assert_eq!(degraded_ready.status, 503);
        assert!(degraded_ready.body.contains("degraded:"));
        assert_eq!(degraded_health.status, 503);
        assert!(degraded_health.body.contains("\"ready\":false"));
        assert_eq!(degraded_metrics.status, 503);
        assert!(degraded_metrics.body.contains("rsf_daemon_ready 0\n"));

        let _ = std::fs::remove_dir_all(state);
    }

    #[test]
    fn daemon_rejects_missing_state_path() {
        let state = test_path("daemon_rejects_missing_state_path");
        let mut out = Vec::new();

        let err = daemon::run_daemon(
            DaemonConfig {
                state: state.clone(),
                once: true,
                tick: Duration::from_millis(1),
            },
            &mut out,
        )
        .expect_err("missing state rejected");

        assert!(err.contains("state path does not exist"));
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsfd-{name}-{}-{nonce}", std::process::id()))
    }

    fn bootstrap_three_nodes(state: &str) {
        run_cli(&[
            "init".to_string(),
            "--state".to_string(),
            state.to_string(),
            "--token".to_string(),
            "cluster-token".to_string(),
            "--node-key".to_string(),
            "node-public-key-bootstrap".to_string(),
            "--node-nonce".to_string(),
            "node-nonce-bootstrap".to_string(),
            "--mtls-trust-domain".to_string(),
            "cluster.example".to_string(),
            "--tls-cert-sha256".to_string(),
            fingerprint_hex("bootstrap"),
            "--region".to_string(),
            "iad".to_string(),
            "--zone".to_string(),
            "iad-a".to_string(),
        ])
        .expect("init cluster");
        for (key, nonce, zone) in [
            ("node-public-key-second", "node-nonce-second", "iad-b"),
            ("node-public-key-third", "node-nonce-third", "iad-c"),
        ] {
            run_cli(&[
                "join".to_string(),
                "--state".to_string(),
                state.to_string(),
                "--token".to_string(),
                "cluster-token".to_string(),
                "--node-key".to_string(),
                key.to_string(),
                "--node-nonce".to_string(),
                nonce.to_string(),
                "--mtls-trust-domain".to_string(),
                "cluster.example".to_string(),
                "--tls-cert-sha256".to_string(),
                fingerprint_hex(key),
                "--region".to_string(),
                "iad".to_string(),
                "--zone".to_string(),
                zone.to_string(),
            ])
            .expect("join node");
        }
    }

    fn fingerprint_hex(seed: &str) -> String {
        let mut bytes = [0_u8; 32];
        for (index, byte) in seed.bytes().enumerate() {
            bytes[index % 32] = bytes[index % 32].wrapping_add(byte).wrapping_add(1);
        }
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
