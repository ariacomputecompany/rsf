use std::collections::BTreeSet;
use std::path::PathBuf;

use rsf_core::{Epoch, ErrorCode, LogIndex, PlacementGroupId, RegionId, storage::WalRecordKind};
use rsf_storage::{SegmentStore, WalAppend};

use super::*;

#[test]
fn durable_deployment_recovers_applied_rollout_after_restart() {
    let path = test_path("recovers_applied_rollout_after_restart");
    let mut ledger = DurableDeploymentLedger::open(&path, Epoch(41)).expect("open ledger");
    let id = ledger
        .plan_rollout(profile("universal-vsr-rados", 5))
        .expect("plan rollout");
    ledger.mark_started(id).expect("start rollout");
    ledger.mark_applied(id).expect("apply rollout");

    let recovered = DurableDeploymentLedger::open(&path, Epoch(42)).expect("reopen ledger");
    let rollout = recovered.rollout(id).expect("recovered rollout");
    let roles = rollout
        .manifest
        .roles
        .iter()
        .map(|role| role.role)
        .collect::<BTreeSet<_>>();

    assert_eq!(rollout.state, DeploymentRolloutState::Applied);
    assert_eq!(rollout.failure, None);
    assert_eq!(rollout.manifest.volumes.len(), 5);
    assert_eq!(rollout.manifest.endpoints.len(), 8);
    assert_eq!(rollout.manifest.probes.len(), 8);
    assert_eq!(roles.len(), 8);
    assert!(recovered.pending().is_empty());
}

#[test]
fn durable_deployment_recovers_started_rollout_as_pending() {
    let path = test_path("recovers_started_rollout_as_pending");
    let mut ledger = DurableDeploymentLedger::open(&path, Epoch(41)).expect("open ledger");
    let id = ledger
        .plan_rollout(profile("geo-local-wpaxos", 6))
        .expect("plan rollout");
    ledger.mark_started(id).expect("start rollout");

    let recovered = DurableDeploymentLedger::open(&path, Epoch(42)).expect("reopen ledger");
    let pending = recovered.pending();

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, id);
    assert_eq!(pending[0].state, DeploymentRolloutState::Started);
}

#[test]
fn durable_deployment_rejects_failure_after_apply() {
    let path = test_path("rejects_failure_after_apply");
    let mut ledger = DurableDeploymentLedger::open(&path, Epoch(41)).expect("open ledger");
    let id = ledger
        .plan_rollout(profile("universal-vsr-rados", 5))
        .expect("plan rollout");
    ledger.mark_started(id).expect("start rollout");
    ledger.mark_applied(id).expect("apply rollout");

    let err = ledger
        .mark_failed(id, "operator attempted to fail terminal rollout")
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::InvalidInput);
    assert_eq!(
        ledger.rollout(id).expect("rollout").state,
        DeploymentRolloutState::Applied
    );
}

#[test]
fn deployment_wal_decode_rejects_trailing_bytes() {
    let profile = profile("universal-vsr-rados", 5);
    let manifest = DeploymentPlanner.plan(profile.clone()).expect("manifest");
    let mut payload = encode_deployment_wal_payload(&DeploymentWalRecord::Planned {
        id: DeploymentRolloutId(7),
        profile,
        manifest,
    })
    .expect("encode payload");
    payload.push(0xff);

    let err = decode_deployment_wal_payload(&payload).unwrap_err();

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

#[test]
fn durable_deployment_replay_rejects_apply_without_start() {
    let path = test_path("replay_rejects_apply_without_start");
    let profile = profile("universal-vsr-rados", 5);
    let manifest = DeploymentPlanner.plan(profile.clone()).expect("manifest");
    append_raw(
        &path,
        1,
        &DeploymentWalRecord::Planned {
            id: DeploymentRolloutId(9),
            profile,
            manifest,
        },
    );
    append_raw(
        &path,
        2,
        &DeploymentWalRecord::Applied {
            id: DeploymentRolloutId(9),
        },
    );

    let err = DurableDeploymentLedger::open(&path, Epoch(42)).unwrap_err();

    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

fn append_raw(path: &PathBuf, index: u64, record: &DeploymentWalRecord) {
    let mut store = SegmentStore::open(path).expect("store");
    let payload = encode_deployment_wal_payload(record).expect("encode");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(PlacementGroupId(58));
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: PlacementGroupId(58),
            epoch: Epoch(41),
            index: LogIndex(index),
            kind: WalRecordKind::Deployment,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw deployment wal");
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

fn test_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-deployment-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
