use rsf_core::{Epoch, LogIndex, NodeId, OperationId, PlacementGroupId, storage::WalRecordKind};
use rsf_storage::{SegmentStore, WalAppend};
use rsf_vsr::{DurableVsrCell, VsrDecision, VsrOperation};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn open(path: &Path) -> DurableVsrCell {
    DurableVsrCell::open(
        path,
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
}
fn legacy() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "rsfd-bootstrap-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut source = open(&root.join("source"));
    source
        .prepare(
            NodeId(0),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(1),
                payload: b"payload".to_vec(),
            },
            7,
        )
        .unwrap();
    source.acknowledge(NodeId(1), Epoch(1), LogIndex(1));
    assert!(matches!(
        source.commit(NodeId(0), LogIndex(1)).unwrap(),
        VsrDecision::Committed(_)
    ));
    let entry = source.committed_prefix().unwrap().pop().unwrap();
    let legacy = root.join("legacy");
    let mut store = SegmentStore::open(&legacy).unwrap();
    let (_, _, first) = store
        .append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(1),
            kind: WalRecordKind::Prepare,
            previous_hash: [0; 32],
            payload: &entry.prepare.encode().unwrap(),
        })
        .unwrap();
    store
        .append_wal(WalAppend {
            pg: PlacementGroupId(1),
            epoch: Epoch(1),
            index: LogIndex(2),
            kind: WalRecordKind::Commit,
            previous_hash: first.record_hash,
            payload: &entry.commit.encode().unwrap(),
        })
        .unwrap();
    legacy
}
fn migrate(path: &Path, voters: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rsfd"))
        .args(["vsr", "migrate-bootstrap", "--store"])
        .arg(path)
        .args([
            "--pg",
            "1",
            "--original-view",
            "1",
            "--original-primary",
            "0",
            "--original-voters",
            voters,
        ])
        .output()
        .unwrap()
}
#[test]
fn bootstrap_migration_cli_preserves_data_is_idempotent_and_enables_later_membership() {
    let path = legacy();
    let before = SegmentStore::recover(&path).unwrap().recovery.wal_records;
    for voters in ["0,2,3", "0,0", "invalid"] {
        assert!(!migrate(&path, voters).status.success());
        assert_eq!(
            SegmentStore::recover(&path).unwrap().recovery.wal_records,
            before
        );
    }
    let first = migrate(&path, "0,1,2");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(report["changed"], true);
    let second = migrate(&path, "0,1,2");
    assert!(second.status.success());
    let report: serde_json::Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(report["changed"], false);
    let after = SegmentStore::recover(&path).unwrap().recovery.wal_records;
    assert_eq!(&after[..before.len()], before.as_slice());
    assert_eq!(after.len(), before.len() + 1);
    let mut cell = open(&path);
    let prefix = cell.committed_prefix().unwrap();
    assert!(matches!(
        cell.reconfigure(Epoch(2), NodeId(0), [NodeId(0), NodeId(1)]),
        VsrDecision::MembershipChanged { .. }
    ));
    drop(cell);
    let recovered = DurableVsrCell::open(
        &path,
        PlacementGroupId(1),
        Epoch(2),
        NodeId(0),
        [NodeId(0), NodeId(1)],
    )
    .unwrap();
    assert_eq!(recovered.committed_prefix().unwrap(), prefix);
    assert_eq!(recovered.view(), Epoch(2));
}
#[test]
fn migration_cli_rejects_unknown_and_repeated_flags_before_opening_storage() {
    for args in [
        vec!["--unknown", "x"],
        vec![
            "--store",
            "absent",
            "--pg",
            "1",
            "--original-view",
            "1",
            "--original-primary",
            "0",
            "--store",
            "again",
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_rsfd"))
            .args(["vsr", "migrate-bootstrap"])
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("usage:"));
    }
}
