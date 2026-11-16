use super::*;
use rsf_vsr::{LearnerHistoryStore, VsrHistorySnapshot, VsrPrefixLimits};

pub(super) fn limits() -> VsrPrefixLimits {
    VsrPrefixLimits {
        max_records: 4096,
        max_encoded_bytes: 16 * 1024 * 1024,
    }
}
pub(super) fn commit_local(f: &mut Fixture, prepare: VsrPrepare) {
    f.control
        .get_mut(&NodeId(2))
        .unwrap()
        .receive_prepare(NodeId(2), NodeId(0), prepare.clone())
        .unwrap();
    f.control
        .get_mut(&NodeId(0))
        .unwrap()
        .acknowledge(NodeId(2), prepare.view, prepare.index);
    assert!(matches!(
        f.control
            .get_mut(&NodeId(0))
            .unwrap()
            .commit(NodeId(0), prepare.index)
            .unwrap(),
        VsrDecision::Committed(_)
    ));
}
pub(super) fn genesis(f: &mut Fixture) {
    let catalog = f.catalog.clone();
    let prepare = prepare_command(
        f.control.get_mut(&NodeId(0)).unwrap(),
        ControlCommand::Genesis(catalog),
    );
    commit_local(f, prepare);
}
pub(super) fn provision_control(f: &mut Fixture) {
    let snapshot = snapshot(f);
    let mut learner =
        LearnerHistoryStore::open(f.root.join("learner-0"), CONTROL, NodeId(3), NodeId(0)).unwrap();
    let id = snapshot.descriptor.id().unwrap();
    let count = learner.begin(snapshot.descriptor).unwrap();
    for (ordinal, record) in snapshot
        .records
        .into_iter()
        .enumerate()
        .skip(count as usize)
    {
        learner.append(id, ordinal as u64, record).unwrap();
    }
    learner.finish(id).unwrap();
}
pub(super) fn unbound_server(f: &Fixture) -> PeerReplicaServer {
    let learner =
        LearnerHistoryStore::open(f.root.join("learner-1"), DATA, NodeId(3), NodeId(2)).unwrap();
    PeerReplicaServer::with_learners(
        NodeId(3),
        &cluster(),
        identity(&f.root, 3),
        f.peers.clone(),
        BTreeMap::new(),
        [(DATA, learner)].into(),
        Duration::from_millis(800),
    )
    .unwrap()
}
pub(super) fn control_server(
    f: &Fixture,
    limits: VsrPrefixLimits,
) -> Result<PeerReplicaServer, String> {
    unbound_server(f).with_control_learner_replication(
        f.anchor.clone(),
        f.catalog.clone(),
        f.root.join("learner-0"),
        NodeId(0),
        limits,
    )
}
pub(super) fn sender(f: &Fixture, running: &Running) -> NetworkVsrTransport {
    let mut peers = f.peers.clone();
    peers.get_mut(&NodeId(3)).unwrap().address = running.address;
    NetworkVsrTransport::new(
        NodeId(0),
        &cluster(),
        identity(&f.root, 0),
        peers,
        Duration::from_millis(800),
    )
    .unwrap()
}
pub(super) fn snapshot(f: &mut Fixture) -> VsrHistorySnapshot {
    f.control
        .get_mut(&NodeId(0))
        .unwrap()
        .history_snapshot()
        .unwrap()
}
pub(super) fn finish_bytes(id: [u8; 32]) -> Vec<u8> {
    let mut bytes = CONTROL.0.to_le_bytes().to_vec();
    bytes.extend_from_slice(&id);
    bytes
}
pub(super) fn record_bytes(snapshot: &VsrHistorySnapshot, ordinal: usize) -> Vec<u8> {
    let mut bytes = finish_bytes(snapshot.descriptor.id().unwrap());
    bytes.extend_from_slice(&(ordinal as u64).to_le_bytes());
    bytes.extend_from_slice(&snapshot.records[ordinal].encode().unwrap());
    bytes
}
pub(super) fn send_snapshot(
    sender: &NetworkVsrTransport,
    snapshot: &VsrHistorySnapshot,
) -> io::Result<()> {
    let reply = sender.exchange_io(
        NodeId(3),
        HISTORY_BEGIN,
        &snapshot.descriptor.encode().unwrap(),
    )?;
    assert_eq!(reply.len(), 8);
    let count = u64::from_le_bytes(reply.try_into().unwrap());
    assert!(count <= snapshot.descriptor.records);
    for ordinal in count as usize..snapshot.records.len() {
        let reply =
            sender.exchange_io(NodeId(3), HISTORY_RECORD, &record_bytes(snapshot, ordinal))?;
        assert_eq!(reply, (ordinal as u64 + 1).to_le_bytes());
    }
    Ok(())
}
pub(super) fn begin_required(result: io::Result<Vec<u8>>, finish: bool, id: [u8; 32]) {
    let error = result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    let status = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<HistoryBeginRequired>())
        .expect("correlated typed BEGIN-required, not timeout/string error");
    assert_eq!(status.placement_group(), CONTROL);
    assert_eq!(status.snapshot(), id);
    assert_eq!(status.is_finish(), finish);
}
