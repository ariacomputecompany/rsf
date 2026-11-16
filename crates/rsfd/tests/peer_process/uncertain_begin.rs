//! Lose the caller's commit observation only after real TLS peers durably apply
//! BEGIN. This transport-boundary fault does not replace or mock peer execution.
use super::*;
use rsf_protocol::{ReplicatedFabricCommand, VsrBackedFabricRouter};
use rsf_sql::{DurableSqlEngine, SqlOutcome, SqlSessionContext};
use rsf_vsr::{
    VsrCommit, VsrCommittedEntry, VsrPrepareMessage, VsrPrimaryProbe, VsrTransport,
    VsrTransportError,
};

struct LostObservation {
    network: NetworkVsrTransport,
    lose: bool,
    lost: usize,
}
impl VsrTransport for LostObservation {
    fn confirm_primary(
        &mut self,
        replica: NodeId,
        probe: &VsrPrimaryProbe,
    ) -> Result<(), VsrTransportError> {
        self.network.confirm_primary(replica, probe)
    }
    fn acknowledge_prepare(
        &mut self,
        message: VsrPrepareMessage,
    ) -> Result<NodeId, VsrTransportError> {
        self.network.acknowledge_prepare(message)
    }
    fn acknowledge_commit(
        &mut self,
        replica: NodeId,
        commit: VsrCommit,
    ) -> Result<NodeId, VsrTransportError> {
        let observed = self.network.acknowledge_commit(replica, commit)?;
        if self.lose {
            self.lost += 1;
            Err(VsrTransportError {
                replica,
                reason: "injected lost observation after real peer commit and materialization"
                    .into(),
            })
        } else {
            Ok(observed)
        }
    }
    fn committed_tail(
        &mut self,
        replica: NodeId,
        pg: PlacementGroupId,
    ) -> Result<LogIndex, VsrTransportError> {
        self.network.committed_tail(replica, pg)
    }
    fn transfer_committed(
        &mut self,
        replica: NodeId,
        entry: VsrCommittedEntry,
    ) -> Result<(), VsrTransportError> {
        self.network.transfer_committed(replica, entry)
    }
}
fn connect(root: &Path, peers: BTreeMap<NodeId, PeerEndpoint>, lose: bool) -> LostObservation {
    LostObservation {
        network: NetworkVsrTransport::new(
            NodeId(0),
            &ClusterId::new("cli-cluster").unwrap(),
            PeerIdentity::load(&root.join("tls-0")).unwrap(),
            peers,
            Duration::from_millis(PEER_TIMEOUT_MS),
        )
        .unwrap(),
        lose,
        lost: 0,
    }
}
fn router(root: &Path, network: LostObservation) -> VsrBackedFabricRouter<LostObservation> {
    VsrBackedFabricRouter::open_with_transport(
        root.join("source-protocol"),
        [PlacementGroupId(1)],
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
        network,
    )
    .unwrap()
}
fn committed(root: &Path, node: u64) -> Vec<VsrCommittedEntry> {
    DurableVsrCell::open(
        root.join(format!("store-{node}/pg-1")),
        PlacementGroupId(1),
        Epoch(1),
        NodeId(0),
        [NodeId(0), NodeId(1), NodeId(2)],
    )
    .unwrap()
    .committed_prefix()
    .unwrap()
}
#[test]
fn automatic_client_takeover_uncertain_sql_begin_preserves_token_after_both_peer_restarts() {
    let root = root();
    let mut peers = provision(&root);
    let first = Process::start(&root.join("peer-1.json"));
    let second = Process::start(&root.join("peer-2.json"));
    peers.get_mut(&NodeId(1)).unwrap().address = first.address;
    peers.get_mut(&NodeId(2)).unwrap().address = second.address;
    let context = SqlSessionContext {
        name: "uncertain-begin".into(),
        transaction: None,
    };
    let mut protocol = router(&root, connect(&root, peers.clone(), true));
    let mut sql = DurableSqlEngine::open_replicated(root.join("source-sql"), "app").unwrap();
    let error = sql
        .execute_session(
            &mut protocol,
            PlacementGroupId(1),
            "app-client",
            &context,
            "BEGIN",
            "original-begin",
        )
        .unwrap_err();
    assert!(
        error
            .message
            .contains("durable commit evidence reached 1 replicas, requires 2"),
        "{error:?}"
    );
    assert_eq!(protocol.transport_mut().lost, 2);
    drop(sql);
    drop(protocol);
    drop(first);
    drop(second);
    let entries = committed(&root, 1);
    assert_eq!(entries.len(), 1);
    assert_eq!(committed(&root, 2), entries);
    let VsrOperation::ReplicatedCommand { payload, .. } = &entries[0].prepare.operation else {
        panic!("missing complete command")
    };
    let command = ReplicatedFabricCommand::decode(payload).unwrap();
    let payload = command
        .application
        .bytes
        .strip_prefix(b"RSFSC3\x01")
        .unwrap();
    let fields = std::str::from_utf8(payload)
        .unwrap()
        .split('\t')
        .collect::<Vec<_>>();
    let session = fields[4]
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    let saved: serde_json::Value =
        serde_json::from_slice(session.strip_prefix(b"RSFSP3").unwrap()).unwrap();
    let token = saved["after"]["token"].as_str().unwrap();
    for _ in 0..2 {
        let first = Process::start(&root.join("peer-1.json"));
        let second = Process::start(&root.join("peer-2.json"));
        peers.get_mut(&NodeId(1)).unwrap().address = first.address;
        peers.get_mut(&NodeId(2)).unwrap().address = second.address;
        let mut protocol = router(&root, connect(&root, peers.clone(), false));
        let mut sql = DurableSqlEngine::open_replicated(root.join("source-sql"), "app").unwrap();
        let result = sql
            .execute_session(
                &mut protocol,
                PlacementGroupId(1),
                "app-client",
                &context,
                "BEGIN",
                "original-begin",
            )
            .unwrap();
        assert!(result.replayed && result.persisted);
        assert_eq!(result.version, Some(command.commit.version));
        let SqlOutcome::TransactionBegan { session_id, .. } = result.outcome else {
            panic!("lost original BEGIN result")
        };
        assert_eq!(session_id, token);
        let changed = SqlSessionContext {
            name: "changed".into(),
            transaction: None,
        };
        assert!(
            sql.execute_session(
                &mut protocol,
                PlacementGroupId(1),
                "app-client",
                &changed,
                "BEGIN",
                "original-begin"
            )
            .is_err()
        );
        let retry = sql
            .execute_session(
                &mut protocol,
                PlacementGroupId(1),
                "app-client",
                &context,
                "BEGIN",
                "original-begin",
            )
            .unwrap();
        assert_eq!(retry.version, Some(command.commit.version));
        drop(sql);
        drop(protocol);
        drop(first);
        drop(second);
        assert_eq!(committed(&root, 1), entries);
        assert_eq!(committed(&root, 2), entries);
    }
    std::fs::remove_dir_all(root).unwrap();
}
