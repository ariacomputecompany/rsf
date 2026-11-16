use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::sync::Arc;

pub(super) struct Fixture {
    pub root: Root,
    pub state: PathBuf,
    pub config: PathBuf,
    pub policy: ControlInitialPolicy,
    pub clients: BTreeMap<u64, Arc<rustls::ClientConfig>>,
    pub receiver_fingerprint: [u8; 32],
    source: DurableVsrCell,
    follower: DurableVsrCell,
}
impl Fixture {
    pub fn new(name: &str) -> Self {
        let root = Root::new(name);
        let state = root.0.join("native");
        let cluster = ClusterId::new("native-control-receiver").unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);
        let mut certificates = Vec::new();
        let mut clients = BTreeMap::new();
        for node in 0..4 {
            let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ];
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            let key = KeyPair::generate().unwrap();
            let cert = params.signed_by(&key, &issuer).unwrap();
            let tls = root.0.join(format!("tls-{node}"));
            fs::create_dir(&tls).unwrap();
            fs::write(tls.join("ca.pem"), ca.pem()).unwrap();
            fs::write(tls.join("certificate.pem"), cert.pem()).unwrap();
            fs::write(tls.join("key.pem"), key.serialize_pem()).unwrap();
            certificates.push(PeerIdentity::load(&tls).unwrap().fingerprint);
            if node == 0 || node == 2 {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(ca.der().clone()).unwrap();
                let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    vec![cert.der().clone()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
                )
                .unwrap();
                clients.insert(node, Arc::new(cfg));
            }
        }
        let source = DurableVsrCell::open(
            root.0.join("source"),
            PlacementGroupId(0),
            Epoch(1),
            NodeId(0),
            (0..3).map(NodeId),
        )
        .unwrap();
        let follower = DurableVsrCell::open(
            root.0.join("follower"),
            PlacementGroupId(0),
            Epoch(1),
            NodeId(0),
            (0..3).map(NodeId),
        )
        .unwrap();
        let data = DurableVsrCell::open(
            root.0.join("data-bootstrap"),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(2),
            (0..3).map(NodeId),
        )
        .unwrap();
        let catalog = ControlCatalog {
            cluster: cluster.clone(),
            control_pg: PlacementGroupId(0),
            routing: CatalogRouting {
                generation: RoutingGeneration(1),
                placement: [3; 32],
                policy: [4; 32],
            },
            nodes: (0..4)
                .map(|n| CatalogNode {
                    id: NodeId(n),
                    incarnation: 1,
                    public_key: format!("node-public-key-{n}"),
                    nonce: format!("nonce-{n}-000"),
                    trust_domain: "rsf.test".into(),
                    spiffe_id: format!("spiffe://rsf.test/rsf/node/node-public-key-{n}"),
                    certificate: certificates[n as usize],
                    role: if n == 3 {
                        AdmissionRole::Learner
                    } else {
                        AdmissionRole::Voter
                    },
                })
                .collect(),
            groups: [(&source, 0), (&data, 1)]
                .into_iter()
                .map(|(cell, pg)| CatalogGroup {
                    pg: PlacementGroupId(pg),
                    lineage: MembershipLineage {
                        bootstrap: cell.membership_lineage().unwrap().bootstrap,
                        plan: None,
                    },
                    voters: (0..3).map(NodeId).collect(),
                })
                .collect(),
        };
        let policy = ControlInitialPolicy {
            anchor: ControlBootstrap {
                cluster,
                control_pg: PlacementGroupId(0),
                control_bootstrap: source.membership_lineage().unwrap().bootstrap,
                genesis_catalog: catalog.root().unwrap(),
            },
            genesis: catalog,
            limits: rsf_vsr::VsrPrefixLimits {
                max_records: 4096,
                max_encoded_bytes: 16 * 1024 * 1024,
            },
        };
        fs::write(
            root.0.join("genesis"),
            ControlCommand::Genesis(policy.genesis.clone())
                .encode()
                .unwrap(),
        )
        .unwrap();
        let peers=certificates.iter().enumerate().map(|(n,c)|serde_json::json!({"node":n,"address":"127.0.0.1:1","server_name":"localhost","fingerprint":c.iter().map(|b|format!("{b:02x}")).collect::<String>()})).collect::<Vec<_>>();
        let cfg = serde_json::json!({"cluster":policy.anchor.cluster.as_str(),"node":3,"state":state,"tls":root.0.join("tls-3"),"listen":"127.0.0.1:0","timeout_ms":800,"peers":peers,"cells":[{"pg":0,"view":1,"primary":0,"voters":[0,1,2]}],"control":{"genesis":"genesis","prefix_limits":{"max_records":4096,"max_encoded_bytes":16777216},"provisioning":{"kind":"bootstrap"},"preferred_transport":1}});
        let config = root.0.join("peer.json");
        fs::write(&config, serde_json::to_vec(&cfg).unwrap()).unwrap();
        Self {
            root,
            state,
            config,
            policy,
            clients,
            receiver_fingerprint: certificates[3],
            source,
            follower,
        }
    }
    pub fn control_root(&self) -> PathBuf {
        self.state.join("learner-0")
    }
    pub fn persisted(&self) -> ControlInitialState {
        state_identity::tests::decode_initial(&self.state.join("IDENTITY.json"))
    }
    pub fn append_intent(&mut self) -> VsrHistorySnapshot {
        let intent = MembershipIntent {
            request: ControlRequestId([8; 32]),
            parent: self.policy.anchor.genesis_catalog,
            revision: ControlRevision(1),
            routing: self.policy.genesis.routing,
            nodes: self
                .policy
                .genesis
                .nodes
                .iter()
                .cloned()
                .map(|mut n| {
                    n.role = if n.id == NodeId(2) {
                        AdmissionRole::Learner
                    } else {
                        AdmissionRole::Voter
                    };
                    n
                })
                .collect(),
            groups: self
                .policy
                .genesis
                .groups
                .iter()
                .map(|g| GroupIntent {
                    pg: g.pg,
                    previous: g.lineage,
                    proposal: rsf_vsr::VsrMembershipProposal {
                        election: rsf_vsr::VsrViewChangeRequest {
                            pg: g.pg,
                            previous_view: Epoch(1),
                            target_view: Epoch(2),
                            candidate: NodeId(0),
                            voters: g.voters.clone(),
                        },
                        old_primary: if g.pg == PlacementGroupId(0) {
                            NodeId(0)
                        } else {
                            NodeId(2)
                        },
                        new_voters: vec![NodeId(0), NodeId(1), NodeId(3)],
                    },
                })
                .collect(),
        };
        commit(
            &mut self.source,
            &mut self.follower,
            NodeId(0),
            NodeId(1),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(2),
                payload: ControlCommand::Begin(intent).encode().unwrap(),
            },
        );
        self.source.history_snapshot().unwrap()
    }
    pub fn application_snapshot(&self, sequence: u64) -> VsrHistorySnapshot {
        let mut source =
            DurableVsrCell::open_existing(self.root.0.join("data-bootstrap"), PlacementGroupId(1))
                .unwrap();
        let mut follower = DurableVsrCell::open(
            self.root.0.join("data-follower"),
            PlacementGroupId(1),
            Epoch(1),
            NodeId(2),
            (0..3).map(NodeId),
        )
        .unwrap();
        commit(
            &mut source,
            &mut follower,
            NodeId(2),
            NodeId(1),
            VsrOperation::ReplicatedCommand {
                operation_id: OperationId(sequence),
                payload: format!("actual-data-{sequence}").into_bytes(),
            },
        );
        source.history_snapshot().unwrap()
    }
    pub fn genesis_entry(&self) -> rsf_vsr::VsrCommittedEntry {
        self.source
            .committed_prefix()
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    }
    pub fn genesis_snapshot(&mut self) -> VsrHistorySnapshot {
        if self.source.committed_tail() == LogIndex(0) {
            let payload = ControlCommand::Genesis(self.policy.genesis.clone())
                .encode()
                .unwrap();
            let VsrDecision::Prepared(prepare) = self
                .source
                .prepare(
                    NodeId(0),
                    VsrOperation::ReplicatedCommand {
                        operation_id: OperationId(1),
                        payload: payload.clone(),
                    },
                    payload.len() as u32,
                )
                .unwrap()
            else {
                panic!("Genesis prepare")
            };
            // Actual independent durable receiver, not a synthesized ACK/DTO.
            self.follower
                .receive_prepare(NodeId(1), NodeId(0), prepare.clone())
                .unwrap();
            assert!(matches!(
                self.source
                    .acknowledge(NodeId(1), prepare.view, prepare.index),
                VsrDecision::Acknowledged { .. }
            ));
            let VsrDecision::Committed(commit) =
                self.source.commit(NodeId(0), prepare.index).unwrap()
            else {
                panic!("Genesis commit")
            };
            self.follower
                .receive_commit(NodeId(1), NodeId(0), commit)
                .unwrap();
        }
        assert_eq!(self.follower.committed_tail(), LogIndex(1));
        assert_eq!(self.source.committed_tail(), LogIndex(1));
        assert_eq!(self.source.next_index(), LogIndex(2));
        self.source.history_snapshot().unwrap()
    }
}

fn commit(
    source: &mut DurableVsrCell,
    follower: &mut DurableVsrCell,
    primary: NodeId,
    replica: NodeId,
    operation: VsrOperation,
) {
    let bytes = match &operation {
        VsrOperation::ReplicatedCommand { payload, .. } => payload.len() as u32,
        _ => unreachable!(),
    };
    let VsrDecision::Prepared(prepare) = source.prepare(primary, operation, bytes).unwrap() else {
        panic!("actual prepare")
    };
    assert_eq!(
        follower
            .receive_prepare(replica, primary, prepare.clone())
            .unwrap(),
        replica
    );
    assert!(matches!(
        source.acknowledge(replica, prepare.view, prepare.index),
        VsrDecision::Acknowledged { .. }
    ));
    let VsrDecision::Committed(commit) = source.commit(primary, prepare.index).unwrap() else {
        panic!("actual commit")
    };
    follower.receive_commit(replica, primary, commit).unwrap();
}
