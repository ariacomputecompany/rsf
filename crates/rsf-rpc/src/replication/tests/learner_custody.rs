use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(base: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let relative = path.strip_prefix(base).unwrap().to_path_buf();
            if entry.file_type().unwrap().is_dir() {
                entries.insert(relative, None);
                visit(base, &path, entries);
            } else {
                entries.insert(relative, Some(std::fs::read(path).unwrap()));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(root, root, &mut entries);
    entries
}

#[test]
fn real_tls_cached_learner_ack_rejects_lost_custody_until_validated_restart() {
    for fault in ["manifest", "segment", "descriptor"] {
        for first in [HISTORY_BEGIN, HISTORY_FINISH] {
            let root = root(&format!("learner-custody-{fault}-{first}"));
            provision(&root);
            let voter = PeerProcess::spawn(&root, 1);
            let mut peers = endpoints(&root);
            peers.get_mut(&NodeId(1)).unwrap().address = voter.address;
            peers.insert(
                NodeId(3),
                PeerEndpoint {
                    address: "127.0.0.1:1".parse().unwrap(),
                    server_name: "localhost".into(),
                    fingerprint: identity(&root, 3).fingerprint,
                },
            );
            let applied = Arc::new(AtomicUsize::new(0));
            let learner = LearnerPeer::start(&root, peers.clone(), Arc::clone(&applied));
            peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
            let mut network = transport(&root, peers.clone());
            let mut leader = cell(&root, 0);
            let prepared = prepare(&mut leader, b"preserve committed learner custody");
            leader
                .replicate_with_transport(NodeId(0), prepared.index, &mut network)
                .unwrap();
            leader
                .remember_client_request(
                    rsf_vsr::VsrClientRequestId::new("custody", 1),
                    prepared.index,
                )
                .unwrap();
            let snapshot = leader.history_snapshot().unwrap();
            let id = snapshot.descriptor.id().unwrap();
            assert_eq!(
                network.sync_learner(NodeId(3), &mut leader).unwrap(),
                prepared.index
            );
            assert_eq!(
                applied.load(Ordering::Relaxed),
                1,
                "warm the actual server materialization cache"
            );

            let begin = snapshot.descriptor.encode().unwrap();
            let mut finish = PlacementGroupId(1).0.to_le_bytes().to_vec();
            finish.extend_from_slice(&id);
            // A real exact retry takes both cached success branches before the
            // fault, without invoking the already completed materializer again.
            assert_eq!(
                network
                    .request_io(NodeId(3), HISTORY_BEGIN, &begin)
                    .unwrap(),
                snapshot.descriptor.records
            );
            assert_eq!(
                network
                    .request_io(NodeId(3), HISTORY_FINISH, &finish)
                    .unwrap(),
                prepared.index.0
            );
            assert_eq!(applied.load(Ordering::Relaxed), 1);

            let target = root.join("learner-3");
            let name = id
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let published = target.join(name);
            let missing = match fault {
                "manifest" => published.join("cell/MANIFEST"),
                "segment" => {
                    let recovered =
                        rsf_storage::SegmentStore::recover_existing(published.join("cell"))
                            .unwrap();
                    published
                        .join("cell/segments")
                        .join(&recovered.segments.last().unwrap().file_name)
                }
                "descriptor" => published.join("DESCRIPTOR"),
                _ => unreachable!(),
            };
            let saved = std::fs::read(&missing).unwrap();
            std::fs::remove_file(&missing).unwrap();
            let unrelated = target.join("f".repeat(64));
            assert_ne!(unrelated, published);
            std::fs::create_dir(&unrelated).unwrap();
            std::fs::write(
                unrelated.join("sentinel"),
                b"must not reclaim before custody check",
            )
            .unwrap();
            let before = tree(&target);
            let second = if first == HISTORY_BEGIN {
                HISTORY_FINISH
            } else {
                HISTORY_BEGIN
            };
            for kind in [first, second] {
                let payload = if kind == HISTORY_BEGIN {
                    &begin
                } else {
                    &finish
                };
                let error = network.request_io(NodeId(3), kind, payload).unwrap_err();
                // A timeout, TLS failure or dropped connection cannot satisfy
                // the oracle: require the peer's structured rejection response.
                assert_eq!(
                    error.kind(),
                    io::ErrorKind::InvalidData,
                    "root={} fault={fault} kind={kind}: {error}",
                    root.display()
                );
                assert!(
                    error.to_string().starts_with("peer rejected request:"),
                    "{error}"
                );
                assert_eq!(applied.load(Ordering::Relaxed), 1);
                assert!(!missing.exists());
                assert_eq!(
                    tree(&target),
                    before,
                    "failed ACK repaired or reclaimed custody"
                );
            }
            std::fs::write(&missing, &saved).unwrap();
            let restored = tree(&target);
            for kind in [HISTORY_BEGIN, HISTORY_FINISH] {
                let payload = if kind == HISTORY_BEGIN {
                    &begin
                } else {
                    &finish
                };
                let error = network.request_io(NodeId(3), kind, payload).unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
                assert!(
                    error.to_string().starts_with("peer rejected request:"),
                    "{error}"
                );
                assert!(
                    error.to_string().contains("learner durability failed"),
                    "{error}"
                );
                assert_eq!(applied.load(Ordering::Relaxed), 1);
                assert_eq!(tree(&target), restored);
            }
            drop(learner);
            // A new actual server must validate the restored on-disk owner and
            // materialize again. Subsequent exact retries reuse that boot's
            // successful materialization, preserving the original data.
            let learner = LearnerPeer::start(&root, peers.clone(), Arc::clone(&applied));
            peers.get_mut(&NodeId(3)).unwrap().address = learner.address;
            let mut network = transport(&root, peers);
            assert_eq!(
                network.sync_learner(NodeId(3), &mut leader).unwrap(),
                prepared.index
            );
            assert_eq!(applied.load(Ordering::Relaxed), 2);
            assert_eq!(
                network
                    .request_io(NodeId(3), HISTORY_BEGIN, &begin)
                    .unwrap(),
                snapshot.descriptor.records
            );
            assert_eq!(
                network
                    .request_io(NodeId(3), HISTORY_FINISH, &finish)
                    .unwrap(),
                prepared.index.0
            );
            assert_eq!(applied.load(Ordering::Relaxed), 2);
            drop(learner);
            let cold = rsf_vsr::LearnerHistoryStore::open(
                &target,
                PlacementGroupId(1),
                NodeId(3),
                NodeId(0),
            )
            .unwrap();
            assert_eq!(
                cold.committed_entries().unwrap(),
                leader.committed_prefix().unwrap()
            );
            drop(cold);
            drop(leader);
            drop(voter);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
