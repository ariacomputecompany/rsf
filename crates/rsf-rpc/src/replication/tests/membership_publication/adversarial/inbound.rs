use super::*;

#[test]
fn real_tls_managed_receiver_retains_published_ancestry_after_ordinary_view_and_promise() {
    let mut ready = fixture();
    ready.publish();
    drop(ready.remote.take());
    let remote = managed(
        server(&ready.root, 3, true, ready.peers.clone()).unwrap(),
        &[PG],
    );
    let peer = GatedPeer::start(remote, ready.root.clone(), false, false);
    ready.peers.get_mut(&NodeId(3)).unwrap().address = peer.address;
    let network = ready.network();
    let mut local = managed(ready.local.take().unwrap(), &[PG]);
    let request = VsrViewChangeRequest {
        pg: PG,
        previous_view: Epoch(2),
        target_view: Epoch(3),
        candidate: NodeId(2),
        voters: proposal().new_voters,
    };
    let cell = local.cells.get_mut(&PG).unwrap();
    let (input, snapshots) = network.prepare_installation_input(cell, &request).unwrap();
    network
        .stage_installation_input(cell, &input, &snapshots)
        .unwrap();
    network
        .resume_view_installation(cell, &mut |entry| apply(&ready.root, 2, entry))
        .unwrap();
    let promise = VsrViewChangeRequest {
        previous_view: Epoch(3),
        target_view: Epoch(4),
        ..request
    };
    cell.begin_view_change(NodeId(2), NodeId(2), promise.clone())
        .unwrap();
    network.fetch_view_change(NodeId(3), &promise).unwrap();
    let bootstrap = local.membership_decisions[&PG]
        .decision_state()
        .unwrap()
        .bootstrap;
    let round = VsrMembershipRound {
        counter: 17,
        coordinator: NodeId(2),
    };
    assert!(matches!(
        network
            .promise_membership_recovery(NodeId(3), &proposal(), bootstrap, round)
            .unwrap(),
        rsf_vsr::VsrMembershipPromiseReply::Granted(_)
    ));
    let report = network
        .query_membership_publication(NodeId(3), PG, ready.quorum.plan_id())
        .unwrap();
    assert_eq!(report.plan, ready.quorum.plan_id());
    let retry = network
        .publish_membership(NodeId(3), &ready.quorum)
        .unwrap();
    assert_eq!(retry, report);
    // The live ordinary promise is not cleared by historical membership work.
    drop(peer);
    let remote = managed(
        server(&ready.root, 3, true, ready.peers.clone()).unwrap(),
        &[PG],
    );
    assert_eq!(remote.cells[&PG].view(), Epoch(3));
    assert_eq!(remote.cells[&PG].view_change_promise(), Some(&promise));
    assert!(remote.membership_admission_required().unwrap());
    assert!(local.membership_admission_required().unwrap());
}

// One request through production authentication/dispatch, with the actual
// idempotent fixture materializer. Join before inspecting or damaging custody.
#[cfg(unix)]
fn publication_tls(
    mut server: PeerReplicaServer,
    root: &Path,
    kind: u8,
    payload: &[u8],
) -> (PeerReplicaServer, io::Result<Vec<u8>>, usize) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut endpoints = peers(root);
    let receiver = server.local;
    endpoints.get_mut(&receiver).unwrap().address = listener.local_addr().unwrap();
    let path = root.to_path_buf();
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "publication request never arrived"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("publication listener: {error}"),
            }
        };
        let mut applied = 0;
        let _ = server.serve_connection_with_apply(socket, &mut |entry| {
            applied += 1;
            persist_staged_entry(&path, receiver.0, entry)
        });
        (server, applied)
    });
    let result = transport_for(root, 2, endpoints).exchange_once(receiver, kind, payload);
    let (server, applied) = worker.join().unwrap();
    (server, result, applied)
}

#[cfg(unix)]
fn publication_tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(base: &Path, at: &Path, result: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        result.insert(at.strip_prefix(base).unwrap().to_path_buf(), None);
        for entry in std::fs::read_dir(at).unwrap() {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            let path = entry.path();
            if kind.is_dir() {
                visit(base, &path, result);
            } else {
                assert!(kind.is_file());
                if path.extension() != Some(std::ffi::OsStr::new("lock")) {
                    result.insert(
                        path.strip_prefix(base).unwrap().to_path_buf(),
                        Some(std::fs::read(path).unwrap()),
                    );
                }
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

#[cfg(unix)]
#[test]
fn real_tls_managed_historical_publication_survives_auxiliary_journal_loss_only() {
    let mut ready = fixture();
    ready.publish();
    drop(ready.remote.take());
    let mut remote = managed(
        server(&ready.root, 3, true, ready.peers.clone()).unwrap(),
        &[PG],
    );
    let committed = remote.cells[&PG].committed_prefix().unwrap();
    let expected_entries = committed.len();
    assert!(expected_entries > 0);
    let applied_indices = committed
        .iter()
        .flat_map(|entry| entry.prepare.index.0.to_le_bytes())
        .collect::<Vec<_>>();
    let mut query = PG.0.to_le_bytes().to_vec();
    query.extend_from_slice(&ready.quorum.plan_id());
    let publication = ready.quorum.encode().unwrap();
    let (returned, result, applied) =
        publication_tls(remote, &ready.root, MEMBERSHIP_PUBLICATION, &query);
    remote = returned;
    let expected = rsf_vsr::VsrMembershipPublishedReport::decode(&result.unwrap()).unwrap();
    assert_eq!(applied, expected_entries);
    assert_eq!(expected.plan, ready.quorum.plan_id());
    assert_eq!(expected.replica, NodeId(3));

    // Reuse the existing fixture's validated active-generation inventory.
    // Read paths before the fault so no hosted guard pre-latches its failure.
    let before = inventory(&ready.root, 3);
    let generation = before.pointer[12..44]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let hosted = ready
        .root
        .join("store-3/vsr-views")
        .join(generation)
        .join("MANIFEST");
    assert_eq!(std::fs::read(&hosted).unwrap(), before.manifest);
    let journal = ready.root.join("membership-3/decision/journal/MANIFEST");
    std::fs::remove_file(&journal).unwrap();
    let mut journal_lost = publication_tree(&ready.root);
    for (kind, payload) in [
        (MEMBERSHIP_PUBLICATION, query.as_slice()),
        (MEMBERSHIP_PUBLISH, publication.as_slice()),
    ] {
        let (returned, result, applied) = publication_tls(remote, &ready.root, kind, payload);
        remote = returned;
        assert_eq!(
            rsf_vsr::VsrMembershipPublishedReport::decode(&result.unwrap()).unwrap(),
            expected
        );
        assert_eq!(applied, expected_entries);
        // The actual materializer records each invocation. Permit exactly the
        // expected current-prefix append, while all custody/data bytes stay equal.
        journal_lost
            .get_mut(Path::new("materialized-3/.applies"))
            .unwrap()
            .as_mut()
            .unwrap()
            .extend_from_slice(&applied_indices);
        assert_eq!(publication_tree(&ready.root), journal_lost);
        assert!(!journal.exists());
        assert!(hosted.is_file());
    }

    // Historical evidence remains contingent on the current owned cell. The
    // first query must fail before materialization; its repeat must stay fenced.
    std::fs::remove_file(&hosted).unwrap();
    let hosted_lost = publication_tree(&ready.root);
    let pending = remote.installation_pending.clone();
    for (attempt, (kind, payload)) in [
        (MEMBERSHIP_PUBLICATION, query.as_slice()),
        (MEMBERSHIP_PUBLISH, publication.as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        let (returned, result, applied) = publication_tls(remote, &ready.root, kind, payload);
        remote = returned;
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
        let message = error.to_string();
        let reason = message
            .strip_prefix("peer rejected request: ")
            .unwrap_or_else(|| panic!("expected structured peer rejection: {error}"));
        let prefix = if attempt == 0 {
            "storage Recover failed: "
        } else {
            "VSR durability failed; reopen before further operations: storage Recover failed: "
        };
        assert!(reason.starts_with(prefix), "{error}");
        assert_eq!(applied, 0);
        assert_eq!(remote.installation_pending, pending);
        assert_eq!(publication_tree(&ready.root), hosted_lost);
        assert!(!journal.exists());
        assert!(!hosted.exists());
    }
}
