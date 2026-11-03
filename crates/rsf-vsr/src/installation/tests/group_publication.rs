use super::*;

#[test]
fn rotated_generation_group_preserves_sealed_bytes_publication_boundaries_and_later_appends() {
    for policy in [rsf_storage::SyncPolicy::Data, rsf_storage::SyncPolicy::Full] {
        for boundary in [Boundary::Staged, Boundary::Renamed, Boundary::Published] {
            let path = root("group-publication");
            let (first, second, snapshots) = fixture(&path);
            let plan = VsrViewInstallation::select(&snapshots).unwrap();
            drop(first);
            let local = path.join("1");
            let mut first = DurableVsrCell::open_with_store_options(
                &local,
                PlacementGroupId(1),
                Epoch(1),
                NodeId(0),
                [NodeId(0), NodeId(1), NodeId(2)],
                rsf_storage::SegmentStoreOptions {
                    max_segment_bytes: 1,
                    log_options: rsf_storage::SegmentLogOptions {
                        sync_policy: policy,
                    },
                },
            )
            .unwrap();
            let mut staged = false;
            let outcome = first.install_view_with_hook(&plan, &mut |at| {
                if matches!(at, Boundary::Staged) {
                    staged = true;
                    assert!(!local.join("VSR_ACTIVE").exists());
                    let generation = std::fs::read_dir(local.join("vsr-views"))
                        .unwrap()
                        .next()
                        .unwrap()
                        .unwrap()
                        .path();
                    assert!(generation.join("VSR_SEAL").is_file());
                    let physical = SegmentStore::recover_existing(&generation).unwrap();
                    assert_eq!(physical.segments.len(), plan.records.len());
                    assert_eq!(physical.recovery.wal_records.len(), plan.records.len());
                    for (actual, expected) in
                        physical.recovery.wal_records.iter().zip(&plan.records)
                    {
                        assert_eq!(actual.pg, PlacementGroupId(1));
                        assert_eq!(actual.epoch, expected.epoch);
                        assert_eq!(actual.kind, expected.kind);
                        assert_eq!(actual.payload, expected.payload);
                    }
                }
                if std::mem::discriminant(&at) == std::mem::discriminant(&boundary) {
                    Err(corrupt(
                        "injected publication boundary failure after group flush",
                    ))
                } else {
                    Ok(())
                }
            });
            assert!(staged);
            assert!(
                outcome.is_err(),
                "publication boundary failure cannot acknowledge installation"
            );
            drop(first);
            let mut recovered = DurableVsrCell::open_existing(&local, PlacementGroupId(1)).unwrap();
            assert_eq!(
                recovered.view(),
                if matches!(boundary, Boundary::Staged) {
                    Epoch(1)
                } else {
                    Epoch(2)
                }
            );
            recovered.install_view(&plan).unwrap();
            assert_eq!(recovered.committed_tail(), LogIndex(1));
            assert_eq!(recovered.next_index(), LogIndex(3));
            assert_eq!(
                recovered.request_commit(&VsrClientRequestId::new("original", 1)),
                Some(LogIndex(1))
            );
            assert_eq!(recovered.history_snapshot().unwrap().records, plan.records);
            // The accepted-only index 2 remains pending. A later ordinary
            // append still persists through the normal VSR writer path.
            let VsrDecision::Prepared(prepared) = recovered
                .prepare(
                    NodeId(1),
                    VsrOperation::ProtocolOperation {
                        operation_id: OperationId(3),
                    },
                    1,
                )
                .unwrap()
            else {
                panic!("ordinary prepare after grouped publication");
            };
            assert_eq!(prepared.index, LogIndex(3));
            drop(recovered);
            let cold = DurableVsrCell::open_existing(&local, PlacementGroupId(1)).unwrap();
            assert_eq!(cold.committed_tail(), LogIndex(1));
            assert_eq!(cold.next_index(), LogIndex(4));
            assert_eq!(cold.prepared(LogIndex(3)).unwrap(), &prepared);
            drop(cold);
            drop(second);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
