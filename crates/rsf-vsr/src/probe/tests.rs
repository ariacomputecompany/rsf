use super::*;
#[test]
fn replica_status_roundtrips_normal_and_promised_views_and_rejects_every_truncated_prefix() {
    let mut status = VsrReplicaStatus {
        normal: VsrPrimaryProbe {
            pg: PlacementGroupId(1),
            view: Epoch(4),
            primary: NodeId(0),
            voters: vec![NodeId(0), NodeId(1), NodeId(2)],
            committed_tail: LogIndex(0),
            committed_tip: Digest::blake3_bytes(b"tip"),
        },
        promise: None,
    };
    for promise in [
        None,
        Some(VsrViewChangeRequest {
            pg: PlacementGroupId(1),
            previous_view: Epoch(1),
            target_view: Epoch(5),
            candidate: NodeId(1),
            voters: status.normal.voters.clone(),
        }),
    ] {
        status.promise = promise;
        let bytes = status.encode().unwrap();
        assert_eq!(VsrReplicaStatus::decode(&bytes).unwrap(), status);
        for length in 0..bytes.len() {
            assert!(VsrReplicaStatus::decode(&bytes[..length]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(VsrReplicaStatus::decode(&trailing).is_err());
    }
    status.promise.as_mut().unwrap().target_view = Epoch(4);
    assert!(status.encode().is_err());
    status.promise.as_mut().unwrap().target_view = Epoch(5);
    status.promise.as_mut().unwrap().pg = PlacementGroupId(2);
    assert!(status.encode().is_err());
}
