use super::*;
#[test]
fn election_timing_covers_network_deadlines_and_rejects_unbounded_or_zero_intervals() {
    let valid = PeerElectionTiming {
        heartbeat: Duration::from_millis(100),
        timeout: Duration::from_millis(1500),
        rank_delay: Duration::from_millis(1000),
    };
    valid.validate(Duration::from_millis(150), 3).unwrap();
    assert!(valid.validate(Duration::from_millis(150), 0).is_err());
    assert!(valid.validate(Duration::from_millis(150), 4097).is_err());
    assert!(valid.validate(Duration::ZERO, 3).is_err());
    for timing in [
        PeerElectionTiming {
            heartbeat: Duration::ZERO,
            ..valid
        },
        PeerElectionTiming {
            heartbeat: Duration::from_nanos(1),
            ..valid
        },
        PeerElectionTiming {
            timeout: Duration::from_millis(800),
            ..valid
        },
        PeerElectionTiming {
            rank_delay: Duration::from_millis(1),
            ..valid
        },
        PeerElectionTiming {
            heartbeat: Duration::from_secs(u64::MAX),
            ..valid
        },
    ] {
        assert!(timing.validate(Duration::from_millis(150), 3).is_err());
    }
}
#[test]
fn successor_rank_rotates_after_each_authority_without_changing_the_voter_set() {
    let voters = vec![NodeId(2), NodeId(5), NodeId(9)];
    assert_eq!(successor_rank(&voters, NodeId(5), NodeId(9)).unwrap(), 1);
    assert_eq!(successor_rank(&voters, NodeId(5), NodeId(2)).unwrap(), 2);
    assert_eq!(successor_rank(&voters, NodeId(9), NodeId(2)).unwrap(), 1);
    assert!(successor_rank(&voters, NodeId(1), NodeId(2)).is_err());
}

// Test-only observation of the exact first authenticated installed-view sighting.
// No clock mutation or production API is exposed to the TLS regression.
impl PeerReplicaServer {
    pub(in crate::replication) fn election_adoption_window(
        &self,
        pg: PlacementGroupId,
    ) -> Option<(Epoch, Instant)> {
        self.election_clock[&pg].newer_installed
    }
}
