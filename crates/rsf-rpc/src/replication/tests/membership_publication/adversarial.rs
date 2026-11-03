//! Scheduler ownership and interleaving tests over the actual TLS transport.
use super::*;
use crate::replication::{PeerMembershipRecoveryTiming, PeerMembershipRecoveryUpdate};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};

const RPC: Duration = Duration::from_millis(800);
const HARNESS: Duration = Duration::from_secs(15);

#[path = "adversarial/groups.rs"]
mod groups;
#[path = "adversarial/lifecycle.rs"]
mod lifecycle;
#[path = "adversarial/peer.rs"]
mod peer;
#[path = "adversarial/replies.rs"]
mod replies;
use peer::{Blackhole, GatedPeer};

struct Joined<T>(Option<std::thread::JoinHandle<T>>);
impl<T> Joined<T> {
    fn join(mut self) -> T {
        self.0.take().unwrap().join().unwrap()
    }
}
impl<T> Drop for Joined<T> {
    fn drop(&mut self) {
        if let Some(thread) = self.0.take() {
            let joined = thread.join();
            if !std::thread::panicking() {
                drop(joined.unwrap());
            }
        }
    }
}

fn managed(server: PeerReplicaServer, groups: &[PlacementGroupId]) -> PeerReplicaServer {
    server
        .with_membership_recovery(
            groups
                .iter()
                .map(|pg| {
                    (
                        *pg,
                        PeerMembershipRecoveryTiming {
                            interval: Duration::from_millis(1),
                        },
                    )
                })
                .collect(),
        )
        .unwrap()
}

fn apply(root: &Path, node: u64, entry: &VsrCommittedEntry) -> Result<(), String> {
    if entry.prepare.pg == PG {
        persist_staged_entry(root, node, entry)
    } else {
        persist_staged_entry(
            &root.join(format!("group-{}", entry.prepare.pg.0)),
            node,
            entry,
        )
    }
}

fn poll(server: &mut PeerReplicaServer, root: &Path) -> Option<PeerMembershipRecoveryUpdate> {
    let local = server.local.0;
    server
        .poll_membership_recovery(&mut |entry| apply(root, local, entry))
        .unwrap()
}

fn until(
    server: &mut PeerReplicaServer,
    root: &Path,
    predicate: impl Fn(&PeerMembershipRecoveryUpdate) -> bool,
) -> PeerMembershipRecoveryUpdate {
    let deadline = Instant::now() + HARNESS;
    loop {
        if let Some(update) = poll(server, root)
            && predicate(&update)
        {
            return update;
        }
        assert!(
            Instant::now() < deadline,
            "scheduler did not reach expected progress"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
