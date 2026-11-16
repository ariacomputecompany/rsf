use super::*;

#[derive(Debug, Default)]
pub(super) struct Detector {
    direct: BTreeMap<NodeId, Activity>,
    multiplier: u32,
}
#[derive(Debug)]
struct Activity {
    round: VsrMembershipRound,
    accepted: Option<(VsrMembershipRound, [u8; 32])>,
    chosen: Option<(VsrMembershipRound, [u8; 32])>,
    copy: Option<([u8; 32], u64)>,
    changed: Instant,
}
impl Detector {
    pub(super) fn coordinator(&mut self, sender: NodeId, round: VsrMembershipRound, now: Instant) {
        if round.coordinator != sender {
            return;
        }
        let old = self.direct.entry(sender).or_insert(Activity {
            round,
            accepted: None,
            chosen: None,
            copy: None,
            changed: now,
        });
        if round > old.round {
            old.round = round;
            old.copy = None;
            old.changed = now;
        }
    }

    pub(super) fn observe(&mut self, state: &VsrMembershipDecisionState, now: Instant) {
        let Some(round) = floor(state).filter(|r| r.coordinator == state.replica) else {
            return;
        };
        let old = self.direct.entry(state.replica).or_insert(Activity {
            round,
            accepted: None,
            chosen: None,
            copy: None,
            changed: now,
        });
        // A reply captured before a newer authenticated inbound operation may
        // arrive later. It cannot lower or renew that newer activity watermark.
        if round < old.round {
            return;
        }
        let accepted = old.accepted.max(state.accepted);
        let chosen = old.chosen.max(state.chosen);
        if round > old.round || accepted != old.accepted || chosen != old.chosen {
            if round > old.round {
                old.copy = None;
            }
            old.round = round;
            old.accepted = accepted;
            old.chosen = chosen;
            old.changed = now;
        }
    }
    pub(super) fn copied(
        &mut self,
        sender: NodeId,
        round: VsrMembershipRound,
        input: [u8; 32],
        ordinal: u64,
        now: Instant,
    ) {
        if round.coordinator != sender {
            return;
        }
        let old = self.direct.entry(sender).or_insert(Activity {
            round,
            accepted: None,
            chosen: None,
            copy: None,
            changed: now,
        });
        if round < old.round {
            return;
        }
        if round > old.round {
            old.round = round;
            old.copy = None;
        }
        // One input per durable round; an oscillating unrelated descriptor may
        // not keep an otherwise stalled coordinator alive indefinitely.
        if old.copy.is_none() || old.copy.is_some_and(|(id, n)| id == input && ordinal > n) {
            old.copy = Some((input, ordinal));
            old.changed = now;
        }
    }
    pub(super) fn contended(&mut self) -> Result<(), String> {
        self.multiplier = self
            .multiplier
            .max(1)
            .checked_mul(2)
            .ok_or_else(|| "membership recovery contention timer exhausted".to_string())?;
        Ok(())
    }
    pub(super) fn wait_for(
        &self,
        local: NodeId,
        proposal: &VsrMembershipProposal,
        started: Instant,
        base: Duration,
        now: Instant,
    ) -> Result<bool, String> {
        let grace = base
            .checked_mul(self.multiplier.max(1))
            .ok_or_else(|| "membership recovery activity timer overflow".to_string())?;
        let active = self
            .direct
            .iter()
            .filter(|(_, a)| now.saturating_duration_since(a.changed) < grace)
            .max_by_key(|(_, a)| a.round);
        if let Some((coordinator, _)) = active {
            return Ok(*coordinator != local);
        }
        Ok(proposal.new_voters.iter().any(|node| *node > local)
            && now.saturating_duration_since(started) < grace)
    }
}

pub(in super::super) fn grace(
    timeout: Duration,
    timings: &BTreeMap<PlacementGroupId, PeerMembershipRecoveryTiming>,
    max_peers: usize,
) -> Result<Duration, String> {
    let groups = u32::try_from(timings.len()).map_err(|_| "too many managed recovery groups")?;
    let peers = u32::try_from(max_peers)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or("too many managed recovery peers")?;
    let interval = timings
        .values()
        .map(|t| t.interval)
        .max()
        .unwrap_or_default();
    timeout
        .checked_mul(2)
        .and_then(|v| v.checked_add(interval))
        .and_then(|v| v.checked_mul(groups))
        .and_then(|v| v.checked_mul(peers))
        .and_then(|v| v.checked_mul(2))
        .ok_or_else(|| "membership recovery fair-cycle timer overflow".into())
}
