#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{ErrorCode, NodeId, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Simulation {
    seed: u64,
    now: LogicalTime,
    events: Vec<Event>,
}

impl Simulation {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            now: LogicalTime(0),
            events: Vec::new(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn now(&self) -> LogicalTime {
        self.now
    }

    pub fn schedule(&mut self, at: LogicalTime, event: Event) {
        self.events.push(event.at(at));
        self.events.sort_by_key(|event| event.at);
    }

    pub fn next_event(&mut self) -> Option<Event> {
        let event = self.events.first().cloned()?;
        self.events.remove(0);
        self.now = event.at;
        Some(event)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Link {
    pub left: NodeId,
    pub right: NodeId,
}

impl Link {
    pub fn new(a: NodeId, b: NodeId) -> RsfResult<Self> {
        if a == b {
            return Err(invalid("link endpoints must be distinct"));
        }
        Ok(if a < b {
            Self { left: a, right: b }
        } else {
            Self { left: b, right: a }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryDecision {
    Deliver { at: LogicalTime },
    Delay { until: LogicalTime },
    Drop { reason: String },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    crashed: BTreeSet<NodeId>,
    partitions: BTreeSet<Link>,
    delays: BTreeMap<Link, u64>,
}

impl FaultPlan {
    pub fn crash(&mut self, node: NodeId) {
        self.crashed.insert(node);
    }

    pub fn restart(&mut self, node: NodeId) {
        self.crashed.remove(&node);
    }

    pub fn partition(&mut self, a: NodeId, b: NodeId) -> RsfResult<()> {
        self.partitions.insert(Link::new(a, b)?);
        Ok(())
    }

    pub fn heal(&mut self, a: NodeId, b: NodeId) -> RsfResult<()> {
        self.partitions.remove(&Link::new(a, b)?);
        Ok(())
    }

    pub fn delay(&mut self, a: NodeId, b: NodeId, ticks: u64) -> RsfResult<()> {
        if ticks == 0 {
            return Err(invalid("delay ticks must be greater than zero"));
        }
        self.delays.insert(Link::new(a, b)?, ticks);
        Ok(())
    }

    pub fn clear_delay(&mut self, a: NodeId, b: NodeId) -> RsfResult<()> {
        self.delays.remove(&Link::new(a, b)?);
        Ok(())
    }

    pub fn delivery(
        &self,
        from: NodeId,
        to: NodeId,
        now: LogicalTime,
    ) -> RsfResult<DeliveryDecision> {
        let link = Link::new(from, to)?;
        if self.crashed.contains(&from) {
            return Ok(DeliveryDecision::Drop {
                reason: format!("sender node {} is crashed", from.0),
            });
        }
        if self.crashed.contains(&to) {
            return Ok(DeliveryDecision::Drop {
                reason: format!("receiver node {} is crashed", to.0),
            });
        }
        if self.partitions.contains(&link) {
            return Ok(DeliveryDecision::Drop {
                reason: format!("nodes {} and {} are partitioned", from.0, to.0),
            });
        }
        if let Some(delay) = self.delays.get(&link) {
            return Ok(DeliveryDecision::Delay {
                until: LogicalTime(now.0 + delay),
            });
        }
        Ok(DeliveryDecision::Deliver { at: now })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureDrillInput {
    pub fault_events: u8,
    pub dropped_messages: u8,
    pub delayed_messages: u8,
    pub recovery_roots: u8,
    pub recovery_rejections: u8,
    pub provider_degraded: u8,
    pub health_demotions: u8,
    pub rebalance_moves: u8,
    pub rebalance_policy_proofs: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureDrillReport {
    pub passed: bool,
    pub domains_covered: u8,
    pub findings: Vec<String>,
}

impl FailureDrillReport {
    pub fn evaluate(input: &FailureDrillInput) -> Self {
        let checks = [
            (
                input.fault_events >= 5,
                "deterministic-event-schedule",
                format!("fault_events={}", input.fault_events),
            ),
            (
                input.dropped_messages >= 1 && input.delayed_messages >= 1,
                "network-drop-delay",
                format!(
                    "dropped_messages={} delayed_messages={}",
                    input.dropped_messages, input.delayed_messages
                ),
            ),
            (
                input.recovery_roots >= 6 && input.recovery_rejections >= 1,
                "recovery-root-rejection",
                format!(
                    "recovery_roots={} recovery_rejections={}",
                    input.recovery_roots, input.recovery_rejections
                ),
            ),
            (
                input.provider_degraded >= 1,
                "provider-degradation",
                format!("provider_degraded={}", input.provider_degraded),
            ),
            (
                input.health_demotions >= 1,
                "health-demotion",
                format!("health_demotions={}", input.health_demotions),
            ),
            (
                input.rebalance_moves >= 1
                    && input.rebalance_policy_proofs >= input.rebalance_moves,
                "rebalance-policy-preservation",
                format!(
                    "rebalance_moves={} rebalance_policy_proofs={}",
                    input.rebalance_moves, input.rebalance_policy_proofs
                ),
            ),
        ];

        let domains_covered = checks.iter().filter(|(passed, _, _)| *passed).count() as u8;
        let findings = checks
            .iter()
            .map(|(passed, domain, detail)| {
                format!(
                    "{}:{} {}",
                    domain,
                    if *passed { "pass" } else { "fail" },
                    detail
                )
            })
            .collect::<Vec<_>>();

        Self {
            passed: domains_covered == checks.len() as u8,
            domains_covered,
            findings,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalTime(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub at: LogicalTime,
    pub kind: EventKind,
}

impl Event {
    pub fn new(kind: EventKind) -> Self {
        Self {
            at: LogicalTime(0),
            kind,
        }
    }

    fn at(mut self, at: LogicalTime) -> Self {
        self.at = at;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventKind {
    NodeStarted(NodeId),
    NodeCrashed(NodeId),
    NodeRestarted(NodeId),
    DiskWriteCompleted(NodeId),
    DiskWriteTorn(NodeId),
    MessageDelivered { from: NodeId, to: NodeId },
    MessageDropped { from: NodeId, to: NodeId },
    TimerFired(NodeId),
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simulation_delivers_events_in_logical_time_order() {
        let mut sim = Simulation::new(42);
        sim.schedule(
            LogicalTime(10),
            Event::new(EventKind::NodeStarted(NodeId(1))),
        );
        sim.schedule(
            LogicalTime(3),
            Event::new(EventKind::NodeStarted(NodeId(2))),
        );

        assert_eq!(sim.seed(), 42);
        assert_eq!(sim.next_event().unwrap().at, LogicalTime(3));
        assert_eq!(sim.next_event().unwrap().at, LogicalTime(10));
        assert!(sim.next_event().is_none());
    }

    #[test]
    fn fault_plan_drops_messages_across_partition() {
        let mut faults = FaultPlan::default();
        faults.partition(NodeId(1), NodeId(2)).unwrap();

        let decision = faults
            .delivery(NodeId(2), NodeId(1), LogicalTime(7))
            .unwrap();

        assert_eq!(
            decision,
            DeliveryDecision::Drop {
                reason: "nodes 2 and 1 are partitioned".to_string()
            }
        );
    }

    #[test]
    fn fault_plan_delays_messages_deterministically() {
        let mut faults = FaultPlan::default();
        faults.delay(NodeId(1), NodeId(2), 5).unwrap();

        let decision = faults
            .delivery(NodeId(1), NodeId(2), LogicalTime(7))
            .unwrap();

        assert_eq!(
            decision,
            DeliveryDecision::Delay {
                until: LogicalTime(12)
            }
        );
    }

    #[test]
    fn healed_link_delivers_after_partition() {
        let mut faults = FaultPlan::default();
        faults.partition(NodeId(1), NodeId(2)).unwrap();
        faults.heal(NodeId(1), NodeId(2)).unwrap();

        let decision = faults
            .delivery(NodeId(1), NodeId(2), LogicalTime(7))
            .unwrap();

        assert_eq!(decision, DeliveryDecision::Deliver { at: LogicalTime(7) });
    }

    #[test]
    fn crashed_receiver_drops_message() {
        let mut faults = FaultPlan::default();
        faults.crash(NodeId(2));

        let decision = faults
            .delivery(NodeId(1), NodeId(2), LogicalTime(7))
            .unwrap();

        assert_eq!(
            decision,
            DeliveryDecision::Drop {
                reason: "receiver node 2 is crashed".to_string()
            }
        );
    }

    #[test]
    fn failure_drill_requires_all_fault_domains() {
        let report = FailureDrillReport::evaluate(&FailureDrillInput {
            fault_events: 5,
            dropped_messages: 1,
            delayed_messages: 1,
            recovery_roots: 6,
            recovery_rejections: 1,
            provider_degraded: 1,
            health_demotions: 2,
            rebalance_moves: 1,
            rebalance_policy_proofs: 1,
        });

        assert!(report.passed);
        assert_eq!(report.domains_covered, 6);
        assert_eq!(report.findings.len(), 6);
    }
}
