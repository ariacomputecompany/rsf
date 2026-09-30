use std::collections::BTreeMap;

use rsf_core::{Epoch, NodeId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemberRole {
    Learner,
    Voter,
}

impl MemberRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Learner => "learner",
            Self::Voter => "voter",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Member {
    pub node: NodeId,
    pub role: MemberRole,
    pub synced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipView {
    pub epoch: Epoch,
    pub members: Vec<Member>,
}

impl MembershipView {
    pub fn voters(&self) -> usize {
        self.members
            .iter()
            .filter(|member| member.role == MemberRole::Voter)
            .count()
    }

    pub fn quorum_size(&self) -> usize {
        self.voters() / 2 + 1
    }

    pub fn learners(&self) -> usize {
        self.members
            .iter()
            .filter(|member| member.role == MemberRole::Learner)
            .count()
    }

    pub fn voter_nodes(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .filter(|member| member.role == MemberRole::Voter)
            .map(|member| member.node)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuorumIntersection {
    pub previous_voters: usize,
    pub next_voters: usize,
    pub previous_quorum: usize,
    pub next_quorum: usize,
    pub shared_voters: usize,
    pub minimum_intersection: usize,
}

impl QuorumIntersection {
    pub fn between(previous: &MembershipView, next: &MembershipView) -> Self {
        let previous_voters = previous.voter_nodes();
        let next_voters = next.voter_nodes();
        let shared_voters = previous_voters
            .iter()
            .filter(|node| next_voters.contains(node))
            .count();
        let union_voters = previous_voters
            .iter()
            .chain(next_voters.iter())
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let previous_quorum = previous.quorum_size();
        let next_quorum = next.quorum_size();
        let minimum_intersection = previous_quorum
            .saturating_add(next_quorum)
            .saturating_sub(union_voters);

        Self {
            previous_voters: previous_voters.len(),
            next_voters: next_voters.len(),
            previous_quorum,
            next_quorum,
            shared_voters,
            minimum_intersection,
        }
    }

    pub fn preserves_quorum_intersection(self) -> bool {
        self.previous_voters >= 1
            && self.next_voters >= 1
            && self.shared_voters >= self.previous_quorum.min(self.next_quorum)
            && self.minimum_intersection >= 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MembershipDecision {
    AddedLearner(Member),
    MarkedSynced(Member),
    Promoted {
        member: Member,
        epoch: Epoch,
        quorum: QuorumIntersection,
    },
    Removed {
        node: NodeId,
        epoch: Epoch,
        quorum: QuorumIntersection,
    },
    Replaced {
        removed: NodeId,
        replacement: Member,
        epoch: Epoch,
        quorum: QuorumIntersection,
    },
    Rejected {
        node: NodeId,
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct MembershipCell {
    epoch: Epoch,
    members: BTreeMap<NodeId, Member>,
}

impl MembershipCell {
    pub fn new(epoch: Epoch, voters: impl IntoIterator<Item = NodeId>) -> Result<Self, String> {
        let members = voters
            .into_iter()
            .map(|node| {
                (
                    node,
                    Member {
                        node,
                        role: MemberRole::Voter,
                        synced: true,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        if members.is_empty() {
            return Err("membership requires at least one initial voter".to_string());
        }
        Ok(Self { epoch, members })
    }

    pub fn add_learner(&mut self, node: NodeId) -> MembershipDecision {
        if self.members.contains_key(&node) {
            return MembershipDecision::Rejected {
                node,
                reason: "node already exists in membership".to_string(),
            };
        }
        let member = Member {
            node,
            role: MemberRole::Learner,
            synced: false,
        };
        self.members.insert(node, member.clone());
        MembershipDecision::AddedLearner(member)
    }

    pub fn mark_synced(&mut self, node: NodeId) -> MembershipDecision {
        let Some(member) = self.members.get_mut(&node) else {
            return MembershipDecision::Rejected {
                node,
                reason: "node is not a member".to_string(),
            };
        };
        member.synced = true;
        MembershipDecision::MarkedSynced(member.clone())
    }

    pub fn promote(&mut self, node: NodeId) -> MembershipDecision {
        let previous = self.view();
        let Some(member) = self.members.get_mut(&node) else {
            return MembershipDecision::Rejected {
                node,
                reason: "node is not a member".to_string(),
            };
        };
        if member.role != MemberRole::Learner {
            return MembershipDecision::Rejected {
                node,
                reason: "only learners can be promoted".to_string(),
            };
        }
        if !member.synced {
            return MembershipDecision::Rejected {
                node,
                reason: "learner must be synced before promotion".to_string(),
            };
        }
        member.role = MemberRole::Voter;
        let promoted = member.clone();
        self.epoch = Epoch(self.epoch.0 + 1);
        let next = self.view();
        MembershipDecision::Promoted {
            member: promoted,
            epoch: self.epoch,
            quorum: QuorumIntersection::between(&previous, &next),
        }
    }

    pub fn remove_voter(&mut self, node: NodeId) -> MembershipDecision {
        let previous = self.view();
        let Some(member) = self.members.get(&node) else {
            return MembershipDecision::Rejected {
                node,
                reason: "node is not a member".to_string(),
            };
        };
        if member.role != MemberRole::Voter {
            return MembershipDecision::Rejected {
                node,
                reason: "only voters can be removed by voter removal".to_string(),
            };
        }
        let remaining_voters = self.view().voters().saturating_sub(1);
        if remaining_voters < 3 {
            return MembershipDecision::Rejected {
                node,
                reason: "removal would violate minimum voter count".to_string(),
            };
        }
        self.members.remove(&node);
        self.epoch = Epoch(self.epoch.0 + 1);
        let next = self.view();
        MembershipDecision::Removed {
            node,
            epoch: self.epoch,
            quorum: QuorumIntersection::between(&previous, &next),
        }
    }

    pub fn replace_voter(&mut self, removed: NodeId, replacement: NodeId) -> MembershipDecision {
        let previous = self.view();
        let Some(removed_member) = self.members.get(&removed).cloned() else {
            return MembershipDecision::Rejected {
                node: removed,
                reason: "node is not a member".to_string(),
            };
        };
        if removed_member.role != MemberRole::Voter {
            return MembershipDecision::Rejected {
                node: removed,
                reason: "only voters can be replaced".to_string(),
            };
        }
        if removed == replacement {
            return MembershipDecision::Rejected {
                node: replacement,
                reason: "replacement voter must be different from removed voter".to_string(),
            };
        }
        if let Some(existing) = self.members.get(&replacement) {
            if existing.role == MemberRole::Voter {
                return MembershipDecision::Rejected {
                    node: replacement,
                    reason: "replacement node is already a voter".to_string(),
                };
            }
            if !existing.synced {
                return MembershipDecision::Rejected {
                    node: replacement,
                    reason: "replacement learner must be synced".to_string(),
                };
            }
        }

        self.members.remove(&removed);
        let replacement_member = Member {
            node: replacement,
            role: MemberRole::Voter,
            synced: true,
        };
        self.members.insert(replacement, replacement_member.clone());
        self.epoch = Epoch(self.epoch.0 + 1);
        let next = self.view();
        let quorum = QuorumIntersection::between(&previous, &next);
        if !quorum.preserves_quorum_intersection() {
            self.members.remove(&replacement);
            self.members.insert(removed, removed_member);
            self.epoch = previous.epoch;
            return MembershipDecision::Rejected {
                node: replacement,
                reason: "replacement would violate quorum intersection".to_string(),
            };
        }
        MembershipDecision::Replaced {
            removed,
            replacement: replacement_member,
            epoch: self.epoch,
            quorum,
        }
    }

    pub fn view(&self) -> MembershipView {
        MembershipView {
            epoch: self.epoch,
            members: self.members.values().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synced_learner_promotes_and_advances_epoch() {
        let mut cell = MembershipCell::new(Epoch(7), [NodeId(1), NodeId(2), NodeId(3)]).unwrap();

        assert!(matches!(
            cell.add_learner(NodeId(4)),
            MembershipDecision::AddedLearner(_)
        ));
        assert!(matches!(
            cell.mark_synced(NodeId(4)),
            MembershipDecision::MarkedSynced(_)
        ));
        assert!(matches!(
            cell.promote(NodeId(4)),
            MembershipDecision::Promoted {
                epoch: Epoch(8),
                quorum,
                ..
            } if quorum.preserves_quorum_intersection()
        ));
        assert_eq!(cell.view().voters(), 4);
    }

    #[test]
    fn promotion_reports_quorum_intersection_evidence() {
        let mut cell = MembershipCell::new(Epoch(7), [NodeId(1), NodeId(2), NodeId(3)]).unwrap();

        cell.add_learner(NodeId(4));
        cell.mark_synced(NodeId(4));
        let MembershipDecision::Promoted { quorum, .. } = cell.promote(NodeId(4)) else {
            panic!("synced learner should promote");
        };

        assert_eq!(quorum.previous_voters, 3);
        assert_eq!(quorum.next_voters, 4);
        assert_eq!(quorum.previous_quorum, 2);
        assert_eq!(quorum.next_quorum, 3);
        assert_eq!(quorum.shared_voters, 3);
        assert_eq!(quorum.minimum_intersection, 1);
        assert!(quorum.preserves_quorum_intersection());
    }

    #[test]
    fn unsynced_learner_cannot_promote() {
        let mut cell = MembershipCell::new(Epoch(7), [NodeId(1), NodeId(2), NodeId(3)]).unwrap();

        cell.add_learner(NodeId(4));
        assert!(matches!(
            cell.promote(NodeId(4)),
            MembershipDecision::Rejected { .. }
        ));
        assert_eq!(cell.view().epoch, Epoch(7));
    }

    #[test]
    fn removal_preserves_minimum_voter_count() {
        let mut cell = MembershipCell::new(Epoch(7), [NodeId(1), NodeId(2), NodeId(3)]).unwrap();

        assert!(matches!(
            cell.remove_voter(NodeId(1)),
            MembershipDecision::Rejected { .. }
        ));
        assert_eq!(cell.view().voters(), 3);
    }

    #[test]
    fn replacement_preserves_quorum_intersection() {
        let mut cell =
            MembershipCell::new(Epoch(7), [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]).unwrap();

        assert!(matches!(
            cell.replace_voter(NodeId(1), NodeId(5)),
            MembershipDecision::Replaced {
                epoch: Epoch(8),
                quorum,
                ..
            } if quorum.preserves_quorum_intersection()
        ));
        assert_eq!(
            cell.view().voter_nodes(),
            vec![NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
        );
    }
}
