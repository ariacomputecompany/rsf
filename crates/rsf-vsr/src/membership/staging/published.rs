//! Historical publication receipts have a stable canonical identity across
//! ordinary elections. They are neither current-primary leases nor admission.
use super::*;

/// Authenticate the reporter and cluster, and require trusted application
/// materialization before emitting or counting this crash-fault evidence.
/// Encoding validates its shape; it does not prove the sender published it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipPublishedReport {
    pub proposal: VsrMembershipProposal,
    pub replica: NodeId,
    pub input: [u8; 32],
    pub plan: [u8; 32],
}
impl VsrMembershipPublishedReport {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let proposal = self.proposal.encode()?;
        if !self.proposal.new_voters.contains(&self.replica) {
            return Err(invalid_input(
                "membership publication reporter is not a proposed voter",
            ));
        }
        let mut out = b"VSRMPR1".to_vec();
        put_bytes(&mut out, &proposal)?;
        put_u64(&mut out, self.replica.0);
        out.extend_from_slice(&self.input);
        out.extend_from_slice(&self.plan);
        if out.len() > MAX_HISTORY_RECORD_BYTES {
            return Err(invalid_input("membership publication report exceeds limit"));
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMPR1")?;
        let value = Self {
            proposal: VsrMembershipProposal::decode(r.bytes()?)?,
            replica: NodeId(r.u64()?),
            input: r.id()?,
            plan: r.id()?,
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership publication report"));
        }
        Ok(value)
    }
}

/// A distinct new majority acknowledged the same historical publication.
/// Later views, pending promises and receiver-local stage suffixes need not
/// match. Current voting roles and replicated client admission are separate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipPublishedQuorum {
    reports: Vec<VsrMembershipPublishedReport>,
}
impl VsrMembershipPublishedQuorum {
    pub fn from_reports(mut reports: Vec<VsrMembershipPublishedReport>) -> RsfResult<Self> {
        reports.sort_by_key(|report| report.replica);
        let first = reports
            .first()
            .ok_or_else(|| invalid_input("membership publication quorum has no reports"))?;
        for report in &reports {
            report.encode()?;
            if report.proposal != first.proposal
                || report.input != first.input
                || report.plan != first.plan
            {
                return Err(invalid_input(
                    "membership publication reports disagree on canonical authority",
                ));
            }
        }
        if reports.len() <= first.proposal.new_voters.len() / 2
            || reports
                .windows(2)
                .any(|pair| pair[0].replica == pair[1].replica)
        {
            return Err(invalid_input(
                "membership publication lacks a distinct new majority",
            ));
        }
        Ok(Self { reports })
    }
    pub fn reports(&self) -> &[VsrMembershipPublishedReport] {
        &self.reports
    }
    pub fn proposal(&self) -> &VsrMembershipProposal {
        &self.reports[0].proposal
    }
    pub fn input_id(&self) -> [u8; 32] {
        self.reports[0].input
    }
    pub fn plan_id(&self) -> [u8; 32] {
        self.reports[0].plan
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut out = b"VSRMPQ1".to_vec();
        put_u64(&mut out, self.reports.len() as u64);
        for report in &self.reports {
            put_bytes(&mut out, &report.encode()?)?;
            if out.len() > MAX_HISTORY_RECORD_BYTES {
                return Err(invalid_input("membership publication quorum exceeds limit"));
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMPQ1")?;
        let count = r.u64()?;
        if count > 4096 {
            return Err(corrupt("too many membership publication reports"));
        }
        let mut reports = Vec::new();
        for _ in 0..count {
            reports.push(VsrMembershipPublishedReport::decode(r.bytes()?)?);
        }
        r.finish()?;
        let value = Self::from_reports(reports)?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership publication quorum"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(replica: u64, voters: u64) -> VsrMembershipPublishedReport {
        VsrMembershipPublishedReport {
            proposal: VsrMembershipProposal {
                election: VsrViewChangeRequest {
                    pg: PlacementGroupId(1),
                    previous_view: Epoch(1),
                    target_view: Epoch(2),
                    candidate: NodeId(1),
                    voters: (0..2).map(NodeId).collect(),
                },
                old_primary: NodeId(0),
                new_voters: (0..voters).map(NodeId).collect(),
            },
            replica: NodeId(replica),
            input: [7; 32],
            plan: [9; 32],
        }
    }

    #[test]
    fn publication_quorums_require_distinct_majorities_of_one_canonical_transition() {
        for voters in [3, 5] {
            for mask in 0_u32..(1 << voters) {
                let reports = (0..voters)
                    .filter(|node| mask & (1 << node) != 0)
                    .map(|node| report(node, voters))
                    .collect::<Vec<_>>();
                let result = VsrMembershipPublishedQuorum::from_reports(reports.clone());
                assert_eq!(result.is_ok(), reports.len() > voters as usize / 2);
                if let Ok(quorum) = result {
                    let mut reverse = reports.clone();
                    reverse.reverse();
                    assert_eq!(
                        VsrMembershipPublishedQuorum::from_reports(reverse).unwrap(),
                        quorum
                    );
                    let bytes = quorum.encode().unwrap();
                    assert_eq!(
                        VsrMembershipPublishedQuorum::decode(&bytes).unwrap(),
                        quorum
                    );
                    let mut duplicate = reports.clone();
                    duplicate.push(reports[0].clone());
                    assert!(VsrMembershipPublishedQuorum::from_reports(duplicate).is_err());
                    for change in 0..4 {
                        let mut mixed = reports.clone();
                        match change {
                            0 => mixed[0].input[0] ^= 1,
                            1 => mixed[0].plan[0] ^= 1,
                            2 => mixed[0].proposal.election.pg = PlacementGroupId(2),
                            _ => mixed[0].replica = NodeId(voters),
                        }
                        assert!(VsrMembershipPublishedQuorum::from_reports(mixed).is_err());
                    }
                }
            }
        }
    }

    #[test]
    fn publication_codecs_reject_truncation_extra_bytes_and_noncanonical_order() {
        let first = report(0, 3);
        let second = report(1, 3);
        let report_bytes = first.encode().unwrap();
        assert_eq!(
            VsrMembershipPublishedReport::decode(&report_bytes).unwrap(),
            first
        );
        for end in 0..report_bytes.len() {
            assert!(VsrMembershipPublishedReport::decode(&report_bytes[..end]).is_err());
        }
        let quorum =
            VsrMembershipPublishedQuorum::from_reports(vec![first.clone(), second.clone()])
                .unwrap();
        let bytes = quorum.encode().unwrap();
        for end in 0..bytes.len() {
            assert!(VsrMembershipPublishedQuorum::decode(&bytes[..end]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(VsrMembershipPublishedQuorum::decode(&trailing).is_err());
        let mut reversed = b"VSRMPQ1".to_vec();
        put_u64(&mut reversed, 2);
        put_bytes(&mut reversed, &second.encode().unwrap()).unwrap();
        put_bytes(&mut reversed, &first.encode().unwrap()).unwrap();
        assert!(VsrMembershipPublishedQuorum::decode(&reversed).is_err());
        let mut huge = b"VSRMPQ1".to_vec();
        put_u64(&mut huge, u64::MAX);
        assert!(VsrMembershipPublishedQuorum::decode(&huge).is_err());
        assert!(VsrMembershipPublishedQuorum::decode(&report_bytes).is_err());
        assert!(VsrMembershipPublishedReport::decode(&quorum.encode().unwrap()).is_err());
    }
}
