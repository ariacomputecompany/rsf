use super::*;
/// Staged, materialized prefix receipt. Authenticate its node and cluster before
/// counting it. It is not an application vote or independent node signature.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipStagedReport {
    pub proposal: VsrMembershipProposal,
    pub replica: NodeId,
    pub input: [u8; 32],
    pub plan: [u8; 32],
    pub stage: [u8; 32],
    pub committed_tail: LogIndex,
    pub accepted_tail: LogIndex,
}
impl VsrMembershipStagedReport {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.proposal.encode()?;
        if !self.proposal.new_voters.contains(&self.replica)
            || self.committed_tail > self.accepted_tail
        {
            return Err(invalid_input(
                "invalid staged membership reporter or prefix",
            ));
        }
        let mut out = b"VSRMSR1".to_vec();
        put_bytes(&mut out, &self.proposal.encode()?)?;
        put_u64(&mut out, self.replica.0);
        for id in [self.input, self.plan, self.stage] {
            out.extend_from_slice(&id);
        }
        put_u64(&mut out, self.committed_tail.0);
        put_u64(&mut out, self.accepted_tail.0);
        if out.len() > MAX_HISTORY_RECORD_BYTES {
            return Err(invalid_input("staged membership report exceeds limit"));
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMSR1")?;
        let value = Self {
            proposal: VsrMembershipProposal::decode(r.bytes()?)?,
            replica: NodeId(r.u64()?),
            input: r.id()?,
            plan: r.id()?,
            stage: r.id()?,
            committed_tail: LogIndex(r.u64()?),
            accepted_tail: LogIndex(r.u64()?),
        };
        r.finish()?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical staged membership report"));
        }
        Ok(value)
    }
}
/// A new majority has durably staged and materialized the same chosen prefix.
/// This proof is input for active publication, not permission to serve clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipStagedQuorum {
    reports: Vec<VsrMembershipStagedReport>,
}
impl VsrMembershipStagedQuorum {
    pub fn from_reports(mut reports: Vec<VsrMembershipStagedReport>) -> RsfResult<Self> {
        reports.sort_by_key(|r| r.replica);
        let first = reports
            .first()
            .ok_or_else(|| invalid_input("membership staging quorum has no reports"))?;
        for report in &reports {
            report.encode()?;
            if report.proposal != first.proposal
                || report.input != first.input
                || report.plan != first.plan
                || report.committed_tail != first.committed_tail
                || report.accepted_tail != first.accepted_tail
            {
                return Err(invalid_input(
                    "membership staging reports disagree on authority or chosen prefix",
                ));
            }
        }
        if reports.len() <= first.proposal.new_voters.len() / 2
            || reports.windows(2).any(|r| r[0].replica == r[1].replica)
        {
            return Err(invalid_input(
                "membership staging lacks a distinct new majority",
            ));
        }
        Ok(Self { reports })
    }
    pub fn reports(&self) -> &[VsrMembershipStagedReport] {
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
        let mut out = b"VSRMSQ1".to_vec();
        put_u64(&mut out, self.reports.len() as u64);
        for report in &self.reports {
            put_bytes(&mut out, &report.encode()?)?;
            if out.len() > MAX_HISTORY_RECORD_BYTES {
                return Err(invalid_input("membership staging quorum exceeds limit"));
            }
        }
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMSQ1")?;
        let count = r.u64()?;
        if count > 4096 {
            return Err(corrupt("too many membership staged reports"));
        }
        let mut reports = Vec::new();
        for _ in 0..count {
            reports.push(VsrMembershipStagedReport::decode(r.bytes()?)?);
        }
        r.finish()?;
        let value = Self::from_reports(reports)?;
        if value.encode()? != bytes {
            return Err(corrupt("noncanonical membership staging quorum"));
        }
        Ok(value)
    }
}
