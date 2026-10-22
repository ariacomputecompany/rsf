use super::*;

/// Canonical, bounded descriptor for all replay-validated old-quorum histories.
/// Its ID identifies an exact installation input, not just the voter proposal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsrMembershipInput {
    reports: Vec<VsrMembershipReport>,
}
impl VsrMembershipInput {
    pub fn from_approval(approval: &VsrMembershipApproval) -> RsfResult<Self> {
        let mut reports = approval
            .snapshots()
            .iter()
            .map(|s| s.report.clone())
            .collect::<Vec<_>>();
        reports.sort_by_key(|r| r.voter.replica);
        let input = Self { reports };
        input.encode()?;
        Ok(input)
    }
    pub fn proposal(&self) -> &VsrMembershipProposal {
        &self.reports[0].proposal
    }
    pub fn reports(&self) -> &[VsrMembershipReport] {
        &self.reports
    }
    pub fn records(&self) -> RsfResult<u64> {
        self.reports.iter().try_fold(0u64, |count, r| {
            count
                .checked_add(r.voter.history.records)
                .ok_or_else(|| invalid_input("membership input record count overflow"))
        })
    }
    pub fn id(&self) -> RsfResult<[u8; 32]> {
        Ok(Digest::blake3_bytes(&self.encode()?).bytes)
    }
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let first = self
            .reports
            .first()
            .ok_or_else(|| invalid_input("membership input has no reports"))?;
        if self.reports.len() <= first.proposal.election.voters.len() / 2
            || self.reports.len() > 4096
            || self
                .reports
                .windows(2)
                .any(|r| r[0].voter.replica >= r[1].voter.replica)
        {
            return Err(invalid_input(
                "membership input needs a canonical old quorum",
            ));
        }
        let mut out = b"VSRMDI1".to_vec();
        put_u64(&mut out, self.reports.len() as u64);
        for report in &self.reports {
            if report.proposal != first.proposal {
                return Err(invalid_input("membership input has mixed proposals"));
            }
            put_bytes(&mut out, &report.encode()?)?;
            if out.len() > MAX_HISTORY_RECORD_BYTES {
                return Err(invalid_input("membership input descriptor exceeds limit"));
            }
        }
        self.records()?;
        Ok(out)
    }
    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        let mut r = Reader::new(bytes, b"VSRMDI1")?;
        let count = r.u64()?;
        if count > 4096 {
            return Err(corrupt("too many membership input reports"));
        }
        let mut reports = Vec::new();
        for _ in 0..count {
            reports.push(VsrMembershipReport::decode(r.bytes()?)?);
        }
        r.finish()?;
        let input = Self { reports };
        if input.encode()? != bytes {
            return Err(corrupt("noncanonical membership input"));
        }
        Ok(input)
    }
    pub fn flattened_records(
        &self,
        approval: &VsrMembershipApproval,
    ) -> RsfResult<Vec<VsrHistoryRecord>> {
        if Self::from_approval(approval)? != *self {
            return Err(invalid_input("membership input differs from approval"));
        }
        let mut records = Vec::new();
        for report in &self.reports {
            let snapshot = approval
                .snapshots()
                .iter()
                .find(|s| s.report.voter.replica == report.voter.replica)
                .ok_or_else(|| corrupt("membership input report disappeared"))?;
            records.extend(snapshot.records.iter().cloned());
        }
        Ok(records)
    }
    pub fn validate_records(
        &self,
        records: &[VsrHistoryRecord],
    ) -> RsfResult<VsrMembershipApproval> {
        self.encode()?;
        if records.len() as u64 != self.records()? {
            return Err(corrupt("membership input is incomplete"));
        }
        let mut at = 0usize;
        let mut snapshots = Vec::new();
        for report in &self.reports {
            let count = usize::try_from(report.voter.history.records)
                .map_err(|_| corrupt("membership history exceeds platform range"))?;
            let end = at
                .checked_add(count)
                .ok_or_else(|| corrupt("membership history length overflow"))?;
            let history = records
                .get(at..end)
                .ok_or_else(|| corrupt("truncated membership input"))?;
            snapshots.push(VsrMembershipSnapshot {
                report: report.clone(),
                records: history.to_vec(),
            });
            at = end;
        }
        VsrMembershipApproval::from_snapshots(snapshots)
    }
}
