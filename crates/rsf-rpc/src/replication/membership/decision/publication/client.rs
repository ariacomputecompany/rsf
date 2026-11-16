use super::*;

impl NetworkVsrTransport {
    pub fn publish_membership(
        &self,
        replica: NodeId,
        quorum: &VsrMembershipStagedQuorum,
    ) -> Result<VsrMembershipPublishedReport, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        if !quorum.proposal().new_voters.contains(&self.local)
            || !quorum.proposal().new_voters.contains(&replica)
        {
            return Err(fail(
                "membership publication requires proposed-voter endpoints".into(),
            ));
        }
        let payload = quorum.encode().map_err(|e| fail(e.message))?;
        let response = self
            .exchange_io(replica, MEMBERSHIP_PUBLISH, &payload)
            .map_err(|e| fail(e.to_string()))?;
        let report =
            VsrMembershipPublishedReport::decode(&response).map_err(|e| fail(e.message))?;
        check_published(&report, replica, quorum).map_err(fail)?;
        Ok(report)
    }

    pub fn query_membership_publication(
        &self,
        replica: NodeId,
        pg: PlacementGroupId,
        plan: [u8; 32],
    ) -> Result<VsrMembershipPublishedReport, VsrTransportError> {
        let fail = |reason: String| VsrTransportError { replica, reason };
        let response = self
            .exchange_io(replica, MEMBERSHIP_PUBLICATION, &input_query(pg, plan))
            .map_err(|e| fail(e.to_string()))?;
        let report =
            VsrMembershipPublishedReport::decode(&response).map_err(|e| fail(e.message))?;
        if report.replica != replica
            || report.proposal.election.pg != pg
            || report.plan != plan
            || !report.proposal.new_voters.contains(&self.local)
        {
            return Err(fail(
                "membership publication query returned another reporter or transition".into(),
            ));
        }
        Ok(report)
    }

    /// The caller's local report must follow actual local publication and trusted
    /// current-prefix materialization. Remote reports are authenticated over TLS.
    /// Success is historical publication evidence, not client admission.
    pub fn collect_membership_publications(
        &self,
        local: VsrMembershipPublishedReport,
        quorum: &VsrMembershipStagedQuorum,
    ) -> Result<VsrMembershipPublishedQuorum, VsrTransportError> {
        let fail = |reason: String| VsrTransportError {
            replica: self.local,
            reason,
        };
        check_published(&local, self.local, quorum).map_err(fail)?;
        let mut reports = vec![local];
        let mut failures = Vec::new();
        for replica in quorum
            .proposal()
            .new_voters
            .iter()
            .copied()
            .filter(|n| *n != self.local)
        {
            match self.publish_membership(replica, quorum) {
                Ok(report) => reports.push(report),
                Err(error) => failures.push(format!("{}: {}", replica.0, error.reason)),
            }
        }
        VsrMembershipPublishedQuorum::from_reports(reports)
            .map_err(|e| fail(format!("{}: {}", e.message, failures.join("; "))))
    }
}
