//! Authenticated historical publication evidence. First publication retains
//! recovery input before changing authority; retries never replace newer history.
use super::*;
use rsf_vsr::{
    VsrMembershipPublishedQuorum, VsrMembershipPublishedReport, VsrMembershipStagedQuorum,
};
mod client;
mod server;

fn check_published(
    report: &VsrMembershipPublishedReport,
    replica: NodeId,
    quorum: &VsrMembershipStagedQuorum,
) -> Result<(), String> {
    report.encode().map_err(|e| e.message)?;
    if report.replica != replica
        || report.proposal != *quorum.proposal()
        || report.input != quorum.input_id()
        || report.plan != quorum.plan_id()
    {
        return Err(
            "membership publication differs from authenticated reporter or canonical transition"
                .into(),
        );
    }
    Ok(())
}
