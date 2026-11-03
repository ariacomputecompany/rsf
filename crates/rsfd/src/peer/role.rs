//! Recover runtime role from certified local publication, independently of a
//! stale bootstrap flag. Partial claims retain only configured learner custody.
use super::*;

pub(super) enum HostedRole {
    Voter(Box<DurableVsrCell>),
    Learner(Box<rsf_vsr::LearnerHistoryStore>),
}

pub(super) fn open(state: &Path, local: NodeId, config: CellConfig) -> Result<HostedRole, String> {
    let pg = PlacementGroupId(config.pg);
    let voter = state.join(format!("pg-{}", config.pg));
    let learner = state.join(format!("learner-{}", config.pg));
    if let Some(source) = config.learner_source {
        if present(&voter)? {
            match DurableVsrCell::open_existing(&voter, pg) {
                Ok(cell) => {
                    return certified(cell, local).map(|cell| HostedRole::Voter(Box::new(cell)));
                }
                Err(original) => {
                    if !present(&learner)? {
                        return Err(original.message);
                    }
                    let learner =
                        rsf_vsr::LearnerHistoryStore::open(&learner, pg, local, NodeId(source))
                            .map_err(|e| e.message)?;
                    if learner.promotion_pending(&voter).map_err(|e| e.message)? {
                        return Ok(HostedRole::Learner(Box::new(learner)));
                    }
                    return Err(original.message);
                }
            }
        }
        return rsf_vsr::LearnerHistoryStore::open(learner, pg, local, NodeId(source))
            .map(|learner| HostedRole::Learner(Box::new(learner)))
            .map_err(|e| e.message);
    }
    if present(&learner)? {
        let cell = DurableVsrCell::open_existing(voter, pg).map_err(|e| {
            format!(
                "learner promotion requires certified membership installation: {}",
                e.message
            )
        })?;
        return certified(cell, local).map(|cell| HostedRole::Voter(Box::new(cell)));
    }
    DurableVsrCell::open(
        voter,
        pg,
        Epoch(config.view),
        NodeId(config.primary),
        config.voters.into_iter().map(NodeId),
    )
    .map(|cell| HostedRole::Voter(Box::new(cell)))
    .map_err(|e| e.message)
}

fn certified(cell: DurableVsrCell, local: NodeId) -> Result<DurableVsrCell, String> {
    if cell.promoted_replica().map_err(|e| e.message)? != Some(local)
        || !cell.contains_replica(local)
    {
        return Err(
            "learner promotion requires certified membership for this configured voter".into(),
        );
    }
    Ok(cell)
}
fn present(path: &Path) -> Result<bool, String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(format!("cannot inspect peer role storage: {e}")),
    }
}
