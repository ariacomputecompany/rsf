//! Recover runtime role from certified local publication, independently of a
//! stale bootstrap flag. Partial claims retain only configured learner custody.
use super::*;

pub(super) enum HostedRole {
    Voter(Box<DurableVsrCell>),
    Learner(Box<rsf_vsr::LearnerHistoryStore>),
}

pub(super) fn open(state: &Path, local: NodeId, config: CellConfig) -> Result<HostedRole, String> {
    open_inner(state, local, config, false)
}

/// Managed recovery must never create a replacement bootstrap or learner after
/// losing its authority anchor. The kernel still owns complete proof replay.
pub(super) fn open_existing(
    state: &Path,
    local: NodeId,
    config: CellConfig,
) -> Result<HostedRole, String> {
    for name in [
        format!("pg-{}", config.pg),
        format!("learner-{}", config.pg),
    ] {
        match std::fs::symlink_metadata(state.join(name)) {
            Ok(metadata) if metadata.is_dir() => {}
            Ok(_) => {
                return Err("managed role custody requires local directories, not symlinks".into());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("cannot inspect managed role custody: {error}")),
        }
    }
    open_inner(state, local, config, true)
}

fn open_inner(
    state: &Path,
    local: NodeId,
    config: CellConfig,
    existing: bool,
) -> Result<HostedRole, String> {
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
                    let learner = open_learner(&learner, pg, local, NodeId(source), existing)?;
                    if learner.promotion_pending(&voter).map_err(|e| e.message)? {
                        return Ok(HostedRole::Learner(Box::new(learner)));
                    }
                    return Err(original.message);
                }
            }
        }
        return open_learner(&learner, pg, local, NodeId(source), existing)
            .map(|learner| HostedRole::Learner(Box::new(learner)));
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
    if existing {
        return DurableVsrCell::open_existing(voter, pg)
            .map(|cell| HostedRole::Voter(Box::new(cell)))
            .map_err(|error| error.message);
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

fn open_learner(
    root: &Path,
    pg: PlacementGroupId,
    local: NodeId,
    source: NodeId,
    existing: bool,
) -> Result<rsf_vsr::LearnerHistoryStore, String> {
    if existing {
        for path in [root.join("IDENTITY"), root.join("ACTIVE")] {
            if !std::fs::symlink_metadata(&path)
                .map_err(|error| format!("managed learner custody is missing: {error}"))?
                .is_file()
            {
                return Err(
                    "managed learner custody requires regular identity and active files".into(),
                );
            }
        }
    }
    rsf_vsr::LearnerHistoryStore::open(root, pg, local, source).map_err(|error| error.message)
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
