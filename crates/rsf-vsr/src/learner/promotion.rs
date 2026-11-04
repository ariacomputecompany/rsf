//! Promotion publishes complete activated history into a separately owned voter
//! target. No partially initialized old-configuration cell is ever returned.
use super::*;
use crate::installation::publication;
use crate::membership::staging::activation;
mod identity;
mod path;
use identity::Identity;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Boundary {
    Owned,
    Staged,
    Renamed,
    Published,
}

impl LearnerHistoryStore {
    /// Inspect an owned, unpublished target while retaining read-only learner
    /// custody. False never grants voting authority: complete targets still
    /// require authoritative open and configured-owner validation.
    pub fn promotion_pending(&self, target: impl AsRef<Path>) -> RsfResult<bool> {
        self.healthy()?;
        let target = target.as_ref();
        let Some(owner) = Identity::read(target)? else {
            return Ok(false);
        };
        let anchor = self.published.as_ref().ok_or_else(|| {
            invalid_input("pending promotion requires a published learner anchor")
        })?;
        if owner.pg != self.pg
            || owner.local != self.local
            || owner.bootstrap != Digest::blake3_bytes(&anchor.cell.initial_bootstrap).bytes
        {
            return Err(corrupt(
                "pending promotion differs from configured learner authority",
            ));
        }
        match fs::symlink_metadata(target.join("VSR_ACTIVE")) {
            Ok(_) => Ok(false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(target.join("VSR_PUBLISHED")) {
                    Ok(_) => Err(corrupt("published promotion lost its active pointer")),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
                    Err(e) => Err(io_error(e)),
                }
            }
            Err(e) => Err(io_error(e)),
        }
    }

    /// Promote this configured new voter using its boot-materialized chosen
    /// stage and authenticated new-quorum staging proof. Callers must retain
    /// role custody and obtain publication quorum before admitting clients.
    pub fn promote_membership(
        &self,
        target: impl AsRef<Path>,
        stage: &MembershipStagingStore,
        quorum: &VsrMembershipStagedQuorum,
    ) -> RsfResult<DurableVsrCell> {
        self.promote_membership_with_hook(target.as_ref(), stage, quorum, &mut |_| Ok(()))
    }

    pub(crate) fn promote_membership_with_hook(
        &self,
        target: &Path,
        stage: &MembershipStagingStore,
        quorum: &VsrMembershipStagedQuorum,
        hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<DurableVsrCell> {
        self.healthy()?;
        let published = self
            .published
            .as_ref()
            .ok_or_else(|| invalid_input("learner has no published authority anchor"))?;
        let report = stage.report()?;
        let proposal = &report.proposal;
        if report.replica != self.local
            || proposal.election.pg != self.pg
            || proposal.election.voters.contains(&self.local)
            || !proposal.new_voters.contains(&self.local)
        {
            return Err(invalid_input(
                "promotion requires this configured newly added learner",
            ));
        }
        stage.validate_anchor(&published.cell)?;
        let records = activation::publication_records(stage, quorum)?;
        installation::recover_records(&records)?;
        let identity = Identity {
            pg: self.pg,
            local: self.local,
            plan: stage.plan().id(),
            bootstrap: Digest::blake3_bytes(&published.cell.initial_bootstrap).bytes,
        };
        let root = path::target(target, &self.root)?;
        let writer_lock = path::lock(&root)?;
        identity.claim(&root)?;
        hook(Boundary::Owned)?;
        stage.validate_custody()?;
        let needs_publication = match fs::symlink_metadata(root.join("VSR_ACTIVE")) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if fs::symlink_metadata(root.join("VSR_PUBLISHED")).is_ok() {
                    return Err(corrupt("published promotion lost its active pointer"));
                }
                true
            }
            Err(e) => return Err(io_error(e)),
        };
        if needs_publication {
            let mut started = false;
            let store = publication::publish(
                &root,
                self.pg,
                SegmentStoreOptions::default(),
                identity.plan,
                &records,
                &mut |boundary| {
                    hook(match boundary {
                        publication::Boundary::Staged => Boundary::Staged,
                        publication::Boundary::Renamed => Boundary::Renamed,
                        publication::Boundary::Published => Boundary::Published,
                    })?;
                    stage.validate_custody()
                },
                &mut started,
            )?;
            drop(store);
        }
        // Keep the same exclusive writer lock while the normal constructor
        // validates authoritative replay and promotion provenance. Existing
        // targets may contain later commits/views; never republish an old stage.
        let bootstrap = decode_bootstrap(&published.cell.initial_bootstrap)?;
        let cell = DurableVsrCell::open_locked(
            &root,
            self.pg,
            bootstrap.view,
            bootstrap.primary,
            bootstrap.replicas,
            SegmentStoreOptions::default(),
            writer_lock,
        )?;
        stage.validate_custody()?;
        if !cell.contains_replica(self.local) || !activation::retains_stage(&cell, stage) {
            return Err(corrupt(
                "promoted target no longer retains this voter and staged prefix",
            ));
        }
        Ok(cell)
    }
}

impl DurableVsrCell {
    /// Original promoted-node identity, bound to the active pointer and replayed
    /// activation proof. Native role admission must match this configured node.
    pub fn promoted_replica(&self) -> RsfResult<Option<NodeId>> {
        self.ensure_healthy()?;
        publication::active(&self.root, self.cell.pg)?;
        Ok(Identity::read(&self.root)?.map(|i| i.local))
    }
}

pub(crate) fn has_claim(root: &Path) -> bool {
    [identity::NAME, identity::TEMPORARY]
        .iter()
        .any(|name| fs::symlink_metadata(root.join(name)).is_ok())
}

pub(crate) fn owner_digest(root: &Path) -> RsfResult<Option<[u8; 32]>> {
    Ok(Identity::read(root)?.map(|i| i.digest()))
}

pub(crate) fn owner_digest_with(
    root: &Path,
    read: &mut impl FnMut(&Path, usize) -> std::io::Result<Vec<u8>>,
) -> RsfResult<Option<[u8; 32]>> {
    Ok(Identity::read_with(root, read)?.map(|identity| identity.digest()))
}

pub(crate) fn validate_history<'a>(
    root: &Path,
    pg: PlacementGroupId,
    records: impl IntoIterator<Item = (WalRecordKind, &'a [u8])>,
) -> RsfResult<()> {
    let Some(identity) = Identity::read(root)? else {
        return Ok(());
    };
    let mut records = records.into_iter();
    if identity.pg != pg
        || records.next().is_none_or(|(kind, payload)| {
            kind != WalRecordKind::Membership
                || Digest::blake3_bytes(payload).bytes != identity.bootstrap
        })
    {
        return Err(corrupt("promotion owner differs from original bootstrap"));
    }
    for (kind, payload) in records {
        if kind == WalRecordKind::ViewInstallation
            && activation::matches_promotion_plan(payload, identity.local, identity.plan)?
        {
            return Ok(());
        }
    }
    Err(corrupt(
        "promotion owner has no matching newly added voter activation",
    ))
}
