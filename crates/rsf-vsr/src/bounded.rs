//! Strict finite cold recovery of the actual voter owner and active generation.
use super::*;
use crate::installation::publication;
use std::fs;
use std::io;
use std::path::{Component, PathBuf};
mod custody;
mod factory;
pub use factory::{ProvisionalVsrVoter, VsrBootstrapCompletion, VsrBootstrapSpec};
mod marker;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Boundary {
    Replayed,
    BeforeRenewal,
    AfterClosedRenewal,
    Renewed,
    MarkerPrepared,
    MarkerPublished,
}

impl DurableVsrCell {
    /// Open an existing actual voter without unbounded preflight, creation or
    /// WAL repair. Limits apply before physical payload allocation. Installed
    /// and promoted generations retain their existing certificate/role checks.
    /// A valid active generation may finish its exact publication marker; no
    /// missing pointer, bootstrap, manifest, segment or sidecar is initialized.
    /// This is cold recovery, not persistent control policy or client admission.
    pub fn open_existing_bounded(
        path: impl AsRef<Path>,
        pg: PlacementGroupId,
        limits: VsrPrefixLimits,
    ) -> RsfResult<Self> {
        Self::open_existing_bounded_with(path.as_ref(), pg, limits, &mut |_| Ok(()))
    }

    fn open_existing_bounded_with(
        path: &Path,
        pg: PlacementGroupId,
        limits: VsrPrefixLimits,
        hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<Self> {
        let storage_limits = limits.storage()?;
        let admission = limits.admission()?;
        let root = normalized(path)?;
        let mut guard = custody::Guard::new(&root)?;
        let lock_path = root.join(".vsr-writer.lock");
        let writer_lock = guard.lock(&lock_path)?;
        let marker_bytes = publication::published_record(pg);
        if let Some(bytes) =
            guard.optional(&root.join(publication::PUBLISHED), marker_bytes.len())?
        {
            publication::validate_published(&bytes, pg)?;
        }
        // These owner claims affect active()'s absent-pointer semantics even
        // when no selected generation exists. Capture absence as well as data.
        guard.optional(&root.join("VSR_PROMOTION"), 118)?;
        guard.optional(&root.join(".VSR_PROMOTION.tmp"), 118)?;
        let generations = root.join(publication::GENERATIONS);
        match fs::symlink_metadata(&generations) {
            Ok(_) => guard.directory(&generations)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        let active =
            publication::active_with(&root, pg, &mut |path, maximum| guard.read(path, maximum))?;
        guard.directory(&active.path)?;
        guard.validate()?;
        guard.held_lock(&lock_path, &writer_lock)?;
        let options = SegmentStoreOptions::default();
        let (store, recovery) = SegmentStore::open_existing_recovered_bounded(
            &active.path,
            options,
            storage_limits,
            admission,
        )
        .map_err(storage_error)?;
        let custody = store.capture_owned_custody()?;
        guard.validate()?;
        guard.held_lock(&lock_path, &writer_lock)?;
        let replayed = Self::replay_store(&recovery.recovery, pg, || {
            Err(corrupt(
                "bounded existing voter requires persisted bootstrap authority",
            ))
        })?;
        hook(Boundary::Replayed)?;
        guard.validate()?;
        store.validate_owned_custody(&custody)?;
        let durable = Self::from_replayed_store_with(
            OpenedVsrStore {
                root: root.clone(),
                active,
                store,
                recovery: recovery.recovery,
                options,
                writer_lock,
            },
            pg,
            replayed,
            |root, active, store| {
                guard.validate()?;
                store.validate_owned_custody(&custody)?;
                // Complete visible records may survive an earlier failed data
                // barrier. Renew the actual bounded retained files after all
                // replay/seal/promotion checks, before any recovered authority
                // or marker success. Neither renewal scans or repairs WAL.
                hook(Boundary::BeforeRenewal)?;
                store.sync_owned_closed_segments(&custody)?;
                hook(Boundary::AfterClosedRenewal)?;
                store.sync_owned_active_segment(&custody)?;
                store.validate_owned_custody(&custody)?;
                hook(Boundary::Renewed)?;
                guard.validate()?;
                store.validate_owned_custody(&custody)?;
                if active.plan.is_some() {
                    marker::finish(
                        root,
                        pg,
                        &mut guard,
                        || store.validate_owned_custody(&custody),
                        hook,
                    )?;
                }
                guard.validate()?;
                store.validate_owned_custody(&custody)
            },
        )?;
        guard.validate()?;
        guard.held_lock(&lock_path, &durable._writer_lock)?;
        durable.store.validate_owned_custody(&custody)?;
        durable.recovery_state_token()?;
        Ok(durable)
    }
}
fn normalized(path: &Path) -> RsfResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(io_error)?.join(path)
    };
    let mut root = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(invalid_input(
                    "bounded voter path contains parent traversal",
                ));
            }
            component => root.push(component.as_os_str()),
        }
    }
    Ok(root)
}
fn io_error(error: io::Error) -> RsfError {
    corrupt(format!("bounded voter custody: {error}"))
}
fn storage_error(error: rsf_storage::SegmentReadError) -> RsfError {
    match error {
        rsf_storage::SegmentReadError::LimitExceeded { kind, .. } => {
            invalid_input(format!("bounded voter physical budget exceeded: {kind:?}"))
        }
        rsf_storage::SegmentReadError::CompactedPrefixUnsupported => {
            invalid_input("bounded voter recovery requires uncompacted history")
        }
        rsf_storage::SegmentReadError::Failure(error) => error,
    }
}
#[cfg(test)]
pub(crate) mod tests;
