//! Retained source ownership for the two existing history-transfer layouts.
//! Native markers always select native custody; errors never select a fallback.
use super::state_identity::{Reservation, StateIdentity};
use rsf_core::{ClusterId, NodeId, PlacementGroupId};
use std::path::{Path, PathBuf};
mod directory;
mod migration;
use directory::{Directory, normalized};
pub(super) use migration::run_cli as migrate_application_identity_cli;

#[derive(Debug)]
pub(super) struct SourceOwner {
    identity: StateIdentity,
    application: Option<Application>,
    failed: bool,
}

#[derive(Debug)]
struct Application {
    parent: Directory,
    source: Directory,
}

fn has_native_marker(root: &Path) -> Result<bool, String> {
    let mut present = false;
    for name in ["IDENTITY.json", "IDENTITY.pending", ".peer.lock"] {
        match std::fs::symlink_metadata(root.join(name)) {
            Ok(_) => present = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err("cannot inspect history source native markers".into()),
        }
    }
    Ok(present)
}

impl Application {
    fn capture(root: &Path) -> Result<Self, String> {
        if root.file_name() != Some(std::ffi::OsStr::new("protocol.vsr")) {
            return Err(
                "history source requires native identity or the daemon protocol.vsr route".into(),
            );
        }
        let parent = root.parent().ok_or("daemon history source has no parent")?;
        let result = Self {
            parent: Directory::capture(parent)?,
            source: Directory::capture(root)?,
        };
        result.validate()?;
        Ok(result)
    }
    fn binding(&self) -> PathBuf {
        self.parent.path().join("replication-identity")
    }
    fn validate(&self) -> Result<(), String> {
        self.parent.validate()?;
        self.source.validate()?;
        if has_native_marker(self.source.path())? {
            return Err("daemon history source acquired native markers".into());
        }
        Ok(())
    }
}

impl SourceOwner {
    pub(super) fn open(
        root: &Path,
        cluster: &ClusterId,
        node: NodeId,
        pg: PlacementGroupId,
        reservation: Option<Reservation>,
    ) -> Result<Self, String> {
        let root = normalized(root)?;
        // A supplied control policy or any native marker is an affirmative
        // native classification, including corrupt, pending-only or lock-only
        // states. No error from that branch can enter the application branch.
        let application = if reservation.is_some() || has_native_marker(&root)? {
            None
        } else {
            Some(Application::capture(&root)?)
        };
        let binding = application
            .as_ref()
            .map_or_else(|| root.clone(), Application::binding);
        let mut identity = StateIdentity::open_existing(&binding, cluster, node, reservation)?;
        if application.is_some() {
            identity.require_unreserved()?;
        }
        identity.require_unreserved_pg(pg)?;
        let mut owner = Self {
            identity,
            application,
            failed: false,
        };
        owner.validate()?;
        Ok(owner)
    }
    pub(super) fn validate(&mut self) -> Result<(), String> {
        if self.failed {
            return Err("history source custody failed; reopen required".into());
        }
        // An error or unwind keeps this owner fenced, including restoration of
        // the original directory or identity after its loss was observed.
        self.failed = true;
        if let Some(application) = &self.application {
            application.validate()?;
        }
        self.identity.validate()?;
        if let Some(application) = &self.application {
            application.validate()?;
        }
        self.failed = false;
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
