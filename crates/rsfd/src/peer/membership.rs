//! Explicit native recovery policy; all transition authority stays in RPC/VSR.
use super::*;
use rsf_rpc::replication::PeerMembershipRecoveryTiming;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryConfig {
    interval_ms: u64,
}

impl RecoveryConfig {
    pub fn timing(&self) -> Result<PeerMembershipRecoveryTiming, String> {
        if !(1..=60_000).contains(&self.interval_ms) {
            return Err("membership recovery interval must be 1..60000 milliseconds".into());
        }
        Ok(PeerMembershipRecoveryTiming {
            interval: Duration::from_millis(self.interval_ms),
        })
    }
}

pub(super) fn configured(
    cells: &[CellConfig],
) -> Result<BTreeMap<PlacementGroupId, PeerMembershipRecoveryTiming>, String> {
    cells
        .iter()
        .filter_map(|cell| {
            cell.membership_recovery.as_ref().map(|config| {
                config
                    .timing()
                    .map(|timing| (PlacementGroupId(cell.pg), timing))
            })
        })
        .collect()
}

pub(super) fn paths(
    root: &Path,
    groups: &BTreeSet<PlacementGroupId>,
) -> BTreeMap<PlacementGroupId, PathBuf> {
    groups
        .iter()
        .map(|pg| (*pg, root.join(format!("membership-{}", pg.0))))
        .collect()
}

fn managed_directory_group(name: &std::ffi::OsStr) -> Result<Option<PlacementGroupId>, String> {
    if !name.as_encoded_bytes().starts_with(b"membership-") {
        return Ok(None);
    }
    let name = name
        .to_str()
        .ok_or("managed membership directory name is not UTF-8")?;
    let pg = name["membership-".len()..]
        .parse::<u64>()
        .map_err(|_| "invalid managed membership directory name")?;
    if name != format!("membership-{pg}") {
        return Err("managed membership directory name is not canonical".into());
    }
    Ok(Some(PlacementGroupId(pg)))
}

/// This slice recovers existing acceptors only. Missing custody never becomes
/// an empty replacement journal. Full proof/identity validation is RPC-owned.
pub(super) fn preflight(
    root: &Path,
    configured: &BTreeSet<PlacementGroupId>,
    enrolled: &BTreeSet<PlacementGroupId>,
    client: bool,
) -> Result<(), String> {
    if configured.len() > 4096 || !enrolled.is_subset(configured) {
        return Err(
            "durably managed membership groups require explicit recovery configuration".into(),
        );
    }
    let mut existing = BTreeSet::new();
    for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let Some(pg) = managed_directory_group(&entry.file_name())? else {
            continue;
        };
        if !entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            return Err("managed membership custody requires a canonical local directory".into());
        }
        existing.insert(pg);
        if existing.len() > 4096 {
            return Err("managed membership directory count exceeds limit".into());
        }
    }
    if !existing.is_subset(configured) {
        return Err("existing membership custody requires explicit recovery configuration".into());
    }
    if client && (!configured.is_empty() || !enrolled.is_empty() || !existing.is_empty()) {
        return Err(
            "managed membership recovery cannot admit clients before replicated control admission"
                .into(),
        );
    }
    for (_, path) in paths(root, configured) {
        for name in [
            "",
            "inbox",
            "inbox/metadata",
            "decision",
            "decision/journal",
        ] {
            let metadata = std::fs::symlink_metadata(path.join(name))
                .map_err(|error| format!("existing membership custody is missing: {error}"))?;
            if !metadata.is_dir() {
                return Err("membership custody contains a non-directory or symlink".into());
            }
        }
        for name in ["inbox/metadata/MANIFEST", "decision/journal/MANIFEST"] {
            let metadata = std::fs::symlink_metadata(path.join(name)).map_err(|error| {
                format!("existing membership identity journal is missing: {error}")
            })?;
            if !metadata.is_file() {
                return Err("membership identity manifest is not a regular file".into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn managed_policy_blocks_flag_removal_and_never_recreates_missing_custody() {
        let root = std::env::temp_dir().join(format!(
            "rsfd-managed-preflight-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let enrolled = [PlacementGroupId(1)].into();
        assert!(preflight(&root, &BTreeSet::new(), &enrolled, true).is_err());
        assert!(preflight(&root, &enrolled, &enrolled, false).is_err());
        assert!(!root.join("membership-1").exists());
        // Presence before enrollment is also a guard; this covers a crash
        // between validated custody and the monotonic identity replacement.
        std::fs::create_dir(root.join("membership-1")).unwrap();
        assert!(preflight(&root, &BTreeSet::new(), &BTreeSet::new(), true).is_err());
        assert!(preflight(&root, &enrolled, &BTreeSet::new(), true).is_err());
        assert!(preflight(&root, &enrolled, &BTreeSet::new(), false).is_err());
        assert!(!root.join("membership-1/inbox").exists());
        std::fs::remove_dir_all(root.join("membership-1")).unwrap();
        std::fs::create_dir(root.join("membership-01")).unwrap();
        assert!(preflight(&root, &BTreeSet::new(), &BTreeSet::new(), true).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            // Some Unix filesystems reject these bytes before a directory can
            // exist. Exercise the same production parser without depending on
            // the host filesystem's filename encoding policy.
            let name = std::ffi::OsString::from_vec(b"membership-\xff".to_vec());
            assert!(managed_directory_group(&name).is_err());
            let unrelated = std::ffi::OsString::from_vec(b"unrelated-\xff".to_vec());
            assert_eq!(managed_directory_group(&unrelated).unwrap(), None);
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
