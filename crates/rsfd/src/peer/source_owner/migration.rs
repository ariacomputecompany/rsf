//! Explicit migration of the old application-only V1 binding. Ordinary startup
//! and history transfer never create or repair a missing identity lock.
use super::*;
mod custody;
use custody::Legacy;

pub(in crate::peer) fn run_cli(args: &[String]) -> Result<(), String> {
    let usage = "usage: rsfd peer migrate-application-identity --state <daemon-state> --cluster <cluster> --node <node>";
    let [_, state_flag, state, cluster_flag, cluster, node_flag, node] = args else {
        return Err(usage.into());
    };
    if state_flag != "--state" || cluster_flag != "--cluster" || node_flag != "--node" {
        return Err(usage.into());
    }
    let cluster = ClusterId::new(cluster).map_err(|e| e.message)?;
    let node = NodeId(node.parse().map_err(|_| usage)?);
    let _completed = migrate(Path::new(state), &cluster, node)?;
    println!(
        "{}",
        serde_json::json!({"event":"peer.application-identity-migrated","cluster":cluster.as_str(),"node":node.0})
    );
    Ok(())
}

struct Completed {
    _legacy: Legacy,
    _lock: custody::Locked,
}

fn migrate(state: &Path, cluster: &ClusterId, node: NodeId) -> Result<Completed, String> {
    let legacy = Legacy::capture(state, cluster, node)?;
    legacy.checked_boundary(None, Boundary::BeforeLock)?;
    let lock = legacy.lock()?;
    legacy.checked_boundary(Some(&lock), Boundary::AfterLock)?;
    legacy.sync_identity(&lock)?;
    legacy.checked_boundary(Some(&lock), Boundary::AfterIdentitySync)?;
    let result = lock.sync();
    legacy.validate(Some(&lock))?;
    result?;
    legacy.checked_boundary(Some(&lock), Boundary::AfterLockSync)?;
    // Renew the binding route and its ancestors child-before-parent without
    // creating entries. The unchanged source route remains validated; original
    // route/file custody brackets the complete publication result.
    let result = rsf_storage::sync_existing_directory(legacy.binding())
        .map_err(|_| "application identity migration directory publication failed".to_string());
    legacy.validate(Some(&lock))?;
    result?;
    legacy.checked_boundary(Some(&lock), Boundary::AfterDirectorySync)?;
    Ok(Completed {
        _legacy: legacy,
        _lock: lock,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Boundary {
    BeforeLock,
    AfterLock,
    AfterIdentitySync,
    AfterLockSync,
    AfterDirectorySync,
}
fn boundary(at: Boundary, root: &Path) -> Result<(), String> {
    #[cfg(all(test, unix))]
    {
        tests::boundary(at, root)
    }
    #[cfg(not(all(test, unix)))]
    {
        let _ = (at, root);
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests;
