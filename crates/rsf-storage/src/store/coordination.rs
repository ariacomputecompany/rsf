//! The OS lock excludes other processes. Same-process recovery handles remain
//! reentrant, but their reads and mutations must serialize at the store root.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use rsf_core::{ErrorCode, RsfError, RsfResult};

pub(super) type StoreGate = Arc<Mutex<()>>;

pub(super) fn store_gate(root: &Path) -> RsfResult<StoreGate> {
    static GATES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();
    let root = std::fs::canonicalize(root)
        .map_err(|err| failure(format!("cannot identify store root: {err}")))?;
    let mut gates = GATES
        .get_or_init(Mutex::default)
        .lock()
        .map_err(|_| failure("store gate registry poisoned"))?;
    gates.retain(|_, gate| gate.strong_count() != 0);
    if let Some(gate) = gates.get(&root).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(Mutex::new(()));
    gates.insert(root, Arc::downgrade(&gate));
    Ok(gate)
}

pub(super) fn lock_gate(gate: &StoreGate) -> RsfResult<MutexGuard<'_, ()>> {
    gate.lock()
        .map_err(|_| failure("store writer poisoned; restart before further operations"))
}

fn failure(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}
