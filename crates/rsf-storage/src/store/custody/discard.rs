//! Opt-in disposal of a caller-authorized, nonselected append-only subtree.
//! This is not recovery or evidence that the subtree is unacknowledged/unbound.
use super::*;
use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
mod inventory;
#[cfg(test)]
pub(in crate::store::custody) mod observation;
mod remove;

/// Original physical inventory for disposal only. Caller must own the outer
/// namespace and prove that no current authoritative/bound reference selects
/// this subtree. It may contain historical ACKed bytes. Never a recovery proof.
#[derive(Debug)]
pub struct SegmentDiscardInventory {
    state: State,
    other: Vec<OsString>,
}
/// Consumed storage-child phase. Caller removes its exact preflighted auxiliary
/// names before finish; no store, cursor, or completion ACK can be derived here.
#[derive(Debug)]
pub struct SegmentDiscardRemainder {
    state: State,
    other: Vec<OsString>,
}

impl SegmentStore {
    /// Bounded, read-only shape inspection. No WAL parse, repair, lock acquisition
    /// or creation. Only segment bytes count against max_encoded_bytes;
    /// metadata is independently bounded. max_record_bytes is not verified.
    /// A manifestless root must satisfy the actual existing empty initializer.
    pub fn inspect_provisional_discard(
        root: impl AsRef<Path>,
        limits: SegmentReadLimits,
    ) -> Result<SegmentDiscardInventory, SegmentReadError> {
        inventory::inspect(root.as_ref(), limits)
    }
}
impl SegmentDiscardInventory {
    /// Bounded observation only, never permission to ignore arbitrary names.
    /// Caller validates the complete set and its own original auxiliary custody
    /// before remove_children. Storage never removes these names in that phase.
    pub fn other_root_names(&self) -> impl ExactSizeIterator<Item = &OsStr> {
        self.other.iter().map(OsString::as_os_str)
    }
    pub fn validate(&self) -> RsfResult<()> {
        let _gate = lock_gate(&self.state.gate)?;
        self.state.validate()
    }
    /// Remove only the inventoried storage children, retaining original MANIFEST
    /// and root for cold retry. Callback must validate actual parent/domain
    /// custody, must not reenter this store gate, and runs with physical checks
    /// on both sides even when it returns Err. Any error consumes this capability.
    pub fn remove_children(
        self,
        validate_parent: impl FnMut() -> RsfResult<()>,
    ) -> RsfResult<SegmentDiscardRemainder> {
        remove::children(self, validate_parent)
    }
}
impl SegmentDiscardRemainder {
    /// Finish only after all reported external names are absent, every owned
    /// removal remains absent, and root contains only the original MANIFEST (or
    /// is empty). Sync/unlink the anchor, remove the empty root, then sync parent.
    pub fn finish(self, validate_parent: impl FnMut() -> RsfResult<()>) -> RsfResult<()> {
        remove::finish(self, validate_parent)
    }
}

#[derive(Debug)]
enum Kind {
    Directory(Identity),
    File {
        stamp: FileStamp,
        bytes: Option<Vec<u8>>,
    },
}
#[derive(Debug)]
struct Entry {
    kind: Kind,
    present: bool,
}
#[derive(Debug)]
struct State {
    root: PathBuf,
    parent: PathBuf,
    root_identity: Identity,
    parent_identity: Identity,
    gate: StoreGate,
    entries: BTreeMap<PathBuf, Entry>,
    root_removed: bool,
}
impl State {
    fn validate(&self) -> RsfResult<()> {
        if directory_identity(&self.parent)? != self.parent_identity {
            return Err(custody_error("discard parent directory changed"));
        }
        if self.root_removed {
            require_absent(&self.root)?;
            return Ok(());
        }
        if directory_identity(&self.root)? != self.root_identity {
            return Err(custody_error("discard original root changed"));
        }
        self.exact_names(Path::new(""))?;
        for (relative, entry) in &self.entries {
            let path = self.root.join(relative);
            if !entry.present {
                require_absent(&path)?;
                continue;
            }
            match &entry.kind {
                Kind::Directory(expected) => {
                    if directory_identity(&path)? != *expected {
                        return Err(custody_error("discard original directory changed"));
                    }
                    self.exact_names(relative)?;
                }
                Kind::File { stamp, bytes } => {
                    if regular_file(&path)? != *stamp {
                        return Err(custody_error("discard original file changed"));
                    }
                    if let Some(bytes) = bytes
                        && immutable::read_checked(&path, stamp, bytes.len() as u64)? != *bytes
                    {
                        return Err(custody_error("discard canonical metadata changed"));
                    }
                }
            }
        }
        if directory_identity(&self.root)? != self.root_identity
            || directory_identity(&self.parent)? != self.parent_identity
        {
            return Err(custody_error("discard directory changed during validation"));
        }
        Ok(())
    }
    fn exact_names(&self, relative: &Path) -> RsfResult<()> {
        let expected = self
            .entries
            .iter()
            .filter(|(path, entry)| {
                entry.present && path.parent().unwrap_or(Path::new("")) == relative
            })
            .map(|(path, _)| {
                path.file_name()
                    .expect("captured named child")
                    .to_os_string()
            })
            .collect::<BTreeSet<_>>();
        let actual = names(&self.root.join(relative), expected.len())?;
        if actual != expected {
            return Err(custody_error(
                "discard namespace changed or contains unknown entries",
            ));
        }
        Ok(())
    }
    fn parent_check(&self, parent: &mut impl FnMut() -> RsfResult<()>) -> RsfResult<()> {
        self.validate()?;
        let result = parent();
        // Callback error/panic never authorizes adopting changed physical state.
        // Panic consumes the capability; on Err physical postcheck dominates.
        self.validate()?;
        result
    }
    fn file_stamp(&self, name: &str) -> Option<&FileStamp> {
        self.entries
            .get(Path::new(name))
            .and_then(|entry| match &entry.kind {
                Kind::File { stamp, .. } => Some(stamp),
                Kind::Directory(_) => None,
            })
    }
}
fn require_absent(path: &Path) -> RsfResult<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(custody_io(error)),
        Ok(_) => Err(custody_error("discard removed name reappeared")),
    }
}
fn names(path: &Path, maximum: usize) -> RsfResult<BTreeSet<OsString>> {
    let mut result = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(custody_io)? {
        let entry = entry.map_err(custody_io)?;
        if result.len() >= maximum {
            return Err(custody_error(
                "discard directory exceeds bounded name inventory",
            ));
        }
        result.insert(entry.file_name());
    }
    Ok(result)
}
fn limit(kind: SegmentReadLimit) -> SegmentReadError {
    SegmentReadError::LimitExceeded {
        kind,
        cursor: SegmentReadCursor::default(),
    }
}
#[cfg(all(test, unix))]
mod tests;
