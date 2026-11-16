//! Bounded removal of non-current provisional candidates under a healthy selection.
//! No recursive deletion, inferred selection, published cleanup or repair path.
use super::*;

const ROOT_FIXED: &[&str] = &[
    "IDENTITY",
    ".learner.lock",
    ".IDENTITY.tmp",
    "ACTIVE",
    ".ACTIVE.tmp",
];
struct Removal {
    directory: PathBuf,
    identity: guard::Identity,
    root_candidates: Vec<[u8; 32]>,
    target: [u8; 32],
    descriptor: Option<guard::Exact>,
    temporary: Option<guard::Exact>,
    cell: Option<rsf_storage::SegmentDiscardInventory>,
    auxiliary: Option<guard::Exact>,
}
fn present(path: &Path) -> RsfResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(io_error(e)),
    }
}
fn names(path: &Path, maximum: usize) -> RsfResult<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(path).map_err(io_error)? {
        if names.len() == maximum {
            return Err(corrupt("initial namespace inventory exceeds finite bound"));
        }
        let entry = entry.map_err(io_error)?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| corrupt("initial namespace has noncanonical name"))?;
        names.push(name);
    }
    Ok(names)
}
fn canonical_id(name: &str) -> Option<[u8; 32]> {
    if name.len() != 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    let mut id = [0; 32];
    for (i, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&name[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(id)
}
/// Bounds names even in Reserved. Only Selected grants permission to remove.
pub(super) fn inventory(owner: &InitialLearnerHistoryStore) -> RsfResult<Vec<[u8; 32]>> {
    owner.validate_owned()?;
    let mut candidates = Vec::new();
    for name in names(&owner.root, ROOT_FIXED.len() + 2)? {
        if ROOT_FIXED.contains(&name.as_str()) {
            let metadata = fs::symlink_metadata(owner.root.join(&name)).map_err(io_error)?;
            guard::file_stamp(&metadata)?;
            let maximum = match name.as_str() {
                ".learner.lock" => 0,
                "ACTIVE" | ".ACTIVE.tmp" => 32,
                "IDENTITY" | ".IDENTITY.tmp" => 53,
                _ => unreachable!("fixed root inventory name"),
            };
            if metadata.len() > maximum {
                return Err(corrupt("initial fixed metadata exceeds its finite bound"));
            }
        } else if let Some(id) = canonical_id(&name) {
            if candidates.len() == 2 {
                return Err(corrupt(
                    "initial namespace has more than two candidate directories",
                ));
            }
            guard::directory(&owner.root.join(&name))?;
            candidates.push(id);
        } else {
            return Err(corrupt("initial namespace has unknown content"));
        }
    }
    owner.validate_owned()?;
    Ok(candidates)
}
fn recognized(owner: &InitialLearnerHistoryStore, id: [u8; 32]) -> RsfResult<Removal> {
    let directory = owner.directory(&id);
    let mut root_candidates = inventory(owner)?;
    root_candidates.sort();
    let mut removal = Removal {
        identity: guard::directory(&directory)?,
        directory,
        root_candidates,
        target: id,
        descriptor: None,
        temporary: None,
        cell: None,
        auxiliary: None,
    };
    let entries = names(&removal.directory, 2)?;
    let descriptor_path = removal.directory.join("DESCRIPTOR");
    if !present(&descriptor_path)? {
        if entries.iter().any(|name| name != ".DESCRIPTOR.tmp") {
            return Err(corrupt(
                "unselected initial factory lost descriptor over other content",
            ));
        }
        if !entries.is_empty() {
            // Existing descriptor factory permits a bounded regular temporary
            // under this owned, otherwise empty directory. No history exists.
            removal.temporary = Some(
                io::read(
                    &removal.directory.join(".DESCRIPTOR.tmp"),
                    io::DESCRIPTOR_BYTES,
                )?
                .1,
            );
        }
        removal.validate_parent(owner)?;
        return Ok(removal);
    }
    if entries
        .iter()
        .any(|name| name != "DESCRIPTOR" && name != "cell")
    {
        return Err(corrupt("unselected initial candidate has unknown files"));
    }
    let (encoded, exact) = io::read(&descriptor_path, io::DESCRIPTOR_BYTES)?;
    let descriptor = VsrHistoryDescriptor::decode(&encoded)?;
    if descriptor.encode()? != encoded || owner.check_descriptor(&descriptor)? != id {
        return Err(corrupt(
            "unselected descriptor differs from canonical directory identity",
        ));
    }
    removal.descriptor = Some(exact);
    let cell = removal.directory.join("cell");
    if present(&cell)? {
        let inventory =
            SegmentStore::inspect_provisional_discard(&cell, owner.policy.limits.storage()?)
                .map_err(stage::storage_error)?;
        // Storage recognizes only its own namespace. The complete bounded
        // external-name set is checked here before its first destructive phase.
        for name in inventory.other_root_names() {
            if name != std::ffi::OsStr::new(".vsr-writer.lock") || removal.auxiliary.is_some() {
                return Err(corrupt(
                    "unselected cell contains unknown auxiliary metadata",
                ));
            }
            removal.auxiliary = Some(guard::Exact::capture(&cell.join(name), &[])?);
        }
        inventory.validate()?;
        removal.cell = Some(inventory);
    }
    removal.validate_parent(owner)?;
    Ok(removal)
}
impl Removal {
    // Cell presence is tracked by the storage capability, including its final
    // authorized rmdir. The outer callback retains actual current B and the
    // candidate/descriptor; it never recaptures cell or storage metadata.
    fn validate_parent(&self, owner: &InitialLearnerHistoryStore) -> RsfResult<()> {
        owner.validate_owned()?;
        let mut candidates = inventory(owner)?;
        candidates.sort();
        if candidates != self.root_candidates {
            return Err(corrupt(
                "initial candidate inventory changed during disposal",
            ));
        }
        if guard::directory(&self.directory)? != self.identity {
            return Err(corrupt("unselected candidate directory changed"));
        }
        if let Some(exact) = &self.descriptor {
            exact.validate()?;
        }
        if let Some(exact) = &self.temporary {
            exact.validate()?;
        }
        for name in names(&self.directory, 2)? {
            let allowed = (self.descriptor.is_some() && (name == "DESCRIPTOR" || name == "cell"))
                || (self.temporary.is_some() && name == ".DESCRIPTOR.tmp");
            if !allowed {
                return Err(corrupt("unselected candidate namespace changed"));
            }
        }
        owner.validate_owned()
    }
    fn remove(mut self, owner: &InitialLearnerHistoryStore) -> RsfResult<()> {
        self.validate_parent(owner)?;
        if let Some(storage) = self.cell.take() {
            if let Some(auxiliary) = &self.auxiliary {
                auxiliary.validate()?;
            }
            storage.validate()?;
            let remainder = storage.remove_children(|| self.validate_parent(owner))?;
            #[cfg(test)]
            observation::at(observation::Boundary::StorageChildren, &self.directory)?;
            self.validate_parent(owner)?;
            if let Some(auxiliary) = self.auxiliary.take() {
                auxiliary.validate()?;
                fs::remove_file(self.directory.join("cell/.vsr-writer.lock")).map_err(io_error)?;
                sync_directory(&self.directory.join("cell"))?;
                self.validate_parent(owner)?;
            }
            // The original storage remainder requires all former external names
            // absent and retains MANIFEST/root until every required barrier.
            remainder.finish(|| self.validate_parent(owner))?;
            #[cfg(test)]
            observation::at(observation::Boundary::Cell, &self.directory)?;
            self.validate_parent(owner)?;
        }
        identity::absent(&self.directory.join("cell"))?;
        // Cell removal must be durable before discarding DESCRIPTOR, even when
        // this cold inspection began with an already absent cell.
        sync_directory(&self.directory)?;
        self.validate_parent(owner)?;
        if let Some(descriptor) = self.descriptor.take() {
            descriptor.validate()?;
            fs::remove_file(self.directory.join("DESCRIPTOR")).map_err(io_error)?;
            sync_directory(&self.directory)?;
            #[cfg(test)]
            observation::at(observation::Boundary::Descriptor, &self.directory)?;
        }
        if let Some(temporary) = self.temporary.take() {
            temporary.validate()?;
            fs::remove_file(self.directory.join(".DESCRIPTOR.tmp")).map_err(io_error)?;
            sync_directory(&self.directory)?;
        }
        self.validate_parent(owner)?;
        // Empty terminal candidate is a recognized cold successor after the
        // descriptor unlink. A newly appearing name fails checked rmdir.
        fs::remove_dir(&self.directory).map_err(io_error)?;
        sync_directory(&owner.root)?;
        let mut candidates = inventory(owner)?;
        candidates.sort();
        self.root_candidates.retain(|id| *id != self.target);
        if candidates != self.root_candidates {
            return Err(corrupt(
                "initial candidate inventory changed after disposal",
            ));
        }
        owner.validate_owned()
    }
}
pub(super) fn clean_selected(owner: &InitialLearnerHistoryStore) -> RsfResult<()> {
    let VsrInitialState::Selected(current) = owner.state else {
        return Err(invalid_input(
            "initial cleanup requires exact selected state",
        ));
    };
    owner.validate_owned()?;
    for id in inventory(owner)? {
        if id != current.history_id {
            recognized(owner, id)?.remove(owner)?;
        }
    }
    owner.validate_owned()
}
