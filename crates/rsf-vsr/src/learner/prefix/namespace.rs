//! Finite complete learner namespace under actual published/staged owners.
//! No directory spelling, inventory or disposal subset grants retention.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
mod candidate;
mod discard;
mod live;
#[cfg(test)]
pub(in crate::learner::prefix) mod observation;
use candidate::Candidate;

const MAX_CANDIDATES: usize = 3;
const MAX_ROOT_NAMES: usize = 5 + MAX_CANDIDATES;

#[derive(Clone, Debug)]
pub(in crate::learner) struct Directory {
    path: PathBuf,
    identity: guard::Identity,
}
impl Directory {
    pub(in crate::learner) fn capture(path: &Path) -> RsfResult<Self> {
        Ok(Self {
            path: path.to_path_buf(),
            identity: guard::directory(path)?,
        })
    }
    pub(in crate::learner) fn validate(&self) -> RsfResult<()> {
        if guard::directory(&self.path)? != self.identity {
            return Err(corrupt("bounded learner original directory changed"));
        }
        Ok(())
    }
    fn sync(&self) -> RsfResult<()> {
        self.validate()?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY);
        }
        let file = options.open(&self.path).map_err(io_error)?;
        // Compare the actual opened directory to the named original; neither
        // a replacement nor a new path becomes the source of a sync proof.
        guard::same_directory(&file.metadata().map_err(io_error)?, &self.identity)?;
        effect(Operation::Sync, &self.path, || {
            file.sync_all().map_err(io_error)
        })?;
        guard::same_directory(&file.metadata().map_err(io_error)?, &self.identity)?;
        self.validate()
    }
}

#[derive(Debug)]
pub(super) struct Namespace {
    root: Directory,
    identity: guard::Exact,
    active: guard::Exact,
    lock: guard::Exact,
    active_id: [u8; 32],
    temporaries: BTreeMap<OsString, guard::Exact>,
    candidate_names: BTreeSet<[u8; 32]>,
    candidates: BTreeMap<[u8; 32], Candidate>,
    limits: VsrPrefixLimits,
}
impl Namespace {
    // Used before cold manifest/WAL opening too. This is a count/name guard,
    // not the complete candidate preflight or an owner construction shortcut.
    pub(super) fn root_names(path: &Path) -> RsfResult<BTreeSet<OsString>> {
        let names = names(path, MAX_ROOT_NAMES)?;
        let mut count = 0usize;
        for name in &names {
            if is_root_metadata(name) {
                continue;
            }
            decode_id(name)?;
            count += 1;
        }
        if count > MAX_CANDIDATES {
            return Err(invalid_input("bounded learner candidate capacity exceeded"));
        }
        Ok(names)
    }

    pub(super) fn capture(owner: &LearnerHistoryStore, limits: VsrPrefixLimits) -> RsfResult<Self> {
        limits.validate()?;
        // Charge all three possible physical subtrees with checked arithmetic.
        let storage = limits.storage()?;
        storage
            .max_encoded_bytes
            .checked_mul(MAX_CANDIDATES as u64)
            .ok_or_else(|| invalid_input("bounded learner aggregate capacity overflow"))?;
        owner.validate_prefix_owners()?;
        let root = Directory::capture(&owner.root)?;
        let root_names = Self::root_names(&owner.root)?;
        let published = owner.published.as_ref().expect("validated published owner");
        let mut identity_bytes = b"RSFL1".to_vec();
        for value in [owner.pg.0, owner.local.0, owner.source.0] {
            identity_bytes.extend_from_slice(&value.to_le_bytes());
        }
        let identity = guard::Exact::capture(&owner.root.join("IDENTITY"), &identity_bytes)?;
        let active = guard::Exact::capture(&owner.root.join("ACTIVE"), &published.id)?;
        let lock = guard::owned_lock(&owner.root.join(".learner.lock"), &owner._lock)?;
        let mut candidates = BTreeMap::new();
        for name in root_names.iter().filter(|name| !is_root_metadata(name)) {
            let id = decode_id(name)?;
            let candidate = Candidate::capture(owner, id, limits)?;
            candidates.insert(id, candidate);
        }
        if !candidates.contains_key(&published.id) {
            return Err(corrupt("bounded learner publication directory is absent"));
        }
        let candidate_names = candidates.keys().copied().collect::<BTreeSet<_>>();
        let mut temporaries = BTreeMap::new();
        for (name, maximum) in [(".IDENTITY.tmp", identity_bytes.len()), (".ACTIVE.tmp", 32)] {
            if root_names.contains(OsStr::new(name)) {
                let (bytes, exact) = io::read(&owner.root.join(name), maximum)?;
                let valid = if name == ".IDENTITY.tmp" {
                    identity_bytes.starts_with(&bytes)
                } else {
                    candidate_names.iter().any(|id| id.starts_with(&bytes))
                };
                if !valid {
                    return Err(corrupt("bounded learner temporary has foreign bytes"));
                }
                temporaries.insert(OsString::from(name), exact);
            }
        }
        let result = Self {
            root,
            identity,
            active,
            lock,
            active_id: published.id,
            temporaries,
            candidate_names,
            candidates,
            limits,
        };
        result.validate(owner)?;
        Ok(result)
    }

    pub(super) fn contains(&self, id: &[u8; 32]) -> bool {
        self.candidate_names.contains(id)
    }

    pub(super) fn create_slot(
        &mut self,
        owner: &LearnerHistoryStore,
        id: [u8; 32],
    ) -> RsfResult<Directory> {
        self.validate(owner)?;
        if self.contains(&id) || self.candidate_names.len() >= MAX_CANDIDATES {
            return Err(invalid_input("bounded learner has no fresh candidate slot"));
        }
        let path = owner.directory(&id);
        absent(&path)?;
        effect(Operation::CreateDirectory, &path, || {
            fs::create_dir(&path).map_err(io_error)
        })?;
        let created = Directory::capture(&path)?;
        self.candidate_names.insert(id);
        self.validate_preparing(owner, id, &created)?;
        self.root.sync()?;
        self.validate_preparing(owner, id, &created)?;
        Ok(created)
    }
    pub(super) fn validate_preparing(
        &self,
        owner: &LearnerHistoryStore,
        id: [u8; 32],
        original: &Directory,
    ) -> RsfResult<()> {
        if !self.candidate_names.contains(&id) || self.candidates.contains_key(&id) {
            return Err(corrupt("bounded factory has no exact reserved slot"));
        }
        original.validate()?;
        self.validate_except(owner, Some(id))?;
        original.validate()
    }

    pub(super) fn validate(&self, owner: &LearnerHistoryStore) -> RsfResult<()> {
        self.validate_except(owner, None)?;
        Ok(())
    }
    fn validate_except(
        &self,
        owner: &LearnerHistoryStore,
        excluded: Option<[u8; 32]>,
    ) -> RsfResult<()> {
        let mut tracked = self.candidates.keys().copied().collect::<BTreeSet<_>>();
        if let Some(id) = excluded {
            tracked.insert(id);
        }
        if tracked != self.candidate_names {
            return Err(corrupt("bounded namespace lost an owned candidate proof"));
        }
        self.root.validate()?;
        self.identity.validate()?;
        self.active.validate()?;
        self.lock.validate()?;
        let published = owner
            .published
            .as_ref()
            .ok_or_else(|| corrupt("bounded learner publication owner disappeared"))?;
        if published.id != self.active_id {
            return Err(corrupt(
                "bounded namespace publication changed without handoff",
            ));
        }
        owner.validate_prefix_owners()?;
        let mut expected = BTreeSet::from([
            OsString::from("IDENTITY"),
            OsString::from("ACTIVE"),
            OsString::from(".learner.lock"),
        ]);
        for name in self.temporaries.keys() {
            expected.insert(name.clone());
        }
        for id in &self.candidate_names {
            expected.insert(OsString::from(encode_id(id)));
        }
        if names(&self.root.path, MAX_ROOT_NAMES)? != expected {
            return Err(corrupt("bounded learner complete root namespace changed"));
        }
        for exact in self.temporaries.values() {
            exact.validate()?;
        }
        for (id, candidate) in &self.candidates {
            if Some(*id) != excluded {
                candidate.validate()?;
            }
        }
        self.root.validate()
    }

    pub(super) fn adopt_opened(
        &mut self,
        owner: &LearnerHistoryStore,
        id: [u8; 32],
    ) -> RsfResult<()> {
        // Caller already owns the strict replayed writer. Compare the old
        // physical inventory before and after deriving its complete live shape.
        self.validate(owner)?;
        let next = Candidate::capture_live(owner, id, self.limits)?;
        self.candidates
            .get(&id)
            .ok_or_else(|| corrupt("bounded existing candidate vanished"))?
            .validate()?;
        self.candidates.insert(id, next);
        self.validate(owner)
    }

    pub(super) fn add_created(
        &mut self,
        owner: &LearnerHistoryStore,
        original: Directory,
        id: [u8; 32],
    ) -> RsfResult<()> {
        if !self.candidate_names.contains(&id) || self.candidates.contains_key(&id) {
            return Err(corrupt("bounded fresh candidate has no reserved slot"));
        }
        original.validate()?;
        let candidate = Candidate::capture_live(owner, id, self.limits)?;
        if candidate.directory.identity != original.identity {
            return Err(corrupt("bounded fresh candidate changed its created root"));
        }
        original.validate()?;
        self.candidates.insert(id, candidate);
        self.validate(owner)?;
        #[cfg(test)]
        observation::at(Operation::CandidateReady, true, &owner.directory(&id))?;
        Ok(())
    }

    pub(super) fn clear_temporaries(&mut self, owner: &LearnerHistoryStore) -> RsfResult<()> {
        for name in self.temporaries.keys().cloned().collect::<Vec<_>>() {
            self.validate(owner)?;
            self.temporaries[&name].validate()?;
            let path = self.root.path.join(&name);
            effect(Operation::Unlink, &path, || {
                fs::remove_file(&path).map_err(io_error)
            })?;
            self.temporaries.remove(&name);
            self.validate(owner)?;
            self.root.sync()?;
        }
        self.validate(owner)
    }

    pub(super) fn published(
        &mut self,
        owner: &LearnerHistoryStore,
        id: [u8; 32],
        exact: guard::Exact,
    ) -> RsfResult<()> {
        if !self.candidate_names.contains(&id) {
            return Err(corrupt("successor publication lacks preflighted candidate"));
        }
        exact.validate()?;
        self.active = exact;
        self.active_id = id;
        absent(&self.root.path.join(".ACTIVE.tmp"))?;
        self.temporaries.remove(OsStr::new(".ACTIVE.tmp"));
        self.validate(owner)
    }

    pub(super) fn prepared_publication(
        &mut self,
        owner: &LearnerHistoryStore,
        publication: &io::PreparedPublication,
    ) -> RsfResult<()> {
        if self.temporaries.contains_key(OsStr::new(".ACTIVE.tmp")) {
            return Err(corrupt("bounded publication reused an unretired temporary"));
        }
        let exact = publication.temporary_guard();
        exact.validate()?;
        self.temporaries
            .insert(OsString::from(".ACTIVE.tmp"), exact);
        self.validate(owner)
    }

    pub(super) fn completed(&mut self, owner: &LearnerHistoryStore, id: [u8; 32]) -> RsfResult<()> {
        let Some(PrefixContext {
            continuation: Continuation::Rejected(rejected),
            ..
        }) = &owner.prefix_context
        else {
            return Err(corrupt("completed candidate has no retained owner"));
        };
        if rejected.candidate.id != id {
            return Err(corrupt("completed candidate differs from namespace"));
        }
        let candidate = self
            .candidates
            .get_mut(&id)
            .ok_or_else(|| corrupt("completed candidate lost namespace"))?;
        candidate.validate_anchor(false)?;
        let Some(candidate::Cell::Live(cell)) = &mut candidate.cell else {
            return Err(corrupt("completed candidate lacked actual live store"));
        };
        cell.completed(&rejected.candidate)?;
        self.validate(owner)
    }
}

fn is_root_metadata(name: &OsStr) -> bool {
    [
        "IDENTITY",
        "ACTIVE",
        ".learner.lock",
        ".IDENTITY.tmp",
        ".ACTIVE.tmp",
    ]
    .iter()
    .any(|value| name == *value)
}
fn names(path: &Path, maximum: usize) -> RsfResult<BTreeSet<OsString>> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(path).map_err(io_error)? {
        let entry = entry.map_err(io_error)?;
        if names.len() >= maximum {
            return Err(invalid_input("bounded learner namespace capacity exceeded"));
        }
        names.insert(entry.file_name());
    }
    Ok(names)
}
fn encode_id(id: &[u8; 32]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn decode_id(name: &OsStr) -> RsfResult<[u8; 32]> {
    let value = name
        .to_str()
        .ok_or_else(|| corrupt("bounded learner has non-UTF8 namespace entry"))?;
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(corrupt("bounded learner has unknown namespace entry"));
    }
    let mut id = [0; 32];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| corrupt("invalid bounded learner ID"))?;
    }
    Ok(id)
}
fn absent(path: &Path) -> RsfResult<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
        Ok(_) => Err(corrupt("bounded learner retired name reappeared")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::learner::prefix) enum Operation {
    #[cfg(test)]
    CandidateReady,
    CreateDirectory,
    RetainDescriptor,
    RemoveChildren,
    FinishStorage,
    Unlink,
    RemoveDirectory,
    Sync,
    PublishActive,
}
pub(in crate::learner::prefix) fn effect<T>(
    operation: Operation,
    path: &Path,
    work: impl FnOnce() -> RsfResult<T>,
) -> RsfResult<T> {
    #[cfg(test)]
    observation::at(operation, false, path)?;
    #[cfg(not(test))]
    let _ = (operation, path);
    let value = work()?;
    #[cfg(test)]
    observation::at(operation, true, path)?;
    Ok(value)
}
