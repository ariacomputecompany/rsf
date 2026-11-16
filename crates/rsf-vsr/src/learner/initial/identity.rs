//! Strict identity/namespace ownership. Factory permission never comes from loss.
use super::*;

#[derive(Debug)]
pub(super) struct RootCustody {
    root: guard::Identity,
    identity: guard::Exact,
    learner_lock: guard::Exact,
    active: Option<guard::Exact>,
    pub(super) source: Option<NodeId>,
}
impl RootCustody {
    // Transfer the same original proofs. Cloning these immutable values does
    // not reopen or recapture their paths; the actual held lock moves separately.
    pub(super) fn publication_custody(&self, root: PathBuf) -> RsfResult<custody::OwnerCustody> {
        if self.source.is_none() || self.active.is_none() {
            return Err(corrupt("initial publication lacks bound source or ACTIVE"));
        }
        Ok(custody::OwnerCustody::from_original(
            root,
            self.root.clone(),
            self.identity.clone(),
            self.learner_lock.clone(),
            self.active.clone(),
        ))
    }
    pub(super) fn validate(
        &self,
        root: &Path,
        lock: &rsf_storage::ExclusiveFileLock,
    ) -> RsfResult<()> {
        if guard::directory(root)? != self.root {
            return Err(corrupt("initial learner root was replaced"));
        }
        self.identity.validate()?;
        self.learner_lock.validate()?;
        guard::owned_lock(&root.join(".learner.lock"), lock)?;
        match &self.active {
            Some(active) => active.validate(),
            None => absent(&root.join("ACTIVE")),
        }
    }
    pub(super) fn bind_source(
        &mut self,
        source: NodeId,
        publication: io::PreparedPublication,
    ) -> RsfResult<()> {
        if self.source.is_some() {
            return Err(corrupt("initial source was already bound"));
        }
        self.identity = publication.publish_guarded()?;
        self.source = Some(source);
        // The prepared inode is retained by publish_guarded; do not capture a
        // replacement name. Exact bytes were derived from the replayed source.
        Ok(())
    }
    pub(super) fn record_active(&mut self, publication: io::PreparedPublication) -> RsfResult<()> {
        if self.active.is_some() {
            return Err(corrupt("initial ACTIVE was already published"));
        }
        self.active = Some(publication.publish_guarded()?);
        Ok(())
    }
    pub(super) fn has_active(&self) -> bool {
        self.active.is_some()
    }
}
pub(super) fn provisional(policy: &VsrInitialPolicy) -> Vec<u8> {
    let mut bytes = b"RSFL2".to_vec();
    bytes.extend_from_slice(&policy.pg.0.to_le_bytes());
    bytes.extend_from_slice(&policy.local.0.to_le_bytes());
    bytes.extend_from_slice(&policy.bootstrap);
    bytes
}
pub(super) fn published(policy: &VsrInitialPolicy, source: NodeId) -> Vec<u8> {
    let mut bytes = b"RSFL1".to_vec();
    for value in [policy.pg.0, policy.local.0, source.0] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}
pub(super) fn absent(path: &Path) -> RsfResult<()> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_error(e)),
        Ok(_) => Err(corrupt(
            "initial learner has unexpected authoritative publication",
        )),
    }
}
fn exists(path: &Path) -> RsfResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(io_error(e)),
    }
}
pub(super) fn open(
    path: &Path,
    policy: VsrInitialPolicy,
    restriction: VsrInitialRestriction,
) -> RsfResult<InitialLearnerHistoryStore> {
    let root = io::normalized(path)?;
    let reserved = restriction == VsrInitialRestriction::Reserved;
    if reserved {
        io::create_directory(&root)?;
    }
    let root_identity = guard::directory(&root)?;
    let lock = io::lock(&root.join(".learner.lock"), reserved)?;
    let learner_lock = guard::owned_lock(&root.join(".learner.lock"), &lock)?;
    let identity_path = root.join("IDENTITY");
    if reserved && !exists(&identity_path)? {
        // No acknowledged selection exists. Only the actual first factory
        // prefixes may create identity; a records directory is not such a prefix.
        for entry in fs::read_dir(&root).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            if entry.file_name() != ".learner.lock" && entry.file_name() != ".IDENTITY.tmp" {
                return Err(corrupt(
                    "initial factory lost identity over existing content",
                ));
            }
        }
        io::publish(&root, "IDENTITY", &provisional(&policy))?;
    }
    let (bytes, identity) = io::read(&identity_path, 53)?;
    let source = match restriction {
        VsrInitialRestriction::Publishing { source, .. } if bytes == published(&policy, source) => {
            Some(source)
        }
        _ if bytes == provisional(&policy) => None,
        _ => {
            return Err(corrupt(
                "initial learner identity differs from trusted restriction",
            ));
        }
    };
    let active = match restriction {
        VsrInitialRestriction::Publishing { selection, .. }
            if source.is_some() && exists(&root.join("ACTIVE"))? =>
        {
            Some(guard::Exact::capture(
                &root.join("ACTIVE"),
                &selection.history_id,
            )?)
        }
        _ => {
            absent(&root.join("ACTIVE"))?;
            None
        }
    };
    let mut owner = InitialLearnerHistoryStore {
        root,
        policy,
        state: restriction.into(),
        candidate: None,
        custody: RootCustody {
            root: root_identity,
            identity,
            learner_lock,
            active,
            source,
        },
        lock: Some(lock),
        failed: AtomicBool::new(false),
        completed: false,
    };
    // Bound the complete outer namespace before selected/pinned replay. This
    // is read-only inventory, never permission to reclaim a current candidate.
    reclaim::inventory(&owner)?;
    if let VsrInitialRestriction::Selected(selection)
    | VsrInitialRestriction::Publishing { selection, .. } = restriction
    {
        let directory = owner.directory(&selection.history_id);
        let (bytes, descriptor) = io::read(&directory.join("DESCRIPTOR"), io::DESCRIPTOR_BYTES)?;
        let value = VsrHistoryDescriptor::decode(&bytes)?;
        if owner.check_descriptor(&value)? != selection.history_id {
            return Err(corrupt("initial selected descriptor identity mismatch"));
        }
        let candidate = prepare_candidate(&owner, value, false)?;
        descriptor.validate()?;
        owner.candidate = Some(candidate);
        if let VsrInitialRestriction::Publishing { source, .. } = restriction {
            owner.replay_candidate(false)?;
            if owner.replayed_source()? != source {
                return Err(corrupt("pinned initial source differs from replay"));
            }
        }
    }
    owner.healthy()?;
    reclaim::inventory(&owner)?;
    Ok(owner)
}
pub(super) fn prepare_candidate(
    owner: &InitialLearnerHistoryStore,
    descriptor: VsrHistoryDescriptor,
    factory: bool,
) -> RsfResult<Candidate> {
    let id = descriptor.id()?;
    let directory = owner.directory(&id);
    if factory {
        // Caller independently holds healthy exact A (or Reserved before any
        // first ACK). Only this currently unselected target may continue an
        // exact empty metadata factory. This makes no never-ACKed claim.
        io::create_directory(&directory)?;
        io::retain_descriptor(&directory, &descriptor.encode()?)?;
    }
    let directory_guard = Directory::capture(&directory)?;
    let descriptor_guard =
        guard::Exact::capture(&directory.join("DESCRIPTOR"), &descriptor.encode()?)?;
    if factory {
        // Only the trusted unselected factory may create its cell. Keep the
        // original outer proofs across creation before shared custody capture.
        owner.validate_owned()?;
        directory_guard.validate()?;
        descriptor_guard.validate()?;
        io::create_directory(&directory.join("cell"))?;
        directory_guard.validate()?;
        descriptor_guard.validate()?;
        owner.validate_owned()?;
    }
    let mut opened = stage::open(
        &directory.join("cell"),
        owner.policy.pg,
        descriptor,
        owner.policy.limits,
        factory,
    )?;
    stage::renew(&mut opened.stage.store, &opened.custody)?;
    opened.stage.store.validate_owned_custody(&opened.custody)?;
    let candidate = Candidate {
        body: Body::Receiving {
            stage: Box::new(opened.stage),
            context: PrefixContext {
                id,
                limits: owner.policy.limits,
                bytes: opened.bytes,
                continuation: Continuation::Staging(opened.custody),
                flat: opened.flat,
                descriptor: descriptor_guard,
                directory: directory_guard,
            },
        },
    };
    candidate.validate()?;
    owner.validate_owned()?;
    Ok(candidate)
}
