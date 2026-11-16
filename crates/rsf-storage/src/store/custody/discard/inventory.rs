//! Bound names and identities first, then interpret authoritative metadata.
use super::*;

pub(super) fn inspect(
    root: &Path,
    limits: SegmentReadLimits,
) -> Result<SegmentDiscardInventory, SegmentReadError> {
    limits.validate()?;
    let root = immutable::normalize_root(root);
    if root.file_name().is_none() {
        return Err(custody_error("discard requires a named child root").into());
    }
    let parent = root
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let gate = store_gate(&root)?;
    let gate_owner = gate.clone();
    let _gate = lock_gate(&gate_owner)?;
    let mut state = State {
        root: root.clone(),
        parent: parent.clone(),
        root_identity: directory_identity(&root)?,
        parent_identity: directory_identity(&parent)?,
        gate,
        entries: BTreeMap::new(),
        root_removed: false,
    };
    let maximum_segments = limits
        .max_records
        .checked_add(1)
        .ok_or_else(|| limit(SegmentReadLimit::Records))?;
    let maximum_names = maximum_segments
        .checked_add(1)
        .and_then(|n| n.checked_mul(2))
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| limit(SegmentReadLimit::Records))?;
    let root_names = names(&root, 5)?;
    let mut other = Vec::new();
    for name in &root_names {
        let relative = PathBuf::from(name);
        let path = root.join(&relative);
        let kind = if name == SEGMENT_DIR {
            Kind::Directory(directory_identity(&path)?)
        } else {
            let stamp = regular_file(&path)?;
            if name != MANIFEST_FILE && name != "MANIFEST.tmp" && name != "MANIFEST.lock" {
                if !other.is_empty() {
                    return Err(
                        custody_error("discard allows at most one caller-owned root name").into(),
                    );
                }
                other.push(name.clone());
            }
            if name == "MANIFEST.lock" && stamp.bytes != 0 {
                return Err(custody_error("discard sidecar must be empty").into());
            }
            if (name == MANIFEST_FILE || name == "MANIFEST.tmp")
                && stamp.bytes > limits.max_manifest_bytes as u64
            {
                return Err(limit(SegmentReadLimit::ManifestBytes));
            }
            Kind::File { stamp, bytes: None }
        };
        state.entries.insert(
            relative,
            Entry {
                kind,
                present: true,
            },
        );
    }
    if root_names.contains(OsStr::new(SEGMENT_DIR)) {
        for name in names(&root.join(SEGMENT_DIR), maximum_names)? {
            let relative = PathBuf::from(SEGMENT_DIR).join(name);
            let stamp = regular_file(&root.join(&relative))?;
            state.entries.insert(
                relative,
                Entry {
                    kind: Kind::File { stamp, bytes: None },
                    present: true,
                },
            );
        }
    }
    state.validate()?;
    let result = classify(&mut state, &other, limits, maximum_segments);
    // A parse/limit/predicate error cannot skip validating originals touched.
    state.validate()?;
    result?;
    Ok(SegmentDiscardInventory { state, other })
}
fn classify(
    state: &mut State,
    other: &[OsString],
    limits: SegmentReadLimits,
    maximum_segments: u64,
) -> Result<(), SegmentReadError> {
    if state.file_stamp(MANIFEST_FILE).is_none() {
        if !other.is_empty() {
            return Err(
                custody_error("manifestless discard cannot retain caller auxiliary names").into(),
            );
        }
        // Pre-capture bounded bytes so the actual existing inspector cannot
        // allocate from a changed untrusted temporary length unnoticed.
        if state.file_stamp("MANIFEST.tmp").is_some() {
            let bytes = metadata(state, "MANIFEST.tmp", limits.max_manifest_bytes)?;
            remember(state, "MANIFEST.tmp", bytes);
        }
        if !SegmentStore::initialization_pending(&state.root)? {
            return Err(
                custody_error("discard initial manifest appeared during inspection").into(),
            );
        }
        return Ok(());
    }
    let bytes = metadata(state, MANIFEST_FILE, limits.max_manifest_bytes)?;
    let manifest = SegmentManifest::decode(&bytes)?;
    if manifest.encode().as_bytes() != bytes {
        return Err(custody_error("discard manifest is not canonical").into());
    }
    remember(state, MANIFEST_FILE, bytes);
    if !manifest.compacted_prefixes.is_empty() {
        return Err(SegmentReadError::CompactedPrefixUnsupported);
    }
    if manifest.segments.len() as u64 > maximum_segments {
        return Err(limit(SegmentReadLimit::Records));
    }
    let next = if manifest.segments.last().expect("decoded nonempty").id == u64::MAX {
        None
    } else {
        Some(manifest.next_rotation()?)
    };
    let mut allowed = BTreeMap::new();
    for descriptor in &manifest.segments {
        allowed.insert(
            PathBuf::from(SEGMENT_DIR).join(&descriptor.file_name),
            false,
        );
        allowed.insert(
            PathBuf::from(SEGMENT_DIR)
                .join(&descriptor.file_name)
                .with_extension("lock"),
            true,
        );
    }
    if let Some((_, descriptor)) = &next {
        allowed.insert(PathBuf::from(SEGMENT_DIR).join(&descriptor.file_name), true);
        allowed.insert(
            PathBuf::from(SEGMENT_DIR)
                .join(&descriptor.file_name)
                .with_extension("lock"),
            true,
        );
    }
    let mut segment_bytes = 0u64;
    for (path, entry) in &state.entries {
        if path.parent() != Some(Path::new(SEGMENT_DIR)) {
            continue;
        }
        let zero = allowed
            .get(path)
            .ok_or_else(|| custody_error("discard contains undeclared segment or sidecar"))?;
        let Kind::File { stamp, .. } = &entry.kind else {
            return Err(custody_error("discard segment child is not a regular file").into());
        };
        if *zero && stamp.bytes != 0 {
            return Err(
                custody_error("discard sidecar or provisional next segment is nonempty").into(),
            );
        }
        if path.extension() == Some(OsStr::new("segment")) {
            segment_bytes = segment_bytes
                .checked_add(stamp.bytes)
                .ok_or_else(|| limit(SegmentReadLimit::EncodedBytes))?;
            if segment_bytes > limits.max_encoded_bytes {
                return Err(limit(SegmentReadLimit::EncodedBytes));
            }
        }
    }
    if state.file_stamp("MANIFEST.tmp").is_some() {
        let bytes = metadata(state, "MANIFEST.tmp", limits.max_manifest_bytes)?;
        let (next, _) = next.ok_or_else(|| {
            custody_error("exhausted manifest cannot have a next rotation temporary")
        })?;
        if !next.encode().as_bytes().starts_with(&bytes) {
            return Err(custody_error(
                "discard rotation temporary is not the canonical next prefix",
            )
            .into());
        }
        remember(state, "MANIFEST.tmp", bytes);
    }
    Ok(())
}
fn metadata(state: &State, name: &str, maximum: usize) -> Result<Vec<u8>, SegmentReadError> {
    let stamp = state.file_stamp(name).expect("captured metadata name");
    if stamp.bytes > maximum as u64 {
        return Err(limit(SegmentReadLimit::ManifestBytes));
    }
    Ok(immutable::read_checked(
        &state.root.join(name),
        stamp,
        maximum as u64,
    )?)
}
fn remember(state: &mut State, name: &str, value: Vec<u8>) {
    let entry = state
        .entries
        .get_mut(Path::new(name))
        .expect("captured metadata entry");
    let Kind::File { bytes, .. } = &mut entry.kind else {
        unreachable!("metadata is a regular file")
    };
    *bytes = Some(value);
}
