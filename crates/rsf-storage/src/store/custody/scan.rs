//! Finite read-only scans under exact physical custody. No resume DTO is accepted.
use super::*;
use crate::reader;

mod factory;
mod limits;
mod open;
pub use limits::{
    SegmentReadCursor, SegmentReadError, SegmentReadLimit, SegmentReadLimits, SegmentScanControl,
    SegmentScanOutcome, append_only_manifest_byte_bound, wal_record_disk_bytes,
};

impl SegmentStore {
    /// Scan checked physical records from this exact validated owner. The visitor
    /// sees WAL records only; every physical record still consumes the budget.
    pub fn scan_owned_prefix(
        &self,
        expected: &SegmentStoreCustody,
        limits: SegmentReadLimits,
        admit: impl FnMut(u64) -> Result<(), SegmentReadLimit>,
        mut visit: impl FnMut(&WalRecord) -> RsfResult<SegmentScanControl>,
    ) -> Result<SegmentScanOutcome, SegmentReadError> {
        limits.validate()?;
        self.ensure_writable()?;
        let _guard = lock_gate(&self.gate)?;
        let cursor = SegmentReadCursor::default();
        // Bound even the canonical manifest encoding used by custody checks.
        manifest_budget(&self.manifest, limits.max_manifest_bytes, cursor)?;
        self.validate_owned_custody_locked(expected)?;
        let result = (|| {
            if !self.manifest.compacted_prefixes.is_empty() {
                return Err(SegmentReadError::CompactedPrefixUnsupported);
            }
            let custody = self.owned_read_custody_locked(expected);
            scan_locked(
                &self.manifest,
                &custody,
                limits,
                admit,
                |_, decoded| match decoded.wal {
                    Some(record) => visit(&record),
                    None => Ok(SegmentScanControl::Continue),
                },
            )
        })();
        // Physical failure takes precedence over callback/cap outcomes.
        self.validate_owned_custody_locked(expected)?;
        result
    }

    /// Recover actual bounded bytes from this exact validated writer without
    /// reopening it, adopting another owner, repairing a tail or syncing routes.
    /// The caller retains exclusive namespace ownership for the entire handoff.
    pub fn recover_owned_immutable_bounded(
        &self,
        expected: &SegmentStoreCustody,
        limits: SegmentReadLimits,
        admit: impl FnMut(u64) -> Result<(), SegmentReadLimit>,
    ) -> Result<SegmentStoreRecovery, SegmentReadError> {
        limits.validate()?;
        self.ensure_writable()?;
        let _guard = lock_gate(&self.gate)?;
        manifest_budget(
            &self.manifest,
            limits.max_manifest_bytes,
            SegmentReadCursor::default(),
        )?;
        self.validate_owned_custody_locked(expected)?;
        let result = (|| {
            if !self.manifest.compacted_prefixes.is_empty() {
                return Err(SegmentReadError::CompactedPrefixUnsupported);
            }
            let custody = self.owned_read_custody_locked(expected);
            let recovery = collect_locked(&self.manifest, &custody, limits, admit)?;
            Ok(SegmentStoreRecovery {
                segments: self.manifest.segments.clone(),
                recovery,
            })
        })();
        // Keep original physical custody authoritative, including on read/cap failure.
        self.validate_owned_custody_locked(expected)?;
        result
    }

    // Called only while the gate is held and original owner custody is valid.
    fn owned_read_custody_locked(&self, expected: &SegmentStoreCustody) -> SegmentReadCustody {
        // Copy original stamps; a newly captured replacement is never authority.
        SegmentReadCustody {
            root: immutable::normalize_root(&self.root),
            gate: self.gate.clone(),
            root_identity: expected.root.clone(),
            directory: expected.directory.clone(),
            manifest: expected.manifest.clone(),
            manifest_bytes: self.manifest.encode().into_bytes(),
            segments: self
                .manifest
                .segments
                .iter()
                .zip(&expected.segments)
                .map(|(descriptor, (_, stamp))| (descriptor.file_name.clone(), stamp.clone()))
                .collect(),
        }
    }

    /// Complete bounded immutable recovery, using the same parser as generic
    /// recovery. Incomplete tails fail; no file or sidecar is created/repaired.
    pub fn recover_immutable_bounded(
        root: impl AsRef<Path>,
        limits: SegmentReadLimits,
        admit: impl FnMut(u64) -> Result<(), SegmentReadLimit>,
    ) -> Result<(SegmentStoreRecovery, SegmentReadCustody), SegmentReadError> {
        limits.validate()?;
        let path = immutable::normalize_root(root.as_ref());
        let gate = store_gate(&path)?;
        let _guard = lock_gate(&gate)?;
        let (manifest, custody) = snapshot_locked(path, gate.clone(), limits)?;
        let recovery = collect_locked(&manifest, &custody, limits, admit)?;
        Ok((
            SegmentStoreRecovery {
                segments: manifest.segments,
                recovery,
            },
            custody,
        ))
    }
}

fn manifest_budget(
    manifest: &SegmentManifest,
    limit: usize,
    cursor: SegmentReadCursor,
) -> Result<(), SegmentReadError> {
    let mut bytes = format!(
        "rsf-segment-manifest {MANIFEST_VERSION}\nactive {}\n",
        manifest.active_id
    )
    .len();
    let mut add = |length: usize| -> Result<(), SegmentReadError> {
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| cursor.limit(SegmentReadLimit::ManifestBytes))?;
        if bytes > limit {
            return Err(cursor.limit(SegmentReadLimit::ManifestBytes));
        }
        Ok(())
    };
    add(0)?;
    for descriptor in &manifest.segments {
        // Numeric strings are fixed at <=20 bytes; no manifest-sized allocation.
        add("segment ".len()
            + descriptor.id.to_string().len()
            + 1
            + descriptor.file_name.len()
            + 1)?;
    }
    for prefix in manifest.compacted_prefixes.values() {
        add("base ".len()
            + prefix.pg.0.to_string().len()
            + 1
            + prefix.epoch.0.to_string().len()
            + 1
            + prefix.index.0.to_string().len()
            + 1
            + 64
            + 1)?;
    }
    Ok(())
}

fn snapshot_locked(
    path: PathBuf,
    gate: StoreGate,
    limits: SegmentReadLimits,
) -> Result<(SegmentManifest, SegmentReadCustody), SegmentReadError> {
    let expected = regular_file(&path.join(MANIFEST_FILE))?;
    if expected.bytes > limits.max_manifest_bytes as u64 {
        return Err(SegmentReadCursor::default().limit(SegmentReadLimit::ManifestBytes));
    }
    let (manifest, custody) = immutable::capture_locked(path, gate, limits.max_manifest_bytes)?;
    // A complete bounded capture must remain the same physical manifest checked
    // before allocation. Never accept a replacement during the cap precheck.
    if custody.manifest != expected {
        return Err(custody_error("bounded manifest changed during capture").into());
    }
    if !manifest.compacted_prefixes.is_empty() {
        custody.validate_locked()?;
        return Err(SegmentReadError::CompactedPrefixUnsupported);
    }
    Ok((manifest, custody))
}

fn collect_locked(
    manifest: &SegmentManifest,
    custody: &SegmentReadCustody,
    limits: SegmentReadLimits,
    admit: impl FnMut(u64) -> Result<(), SegmentReadLimit>,
) -> Result<Recovery, SegmentReadError> {
    let mut records = Vec::new();
    let mut wal_records = Vec::new();
    let mut maintenance_records = Vec::new();
    let mut tails = BTreeMap::new();
    let outcome = scan_locked(manifest, custody, limits, admit, |record, decoded| {
        if let Some(wal) = decoded.wal {
            tails.insert(wal.pg, WalTail::from(&wal));
            wal_records.push(wal);
        }
        if let Some(maintenance) = decoded.maintenance {
            maintenance_records.push(maintenance);
        }
        records.push(record);
        Ok(SegmentScanControl::Continue)
    })?;
    let SegmentScanOutcome::Complete(cursor) = outcome else {
        unreachable!("collector never stops")
    };
    Ok(Recovery {
        records,
        wal_records,
        maintenance_records,
        pg_tails: tails,
        compacted_prefixes: BTreeMap::new(),
        truncated_to: cursor.encoded_bytes(),
    })
}

fn scan_locked(
    manifest: &SegmentManifest,
    custody: &SegmentReadCustody,
    limits: SegmentReadLimits,
    mut admit: impl FnMut(u64) -> Result<(), SegmentReadLimit>,
    mut visit: impl FnMut(crate::SegmentRecord, reader::Decoded) -> RsfResult<SegmentScanControl>,
) -> Result<SegmentScanOutcome, SegmentReadError> {
    custody.validate_locked()?;
    let result = (|| {
        let mut cursor = SegmentReadCursor::default();
        let mut tails = BTreeMap::new();
        for (descriptor, (name, expected)) in manifest.segments.iter().zip(&custody.segments) {
            let path = custody.root.join(SEGMENT_DIR).join(name);
            let mut file = immutable::open_checked(&path, expected)?;
            let result = (|| {
                let mut offset = 0;
                loop {
                    let record =
                        match reader::next_frame(&mut file, expected.bytes, offset, |bytes| {
                            cursor.before_record(limits, bytes)?;
                            admit(bytes)
                        }) {
                            Ok(Some(record)) => record,
                            Ok(None) => {
                                crate::truncate_recovery_tail(&file, offset, false)?;
                                return Ok(None);
                            }
                            Err(reader::FrameError::Failure(error)) => {
                                return Err(SegmentReadError::Failure(error));
                            }
                            Err(reader::FrameError::Policy(kind)) => return Err(cursor.limit(kind)),
                        };
                    let decoded = reader::decode_and_chain(&record, &mut tails)?;
                    let bytes = (crate::HEADER_LEN as u64)
                        .checked_add(record.header.payload_len)
                        .ok_or_else(|| custody_error("physical record length overflow"))?;
                    cursor.completed(descriptor.id, offset, bytes)?;
                    offset = cursor.offset();
                    if visit(record, decoded)? == SegmentScanControl::Stop {
                        return Ok(Some(cursor));
                    }
                }
            })();
            if file_stamp(&file.metadata().map_err(custody_io)?)? != *expected
                || regular_file(&path)? != *expected
            {
                return Err(custody_error("bounded segment changed during read").into());
            }
            if let Some(cursor) = result? {
                return Ok(SegmentScanOutcome::Stopped(cursor));
            }
        }
        Ok(SegmentScanOutcome::Complete(cursor))
    })();
    custody.validate_locked()?;
    result
}

#[cfg(all(test, unix))]
mod tests;
