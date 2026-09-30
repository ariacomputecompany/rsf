#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rsf_core::{Bytes, Digest, ErrorCode, RsfError, RsfResult, blob::ContentId};

const CHUNK_DIR: &str = "chunks";
const QUARANTINE_DIR: &str = "quarantine";
const CHUNK_MAGIC: &[u8; 8] = b"RSFCHK01";
const CHUNK_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ChunkClass {
    RelationalLargeValue,
    ObjectBody,
    BlobPayload,
    SnapshotBlock,
    SstableBlock,
    BackupBlock,
}

impl ChunkClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RelationalLargeValue => "relational-large-value",
            Self::ObjectBody => "object-body",
            Self::BlobPayload => "blob-payload",
            Self::SnapshotBlock => "snapshot-block",
            Self::SstableBlock => "sstable-block",
            Self::BackupBlock => "backup-block",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkWrite {
    pub class: ChunkClass,
    pub namespace: String,
    pub bytes: Bytes,
    pub pin: bool,
}

impl ChunkWrite {
    pub fn new(
        class: ChunkClass,
        namespace: impl Into<String>,
        bytes: impl Into<Bytes>,
        pin: bool,
    ) -> Self {
        Self {
            class,
            namespace: namespace.into(),
            bytes: bytes.into(),
            pin,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredChunk {
    pub id: ContentId,
    pub class: ChunkClass,
    pub namespace: String,
    pub bytes: Bytes,
    pub ref_count: u32,
    pub pin_count: u32,
    pub checksum: Digest,
    pub quarantined: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableChunkMeta {
    pub id: ContentId,
    pub class: ChunkClass,
    pub namespace: String,
    pub bytes: u64,
    pub ref_count: u32,
    pub pin_count: u32,
    pub checksum: Digest,
    pub quarantined: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuarantineEntry {
    pub id: Option<ContentId>,
    pub reason: String,
    pub source_name: String,
    pub corrupt_path: PathBuf,
    pub manifest_path: PathBuf,
    pub recorded_at_unix_millis: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChunkDecision {
    Stored {
        id: ContentId,
        class: ChunkClass,
        bytes: u64,
        deduped: bool,
        pins: u32,
    },
    ReadVerified {
        id: ContentId,
        bytes: u64,
        checksum: Digest,
    },
    Pinned {
        id: ContentId,
        pins: u32,
    },
    Released {
        id: ContentId,
        remaining_pins: u32,
    },
    Quarantined {
        id: ContentId,
        reason: String,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct ChunkStore {
    chunks: BTreeMap<ContentId, StoredChunk>,
}

#[derive(Debug)]
pub struct DurableChunkStore {
    root: PathBuf,
    chunks: BTreeMap<ContentId, DurableChunkMeta>,
    quarantines: Vec<QuarantineEntry>,
    failure: Option<String>,
    _writer_lock: rsf_storage::ExclusiveFileLock,
}

impl ChunkStore {
    pub fn put(&mut self, write: ChunkWrite) -> ChunkDecision {
        if write.namespace.trim().is_empty() {
            return reject("chunk namespace must not be empty");
        }
        if write.bytes.is_empty() {
            return reject("chunk payload must not be empty");
        }

        let id = ContentId::for_bytes(&write.bytes);
        if let Some(existing) = self.chunks.get_mut(&id) {
            if existing.checksum != id.digest().expect("content id digest")
                || existing.bytes != write.bytes
            {
                existing.quarantined = true;
                return ChunkDecision::Quarantined {
                    id,
                    reason: "dedupe digest collision or stored bytes changed".to_string(),
                };
            }
            existing.ref_count += 1;
            if write.pin {
                existing.pin_count += 1;
            }
            return ChunkDecision::Stored {
                id,
                class: existing.class,
                bytes: existing.bytes.len() as u64,
                deduped: true,
                pins: existing.pin_count,
            };
        }

        let checksum = id.digest().expect("content id digest");
        let chunk = StoredChunk {
            id: id.clone(),
            class: write.class,
            namespace: write.namespace,
            bytes: write.bytes,
            ref_count: 1,
            pin_count: u32::from(write.pin),
            checksum,
            quarantined: false,
        };
        let bytes = chunk.bytes.len() as u64;
        let class = chunk.class;
        let pins = chunk.pin_count;
        self.chunks.insert(id.clone(), chunk);
        ChunkDecision::Stored {
            id,
            class,
            bytes,
            deduped: false,
            pins,
        }
    }

    pub fn read_verified(&self, id: &ContentId) -> ChunkDecision {
        let Some(chunk) = self.chunks.get(id) else {
            return reject("chunk is not present");
        };
        if chunk.quarantined {
            return ChunkDecision::Quarantined {
                id: id.clone(),
                reason: "chunk is quarantined".to_string(),
            };
        }
        if chunk.checksum != ContentId::for_bytes(&chunk.bytes).digest().expect("digest") {
            return ChunkDecision::Quarantined {
                id: id.clone(),
                reason: "chunk checksum mismatch".to_string(),
            };
        }
        ChunkDecision::ReadVerified {
            id: id.clone(),
            bytes: chunk.bytes.len() as u64,
            checksum: chunk.checksum,
        }
    }

    pub fn pin(&mut self, id: &ContentId) -> ChunkDecision {
        let Some(chunk) = self.chunks.get_mut(id) else {
            return reject("chunk is not present");
        };
        if chunk.quarantined {
            return ChunkDecision::Quarantined {
                id: id.clone(),
                reason: "chunk is quarantined".to_string(),
            };
        }
        chunk.pin_count = chunk.pin_count.saturating_add(1);
        ChunkDecision::Pinned {
            id: id.clone(),
            pins: chunk.pin_count,
        }
    }

    pub fn release_pin(&mut self, id: &ContentId) -> ChunkDecision {
        let Some(chunk) = self.chunks.get_mut(id) else {
            return reject("chunk is not present");
        };
        if chunk.pin_count == 0 {
            return reject("chunk has no pins to release");
        }
        chunk.pin_count -= 1;
        ChunkDecision::Released {
            id: id.clone(),
            remaining_pins: chunk.pin_count,
        }
    }

    pub fn quarantine(&mut self, id: &ContentId, reason: impl Into<String>) -> ChunkDecision {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return reject("quarantine reason must not be empty");
        }
        let Some(chunk) = self.chunks.get_mut(id) else {
            return reject("chunk is not present");
        };
        chunk.quarantined = true;
        ChunkDecision::Quarantined {
            id: id.clone(),
            reason,
        }
    }

    pub fn verify_all(&self) -> ChunkStoreReport {
        let mut total_bytes = 0;
        let mut pinned = 0;
        let mut quarantined = 0;
        for chunk in self.chunks.values() {
            total_bytes += chunk.bytes.len() as u64;
            if chunk.pin_count > 0 {
                pinned += 1;
            }
            if chunk.quarantined
                || chunk.checksum != ContentId::for_bytes(&chunk.bytes).digest().expect("digest")
            {
                quarantined += 1;
            }
        }
        ChunkStoreReport {
            chunks: self.chunks.len(),
            pinned,
            total_bytes,
            quarantined,
        }
    }

    pub fn get(&self, id: &ContentId) -> Option<&StoredChunk> {
        self.chunks.get(id)
    }

    #[cfg(test)]
    fn corrupt_for_test(&mut self, id: &ContentId) {
        if let Some(chunk) = self.chunks.get_mut(id)
            && let Some(first) = chunk.bytes.first_mut()
        {
            *first = first.wrapping_add(1);
        }
    }
}

impl DurableChunkStore {
    pub fn open(root: impl AsRef<Path>) -> RsfResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join(CHUNK_DIR)).map_err(|err| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("could not create chunk directory: {err}"),
            )
        })?;
        fs::create_dir_all(root.join(QUARANTINE_DIR)).map_err(|err| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("could not create chunk quarantine directory: {err}"),
            )
        })?;
        let writer_lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(root.join(".chunk-writer.lock"))
            .map_err(io_error)?;
        let writer_lock = rsf_storage::ExclusiveFileLock::acquire(writer_lock).map_err(|err| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("chunk store already has a writer: {err}"),
            )
        })?;
        let mut store = Self {
            root,
            chunks: BTreeMap::new(),
            quarantines: Vec::new(),
            failure: None,
            _writer_lock: writer_lock,
        };
        store.load_quarantines()?;
        store.recover()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn put(&mut self, write: ChunkWrite) -> RsfResult<ChunkDecision> {
        self.ensure_healthy()?;
        if write.namespace.trim().is_empty() {
            return Ok(reject("chunk namespace must not be empty"));
        }
        if write.bytes.is_empty() {
            return Ok(reject("chunk payload must not be empty"));
        }

        let id = ContentId::for_bytes(&write.bytes);
        if let Some(existing) = self.chunks.get(&id).cloned() {
            if existing.quarantined {
                return Ok(ChunkDecision::Quarantined {
                    id,
                    reason: "chunk is quarantined".to_string(),
                });
            }
            let bytes = self.read_chunk_bytes(&id)?;
            if bytes != write.bytes
                || existing.checksum != id.digest().expect("content id digest")
                || existing.bytes != bytes.len() as u64
            {
                self.quarantine(&id, "dedupe digest collision or stored bytes changed")?;
                return Ok(ChunkDecision::Quarantined {
                    id,
                    reason: "dedupe digest collision or stored bytes changed".to_string(),
                });
            }
            let mut updated = existing;
            updated.ref_count = updated.ref_count.saturating_add(1);
            if write.pin {
                updated.pin_count = updated.pin_count.saturating_add(1);
            }
            self.write_chunk_file(&updated, &bytes)?;
            self.chunks.insert(id.clone(), updated.clone());
            return Ok(ChunkDecision::Stored {
                id,
                class: updated.class,
                bytes: updated.bytes,
                deduped: true,
                pins: updated.pin_count,
            });
        }

        let meta = DurableChunkMeta {
            id: id.clone(),
            class: write.class,
            namespace: write.namespace,
            bytes: write.bytes.len() as u64,
            ref_count: 1,
            pin_count: u32::from(write.pin),
            checksum: id.digest().expect("content id digest"),
            quarantined: false,
        };
        self.write_chunk_file(&meta, &write.bytes)?;
        self.chunks.insert(id.clone(), meta.clone());
        Ok(ChunkDecision::Stored {
            id,
            class: meta.class,
            bytes: meta.bytes,
            deduped: false,
            pins: meta.pin_count,
        })
    }

    /// Ensure one content-addressed pin without incrementing reference counts
    /// when the same immutable blob is retried.
    pub fn ensure_pinned(&mut self, mut write: ChunkWrite) -> RsfResult<ChunkDecision> {
        self.ensure_healthy()?;
        let id = ContentId::for_bytes(&write.bytes);
        if let Some(existing) = self.chunks.get(&id).cloned() {
            if existing.class != write.class || existing.namespace != write.namespace {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    "content-addressed pin scope does not match stored chunk",
                ));
            }
            let bytes = self.read_bytes_verified(&id)?;
            if bytes != write.bytes {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    "content-addressed pin payload mismatch",
                ));
            }
            if existing.pin_count == 0 {
                self.pin(&id)?;
            }
            let pins = self.chunks[&id].pin_count;
            return Ok(ChunkDecision::Stored {
                id,
                class: existing.class,
                bytes: existing.bytes,
                deduped: true,
                pins,
            });
        }
        write.pin = true;
        self.put(write)
    }

    fn ensure_healthy(&self) -> RsfResult<()> {
        if let Some(failure) = &self.failure {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("chunk persistence failed; reopen before further operations: {failure}"),
            ));
        }
        Ok(())
    }

    pub fn read_verified(&mut self, id: &ContentId) -> RsfResult<ChunkDecision> {
        self.ensure_healthy()?;
        let Some(meta) = self.chunks.get(id).cloned() else {
            return Ok(reject("chunk is not present"));
        };
        if meta.quarantined {
            return Ok(ChunkDecision::Quarantined {
                id: id.clone(),
                reason: "chunk is quarantined".to_string(),
            });
        }
        match self.read_chunk_file(id) {
            Ok((recovered, bytes)) => {
                if recovered.id != *id
                    || recovered.checksum != ContentId::for_bytes(&bytes).digest().expect("digest")
                    || recovered.bytes != bytes.len() as u64
                {
                    self.quarantine(id, "chunk checksum mismatch")?;
                    return Ok(ChunkDecision::Quarantined {
                        id: id.clone(),
                        reason: "chunk checksum mismatch".to_string(),
                    });
                }
                Ok(ChunkDecision::ReadVerified {
                    id: id.clone(),
                    bytes: recovered.bytes,
                    checksum: recovered.checksum,
                })
            }
            Err(err) if err.code == ErrorCode::CorruptionDetected => {
                self.quarantine(id, &err.message)?;
                Ok(ChunkDecision::Quarantined {
                    id: id.clone(),
                    reason: err.message,
                })
            }
            Err(err) => Err(err),
        }
    }

    pub fn read_bytes_verified(&mut self, id: &ContentId) -> RsfResult<Bytes> {
        match self.read_verified(id)? {
            ChunkDecision::ReadVerified { .. } => self.read_chunk_bytes(id),
            ChunkDecision::Rejected { reason } => {
                Err(RsfError::new(ErrorCode::InvalidInput, reason))
            }
            ChunkDecision::Quarantined { reason, .. } => {
                Err(RsfError::new(ErrorCode::CorruptionDetected, reason))
            }
            other => Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("unexpected chunk read decision {other:?}"),
            )),
        }
    }

    pub fn pin(&mut self, id: &ContentId) -> RsfResult<ChunkDecision> {
        self.ensure_healthy()?;
        let Some(meta) = self.chunks.get(id).cloned() else {
            return Ok(reject("chunk is not present"));
        };
        if meta.quarantined {
            return Ok(ChunkDecision::Quarantined {
                id: id.clone(),
                reason: "chunk is quarantined".to_string(),
            });
        }
        let bytes = self.read_chunk_bytes(id)?;
        let mut updated = meta;
        updated.pin_count = updated.pin_count.saturating_add(1);
        self.write_chunk_file(&updated, &bytes)?;
        self.chunks.insert(id.clone(), updated.clone());
        Ok(ChunkDecision::Pinned {
            id: id.clone(),
            pins: updated.pin_count,
        })
    }

    pub fn release_pin(&mut self, id: &ContentId) -> RsfResult<ChunkDecision> {
        self.ensure_healthy()?;
        let Some(meta) = self.chunks.get(id).cloned() else {
            return Ok(reject("chunk is not present"));
        };
        if meta.pin_count == 0 {
            return Ok(reject("chunk has no pins to release"));
        }
        let bytes = self.read_chunk_bytes(id)?;
        let mut updated = meta;
        updated.pin_count -= 1;
        self.write_chunk_file(&updated, &bytes)?;
        self.chunks.insert(id.clone(), updated.clone());
        Ok(ChunkDecision::Released {
            id: id.clone(),
            remaining_pins: updated.pin_count,
        })
    }

    pub fn verify_all(&mut self) -> RsfResult<ChunkStoreReport> {
        let ids = self.chunks.keys().cloned().collect::<Vec<_>>();
        for id in ids {
            let _ = self.read_verified(&id)?;
        }
        let mut total_bytes = 0;
        let mut pinned = 0;
        let mut quarantined = 0;
        for chunk in self.chunks.values() {
            total_bytes += chunk.bytes;
            if chunk.pin_count > 0 {
                pinned += 1;
            }
            if chunk.quarantined {
                quarantined += 1;
            }
        }
        Ok(ChunkStoreReport {
            chunks: self.chunks.len(),
            pinned,
            total_bytes,
            quarantined,
        })
    }

    pub fn get(&self, id: &ContentId) -> Option<&DurableChunkMeta> {
        self.chunks.get(id)
    }

    pub fn quarantine_entries(&self) -> &[QuarantineEntry] {
        &self.quarantines
    }

    fn recover(&mut self) -> RsfResult<()> {
        for entry in fs::read_dir(self.root.join(CHUNK_DIR)).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("chunk") {
                continue;
            }
            match read_chunk_file_at(&path) {
                Ok((meta, bytes)) => {
                    let expected_name = chunk_file_name(&meta.id);
                    if path.file_name().and_then(|name| name.to_str()) != Some(&expected_name) {
                        self.quarantine_file(
                            &path,
                            path.file_name()
                                .and_then(|name| name.to_str())
                                .unwrap_or("chunk"),
                            Some(meta.id),
                            "chunk file name does not match content id",
                        )?;
                        continue;
                    }
                    if meta.checksum != ContentId::for_bytes(&bytes).digest().expect("digest") {
                        self.quarantine_file(
                            &path,
                            &expected_name,
                            Some(meta.id),
                            "chunk checksum mismatch",
                        )?;
                        continue;
                    }
                    self.chunks.insert(meta.id.clone(), meta);
                }
                Err(err) => {
                    let name = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("chunk");
                    self.quarantine_file(&path, name, None, &err.message)?;
                }
            }
        }
        Ok(())
    }

    fn read_chunk_file(&self, id: &ContentId) -> RsfResult<(DurableChunkMeta, Bytes)> {
        read_chunk_file_at(&self.chunk_path(id))
    }

    fn read_chunk_bytes(&self, id: &ContentId) -> RsfResult<Bytes> {
        let (_, bytes) = self.read_chunk_file(id)?;
        Ok(bytes)
    }

    fn write_chunk_file(&mut self, meta: &DurableChunkMeta, bytes: &[u8]) -> RsfResult<()> {
        self.ensure_healthy()?;
        let result = self.write_chunk_file_inner(meta, bytes);
        if let Err(err) = &result {
            self.failure = Some(err.message.clone());
        }
        result
    }

    fn write_chunk_file_inner(&self, meta: &DurableChunkMeta, bytes: &[u8]) -> RsfResult<()> {
        if bytes.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "chunk payload must not be empty",
            ));
        }
        if ContentId::for_bytes(bytes) != meta.id {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "chunk payload does not match content id",
            ));
        }

        let final_path = self.chunk_path(&meta.id);
        let tmp_path = self.temp_chunk_path(&meta.id);
        let encoded = encode_chunk_file(meta, bytes)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)
            .map_err(io_error)?;
        file.write_all(&encoded).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        drop(file);
        fs::rename(&tmp_path, &final_path).map_err(io_error)?;
        sync_dir(&self.root.join(CHUNK_DIR))
    }

    fn quarantine(&mut self, id: &ContentId, reason: &str) -> RsfResult<()> {
        if reason.is_empty() {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine reason must not be empty",
            ));
        }
        let path = self.chunk_path(id);
        if path.exists() {
            self.quarantine_file(&path, &chunk_file_name(id), Some(id.clone()), reason)?;
        }
        if let Some(meta) = self.chunks.get_mut(id) {
            meta.quarantined = true;
        }
        Ok(())
    }

    fn quarantine_file(
        &mut self,
        path: &Path,
        source_name: &str,
        id: Option<ContentId>,
        reason: &str,
    ) -> RsfResult<()> {
        if reason.is_empty() {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine reason must not be empty",
            ));
        }
        let corrupt_path = quarantine_path(&self.root, path, source_name)?;
        let entry = QuarantineEntry {
            id,
            reason: reason.to_string(),
            source_name: source_name.to_string(),
            manifest_path: quarantine_manifest_path(&corrupt_path),
            corrupt_path,
            recorded_at_unix_millis: unix_millis_now()?,
        };
        write_quarantine_manifest(&entry)?;
        self.upsert_quarantine(entry);
        Ok(())
    }

    fn upsert_quarantine(&mut self, entry: QuarantineEntry) {
        if let Some(existing) = self
            .quarantines
            .iter_mut()
            .find(|existing| existing.corrupt_path == entry.corrupt_path)
        {
            *existing = entry;
        } else {
            self.quarantines.push(entry);
        }
        self.quarantines
            .sort_by(|left, right| left.corrupt_path.cmp(&right.corrupt_path));
    }

    fn load_quarantines(&mut self) -> RsfResult<()> {
        for entry in fs::read_dir(self.root.join(QUARANTINE_DIR)).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("quarantine") {
                continue;
            }
            self.upsert_quarantine(read_quarantine_manifest(&path)?);
        }
        Ok(())
    }

    fn chunk_path(&self, id: &ContentId) -> PathBuf {
        self.root.join(CHUNK_DIR).join(chunk_file_name(id))
    }

    fn temp_chunk_path(&self, id: &ContentId) -> PathBuf {
        self.root.join(CHUNK_DIR).join(format!(
            ".{}.{}.tmp",
            hex_digest(id.as_bytes()),
            std::process::id()
        ))
    }

    #[cfg(test)]
    fn corrupt_for_test(&self, id: &ContentId) {
        let path = self.chunk_path(id);
        let mut bytes = fs::read(&path).expect("chunk file");
        let last = bytes.len() - 1;
        bytes[last] = bytes[last].wrapping_add(1);
        fs::write(path, bytes).expect("corrupt chunk file");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkStoreReport {
    pub chunks: usize,
    pub pinned: usize,
    pub total_bytes: u64,
    pub quarantined: usize,
}

impl ChunkDecision {
    pub fn id(&self) -> Option<&ContentId> {
        match self {
            Self::Stored { id, .. }
            | Self::ReadVerified { id, .. }
            | Self::Pinned { id, .. }
            | Self::Released { id, .. }
            | Self::Quarantined { id, .. } => Some(id),
            Self::Rejected { .. } => None,
        }
    }
}

fn reject(reason: impl Into<String>) -> ChunkDecision {
    ChunkDecision::Rejected {
        reason: reason.into(),
    }
}

pub fn read_bytes_verified(store: &ChunkStore, id: &ContentId) -> RsfResult<Bytes> {
    let Some(chunk) = store.get(id) else {
        return Err(RsfError::new(
            ErrorCode::InvalidInput,
            "chunk is not present",
        ));
    };
    if chunk.quarantined
        || chunk.checksum != ContentId::for_bytes(&chunk.bytes).digest().expect("digest")
    {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk checksum verification failed",
        ));
    }
    Ok(chunk.bytes.clone())
}

fn encode_chunk_file(meta: &DurableChunkMeta, bytes: &[u8]) -> RsfResult<Bytes> {
    let namespace = meta.namespace.as_bytes();
    let namespace_len = u16::try_from(namespace.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "chunk namespace is too large for durable encoding",
        )
    })?;
    if meta.bytes != bytes.len() as u64 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk metadata byte length does not match payload",
        ));
    }
    if meta.checksum != meta.id.digest().expect("content id digest") {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk metadata checksum does not match content id",
        ));
    }

    let mut encoded = Vec::with_capacity(64 + namespace.len() + bytes.len());
    encoded.extend_from_slice(CHUNK_MAGIC);
    encoded.push(CHUNK_VERSION);
    encoded.push(class_to_u8(meta.class));
    encoded.extend_from_slice(&namespace_len.to_le_bytes());
    encoded.extend_from_slice(&meta.bytes.to_le_bytes());
    encoded.extend_from_slice(&meta.ref_count.to_le_bytes());
    encoded.extend_from_slice(&meta.pin_count.to_le_bytes());
    encoded.extend_from_slice(meta.checksum.as_bytes());
    encoded.extend_from_slice(namespace);
    encoded.extend_from_slice(bytes);
    Ok(encoded)
}

fn read_chunk_file_at(path: &Path) -> RsfResult<(DurableChunkMeta, Bytes)> {
    let mut file = File::open(path).map_err(io_error)?;
    let mut encoded = Vec::new();
    file.read_to_end(&mut encoded).map_err(io_error)?;
    decode_chunk_file(&encoded)
}

fn decode_chunk_file(encoded: &[u8]) -> RsfResult<(DurableChunkMeta, Bytes)> {
    let mut cursor = Cursor::new(encoded);
    let mut magic = [0_u8; 8];
    cursor.read_exact(&mut magic).map_err(corrupt_file)?;
    if &magic != CHUNK_MAGIC {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk file magic is invalid",
        ));
    }
    let version = read_u8(&mut cursor)?;
    if version != CHUNK_VERSION {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("unsupported chunk file version {version}"),
        ));
    }
    let class = class_from_u8(read_u8(&mut cursor)?)?;
    let namespace_len = read_u16(&mut cursor)? as usize;
    let payload_len = read_u64(&mut cursor)? as usize;
    let ref_count = read_u32(&mut cursor)?;
    let pin_count = read_u32(&mut cursor)?;
    let mut checksum_bytes = [0_u8; 32];
    cursor
        .read_exact(&mut checksum_bytes)
        .map_err(corrupt_file)?;
    let checksum = Digest::blake3(checksum_bytes)?;
    let mut namespace = vec![0_u8; namespace_len];
    cursor.read_exact(&mut namespace).map_err(corrupt_file)?;
    let mut bytes = vec![0_u8; payload_len];
    cursor.read_exact(&mut bytes).map_err(corrupt_file)?;
    if cursor.position() != encoded.len() as u64 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk file has trailing bytes",
        ));
    }
    let namespace = String::from_utf8(namespace).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk namespace is not valid utf-8",
        )
    })?;
    let id = ContentId::for_bytes(&bytes);
    if checksum != id.digest().expect("content id digest") {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "chunk checksum mismatch",
        ));
    }
    Ok((
        DurableChunkMeta {
            id,
            class,
            namespace,
            bytes: payload_len as u64,
            ref_count,
            pin_count,
            checksum,
            quarantined: false,
        },
        bytes,
    ))
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> RsfResult<u8> {
    let mut bytes = [0_u8; 1];
    cursor.read_exact(&mut bytes).map_err(corrupt_file)?;
    Ok(bytes[0])
}

fn read_u16(cursor: &mut Cursor<&[u8]>) -> RsfResult<u16> {
    let mut bytes = [0_u8; 2];
    cursor.read_exact(&mut bytes).map_err(corrupt_file)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> RsfResult<u32> {
    let mut bytes = [0_u8; 4];
    cursor.read_exact(&mut bytes).map_err(corrupt_file)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> RsfResult<u64> {
    let mut bytes = [0_u8; 8];
    cursor.read_exact(&mut bytes).map_err(corrupt_file)?;
    Ok(u64::from_le_bytes(bytes))
}

fn class_to_u8(class: ChunkClass) -> u8 {
    match class {
        ChunkClass::RelationalLargeValue => 1,
        ChunkClass::ObjectBody => 2,
        ChunkClass::BlobPayload => 3,
        ChunkClass::SnapshotBlock => 4,
        ChunkClass::SstableBlock => 5,
        ChunkClass::BackupBlock => 6,
    }
}

fn class_from_u8(value: u8) -> RsfResult<ChunkClass> {
    match value {
        1 => Ok(ChunkClass::RelationalLargeValue),
        2 => Ok(ChunkClass::ObjectBody),
        3 => Ok(ChunkClass::BlobPayload),
        4 => Ok(ChunkClass::SnapshotBlock),
        5 => Ok(ChunkClass::SstableBlock),
        6 => Ok(ChunkClass::BackupBlock),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            format!("unknown chunk class {value}"),
        )),
    }
}

fn chunk_file_name(id: &ContentId) -> String {
    format!("{}.chunk", hex_digest(id.as_bytes()))
}

fn hex_digest(digest: &[u8; 32]) -> String {
    hex_bytes(digest)
}

fn quarantine_path(root: &Path, path: &Path, name: &str) -> RsfResult<PathBuf> {
    let quarantine_dir = root.join(QUARANTINE_DIR);
    fs::create_dir_all(&quarantine_dir).map_err(io_error)?;
    let final_path = quarantine_dir.join(format!("{name}.corrupt"));
    let _ = fs::remove_file(&final_path);
    fs::rename(path, &final_path).map_err(io_error)?;
    sync_dir(&quarantine_dir)?;
    Ok(final_path)
}

fn quarantine_manifest_path(corrupt_path: &Path) -> PathBuf {
    corrupt_path.with_extension("quarantine")
}

fn write_quarantine_manifest(entry: &QuarantineEntry) -> RsfResult<()> {
    let id = entry
        .id
        .as_ref()
        .map(|id| hex_bytes(id.as_bytes()))
        .unwrap_or_else(|| "none".to_string());
    let encoded = format!(
        "rsf-quarantine-v1\nid={id}\nsource={}\ncorrupt={}\nreason={}\nrecorded_at_unix_millis={}\n",
        hex_bytes(entry.source_name.as_bytes()),
        hex_bytes(entry.corrupt_path.to_string_lossy().as_bytes()),
        hex_bytes(entry.reason.as_bytes()),
        entry.recorded_at_unix_millis
    );
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&entry.manifest_path)
        .map_err(io_error)?;
    file.write_all(encoded.as_bytes()).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    if let Some(parent) = entry.manifest_path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn read_quarantine_manifest(path: &Path) -> RsfResult<QuarantineEntry> {
    let manifest = fs::read_to_string(path).map_err(io_error)?;
    let mut lines = manifest.lines();
    if lines.next() != Some("rsf-quarantine-v1") {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "quarantine manifest version is invalid",
        ));
    }
    let mut id = None;
    let mut source_name = None;
    let mut corrupt_path = None;
    let mut reason = None;
    let mut recorded_at_unix_millis = None;
    for line in lines {
        let Some((key, value)) = line.split_once('=') else {
            return Err(RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine manifest line is malformed",
            ));
        };
        match key {
            "id" if value == "none" => id = None,
            "id" => {
                let bytes = hex_decode(value)?;
                id = Some(ContentId::from_blake3(bytes_to_digest(&bytes)?)?);
            }
            "source" => source_name = Some(string_from_hex(value)?),
            "corrupt" => corrupt_path = Some(PathBuf::from(string_from_hex(value)?)),
            "reason" => reason = Some(string_from_hex(value)?),
            "recorded_at_unix_millis" => {
                recorded_at_unix_millis = Some(value.parse::<u128>().map_err(|err| {
                    RsfError::new(
                        ErrorCode::CorruptionDetected,
                        format!("quarantine manifest timestamp is invalid: {err}"),
                    )
                })?);
            }
            _ => {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!("unknown quarantine manifest key {key}"),
                ));
            }
        }
    }
    Ok(QuarantineEntry {
        id,
        reason: reason.ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine manifest is missing reason",
            )
        })?,
        source_name: source_name.ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine manifest is missing source",
            )
        })?,
        corrupt_path: corrupt_path.ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine manifest is missing corrupt path",
            )
        })?,
        manifest_path: path.to_path_buf(),
        recorded_at_unix_millis: recorded_at_unix_millis.ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "quarantine manifest is missing timestamp",
            )
        })?,
    })
}

fn unix_millis_now() -> RsfResult<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                format!("system clock is before unix epoch: {err}"),
            )
        })?
        .as_millis())
}

fn string_from_hex(value: &str) -> RsfResult<String> {
    String::from_utf8(hex_decode(value)?).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "quarantine manifest value is not valid utf-8",
        )
    })
}

fn bytes_to_digest(bytes: &[u8]) -> RsfResult<[u8; 32]> {
    <[u8; 32]>::try_from(bytes).map_err(|_| {
        RsfError::new(
            ErrorCode::CorruptionDetected,
            "quarantine manifest content id length is invalid",
        )
    })
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn hex_decode(value: &str) -> RsfResult<Bytes> {
    if value.len() % 2 != 0 {
        return Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex value has odd length",
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> RsfResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(RsfError::new(
            ErrorCode::CorruptionDetected,
            "hex value contains a non-hex character",
        )),
    }
}

fn sync_dir(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(io_error)
}

fn io_error(err: std::io::Error) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, err.to_string())
}

fn corrupt_file(err: std::io::Error) -> RsfError {
    RsfError::new(
        ErrorCode::CorruptionDetected,
        format!("chunk file is truncated or malformed: {err}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn stores_and_reads_verified_chunk() {
        let mut store = ChunkStore::default();
        let decision = store.put(ChunkWrite::new(
            ChunkClass::ObjectBody,
            "app",
            b"payload".to_vec(),
            true,
        ));
        let id = decision.id().expect("stored id").clone();

        assert!(matches!(
            decision,
            ChunkDecision::Stored {
                deduped: false,
                pins: 1,
                ..
            }
        ));
        assert_eq!(
            store.read_verified(&id),
            ChunkDecision::ReadVerified {
                checksum: id.digest().expect("digest"),
                id,
                bytes: 7
            }
        );
    }

    #[test]
    fn stored_chunk_uses_core_blake3_content_id_and_tagged_checksum() {
        let mut store = ChunkStore::default();
        let decision = store.put(ChunkWrite::new(
            ChunkClass::ObjectBody,
            "app",
            b"payload".to_vec(),
            true,
        ));
        let id = decision.id().expect("stored id").clone();
        let chunk = store.get(&id).expect("chunk");

        assert_eq!(id, ContentId::for_bytes(b"payload"));
        assert_eq!(chunk.checksum, id.digest().expect("digest"));
    }

    #[test]
    fn duplicate_payload_dedupes_and_increments_pin_count() {
        let mut store = ChunkStore::default();
        let first = store.put(ChunkWrite::new(
            ChunkClass::BlobPayload,
            "app",
            b"payload".to_vec(),
            true,
        ));
        let id = first.id().expect("stored id").clone();
        let second = store.put(ChunkWrite::new(
            ChunkClass::BlobPayload,
            "app",
            b"payload".to_vec(),
            true,
        ));

        assert!(matches!(
            second,
            ChunkDecision::Stored {
                deduped: true,
                pins: 2,
                ..
            }
        ));
        assert_eq!(store.verify_all().chunks, 1);
        assert_eq!(store.get(&id).expect("chunk").ref_count, 2);
    }

    #[test]
    fn release_pin_tracks_remaining_pins() {
        let mut store = ChunkStore::default();
        let id = store
            .put(ChunkWrite::new(
                ChunkClass::SnapshotBlock,
                "app",
                b"payload".to_vec(),
                true,
            ))
            .id()
            .expect("stored id")
            .clone();

        assert_eq!(
            store.release_pin(&id),
            ChunkDecision::Released {
                id,
                remaining_pins: 0
            }
        );
    }

    #[test]
    fn explicit_pin_increments_pin_count() {
        let mut store = ChunkStore::default();
        let id = store
            .put(ChunkWrite::new(
                ChunkClass::SnapshotBlock,
                "app",
                b"payload".to_vec(),
                false,
            ))
            .id()
            .expect("stored id")
            .clone();

        assert_eq!(
            store.pin(&id),
            ChunkDecision::Pinned {
                id: id.clone(),
                pins: 1
            }
        );
        assert_eq!(
            store.release_pin(&id),
            ChunkDecision::Released {
                id,
                remaining_pins: 0
            }
        );
    }

    #[test]
    fn explicit_quarantine_blocks_verified_reads() {
        let mut store = ChunkStore::default();
        let id = store
            .put(ChunkWrite::new(
                ChunkClass::ObjectBody,
                "app",
                b"payload".to_vec(),
                true,
            ))
            .id()
            .expect("stored id")
            .clone();

        assert_eq!(
            store.quarantine(&id, "operator isolated unhealthy replica"),
            ChunkDecision::Quarantined {
                id: id.clone(),
                reason: "operator isolated unhealthy replica".to_string()
            }
        );
        assert!(matches!(
            store.read_verified(&id),
            ChunkDecision::Quarantined { reason, .. } if reason == "chunk is quarantined"
        ));
        assert!(read_bytes_verified(&store, &id).is_err());
    }

    #[test]
    fn corrupt_chunk_is_quarantined_on_verified_read() {
        let mut store = ChunkStore::default();
        let id = store
            .put(ChunkWrite::new(
                ChunkClass::SstableBlock,
                "app",
                b"payload".to_vec(),
                false,
            ))
            .id()
            .expect("stored id")
            .clone();
        store.corrupt_for_test(&id);

        assert!(matches!(
            store.read_verified(&id),
            ChunkDecision::Quarantined { reason, .. } if reason.contains("checksum")
        ));
        assert!(read_bytes_verified(&store, &id).is_err());
    }

    #[test]
    fn rejects_empty_payloads() {
        let mut store = ChunkStore::default();

        assert!(matches!(
            store.put(ChunkWrite::new(
                ChunkClass::BackupBlock,
                "app",
                Vec::new(),
                false
            )),
            ChunkDecision::Rejected { reason } if reason.contains("payload")
        ));
    }

    #[test]
    fn durable_chunk_store_recovers_verified_chunks_after_restart() {
        let root = test_root("durable_chunk_store_recovers_verified_chunks_after_restart");
        let id = {
            let mut store = DurableChunkStore::open(&root).expect("open durable store");
            let stored = store
                .put(ChunkWrite::new(
                    ChunkClass::ObjectBody,
                    "app",
                    b"payload".to_vec(),
                    true,
                ))
                .expect("put");
            let id = stored.id().expect("stored id").clone();
            assert!(matches!(
                stored,
                ChunkDecision::Stored {
                    deduped: false,
                    pins: 1,
                    ..
                }
            ));
            assert_eq!(
                store.read_bytes_verified(&id).expect("read verified"),
                b"payload".to_vec()
            );
            id
        };

        let mut reopened = DurableChunkStore::open(&root).expect("reopen durable store");

        assert_eq!(
            reopened.get(&id).expect("recovered metadata").checksum,
            id.digest().expect("digest")
        );
        assert_eq!(
            reopened.read_verified(&id).expect("verified"),
            ChunkDecision::ReadVerified {
                checksum: id.digest().expect("digest"),
                id,
                bytes: 7
            }
        );
        let report = reopened.verify_all().expect("report");
        assert_eq!(report.chunks, 1);
        assert_eq!(report.pinned, 1);
        assert_eq!(report.total_bytes, 7);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_chunk_store_persists_dedupe_and_pin_updates() {
        let root = test_root("durable_chunk_store_persists_dedupe_and_pin_updates");
        let id = {
            let mut store = DurableChunkStore::open(&root).expect("open durable store");
            let first = store
                .put(ChunkWrite::new(
                    ChunkClass::BlobPayload,
                    "app",
                    b"payload".to_vec(),
                    true,
                ))
                .expect("first put");
            let id = first.id().expect("stored id").clone();
            let second = store
                .put(ChunkWrite::new(
                    ChunkClass::BlobPayload,
                    "app",
                    b"payload".to_vec(),
                    true,
                ))
                .expect("second put");
            assert!(matches!(
                second,
                ChunkDecision::Stored {
                    deduped: true,
                    pins: 2,
                    ..
                }
            ));
            store.release_pin(&id).expect("release");
            id
        };

        let reopened = DurableChunkStore::open(&root).expect("reopen durable store");
        let meta = reopened.get(&id).expect("metadata");
        assert_eq!(meta.ref_count, 2);
        assert_eq!(meta.pin_count, 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn ensure_pinned_is_idempotent_and_exclusively_owned() {
        let root = test_root("ensure_pinned_is_idempotent_and_exclusively_owned");
        let write = || ChunkWrite::new(ChunkClass::BlobPayload, "blob", b"payload".to_vec(), true);
        let mut store = DurableChunkStore::open(&root).unwrap();
        assert!(DurableChunkStore::open(&root).is_err());
        let id = store.ensure_pinned(write()).unwrap().id().unwrap().clone();
        for _ in 0..4 {
            store.ensure_pinned(write()).unwrap();
        }
        assert_eq!(store.get(&id).unwrap().ref_count, 1);
        assert_eq!(store.get(&id).unwrap().pin_count, 1);
        drop(store);
        let mut store = DurableChunkStore::open(&root).unwrap();
        store.ensure_pinned(write()).unwrap();
        assert_eq!(store.get(&id).unwrap().ref_count, 1);
        assert_eq!(store.get(&id).unwrap().pin_count, 1);
        store.release_pin(&id).unwrap();
        store.ensure_pinned(write()).unwrap();
        assert_eq!(store.get(&id).unwrap().pin_count, 1);
        assert_eq!(store.get(&id).unwrap().ref_count, 1);
    }

    #[test]
    fn failed_chunk_persistence_fences_the_writer() {
        let root = test_root("failed_chunk_persistence_fences_the_writer");
        let mut store = DurableChunkStore::open(&root).unwrap();
        let directory = root.join(CHUNK_DIR);
        let displaced = root.join("displaced");
        fs::rename(&directory, &displaced).unwrap();
        fs::write(&directory, b"not a directory").unwrap();
        let write = || ChunkWrite::new(ChunkClass::BlobPayload, "blob", b"payload".to_vec(), true);
        assert!(store.ensure_pinned(write()).is_err());
        fs::remove_file(&directory).unwrap();
        fs::rename(displaced, directory).unwrap();
        assert!(store.ensure_pinned(write()).is_err());
        drop(store);
        let mut store = DurableChunkStore::open(&root).unwrap();
        assert!(store.ensure_pinned(write()).is_ok());
    }

    #[test]
    fn durable_chunk_store_dedupes_after_restart() {
        let root = test_root("durable_chunk_store_dedupes_after_restart");
        let id = {
            let mut store = DurableChunkStore::open(&root).expect("open durable store");
            store
                .put(ChunkWrite::new(
                    ChunkClass::BlobPayload,
                    "app",
                    b"payload".to_vec(),
                    true,
                ))
                .expect("first put")
                .id()
                .expect("stored id")
                .clone()
        };

        let mut reopened = DurableChunkStore::open(&root).expect("reopen durable store");
        let second = reopened
            .put(ChunkWrite::new(
                ChunkClass::BlobPayload,
                "app",
                b"payload".to_vec(),
                true,
            ))
            .expect("second put");

        assert!(matches!(
            second,
            ChunkDecision::Stored {
                deduped: true,
                pins: 2,
                ..
            }
        ));
        let meta = reopened.get(&id).expect("metadata");
        assert_eq!(meta.ref_count, 2);
        assert_eq!(meta.pin_count, 2);
        assert_eq!(reopened.verify_all().expect("report").chunks, 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_chunk_store_persists_explicit_pins_after_restart() {
        let root = test_root("durable_chunk_store_persists_explicit_pins_after_restart");
        let id = {
            let mut store = DurableChunkStore::open(&root).expect("open durable store");
            let id = store
                .put(ChunkWrite::new(
                    ChunkClass::SnapshotBlock,
                    "app",
                    b"payload".to_vec(),
                    false,
                ))
                .expect("put")
                .id()
                .expect("stored id")
                .clone();
            assert_eq!(
                store.pin(&id).expect("pin"),
                ChunkDecision::Pinned {
                    id: id.clone(),
                    pins: 1
                }
            );
            assert_eq!(
                store.pin(&id).expect("pin again"),
                ChunkDecision::Pinned {
                    id: id.clone(),
                    pins: 2
                }
            );
            id
        };

        let mut reopened = DurableChunkStore::open(&root).expect("reopen durable store");
        assert_eq!(reopened.get(&id).expect("metadata").pin_count, 2);
        assert_eq!(
            reopened.release_pin(&id).expect("release"),
            ChunkDecision::Released {
                id,
                remaining_pins: 1
            }
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_chunk_store_quarantines_corrupt_chunk_files() {
        let root = test_root("durable_chunk_store_quarantines_corrupt_chunk_files");
        let mut store = DurableChunkStore::open(&root).expect("open durable store");
        let id = store
            .put(ChunkWrite::new(
                ChunkClass::SstableBlock,
                "app",
                b"payload".to_vec(),
                false,
            ))
            .expect("put")
            .id()
            .expect("stored id")
            .clone();
        store.corrupt_for_test(&id);

        assert!(matches!(
            store.read_verified(&id).expect("read corrupt"),
            ChunkDecision::Quarantined { reason, .. } if reason.contains("checksum")
        ));
        assert_eq!(store.verify_all().expect("report").quarantined, 1);
        assert!(
            std::fs::read_dir(root.join(QUARANTINE_DIR))
                .expect("quarantine")
                .any(|entry| entry
                    .expect("entry")
                    .path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    == Some("corrupt"))
        );
        let entries = store.quarantine_entries().to_vec();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id.as_ref(), Some(&id));
        assert!(entries[0].reason.contains("checksum"));
        assert_eq!(entries[0].source_name, chunk_file_name(&id));
        assert!(entries[0].corrupt_path.exists());
        assert!(entries[0].manifest_path.exists());

        drop(store);
        let reopened = DurableChunkStore::open(&root).expect("reopen durable store");
        assert_eq!(reopened.quarantine_entries(), entries);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_chunk_store_quarantines_malformed_files_during_recovery() {
        let root = test_root("durable_chunk_store_quarantines_malformed_files_during_recovery");
        let chunk_dir = root.join(CHUNK_DIR);
        std::fs::create_dir_all(&chunk_dir).expect("chunk dir");
        std::fs::write(chunk_dir.join("broken.chunk"), b"not-a-valid-chunk-file")
            .expect("write malformed chunk");

        let mut store = DurableChunkStore::open(&root).expect("open durable store");

        assert!(
            store
                .get(&ContentId::for_bytes(b"not-a-valid-chunk-file"))
                .is_none()
        );
        assert_eq!(store.verify_all().expect("report").chunks, 0);
        let entries = store.quarantine_entries().to_vec();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, None);
        assert_eq!(entries[0].source_name, "broken.chunk");
        assert!(entries[0].reason.contains("magic") || entries[0].reason.contains("malformed"));
        assert!(entries[0].corrupt_path.exists());
        assert!(entries[0].manifest_path.exists());

        drop(store);
        let reopened = DurableChunkStore::open(&root).expect("reopen durable store");
        assert_eq!(reopened.quarantine_entries(), entries);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn durable_chunk_store_rejects_pin_for_quarantined_chunk() {
        let root = test_root("durable_chunk_store_rejects_pin_for_quarantined_chunk");
        let mut store = DurableChunkStore::open(&root).expect("open durable store");
        let id = store
            .put(ChunkWrite::new(
                ChunkClass::SstableBlock,
                "app",
                b"payload".to_vec(),
                false,
            ))
            .expect("put")
            .id()
            .expect("stored id")
            .clone();
        store.corrupt_for_test(&id);
        assert!(matches!(
            store.read_verified(&id).expect("read corrupt"),
            ChunkDecision::Quarantined { .. }
        ));

        assert!(matches!(
            store.pin(&id).expect("pin quarantined"),
            ChunkDecision::Quarantined { .. }
        ));

        let _ = std::fs::remove_dir_all(root);
    }

    fn test_root(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rsf-chunkstore-{name}-{}-{nonce}",
            std::process::id()
        ))
    }
}
