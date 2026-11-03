use super::*;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const POINTER: &str = "VSR_ACTIVE";
const GENERATIONS: &str = "vsr-views";
const SEAL: &str = "VSR_SEAL";
const PUBLISHED: &str = "VSR_PUBLISHED";

#[derive(Clone, Copy)]
pub(crate) enum Boundary {
    Staged,
    Renamed,
    Published,
}

pub(crate) struct Active {
    pub path: PathBuf,
    pub plan: Option<[u8; 32]>,
    seal: Option<([u8; 32], u64)>,
}

fn io_error(err: std::io::Error) -> RsfError {
    corrupt(format!("VSR installation storage: {err}"))
}
fn sync(path: &Path) -> RsfResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(io_error)
}
fn checked_file(bytes: &[u8], magic: &[u8; 4], length: usize) -> RsfResult<()> {
    if bytes.len() != length
        || !bytes.starts_with(magic)
        || bytes[length - 32..] != Digest::blake3_bytes(&bytes[..length - 32]).bytes
    {
        return Err(corrupt("invalid VSR installation pointer or seal"));
    }
    Ok(())
}
fn checksum(bytes: &mut Vec<u8>) {
    bytes.extend_from_slice(Digest::blake3_bytes(bytes).as_bytes());
}
fn write_synced(path: &Path, bytes: &[u8]) -> RsfResult<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)
}

pub(crate) fn publication_marked(root: &Path) -> bool {
    root.join(PUBLISHED).is_file()
}

pub(crate) fn mark_published(root: &Path, pg: PlacementGroupId) -> RsfResult<()> {
    match fs::read(root.join(PUBLISHED)) {
        Ok(bytes) => {
            checked_file(&bytes, b"VSP1", 44)?;
            if read_u64(&bytes, 4)? != pg.0 {
                return Err(corrupt("VSR publication marker belongs to another group"));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(error)),
    }
    let mut bytes = b"VSP1".to_vec();
    bytes.extend_from_slice(&pg.0.to_le_bytes());
    checksum(&mut bytes);
    let temporary = root.join(".VSR_PUBLISHED.tmp");
    write_synced(&temporary, &bytes)?;
    fs::rename(temporary, root.join(PUBLISHED)).map_err(io_error)?;
    sync(root)
}

pub(crate) fn active(root: &Path, pg: PlacementGroupId) -> RsfResult<Active> {
    let bytes = match fs::read(root.join(POINTER)) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if crate::learner::promotion::has_claim(root) {
                return Err(corrupt("claimed promotion target has no active generation"));
            }
            if root.join(PUBLISHED).exists() {
                return Err(corrupt("published VSR generation lost its active pointer"));
            }
            if root.join(GENERATIONS).exists() && !root.join("MANIFEST").is_file() {
                return Err(corrupt(
                    "VSR generation history exists without root authority or an active pointer",
                ));
            }
            return Ok(Active {
                path: root.to_path_buf(),
                plan: None,
                seal: None,
            });
        }
        Err(err) => return Err(io_error(err)),
    };
    let promoted = bytes.starts_with(b"VSA2");
    checked_file(
        &bytes,
        if promoted { b"VSA2" } else { b"VSA1" },
        if promoted { 140 } else { 108 },
    )?;
    match (promoted, crate::learner::promotion::owner_digest(root)?) {
        (false, None) => {}
        (true, Some(owner)) if bytes[76..108] == owner => {}
        _ => return Err(corrupt("active pointer differs from promotion ownership")),
    }
    if read_u64(&bytes, 4)? != pg.0 {
        return Err(corrupt(
            "active VSR generation belongs to another placement group",
        ));
    }
    let id: [u8; 32] = bytes[12..44].try_into().unwrap();
    let plan: [u8; 32] = bytes[44..76].try_into().unwrap();
    let name = id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let path = root.join(GENERATIONS).join(name);
    if !fs::symlink_metadata(&path).map_err(io_error)?.is_dir() {
        return Err(corrupt(
            "active VSR generation must be an existing directory",
        ));
    }
    let seal = fs::read(path.join(SEAL)).map_err(io_error)?;
    checked_file(&seal, b"VSS1", 116)?;
    if read_u64(&seal, 4)? != pg.0 || seal[12..44] != id || seal[44..76] != plan {
        return Err(corrupt("active VSR generation seal differs from pointer"));
    }
    Ok(Active {
        path,
        plan: Some(plan),
        seal: Some((id, read_u64(&seal, 76)?)),
    })
}

pub(crate) fn validate_active(
    active: &Active,
    pg: PlacementGroupId,
    records: &[&rsf_core::storage::WalRecord],
) -> RsfResult<()> {
    let Some((id, count)) = active.seal else {
        return Ok(());
    };
    let count = usize::try_from(count)
        .map_err(|_| corrupt("VSR seal record count exceeds platform range"))?;
    let prefix = records
        .get(..count)
        .ok_or_else(|| corrupt("active generation lost its installation prefix"))?;
    let history = prefix
        .iter()
        .map(|record| VsrHistoryRecord {
            kind: record.kind,
            epoch: record.epoch,
            payload: record.payload.to_vec(),
        })
        .collect::<Vec<_>>();
    if prefix.iter().any(|record| record.pg != pg)
        || !prefix.last().is_some_and(|record| {
            record.kind == WalRecordKind::ViewInstallation && is_activation_marker(&record.payload)
        })
        || transfer::history_digest(&history)?.bytes != id
    {
        return Err(corrupt(
            "active VSR generation prefix does not match its sealed identity",
        ));
    }
    Ok(())
}

pub(crate) fn publish(
    root: &Path,
    pg: PlacementGroupId,
    options: rsf_storage::SegmentStoreOptions,
    plan_id: [u8; 32],
    records: &[VsrHistoryRecord],
    hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    publication_started: &mut bool,
) -> RsfResult<SegmentStore> {
    crate::learner::promotion::validate_history(
        root,
        pg,
        records.iter().map(|r| (r.kind, r.payload.as_slice())),
    )?;
    let id = transfer::history_digest(records)?.bytes;
    let parent = root.join(GENERATIONS);
    fs::create_dir_all(&parent).map_err(io_error)?;
    sync(root)?;
    let name = id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let path = parent.join(name);
    fs::create_dir_all(&path).map_err(io_error)?;
    sync(&parent)?;
    let (mut store, previous) = SegmentStore::open_recovered(&path, options)?;
    let existing = &previous.recovery.wal_records;
    if existing.len() > records.len()
        || existing.iter().zip(records).any(|(old, next)| {
            old.pg != pg
                || old.epoch != next.epoch
                || old.kind != next.kind
                || old.payload.as_slice() != next.payload.as_slice()
        })
    {
        return Err(corrupt("staged VSR generation conflicts with installation"));
    }
    for record in records.iter().skip(existing.len()) {
        store.append_next_wal(pg, record.epoch, record.kind, &record.payload)?;
    }
    let mut seal = b"VSS1".to_vec();
    seal.extend_from_slice(&pg.0.to_le_bytes());
    seal.extend_from_slice(&id);
    seal.extend_from_slice(&plan_id);
    seal.extend_from_slice(&(records.len() as u64).to_le_bytes());
    checksum(&mut seal);
    write_synced(&path.join(SEAL), &seal)?;
    sync(&path)?;
    hook(Boundary::Staged)?;
    let owner = crate::learner::promotion::owner_digest(root)?;
    let mut pointer = if owner.is_some() { b"VSA2" } else { b"VSA1" }.to_vec();
    pointer.extend_from_slice(&pg.0.to_le_bytes());
    pointer.extend_from_slice(&id);
    pointer.extend_from_slice(&plan_id);
    if let Some(owner) = owner {
        pointer.extend_from_slice(&owner);
    }
    checksum(&mut pointer);
    let temporary = root.join(".VSR_ACTIVE.tmp");
    write_synced(&temporary, &pointer)?;
    *publication_started = true;
    fs::rename(temporary, root.join(POINTER)).map_err(io_error)?;
    // Once rename may have occurred, any publication error fences this
    // handle until reopen resolves the authoritative pointer.
    let publication = hook(Boundary::Renamed)
        .and_then(|()| sync(root))
        .and_then(|()| mark_published(root, pg))
        .and_then(|()| hook(Boundary::Published));
    publication?;
    Ok(store)
}

impl DurableVsrCell {
    pub fn install_view(&mut self, plan: &VsrViewInstallation) -> RsfResult<()> {
        self.install_view_with_hook(plan, &mut |_| Ok(()))
    }

    pub(super) fn install_view_with_hook(
        &mut self,
        plan: &VsrViewInstallation,
        hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.ensure_healthy()?;
        if self.membership_fence.is_some() {
            return Err(invalid_input(
                "ordinary installation cannot replace a membership transition",
            ));
        }
        if self.installed_plan.is_some()
            && self.cell.view == plan.request.target_view
            && self.cell.primary == plan.request.candidate
            && self.view_change.is_none()
            && self.cell.replicas.iter().copied().collect::<Vec<_>>() == plan.request.voters
            && plan.records.first().map(|record| record.payload.as_slice())
                == Some(self.initial_bootstrap.as_slice())
            && self.history.compatible(&plan.recovered.1)
            && self.cell.committed_tail() >= plan.committed_tail()
            && plan.recovered.0.pending.values().all(|pending| {
                self.cell
                    .pending
                    .get(&pending.prepare.index)
                    .is_some_and(|current| same_operation(&current.prepare, &pending.prepare))
            })
        {
            // Retry identity is the installed decision, not its codec version
            // or the particular valid signer subsets used to encode it.
            self.reclaim_inactive_views()?;
            return Ok(());
        }
        if self.view_change.as_ref() != Some(&plan.request) {
            return Err(invalid_input(
                "installation does not match this replica's active promise",
            ));
        }
        if plan.records.first().map(|record| record.payload.as_slice())
            != Some(self.initial_bootstrap.as_slice())
            || !plan.recovered.1.compatible(&self.history)
        {
            return Err(corrupt(
                "installation would replace historical configuration authority",
            ));
        }
        for commit in &self.cell.committed {
            let selected =
                plan.recovered.0.pending.get(&commit.index).ok_or_else(|| {
                    corrupt("installation would drop a locally committed operation")
                })?;
            if !selected.committed || selected.prepare.operation != commit.certificate.operation {
                return Err(corrupt(
                    "installation conflicts with locally committed data",
                ));
            }
        }
        // Preserve receiver-local durable request receipts even when their
        // original primary was outside the election quorum.
        let mut records = plan.records.clone();
        let last = records
            .pop()
            .ok_or_else(|| corrupt("installation has no activation marker"))?;
        let mut marker = Installation::decode(&last.payload)?;
        for (request, index) in &self.cell.client_table {
            if plan
                .recovered
                .0
                .client_table
                .get(request)
                .is_some_and(|selected| selected != index)
            {
                return Err(corrupt("installation would remap a local client receipt"));
            }
            if plan.recovered.0.client_table.contains_key(request) {
                continue;
            }
            records.push(VsrHistoryRecord {
                kind: WalRecordKind::ViewInstallation,
                epoch: marker.selected_report()?.history.probe.view,
                payload: codec::client_delta(&VsrClientRequestCommit {
                    pg: plan.request.pg,
                    request: request.clone(),
                    index: *index,
                })?,
            });
        }
        let start = marker.selected_report()?.history.records as usize;
        marker.delta_count = (records.len() - start) as u64;
        marker.delta_digest = transfer::history_digest(&records[start..])?;
        records.push(VsrHistoryRecord {
            payload: marker.encode()?,
            ..last
        });
        let recovered = recover_records(&records)?;
        self.publish_generation(plan.id, &records, recovered, hook)
    }

    // Callers validate the complete logical records with authoritative replay
    // before entering this shared physical publication transaction.
    pub(crate) fn publish_generation(
        &mut self,
        plan_id: [u8; 32],
        records: &[VsrHistoryRecord],
        recovered: replay::Recovered,
        hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
    ) -> RsfResult<()> {
        self.ensure_healthy()?;
        let next_revision = self.next_state_revision()?;
        let mut publication_started = false;
        let (store, store_custody) = match publish(
            &self.root,
            self.cell.pg,
            self.options,
            plan_id,
            records,
            hook,
            &mut publication_started,
        )
        .and_then(|store| {
            let custody = store.capture_owned_custody()?;
            Ok((store, custody))
        }) {
            Ok(owned) => owned,
            Err(error) => {
                if publication_started {
                    *self
                        .durability_failure
                        .lock()
                        .map_err(|_| corrupt("VSR durability fence is poisoned"))? =
                        Some(error.message.clone());
                }
                return Err(error);
            }
        };
        self.state_revision = next_revision;
        self.store = store;
        self.store_custody = store_custody;
        self.cell = recovered.0;
        self.history = recovered.1;
        self.view_change = recovered.2;
        self.membership_fence = recovered.3;
        self.installed_plan = Some(plan_id);
        self.quorum_history = true;
        self.reclaim_inactive_views()?;
        Ok(())
    }
    /// Reclaim only owned, inactive immutable generations after validating the
    /// current pointer. The original root WAL retains its downgrade fence.
    pub fn reclaim_inactive_views(&mut self) -> RsfResult<usize> {
        self.ensure_healthy()?;
        let current = active(&self.root, self.cell.pg)?;
        if current.plan.is_none() {
            return Ok(0);
        }
        let parent = self.root.join(GENERATIONS);
        let mut removed = 0;
        for entry in fs::read_dir(&parent).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.len() != 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                || !entry.file_type().map_err(io_error)?.is_dir()
                || entry.path() == current.path
            {
                continue;
            }
            fs::remove_dir_all(entry.path()).map_err(io_error)?;
            removed += 1;
        }
        sync(&parent)?;
        Ok(removed)
    }
}
