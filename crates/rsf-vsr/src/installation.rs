//! Quorum history selection and immutable generation installation.
use super::*;
use rsf_core::Digest;

mod codec;
pub(super) mod input;
pub use input::VsrInstallationInput;
pub(super) mod publication;
use codec::{Installation, apply_delta};

/// A plan can only be produced by replaying an entire quorum's frozen histories.
/// The caller must authenticate the reporting nodes; this is a crash-fault
/// protocol, and node IDs in public Rust values are not signatures.
#[derive(Debug)]
pub struct VsrViewInstallation {
    pub(super) request: VsrViewChangeRequest,
    pub(super) records: Vec<VsrHistoryRecord>,
    pub(super) recovered: replay::Recovered,
    pub(super) id: [u8; 32],
}

impl VsrViewInstallation {
    pub fn request(&self) -> &VsrViewChangeRequest {
        &self.request
    }
    pub fn id(&self) -> [u8; 32] {
        self.id
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.recovered.0.committed_tail()
    }
    pub fn accepted_tail(&self) -> LogIndex {
        LogIndex(self.recovered.0.next_index - 1)
    }

    pub fn select(snapshots: &[VsrViewChangeSnapshot]) -> RsfResult<Self> {
        let first = snapshots
            .first()
            .ok_or_else(|| invalid_input("view installation requires a quorum of reports"))?;
        let request = first.report.request.clone();
        request.encode()?;
        let mut voters = BTreeSet::new();
        let mut states = Vec::new();
        for snapshot in snapshots {
            if snapshot.report.request != request || !voters.insert(snapshot.report.replica) {
                return Err(invalid_input(
                    "view installation has mixed requests or duplicate voters",
                ));
            }
            states.push(snapshot.recover_state()?);
        }
        if voters.len() <= request.voters.len() / 2 {
            return Err(invalid_input(
                "view installation reports do not form a quorum",
            ));
        }
        let source = snapshots
            .iter()
            .enumerate()
            .max_by_key(|(_, snapshot)| {
                (
                    snapshot.report.history.probe.view,
                    snapshot.report.accepted_tail,
                    std::cmp::Reverse(snapshot.report.replica),
                )
            })
            .map(|(position, _)| position)
            .expect("nonempty quorum");
        let source_view = snapshots[source].report.history.probe.view;
        let source_epoch = states[source].0.view;
        for (position, state) in states.iter().enumerate() {
            if snapshots[position].records.first() != snapshots[source].records.first()
                || !states[source].1.compatible(&state.1)
            {
                return Err(corrupt(
                    "view-change histories disagree on configuration authority or bootstrap",
                ));
            }
            for (index, pending) in &state.0.pending {
                // In older normal views, only committed entries constrain the
                // chosen log. Among the newest normal views, accepted prefixes
                // must agree even when their reprepare headers differ.
                if !pending.committed && state.0.view != source_view {
                    continue;
                }
                let selected = states[source].0.pending.get(index).ok_or_else(|| {
                    corrupt("selected view-change log is missing a required operation")
                })?;
                if !same_operation(&selected.prepare, &pending.prepare) {
                    return Err(corrupt(
                        "view-change histories disagree on a required operation",
                    ));
                }
            }
        }
        let mut records = snapshots[source].records.clone();
        let (_, deltas) = merge_quorum(
            states,
            source,
            request.pg,
            source_epoch,
            WalRecordKind::ViewInstallation,
        )?;
        let marker = Installation {
            reports: snapshots
                .iter()
                .map(|snapshot| snapshot.report.clone())
                .collect(),
            source: snapshots[source].report.replica,
            delta_count: deltas.len() as u64,
            delta_digest: transfer::history_digest(&deltas)?,
        };
        records.extend(deltas);
        records.push(VsrHistoryRecord {
            epoch: source_epoch,
            kind: WalRecordKind::ViewInstallation,
            payload: marker.encode()?,
        });
        // Replay the exact persisted form, including source digest and delta
        // validation, before allowing the plan to reach a writer.
        let recovered = recover_records(&records)?;
        let id = transfer::history_digest(&records)?.bytes;
        Ok(Self {
            request,
            records,
            recovered,
            id,
        })
    }
}

pub(super) fn merge_quorum(
    mut states: Vec<replay::Recovered>,
    source: usize,
    pg: PlacementGroupId,
    source_epoch: Epoch,
    kind: WalRecordKind,
) -> RsfResult<(replay::Recovered, Vec<VsrHistoryRecord>)> {
    let mut selected = states.remove(source);
    let mut commits = BTreeMap::new();
    let mut clients = BTreeMap::new();
    for state in states.iter().chain(std::iter::once(&selected)) {
        for commit in &state.0.committed {
            match commits.entry(commit.index) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(commit.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let previous: &VsrCommit = entry.get();
                    if previous.certificate.operation != commit.certificate.operation {
                        return Err(corrupt("quorum committed histories conflict"));
                    }
                    if (commit.view, std::cmp::Reverse(commit.encode()?))
                        > (previous.view, std::cmp::Reverse(previous.encode()?))
                    {
                        entry.insert(commit.clone());
                    }
                }
            }
        }
        for (client, index) in &state.0.client_table {
            if let Some(previous) = clients.insert(client.clone(), *index)
                && previous != *index
            {
                return Err(corrupt("quorum client request mappings conflict"));
            }
        }
    }
    let mut deltas = Vec::new();
    for (index, commit) in commits {
        if index <= selected.0.committed_tail() {
            continue;
        }
        let record = VsrHistoryRecord {
            epoch: source_epoch,
            kind,
            payload: codec::learned_commit_delta(
                &VsrPrepare {
                    pg: commit.pg,
                    view: commit.view,
                    index: commit.index,
                    primary: commit.primary,
                    operation: commit.certificate.operation.clone(),
                    bytes: selected
                        .0
                        .pending
                        .get(&index)
                        .ok_or_else(|| {
                            corrupt("selected quorum prefix omits a committed operation")
                        })?
                        .prepare
                        .bytes,
                },
                &commit,
            )?,
        };
        apply_delta(&mut selected.0, &mut selected.1, &record.payload)?;
        deltas.push(record);
    }
    for (client, index) in clients {
        if selected.0.client_table.get(&client) == Some(&index) {
            continue;
        }
        let record = VsrHistoryRecord {
            epoch: source_epoch,
            kind,
            payload: codec::client_delta(&VsrClientRequestCommit {
                pg,
                request: client,
                index,
            })?,
        };
        apply_delta(&mut selected.0, &mut selected.1, &record.payload)?;
        deltas.push(record);
    }
    Ok((selected, deltas))
}

impl VsrViewChangeSnapshot {
    pub(super) fn recover_state(&self) -> RsfResult<replay::Recovered> {
        self.validate_integrity()?;
        let state = recover_records(&self.records)?;
        if state.2.as_ref() != Some(&self.report.request)
            || probe_for_cell(&state.0)? != self.report.history.probe
            || state.0.next_index.checked_sub(1).map(LogIndex) != Some(self.report.accepted_tail)
        {
            return Err(corrupt(
                "view-change report does not describe its replayed history",
            ));
        }
        Ok(state)
    }
}

pub(super) fn membership_client_delta(client: &VsrClientRequestCommit) -> RsfResult<Vec<u8>> {
    codec::client_delta(client)
}
pub(super) fn apply_membership_delta(
    cell: &mut VsrCell,
    history: &mut history::ConfigurationHistory,
    payload: &[u8],
) -> RsfResult<()> {
    apply_delta(cell, history, payload)
}

pub(super) fn same_operation(left: &VsrPrepare, right: &VsrPrepare) -> bool {
    left.pg == right.pg
        && left.index == right.index
        && left.bytes == right.bytes
        && left.operation == right.operation
}

pub(super) fn preserves_committed(new: &VsrCell, old: &VsrCell) -> bool {
    old.pending
        .values()
        .filter(|pending| pending.committed)
        .all(|old| {
            new.pending
                .get(&old.prepare.index)
                .is_some_and(|new| new.committed && same_operation(&new.prepare, &old.prepare))
        })
}

pub(super) fn recover_records(records: &[VsrHistoryRecord]) -> RsfResult<replay::Recovered> {
    let first = records
        .first()
        .ok_or_else(|| corrupt("installation history is empty"))?;
    if first.kind != WalRecordKind::Membership {
        return Err(corrupt("installation history has no bootstrap authority"));
    }
    let bootstrap = decode_bootstrap(&first.payload)?;
    if first.epoch != bootstrap.view {
        return Err(corrupt("installation bootstrap epoch mismatch"));
    }
    let borrowed = records
        .iter()
        .map(|record| replay::Record {
            kind: record.kind,
            epoch: record.epoch,
            payload: &record.payload,
        })
        .collect::<Vec<_>>();
    replay::recover(bootstrap, &borrowed)
}

pub(super) fn probe_for_cell(cell: &VsrCell) -> RsfResult<VsrPrimaryProbe> {
    let tail = cell.committed_tail();
    let tip = if tail.0 == 0 {
        Vec::new()
    } else {
        cell.pending
            .get(&tail)
            .ok_or_else(|| corrupt("committed tip has no prepare"))?
            .prepare
            .encode()?
    };
    Ok(VsrPrimaryProbe {
        pg: cell.pg,
        view: cell.view,
        primary: cell.primary,
        voters: cell.replicas.iter().copied().collect(),
        committed_tail: tail,
        committed_tip: Digest::blake3_bytes(&tip),
    })
}

pub(super) fn replay_installation(
    cell: &mut VsrCell,
    history: &mut history::ConfigurationHistory,
    promise: &mut Option<VsrViewChangeRequest>,
    records: &[replay::Record<'_>],
    position: usize,
    delta_start: &mut Option<usize>,
) -> RsfResult<()> {
    let record = records[position];
    let active = promise
        .as_ref()
        .ok_or_else(|| corrupt("installation record has no durable promise"))?;
    if record.epoch != cell.view {
        return Err(corrupt("installation record epoch mismatch"));
    }
    if codec::is_delta(record.payload) {
        delta_start.get_or_insert(position);
        return Ok(());
    }
    let marker = Installation::decode(record.payload)?;
    let start = delta_start.unwrap_or(position);
    let source = marker.selected_report()?;
    if source.request != *active
        || source.history.records != start as u64
        || source.accepted_tail != LogIndex(cell.next_index - 1)
        || source.history.probe != probe_for_cell(cell)?
    {
        return Err(corrupt("installation source differs from fenced history"));
    }
    let prefix = records[..start]
        .iter()
        .map(|record| VsrHistoryRecord {
            kind: record.kind,
            epoch: record.epoch,
            payload: record.payload.to_vec(),
        })
        .collect::<Vec<_>>();
    if transfer::history_digest(&prefix)? != source.history.digest
        || marker.delta_count != (position - start) as u64
    {
        return Err(corrupt(
            "installation source digest or delta count mismatch",
        ));
    }
    let deltas = records[start..position]
        .iter()
        .map(|record| VsrHistoryRecord {
            kind: record.kind,
            epoch: record.epoch,
            payload: record.payload.to_vec(),
        })
        .collect::<Vec<_>>();
    if transfer::history_digest(&deltas)? != marker.delta_digest {
        return Err(corrupt("installation delta digest mismatch"));
    }
    for delta in &deltas {
        if delta.kind != WalRecordKind::ViewInstallation || delta.epoch != cell.view {
            return Err(corrupt("invalid installation delta envelope"));
        }
        apply_delta(cell, history, &delta.payload)?;
    }
    let committed = marker
        .reports
        .iter()
        .map(|report| report.history.probe.committed_tail)
        .max()
        .unwrap();
    if cell.committed_tail() != committed {
        return Err(corrupt(
            "installation did not preserve the quorum committed boundary",
        ));
    }
    cell.view = active.target_view;
    cell.primary = active.candidate;
    for pending in cell
        .pending
        .values_mut()
        .filter(|pending| !pending.committed)
    {
        pending.acknowledgements.clear();
    }
    history.record(cell);
    *promise = None;
    *delta_start = None;
    Ok(())
}

#[cfg(test)]
mod tests;

pub(super) fn is_membership_delta(payload: &[u8]) -> bool {
    codec::is_delta(payload)
}

pub(super) fn is_activation_marker(payload: &[u8]) -> bool {
    payload.starts_with(b"VSRI1")
        || payload.starts_with(b"VSRI2")
        || payload.starts_with(b"VSRMAI1")
}
