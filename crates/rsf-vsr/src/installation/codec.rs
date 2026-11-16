use super::*;

pub(super) struct Installation {
    pub reports: Vec<VsrViewChangeReport>,
    pub source: NodeId,
    pub delta_count: u64,
    pub delta_digest: Digest,
}

impl Installation {
    pub fn selected_report(&self) -> RsfResult<&VsrViewChangeReport> {
        let first = self
            .reports
            .first()
            .ok_or_else(|| corrupt("installation certificate is empty"))?;
        let mut voters = BTreeSet::new();
        let mut primaries = BTreeMap::new();
        for report in &self.reports {
            report.encode()?;
            if let Some(previous) =
                primaries.insert(report.history.probe.view, report.history.probe.primary)
                && previous != report.history.probe.primary
            {
                return Err(corrupt(
                    "installation reports disagree on the primary of one normal view",
                ));
            }

            if report.request != first.request || !voters.insert(report.replica) {
                return Err(corrupt(
                    "installation certificate has mixed configurations or duplicate voters",
                ));
            }
        }
        if voters.len() <= first.request.voters.len() / 2 {
            return Err(corrupt("installation certificate lacks a quorum"));
        }
        let selected = self
            .reports
            .iter()
            .max_by_key(|report| {
                (
                    report.history.probe.view,
                    report.accepted_tail,
                    std::cmp::Reverse(report.replica),
                )
            })
            .unwrap();
        if selected.replica != self.source {
            return Err(corrupt(
                "installation selected a shorter or noncanonical source",
            ));
        }
        self.delta_digest.validate()?;
        if self.delta_digest.algorithm != rsf_core::DigestAlgorithm::Blake3 {
            return Err(corrupt("installation delta digest must use BLAKE3"));
        }
        Ok(selected)
    }

    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        self.encode_version(false)
    }

    pub(super) fn encode_version(&self, legacy: bool) -> RsfResult<Vec<u8>> {
        let selected = self.selected_report()?;
        if legacy
            && self.reports.iter().any(|report| {
                report.history.probe.view != selected.request.previous_view
                    || report.history.probe.primary != selected.history.probe.primary
            })
        {
            return Err(corrupt(
                "legacy installation marker cannot represent mixed views",
            ));
        }
        let request = selected.request.encode()?;
        let mut out = if legacy {
            b"VSRI1".to_vec()
        } else {
            b"VSRI2".to_vec()
        };
        out.extend_from_slice(&(request.len() as u32).to_le_bytes());
        out.extend_from_slice(&request);
        for n in [
            selected.history.probe.primary.0,
            self.source.0,
            self.delta_count,
        ] {
            out.extend_from_slice(&n.to_le_bytes());
        }
        out.extend_from_slice(self.delta_digest.as_bytes());
        out.extend_from_slice(&(self.reports.len() as u32).to_le_bytes());
        let mut reports = self.reports.iter().collect::<Vec<_>>();
        reports.sort_by_key(|report| report.replica);
        for report in reports {
            for n in [
                report.replica.0,
                report.accepted_tail.0,
                report.history.records,
            ] {
                out.extend_from_slice(&n.to_le_bytes());
            }
            out.extend_from_slice(report.history.digest.as_bytes());
            out.extend_from_slice(&report.history.probe.committed_tail.0.to_le_bytes());
            out.extend_from_slice(report.history.probe.committed_tip.as_bytes());
            if !legacy {
                out.extend_from_slice(&report.history.probe.view.0.to_le_bytes());
                out.extend_from_slice(&report.history.probe.primary.0.to_le_bytes());
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() < 9 || (!bytes.starts_with(b"VSRI1") && !bytes.starts_with(b"VSRI2")) {
            return Err(corrupt("invalid installation marker"));
        }
        let legacy = bytes.starts_with(b"VSRI1");
        let width = if legacy { 96 } else { 112 };
        let length = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let end = 9usize
            .checked_add(length)
            .ok_or_else(|| corrupt("installation length overflow"))?;
        let request = VsrViewChangeRequest::decode(
            bytes
                .get(9..end)
                .ok_or_else(|| corrupt("truncated installation request"))?,
        )?;
        let primary = NodeId(read_u64(bytes, end)?);
        let source = NodeId(read_u64(bytes, end + 8)?);
        let delta_count = read_u64(bytes, end + 16)?;
        let digest = bytes
            .get(end + 24..end + 56)
            .ok_or_else(|| corrupt("truncated installation digest"))?;
        let count = bytes
            .get(end + 56..end + 60)
            .ok_or_else(|| corrupt("truncated installation voter count"))?;
        let count = u32::from_le_bytes(count.try_into().unwrap()) as usize;
        if count > 4096 || bytes.len() != end + 60 + count * width {
            return Err(corrupt("invalid installation certificate length"));
        }
        let mut reports = Vec::new();
        for i in 0..count {
            let at = end + 60 + i * width;
            reports.push(VsrViewChangeReport {
                request: request.clone(),
                replica: NodeId(read_u64(bytes, at)?),
                accepted_tail: LogIndex(read_u64(bytes, at + 8)?),
                history: VsrHistoryDescriptor {
                    records: read_u64(bytes, at + 16)?,
                    digest: Digest::blake3(bytes[at + 24..at + 56].try_into().unwrap())?,
                    probe: VsrPrimaryProbe {
                        pg: request.pg,
                        view: if legacy {
                            request.previous_view
                        } else {
                            Epoch(read_u64(bytes, at + 96)?)
                        },
                        primary: if legacy {
                            primary
                        } else {
                            NodeId(read_u64(bytes, at + 104)?)
                        },
                        voters: request.voters.clone(),
                        committed_tail: LogIndex(read_u64(bytes, at + 56)?),
                        committed_tip: Digest::blake3(bytes[at + 64..at + 96].try_into().unwrap())?,
                    },
                },
            });
        }
        let result = Self {
            reports,
            source,
            delta_count,
            delta_digest: Digest::blake3(digest.try_into().unwrap())?,
        };
        if result.encode_version(legacy)? != bytes {
            return Err(corrupt("noncanonical installation marker"));
        }
        Ok(result)
    }
}

pub(super) fn is_delta(payload: &[u8]) -> bool {
    payload.starts_with(b"VSIC1") || payload.starts_with(b"VSIT1") || payload.starts_with(b"VSIL1")
}

#[cfg(test)]
pub(super) fn commit_delta(commit: &VsrCommit) -> RsfResult<Vec<u8>> {
    let mut out = b"VSIC1".to_vec();
    out.extend_from_slice(&commit.encode()?);
    Ok(out)
}
pub(super) fn learned_commit_delta(prepare: &VsrPrepare, commit: &VsrCommit) -> RsfResult<Vec<u8>> {
    prepare.encode()?;
    if prepare.pg != commit.pg
        || prepare.view != commit.view
        || prepare.index != commit.index
        || prepare.primary != commit.primary
        || prepare.operation != commit.certificate.operation
    {
        return Err(corrupt("learned certificate differs from its prepare"));
    }
    // The certificate already contains the complete operation. Store only the
    // prepare's missing byte-count metadata, keeping maximum-size commands in
    // one bounded history record rather than duplicating their payload.
    let mut out = b"VSIL1".to_vec();
    out.extend_from_slice(&prepare.bytes.to_le_bytes());
    out.extend_from_slice(&commit.encode()?);
    Ok(out)
}

pub(super) fn client_delta(client: &VsrClientRequestCommit) -> RsfResult<Vec<u8>> {
    let mut out = b"VSIT1".to_vec();
    out.extend_from_slice(&encode_client_request_commit(client)?);
    Ok(out)
}

pub(super) fn apply_delta(
    cell: &mut VsrCell,
    history: &mut history::ConfigurationHistory,
    payload: &[u8],
) -> RsfResult<()> {
    if payload.starts_with(b"VSIL1") {
        let bytes = payload
            .get(5..9)
            .ok_or_else(|| corrupt("truncated learned-commit byte count"))?;
        let commit = VsrCommit::decode(
            payload
                .get(9..)
                .ok_or_else(|| corrupt("missing learned certificate"))?,
        )?;
        let prepare = VsrPrepare {
            pg: commit.pg,
            view: commit.view,
            index: commit.index,
            primary: commit.primary,
            operation: commit.certificate.operation.clone(),
            bytes: u32::from_le_bytes(bytes.try_into().unwrap()),
        };
        let pending = cell
            .pending
            .get(&prepare.index)
            .ok_or_else(|| corrupt("learned commit has no selected operation"))?;
        if prepare.pg != cell.pg
            || cell.committed_tail().0.checked_add(1) != Some(prepare.index.0)
            || !same_operation(&prepare, &pending.prepare)
        {
            return Err(corrupt(
                "learned certificate differs from the selected contiguous log",
            ));
        }
        history.learn_committed(&prepare, &commit)?;
        let pending = cell.pending.get_mut(&prepare.index).unwrap();
        pending.prepare = prepare;
        pending.committed = true;
        pending.acknowledgements.clear();
        cell.committed.push(commit);
        Ok(())
    } else if payload.starts_with(b"VSIC1") {
        let commit = VsrCommit::decode(&payload[5..])?;
        let pending = cell
            .pending
            .get(&commit.index)
            .ok_or_else(|| corrupt("installation commit has no accepted operation"))?;
        if commit.pg != cell.pg || cell.committed_tail().0.checked_add(1) != Some(commit.index.0) {
            return Err(corrupt(
                "installation commit does not extend the contiguous prefix",
            ));
        }
        history.validate(&pending.prepare, &commit)?;
        cell.pending.get_mut(&commit.index).unwrap().committed = true;
        cell.committed.push(commit);
        Ok(())
    } else if payload.starts_with(b"VSIT1") {
        let client = decode_client_request_commit(&payload[5..])?;
        if client.pg != cell.pg
            || !cell
                .pending
                .get(&client.index)
                .is_some_and(|pending| pending.committed)
            || cell
                .client_table
                .get(&client.request)
                .is_some_and(|index| *index != client.index)
        {
            return Err(corrupt(
                "installation client mapping conflicts or has no committed operation",
            ));
        }
        cell.client_table.insert(client.request, client.index);
        Ok(())
    } else {
        Err(corrupt("unknown installation delta"))
    }
}
