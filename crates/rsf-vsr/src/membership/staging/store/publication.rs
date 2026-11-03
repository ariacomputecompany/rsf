//! Recovery input retained before activating a chosen membership. This file is
//! not an application acknowledgement or proof that a new quorum has published.
use super::*;

const NAME: &str = "PUBLICATION";
const MAGIC: &[u8] = b"VSRMPU1";

impl MembershipStagingStore {
    /// Retain an authenticated new-majority staging proof before publishing
    /// authority. Every call requires this boot's successful materialization.
    /// Equivalent proof subsets preserve the first synced proof bytes.
    pub fn retain_publication(&mut self, quorum: &VsrMembershipStagedQuorum) -> RsfResult<()> {
        self.retain_publication_with_hook(quorum, || {})
    }

    pub(super) fn retain_publication_with_hook(
        &mut self,
        quorum: &VsrMembershipStagedQuorum,
        input_synced: impl FnOnce(),
    ) -> RsfResult<()> {
        let local = self.report()?;
        validate_quorum(&local, quorum)?;
        self.validate_publication_receipt()?;
        let existing = self.read_publication()?;
        let needs_write = existing.is_none();
        let bytes = match existing {
            Some((bytes, _)) => bytes,
            None => encode(&local, quorum)?,
        };
        let digest = Digest::blake3_bytes(&bytes).bytes;
        if self.publication == Some(digest) {
            return Ok(());
        }
        let metadata = encode_meta(self.local, &self.plan, self.stage, Some(digest))?;
        // Both complete files precede success. A crash after the first sync
        // leaves a V1 stage with recoverable input but no acknowledged retention.
        let result = (|| {
            if needs_write {
                atomic(&self.root, NAME, &bytes)?;
            }
            input_synced();
            atomic(&self.root, "META", &metadata)
        })();
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        self.publication = Some(digest);
        Ok(())
    }

    /// Read replay-validated publication input, including after a cold open.
    /// This does not restore this boot's materialization receipt or grant a vote.
    pub fn publication_quorum(&self) -> RsfResult<Option<VsrMembershipStagedQuorum>> {
        self.healthy()?;
        self.read_publication()
            .map(|value| value.map(|(_, quorum)| quorum))
    }

    fn read_publication(&self) -> RsfResult<Option<(Vec<u8>, VsrMembershipStagedQuorum)>> {
        let metadata = fs::read(self.root.join("META")).map_err(io_error)?;
        if metadata != encode_meta(self.local, &self.plan, self.stage, self.publication)? {
            return Err(corrupt(
                "membership stage metadata changed during publication custody",
            ));
        }
        let path = self.root.join(NAME);
        let bytes = match fs::symlink_metadata(&path) {
            Ok(meta) => {
                if !meta.is_file()
                    || meta.file_type().is_symlink()
                    || meta.len() > MAX_HISTORY_RECORD_BYTES as u64
                {
                    return Err(corrupt(
                        "membership publication input is not a bounded regular file",
                    ));
                }
                fs::read(&path).map_err(io_error)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if self.publication.is_some() {
                    return Err(corrupt("sealed membership publication input is missing"));
                }
                return Ok(None);
            }
            Err(error) => return Err(io_error(error)),
        };
        let (local, quorum) = decode(&bytes)?;
        let expected = self.expected_report()?;
        if local != expected {
            return Err(corrupt(
                "membership publication input belongs to another local stage",
            ));
        }
        validate_quorum(&expected, &quorum)?;
        if self
            .publication
            .is_some_and(|digest| digest != Digest::blake3_bytes(&bytes).bytes)
        {
            return Err(corrupt(
                "membership publication input differs from sealed metadata",
            ));
        }
        self.validate_publication_receipt()?;
        Ok(Some((bytes, quorum)))
    }

    fn validate_publication_receipt(&self) -> RsfResult<()> {
        let bytes = fs::read(self.root.join("INSTALLED")).map_err(io_error)?;
        if VsrMembershipStagedReport::decode(&bytes)? != self.expected_report()? {
            return Err(corrupt(
                "membership publication lost its original staging receipt",
            ));
        }
        Ok(())
    }
}

fn validate_quorum(
    local: &VsrMembershipStagedReport,
    quorum: &VsrMembershipStagedQuorum,
) -> RsfResult<()> {
    quorum.encode()?;
    let first = &quorum.reports()[0];
    if first.proposal != local.proposal
        || first.input != local.input
        || first.plan != local.plan
        || first.committed_tail != local.committed_tail
        || first.accepted_tail != local.accepted_tail
        || quorum
            .reports()
            .iter()
            .any(|report| report.replica == local.replica && report != local)
    {
        return Err(invalid_input(
            "membership publication quorum differs from the sealed local stage",
        ));
    }
    Ok(())
}

pub(super) fn encode(
    local: &VsrMembershipStagedReport,
    quorum: &VsrMembershipStagedQuorum,
) -> RsfResult<Vec<u8>> {
    validate_quorum(local, quorum)?;
    let mut bytes = MAGIC.to_vec();
    put_bytes(&mut bytes, &local.encode()?)?;
    put_bytes(&mut bytes, &quorum.encode()?)?;
    let checksum = Digest::blake3_bytes(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    if bytes.len() > MAX_HISTORY_RECORD_BYTES {
        return Err(invalid_input(
            "membership publication input exceeds record limit",
        ));
    }
    Ok(bytes)
}

pub(super) fn decode(
    bytes: &[u8],
) -> RsfResult<(VsrMembershipStagedReport, VsrMembershipStagedQuorum)> {
    let mut reader = Reader::new(bytes, MAGIC)?;
    let local = VsrMembershipStagedReport::decode(reader.bytes()?)?;
    let quorum = VsrMembershipStagedQuorum::decode(reader.bytes()?)?;
    let checksum = reader.id()?;
    reader.finish()?;
    if Digest::blake3_bytes(&bytes[..bytes.len() - 32]).bytes != checksum
        || encode(&local, &quorum)? != bytes
    {
        return Err(corrupt(
            "invalid membership publication input checksum or encoding",
        ));
    }
    Ok((local, quorum))
}

pub(super) fn present(path: &Path) -> RsfResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(error)),
    }
}
