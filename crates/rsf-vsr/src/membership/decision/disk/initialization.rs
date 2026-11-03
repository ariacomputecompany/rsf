//! Recognize only unpublished input-factory prefixes. Callers must first prove
//! that no authority/accepted/chosen/staged journal reference binds the input.
use super::*;

pub(in crate::membership) fn initialization_pending(
    root: &Path,
    id: [u8; 32],
    proposal: &VsrMembershipProposal,
) -> RsfResult<bool> {
    let dir = input_directory(root, id, false)?;
    for entry in fs::read_dir(&dir).map_err(io_error)? {
        let name = entry.map_err(io_error)?.file_name();
        if name != "DESCRIPTOR" && name != ".DESCRIPTOR.tmp" && name != "records" {
            return Err(corrupt(
                "unbound membership input has foreign factory entries",
            ));
        }
    }
    let descriptor = bounded_file(&dir.join("DESCRIPTOR"))?;
    let temp = bounded_file(&dir.join(".DESCRIPTOR.tmp"))?;
    if let Some(bytes) = descriptor {
        let input = VsrMembershipInput::decode(&bytes)?;
        if input.id()? != id || input.proposal() != proposal {
            return Err(corrupt(
                "membership factory descriptor differs from bound input/proposal",
            ));
        }
        if temp.is_some() {
            return Err(corrupt(
                "membership factory retained a temporary after publishing its descriptor",
            ));
        }
        return SegmentStore::initialization_pending(dir.join("records"));
    }
    match fs::symlink_metadata(dir.join("records")) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(io_error(error)),
        Ok(_) => Err(corrupt(
            "membership records exist before their factory descriptor",
        )),
    }
}

pub(in crate::membership) fn resume_input_initialization(
    root: &Path,
    input: &VsrMembershipInput,
) -> RsfResult<()> {
    let id = input.id()?;
    if !initialization_pending(root, id, input.proposal())? {
        return Err(invalid_input(
            "membership input factory is already published",
        ));
    }
    let dir = input_directory(root, id, false)?;
    let expected = input.encode()?;
    match bounded_file(&dir.join("DESCRIPTOR"))? {
        Some(bytes) => {
            if bytes != expected {
                return Err(corrupt("membership factory descriptor changed"));
            }
            SegmentStore::resume_empty_initialization(dir.join("records"))?;
            sync(&dir)
        }
        None => {
            // No records route is permitted before descriptor publication. The
            // canonical helper discards only an ordinary owned temporary before
            // writing the supplied exact descriptor. Preserve this qualified
            // unacknowledged-temp behavior; temp bytes are not retained proof.
            create_input(root, input)
        }
    }
}

fn bounded_file(path: &Path) -> RsfResult<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(error)),
    };
    if !metadata.is_file() || metadata.len() > MAX_HISTORY_RECORD_BYTES as u64 {
        return Err(corrupt(
            "membership factory file is not regular bounded custody",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt("membership factory file has another hard link"));
        }
    }
    fs::read(path).map(Some).map_err(io_error)
}
