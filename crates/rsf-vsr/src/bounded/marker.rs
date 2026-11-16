//! Complete only the existing publication marker format, under original custody.
use super::*;
use std::fs::OpenOptions;
use std::io::Write;

pub(super) fn finish(
    root: &Path,
    pg: PlacementGroupId,
    guard: &mut custody::Guard,
    validate_store: impl Fn() -> RsfResult<()>,
    hook: &mut impl FnMut(Boundary) -> RsfResult<()>,
) -> RsfResult<()> {
    let bytes = publication::published_record(pg);
    let destination = root.join(publication::PUBLISHED);
    if let Some(existing) = guard.optional(&destination, bytes.len())? {
        return publication::validate_published(&existing, pg);
    }
    guard.validate()?;
    validate_store()?;
    let temporary = root.join(".VSR_PUBLISHED.tmp");
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    custody::nofollow(&mut options);
    let (mut file, prefix, before) = match options.open(&temporary) {
        Ok(file) => {
            let stamp = custody::regular(&file.metadata().map_err(io_error)?)?;
            custody::validate_descriptor(&temporary, &file, &stamp)?;
            (file, 0, stamp)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            let existing = custody::Exact::read(&temporary, bytes.len())?;
            if !bytes.starts_with(&existing.bytes) {
                return Err(corrupt(
                    "publication temporary is not the canonical expected prefix",
                ));
            }
            let mut options = OpenOptions::new();
            options.append(true);
            custody::nofollow(&mut options);
            let file = options.open(&temporary).map_err(io_error)?;
            custody::validate_descriptor(&temporary, &file, &existing.stamp)?;
            (file, existing.bytes.len(), existing.stamp)
        }
        Err(error) => return Err(io_error(error)),
    };
    // Never truncate a present file. Interrupted canonical prefixes resume by
    // appending only their missing suffix through this exact opened descriptor.
    file.write_all(&bytes[prefix..]).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    let stamp = custody::after_owned_write(&temporary, &file, &before)?;
    let prepared = custody::Exact::read(&temporary, bytes.len())?;
    if prepared.stamp != stamp || prepared.bytes != bytes {
        return Err(corrupt(
            "publication temporary differs from completed expected record",
        ));
    }
    hook(Boundary::MarkerPrepared)?;
    guard.validate()?;
    validate_store()?;
    prepared.validate()?;
    custody::validate_descriptor(&temporary, &file, &stamp)?;
    fs::rename(&temporary, &destination).map_err(io_error)?;
    rsf_storage::sync_existing_directory(root).map_err(io_error)?;
    let published = custody::Exact {
        path: destination,
        bytes,
        stamp,
    };
    published.validate()?;
    guard.publish_marker(root, published)?;
    hook(Boundary::MarkerPublished)?;
    guard.validate()?;
    validate_store()
}
