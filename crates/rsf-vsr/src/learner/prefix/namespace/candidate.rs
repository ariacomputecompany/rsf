//! Candidate evidence stays intact until its storage and descriptor barriers.
use super::*;
use rsf_storage::SegmentDiscardInventory;

#[derive(Debug)]
pub(super) enum Cell {
    Live(live::LiveCell),
    Disposable {
        inventory: SegmentDiscardInventory,
        writer: Option<guard::Exact>,
        directory: Directory,
    },
}
#[derive(Debug)]
pub(super) struct Candidate {
    pub(super) directory: Directory,
    pub(super) descriptor: Option<guard::Exact>,
    pub(super) temporary: Option<guard::Exact>,
    pub(super) cell: Option<Cell>,
}
impl Candidate {
    pub(super) fn capture(
        owner: &LearnerHistoryStore,
        id: [u8; 32],
        limits: VsrPrefixLimits,
    ) -> RsfResult<Self> {
        let directory = Directory::capture(&owner.directory(&id))?;
        let entries = names(&directory.path, 2)?;
        let has_descriptor = entries.contains(OsStr::new("DESCRIPTOR"));
        if entries
            .iter()
            .any(|n| n != "DESCRIPTOR" && n != ".DESCRIPTOR.tmp" && n != "cell")
            || (has_descriptor && entries.contains(OsStr::new(".DESCRIPTOR.tmp")))
        {
            return Err(corrupt("bounded learner candidate has unknown entries"));
        }
        let descriptor = if has_descriptor {
            let (bytes, exact) =
                io::read(&directory.path.join("DESCRIPTOR"), io::DESCRIPTOR_BYTES)?;
            let descriptor = VsrHistoryDescriptor::decode(&bytes)?;
            owner.validate_descriptor(&descriptor)?;
            limits.check(descriptor.records, 0)?;
            if descriptor.id()? != id || descriptor.encode()? != bytes {
                return Err(corrupt("bounded candidate descriptor identity differs"));
            }
            Some(exact)
        } else {
            None
        };
        let temporary = if entries.contains(OsStr::new(".DESCRIPTOR.tmp")) {
            Some(
                io::read(
                    &directory.path.join(".DESCRIPTOR.tmp"),
                    io::DESCRIPTOR_BYTES,
                )?
                .1,
            )
        } else {
            None
        };
        if entries.contains(OsStr::new("cell")) && descriptor.is_none() {
            return Err(corrupt("bounded candidate cell lost descriptor"));
        }
        let cell = if entries.contains(OsStr::new("cell")) {
            Some(match owner.prefix_store(&id)? {
                Some(store) => Cell::Live(live::LiveCell::capture(
                    store,
                    owner.prefix_count(&id)?,
                    limits,
                )?),
                None => Self::disposable(&directory.path.join("cell"), limits)?,
            })
        } else {
            None
        };
        if owner.prefix_store(&id)?.is_some() && cell.is_none() {
            return Err(corrupt("bounded selected candidate lost its cell"));
        }
        let result = Self {
            directory,
            descriptor,
            temporary,
            cell,
        };
        result.validate()?;
        Ok(result)
    }
    pub(super) fn capture_live(
        owner: &LearnerHistoryStore,
        id: [u8; 32],
        limits: VsrPrefixLimits,
    ) -> RsfResult<Self> {
        if owner.prefix_store(&id)?.is_none() {
            return Err(corrupt("bounded live candidate has no actual owner"));
        }
        let result = Self::capture(owner, id, limits)?;
        if !matches!(result.cell, Some(Cell::Live(_))) {
            return Err(corrupt("bounded candidate is not a complete owned cell"));
        }
        Ok(result)
    }
    fn disposable(path: &Path, limits: VsrPrefixLimits) -> RsfResult<Cell> {
        let directory = Directory::capture(path)?;
        let inventory = SegmentStore::inspect_provisional_discard(path, limits.storage()?)
            .map_err(stage::storage_error)?;
        let other = inventory.other_root_names().collect::<Vec<_>>();
        let writer = match other.as_slice() {
            [] => None,
            [name] if *name == OsStr::new(".vsr-writer.lock") => {
                Some(guard::Exact::capture(&path.join(name), &[])?)
            }
            _ => return Err(corrupt("bounded disposable cell has foreign auxiliary")),
        };
        inventory.validate()?;
        directory.validate()?;
        Ok(Cell::Disposable {
            inventory,
            writer,
            directory,
        })
    }
    pub(super) fn permit_discard(&mut self, limits: VsrPrefixLimits) -> RsfResult<()> {
        self.validate()?;
        if matches!(self.cell, Some(Cell::Live(_))) {
            let replacement = Self::disposable(&self.directory.path.join("cell"), limits)?;
            self.validate()?;
            self.cell = Some(replacement);
        }
        self.validate()
    }
    pub(super) fn validate_anchor(&self, changing_cell: bool) -> RsfResult<()> {
        self.directory.validate()?;
        let mut expected = BTreeSet::new();
        if let Some(descriptor) = &self.descriptor {
            descriptor.validate()?;
            expected.insert(OsString::from("DESCRIPTOR"));
        }
        if let Some(temporary) = &self.temporary {
            temporary.validate()?;
            expected.insert(OsString::from(".DESCRIPTOR.tmp"));
        }
        if self.cell.is_some() {
            expected.insert(OsString::from("cell"));
        }
        let actual = names(&self.directory.path, 2)?;
        if changing_cell {
            expected.insert(OsString::from("cell"));
        }
        let valid = if changing_cell {
            actual.is_subset(&expected)
                && actual.iter().filter(|n| *n != "cell").count()
                    == expected.iter().filter(|n| *n != "cell").count()
        } else {
            actual == expected
        };
        if !valid {
            return Err(corrupt("bounded candidate namespace changed"));
        }
        self.directory.validate()
    }
    pub(super) fn validate(&self) -> RsfResult<()> {
        self.validate_anchor(false)?;
        match &self.cell {
            Some(Cell::Live(cell)) => cell.validate()?,
            Some(Cell::Disposable {
                inventory,
                writer,
                directory,
            }) => {
                directory.validate()?;
                if let Some(writer) = writer {
                    writer.validate()?;
                }
                inventory.validate()?;
            }
            None => (),
        }
        self.directory.validate()
    }
}
