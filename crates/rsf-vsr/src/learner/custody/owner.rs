//! Original outer metadata and actual lock ownership, retained until drop.
use super::*;
use crate::learner::prefix::{guard, io};

#[derive(Debug)]
pub(in crate::learner) struct OwnerCustody {
    root: PathBuf,
    root_identity: guard::Identity,
    identity: guard::Exact,
    lock: guard::Exact,
    active: Option<guard::Exact>,
}
impl OwnerCustody {
    pub(in crate::learner) fn from_original(
        root: PathBuf,
        root_identity: guard::Identity,
        identity: guard::Exact,
        lock: guard::Exact,
        active: Option<guard::Exact>,
    ) -> Self {
        Self {
            root,
            root_identity,
            identity,
            lock,
            active,
        }
    }
    pub(in crate::learner) fn validate(
        &self,
        actual_root: &Path,
        actual_lock: &rsf_storage::ExclusiveFileLock,
    ) -> RsfResult<()> {
        if io::normalized(actual_root)? != self.root {
            return Err(corrupt("learner owner route differs from original custody"));
        }
        if guard::directory(&self.root)? != self.root_identity {
            return Err(corrupt("learner owner directory was replaced"));
        }
        self.identity.validate()?;
        self.lock.validate()?;
        guard::owned_lock(&self.root.join(".learner.lock"), actual_lock)?;
        match &self.active {
            Some(exact) => exact.validate()?,
            None => absent(&self.root.join("ACTIVE"))?,
        }
        self.lock.validate()?;
        if guard::directory(&self.root)? != self.root_identity {
            return Err(corrupt("learner owner directory changed during validation"));
        }
        Ok(())
    }
    // Only the exact prepared FD relocated by our successful publication may
    // update this expectation. No path lookup can manufacture this transition.
    pub(in crate::learner) fn published(&mut self, active: guard::Exact) {
        self.active = Some(active);
    }
}

#[derive(Debug)]
pub(in crate::learner) struct CandidateCustody {
    directory: PathBuf,
    directory_identity: guard::Identity,
    descriptor: guard::Exact,
    cell_identity: guard::Identity,
    writer: Option<guard::Exact>,
}
impl CandidateCustody {
    pub(in crate::learner) fn capture(
        directory: &Path,
        descriptor: &VsrHistoryDescriptor,
    ) -> RsfResult<Self> {
        let directory_identity = guard::directory(directory)?;
        let exact = guard::Exact::capture(&directory.join("DESCRIPTOR"), &descriptor.encode()?)?;
        let cell_identity = guard::directory(&directory.join("cell"))?;
        let writer = match fs::symlink_metadata(directory.join("cell/.vsr-writer.lock")) {
            Ok(_) => Some(guard::Exact::capture(
                &directory.join("cell/.vsr-writer.lock"),
                &[],
            )?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(io_error(error)),
        };
        let custody = Self {
            directory: directory.to_path_buf(),
            directory_identity,
            descriptor: exact,
            cell_identity,
            writer,
        };
        custody.validate()?;
        Ok(custody)
    }
    fn metadata(&self) -> RsfResult<()> {
        if guard::directory(&self.directory)? != self.directory_identity
            || guard::directory(&self.directory.join("cell"))? != self.cell_identity
        {
            return Err(corrupt("learner candidate directory was replaced"));
        }
        self.descriptor.validate()
    }
    pub(in crate::learner) fn validate(&self) -> RsfResult<()> {
        self.metadata()?;
        match &self.writer {
            Some(exact) => exact.validate()?,
            None => absent(&self.directory.join("cell/.vsr-writer.lock"))?,
        }
        self.metadata()
    }
    pub(in crate::learner) fn require_writer(&self) -> RsfResult<()> {
        // Obtain the real missing-name error, never create this acknowledged lock.
        self.writer.as_ref().map_or_else(
            || {
                guard::Exact::capture(&self.directory.join("cell/.vsr-writer.lock"), &[])
                    .map(|_| ())
            },
            guard::Exact::validate,
        )
    }
    pub(in crate::learner) fn acquire_writer(
        &mut self,
        create: bool,
    ) -> RsfResult<rsf_storage::ExclusiveFileLock> {
        self.validate()?;
        if !create {
            self.require_writer()?;
        }
        let path = self.directory.join("cell/.vsr-writer.lock");
        let lock = io::lock_captured(&path, self.writer.clone())?;
        self.metadata()?;
        if let Some(original) = &self.writer {
            original.validate()?;
        }
        let exact = guard::owned_lock(&path, &lock)?;
        self.writer = Some(exact);
        self.validate()?;
        Ok(lock)
    }
    pub(in crate::learner) fn validate_cell(&self, cell: &DurableVsrCell) -> RsfResult<()> {
        self.validate()?;
        if cell.root != self.directory.join("cell") {
            return Err(corrupt("learner candidate moved to a different cell root"));
        }
        self.require_writer()?;
        guard::owned_lock(&cell.root.join(".vsr-writer.lock"), &cell._writer_lock)?;
        self.validate()
    }
}
fn absent(path: &Path) -> RsfResult<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
        Ok(_) => Err(corrupt(
            "learner metadata appeared outside its owned transition",
        )),
    }
}
