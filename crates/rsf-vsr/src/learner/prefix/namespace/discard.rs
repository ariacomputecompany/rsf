//! Only explicitly retired candidates reach the storage disposal capability.
use super::*;
use candidate::Cell;

impl Namespace {
    /// Call only after installing the successor volatile selection/publication,
    /// while retaining the former owner's handles until this capture succeeds.
    pub(in crate::learner::prefix) fn permit_retirement(
        &mut self,
        owner: &LearnerHistoryStore,
        id: [u8; 32],
    ) -> RsfResult<()> {
        self.validate(owner)?;
        if owner.prefix_store(&id)?.is_some() {
            return Err(corrupt("cannot dispose a current bounded owner"));
        }
        self.candidates
            .get_mut(&id)
            .ok_or_else(|| corrupt("retired bounded candidate missing from inventory"))?
            .permit_discard(self.limits)?;
        self.validate(owner)
    }

    pub(in crate::learner::prefix) fn discard_obsolete(
        &mut self,
        owner: &LearnerHistoryStore,
        preserve: Option<[u8; 32]>,
    ) -> RsfResult<()> {
        let ids = self
            .candidate_names
            .iter()
            .copied()
            .filter(|id| Some(*id) != preserve)
            .collect::<Vec<_>>();
        for id in ids {
            if owner.prefix_store(&id)?.is_none() {
                self.discard_one(owner, id)?;
            }
        }
        self.validate(owner)
    }

    fn discard_one(&mut self, owner: &LearnerHistoryStore, id: [u8; 32]) -> RsfResult<()> {
        self.validate(owner)?;
        if owner.prefix_store(&id)?.is_some() {
            return Err(corrupt("bounded disposal target remains selected"));
        }
        self.clear_temporaries(owner)?;
        let mut candidate = self
            .candidates
            .remove(&id)
            .ok_or_else(|| corrupt("bounded disposal target absent"))?;
        let validate = |candidate: &Candidate| -> RsfResult<()> {
            self.validate_except(owner, Some(id))?;
            candidate.validate_anchor(true)
        };
        validate(&candidate)?;
        if let Some(cell) = candidate.cell.take() {
            let Cell::Disposable {
                inventory,
                writer,
                directory,
            } = cell
            else {
                return Err(corrupt(
                    "bounded owner was not explicitly retired before disposal",
                ));
            };
            let remainder = effect(Operation::RemoveChildren, &directory.path, || {
                inventory.remove_children(|| validate(&candidate))
            })?;
            validate(&candidate)?;
            directory.validate()?;
            if let Some(writer) = writer {
                writer.validate()?;
                let path = directory.path.join(".vsr-writer.lock");
                effect(Operation::Unlink, &path, || {
                    fs::remove_file(&path).map_err(io_error)
                })?;
                absent(&directory.path.join(".vsr-writer.lock"))?;
                directory.sync()?;
                validate(&candidate)?;
            }
            effect(Operation::FinishStorage, &directory.path, || {
                remainder.finish(|| validate(&candidate))
            })?;
            absent(&candidate.directory.path.join("cell"))?;
            // Cell's parent name is durable before its descriptor can vanish.
            candidate.directory.sync()?;
            validate(&candidate)?;
        }
        if let Some(descriptor) = candidate.descriptor.take() {
            descriptor.validate()?;
            let path = candidate.directory.path.join("DESCRIPTOR");
            effect(Operation::Unlink, &path, || {
                fs::remove_file(&path).map_err(io_error)
            })?;
            absent(&candidate.directory.path.join("DESCRIPTOR"))?;
            candidate.directory.sync()?;
            validate(&candidate)?;
        }
        if let Some(temporary) = candidate.temporary.take() {
            temporary.validate()?;
            let path = candidate.directory.path.join(".DESCRIPTOR.tmp");
            effect(Operation::Unlink, &path, || {
                fs::remove_file(&path).map_err(io_error)
            })?;
            absent(&candidate.directory.path.join(".DESCRIPTOR.tmp"))?;
            candidate.directory.sync()?;
            validate(&candidate)?;
        }
        candidate.validate_anchor(false)?;
        self.validate_except(owner, Some(id))?;
        effect(
            Operation::RemoveDirectory,
            &candidate.directory.path,
            || fs::remove_dir(&candidate.directory.path).map_err(io_error),
        )?;
        absent(&candidate.directory.path)?;
        self.candidate_names.remove(&id);
        self.validate(owner)?;
        self.root.sync()?;
        self.validate(owner)
    }
}
