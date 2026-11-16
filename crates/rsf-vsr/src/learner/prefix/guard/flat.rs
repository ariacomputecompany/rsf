//! Original flat storage namespace; absence is retained, never rediscovered.
use super::*;

#[derive(Clone, Debug)]
pub(in crate::learner) struct Flat {
    root: PathBuf,
    identity: Identity,
}
impl Flat {
    pub(in crate::learner) fn capture(root: &Path) -> RsfResult<Self> {
        let guard = Self {
            root: root.to_path_buf(),
            identity: directory(root)?,
        };
        guard.validate()?;
        Ok(guard)
    }
    pub(in crate::learner) fn validate(&self) -> RsfResult<()> {
        if directory(&self.root)? != self.identity {
            return Err(corrupt("bounded flat root differs from original custody"));
        }
        io::flat(&self.root)?;
        if directory(&self.root)? != self.identity {
            return Err(corrupt(
                "bounded flat root changed during absence validation",
            ));
        }
        Ok(())
    }
    pub(in crate::learner) fn validate_cell(&self, cell: &DurableVsrCell) -> RsfResult<()> {
        self.validate()?;
        if cell.root != self.root
            || cell.store.root() != self.root.as_path()
            || cell.installed_plan.is_some()
        {
            return Err(corrupt(
                "bounded flat owner changed its original storage route",
            ));
        }
        owned_lock(&self.root.join(".vsr-writer.lock"), &cell._writer_lock)?.validate()?;
        self.validate()
    }
}
