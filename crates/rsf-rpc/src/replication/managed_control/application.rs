//! Scoped data-owner access; the reserved control owner is never lent.
use super::*;

struct ApplicationCells<'a> {
    cells: &'a mut BTreeMap<PlacementGroupId, DurableVsrCell>,
    control: Option<PlacementGroupId>,
    reserved: Option<DurableVsrCell>,
}
impl Drop for ApplicationCells<'_> {
    fn drop(&mut self) {
        if let Some(pg) = self.control {
            // Remove a callback's conflicting insertion before restoring the
            // exact original owner. Even unwinding cannot substitute its handle.
            self.cells.remove(&pg);
            if let Some(cell) = self.reserved.take() {
                self.cells.insert(pg, cell);
            }
        }
    }
}
impl PeerReplicaServer {
    /// Lend only data voters under one serialized owner. This is not admission:
    /// callers must retain their existing per-request control/policy checks.
    /// The closure must restore its data custody and return true only after all
    /// data effects materialize. Reserved-PG insertion errors after the closure,
    /// so callers must not label that error as an unattempted mutation.
    pub fn with_application_cells<R>(
        &mut self,
        operation: impl FnOnce(
            NodeId,
            &mut BTreeMap<PlacementGroupId, DurableVsrCell>,
            NetworkVsrTransport,
        ) -> (R, bool),
    ) -> Result<R, String> {
        let control = self.control_placement_group();
        let reserved = control.and_then(|pg| self.cells.remove(&pg));
        let scoped = ApplicationCells {
            cells: &mut self.cells,
            control,
            reserved,
        };
        let groups = scoped.cells.keys().copied().collect::<Vec<_>>();
        self.installation_pending.extend(groups.iter().copied());
        let (result, materialized) =
            operation(self.local, scoped.cells, self.control_transport.clone());
        let conflict = control.is_some_and(|pg| scoped.cells.contains_key(&pg));
        drop(scoped);
        if conflict {
            return Err("application callback inserted reserved control placement group".into());
        }
        if materialized {
            for pg in groups {
                self.installation_pending.remove(&pg);
            }
        }
        Ok(result)
    }

    /// Recover control independently, then return only data voter/learner
    /// commands for Fabric version ordering. The legacy full-history accessor
    /// remains explicit and unchanged.
    pub fn committed_application_entries(&mut self) -> Result<Vec<VsrCommittedEntry>, String> {
        let control = self.control_placement_group();
        if let Some(pg) = control {
            if self.cells.contains_key(&pg) {
                // No application callback is supplied or fabricated here.
                self.managed_control
                    .as_mut()
                    .unwrap()
                    .refresh_voter(&self.cells[&pg])?;
            } else {
                let learner = &self.learners[&pg];
                if learner
                    .published_history_id()
                    .map_err(|e| e.message)?
                    .is_some()
                {
                    self.managed_control
                        .as_mut()
                        .unwrap()
                        .refresh_learner(learner)?;
                }
            }
        }
        let mut entries = Vec::new();
        for (pg, cell) in &self.cells {
            if Some(*pg) != control {
                entries.extend(cell.committed_prefix().map_err(|e| e.message)?);
            }
        }
        for (pg, learner) in &self.learners {
            if Some(*pg) != control {
                entries.extend(learner.committed_entries().map_err(|e| e.message)?);
            }
        }
        for entry in &entries {
            self.validate_application_domain(&entry.prepare)?;
        }
        Ok(entries)
    }
}
