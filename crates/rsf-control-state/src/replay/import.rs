//! Validation-only result from an actual candidate preview; no authority is created.
use super::*;
use rsf_vsr::VsrImportPreview;

impl ControlState {
    /// Exact first-publication semantics over an actual bounded candidate.
    /// This returns no observation, source identity or admission capability.
    pub fn validate_initial_import_preview(
        anchor: &ControlBootstrap,
        genesis: &ControlCatalog,
        preview: &VsrImportPreview<'_>,
    ) -> RsfResult<()> {
        preview.validate_live()?;
        let result = (|| {
            if genesis.cluster != anchor.cluster
                || genesis.control_pg != anchor.control_pg
                || genesis.root()? != anchor.genesis_catalog
                || preview.committed_tail() != LogIndex(1)
                || preview.committed_count() != 1
                || preview.next_index() != LogIndex(2)
            {
                return Err(corrupt(
                    "initial control prefix must contain only provisioned committed Genesis",
                ));
            }
            Self::validate_import_preview(anchor, preview)?;
            let current = preview.membership_lineage()?;
            let expected = genesis
                .group(anchor.control_pg)
                .ok_or_else(|| corrupt("provisioned control group is absent"))?;
            if current.bootstrap != anchor.control_bootstrap
                || current.plan.is_some()
                || expected.lineage.plan.is_some()
                || current.voters != expected.voters
            {
                return Err(corrupt(
                    "initial control prefix differs from original provisioned membership",
                ));
            }
            preview.for_each_committed(|prepare, _commit| {
                if prepare.index != LogIndex(1)
                    || preflight::checked_command(prepare)?
                        != ControlCommand::Genesis(genesis.clone())
                {
                    return Err(corrupt(
                        "initial control command differs from canonical Genesis",
                    ));
                }
                Ok(())
            })
        })();
        preview.validate_live()?;
        result
    }

    /// Validate the provisioned Genesis, committed semantics and all retained
    /// accepted-only work. This never creates a voter or published observation.
    pub fn validate_import_preview(
        anchor: &ControlBootstrap,
        preview: &VsrImportPreview<'_>,
    ) -> RsfResult<()> {
        preview.validate_live()?;
        let result = (|| {
            let current = preview.membership_lineage()?;
            let mut committed = read::CommittedReplay::new(
                anchor,
                &current,
                preview.committed_tail(),
                preview.committed_count(),
            )?;
            preview.for_each_committed(|prepare, commit| {
                let historical = preview
                    .membership_lineage_at(prepare.view)?
                    .ok_or_else(|| corrupt("imported control commit lacks historical lineage"))?;
                committed.push(prepare, commit, &historical)
            })?;
            let (state, bytes) = committed.finish()?;
            let mut accepted =
                preflight::AcceptedReplay::new(state, bytes, &current, preview.next_index())?;
            preview.for_each_accepted_tail(|prepare| {
                let historical = preview
                    .membership_lineage_at(prepare.view)?
                    .ok_or_else(|| corrupt("imported accepted control lacks historical lineage"))?;
                accepted.push(prepare, &historical, &current)
            })?;
            accepted.finish(preview.next_index())?;
            Ok(())
        })();
        // A semantic error must not mask changed candidate custody.
        preview.validate_live()?;
        result
    }
}
