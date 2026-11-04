//! Validation-only result from an actual candidate preview; no authority is created.
use super::*;
use rsf_vsr::VsrImportPreview;

impl ControlState {
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
