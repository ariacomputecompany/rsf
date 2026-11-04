//! Receiver semantic preflight, separate from certificate and sender authority.
use super::*;
use rsf_vsr::VsrCommittedEntry;

impl ControlState {
    /// Validate retained command/capacity semantics even while an ordinary
    /// promise fences writes. This neither clears that promise nor probes/votes.
    pub fn validate_retained_history(&self, control: &DurableVsrCell) -> RsfResult<()> {
        self.replay_accepted(control)?;
        self.validate_projection(control)
    }

    /// Validate a provisioned replica before receiving its first Genesis.
    /// This does not create a catalog/projection or grant admission. Normal
    /// recover continues to require a complete committed Genesis.
    pub fn validate_initial_replica(
        anchor: &ControlBootstrap,
        genesis: &ControlCatalog,
        control: &DurableVsrCell,
    ) -> RsfResult<()> {
        let stamp = control.recovery_state_token()?;
        validate_binding(anchor, genesis, control)?;
        if control.committed_tail() != LogIndex(0)
            || control.committed_count() != 0
            || control.next_index().0 == 0
            || control.next_index().0 > 2
        {
            return Err(invalid(
                "initial control replica is not empty or Genesis-only",
            ));
        }
        if control.next_index() == LogIndex(2) {
            let prepare = control
                .prepared(LogIndex(1))
                .ok_or_else(|| corrupt("initial control accepted prefix has a gap"))?;
            validate_genesis(anchor, genesis, control, prepare)?;
        } else if control.prepared(LogIndex(1)).is_some() {
            return Err(corrupt("initial control accepted index is inconsistent"));
        }
        if stamp != control.recovery_state_token()? {
            return Err(corrupt("initial control owner changed during validation"));
        }
        Ok(())
    }

    /// Receive only the configured command domain; sender eligibility, current
    /// membership and durable PREPARE remain the transport/kernel's duties.
    pub fn preflight_received_prepare(
        anchor: &ControlBootstrap,
        genesis: &ControlCatalog,
        control: &DurableVsrCell,
        prepare: &VsrPrepare,
    ) -> RsfResult<usize> {
        validate_binding(anchor, genesis, control)?;
        if control.committed_tail().0 != 0 {
            return Self::recover(anchor, control)?.preflight_prepare(control, prepare);
        }
        Self::validate_initial_replica(anchor, genesis, control)?;
        let stamp = control.recovery_state_token()?;
        let current = control.primary_probe()?;
        if prepare.pg != current.pg
            || prepare.view != current.view
            || prepare.primary != current.primary
        {
            return Err(invalid(
                "initial control prepare differs from current authority",
            ));
        }
        validate_genesis(anchor, genesis, control, prepare)?;
        if let Some(retained) = control.prepared(LogIndex(1))
            && (retained.operation != prepare.operation
                || retained.bytes != prepare.bytes
                || retained.view > prepare.view)
        {
            return Err(invalid("initial control prepare replaces retained Genesis"));
        }
        if stamp != control.recovery_state_token()? {
            return Err(corrupt("initial control owner changed during preflight"));
        }
        Ok(prepare.bytes as usize)
    }

    /// Validate the actual committed extension under historical membership,
    /// including certified replacement of an older accepted operation. This
    /// does not verify the certificate, mutate the owner or acknowledge commit.
    /// Exact committed retries retain their original envelope; accepted-only
    /// conflicts and certificate authority remain kernel catch_up_from duties.
    pub fn preflight_committed_extension(
        anchor: &ControlBootstrap,
        genesis: &ControlCatalog,
        control: &DurableVsrCell,
        entry: &VsrCommittedEntry,
    ) -> RsfResult<()> {
        let stamp = control.recovery_state_token()?;
        validate_binding(anchor, genesis, control)?;
        let prepare = &entry.prepare;
        if prepare.pg != anchor.control_pg
            || entry.commit.pg != anchor.control_pg
            || prepare.index != entry.commit.index
            || prepare.operation != entry.commit.certificate.operation
        {
            return Err(invalid("control committed extension has foreign identity"));
        }
        let command = checked_command(prepare)?;
        let tail = control.committed_tail();
        if prepare.index <= tail {
            if control.prepared(prepare.index) != Some(prepare)
                || control.committed_operation(prepare.index) != Some(&prepare.operation)
            {
                return Err(invalid(
                    "control committed retry changes retained operation",
                ));
            }
            Self::recover(anchor, control)?.validate_retained_history(control)?;
        } else {
            if tail.0.checked_add(1) != Some(prepare.index.0) {
                return Err(invalid("control committed extension is not contiguous"));
            }
            let historical = control
                .membership_lineage_at(prepare.view)?
                .ok_or_else(|| invalid("control committed extension has unknown lineage"))?;
            let (state, bytes) = if tail.0 == 0 {
                // Provisioning binds the Genesis command/catalog, not its
                // operation id. A certified newer operation can replace an
                // older uncommitted Genesis; kernel catch_up_from alone decides
                // certificate authority and same/newer-view conflict safety.
                Self::validate_initial_replica(anchor, genesis, control)?;
                validate_genesis(anchor, genesis, control, prepare)?;
                (ReplayState::new(anchor.clone()), 0)
            } else {
                let recovered = Self::recover(anchor, control)?;
                (recovered.state, recovered.replay_bytes)
            };
            let mut replay = AcceptedReplay {
                state,
                bytes,
                next: prepare.index,
            };
            replay.append(prepare.index, &historical, command, prepare.bytes as usize)?;
            let current = control.membership_lineage()?;
            let next = control.next_index().0.max(replay.next.0);
            if next > MAX_CONTROL_RECORDS as u64 + 1 {
                return Err(invalid(
                    "control accepted suffix exceeds qualified record bound",
                ));
            }
            for index in replay.next.0..next {
                let retained = control
                    .prepared(LogIndex(index))
                    .ok_or_else(|| corrupt("control surviving accepted suffix has a gap"))?;
                let authority = control
                    .membership_lineage_at(retained.view)?
                    .ok_or_else(|| corrupt("control surviving suffix has unknown lineage"))?;
                if retained.pg != anchor.control_pg
                    || retained.index != LogIndex(index)
                    || authority != current
                {
                    return Err(invalid(
                        "control surviving suffix crosses membership authority",
                    ));
                }
                replay.append(
                    retained.index,
                    &authority,
                    checked_command(retained)?,
                    retained.bytes as usize,
                )?;
            }
            replay.reserve_finalization()?;
        }
        if stamp != control.recovery_state_token()? {
            return Err(corrupt("control owner changed during committed preflight"));
        }
        Ok(())
    }
}

fn validate_binding(
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
    control: &DurableVsrCell,
) -> RsfResult<()> {
    anchor.validate()?;
    if genesis.root()? != anchor.genesis_catalog
        || genesis.cluster != anchor.cluster
        || genesis.control_pg != anchor.control_pg
        || genesis
            .group(anchor.control_pg)
            .is_none_or(|group| group.lineage.bootstrap != anchor.control_bootstrap)
    {
        return Err(invalid(
            "configured control Genesis differs from provisioned anchor",
        ));
    }
    let current = control.membership_lineage()?;
    if current.pg != anchor.control_pg || current.bootstrap != anchor.control_bootstrap {
        return Err(invalid(
            "configured control owner differs from provisioned bootstrap",
        ));
    }
    Ok(())
}
fn validate_genesis(
    anchor: &ControlBootstrap,
    genesis: &ControlCatalog,
    control: &DurableVsrCell,
    prepare: &VsrPrepare,
) -> RsfResult<()> {
    if prepare.pg != anchor.control_pg
        || prepare.index != LogIndex(1)
        || checked_command(prepare)? != ControlCommand::Genesis(genesis.clone())
    {
        return Err(invalid(
            "initial control command is not exact configured Genesis",
        ));
    }
    let historical = control
        .membership_lineage_at(prepare.view)?
        .ok_or_else(|| invalid("Genesis command has unknown historical lineage"))?;
    if !genesis
        .group(anchor.control_pg)
        .expect("validated Genesis group")
        .matches(&historical)
    {
        return Err(invalid(
            "Genesis command differs from provisioned membership lineage",
        ));
    }
    check_budget(prepare.bytes as usize, LogIndex(1), 0)
}
