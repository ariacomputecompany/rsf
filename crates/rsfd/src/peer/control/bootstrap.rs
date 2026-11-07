//! Fresh control initialization and explicit Genesis goal; no admission grants.
use super::*;
use rsf_core::{ErrorCode, RsfError};
use rsf_rpc::replication::ControlWriterProgress;

// Covers kernel construction, native callback, kernel postchecks and unwinding.
// A callback error must never leave a warm native owner reusable.
struct Attempt<'a> {
    identity: &'a mut StateIdentity,
    completed: bool,
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.identity.fail_bootstrap();
        }
    }
}
impl Trusted {
    fn check_initialized(&self, identity: &mut StateIdentity) -> Result<(), String> {
        self.check_existing_policy(identity)?;
        if self.bootstrap.is_none()
            || identity.reservation()?.map(Reservation::custody)
                != Some(&ControlCustody::InitializedVoter {})
        {
            return Err(
                "Genesis scheduling requires exact initialized native voter custody".into(),
            );
        }
        Ok(())
    }
    pub(in crate::peer) fn scheduler(&self) -> Option<Scheduler> {
        self.bootstrap.as_ref().map(|_| Scheduler::default())
    }
    pub(in crate::peer) fn open_voter(
        &self,
        state: &Path,
        identity: &mut StateIdentity,
    ) -> Result<Option<DurableVsrCell>, String> {
        if !self.voter() {
            return Ok(None);
        }
        self.check_existing_policy(identity)?;
        let mut attempt = Attempt {
            identity,
            completed: false,
        };
        let path = state.join(format!("pg-{}", self.pg().0));
        let cell = if let Some(spec) = &self.bootstrap {
            match attempt.identity.reservation()?.map(Reservation::custody) {
                Some(ControlCustody::ReservedVoter {}) => {
                    let provisional =
                        rsf_vsr::ProvisionalVsrVoter::open(&path, spec.clone(), self.policy.limits)
                            .map_err(|e| e.message)?;
                    attempt.identity.validate()?;
                    provisional
                        .finish_with(|completion| {
                            attempt
                                .identity
                                .persist_bootstrap(&self.policy, spec, completion)
                                .map_err(|message| {
                                    RsfError::new(ErrorCode::CorruptionDetected, message)
                                })
                        })
                        .map_err(|e| e.message)?
                }
                Some(ControlCustody::InitializedVoter {}) => {
                    // Once initialized, missing/corrupt storage is never a new factory.
                    DurableVsrCell::open_existing_bounded(path, self.pg(), self.policy.limits)
                        .map_err(|e| e.message)?
                }
                _ => return Err("fresh control voter has incompatible native custody".into()),
            }
        } else {
            DurableVsrCell::open_existing_bounded(path, self.pg(), self.policy.limits)
                .map_err(|e| e.message)?
        };
        self.check_existing_policy(attempt.identity)?;
        attempt.completed = true;
        Ok(Some(cell))
    }
}

#[derive(Default)]
pub(in crate::peer) struct Scheduler {
    queued: bool,
    observed: bool,
}
impl Scheduler {
    /// One bounded writer step. The writer independently rechecks current view,
    /// original membership, owned prefix and actual authenticated reply custody.
    pub(in crate::peer) fn poll(
        &mut self,
        local: NodeId,
        control: &Trusted,
        identity: &mut StateIdentity,
        server: &mut PeerReplicaServer,
    ) -> Result<Option<ControlWriterProgress>, String> {
        control.check_initialized(identity)?;
        if self.observed {
            return Ok(None);
        }
        let primary = server.control_primary();
        control.check_initialized(identity)?;
        if primary? != local {
            return Ok(None);
        }
        if !self.queued {
            let queued = server.request_control_genesis();
            control.check_initialized(identity)?;
            queued?;
            self.queued = true;
        }
        let progress = server.poll_control_writer();
        control.check_initialized(identity)?;
        let progress = progress?;
        match &progress {
            ControlWriterProgress::ObservedGenesisDelivery(_) => {
                // Volatile goal completion only. Retention/sending/admission
                // must acquire their own actual proof; this is not authority.
                self.observed = true;
            }
            ControlWriterProgress::Idle
            | ControlWriterProgress::GenesisRequired
            | ControlWriterProgress::GenesisLocallyCommitted
            | ControlWriterProgress::Waiting
            | ControlWriterProgress::Accepted { .. }
            | ControlWriterProgress::Retrying { .. } => {}
            // A future writer continuation must not become a native permission
            // merely because this Genesis-only scheduler sees its observation.
            _ => return Err("Genesis scheduler received unrelated control writer progress".into()),
        }
        Ok(Some(progress))
    }
}
