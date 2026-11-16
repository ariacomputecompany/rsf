//! Non-voting provisional ownership before the first guarded publication.
//! Trusted restrictions come from the caller's independently owned identity.
use super::*;
use crate::transfer::prefix::VsrPrefixLimits;
use prefix::{Continuation, Directory, PrefixContext, VsrImportPreview, append, guard, io, stage};
mod identity;
mod publication;
mod reclaim;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VsrInitialSelection {
    generation: u64,
    history_id: [u8; 32],
}
impl VsrInitialSelection {
    pub fn new(generation: u64, history_id: [u8; 32]) -> RsfResult<Self> {
        if generation == 0 {
            return Err(invalid_input(
                "initial selection generation must be nonzero",
            ));
        }
        Ok(Self {
            generation,
            history_id,
        })
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn history_id(&self) -> [u8; 32] {
        self.history_id
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VsrInitialRestriction {
    Reserved,
    Selected(VsrInitialSelection),
    Publishing {
        selection: VsrInitialSelection,
        source: NodeId,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VsrInitialState {
    Reserved,
    Selected(VsrInitialSelection),
    Publishing {
        selection: VsrInitialSelection,
        source: NodeId,
    },
    Published {
        selection: VsrInitialSelection,
        source: NodeId,
    },
}
impl From<VsrInitialRestriction> for VsrInitialState {
    fn from(value: VsrInitialRestriction) -> Self {
        match value {
            VsrInitialRestriction::Reserved => Self::Reserved,
            VsrInitialRestriction::Selected(s) => Self::Selected(s),
            VsrInitialRestriction::Publishing { selection, source } => {
                Self::Publishing { selection, source }
            }
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VsrInitialPolicy {
    pub pg: PlacementGroupId,
    pub local: NodeId,
    pub bootstrap: [u8; 32],
    pub limits: VsrPrefixLimits,
}
impl VsrInitialPolicy {
    fn validate(&self) -> RsfResult<()> {
        self.limits.validate()?;
        if self.bootstrap == [0; 32] {
            return Err(invalid_input("initial learner bootstrap is absent"));
        }
        Ok(())
    }
}
#[derive(Debug)]
pub enum VsrInitialBegin {
    Start(VsrHistoryDescriptor),
    Resume {
        selection: VsrInitialSelection,
        descriptor: VsrHistoryDescriptor,
    },
    Replace {
        expected: VsrInitialSelection,
        descriptor: VsrHistoryDescriptor,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VsrInitialCursor {
    selection: VsrInitialSelection,
    records: u64,
}
impl VsrInitialCursor {
    pub fn selection(&self) -> VsrInitialSelection {
        self.selection
    }
    pub fn records(&self) -> u64 {
        self.records
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VsrInitialStatus {
    state: VsrInitialState,
    records: u64,
}
impl VsrInitialStatus {
    pub fn state(&self) -> VsrInitialState {
        self.state
    }
    pub fn records(&self) -> u64 {
        self.records
    }
}
#[derive(Debug)]
pub enum VsrInitialError {
    SelectionConflict { actual: VsrInitialState },
    PublicationPinned,
    Failure(RsfError),
}
impl From<RsfError> for VsrInitialError {
    fn from(value: RsfError) -> Self {
        Self::Failure(value)
    }
}
impl std::fmt::Display for VsrInitialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SelectionConflict { actual } => {
                write!(f, "initial selection conflicts with {actual:?}")
            }
            Self::PublicationPinned => f.write_str("initial publication is pinned"),
            Self::Failure(e) => f.write_str(&e.message),
        }
    }
}
impl std::error::Error for VsrInitialError {}

/// Borrowed from the real owner only while its original physical guards hold.
/// No constructor/decoder exists; getters cannot mint completion or authority.
#[derive(Debug)]
pub struct VsrInitialTransition<'a> {
    policy: &'a VsrInitialPolicy,
    expected: VsrInitialState,
    next: VsrInitialState,
}
impl VsrInitialTransition<'_> {
    pub fn policy(&self) -> &VsrInitialPolicy {
        self.policy
    }
    pub fn expected(&self) -> VsrInitialState {
        self.expected
    }
    pub fn next(&self) -> VsrInitialState {
        self.next
    }
}

/// Contains the actual held published owner, only after all required barriers.
/// It is intentionally not Clone and has no provisional extraction operation.
#[derive(Debug)]
pub struct VsrInitialPublication {
    owner: LearnerHistoryStore,
    selection: VsrInitialSelection,
    source: NodeId,
}
impl VsrInitialPublication {
    pub fn selection(&self) -> VsrInitialSelection {
        self.selection
    }
    pub fn source(&self) -> NodeId {
        self.source
    }
    pub fn into_published(self) -> LearnerHistoryStore {
        self.owner
    }
}

// A completed initial transfer is not a published-prefix continuation: it
// has no prior ACTIVE anchor. Keep its original transfer guards with the actual
// replayed owner until the restrictive native and ACTIVE publication completes.
#[derive(Debug)]
struct CompletedTransfer {
    id: [u8; 32],
    flat: guard::Flat,
    descriptor: guard::Exact,
    directory: Directory,
}
#[derive(Debug)]
enum Body {
    Receiving {
        stage: Box<Stage>,
        context: PrefixContext,
    },
    Replayed {
        published: Box<Published>,
        token: VsrStateToken,
        writer: guard::Exact,
        transfer: CompletedTransfer,
    },
}
#[derive(Debug)]
struct Candidate {
    body: Body,
}
impl Candidate {
    fn id(&self) -> [u8; 32] {
        match &self.body {
            Body::Receiving { context, .. } => context.id,
            Body::Replayed { transfer, .. } => transfer.id,
        }
    }
    fn descriptor(&self) -> &VsrHistoryDescriptor {
        match &self.body {
            Body::Receiving { stage, .. } => &stage.descriptor,
            Body::Replayed { published, .. } => &published.descriptor,
        }
    }
    fn count(&self) -> u64 {
        match &self.body {
            Body::Receiving { stage, .. } => stage.count,
            Body::Replayed { published, .. } => published.descriptor.records,
        }
    }
    fn validate(&self) -> RsfResult<()> {
        if self.descriptor().id()? != self.id() {
            return Err(corrupt("initial candidate descriptor changed"));
        }
        match &self.body {
            Body::Receiving { stage, context } => {
                context.directory.validate()?;
                context.descriptor.validate()?;
                context.flat.validate()?;
                let Continuation::Staging(custody) = &context.continuation else {
                    return Err(corrupt("initial receiving continuation is not staging"));
                };
                stage.store.validate_owned_custody(custody)
            }
            Body::Replayed {
                published,
                token,
                writer,
                transfer,
            } => {
                transfer.directory.validate()?;
                transfer.descriptor.validate()?;
                transfer.flat.validate_cell(&published.cell)?;
                published
                    .bounded_flat
                    .as_ref()
                    .ok_or_else(|| corrupt("initial candidate lost bounded flat custody"))?
                    .validate()?;
                writer.validate()?;
                guard::owned_lock(
                    &published.cell.root.join(".vsr-writer.lock"),
                    &published.cell._writer_lock,
                )?;
                if published.cell.recovery_state_token()? != *token {
                    return Err(corrupt("initial candidate owner changed"));
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct InitialLearnerHistoryStore {
    root: PathBuf,
    policy: VsrInitialPolicy,
    state: VsrInitialState,
    candidate: Option<Candidate>,
    custody: identity::RootCustody,
    lock: Option<rsf_storage::ExclusiveFileLock>,
    failed: AtomicBool,
    completed: bool,
}
impl InitialLearnerHistoryStore {
    /// Restriction is independent trusted custody policy, not inferred from files.
    pub fn open(
        path: impl AsRef<Path>,
        policy: VsrInitialPolicy,
        restriction: VsrInitialRestriction,
    ) -> RsfResult<Self> {
        policy.validate()?;
        identity::open(path.as_ref(), policy, restriction)
    }
    fn validate_owned(&self) -> RsfResult<()> {
        self.custody.validate(
            &self.root,
            self.lock
                .as_ref()
                .ok_or_else(|| corrupt("initial owner consumed"))?,
        )?;
        if let Some(candidate) = &self.candidate {
            candidate.validate()?;
        }
        Ok(())
    }
    fn healthy(&self) -> RsfResult<()> {
        if self.completed || self.failed.load(Ordering::Relaxed) {
            return Err(corrupt(
                "initial learner durability failed or owner consumed; reopen required",
            ));
        }
        self.validate_owned()
            .inspect_err(|_| self.failed.store(true, Ordering::Relaxed))
    }
    fn directory(&self, id: &[u8; 32]) -> PathBuf {
        self.root
            .join(id.iter().map(|b| format!("{b:02x}")).collect::<String>())
    }
    pub fn status(&self) -> RsfResult<VsrInitialStatus> {
        self.healthy()?;
        Ok(VsrInitialStatus {
            state: self.state,
            records: self.candidate.as_ref().map_or(0, Candidate::count),
        })
    }
    fn check_descriptor(&self, descriptor: &VsrHistoryDescriptor) -> RsfResult<[u8; 32]> {
        self.policy.limits.check(descriptor.records, 0)?;
        if descriptor.probe.pg != self.policy.pg
            || descriptor.probe.voters.contains(&self.policy.local)
        {
            return Err(invalid_input(
                "initial descriptor PG/local voter identity mismatch",
            ));
        }
        descriptor.id()
    }
    fn cursor(&self, selection: VsrInitialSelection) -> RsfResult<VsrInitialCursor> {
        self.healthy()?;
        let candidate = self
            .candidate
            .as_ref()
            .ok_or_else(|| corrupt("initial selected candidate is missing"))?;
        if candidate.id() != selection.history_id {
            return Err(corrupt("initial selected candidate differs"));
        }
        Ok(VsrInitialCursor {
            selection,
            records: candidate.count(),
        })
    }
    pub fn begin(
        &mut self,
        request: VsrInitialBegin,
        persist: &mut impl FnMut(&VsrInitialTransition<'_>) -> RsfResult<()>,
    ) -> Result<VsrInitialCursor, VsrInitialError> {
        self.healthy()?;
        if matches!(
            self.state,
            VsrInitialState::Publishing { .. } | VsrInitialState::Published { .. }
        ) {
            return Err(VsrInitialError::PublicationPinned);
        }
        let (expected, descriptor, next) = match request {
            VsrInitialBegin::Start(descriptor) => {
                let id = self.check_descriptor(&descriptor)?;
                (
                    VsrInitialState::Reserved,
                    descriptor,
                    VsrInitialSelection::new(1, id)?,
                )
            }
            VsrInitialBegin::Resume {
                selection,
                descriptor,
            } => {
                let id = self.check_descriptor(&descriptor)?;
                if self.state != VsrInitialState::Selected(selection) || id != selection.history_id
                {
                    return Err(VsrInitialError::SelectionConflict { actual: self.state });
                }
                if self
                    .candidate
                    .as_ref()
                    .is_none_or(|candidate| candidate.descriptor() != &descriptor)
                {
                    return Err(corrupt(
                        "initial resume descriptor differs from retained selection",
                    )
                    .into());
                }
                self.renew_candidate()?;
                return self.cursor(selection).map_err(Into::into);
            }
            VsrInitialBegin::Replace {
                expected,
                descriptor,
            } => {
                let id = self.check_descriptor(&descriptor)?;
                if id == expected.history_id {
                    return Err(invalid_input("identical initial candidate requires Resume").into());
                }
                let generation = expected
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| invalid_input("initial selection generation overflow"))?;
                (
                    VsrInitialState::Selected(expected),
                    descriptor,
                    VsrInitialSelection::new(generation, id)?,
                )
            }
        };
        if self.state == VsrInitialState::Selected(next) {
            if self
                .candidate
                .as_ref()
                .is_none_or(|c| c.descriptor() != &descriptor)
            {
                return Err(
                    corrupt("initial retry descriptor differs from retained selection").into(),
                );
            }
            self.renew_candidate()?;
            return self.cursor(next).map_err(Into::into);
        }
        if self.state != expected {
            return Err(VsrInitialError::SelectionConflict { actual: self.state });
        }
        self.failed.store(true, Ordering::Relaxed);
        let existing = reclaim::inventory(self)?;
        if matches!(self.state, VsrInitialState::Selected(_)) {
            reclaim::clean_selected(self)?;
        } else if existing.len() == 2 && !existing.contains(&next.history_id) {
            // Reserved has no permission to reclaim a previous unacknowledged
            // factory. Resume one exact known descriptor before replacement.
            self.failed.store(false, Ordering::Relaxed);
            return Err(invalid_input(
                "reserved initial factory inventory is full; resume an exact retained descriptor",
            )
            .into());
        }
        let candidate = identity::prepare_candidate(self, descriptor, true)?;
        self.validate_owned()?;
        candidate.validate()?;
        let next_state = VsrInitialState::Selected(next);
        let result = persist(&VsrInitialTransition {
            policy: &self.policy,
            expected,
            next: next_state,
        });
        // The original A and new B guards both survive the external callback.
        self.validate_owned()?;
        candidate.validate()?;
        result?;
        let old = self.candidate.replace(candidate);
        self.state = next_state;
        // A is no longer selected only after the restrictive callback succeeds.
        drop(old);
        reclaim::clean_selected(self)?;
        self.failed.store(false, Ordering::Relaxed);
        self.cursor(next).map_err(Into::into)
    }
    fn renew_candidate(&mut self) -> RsfResult<()> {
        self.failed.store(true, Ordering::Relaxed);
        let candidate = self
            .candidate
            .as_mut()
            .ok_or_else(|| corrupt("initial selected candidate missing"))?;
        match &mut candidate.body {
            Body::Receiving { stage, context } => {
                stage::renew(
                    &mut stage.store,
                    context
                        .custody()
                        .ok_or_else(|| corrupt("initial stage custody absent"))?,
                )?;
            }
            Body::Replayed { published, .. } => {
                stage::renew(&mut published.cell.store, &published.cell.store_custody)?;
            }
        }
        self.validate_owned()?;
        self.failed.store(false, Ordering::Relaxed);
        Ok(())
    }
    pub fn append(
        &mut self,
        selection: &VsrInitialSelection,
        batch: &VsrHistoryBatch,
    ) -> Result<VsrInitialCursor, VsrInitialError> {
        self.healthy()?;
        if matches!(
            self.state,
            VsrInitialState::Publishing { .. } | VsrInitialState::Published { .. }
        ) {
            return Err(VsrInitialError::PublicationPinned);
        }
        if self.state != VsrInitialState::Selected(*selection) {
            return Err(VsrInitialError::SelectionConflict { actual: self.state });
        }
        let candidate = self
            .candidate
            .as_mut()
            .ok_or_else(|| corrupt("initial selected candidate missing"))?;
        let Body::Receiving { stage, context } = &mut candidate.body else {
            return Err(invalid_input(
                "completed initial candidate requires FINISH or replacement",
            )
            .into());
        };
        append::append(self.policy.pg, stage, context, &self.failed, batch)?;
        self.healthy()?;
        self.cursor(*selection).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod observation;
