//! Retained native identity and monotonic restrictions, never VSR authority.
use super::*;
use rsf_rpc::replication::{
    ControlInitialBinding, ControlInitialPolicy, ControlInitialState, ControlInitialTransition,
};
mod bootstrap;
mod custody;
mod schema;
#[cfg(test)]
pub(super) mod tests;
pub(super) use schema::{ControlCustody, Reservation};
const MAX_GROUPS: usize = 4096;
const MAX_IDENTITY_BYTES: u64 = 128 * 1024;

pub(crate) struct StateIdentity {
    custody: custody::Custody,
    value: schema::Value,
    failed: bool,
}
impl std::fmt::Debug for StateIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateIdentity")
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}
impl StateIdentity {
    /// The caller validates trusted provisioning before creating a reservation.
    /// Acquisition retains the exact same peer-lock descriptor for this owner.
    pub(super) fn open(
        root: &Path,
        cluster: &ClusterId,
        node: NodeId,
        reservation: Option<Reservation>,
    ) -> Result<Self, String> {
        Self::open_inner(root, cluster, node, reservation, false)
    }
    fn open_inner(
        root: &Path,
        cluster: &ClusterId,
        node: NodeId,
        reservation: Option<Reservation>,
        existing: bool,
    ) -> Result<Self, String> {
        let custody = custody::Custody::open(root, existing)?;
        if existing && custody.bytes().is_none() {
            return Err("native identity is required".into());
        }
        let value = match custody.bytes() {
            Some(bytes) => schema::Value::decode(bytes)?,
            None => {
                custody.empty_factory()?;
                schema::Value {
                    cluster: cluster.as_str().into(),
                    node,
                    managed: BTreeSet::new(),
                    control: None,
                }
            }
        };
        if value.cluster != cluster.as_str() || value.node != node {
            return Err("peer state belongs to a different cluster or node".into());
        }
        match (&value.control, &reservation) {
            (Some(_), None) => {
                return Err("reserved control configuration cannot be omitted".into());
            }
            (Some(old), Some(next)) if !old.compatible_provisioning(next) => {
                return Err("reserved control policy or owner kind cannot change".into());
            }
            _ => {}
        }
        if value.control.is_none()
            && reservation.as_ref().is_some_and(|r| {
                matches!(
                    r.custody,
                    ControlCustody::ReservedLearner {} | ControlCustody::ReservedVoter {}
                )
            })
        {
            let pg = reservation.as_ref().expect("checked reservation").pg().0;
            for name in [format!("pg-{pg}"), format!("learner-{pg}")] {
                match std::fs::symlink_metadata(custody.root().join(name)) {
                    Ok(_) => return Err("existing control root requires explicit adoption".into()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err("cannot inspect native control reservation root".into()),
                }
            }
        }
        let mut owner = Self {
            custody,
            value,
            failed: false,
        };
        if owner.custody.bytes().is_none()
            || (owner.value.control.is_none() && reservation.is_some())
        {
            let mut next = owner.value.clone();
            next.control = reservation;
            owner.replace(next)?;
        } else {
            owner.physical(|custody| custody.renew())?;
        }
        owner.validate()?;
        Ok(owner)
    }
    pub(super) fn open_existing(
        root: &Path,
        cluster: &ClusterId,
        node: NodeId,
        reservation: Option<Reservation>,
    ) -> Result<Self, String> {
        Self::open_inner(root, cluster, node, reservation, true)
    }
    pub(super) fn validate(&mut self) -> Result<(), String> {
        if self.failed {
            return Err("native identity durability failed; reopen required".into());
        }
        if let Err(error) = boundary(Boundary::BeforeValidate, self.custody.root())
            .and_then(|()| self.custody.validate())
        {
            self.failed = true;
            return Err(error);
        }
        Ok(())
    }
    fn physical<T>(
        &mut self,
        action: impl FnOnce(&mut custody::Custody) -> Result<T, String>,
    ) -> Result<T, String> {
        self.validate()?;
        self.failed = true;
        let value = action(&mut self.custody)?;
        self.custody.validate()?;
        self.failed = false;
        Ok(value)
    }
    fn replace(&mut self, next: schema::Value) -> Result<(), String> {
        self.validate()?;
        let bytes = next.encode()?;
        self.physical(|custody| custody.publish(&bytes))?;
        self.value = next;
        Ok(())
    }
    pub(super) fn managed(&self) -> &BTreeSet<PlacementGroupId> {
        &self.value.managed
    }
    pub(super) fn reservation(&mut self) -> Result<Option<&Reservation>, String> {
        self.validate()?;
        Ok(self.value.control.as_ref())
    }
    /// Called after bound membership recovery has validated every new group.
    pub(super) fn enroll(&mut self, configured: &BTreeSet<PlacementGroupId>) -> Result<(), String> {
        self.validate()?;
        if !self.value.managed.is_subset(configured) || configured.len() > MAX_GROUPS {
            return Err("managed peer groups cannot be omitted or removed".into());
        }
        if self.value.managed == *configured {
            return Ok(());
        }
        let mut next = self.value.clone();
        next.managed = configured.clone();
        self.replace(next)
    }
    pub(super) fn initial_binding(
        &mut self,
        policy: &ControlInitialPolicy,
    ) -> Result<ControlInitialBinding, String> {
        self.validate()?;
        let reservation = self
            .value
            .control
            .as_ref()
            .ok_or("control reservation is absent")?;
        if !reservation.matches(policy)? {
            return Err("control policy differs from native reservation".into());
        }
        Ok(ControlInitialBinding {
            policy: policy.clone(),
            state: reservation.custody.initial()?,
        })
    }
    pub(super) fn persist_initial(
        &mut self,
        policy: &ControlInitialPolicy,
        event: &ControlInitialTransition<'_>,
    ) -> Result<(), String> {
        self.validate()?;
        let reservation = self
            .value
            .control
            .as_ref()
            .ok_or("control reservation is absent")?;
        if event.local() != self.value.node
            || event.policy().anchor != policy.anchor
            || event.policy().genesis != policy.genesis
            || event.policy().limits != policy.limits
            || !reservation.matches(policy)?
        {
            return Err("initial transition differs from native control policy".into());
        }
        let expected = reservation.custody.initial()?;
        if expected != event.expected() || !schema::permitted(expected, event.next()) {
            return Err("initial transition differs from native selected generation".into());
        }
        if let ControlInitialState::Publishing { source, .. }
        | ControlInitialState::Published { source, .. } = event.next()
            && (source == self.value.node
                || !policy
                    .genesis
                    .group(policy.anchor.control_pg)
                    .ok_or("control group absent")?
                    .voters
                    .contains(&source))
        {
            return Err("initial historical source is not an original voter".into());
        }
        let mut next = self.value.clone();
        next.control.as_mut().expect("reservation checked").custody =
            ControlCustody::from_initial(event.next());
        self.replace(next)
    }
    pub(super) fn require_unreserved_pg(&mut self, pg: PlacementGroupId) -> Result<(), String> {
        self.validate()?;
        if self.value.control.as_ref().is_some_and(|r| r.pg() == pg) {
            return Err("reserved control cannot use raw learner synchronization".into());
        }
        Ok(())
    }
    pub(crate) fn require_unreserved(&mut self) -> Result<(), String> {
        self.validate()?;
        if self.value.control.is_some() {
            return Err("reserved control cannot use legacy replication binding".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Boundary {
    BeforeValidate,
    BeforeWrite,
    BeforeFileSync,
    BeforeRename,
    AfterRename,
    BeforeDirectorySync,
    AfterDirectorySync,
}
fn boundary(at: Boundary, root: &Path) -> Result<(), String> {
    #[cfg(test)]
    {
        tests::boundary(at, root)
    }
    #[cfg(not(test))]
    {
        let _ = (at, root);
        Ok(())
    }
}
