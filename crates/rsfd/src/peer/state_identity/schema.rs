//! One bounded identity envelope; control restriction never replaces managed policy.
use super::*;
use rsf_rpc::replication::{ControlInitialPolicy, ControlInitialState};
use rsf_vsr::VsrInitialSelection;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub(in crate::peer) struct Hex32([u8; 32]);
impl From<Hex32> for String {
    fn from(value: Hex32) -> Self {
        value.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}
impl TryFrom<String> for Hex32 {
    type Error = String;
    fn try_from(value: String) -> Result<Self, String> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("control identity digest is not canonical".into());
        }
        let mut result = [0; 32];
        for (i, b) in result.iter_mut().enumerate() {
            *b = u8::from_str_radix(&value[i * 2..i * 2 + 2], 16)
                .map_err(|_| "invalid identity digest")?;
        }
        Ok(Self(result))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::peer) enum ControlCustody {
    ReservedLearner {},
    ProvisionalLearner {
        generation: u64,
        history_id: Hex32,
    },
    InitialPublication {
        generation: u64,
        history_id: Hex32,
        source: u64,
    },
    PublishedLearner {
        generation: u64,
        initial_history_id: Hex32,
        source: u64,
    },
    ExistingLearner {
        source: u64,
    },
    ReservedVoter {},
    InitializedVoter {},
    ExistingVoter {},
}
impl ControlCustody {
    pub(super) fn initial(&self) -> Result<ControlInitialState, String> {
        let selection = |generation, id: &Hex32| {
            VsrInitialSelection::new(generation, id.0).map_err(|e| e.message)
        };
        Ok(match self {
            Self::ReservedLearner {} => ControlInitialState::Reserved,
            Self::ProvisionalLearner {
                generation,
                history_id,
            } => ControlInitialState::Selected(selection(*generation, history_id)?),
            Self::InitialPublication {
                generation,
                history_id,
                source,
            } => ControlInitialState::Publishing {
                selection: selection(*generation, history_id)?,
                source: NodeId(*source),
            },
            Self::PublishedLearner {
                generation,
                initial_history_id,
                source,
            } => ControlInitialState::Published {
                selection: selection(*generation, initial_history_id)?,
                source: NodeId(*source),
            },
            _ => return Err("adopted control owner has no initial transfer state".into()),
        })
    }
    pub(super) fn from_initial(state: ControlInitialState) -> Self {
        match state {
            ControlInitialState::Reserved => Self::ReservedLearner {},
            ControlInitialState::Selected(s) => Self::ProvisionalLearner {
                generation: s.generation(),
                history_id: Hex32(s.history_id()),
            },
            ControlInitialState::Publishing {
                selection: s,
                source,
            } => Self::InitialPublication {
                generation: s.generation(),
                history_id: Hex32(s.history_id()),
                source: source.0,
            },
            ControlInitialState::Published {
                selection: s,
                source,
            } => Self::PublishedLearner {
                generation: s.generation(),
                initial_history_id: Hex32(s.history_id()),
                source: source.0,
            },
        }
    }
    fn validate(&self) -> Result<(), String> {
        match self {
            Self::ReservedVoter {}
            | Self::InitializedVoter {}
            | Self::ExistingVoter {}
            | Self::ExistingLearner { .. } => Ok(()),
            _ => self.initial().map(|_| ()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::peer) struct Reservation {
    pg: u64,
    bootstrap: Hex32,
    genesis_catalog: Hex32,
    max_records: u64,
    max_encoded_bytes: u64,
    pub(super) custody: ControlCustody,
}
impl Reservation {
    pub(in crate::peer) fn new(
        policy: &ControlInitialPolicy,
        custody: ControlCustody,
    ) -> Result<Self, String> {
        let result = Self {
            pg: policy.anchor.control_pg.0,
            bootstrap: Hex32(policy.anchor.control_bootstrap),
            genesis_catalog: Hex32(policy.anchor.genesis_catalog.0),
            max_records: policy.limits.max_records,
            max_encoded_bytes: policy.limits.max_encoded_bytes,
            custody,
        };
        result.validate()?;
        Ok(result)
    }
    pub(in crate::peer) fn compatible_provisioning(&self, requested: &Self) -> bool {
        let same_policy = self.pg == requested.pg
            && self.bootstrap == requested.bootstrap
            && self.genesis_catalog == requested.genesis_catalog
            && self.max_records == requested.max_records
            && self.max_encoded_bytes == requested.max_encoded_bytes;
        same_policy
            && match (&self.custody, &requested.custody) {
                (old, ControlCustody::ReservedLearner {}) => old.initial().is_ok(),
                (
                    ControlCustody::ReservedVoter {} | ControlCustody::InitializedVoter {},
                    ControlCustody::ReservedVoter {},
                ) => true,
                (a, b) => a == b,
            }
    }
    pub(in crate::peer) fn custody(&self) -> &ControlCustody {
        &self.custody
    }
    pub(in crate::peer) fn pg(&self) -> PlacementGroupId {
        PlacementGroupId(self.pg)
    }
    pub(in crate::peer) fn matches(&self, policy: &ControlInitialPolicy) -> Result<bool, String> {
        let expected = Self::new(policy, self.custody.clone())?;
        Ok(*self == expected)
    }
    fn validate(&self) -> Result<(), String> {
        rsf_vsr::VsrPrefixLimits {
            max_records: self.max_records,
            max_encoded_bytes: self.max_encoded_bytes,
        }
        .validate()
        .map_err(|e| e.message)?;
        if self.bootstrap.0 == [0; 32] || self.genesis_catalog.0 == [0; 32] {
            return Err("control reservation lacks original authority".into());
        }
        self.custody.validate()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Value {
    pub(super) cluster: String,
    pub(super) node: NodeId,
    pub(super) managed: BTreeSet<PlacementGroupId>,
    pub(super) control: Option<Reservation>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredIdentity {
    version: u64,
    cluster: String,
    node: u64,
    #[serde(
        default,
        deserialize_with = "present_groups",
        skip_serializing_if = "Option::is_none"
    )]
    managed_membership: Option<Vec<u64>>,
    #[serde(
        default,
        deserialize_with = "present_control",
        skip_serializing_if = "Option::is_none"
    )]
    reserved_control: Option<Reservation>,
}
fn present_groups<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Vec<u64>>, D::Error> {
    Vec::<u64>::deserialize(d).map(Some)
}
fn present_control<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<Reservation>, D::Error> {
    Reservation::deserialize(d).map(Some)
}
impl Value {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_IDENTITY_BYTES as usize {
            return Err("peer identity exceeds finite bound".into());
        }
        let stored: StoredIdentity =
            serde_json::from_slice(bytes).map_err(|_| "peer state identity is corrupt")?;
        ClusterId::new(&stored.cluster).map_err(|e| e.message)?;
        match (
            &stored.managed_membership,
            &stored.reserved_control,
            stored.version,
        ) {
            (None, None, 1) => {}
            (Some(groups), None, 2) if !groups.is_empty() => {}
            (Some(_), Some(reservation), 3) => reservation.validate()?,
            _ => return Err("invalid peer identity schema".into()),
        }
        let groups = stored.managed_membership.unwrap_or_default();
        if groups.len() > MAX_GROUPS || groups.windows(2).any(|p| p[0] >= p[1]) {
            return Err("managed peer groups must be bounded, unique and sorted".into());
        }
        Ok(Self {
            cluster: stored.cluster,
            node: NodeId(stored.node),
            managed: groups.into_iter().map(PlacementGroupId).collect(),
            control: stored.reserved_control,
        })
    }
    pub(super) fn encode(&self) -> Result<Vec<u8>, String> {
        let v3 = self.control.is_some();
        let stored = StoredIdentity {
            version: if v3 {
                3
            } else if self.managed.is_empty() {
                1
            } else {
                2
            },
            cluster: self.cluster.clone(),
            node: self.node.0,
            managed_membership: if v3 || !self.managed.is_empty() {
                Some(self.managed.iter().map(|g| g.0).collect())
            } else {
                None
            },
            reserved_control: self.control.clone(),
        };
        let bytes = serde_json::to_vec(&stored).map_err(|_| "cannot encode peer identity")?;
        if Self::decode(&bytes)? != *self {
            return Err("peer identity did not encode canonically".into());
        }
        Ok(bytes)
    }
}

pub(super) fn permitted(expected: ControlInitialState, next: ControlInitialState) -> bool {
    match (expected, next) {
        (ControlInitialState::Reserved, ControlInitialState::Selected(s)) => s.generation() == 1,
        (ControlInitialState::Selected(a), ControlInitialState::Selected(b)) => {
            a.generation().checked_add(1) == Some(b.generation())
                && a.history_id() != b.history_id()
        }
        (
            ControlInitialState::Selected(a),
            ControlInitialState::Publishing { selection: b, .. },
        ) => a == b,
        (
            ControlInitialState::Publishing {
                selection: a,
                source: x,
            },
            ControlInitialState::Published {
                selection: b,
                source: y,
            },
        ) => a == b && x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests;
