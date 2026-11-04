use super::*;
use rsf_control_state::{AdmissionRole, CatalogNode, CommittedIntent};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::replication) struct Reporter {
    pub node: NodeId,
    pub incarnation: u64,
    pub certificate: [u8; 32],
}
/// A private wire key is only data. It cannot create an observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::replication) struct Key {
    pub pg: PlacementGroupId,
    pub anchor: [u8; 32],
    pub intent: IntentId,
    pub parent: [u8; 32],
    pub revision: u64,
    pub begin: LogIndex,
    pub operation: [u8; 32],
    pub bootstrap: [u8; 32],
    pub plan: Option<[u8; 32]>,
    pub old: [u8; 32],
    pub new: [u8; 32],
}
#[derive(Clone, Debug)]
pub(in crate::replication) struct Context {
    pub key: Key,
    pub old: BTreeMap<NodeId, Reporter>,
}
impl Context {
    pub fn recover(state: &ControlState, cell: &DurableVsrCell) -> Result<Self, String> {
        let pending = state
            .pending()
            .ok_or("no committed pending membership intent")?;
        let committed = state
            .committed_intent(cell, pending.id().map_err(|e| e.message)?)
            .map_err(|e| e.message)?;
        let result = Self::from_owned(state, &committed)?;
        committed.validate_live(cell).map_err(|e| e.message)?;
        Ok(result)
    }
    fn from_owned(state: &ControlState, committed: &CommittedIntent) -> Result<Self, String> {
        let lineage = committed.old_control();
        let group = state
            .catalog()
            .group(lineage.pg)
            .ok_or("old control catalog group absent")?;
        if group.voters != lineage.voters
            || group.lineage.bootstrap != lineage.bootstrap
            || group.lineage.plan != lineage.plan
            || state.root() != committed.intent().parent
            || state.revision() != committed.intent().revision
        {
            return Err("pending intent differs from its original control catalog".into());
        }
        let old_nodes = state
            .catalog()
            .nodes
            .iter()
            .filter(|node| lineage.voters.contains(&node.id))
            .collect::<Vec<_>>();
        if old_nodes.len() != lineage.voters.len()
            || old_nodes
                .iter()
                .any(|node| node.role != AdmissionRole::Voter)
        {
            return Err("old control voter identity set is incomplete".into());
        }
        let proposed = committed
            .intent()
            .nodes
            .iter()
            .filter(|node| node.role == AdmissionRole::Voter)
            .collect::<Vec<_>>();
        let mut anchor = Vec::new();
        append_text(&mut anchor, committed.anchor().cluster.as_str());
        anchor.extend_from_slice(&committed.anchor().control_pg.0.to_le_bytes());
        anchor.extend_from_slice(&committed.anchor().control_bootstrap);
        anchor.extend_from_slice(&committed.anchor().genesis_catalog.0);
        let key = Key {
            pg: committed.anchor().control_pg,
            anchor: hash(b"RSF control observation anchor v1", &anchor),
            intent: committed.id(),
            parent: committed.intent().parent.0,
            revision: committed.intent().revision.0,
            begin: committed.begin_index(),
            operation: committed.operation_digest(),
            bootstrap: lineage.bootstrap,
            plan: lineage.plan,
            old: identities(&old_nodes)?,
            new: identities(&proposed)?,
        };
        let old = old_nodes
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    Reporter {
                        node: node.id,
                        incarnation: node.incarnation,
                        certificate: node.certificate,
                    },
                )
            })
            .collect();
        Ok(Self { key, old })
    }
    pub fn reporter(&self, node: NodeId, certificate: [u8; 32]) -> Result<Reporter, String> {
        let expected = self
            .old
            .get(&node)
            .ok_or("reporter is not an original control voter")?;
        if expected.certificate != certificate {
            return Err("control reporter certificate differs from original identity".into());
        }
        Ok(expected.clone())
    }
}
fn identities(nodes: &[&CatalogNode]) -> Result<[u8; 32], String> {
    if nodes.is_empty()
        || nodes.len() > rsf_control_state::MAX_NODES
        || nodes.windows(2).any(|pair| pair[0].id >= pair[1].id)
    {
        return Err("control voter identities are not sorted, unique and bounded".into());
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(nodes.len() as u64).to_le_bytes());
    for node in nodes {
        bytes.extend_from_slice(&node.id.0.to_le_bytes());
        bytes.extend_from_slice(&node.incarnation.to_le_bytes());
        bytes.extend_from_slice(&node.certificate);
        for value in [
            &node.public_key,
            &node.nonce,
            &node.trust_domain,
            &node.spiffe_id,
        ] {
            append_text(&mut bytes, value);
        }
    }
    Ok(hash(b"RSF control voter identities v1", &bytes))
}
fn append_text(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}
fn hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    hash.finalize().into()
}
