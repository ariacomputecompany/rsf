//! Canonical, allocation-bounded wire format. No serde map-key ambiguity.
use crate::*;
use rsf_core::{ClusterId, Digest, NodeId, PlacementGroupId};
use rsf_vsr::{VsrMembershipProposal, VsrMembershipPublishedQuorum};

const MAGIC: &[u8] = b"RSFCTRL1";

pub(crate) fn hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut input = Vec::with_capacity(domain.len() + 8 + bytes.len());
    input.extend_from_slice(domain);
    input.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    input.extend_from_slice(bytes);
    Digest::blake3_bytes(&input).bytes
}

impl ControlCommand {
    pub fn encode(&self) -> RsfResult<Vec<u8>> {
        let mut w = Writer::new();
        w.raw(MAGIC)?;
        match self {
            Self::Genesis(catalog) => {
                catalog.validate()?;
                w.byte(0)?;
                put_catalog(&mut w, catalog)?;
            }
            Self::Begin(intent) => {
                nonzero(&intent.request.0)?;
                nonzero(&intent.parent.0)?;
                if intent.revision.0 == 0 {
                    return Err(invalid("intent revision is zero"));
                }
                validate_nodes(&intent.nodes)?;
                intent.routing.validate()?;
                if intent.groups.len() > MAX_GROUPS
                    || intent.groups.windows(2).any(|p| p[0].pg >= p[1].pg)
                {
                    return Err(invalid("intent groups must be sorted, unique and bounded"));
                }
                w.byte(1)?;
                w.raw(&intent.request.0)?;
                w.raw(&intent.parent.0)?;
                w.u64(intent.revision.0)?;
                put_routing(&mut w, intent.routing)?;
                put_nodes(&mut w, &intent.nodes)?;
                w.count(intent.groups.len())?;
                for group in &intent.groups {
                    group.previous.validate()?;
                    bounded_proposal(&group.proposal)?;
                    w.u64(group.pg.0)?;
                    put_lineage(&mut w, group.previous)?;
                    w.blob(&group.proposal.encode()?)?;
                }
            }
            Self::Finalize(finalization) => {
                nonzero(&finalization.intent.0)?;
                nonzero(&finalization.parent.0)?;
                if finalization.proofs.len() > MAX_GROUPS
                    || finalization.proofs.windows(2).any(|p| p[0].pg >= p[1].pg)
                {
                    return Err(invalid(
                        "publication groups must be sorted, unique and bounded",
                    ));
                }
                w.byte(2)?;
                w.raw(&finalization.intent.0)?;
                w.raw(&finalization.parent.0)?;
                w.count(finalization.proofs.len())?;
                for proof in &finalization.proofs {
                    bounded_proposal(proof.quorum.proposal())?;
                    if proof.quorum.reports().len() > MAX_NODES {
                        return Err(invalid("too many publication reporters"));
                    }
                    nonzero(&proof.quorum.input_id())?;
                    nonzero(&proof.quorum.plan_id())?;
                    w.u64(proof.pg.0)?;
                    w.blob(&proof.quorum.encode()?)?;
                }
            }
        }
        Ok(w.0)
    }

    pub fn decode(bytes: &[u8]) -> RsfResult<Self> {
        if bytes.len() > MAX_COMMAND_BYTES || !bytes.starts_with(MAGIC) {
            return Err(corrupt("invalid control command header or size"));
        }
        let mut r = Reader {
            bytes,
            position: MAGIC.len(),
        };
        let command = match r.byte()? {
            0 => Self::Genesis(read_catalog(&mut r)?),
            1 => {
                let request = ControlRequestId(r.id()?);
                let parent = CatalogRoot(r.id()?);
                let revision = ControlRevision(r.u64()?);
                let routing = read_routing(&mut r)?;
                let nodes = read_nodes(&mut r)?;
                let count = r.count(MAX_GROUPS)?;
                let mut groups = Vec::with_capacity(count);
                for _ in 0..count {
                    groups.push(GroupIntent {
                        pg: PlacementGroupId(r.u64()?),
                        previous: read_lineage(&mut r)?,
                        proposal: VsrMembershipProposal::decode(r.blob(MAX_COMMAND_BYTES)?)
                            .map_err(|error| corrupt(error.message))?,
                    });
                }
                Self::Begin(MembershipIntent {
                    request,
                    parent,
                    revision,
                    routing,
                    nodes,
                    groups,
                })
            }
            2 => {
                let intent = IntentId(r.id()?);
                let parent = CatalogRoot(r.id()?);
                let count = r.count(MAX_GROUPS)?;
                let mut proofs = Vec::with_capacity(count);
                for _ in 0..count {
                    proofs.push(GroupPublication {
                        pg: PlacementGroupId(r.u64()?),
                        quorum: VsrMembershipPublishedQuorum::decode(r.blob(MAX_COMMAND_BYTES)?)
                            .map_err(|error| corrupt(error.message))?,
                    });
                }
                Self::Finalize(MembershipFinalization {
                    intent,
                    parent,
                    proofs,
                })
            }
            _ => return Err(corrupt("unknown control command variant")),
        };
        if r.position != bytes.len() {
            return Err(corrupt("trailing control command bytes"));
        }
        let canonical = command.encode().map_err(|error| corrupt(error.message))?;
        if canonical != bytes {
            return Err(corrupt("noncanonical control command"));
        }
        Ok(command)
    }
}

fn bounded_proposal(proposal: &VsrMembershipProposal) -> RsfResult<()> {
    if proposal.election.voters.len() > MAX_NODES || proposal.new_voters.len() > MAX_NODES {
        return Err(invalid("membership proposal exceeds control voter bound"));
    }
    Ok(())
}

pub(crate) fn catalog_bytes(catalog: &ControlCatalog) -> RsfResult<Vec<u8>> {
    let mut w = Writer::new();
    put_catalog(&mut w, catalog)?;
    Ok(w.0)
}
fn put_catalog(w: &mut Writer, catalog: &ControlCatalog) -> RsfResult<()> {
    w.text(catalog.cluster.as_str())?;
    w.u64(catalog.control_pg.0)?;
    put_routing(w, catalog.routing)?;
    put_nodes(w, &catalog.nodes)?;
    w.count(catalog.groups.len())?;
    for group in &catalog.groups {
        w.u64(group.pg.0)?;
        put_lineage(w, group.lineage)?;
        w.count(group.voters.len())?;
        for voter in &group.voters {
            w.u64(voter.0)?;
        }
    }
    Ok(())
}
fn read_catalog(r: &mut Reader<'_>) -> RsfResult<ControlCatalog> {
    let cluster = ClusterId::new(r.text()?).map_err(|error| corrupt(error.message))?;
    let control_pg = PlacementGroupId(r.u64()?);
    let routing = read_routing(r)?;
    let nodes = read_nodes(r)?;
    let count = r.count(MAX_GROUPS)?;
    let mut groups = Vec::with_capacity(count);
    for _ in 0..count {
        let pg = PlacementGroupId(r.u64()?);
        let lineage = read_lineage(r)?;
        let voters = (0..r.count(MAX_NODES)?)
            .map(|_| r.u64().map(NodeId))
            .collect::<RsfResult<_>>()?;
        groups.push(CatalogGroup {
            pg,
            lineage,
            voters,
        });
    }
    Ok(ControlCatalog {
        cluster,
        control_pg,
        routing,
        nodes,
        groups,
    })
}
fn put_lineage(w: &mut Writer, lineage: MembershipLineage) -> RsfResult<()> {
    w.raw(&lineage.bootstrap)?;
    match lineage.plan {
        None => w.byte(0),
        Some(plan) => {
            w.byte(1)?;
            w.raw(&plan)
        }
    }
}
fn read_lineage(r: &mut Reader<'_>) -> RsfResult<MembershipLineage> {
    let bootstrap = r.id()?;
    let plan = match r.byte()? {
        0 => None,
        1 => Some(r.id()?),
        _ => return Err(corrupt("invalid lineage tag")),
    };
    Ok(MembershipLineage { bootstrap, plan })
}
fn put_routing(w: &mut Writer, routing: CatalogRouting) -> RsfResult<()> {
    w.u64(routing.generation.0)?;
    w.raw(&routing.placement)?;
    w.raw(&routing.policy)
}
fn read_routing(r: &mut Reader<'_>) -> RsfResult<CatalogRouting> {
    Ok(CatalogRouting {
        generation: RoutingGeneration(r.u64()?),
        placement: r.id()?,
        policy: r.id()?,
    })
}
fn put_nodes(w: &mut Writer, nodes: &[CatalogNode]) -> RsfResult<()> {
    w.count(nodes.len())?;
    for node in nodes {
        w.u64(node.id.0)?;
        w.u64(node.incarnation)?;
        for text in [
            &node.public_key,
            &node.nonce,
            &node.trust_domain,
            &node.spiffe_id,
        ] {
            w.text(text)?;
        }
        w.raw(&node.certificate)?;
        w.byte(match node.role {
            AdmissionRole::Learner => 0,
            AdmissionRole::Voter => 1,
            AdmissionRole::Removed => 2,
        })?;
    }
    Ok(())
}
fn read_nodes(r: &mut Reader<'_>) -> RsfResult<Vec<CatalogNode>> {
    let count = r.count(MAX_NODES)?;
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(CatalogNode {
            id: NodeId(r.u64()?),
            incarnation: r.u64()?,
            public_key: r.text()?,
            nonce: r.text()?,
            trust_domain: r.text()?,
            spiffe_id: r.text()?,
            certificate: r.id()?,
            role: match r.byte()? {
                0 => AdmissionRole::Learner,
                1 => AdmissionRole::Voter,
                2 => AdmissionRole::Removed,
                _ => return Err(corrupt("unknown admission role")),
            },
        });
    }
    Ok(nodes)
}

struct Writer(Vec<u8>);
impl Writer {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn raw(&mut self, bytes: &[u8]) -> RsfResult<()> {
        if bytes.len() > MAX_COMMAND_BYTES.saturating_sub(self.0.len()) {
            return Err(invalid("control command exceeds VSR payload budget"));
        }
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn byte(&mut self, value: u8) -> RsfResult<()> {
        self.raw(&[value])
    }
    fn u64(&mut self, value: u64) -> RsfResult<()> {
        self.raw(&value.to_le_bytes())
    }
    fn count(&mut self, value: usize) -> RsfResult<()> {
        let value = u32::try_from(value).map_err(|_| invalid("control count overflow"))?;
        self.raw(&value.to_le_bytes())
    }
    fn blob(&mut self, value: &[u8]) -> RsfResult<()> {
        self.count(value.len())?;
        self.raw(value)
    }
    fn text(&mut self, value: &str) -> RsfResult<()> {
        self.blob(value.as_bytes())
    }
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> RsfResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| corrupt("control offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| corrupt("truncated control command"))?;
        self.position = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> RsfResult<u8> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> RsfResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }
    fn id(&mut self) -> RsfResult<[u8; 32]> {
        Ok(self.take(32)?.try_into().expect("length checked"))
    }
    fn count(&mut self, max: usize) -> RsfResult<usize> {
        let count = u32::from_le_bytes(self.take(4)?.try_into().expect("length checked")) as usize;
        if count > max {
            return Err(corrupt("control count exceeds bound"));
        }
        Ok(count)
    }
    fn blob(&mut self, max: usize) -> RsfResult<&'a [u8]> {
        let count = self.count(max)?;
        self.take(count)
    }
    fn text(&mut self) -> RsfResult<String> {
        String::from_utf8(self.blob(MAX_IDENTITY_BYTES)?.to_vec())
            .map_err(|_| corrupt("control text is not UTF-8"))
    }
}
