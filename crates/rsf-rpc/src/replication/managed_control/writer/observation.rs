use super::*;
use identity::{Context, Key, Reporter};

const KEY_BYTES: usize = 286;
const REPORT_BYTES: usize = KEY_BYTES + 48;

/// Only this module's authenticated exchange and owned local replay can mint an
/// observation. Decoded Report is intentionally a different private type.
#[derive(Debug)]
pub(in crate::replication) struct Observation {
    key: Key,
    reporter: Reporter,
}
struct Report {
    key: Key,
    reporter: Reporter,
}
#[derive(Debug)]
pub(in crate::replication) struct Collector {
    context: Context,
    reports: BTreeMap<NodeId, Reporter>,
}
impl Collector {
    pub fn new(context: Context) -> Self {
        Self {
            context,
            reports: BTreeMap::new(),
        }
    }
    pub fn key(&self) -> &Key {
        &self.context.key
    }
    pub fn insert(&mut self, observation: Observation) -> Result<(), String> {
        if observation.key != self.context.key
            || self.context.old.get(&observation.reporter.node) != Some(&observation.reporter)
        {
            return Err("mixed control delivery key or foreign reporter identity".into());
        }
        self.reports
            .insert(observation.reporter.node, observation.reporter);
        Ok(())
    }
    pub fn observed(&self) -> Option<ObservedOldControlDelivery> {
        (self.reports.len() > self.context.old.len() / 2).then(|| ObservedOldControlDelivery {
            key: Box::new(self.context.key.clone()),
            reporters: self.reports.keys().copied().collect(),
        })
    }
}
pub(in crate::replication) fn local(
    state: &ControlState,
    cell: &DurableVsrCell,
    node: NodeId,
    certificate: [u8; 32],
) -> Result<Observation, String> {
    let context = Context::recover(state, cell)?;
    Ok(Observation {
        reporter: context.reporter(node, certificate)?,
        key: context.key,
    })
}
pub(in crate::replication) fn remote(
    transport: &NetworkVsrTransport,
    replica: NodeId,
    context: &Context,
) -> Result<Observation, String> {
    let endpoint = transport
        .peers
        .get(&replica)
        .ok_or("control reporter endpoint absent")?;
    let expected = context.reporter(replica, endpoint.fingerprint)?;
    // One bounded TLS exchange. No recovery retry wrapper or additional socket.
    let bytes = transport
        .exchange_once(replica, CONTROL_COMMITTED, &context.key.encode())
        .map_err(|e| e.to_string())?;
    let report = Report::decode(&bytes).map_err(|e| e.to_string())?;
    if report.key != context.key || report.reporter != expected {
        return Err("authenticated control report differs from requested key/reporter".into());
    }
    Ok(Observation {
        key: report.key,
        reporter: report.reporter,
    })
}
impl PeerReplicaServer {
    pub(in crate::replication) fn dispatch_control_committed(
        &mut self,
        sender: NodeId,
        payload: &[u8],
    ) -> io::Result<Vec<u8>> {
        let requested = Key::decode(payload)?;
        let binding = self
            .managed_control
            .as_mut()
            .ok_or_else(|| invalid("control replication is not configured"))?;
        if binding.pg() != requested.pg {
            return Err(invalid("control query PG is not configured"));
        }
        let cell = self
            .cells
            .get(&requested.pg)
            .ok_or_else(|| invalid("old control delivery requires a hosted voter"))?;
        binding.refresh_voter(cell).map_err(invalid)?;
        let state = binding
            .voter
            .as_ref()
            .ok_or_else(|| invalid("control Genesis is not committed"))?;
        let context = Context::recover(state, cell).map_err(invalid)?;
        let sender_certificate = self
            .nodes
            .iter()
            .find_map(|(certificate, node)| (*node == sender).then_some(*certificate))
            .ok_or_else(|| invalid("unmapped control caller"))?;
        context
            .reporter(sender, sender_certificate)
            .map_err(invalid)?;
        if requested != context.key {
            return Err(invalid(
                "control query differs from exact pending committed Begin",
            ));
        }
        let observed =
            local(state, cell, self.local, self.identity.fingerprint).map_err(invalid)?;
        Ok(Report {
            key: observed.key,
            reporter: observed.reporter,
        }
        .encode())
    }
}
impl Key {
    pub(in crate::replication) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_BYTES);
        out.extend_from_slice(b"RSFQ1");
        out.extend_from_slice(&self.pg.0.to_le_bytes());
        for digest in [self.anchor, self.intent.0, self.parent] {
            out.extend_from_slice(&digest);
        }
        out.extend_from_slice(&self.revision.to_le_bytes());
        out.extend_from_slice(&self.begin.0.to_le_bytes());
        out.extend_from_slice(&self.operation);
        out.extend_from_slice(&self.bootstrap);
        out.push(u8::from(self.plan.is_some()));
        out.extend_from_slice(&self.plan.unwrap_or([0; 32]));
        out.extend_from_slice(&self.old);
        out.extend_from_slice(&self.new);
        out
    }
    pub(in crate::replication) fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != KEY_BYTES || &bytes[..5] != b"RSFQ1" {
            return Err(invalid("control query has wrong canonical size/domain"));
        }
        let mut cursor = 5;
        let pg = PlacementGroupId(number(bytes, &mut cursor));
        let anchor = digest(bytes, &mut cursor);
        let intent = IntentId(digest(bytes, &mut cursor));
        let parent = digest(bytes, &mut cursor);
        let revision = number(bytes, &mut cursor);
        let begin = LogIndex(number(bytes, &mut cursor));
        let operation = digest(bytes, &mut cursor);
        let bootstrap = digest(bytes, &mut cursor);
        let tag = bytes[cursor];
        cursor += 1;
        let plan = digest(bytes, &mut cursor);
        let plan = match (tag, plan) {
            (0, plan) if plan == [0; 32] => None,
            (1, plan) if plan != [0; 32] => Some(plan),
            _ => return Err(invalid("control query lineage is noncanonical")),
        };
        let old = digest(bytes, &mut cursor);
        let new = digest(bytes, &mut cursor);
        if revision == 0
            || begin.0 == 0
            || [anchor, intent.0, parent, operation, bootstrap, old, new].contains(&[0; 32])
        {
            return Err(invalid("control query contains an empty identity"));
        }
        Ok(Self {
            pg,
            anchor,
            intent,
            parent,
            revision,
            begin,
            operation,
            bootstrap,
            plan,
            old,
            new,
        })
    }
}
impl Report {
    fn encode(&self) -> Vec<u8> {
        let mut out = self.key.encode();
        out.extend_from_slice(&self.reporter.node.0.to_le_bytes());
        out.extend_from_slice(&self.reporter.incarnation.to_le_bytes());
        out.extend_from_slice(&self.reporter.certificate);
        out
    }
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != REPORT_BYTES {
            return Err(invalid("control report has wrong canonical size"));
        }
        let key = Key::decode(&bytes[..KEY_BYTES])?;
        let mut cursor = KEY_BYTES;
        let node = NodeId(number(bytes, &mut cursor));
        let incarnation = number(bytes, &mut cursor);
        let certificate = digest(bytes, &mut cursor);
        if incarnation == 0 || certificate == [0; 32] {
            return Err(invalid("control report identity is empty"));
        }
        Ok(Self {
            key,
            reporter: Reporter {
                node,
                incarnation,
                certificate,
            },
        })
    }
}
// Exact-size checks above make these fixed-width reads bounded and total.
fn number(bytes: &[u8], cursor: &mut usize) -> u64 {
    let value = u64::from_le_bytes(
        bytes[*cursor..*cursor + 8]
            .try_into()
            .expect("bounded number"),
    );
    *cursor += 8;
    value
}
fn digest(bytes: &[u8], cursor: &mut usize) -> [u8; 32] {
    let value = bytes[*cursor..*cursor + 32]
        .try_into()
        .expect("bounded digest");
    *cursor += 32;
    value
}
