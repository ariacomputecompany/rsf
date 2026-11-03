use super::*;
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::JoinHandle;

#[derive(Clone, Copy, Debug)]
pub(super) enum Kind {
    Begin { total: u64 },
    Batch { next: u64, total: u64 },
    Finish,
    Stage,
    Publish,
    Query,
    Choice(choice::exchange::Kind),
}

pub(super) enum Action {
    Choice(choice::exchange::Request),
    Upload {
        kind: Kind,
        payload: Vec<u8>,
    },
    Stage {
        chosen: VsrMembershipChosen,
        expected: VsrMembershipStagedReport,
    },
    Publish(Arc<VsrMembershipStagedQuorum>),
    Query {
        pg: PlacementGroupId,
        plan: [u8; 32],
    },
}
impl Action {
    pub(super) fn kind(&self) -> Result<Kind, String> {
        Ok(match self {
            Self::Choice(request) => Kind::Choice(request.kind()?),
            Self::Upload { kind, .. } => *kind,
            Self::Stage { .. } => Kind::Stage,
            Self::Publish(_) => Kind::Publish,
            Self::Query { .. } => Kind::Query,
        })
    }
    pub(super) fn phase(&self) -> &'static str {
        if let Self::Choice(request) = self {
            return request.phase();
        }
        match self {
            Self::Upload { .. } => "provisioning",
            Self::Stage { .. } => "staging",
            Self::Publish(_) => "publishing",
            Self::Query { .. } => "querying",
            Self::Choice(_) => unreachable!(),
        }
    }
    fn execute(self, transport: &NetworkVsrTransport, replica: NodeId) -> Result<Reply, String> {
        match self {
            Self::Choice(request) => request.execute(transport, replica).map(Reply::Choice),
            Self::Stage { chosen, expected } => transport
                .stage_membership_report(replica, &chosen, &expected)
                .map(Reply::Stage)
                .map_err(|e| e.reason),
            Self::Publish(quorum) => transport
                .publish_membership(replica, &quorum)
                .map(Reply::Publication)
                .map_err(|e| e.reason),
            Self::Query { pg, plan } => transport
                .query_membership_publication(replica, pg, plan)
                .map(Reply::Publication)
                .map_err(|e| e.reason),
            Self::Upload { kind, payload } => match kind {
                Kind::Begin { total } => {
                    let next = transport
                        .request_io(replica, MEMBERSHIP_AUTH_BEGIN, &payload)
                        .map_err(|e| e.to_string())?;
                    if next > total {
                        return Err("authority upload response exceeds chosen input".into());
                    }
                    Ok(Reply::Offset(next))
                }
                Kind::Batch { next, .. } => {
                    let actual = transport
                        .request_io(replica, MEMBERSHIP_AUTH_BATCH, &payload)
                        .map_err(|e| e.to_string())?;
                    if actual != next {
                        return Err("authority batch acknowledgement range mismatch".into());
                    }
                    Ok(Reply::Offset(actual))
                }
                Kind::Finish => {
                    let response = transport
                        .exchange_io(replica, MEMBERSHIP_AUTH_FINISH, &payload)
                        .map_err(|e| e.to_string())?;
                    if payload.len() != 40 || response != payload[8..] {
                        return Err("authority finish acknowledgement identity mismatch".into());
                    }
                    Ok(Reply::Finished)
                }
                _ => Err("invalid recovery upload operation".into()),
            },
        }
    }
}

#[derive(Debug)]
pub(super) enum Reply {
    Choice(choice::exchange::Response),
    Offset(u64),
    Finished,
    Stage(VsrMembershipStagedReport),
    Publication(VsrMembershipPublishedReport),
}

#[derive(Debug)]
pub(super) struct Worker {
    pub receiver: Receiver<Result<Reply, String>>,
    pub handle: JoinHandle<()>,
}

pub(super) fn spawn(
    transport: Arc<NetworkVsrTransport>,
    replica: NodeId,
    action: Action,
) -> Result<Worker, String> {
    let (sender, receiver) = sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("rsf-membership-recovery".into())
        .spawn(move || {
            let result = action.execute(&transport, replica);
            // One result, then return. The owned receiver may be dropped only
            // while the server closes; no application effects occur here.
            let _ = sender.send(result);
        })
        .map_err(|e| format!("cannot start membership recovery transport: {e}"))?;
    Ok(Worker { receiver, handle })
}
