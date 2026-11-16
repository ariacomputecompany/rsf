use super::*;
use std::sync::mpsc::{Receiver, TryRecvError, sync_channel};
use std::thread::JoinHandle;

#[derive(Debug)]
pub(super) enum Action {
    Tail(PlacementGroupId),
    Transfer(VsrCommittedEntry),
    Prepare(VsrPrepare),
    Commit(VsrCommit),
    Observe(identity::Context),
}
#[derive(Debug)]
pub(super) enum Reply {
    Tail(LogIndex),
    Stored,
    Observation(Box<observation::Observation>),
}
#[derive(Debug)]
pub(super) struct Flight {
    pub replica: NodeId,
    pub action: Arc<Action>,
    receiver: Receiver<Result<Reply, String>>,
    handle: Option<JoinHandle<()>>,
}
impl Flight {
    pub fn start(
        transport: NetworkVsrTransport,
        replica: NodeId,
        action: Action,
    ) -> Result<Self, String> {
        let (sender, receiver) = sync_channel(1);
        let action = Arc::new(action);
        let sent = Arc::clone(&action);
        let handle = std::thread::Builder::new()
            .name("rsf-control-writer".into())
            .spawn(move || {
                let _ = sender.send(sent.execute(transport, replica));
            })
            .map_err(|e| format!("cannot start control writer exchange: {e}"))?;
        Ok(Self {
            replica,
            action,
            receiver,
            handle: Some(handle),
        })
    }
    pub fn take_reply(&mut self) -> Option<Result<Reply, String>> {
        match self.receiver.try_recv() {
            Err(TryRecvError::Empty) => None,
            result => {
                let joined = self.handle.take().expect("one live writer worker").join();
                Some(if joined.is_err() {
                    Err("control writer transport panicked".into())
                } else {
                    result.unwrap_or_else(|_| Err("control writer transport disconnected".into()))
                })
            }
        }
    }
}
impl Drop for Flight {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
impl Action {
    fn execute(
        &self,
        mut transport: NetworkVsrTransport,
        replica: NodeId,
    ) -> Result<Reply, String> {
        match self {
            Self::Tail(pg) => transport
                .committed_tail(replica, *pg)
                .map(Reply::Tail)
                .map_err(|e| e.reason),
            Self::Transfer(entry) => transport
                .transfer_committed(replica, entry.clone())
                .map(|_| Reply::Stored)
                .map_err(|e| e.reason),
            Self::Prepare(prepare) => transport
                .acknowledge_prepare(VsrPrepareMessage {
                    replica,
                    prepare: prepare.clone(),
                })
                .map(|_| Reply::Stored)
                .map_err(|e| e.reason),
            Self::Commit(commit) => transport
                .acknowledge_commit(replica, commit.clone())
                .map(|_| Reply::Stored)
                .map_err(|e| e.reason),
            Self::Observe(context) => observation::remote(&transport, replica, context)
                .map(|report| Reply::Observation(Box::new(report))),
        }
    }
}
