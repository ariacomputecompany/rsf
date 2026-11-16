use super::*;
use std::sync::mpsc::{Receiver, TryRecvError, sync_channel};
use std::thread::JoinHandle;

#[derive(Debug)]
pub(super) struct Flight {
    pub request: Arc<Request>,
    receiver: Receiver<io::Result<Reply>>,
    handle: Option<JoinHandle<()>>,
}
impl Flight {
    pub fn start(
        transport: NetworkVsrTransport,
        target: NodeId,
        policy: ControlInitialPolicy,
        request: Request,
    ) -> Result<Self, String> {
        let (sender, receiver) = sync_channel(1);
        let request = Arc::new(request);
        let sent = Arc::clone(&request);
        let handle = std::thread::Builder::new()
            .name("rsf-control-initial".into())
            .spawn(move || {
                let _ = sender.send(transport.initial_control_exchange(target, &policy, &sent));
            })
            .map_err(|e| format!("cannot start initial control exchange: {e}"))?;
        Ok(Self {
            request,
            receiver,
            handle: Some(handle),
        })
    }
    pub fn take_reply(&mut self) -> Option<io::Result<Reply>> {
        match self.receiver.try_recv() {
            Err(TryRecvError::Empty) => None,
            result => {
                let joined = self.handle.take().expect("one initial worker").join();
                Some(if joined.is_err() {
                    Err(invalid("initial control transport panicked"))
                } else {
                    result
                        .unwrap_or_else(|_| Err(invalid("initial control transport disconnected")))
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
