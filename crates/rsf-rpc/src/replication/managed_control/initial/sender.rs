//! One volatile, custody-bound initial transfer. No native factory or admission.
use super::*;
use wire::{Reply, Request};
mod worker;

/// A historical authenticated publication observation, not a retention proof,
/// role transition, durable delivery certificate, Ready, or admission permit.
#[derive(Debug)]
pub struct ObservedControlInitialPublication {
    target: NodeId,
    origin: VsrInitialSelection,
    current: [u8; 32],
    tail: LogIndex,
}
impl ObservedControlInitialPublication {
    pub fn target(&self) -> NodeId {
        self.target
    }
    pub fn origin(&self) -> VsrInitialSelection {
        self.origin
    }
    pub fn current_history(&self) -> [u8; 32] {
        self.current
    }
    pub fn committed_tail(&self) -> LogIndex {
        self.tail
    }
}
#[derive(Debug)]
pub enum ControlInitialProgress {
    Idle,
    Waiting,
    Advanced {
        target: NodeId,
        records: u64,
    },
    Retrying {
        target: NodeId,
        reason: String,
    },
    /// No replacement was authorized for this exact durable selection.
    Conflict {
        target: NodeId,
        state: ControlInitialState,
    },
    /// Publication is pinned; no cancellation or replacement was attempted.
    Pinned {
        target: NodeId,
    },
    Observed(ObservedControlInitialPublication),
}
#[derive(Debug)]
pub(in crate::replication::managed_control) struct Sender {
    target: NodeId,
    policy: ControlInitialPolicy,
    source: ControlInitialSource,
    replacement: Option<VsrInitialSelection>,
    next: Option<Request>,
    flight: Option<worker::Flight>,
    terminal: bool,
    retry_at: Option<Instant>,
}
impl PeerReplicaServer {
    /// Schedule one learner using actual current-primary Genesis delivery.
    /// Exact replacement is an explicit authorization to abandon `expected`;
    /// neither a different history nor an ABA generation is implicitly replaced.
    /// A restart must recollect delivery and query the receiver's durable state.
    pub fn request_initial_control_transfer(
        &mut self,
        delivery: &writer::ObservedControlGenesisDelivery,
        target: NodeId,
        limits: VsrPrefixLimits,
        replacement: Option<VsrInitialSelection>,
    ) -> Result<(), String> {
        let binding = self
            .managed_control
            .as_ref()
            .ok_or("control is not configured")?;
        if binding.initial_sender.is_some() || binding.writer.has_flight() {
            return Err("control transport already owns active work".into());
        }
        let policy = ControlInitialPolicy {
            anchor: binding.anchor.clone(),
            genesis: binding.genesis.clone(),
            limits,
        };
        policy.validate_local(target)?;
        policy
            .authorize(self.local, target)
            .map_err(|e| e.to_string())?;
        let configured = policy
            .genesis
            .nodes
            .iter()
            .find(|node| node.id == target)
            .ok_or("initial target absent from catalog")?;
        let peer = self
            .control_transport
            .peers
            .get(&target)
            .ok_or("initial target has no configured TLS endpoint")?;
        if peer.fingerprint != configured.certificate {
            return Err("initial target certificate differs from configured catalog".into());
        }
        if replacement.is_some_and(|expected| expected.generation() == u64::MAX) {
            return Err("initial replacement generation exhausted".into());
        }
        let source = self
            .export_initial_control_source(delivery, limits)
            .map_err(|e| e.to_string())?;
        self.validate_initial_control_source(&source)
            .map_err(|e| e.to_string())?;
        if replacement.is_some_and(|expected| {
            source
                .descriptor()
                .id()
                .is_ok_and(|id| id == expected.history_id())
        }) {
            return Err("initial replacement requires a different history; use Resume".into());
        }
        self.managed_control
            .as_mut()
            .expect("validated binding")
            .initial_sender = Some(Sender {
            target,
            policy,
            source,
            replacement,
            next: Some(Request::Status),
            flight: None,
            terminal: false,
            retry_at: None,
        });
        Ok(())
    }

    /// Cancel only this process-local scheduling attempt. A bounded in-flight
    /// exchange is joined and may block for its remaining configured socket
    /// deadline; remote effects may already be durable. This never
    /// deletes or abandons receiver state. A later attempt must query Status.
    pub fn cancel_initial_control_transfer(&mut self) -> bool {
        self.managed_control
            .as_mut()
            .and_then(|binding| binding.initial_sender.take())
            .is_some()
    }

    /// Starts at most one bounded exchange or consumes one completed exchange.
    /// Only the transport and one finite request enter a worker. The server
    /// retains every source owner; source custody dominates every result/error.
    /// Writer polling waits while this scheduler is active, preserving one
    /// control worker plus the independent existing membership worker.
    pub fn poll_initial_control_transfer(&mut self) -> Result<ControlInitialProgress, String> {
        let Some(mut sender) = self
            .managed_control
            .as_mut()
            .and_then(|binding| binding.initial_sender.take())
        else {
            return Ok(ControlInitialProgress::Idle);
        };
        let result = sender.poll(self);
        // Semantic/source failure terminates this volatile attempt. The caller
        // must obtain fresh authority; it cannot restore a persisted DTO here.
        if result.is_ok() && !sender.terminal {
            self.managed_control
                .as_mut()
                .expect("binding retained")
                .initial_sender = Some(sender);
        }
        result
    }
}
impl Sender {
    fn validate(&self, server: &PeerReplicaServer) -> Result<(), String> {
        server
            .validate_initial_control_source(&self.source)
            .map_err(|e| e.to_string())
    }
    fn poll(&mut self, server: &PeerReplicaServer) -> Result<ControlInitialProgress, String> {
        self.validate(server)?;
        if self.retry_at.is_some_and(|at| Instant::now() < at) {
            return Ok(ControlInitialProgress::Waiting);
        }
        self.retry_at = None;
        if let Some(flight) = &mut self.flight {
            let Some(result) = flight.take_reply() else {
                return Ok(ControlInitialProgress::Waiting);
            };
            let flight = self.flight.take().expect("completed flight");
            // Even a semantic refusal or transport failure cannot hide a lost
            // original source inode/lock. No reply is trusted before this check.
            self.validate(server)?;
            let reply = match result {
                Ok(reply) => reply,
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    return Err(error.to_string());
                }
                Err(error) => {
                    // Never guess whether a mutation took effect. Next poll
                    // queries the actual durable receiver, then resumes it.
                    self.next = Some(Request::Status);
                    self.retry_at = Some(Instant::now() + server.control_transport.timeout);
                    return Ok(ControlInitialProgress::Retrying {
                        target: self.target,
                        reason: error.to_string(),
                    });
                }
            };
            let progress = self.accept(server, &flight.request, reply);
            self.validate(server)?;
            return progress;
        }
        let request = self
            .next
            .take()
            .ok_or("initial sender lost its next action")?;
        self.validate(server)?;
        let result = worker::Flight::start(
            server.control_transport.clone(),
            self.target,
            self.policy.clone(),
            request,
        );
        self.validate(server)?;
        self.flight = Some(result?);
        Ok(ControlInitialProgress::Waiting)
    }
    fn desired(&self) -> Result<Option<VsrInitialSelection>, String> {
        self.replacement
            .map(|expected| {
                VsrInitialSelection::new(
                    expected
                        .generation()
                        .checked_add(1)
                        .ok_or("initial generation overflow")?,
                    self.source.descriptor().id().map_err(|e| e.message)?,
                )
                .map_err(|e| e.message)
            })
            .transpose()
    }
    fn matches(&self, selected: VsrInitialSelection) -> Result<bool, String> {
        Ok(
            selected.history_id() == self.source.descriptor().id().map_err(|e| e.message)?
                && self.desired()?.is_none_or(|desired| selected == desired),
        )
    }
    fn conflict(&mut self, state: ControlInitialState) -> ControlInitialProgress {
        self.terminal = true;
        ControlInitialProgress::Conflict {
            target: self.target,
            state,
        }
    }
    fn advance(
        &mut self,
        server: &PeerReplicaServer,
        selected: VsrInitialSelection,
        records: u64,
    ) -> Result<ControlInitialProgress, String> {
        if !self.matches(selected)? || records > self.source.descriptor().records {
            return Err("initial cursor differs from owned source/authorized generation".into());
        }
        self.next = Some(if records == self.source.descriptor().records {
            Request::Finish(selected)
        } else {
            let batch = server
                .read_initial_control_batch(&self.source, records)
                .map_err(|e| e.to_string())?;
            if batch.ordinal != records
                || batch.records.is_empty()
                || records
                    .checked_add(batch.records.len() as u64)
                    .is_none_or(|end| end > self.source.descriptor().records)
            {
                return Err("initial source produced a non-progressing or overrun batch".into());
            }
            Request::Batch {
                selection: selected,
                batch,
            }
        });
        Ok(ControlInitialProgress::Advanced {
            target: self.target,
            records,
        })
    }
    fn accept(
        &mut self,
        server: &PeerReplicaServer,
        request: &Request,
        reply: Reply,
    ) -> Result<ControlInitialProgress, String> {
        match reply {
            Reply::Progress { state, .. } => {
                // Status is only scheduling data; Resume rechecks the complete
                // descriptor and returns the actual retained cursor.
                self.next = Some(match state {
                    ControlInitialState::Reserved if self.replacement.is_none() => {
                        Request::Start(self.source.descriptor().clone())
                    }
                    ControlInitialState::Selected(selected)
                        if self.replacement == Some(selected) =>
                    {
                        Request::Replace {
                            expected: selected,
                            descriptor: self.source.descriptor().clone(),
                        }
                    }
                    ControlInitialState::Selected(selected) if self.matches(selected)? => {
                        Request::Resume {
                            selection: selected,
                            descriptor: self.source.descriptor().clone(),
                        }
                    }
                    ControlInitialState::Publishing { selection, source }
                        if self.matches(selection)?
                            && source == self.source.descriptor().probe.primary =>
                    {
                        Request::Finish(selection)
                    }
                    _ => return Ok(self.conflict(state)),
                });
                Ok(ControlInitialProgress::Waiting)
            }
            Reply::Cursor { selection, records } => self.advance(server, selection, records),
            Reply::Published {
                origin,
                current,
                tail,
            }
            | Reply::Initialized {
                origin,
                current,
                tail,
            } => {
                if !self.matches(origin)?
                    || tail < LogIndex(1)
                    || !matches!(request, Request::Status | Request::Finish(_))
                {
                    return Err(
                        "initial publication differs from owned source/authorized generation"
                            .into(),
                    );
                }
                self.terminal = true;
                Ok(ControlInitialProgress::Observed(
                    ObservedControlInitialPublication {
                        target: self.target,
                        origin,
                        current,
                        tail,
                    },
                ))
            }
            Reply::Conflict(state) => Ok(self.conflict(state)),
            Reply::Pinned => {
                self.terminal = true;
                Ok(ControlInitialProgress::Pinned {
                    target: self.target,
                })
            }
        }
    }
}
