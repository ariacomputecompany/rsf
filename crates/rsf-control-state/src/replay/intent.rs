use super::*;

/// One pending intent derived from a healthy committed control history. There
/// is no public constructor/decoder, and a deserialized command/report cannot
/// create this value. This is neither old-control commit-delivery majority
/// evidence nor permission to fence a target, vote, or admit clients.
#[derive(Debug)]
pub struct CommittedIntent {
    anchor: ControlBootstrap,
    intent: MembershipIntent,
    id: IntentId,
    begin: LogIndex,
    old_control: VsrMembershipLineage,
    operation: [u8; 32],
    stamp: VsrStateToken,
}
impl ControlState {
    /// Observational receipt lookup for the complete canonical Begin. Like
    /// request_outcome, callers must validate this projection against their live
    /// owner before reporting it. A reused request with changed payload is a
    /// conflict even when the original command was deterministically rejected.
    pub fn intent_outcome(&self, intent: &MembershipIntent) -> RsfResult<Option<ControlOutcome>> {
        let id = intent.id()?;
        match self.state.receipts.get(&intent.request) {
            Some(receipt) if receipt.command != id.0 => Err(invalid(
                "control request conflicts with its retained canonical command",
            )),
            Some(receipt) => Ok(Some(receipt.outcome.clone())),
            None => Ok(None),
        }
    }

    /// Bind the exact pending Begin to its first successful committed index and
    /// historical membership authority. An ordinary reprepare changes no part
    /// of that stable operation identity. Refresh this value from replay after
    /// any live cell change/reopen; it never refreshes itself from caller DTOs.
    pub fn committed_intent(
        &self,
        control: &DurableVsrCell,
        id: IntentId,
    ) -> RsfResult<CommittedIntent> {
        self.validate_projection(control)?;
        let pending = self
            .state
            .pending
            .as_ref()
            .ok_or_else(|| invalid("no committed pending membership intent"))?;
        if pending.intent.id()? != id {
            return Err(invalid(
                "requested intent differs from committed pending work",
            ));
        }
        let receipt = self
            .state
            .receipts
            .get(&pending.intent.request)
            .ok_or_else(|| corrupt("pending intent lost its accepted request receipt"))?;
        if receipt.command != id.0
            || receipt.begin != Some(pending.begin)
            || receipt.outcome != (ControlOutcome::IntentAccepted { intent: id })
        {
            return Err(corrupt(
                "pending intent origin differs from accepted request receipt",
            ));
        }
        let prepared = control
            .prepared(pending.begin)
            .ok_or_else(|| corrupt("pending intent origin has no retained prepare"))?;
        let operation = control
            .committed_operation(pending.begin)
            .ok_or_else(|| corrupt("pending intent origin is not durably committed"))?;
        let VsrOperation::ReplicatedCommand { payload, .. } = operation else {
            return Err(corrupt("pending intent origin is not a control command"));
        };
        if prepared.pg != self.state.anchor.control_pg
            || prepared.index != pending.begin
            || &prepared.operation != operation
            || payload != &ControlCommand::Begin(pending.intent.clone()).encode()?
        {
            return Err(corrupt(
                "pending intent differs from its complete committed operation",
            ));
        }
        let historical = control
            .membership_lineage_at(prepared.view)?
            .ok_or_else(|| corrupt("pending intent origin lost historical control lineage"))?;
        if historical != pending.old_control {
            return Err(corrupt(
                "pending intent origin changed its historical control lineage",
            ));
        }
        let operation = operation_digest(operation)?;
        self.validate_projection(control)?;
        Ok(CommittedIntent {
            anchor: self.state.anchor.clone(),
            intent: pending.intent.clone(),
            id,
            begin: pending.begin,
            old_control: historical,
            operation,
            stamp: self.stamp.clone(),
        })
    }
}
impl CommittedIntent {
    pub fn anchor(&self) -> &ControlBootstrap {
        &self.anchor
    }
    pub fn intent(&self) -> &MembershipIntent {
        &self.intent
    }
    pub fn id(&self) -> IntentId {
        self.id
    }
    pub fn begin_index(&self) -> LogIndex {
        self.begin
    }
    pub fn old_control(&self) -> &VsrMembershipLineage {
        &self.old_control
    }
    /// Domain-separated digest of the full stable VSR operation (operation ID
    /// and canonical command bytes). View, primary and certificate are absent;
    /// the anchor and first index are separate mandatory protocol bindings.
    pub fn operation_digest(&self) -> [u8; 32] {
        self.operation
    }
    pub fn validate_live(&self, control: &DurableVsrCell) -> RsfResult<()> {
        if self.stamp != control.recovery_state_token()? {
            return Err(invalid(
                "committed intent custody changed; replay before use",
            ));
        }
        Ok(())
    }
}
pub(super) fn operation_digest(operation: &VsrOperation) -> RsfResult<[u8; 32]> {
    let VsrOperation::ReplicatedCommand {
        operation_id,
        payload,
    } = operation
    else {
        return Err(corrupt(
            "committed intent requires a replicated control operation",
        ));
    };
    if payload.len() > MAX_COMMAND_BYTES {
        return Err(corrupt("committed control operation exceeds command bound"));
    }
    let mut bytes = Vec::with_capacity(16 + payload.len());
    bytes.extend_from_slice(&operation_id.0.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(codec::hash(b"RSF committed control operation v1", &bytes))
}
