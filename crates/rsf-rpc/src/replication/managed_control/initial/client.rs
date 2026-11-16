//! One authenticated bounded exchange; only the owned sender schedules mutations.
use super::*;
impl NetworkVsrTransport {
    pub(in crate::replication) fn initial_control_exchange(
        &self,
        target: NodeId,
        policy: &ControlInitialPolicy,
        request: &wire::Request,
    ) -> io::Result<wire::Reply> {
        let id = wire::policy_id(self.cluster, target, policy);
        let bytes = wire::encode_request(id, request)?;
        let reply = self.exchange_once(target, CONTROL_INITIAL, &bytes)?;
        wire::decode_reply(&reply, id, &bytes, request, policy.limits)
    }
}
