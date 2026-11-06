//! Canonical finite initial requests and authenticated request-correlated replies.
use super::*;
use rsf_vsr::{VsrHistoryBatch, VsrHistoryDescriptor};
use sha2::{Digest, Sha256};
const REQUEST: &[u8; 6] = b"RSFIC1";
const RESPONSE: &[u8; 6] = b"RSFIR1";
const DESCRIPTOR_LIMIT: usize = 45 + 73 + 8 * 4096;
#[derive(Debug)]
pub(in crate::replication) enum Request {
    Status,
    Start(VsrHistoryDescriptor),
    Resume {
        selection: VsrInitialSelection,
        descriptor: VsrHistoryDescriptor,
    },
    Replace {
        expected: VsrInitialSelection,
        descriptor: VsrHistoryDescriptor,
    },
    Batch {
        selection: VsrInitialSelection,
        batch: VsrHistoryBatch,
    },
    Finish(VsrInitialSelection),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::replication) enum Reply {
    Progress {
        state: ControlInitialState,
        records: u64,
    },
    Cursor {
        selection: VsrInitialSelection,
        records: u64,
    },
    Published {
        origin: VsrInitialSelection,
        current: [u8; 32],
        tail: LogIndex,
    },
    Initialized {
        origin: VsrInitialSelection,
        current: [u8; 32],
        tail: LogIndex,
    },
    Conflict(ControlInitialState),
    Pinned,
}
pub(in crate::replication) fn policy_id(
    cluster: [u8; 32],
    local: NodeId,
    policy: &ControlInitialPolicy,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"RSFCI1");
    hash.update(cluster);
    hash.update(policy.anchor.control_pg.0.to_le_bytes());
    hash.update(policy.anchor.control_bootstrap);
    hash.update(policy.anchor.genesis_catalog.0);
    hash.update(local.0.to_le_bytes());
    hash.update(policy.limits.max_records.to_le_bytes());
    hash.update(policy.limits.max_encoded_bytes.to_le_bytes());
    hash.finalize().into()
}
fn selection(bytes: &[u8]) -> io::Result<VsrInitialSelection> {
    if bytes.len() != 40 {
        return Err(invalid("invalid initial selection length"));
    }
    VsrInitialSelection::new(u64_at(bytes, 0)?, bytes[8..40].try_into().unwrap()).map_err(rsf)
}
fn descriptor(bytes: &[u8], limits: VsrPrefixLimits) -> io::Result<VsrHistoryDescriptor> {
    if bytes.len() > DESCRIPTOR_LIMIT || bytes.len() < 45 || u64_at(bytes, 5)? > limits.max_records
    {
        return Err(invalid("initial descriptor exceeds configured bounds"));
    }
    let descriptor = VsrHistoryDescriptor::decode(bytes).map_err(rsf)?;
    if descriptor.encode().map_err(rsf)? != bytes {
        return Err(invalid("noncanonical initial descriptor"));
    }
    Ok(descriptor)
}
pub(in crate::replication) fn decode_request(
    bytes: &[u8],
    policy: [u8; 32],
    limits: VsrPrefixLimits,
) -> io::Result<Request> {
    if bytes.len() < 39 || !bytes.starts_with(REQUEST) || bytes[6..38] != policy {
        return Err(invalid("initial request differs from configured policy"));
    }
    let payload = &bytes[39..];
    Ok(match bytes[38] {
        0 if payload.is_empty() => Request::Status,
        1 => Request::Start(descriptor(payload, limits)?),
        2 | 3 if payload.len() >= 40 => {
            let selected = selection(&payload[..40])?;
            let descriptor = descriptor(&payload[40..], limits)?;
            if bytes[38] == 2 {
                Request::Resume {
                    selection: selected,
                    descriptor,
                }
            } else {
                Request::Replace {
                    expected: selected,
                    descriptor,
                }
            }
        }
        4 if payload.len() >= 57 => {
            let selected = selection(&payload[..40])?;
            let encoded = &payload[40..];
            let count = u32::from_le_bytes(encoded[13..17].try_into().unwrap()) as u64;
            let framing = 17u64
                .checked_add(
                    count
                        .checked_mul(4)
                        .ok_or_else(|| invalid("initial batch length overflow"))?,
                )
                .ok_or_else(|| invalid("initial batch length overflow"))?;
            if count == 0
                || count > rsf_vsr::MAX_HISTORY_BATCH_RECORDS as u64
                || count > limits.max_records
                || u64_at(encoded, 5)?.checked_add(count).is_none()
                || (encoded.len() as u64)
                    .checked_sub(framing)
                    .is_none_or(|size| size > limits.max_encoded_bytes)
            {
                return Err(invalid("initial batch exceeds configured bounds"));
            }
            Request::Batch {
                selection: selected,
                batch: VsrHistoryBatch::decode(encoded).map_err(rsf)?,
            }
        }
        5 => Request::Finish(selection(payload)?),
        _ => return Err(invalid("invalid initial request operation or length")),
    })
}
fn put_selection(bytes: &mut Vec<u8>, selection: VsrInitialSelection) {
    bytes.extend_from_slice(&selection.generation().to_le_bytes());
    bytes.extend_from_slice(&selection.history_id());
}
fn put_state(bytes: &mut Vec<u8>, state: ControlInitialState) {
    match state {
        ControlInitialState::Reserved => bytes.push(0),
        ControlInitialState::Selected(s) => {
            bytes.push(1);
            put_selection(bytes, s);
        }
        ControlInitialState::Publishing { selection, source }
        | ControlInitialState::Published { selection, source } => {
            bytes.push(if matches!(state, ControlInitialState::Publishing { .. }) {
                2
            } else {
                3
            });
            put_selection(bytes, selection);
            bytes.extend_from_slice(&source.0.to_le_bytes());
        }
    }
}
pub(in crate::replication) fn encode_reply(
    request: &[u8],
    policy: [u8; 32],
    reply: &Reply,
) -> io::Result<Vec<u8>> {
    let mut bytes = RESPONSE.to_vec();
    bytes.extend_from_slice(&policy);
    bytes.extend_from_slice(&Sha256::digest(request));
    match reply {
        Reply::Progress { state, records } => {
            bytes.push(0);
            put_state(&mut bytes, *state);
            bytes.extend_from_slice(&records.to_le_bytes());
        }
        Reply::Cursor { selection, records } => {
            bytes.push(1);
            put_selection(&mut bytes, *selection);
            bytes.extend_from_slice(&records.to_le_bytes());
        }
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
            bytes.push(if matches!(reply, Reply::Published { .. }) {
                2
            } else {
                3
            });
            put_selection(&mut bytes, *origin);
            bytes.extend_from_slice(current);
            bytes.extend_from_slice(&tail.0.to_le_bytes());
        }
        Reply::Conflict(state) => {
            bytes.push(4);
            put_state(&mut bytes, *state);
        }
        Reply::Pinned => bytes.push(5),
    }
    Ok(bytes)
}
#[cfg(test)]
pub(in crate::replication) fn encode_request(
    policy: [u8; 32],
    request: &Request,
) -> io::Result<Vec<u8>> {
    let mut bytes = REQUEST.to_vec();
    bytes.extend_from_slice(&policy);
    match request {
        Request::Status => bytes.push(0),
        Request::Start(d) => {
            bytes.push(1);
            bytes.extend_from_slice(&d.encode().map_err(rsf)?);
        }
        Request::Resume {
            selection,
            descriptor,
        }
        | Request::Replace {
            expected: selection,
            descriptor,
        } => {
            bytes.push(if matches!(request, Request::Resume { .. }) {
                2
            } else {
                3
            });
            put_selection(&mut bytes, *selection);
            bytes.extend_from_slice(&descriptor.encode().map_err(rsf)?);
        }
        Request::Batch { selection, batch } => {
            bytes.push(4);
            put_selection(&mut bytes, *selection);
            bytes.extend_from_slice(&batch.encode().map_err(rsf)?);
        }
        Request::Finish(s) => {
            bytes.push(5);
            put_selection(&mut bytes, *s);
        }
    }
    Ok(bytes)
}
#[cfg(test)]
fn read_state(bytes: &[u8]) -> io::Result<(ControlInitialState, usize)> {
    Ok(match bytes.first() {
        Some(0) => (ControlInitialState::Reserved, 1),
        Some(1) if bytes.len() >= 41 => {
            (ControlInitialState::Selected(selection(&bytes[1..41])?), 41)
        }
        Some(2 | 3) if bytes.len() >= 49 => {
            let selection = selection(&bytes[1..41])?;
            let source = NodeId(u64_at(bytes, 41)?);
            (
                if bytes[0] == 2 {
                    ControlInitialState::Publishing { selection, source }
                } else {
                    ControlInitialState::Published { selection, source }
                },
                49,
            )
        }
        _ => return Err(invalid("malformed initial response state")),
    })
}
#[cfg(test)]
pub(in crate::replication) fn decode_reply(
    bytes: &[u8],
    policy: [u8; 32],
    request: &[u8],
    expected: &Request,
    limits: VsrPrefixLimits,
) -> io::Result<Reply> {
    if bytes.len() < 71
        || bytes.len() > 160
        || !bytes.starts_with(RESPONSE)
        || bytes[6..38] != policy
        || bytes[38..70] != Sha256::digest(request)[..]
    {
        return Err(invalid(
            "initial reply differs from authenticated requested context",
        ));
    }
    let payload = &bytes[71..];
    let reply = match bytes[70] {
        0 => {
            let (state, n) = read_state(payload)?;
            if payload.len() != n + 8 {
                return Err(invalid("initial progress length mismatch"));
            }
            Reply::Progress {
                state,
                records: u64_at(payload, n)?,
            }
        }
        1 if payload.len() == 48 => Reply::Cursor {
            selection: selection(&payload[..40])?,
            records: u64_at(payload, 40)?,
        },
        2 | 3 if payload.len() == 80 => {
            let origin = selection(&payload[..40])?;
            let current = payload[40..72].try_into().unwrap();
            let tail = LogIndex(u64_at(payload, 72)?);
            if bytes[70] == 2 {
                Reply::Published {
                    origin,
                    current,
                    tail,
                }
            } else {
                Reply::Initialized {
                    origin,
                    current,
                    tail,
                }
            }
        }
        4 => {
            let (state, n) = read_state(payload)?;
            if payload.len() != n {
                return Err(invalid("initial conflict length mismatch"));
            }
            Reply::Conflict(state)
        }
        5 if payload.is_empty() => Reply::Pinned,
        _ => return Err(invalid("malformed initial response")),
    };
    if encode_reply(request, policy, &reply)? != bytes {
        return Err(invalid("noncanonical initial response"));
    }
    validate_reply(expected, &reply, limits)?;
    Ok(reply)
}

// Keep the already owned request; decoding its possibly large batch again is
// unnecessary. This test-only client is deliberately not a production sender.
#[cfg(test)]
fn validate_reply(request: &Request, reply: &Reply, limits: VsrPrefixLimits) -> io::Result<()> {
    let valid = match (request, reply) {
        (Request::Status, Reply::Progress { state, records }) => {
            *records <= limits.max_records
                && (!matches!(state, ControlInitialState::Reserved) || *records == 0)
        }
        (Request::Status, Reply::Initialized { tail, .. }) => *tail >= LogIndex(1),
        (Request::Start(descriptor), Reply::Cursor { selection, records }) => {
            selection.generation() == 1
                && selection.history_id() == descriptor.id().map_err(rsf)?
                && *records <= descriptor.records
        }
        (
            Request::Resume {
                selection: expected,
                descriptor,
            },
            Reply::Cursor { selection, records },
        ) => selection == expected && *records <= descriptor.records,
        (
            Request::Replace {
                expected,
                descriptor,
            },
            Reply::Cursor { selection, records },
        ) => {
            expected.generation().checked_add(1) == Some(selection.generation())
                && selection.history_id() == descriptor.id().map_err(rsf)?
                && *records <= descriptor.records
        }
        (
            Request::Batch {
                selection: expected,
                batch,
            },
            Reply::Cursor { selection, records },
        ) => {
            selection == expected
                && *records <= limits.max_records
                && batch
                    .ordinal
                    .checked_add(batch.records.len() as u64)
                    .is_some_and(|end| *records >= end)
        }
        (
            Request::Finish(expected),
            Reply::Published {
                origin,
                current,
                tail,
            },
        ) => origin == expected && *current == origin.history_id() && *tail == LogIndex(1),
        (
            Request::Finish(expected),
            Reply::Initialized {
                origin,
                current,
                tail,
            },
        ) => origin == expected && *current != origin.history_id() && *tail >= LogIndex(1),
        (Request::Status, Reply::Conflict(_) | Reply::Pinned) => false,
        (_, Reply::Conflict(_) | Reply::Pinned) => true,
        _ => false,
    };
    if !valid {
        return Err(invalid(
            "initial reply operation or progress differs from requested context",
        ));
    }
    Ok(())
}
