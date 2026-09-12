use std::path::PathBuf;

use rsf_core::{ErrorCode, LogIndex};

use super::*;

#[test]
fn durable_rpc_recovers_completed_accepted_call_after_restart() {
    let path = test_path("completed_accepted_call");
    let envelope = envelope("trace-rpc-1", 1, RetryClass::Never);

    {
        let mut ledger =
            DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("open");
        let (id, decision) = ledger.submit(envelope.clone()).expect("submit");
        assert!(matches!(decision, RpcDecision::Accepted(_)));
        ledger
            .mark_completed(id, "sql response sent")
            .expect("complete");
    }

    let recovered =
        DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("recover");
    let call = recovered.call(RpcCallId(1)).expect("call");
    assert_eq!(call.envelope, envelope);
    assert_eq!(call.state, RpcCallState::Completed);
    assert_eq!(call.completion.as_deref(), Some("sql response sent"));
    assert!(recovered.in_flight().is_empty());
}

#[test]
fn durable_rpc_recovers_retryable_call_as_in_flight() {
    let path = test_path("retryable_in_flight");
    let envelope = envelope("trace-rpc-2", 2, RetryClass::Idempotent);

    {
        let mut ledger =
            DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("open");
        let (id, decision) = ledger.submit(envelope.clone()).expect("submit");
        assert_eq!(id, RpcCallId(1));
        assert!(matches!(decision, RpcDecision::Retryable(_)));
    }

    let recovered =
        DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("recover");
    let in_flight = recovered.in_flight();
    assert_eq!(in_flight.len(), 1);
    assert_eq!(in_flight[0].envelope, envelope);
    assert_eq!(in_flight[0].state, RpcCallState::Retryable);
}

#[test]
fn durable_rpc_recovers_rejected_call_with_original_envelope() {
    let path = test_path("rejected_call");
    let envelope = envelope("trace-rpc-3", 2, RetryClass::Never);

    {
        let mut ledger =
            DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("open");
        let (id, decision) = ledger.submit(envelope.clone()).expect("submit");
        assert_eq!(id, RpcCallId(1));
        assert!(matches!(decision, RpcDecision::Rejected { .. }));
    }

    let recovered =
        DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("recover");
    let call = recovered.call(RpcCallId(1)).expect("call");
    assert_eq!(call.envelope, envelope);
    assert_eq!(call.state, RpcCallState::Rejected);
    assert_eq!(
        call.rejection.as_deref(),
        Some("non-retryable operation retried")
    );
    assert!(recovered.in_flight().is_empty());
}

#[test]
fn durable_rpc_rejects_completion_for_rejected_call() {
    let path = test_path("rejects_completion_for_rejected");
    let envelope = envelope("trace-rpc-4", 2, RetryClass::Never);
    let mut ledger =
        DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database()).expect("open");
    let (id, _) = ledger.submit(envelope).expect("submit");
    let err = ledger
        .mark_completed(id, "should not complete")
        .expect_err("rejected call cannot complete");
    assert_eq!(err.code, ErrorCode::InvalidInput);
}

#[test]
fn rpc_wal_decode_rejects_trailing_bytes() {
    let mut payload = encode_rpc_wal_payload(&RpcWalRecord::Submitted {
        id: RpcCallId(1),
        envelope: envelope("trace-rpc-5", 1, RetryClass::Never),
        state: RpcCallState::Accepted,
        rejection: None,
    })
    .expect("encode");
    payload.push(99);

    let err = decode_rpc_wal_payload(&payload).expect_err("trailing bytes rejected");
    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

#[test]
fn durable_rpc_replay_rejects_completion_without_submission() {
    let path = test_path("completion_without_submission");
    append_raw(
        &path,
        &RpcWalRecord::Completed {
            id: RpcCallId(1),
            detail: "late response".to_string(),
        },
        1,
    );

    let err = DurableRpcLedger::open(&path, Epoch(9), RpcPolicy::strict_database())
        .expect_err("completion without submission fails closed");
    assert_eq!(err.code, ErrorCode::CorruptionDetected);
}

fn envelope(trace: &str, attempt: u8, retry_class: RetryClass) -> RpcEnvelope {
    RpcEnvelope::new(
        TraceId::new(trace).expect("trace"),
        ServiceRole::Gateway,
        ServiceRole::Sql,
        "sql.execute",
        100,
        retry_class,
        512,
    )
    .attempt(attempt)
}

fn append_raw(path: &PathBuf, record: &RpcWalRecord, index: u64) {
    let mut store = SegmentStore::open(path).expect("store");
    let payload = encode_rpc_wal_payload(record).expect("encode");
    let recovery = store.recover_current().expect("recover").recovery;
    let tail = recovery.pg_tail(PlacementGroupId(57));
    let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
    store
        .append_wal(WalAppend {
            pg: PlacementGroupId(57),
            epoch: Epoch(9),
            index: LogIndex(index),
            kind: WalRecordKind::Rpc,
            previous_hash,
            payload: &payload,
        })
        .expect("append raw rpc wal");
}

fn test_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("rsf-rpc-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}
