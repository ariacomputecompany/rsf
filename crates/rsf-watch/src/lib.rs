#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    Bytes, CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_kv::KvChange;
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const WATCH_PG: PlacementGroupId = PlacementGroupId(0);
const WATCH_WAL_MAGIC: &[u8; 8] = b"RSFWATCH";
const WATCH_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WatchId(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WatchCursor {
    pub after: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchSubscription {
    pub id: WatchId,
    pub client: String,
    pub prefix: Bytes,
    pub cursor: WatchCursor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WatchDelivery {
    pub watch: WatchId,
    pub version: CommitVersion,
    pub key: Bytes,
    pub value: Option<Bytes>,
}

#[derive(Clone, Debug)]
pub struct WatchRegistry {
    next_id: u64,
    subscriptions: BTreeMap<WatchId, WatchSubscription>,
}

#[derive(Debug)]
pub struct DurableWatchRegistry {
    registry: WatchRegistry,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WatchWalRecord {
    Register(WatchSubscription),
    Ack { id: WatchId, cursor: WatchCursor },
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self {
            next_id: 1,
            subscriptions: BTreeMap::new(),
        }
    }
}

impl WatchRegistry {
    pub fn register(
        &mut self,
        client: impl Into<String>,
        prefix: impl Into<Bytes>,
        cursor: WatchCursor,
    ) -> RsfResult<WatchSubscription> {
        let client = client.into();
        let prefix = prefix.into();
        if client.trim().is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "watch client must not be empty",
            ));
        }
        if prefix.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "watch prefix must not be empty",
            ));
        }

        let subscription = WatchSubscription {
            id: WatchId(self.next_id),
            client,
            prefix,
            cursor,
        };
        self.next_id += 1;
        self.subscriptions
            .insert(subscription.id, subscription.clone());
        Ok(subscription)
    }

    pub fn deliver(&self, id: WatchId, changes: &[KvChange]) -> RsfResult<Vec<WatchDelivery>> {
        let subscription = self.subscription(id)?;
        Ok(changes
            .iter()
            .filter(|change| change.version > subscription.cursor.after)
            .filter(|change| change.key.starts_with(&subscription.prefix))
            .map(|change| WatchDelivery {
                watch: subscription.id,
                version: change.version,
                key: change.key.clone(),
                value: change.value.clone(),
            })
            .collect())
    }

    pub fn ack(&mut self, id: WatchId, delivered: &[WatchDelivery]) -> RsfResult<WatchCursor> {
        let subscription = self.subscription_mut(id)?;
        if let Some(version) = delivered
            .iter()
            .filter(|delivery| delivery.watch == id)
            .map(|delivery| delivery.version)
            .max()
            .filter(|version| *version > subscription.cursor.after)
        {
            subscription.cursor.after = version;
        }
        Ok(subscription.cursor)
    }

    pub fn resume(&self, id: WatchId) -> RsfResult<WatchSubscription> {
        Ok(self.subscription(id)?.clone())
    }

    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    fn subscription(&self, id: WatchId) -> RsfResult<&WatchSubscription> {
        self.subscriptions.get(&id).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("watch subscription {} does not exist", id.0),
            )
        })
    }

    fn subscription_mut(&mut self, id: WatchId) -> RsfResult<&mut WatchSubscription> {
        self.subscriptions.get_mut(&id).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                format!("watch subscription {} does not exist", id.0),
            )
        })
    }

    fn ack_with_progress(
        &mut self,
        id: WatchId,
        delivered: &[WatchDelivery],
    ) -> RsfResult<(WatchCursor, bool)> {
        let subscription = self.subscription_mut(id)?;
        let previous = subscription.cursor;
        if let Some(version) = delivered
            .iter()
            .filter(|delivery| delivery.watch == id)
            .map(|delivery| delivery.version)
            .max()
            .filter(|version| *version > subscription.cursor.after)
        {
            subscription.cursor.after = version;
        }
        Ok((subscription.cursor, subscription.cursor != previous))
    }

    fn apply_recovered_register(&mut self, subscription: WatchSubscription) -> RsfResult<()> {
        if subscription.id.0 == 0 {
            return Err(corrupt("watch register record has zero subscription id"));
        }
        if self.subscriptions.contains_key(&subscription.id) {
            return Err(corrupt(format!(
                "watch register record duplicated subscription {}",
                subscription.id.0
            )));
        }
        self.next_id = self.next_id.max(subscription.id.0.saturating_add(1));
        self.subscriptions.insert(subscription.id, subscription);
        Ok(())
    }

    fn apply_recovered_ack(&mut self, id: WatchId, cursor: WatchCursor) -> RsfResult<()> {
        let subscription = self.subscription_mut(id)?;
        if cursor.after < subscription.cursor.after {
            return Err(corrupt(format!(
                "watch ack for subscription {} rewound cursor from {} to {}",
                id.0, subscription.cursor.after.0, cursor.after.0
            )));
        }
        subscription.cursor = cursor;
        Ok(())
    }
}

impl DurableWatchRegistry {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(WATCH_PG) })?;
        let mut registry = WatchRegistry::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Watch)
        {
            match decode_watch_wal_payload(&record.payload)? {
                WatchWalRecord::Register(subscription) => {
                    registry.apply_recovered_register(subscription)?;
                }
                WatchWalRecord::Ack { id, cursor } => {
                    registry.apply_recovered_ack(id, cursor)?;
                }
            }
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            registry,
            store,
            epoch,
            next_index,
        })
    }

    pub fn register(
        &mut self,
        client: impl Into<String>,
        prefix: impl Into<Bytes>,
        cursor: WatchCursor,
    ) -> RsfResult<WatchSubscription> {
        let mut registry = self.registry.clone();
        let subscription = registry.register(client, prefix, cursor)?;
        self.append_record(&WatchWalRecord::Register(subscription.clone()))?;
        self.registry = registry;
        Ok(subscription)
    }

    pub fn deliver(&self, id: WatchId, changes: &[KvChange]) -> RsfResult<Vec<WatchDelivery>> {
        self.registry.deliver(id, changes)
    }

    pub fn ack(&mut self, id: WatchId, delivered: &[WatchDelivery]) -> RsfResult<WatchCursor> {
        let mut registry = self.registry.clone();
        let (cursor, advanced) = registry.ack_with_progress(id, delivered)?;
        if advanced {
            self.append_record(&WatchWalRecord::Ack { id, cursor })?;
        }
        self.registry = registry;
        Ok(cursor)
    }

    pub fn resume(&self, id: WatchId) -> RsfResult<WatchSubscription> {
        self.registry.resume(id)
    }

    pub fn len(&self) -> usize {
        self.registry.len()
    }

    pub fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    fn append_record(&mut self, record: &WatchWalRecord) -> RsfResult<()> {
        let payload = encode_watch_wal_payload(record)?;
        let previous_hash = self
            .store
            .recover_current()?
            .recovery
            .pg_tail(WATCH_PG)
            .map_or([0; 32], |tail| tail.record_hash);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: WATCH_PG,
            epoch: self.epoch,
            index: LogIndex(self.next_index),
            kind: WalRecordKind::Watch,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

impl WatchCursor {
    pub fn new(after: CommitVersion) -> Self {
        Self { after }
    }
}

impl WatchDelivery {
    pub fn deleted(&self) -> bool {
        self.value.is_none()
    }
}

fn encode_watch_wal_payload(record: &WatchWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(WATCH_WAL_MAGIC);
    out.push(WATCH_WAL_VERSION);
    match record {
        WatchWalRecord::Register(subscription) => {
            out.push(1);
            out.extend_from_slice(&subscription.id.0.to_le_bytes());
            write_string(&subscription.client, &mut out)?;
            write_bytes(&subscription.prefix, &mut out)?;
            out.extend_from_slice(&subscription.cursor.after.0.to_le_bytes());
        }
        WatchWalRecord::Ack { id, cursor } => {
            out.push(2);
            out.extend_from_slice(&id.0.to_le_bytes());
            out.extend_from_slice(&cursor.after.0.to_le_bytes());
        }
    }
    Ok(out)
}

fn decode_watch_wal_payload(bytes: &[u8]) -> RsfResult<WatchWalRecord> {
    if !bytes.starts_with(WATCH_WAL_MAGIC) {
        return Err(corrupt("watch wal record has invalid magic"));
    }
    let mut offset = WATCH_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != WATCH_WAL_VERSION {
        return Err(corrupt("watch wal record has unsupported version"));
    }
    let tag = read_u8(bytes, &mut offset)?;
    let record = match tag {
        1 => {
            let id = WatchId(read_u64(bytes, &mut offset)?);
            let client = read_string(bytes, &mut offset)?;
            let prefix = read_bytes(bytes, &mut offset)?;
            let cursor = WatchCursor::new(CommitVersion(read_u64(bytes, &mut offset)?));
            if client.trim().is_empty() || prefix.is_empty() {
                return Err(corrupt("watch register record has invalid subscription"));
            }
            WatchWalRecord::Register(WatchSubscription {
                id,
                client,
                prefix,
                cursor,
            })
        }
        2 => WatchWalRecord::Ack {
            id: WatchId(read_u64(bytes, &mut offset)?),
            cursor: WatchCursor::new(CommitVersion(read_u64(bytes, &mut offset)?)),
        },
        _ => return Err(corrupt("watch wal record has unknown tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("watch wal record has trailing bytes"));
    }
    Ok(record)
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    write_bytes(value.as_bytes(), out)
}

fn write_bytes(bytes: &[u8], out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        RsfError::new(
            ErrorCode::InvalidInput,
            "watch wal byte field does not fit in record",
        )
    })?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("watch wal record ended early"))?;
    *offset += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let end = (*offset)
        .checked_add(4)
        .ok_or_else(|| corrupt("watch wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("watch wal record ended early"))?;
    *offset = end;
    Ok(u32::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let end = (*offset)
        .checked_add(8)
        .ok_or_else(|| corrupt("watch wal offset overflow"))?;
    let raw = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("watch wal record ended early"))?;
    *offset = end;
    Ok(u64::from_le_bytes(
        raw.try_into().expect("slice length checked"),
    ))
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> RsfResult<Bytes> {
    let len = read_u32(bytes, offset)? as usize;
    let end = (*offset)
        .checked_add(len)
        .ok_or_else(|| corrupt("watch wal byte field overflow"))?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| corrupt("watch wal record ended early"))?;
    *offset = end;
    Ok(value.to_vec())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    String::from_utf8(read_bytes(bytes, offset)?)
        .map_err(|_| corrupt("watch wal string is not utf-8"))
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::{Epoch, NodeId, PlacementGroupId};
    use rsf_kv::{DurableKvEngine, KvBatch, KvMutation};
    use rsf_protocol::VsrBackedFabricRouter;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn change(version: u64, key: &[u8], value: Option<&[u8]>) -> KvChange {
        KvChange {
            version: CommitVersion(version),
            key: key.to_vec(),
            value: value.map(|value| value.to_vec()),
        }
    }

    #[test]
    fn prefix_watch_delivers_matching_changes_and_advances_cursor() {
        let mut registry = WatchRegistry::default();
        let subscription = registry
            .register(
                "app-client",
                b"config/".to_vec(),
                WatchCursor::new(CommitVersion(1)),
            )
            .expect("register");

        let deliveries = registry
            .deliver(
                subscription.id,
                &[
                    change(1, b"config/old", Some(b"ignored")),
                    change(2, b"config/a", Some(b"one")),
                    change(3, b"user/a", Some(b"ignored")),
                    change(4, b"config/b", None),
                ],
            )
            .expect("deliver");

        assert_eq!(deliveries.len(), 2);
        assert_eq!(deliveries[0].key, b"config/a".to_vec());
        assert!(!deliveries[0].deleted());
        assert_eq!(deliveries[1].key, b"config/b".to_vec());
        assert!(deliveries[1].deleted());
        assert_eq!(
            registry
                .ack(subscription.id, &deliveries)
                .expect("ack")
                .after,
            CommitVersion(4)
        );
    }

    #[test]
    fn resume_returns_cursor_without_redelivering_acknowledged_changes() {
        let mut registry = WatchRegistry::default();
        let subscription = registry
            .register(
                "app-client",
                b"config/".to_vec(),
                WatchCursor::new(CommitVersion(0)),
            )
            .expect("register");
        let changes = [
            change(1, b"config/a", Some(b"one")),
            change(2, b"config/b", Some(b"two")),
        ];
        let deliveries = registry
            .deliver(subscription.id, &changes)
            .expect("deliver");
        registry.ack(subscription.id, &deliveries).expect("ack");

        let resumed = registry.resume(subscription.id).expect("resume");
        assert_eq!(resumed.cursor.after, CommitVersion(2));
        assert!(
            registry
                .deliver(subscription.id, &changes)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn register_rejects_empty_identity_or_prefix() {
        let mut registry = WatchRegistry::default();
        assert!(
            registry
                .register("", b"config/".to_vec(), WatchCursor::new(CommitVersion(0)))
                .is_err()
        );
        assert!(
            registry
                .register("app-client", Vec::new(), WatchCursor::new(CommitVersion(0)))
                .is_err()
        );
    }

    #[test]
    fn durable_watch_resumes_after_restart_over_durable_kv_changes() {
        let router_path = test_path("durable_watch_router");
        let kv_path = test_path("durable_watch_kv");
        let watch_path = test_path("durable_watch_registry");
        let voters = [NodeId(0), NodeId(1), NodeId(2)];
        let watch_id;

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("router");
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("kv");
            let mut watches = DurableWatchRegistry::open(&watch_path, Epoch(1)).expect("watch");
            let subscription = watches
                .register(
                    "app-client",
                    b"config/".to_vec(),
                    WatchCursor::new(CommitVersion(0)),
                )
                .expect("register");
            watch_id = subscription.id;

            commit_config(&mut kv, &mut router, b"config/a", b"one");
            commit_config(&mut kv, &mut router, b"config/b", b"two");
            let deliveries = watches
                .deliver(watch_id, &kv.changes_since(CommitVersion(0)))
                .expect("deliver");
            assert_eq!(deliveries.len(), 2);
            assert_eq!(
                watches.ack(watch_id, &deliveries).expect("ack").after,
                CommitVersion(2)
            );
        }

        {
            let mut router = VsrBackedFabricRouter::open(
                &router_path,
                [PlacementGroupId(1)],
                Epoch(1),
                NodeId(0),
                voters,
            )
            .expect("recover router");
            let mut kv = DurableKvEngine::open(&kv_path, Epoch(1)).expect("recover kv");
            let watches = DurableWatchRegistry::open(&watch_path, Epoch(1)).expect("recover watch");

            let resumed = watches.resume(watch_id).expect("resume");
            assert_eq!(resumed.cursor.after, CommitVersion(2));
            assert!(
                watches
                    .deliver(watch_id, &kv.changes_since(CommitVersion(0)))
                    .expect("redelivery check")
                    .is_empty()
            );

            commit_config(&mut kv, &mut router, b"config/c", b"three");
            let deliveries = watches
                .deliver(watch_id, &kv.changes_since(resumed.cursor.after))
                .expect("deliver after restart");
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].version, CommitVersion(3));
            assert_eq!(deliveries[0].key, b"config/c".to_vec());
            assert_eq!(deliveries[0].value, Some(b"three".to_vec()));
        }

        let _ = std::fs::remove_dir_all(router_path);
        let _ = std::fs::remove_dir_all(kv_path);
        let _ = std::fs::remove_dir_all(watch_path);
    }

    #[test]
    fn durable_watch_uses_first_class_wal_kind_and_skips_noop_acks() {
        let watch_path = test_path("durable_watch_wal_kind");
        {
            let mut watches = DurableWatchRegistry::open(&watch_path, Epoch(1)).expect("watch");
            let subscription = watches
                .register(
                    "app-client",
                    b"config/".to_vec(),
                    WatchCursor::new(CommitVersion(3)),
                )
                .expect("register");

            let cursor = watches
                .ack(subscription.id, &[])
                .expect("noop ack should not fail");
            assert_eq!(cursor.after, CommitVersion(3));
        }

        let store = SegmentStore::open(&watch_path).expect("store");
        let scan = store
            .scan(SegmentScanFilter { pg: Some(WATCH_PG) })
            .expect("scan");
        assert_eq!(scan.wal_records.len(), 1);
        assert_eq!(scan.wal_records[0].kind, WalRecordKind::Watch);

        let _ = std::fs::remove_dir_all(watch_path);
    }

    #[test]
    fn durable_watch_recovery_rejects_duplicate_subscription_ids() {
        let watch_path = test_path("durable_watch_duplicate");
        let subscription = WatchSubscription {
            id: WatchId(1),
            client: "app-client".to_string(),
            prefix: b"config/".to_vec(),
            cursor: WatchCursor::new(CommitVersion(0)),
        };
        append_raw_watch_record(
            &watch_path,
            WatchWalRecord::Register(subscription.clone()),
            Epoch(1),
        );
        append_raw_watch_record(
            &watch_path,
            WatchWalRecord::Register(subscription),
            Epoch(1),
        );

        let err = DurableWatchRegistry::open(&watch_path, Epoch(1))
            .expect_err("duplicate recovered watch id must reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("duplicated subscription"));

        let _ = std::fs::remove_dir_all(watch_path);
    }

    #[test]
    fn durable_watch_recovery_rejects_cursor_rewind() {
        let watch_path = test_path("durable_watch_rewind");
        append_raw_watch_record(
            &watch_path,
            WatchWalRecord::Register(WatchSubscription {
                id: WatchId(1),
                client: "app-client".to_string(),
                prefix: b"config/".to_vec(),
                cursor: WatchCursor::new(CommitVersion(5)),
            }),
            Epoch(1),
        );
        append_raw_watch_record(
            &watch_path,
            WatchWalRecord::Ack {
                id: WatchId(1),
                cursor: WatchCursor::new(CommitVersion(4)),
            },
            Epoch(1),
        );

        let err = DurableWatchRegistry::open(&watch_path, Epoch(1))
            .expect_err("rewound recovered watch cursor must reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("rewound cursor"));

        let _ = std::fs::remove_dir_all(watch_path);
    }

    #[test]
    fn durable_watch_recovery_rejects_malformed_payload() {
        let watch_path = test_path("durable_watch_malformed");
        let mut store = SegmentStore::open(&watch_path).expect("store");
        store
            .append_wal(WalAppend {
                pg: WATCH_PG,
                epoch: Epoch(1),
                index: LogIndex(1),
                kind: WalRecordKind::Watch,
                previous_hash: [0; 32],
                payload: b"not-watch-wal",
            })
            .expect("append corrupt watch payload");

        let err = DurableWatchRegistry::open(&watch_path, Epoch(1))
            .expect_err("malformed recovered watch payload must reject");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
        assert!(err.message.contains("invalid magic"));

        let _ = std::fs::remove_dir_all(watch_path);
    }

    fn commit_config(
        kv: &mut DurableKvEngine,
        router: &mut VsrBackedFabricRouter,
        key: &[u8],
        value: &[u8],
    ) {
        let snapshot = kv.snapshot(router);
        kv.commit_batch(
            router,
            PlacementGroupId(1),
            KvBatch {
                client_request: None,
                snapshot,
                mutations: vec![KvMutation::Set {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }],
            },
        )
        .expect("commit config");
    }

    fn append_raw_watch_record(path: &Path, record: WatchWalRecord, epoch: Epoch) {
        let mut store = SegmentStore::open(path).expect("store");
        let recovery = store.recover_current().expect("recover");
        let tail = recovery.recovery.pg_tail(WATCH_PG);
        let previous_hash = tail.map_or([0; 32], |tail| tail.record_hash);
        let index = tail.map_or(LogIndex(1), |tail| LogIndex(tail.index.0 + 1));
        let payload = encode_watch_wal_payload(&record).expect("encode watch wal record");
        store
            .append_wal(WalAppend {
                pg: WATCH_PG,
                epoch,
                index,
                kind: WalRecordKind::Watch,
                previous_hash,
                payload: &payload,
            })
            .expect("append watch record");
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("rsf-watch-{name}-{}-{nonce}", std::process::id()))
    }
}
