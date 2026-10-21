//! Incremental persistence for namespace-bound SQL. The checkpoint and journal
//! head independently bind acknowledged history before any prefix is reclaimed.
use super::*;
use rsf_core::{Epoch, LogIndex, storage::WalRecordKind};
use rsf_protocol::ReplicatedFabricCommand;
use rsf_storage::{SegmentStore, SegmentStoreOptions, WalAppend};

pub(super) const DIRECTORY: &str = "sql.journal";
pub(super) const HEAD_FILE: &str = "sql.journal.head";
const HEAD_TMP: &str = "sql.journal.head.tmp";
const HEAD_FORMAT: &str = "RSF_SQL_HEAD_V1";
const HEADER_FORMAT: &str = "RSF_SQL_JOURNAL_V1";
const PG: PlacementGroupId = PlacementGroupId(1);
const EPOCH: Epoch = Epoch(1);
const CHECKPOINT_COMMANDS: usize = 64;
const CHECKPOINT_BYTES: usize = 1024 * 1024;

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct JournalFence {
    index: LogIndex,
    hash: [u8; 32],
}
impl JournalFence {
    pub(super) fn line(self) -> String {
        format!(
            "journal\t{}\tblake3\t{}\n",
            self.index.0,
            hex_bytes(&self.hash)
        )
    }
    pub(super) fn decode(fields: &[&str]) -> RsfResult<Self> {
        let ["journal", index, "blake3", hash] = fields else {
            return Err(corrupt("invalid SQL journal fence"));
        };
        let index = index
            .parse::<u64>()
            .map_err(|_| corrupt("invalid SQL journal index"))?;
        if index == 0 {
            return Err(corrupt("SQL journal index must be positive"));
        }
        let hash = hex_decode(hash)?
            .try_into()
            .map_err(|_| corrupt("invalid SQL journal hash"))?;
        Ok(Self {
            index: LogIndex(index),
            hash,
        })
    }
    fn publish_head(self, root: &Path) -> RsfResult<()> {
        let mut text = format!("{HEAD_FORMAT}\n{}", self.line());
        append_checksum(&mut text);
        atomic_write(root, HEAD_FILE, HEAD_TMP, &text)
    }
    fn read_head(root: &Path) -> RsfResult<Self> {
        let text = fs::read_to_string(root.join(HEAD_FILE))
            .map_err(|err| corrupt(format!("cannot read SQL journal head: {err}")))?;
        let mut lines = verify_checksum(&text)?.lines();
        if lines.next() != Some(HEAD_FORMAT) {
            return Err(corrupt("unsupported SQL journal head"));
        }
        let fence = Self::decode(&lines.next().unwrap_or("").split('\t').collect::<Vec<_>>())?;
        if lines.next().is_some() {
            return Err(corrupt("extra SQL journal head fields"));
        }
        Ok(fence)
    }
}

/// Local persistence counters, not a bound on total SQL history or memory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlPersistenceStats {
    pub journal_index: u64,
    pub checkpoint_index: u64,
    pub commands_since_checkpoint: usize,
    pub bytes_since_checkpoint: usize,
}

#[derive(Debug)]
pub(super) struct SqlJournal {
    store: SegmentStore,
    tail: JournalFence,
    stats: SqlPersistenceStats,
}
impl SqlJournal {
    pub(super) fn tail(&self) -> JournalFence {
        self.tail
    }
    pub(super) fn append(
        &mut self,
        root: &Path,
        command: &ReplicatedFabricCommand,
    ) -> RsfResult<()> {
        let bytes = command.encode()?;
        let next = self
            .tail
            .index
            .0
            .checked_add(1)
            .ok_or_else(|| corrupt("SQL journal index exhausted"))?;
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: PG,
            epoch: EPOCH,
            index: LogIndex(next),
            kind: WalRecordKind::Commit,
            previous_hash: self.tail.hash,
            payload: &bytes,
        })?;
        let tail = JournalFence {
            index: record.index,
            hash: record.record_hash,
        };
        // The default SegmentStore syncs record data before this atomic head.
        // Never acknowledge a record while its independent head still lags.
        tail.publish_head(root)?;
        self.tail = tail;
        self.stats.journal_index = tail.index.0;
        self.stats.commands_since_checkpoint += 1;
        self.stats.bytes_since_checkpoint = self
            .stats
            .bytes_since_checkpoint
            .saturating_add(bytes.len());
        Ok(())
    }
    pub(super) fn checkpoint_due(&self) -> bool {
        self.stats.commands_since_checkpoint >= CHECKPOINT_COMMANDS
            || self.stats.bytes_since_checkpoint >= CHECKPOINT_BYTES
    }
    fn reclaim(&mut self) -> RsfResult<()> {
        // Membership at index one is the migration anchor, not a Commit.
        if self.tail.index.0 > 1 {
            self.store
                .compact(BTreeMap::from([(PG, self.tail.index)]))?;
        }
        self.stats.checkpoint_index = self.tail.index.0;
        self.stats.commands_since_checkpoint = 0;
        self.stats.bytes_since_checkpoint = 0;
        Ok(())
    }
}

impl DurableSqlEngine {
    pub fn persistence_stats(&self) -> Option<SqlPersistenceStats> {
        self.journal.as_ref().map(|journal| journal.stats)
    }

    /// Publish the complete materialized state before reclaiming its journal
    /// prefix. Receipts and historical values remain in the checkpoint.
    pub fn checkpoint(&mut self) -> RsfResult<()> {
        self.ensure_healthy()?;
        let result = (|| {
            self.persist()?;
            if let Some(journal) = &mut self.journal {
                journal.reclaim()?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.persistence_failed = true;
        }
        result
    }

    pub(super) fn recover_journal(&mut self, checkpoint: Option<JournalFence>) -> RsfResult<()> {
        let namespace = self
            .namespace
            .clone()
            .ok_or_else(|| corrupt("SQL journal requires namespace"))?;
        let path = self.root.join(DIRECTORY);
        if checkpoint.is_some()
            && (!path.join("MANIFEST").is_file() || !self.root.join(HEAD_FILE).is_file())
        {
            return Err(corrupt(
                "journaled SQL checkpoint is missing its journal or head",
            ));
        }
        let (mut store, recovered) =
            SegmentStore::open_recovered(&path, SegmentStoreOptions::default())?;
        sync_dir(&self.root)?;
        let recovery = recovered.recovery;
        if recovery.pg_tails.keys().any(|pg| *pg != PG)
            || recovery
                .compacted_prefixes
                .values()
                .any(|prefix| prefix.pg != PG || prefix.epoch != EPOCH)
            || recovery
                .wal_records
                .iter()
                .any(|record| record.pg != PG || record.epoch != EPOCH)
        {
            return Err(corrupt("SQL journal has foreign placement or epoch"));
        }
        let mut records = recovery.wal_records;
        let prefix = recovery
            .compacted_prefixes
            .get(&PG)
            .map(|prefix| JournalFence {
                index: prefix.index,
                hash: prefix.record_hash,
            });
        if prefix.is_some_and(|prefix| {
            prefix.index > checkpoint.map_or(LogIndex(0), |fence| fence.index)
        }) {
            return Err(corrupt(
                "SQL checkpoint does not cover reclaimed journal history",
            ));
        }
        let header = format!(
            "{HEADER_FORMAT}\nnamespace\t{}\nbase\tblake3\t{}\n",
            hex_bytes(namespace.as_bytes()),
            hex_bytes(
                Digest::blake3_bytes(
                    &fs::read(self.root.join(STATE_FILE))
                        .map_err(|err| corrupt(format!("cannot read SQL migration base: {err}")))?
                )
                .as_bytes()
            )
        );
        if records.is_empty() && prefix.is_none() && checkpoint.is_none() {
            if self.root.join(HEAD_FILE).exists() {
                return Err(corrupt(
                    "SQL journal lost its acknowledged migration header",
                ));
            }
            let (_, _, record) =
                store.append_next_wal(PG, EPOCH, WalRecordKind::Membership, header.as_bytes())?;
            let fence = JournalFence {
                index: record.index,
                hash: record.record_hash,
            };
            fence.publish_head(&self.root)?;
            records.push(record);
        }
        let tail = records
            .last()
            .map(|record| JournalFence {
                index: record.index,
                hash: record.record_hash,
            })
            .or(prefix)
            .ok_or_else(|| corrupt("SQL journal is empty"))?;
        let reached = |fence: JournalFence| {
            prefix == Some(fence)
                || records
                    .iter()
                    .any(|record| record.index == fence.index && record.record_hash == fence.hash)
        };
        if checkpoint.is_some_and(|fence| !reached(fence)) {
            return Err(corrupt("SQL checkpoint journal fence is absent or changed"));
        }
        // A header append may precede publication of its first head. No command
        // can be acknowledged until migration and the V5 checkpoint finish.
        let head = if self.root.join(HEAD_FILE).exists() {
            JournalFence::read_head(&self.root)?
        } else if checkpoint.is_none() && records.len() == 1 && records[0].index == LogIndex(1) {
            tail
        } else {
            return Err(corrupt(
                "SQL journal is missing acknowledged-history evidence",
            ));
        };
        if !reached(head) || checkpoint.is_some_and(|fence| fence.index > head.index) {
            return Err(corrupt(
                "SQL journal no longer reaches acknowledged history",
            ));
        }
        let mut stats = SqlPersistenceStats {
            journal_index: tail.index.0,
            checkpoint_index: checkpoint.map_or(1, |fence| fence.index.0),
            ..Default::default()
        };
        for record in &records {
            if record.index == LogIndex(1) {
                if record.kind != WalRecordKind::Membership {
                    return Err(corrupt("SQL journal has no migration header"));
                }
                let text = std::str::from_utf8(&record.payload)
                    .map_err(|_| corrupt("invalid SQL journal header"))?;
                let expected_namespace = format!(
                    "{HEADER_FORMAT}\nnamespace\t{}\nbase\tblake3\t",
                    hex_bytes(namespace.as_bytes())
                );
                if !text.starts_with(&expected_namespace)
                    || text.len() != expected_namespace.len() + 65
                    || !text.ends_with('\n')
                    || hex_decode(&text[expected_namespace.len()..text.len() - 1])?.len() != 32
                    || (checkpoint.is_none() && text != header)
                {
                    return Err(corrupt(
                        "SQL journal migration identity differs from checkpoint",
                    ));
                }
                continue;
            }
            if record.kind != WalRecordKind::Commit {
                return Err(corrupt("unexpected SQL journal record"));
            }
            let command = ReplicatedFabricCommand::decode(&record.payload)?;
            if record.index <= checkpoint.map_or(LogIndex(1), |fence| fence.index) {
                if self.commands.get(&command.request) != Some(&command) {
                    return Err(corrupt("SQL checkpoint lost a covered command"));
                }
            } else {
                self.replay_journal_command(&command)?;
                stats.commands_since_checkpoint += 1;
                stats.bytes_since_checkpoint = stats
                    .bytes_since_checkpoint
                    .saturating_add(record.payload.len());
            }
        }
        replicated::validate_checkpoint_commands(
            self.namespace.as_deref(),
            &self.commands,
            &self.receipts,
        )?;
        replicated::validate_recovered_operations(&self.engine, &self.commands)?;
        for receipt in self.receipts.values() {
            receipt.outcome(&self.engine)?;
        }
        session::validate_recovered(self)?;
        let (engine, report) = SqlEngine::recover_from_committed_kv(
            self.engine.kv.clone(),
            self.engine.recovered_tables_for_checkpoint(),
        )?;
        self.engine = engine;
        self.recovery = DurableSqlRecovery {
            tables: report.tables,
            row_versions: report.row_versions,
            live_rows: report.live_rows,
            tombstones: report.tombstones,
        };
        // Complete valid appends beyond the old head are uncertain operations.
        // Recover them before serving and advance the head before checkpointing.
        tail.publish_head(&self.root)?;
        self.journal = Some(SqlJournal { store, tail, stats });
        if checkpoint.is_none()
            || self
                .journal
                .as_ref()
                .is_some_and(SqlJournal::checkpoint_due)
        {
            self.checkpoint()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
