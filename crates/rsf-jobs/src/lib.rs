#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::path::Path;

use rsf_core::{
    CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const JOB_PG: PlacementGroupId = PlacementGroupId(44);
const JOB_WAL_MAGIC: &[u8; 7] = b"RSFJOB1";
const JOB_WAL_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobId(pub String);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum JobKind {
    SchemaBackfill,
    IndexBuild,
    StatsRefresh,
    SstableCompaction,
    BackupExport,
    RetentionSweep,
    RepairCopy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobSpec {
    pub id: JobId,
    pub kind: JobKind,
    pub namespace: String,
    pub target: String,
    pub depends_on: Vec<JobId>,
    pub max_attempts: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobLease {
    pub id: JobId,
    pub worker: String,
    pub attempt: u8,
    pub expires_at: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobState {
    Queued,
    Leased(JobLease),
    Completed { at: CommitVersion },
    Failed { attempts: u8, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JobDecision {
    Enqueued,
    Duplicate,
    Leased(JobLease),
    Blocked { missing: Vec<JobId> },
    Completed,
    Retrying { next_attempt: u8 },
    Failed,
}

#[derive(Clone, Debug)]
struct JobRecord {
    spec: JobSpec,
    state: JobState,
    attempts: u8,
}

#[derive(Clone, Debug, Default)]
pub struct JobQueue {
    jobs: BTreeMap<JobId, JobRecord>,
}

impl JobQueue {
    pub fn enqueue(&mut self, spec: JobSpec) -> RsfResult<JobDecision> {
        validate_spec(&spec)?;
        if self.jobs.contains_key(&spec.id) {
            return Ok(JobDecision::Duplicate);
        }
        self.jobs.insert(
            spec.id.clone(),
            JobRecord {
                spec,
                state: JobState::Queued,
                attempts: 0,
            },
        );
        Ok(JobDecision::Enqueued)
    }

    pub fn lease_ready(
        &mut self,
        id: &JobId,
        worker: impl Into<String>,
        now: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<JobDecision> {
        if ttl_versions == 0 {
            return Err(invalid("job lease ttl must be greater than zero"));
        }
        let missing = self.missing_dependencies(id)?;
        if !missing.is_empty() {
            return Ok(JobDecision::Blocked { missing });
        }
        let record = self
            .jobs
            .get_mut(id)
            .ok_or_else(|| invalid(format!("job {} does not exist", id.0)))?;
        match record.state {
            JobState::Queued => {
                record.attempts = record.attempts.saturating_add(1);
                let lease = JobLease {
                    id: id.clone(),
                    worker: worker.into(),
                    attempt: record.attempts,
                    expires_at: CommitVersion(now.0 + ttl_versions),
                };
                record.state = JobState::Leased(lease.clone());
                Ok(JobDecision::Leased(lease))
            }
            JobState::Leased(ref lease) if now > lease.expires_at => {
                record.attempts = record.attempts.saturating_add(1);
                let lease = JobLease {
                    id: id.clone(),
                    worker: worker.into(),
                    attempt: record.attempts,
                    expires_at: CommitVersion(now.0 + ttl_versions),
                };
                record.state = JobState::Leased(lease.clone());
                Ok(JobDecision::Leased(lease))
            }
            JobState::Leased(ref lease) => Ok(JobDecision::Leased(lease.clone())),
            JobState::Completed { .. } => Ok(JobDecision::Completed),
            JobState::Failed { .. } => Ok(JobDecision::Failed),
        }
    }

    pub fn complete(&mut self, lease: &JobLease, at: CommitVersion) -> RsfResult<JobDecision> {
        let record = self
            .jobs
            .get_mut(&lease.id)
            .ok_or_else(|| invalid(format!("job {} does not exist", lease.id.0)))?;
        match &record.state {
            JobState::Leased(active) if active == lease => {
                record.state = JobState::Completed { at };
                Ok(JobDecision::Completed)
            }
            JobState::Leased(_) => Err(invalid("job lease does not match active lease")),
            _ => Err(invalid("job is not currently leased")),
        }
    }

    pub fn fail(&mut self, lease: &JobLease, reason: impl Into<String>) -> RsfResult<JobDecision> {
        let record = self
            .jobs
            .get_mut(&lease.id)
            .ok_or_else(|| invalid(format!("job {} does not exist", lease.id.0)))?;
        match &record.state {
            JobState::Leased(active) if active == lease => {
                let reason = reason.into();
                if record.attempts < record.spec.max_attempts {
                    record.state = JobState::Queued;
                    return Ok(JobDecision::Retrying {
                        next_attempt: record.attempts + 1,
                    });
                }
                record.state = JobState::Failed {
                    attempts: record.attempts,
                    reason,
                };
                Ok(JobDecision::Failed)
            }
            JobState::Leased(_) => Err(invalid("job lease does not match active lease")),
            _ => Err(invalid("job is not currently leased")),
        }
    }

    pub fn state(&self, id: &JobId) -> Option<&JobState> {
        self.jobs.get(id).map(|record| &record.state)
    }

    pub fn job_count(&self) -> usize {
        self.jobs.len()
    }

    pub fn attempts(&self, id: &JobId) -> Option<u8> {
        self.jobs.get(id).map(|record| record.attempts)
    }

    fn missing_dependencies(&self, id: &JobId) -> RsfResult<Vec<JobId>> {
        let record = self
            .jobs
            .get(id)
            .ok_or_else(|| invalid(format!("job {} does not exist", id.0)))?;
        Ok(record
            .spec
            .depends_on
            .iter()
            .filter(|dependency| {
                !matches!(
                    self.jobs.get(*dependency).map(|record| &record.state),
                    Some(JobState::Completed { .. })
                )
            })
            .cloned()
            .collect())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum JobWalRecord {
    Enqueued(JobSpec),
    Leased(JobLease),
    Finished { lease: JobLease, at: CommitVersion },
    FailedLease { lease: JobLease, reason: String },
}

#[derive(Debug)]
pub struct DurableJobQueue {
    queue: JobQueue,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl DurableJobQueue {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter { pg: Some(JOB_PG) })?;
        let mut queue = JobQueue::default();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Job)
        {
            apply_job_record(&mut queue, decode_job_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            queue,
            store,
            epoch,
            next_index,
        })
    }

    pub fn enqueue(&mut self, spec: JobSpec) -> RsfResult<JobDecision> {
        let mut queue = self.queue.clone();
        let decision = queue.enqueue(spec.clone())?;
        if decision == JobDecision::Enqueued {
            self.append_record(&JobWalRecord::Enqueued(spec))?;
            self.queue = queue;
        }
        Ok(decision)
    }

    pub fn lease_ready(
        &mut self,
        id: &JobId,
        worker: impl Into<String>,
        now: CommitVersion,
        ttl_versions: u64,
    ) -> RsfResult<JobDecision> {
        let worker = worker.into();
        let mut queue = self.queue.clone();
        let before = queue.state(id).cloned();
        let decision = queue.lease_ready(id, worker, now, ttl_versions)?;
        if let JobDecision::Leased(lease) = &decision
            && before.as_ref() != Some(&JobState::Leased(lease.clone()))
        {
            self.append_record(&JobWalRecord::Leased(lease.clone()))?;
            self.queue = queue;
        }
        Ok(decision)
    }

    pub fn complete(&mut self, lease: &JobLease, at: CommitVersion) -> RsfResult<JobDecision> {
        let mut queue = self.queue.clone();
        let decision = queue.complete(lease, at)?;
        if decision == JobDecision::Completed {
            self.append_record(&JobWalRecord::Finished {
                lease: lease.clone(),
                at,
            })?;
            self.queue = queue;
        }
        Ok(decision)
    }

    pub fn fail(&mut self, lease: &JobLease, reason: impl Into<String>) -> RsfResult<JobDecision> {
        let reason = reason.into();
        let mut queue = self.queue.clone();
        let decision = queue.fail(lease, reason.clone())?;
        if matches!(decision, JobDecision::Retrying { .. } | JobDecision::Failed) {
            self.append_record(&JobWalRecord::FailedLease {
                lease: lease.clone(),
                reason,
            })?;
            self.queue = queue;
        }
        Ok(decision)
    }

    pub fn state(&self, id: &JobId) -> Option<&JobState> {
        self.queue.state(id)
    }

    pub fn attempts(&self, id: &JobId) -> Option<u8> {
        self.queue.attempts(id)
    }

    pub fn job_count(&self) -> usize {
        self.queue.job_count()
    }

    pub fn queue(&self) -> &JobQueue {
        &self.queue
    }

    fn append_record(&mut self, record: &JobWalRecord) -> RsfResult<()> {
        let payload = encode_job_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(JOB_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: JOB_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Job,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

fn apply_job_record(queue: &mut JobQueue, record: JobWalRecord) -> RsfResult<()> {
    match record {
        JobWalRecord::Enqueued(spec) => match queue.enqueue(spec)? {
            JobDecision::Enqueued => Ok(()),
            JobDecision::Duplicate => Err(corrupt("job WAL replay found duplicate enqueue")),
            other => Err(corrupt(format!(
                "job WAL enqueue replay produced unexpected decision {other:?}"
            ))),
        },
        JobWalRecord::Leased(lease) => {
            validate_lease(&lease)?;
            let now = lease
                .expires_at
                .0
                .checked_sub(1)
                .ok_or_else(|| corrupt("job WAL lease expiry cannot be zero"))?;
            match queue.lease_ready(&lease.id, lease.worker.clone(), CommitVersion(now), 1)? {
                JobDecision::Leased(replayed) if replayed == lease => Ok(()),
                JobDecision::Blocked { missing } => Err(corrupt(format!(
                    "job WAL lease replay found missing dependencies: {missing:?}"
                ))),
                other => Err(corrupt(format!(
                    "job WAL lease replay produced unexpected decision {other:?}"
                ))),
            }
        }
        JobWalRecord::Finished { lease, at } => {
            validate_lease(&lease)?;
            match queue.complete(&lease, at)? {
                JobDecision::Completed => Ok(()),
                other => Err(corrupt(format!(
                    "job WAL finish replay produced unexpected decision {other:?}"
                ))),
            }
        }
        JobWalRecord::FailedLease { lease, reason } => {
            validate_lease(&lease)?;
            match queue.fail(&lease, reason)? {
                JobDecision::Retrying { .. } | JobDecision::Failed => Ok(()),
                other => Err(corrupt(format!(
                    "job WAL fail replay produced unexpected decision {other:?}"
                ))),
            }
        }
    }
}

fn encode_job_wal_payload(record: &JobWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(JOB_WAL_MAGIC);
    out.push(JOB_WAL_VERSION);
    match record {
        JobWalRecord::Enqueued(spec) => {
            out.push(1);
            encode_spec(spec, &mut out)?;
        }
        JobWalRecord::Leased(lease) => {
            out.push(2);
            encode_lease(lease, &mut out)?;
        }
        JobWalRecord::Finished { lease, at } => {
            out.push(3);
            encode_lease(lease, &mut out)?;
            write_u64(at.0, &mut out);
        }
        JobWalRecord::FailedLease { lease, reason } => {
            out.push(4);
            encode_lease(lease, &mut out)?;
            write_string(reason, &mut out)?;
        }
    }
    Ok(out)
}

fn decode_job_wal_payload(bytes: &[u8]) -> RsfResult<JobWalRecord> {
    if bytes.len() < JOB_WAL_MAGIC.len() + 2 {
        return Err(corrupt("job WAL payload is too short"));
    }
    if &bytes[..JOB_WAL_MAGIC.len()] != JOB_WAL_MAGIC {
        return Err(corrupt("job WAL payload has invalid magic"));
    }
    let mut offset = JOB_WAL_MAGIC.len();
    let version = read_u8(bytes, &mut offset)?;
    if version != JOB_WAL_VERSION {
        return Err(corrupt(format!(
            "job WAL payload version {version} is not supported"
        )));
    }
    let kind = read_u8(bytes, &mut offset)?;
    let record = match kind {
        1 => JobWalRecord::Enqueued(decode_spec(bytes, &mut offset)?),
        2 => JobWalRecord::Leased(decode_lease(bytes, &mut offset)?),
        3 => JobWalRecord::Finished {
            lease: decode_lease(bytes, &mut offset)?,
            at: CommitVersion(read_u64(bytes, &mut offset)?),
        },
        4 => JobWalRecord::FailedLease {
            lease: decode_lease(bytes, &mut offset)?,
            reason: read_string(bytes, &mut offset)?,
        },
        other => {
            return Err(corrupt(format!(
                "job WAL payload kind {other} is not supported"
            )));
        }
    };
    if offset != bytes.len() {
        return Err(corrupt("job WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn encode_spec(spec: &JobSpec, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_spec(spec)?;
    write_string(&spec.id.0, out)?;
    out.push(job_kind_to_u8(spec.kind));
    write_string(&spec.namespace, out)?;
    write_string(&spec.target, out)?;
    let dep_len = u32::try_from(spec.depends_on.len())
        .map_err(|_| invalid("job dependency list is too large for WAL"))?;
    out.extend_from_slice(&dep_len.to_le_bytes());
    for dependency in &spec.depends_on {
        write_string(&dependency.0, out)?;
    }
    out.push(spec.max_attempts);
    Ok(())
}

fn decode_spec(bytes: &[u8], offset: &mut usize) -> RsfResult<JobSpec> {
    let id = JobId(read_string(bytes, offset)?);
    let kind = job_kind_from_u8(read_u8(bytes, offset)?)?;
    let namespace = read_string(bytes, offset)?;
    let target = read_string(bytes, offset)?;
    let dep_len = read_u32(bytes, offset)?;
    let dep_len =
        usize::try_from(dep_len).map_err(|_| corrupt("job dependency count is too large"))?;
    let mut depends_on = Vec::with_capacity(dep_len);
    for _ in 0..dep_len {
        depends_on.push(JobId(read_string(bytes, offset)?));
    }
    let spec = JobSpec {
        id,
        kind,
        namespace,
        target,
        depends_on,
        max_attempts: read_u8(bytes, offset)?,
    };
    validate_spec(&spec)?;
    Ok(spec)
}

fn encode_lease(lease: &JobLease, out: &mut Vec<u8>) -> RsfResult<()> {
    validate_lease(lease)?;
    write_string(&lease.id.0, out)?;
    write_string(&lease.worker, out)?;
    out.push(lease.attempt);
    write_u64(lease.expires_at.0, out);
    Ok(())
}

fn decode_lease(bytes: &[u8], offset: &mut usize) -> RsfResult<JobLease> {
    let lease = JobLease {
        id: JobId(read_string(bytes, offset)?),
        worker: read_string(bytes, offset)?,
        attempt: read_u8(bytes, offset)?,
        expires_at: CommitVersion(read_u64(bytes, offset)?),
    };
    validate_lease(&lease)?;
    Ok(lease)
}

fn validate_lease(lease: &JobLease) -> RsfResult<()> {
    validate_name("job", &lease.id.0)?;
    if lease.worker.trim().is_empty() {
        return Err(invalid("job lease worker must not be empty"));
    }
    if lease.attempt == 0 {
        return Err(corrupt("job WAL payload has zero lease attempt"));
    }
    Ok(())
}

fn job_kind_to_u8(kind: JobKind) -> u8 {
    match kind {
        JobKind::SchemaBackfill => 1,
        JobKind::IndexBuild => 2,
        JobKind::StatsRefresh => 3,
        JobKind::SstableCompaction => 4,
        JobKind::BackupExport => 5,
        JobKind::RetentionSweep => 6,
        JobKind::RepairCopy => 7,
    }
}

fn job_kind_from_u8(value: u8) -> RsfResult<JobKind> {
    match value {
        1 => Ok(JobKind::SchemaBackfill),
        2 => Ok(JobKind::IndexBuild),
        3 => Ok(JobKind::StatsRefresh),
        4 => Ok(JobKind::SstableCompaction),
        5 => Ok(JobKind::BackupExport),
        6 => Ok(JobKind::RetentionSweep),
        7 => Ok(JobKind::RepairCopy),
        _ => Err(corrupt(format!("unknown job kind id {value}"))),
    }
}

fn write_string(value: &str, out: &mut Vec<u8>) -> RsfResult<()> {
    let len = u32::try_from(value.len()).map_err(|_| invalid("job string is too large for WAL"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u32(bytes, offset)?;
    let len = usize::try_from(len).map_err(|_| corrupt("job string length is too large"))?;
    let raw = bytes
        .get(*offset..offset.saturating_add(len))
        .ok_or_else(|| corrupt("job WAL payload ended while reading string"))?;
    *offset = offset.saturating_add(len);
    String::from_utf8(raw.to_vec()).map_err(|_| corrupt("job WAL payload has invalid UTF-8"))
}

fn write_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    let value = *bytes
        .get(*offset)
        .ok_or_else(|| corrupt("job WAL payload ended while reading u8"))?;
    *offset = offset.saturating_add(1);
    Ok(value)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = bytes
        .get(*offset..offset.saturating_add(4))
        .ok_or_else(|| corrupt("job WAL payload ended while reading u32"))?;
    *offset = offset.saturating_add(4);
    Ok(u32::from_le_bytes(raw.try_into().expect("slice length")))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = bytes
        .get(*offset..offset.saturating_add(8))
        .ok_or_else(|| corrupt("job WAL payload ended while reading u64"))?;
    *offset = offset.saturating_add(8);
    Ok(u64::from_le_bytes(raw.try_into().expect("slice length")))
}

impl JobKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SchemaBackfill => "schema-backfill",
            Self::IndexBuild => "index-build",
            Self::StatsRefresh => "stats-refresh",
            Self::SstableCompaction => "sstable-compaction",
            Self::BackupExport => "backup-export",
            Self::RetentionSweep => "retention-sweep",
            Self::RepairCopy => "repair-copy",
        }
    }
}

fn validate_spec(spec: &JobSpec) -> RsfResult<()> {
    validate_name("job", &spec.id.0)?;
    validate_name("namespace", &spec.namespace)?;
    if spec.target.trim().is_empty() {
        return Err(invalid("job target must not be empty"));
    }
    if spec.max_attempts == 0 {
        return Err(invalid("job max attempts must be greater than zero"));
    }
    Ok(())
}

fn validate_name(kind: &str, value: &str) -> RsfResult<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(invalid(format!(
            "{kind} name must be non-empty ASCII alphanumeric, '_' or '-'"
        )));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_and_completes_ready_job() {
        let mut queue = JobQueue::default();
        let id = JobId("stats-users".to_string());
        queue
            .enqueue(spec(id.clone(), JobKind::StatsRefresh, []))
            .unwrap();
        let decision = queue
            .lease_ready(&id, "worker-a", CommitVersion(10), 5)
            .unwrap();
        let JobDecision::Leased(lease) = decision else {
            panic!("job should lease");
        };

        assert_eq!(lease.attempt, 1);
        assert_eq!(
            queue.complete(&lease, CommitVersion(11)).unwrap(),
            JobDecision::Completed
        );
        assert_eq!(
            queue.state(&id),
            Some(&JobState::Completed {
                at: CommitVersion(11)
            })
        );
    }

    #[test]
    fn dependency_blocks_until_completed() {
        let mut queue = JobQueue::default();
        let first = JobId("index-users-name".to_string());
        let second = JobId("stats-users".to_string());
        queue
            .enqueue(spec(first.clone(), JobKind::IndexBuild, []))
            .unwrap();
        queue
            .enqueue(spec(second.clone(), JobKind::StatsRefresh, [first.clone()]))
            .unwrap();

        assert_eq!(
            queue
                .lease_ready(&second, "worker-a", CommitVersion(1), 5)
                .unwrap(),
            JobDecision::Blocked {
                missing: vec![first.clone()]
            }
        );

        let JobDecision::Leased(lease) = queue
            .lease_ready(&first, "worker-a", CommitVersion(1), 5)
            .unwrap()
        else {
            panic!("first job should lease");
        };
        queue.complete(&lease, CommitVersion(2)).unwrap();
        assert!(matches!(
            queue
                .lease_ready(&second, "worker-b", CommitVersion(3), 5)
                .unwrap(),
            JobDecision::Leased(_)
        ));
    }

    #[test]
    fn failed_lease_requeues_until_attempt_budget_is_spent() {
        let mut queue = JobQueue::default();
        let id = JobId("repair-pg-2".to_string());
        queue
            .enqueue(spec(id.clone(), JobKind::RepairCopy, []))
            .unwrap();
        let JobDecision::Leased(first) = queue
            .lease_ready(&id, "worker-a", CommitVersion(1), 5)
            .unwrap()
        else {
            panic!("first lease");
        };
        assert_eq!(
            queue.fail(&first, "copy source busy").unwrap(),
            JobDecision::Retrying { next_attempt: 2 }
        );
        let JobDecision::Leased(second) = queue
            .lease_ready(&id, "worker-a", CommitVersion(2), 5)
            .unwrap()
        else {
            panic!("second lease");
        };
        assert_eq!(
            queue.fail(&second, "copy source gone").unwrap(),
            JobDecision::Failed
        );
    }

    #[test]
    fn durable_jobs_recover_completed_dependency_and_lease_ready_child() {
        let path = test_path("durable_jobs_recover_completed_dependency_and_lease_ready_child");
        let first = JobId("index-users-name".to_string());
        let second = JobId("stats-users".to_string());

        {
            let mut queue = DurableJobQueue::open(&path, Epoch(4)).expect("open jobs");
            queue
                .enqueue(spec(first.clone(), JobKind::IndexBuild, []))
                .expect("enqueue first");
            queue
                .enqueue(spec(second.clone(), JobKind::StatsRefresh, [first.clone()]))
                .expect("enqueue second");
            let JobDecision::Leased(lease) = queue
                .lease_ready(&first, "worker-a", CommitVersion(10), 5)
                .expect("lease first")
            else {
                panic!("first should lease");
            };
            queue.complete(&lease, CommitVersion(12)).expect("complete");
        }

        let mut recovered = DurableJobQueue::open(&path, Epoch(4)).expect("recover jobs");
        assert_eq!(
            recovered.state(&first),
            Some(&JobState::Completed {
                at: CommitVersion(12)
            })
        );
        assert!(matches!(
            recovered
                .lease_ready(&second, "worker-b", CommitVersion(13), 5)
                .expect("lease second"),
            JobDecision::Leased(_)
        ));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_jobs_recover_retry_attempts_and_terminal_failure() {
        let path = test_path("durable_jobs_recover_retry_attempts_and_terminal_failure");
        let id = JobId("repair-pg-2".to_string());

        {
            let mut queue = DurableJobQueue::open(&path, Epoch(5)).expect("open jobs");
            queue
                .enqueue(spec(id.clone(), JobKind::RepairCopy, []))
                .expect("enqueue");
            let JobDecision::Leased(first) = queue
                .lease_ready(&id, "worker-a", CommitVersion(1), 5)
                .expect("first lease")
            else {
                panic!("first lease");
            };
            assert_eq!(
                queue.fail(&first, "copy source busy").expect("fail"),
                JobDecision::Retrying { next_attempt: 2 }
            );
        }

        let mut recovered = DurableJobQueue::open(&path, Epoch(5)).expect("recover jobs");
        assert_eq!(recovered.attempts(&id), Some(1));
        assert_eq!(recovered.state(&id), Some(&JobState::Queued));

        let JobDecision::Leased(second) = recovered
            .lease_ready(&id, "worker-b", CommitVersion(8), 5)
            .expect("second lease")
        else {
            panic!("second lease");
        };
        assert_eq!(second.attempt, 2);
        assert_eq!(
            recovered
                .fail(&second, "copy source gone")
                .expect("terminal fail"),
            JobDecision::Failed
        );

        let recovered = DurableJobQueue::open(&path, Epoch(5)).expect("recover terminal");
        assert_eq!(
            recovered.state(&id),
            Some(&JobState::Failed {
                attempts: 2,
                reason: "copy source gone".to_string()
            })
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn durable_jobs_recover_existing_lease_without_duplicate_attempt() {
        let path = test_path("durable_jobs_recover_existing_lease_without_duplicate_attempt");
        let id = JobId("stats-users".to_string());
        let lease;

        {
            let mut queue = DurableJobQueue::open(&path, Epoch(6)).expect("open jobs");
            queue
                .enqueue(spec(id.clone(), JobKind::StatsRefresh, []))
                .expect("enqueue");
            let JobDecision::Leased(active) = queue
                .lease_ready(&id, "worker-a", CommitVersion(20), 5)
                .expect("lease")
            else {
                panic!("lease");
            };
            lease = active;
        }

        let mut recovered = DurableJobQueue::open(&path, Epoch(6)).expect("recover jobs");
        assert_eq!(recovered.attempts(&id), Some(1));
        assert_eq!(
            recovered
                .lease_ready(&id, "worker-b", CommitVersion(21), 5)
                .expect("existing lease"),
            JobDecision::Leased(lease)
        );
        assert_eq!(recovered.attempts(&id), Some(1));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn job_wal_decode_rejects_trailing_bytes() {
        let mut payload = encode_job_wal_payload(&JobWalRecord::Enqueued(spec(
            JobId("stats-users".to_string()),
            JobKind::StatsRefresh,
            [],
        )))
        .expect("encode");
        payload.push(99);

        let err = decode_job_wal_payload(&payload).expect_err("trailing bytes reject");

        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn spec(id: JobId, kind: JobKind, depends_on: impl IntoIterator<Item = JobId>) -> JobSpec {
        JobSpec {
            id,
            kind,
            namespace: "app".to_string(),
            target: "users".to_string(),
            depends_on: depends_on.into_iter().collect(),
            max_attempts: 2,
        }
    }

    fn test_path(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rsf-jobs-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        path
    }
}
