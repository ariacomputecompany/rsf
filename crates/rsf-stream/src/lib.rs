#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use rsf_core::{
    Bytes, CommitVersion, Epoch, ErrorCode, LogIndex, PlacementGroupId, RsfError, RsfResult,
    storage::WalRecordKind,
};
use rsf_storage::{SegmentScanFilter, SegmentStore, WalAppend};

const STREAM_PG: PlacementGroupId = PlacementGroupId(63);
const STREAM_WAL_MAGIC: &[u8; 7] = b"RSFSTR1";
const STREAM_WAL_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StreamSurface {
    SqlPage,
    KvRange,
    Object,
    Blob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamTarget {
    pub surface: StreamSurface,
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamPolicy {
    pub max_chunk_bytes: u64,
    pub max_inflight_bytes: u64,
    pub read_lanes: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamChunk {
    pub target: StreamTarget,
    pub offset: u64,
    pub len: u64,
    pub lane: u8,
    pub expected_hash: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamChunkData {
    pub chunk: StreamChunk,
    pub bytes: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteStream {
    pub target: StreamTarget,
    pub total_bytes: u64,
    pub chunks: Vec<StreamChunk>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadStream {
    pub target: StreamTarget,
    pub min_version: CommitVersion,
    pub lanes: u8,
    pub chunks: Vec<StreamChunk>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowWindow {
    capacity_bytes: u64,
    in_flight_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FlowDecision {
    Reserved { bytes: u64, remaining_bytes: u64 },
    Rejected { bytes: u64, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationReport {
    pub verified_chunks: usize,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamPlanner {
    policy: StreamPolicy,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StreamTransferId(String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamTransferState {
    Pending,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamTransfer {
    pub id: StreamTransferId,
    pub write: WriteStream,
    pub completed_chunks: BTreeSet<u64>,
    pub state: StreamTransferState,
}

#[derive(Debug)]
pub struct DurableStreamLedger {
    transfers: BTreeMap<StreamTransferId, StreamTransfer>,
    store: SegmentStore,
    epoch: Epoch,
    next_index: u64,
}

impl StreamPlanner {
    pub fn new(policy: StreamPolicy) -> RsfResult<Self> {
        validate_policy(&policy)?;
        Ok(Self { policy })
    }

    pub fn plan_write(&self, target: StreamTarget, total_bytes: u64) -> RsfResult<WriteStream> {
        validate_target(&target)?;
        if total_bytes == 0 {
            return Err(invalid("stream write must contain bytes"));
        }

        Ok(WriteStream {
            chunks: self.layout_chunks(&target, total_bytes, |_| [0_u8; 32])?,
            target,
            total_bytes,
        })
    }

    pub fn plan_write_bytes(&self, target: StreamTarget, bytes: &[u8]) -> RsfResult<WriteStream> {
        validate_target(&target)?;
        if bytes.is_empty() {
            return Err(invalid("stream write must contain bytes"));
        }

        let total_bytes = u64::try_from(bytes.len())
            .map_err(|_| invalid("stream write length does not fit in u64"))?;
        let chunks =
            self.layout_chunks(&target, total_bytes, |range| blake3_hash(&bytes[range]))?;
        Ok(WriteStream {
            target,
            total_bytes,
            chunks,
        })
    }

    pub fn chunk_bytes(
        &self,
        stream: &WriteStream,
        bytes: &[u8],
    ) -> RsfResult<Vec<StreamChunkData>> {
        let total_bytes = u64::try_from(bytes.len())
            .map_err(|_| invalid("stream write length does not fit in u64"))?;
        if total_bytes != stream.total_bytes {
            return Err(invalid(format!(
                "stream payload length {} does not match planned length {}",
                total_bytes, stream.total_bytes
            )));
        }

        let mut chunks = Vec::with_capacity(stream.chunks.len());
        for chunk in &stream.chunks {
            let start = usize::try_from(chunk.offset)
                .map_err(|_| invalid("stream chunk offset does not fit in usize"))?;
            let len = usize::try_from(chunk.len)
                .map_err(|_| invalid("stream chunk length does not fit in usize"))?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| invalid("stream chunk range overflows usize"))?;
            let Some(part) = bytes.get(start..end) else {
                return Err(invalid("stream chunk range exceeds payload length"));
            };
            if blake3_hash(part) != chunk.expected_hash {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "stream chunk at offset {} does not match planned hash",
                        chunk.offset
                    ),
                ));
            }
            chunks.push(StreamChunkData {
                chunk: chunk.clone(),
                bytes: part.to_vec(),
            });
        }
        Ok(chunks)
    }

    fn layout_chunks(
        &self,
        target: &StreamTarget,
        total_bytes: u64,
        mut hash_range: impl FnMut(std::ops::Range<usize>) -> [u8; 32],
    ) -> RsfResult<Vec<StreamChunk>> {
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < total_bytes {
            let len = self.policy.max_chunk_bytes.min(total_bytes - offset);
            let lane = (chunks.len() % usize::from(self.policy.read_lanes)) as u8;
            let start = usize::try_from(offset)
                .map_err(|_| invalid("stream chunk offset does not fit in usize"))?;
            let chunk_len = usize::try_from(len)
                .map_err(|_| invalid("stream chunk length does not fit in usize"))?;
            let end = start
                .checked_add(chunk_len)
                .ok_or_else(|| invalid("stream chunk range overflows usize"))?;
            chunks.push(StreamChunk {
                target: target.clone(),
                offset,
                len,
                lane,
                expected_hash: hash_range(start..end),
            });
            offset += len;
        }
        Ok(chunks)
    }

    pub fn plan_read(
        &self,
        target: StreamTarget,
        min_version: CommitVersion,
        chunks: Vec<StreamChunk>,
    ) -> RsfResult<ReadStream> {
        validate_target(&target)?;
        validate_chunks(&target, &chunks)?;
        Ok(ReadStream {
            target,
            min_version,
            lanes: self.policy.read_lanes,
            chunks,
        })
    }

    pub fn verify(&self, stream: &ReadStream) -> RsfResult<VerificationReport> {
        if stream
            .chunks
            .iter()
            .any(|chunk| chunk.expected_hash == [0_u8; 32])
        {
            return Err(invalid(
                "stream verification requires content hashes from a byte-backed write plan",
            ));
        }

        Ok(VerificationReport {
            verified_chunks: stream.chunks.len(),
            total_bytes: stream.chunks.iter().map(|chunk| chunk.len).sum(),
        })
    }

    pub fn verify_bytes(
        &self,
        stream: &ReadStream,
        chunks: &[StreamChunkData],
    ) -> RsfResult<VerificationReport> {
        if chunks.len() != stream.chunks.len() {
            return Err(invalid("stream verification chunk count mismatch"));
        }
        let mut total_bytes = 0;
        for (expected, actual) in stream.chunks.iter().zip(chunks) {
            if &actual.chunk != expected {
                return Err(invalid("stream verification chunk metadata mismatch"));
            }
            if u64::try_from(actual.bytes.len()).ok() != Some(expected.len) {
                return Err(invalid("stream verification chunk byte length mismatch"));
            }
            if blake3_hash(&actual.bytes) != expected.expected_hash {
                return Err(RsfError::new(
                    ErrorCode::CorruptionDetected,
                    format!(
                        "stream chunk at offset {} failed hash verification",
                        expected.offset
                    ),
                ));
            }
            total_bytes += expected.len;
        }

        Ok(VerificationReport {
            verified_chunks: chunks.len(),
            total_bytes,
        })
    }
}

impl FlowWindow {
    pub fn new(capacity_bytes: u64) -> RsfResult<Self> {
        if capacity_bytes == 0 {
            return Err(invalid("flow window capacity must be greater than zero"));
        }
        Ok(Self {
            capacity_bytes,
            in_flight_bytes: 0,
        })
    }

    pub fn reserve(&mut self, bytes: u64) -> FlowDecision {
        if bytes == 0 {
            return FlowDecision::Rejected {
                bytes,
                reason: "reservation must be greater than zero".to_string(),
            };
        }
        if self.in_flight_bytes + bytes > self.capacity_bytes {
            return FlowDecision::Rejected {
                bytes,
                reason: "flow-control window exhausted".to_string(),
            };
        }

        self.in_flight_bytes += bytes;
        FlowDecision::Reserved {
            bytes,
            remaining_bytes: self.capacity_bytes - self.in_flight_bytes,
        }
    }

    pub fn release(&mut self, bytes: u64) -> RsfResult<()> {
        if bytes > self.in_flight_bytes {
            return Err(invalid("release exceeds in-flight bytes"));
        }
        self.in_flight_bytes -= bytes;
        Ok(())
    }

    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight_bytes
    }
}

impl DurableStreamLedger {
    pub fn open(path: impl AsRef<Path>, epoch: Epoch) -> RsfResult<Self> {
        let store = SegmentStore::open(path)?;
        let scan = store.scan(SegmentScanFilter {
            pg: Some(STREAM_PG),
        })?;
        let mut transfers = BTreeMap::new();
        let mut next_index = 1;

        for record in scan
            .wal_records
            .iter()
            .filter(|record| record.kind == WalRecordKind::Stream)
        {
            apply_stream_record(&mut transfers, decode_stream_wal_payload(&record.payload)?)?;
            next_index = next_index.max(record.index.0.saturating_add(1));
        }

        Ok(Self {
            transfers,
            store,
            epoch,
            next_index,
        })
    }

    pub fn plan_write(
        &mut self,
        id: StreamTransferId,
        planner: &StreamPlanner,
        target: StreamTarget,
        bytes: &[u8],
    ) -> RsfResult<WriteStream> {
        let write = planner.plan_write_bytes(target, bytes)?;
        let record = StreamWalRecord::Planned {
            id: id.clone(),
            write: write.clone(),
        };
        apply_stream_record(&mut self.transfers, record.clone())?;
        self.append_record(&record)?;
        Ok(write)
    }

    pub fn record_chunk(&mut self, id: &StreamTransferId, data: StreamChunkData) -> RsfResult<()> {
        validate_completed_chunk(&self.transfers, id, &data)?;
        let record = StreamWalRecord::ChunkWritten {
            id: id.clone(),
            chunk: data.chunk,
            bytes: u64::try_from(data.bytes.len())
                .map_err(|_| invalid("stream chunk byte length does not fit in u64"))?,
        };
        apply_stream_record(&mut self.transfers, record.clone())?;
        self.append_record(&record)
    }

    pub fn complete(&mut self, id: &StreamTransferId) -> RsfResult<()> {
        let record = StreamWalRecord::Completed { id: id.clone() };
        apply_stream_record(&mut self.transfers, record.clone())?;
        self.append_record(&record)
    }

    pub fn transfer(&self, id: &StreamTransferId) -> Option<&StreamTransfer> {
        self.transfers.get(id)
    }

    pub fn pending_count(&self) -> usize {
        self.transfers
            .values()
            .filter(|transfer| transfer.state == StreamTransferState::Pending)
            .count()
    }

    pub fn completed_count(&self) -> usize {
        self.transfers
            .values()
            .filter(|transfer| transfer.state == StreamTransferState::Complete)
            .count()
    }

    fn append_record(&mut self, record: &StreamWalRecord) -> RsfResult<()> {
        let payload = encode_stream_wal_payload(record)?;
        let recovery = self.store.recover_current()?.recovery;
        let tail = recovery.pg_tail(STREAM_PG);
        let previous_hash = tail.map_or([0_u8; 32], |tail| tail.record_hash);
        let index = LogIndex(self.next_index);
        let (_, _, record) = self.store.append_wal(WalAppend {
            pg: STREAM_PG,
            epoch: self.epoch,
            index,
            kind: WalRecordKind::Stream,
            previous_hash,
            payload: &payload,
        })?;
        self.next_index = record.index.0.saturating_add(1);
        Ok(())
    }
}

impl StreamTransferId {
    pub fn new(value: impl Into<String>) -> RsfResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(invalid("stream transfer id must not be empty"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl StreamTarget {
    pub fn new(
        surface: StreamSurface,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            surface,
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl StreamSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SqlPage => "sql-page",
            Self::KvRange => "kv-range",
            Self::Object => "object",
            Self::Blob => "blob",
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::SqlPage => 1,
            Self::KvRange => 2,
            Self::Object => 3,
            Self::Blob => 4,
        }
    }

    fn from_tag(tag: u8) -> RsfResult<Self> {
        match tag {
            1 => Ok(Self::SqlPage),
            2 => Ok(Self::KvRange),
            3 => Ok(Self::Object),
            4 => Ok(Self::Blob),
            _ => Err(corrupt("unknown stream surface tag")),
        }
    }
}

impl FlowDecision {
    pub fn is_reserved(&self) -> bool {
        matches!(self, Self::Reserved { .. })
    }
}

fn validate_policy(policy: &StreamPolicy) -> RsfResult<()> {
    if policy.max_chunk_bytes == 0 {
        return Err(invalid("max chunk bytes must be greater than zero"));
    }
    if policy.max_inflight_bytes == 0 {
        return Err(invalid("max in-flight bytes must be greater than zero"));
    }
    if policy.read_lanes == 0 {
        return Err(invalid("read lanes must be greater than zero"));
    }
    Ok(())
}

fn validate_target(target: &StreamTarget) -> RsfResult<()> {
    if target.namespace.trim().is_empty() {
        return Err(invalid("stream namespace must not be empty"));
    }
    if target.name.trim().is_empty() {
        return Err(invalid("stream target name must not be empty"));
    }
    Ok(())
}

fn validate_chunks(target: &StreamTarget, chunks: &[StreamChunk]) -> RsfResult<()> {
    if chunks.is_empty() {
        return Err(invalid("stream read must include chunks"));
    }

    let mut expected_offset = 0;
    for chunk in chunks {
        if &chunk.target != target {
            return Err(invalid("stream chunk target mismatch"));
        }
        if chunk.len == 0 {
            return Err(invalid("stream chunk length must be greater than zero"));
        }
        if chunk.offset != expected_offset {
            return Err(invalid("stream chunks must be contiguous"));
        }
        if chunk.expected_hash == [0_u8; 32] {
            return Err(invalid("stream chunk must carry a content hash"));
        }
        expected_offset += chunk.len;
    }
    Ok(())
}

fn blake3_hash(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

fn corrupt(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StreamWalRecord {
    Planned {
        id: StreamTransferId,
        write: WriteStream,
    },
    ChunkWritten {
        id: StreamTransferId,
        chunk: StreamChunk,
        bytes: u64,
    },
    Completed {
        id: StreamTransferId,
    },
}

fn apply_stream_record(
    transfers: &mut BTreeMap<StreamTransferId, StreamTransfer>,
    record: StreamWalRecord,
) -> RsfResult<()> {
    match record {
        StreamWalRecord::Planned { id, write } => {
            validate_target(&write.target)?;
            validate_chunks(&write.target, &write.chunks)?;
            if write.total_bytes != write.chunks.iter().map(|chunk| chunk.len).sum::<u64>() {
                return Err(corrupt("stream WAL plan byte count does not match chunks"));
            }
            if transfers.contains_key(&id) {
                return Err(corrupt("stream WAL planned duplicate transfer id"));
            }
            transfers.insert(
                id.clone(),
                StreamTransfer {
                    id,
                    write,
                    completed_chunks: BTreeSet::new(),
                    state: StreamTransferState::Pending,
                },
            );
            Ok(())
        }
        StreamWalRecord::ChunkWritten { id, chunk, bytes } => {
            let transfer = transfers
                .get_mut(&id)
                .ok_or_else(|| corrupt("stream WAL chunk references unknown transfer"))?;
            if transfer.state != StreamTransferState::Pending {
                return Err(corrupt("stream WAL chunk references completed transfer"));
            }
            let planned = transfer
                .write
                .chunks
                .iter()
                .find(|planned| planned.offset == chunk.offset)
                .ok_or_else(|| corrupt("stream WAL chunk offset is not in plan"))?;
            if planned != &chunk {
                return Err(corrupt("stream WAL chunk metadata does not match plan"));
            }
            if bytes != chunk.len {
                return Err(corrupt("stream WAL chunk byte count does not match plan"));
            }
            if !transfer.completed_chunks.insert(chunk.offset) {
                return Err(corrupt("stream WAL chunk completed twice"));
            }
            Ok(())
        }
        StreamWalRecord::Completed { id } => {
            let transfer = transfers
                .get_mut(&id)
                .ok_or_else(|| corrupt("stream WAL completion references unknown transfer"))?;
            if transfer.state != StreamTransferState::Pending {
                return Err(corrupt("stream WAL transfer completed twice"));
            }
            if transfer.completed_chunks.len() != transfer.write.chunks.len() {
                return Err(corrupt("stream WAL completion before every chunk finished"));
            }
            transfer.state = StreamTransferState::Complete;
            Ok(())
        }
    }
}

fn validate_completed_chunk(
    transfers: &BTreeMap<StreamTransferId, StreamTransfer>,
    id: &StreamTransferId,
    data: &StreamChunkData,
) -> RsfResult<()> {
    let transfer = transfers
        .get(id)
        .ok_or_else(|| invalid("stream transfer id is unknown"))?;
    if transfer.state != StreamTransferState::Pending {
        return Err(invalid("stream transfer is already complete"));
    }
    let Some(planned) = transfer
        .write
        .chunks
        .iter()
        .find(|planned| planned.offset == data.chunk.offset)
    else {
        return Err(invalid("stream chunk offset is not in transfer plan"));
    };
    if planned != &data.chunk {
        return Err(invalid(
            "stream chunk metadata does not match transfer plan",
        ));
    }
    if transfer.completed_chunks.contains(&data.chunk.offset) {
        return Err(invalid("stream chunk has already been recorded"));
    }
    if u64::try_from(data.bytes.len()).ok() != Some(data.chunk.len) {
        return Err(invalid(
            "stream chunk byte length does not match transfer plan",
        ));
    }
    if blake3_hash(&data.bytes) != data.chunk.expected_hash {
        return Err(corrupt("stream chunk bytes do not match planned hash"));
    }
    Ok(())
}

fn encode_stream_wal_payload(record: &StreamWalRecord) -> RsfResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(STREAM_WAL_MAGIC);
    out.push(STREAM_WAL_VERSION);
    match record {
        StreamWalRecord::Planned { id, write } => {
            out.push(1);
            write_id(&mut out, id)?;
            write_stream(&mut out, write)?;
        }
        StreamWalRecord::ChunkWritten { id, chunk, bytes } => {
            out.push(2);
            write_id(&mut out, id)?;
            write_chunk(&mut out, chunk)?;
            write_u64(&mut out, *bytes);
        }
        StreamWalRecord::Completed { id } => {
            out.push(3);
            write_id(&mut out, id)?;
        }
    }
    Ok(out)
}

fn decode_stream_wal_payload(bytes: &[u8]) -> RsfResult<StreamWalRecord> {
    let mut offset = 0;
    take_magic(bytes, &mut offset)?;
    let version = read_u8(bytes, &mut offset)?;
    if version != STREAM_WAL_VERSION {
        return Err(corrupt("unsupported stream WAL payload version"));
    }
    let record = match read_u8(bytes, &mut offset)? {
        1 => StreamWalRecord::Planned {
            id: read_id(bytes, &mut offset)?,
            write: read_stream(bytes, &mut offset)?,
        },
        2 => StreamWalRecord::ChunkWritten {
            id: read_id(bytes, &mut offset)?,
            chunk: read_chunk(bytes, &mut offset)?,
            bytes: read_u64(bytes, &mut offset)?,
        },
        3 => StreamWalRecord::Completed {
            id: read_id(bytes, &mut offset)?,
        },
        _ => return Err(corrupt("unknown stream WAL record tag")),
    };
    if offset != bytes.len() {
        return Err(corrupt("stream WAL payload has trailing bytes"));
    }
    Ok(record)
}

fn write_id(out: &mut Vec<u8>, id: &StreamTransferId) -> RsfResult<()> {
    write_string(out, id.as_str())
}

fn read_id(bytes: &[u8], offset: &mut usize) -> RsfResult<StreamTransferId> {
    StreamTransferId::new(read_string(bytes, offset)?)
}

fn write_stream(out: &mut Vec<u8>, stream: &WriteStream) -> RsfResult<()> {
    write_target(out, &stream.target)?;
    write_u64(out, stream.total_bytes);
    let count = u32::try_from(stream.chunks.len())
        .map_err(|_| invalid("stream WAL chunk count does not fit in u32"))?;
    write_u32(out, count);
    for chunk in &stream.chunks {
        write_chunk(out, chunk)?;
    }
    Ok(())
}

fn read_stream(bytes: &[u8], offset: &mut usize) -> RsfResult<WriteStream> {
    let target = read_target(bytes, offset)?;
    let total_bytes = read_u64(bytes, offset)?;
    let count = read_u32(bytes, offset)? as usize;
    let mut chunks = Vec::with_capacity(count);
    for _ in 0..count {
        chunks.push(read_chunk(bytes, offset)?);
    }
    Ok(WriteStream {
        target,
        total_bytes,
        chunks,
    })
}

fn write_target(out: &mut Vec<u8>, target: &StreamTarget) -> RsfResult<()> {
    out.push(target.surface.tag());
    write_string(out, &target.namespace)?;
    write_string(out, &target.name)
}

fn read_target(bytes: &[u8], offset: &mut usize) -> RsfResult<StreamTarget> {
    Ok(StreamTarget::new(
        StreamSurface::from_tag(read_u8(bytes, offset)?)?,
        read_string(bytes, offset)?,
        read_string(bytes, offset)?,
    ))
}

fn write_chunk(out: &mut Vec<u8>, chunk: &StreamChunk) -> RsfResult<()> {
    write_target(out, &chunk.target)?;
    write_u64(out, chunk.offset);
    write_u64(out, chunk.len);
    out.push(chunk.lane);
    out.extend_from_slice(&chunk.expected_hash);
    Ok(())
}

fn read_chunk(bytes: &[u8], offset: &mut usize) -> RsfResult<StreamChunk> {
    let target = read_target(bytes, offset)?;
    let offset_value = read_u64(bytes, offset)?;
    let len = read_u64(bytes, offset)?;
    let lane = read_u8(bytes, offset)?;
    let hash = take(bytes, offset, 32)?;
    let mut expected_hash = [0_u8; 32];
    expected_hash.copy_from_slice(hash);
    Ok(StreamChunk {
        target,
        offset: offset_value,
        len,
        lane,
        expected_hash,
    })
}

fn take_magic(bytes: &[u8], offset: &mut usize) -> RsfResult<()> {
    let magic = take(bytes, offset, STREAM_WAL_MAGIC.len())?;
    if magic != STREAM_WAL_MAGIC {
        return Err(corrupt("stream WAL payload magic mismatch"));
    }
    Ok(())
}

fn write_string(out: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    if value.trim().is_empty() {
        return Err(invalid("stream WAL string must not be empty"));
    }
    let len = u16::try_from(value.len()).map_err(|_| invalid("stream WAL string is too long"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(bytes: &[u8], offset: &mut usize) -> RsfResult<String> {
    let len = read_u16(bytes, offset)? as usize;
    let value = take(bytes, offset, len)?;
    if value.is_empty() {
        return Err(corrupt("stream WAL string must not be empty"));
    }
    String::from_utf8(value.to_vec()).map_err(|_| corrupt("stream WAL string is not UTF-8"))
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> RsfResult<u8> {
    Ok(take(bytes, offset, 1)?[0])
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> RsfResult<u16> {
    let raw = take(bytes, offset, 2)?;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> RsfResult<u32> {
    let raw = take(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> RsfResult<u64> {
    let raw = take(bytes, offset, 8)?;
    Ok(u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> RsfResult<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| corrupt("stream WAL payload offset overflow"))?;
    if end > bytes.len() {
        return Err(corrupt("stream WAL payload is truncated"));
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_storage::WalAppend;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn write_plans_split_large_payloads_into_lanes() {
        let planner = planner();
        let target = StreamTarget::new(StreamSurface::Object, "app", "media/hero.png");
        let stream = planner
            .plan_write_bytes(target.clone(), b"0123456789")
            .expect("write");

        assert_eq!(stream.target, target);
        assert_eq!(stream.chunks.len(), 3);
        assert_eq!(
            stream
                .chunks
                .iter()
                .map(|chunk| chunk.len)
                .collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        assert_eq!(
            stream
                .chunks
                .iter()
                .map(|chunk| chunk.lane)
                .collect::<Vec<_>>(),
            vec![0, 1, 0]
        );
        assert_ne!(stream.chunks[0].expected_hash, [0_u8; 32]);
    }

    #[test]
    fn read_plans_require_contiguous_chunks_for_one_target() {
        let planner = planner();
        let target = StreamTarget::new(StreamSurface::Blob, "app", "sha256-content");
        let payload = b"abcdefgh";
        let write = planner
            .plan_write_bytes(target.clone(), payload)
            .expect("write");
        let chunk_bytes = planner
            .chunk_bytes(&write, payload)
            .expect("chunk payloads");
        let read = planner
            .plan_read(target, CommitVersion(7), write.chunks.clone())
            .expect("read");

        assert_eq!(read.min_version, CommitVersion(7));
        assert_eq!(read.lanes, 2);
        assert_eq!(
            planner.verify_bytes(&read, &chunk_bytes).expect("verify"),
            VerificationReport {
                verified_chunks: 2,
                total_bytes: 8
            }
        );
    }

    #[test]
    fn flow_window_rejects_over_capacity_without_charging() {
        let mut window = FlowWindow::new(8).expect("window");

        assert!(window.reserve(5).is_reserved());
        assert_eq!(
            window.reserve(4),
            FlowDecision::Rejected {
                bytes: 4,
                reason: "flow-control window exhausted".to_string()
            }
        );
        assert_eq!(window.in_flight_bytes(), 5);
        window.release(3).expect("release");
        assert_eq!(window.in_flight_bytes(), 2);
    }

    #[test]
    fn verification_detects_corrupt_stream_hashes() {
        let planner = planner();
        let target = StreamTarget::new(StreamSurface::KvRange, "app", "config/");
        let payload = b"data";
        let write = planner
            .plan_write_bytes(target.clone(), payload)
            .expect("write");
        let mut chunk_bytes = planner
            .chunk_bytes(&write, payload)
            .expect("chunk payloads");
        chunk_bytes[0].bytes[0] ^= 1;
        let read = planner
            .plan_read(target, CommitVersion(1), write.chunks)
            .expect("read");

        let err = planner
            .verify_bytes(&read, &chunk_bytes)
            .expect_err("corruption must be detected");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn layout_only_write_plans_cannot_be_verified_as_content_streams() {
        let planner = planner();
        let target = StreamTarget::new(StreamSurface::Object, "app", "media/hero.png");
        let write = planner.plan_write(target.clone(), 8).expect("layout");

        assert!(
            planner
                .plan_read(target, CommitVersion(1), write.chunks)
                .is_err()
        );
    }

    #[test]
    fn durable_stream_recovers_completed_transfer_after_restart() {
        let root = test_root("durable_stream_recovers_completed_transfer_after_restart");
        let planner = planner();
        let id = StreamTransferId::new("upload-1").expect("id");
        let target = StreamTarget::new(StreamSurface::Object, "app", "media/video.bin");
        let payload = b"abcdefgh";

        {
            let mut ledger = DurableStreamLedger::open(&root, Epoch(11)).expect("open ledger");
            let write = ledger
                .plan_write(id.clone(), &planner, target, payload)
                .expect("plan");
            let chunks = planner.chunk_bytes(&write, payload).expect("chunks");
            for chunk in chunks {
                ledger.record_chunk(&id, chunk).expect("chunk");
            }
            ledger.complete(&id).expect("complete");
        }

        let recovered = DurableStreamLedger::open(&root, Epoch(11)).expect("recover ledger");
        let transfer = recovered.transfer(&id).expect("transfer");
        assert_eq!(transfer.state, StreamTransferState::Complete);
        assert_eq!(transfer.completed_chunks.len(), transfer.write.chunks.len());
        assert_eq!(recovered.completed_count(), 1);
        assert_eq!(recovered.pending_count(), 0);
    }

    #[test]
    fn durable_stream_recovers_pending_transfer_without_marking_complete() {
        let root = test_root("durable_stream_recovers_pending_transfer_without_marking_complete");
        let planner = planner();
        let id = StreamTransferId::new("upload-1").expect("id");
        let target = StreamTarget::new(StreamSurface::Blob, "app", "backup.bin");
        let payload = b"abcdefgh";

        {
            let mut ledger = DurableStreamLedger::open(&root, Epoch(11)).expect("open ledger");
            let write = ledger
                .plan_write(id.clone(), &planner, target, payload)
                .expect("plan");
            let chunks = planner.chunk_bytes(&write, payload).expect("chunks");
            ledger.record_chunk(&id, chunks[0].clone()).expect("chunk");
        }

        let recovered = DurableStreamLedger::open(&root, Epoch(11)).expect("recover ledger");
        let transfer = recovered.transfer(&id).expect("transfer");
        assert_eq!(transfer.state, StreamTransferState::Pending);
        assert_eq!(transfer.completed_chunks.len(), 1);
        assert_eq!(recovered.pending_count(), 1);
    }

    #[test]
    fn durable_stream_rejects_corrupt_or_duplicate_chunk_records() {
        let root = test_root("durable_stream_rejects_corrupt_or_duplicate_chunk_records");
        let planner = planner();
        let id = StreamTransferId::new("upload-1").expect("id");
        let target = StreamTarget::new(StreamSurface::Blob, "app", "backup.bin");
        let payload = b"abcdefgh";
        let mut ledger = DurableStreamLedger::open(&root, Epoch(11)).expect("open ledger");
        let write = ledger
            .plan_write(id.clone(), &planner, target, payload)
            .expect("plan");
        let chunks = planner.chunk_bytes(&write, payload).expect("chunks");

        let mut corrupt = chunks[0].clone();
        corrupt.bytes[0] ^= 1;
        let err = ledger
            .record_chunk(&id, corrupt)
            .expect_err("corrupt chunk rejected");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);

        ledger.record_chunk(&id, chunks[0].clone()).expect("chunk");
        let err = ledger
            .record_chunk(&id, chunks[0].clone())
            .expect_err("duplicate chunk rejected");
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn durable_stream_rejects_completion_before_all_chunks_finish() {
        let root = test_root("durable_stream_rejects_completion_before_all_chunks_finish");
        let planner = planner();
        let id = StreamTransferId::new("upload-1").expect("id");
        let target = StreamTarget::new(StreamSurface::Object, "app", "media/video.bin");
        let payload = b"abcdefgh";
        let mut ledger = DurableStreamLedger::open(&root, Epoch(11)).expect("open ledger");
        let write = ledger
            .plan_write(id.clone(), &planner, target, payload)
            .expect("plan");
        let chunks = planner.chunk_bytes(&write, payload).expect("chunks");
        ledger.record_chunk(&id, chunks[0].clone()).expect("chunk");

        let err = ledger
            .complete(&id)
            .expect_err("incomplete stream cannot complete");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    #[test]
    fn durable_stream_recovery_rejects_malformed_payload() {
        let root = test_root("durable_stream_recovery_rejects_malformed_payload");
        let mut store = SegmentStore::open(&root).expect("store");
        store
            .append_wal(WalAppend {
                pg: STREAM_PG,
                epoch: Epoch(11),
                index: LogIndex(1),
                kind: WalRecordKind::Stream,
                previous_hash: [0; 32],
                payload: b"bad stream payload",
            })
            .expect("append corrupt logical payload");

        let err = DurableStreamLedger::open(&root, Epoch(11))
            .expect_err("malformed logical payload must fail closed");
        assert_eq!(err.code, ErrorCode::CorruptionDetected);
    }

    fn planner() -> StreamPlanner {
        StreamPlanner::new(StreamPolicy {
            max_chunk_bytes: 4,
            max_inflight_bytes: 8,
            read_lanes: 2,
        })
        .expect("planner")
    }

    fn test_root(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!("rsf-stream-{name}-{nanos}"));
        let _ = fs::remove_dir_all(&path);
        path
    }
}
