mod movement;
pub use movement::PageMove;
use movement::RecoveryRecord;

use std::collections::BTreeMap;

use rsf_core::CommitVersion;

const PAGE_MAGIC: &[u8; 7] = b"RSFPAGE";
const PAGE_FORMAT_VERSION: u8 = 1;
const REDO_MAGIC: &[u8; 7] = b"RSFREDO";
const REDO_FORMAT_VERSION: u8 = 1;
const PAGE_HEADER_BYTES: u32 = 64;
const SLOT_METADATA_BYTES: u32 = 16;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PageId {
    pub table: String,
    pub number: u32,
}

impl PageId {
    pub fn new(table: impl Into<String>, number: u32) -> Self {
        Self {
            table: table.into(),
            number,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageSlot {
    pub row_key: Vec<u8>,
    pub offset: u32,
    pub length: u32,
    pub deleted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageImage {
    pub id: PageId,
    pub lsn: CommitVersion,
    pub page_size: u32,
    legacy_rows: u16,
    slots: BTreeMap<Vec<u8>, Vec<u8>>,
    tombstones: BTreeMap<Vec<u8>, PageSlot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageFrame {
    pub id: PageId,
    pub lsn: CommitVersion,
    pub rows: u16,
    pub bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LsnValidation {
    pub page: PageId,
    pub expected: CommitVersion,
    pub actual: CommitVersion,
}

impl LsnValidation {
    pub fn require_exact(
        page: &PageId,
        expected: CommitVersion,
        actual: CommitVersion,
    ) -> Result<Self, String> {
        if expected == actual {
            Ok(Self {
                page: page.clone(),
                expected,
                actual,
            })
        } else {
            Err(format!(
                "page lsn mismatch: expected {} actual {}",
                expected.0, actual.0
            ))
        }
    }

    pub fn require_at_least(
        page: &PageId,
        minimum: CommitVersion,
        actual: CommitVersion,
    ) -> Result<Self, String> {
        if actual >= minimum {
            Ok(Self {
                page: page.clone(),
                expected: minimum,
                actual,
            })
        } else {
            Err(format!(
                "page lsn below requested minimum: minimum {} actual {}",
                minimum.0, actual.0
            ))
        }
    }

    pub fn require_advance(
        page: &PageId,
        base: CommitVersion,
        commit: CommitVersion,
    ) -> Result<Self, String> {
        if commit > base {
            Ok(Self {
                page: page.clone(),
                expected: base,
                actual: commit,
            })
        } else {
            Err(format!(
                "redo lsn must advance: base {} commit {}",
                base.0, commit.0
            ))
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedoOperation {
    InsertRow,
    UpdateRow,
    DeleteRow,
    SplitPage,
}

impl RedoOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InsertRow => "insert-row",
            Self::UpdateRow => "update-row",
            Self::DeleteRow => "delete-row",
            Self::SplitPage => "split-page",
        }
    }

    fn tag(&self) -> u8 {
        match self {
            Self::InsertRow => 1,
            Self::UpdateRow => 2,
            Self::DeleteRow => 3,
            Self::SplitPage => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, String> {
        match tag {
            1 => Ok(Self::InsertRow),
            2 => Ok(Self::UpdateRow),
            3 => Ok(Self::DeleteRow),
            4 => Ok(Self::SplitPage),
            _ => Err("redo record has unknown operation tag".to_string()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedoRecord {
    pub page: PageId,
    pub base_lsn: CommitVersion,
    pub commit_lsn: CommitVersion,
    pub operation: RedoOperation,
    pub row_delta: i16,
    pub byte_delta: i32,
    pub row_key: Option<Vec<u8>>,
    pub row_payload: Option<Vec<u8>>,
}

impl RedoRecord {
    pub fn new(
        page: PageId,
        base_lsn: CommitVersion,
        commit_lsn: CommitVersion,
        operation: RedoOperation,
        row_delta: i16,
        byte_delta: i32,
    ) -> Self {
        Self {
            page,
            base_lsn,
            commit_lsn,
            operation,
            row_delta,
            byte_delta,
            row_key: None,
            row_payload: None,
        }
    }

    pub fn insert_row(
        page: PageId,
        base_lsn: CommitVersion,
        commit_lsn: CommitVersion,
        row_key: impl Into<Vec<u8>>,
        row_payload: impl Into<Vec<u8>>,
    ) -> Self {
        let row_payload = row_payload.into();
        Self {
            page,
            base_lsn,
            commit_lsn,
            operation: RedoOperation::InsertRow,
            row_delta: 1,
            byte_delta: i32::try_from(row_payload.len()).unwrap_or(i32::MAX),
            row_key: Some(row_key.into()),
            row_payload: Some(row_payload),
        }
    }

    pub fn update_row(
        page: PageId,
        base_lsn: CommitVersion,
        commit_lsn: CommitVersion,
        row_key: impl Into<Vec<u8>>,
        row_payload: impl Into<Vec<u8>>,
    ) -> Self {
        let row_payload = row_payload.into();
        Self {
            page,
            base_lsn,
            commit_lsn,
            operation: RedoOperation::UpdateRow,
            row_delta: 0,
            byte_delta: i32::try_from(row_payload.len()).unwrap_or(i32::MAX),
            row_key: Some(row_key.into()),
            row_payload: Some(row_payload),
        }
    }

    pub fn delete_row(
        page: PageId,
        base_lsn: CommitVersion,
        commit_lsn: CommitVersion,
        row_key: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            page,
            base_lsn,
            commit_lsn,
            operation: RedoOperation::DeleteRow,
            row_delta: -1,
            byte_delta: 0,
            row_key: Some(row_key.into()),
            row_payload: None,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let mut out = Vec::new();
        out.extend_from_slice(REDO_MAGIC);
        out.push(REDO_FORMAT_VERSION);
        out.push(self.operation.tag());
        put_string(&mut out, &self.page.table)?;
        put_u32(&mut out, self.page.number);
        put_u64(&mut out, self.base_lsn.0);
        put_u64(&mut out, self.commit_lsn.0);
        put_i16(&mut out, self.row_delta);
        put_i32(&mut out, self.byte_delta);
        match &self.row_key {
            Some(row_key) => {
                out.push(1);
                put_bytes(&mut out, row_key)?;
            }
            None => out.push(0),
        }
        match &self.row_payload {
            Some(row_payload) => {
                out.push(1);
                put_bytes(&mut out, row_payload)?;
            }
            None => out.push(0),
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        cursor.expect_magic(REDO_MAGIC)?;
        let version = cursor.u8()?;
        if version != REDO_FORMAT_VERSION {
            return Err("unsupported redo record format version".to_string());
        }
        let operation = RedoOperation::from_tag(cursor.u8()?)?;
        let table = cursor.string()?;
        let number = cursor.u32()?;
        let base_lsn = CommitVersion(cursor.u64()?);
        let commit_lsn = CommitVersion(cursor.u64()?);
        let row_delta = cursor.i16()?;
        let byte_delta = cursor.i32()?;
        let row_key = match cursor.u8()? {
            0 => None,
            1 => Some(cursor.bytes()?.to_vec()),
            _ => return Err("redo record has invalid row-key presence flag".to_string()),
        };
        let row_payload = match cursor.u8()? {
            0 => None,
            1 => Some(cursor.bytes()?.to_vec()),
            _ => return Err("redo record has invalid row-payload presence flag".to_string()),
        };
        cursor.finish()?;
        let record = Self {
            page: PageId { table, number },
            base_lsn,
            commit_lsn,
            operation,
            row_delta,
            byte_delta,
            row_key,
            row_payload,
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_page_id(&self.page)?;
        LsnValidation::require_advance(&self.page, self.base_lsn, self.commit_lsn)?;
        match self.operation {
            RedoOperation::InsertRow => {
                validate_row_key(
                    self.row_key
                        .as_deref()
                        .ok_or_else(|| "insert redo requires row key".to_string())?,
                )?;
                validate_row_payload(
                    self.row_payload
                        .as_deref()
                        .ok_or_else(|| "insert redo requires row payload".to_string())?,
                )?;
            }
            RedoOperation::UpdateRow => {
                validate_row_key(
                    self.row_key
                        .as_deref()
                        .ok_or_else(|| "update redo requires row key".to_string())?,
                )?;
                validate_row_payload(
                    self.row_payload
                        .as_deref()
                        .ok_or_else(|| "update redo requires row payload".to_string())?,
                )?;
            }
            RedoOperation::DeleteRow => {
                validate_row_key(
                    self.row_key
                        .as_deref()
                        .ok_or_else(|| "delete redo requires row key".to_string())?,
                )?;
                if self.row_payload.is_some() {
                    return Err("delete redo must not carry row payload".to_string());
                }
            }
            RedoOperation::SplitPage => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PageDecision {
    Applied(PageFrame),
    Read(PageFrame),
    Checkpoint(Checkpoint),
    Replay(ReplayReport),
    Rejected { page: PageId, reason: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    pub pages: usize,
    pub low_lsn: CommitVersion,
    pub high_lsn: CommitVersion,
    pub bytes: u64,
    pub live_rows: u64,
    pub tombstones: u64,
    pub summary_bytes: u64,
    pub page_summaries: Vec<CheckpointPageSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPageSummary {
    pub page: PageId,
    pub lsn: CommitVersion,
    pub bytes: u32,
    pub live_rows: u16,
    pub tombstones: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayReport {
    pub pages: usize,
    pub decoded_images: usize,
    pub decoded_records: usize,
    pub applied_records: usize,
    pub skipped_records: usize,
    pub high_lsn: CommitVersion,
    pub bytes: u64,
}

impl PageImage {
    pub fn new(id: PageId, lsn: CommitVersion, page_size: u32) -> Result<Self, String> {
        validate_page_id(&id)?;
        if page_size < PAGE_HEADER_BYTES {
            return Err("page size must fit the page header".to_string());
        }
        let image = Self {
            id,
            lsn,
            page_size,
            legacy_rows: 0,
            slots: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        };
        image.ensure_capacity_for(None)?;
        Ok(image)
    }

    pub fn insert_row(&mut self, key: Vec<u8>, payload: Vec<u8>) -> Result<(), String> {
        validate_row_key(&key)?;
        validate_row_payload(&payload)?;
        if self.slots.contains_key(&key) {
            return Err("page row already exists".to_string());
        }
        self.ensure_capacity_for(Some((&key, &payload)))?;
        self.tombstones.remove(&key);
        self.slots.insert(key, payload);
        Ok(())
    }

    pub fn update_row(&mut self, key: Vec<u8>, payload: Vec<u8>) -> Result<(), String> {
        validate_row_key(&key)?;
        validate_row_payload(&payload)?;
        if !self.slots.contains_key(&key) {
            return Err("page row is missing".to_string());
        }
        self.ensure_capacity_for(Some((&key, &payload)))?;
        self.slots.insert(key, payload);
        Ok(())
    }

    pub fn delete_row(&mut self, key: &[u8]) -> Result<(), String> {
        validate_row_key(key)?;
        let Some(payload) = self.slots.remove(key) else {
            return Err("page row is missing".to_string());
        };
        self.tombstones.insert(
            key.to_vec(),
            PageSlot {
                row_key: key.to_vec(),
                offset: 0,
                length: payload.len() as u32,
                deleted: true,
            },
        );
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        let slots = self.slots()?;
        let mut out = Vec::new();
        out.extend_from_slice(PAGE_MAGIC);
        out.push(PAGE_FORMAT_VERSION);
        put_u32(&mut out, self.page_size);
        put_u64(&mut out, self.lsn.0);
        put_string(&mut out, &self.id.table)?;
        put_u32(&mut out, self.id.number);
        put_u16(&mut out, self.legacy_rows);
        put_u32(&mut out, slots.len() as u32);
        for slot in &slots {
            put_bytes(&mut out, &slot.row_key)?;
            put_u32(&mut out, slot.offset);
            put_u32(&mut out, slot.length);
            out.push(u8::from(slot.deleted));
            if slot.deleted {
                put_bytes(&mut out, &[])?;
            } else {
                let payload = self
                    .slots
                    .get(&slot.row_key)
                    .ok_or_else(|| "live page slot is missing payload".to_string())?;
                put_bytes(&mut out, payload)?;
            }
        }
        if out.len() > self.page_size as usize {
            return Err("encoded page exceeds page size".to_string());
        }
        out.resize(self.page_size as usize, 0);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        cursor.expect_magic(PAGE_MAGIC)?;
        let version = cursor.u8()?;
        if version != PAGE_FORMAT_VERSION {
            return Err("unsupported page format version".to_string());
        }
        let page_size = cursor.u32()?;
        if page_size as usize != bytes.len() {
            return Err("encoded page size does not match payload length".to_string());
        }
        let lsn = CommitVersion(cursor.u64()?);
        let table = cursor.string()?;
        let number = cursor.u32()?;
        let legacy_rows = cursor.u16()?;
        let slot_count = cursor.u32()? as usize;
        let id = PageId { table, number };
        validate_page_id(&id)?;
        let mut image = Self {
            id,
            lsn,
            page_size,
            legacy_rows,
            slots: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        };
        let mut encoded_slots = Vec::new();
        for _ in 0..slot_count {
            let row_key = cursor.bytes()?;
            validate_row_key(row_key)?;
            if image.slots.contains_key(row_key) || image.tombstones.contains_key(row_key) {
                return Err("page contains duplicate row keys".to_string());
            }
            let offset = cursor.u32()?;
            let length = cursor.u32()?;
            let deleted = match cursor.u8()? {
                0 => false,
                1 => true,
                _ => return Err("page slot has invalid deleted flag".to_string()),
            };
            let payload = cursor.bytes()?;
            let slot = PageSlot {
                row_key: row_key.to_vec(),
                offset,
                length,
                deleted,
            };
            encoded_slots.push(slot.clone());
            if deleted {
                if !payload.is_empty() {
                    return Err("deleted page slot must not carry payload".to_string());
                }
                image.tombstones.insert(row_key.to_vec(), slot);
            } else {
                if payload.len() as u32 != length {
                    return Err("page slot payload length mismatch".to_string());
                }
                image.slots.insert(row_key.to_vec(), payload.to_vec());
            }
        }
        image.ensure_capacity_for(None)?;
        if image.slots()? != encoded_slots {
            return Err("page slot offsets or ordering are invalid".to_string());
        }
        if bytes[cursor.offset..].iter().any(|byte| *byte != 0) {
            return Err("page padding must be zero".to_string());
        }
        Ok(image)
    }

    pub fn frame(&self) -> PageFrame {
        PageFrame {
            id: self.id.clone(),
            lsn: self.lsn,
            rows: self.rows(),
            bytes: self.page_size,
        }
    }

    pub fn checkpoint_summary(&self) -> CheckpointPageSummary {
        CheckpointPageSummary {
            page: self.id.clone(),
            lsn: self.lsn,
            bytes: self.page_size,
            live_rows: self.rows(),
            tombstones: u16::try_from(self.tombstones.len()).unwrap_or(u16::MAX),
        }
    }

    pub fn slots(&self) -> Result<Vec<PageSlot>, String> {
        let mut offset = PAGE_HEADER_BYTES;
        let mut slots = Vec::new();
        for (key, payload) in &self.slots {
            slots.push(PageSlot {
                row_key: key.clone(),
                offset,
                length: payload.len() as u32,
                deleted: false,
            });
            offset = offset
                .checked_add(SLOT_METADATA_BYTES)
                .and_then(|value| value.checked_add(payload.len() as u32))
                .ok_or_else(|| "page slot offset overflow".to_string())?;
        }
        slots.extend(self.tombstones.values().cloned());
        Ok(slots)
    }

    fn rows(&self) -> u16 {
        self.legacy_rows
            .saturating_add(u16::try_from(self.slots.len()).unwrap_or(u16::MAX))
    }

    fn apply_legacy_delta(&mut self, row_delta: i16) {
        self.legacy_rows = apply_delta(self.legacy_rows, row_delta);
    }

    fn ensure_capacity_for(&self, replacement: Option<(&[u8], &[u8])>) -> Result<(), String> {
        // The slot-offset model and the serialized envelope have different
        // overheads. Both must fit, including retained deletion markers.
        let mut logical_bytes = u64::from(PAGE_HEADER_BYTES);
        let mut encoded_bytes = 34u64 + self.id.table.len() as u64;
        let replacement_key = replacement.map(|(key, _)| key);
        for (key, payload) in &self.slots {
            let payload = match replacement {
                Some((candidate, payload)) if candidate == key => payload,
                _ => payload.as_slice(),
            };
            logical_bytes +=
                u64::from(SLOT_METADATA_BYTES) + key.len() as u64 + payload.len() as u64;
            encoded_bytes += 17 + key.len() as u64 + payload.len() as u64;
        }
        if let Some((key, payload)) = replacement
            && !self.slots.contains_key(key)
        {
            logical_bytes +=
                u64::from(SLOT_METADATA_BYTES) + key.len() as u64 + payload.len() as u64;
            encoded_bytes += 17 + key.len() as u64 + payload.len() as u64;
        }
        for key in self.tombstones.keys() {
            if replacement_key != Some(key.as_slice()) {
                encoded_bytes += 17 + key.len() as u64;
            }
        }
        if logical_bytes.max(encoded_bytes) > u64::from(self.page_size) {
            Err("page does not have enough free space".to_string())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PageServer {
    pages: BTreeMap<PageId, PageImage>,
    redo_records: usize,
}

impl PageServer {
    pub fn recover(
        page_images: &[Vec<u8>],
        redo_records: &[Vec<u8>],
    ) -> Result<Self, PageDecision> {
        Self::recover_with_report(page_images, redo_records).map(|(server, _)| server)
    }

    pub fn recover_with_report(
        page_images: &[Vec<u8>],
        redo_records: &[Vec<u8>],
    ) -> Result<(Self, ReplayReport), PageDecision> {
        replay_page_server(page_images, redo_records)
    }

    pub fn replay(page_images: &[Vec<u8>], redo_records: &[Vec<u8>]) -> PageDecision {
        replay_page_server(page_images, redo_records)
            .map(|(_, report)| PageDecision::Replay(report))
            .unwrap_or_else(|decision| decision)
    }

    pub fn create_page(&mut self, page: PageId, lsn: CommitVersion, bytes: u32) -> PageDecision {
        if let Err(reason) = validate_page_id(&page) {
            return PageDecision::Rejected { page, reason };
        }
        if self.pages.contains_key(&page) {
            return PageDecision::Rejected {
                page,
                reason: "page already exists".to_string(),
            };
        }
        let image = match PageImage::new(page.clone(), lsn, bytes) {
            Ok(image) => image,
            Err(reason) => {
                return PageDecision::Rejected { page, reason };
            }
        };
        let frame = image.frame();
        self.pages.insert(page, image);
        PageDecision::Applied(frame)
    }

    pub fn apply_redo(&mut self, record: RedoRecord) -> PageDecision {
        let Some(image) = self.pages.get_mut(&record.page) else {
            return PageDecision::Rejected {
                page: record.page,
                reason: "page not found".to_string(),
            };
        };
        if let Err(reason) = LsnValidation::require_exact(&record.page, record.base_lsn, image.lsn)
        {
            return PageDecision::Rejected {
                page: record.page,
                reason,
            };
        }
        if let Err(reason) =
            LsnValidation::require_advance(&record.page, record.base_lsn, record.commit_lsn)
        {
            return PageDecision::Rejected {
                page: record.page,
                reason,
            };
        }

        if let Err(reason) = apply_redo_to_image(image, &record) {
            return PageDecision::Rejected {
                page: record.page,
                reason,
            };
        }
        image.lsn = record.commit_lsn;
        self.redo_records += 1;
        PageDecision::Applied(image.frame())
    }

    pub fn read_page(&self, page: &PageId, min_lsn: CommitVersion) -> PageDecision {
        let Some(image) = self.pages.get(page) else {
            return PageDecision::Rejected {
                page: page.clone(),
                reason: "page not found".to_string(),
            };
        };
        if let Err(reason) = LsnValidation::require_at_least(page, min_lsn, image.lsn) {
            return PageDecision::Rejected {
                page: page.clone(),
                reason,
            };
        }
        PageDecision::Read(image.frame())
    }

    pub fn read_image(&self, page: &PageId, min_lsn: CommitVersion) -> Result<PageImage, String> {
        let Some(image) = self.pages.get(page) else {
            return Err("page not found".to_string());
        };
        LsnValidation::require_at_least(page, min_lsn, image.lsn)?;
        Ok(image.clone())
    }

    pub fn checkpoint(&self) -> PageDecision {
        let page_summaries = self
            .pages
            .values()
            .map(PageImage::checkpoint_summary)
            .collect::<Vec<_>>();
        let summary_bytes = encode_checkpoint_summary(&page_summaries)
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0);
        let checkpoint = Checkpoint {
            pages: self.pages.len(),
            low_lsn: self.low_lsn(),
            high_lsn: self.high_lsn(),
            bytes: self.page_bytes(),
            live_rows: page_summaries
                .iter()
                .map(|summary| u64::from(summary.live_rows))
                .sum(),
            tombstones: page_summaries
                .iter()
                .map(|summary| u64::from(summary.tombstones))
                .sum(),
            summary_bytes,
            page_summaries,
        };
        PageDecision::Checkpoint(checkpoint)
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    pub fn page_ids(&self) -> Vec<PageId> {
        self.pages.keys().cloned().collect()
    }

    pub fn redo_count(&self) -> usize {
        self.redo_records
    }

    fn high_lsn(&self) -> CommitVersion {
        self.pages
            .values()
            .map(|image| image.lsn)
            .max()
            .unwrap_or(CommitVersion(0))
    }

    fn low_lsn(&self) -> CommitVersion {
        self.pages
            .values()
            .map(|image| image.lsn)
            .min()
            .unwrap_or(CommitVersion(0))
    }

    fn page_bytes(&self) -> u64 {
        self.pages
            .values()
            .map(|image| u64::from(image.page_size))
            .sum()
    }
}

fn apply_redo_to_image(image: &mut PageImage, record: &RedoRecord) -> Result<(), String> {
    match (&record.operation, &record.row_key, &record.row_payload) {
        (RedoOperation::InsertRow, Some(key), Some(payload)) => {
            image.insert_row(key.clone(), payload.clone())
        }
        (RedoOperation::UpdateRow, Some(key), Some(payload)) => {
            image.update_row(key.clone(), payload.clone())
        }
        (RedoOperation::DeleteRow, Some(key), None) => image.delete_row(key),
        (RedoOperation::SplitPage, _, _) => {
            image.apply_legacy_delta(record.row_delta);
            Ok(())
        }
        (_, None, None) => {
            image.apply_legacy_delta(record.row_delta);
            Ok(())
        }
        _ => Err("redo row payload does not match operation".to_string()),
    }
}

fn replay_page_server(
    page_images: &[Vec<u8>],
    redo_records: &[Vec<u8>],
) -> Result<(PageServer, ReplayReport), PageDecision> {
    let mut server = PageServer::default();
    let mut decoded_images = 0;
    let mut decoded_records = Vec::new();

    for bytes in page_images {
        let image = PageImage::decode(bytes).map_err(|reason| PageDecision::Rejected {
            page: PageId::new("replay", 1),
            reason: format!("page image replay decode rejected: {reason}"),
        })?;
        if server.pages.contains_key(&image.id) {
            return Err(PageDecision::Rejected {
                page: image.id,
                reason: "duplicate replay page image".to_string(),
            });
        }
        decoded_images += 1;
        server.pages.insert(image.id.clone(), image);
    }

    let mut seen = BTreeMap::new();
    for bytes in redo_records {
        let record = RecoveryRecord::decode(bytes).map_err(|reason| PageDecision::Rejected {
            page: PageId::new("replay", 1),
            reason: format!("redo replay decode rejected: {reason}"),
        })?;
        for row in record.rows() {
            let key = (row.page.clone(), row.commit_lsn);
            if let Some(previous) = seen.insert(key, bytes)
                && previous != bytes
            {
                return Err(PageDecision::Rejected {
                    page: row.page.clone(),
                    reason: "conflicting replay records at the same page LSN".to_string(),
                });
            }
        }
        decoded_records.push(record);
    }
    // Moves introduce dependencies between pages. Global LSN order preserves
    // these dependencies; grouping replay by page would break them.
    decoded_records.sort_by_key(RecoveryRecord::order);

    let mut skipped_records = 0;
    for record in decoded_records {
        let skipped = match record {
            RecoveryRecord::Row(row) => apply_replay_record(&mut server, row)?,
            RecoveryRecord::Move(record) => {
                server
                    .apply_move_inner(&record, true)
                    .map_err(|reason| PageDecision::Rejected {
                        page: record.remove.page,
                        reason,
                    })?
            }
        };
        if skipped {
            skipped_records += 1;
        }
    }

    let report = ReplayReport {
        pages: server.page_count(),
        decoded_images,
        decoded_records: redo_records.len(),
        applied_records: server.redo_count(),
        skipped_records,
        high_lsn: server.high_lsn(),
        bytes: server.page_bytes(),
    };
    Ok((server, report))
}

fn apply_replay_record(server: &mut PageServer, record: RedoRecord) -> Result<bool, PageDecision> {
    let Some(image) = server.pages.get(&record.page) else {
        return Err(PageDecision::Rejected {
            page: record.page,
            reason: "redo replay page not found".to_string(),
        });
    };
    if record.commit_lsn <= image.lsn {
        if record.base_lsn < image.lsn {
            return Ok(true);
        }
        return Err(PageDecision::Rejected {
            page: record.page,
            reason: "redo replay does not advance page lsn".to_string(),
        });
    }
    if record.base_lsn != image.lsn {
        return Err(PageDecision::Rejected {
            page: record.page,
            reason: format!(
                "redo replay lsn gap: page_lsn={} record_base_lsn={}",
                image.lsn.0, record.base_lsn.0
            ),
        });
    }
    match server.apply_redo(record) {
        PageDecision::Applied(_) => Ok(false),
        PageDecision::Rejected { page, reason } => Err(PageDecision::Rejected {
            page,
            reason: format!("redo replay apply rejected: {reason}"),
        }),
        other => Err(PageDecision::Rejected {
            page: PageId::new("replay", 1),
            reason: format!("unexpected replay apply decision {other:?}"),
        }),
    }
}

fn encode_checkpoint_summary(summaries: &[CheckpointPageSummary]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    out.extend_from_slice(b"RSFCKPT");
    out.push(1);
    put_u32(
        &mut out,
        u32::try_from(summaries.len())
            .map_err(|_| "checkpoint summary has too many pages".to_string())?,
    );
    for summary in summaries {
        put_string(&mut out, &summary.page.table)?;
        put_u32(&mut out, summary.page.number);
        put_u64(&mut out, summary.lsn.0);
        put_u32(&mut out, summary.bytes);
        put_u16(&mut out, summary.live_rows);
        put_u16(&mut out, summary.tombstones);
    }
    Ok(out)
}

fn apply_delta<T>(value: T, delta: impl Into<i64>) -> T
where
    T: TryFrom<i64> + Into<i64>,
{
    let next = value.into() + delta.into();
    T::try_from(next.max(0))
        .ok()
        .expect("delta result must fit")
}

fn validate_page_id(page: &PageId) -> Result<(), String> {
    if page.table.trim().is_empty() {
        return Err("page table must not be empty".to_string());
    }
    if page.number == 0 {
        return Err("page number must be non-zero".to_string());
    }
    Ok(())
}

fn validate_row_key(key: &[u8]) -> Result<(), String> {
    if key.is_empty() {
        Err("page row key must not be empty".to_string())
    } else {
        Ok(())
    }
}

fn validate_row_payload(payload: &[u8]) -> Result<(), String> {
    if payload.is_empty() {
        Err("page row payload must not be empty".to_string())
    } else {
        Ok(())
    }
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_i16(out: &mut Vec<u8>, value: i16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), String> {
    put_bytes(out, value.as_bytes())
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), String> {
    let len = u32::try_from(value.len()).map_err(|_| "byte field is too large".to_string())?;
    put_u32(out, len);
    out.extend_from_slice(value);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8]) -> Result<(), String> {
        let actual = self.take(magic.len())?;
        if actual == magic {
            Ok(())
        } else {
            Err("page has invalid magic".to_string())
        }
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(*self.take(1)?.first().expect("take(1) returns one byte"))
    }

    fn u16(&mut self) -> Result<u16, String> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> Result<u32, String> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> Result<u64, String> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn i16(&mut self) -> Result<i16, String> {
        let bytes = self.take(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn i32(&mut self) -> Result<i32, String> {
        let bytes = self.take(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn string(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| "page string field is not utf-8".to_string())
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| "page cursor offset overflow".to_string())?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| "page ended early".to_string())?;
        self.offset = end;
        Ok(slice)
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("record has trailing bytes".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_decode_rejects_duplicate_keys_invalid_offsets_and_padding() {
        let mut image = PageImage::new(PageId::new("users", 1), CommitVersion(1), 256).unwrap();
        image.insert_row(b"a".to_vec(), b"one".to_vec()).unwrap();
        image.insert_row(b"b".to_vec(), b"two".to_vec()).unwrap();
        let encoded = image.encode().unwrap();
        // Header is 34 + table length; each slot has 17 bytes of metadata.
        let first = 34 + "users".len();
        let second = first + 17 + 1 + 3;
        let mut duplicate = encoded.clone();
        duplicate[second + 4] = b'a';
        assert!(
            PageImage::decode(&duplicate)
                .unwrap_err()
                .contains("duplicate")
        );
        let mut offset = encoded.clone();
        offset[first + 5..first + 9].copy_from_slice(&0u32.to_be_bytes());
        assert!(PageImage::decode(&offset).unwrap_err().contains("offsets"));
        let mut padding = encoded;
        *padding.last_mut().unwrap() = 1;
        assert!(PageImage::decode(&padding).unwrap_err().contains("padding"));
    }

    #[test]
    fn accepted_rows_and_tombstones_always_fit_the_serialized_page() {
        let mut image = PageImage::new(PageId::new("users", 1), CommitVersion(0), 256).unwrap();
        for n in 0..100u32 {
            let before = image.clone();
            let key = n.to_be_bytes().to_vec();
            if image.insert_row(key.clone(), vec![b'x'; 8]).is_err() {
                assert_eq!(image, before, "rejected insertion must be atomic");
                break;
            }
            assert_eq!(
                PageImage::decode(&image.encode().expect("accepted insert fits")).unwrap(),
                image
            );
            image.delete_row(&key).unwrap();
            assert_eq!(
                PageImage::decode(&image.encode().expect("delete fits")).unwrap(),
                image
            );
        }
    }

    #[test]
    fn long_table_names_are_included_in_page_capacity() {
        let id = PageId::new("t".repeat(80), 1);
        let mut image = PageImage::new(id.clone(), CommitVersion(0), 128).unwrap();
        assert!(image.insert_row(b"k".to_vec(), vec![1; 20]).is_err());
        assert!(PageImage::new(id, CommitVersion(0), 64).is_err());
    }

    #[test]
    fn applies_redo_when_lsn_matches() {
        let mut pages = PageServer::default();
        let page = PageId::new("users", 1);
        pages.create_page(page.clone(), CommitVersion(10), 4096);

        let decision = pages.apply_redo(RedoRecord::new(
            page,
            CommitVersion(10),
            CommitVersion(11),
            RedoOperation::InsertRow,
            1,
            128,
        ));

        let PageDecision::Applied(frame) = decision else {
            panic!("expected redo to apply");
        };
        assert_eq!(frame.rows, 1);
        assert_eq!(frame.bytes, 4096);
        assert_eq!(pages.redo_count(), 1);
    }

    #[test]
    fn page_image_encodes_decodes_header_slots_and_rows() {
        let page = PageId::new("users", 1);
        let mut image = PageImage::new(page.clone(), CommitVersion(7), 512).expect("page image");
        image
            .insert_row(b"user/1".to_vec(), b"ada".to_vec())
            .expect("insert row");
        image
            .insert_row(b"user/2".to_vec(), b"grace".to_vec())
            .expect("insert second row");

        let encoded = image.encode().expect("encode page");
        assert_eq!(encoded.len(), 512);
        assert_eq!(&encoded[..7], b"RSFPAGE");

        let decoded = PageImage::decode(&encoded).expect("decode page");
        assert_eq!(decoded.id, page);
        assert_eq!(decoded.lsn, CommitVersion(7));
        assert_eq!(decoded.frame().rows, 2);
        assert_eq!(decoded.frame().bytes, 512);
        let slots = decoded.slots().expect("slots");
        assert_eq!(slots.len(), 2);
        assert!(slots.iter().all(|slot| !slot.deleted));
        assert!(slots.windows(2).all(|pair| pair[0].offset < pair[1].offset));
    }

    #[test]
    fn page_server_applies_row_redo_to_physical_slots() {
        let mut pages = PageServer::default();
        let page = PageId::new("users", 1);
        pages.create_page(page.clone(), CommitVersion(10), 512);

        assert!(matches!(
            pages.apply_redo(RedoRecord::insert_row(
                page.clone(),
                CommitVersion(10),
                CommitVersion(11),
                b"user/1".to_vec(),
                b"ada".to_vec(),
            )),
            PageDecision::Applied(_)
        ));
        assert!(matches!(
            pages.apply_redo(RedoRecord::update_row(
                page.clone(),
                CommitVersion(11),
                CommitVersion(12),
                b"user/1".to_vec(),
                b"grace".to_vec(),
            )),
            PageDecision::Applied(_)
        ));
        assert!(matches!(
            pages.apply_redo(RedoRecord::delete_row(
                page.clone(),
                CommitVersion(12),
                CommitVersion(13),
                b"user/1".to_vec(),
            )),
            PageDecision::Applied(_)
        ));

        let image = pages
            .read_image(&page, CommitVersion(13))
            .expect("page image");
        assert_eq!(image.frame().rows, 0);
        let slots = image.slots().expect("slots");
        assert_eq!(slots.len(), 1);
        assert!(slots[0].deleted);
        assert_eq!(pages.redo_count(), 3);
    }

    #[test]
    fn page_format_rejects_oversized_rows() {
        let mut image =
            PageImage::new(PageId::new("users", 1), CommitVersion(1), 96).expect("page image");

        let err = image
            .insert_row(b"user/large".to_vec(), vec![b'x'; 256])
            .expect_err("row should not fit");

        assert_eq!(err, "page does not have enough free space");
    }

    #[test]
    fn redo_record_encodes_decodes_row_payloads_deterministically() {
        let record = RedoRecord::update_row(
            PageId::new("users", 7),
            CommitVersion(21),
            CommitVersion(22),
            b"user/7".to_vec(),
            b"lovelace".to_vec(),
        );

        let encoded = record.encode().expect("encode redo");
        assert_eq!(&encoded[..7], b"RSFREDO");
        let decoded = RedoRecord::decode(&encoded).expect("decode redo");

        assert_eq!(decoded, record);
        assert_eq!(decoded.row_key.as_deref(), Some(&b"user/7"[..]));
        assert_eq!(decoded.row_payload.as_deref(), Some(&b"lovelace"[..]));
    }

    #[test]
    fn redo_record_rejects_invalid_payload_shape_and_corruption() {
        let missing_payload = RedoRecord {
            page: PageId::new("users", 1),
            base_lsn: CommitVersion(1),
            commit_lsn: CommitVersion(2),
            operation: RedoOperation::InsertRow,
            row_delta: 1,
            byte_delta: 3,
            row_key: Some(b"user/1".to_vec()),
            row_payload: None,
        };
        assert_eq!(
            missing_payload.validate().expect_err("missing payload"),
            "insert redo requires row payload"
        );

        let mut encoded = RedoRecord::delete_row(
            PageId::new("users", 1),
            CommitVersion(2),
            CommitVersion(3),
            b"user/1".to_vec(),
        )
        .encode()
        .expect("encode delete");
        encoded[8] = 99;
        assert_eq!(
            RedoRecord::decode(&encoded).expect_err("bad tag"),
            "redo record has unknown operation tag"
        );
    }

    #[test]
    fn lsn_validation_reports_exact_minimum_and_advance_errors() {
        let page = PageId::new("users", 1);

        let exact = LsnValidation::require_exact(&page, CommitVersion(10), CommitVersion(10))
            .expect("exact lsn");
        assert_eq!(exact.page, page);
        assert_eq!(exact.expected, CommitVersion(10));
        assert_eq!(exact.actual, CommitVersion(10));

        let exact_error = LsnValidation::require_exact(&page, CommitVersion(10), CommitVersion(9))
            .expect_err("mismatch");
        assert!(exact_error.contains("page lsn mismatch"));

        let minimum = LsnValidation::require_at_least(&page, CommitVersion(10), CommitVersion(11))
            .expect("minimum satisfied");
        assert_eq!(minimum.expected, CommitVersion(10));
        assert_eq!(minimum.actual, CommitVersion(11));

        let minimum_error =
            LsnValidation::require_at_least(&page, CommitVersion(10), CommitVersion(9))
                .expect_err("below minimum");
        assert!(minimum_error.contains("below requested minimum"));

        let advance = LsnValidation::require_advance(&page, CommitVersion(10), CommitVersion(11))
            .expect("advance");
        assert_eq!(advance.expected, CommitVersion(10));
        assert_eq!(advance.actual, CommitVersion(11));

        let advance_error =
            LsnValidation::require_advance(&page, CommitVersion(10), CommitVersion(10))
                .expect_err("not advanced");
        assert!(advance_error.contains("redo lsn must advance"));
    }

    #[test]
    fn rejects_stale_redo() {
        let mut pages = PageServer::default();
        let page = PageId::new("users", 1);
        pages.create_page(page.clone(), CommitVersion(10), 4096);

        let decision = pages.apply_redo(RedoRecord::new(
            page,
            CommitVersion(9),
            CommitVersion(11),
            RedoOperation::InsertRow,
            1,
            128,
        ));

        let PageDecision::Rejected { reason, .. } = decision else {
            panic!("expected stale redo rejection");
        };
        assert!(reason.contains("page lsn mismatch"));
        assert_eq!(pages.redo_count(), 0);
    }

    #[test]
    fn page_replay_decodes_images_and_redo_into_deterministic_state() {
        let page = PageId::new("users", 1);
        let base = PageImage::new(page.clone(), CommitVersion(10), 512)
            .expect("base image")
            .encode()
            .expect("encode base");
        let records = [
            RedoRecord::insert_row(
                page.clone(),
                CommitVersion(10),
                CommitVersion(11),
                b"user/1".to_vec(),
                b"ada".to_vec(),
            ),
            RedoRecord::update_row(
                page.clone(),
                CommitVersion(11),
                CommitVersion(12),
                b"user/1".to_vec(),
                b"grace".to_vec(),
            ),
        ]
        .into_iter()
        .map(|record| record.encode().expect("encode redo"))
        .collect::<Vec<_>>();

        let decision = PageServer::replay(&[base], &records);

        let PageDecision::Replay(report) = decision else {
            panic!("expected replay report");
        };
        assert_eq!(report.pages, 1);
        assert_eq!(report.decoded_images, 1);
        assert_eq!(report.decoded_records, 2);
        assert_eq!(report.applied_records, 2);
        assert_eq!(report.skipped_records, 0);
        assert_eq!(report.high_lsn, CommitVersion(12));
        assert_eq!(report.bytes, 512);
    }

    #[test]
    fn page_replay_skips_already_applied_redo_idempotently() {
        let page = PageId::new("users", 1);
        let mut image = PageImage::new(page.clone(), CommitVersion(10), 512).expect("base image");
        image
            .insert_row(b"user/1".to_vec(), b"ada".to_vec())
            .expect("insert row");
        image.lsn = CommitVersion(11);
        let encoded_image = image.encode().expect("encode image");
        let records = [
            RedoRecord::insert_row(
                page.clone(),
                CommitVersion(10),
                CommitVersion(11),
                b"user/1".to_vec(),
                b"ada".to_vec(),
            ),
            RedoRecord::update_row(
                page.clone(),
                CommitVersion(11),
                CommitVersion(12),
                b"user/1".to_vec(),
                b"grace".to_vec(),
            ),
        ]
        .into_iter()
        .map(|record| record.encode().expect("encode redo"))
        .collect::<Vec<_>>();

        let decision = PageServer::replay(&[encoded_image], &records);

        let PageDecision::Replay(report) = decision else {
            panic!("expected replay report");
        };
        assert_eq!(report.applied_records, 1);
        assert_eq!(report.skipped_records, 1);
        assert_eq!(report.high_lsn, CommitVersion(12));
    }

    #[test]
    fn page_replay_rejects_lsn_gaps() {
        let page = PageId::new("users", 1);
        let base = PageImage::new(page.clone(), CommitVersion(10), 512)
            .expect("base image")
            .encode()
            .expect("encode base");
        let record = RedoRecord::insert_row(
            page.clone(),
            CommitVersion(11),
            CommitVersion(12),
            b"user/1".to_vec(),
            b"ada".to_vec(),
        )
        .encode()
        .expect("encode redo");

        let decision = PageServer::replay(&[base], &[record]);

        let PageDecision::Rejected { reason, .. } = decision else {
            panic!("expected replay gap rejection");
        };
        assert!(reason.contains("redo replay lsn gap"));
    }

    #[test]
    fn checkpoints_page_bytes_and_high_lsn() {
        let mut pages = PageServer::default();
        let page_one = PageId::new("users", 1);
        let page_two = PageId::new("users", 2);
        pages.create_page(page_one.clone(), CommitVersion(10), 4096);
        pages.create_page(page_two.clone(), CommitVersion(14), 4096);
        assert!(matches!(
            pages.apply_redo(RedoRecord::insert_row(
                page_one.clone(),
                CommitVersion(10),
                CommitVersion(15),
                b"user/1".to_vec(),
                b"ada".to_vec(),
            )),
            PageDecision::Applied(_)
        ));
        assert!(matches!(
            pages.apply_redo(RedoRecord::insert_row(
                page_two.clone(),
                CommitVersion(14),
                CommitVersion(16),
                b"user/2".to_vec(),
                b"grace".to_vec(),
            )),
            PageDecision::Applied(_)
        ));
        assert!(matches!(
            pages.apply_redo(RedoRecord::delete_row(
                page_two.clone(),
                CommitVersion(16),
                CommitVersion(17),
                b"user/2".to_vec(),
            )),
            PageDecision::Applied(_)
        ));

        let PageDecision::Checkpoint(checkpoint) = pages.checkpoint() else {
            panic!("expected checkpoint");
        };

        assert_eq!(checkpoint.pages, 2);
        assert_eq!(checkpoint.low_lsn, CommitVersion(15));
        assert_eq!(checkpoint.high_lsn, CommitVersion(17));
        assert_eq!(checkpoint.bytes, 8192);
        assert_eq!(checkpoint.live_rows, 1);
        assert_eq!(checkpoint.tombstones, 1);
        assert_eq!(checkpoint.summary_bytes, 70);
        assert_eq!(checkpoint.page_summaries.len(), 2);
        assert_eq!(checkpoint.page_summaries[0].page, page_one);
        assert_eq!(checkpoint.page_summaries[0].lsn, CommitVersion(15));
        assert_eq!(checkpoint.page_summaries[0].live_rows, 1);
        assert_eq!(checkpoint.page_summaries[0].tombstones, 0);
        assert_eq!(checkpoint.page_summaries[1].page, page_two);
        assert_eq!(checkpoint.page_summaries[1].lsn, CommitVersion(17));
        assert_eq!(checkpoint.page_summaries[1].live_rows, 0);
        assert_eq!(checkpoint.page_summaries[1].tombstones, 1);
    }
}
