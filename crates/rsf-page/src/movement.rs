use super::*;

const MOVE_MAGIC: &[u8; 7] = b"RSFMOVE";

/// A row relocation is one replay unit, even when only one page reached disk.
/// Its two LSNs preserve the WAL's global ordering contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageMove {
    pub remove: RedoRecord,
    pub insert: RedoRecord,
}

impl PageMove {
    fn validate(&self) -> Result<(), String> {
        self.remove.validate()?;
        self.insert.validate()?;
        if self.remove.operation != RedoOperation::DeleteRow
            || self.insert.operation != RedoOperation::InsertRow
            || self.remove.row_key != self.insert.row_key
            || self.remove.page.table != self.insert.page.table
            || self.remove.page == self.insert.page
            || self.remove.commit_lsn >= self.insert.commit_lsn
            || self.insert.base_lsn >= self.remove.commit_lsn
        {
            return Err("invalid atomic page move".to_string());
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        let mut out = MOVE_MAGIC.to_vec();
        out.push(1);
        put_bytes(&mut out, &self.remove.encode()?)?;
        put_bytes(&mut out, &self.insert.encode()?)?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        cursor.expect_magic(MOVE_MAGIC)?;
        if cursor.u8()? != 1 {
            return Err("unsupported page move format version".to_string());
        }
        let record = Self {
            remove: RedoRecord::decode(cursor.bytes()?)?,
            insert: RedoRecord::decode(cursor.bytes()?)?,
        };
        cursor.finish()?;
        record.validate()?;
        Ok(record)
    }
}

impl PageServer {
    pub fn apply_move(&mut self, record: PageMove) -> PageDecision {
        match self.apply_move_inner(&record, false) {
            Ok(_) => match self.read_page(&record.insert.page, record.insert.commit_lsn) {
                PageDecision::Read(frame) => PageDecision::Applied(frame),
                decision => decision,
            },
            Err(reason) => PageDecision::Rejected {
                page: record.remove.page,
                reason,
            },
        }
    }

    // Both images are staged before either is published. Recovery independently
    // skips flushed halves, but never exposes a partially applied move.
    pub(super) fn apply_move_inner(
        &mut self,
        record: &PageMove,
        replay: bool,
    ) -> Result<bool, String> {
        record.validate()?;
        let mut source = self.read_image(&record.remove.page, CommitVersion(0))?;
        let mut destination = self.read_image(&record.insert.page, CommitVersion(0))?;
        let source_skipped = stage(&mut source, &record.remove, replay)?;
        let destination_skipped = stage(&mut destination, &record.insert, replay)?;
        let skipped = source_skipped && destination_skipped;
        self.pages.insert(source.id.clone(), source);
        self.pages.insert(destination.id.clone(), destination);
        if !skipped {
            self.redo_records += 1;
        }
        Ok(skipped)
    }
}

fn stage(image: &mut PageImage, record: &RedoRecord, replay: bool) -> Result<bool, String> {
    if replay && image.lsn >= record.commit_lsn {
        if image.lsn == record.commit_lsn {
            let key = record.row_key.as_deref().expect("validated move key");
            let agrees = match record.operation {
                RedoOperation::DeleteRow => !image.slots.contains_key(key),
                RedoOperation::InsertRow => {
                    image.slots.get(key).map(Vec::as_slice) == record.row_payload.as_deref()
                }
                _ => false,
            };
            if !agrees {
                return Err("page move conflicts with flushed page".to_string());
            }
        }
        return Ok(true);
    }
    LsnValidation::require_exact(&image.id, record.base_lsn, image.lsn)?;
    apply_redo_to_image(image, record)?;
    image.lsn = record.commit_lsn;
    image.encode()?;
    Ok(false)
}

pub(super) enum RecoveryRecord {
    Row(RedoRecord),
    Move(PageMove),
}

impl RecoveryRecord {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.starts_with(MOVE_MAGIC) {
            PageMove::decode(bytes).map(Self::Move)
        } else {
            RedoRecord::decode(bytes).map(Self::Row)
        }
    }

    pub(super) fn order(&self) -> CommitVersion {
        match self {
            Self::Row(row) => row.commit_lsn,
            Self::Move(record) => record.insert.commit_lsn,
        }
    }

    pub(super) fn rows(&self) -> Vec<&RedoRecord> {
        match self {
            Self::Row(row) => vec![row],
            Self::Move(record) => vec![&record.remove, &record.insert],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (PageServer, PageMove) {
        let mut pages = PageServer::default();
        let source = PageId::new("users", 1);
        let destination = PageId::new("users", 2);
        pages.create_page(source.clone(), CommitVersion(0), 256);
        pages.create_page(destination.clone(), CommitVersion(0), 256);
        assert!(matches!(
            pages.apply_redo(RedoRecord::insert_row(
                source.clone(),
                CommitVersion(0),
                CommitVersion(1),
                b"key".to_vec(),
                b"old".to_vec()
            )),
            PageDecision::Applied(_)
        ));
        let record = PageMove {
            remove: RedoRecord::delete_row(
                source,
                CommitVersion(1),
                CommitVersion(2),
                b"key".to_vec(),
            ),
            insert: RedoRecord::insert_row(
                destination,
                CommitVersion(0),
                CommitVersion(3),
                b"key".to_vec(),
                b"new".to_vec(),
            ),
        };
        (pages, record)
    }

    #[test]
    fn move_recovery_handles_either_flushed_half_and_duplicate_records() {
        let (pages, record) = fixture();
        let old_source = pages
            .read_image(&record.remove.page, CommitVersion(0))
            .unwrap();
        let old_destination = pages
            .read_image(&record.insert.page, CommitVersion(0))
            .unwrap();
        let mut completed = pages.clone();
        assert!(matches!(
            completed.apply_move(record.clone()),
            PageDecision::Applied(_)
        ));
        let new_source = completed
            .read_image(&record.remove.page, CommitVersion(0))
            .unwrap();
        let new_destination = completed
            .read_image(&record.insert.page, CommitVersion(0))
            .unwrap();
        for source in [&old_source, &new_source] {
            for destination in [&old_destination, &new_destination] {
                let (recovered, _) = PageServer::recover_with_report(
                    &[source.encode().unwrap(), destination.encode().unwrap()],
                    &[record.encode().unwrap(), record.encode().unwrap()],
                )
                .unwrap();
                assert_eq!(
                    recovered
                        .read_image(&record.remove.page, CommitVersion(0))
                        .unwrap(),
                    new_source
                );
                assert_eq!(
                    recovered
                        .read_image(&record.insert.page, CommitVersion(0))
                        .unwrap(),
                    new_destination
                );
            }
        }
    }

    #[test]
    fn rejected_move_leaves_both_pages_unchanged_and_truncated_moves_never_replay() {
        let (mut pages, record) = fixture();
        let original = pages.pages.clone();
        let mut invalid = record.clone();
        invalid.insert.row_payload = Some(vec![1; 1000]);
        assert!(matches!(
            pages.apply_move(invalid),
            PageDecision::Rejected { .. }
        ));
        assert_eq!(pages.pages, original);
        let bytes = record.encode().unwrap();
        let images = original
            .values()
            .map(|page| page.encode().unwrap())
            .collect::<Vec<_>>();
        for len in 0..bytes.len() {
            assert!(PageServer::recover(&images, &[bytes[..len].to_vec()]).is_err());
        }
    }

    #[test]
    fn interleaved_moves_replay_in_global_order_and_reject_conflicting_records() {
        let (pages, record) = fixture();
        let images = pages
            .pages
            .values()
            .map(|page| page.encode().unwrap())
            .collect::<Vec<_>>();
        let returned = PageMove {
            remove: RedoRecord::delete_row(
                record.insert.page.clone(),
                CommitVersion(3),
                CommitVersion(4),
                b"key".to_vec(),
            ),
            insert: RedoRecord::insert_row(
                record.remove.page.clone(),
                CommitVersion(2),
                CommitVersion(5),
                b"key".to_vec(),
                b"returned".to_vec(),
            ),
        };
        let replay = PageServer::recover(
            &images,
            &[returned.encode().unwrap(), record.encode().unwrap()],
        )
        .unwrap();
        assert_eq!(
            replay.pages[&record.remove.page].slots[b"key".as_slice()],
            b"returned"
        );
        assert!(replay.pages[&record.insert.page].slots.is_empty());
        let mut conflict = record.clone();
        conflict.insert.row_payload = Some(b"conflict".to_vec());
        assert!(
            PageServer::recover(
                &images,
                &[record.encode().unwrap(), conflict.encode().unwrap()]
            )
            .is_err()
        );
    }
}
