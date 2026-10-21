use super::*;
use rsf_page::{PageImage, PageMove};

impl SqlEngine {
    pub(super) fn validate_page_payload(
        &self,
        table: &str,
        key: &[u8],
        payload: &[u8],
    ) -> RsfResult<()> {
        let layout = self.layout_decision_for_table(table)?;
        if layout.layout == SqlTableLayout::OrderedKv {
            return Ok(());
        }
        let mut image = PageImage::new(PageId::new(table, 1), CommitVersion(0), layout.page_size)
            .map_err(|reason| RsfError::new(ErrorCode::InvalidInput, reason))?;
        image
            .insert_row(key.to_vec(), payload.to_vec())
            .map_err(|reason| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("SQL row exceeds page capacity: {reason}"),
                )
            })
    }

    pub(super) fn row_page(&self, key: &[u8]) -> RsfResult<PageId> {
        self.row_pages.get(key).cloned().ok_or_else(|| {
            RsfError::new(
                ErrorCode::CorruptionDetected,
                "SQL row has no physical page",
            )
        })
    }

    pub(super) fn insertion_page(
        &mut self,
        table: &str,
        key: &[u8],
        payload: &[u8],
        page_size: u32,
        exclude: Option<&PageId>,
    ) -> RsfResult<PageId> {
        self.table_record(table)?;
        if self.row_pages.contains_key(key) && exclude.is_none() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "SQL page row already exists",
            ));
        }
        let pages = self.table_pages.get(table);
        // Prefer the newest page for append workloads, then reuse available holes.
        if let Some(pages) = pages {
            for page in pages.iter().rev() {
                if Some(page) == exclude {
                    continue;
                }
                let mut image = self
                    .pages
                    .read_image(page, CommitVersion(0))
                    .map_err(|reason| RsfError::new(ErrorCode::CorruptionDetected, reason))?;
                if image.insert_row(key.to_vec(), payload.to_vec()).is_ok() {
                    return Ok(page.clone());
                }
            }
        }
        let number = pages
            .and_then(|pages| pages.last())
            .map_or(Some(1), |page| page.number.checked_add(1))
            .ok_or_else(|| {
                RsfError::new(ErrorCode::InvalidInput, "SQL table page numbers exhausted")
            })?;
        let page = PageId::new(table, number);
        let image = PageImage::new(page.clone(), CommitVersion(0), page_size)
            .map_err(|reason| RsfError::new(ErrorCode::InvalidInput, reason))?;
        let mut candidate = image.clone();
        candidate
            .insert_row(key.to_vec(), payload.to_vec())
            .map_err(|reason| {
                RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("SQL row exceeds page capacity: {reason}"),
                )
            })?;
        let encoded = image
            .encode()
            .map_err(|reason| RsfError::new(ErrorCode::CorruptionDetected, reason))?;
        match self
            .pages
            .create_page(page.clone(), CommitVersion(0), page_size)
        {
            PageDecision::Applied(_) => {}
            other => return Err(page_replay_error(other)),
        }
        self.page_wal_base_images.insert(page.clone(), encoded);
        self.table_pages
            .entry(table.to_string())
            .or_default()
            .insert(page.clone());
        Ok(page)
    }

    pub(super) fn move_page_row(
        &mut self,
        table: &str,
        commit: CommitVersion,
        key: Vec<u8>,
        payload: Vec<u8>,
        page_size: u32,
    ) -> RsfResult<PageFrame> {
        let source = self.row_page(&key)?;
        let destination = self.insertion_page(table, &key, &payload, page_size, Some(&source))?;
        let source_frame = self.read_page_frame(&source, CommitVersion(0))?;
        let destination_frame = self.read_page_frame(&destination, CommitVersion(0))?;
        let remove_lsn = self.allocate_page_lsn(commit)?;
        let insert_lsn = self.allocate_page_lsn(commit)?;
        let movement = PageMove {
            remove: RedoRecord::delete_row(
                source.clone(),
                source_frame.lsn,
                remove_lsn,
                key.clone(),
            ),
            insert: RedoRecord::insert_row(
                destination.clone(),
                destination_frame.lsn,
                insert_lsn,
                key.clone(),
                payload,
            ),
        };
        let encoded = movement
            .encode()
            .map_err(|reason| RsfError::new(ErrorCode::InvalidInput, reason))?;
        // One encoded redo unit, with WAL-before-page ordering for both frames.
        for (page, lsn, bytes) in [
            (&source, remove_lsn, 1),
            (&destination, insert_lsn, encoded.len() as u32),
        ] {
            match self.wal.append(page.clone(), lsn, bytes) {
                WalDecision::Appended(_) => {}
                other => {
                    return Err(RsfError::new(
                        ErrorCode::InvalidInput,
                        format!("SQL move WAL append rejected: {other:?}"),
                    ));
                }
            }
        }
        match self.wal.sync() {
            WalDecision::Synced { .. } => {}
            other => {
                return Err(RsfError::new(
                    ErrorCode::InvalidInput,
                    format!("SQL move WAL sync rejected: {other:?}"),
                ));
            }
        }
        expect_buffer_ok(
            self.buffer.pin(source.clone(), source_frame.bytes),
            "pin move source",
        )?;
        expect_buffer_ok(
            self.buffer
                .pin(destination.clone(), destination_frame.bytes),
            "pin move destination",
        )?;
        let frame = match self.pages.apply_move(movement) {
            PageDecision::Applied(frame) => frame,
            other => return Err(page_replay_error(other)),
        };
        self.page_wal_redo_records.push(encoded);
        for (page, lsn, bytes) in [
            (source, remove_lsn, source_frame.bytes),
            (destination.clone(), insert_lsn, destination_frame.bytes),
        ] {
            expect_buffer_ok(self.buffer.mark_dirty_at(&page, lsn), "dirty move")?;
            expect_buffer_ok(
                self.buffer
                    .flush_after_wal(FlushRequest::new(page.clone(), lsn, bytes), &self.wal),
                "flush move",
            )?;
            expect_buffer_ok(self.buffer.unpin(&page), "unpin move")?;
        }
        self.row_pages.insert(key, destination);
        Ok(frame)
    }
}
