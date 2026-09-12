use std::collections::{BTreeMap, VecDeque};

use rsf_core::CommitVersion;
use rsf_page::PageId;
use rsf_wal::{FlushRequest, WalDecision, WalGate};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferFrame {
    pub page: PageId,
    pub pins: u16,
    pub dirty: bool,
    pub bytes: u32,
    pub dirty_lsn: Option<CommitVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferPoolConfig {
    pub max_frames: usize,
    pub max_bytes: u64,
}

impl BufferPoolConfig {
    pub fn new(max_frames: usize, max_bytes: u64) -> Result<Self, String> {
        if max_frames == 0 {
            return Err("buffer frame capacity must be non-zero".to_string());
        }
        if max_bytes == 0 {
            return Err("buffer byte capacity must be non-zero".to_string());
        }
        Ok(Self {
            max_frames,
            max_bytes,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirtyFlushPolicy {
    pub max_dirty_frames: usize,
    pub max_dirty_bytes: u64,
    pub target_clean_frames: usize,
}

impl DirtyFlushPolicy {
    pub fn new(
        max_dirty_frames: usize,
        max_dirty_bytes: u64,
        target_clean_frames: usize,
    ) -> Result<Self, String> {
        if max_dirty_frames == 0 {
            return Err("dirty flush frame limit must be non-zero".to_string());
        }
        if max_dirty_bytes == 0 {
            return Err("dirty flush byte limit must be non-zero".to_string());
        }
        if target_clean_frames == 0 {
            return Err("dirty flush target must be non-zero".to_string());
        }
        Ok(Self {
            max_dirty_frames,
            max_dirty_bytes,
            target_clean_frames,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirtyFlushPlan {
    pub dirty_frames: usize,
    pub dirty_bytes: u64,
    pub candidates: Vec<FlushRequest>,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BufferDecision {
    Hit(BufferFrame),
    Loaded(BufferFrame),
    Dirtied(BufferFrame),
    Unpinned(BufferFrame),
    Flushed { page: PageId, bytes: u32 },
    Evicted { page: PageId, dirty: bool },
    Rejected { page: PageId, reason: String },
}

#[derive(Clone, Debug)]
pub struct BufferPool {
    config: BufferPoolConfig,
    frames: BTreeMap<PageId, BufferFrame>,
    lru: VecDeque<PageId>,
    resident_bytes: u64,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Result<Self, String> {
        Self::with_config(BufferPoolConfig::new(capacity, u64::MAX)?)
    }

    pub fn with_config(config: BufferPoolConfig) -> Result<Self, String> {
        BufferPoolConfig::new(config.max_frames, config.max_bytes)?;
        Ok(Self {
            config,
            frames: BTreeMap::new(),
            lru: VecDeque::new(),
            resident_bytes: 0,
        })
    }

    pub fn pin(&mut self, page: PageId, bytes: u32) -> BufferDecision {
        if page.table.is_empty() {
            return BufferDecision::Rejected {
                page,
                reason: "page table must not be empty".to_string(),
            };
        }
        if bytes == 0 {
            return BufferDecision::Rejected {
                page,
                reason: "page bytes must be non-zero".to_string(),
            };
        }
        if u64::from(bytes) > self.config.max_bytes {
            return BufferDecision::Rejected {
                page,
                reason: "page exceeds buffer byte capacity".to_string(),
            };
        }
        if let Some(frame) = self.frames.get_mut(&page) {
            if frame.bytes != bytes {
                return BufferDecision::Rejected {
                    page,
                    reason: format!(
                        "resident page bytes {} do not match requested bytes {}",
                        frame.bytes, bytes
                    ),
                };
            }
            let Some(pins) = frame.pins.checked_add(1) else {
                return BufferDecision::Rejected {
                    page,
                    reason: "page pin count overflow".to_string(),
                };
            };
            frame.pins = pins;
            let hit = frame.clone();
            self.touch(&page);
            return BufferDecision::Hit(hit);
        }
        while self.frames.len() == self.config.max_frames
            || self.resident_bytes.saturating_add(u64::from(bytes)) > self.config.max_bytes
        {
            let Some(victim) = self.next_clean_evictable() else {
                return BufferDecision::Rejected {
                    page,
                    reason: "no clean unpinned frame available within buffer capacity".to_string(),
                };
            };
            self.frames.remove(&victim.page);
            self.lru.retain(|existing| existing != &victim.page);
            self.resident_bytes = self.resident_bytes.saturating_sub(u64::from(victim.bytes));
        }
        let frame = BufferFrame {
            page: page.clone(),
            pins: 1,
            dirty: false,
            bytes,
            dirty_lsn: None,
        };
        self.frames.insert(page.clone(), frame.clone());
        self.resident_bytes = self.resident_bytes.saturating_add(u64::from(bytes));
        self.touch(&page);
        BufferDecision::Loaded(frame)
    }

    pub fn mark_dirty(&mut self, page: &PageId) -> BufferDecision {
        self.mark_dirty_at(page, CommitVersion(0))
    }

    pub fn mark_dirty_at(&mut self, page: &PageId, lsn: CommitVersion) -> BufferDecision {
        let Some(frame) = self.frames.get_mut(page) else {
            return BufferDecision::Rejected {
                page: page.clone(),
                reason: "page not resident".to_string(),
            };
        };
        frame.dirty = true;
        frame.dirty_lsn = Some(lsn);
        BufferDecision::Dirtied(frame.clone())
    }

    pub fn unpin(&mut self, page: &PageId) -> BufferDecision {
        let Some(frame) = self.frames.get_mut(page) else {
            return BufferDecision::Rejected {
                page: page.clone(),
                reason: "page not resident".to_string(),
            };
        };
        if frame.pins == 0 {
            return BufferDecision::Rejected {
                page: page.clone(),
                reason: "page is already unpinned".to_string(),
            };
        }
        frame.pins -= 1;
        BufferDecision::Unpinned(frame.clone())
    }

    pub fn flush_after_wal(&mut self, request: FlushRequest, wal: &WalGate) -> BufferDecision {
        let page = request.page.clone();
        let Some(frame) = self.frames.get(&page) else {
            return BufferDecision::Rejected {
                page,
                reason: "page not resident".to_string(),
            };
        };
        if !frame.dirty {
            return BufferDecision::Rejected {
                page,
                reason: "page is not dirty".to_string(),
            };
        }
        if frame.dirty_lsn != Some(request.page_lsn) {
            return BufferDecision::Rejected {
                page,
                reason: format!(
                    "flush request lsn {} does not match dirty frame lsn {}",
                    request.page_lsn.0,
                    frame.dirty_lsn.map_or(0, |lsn| lsn.0)
                ),
            };
        }
        if frame.bytes != request.bytes {
            return BufferDecision::Rejected {
                page,
                reason: format!(
                    "flush request bytes {} do not match resident frame bytes {}",
                    request.bytes, frame.bytes
                ),
            };
        }
        match wal.allow_page_flush(request) {
            WalDecision::FlushAllowed { .. } => self.flush(&page),
            WalDecision::Rejected { reason, .. } => BufferDecision::Rejected {
                page,
                reason: format!("wal gate rejected page flush: {reason}"),
            },
            other => BufferDecision::Rejected {
                page,
                reason: format!("unexpected wal decision during page flush {other:?}"),
            },
        }
    }

    fn flush(&mut self, page: &PageId) -> BufferDecision {
        let Some(frame) = self.frames.get_mut(page) else {
            return BufferDecision::Rejected {
                page: page.clone(),
                reason: "page not resident".to_string(),
            };
        };
        frame.dirty = false;
        frame.dirty_lsn = None;
        BufferDecision::Flushed {
            page: page.clone(),
            bytes: frame.bytes,
        }
    }

    pub fn evict_one(&mut self) -> BufferDecision {
        let Some(victim) = self.next_clean_evictable() else {
            return BufferDecision::Rejected {
                page: PageId::new("buffer", 0),
                reason: "no clean unpinned frame available".to_string(),
            };
        };
        self.frames.remove(&victim.page);
        self.lru.retain(|existing| existing != &victim.page);
        self.resident_bytes = self.resident_bytes.saturating_sub(u64::from(victim.bytes));
        BufferDecision::Evicted {
            page: victim.page,
            dirty: victim.dirty,
        }
    }

    pub fn resident_count(&self) -> usize {
        self.frames.len()
    }

    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    pub fn frame(&self, page: &PageId) -> Option<&BufferFrame> {
        self.frames.get(page)
    }

    pub fn dirty_flush_plan(&self, policy: &DirtyFlushPolicy) -> DirtyFlushPlan {
        let dirty_frames = self.frames.values().filter(|frame| frame.dirty).count();
        let dirty_bytes = self
            .frames
            .values()
            .filter(|frame| frame.dirty)
            .map(|frame| u64::from(frame.bytes))
            .sum();

        if dirty_frames <= policy.max_dirty_frames && dirty_bytes <= policy.max_dirty_bytes {
            return DirtyFlushPlan {
                dirty_frames,
                dirty_bytes,
                candidates: Vec::new(),
                reason: "dirty set is within policy".to_string(),
            };
        }

        let mut candidates = Vec::new();
        for page in &self.lru {
            let Some(frame) = self.frames.get(page) else {
                continue;
            };
            if !frame.dirty || frame.pins != 0 {
                continue;
            }
            let Some(lsn) = frame.dirty_lsn else {
                continue;
            };
            candidates.push(FlushRequest::new(frame.page.clone(), lsn, frame.bytes));
            if candidates.len() >= policy.target_clean_frames {
                break;
            }
        }

        DirtyFlushPlan {
            dirty_frames,
            dirty_bytes,
            candidates,
            reason: "dirty set exceeds policy".to_string(),
        }
    }

    fn next_clean_evictable(&self) -> Option<BufferFrame> {
        self.lru
            .iter()
            .filter_map(|page| self.frames.get(page))
            .find(|frame| frame.pins == 0 && !frame.dirty)
            .cloned()
    }

    fn touch(&mut self, page: &PageId) {
        self.lru.retain(|existing| existing != page);
        self.lru.push_back(page.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::CommitVersion;
    use rsf_wal::{FlushRequest, WalGate, WalPolicy};

    #[test]
    fn loads_hits_dirties_flushes_and_evicts() {
        let mut pool = BufferPool::new(1).unwrap();
        let mut wal = WalGate::new(WalPolicy::new(1_024, true).unwrap());
        let page = PageId::new("users", 1);

        assert!(matches!(
            wal.append(page.clone(), CommitVersion(31), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(wal.sync(), WalDecision::Synced { .. }));
        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Hit(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&page, CommitVersion(31)),
            BufferDecision::Dirtied(_)
        ));
        pool.unpin(&page);
        pool.unpin(&page);
        assert!(matches!(
            pool.flush_after_wal(
                FlushRequest::new(page.clone(), CommitVersion(31), 4096),
                &wal
            ),
            BufferDecision::Flushed { bytes: 4096, .. }
        ));
        assert!(matches!(pool.evict_one(), BufferDecision::Evicted { .. }));
    }

    #[test]
    fn rejects_when_all_frames_are_pinned() {
        let mut pool = BufferPool::new(1).unwrap();
        pool.pin(PageId::new("users", 1), 4096);

        let decision = pool.pin(PageId::new("users", 2), 4096);

        assert!(matches!(decision, BufferDecision::Rejected { .. }));
        assert_eq!(pool.resident_count(), 1);
    }

    #[test]
    fn evicts_clean_lru_frame_to_satisfy_frame_and_byte_capacity() {
        let mut pool = BufferPool::with_config(BufferPoolConfig::new(2, 6_144).unwrap()).unwrap();
        let users = PageId::new("users", 1);
        let orders = PageId::new("orders", 1);
        let invoices = PageId::new("invoices", 1);

        assert!(matches!(
            pool.pin(users.clone(), 2_048),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(pool.unpin(&users), BufferDecision::Unpinned(_)));
        assert!(matches!(
            pool.pin(orders.clone(), 2_048),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(pool.unpin(&orders), BufferDecision::Unpinned(_)));

        assert!(matches!(
            pool.pin(invoices.clone(), 4_096),
            BufferDecision::Loaded(_)
        ));

        assert!(pool.frame(&users).is_none());
        assert!(pool.frame(&orders).is_some());
        assert!(pool.frame(&invoices).is_some());
        assert_eq!(pool.resident_count(), 2);
        assert_eq!(pool.resident_bytes(), 6_144);
    }

    #[test]
    fn refuses_to_evict_dirty_or_pinned_frames() {
        let mut pool = BufferPool::new(1).unwrap();
        let users = PageId::new("users", 1);
        let orders = PageId::new("orders", 1);

        assert!(matches!(
            pool.pin(users.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&users, CommitVersion(31)),
            BufferDecision::Dirtied(_)
        ));
        assert!(matches!(pool.unpin(&users), BufferDecision::Unpinned(_)));

        let decision = pool.pin(orders, 4096);
        let BufferDecision::Rejected { reason, .. } = decision else {
            panic!("expected dirty victim rejection");
        };
        assert!(reason.contains("no clean unpinned frame"));
        assert!(pool.frame(&users).is_some());
    }

    #[test]
    fn rejects_resident_page_byte_mismatch() {
        let mut pool = BufferPool::new(1).unwrap();
        let page = PageId::new("users", 1);

        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        let decision = pool.pin(page, 2048);

        let BufferDecision::Rejected { reason, .. } = decision else {
            panic!("expected resident byte mismatch");
        };
        assert!(reason.contains("resident page bytes"));
    }

    #[test]
    fn rejects_page_shapes_that_cannot_fit_the_pool() {
        let mut pool = BufferPool::with_config(BufferPoolConfig::new(1, 2_048).unwrap()).unwrap();
        let page = PageId::new("users", 1);

        let decision = pool.pin(page, 4_096);

        let BufferDecision::Rejected { reason, .. } = decision else {
            panic!("expected page capacity rejection");
        };
        assert!(reason.contains("page exceeds buffer byte capacity"));
    }

    #[test]
    fn rejects_dirty_flush_without_durable_wal_approval() {
        let mut pool = BufferPool::new(1).unwrap();
        let mut wal = WalGate::new(WalPolicy::new(1_024, true).unwrap());
        let page = PageId::new("users", 1);

        assert!(matches!(
            wal.append(page.clone(), CommitVersion(31), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&page, CommitVersion(31)),
            BufferDecision::Dirtied(_)
        ));

        let decision = pool.flush_after_wal(FlushRequest::new(page, CommitVersion(31), 4096), &wal);

        let BufferDecision::Rejected { reason, .. } = decision else {
            panic!("expected wal-before-page-flush rejection");
        };
        assert!(reason.contains("wal gate rejected page flush"));
    }

    #[test]
    fn rejects_flush_when_wal_request_does_not_match_frame() {
        let mut pool = BufferPool::new(1).unwrap();
        let mut wal = WalGate::new(WalPolicy::new(1_024, true).unwrap());
        let page = PageId::new("users", 1);

        assert!(matches!(
            wal.append(page.clone(), CommitVersion(31), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(wal.sync(), WalDecision::Synced { .. }));
        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&page, CommitVersion(31)),
            BufferDecision::Dirtied(_)
        ));

        let decision = pool.flush_after_wal(FlushRequest::new(page, CommitVersion(31), 2048), &wal);

        let BufferDecision::Rejected { reason, .. } = decision else {
            panic!("expected mismatched flush request rejection");
        };
        assert!(reason.contains("do not match resident frame bytes"));
    }

    #[test]
    fn rejects_flush_when_request_lsn_does_not_match_dirty_frame_lsn() {
        let mut pool = BufferPool::new(1).unwrap();
        let mut wal = WalGate::new(WalPolicy::new(1_024, true).unwrap());
        let page = PageId::new("users", 1);

        assert!(matches!(
            wal.append(page.clone(), CommitVersion(31), 128),
            WalDecision::Appended(_)
        ));
        assert!(matches!(wal.sync(), WalDecision::Synced { .. }));
        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&page, CommitVersion(30)),
            BufferDecision::Dirtied(_)
        ));

        let decision = pool.flush_after_wal(FlushRequest::new(page, CommitVersion(31), 4096), &wal);

        let BufferDecision::Rejected { reason, .. } = decision else {
            panic!("expected dirty lsn mismatch rejection");
        };
        assert!(reason.contains("does not match dirty frame lsn"));
    }

    #[test]
    fn dirty_flush_policy_plans_oldest_unpinned_dirty_frames() {
        let mut pool = BufferPool::with_config(BufferPoolConfig::new(3, 12_288).unwrap()).unwrap();
        let users = PageId::new("users", 1);
        let orders = PageId::new("orders", 1);
        let invoices = PageId::new("invoices", 1);
        let policy = DirtyFlushPolicy::new(1, 4_096, 2).unwrap();

        assert!(matches!(
            pool.pin(users.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&users, CommitVersion(31)),
            BufferDecision::Dirtied(_)
        ));
        assert!(matches!(pool.unpin(&users), BufferDecision::Unpinned(_)));

        assert!(matches!(
            pool.pin(orders.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&orders, CommitVersion(32)),
            BufferDecision::Dirtied(_)
        ));
        assert!(matches!(pool.unpin(&orders), BufferDecision::Unpinned(_)));

        assert!(matches!(
            pool.pin(invoices.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&invoices, CommitVersion(33)),
            BufferDecision::Dirtied(_)
        ));

        let plan = pool.dirty_flush_plan(&policy);

        assert_eq!(plan.dirty_frames, 3);
        assert_eq!(plan.dirty_bytes, 12_288);
        assert_eq!(plan.reason, "dirty set exceeds policy");
        assert_eq!(
            plan.candidates,
            vec![
                FlushRequest::new(users, CommitVersion(31), 4096),
                FlushRequest::new(orders, CommitVersion(32), 4096)
            ]
        );
    }

    #[test]
    fn dirty_flush_policy_stays_idle_when_under_threshold() {
        let mut pool = BufferPool::new(2).unwrap();
        let page = PageId::new("users", 1);
        let policy = DirtyFlushPolicy::new(2, 8_192, 1).unwrap();

        assert!(matches!(
            pool.pin(page.clone(), 4096),
            BufferDecision::Loaded(_)
        ));
        assert!(matches!(
            pool.mark_dirty_at(&page, CommitVersion(31)),
            BufferDecision::Dirtied(_)
        ));
        assert!(matches!(pool.unpin(&page), BufferDecision::Unpinned(_)));

        let plan = pool.dirty_flush_plan(&policy);

        assert_eq!(plan.dirty_frames, 1);
        assert_eq!(plan.dirty_bytes, 4096);
        assert!(plan.candidates.is_empty());
        assert_eq!(plan.reason, "dirty set is within policy");
    }
}
