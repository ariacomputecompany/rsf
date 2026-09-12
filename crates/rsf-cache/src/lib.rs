#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{CommitVersion, ErrorCode, RsfError, RsfResult};

mod ledger;

pub use ledger::DurableReadCache;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CacheSurface {
    Sql,
    Kv,
    Object,
    Blob,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CacheKey {
    pub surface: CacheSurface,
    pub namespace: String,
    pub key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry {
    pub key: CacheKey,
    pub version: CommitVersion,
    pub bytes: u64,
    pub cost: u8,
    pub pinned: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachePolicy {
    pub capacity_bytes: u64,
    pub max_entry_bytes: u64,
    pub max_read_ahead: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheHit {
    pub key: CacheKey,
    pub version: CommitVersion,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheRead {
    Hit(CacheHit),
    Miss {
        key: CacheKey,
        reason: String,
    },
    Stale {
        key: CacheKey,
        cached_version: CommitVersion,
        required_version: CommitVersion,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrictCacheRead {
    Serve(CacheHit),
    ReadThrough {
        key: CacheKey,
        required_version: CommitVersion,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CacheAdmission {
    Stored {
        key: CacheKey,
        used_bytes: u64,
        evicted: Vec<CacheKey>,
    },
    Rejected {
        key: CacheKey,
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAheadPlan {
    pub anchor: CacheKey,
    pub required_version: CommitVersion,
    pub hints: Vec<CacheKey>,
    pub prefetches: Vec<ReadAheadPrefetch>,
    pub skipped: Vec<ReadAheadSkip>,
    pub planned_bytes: u64,
    pub evicted: Vec<CacheKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAheadCandidate {
    pub key: CacheKey,
    pub version: CommitVersion,
    pub bytes: u64,
    pub cost: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAheadPrefetch {
    pub key: CacheKey,
    pub version: CommitVersion,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadAheadSkip {
    pub key: CacheKey,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct ReadCache {
    policy: CachePolicy,
    entries: BTreeMap<CacheKey, StoredEntry>,
    clock: u64,
    used_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredEntry {
    entry: CacheEntry,
    last_used: u64,
}

impl ReadCache {
    pub fn new(policy: CachePolicy) -> RsfResult<Self> {
        validate_policy(&policy)?;
        Ok(Self {
            policy,
            entries: BTreeMap::new(),
            clock: 0,
            used_bytes: 0,
        })
    }

    pub fn put(&mut self, entry: CacheEntry) -> RsfResult<CacheAdmission> {
        validate_entry(&entry, &self.policy)?;

        if entry.bytes > self.policy.capacity_bytes {
            return Ok(CacheAdmission::Rejected {
                key: entry.key,
                reason: "entry exceeds cache capacity".to_string(),
            });
        }

        if let Some(existing) = self.entries.remove(&entry.key) {
            self.used_bytes -= existing.entry.bytes;
        }

        let mut evicted = Vec::new();
        while self.used_bytes + entry.bytes > self.policy.capacity_bytes {
            let Some(key) = self.evictable_key() else {
                return Ok(CacheAdmission::Rejected {
                    key: entry.key,
                    reason: "capacity held by pinned entries".to_string(),
                });
            };
            let removed = self
                .entries
                .remove(&key)
                .expect("evictable key must still exist");
            self.used_bytes -= removed.entry.bytes;
            evicted.push(key);
        }

        self.clock += 1;
        self.used_bytes += entry.bytes;
        let key = entry.key.clone();
        self.entries.insert(
            key.clone(),
            StoredEntry {
                entry,
                last_used: self.clock,
            },
        );

        Ok(CacheAdmission::Stored {
            key,
            used_bytes: self.used_bytes,
            evicted,
        })
    }

    pub fn read(&mut self, key: &CacheKey, min_version: CommitVersion) -> CacheRead {
        let Some(stored) = self.entries.get(key) else {
            return CacheRead::Miss {
                key: key.clone(),
                reason: "not cached".to_string(),
            };
        };

        if stored.entry.version < min_version {
            let cached_version = stored.entry.version;
            let removed = self
                .entries
                .remove(key)
                .expect("stale cache entry must still exist");
            self.used_bytes -= removed.entry.bytes;
            return CacheRead::Stale {
                key: key.clone(),
                cached_version,
                required_version: min_version,
            };
        }

        let stored = self
            .entries
            .get_mut(key)
            .expect("cache hit entry must still exist");
        self.clock += 1;
        stored.last_used = self.clock;
        CacheRead::Hit(CacheHit {
            key: stored.entry.key.clone(),
            version: stored.entry.version,
            bytes: stored.entry.bytes,
        })
    }

    pub fn invalidate(&mut self, key: &CacheKey) -> Option<CacheEntry> {
        let removed = self.entries.remove(key)?;
        self.used_bytes -= removed.entry.bytes;
        Some(removed.entry)
    }

    pub fn strict_read(
        &mut self,
        key: &CacheKey,
        required_version: CommitVersion,
    ) -> StrictCacheRead {
        match self.read(key, required_version) {
            CacheRead::Hit(hit) => StrictCacheRead::Serve(hit),
            CacheRead::Miss { key, reason } => StrictCacheRead::ReadThrough {
                key,
                required_version,
                reason,
            },
            CacheRead::Stale {
                key,
                cached_version,
                required_version,
            } => StrictCacheRead::ReadThrough {
                key,
                required_version,
                reason: format!(
                    "cached version {} is older than required {}",
                    cached_version.0, required_version.0
                ),
            },
        }
    }

    pub fn read_ahead(
        &mut self,
        anchor: CacheKey,
        required_version: CommitVersion,
        candidates: impl IntoIterator<Item = ReadAheadCandidate>,
    ) -> RsfResult<ReadAheadPlan> {
        validate_key(&anchor)?;
        let mut planned_bytes = 0;
        let mut prefetches = Vec::new();
        let mut skipped = Vec::new();
        let mut evicted = Vec::new();

        for candidate in candidates {
            validate_candidate(&candidate, &self.policy)?;
            if candidate.key == anchor {
                skipped.push(ReadAheadSkip {
                    key: candidate.key,
                    reason: "anchor is already being read".to_string(),
                });
                continue;
            }
            if candidate.key.surface != anchor.surface
                || candidate.key.namespace != anchor.namespace
            {
                skipped.push(ReadAheadSkip {
                    key: candidate.key,
                    reason: "candidate is outside anchor surface or namespace".to_string(),
                });
                continue;
            }
            if prefetches.len() >= self.policy.max_read_ahead {
                skipped.push(ReadAheadSkip {
                    key: candidate.key,
                    reason: "read-ahead limit reached".to_string(),
                });
                continue;
            }
            if candidate.version < required_version {
                skipped.push(ReadAheadSkip {
                    key: candidate.key,
                    reason: format!(
                        "candidate version {} is older than required {}",
                        candidate.version.0, required_version.0
                    ),
                });
                continue;
            }
            if let Some(stored) = self.entries.get(&candidate.key) {
                if stored.entry.version >= required_version {
                    skipped.push(ReadAheadSkip {
                        key: candidate.key,
                        reason: "already cached at required version".to_string(),
                    });
                    continue;
                }
                let removed = self
                    .entries
                    .remove(&candidate.key)
                    .expect("stale read-ahead candidate must still exist");
                self.used_bytes -= removed.entry.bytes;
                evicted.push(candidate.key.clone());
            }
            if planned_bytes + candidate.bytes > self.remaining_prefetch_capacity() {
                skipped.push(ReadAheadSkip {
                    key: candidate.key,
                    reason: "read-ahead capacity budget exhausted".to_string(),
                });
                continue;
            }
            planned_bytes += candidate.bytes;
            prefetches.push(ReadAheadPrefetch {
                key: candidate.key,
                version: candidate.version,
                bytes: candidate.bytes,
            });
        }

        let hints = prefetches
            .iter()
            .map(|prefetch| prefetch.key.clone())
            .collect::<Vec<_>>();
        Ok(ReadAheadPlan {
            anchor,
            required_version,
            hints,
            prefetches,
            skipped,
            planned_bytes,
            evicted,
        })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn remaining_prefetch_capacity(&self) -> u64 {
        self.policy.capacity_bytes.saturating_sub(self.used_bytes)
    }

    fn evictable_key(&self) -> Option<CacheKey> {
        self.entries
            .iter()
            .filter(|(_, stored)| !stored.entry.pinned)
            .min_by_key(|(key, stored)| (stored.last_used, stored.entry.cost, (*key).clone()))
            .map(|(key, _)| key.clone())
    }
}

impl CachePolicy {
    pub fn bounded(capacity_bytes: u64, max_entry_bytes: u64, max_read_ahead: usize) -> Self {
        Self {
            capacity_bytes,
            max_entry_bytes,
            max_read_ahead,
        }
    }
}

impl CacheKey {
    pub fn new(
        surface: CacheSurface,
        namespace: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            surface,
            namespace: namespace.into(),
            key: key.into(),
        }
    }
}

impl ReadAheadCandidate {
    pub fn new(key: CacheKey, version: CommitVersion, bytes: u64, cost: u8) -> Self {
        Self {
            key,
            version,
            bytes,
            cost,
        }
    }
}

impl CacheSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Kv => "kv",
            Self::Object => "object",
            Self::Blob => "blob",
        }
    }
}

impl CacheRead {
    pub fn is_hit(&self) -> bool {
        matches!(self, Self::Hit(_))
    }

    pub fn is_stale(&self) -> bool {
        matches!(self, Self::Stale { .. })
    }
}

impl CacheAdmission {
    pub fn is_stored(&self) -> bool {
        matches!(self, Self::Stored { .. })
    }
}

fn validate_policy(policy: &CachePolicy) -> RsfResult<()> {
    if policy.capacity_bytes == 0 {
        return Err(invalid("cache capacity must be greater than zero"));
    }
    if policy.max_entry_bytes == 0 {
        return Err(invalid("cache max entry bytes must be greater than zero"));
    }
    if policy.max_entry_bytes > policy.capacity_bytes {
        return Err(invalid("cache max entry bytes must fit capacity"));
    }
    if policy.max_read_ahead == 0 {
        return Err(invalid("cache read-ahead limit must be greater than zero"));
    }
    Ok(())
}

fn validate_entry(entry: &CacheEntry, policy: &CachePolicy) -> RsfResult<()> {
    validate_key(&entry.key)?;
    if entry.bytes == 0 {
        return Err(invalid("cache entry bytes must be greater than zero"));
    }
    if entry.bytes > policy.max_entry_bytes {
        return Err(invalid("cache entry exceeds max entry bytes"));
    }
    Ok(())
}

fn validate_candidate(candidate: &ReadAheadCandidate, policy: &CachePolicy) -> RsfResult<()> {
    validate_key(&candidate.key)?;
    if candidate.bytes == 0 {
        return Err(invalid(
            "read-ahead candidate bytes must be greater than zero",
        ));
    }
    if candidate.bytes > policy.max_entry_bytes {
        return Err(invalid("read-ahead candidate exceeds max entry bytes"));
    }
    Ok(())
}

fn validate_key(key: &CacheKey) -> RsfResult<()> {
    if key.namespace.trim().is_empty() {
        return Err(invalid("cache namespace must not be empty"));
    }
    if key.key.trim().is_empty() {
        return Err(invalid("cache key must not be empty"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_respects_minimum_visible_version() {
        let mut cache = ReadCache::new(CachePolicy::bounded(1024, 512, 2)).expect("cache");
        let key = CacheKey::new(CacheSurface::Sql, "app", "users/1");
        cache.put(entry(key.clone(), 8, 128, false)).expect("put");

        assert!(cache.read(&key, CommitVersion(8)).is_hit());
        assert_eq!(
            cache.read(&key, CommitVersion(9)),
            CacheRead::Stale {
                key: key.clone(),
                cached_version: CommitVersion(8),
                required_version: CommitVersion(9),
            }
        );
        assert_eq!(
            cache.read(&key, CommitVersion(8)),
            CacheRead::Miss {
                key,
                reason: "not cached".to_string()
            }
        );
        assert_eq!(cache.used_bytes(), 0);
    }

    #[test]
    fn strict_read_fails_closed_for_stale_cache_entries() {
        let mut cache = ReadCache::new(CachePolicy::bounded(1024, 512, 2)).expect("cache");
        let key = CacheKey::new(CacheSurface::Sql, "app", "users/1");
        cache.put(entry(key.clone(), 8, 128, false)).expect("put");

        assert_eq!(
            cache.strict_read(&key, CommitVersion(9)),
            StrictCacheRead::ReadThrough {
                key: key.clone(),
                required_version: CommitVersion(9),
                reason: "cached version 8 is older than required 9".to_string()
            }
        );
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(
            cache.strict_read(&key, CommitVersion(8)),
            StrictCacheRead::ReadThrough {
                key,
                required_version: CommitVersion(8),
                reason: "not cached".to_string()
            }
        );
    }

    #[test]
    fn admission_evicts_least_recent_unpinned_entry() {
        let mut cache = ReadCache::new(CachePolicy::bounded(300, 200, 2)).expect("cache");
        let sql = CacheKey::new(CacheSurface::Sql, "app", "users/1");
        let kv = CacheKey::new(CacheSurface::Kv, "app", "config/a");
        let object = CacheKey::new(CacheSurface::Object, "app", "media/hero.png");
        cache.put(entry(sql.clone(), 4, 100, false)).expect("put");
        cache.put(entry(kv.clone(), 5, 100, false)).expect("put");
        assert!(cache.read(&sql, CommitVersion(4)).is_hit());

        let admission = cache
            .put(entry(object.clone(), 6, 200, false))
            .expect("put");

        assert_eq!(
            admission,
            CacheAdmission::Stored {
                key: object,
                used_bytes: 300,
                evicted: vec![kv.clone()]
            }
        );
        assert!(!cache.read(&kv, CommitVersion(5)).is_hit());
    }

    #[test]
    fn pinned_entries_prevent_capacity_eviction() {
        let mut cache = ReadCache::new(CachePolicy::bounded(300, 200, 2)).expect("cache");
        let pinned = CacheKey::new(CacheSurface::Blob, "app", "digest-a");
        let next = CacheKey::new(CacheSurface::Blob, "app", "digest-b");
        cache.put(entry(pinned.clone(), 1, 200, true)).expect("put");

        let admission = cache.put(entry(next.clone(), 2, 200, false)).expect("put");

        assert_eq!(
            admission,
            CacheAdmission::Rejected {
                key: next,
                reason: "capacity held by pinned entries".to_string()
            }
        );
        assert!(cache.read(&pinned, CommitVersion(1)).is_hit());
    }

    #[test]
    fn read_ahead_limits_candidates_to_same_surface_and_namespace() {
        let mut cache = ReadCache::new(CachePolicy::bounded(1024, 512, 2)).expect("cache");
        let anchor = CacheKey::new(CacheSurface::Object, "app", "media/hero.png");
        let plan = cache
            .read_ahead(
                anchor.clone(),
                CommitVersion(4),
                [
                    candidate(CacheSurface::Object, "app", "media/hero.1", 4, 100),
                    candidate(CacheSurface::Kv, "app", "config/a", 4, 100),
                    candidate(CacheSurface::Object, "audit", "media/hero.2", 4, 100),
                    candidate(CacheSurface::Object, "app", "media/hero.3", 4, 100),
                    candidate(CacheSurface::Object, "app", "media/hero.4", 4, 100),
                ],
            )
            .expect("read ahead");

        assert_eq!(plan.anchor, anchor);
        assert_eq!(plan.required_version, CommitVersion(4));
        assert_eq!(plan.hints.len(), 2);
        assert_eq!(plan.hints[0].key, "media/hero.1");
        assert_eq!(plan.hints[1].key, "media/hero.3");
        assert_eq!(plan.prefetches.len(), 2);
        assert_eq!(plan.planned_bytes, 200);
        assert_eq!(plan.skipped.len(), 3);
        assert_eq!(
            plan.skipped[0].reason,
            "candidate is outside anchor surface or namespace"
        );
        assert_eq!(
            plan.skipped[1].reason,
            "candidate is outside anchor surface or namespace"
        );
        assert_eq!(plan.skipped[2].reason, "read-ahead limit reached");
    }

    #[test]
    fn read_ahead_skips_fresh_and_replans_stale_cached_entries() {
        let mut cache = ReadCache::new(CachePolicy::bounded(1024, 512, 3)).expect("cache");
        let anchor = CacheKey::new(CacheSurface::Object, "app", "media/hero.png");
        let fresh = CacheKey::new(CacheSurface::Object, "app", "media/fresh");
        let stale = CacheKey::new(CacheSurface::Object, "app", "media/stale");
        cache
            .put(CacheEntry {
                key: fresh.clone(),
                version: CommitVersion(6),
                bytes: 100,
                cost: 1,
                pinned: false,
            })
            .expect("fresh put");
        cache
            .put(CacheEntry {
                key: stale.clone(),
                version: CommitVersion(4),
                bytes: 100,
                cost: 1,
                pinned: false,
            })
            .expect("stale put");

        let plan = cache
            .read_ahead(
                anchor,
                CommitVersion(5),
                [
                    ReadAheadCandidate::new(fresh.clone(), CommitVersion(6), 100, 1),
                    ReadAheadCandidate::new(stale.clone(), CommitVersion(6), 100, 1),
                ],
            )
            .expect("read ahead");

        assert_eq!(plan.hints, vec![stale.clone()]);
        assert_eq!(plan.evicted, vec![stale.clone()]);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].key, fresh);
        assert_eq!(plan.skipped[0].reason, "already cached at required version");
        assert_eq!(cache.used_bytes(), 100);
        assert_eq!(
            cache.read(&stale, CommitVersion(5)),
            CacheRead::Miss {
                key: stale,
                reason: "not cached".to_string()
            }
        );
    }

    #[test]
    fn read_ahead_stays_inside_remaining_capacity_budget() {
        let mut cache = ReadCache::new(CachePolicy::bounded(300, 256, 3)).expect("cache");
        cache
            .put(entry(
                CacheKey::new(CacheSurface::Object, "app", "resident"),
                7,
                220,
                true,
            ))
            .expect("resident put");
        let plan = cache
            .read_ahead(
                CacheKey::new(CacheSurface::Object, "app", "anchor"),
                CommitVersion(7),
                [
                    candidate(CacheSurface::Object, "app", "small", 7, 80),
                    candidate(CacheSurface::Object, "app", "large", 7, 80),
                ],
            )
            .expect("read ahead");

        assert_eq!(plan.hints.len(), 1);
        assert_eq!(plan.hints[0].key, "small");
        assert_eq!(plan.planned_bytes, 80);
        assert_eq!(plan.skipped.len(), 1);
        assert_eq!(plan.skipped[0].key.key, "large");
        assert_eq!(
            plan.skipped[0].reason,
            "read-ahead capacity budget exhausted"
        );
    }

    fn entry(key: CacheKey, version: u64, bytes: u64, pinned: bool) -> CacheEntry {
        CacheEntry {
            key,
            version: CommitVersion(version),
            bytes,
            cost: 1,
            pinned,
        }
    }

    fn candidate(
        surface: CacheSurface,
        namespace: &'static str,
        key: &'static str,
        version: u64,
        bytes: u64,
    ) -> ReadAheadCandidate {
        ReadAheadCandidate::new(
            CacheKey::new(surface, namespace, key),
            CommitVersion(version),
            bytes,
            1,
        )
    }
}
