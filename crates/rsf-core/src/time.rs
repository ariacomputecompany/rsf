use crate::{CommitVersion, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurableTimestamp {
    pub physical_millis: u64,
    pub logical: u32,
}

impl DurableTimestamp {
    pub const EPOCH: Self = Self {
        physical_millis: 0,
        logical: 0,
    };

    pub fn new(physical_millis: u64, logical: u32) -> Self {
        Self {
            physical_millis,
            logical,
        }
    }

    pub fn observe(self, observed: Self) -> Self {
        self.max(observed)
            .checked_next_logical()
            .unwrap_or(observed)
    }

    pub fn checked_next_logical(self) -> RsfResult<Self> {
        let logical = self.logical.checked_add(1).ok_or_else(|| {
            RsfError::new(
                ErrorCode::InvalidInput,
                "durable timestamp logical counter overflowed",
            )
        })?;
        Ok(Self {
            physical_millis: self.physical_millis,
            logical,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitClock {
    version: CommitVersion,
    timestamp: DurableTimestamp,
}

impl CommitClock {
    pub fn new(version: CommitVersion, timestamp: DurableTimestamp) -> Self {
        Self { version, timestamp }
    }

    pub fn genesis() -> Self {
        Self {
            version: CommitVersion::GENESIS,
            timestamp: DurableTimestamp::EPOCH,
        }
    }

    pub fn version(&self) -> CommitVersion {
        self.version
    }

    pub fn timestamp(&self) -> DurableTimestamp {
        self.timestamp
    }

    pub fn allocate(&mut self, observed: DurableTimestamp) -> RsfResult<CommitStamp> {
        self.version = self.version.checked_next()?;
        self.timestamp = self.timestamp.observe(observed);
        Ok(CommitStamp {
            version: self.version,
            timestamp: self.timestamp,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitStamp {
    pub version: CommitVersion,
    pub timestamp: DurableTimestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_clock_allocates_monotonic_versions_and_timestamps() {
        let mut clock = CommitClock::genesis();
        let first = clock.allocate(DurableTimestamp::new(10, 0)).expect("first");
        let second = clock
            .allocate(DurableTimestamp::new(10, 0))
            .expect("second");

        assert_eq!(first.version, CommitVersion(1));
        assert_eq!(second.version, CommitVersion(2));
        assert!(second.timestamp > first.timestamp);
    }

    #[test]
    fn observed_future_timestamp_advances_clock() {
        let mut clock = CommitClock::genesis();
        let stamp = clock
            .allocate(DurableTimestamp::new(500, 7))
            .expect("stamp");

        assert_eq!(stamp.timestamp, DurableTimestamp::new(500, 8));
    }
}
