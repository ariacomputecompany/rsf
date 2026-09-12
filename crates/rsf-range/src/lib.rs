use std::collections::BTreeMap;

use rsf_core::{
    Bytes, CommitVersion, Digest, Epoch, ErrorCode, PlacementGroupId, RsfError, RsfResult,
};

mod ledger;

pub use ledger::DurableRangeLayout;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RangeId(pub u64);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRange {
    pub start: Bytes,
    pub end: Bytes,
}

impl KeyRange {
    pub fn new(start: impl Into<Bytes>, end: impl Into<Bytes>) -> RsfResult<Self> {
        let range = Self {
            start: start.into(),
            end: end.into(),
        };
        range.validate()?;
        Ok(range)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && key < self.end.as_slice()
    }

    pub fn validate(&self) -> RsfResult<()> {
        if self.start >= self.end {
            return Err(invalid("range start must be lower than range end"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeDescriptor {
    pub id: RangeId,
    pub span: KeyRange,
    pub pg: PlacementGroupId,
    pub owner_epoch: Epoch,
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeOwnershipTransfer {
    pub range: RangeId,
    pub previous_pg: PlacementGroupId,
    pub new_pg: PlacementGroupId,
    pub previous_owner_epoch: Epoch,
    pub new_owner_epoch: Epoch,
    pub previous_version: CommitVersion,
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SstableManifest {
    pub range: RangeId,
    pub level: u8,
    pub first_key: Bytes,
    pub last_key: Bytes,
    pub version: CommitVersion,
    pub bytes: u64,
    pub checksum: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionPlan {
    pub range: RangeId,
    pub inputs: Vec<SstableManifest>,
    pub output_level: u8,
    pub estimated_bytes: u64,
}

#[derive(Clone, Debug, Default)]
pub struct RangeLayout {
    ranges: BTreeMap<RangeId, RangeDescriptor>,
    tables: Vec<SstableManifest>,
    next_range_id: u64,
}

impl RangeLayout {
    pub fn create_range(
        &mut self,
        span: KeyRange,
        pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<RangeDescriptor> {
        if self
            .ranges
            .values()
            .any(|existing| ranges_overlap(&existing.span, &span))
        {
            return Err(invalid("range overlaps an existing descriptor"));
        }
        self.next_range_id += 1;
        let descriptor = RangeDescriptor {
            id: RangeId(self.next_range_id),
            span,
            pg,
            owner_epoch: Epoch(version.0),
            version,
        };
        self.ranges
            .insert(descriptor.id.clone(), descriptor.clone());
        Ok(descriptor)
    }

    pub fn split(
        &mut self,
        range: &RangeId,
        split_key: impl Into<Bytes>,
        left_pg: PlacementGroupId,
        right_pg: PlacementGroupId,
        version: CommitVersion,
    ) -> RsfResult<(RangeDescriptor, RangeDescriptor)> {
        let split_key = split_key.into();
        let original = self
            .ranges
            .remove(range)
            .ok_or_else(|| invalid(format!("unknown range {}", range.0)))?;
        if !original.span.contains(&split_key) || split_key == original.span.start {
            self.ranges.insert(original.id.clone(), original);
            return Err(invalid("split key must be inside range"));
        }

        self.next_range_id += 1;
        let left = RangeDescriptor {
            id: RangeId(self.next_range_id),
            span: KeyRange::new(original.span.start, split_key.clone())?,
            pg: left_pg,
            owner_epoch: Epoch(version.0),
            version,
        };
        self.next_range_id += 1;
        let right = RangeDescriptor {
            id: RangeId(self.next_range_id),
            span: KeyRange::new(split_key, original.span.end)?,
            pg: right_pg,
            owner_epoch: Epoch(version.0),
            version,
        };
        self.ranges.insert(left.id.clone(), left.clone());
        self.ranges.insert(right.id.clone(), right.clone());
        Ok((left, right))
    }

    pub fn transfer_ownership(
        &mut self,
        range: &RangeId,
        expected_version: CommitVersion,
        expected_owner_epoch: Epoch,
        new_pg: PlacementGroupId,
        new_owner_epoch: Epoch,
        version: CommitVersion,
    ) -> RsfResult<RangeOwnershipTransfer> {
        let descriptor = self
            .ranges
            .get_mut(range)
            .ok_or_else(|| invalid(format!("unknown range {}", range.0)))?;
        if descriptor.version != expected_version {
            return Err(invalid(format!(
                "range {} ownership transfer expected version {} but current version is {}",
                range.0, expected_version.0, descriptor.version.0
            )));
        }
        if descriptor.owner_epoch != expected_owner_epoch {
            return Err(invalid(format!(
                "range {} ownership transfer expected owner epoch {} but current owner epoch is {}",
                range.0, expected_owner_epoch.0, descriptor.owner_epoch.0
            )));
        }
        if version <= descriptor.version {
            return Err(invalid(format!(
                "range {} ownership transfer must advance version",
                range.0
            )));
        }
        if new_owner_epoch <= descriptor.owner_epoch {
            return Err(invalid(format!(
                "range {} ownership transfer must advance owner epoch",
                range.0
            )));
        }
        if descriptor.pg == new_pg {
            return Err(invalid(format!(
                "range {} is already owned by placement group {}",
                range.0, new_pg.0
            )));
        }

        let transfer = RangeOwnershipTransfer {
            range: descriptor.id.clone(),
            previous_pg: descriptor.pg,
            new_pg,
            previous_owner_epoch: descriptor.owner_epoch,
            new_owner_epoch,
            previous_version: descriptor.version,
            version,
        };
        descriptor.pg = new_pg;
        descriptor.owner_epoch = new_owner_epoch;
        descriptor.version = version;
        Ok(transfer)
    }

    pub fn add_table(&mut self, table: SstableManifest) -> RsfResult<()> {
        let range = self
            .ranges
            .get(&table.range)
            .ok_or_else(|| invalid(format!("unknown range {}", table.range.0)))?;
        if !range.span.contains(&table.first_key) || !range.span.contains(&table.last_key) {
            return Err(invalid("SSTable keys must be inside range span"));
        }
        if table.first_key > table.last_key {
            return Err(invalid("SSTable first key must not exceed last key"));
        }
        if table.bytes == 0 {
            return Err(invalid("SSTable must contain bytes"));
        }
        if table.checksum.validate().is_err() {
            return Err(invalid("SSTable checksum must not be empty"));
        }
        self.tables.push(table);
        Ok(())
    }

    pub fn plan_compaction(&self, range: &RangeId, max_level: u8) -> RsfResult<CompactionPlan> {
        let mut inputs = self
            .tables
            .iter()
            .filter(|table| &table.range == range)
            .cloned()
            .collect::<Vec<_>>();
        if inputs.len() < 2 {
            return Err(invalid("compaction requires at least two input tables"));
        }
        inputs.sort_by_key(|table| (table.level, table.version));
        let output_level = inputs
            .iter()
            .map(|table| table.level.saturating_add(1))
            .max()
            .unwrap_or_default()
            .min(max_level);
        let estimated_bytes = inputs.iter().map(|table| table.bytes).sum();
        Ok(CompactionPlan {
            range: range.clone(),
            inputs,
            output_level,
            estimated_bytes,
        })
    }

    pub fn range_for_key(&self, key: &[u8]) -> Option<&RangeDescriptor> {
        self.ranges
            .values()
            .find(|descriptor| descriptor.span.contains(key))
    }

    pub fn descriptor(&self, range: &RangeId) -> Option<&RangeDescriptor> {
        self.ranges.get(range)
    }

    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub fn non_overlapping(&self) -> bool {
        let mut spans = self
            .ranges
            .values()
            .map(|descriptor| &descriptor.span)
            .collect::<Vec<_>>();
        spans.sort_by(|left, right| left.start.cmp(&right.start));
        spans.windows(2).all(|pair| pair[0].end <= pair[1].start)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    left.start < right.end && right.start < left.end
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_replaces_one_descriptor_with_two_non_overlapping_ranges() {
        let mut layout = RangeLayout::default();
        let root = layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(1),
            )
            .unwrap();

        let (left, right) = layout
            .split(
                &root.id,
                b"m".to_vec(),
                PlacementGroupId(1),
                PlacementGroupId(2),
                CommitVersion(2),
            )
            .unwrap();

        assert_eq!(layout.range_count(), 2);
        assert!(layout.non_overlapping());
        assert!(left.span.contains(b"b"));
        assert!(right.span.contains(b"x"));
        assert_eq!(layout.range_for_key(b"m").unwrap().id, right.id);
    }

    #[test]
    fn overlapping_range_is_rejected() {
        let mut layout = RangeLayout::default();
        layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"m".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(1),
            )
            .unwrap();

        let err = layout
            .create_range(
                KeyRange::new(b"l".to_vec(), b"z".to_vec()).unwrap(),
                PlacementGroupId(2),
                CommitVersion(1),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn ownership_transfer_advances_version_without_changing_span() {
        let mut layout = RangeLayout::default();
        let range = layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(7),
            )
            .unwrap();

        let transfer = layout
            .transfer_ownership(
                &range.id,
                CommitVersion(7),
                Epoch(7),
                PlacementGroupId(2),
                Epoch(8),
                CommitVersion(8),
            )
            .unwrap();

        let moved = layout.descriptor(&range.id).unwrap();
        assert_eq!(transfer.previous_pg, PlacementGroupId(1));
        assert_eq!(transfer.new_pg, PlacementGroupId(2));
        assert_eq!(transfer.previous_owner_epoch, Epoch(7));
        assert_eq!(transfer.new_owner_epoch, Epoch(8));
        assert_eq!(transfer.previous_version, CommitVersion(7));
        assert_eq!(transfer.version, CommitVersion(8));
        assert_eq!(moved.pg, PlacementGroupId(2));
        assert_eq!(moved.owner_epoch, Epoch(8));
        assert_eq!(moved.version, CommitVersion(8));
        assert_eq!(moved.span, range.span);
        assert!(layout.non_overlapping());
    }

    #[test]
    fn ownership_transfer_rejects_stale_expected_version() {
        let mut layout = RangeLayout::default();
        let range = layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(7),
            )
            .unwrap();
        layout
            .transfer_ownership(
                &range.id,
                CommitVersion(7),
                Epoch(7),
                PlacementGroupId(2),
                Epoch(8),
                CommitVersion(8),
            )
            .unwrap();

        let err = layout
            .transfer_ownership(
                &range.id,
                CommitVersion(7),
                Epoch(7),
                PlacementGroupId(3),
                Epoch(9),
                CommitVersion(9),
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        let moved = layout.descriptor(&range.id).unwrap();
        assert_eq!(moved.pg, PlacementGroupId(2));
        assert_eq!(moved.owner_epoch, Epoch(8));
        assert_eq!(moved.version, CommitVersion(8));
    }

    #[test]
    fn ownership_transfer_rejects_stale_or_non_advancing_owner_epoch() {
        let mut layout = RangeLayout::default();
        let range = layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(7),
            )
            .unwrap();

        let stale_epoch = layout
            .transfer_ownership(
                &range.id,
                CommitVersion(7),
                Epoch(6),
                PlacementGroupId(2),
                Epoch(8),
                CommitVersion(8),
            )
            .unwrap_err();
        assert!(stale_epoch.message.contains("expected owner epoch"));

        let non_advancing_epoch = layout
            .transfer_ownership(
                &range.id,
                CommitVersion(7),
                Epoch(7),
                PlacementGroupId(2),
                Epoch(7),
                CommitVersion(8),
            )
            .unwrap_err();
        assert!(
            non_advancing_epoch
                .message
                .contains("must advance owner epoch")
        );

        let original = layout.descriptor(&range.id).unwrap();
        assert_eq!(original.pg, PlacementGroupId(1));
        assert_eq!(original.owner_epoch, Epoch(7));
        assert_eq!(original.version, CommitVersion(7));
    }

    #[test]
    fn sstable_manifest_must_fit_range() {
        let mut layout = RangeLayout::default();
        let range = layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"m".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(1),
            )
            .unwrap();

        let err = layout
            .add_table(table(range.id, b"a", b"z", 1, 64))
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn compaction_plan_sorts_inputs_and_estimates_output() {
        let mut layout = RangeLayout::default();
        let range = layout
            .create_range(
                KeyRange::new(b"a".to_vec(), b"z".to_vec()).unwrap(),
                PlacementGroupId(1),
                CommitVersion(1),
            )
            .unwrap();
        layout
            .add_table(table(range.id.clone(), b"a", b"h", 2, 100))
            .unwrap();
        layout
            .add_table(table(range.id.clone(), b"h", b"y", 1, 50))
            .unwrap();

        let plan = layout.plan_compaction(&range.id, 6).unwrap();

        assert_eq!(plan.inputs[0].version, CommitVersion(1));
        assert_eq!(plan.output_level, 1);
        assert_eq!(plan.estimated_bytes, 150);
    }

    fn table(
        range: RangeId,
        first: &[u8],
        last: &[u8],
        version: u64,
        bytes: u64,
    ) -> SstableManifest {
        let checksum = Digest::blake3_bytes(
            format!(
                "sstable:{}:{}:{}:{}",
                range.0,
                String::from_utf8_lossy(first),
                String::from_utf8_lossy(last),
                version
            )
            .as_bytes(),
        );
        SstableManifest {
            range,
            level: 0,
            first_key: first.to_vec(),
            last_key: last.to_vec(),
            version: CommitVersion(version),
            bytes,
            checksum,
        }
    }
}
