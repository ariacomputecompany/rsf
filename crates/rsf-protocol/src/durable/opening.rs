//! Constructor-local metadata initialization from one actual storage replay.
use super::*;

pub(super) fn metadata(
    store: &mut SegmentStore,
    recovery: &SegmentStoreRecovery,
    groups: impl IntoIterator<Item = PlacementGroupId>,
    epoch: Epoch,
) -> RsfResult<(PlacementMap, ProtocolPolicy)> {
    // Preserve placement-before-policy decode/initialization and partial failure
    // behavior. Only PG0 membership records are appended; mapped PG0 is invalid.
    let placement = match placement_from_recovery(recovery)? {
        Some(placement) => placement,
        None => {
            let placement = PlacementMap::new(1, groups)?;
            store.append_next_wal(
                METADATA_PG,
                epoch,
                WalRecordKind::Membership,
                &placement.encode()?,
            )?;
            placement
        }
    };
    let policy = match policy_from_recovery(recovery)? {
        Some(policy) => policy,
        None => {
            let policy = ProtocolPolicy::default();
            store.append_next_wal(
                METADATA_PG,
                epoch,
                WalRecordKind::Membership,
                &policy.encode()?,
            )?;
            policy
        }
    };
    // Subsequent protocol cursors and receipts filter nonmetadata Commit WAL;
    // own newly appended metadata cannot change those recovered records.
    Ok((placement, policy))
}
