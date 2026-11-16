//! One authoritative state replay for disk recovery and election validation.
use super::*;

#[derive(Clone, Copy)]
pub(super) struct Record<'a> {
    pub kind: WalRecordKind,
    pub epoch: Epoch,
    pub payload: &'a [u8],
}

pub(super) type Recovered = (
    VsrCell,
    history::ConfigurationHistory,
    Option<VsrViewChangeRequest>,
    Option<VsrMembershipProposal>,
);

pub(super) fn recover(mut cell: VsrCell, records: &[Record<'_>]) -> RsfResult<Recovered> {
    let pg = cell.pg;
    let mut history = history::ConfigurationHistory::new(&cell);
    let mut view_change: Option<VsrViewChangeRequest> = None;
    let mut membership_fence: Option<VsrMembershipProposal> = None;
    let mut delta_start = None;
    let mut installed_authority = false;
    for (position, record) in records.iter().enumerate() {
        if record.kind == WalRecordKind::ViewInstallation {
            if membership_fence.is_some() {
                membership::staging::activation::replay_activation(
                    &mut cell,
                    &mut history,
                    (&mut view_change, &mut membership_fence),
                    records,
                    position,
                    &mut delta_start,
                )?;
                if membership_fence.is_none() {
                    installed_authority = true;
                }
                continue;
            }
            installation::replay_installation(
                &mut cell,
                &mut history,
                &mut view_change,
                records,
                position,
                &mut delta_start,
            )?;
            if view_change.is_none() {
                installed_authority = true;
            }
            continue;
        }
        if delta_start.is_some() {
            return Err(corrupt(
                "installation deltas interrupted before activation marker",
            ));
        }
        if record.kind == WalRecordKind::MembershipPromise
            || record.payload.starts_with(membership::MAGIC)
        {
            let proposal = VsrMembershipProposal::decode(record.payload)?;
            if record.kind != WalRecordKind::MembershipPromise || record.epoch != cell.view {
                return Err(corrupt("invalid membership promise envelope"));
            }
            proposal
                .validate_cell(&cell)
                .map_err(|error| corrupt(error.message))?;
            if membership_fence
                .as_ref()
                .is_some_and(|previous| previous != &proposal)
                || (membership_fence.is_none() && view_change.is_some())
            {
                return Err(corrupt(
                    "membership promise conflicts with durable authority fence",
                ));
            }
            view_change = Some(proposal.election.clone());
            membership_fence = Some(proposal);
            continue;
        }
        if membership_fence.is_some() {
            return Err(corrupt(
                "VSR history mutates a fenced membership transition",
            ));
        }
        if record.kind == WalRecordKind::ViewPromise
            || record.payload.starts_with(election::PROMISE_MAGIC)
        {
            let request = VsrViewChangeRequest::decode(record.payload)?;
            if record.kind != WalRecordKind::ViewPromise || record.epoch != cell.view {
                return Err(corrupt("invalid durable view-change promise envelope"));
            }
            request
                .validate_cell(&cell)
                .map_err(|err| corrupt(err.message))?;
            if view_change
                .as_ref()
                .is_some_and(|previous| !request.extends(previous))
            {
                return Err(corrupt(
                    "recovered view-change promise regresses or conflicts",
                ));
            }
            view_change = Some(request);
            continue;
        }
        if view_change.is_some() {
            return Err(corrupt(
                "VSR history mutates a view after its durable promise fence",
            ));
        }
        if migration::is_record(record.payload) {
            if record.epoch != cell.view {
                return Err(corrupt(
                    "VSR bootstrap migration epoch differs from replayed view",
                ));
            }
            continue;
        }
        if record.payload.starts_with(VSR_BOOTSTRAP_MAGIC) {
            if position != 0 {
                return Err(corrupt("VSR bootstrap record must be first and unique"));
            }
            continue;
        }
        match record.kind {
            WalRecordKind::Prepare => {
                let prepare = decode_prepare(record.payload)?;
                if prepare.pg != pg {
                    return Err(corrupt("vsr prepare payload placement group mismatch"));
                }
                replica::validate_payload(&prepare.operation, prepare.bytes).map_err(corrupt)?;
                history.validate_prepare(&prepare)?;
                if prepare.index.0 == 0 || prepare.bytes == 0 || prepare.index.0 > cell.next_index {
                    return Err(corrupt(
                        "recovered VSR prepare has an invalid index, size, or missing prefix",
                    ));
                }
                if let Some(previous) = cell.pending.get(&prepare.index)
                    && (previous.committed
                        || (previous.prepare != prepare && prepare.view <= previous.prepare.view))
                {
                    return Err(corrupt(
                        "recovered VSR prepare overwrites a committed entry or conflicts within a view",
                    ));
                }
                cell.next_index = cell.next_index.max(
                    prepare
                        .index
                        .0
                        .checked_add(1)
                        .ok_or_else(|| corrupt("VSR index overflow"))?,
                );
                let mut acknowledgements = BTreeSet::new();
                acknowledgements.insert(prepare.primary);
                cell.pending.insert(
                    prepare.index,
                    PendingPrepare {
                        prepare,
                        acknowledgements,
                        committed: false,
                    },
                );
            }
            WalRecordKind::Commit => {
                if !is_vsr_commit_payload(record.payload) {
                    continue;
                }
                let commit = decode_commit(record.payload)?;
                if commit.pg != pg {
                    return Err(corrupt("vsr commit payload placement group mismatch"));
                }
                let Some(prepare) = cell
                    .pending
                    .get(&commit.index)
                    .map(|pending| pending.prepare.clone())
                else {
                    return Err(corrupt("vsr commit recovered without prepare"));
                };
                if cell.committed_tail().0.checked_add(1) != Some(commit.index.0) {
                    return Err(corrupt(
                        "recovered VSR commit does not extend the contiguous prefix",
                    ));
                }
                history.validate(&prepare, &commit)?;
                let pending = cell
                    .pending
                    .get_mut(&commit.index)
                    .ok_or_else(|| corrupt("vsr commit recovered without prepare"))?;
                pending.committed = true;
                cell.committed.push(commit);
            }
            WalRecordKind::Membership => {
                if installed_authority
                    && (is_vsr_membership_change_payload(record.payload)
                        || is_vsr_view_change_payload(record.payload))
                {
                    return Err(corrupt(
                        "unilateral authority change follows quorum-installed VSR history",
                    ));
                }
                if is_vsr_membership_change_payload(record.payload) {
                    let change = decode_membership_change(record.payload)?;
                    if change.pg != pg {
                        return Err(corrupt("vsr membership payload placement group mismatch"));
                    }
                    if change.view <= cell.view {
                        return Err(corrupt(
                            "recovered vsr membership must strictly advance view",
                        ));
                    }
                    if change.old_replicas != cell.replicas {
                        return Err(corrupt(
                            "recovered vsr membership old voters do not match current voters",
                        ));
                    }
                    if change.committed_tail != cell.committed_tail() {
                        return Err(corrupt(
                            "recovered vsr membership committed prefix proof does not match wal",
                        ));
                    }
                    validate_membership_change(
                        &cell.replicas,
                        &change.new_replicas,
                        change.primary,
                    )
                    .map_err(corrupt)?;
                    cell.view = change.view;
                    cell.primary = change.primary;
                    cell.replicas = change.new_replicas;
                    history.record_legacy_membership(&cell);
                    continue;
                }
                if !is_vsr_view_change_payload(record.payload) {
                    if !is_vsr_client_request_payload(record.payload) {
                        continue;
                    }
                    let request_commit = decode_client_request_commit(record.payload)?;
                    if request_commit.pg != pg {
                        return Err(corrupt(
                            "vsr client request payload placement group mismatch",
                        ));
                    }
                    if !cell
                        .pending
                        .get(&request_commit.index)
                        .is_some_and(|pending| pending.committed)
                    {
                        return Err(corrupt(
                            "vsr client request recovered without committed prepare",
                        ));
                    }
                    if cell
                        .client_table
                        .get(&request_commit.request)
                        .is_some_and(|index| *index != request_commit.index)
                    {
                        return Err(corrupt(
                            "recovered client request remaps an existing committed identity",
                        ));
                    }
                    cell.client_table
                        .insert(request_commit.request, request_commit.index);
                    continue;
                }
                let change = decode_view_change(record.payload)?;
                if change.pg != pg {
                    return Err(corrupt("vsr view-change payload placement group mismatch"));
                }
                if change.view <= cell.view {
                    return Err(corrupt(
                        "recovered vsr view change must strictly advance view",
                    ));
                }
                if !cell.replicas.contains(&change.primary) {
                    return Err(corrupt(
                        "recovered vsr view-change primary is not a replica",
                    ));
                }
                let committed_tail = cell.committed_tail();
                if change.committed_tail != committed_tail {
                    return Err(corrupt(
                        "recovered vsr view-change committed prefix proof does not match wal",
                    ));
                }
                cell.view = change.view;
                cell.primary = change.primary;
                history.record(&cell);
            }
            _ => {}
        }
    }

    if delta_start.is_some() {
        return Err(corrupt("incomplete installation delta sequence"));
    }
    Ok((cell, history, view_change, membership_fence))
}
