use super::*;

#[test]
fn ordinary_cold_begin_renews_before_count_then_continues_exact_history() {
    let f = Fixture::new("cold-count");
    let count = f.snapshot.records.len() - 1;
    f.retained(count);
    let before = inventory(&f.target);
    let mut owner = learner(&f.target);
    let watch = observation::fault(Boundary::AfterActiveRenewal, |cell| {
        assert!(fs::metadata(active(cell)).unwrap().len() > 0);
    });
    let result = owner.begin(f.snapshot.descriptor.clone());
    assert_eq!(
        watch.calls(),
        1,
        "cold progress must pass actual retained-data renewal"
    );
    assert_eq!(result.unwrap(), count as u64);
    assert_eq!(inventory(&f.target), before);
    drop(watch);
    let warm = observation::fault(Boundary::AfterActiveRenewal, |_| {
        panic!("warm cursor must not renew")
    });
    assert_eq!(
        owner.begin(f.snapshot.descriptor.clone()).unwrap(),
        count as u64
    );
    assert_eq!(warm.calls(), 0);
    drop(warm);
    assert_eq!(
        owner
            .append(f.id(), count as u64, f.snapshot.records[count].clone())
            .unwrap(),
        f.snapshot.descriptor.records
    );
    assert_eq!(owner.finish(f.id()).unwrap(), LogIndex(1));
    f.assert_published(&owner);
    let hot = observation::fault(Boundary::AfterActiveRenewal, |_| {
        panic!("hot token must not renew")
    });
    owner.published_state_token().unwrap();
    assert_eq!(
        owner.begin(f.snapshot.descriptor.clone()).unwrap(),
        f.snapshot.descriptor.records
    );
    assert_eq!(owner.finish(f.id()).unwrap(), LogIndex(1));
    assert_eq!(hot.calls(), 0);
    drop(hot);
    drop(owner);
    let watch = observation::fault(Boundary::AfterActiveRenewal, |_| {});
    let owner = learner(&f.target);
    assert_eq!(watch.calls(), 1);
    f.assert_published(&owner);
}

#[test]
fn ordinary_cold_begin_after_real_renewal_loss_refuses_count_and_restoration_stays_fenced() {
    let f = Fixture::new("cold-count-loss");
    f.retained(f.snapshot.records.len() - 1);
    let mut owner = learner(&f.target);
    let file = active(&f.cell());
    let original = identity(&file);
    let bytes = fs::read(&file).unwrap();
    let saved = f.root.join("original-active");
    let for_fault = saved.clone();
    let watch = observation::fault(Boundary::AfterActiveRenewal, move |cell| {
        fs::rename(active(cell), for_fault).unwrap();
    });
    let result = owner.begin(f.snapshot.descriptor.clone());
    assert_eq!(
        watch.calls(),
        1,
        "fault must follow actual closed and active renewal"
    );
    not_found(&result.unwrap_err(), &file);
    assert!(!file.exists());
    assert_eq!(fs::read(&saved).unwrap(), bytes);
    assert!(!f.target.join("ACTIVE").exists());
    fs::rename(saved, &file).unwrap();
    assert_eq!(identity(&file), original);
    fenced(&mut owner, &f.snapshot);
    drop(watch);
    drop(owner);
    let mut cold = learner(&f.target);
    let watch = observation::fault(Boundary::AfterActiveRenewal, |_| {});
    assert_eq!(
        cold.begin(f.snapshot.descriptor.clone()).unwrap(),
        f.snapshot.descriptor.records - 1
    );
    assert_eq!(watch.calls(), 1);
    drop(watch);
    stage(&mut cold, &f.snapshot);
    cold.finish(f.id()).unwrap();
    f.assert_published(&cold);
}

#[test]
fn ordinary_cold_begin_after_real_renewal_unwind_fences_without_ack() {
    let f = Fixture::new("cold-count-unwind");
    f.retained(f.snapshot.records.len() - 1);
    let before = inventory(&f.target);
    let mut owner = learner(&f.target);
    let watch = observation::fault(Boundary::AfterActiveRenewal, |_| {
        panic!("injected unwind after actual retained-data renewal")
    });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        owner.begin(f.snapshot.descriptor.clone())
    }));
    assert_eq!(watch.calls(), 1);
    let panic = result.unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>().copied(),
        Some("injected unwind after actual retained-data renewal")
    );
    assert_eq!(inventory(&f.target), before);
    fenced(&mut owner, &f.snapshot);
    drop(watch);
    drop(owner);
    let mut cold = learner(&f.target);
    stage(&mut cold, &f.snapshot);
    cold.finish(f.id()).unwrap();
    f.assert_published(&cold);
}

#[test]
fn ordinary_cold_begin_changed_tail_between_real_barriers_never_repairs() {
    let f = Fixture::new("cold-between-barriers");
    f.retained(f.snapshot.records.len() - 1);
    let file = active(&f.cell());
    let before = fs::read(&file).unwrap();
    let mut damaged = before.clone();
    damaged.push(0xa7);
    let mut owner = learner(&f.target);
    let watch = observation::fault(Boundary::AfterClosedRenewal, |cell| {
        OpenOptions::new()
            .append(true)
            .open(active(cell))
            .unwrap()
            .write_all(&[0xa7])
            .unwrap();
    });
    let result = owner.begin(f.snapshot.descriptor.clone());
    assert_eq!(watch.calls(), 1);
    let error = result.unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "owned segment changed after validated WAL state"
    );
    assert_eq!(fs::read(&file).unwrap(), damaged);
    assert!(!f.target.join("ACTIVE").exists());
    fs::write(&file, before).unwrap();
    fenced(&mut owner, &f.snapshot);
}

fn cold_published_fault(strict: bool) {
    let f = Fixture::new(if strict {
        "strict-publication"
    } else {
        "ordinary-publication"
    });
    f.publish();
    let before = inventory(&f.target);
    let file = active(&f.cell());
    let original = identity(&file);
    let saved = f.root.join("original-active");
    let for_fault = saved.clone();
    let watch = observation::fault(Boundary::AfterActiveRenewal, move |cell| {
        fs::rename(active(cell), for_fault).unwrap();
    });
    let result = if strict {
        f.strict_open()
    } else {
        LearnerHistoryStore::open(&f.target, PlacementGroupId(1), NodeId(9), NodeId(0))
    };
    assert_eq!(
        watch.calls(),
        1,
        "cold published owner cannot escape before renewal"
    );
    not_found(&result.unwrap_err(), &file);
    assert!(!file.exists());
    assert_eq!(fs::read(f.target.join("ACTIVE")).unwrap(), f.id());
    fs::rename(saved, &file).unwrap();
    assert_eq!(identity(&file), original);
    assert_eq!(inventory(&f.target), before);
    drop(watch);
    let watch = observation::fault(Boundary::AfterActiveRenewal, |_| {});
    let owner = if strict {
        f.strict_open().unwrap()
    } else {
        learner(&f.target)
    };
    assert_eq!(watch.calls(), 1);
    f.assert_published(&owner);
    owner.published_state_token().unwrap();
    assert_eq!(inventory(&f.target), before);
}
#[test]
fn ordinary_cold_published_open_renews_legacy_data_before_owner_escape() {
    cold_published_fault(false);
}
#[test]
fn strict_cold_published_open_renews_legacy_data_before_owner_escape() {
    cold_published_fault(true);
}
