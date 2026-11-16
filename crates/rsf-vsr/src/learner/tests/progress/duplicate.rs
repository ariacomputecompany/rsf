use super::*;

#[test]
fn ordinary_duplicate_torn_tail_refuses_without_repair_and_restoration_stays_fenced() {
    let f = Fixture::new("duplicate-tail");
    let mut owner = learner(&f.target);
    stage(&mut owner, &f.snapshot);
    let file = active(&f.cell());
    let before = fs::read(&file).unwrap();
    let original = identity(&file);
    let modified = fs::metadata(&file).unwrap().modified().unwrap();
    OpenOptions::new()
        .append(true)
        .open(&file)
        .unwrap()
        .write_all(&[0xa7])
        .unwrap();
    let damaged = inventory(&f.target);
    let error = owner
        .append(f.id(), 1, f.snapshot.records[1].clone())
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "owned segment changed after validated WAL state"
    );
    assert_eq!(inventory(&f.target), damaged);
    assert!(!f.target.join("ACTIVE").exists());
    fs::write(&file, before).unwrap();
    File::open(&file)
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(modified))
        .unwrap();
    assert_eq!(identity(&file), original);
    fenced(&mut owner, &f.snapshot);
    drop(owner);
    let mut cold = learner(&f.target);
    stage(&mut cold, &f.snapshot);
    cold.finish(f.id()).unwrap();
    f.assert_published(&cold);
}

#[test]
fn ordinary_duplicate_mid_call_manifest_replacement_never_adopts_new_custody() {
    let f = Fixture::new("duplicate-manifest");
    let mut owner = learner(&f.target);
    stage(&mut owner, &f.snapshot);
    let manifest = f.cell().join("MANIFEST");
    let original = identity(&manifest);
    let bytes = fs::read(&manifest).unwrap();
    let modified = fs::metadata(&manifest).unwrap().modified().unwrap();
    let saved = f.root.join("original-manifest");
    let for_fault = saved.clone();
    let watch = observation::fault(Boundary::BeforeOrdinaryDuplicate, move |cell| {
        let path = cell.join("MANIFEST");
        fs::rename(&path, &for_fault).unwrap();
        fs::write(&path, fs::read(&for_fault).unwrap()).unwrap();
        File::open(&path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
    });
    let result = owner.append(f.id(), 1, f.snapshot.records[1].clone());
    assert_eq!(watch.calls(), 1);
    let error = result.unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "exclusive store physical custody changed; reopen required"
    );
    assert_eq!(fs::read(&manifest).unwrap(), bytes);
    assert_ne!(identity(&manifest), original);
    assert!(!f.target.join("ACTIVE").exists());
    fs::remove_file(&manifest).unwrap();
    fs::rename(saved, &manifest).unwrap();
    assert_eq!(identity(&manifest), original);
    fenced(&mut owner, &f.snapshot);
    drop(watch);
    drop(owner);
    let mut cold = learner(&f.target);
    stage(&mut cold, &f.snapshot);
    cold.finish(f.id()).unwrap();
    f.assert_published(&cold);
}

#[test]
fn ordinary_duplicate_exact_and_semantic_mismatch_keep_healthy_owner_and_bytes() {
    let f = Fixture::new("duplicate-healthy");
    let mut owner = learner(&f.target);
    stage(&mut owner, &f.snapshot);
    let before = inventory(&f.target);
    let mut different = f.snapshot.records[1].clone();
    different.payload.push(0xa7);
    let error = owner.append(f.id(), 1, different).unwrap_err();
    assert_eq!(error.code, ErrorCode::CorruptionDetected);
    assert_eq!(
        error.message,
        "learner duplicate record differs from durable history"
    );
    assert!(owner.committed_entries().unwrap().is_empty());
    assert_eq!(inventory(&f.target), before);
    assert_eq!(
        owner
            .append(f.id(), 1, f.snapshot.records[1].clone())
            .unwrap(),
        f.snapshot.descriptor.records
    );
    assert_eq!(inventory(&f.target), before);
    owner.finish(f.id()).unwrap();
    f.assert_published(&owner);
}
