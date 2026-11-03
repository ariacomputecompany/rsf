use super::*;
use std::collections::BTreeMap;

enum Owner {
    Inbox(Box<MembershipAuthorityInbox>),
    Decision(Box<MembershipDecisionStore>),
}
impl Owner {
    fn create(path: &Path, decision: bool, approval: &VsrMembershipApproval) -> Self {
        if decision {
            let mut store = MembershipDecisionStore::open(path, NodeId(2), approval).unwrap();
            store.promise(NodeId(1), round(1, 1)).unwrap();
            Self::Decision(Box::new(store))
        } else {
            let mut inbox =
                MembershipAuthorityInbox::open(path, PlacementGroupId(1), NodeId(2)).unwrap();
            let input = VsrMembershipInput::from_approval(approval).unwrap();
            let id = input.id().unwrap();
            inbox.begin(NodeId(1), input.clone()).unwrap();
            let records = input.flattened_records(approval).unwrap();
            let mut offset = 0;
            while offset < records.len() {
                let batch = VsrHistoryBatch::pack(offset as u64, &records[offset..]).unwrap();
                offset += batch.records.len();
                inbox.append(NodeId(1), id, batch).unwrap();
            }
            inbox.finish(NodeId(1), id).unwrap();
            Self::Inbox(Box::new(inbox))
        }
    }
    fn reopen(path: &Path, decision: bool) -> RsfResult<Self> {
        if decision {
            MembershipDecisionStore::open_existing(path, PlacementGroupId(1), NodeId(2))
                .map(|store| Self::Decision(Box::new(store)))
        } else {
            MembershipAuthorityInbox::open_existing(path, PlacementGroupId(1), NodeId(2))
                .map(|store| Self::Inbox(Box::new(store)))
        }
    }
    fn begin(&mut self, input: VsrMembershipInput) -> RsfResult<u64> {
        match self {
            Self::Inbox(store) => store.begin(NodeId(1), input),
            Self::Decision(store) => store.begin_input(NodeId(1), round(1, 1), input),
        }
    }
    fn read(&self, id: [u8; 32]) -> RsfResult<()> {
        match self {
            Self::Inbox(store) => store.retained_approval(NodeId(1), id).map(|_| ()),
            Self::Decision(store) => store.approval(id).map(|_| ()),
        }
    }
    fn append(&mut self, input: &VsrMembershipInput, batch: VsrHistoryBatch) -> RsfResult<u64> {
        match self {
            Self::Inbox(store) => store.append(NodeId(1), input.id()?, batch),
            Self::Decision(store) => store.append_input(NodeId(1), round(1, 1), input.id()?, batch),
        }
    }
}

fn input_path(path: &Path, input: &VsrMembershipInput) -> std::path::PathBuf {
    path.join("inputs").join(hex(input.id().unwrap()))
}
fn journal_bytes(path: &Path, decision: bool) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    let journal = path.join(if decision { "journal" } else { "metadata" });
    let mut bytes = BTreeMap::from([(
        journal.join("MANIFEST"),
        std::fs::read(journal.join("MANIFEST")).unwrap(),
    )]);
    for entry in std::fs::read_dir(journal.join("segments")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension() != Some(std::ffi::OsStr::new("lock")) {
            bytes.insert(path.clone(), std::fs::read(path).unwrap());
        }
    }
    bytes
}

#[cfg(unix)]
#[test]
fn authority_input_temporaries_preserve_foreign_targets_warm_and_cold() {
    use std::os::unix::fs::symlink;
    for decision in [false, true] {
        for cold in [false, true] {
            for obstruction in [
                "temporary-link",
                "temporary-directory",
                "descriptor-link",
                "dangling-descriptor",
            ] {
                let path = root("input-custody-temporary");
                let (a, b) = approvals(&path);
                let input = VsrMembershipInput::from_approval(&b).unwrap();
                let owned = path.join("owned");
                let mut owner = Owner::create(&owned, decision, &a);
                let directory = input_path(&owned, &input);
                std::fs::create_dir_all(&directory).unwrap();
                let foreign = path.join("foreign");
                let bytes = if obstruction == "descriptor-link" {
                    input.encode().unwrap()
                } else {
                    b"outside target must survive".to_vec()
                };
                if obstruction != "dangling-descriptor" {
                    std::fs::write(&foreign, &bytes).unwrap();
                }
                match obstruction {
                    "temporary-link" => {
                        symlink(&foreign, directory.join(".DESCRIPTOR.tmp")).unwrap()
                    }
                    "temporary-directory" => {
                        std::fs::create_dir(directory.join(".DESCRIPTOR.tmp")).unwrap()
                    }
                    _ => symlink(&foreign, directory.join("DESCRIPTOR")).unwrap(),
                }
                if cold {
                    drop(owner);
                    owner = Owner::reopen(&owned, decision).unwrap();
                }
                let before = journal_bytes(&owned, decision);
                for _ in 0..2 {
                    let result = owner.begin(input.clone());
                    if obstruction != "dangling-descriptor" {
                        assert_eq!(
                            std::fs::read(&foreign).unwrap(),
                            bytes,
                            "{decision}/{cold}/{obstruction}: external bytes changed"
                        );
                    } else {
                        assert!(!foreign.exists());
                    }
                    assert!(
                        result.is_err(),
                        "{decision}/{cold}/{obstruction}: aliased custody accepted"
                    );
                    assert_eq!(journal_bytes(&owned, decision), before);
                    assert!(!directory.join("records/MANIFEST").exists());
                }
                drop(owner);
                std::fs::remove_dir_all(path).unwrap();
            }
        }
    }
}

#[test]
fn authority_input_torn_regular_temporary_resumes_without_replacing_authority() {
    for decision in [false, true] {
        let path = root("input-custody-regular-temporary");
        let (a, b) = approvals(&path);
        let input = VsrMembershipInput::from_approval(&b).unwrap();
        let owned = path.join("owned");
        drop(Owner::create(&owned, decision, &a));
        let directory = input_path(&owned, &input);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(".DESCRIPTOR.tmp"), b"torn descriptor").unwrap();
        let mut owner = Owner::reopen(&owned, decision).unwrap();
        assert_eq!(owner.begin(input.clone()).unwrap(), 0);
        assert_eq!(
            std::fs::read(directory.join("DESCRIPTOR")).unwrap(),
            input.encode().unwrap()
        );
        assert!(!directory.join(".DESCRIPTOR.tmp").exists());
        let records = input.flattened_records(&b).unwrap();
        let mut offset = 0;
        while offset < records.len() {
            let batch = VsrHistoryBatch::pack(offset as u64, &records[offset..]).unwrap();
            offset += batch.records.len();
            owner.append(&input, batch).unwrap();
        }
        owner.read(input.id().unwrap()).unwrap();
        drop(owner);
        let owner = Owner::reopen(&owned, decision).unwrap();
        owner.read(input.id().unwrap()).unwrap();
        owner
            .read(VsrMembershipInput::from_approval(&a).unwrap().id().unwrap())
            .unwrap();
        drop(owner);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[path = "input_custody_tests/routes.rs"]
mod routes;
