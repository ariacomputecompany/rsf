use super::*;

#[test]
fn first_unbound_accept_or_choice_requires_publication_before_journal_binding() {
    for choose in [false, true] {
        let path = root("first-unbound-custody");
        let (a, b) = approvals(&path);
        let mut sources = stores(&path, &a);
        let chosen = super::recovery::choose_b(&mut sources, &b);
        let input = VsrMembershipInput::from_approval(&b).unwrap();
        let id = input.id().unwrap();
        let records = input.flattened_records(&b).unwrap();
        let target = path.join("target");
        let mut store = MembershipDecisionStore::open(&target, NodeId(3), &a).unwrap();
        let own_round = round(9, 3);
        let promises = sources[..3]
            .iter_mut()
            .map(|source| source.promise(NodeId(3), own_round).unwrap())
            .collect();
        let decision = VsrMembershipDecision::from_promises(promises, input.clone()).unwrap();
        store.promise(NodeId(3), own_round).unwrap();
        // Retained complete bytes alone do not bind local durable custody. This
        // models a finished source copy before any journal reference to B.
        disk::create_input(&target, &input).unwrap();
        disk::retain_records(&target, &mut false, &input, &records).unwrap();
        store.read_approval(id).unwrap();
        assert!(!store.input_is_bound(id));
        let state = store.state.clone();
        let journal = store
            .journal
            .recover_current()
            .unwrap()
            .recovery
            .wal_records;
        let mut called = 0;
        let fail = |root: &Path, input| {
            assert_eq!(root, target);
            assert_eq!(input, id);
            called += 1;
            Err(corrupt("injected first-custody publication failure"))
        };
        let result = if choose {
            store.learn_verified_chosen_with_publication(chosen.clone(), fail)
        } else {
            store
                .accept_plan_with_publication(NodeId(3), &decision, || {}, fail)
                .map(|_| ())
        };
        assert!(result.is_err());
        assert_eq!(called, 1);
        assert_eq!(store.state, state);
        assert_eq!(
            store
                .journal
                .recover_current()
                .unwrap()
                .recovery
                .wal_records,
            journal
        );
        assert!(!store.input_is_bound(id));

        if choose {
            store.learn_verified_chosen(chosen.clone()).unwrap();
            store
                .learn_verified_chosen_with_publication(chosen.clone(), |_, _| {
                    panic!("idempotent chosen retry repeated durable custody publication")
                })
                .unwrap();
        } else {
            store.nominate(&decision).unwrap();
            store
                .accept_plan_with_publication(
                    NodeId(3),
                    &decision,
                    || {},
                    |_, _| panic!("already accepted input repeated durable custody publication"),
                )
                .unwrap();
        }
        assert!(store.input_is_bound(id));
        drop(store);
        let store = MembershipDecisionStore::open_existing(&target, PlacementGroupId(1), NodeId(3))
            .unwrap();
        assert!(store.input_is_bound(id));
        assert_eq!(
            input
                .flattened_records(&store.approval(id).unwrap())
                .unwrap(),
            records
        );
        drop(store);
        drop(sources);
        std::fs::remove_dir_all(path).unwrap();
    }
}
