use super::*;

#[test]
fn control_provisioning_rejects_foreign_bootstrap_inventory_and_peer_fingerprints() {
    for fault in ["bootstrap", "inventory", "fingerprint"] {
        let mut f = Fixture::unbound("control-domain-config");
        let mut catalog = f.catalog.clone();
        let mut anchor = f.anchor.clone();
        match fault {
            "bootstrap" => anchor.control_bootstrap = [99; 32],
            "inventory" => {
                // Keep a valid complete control+data catalog, but require a
                // different data PG than the server actually owns.
                catalog.groups[1].pg = PlacementGroupId(DATA.0 + 1);
                anchor.genesis_catalog = catalog.root().unwrap();
            }
            "fingerprint" => {
                catalog.nodes[1].certificate = [99; 32];
                anchor.genesis_catalog = catalog.root().unwrap();
            }
            _ => unreachable!(),
        }
        let before = tree(&f.root);
        let server = f.server.take().unwrap();
        let error = server
            .with_control_replication(anchor, catalog)
            .unwrap_err();
        assert!(
            error.contains(match fault {
                "bootstrap" => "Genesis",
                "inventory" => "inventory",
                _ => "certificate mapping",
            }),
            "{error}"
        );
        assert_eq!(tree(&f.root), before);
        f.cleanup();
    }
}

#[test]
fn application_borrow_restores_exact_control_owner_on_error_panic_and_collision() {
    let mut f = Fixture::new("control-domain-borrow");
    let mut server = f.server.take().unwrap();
    let original = server.cells[&CONTROL].recovery_state_token().unwrap();
    assert!(server.with_voting_cells(|_, _, _| ((), true)).is_err());
    let inner: Result<(), &str> = server
        .with_application_cells(|_, cells, _| {
            assert_eq!(cells.keys().copied().collect::<Vec<_>>(), vec![DATA]);
            assert_eq!(cells[&DATA].primary(), NodeId(2));
            (Err("application failure"), false)
        })
        .unwrap();
    assert_eq!(inner, Err("application failure"));
    assert_eq!(
        server.cells[&CONTROL].recovery_state_token().unwrap(),
        original
    );
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = server.with_application_cells::<()>(|_, cells, _| {
                assert!(!cells.contains_key(&CONTROL));
                panic!("callback unwound")
            });
        }))
        .is_err()
    );
    assert_eq!(
        server.cells[&CONTROL].recovery_state_token().unwrap(),
        original
    );
    let impostor = DurableVsrCell::open(
        f.root.join("impostor"),
        CONTROL,
        Epoch(1),
        NodeId(0),
        (0..3).map(NodeId),
    )
    .unwrap();
    let error = server
        .with_application_cells(|_, cells, _| {
            cells.insert(CONTROL, impostor);
            ((), true)
        })
        .unwrap_err();
    assert!(error.contains("inserted reserved"));
    assert_eq!(
        server.cells[&CONTROL].recovery_state_token().unwrap(),
        original
    );
    assert!(server.installation_pending.contains(&DATA));
    server
        .with_application_cells(|_, cells, _| {
            assert_eq!(cells.len(), 1);
            ((), true)
        })
        .unwrap();
    assert!(!server.installation_pending.contains(&DATA));
    assert!(!server.installation_pending.contains(&CONTROL));
    drop(server);
    f.cleanup();
}
