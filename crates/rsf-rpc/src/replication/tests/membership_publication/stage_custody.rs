//! Actual authenticated cached STAGE and independently backed historical ACKs.
use super::*;

fn rejected(error: io::Error) {
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert!(
        error.to_string().starts_with("peer rejected request:"),
        "{error}"
    );
    assert!(
        !error
            .to_string()
            .contains("injected publication materializer failure"),
        "custody must fail before callback: {error}"
    );
}

#[test]
fn real_tls_fresh_publication_rechecks_chosen_input_with_a_healthy_cached_stage() {
    for missing in [true, false] {
        let ready = fixture();
        let network = ready.network();
        let chosen = ready.chosen.encode().unwrap();
        let expected = network
            .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
            .unwrap();
        let input = ready
            .chosen
            .input_id()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let manifest = ready
            .root
            .join("membership-3/decision/inputs")
            .join(input)
            .join("records/MANIFEST");
        assert!(std::fs::metadata(&manifest).unwrap().len() > 0);
        if missing {
            std::fs::remove_file(&manifest).unwrap();
        } else {
            std::fs::write(&manifest, b"corrupt retained choice").unwrap();
        }
        let stage = ready.root.join("membership-3/staging");
        let metadata = std::fs::read(stage.join("META")).unwrap();
        let receipt = std::fs::read(stage.join("INSTALLED")).unwrap();
        let active = std::fs::read(ready.root.join("learner-3/ACTIVE")).unwrap();
        let application = ready.root.join("materialized-3/1");
        std::fs::remove_file(&application).unwrap();
        std::fs::write(ready.root.join("fail-apply-3"), b"no callback permitted").unwrap();

        // The independent stage remains healthy and boot-materialized. Its
        // opaque proof cannot excuse a fresh publication's lost bound input.
        assert_eq!(
            network
                .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
                .unwrap(),
            expected
        );
        rejected(
            network
                .exchange_once(
                    NodeId(3),
                    MEMBERSHIP_PUBLISH,
                    &ready.quorum.encode().unwrap(),
                )
                .unwrap_err(),
        );
        assert!(!application.exists());
        assert!(!stage.join("PUBLICATION").exists());
        assert!(!ready.root.join("store-3").exists());
        assert_eq!(std::fs::read(stage.join("META")).unwrap(), metadata);
        assert_eq!(std::fs::read(stage.join("INSTALLED")).unwrap(), receipt);
        assert_eq!(
            std::fs::read(ready.root.join("learner-3/ACTIVE")).unwrap(),
            active
        );
        if missing {
            assert!(!manifest.exists(), "publication repaired its bound input");
        } else {
            assert_eq!(std::fs::read(manifest).unwrap(), b"corrupt retained choice");
        }
    }
}

#[test]
fn real_tls_cached_stage_rejects_own_history_loss_before_callback_and_stays_latched() {
    let mut r = fixture();
    let chosen = r.chosen.encode().unwrap();
    let network = r.network();
    let reply = network
        .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
        .unwrap();
    let expected = rsf_vsr::VsrMembershipStagedReport::decode(&reply).unwrap();
    assert_eq!(expected.plan, r.quorum.plan_id());
    let root = r.root.join("membership-3/staging");
    let manifest = root.join("selected/MANIFEST");
    let original = r.root.join("original-stage-manifest");
    std::fs::rename(&manifest, &original).unwrap();
    let installed = std::fs::read(root.join("INSTALLED")).unwrap();
    let meta = std::fs::read(root.join("META")).unwrap();
    let application = r.root.join("materialized-3/1");
    std::fs::remove_file(&application).unwrap();
    std::fs::write(r.root.join("fail-apply-3"), b"no callback permitted").unwrap();
    for _ in 0..2 {
        rejected(
            network
                .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
                .unwrap_err(),
        );
        assert!(!manifest.exists());
        assert!(!application.exists());
        assert_eq!(std::fs::read(root.join("INSTALLED")).unwrap(), installed);
        assert_eq!(std::fs::read(root.join("META")).unwrap(), meta);
    }
    std::fs::rename(&original, &manifest).unwrap();
    rejected(
        network
            .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
            .unwrap_err(),
    );
    assert!(!application.exists());
    std::fs::remove_file(r.root.join("fail-apply-3")).unwrap();
    r.restart_remote(true);
    let reply = r
        .network()
        .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
        .unwrap();
    assert_eq!(
        rsf_vsr::VsrMembershipStagedReport::decode(&reply).unwrap(),
        expected
    );
    assert_eq!(
        std::fs::read(application).unwrap(),
        r.committed.encode().unwrap()
    );
}

#[test]
fn real_tls_stage_loss_cannot_block_independently_backed_historical_publication() {
    let mut r = fixture();
    r.publish();
    let network = r.network();
    let chosen = r.chosen.encode().unwrap();
    network
        .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
        .unwrap();
    let before = inventory(&r.root, 3);
    let root = r.root.join("membership-3/staging");
    let manifest = root.join("selected/MANIFEST");
    std::fs::remove_file(&manifest).unwrap();
    rejected(
        network
            .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
            .unwrap_err(),
    );
    let application = r.root.join("materialized-3/1");
    std::fs::remove_file(&application).unwrap();
    let query = network
        .query_membership_publication(NodeId(3), PG, r.quorum.plan_id())
        .unwrap();
    assert_eq!(
        std::fs::read(&application).unwrap(),
        r.committed.encode().unwrap()
    );
    std::fs::remove_file(&application).unwrap();
    let retry = network.publish_membership(NodeId(3), &r.alternate).unwrap();
    assert_eq!(retry, query);
    assert_eq!(
        std::fs::read(application).unwrap(),
        r.committed.encode().unwrap()
    );
    assert!(
        !manifest.exists(),
        "historical ACK must not reconstruct the stage"
    );
    assert_eq!(
        inventory(&r.root, 3),
        before,
        "historical ACK changed active history or first PUBLICATION"
    );
    rejected(
        network
            .exchange_once(NodeId(3), MEMBERSHIP_STAGE, &chosen)
            .unwrap_err(),
    );
}
