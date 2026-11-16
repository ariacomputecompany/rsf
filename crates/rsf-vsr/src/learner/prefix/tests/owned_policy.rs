//! Private shared-helper contract, separate from old-public-API red tests.
use super::*;
#[test]
fn shared_bounded_open_checks_finite_policy_before_provisional_factory_effects() {
    let fixture = Fixture::new("shared-open-policy");
    let descriptor = fixture.four.published.as_ref().unwrap().descriptor.clone();
    for policy in [
        VsrPrefixLimits {
            max_records: 0,
            max_encoded_bytes: 1,
        },
        VsrPrefixLimits {
            max_records: 1,
            max_encoded_bytes: 0,
        },
    ] {
        let path = fixture.root.join("must-not-initialize");
        assert!(!path.exists());
        let result = stage::open(&path, PlacementGroupId(1), descriptor.clone(), policy, true);
        assert!(result.is_err());
        assert!(
            !path.exists(),
            "invalid policy must precede even empty metadata completion"
        );
    }
    let root = fixture.root.clone();
    drop(fixture);
    fs::remove_dir_all(root).unwrap();
}
