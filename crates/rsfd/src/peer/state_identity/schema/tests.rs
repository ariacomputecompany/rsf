use super::*;

#[test]
fn native_control_empty_custody_variants_reject_unknown_fields_without_changing_wire_shape() {
    for kind in [
        "reserved_learner",
        "reserved_voter",
        "existing_voter",
        "initialized_voter",
    ] {
        let canonical = serde_json::json!({"kind": kind});
        let decoded: ControlCustody = serde_json::from_value(canonical.clone()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), canonical);

        // A source on a reserved or voter-only identity must never be silently
        // discarded. This is the actual extra field accepted by the old unit
        // variant decoder despite its deny_unknown_fields declaration.
        let mut unexpected = canonical.clone();
        unexpected["source"] = 0.into();
        let error = serde_json::from_value::<ControlCustody>(unexpected).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `source`"),
            "{kind}: {error}"
        );

        let mut unrelated = canonical;
        unrelated["unexpected"] = true.into();
        let error = serde_json::from_value::<ControlCustody>(unrelated).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `unexpected`"),
            "{kind}: {error}"
        );
    }
}
