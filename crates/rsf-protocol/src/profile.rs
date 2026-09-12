#![forbid(unsafe_code)]

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CustomProtocolProfile {
    pub id: &'static str,
    pub name: &'static str,
    pub spine: ProtocolSpine,
    pub borrowed_parts: &'static [BorrowedProtocolPart],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolSpine {
    UniversalVsrRados,
}

impl ProtocolSpine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UniversalVsrRados => "universal-vsr-rados",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BorrowedProtocolPart {
    FoundationDbTransactions,
    AuroraSocratesPageWal,
    DualModeCpApPolicy,
    GeoLocalWpaxosOwnership,
}

impl BorrowedProtocolPart {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FoundationDbTransactions => "foundationdb-transactions",
            Self::AuroraSocratesPageWal => "aurora-socrates-page-wal",
            Self::DualModeCpApPolicy => "dual-mode-cp-ap-policy",
            Self::GeoLocalWpaxosOwnership => "geo-local-wpaxos-ownership",
        }
    }
}

const BORROWED_PARTS: &[BorrowedProtocolPart] = &[
    BorrowedProtocolPart::FoundationDbTransactions,
    BorrowedProtocolPart::AuroraSocratesPageWal,
    BorrowedProtocolPart::DualModeCpApPolicy,
    BorrowedProtocolPart::GeoLocalWpaxosOwnership,
];

pub const fn custom_protocol_profile() -> CustomProtocolProfile {
    CustomProtocolProfile {
        id: "custom-rsf",
        name: "Custom RSF Universal VSR-RADOS Database Protocol",
        spine: ProtocolSpine::UniversalVsrRados,
        borrowed_parts: BORROWED_PARTS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_protocol_profile_names_the_single_production_protocol() {
        let profile = custom_protocol_profile();

        assert_eq!(profile.id, "custom-rsf");
        assert_eq!(profile.spine.as_str(), "universal-vsr-rados");
        assert_eq!(
            profile
                .borrowed_parts
                .iter()
                .map(|part| part.as_str())
                .collect::<Vec<_>>(),
            vec![
                "foundationdb-transactions",
                "aurora-socrates-page-wal",
                "dual-mode-cp-ap-policy",
                "geo-local-wpaxos-ownership"
            ]
        );
    }
}
