use std::fmt;

use crate::{ErrorCode, RsfError, RsfResult};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DigestAlgorithm {
    Blake3,
    Sha256,
}

impl DigestAlgorithm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blake3 => "blake3",
            Self::Sha256 => "sha256",
        }
    }
}

impl fmt::Display for DigestAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest {
    pub algorithm: DigestAlgorithm,
    pub bytes: [u8; 32],
}

impl Digest {
    pub fn new(algorithm: DigestAlgorithm, bytes: [u8; 32]) -> RsfResult<Self> {
        let digest = Self { algorithm, bytes };
        digest.validate()?;
        Ok(digest)
    }

    pub fn blake3(bytes: [u8; 32]) -> RsfResult<Self> {
        Self::new(DigestAlgorithm::Blake3, bytes)
    }

    pub fn blake3_bytes(bytes: &[u8]) -> Self {
        Self {
            algorithm: DigestAlgorithm::Blake3,
            bytes: *blake3::hash(bytes).as_bytes(),
        }
    }

    pub fn sha256(bytes: [u8; 32]) -> RsfResult<Self> {
        Self::new(DigestAlgorithm::Sha256, bytes)
    }

    pub fn validate(&self) -> RsfResult<()> {
        if self.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "digest bytes must not be all zero",
            ));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.bytes == [0_u8; 32]
    }

    pub fn algorithm(&self) -> DigestAlgorithm {
        self.algorithm
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:", self.algorithm)?;
        for byte in self.bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_display_includes_algorithm_identity() {
        let digest = Digest::blake3([7_u8; 32]).expect("digest");

        assert!(digest.to_string().starts_with("blake3:"));
        assert_eq!(digest.as_bytes(), &[7_u8; 32]);
    }

    #[test]
    fn blake3_bytes_hashes_payloads_into_algorithm_tagged_digest() {
        let digest = Digest::blake3_bytes(b"rsf");

        assert_eq!(digest.algorithm, DigestAlgorithm::Blake3);
        assert_ne!(digest.as_bytes(), &[0_u8; 32]);
    }

    #[test]
    fn digest_rejects_empty_all_zero_value() {
        let err = Digest::sha256([0_u8; 32]).expect_err("empty digest rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert_eq!(err.category(), crate::ErrorCategory::Configuration);
    }
}
