use crate::{Bytes, Digest, DigestAlgorithm, RsfResult};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentId(pub [u8; 32]);

impl ContentId {
    pub fn for_bytes(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub fn from_blake3(bytes: [u8; 32]) -> RsfResult<Self> {
        Digest::blake3(bytes)?;
        Ok(Self(bytes))
    }

    pub fn digest(&self) -> RsfResult<Digest> {
        Digest::new(DigestAlgorithm::Blake3, self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlobChunk {
    pub id: ContentId,
    pub bytes: Bytes,
}

impl BlobChunk {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn verify(&self) -> bool {
        self.id == ContentId::for_bytes(&self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_id_uses_blake3_digest_bytes() {
        let id = ContentId::for_bytes(b"payload");

        assert_eq!(id.as_bytes(), blake3::hash(b"payload").as_bytes());
        assert_eq!(
            id.digest().expect("digest").algorithm,
            DigestAlgorithm::Blake3
        );
    }

    #[test]
    fn content_id_rejects_empty_digest_bytes() {
        assert!(ContentId::from_blake3([0_u8; 32]).is_err());
    }

    #[test]
    fn blob_chunk_verifies_content_identity() {
        let chunk = BlobChunk {
            id: ContentId::for_bytes(b"payload"),
            bytes: b"payload".to_vec(),
        };

        assert!(chunk.verify());
    }
}
