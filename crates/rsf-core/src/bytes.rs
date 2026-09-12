use crate::{Bytes, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ByteBuf(Bytes);

impl ByteBuf {
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    pub fn non_empty(bytes: impl Into<Bytes>, label: &'static str) -> RsfResult<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                format!("{label} must not be empty"),
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl From<Bytes> for ByteBuf {
    fn from(bytes: Bytes) -> Self {
        Self::new(bytes)
    }
}

impl From<ByteBuf> for Bytes {
    fn from(bytes: ByteBuf) -> Self {
        bytes.into_bytes()
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyBytes(ByteBuf);

impl KeyBytes {
    pub fn new(bytes: impl Into<Bytes>) -> RsfResult<Self> {
        Ok(Self(ByteBuf::non_empty(bytes, "key")?))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn into_bytes(self) -> Bytes {
        self.0.into_bytes()
    }
}

impl TryFrom<Bytes> for KeyBytes {
    type Error = RsfError;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Self::new(bytes)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ValueBytes(ByteBuf);

impl ValueBytes {
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(ByteBuf::new(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn into_bytes(self) -> Bytes {
        self.0.into_bytes()
    }
}

impl From<Bytes> for ValueBytes {
    fn from(bytes: Bytes) -> Self {
        Self::new(bytes)
    }
}

impl From<ValueBytes> for Bytes {
    fn from(bytes: ValueBytes) -> Self {
        bytes.into_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRangeBytes {
    pub start: KeyBytes,
    pub end: KeyBytes,
}

impl KeyRangeBytes {
    pub fn new(start: impl Into<Bytes>, end: impl Into<Bytes>) -> RsfResult<Self> {
        let start = KeyBytes::new(start)?;
        let end = KeyBytes::new(end)?;
        if start >= end {
            return Err(RsfError::new(
                ErrorCode::InvalidInput,
                "key range start must be lower than key range end",
            ));
        }
        Ok(Self { start, end })
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_bytes_reject_empty_keys() {
        let err = KeyBytes::new(Vec::new()).expect_err("empty key rejected");

        assert_eq!(err.code, ErrorCode::InvalidInput);
    }

    #[test]
    fn values_allow_empty_payloads() {
        let value = ValueBytes::new(Vec::new());

        assert!(value.as_slice().is_empty());
    }

    #[test]
    fn key_ranges_validate_order_and_overlap() {
        let left = KeyRangeBytes::new(b"a".to_vec(), b"m".to_vec()).expect("left");
        let right = KeyRangeBytes::new(b"h".to_vec(), b"z".to_vec()).expect("right");
        let disjoint = KeyRangeBytes::new(b"z".to_vec(), b"zz".to_vec()).expect("disjoint");

        assert!(left.overlaps(&right));
        assert!(!left.overlaps(&disjoint));
        assert_eq!(
            KeyRangeBytes::new(b"m".to_vec(), b"a".to_vec())
                .expect_err("bad range")
                .code,
            ErrorCode::InvalidInput
        );
    }
}
