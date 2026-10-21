//! Versioned relational row/identity key domains. A framed table component and
//! an uninterpreted primary-key suffix preserve primary-key byte ordering.
use crate::{ErrorCode, RsfError, RsfResult};

const ROW_V2: &[u8] = b"\0RSFSQLR2";
const IDENTITY_V2: &[u8] = b"\0RSFSQLU2";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RelationalKeyFormat {
    LegacyDelimitedV1,
    #[default]
    LengthPrefixedV2,
}
impl RelationalKeyFormat {
    pub fn row_prefix(self, table: &str) -> Vec<u8> {
        match self {
            Self::LegacyDelimitedV1 => format!("table/{table}/pk/").into_bytes(),
            Self::LengthPrefixedV2 => framed_prefix(ROW_V2, table),
        }
    }
    pub fn row_key(self, table: &str, primary: &str) -> Vec<u8> {
        let mut key = self.row_prefix(table);
        key.extend_from_slice(primary.as_bytes());
        key
    }
    pub fn primary_key(self, table: &str, key: &[u8]) -> RsfResult<String> {
        let prefix = self.row_prefix(table);
        let primary = key
            .strip_prefix(prefix.as_slice())
            .ok_or_else(|| corrupt("relational row key belongs to a different table or format"))?;
        if self == Self::LegacyDelimitedV1 && primary.is_empty() {
            return Err(corrupt(
                "legacy relational row key has an empty primary key",
            ));
        }
        String::from_utf8(primary.to_vec())
            .map_err(|_| corrupt("relational primary key is not UTF-8"))
    }
    pub fn primary_identity(self, table: &str, primary: &str) -> Vec<u8> {
        self.constraint_identity(table, "primary", &[primary.to_string()])
    }
    pub fn constraint_identity(self, table: &str, name: &str, values: &[String]) -> Vec<u8> {
        match self {
            Self::LegacyDelimitedV1 => {
                format!("unique/{table}/{name}/{}", values.join("/")).into_bytes()
            }
            Self::LengthPrefixedV2 => {
                let mut key = framed_prefix(IDENTITY_V2, table);
                field(&mut key, name.as_bytes());
                key.extend_from_slice(&(values.len() as u64).to_be_bytes());
                for value in values {
                    field(&mut key, value.as_bytes());
                }
                key
            }
        }
    }
    /// For KV-only recovery without a checkpoint manifest. Durable stores use
    /// their explicit persisted format. A mixed row-key domain requires migration.
    pub fn detect<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> RsfResult<Self> {
        let mut format = None;
        for key in keys {
            let next = Self::format_of(key);
            if let Some(next) = next {
                if format.is_some_and(|format| format != next) {
                    return Err(corrupt(
                        "mixed relational key formats require explicit migration",
                    ));
                }
                format = Some(next);
            }
        }
        Ok(format.unwrap_or_default())
    }
    pub fn format_of(key: &[u8]) -> Option<Self> {
        if key.starts_with(ROW_V2) {
            Some(Self::LengthPrefixedV2)
        } else if key.starts_with(b"table/") {
            Some(Self::LegacyDelimitedV1)
        } else {
            None
        }
    }
}
fn framed_prefix(domain: &[u8], table: &str) -> Vec<u8> {
    let mut key = domain.to_vec();
    field(&mut key, table.as_bytes());
    key
}
fn field(key: &mut Vec<u8>, bytes: &[u8]) {
    key.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    key.extend_from_slice(bytes);
}
fn corrupt(message: &str) -> RsfError {
    RsfError::new(ErrorCode::CorruptionDetected, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn framed_relational_components_are_injective_ordered_and_domain_separated() {
        let format = RelationalKeyFormat::default();
        let parts = [
            "",
            "a",
            "a/pk/b",
            "b/pk/c",
            "x\0y",
            "é",
            "table/a/pk/",
            "🙂",
        ];
        let mut keys = std::collections::BTreeSet::new();
        for table in parts {
            let prefix = format.row_prefix(table);
            let mut previous = None;
            let mut sorted = parts.to_vec();
            sorted.sort();
            for primary in sorted {
                let key = format.row_key(table, primary);
                assert!(key.starts_with(&prefix));
                assert_eq!(format.primary_key(table, &key).unwrap(), primary);
                assert_ne!(format.primary_identity(table, primary), key);
                assert!(previous.as_ref().is_none_or(|previous| previous < &key));
                previous = Some(key.clone());
                assert!(keys.insert(key));
            }
        }
        assert_eq!(keys.len(), parts.len() * parts.len());
        assert_ne!(
            format.constraint_identity("a", "n", &["b/c".into(), "d".into()]),
            format.constraint_identity("a", "n", &["b".into(), "c/d".into()])
        );
        assert_eq!(
            RelationalKeyFormat::LegacyDelimitedV1.row_key("users", "1"),
            b"table/users/pk/1"
        );
        assert!(
            RelationalKeyFormat::detect([
                b"table/a/pk/1".as_slice(),
                format.row_key("a", "1").as_slice()
            ])
            .is_err()
        );
    }
}
