#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    Bytes, CommitVersion, ErrorCode, PlacementGroupId, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryKeyDescriptor {
    pub table: TableName,
    pub columns: Vec<String>,
}

impl PrimaryKeyDescriptor {
    pub fn from_table(table: &TableDescriptor) -> RsfResult<Self> {
        if table.primary_key.is_empty() {
            return Err(invalid("table must define a primary key"));
        }
        let mut seen = BTreeSet::new();
        for column in &table.primary_key {
            if !seen.insert(column) {
                return Err(invalid(format!(
                    "primary key column {column} is declared more than once"
                )));
            }
            if !table.columns.iter().any(|known| known.name == *column) {
                return Err(invalid(format!(
                    "primary key column {column} is not in table {}",
                    table.name.0
                )));
            }
        }
        Ok(Self {
            table: table.name.clone(),
            columns: table.primary_key.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryRow {
    pub values: BTreeMap<String, String>,
    pub row_key: Bytes,
    pub placement_group: PlacementGroupId,
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryEntry {
    pub encoded_key: Bytes,
    pub row_key: Bytes,
    pub placement_group: PlacementGroupId,
    pub version: CommitVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrimaryDecision {
    Inserted(PrimaryEntry),
    Updated {
        before: PrimaryEntry,
        after: PrimaryEntry,
    },
    Deleted(PrimaryEntry),
    Found(PrimaryEntry),
    Missing {
        encoded_key: Bytes,
    },
    Rejected {
        encoded_key: Bytes,
        reason: String,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrimaryIndex {
    entries: BTreeMap<Bytes, PrimaryEntry>,
}

impl PrimaryIndex {
    pub fn insert(
        &mut self,
        descriptor: &PrimaryKeyDescriptor,
        row: PrimaryRow,
    ) -> RsfResult<PrimaryDecision> {
        let encoded_key = encode_primary_key(descriptor, &row.values)?;
        if self.entries.contains_key(&encoded_key) {
            return Ok(PrimaryDecision::Rejected {
                encoded_key,
                reason: "primary key already exists".to_string(),
            });
        }
        let entry = PrimaryEntry {
            encoded_key: encoded_key.clone(),
            row_key: row.row_key,
            placement_group: row.placement_group,
            version: row.version,
        };
        self.entries.insert(encoded_key, entry.clone());
        Ok(PrimaryDecision::Inserted(entry))
    }

    pub fn update(
        &mut self,
        descriptor: &PrimaryKeyDescriptor,
        expected_key: &[String],
        row: PrimaryRow,
    ) -> RsfResult<PrimaryDecision> {
        let encoded_key = encode_key_parts(descriptor, expected_key)?;
        let Some(before) = self.entries.get(&encoded_key).cloned() else {
            return Ok(PrimaryDecision::Missing { encoded_key });
        };
        let after_key = encode_primary_key(descriptor, &row.values)?;
        if after_key != encoded_key {
            return Ok(PrimaryDecision::Rejected {
                encoded_key: after_key,
                reason: "primary key update would move row identity".to_string(),
            });
        }
        let after = PrimaryEntry {
            encoded_key: encoded_key.clone(),
            row_key: row.row_key,
            placement_group: row.placement_group,
            version: row.version,
        };
        self.entries.insert(encoded_key, after.clone());
        Ok(PrimaryDecision::Updated { before, after })
    }

    pub fn delete(
        &mut self,
        descriptor: &PrimaryKeyDescriptor,
        key_parts: &[String],
    ) -> RsfResult<PrimaryDecision> {
        let encoded_key = encode_key_parts(descriptor, key_parts)?;
        let Some(entry) = self.entries.remove(&encoded_key) else {
            return Ok(PrimaryDecision::Missing { encoded_key });
        };
        Ok(PrimaryDecision::Deleted(entry))
    }

    pub fn lookup(
        &self,
        descriptor: &PrimaryKeyDescriptor,
        key_parts: &[String],
    ) -> RsfResult<PrimaryDecision> {
        let encoded_key = encode_key_parts(descriptor, key_parts)?;
        Ok(self
            .entries
            .get(&encoded_key)
            .cloned()
            .map(PrimaryDecision::Found)
            .unwrap_or(PrimaryDecision::Missing { encoded_key }))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub fn encode_primary_key(
    descriptor: &PrimaryKeyDescriptor,
    values: &BTreeMap<String, String>,
) -> RsfResult<Bytes> {
    let mut parts = Vec::new();
    for column in &descriptor.columns {
        let value = values.get(column).ok_or_else(|| {
            invalid(format!(
                "primary key column {column} is missing for table {}",
                descriptor.table.0
            ))
        })?;
        parts.push(value.clone());
    }
    encode_key_parts(descriptor, &parts)
}

pub fn encode_key_parts(
    descriptor: &PrimaryKeyDescriptor,
    key_parts: &[String],
) -> RsfResult<Bytes> {
    if key_parts.len() != descriptor.columns.len() {
        return Err(invalid(format!(
            "primary key for table {} expects {} parts, got {}",
            descriptor.table.0,
            descriptor.columns.len(),
            key_parts.len()
        )));
    }
    let mut key = b"pk\0".to_vec();
    push_bytes(&mut key, descriptor.table.0.as_bytes())?;
    push_u16(&mut key, key_parts.len())?;
    for part in key_parts {
        if part.is_empty() {
            return Err(invalid("primary key values must be non-empty"));
        }
        push_bytes(&mut key, part.as_bytes())?;
    }
    Ok(key)
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> RsfResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("primary key part is too large"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_u16(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u16::try_from(value).map_err(|_| invalid("primary key has too many columns"))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName};

    use super::*;

    #[test]
    fn insert_lookup_update_and_delete_primary_entry() {
        let descriptor = PrimaryKeyDescriptor::from_table(&table()).expect("descriptor");
        let mut index = PrimaryIndex::default();
        let original = row("1", "ada", 11);

        assert!(matches!(
            index.insert(&descriptor, original).expect("insert"),
            PrimaryDecision::Inserted(_)
        ));
        assert!(matches!(
            index
                .lookup(&descriptor, &["1".to_string()])
                .expect("lookup"),
            PrimaryDecision::Found(_)
        ));

        match index
            .update(
                &descriptor,
                &["1".to_string()],
                row("1", "ada-lovelace", 12),
            )
            .expect("update")
        {
            PrimaryDecision::Updated { before, after } => {
                assert_eq!(before.version, CommitVersion(11));
                assert_eq!(after.version, CommitVersion(12));
            }
            other => panic!("expected update, got {other:?}"),
        }

        assert!(matches!(
            index
                .delete(&descriptor, &["1".to_string()])
                .expect("delete"),
            PrimaryDecision::Deleted(_)
        ));
        assert!(index.is_empty());
    }

    #[test]
    fn duplicate_primary_key_rejects_insert() {
        let descriptor = PrimaryKeyDescriptor::from_table(&table()).expect("descriptor");
        let mut index = PrimaryIndex::default();
        index
            .insert(&descriptor, row("1", "ada", 1))
            .expect("first insert");

        match index
            .insert(&descriptor, row("1", "different", 2))
            .expect("duplicate insert")
        {
            PrimaryDecision::Rejected { reason, .. } => {
                assert!(reason.contains("already exists"));
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn primary_key_update_cannot_move_identity() {
        let descriptor = PrimaryKeyDescriptor::from_table(&table()).expect("descriptor");
        let mut index = PrimaryIndex::default();
        index
            .insert(&descriptor, row("1", "ada", 1))
            .expect("first insert");

        match index
            .update(&descriptor, &["1".to_string()], row("2", "ada", 2))
            .expect("move identity")
        {
            PrimaryDecision::Rejected { reason, .. } => {
                assert!(reason.contains("move row identity"));
            }
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn missing_primary_key_part_rejects_encoding() {
        let descriptor = PrimaryKeyDescriptor::from_table(&table()).expect("descriptor");
        let err = encode_primary_key(&descriptor, &BTreeMap::new()).expect_err("missing key");
        assert!(err.message.contains("missing"));
    }

    #[test]
    fn composite_primary_keys_are_length_delimited() {
        let descriptor = PrimaryKeyDescriptor::from_table(&composite_table()).expect("descriptor");

        let left =
            encode_key_parts(&descriptor, &["a/b".to_string(), "c".to_string()]).expect("left key");
        let right = encode_key_parts(&descriptor, &["a".to_string(), "b/c".to_string()])
            .expect("right key");

        assert_ne!(left, right);
        assert!(left.starts_with(b"pk\0"));
        assert!(right.starts_with(b"pk\0"));
    }

    #[test]
    fn duplicate_primary_key_columns_are_rejected() {
        let mut table = composite_table();
        table.primary_key = vec!["tenant".to_string(), "tenant".to_string()];

        let err = PrimaryKeyDescriptor::from_table(&table).expect_err("duplicate column");
        assert!(err.message.contains("more than once"));
    }

    fn table() -> TableDescriptor {
        TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("users".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "name".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn composite_table() -> TableDescriptor {
        TableDescriptor {
            namespace: NamespaceName("app".to_string()),
            name: TableName("events".to_string()),
            columns: vec![
                ColumnDescriptor {
                    name: "tenant".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "id".to_string(),
                    ty: ColumnType::Text,
                    nullable: false,
                },
                ColumnDescriptor {
                    name: "payload".to_string(),
                    ty: ColumnType::Bytes,
                    nullable: false,
                },
            ],
            primary_key: vec!["tenant".to_string(), "id".to_string()],
        }
    }

    fn row(id: &str, name: &str, version: u64) -> PrimaryRow {
        PrimaryRow {
            values: BTreeMap::from([
                ("id".to_string(), id.to_string()),
                ("name".to_string(), name.to_string()),
            ]),
            row_key: format!("row/users/{id}").into_bytes(),
            placement_group: PlacementGroupId(1),
            version: CommitVersion(version),
        }
    }
}
