#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{Bytes, CommitVersion, ErrorCode, RsfError, RsfResult};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TupleKey {
    pub table: String,
    pub primary_key: Bytes,
}

impl TupleKey {
    pub fn new(table: impl Into<String>, primary_key: Bytes) -> Self {
        Self {
            table: table.into(),
            primary_key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TupleHeader {
    pub create_version: CommitVersion,
    pub delete_version: Option<CommitVersion>,
    pub schema_version: CommitVersion,
    pub row_bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TupleVersion {
    pub key: TupleKey,
    pub header: TupleHeader,
    pub row: Bytes,
}

impl TupleVersion {
    pub fn visible_at(&self, snapshot: CommitVersion) -> bool {
        self.header.create_version <= snapshot
            && self
                .header
                .delete_version
                .is_none_or(|delete_version| snapshot < delete_version)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TupleChain {
    pub key: TupleKey,
    pub versions: Vec<TupleVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TupleDecision {
    Inserted(TupleVersion),
    Updated {
        previous: TupleHeader,
        current: TupleVersion,
    },
    Deleted(TupleHeader),
    Visible {
        snapshot: CommitVersion,
        version: TupleVersion,
    },
    Invisible {
        snapshot: CommitVersion,
        key: TupleKey,
    },
    Rejected {
        key: TupleKey,
        reason: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct TupleStore {
    chains: BTreeMap<TupleKey, TupleChain>,
}

impl TupleStore {
    pub fn insert(
        &mut self,
        key: TupleKey,
        row: Bytes,
        create_version: CommitVersion,
        schema_version: CommitVersion,
    ) -> RsfResult<TupleDecision> {
        validate_key(&key)?;
        validate_row(&row)?;
        validate_version("create", create_version)?;
        validate_version("schema", schema_version)?;
        if self.visible(&key, create_version).is_some() {
            return Ok(rejected(key, "live tuple already exists at create version"));
        }

        let version = tuple_version(key.clone(), row, create_version, None, schema_version);
        let chain = self
            .chains
            .entry(key.clone())
            .or_insert_with(|| TupleChain {
                key,
                versions: Vec::new(),
            });
        if let Some(last) = chain.versions.last() {
            if create_version <= last.header.create_version {
                return Ok(rejected(
                    chain.key.clone(),
                    "tuple versions must be appended in commit order",
                ));
            }
        }
        chain.versions.push(version.clone());
        Ok(TupleDecision::Inserted(version))
    }

    pub fn update(
        &mut self,
        key: &TupleKey,
        row: Bytes,
        commit_version: CommitVersion,
        schema_version: CommitVersion,
    ) -> RsfResult<TupleDecision> {
        validate_key(key)?;
        validate_row(&row)?;
        validate_version("commit", commit_version)?;
        validate_version("schema", schema_version)?;

        let Some(chain) = self.chains.get_mut(key) else {
            return Ok(rejected(key.clone(), "tuple chain is missing"));
        };
        let Some(previous_index) = visible_index(chain, CommitVersion(commit_version.0 - 1)) else {
            return Ok(rejected(
                key.clone(),
                "no visible tuple version exists before update",
            ));
        };
        if commit_version <= chain.versions[previous_index].header.create_version {
            return Ok(rejected(
                key.clone(),
                "update commit version must advance the tuple chain",
            ));
        }

        chain.versions[previous_index].header.delete_version = Some(commit_version);
        let previous = chain.versions[previous_index].header.clone();
        let current = tuple_version(key.clone(), row, commit_version, None, schema_version);
        chain.versions.push(current.clone());
        Ok(TupleDecision::Updated { previous, current })
    }

    pub fn delete(
        &mut self,
        key: &TupleKey,
        commit_version: CommitVersion,
    ) -> RsfResult<TupleDecision> {
        validate_key(key)?;
        validate_version("commit", commit_version)?;

        let Some(chain) = self.chains.get_mut(key) else {
            return Ok(rejected(key.clone(), "tuple chain is missing"));
        };
        let Some(index) = visible_index(chain, CommitVersion(commit_version.0 - 1)) else {
            return Ok(rejected(
                key.clone(),
                "no visible tuple version exists before delete",
            ));
        };
        chain.versions[index].header.delete_version = Some(commit_version);
        Ok(TupleDecision::Deleted(chain.versions[index].header.clone()))
    }

    pub fn read_visible(&self, key: &TupleKey, snapshot: CommitVersion) -> TupleDecision {
        match self.visible(key, snapshot) {
            Some(version) => TupleDecision::Visible {
                snapshot,
                version: version.clone(),
            },
            None => TupleDecision::Invisible {
                snapshot,
                key: key.clone(),
            },
        }
    }

    pub fn chain(&self, key: &TupleKey) -> Option<&TupleChain> {
        self.chains.get(key)
    }

    pub fn chains_for_table(&self, table: &str) -> Vec<TupleChain> {
        self.chains
            .values()
            .filter(|chain| chain.key.table == table)
            .cloned()
            .collect()
    }

    pub fn replace_chain_versions(
        &mut self,
        key: &TupleKey,
        versions: Vec<TupleVersion>,
    ) -> RsfResult<()> {
        validate_key(key)?;
        if versions.is_empty() {
            return Err(invalid("tuple vacuum must retain at least one version"));
        }
        for version in &versions {
            if version.key != *key {
                return Err(invalid("tuple vacuum version key mismatch"));
            }
            validate_tuple_version(version)?;
        }
        for pair in versions.windows(2) {
            if pair[0].header.create_version >= pair[1].header.create_version {
                return Err(invalid(
                    "tuple vacuum retained versions must stay in commit order",
                ));
            }
        }
        let Some(chain) = self.chains.get_mut(key) else {
            return Err(invalid("tuple vacuum target chain is missing"));
        };
        chain.versions = versions;
        Ok(())
    }

    pub fn version_count(&self) -> usize {
        self.chains
            .values()
            .map(|chain| chain.versions.len())
            .sum::<usize>()
    }

    fn visible(&self, key: &TupleKey, snapshot: CommitVersion) -> Option<&TupleVersion> {
        self.chains.get(key).and_then(|chain| {
            chain
                .versions
                .iter()
                .rev()
                .find(|version| version.visible_at(snapshot))
        })
    }
}

fn tuple_version(
    key: TupleKey,
    row: Bytes,
    create_version: CommitVersion,
    delete_version: Option<CommitVersion>,
    schema_version: CommitVersion,
) -> TupleVersion {
    TupleVersion {
        key,
        header: TupleHeader {
            create_version,
            delete_version,
            schema_version,
            row_bytes: row.len() as u32,
        },
        row,
    }
}

fn visible_index(chain: &TupleChain, snapshot: CommitVersion) -> Option<usize> {
    chain
        .versions
        .iter()
        .enumerate()
        .rev()
        .find_map(|(index, version)| version.visible_at(snapshot).then_some(index))
}

fn validate_key(key: &TupleKey) -> RsfResult<()> {
    if key.table.is_empty() {
        return Err(invalid("tuple table must not be empty"));
    }
    if key.primary_key.is_empty() {
        return Err(invalid("tuple primary key must not be empty"));
    }
    Ok(())
}

fn validate_tuple_version(version: &TupleVersion) -> RsfResult<()> {
    validate_key(&version.key)?;
    validate_row(&version.row)?;
    validate_version("create", version.header.create_version)?;
    validate_version("schema", version.header.schema_version)?;
    if version.header.row_bytes as usize != version.row.len() {
        return Err(invalid(format!(
            "tuple row byte header {} does not match payload length {}",
            version.header.row_bytes,
            version.row.len()
        )));
    }
    if let Some(delete_version) = version.header.delete_version {
        validate_version("delete", delete_version)?;
        if delete_version <= version.header.create_version {
            return Err(invalid(
                "tuple delete version must be greater than create version",
            ));
        }
    }
    Ok(())
}

fn validate_row(row: &[u8]) -> RsfResult<()> {
    if row.is_empty() {
        return Err(invalid("tuple row bytes must not be empty"));
    }
    if row.len() > u32::MAX as usize {
        return Err(invalid("tuple row bytes exceed u32 header capacity"));
    }
    Ok(())
}

fn validate_version(label: &str, version: CommitVersion) -> RsfResult<()> {
    if version.0 == 0 {
        return Err(invalid(format!("{label} version must be non-zero")));
    }
    Ok(())
}

fn rejected(key: TupleKey, reason: impl Into<String>) -> TupleDecision {
    TupleDecision::Rejected {
        key,
        reason: reason.into(),
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_insert_update_delete_visibility() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"7".to_vec());

        assert!(matches!(
            store.insert(
                key.clone(),
                b"ada".to_vec(),
                CommitVersion(10),
                CommitVersion(3)
            ),
            Ok(TupleDecision::Inserted(_))
        ));
        assert!(matches!(
            store.update(
                &key,
                b"ada-lovelace".to_vec(),
                CommitVersion(12),
                CommitVersion(3)
            ),
            Ok(TupleDecision::Updated { .. })
        ));
        assert!(matches!(
            store.read_visible(&key, CommitVersion(11)),
            TupleDecision::Visible { version, .. } if version.row == b"ada"
        ));
        assert!(matches!(
            store.read_visible(&key, CommitVersion(12)),
            TupleDecision::Visible { version, .. } if version.row == b"ada-lovelace"
        ));
        assert!(matches!(
            store.delete(&key, CommitVersion(14)),
            Ok(TupleDecision::Deleted(_))
        ));
        assert!(matches!(
            store.read_visible(&key, CommitVersion(14)),
            TupleDecision::Invisible { .. }
        ));
        assert_eq!(store.version_count(), 2);
    }

    #[test]
    fn rejects_duplicate_live_insert() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"7".to_vec());
        store
            .insert(
                key.clone(),
                b"ada".to_vec(),
                CommitVersion(10),
                CommitVersion(3),
            )
            .unwrap();

        assert!(matches!(
            store.insert(key, b"again".to_vec(), CommitVersion(11), CommitVersion(3)),
            Ok(TupleDecision::Rejected { .. })
        ));
    }

    #[test]
    fn rejects_missing_update() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"missing".to_vec());

        assert!(matches!(
            store.update(&key, b"row".to_vec(), CommitVersion(12), CommitVersion(3)),
            Ok(TupleDecision::Rejected { .. })
        ));
    }

    #[test]
    fn rejects_invalid_metadata() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("", b"7".to_vec());

        assert!(
            store
                .insert(key, b"row".to_vec(), CommitVersion(10), CommitVersion(3))
                .is_err()
        );
    }

    #[test]
    fn replaces_chain_versions_after_vacuum_plan() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"7".to_vec());
        store
            .insert(
                key.clone(),
                b"ada".to_vec(),
                CommitVersion(10),
                CommitVersion(3),
            )
            .unwrap();
        store
            .update(
                &key,
                b"ada-lovelace".to_vec(),
                CommitVersion(12),
                CommitVersion(3),
            )
            .unwrap();
        let retained = store.chain(&key).unwrap().versions[1..].to_vec();

        store.replace_chain_versions(&key, retained).unwrap();

        assert_eq!(store.version_count(), 1);
        assert!(matches!(
            store.read_visible(&key, CommitVersion(12)),
            TupleDecision::Visible { version, .. } if version.row == b"ada-lovelace"
        ));
        assert!(matches!(
            store.read_visible(&key, CommitVersion(11)),
            TupleDecision::Invisible { .. }
        ));
    }

    #[test]
    fn rejects_vacuum_replacement_with_row_byte_mismatch() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"7".to_vec());
        store
            .insert(
                key.clone(),
                b"ada".to_vec(),
                CommitVersion(10),
                CommitVersion(3),
            )
            .unwrap();
        let mut retained = store.chain(&key).unwrap().versions.clone();
        retained[0].header.row_bytes += 1;

        let err = store
            .replace_chain_versions(&key, retained)
            .expect_err("row byte mismatch must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("does not match payload length"));
    }

    #[test]
    fn rejects_vacuum_replacement_with_invalid_delete_order() {
        let mut store = TupleStore::default();
        let key = TupleKey::new("users", b"7".to_vec());
        store
            .insert(
                key.clone(),
                b"ada".to_vec(),
                CommitVersion(10),
                CommitVersion(3),
            )
            .unwrap();
        let mut retained = store.chain(&key).unwrap().versions.clone();
        retained[0].header.delete_version = Some(CommitVersion(10));

        let err = store
            .replace_chain_versions(&key, retained)
            .expect_err("delete version at create version must fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("greater than create version"));
    }
}
