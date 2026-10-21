#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    Bytes, ErrorCode, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
};

pub type RowValues = BTreeMap<String, String>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexDescriptor {
    pub table: TableName,
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowImage {
    pub primary_key: String,
    pub values: RowValues,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexMutation {
    Put {
        index: String,
        key: Bytes,
        primary_key: String,
    },
    Delete {
        index: String,
        key: Bytes,
        primary_key: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexPlan {
    pub table: TableName,
    pub mutations: Vec<IndexMutation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildPlan {
    pub table: TableName,
    pub index: String,
    pub scanned_rows: usize,
    pub mutations: Vec<IndexMutation>,
}

#[derive(Clone, Debug, Default)]
pub struct IndexStore {
    entries: BTreeMap<(String, Bytes), BTreeSet<String>>,
}

#[derive(Clone, Debug)]
pub struct IndexPlanner {
    descriptors: Vec<IndexDescriptor>,
}

impl IndexPlanner {
    pub fn new(table: &TableDescriptor, descriptors: Vec<IndexDescriptor>) -> RsfResult<Self> {
        let mut names = BTreeSet::new();
        for descriptor in &descriptors {
            validate_descriptor(table, descriptor)?;
            if !names.insert(descriptor.name.as_str()) {
                return Err(invalid(format!(
                    "index descriptor {} is declared more than once",
                    descriptor.name
                )));
            }
        }
        Ok(Self { descriptors })
    }

    pub fn plan_insert(&self, table: &TableDescriptor, row: &RowImage) -> RsfResult<IndexPlan> {
        self.plan(table, None, Some(row))
    }

    pub fn plan_update(
        &self,
        table: &TableDescriptor,
        before: &RowImage,
        after: &RowImage,
    ) -> RsfResult<IndexPlan> {
        self.plan(table, Some(before), Some(after))
    }

    pub fn plan_delete(&self, table: &TableDescriptor, row: &RowImage) -> RsfResult<IndexPlan> {
        self.plan(table, Some(row), None)
    }

    pub fn plan_rebuild(
        &self,
        descriptor: &IndexDescriptor,
        rows: &[RowImage],
    ) -> RsfResult<RebuildPlan> {
        if !self.descriptors.iter().any(|known| known == descriptor) {
            return Err(invalid(format!(
                "unknown index descriptor {} cannot be rebuilt",
                descriptor.name
            )));
        }
        index_scope(&descriptor.table, &descriptor.name)?;
        let mut mutations = Vec::new();
        for row in rows {
            mutations.push(IndexMutation::Put {
                index: descriptor.name.clone(),
                key: encode_index_key(descriptor, row)?,
                primary_key: row.primary_key.clone(),
            });
        }
        Ok(RebuildPlan {
            table: descriptor.table.clone(),
            index: descriptor.name.clone(),
            scanned_rows: rows.len(),
            mutations,
        })
    }

    fn plan(
        &self,
        table: &TableDescriptor,
        before: Option<&RowImage>,
        after: Option<&RowImage>,
    ) -> RsfResult<IndexPlan> {
        let mut mutations = Vec::new();
        for descriptor in &self.descriptors {
            validate_descriptor(table, descriptor)?;
            if let Some(row) = before {
                mutations.push(IndexMutation::Delete {
                    index: descriptor.name.clone(),
                    key: encode_index_key(descriptor, row)?,
                    primary_key: row.primary_key.clone(),
                });
            }
            if let Some(row) = after {
                mutations.push(IndexMutation::Put {
                    index: descriptor.name.clone(),
                    key: encode_index_key(descriptor, row)?,
                    primary_key: row.primary_key.clone(),
                });
            }
        }
        Ok(IndexPlan {
            table: table.name.clone(),
            mutations,
        })
    }
}

impl IndexStore {
    pub fn apply(&mut self, plan: &IndexPlan) {
        for mutation in &plan.mutations {
            self.apply_mutation(mutation);
        }
    }

    pub fn apply_rebuild(&mut self, plan: &RebuildPlan) -> RsfResult<()> {
        let scope = index_scope(&plan.table, &plan.index)?;
        for mutation in &plan.mutations {
            let (index, key) = match mutation {
                IndexMutation::Put { index, key, .. }
                | IndexMutation::Delete { index, key, .. } => (index, key),
            };
            if index != &plan.index || !key.starts_with(&scope) {
                return Err(invalid(
                    "index rebuild contains a foreign table/index mutation",
                ));
            }
        }
        self.entries
            .retain(|(index, key), _| index != &plan.index || !key.starts_with(&scope));
        for mutation in &plan.mutations {
            self.apply_mutation(mutation);
        }
        Ok(())
    }

    /// Borrow matching primary keys in byte order without collecting the set.
    pub fn lookup_iter<'a>(
        &'a self,
        index: &str,
        key: &[u8],
    ) -> impl Iterator<Item = &'a str> + use<'a> {
        self.entries
            .get(&(index.to_string(), key.to_vec()))
            .into_iter()
            .flat_map(|keys| keys.iter().map(String::as_str))
    }

    pub fn lookup(&self, index: &str, key: &[u8]) -> Vec<String> {
        self.lookup_iter(index, key).map(str::to_owned).collect()
    }

    pub fn entry_count(&self) -> usize {
        self.entries
            .values()
            .map(std::collections::BTreeSet::len)
            .sum()
    }

    fn apply_mutation(&mut self, mutation: &IndexMutation) {
        match mutation {
            IndexMutation::Put {
                index,
                key,
                primary_key,
            } => {
                self.entries
                    .entry((index.clone(), key.clone()))
                    .or_default()
                    .insert(primary_key.clone());
            }
            IndexMutation::Delete {
                index,
                key,
                primary_key,
            } => {
                let map_key = (index.clone(), key.clone());
                if let Some(primary_keys) = self.entries.get_mut(&map_key) {
                    primary_keys.remove(primary_key);
                    if primary_keys.is_empty() {
                        self.entries.remove(&map_key);
                    }
                }
            }
        }
    }
}

pub fn encode_index_key(descriptor: &IndexDescriptor, row: &RowImage) -> RsfResult<Bytes> {
    let mut key = index_scope(&descriptor.table, &descriptor.name)?;
    push_u16(&mut key, descriptor.columns.len())?;
    for column in &descriptor.columns {
        let value = row.values.get(column).ok_or_else(|| {
            invalid(format!(
                "row {} is missing indexed column {column}",
                row.primary_key
            ))
        })?;
        push_bytes(&mut key, value.as_bytes())?;
    }
    Ok(key)
}
fn index_scope(table: &TableName, index: &str) -> RsfResult<Bytes> {
    let mut prefix = b"idx\0".to_vec();
    push_bytes(&mut prefix, table.0.as_bytes())?;
    push_bytes(&mut prefix, index.as_bytes())?;
    Ok(prefix)
}

fn validate_descriptor(table: &TableDescriptor, descriptor: &IndexDescriptor) -> RsfResult<()> {
    if descriptor.table != table.name {
        return Err(invalid(format!(
            "index {} targets table {}, not {}",
            descriptor.name, descriptor.table.0, table.name.0
        )));
    }
    if descriptor.name.trim().is_empty() {
        return Err(invalid("index name must not be empty"));
    }
    if descriptor.columns.is_empty() {
        return Err(invalid("index must include at least one column"));
    }
    let mut columns = BTreeSet::new();
    for column in &descriptor.columns {
        if !columns.insert(column) {
            return Err(invalid(format!(
                "index column {column} is declared more than once"
            )));
        }
        if !table.columns.iter().any(|known| known.name == *column) {
            return Err(invalid(format!(
                "index column {column} is not in table {}",
                table.name.0
            )));
        }
    }
    Ok(())
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> RsfResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| invalid("index key part is too large"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn push_u16(out: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u16::try_from(value).map_err(|_| invalid("index key has too many columns"))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    #[test]
    fn borrowed_index_lookup_preserves_order_scope_and_storage_identity() {
        let table = table();
        let index = users_name_index();
        let planner = IndexPlanner::new(&table, vec![index.clone()]).unwrap();
        let mut store = IndexStore::default();
        for id in ["3", "1", "2"] {
            store.apply(
                &planner
                    .plan_insert(&table, &row(id, "shared", true))
                    .unwrap(),
            );
        }
        let key = encode_index_key(&index, &row("ignored", "shared", true)).unwrap();
        let borrowed = store.lookup_iter(&index.name, &key).collect::<Vec<_>>();
        assert_eq!(borrowed, ["1", "2", "3"]);
        let stored = store
            .entries
            .get(&(index.name.clone(), key.clone()))
            .unwrap()
            .first()
            .unwrap();
        assert!(std::ptr::eq(borrowed[0].as_ptr(), stored.as_ptr()));
        assert_eq!(store.lookup_iter(&index.name, &key).take(1).count(), 1);
        assert_eq!(store.lookup_iter("foreign", &key).count(), 0);
        assert_eq!(store.lookup_iter(&index.name, b"foreign").count(), 0);
    }

    use super::*;

    #[test]
    fn rebuilds_are_scoped_by_table_and_reject_foreign_mutations_before_clearing() {
        let left = table();
        let mut right = left.clone();
        right.name = TableName("orders".into());
        let mut left_index = users_name_index();
        left_index.name = "shared".into();
        let mut right_index = left_index.clone();
        right_index.table = right.name.clone();
        let left_planner = IndexPlanner::new(&left, vec![left_index.clone()]).unwrap();
        let right_planner = IndexPlanner::new(&right, vec![right_index.clone()]).unwrap();
        let a = row("1", "left", true);
        let b = row("2", "right", true);
        let mut store = IndexStore::default();
        store.apply(&left_planner.plan_insert(&left, &a).unwrap());
        store.apply(&right_planner.plan_insert(&right, &b).unwrap());
        let rebuild = left_planner
            .plan_rebuild(&left_index, std::slice::from_ref(&a))
            .unwrap();
        store.apply_rebuild(&rebuild).unwrap();
        assert_eq!(store.entry_count(), 2);
        assert_eq!(
            store.lookup("shared", &encode_index_key(&right_index, &b).unwrap()),
            vec!["2"]
        );
        assert!(left_planner.plan_rebuild(&right_index, &[b]).is_err());
        let before = store.entries.clone();
        let mut foreign = rebuild;
        foreign.table = right.name;
        assert!(store.apply_rebuild(&foreign).is_err());
        assert_eq!(store.entries, before);
    }
    use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName};

    #[test]
    fn insert_update_and_delete_plan_secondary_index_mutations() {
        let table = table();
        let descriptor = users_name_index();
        let planner = IndexPlanner::new(&table, vec![descriptor.clone()]).expect("planner");
        let mut store = IndexStore::default();

        let ada = row("1", "ada", true);
        let insert = planner.plan_insert(&table, &ada).expect("insert");
        store.apply(&insert);
        assert_eq!(
            store.lookup(
                &descriptor.name,
                &encode_index_key(&descriptor, &ada).unwrap()
            ),
            vec!["1".to_string()]
        );

        let grace = row("1", "grace", true);
        let update = planner.plan_update(&table, &ada, &grace).expect("update");
        store.apply(&update);
        assert!(
            store
                .lookup(
                    &descriptor.name,
                    &encode_index_key(&descriptor, &ada).unwrap()
                )
                .is_empty()
        );
        assert_eq!(
            store.lookup(
                &descriptor.name,
                &encode_index_key(&descriptor, &grace).unwrap()
            ),
            vec!["1".to_string()]
        );

        let delete = planner.plan_delete(&table, &grace).expect("delete");
        store.apply(&delete);
        assert_eq!(store.entry_count(), 0);
    }

    #[test]
    fn rebuild_replaces_existing_entries_for_one_index() {
        let table = table();
        let descriptor = users_name_index();
        let planner = IndexPlanner::new(&table, vec![descriptor.clone()]).expect("planner");
        let mut store = IndexStore::default();
        let stale = row("old", "stale", true);
        store.apply(&planner.plan_insert(&table, &stale).unwrap());

        let rows = [row("1", "ada", true), row("2", "grace", false)];
        let rebuild = planner.plan_rebuild(&descriptor, &rows).expect("rebuild");
        store.apply_rebuild(&rebuild).unwrap();

        assert_eq!(rebuild.scanned_rows, 2);
        assert_eq!(store.entry_count(), 2);
        assert!(
            store
                .lookup(
                    &descriptor.name,
                    &encode_index_key(&descriptor, &stale).unwrap()
                )
                .is_empty()
        );
    }

    #[test]
    fn descriptor_rejects_missing_table_column() {
        let table = table();
        let descriptor = IndexDescriptor {
            table: table.name.clone(),
            name: "users_missing".to_string(),
            columns: vec!["missing".to_string()],
            unique: false,
        };

        let err = IndexPlanner::new(&table, vec![descriptor]).expect_err("invalid descriptor");
        assert!(err.message.contains("not in table"));
    }

    #[test]
    fn key_encoding_rejects_rows_missing_indexed_column() {
        let descriptor = users_name_index();
        let row = RowImage {
            primary_key: "1".to_string(),
            values: BTreeMap::new(),
        };

        let err = encode_index_key(&descriptor, &row).expect_err("missing column");
        assert!(err.message.contains("missing indexed column"));
    }

    #[test]
    fn composite_index_keys_are_length_delimited() {
        let descriptor = IndexDescriptor {
            table: TableName("users".to_string()),
            name: "users_name_active".to_string(),
            columns: vec!["name".to_string(), "active".to_string()],
            unique: false,
        };
        let left = RowImage {
            primary_key: "1".to_string(),
            values: BTreeMap::from([
                ("name".to_string(), "a/b".to_string()),
                ("active".to_string(), "c".to_string()),
            ]),
        };
        let right = RowImage {
            primary_key: "2".to_string(),
            values: BTreeMap::from([
                ("name".to_string(), "a".to_string()),
                ("active".to_string(), "b/c".to_string()),
            ]),
        };

        let left_key = encode_index_key(&descriptor, &left).expect("left key");
        let right_key = encode_index_key(&descriptor, &right).expect("right key");

        assert_ne!(left_key, right_key);
        assert!(left_key.starts_with(b"idx\0"));
        assert!(right_key.starts_with(b"idx\0"));
    }

    #[test]
    fn duplicate_index_descriptors_are_rejected() {
        let table = table();
        let descriptor = users_name_index();

        let err = IndexPlanner::new(&table, vec![descriptor.clone(), descriptor])
            .expect_err("duplicate descriptor");

        assert!(err.message.contains("declared more than once"));
    }

    #[test]
    fn duplicate_index_columns_are_rejected() {
        let table = table();
        let descriptor = IndexDescriptor {
            table: table.name.clone(),
            name: "users_bad".to_string(),
            columns: vec!["name".to_string(), "name".to_string()],
            unique: false,
        };

        let err = IndexPlanner::new(&table, vec![descriptor]).expect_err("duplicate column");

        assert!(err.message.contains("declared more than once"));
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
                ColumnDescriptor {
                    name: "active".to_string(),
                    ty: ColumnType::Bool,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }

    fn users_name_index() -> IndexDescriptor {
        IndexDescriptor {
            table: TableName("users".to_string()),
            name: "users_name".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
        }
    }

    fn row(primary_key: &str, name: &str, active: bool) -> RowImage {
        RowImage {
            primary_key: primary_key.to_string(),
            values: BTreeMap::from([
                ("id".to_string(), primary_key.to_string()),
                ("name".to_string(), name.to_string()),
                ("active".to_string(), active.to_string()),
            ]),
        }
    }
}
