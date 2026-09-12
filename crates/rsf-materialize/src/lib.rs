#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use rsf_core::{
    CommitVersion, ErrorCode, RsfError, RsfResult,
    catalog::{TableDescriptor, TableName},
};
use rsf_query::{Projection, QueryRow, QueryValue};
use rsf_rowcodec::{RowCodec, RowValue};
use rsf_tuple::TupleVersion;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedRow {
    pub row: QueryRow,
    pub schema_version: CommitVersion,
    pub create_version: CommitVersion,
    pub delete_version: Option<CommitVersion>,
    pub bytes: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaterializationDecision {
    Materialized(MaterializedRow),
    Projected(MaterializedRow),
    Rejected { table: TableName, reason: String },
}

#[derive(Clone, Debug, Default)]
pub struct RowMaterializer {
    codec: RowCodec,
}

impl RowMaterializer {
    pub fn materialize_visible(
        &self,
        table: &TableDescriptor,
        tuple: &TupleVersion,
        projection: &Projection,
        snapshot: CommitVersion,
    ) -> MaterializationDecision {
        if !tuple.visible_at(snapshot) {
            return rejected(
                table.name.clone(),
                format!(
                    "tuple version created at {} and deleted at {} is not visible at snapshot {}",
                    tuple.header.create_version.0,
                    tuple
                        .header
                        .delete_version
                        .map(|version| version.0.to_string())
                        .unwrap_or_else(|| "none".to_string()),
                    snapshot.0
                ),
            );
        }
        self.materialize(table, tuple, projection)
    }

    pub fn materialize(
        &self,
        table: &TableDescriptor,
        tuple: &TupleVersion,
        projection: &Projection,
    ) -> MaterializationDecision {
        if tuple.key.table != table.name.0 {
            return rejected(
                table.name.clone(),
                format!(
                    "tuple table {} does not match descriptor {}",
                    tuple.key.table, table.name.0
                ),
            );
        }

        let decoded = match self.codec.decode(table, &tuple.row) {
            Ok(decoded) => decoded,
            Err(error) => return rejected(table.name.clone(), error.message),
        };
        if decoded.schema_version != tuple.header.schema_version {
            return rejected(
                table.name.clone(),
                format!(
                    "tuple schema version {} does not match row schema version {}",
                    tuple.header.schema_version.0, decoded.schema_version.0
                ),
            );
        }

        let row = match project_values(table.name.clone(), &decoded.values, projection) {
            Ok(row) => row,
            Err(error) => return rejected(table.name.clone(), error.message),
        };
        let materialized = MaterializedRow {
            row,
            schema_version: decoded.schema_version,
            create_version: tuple.header.create_version,
            delete_version: tuple.header.delete_version,
            bytes: tuple.header.row_bytes,
        };

        match projection {
            Projection::All => MaterializationDecision::Materialized(materialized),
            Projection::Columns(_) => MaterializationDecision::Projected(materialized),
        }
    }
}

fn project_values(
    table: TableName,
    values: &BTreeMap<String, RowValue>,
    projection: &Projection,
) -> RsfResult<QueryRow> {
    let columns = match projection {
        Projection::All => values.keys().cloned().collect::<Vec<_>>(),
        Projection::Columns(columns) => {
            if columns.is_empty() {
                return Err(invalid("projection must include at least one column"));
            }
            columns.clone()
        }
    };

    let mut projected = BTreeMap::new();
    for column in columns {
        let Some(value) = values.get(&column) else {
            return Err(invalid(format!(
                "decoded row is missing projected column {column}"
            )));
        };
        let Some(query_value) = query_value(value) else {
            return Err(invalid(format!(
                "projected column {column} is null and cannot be materialized"
            )));
        };
        projected.insert(column, query_value);
    }
    Ok(QueryRow {
        table,
        columns: projected,
    })
}

fn query_value(value: &RowValue) -> Option<QueryValue> {
    match value {
        RowValue::Null => None,
        RowValue::Int64(value) => Some(QueryValue::Int64(*value)),
        RowValue::Text(value) => Some(QueryValue::Text(value.clone())),
        RowValue::Bool(value) => Some(QueryValue::Bool(*value)),
        RowValue::Bytes(value) => Some(QueryValue::Bytes(value.clone())),
    }
}

fn rejected(table: TableName, reason: impl Into<String>) -> MaterializationDecision {
    MaterializationDecision::Rejected {
        table,
        reason: reason.into(),
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use rsf_core::catalog::{ColumnDescriptor, ColumnType, NamespaceName};
    use rsf_rowcodec::{RowCodecPolicy, RowValue};
    use rsf_tuple::TupleKey;

    use super::*;

    #[test]
    fn materializes_encoded_tuple_to_query_row() {
        let table = table();
        let tuple = tuple(&table, CommitVersion(10), None);

        let decision = RowMaterializer::default().materialize(&table, &tuple, &Projection::All);

        assert!(matches!(
            decision,
            MaterializationDecision::Materialized(MaterializedRow { ref row, schema_version, create_version, .. })
                if row.columns.get("name") == Some(&QueryValue::Text("ada".to_string()))
                    && schema_version == CommitVersion(3)
                    && create_version == CommitVersion(10)
        ));
    }

    #[test]
    fn applies_projection() {
        let table = table();
        let tuple = tuple(&table, CommitVersion(10), None);

        let decision = RowMaterializer::default().materialize(
            &table,
            &tuple,
            &Projection::Columns(vec!["name".to_string()]),
        );

        assert!(matches!(
            decision,
            MaterializationDecision::Projected(MaterializedRow { ref row, .. })
                if row.columns.len() == 1 && row.columns.contains_key("name")
        ));
    }

    #[test]
    fn rejects_table_mismatch() {
        let table = table();
        let mut tuple = tuple(&table, CommitVersion(10), None);
        tuple.key = TupleKey::new("orders", b"id=7".to_vec());

        assert!(matches!(
            RowMaterializer::default().materialize(&table, &tuple, &Projection::All),
            MaterializationDecision::Rejected { .. }
        ));
    }

    #[test]
    fn rejects_unknown_projection_column() {
        let table = table();
        let tuple = tuple(&table, CommitVersion(10), None);

        assert!(matches!(
            RowMaterializer::default().materialize(
                &table,
                &tuple,
                &Projection::Columns(vec!["missing".to_string()])
            ),
            MaterializationDecision::Rejected { .. }
        ));
    }

    #[test]
    fn materializes_visible_tuple_at_snapshot() {
        let table = table();
        let tuple = tuple(&table, CommitVersion(10), Some(CommitVersion(20)));

        let decision = RowMaterializer::default().materialize_visible(
            &table,
            &tuple,
            &Projection::All,
            CommitVersion(19),
        );

        assert!(matches!(
            decision,
            MaterializationDecision::Materialized(MaterializedRow {
                create_version: CommitVersion(10),
                delete_version: Some(CommitVersion(20)),
                ..
            })
        ));
    }

    #[test]
    fn rejects_tuple_created_after_snapshot() {
        let table = table();
        let tuple = tuple(&table, CommitVersion(10), None);

        let decision = RowMaterializer::default().materialize_visible(
            &table,
            &tuple,
            &Projection::All,
            CommitVersion(9),
        );

        assert!(matches!(
            decision,
            MaterializationDecision::Rejected { reason, .. }
                if reason.contains("not visible at snapshot 9")
        ));
    }

    #[test]
    fn rejects_tuple_deleted_at_snapshot() {
        let table = table();
        let tuple = tuple(&table, CommitVersion(10), Some(CommitVersion(20)));

        let decision = RowMaterializer::default().materialize_visible(
            &table,
            &tuple,
            &Projection::All,
            CommitVersion(20),
        );

        assert!(matches!(
            decision,
            MaterializationDecision::Rejected { reason, .. }
                if reason.contains("not visible at snapshot 20")
        ));
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

    fn tuple(
        table: &TableDescriptor,
        create_version: CommitVersion,
        delete_version: Option<CommitVersion>,
    ) -> TupleVersion {
        let encoded = RowCodec
            .encode(
                table,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                    ("active".to_string(), RowValue::Bool(true)),
                ]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .expect("encoded row");
        TupleVersion {
            key: TupleKey::new("users", b"id=7".to_vec()),
            header: rsf_tuple::TupleHeader {
                create_version,
                delete_version,
                schema_version: CommitVersion(3),
                row_bytes: encoded.bytes.len() as u32,
            },
            row: encoded.bytes,
        }
    }
}
