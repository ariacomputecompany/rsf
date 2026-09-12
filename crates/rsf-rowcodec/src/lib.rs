#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use rsf_core::{
    CommitVersion, ErrorCode, RsfError, RsfResult,
    catalog::{ColumnDescriptor, ColumnType, TableDescriptor},
};

const MAGIC: &[u8; 5] = b"RSFR1";
const TYPE_NULL: u8 = 0;
const TYPE_INT64: u8 = 1;
const TYPE_TEXT: u8 = 2;
const TYPE_BOOL: u8 = 3;
const TYPE_BYTES: u8 = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowValue {
    Null,
    Int64(i64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GeneratedColumn {
    TextLength { column: String, source: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowCodecPolicy {
    pub schema_version: CommitVersion,
    pub defaults: BTreeMap<String, RowValue>,
    pub generated: Vec<GeneratedColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedRow {
    pub table: String,
    pub schema_version: CommitVersion,
    pub bytes: Vec<u8>,
    pub defaults_applied: usize,
    pub generated_applied: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedRow {
    pub table: String,
    pub schema_version: CommitVersion,
    pub values: BTreeMap<String, RowValue>,
}

#[derive(Clone, Debug, Default)]
pub struct RowCodec;

impl RowCodec {
    pub fn encode(
        &self,
        table: &TableDescriptor,
        input: BTreeMap<String, RowValue>,
        policy: RowCodecPolicy,
    ) -> RsfResult<EncodedRow> {
        validate_descriptor(table)?;
        let mut values = input;
        let mut defaults_applied = 0;
        let mut generated_applied = 0;

        for (column, value) in &policy.defaults {
            if !values.contains_key(column) {
                ensure_column(table, column)?;
                values.insert(column.clone(), value.clone());
                defaults_applied += 1;
            }
        }

        for generated in &policy.generated {
            match generated {
                GeneratedColumn::TextLength { column, source } => {
                    let source_value = values.get(source).ok_or_else(|| {
                        invalid(format!("generated source column {source} is missing"))
                    })?;
                    let length = match source_value {
                        RowValue::Text(value) => value.len() as i64,
                        RowValue::Bytes(value) => value.len() as i64,
                        other => {
                            return Err(invalid(format!(
                                "generated source column {source} must be text or bytes, got {}",
                                value_type_name(other)
                            )));
                        }
                    };
                    ensure_column(table, column)?;
                    values.insert(column.clone(), RowValue::Int64(length));
                    generated_applied += 1;
                }
            }
        }

        for column in values.keys() {
            ensure_column(table, column)?;
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&policy.schema_version.0.to_be_bytes());
        push_u16(&mut bytes, table.columns.len())?;

        for column in &table.columns {
            let value = values.get(&column.name).cloned().unwrap_or(RowValue::Null);
            validate_column_value(column, &value)?;
            let value_bytes = value_payload(&value);
            push_string(&mut bytes, &column.name)?;
            bytes.push(type_tag(&value));
            push_u32(&mut bytes, value_bytes.len())?;
            bytes.extend_from_slice(&value_bytes);
        }

        Ok(EncodedRow {
            table: table.name.0.clone(),
            schema_version: policy.schema_version,
            bytes,
            defaults_applied,
            generated_applied,
        })
    }

    pub fn decode(&self, table: &TableDescriptor, bytes: &[u8]) -> RsfResult<DecodedRow> {
        validate_descriptor(table)?;
        let mut cursor = Cursor::new(bytes);
        cursor.expect_magic(MAGIC)?;
        let schema_version = CommitVersion(cursor.u64()?);
        let column_count = cursor.u16()? as usize;
        if column_count != table.columns.len() {
            return Err(invalid(format!(
                "encoded column count {column_count} does not match descriptor count {}",
                table.columns.len()
            )));
        }

        let mut values = BTreeMap::new();
        for expected in &table.columns {
            let name = cursor.string()?;
            if name != expected.name {
                return Err(invalid(format!(
                    "encoded column {name} does not match expected {}",
                    expected.name
                )));
            }
            let tag = cursor.u8()?;
            let payload = cursor.bytes()?;
            let value = decode_value(tag, payload)?;
            validate_column_value(expected, &value)?;
            values.insert(name, value);
        }
        cursor.finish()?;

        Ok(DecodedRow {
            table: table.name.0.clone(),
            schema_version,
            values,
        })
    }
}

fn validate_descriptor(table: &TableDescriptor) -> RsfResult<()> {
    let mut names = BTreeSet::new();
    for column in &table.columns {
        if column.name.trim().is_empty() {
            return Err(invalid(format!(
                "table {} contains a blank column name",
                table.name.0
            )));
        }
        if !names.insert(column.name.as_str()) {
            return Err(invalid(format!(
                "table {} contains duplicate column {}",
                table.name.0, column.name
            )));
        }
    }
    Ok(())
}

fn ensure_column(table: &TableDescriptor, column: &str) -> RsfResult<()> {
    if table
        .columns
        .iter()
        .any(|descriptor| descriptor.name == column)
    {
        Ok(())
    } else {
        Err(invalid(format!(
            "column {column} is not in table {}",
            table.name.0
        )))
    }
}

fn validate_column_value(column: &ColumnDescriptor, value: &RowValue) -> RsfResult<()> {
    if matches!(value, RowValue::Null) && !column.nullable {
        return Err(invalid(format!(
            "required column {} is missing or null",
            column.name
        )));
    }
    validate_value(&column.ty, value, &column.name)
}

fn validate_value(expected: &ColumnType, value: &RowValue, column: &str) -> RsfResult<()> {
    match (expected, value) {
        (_, RowValue::Null) => Ok(()),
        (ColumnType::Int64, RowValue::Int64(_))
        | (ColumnType::Text, RowValue::Text(_))
        | (ColumnType::Bool, RowValue::Bool(_))
        | (ColumnType::Bytes, RowValue::Bytes(_)) => Ok(()),
        _ => Err(invalid(format!(
            "column {column} expected {}, got {}",
            column_type_name(expected),
            value_type_name(value)
        ))),
    }
}

fn decode_value(tag: u8, payload: &[u8]) -> RsfResult<RowValue> {
    match tag {
        TYPE_NULL => {
            if payload.is_empty() {
                Ok(RowValue::Null)
            } else {
                Err(invalid("null value must not carry payload"))
            }
        }
        TYPE_INT64 => {
            let bytes: [u8; 8] = payload
                .try_into()
                .map_err(|_| invalid("int64 payload must be 8 bytes"))?;
            Ok(RowValue::Int64(i64::from_be_bytes(bytes)))
        }
        TYPE_TEXT => String::from_utf8(payload.to_vec())
            .map(RowValue::Text)
            .map_err(|_| invalid("text payload must be valid utf-8")),
        TYPE_BOOL => match payload {
            [0] => Ok(RowValue::Bool(false)),
            [1] => Ok(RowValue::Bool(true)),
            _ => Err(invalid("bool payload must be one byte")),
        },
        TYPE_BYTES => Ok(RowValue::Bytes(payload.to_vec())),
        other => Err(invalid(format!("unknown row value type tag {other}"))),
    }
}

fn value_payload(value: &RowValue) -> Vec<u8> {
    match value {
        RowValue::Null => Vec::new(),
        RowValue::Int64(value) => value.to_be_bytes().to_vec(),
        RowValue::Text(value) => value.as_bytes().to_vec(),
        RowValue::Bool(value) => vec![u8::from(*value)],
        RowValue::Bytes(value) => value.clone(),
    }
}

fn type_tag(value: &RowValue) -> u8 {
    match value {
        RowValue::Null => TYPE_NULL,
        RowValue::Int64(_) => TYPE_INT64,
        RowValue::Text(_) => TYPE_TEXT,
        RowValue::Bool(_) => TYPE_BOOL,
        RowValue::Bytes(_) => TYPE_BYTES,
    }
}

fn push_string(bytes: &mut Vec<u8>, value: &str) -> RsfResult<()> {
    push_u16(bytes, value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn push_u16(bytes: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u16::try_from(value).map_err(|_| invalid("value does not fit u16"))?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn push_u32(bytes: &mut Vec<u8>, value: usize) -> RsfResult<()> {
    let value = u32::try_from(value).map_err(|_| invalid("value does not fit u32"))?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn column_type_name(ty: &ColumnType) -> &'static str {
    match ty {
        ColumnType::Int64 => "int64",
        ColumnType::Bytes => "bytes",
        ColumnType::Text => "text",
        ColumnType::Bool => "bool",
    }
}

fn value_type_name(value: &RowValue) -> &'static str {
    match value {
        RowValue::Null => "null",
        RowValue::Int64(_) => "int64",
        RowValue::Text(_) => "text",
        RowValue::Bool(_) => "bool",
        RowValue::Bytes(_) => "bytes",
    }
}

fn invalid(message: impl Into<String>) -> RsfError {
    RsfError::new(ErrorCode::InvalidInput, message)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8]) -> RsfResult<()> {
        let actual = self.take(magic.len())?;
        if actual == magic {
            Ok(())
        } else {
            Err(invalid("row payload has invalid magic"))
        }
    }

    fn u8(&mut self) -> RsfResult<u8> {
        self.take(1).map(|bytes| bytes[0])
    }

    fn u16(&mut self) -> RsfResult<u16> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| invalid("u16 read failed"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> RsfResult<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| invalid("u64 read failed"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn string(&mut self) -> RsfResult<String> {
        let len = self.u16()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| invalid("column name must be valid utf-8"))
    }

    fn bytes(&mut self) -> RsfResult<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn u32(&mut self) -> RsfResult<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| invalid("u32 read failed"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn finish(&self) -> RsfResult<()> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("row payload has trailing bytes"))
        }
    }

    fn take(&mut self, len: usize) -> RsfResult<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid("row payload offset overflow"))?;
        if end > self.bytes.len() {
            return Err(invalid("row payload ended early"));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_core::catalog::{ColumnDescriptor, NamespaceName, TableName};

    #[test]
    fn encodes_and_decodes_schema_versioned_row() {
        let codec = RowCodec;
        let encoded = codec
            .encode(
                &table(),
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                ]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::from([("active".to_string(), RowValue::Bool(true))]),
                    generated: vec![GeneratedColumn::TextLength {
                        column: "name_len".to_string(),
                        source: "name".to_string(),
                    }],
                },
            )
            .unwrap();

        assert_eq!(encoded.defaults_applied, 1);
        assert_eq!(encoded.generated_applied, 1);
        let decoded = codec.decode(&table(), &encoded.bytes).unwrap();
        assert_eq!(decoded.schema_version, CommitVersion(3));
        assert_eq!(decoded.values["active"], RowValue::Bool(true));
        assert_eq!(decoded.values["name_len"], RowValue::Int64(3));
    }

    #[test]
    fn rejects_missing_required_column() {
        let err = RowCodec
            .encode(
                &table(),
                BTreeMap::from([("id".to_string(), RowValue::Int64(7))]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("required column name"));
    }

    #[test]
    fn rejects_type_mismatch() {
        let err = RowCodec
            .encode(
                &table(),
                BTreeMap::from([
                    ("id".to_string(), RowValue::Text("7".to_string())),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(3)),
                ]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .unwrap_err();

        assert!(err.message.contains("column id expected int64"));
    }

    #[test]
    fn rejects_descriptor_mismatch_on_decode() {
        let mut encoded = RowCodec
            .encode(
                &table(),
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("name".to_string(), RowValue::Text("ada".to_string())),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(3)),
                ]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .unwrap()
            .bytes;
        encoded.push(0);

        let err = RowCodec.decode(&table(), &encoded).unwrap_err();
        assert!(err.message.contains("trailing bytes"));
    }

    #[test]
    fn decode_rejects_null_required_column() {
        let mut nullable = table();
        nullable.columns[1].nullable = true;
        let encoded = RowCodec
            .encode(
                &nullable,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(0)),
                ]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .expect("nullable descriptor should encode null name");

        let err = RowCodec.decode(&table(), &encoded.bytes).unwrap_err();

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(
            err.message
                .contains("required column name is missing or null")
        );
    }

    #[test]
    fn duplicate_descriptor_columns_are_error() {
        let mut duplicate = table();
        duplicate.columns[1].name = "id".to_string();

        let err = RowCodec
            .encode(
                &duplicate,
                BTreeMap::from([
                    ("id".to_string(), RowValue::Int64(7)),
                    ("active".to_string(), RowValue::Bool(true)),
                    ("name_len".to_string(), RowValue::Int64(3)),
                ]),
                RowCodecPolicy {
                    schema_version: CommitVersion(3),
                    defaults: BTreeMap::new(),
                    generated: Vec::new(),
                },
            )
            .expect_err("duplicate descriptor columns should fail");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("duplicate column id"));
    }

    #[test]
    fn blank_descriptor_columns_are_error() {
        let mut blank = table();
        blank.columns[1].name = " ".to_string();

        let err = RowCodec
            .decode(&blank, &[])
            .expect_err("blank descriptor columns should fail before decoding bytes");

        assert_eq!(err.code, ErrorCode::InvalidInput);
        assert!(err.message.contains("blank column name"));
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
                ColumnDescriptor {
                    name: "name_len".to_string(),
                    ty: ColumnType::Int64,
                    nullable: false,
                },
            ],
            primary_key: vec!["id".to_string()],
        }
    }
}
