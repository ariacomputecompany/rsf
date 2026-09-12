#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceName(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableName(pub String);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableDescriptor {
    pub namespace: NamespaceName,
    pub name: TableName,
    pub columns: Vec<ColumnDescriptor>,
    pub primary_key: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDescriptor {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Int64,
    Bytes,
    Text,
    Bool,
}
