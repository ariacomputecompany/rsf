use crate::catalog::TableDescriptor;
use crate::transaction::CommitIntent;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationPlan {
    pub statement: RelationalStatement,
    pub table: Option<TableDescriptor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RelationalStatement {
    CreateTable,
    Insert,
    Update,
    Delete,
    Select,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationalWrite {
    pub table: TableDescriptor,
    pub commit: CommitIntent,
}
