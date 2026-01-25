use crate::{record::Record, schema::TableSchema};

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, PartialEq)]
pub(crate) enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl BTreePageType {
    pub(crate) fn is_interior(&self) -> bool {
        match self {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => true,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => false,
        }
    }
}

pub(crate) struct SchemaDefinition {
    pub(crate) table_name: String,
    pub(crate) root_page: Record,
    pub(crate) sql_schema: TableSchema,
}
