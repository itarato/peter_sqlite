use std::collections::HashMap;

use crate::schema::TableSchema;

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

#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) table_name: String,
    pub(crate) root_page: usize,
    pub(crate) sql_schema: TableSchema,
}

impl Table {
    pub(crate) fn new(table_name: String, root_page: usize, sql_schema: TableSchema) -> Self {
        Self {
            table_name,
            root_page,
            sql_schema,
        }
    }
}
