use std::collections::HashMap;

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

#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) table_name: String,
    pub(crate) root_page: usize,
    pub(crate) sql_schema: TableSchema,
    field_index_cache: HashMap<String, usize>,
}

impl Table {
    pub(crate) fn new(table_name: String, root_page: usize, sql_schema: TableSchema) -> Self {
        let mut field_index_cache = HashMap::new();
        for (i, field) in sql_schema.fields.iter().enumerate() {
            field_index_cache.insert(field.name.clone(), i);
        }

        Self {
            table_name,
            root_page,
            sql_schema,
            field_index_cache,
        }
    }

    pub(crate) fn field_index(&self, name: &str) -> usize {
        self.field_index_cache[name]
    }
}
