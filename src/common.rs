use crate::schema::{IndexSchema, TableSchema};

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

#[derive(Debug)]
pub(crate) struct Index {
    pub(crate) table_name: String,
    pub(crate) index_name: String,
    pub(crate) root_page: usize,
    pub(crate) sql_schema: IndexSchema,
}

impl Index {
    pub(crate) fn new(
        table_name: String,
        index_name: String,
        root_page: usize,
        sql_schema: IndexSchema,
    ) -> Self {
        Self {
            table_name,
            index_name,
            root_page,
            sql_schema,
        }
    }
}

pub(crate) enum Schema {
    Table(Table),
    Index(Index),
}

#[derive(Debug)]
pub(crate) struct Incrementer {
    value: u64,
}

impl Incrementer {
    pub(crate) fn new(value: u64) -> Self {
        Self { value }
    }

    pub(crate) fn next_value(&mut self) -> u64 {
        self.value += 1;
        self.value - 1
    }
}
