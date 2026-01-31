use crate::{
    btree_page_header::BTreePageHeader,
    cell::TableBTreeLeafCell,
    common::{BTreePageType, Error, Index, Schema, Table},
    database_header::DatabaseHeader,
    reader::Reader,
};
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct Database {
    pub(crate) header: DatabaseHeader,
    pub(crate) tables: HashMap<String, Table>,
    pub(crate) indices: HashMap<String, Index>,
}

impl Database {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Result<Self, Error> {
        let file_header = DatabaseHeader::from(&reader);
        let first_header = BTreePageHeader::from(&reader.at(100));
        assert_eq!(BTreePageType::LeafTable, first_header.kind);

        let mut tables = HashMap::new();
        let mut indices = HashMap::new();

        for cell_offset in first_header.cell_offsets {
            let cell = TableBTreeLeafCell::from(&reader.at(cell_offset));
            match cell.payload.read_as_schema_definition() {
                Schema::Table(table) => {
                    tables.insert(table.table_name.clone(), table);
                }
                Schema::Index(index) => {
                    indices.insert(index.table_name.clone(), index);
                }
            }
        }

        Ok(Self {
            header: file_header,
            tables,
            indices,
        })
    }

    pub(crate) fn table_names_sorted(&self) -> Vec<String> {
        let mut names = self
            .tables
            .keys()
            .map(|table_name| table_name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}
