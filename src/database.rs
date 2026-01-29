use log::debug;

use crate::{
    btree_page_header::BTreePageHeader,
    cell::TableBTreeLeafCell,
    common::{BTreePageType, Error, Table},
    database_header::DatabaseHeader,
    reader::Reader,
};
use std::collections::HashMap;

pub(crate) struct Database {
    pub(crate) header: DatabaseHeader,
    pub(crate) tables: HashMap<String, Table>,
}

impl Database {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Result<Self, Error> {
        let file_header = DatabaseHeader::from(&reader);
        let first_header = BTreePageHeader::from(&reader.at(100));
        assert_eq!(BTreePageType::LeafTable, first_header.kind);
        // debug!("Header: {:?}", first_header);

        let mut tables = HashMap::new();
        for cell_offset in first_header.cell_offsets {
            let cell = TableBTreeLeafCell::from(&reader.at(cell_offset));
            // debug!("Schema cell: {:?}", cell);

            let table = cell.payload.read_as_schema_definition();
            debug!("Schema: {:?}", table);

            tables.insert(table.table_name.clone(), table);
        }

        Ok(Self {
            header: file_header,
            tables,
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
