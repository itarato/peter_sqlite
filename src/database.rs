use crate::{
    btree_page_header::BTreePageHeader,
    cell::TableBTreeLeafCell,
    common::{BTreePageType, Error},
    database_header::DatabaseHeader,
    reader::Reader,
};
use log::debug;
use std::{fs::File, io::Read};

pub(crate) struct Database {
    pub(crate) header: DatabaseHeader,
    pub(crate) table_count: usize,
    pub(crate) table_names: Vec<String>,
}

impl Database {
    pub(crate) fn from(mut file: File) -> Result<Self, Error> {
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).unwrap();
        debug!("Database size: {} bytes", buffer.len());

        let reader = Reader::new(&buffer[..]);
        let file_header = DatabaseHeader::from(&reader);

        let first_header = BTreePageHeader::from(&reader.at(100));
        assert_eq!(BTreePageType::LeafTable, first_header.kind);
        debug!("Header: {:?}", first_header);

        let mut table_names = vec![];
        for cell_offset in first_header.cell_offsets {
            let cell = TableBTreeLeafCell::from(&reader.at(cell_offset));
            debug!("Cell: {:?}", cell);

            table_names.push(cell.payload.read_as_schema_definition().table_name);
        }

        table_names.sort();

        let mut table_count = 0;
        let mut offset = file_header.page_size;

        while offset < reader.len() {
            debug!("Reading at offset: {}", offset);
            let page_header = BTreePageHeader::from(&reader.at(offset));
            if page_header.kind == BTreePageType::LeafTable {
                table_count += 1;
            }

            offset += file_header.page_size;
        }

        Ok(Self {
            header: file_header,
            table_count,
            table_names,
        })
    }
}
