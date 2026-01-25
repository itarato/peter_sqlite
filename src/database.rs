use crate::{
    btree_page_header::BTreePageHeader,
    cell::TableBTreeLeafCell,
    common::{BTreePageType, Error},
    database_header::DatabaseHeader,
    reader::Reader,
};
use log::{debug, error};
use std::{fs::File, io::Read};

pub(crate) struct Database {
    pub(crate) header: DatabaseHeader,
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
            debug!("Schema cell: {:?}", cell);

            let schema_def = cell.payload.read_as_schema_definition();
            debug!("Schema: {:?}", schema_def);
            table_names.push(schema_def.table_name);

            let mut table_offset = file_header.page_size * (schema_def.root_page - 1);
            loop {
                debug!("Reader-len = {}", reader.len());
                if table_offset >= reader.len() {
                    error!("Page scanning overflow - missing leaf page");
                    break;
                }

                debug!("Reading page at offset: {}", table_offset);
                let page_header = BTreePageHeader::from(&reader.at(table_offset));
                debug!("Page header: {:?}", page_header);

                match page_header.kind {
                    BTreePageType::LeafTable => {
                        for cell_offset in page_header.cell_offsets {
                            let cell =
                                TableBTreeLeafCell::from(&reader.at(table_offset + cell_offset));
                            debug!("Page cell: {:?}", cell);
                            cell.payload.read_as_table_row(&schema_def.sql_schema);
                        }

                        break;
                    }
                    BTreePageType::InteriorTable => {
                        unimplemented!();
                    }
                    other => unimplemented!("Page type {:?} not implemented", other),
                }

                table_offset += file_header.page_size;
            }
        }

        table_names.sort();

        Ok(Self {
            header: file_header,
            table_names,
        })
    }
}
