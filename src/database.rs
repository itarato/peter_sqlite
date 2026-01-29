use log::{debug, error, warn};

use crate::{
    btree_page_header::BTreePageHeader,
    cell::{TableBTreeInteriorCell, TableBTreeLeafCell},
    common::{BTreePageType, Error, Table},
    database_header::DatabaseHeader,
    reader::Reader,
};
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::Read,
};

pub(crate) struct Database {
    pub(crate) header: DatabaseHeader,
    pub(crate) tables: HashMap<String, Table>,
}

impl Database {
    pub(crate) fn from(mut file: File) -> Result<Self, Error> {
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).unwrap();
        // debug!("Database size: {} bytes", buffer.len());

        let reader = Reader::new(&buffer[..]);
        let file_header = DatabaseHeader::from(&reader);

        let first_header = BTreePageHeader::from(&reader.at(100));
        assert_eq!(BTreePageType::LeafTable, first_header.kind);
        // debug!("Header: {:?}", first_header);

        let mut tables = HashMap::new();
        for cell_offset in first_header.cell_offsets {
            let cell = TableBTreeLeafCell::from(&reader.at(cell_offset));
            // debug!("Schema cell: {:?}", cell);

            let mut table = cell.payload.read_as_schema_definition();
            debug!("Schema: {:?}", table);

            let mut offset_stack: VecDeque<usize> = VecDeque::new();
            offset_stack.push_back(file_header.page_size * (table.root_page - 1));

            // while let Some(offset) = offset_stack.pop_front() {
            //     // debug!("Reading page at offset: {}", offset);
            //     let page_header = BTreePageHeader::from(&reader.at(offset));
            //     // debug!("Page kind: {:?}", page_header.kind);

            //     match page_header.kind {
            //         BTreePageType::LeafTable => {
            //             for cell_offset in page_header.cell_offsets {
            //                 let cell = TableBTreeLeafCell::from(&reader.at(offset + cell_offset));
            //                 // debug!("Table leaf cell: {:?}", cell);
            //                 let row = cell.payload.read_as_table_row(&table.sql_schema); // TODO: save the records.
            //                 table.insert_row(row);
            //             }
            //             // debug!("Found {} rows", table.rows.len());
            //         }

            //         BTreePageType::InteriorTable => {
            //             for cell_offset in page_header.cell_offsets {
            //                 let cell =
            //                     TableBTreeInteriorCell::from(&reader.at(offset + cell_offset));
            //                 // debug!("Table interior cell: {:?}", cell);

            //                 offset_stack
            //                     .push_back((cell.left_child_pointer - 1) * file_header.page_size);
            //             }

            //             offset_stack.push_back(
            //                 (page_header.rightmost_pointer.unwrap() - 1) * file_header.page_size,
            //             );
            //         }
            //         other => unimplemented!("Page type {:?} not implemented", other),
            //     }
            // }

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
