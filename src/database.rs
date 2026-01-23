use crate::{common::Error, reader::Reader};
use log::debug;
use std::{fs::File, io::Read};

pub(crate) struct Header {
    pub(crate) page_size: usize,
    schema_format: i32,
}

impl Header {
    fn from(reader: &Reader<'_, u8>) -> Self {
        // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.f[..2]).unwrap();
        let page_size = reader.at(16).peek_u16();
        let page_size: usize = if page_size == 1 {
            0x10_000
        } else {
            assert!(page_size >= 512);
            assert!(page_size <= 32768);
            page_size as usize
        };
        debug!("Page size: {}", page_size);

        // The schema format number. Supported schema formats are 1, 2, 3, and 4.
        let schema_format = reader.at(44).peek_i32();
        debug!("Scheme format: {}", schema_format);

        Self {
            page_size,
            schema_format,
        }
    }
}

#[derive(Debug, PartialEq)]
enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

#[derive(Debug)]
struct BTreePageHeader {
    kind: BTreePageType,
    cell_count: u16,
    cell_start_offset: usize,
}

impl BTreePageHeader {
    fn from(reader: &Reader<'_, u8>) -> Self {
        let kind = match reader.peek_i8() {
            2 => BTreePageType::InteriorIndex,
            5 => BTreePageType::InteriorTable,
            10 => BTreePageType::LeafIndex,
            13 => BTreePageType::LeafTable,
            other => panic!("Unexpected b-tree page type: {}", other),
        };

        let cell_count = reader.at(3).peek_u16();
        let mut cell_start_offset = reader.at(5).peek_u16() as usize;
        if cell_start_offset == 0 {
            cell_start_offset = 0x10_0000;
        }

        Self {
            kind,
            cell_count,
            cell_start_offset,
        }
    }
}

#[derive(Debug)]
struct Cell {
    size: usize,
    table_name: String,
}

impl Cell {
    fn from(reader: &Reader<'_, u8>) -> Self {
        let mut reader = reader.clone();

        let size = reader.pop_varint() as usize;
        reader.pop_varint(); // The rowid
        reader.pop_varint(); // Size of record header (varint)

        let schema_type_size_byte = reader.pop_varint();
        let scheme_type_size = (schema_type_size_byte - 13) / 2;

        let schema_name_size_byte = reader.pop_varint();
        let schema_name_size = (schema_name_size_byte - 13) / 2;

        let table_name_size_byte = reader.pop_varint();
        let table_name_size = (table_name_size_byte - 13) / 2;

        reader.pop_varint(); // Serial type for sqlite_schema.rootpage (varint)
        reader.pop_varint(); // Serial type for sqlite_schema.sql (varint)

        reader.pop(scheme_type_size as usize);
        reader.pop(schema_name_size as usize);
        let table_name = reader.pop_str(table_name_size as usize);

        Self { size, table_name }
    }
}

pub(crate) struct Database {
    pub(crate) header: Header,
    pub(crate) table_count: usize,
    pub(crate) table_names: Vec<String>,
}

impl Database {
    pub(crate) fn from(mut file: File) -> Result<Self, Error> {
        let mut buffer = vec![];
        file.read_to_end(&mut buffer).unwrap();
        debug!("Database size: {} bytes", buffer.len());

        let reader = Reader::new(&buffer[..]);
        let file_header = Header::from(&reader);

        let first_header = BTreePageHeader::from(&reader.at(100));
        debug!("Header: {:?}", first_header);

        let mut table_names = vec![];
        let mut cell_offset = first_header.cell_start_offset;
        for _ in 0..first_header.cell_count {
            let cell = Cell::from(&reader.at(cell_offset));
            debug!("Cell: {:?}", cell);
            cell_offset += cell.size + 2;

            table_names.push(cell.table_name);
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
