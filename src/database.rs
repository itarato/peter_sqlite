use std::{fs::File, io::Read};

use clap::builder::Str;
use log::debug;

use crate::common::Error;

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
    fn from(bytes: &[u8]) -> Self {
        let kind = match bytes[0] {
            2 => BTreePageType::InteriorIndex,
            5 => BTreePageType::InteriorTable,
            10 => BTreePageType::LeafIndex,
            13 => BTreePageType::LeafTable,
            other => panic!("Unexpected b-tree page type: {}", other),
        };

        let cell_count = u16::from_be_bytes(bytes[3..5].try_into().unwrap());
        let mut cell_start_offset = u16::from_be_bytes(bytes[5..7].try_into().unwrap()) as usize;
        if cell_start_offset == 0 {
            cell_start_offset = 0x10_000;
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
    fn from(bytes: &[u8]) -> Self {
        let size = bytes[0] as usize;
        let record_header_size = bytes[2];

        let schema_type_size_byte = bytes[3];
        let scheme_type_size = (schema_type_size_byte - 13) / 2;

        let schema_name_size_byte = bytes[4];
        let schema_name_size = (schema_name_size_byte - 13) / 2;

        let table_name_size_byte = bytes[4];
        let table_name_size = (table_name_size_byte - 13) / 2;

        let table_name_offset =
            (record_header_size + 2 + scheme_type_size + schema_name_size) as usize;
        let table_name_bytes =
            &bytes[table_name_offset..table_name_offset + table_name_size as usize];
        let table_name = String::from_utf8_lossy(table_name_bytes).to_string();

        Self { size, table_name }
    }
}

pub(crate) struct Database {
    pub(crate) page_size: usize,
    pub(crate) table_count: usize,
    pub(crate) table_names: Vec<String>,
}

impl Database {
    pub(crate) fn from(mut file: File) -> Result<Self, Error> {
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        debug!("Database size: {} bytes", buf.len());

        // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.f[..2]).unwrap();
        let page_size = u16::from_be_bytes(buf[16..18].try_into().unwrap());
        let page_size: usize = if page_size == 1 {
            0x10_000
        } else {
            assert!(page_size >= 512);
            assert!(page_size <= 32768);
            page_size as usize
        };
        debug!("Page size: {}", page_size);

        let schema_format = u32::from_be_bytes(buf[44..48].try_into().unwrap());
        debug!("Scheme format: {}", schema_format);

        let first_header = BTreePageHeader::from(&buf[100..]);
        debug!("Header: {:?}", first_header);

        let mut table_names = vec![];
        let mut cell_offset = first_header.cell_start_offset;
        for _ in 0..first_header.cell_count {
            let cell = Cell::from(&buf[cell_offset..]);
            debug!("Cell: {:?}", cell);
            cell_offset += cell.size + 2;

            table_names.push(cell.table_name);
        }

        table_names.sort();

        let mut table_count = 0;
        let mut offset = page_size;

        while offset < buf.len() {
            debug!("Reading at offset: {}", offset);
            let header = BTreePageHeader::from(&buf[offset..]);
            if header.kind == BTreePageType::LeafTable {
                table_count += 1;
            }

            offset += page_size;
        }

        Ok(Self {
            page_size,
            table_count,
            table_names,
        })
    }
}
