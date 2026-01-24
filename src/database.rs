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

impl BTreePageType {
    fn is_interior(&self) -> bool {
        match self {
            BTreePageType::InteriorIndex | BTreePageType::InteriorTable => true,
            BTreePageType::LeafIndex | BTreePageType::LeafTable => false,
        }
    }
}

#[derive(Debug)]
struct BTreePageHeader {
    kind: BTreePageType,
    cell_count: u16,
    cell_start_offset: usize,
    rightmost_pointer: Option<usize>,
    cell_offsets: Vec<usize>,
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

        let rightmost_pointer = if kind.is_interior() {
            Some(reader.at(8).peek_i32() as usize)
        } else {
            None
        };

        let mut cell_offsets = vec![];
        let mut cell_offset_location = if kind.is_interior() { 12 } else { 8 };
        for _ in 0..cell_count {
            cell_offsets.push(reader.at(cell_offset_location).peek_u16() as usize);
            cell_offset_location += 2;
        }

        Self {
            kind,
            cell_count,
            cell_start_offset,
            rightmost_pointer,
            cell_offsets,
        }
    }

    fn byte_len(&self) -> usize {
        if self.kind.is_interior() { 12 } else { 8 }
    }
}

enum Record {
    String(String),
    Null,
}

enum RecordFormat {
    Null,
    TwoCompInt(u8),
    Float64,
    Zero,
    One,
    Blob(usize),
    String(usize),
}

impl RecordFormat {
    fn from(v: i64) -> Self {
        match v {
            0 => Self::Null,
            1 => Self::TwoCompInt(1),
            2 => Self::TwoCompInt(2),
            3 => Self::TwoCompInt(3),
            4 => Self::TwoCompInt(4),
            5 => Self::TwoCompInt(6),
            6 => Self::TwoCompInt(8),
            7 => Self::Float64,
            8 => Self::Zero,
            9 => Self::One,
            10 | 11 => panic!("Not supported"),
            other => {
                if other % 2 == 0 {
                    Self::Blob((other as usize - 12) / 2)
                } else {
                    Self::String((other as usize - 13) / 2)
                }
            }
        }
    }

    fn byte_len(&self) -> usize {
        match self {
            Self::Blob(len) | Self::String(len) => *len,
            Self::Float64 => 8,
            Self::Null | Self::Zero | Self::One => 0,
            Self::TwoCompInt(n) => *n as usize,
        }
    }

    fn pop_value(&self, reader: &mut Reader<'_, u8>) -> Record {
        match self {
            Self::String(len) => Record::String(reader.pop_str(*len)),
            Self::Null => Record::Null,
            _ => unimplemented!(),
        }
    }
}

enum Cell {
    TableLeaf(TableBTreeLeafCell),
}

impl Cell {
    fn from(reader: &Reader<'_, u8>, kind: BTreePageType) -> Self {
        match kind {
            BTreePageType::LeafTable => Cell::TableLeaf(TableBTreeLeafCell::from(reader)),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
struct TableBTreeLeafCell {
    payload_size: usize,
    rowid: i64,
    table_name: String,
}

impl TableBTreeLeafCell {
    fn from(reader: &Reader<'_, u8>) -> Self {
        let mut reader = reader.clone();

        let payload_size = reader.pop_varint() as usize;
        let rowid = reader.pop_varint();
        reader.pop_varint(); // Size of record header (varint)

        let schema_type = RecordFormat::from(reader.pop_varint());
        let schema_name = RecordFormat::from(reader.pop_varint());
        let table_name = RecordFormat::from(reader.pop_varint());

        reader.pop_varint(); // Serial type for sqlite_schema.rootpage (varint)
        reader.pop_varint(); // Serial type for sqlite_schema.sql (varint)

        reader.pop(schema_type.byte_len());
        reader.pop(schema_name.byte_len());
        let Record::String(table_name) = table_name.pop_value(&mut reader) else {
            panic!("Expected string for table name");
        };

        Self {
            payload_size,
            rowid,
            table_name,
        }
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
        assert_eq!(BTreePageType::LeafTable, first_header.kind);
        debug!("Header: {:?}", first_header);

        let mut table_names = vec![];
        for cell_offset in first_header.cell_offsets {
            let cell = TableBTreeLeafCell::from(&reader.at(cell_offset));
            debug!("Cell: {:?}", cell);

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
