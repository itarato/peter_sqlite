use log::debug;

use crate::{
    common::{BTreePageType, SchemaDefinition},
    reader::Reader,
    record::{Record, RecordFormat},
    schema::TableSchema,
};

#[derive(Debug)]
pub(crate) struct CellPayload {
    bytes: Vec<u8>,
}

impl CellPayload {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(crate) fn read_as_schema_definition(&self) -> SchemaDefinition {
        let mut reader = Reader::new(&self.bytes[..]);

        reader.pop_varint(); // Size of record header (varint)

        let schema_type_header = RecordFormat::from(reader.pop_varint());
        let schema_name_header = RecordFormat::from(reader.pop_varint());
        let table_name_header = RecordFormat::from(reader.pop_varint());

        let root_page_header = RecordFormat::from(reader.pop_varint()); // Serial type for sqlite_schema.rootpage (varint)
        let sql_schema_header = RecordFormat::from(reader.pop_varint()); // Serial type for sqlite_schema.sql (varint)

        reader.pop(schema_type_header.byte_len());
        reader.pop(schema_name_header.byte_len());
        let Record::String(table_name) = table_name_header.pop_value(&mut reader) else {
            panic!("Expected string for table name");
        };

        let root_page = root_page_header.pop_value(&mut reader).unwrap_usize();
        debug!("Root page: {:?}", root_page);
        let sql_schema_raw = sql_schema_header.pop_value(&mut reader);
        let sql_schema = TableSchema::from(sql_schema_raw.unwrap_string()).unwrap();

        SchemaDefinition {
            table_name,
            root_page,
            sql_schema,
        }
    }

    pub(crate) fn read_as_table_row(&self, schema: &TableSchema) {
        let mut reader = Reader::new(&self.bytes[..]);

        reader.pop_varint(); // Size of record header (varint)

        let record_formats = &schema
            .fields
            .iter()
            .map(|_| RecordFormat::from(reader.pop_varint()))
            .collect::<Vec<_>>();

        let records = record_formats
            .iter()
            .map(|format| format.pop_value(&mut reader))
            .collect::<Vec<_>>();
        dbg!(records);
    }
}

pub(crate) enum Cell {
    TableLeaf(TableBTreeLeafCell),
}

impl Cell {
    pub(crate) fn from(reader: &Reader<'_, u8>, kind: BTreePageType) -> Self {
        match kind {
            BTreePageType::LeafTable => Cell::TableLeaf(TableBTreeLeafCell::from(reader)),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TableBTreeLeafCell {
    payload_size: usize,
    rowid: i64,
    pub(crate) payload: CellPayload,
}

impl TableBTreeLeafCell {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Self {
        let mut reader = reader.clone();

        debug!("Cell read starts, capacity = {}", reader.len());
        let payload_size = reader.pop_varint() as usize;
        debug!("Payload size = {}", payload_size);

        let rowid = reader.pop_varint();
        let payload_bytes = reader.pop(payload_size).to_vec();

        // There is a 4 byte overflow page - but it feels like only should be loaded strictly when the content is not fitting on the page.

        Self {
            payload_size,
            rowid,
            payload: CellPayload::new(payload_bytes),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TableBTreeInteriorCell {
    left_child_pointer: usize,
    rowid: i64,
}

impl TableBTreeInteriorCell {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Self {
        let mut reader = reader.clone();

        let left_child_pointer = reader.pop_i32() as usize;
        let rowid = reader.pop_varint();

        Self {
            left_child_pointer,
            rowid,
        }
    }
}
