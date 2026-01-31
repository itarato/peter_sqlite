use crate::{
    common::{BTreePageType, Index, Schema, Table},
    reader::Reader,
    record::{Record, RecordFormat},
    schema::{IndexSchema, TableSchema},
};

#[derive(Debug)]
pub(crate) struct CellPayload {
    bytes: Vec<u8>,
}

impl CellPayload {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub(crate) fn read_as_schema_definition(&self) -> Schema {
        let mut reader = Reader::new(&self.bytes[..]);

        reader.pop_varint(); // Size of record header (varint)

        let schema_type_header = RecordFormat::from(reader.pop_varint());
        let schema_name_header = RecordFormat::from(reader.pop_varint());
        let table_name_header = RecordFormat::from(reader.pop_varint());
        let root_page_header = RecordFormat::from(reader.pop_varint()); // Serial type for sqlite_schema.rootpage (varint)
        let sql_schema_header = RecordFormat::from(reader.pop_varint()); // Serial type for sqlite_schema.sql (varint)

        let Record::String(schema_type_header) = schema_type_header.pop_value(&mut reader) else {
            panic!();
        };
        let Record::String(schema_name_header) = schema_name_header.pop_value(&mut reader) else {
            panic!();
        };
        let Record::String(table_name) = table_name_header.pop_value(&mut reader) else {
            panic!();
        };

        let root_page = root_page_header.pop_value(&mut reader).unwrap_usize();
        let sql_schema_raw = sql_schema_header.pop_value(&mut reader);

        match schema_type_header.as_str() {
            "index" => {
                let sql_schema = IndexSchema::from(sql_schema_raw.unwrap_string());
                Schema::Index(Index::new(
                    table_name,
                    schema_name_header,
                    root_page,
                    sql_schema,
                ))
            }
            "table" => {
                let sql_schema = TableSchema::from(sql_schema_raw.unwrap_string());
                Schema::Table(Table::new(table_name, root_page, sql_schema))
            }
            other => unimplemented!("Schema type {} not implemented", other),
        }
    }

    pub(crate) fn read_as_table_row(&self, schema: &TableSchema) -> Vec<Record> {
        let mut reader = Reader::new(&self.bytes[..]);

        reader.pop_varint(); // Size of record header (varint)

        let record_formats = &schema
            .fields
            .iter()
            .map(|_| RecordFormat::from(reader.pop_varint()))
            .collect::<Vec<_>>();

        record_formats
            .iter()
            .map(|format| format.pop_value(&mut reader))
            .collect::<Vec<_>>()
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
        let payload_size = reader.pop_varint() as usize;
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
    pub(crate) left_child_pointer: usize,
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

#[derive(Debug)]
pub(crate) struct IndexBTreeLeafCell {
    payload_size: usize,
    pub(crate) payload: CellPayload,
}

impl IndexBTreeLeafCell {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Self {
        let mut reader = reader.clone();
        let payload_size = reader.pop_varint() as usize;
        let payload_bytes = reader.pop(payload_size).to_vec();
        // There is a 4 byte overflow page - but it feels like only should be loaded strictly when the content is not fitting on the page.

        Self {
            payload_size,
            payload: CellPayload::new(payload_bytes),
        }
    }
}
