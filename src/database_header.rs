use crate::reader::Reader;

pub(crate) struct DatabaseHeader {
    pub(crate) page_size: usize,
    schema_format: i32,
}

impl DatabaseHeader {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Self {
        // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.f[..2]).unwrap();
        let page_size = reader.at(16).peek_u16();
        let page_size: usize = if page_size == 1 {
            0x10_000
        } else {
            assert!(page_size >= 512);
            assert!(page_size <= 32768);
            page_size as usize
        };
        // debug!("Page size: {}", page_size);

        // The schema format number. Supported schema formats are 1, 2, 3, and 4.
        let schema_format = reader.at(44).peek_i32();
        // debug!("Scheme format: {}", schema_format);

        Self {
            page_size,
            schema_format,
        }
    }
}
