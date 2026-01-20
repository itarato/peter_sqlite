use std::{fs::File, io::Read};

use log::debug;

use crate::common::Error;

pub(crate) struct Database {
    pub(crate) page_size: usize,
    pub(crate) table_count: usize,
}

impl Database {
    pub(crate) fn from(mut file: File) -> Result<Self, Error> {
        let mut buf = vec![];
        file.read_to_end(&mut buf).unwrap();
        debug!("Database size: {} bytes", buf.len());

        // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.f[..2]).unwrap();
        let page_size = u16::from_be_bytes(buf[16..18].try_into().unwrap());
        let page_size: usize = if page_size == 1 {
            65536
        } else {
            assert!(page_size >= 512);
            assert!(page_size <= 32768);
            page_size as usize
        };
        debug!("Page size: {}", page_size);

        let mut table_count = 0;
        let mut offset = page_size;

        while offset < buf.len() {
            debug!("Reading at offset: {}", offset);
            match buf[offset] {
                2 => {}
                5 => {}
                10 => {}
                13 => {
                    table_count += 1;
                }
                other => return Err(format!("Invalid page header byte: {}", other).into()),
            }

            offset += page_size;
        }

        Ok(Self {
            page_size,
            table_count,
        })
    }
}
