use std::{
    fs::File,
    io::{BufReader, Read},
};

use crate::common::Error;

pub(crate) struct Database {
    pub(crate) page_size: u16,
}

impl Database {
    pub(crate) fn from(reader: &mut BufReader<File>) -> Result<Self, Error> {
        let mut buf: [u8; 1024] = [0; 1024];

        // The header string: "SQLite format 3\000"
        reader.read_exact(&mut buf[..16]).unwrap();

        // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
        reader.read_exact(&mut buf[..2]).unwrap();
        let page_size = u16::from_be_bytes(buf[..2].try_into().unwrap());

        Ok(Self { page_size })
    }
}
