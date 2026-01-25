use crate::{common::BTreePageType, reader::Reader};

#[derive(Debug)]
pub(crate) struct BTreePageHeader {
    pub(crate) kind: BTreePageType,
    pub(crate) cell_count: u16,
    pub(crate) cell_start_offset: usize,
    pub(crate) rightmost_pointer: Option<usize>,
    pub(crate) cell_offsets: Vec<usize>,
}

impl BTreePageHeader {
    pub(crate) fn from(reader: &Reader<'_, u8>) -> Self {
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

    pub(crate) fn byte_len(&self) -> usize {
        if self.kind.is_interior() { 12 } else { 8 }
    }
}
