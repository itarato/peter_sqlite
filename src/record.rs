use crate::reader::Reader;

#[derive(Debug)]
pub(crate) enum Record {
    String(String),
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I64(i64),
    Null,
}

impl Record {
    pub(crate) fn unwrap_string(&self) -> &String {
        match self {
            Self::String(s) => s,
            _ => panic!("Expected string field"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum RecordFormat {
    Null,
    TwoCompInt(u8),
    Float64,
    Zero,
    One,
    Blob(usize),
    String(usize),
}

impl RecordFormat {
    pub(crate) fn from(v: i64) -> Self {
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

    pub(crate) fn byte_len(&self) -> usize {
        match self {
            Self::Blob(len) | Self::String(len) => *len,
            Self::Float64 => 8,
            Self::Null | Self::Zero | Self::One => 0,
            Self::TwoCompInt(n) => *n as usize,
        }
    }

    pub(crate) fn pop_value(&self, reader: &mut Reader<'_, u8>) -> Record {
        match self {
            Self::String(len) => Record::String(reader.pop_str(*len)),
            Self::Null => Record::Null,
            Self::TwoCompInt(byte_len) => match byte_len {
                1 => Record::I8(reader.pop(1)[0] as i8),
                other => unimplemented!("Two comp int fetch for size {} not implemented", other),
            },
            other => unimplemented!("Record fetch not implemented for: {:?}", other),
        }
    }
}
