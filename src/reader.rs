#[derive(Debug, Clone)]
pub(crate) struct Reader<'a, T> {
    slice: &'a [T],
}

impl<'a, T> Reader<'a, T> {
    pub(crate) fn new(slice: &'a [T]) -> Self {
        Self { slice }
    }

    pub(crate) fn peek(&self, len: usize) -> &[T] {
        &self.slice[..len]
    }

    pub(crate) fn pop(&mut self, len: usize) -> &[T] {
        let out = &self.slice[..len];
        self.slice = &self.slice[len..];
        out
    }

    pub(crate) fn at(&self, at: usize) -> Self {
        Self {
            slice: &self.slice[at..],
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slice.len()
    }
}

impl<'a> Reader<'a, u8> {
    pub(crate) fn peek_u16(&self) -> u16 {
        u16::from_be_bytes(self.slice[..2].try_into().expect("Casting to 2 bytes"))
    }

    pub(crate) fn peek_i16(&self) -> i16 {
        i16::from_be_bytes(self.slice[..2].try_into().expect("Casting to 2 bytes"))
    }

    pub(crate) fn pop_i16(&mut self) -> i16 {
        i16::from_be_bytes(self.pop(2).try_into().expect("Casting to 2 bytes"))
    }

    pub(crate) fn pop_i32(&mut self) -> i32 {
        i32::from_be_bytes(self.pop(4).try_into().expect("Casting to 4 bytes"))
    }

    pub(crate) fn peek_i32(&self) -> i32 {
        i32::from_be_bytes(self.slice[..4].try_into().expect("Casting to 4 bytes"))
    }

    pub(crate) fn peek_i8(&self) -> i8 {
        self.slice[0] as i8
    }

    pub(crate) fn pop_varint(&mut self) -> i64 {
        let mut out = 0;

        let mut i = 0;
        loop {
            let byte = self.pop(1)[0];

            out <<= 7;
            out |= (byte & 0b0111_1111) as i64;

            if byte & 0b1000_0000 == 0 {
                break;
            }

            i += 1;
            if i >= 9 {
                panic!("Varint overflow");
            }
        }

        out
    }

    pub(crate) fn pop_str(&mut self, len: usize) -> String {
        let bytes = self.pop(len);
        String::from_utf8_lossy(bytes).to_string()
    }
}
