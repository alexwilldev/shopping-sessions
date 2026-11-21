//! Minimal hand-written Parquet writer: one row group, one uncompressed PLAIN data page per
//! column, no statistics. Only what the two output tables need: required INT32 (optionally
//! DATE), required INT64, required or optional BYTE_ARRAY (UTF8). Metadata is Thrift compact.

pub enum Col<'a> {
    I32 { name: &'a str, date: bool, v: Vec<i32> },
    I64 { name: &'a str, v: Vec<i64> },
    /// Strings: PLAIN bytes already encoded (4-byte LE length + bytes per present value).
    Str { name: &'a str, optional: bool, plain: Vec<u8>, present: Option<Vec<bool>> },
}

/// Appends one PLAIN BYTE_ARRAY value.
#[inline]
pub fn push_str(plain: &mut Vec<u8>, s: &[u8]) {
    plain.extend_from_slice(&(s.len() as u32).to_le_bytes());
    plain.extend_from_slice(s);
}

// ---------------------------------------------------------------- thrift compact

struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> T {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ety: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
}

// ---------------------------------------------------------------- file

struct Meta {
    ptype: i32,
    offset: i64,
    size: i64,
}

/// Builds the whole file in memory and writes it with one write(2).
pub fn write(path: &std::path::Path, schema_name: &str, rows: usize, cols: &[Col]) {
    let mut f: Vec<u8> = Vec::with_capacity(64 + cols.iter().map(col_bytes).sum::<usize>() + 4096);
    f.extend_from_slice(b"PAR1");
    let mut metas = Vec::with_capacity(cols.len());
    for c in cols {
        let offset = f.len() as i64;
        // page body: [def levels] values
        let mut levels: Vec<u8> = Vec::new();
        let (ptype, values): (i32, &[u8]) = match c {
            Col::I32 { v, .. } => (1, as_bytes(v)),
            Col::I64 { v, .. } => (2, as_bytes(v)),
            Col::Str { plain, present, .. } => {
                if let Some(p) = present {
                    levels = def_levels(p);
                }
                (6, plain.as_slice())
            }
        };
        let body_len = levels.len() + values.len();
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, body_len as i32);
        h.i32f(3, body_len as i32);
        h.begin_struct(5);
        h.i32f(1, rows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.end_struct();
        h.b.push(0);
        f.extend_from_slice(&h.b);
        f.extend_from_slice(&levels);
        f.extend_from_slice(values);
        metas.push(Meta { ptype, offset, size: f.len() as i64 - offset });
    }
    // FileMetaData
    let mut t = T::new();
    t.i32f(1, 1); // version
    t.list(2, 12, cols.len() + 1);
    t.begin_elem();
    t.strf(4, schema_name.as_bytes());
    t.i32f(5, cols.len() as i32);
    t.end_struct();
    for c in cols {
        t.begin_elem();
        match c {
            Col::I32 { name, date, .. } => {
                t.i32f(1, 1);
                t.i32f(3, 0);
                t.strf(4, name.as_bytes());
                if *date {
                    t.i32f(6, 6); // converted DATE
                    t.begin_struct(10);
                    t.begin_struct(6); // LogicalType.DATE
                    t.end_struct();
                    t.end_struct();
                }
            }
            Col::I64 { name, .. } => {
                t.i32f(1, 2);
                t.i32f(3, 0);
                t.strf(4, name.as_bytes());
            }
            Col::Str { name, optional, .. } => {
                t.i32f(1, 6);
                t.i32f(3, if *optional { 1 } else { 0 });
                t.strf(4, name.as_bytes());
                t.i32f(6, 0); // converted UTF8
                t.begin_struct(10);
                t.begin_struct(1); // LogicalType.STRING
                t.end_struct();
                t.end_struct();
            }
        }
        t.end_struct();
    }
    t.i64f(3, rows as i64);
    t.list(4, 12, 1);
    t.begin_elem();
    t.list(1, 12, cols.len());
    let total: i64 = metas.iter().map(|m| m.size).sum();
    for (c, m) in cols.iter().zip(metas.iter()) {
        let name = match c {
            Col::I32 { name, .. } | Col::I64 { name, .. } | Col::Str { name, .. } => name,
        };
        t.begin_elem();
        t.i64f(2, m.offset);
        t.begin_struct(3);
        t.i32f(1, m.ptype);
        t.list(2, 5, 2);
        t.zz(0); // PLAIN
        t.zz(3); // RLE
        t.list(3, 8, 1);
        t.varint(name.len() as u64);
        t.b.extend_from_slice(name.as_bytes());
        t.i32f(4, 0); // UNCOMPRESSED
        t.i64f(5, rows as i64);
        t.i64f(6, m.size);
        t.i64f(7, m.size);
        t.i64f(9, m.offset);
        t.end_struct();
        t.end_struct();
    }
    t.i64f(2, total);
    t.i64f(3, rows as i64);
    t.end_struct();
    t.strf(6, b"shopsess");
    t.b.push(0);
    f.extend_from_slice(&t.b);
    f.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    f.extend_from_slice(b"PAR1");
    std::fs::write(path, &f).expect("write parquet");
}

fn col_bytes(c: &Col) -> usize {
    match c {
        Col::I32 { v, .. } => v.len() * 4,
        Col::I64 { v, .. } => v.len() * 8,
        Col::Str { plain, present, .. } => plain.len() + present.as_ref().map_or(0, |p| p.len() / 8 + 16),
    }
}

fn as_bytes<T: Copy>(v: &[T]) -> &[u8] {
    // little-endian targets only (x86-64, aarch64)
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// Definition levels (bit width 1) as one bit-packed run, with the 4-byte length prefix.
fn def_levels(p: &[bool]) -> Vec<u8> {
    let groups = p.len().div_ceil(8);
    let mut run = Vec::with_capacity(groups + 10);
    let mut h = (groups as u64) << 1 | 1;
    while h >= 0x80 {
        run.push((h as u8) | 0x80);
        h >>= 7;
    }
    run.push(h as u8);
    for g in p.chunks(8) {
        let mut byte = 0u8;
        for (i, &b) in g.iter().enumerate() {
            byte |= (b as u8) << i;
        }
        run.push(byte);
    }
    let mut out = Vec::with_capacity(run.len() + 4);
    out.extend_from_slice(&(run.len() as u32).to_le_bytes());
    out.extend_from_slice(&run);
    out
}
