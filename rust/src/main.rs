//! Shopping Sessions v2: a native implementation of rules.md.
//!
//! Pipeline (all stages parallel with rayon):
//!   1. products, campaigns (small, single-threaded) -> category / campaign indexes
//!   2. links -> iterative union-find -> person name per linked ID
//!   3. clicks: parse + validate -> rows bucketed by hash(person)
//!   4. events: mmap + chunked parse -> raw rows bucketed by hash(event_id)
//!   5. per event bucket: dedup (smallest ingest_ts) -> validate -> rows bucketed by hash(person)
//!   6. per person bucket: sort, sessionize, credit, assists, aggregate
//!   7. write both tables as Parquet
//!
//! IDs are packed into a u128 "Key" (see `key_of`) so that equality and byte-wise order are
//! integer operations. IDs longer than 15 bytes go through a global side table.

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const HALF_HOUR: i64 = 1_800_000;
const CHUNK: usize = 4 << 20;
const NB: usize = 64; // buckets for both partition passes

// ---------------------------------------------------------------- keys

/// Packed ID. Short IDs (<= 15 bytes): bytes big-endian from bit 127 down, length in the low
/// byte, so u128 order == byte-wise order (a prefix sorts first) and equality is exact.
/// Long IDs: low byte 0xFF, a side-table index in bytes 11..15, the first 11 bytes on top;
/// they are compared through the side table (`key_cmp`). A real key is never 0.
type Key = u128;
const NONE: Key = 0;

type LongTable = Mutex<(FxHashMap<Vec<u8>, u32>, Vec<Vec<u8>>)>;
static LONG: OnceLock<LongTable> = OnceLock::new();

fn long_table() -> &'static LongTable {
    LONG.get_or_init(|| Mutex::new((FxHashMap::default(), Vec::new())))
}

#[inline]
fn key_of(b: &[u8]) -> Key {
    if b.is_empty() {
        return NONE;
    }
    if b.len() <= 15 {
        let l = b.len();
        let p = b.as_ptr();
        if (p as usize & 4095) <= 4096 - 16 {
            // 16 bytes from p stay inside p's page, which is mapped: safe to over-read.
            let raw = u128::from_be_bytes(unsafe { std::ptr::read_unaligned(p as *const [u8; 16]) });
            let keep = !0u128 << (8 * (16 - l)); // l in 1..=15: keep the first l bytes
            return (raw & keep) | l as u128;
        }
        let mut buf = [0u8; 16];
        buf[..l].copy_from_slice(b);
        buf[15] = l as u8;
        u128::from_be_bytes(buf)
    } else {
        long_key(b)
    }
}

#[cold]
fn long_key(b: &[u8]) -> Key {
    let mut t = long_table().lock().unwrap();
    let id = match t.0.get(b) {
        Some(&id) => id,
        None => {
            let id = t.1.len() as u32;
            t.1.push(b.to_vec());
            t.0.insert(b.to_vec(), id);
            id
        }
    };
    let mut buf = [0u8; 16];
    buf[..11].copy_from_slice(&b[..11]);
    buf[11..15].copy_from_slice(&id.to_be_bytes());
    buf[15] = 0xFF;
    u128::from_be_bytes(buf)
}

#[inline]
fn is_long(k: Key) -> bool {
    (k & 0xFF) == 0xFF
}

fn key_bytes(k: Key) -> Vec<u8> {
    let buf = k.to_be_bytes();
    if is_long(k) {
        let id = u32::from_be_bytes([buf[11], buf[12], buf[13], buf[14]]);
        long_table().lock().unwrap().1[id as usize].clone()
    } else {
        buf[..buf[15] as usize].to_vec()
    }
}

/// Byte-wise comparison of the IDs behind two keys.
#[inline]
fn key_cmp(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        key_bytes(a).cmp(&key_bytes(b))
    }
}

#[inline]
fn bucket(k: Key) -> usize {
    let x = (k as u64) ^ ((k >> 64) as u64).rotate_left(29);
    let x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    ((x ^ (x >> 29)) >> 56) as usize % NB
}

// ---------------------------------------------------------------- CSV dialect

/// Split one line (without its '\n') into exactly `N` fields per rules.md; false if malformed.
/// Fields come back unquoted; an empty slice means null.
#[inline]
fn split_fields<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    // Split on every comma (a value never holds one); exactly N-1 commas or malformed.
    let mut n = 0;
    let mut start = 0;
    for p in memchr::memchr_iter(b',', line) {
        if n + 1 >= N {
            return false;
        }
        out[n] = &line[start..p];
        n += 1;
        start = p + 1;
    }
    if n != N - 1 {
        return false;
    }
    out[n] = &line[start..];
    // Quotes are rare per field: only lines holding a '"' need the per-field check.
    if memchr::memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            if memchr::memchr(b'"', f).is_some() {
                let l = f.len();
                if l < 2 || f[0] != b'"' || f[l - 1] != b'"' || memchr::memchr(b'"', &f[1..l - 1]).is_some() {
                    return false;
                }
                *f = &f[1..l - 1];
            }
        }
    }
    true
}

// ---------------------------------------------------------------- timestamps

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (a, c) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
    if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 }
        }
    }
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 digits (epoch ms).
/// None = bad. F3 with more than 18 significant digits is far outside any window, so None.
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    if b.iter().all(u8::is_ascii_digit) {
        let mut i = 0;
        while i < b.len() && b[i] == b'0' {
            i += 1;
        }
        let s = &b[i..];
        if s.len() > 18 {
            return None;
        }
        let mut v: i64 = 0;
        for &c in s {
            v = v * 10 + (c - b'0') as i64;
        }
        return Some(v);
    }
    if b.len() != 20 && b.len() != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let mut off = 0i64;
    if b.len() == 20 {
        if b[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if b[22] != b':' {
            return None;
        }
        let oh = d2(b, 20)?;
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = sign * (oh * 60 + om);
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off * 60_000)
}

/// Purchase price: ASCII digits only, value 1..=10,000,000. None = bad.
#[inline]
fn parse_price(b: &[u8]) -> Option<i32> {
    if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut i = 0;
    while i < b.len() && b[i] == b'0' {
        i += 1;
    }
    let s = &b[i..];
    if s.is_empty() || s.len() > 8 {
        return None;
    }
    let mut v: i64 = 0;
    for &c in s {
        v = v * 10 + (c - b'0') as i64;
    }
    if v <= 10_000_000 { Some(v as i32) } else { None }
}

// ---------------------------------------------------------------- files and chunks

struct Chunk {
    map: &'static [u8],
    start: usize,
    end: usize,
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> &'static [u8] {
    let f = File::open(p).expect("open");
    if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    let m = unsafe { memmap2::Mmap::map(&f).expect("mmap") };
    let _ = m.advise(memmap2::Advice::Sequential);
    let s: &'static [u8] = unsafe { std::slice::from_raw_parts(m.as_ptr(), m.len()) };
    std::mem::forget(m); // stays mapped for the life of the process
    s
}

/// Chunks covering the data lines (header skipped by position) of every file, cut at '\n'.
fn chunks(paths: &[PathBuf]) -> Vec<Chunk> {
    let mut out = Vec::new();
    for p in paths {
        let map = map_file(p);
        let first = match map.iter().position(|&c| c == b'\n') {
            Some(i) => i + 1,
            None => continue, // header only, or empty
        };
        let mut s = first;
        while s < map.len() {
            let mut e = (s + CHUNK).min(map.len());
            if e < map.len() {
                e = match map[e..].iter().position(|&c| c == b'\n') {
                    Some(i) => e + i + 1,
                    None => map.len(),
                };
            }
            out.push(Chunk { map, start: s, end: e });
            s = e;
        }
    }
    out
}

/// Calls f on every line of the chunk (without its '\n'). A final line without '\n' counts.
#[inline]
fn for_lines(c: &Chunk, mut f: impl FnMut(&[u8])) {
    let data = &c.map[c.start..c.end];
    let mut s = 0;
    for e in memchr::memchr_iter(b'\n', data) {
        f(&data[s..e]);
        s = e + 1;
    }
    if s < data.len() {
        f(&data[s..]); // final line without '\n'
    }
}

const LO: u64 = 0x0101_0101_0101_0101;
const H7: u64 = 0x7F7F_7F7F_7F7F_7F7F;

/// High bit set in exactly the bytes of `w` equal to `c` (exact, no false positives).
#[inline(always)]
fn eq_mask(w: u64, c: u8) -> u64 {
    let x = w ^ (LO * c as u64);
    !(((x & H7).wrapping_add(H7)) | x | H7)
}

/// Single-pass tokenizer: calls f with the N unquoted fields of every well-formed line of the
/// chunk (rules.md dialect). Scans 8 bytes at a time for ',', '"' and '\n' together.
/// A field with quotes must be exactly `"..."` (first quote at its start, exactly two quotes,
/// the second at its end); anything else, or a wrong field count, drops the line.
#[inline]
fn for_records<const N: usize>(c: &Chunk, mut f: impl FnMut(&[&[u8]; N])) {
    let data = &c.map[c.start..c.end];
    let len = data.len();
    let mut fields: [&[u8]; N] = [&[]; N];
    let mut n = 0usize; // fields ended so far on this line
    let mut fs = 0usize; // start of the current field
    let mut qc = 0u32; // quotes seen in the current field
    let mut ql = 0usize; // position of the last quote in the current field
    let mut bad = false;

    macro_rules! event {
        ($p:expr) => {{
            let p: usize = $p;
            let ch = if p < len { data[p] } else { b'\n' };
            if ch == b'"' {
                if qc == 0 && p != fs {
                    bad = true;
                }
                qc += 1;
                ql = p;
            } else {
                // ',' or '\n' ends the field [fs, p)
                let mut v = &data[fs..p];
                if qc != 0 {
                    if qc != 2 || ql + 1 != p || v.len() < 2 {
                        bad = true;
                    } else {
                        v = &v[1..v.len() - 1];
                    }
                }
                if n < N {
                    fields[n] = v;
                }
                n += 1;
                qc = 0;
                fs = p + 1;
                if ch == b'\n' {
                    if !bad && n == N {
                        f(&fields);
                    }
                    n = 0;
                    bad = false;
                }
            }
        }};
    }

    let mut i = 0usize;
    while i + 8 <= len {
        let w = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
        let mut m = eq_mask(w, b',') | eq_mask(w, b'\n') | eq_mask(w, b'"');
        while m != 0 {
            let p = i + (m.trailing_zeros() / 8) as usize;
            m &= m - 1;
            event!(p);
        }
        i += 8;
    }
    while i < len {
        let ch = data[i];
        if ch == b',' || ch == b'\n' || ch == b'"' {
            event!(i);
        }
        i += 1;
    }
    if fs < len || n > 0 {
        event!(len); // final line without '\n'
    }
}

fn small_csv<const N: usize>(path: &Path, mut f: impl FnMut(&[&[u8]; N])) {
    for c in &chunks(&[path.to_path_buf()]) {
        for_records::<N>(c, |fl| f(fl));
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct RawEv {
    eid: Key,
    user: Key, // raw user_id key, NONE if empty (resolved to a person after dedup)
    ts: i64,   // i64::MIN if bad
    ingest: i64,
    price: i32, // -1 if not a valid purchase price
    cat: u16,   // u16::MAX if product_id empty
    etype: u8,  // 0 bad, 1 view, 2 cart, 3 purchase
}

#[derive(Clone, Copy)]
struct Ev {
    person: Key,
    ts: i64,
    price: i32, // purchase price, 0 for view/cart
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct RawClick {
    user: Key,
    ts: i64,
    cid: Key,
    camp: u16,
}

#[derive(Clone, Copy)]
struct Click {
    person: Key,
    ts: i64,
    cid: Key,
    camp: u16,
}

#[derive(Clone, Copy)]
struct UserRow {
    person: Key,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

fn log(t0: Instant, msg: &str) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let cpu = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    eprintln!(
        "[{:7.3}s] {}  (user {:.2}s sys {:.2}s minflt {})",
        t0.elapsed().as_secs_f64(),
        msg,
        cpu(ru.ru_utime),
        cpu(ru.ru_stime),
        ru.ru_minflt
    );
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let i = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[i + key.len() + 2..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = rest[q1..].find('"').unwrap() + q1;
    parse_ts(rest[q1..q2].as_bytes()).expect("manifest ts")
}

fn flush_row(row: &mut UserRow, rows: &mut Vec<UserRow>, catrev: &mut [i64], touched: &mut Vec<u16>, rank: &[u16]) {
    if row.sessions == 0 {
        return;
    }
    let mut top: Option<u16> = None;
    for &c in touched.iter() {
        top = match top {
            None => Some(c),
            Some(t) => {
                let (r, tr) = (catrev[c as usize], catrev[t as usize]);
                if r > tr || (r == tr && rank[c as usize] < rank[t as usize]) { Some(c) } else { Some(t) }
            }
        };
    }
    for &c in touched.iter() {
        catrev[c as usize] = 0;
    }
    touched.clear();
    rows.push(UserRow { top, ..*row });
    row.sessions = 0;
}

/// [chunk][bucket] -> [bucket][chunk], moving the inner Vecs (no element copies), so each
/// bucket can be consumed (and freed) on its own.
fn transpose<T>(parts: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut by: Vec<Vec<Vec<T>>> = (0..NB).map(|_| Vec::with_capacity(parts.len())).collect();
    for chunk in parts {
        for (bi, v) in chunk.into_iter().enumerate() {
            by[bi].push(v);
        }
    }
    by
}

// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().ok();

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    let day0 = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // ---- products -> category index; campaigns -> campaign index
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: FxHashMap<Vec<u8>, u16> = FxHashMap::default();
    let mut product_cat: FxHashMap<Key, u16> = FxHashMap::default();
    small_csv::<2>(&data.join("products.csv"), |f| {
        let c = *cat_idx.entry(f[1].to_vec()).or_insert_with(|| {
            cat_names.push(f[1].to_vec());
            (cat_names.len() - 1) as u16
        });
        if !f[0].is_empty() {
            product_cat.insert(key_of(f[0]), c);
        }
    });
    let unknown = cat_names.len() as u16;
    cat_names.push(b"unknown".to_vec());
    let mut order: Vec<u16> = (0..cat_names.len() as u16).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(&cat_names[b as usize]));
    let mut cat_rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u16;
    }
    let ncat = cat_names.len();

    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channel: Vec<Vec<u8>> = Vec::new();
    let mut camp_idx: FxHashMap<Key, u16> = FxHashMap::default();
    small_csv::<2>(&data.join("campaigns.csv"), |f| {
        if !f[0].is_empty() {
            camp_idx.entry(key_of(f[0])).or_insert_with(|| {
                camp_ids.push(f[0].to_vec());
                camp_channel.push(f[1].to_vec());
                (camp_ids.len() - 1) as u16
            });
        }
    });
    let organic = camp_ids.len() as u16;
    let ncamp = camp_ids.len() + 1;
    log(t0, &format!("products {} categories {} campaigns {}", product_cat.len(), ncat, ncamp - 1));

    // ---- links -> iterative union-find -> person name (byte-wise smallest ID) per linked ID.
    // Runs concurrently with event/click parsing; only IDs whose person differs from
    // themselves are stored (resolve() falls back to the ID itself).
    let build_links = || -> FxHashMap<Key, Key> {
    let link_chunks = chunks(&csv_files(&data.join("links")));
    let pairs: Vec<(Key, Key)> = link_chunks
        .par_iter()
        .flat_map_iter(|c| {
            let mut v = Vec::new();
            for_records::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((key_of(f[0]), key_of(f[1])));
                }
            });
            v.into_iter()
        })
        .collect();
    let mut id_ix: FxHashMap<Key, u32> = FxHashMap::default();
    id_ix.reserve(pairs.len());
    let mut ids: Vec<Key> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = parent[x as usize];
        }
        x
    }
    for &(a, b) in &pairs {
        let mut ix = |k: Key| -> u32 {
            *id_ix.entry(k).or_insert_with(|| {
                ids.push(k);
                parent.push(ids.len() as u32 - 1);
                ids.len() as u32 - 1
            })
        };
        let (ia, ib) = (ix(a), ix(b));
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    drop(pairs);
    drop(id_ix);
    let mut root_min: Vec<Key> = vec![NONE; ids.len()];
    for i in 0..ids.len() as u32 {
        let r = find(&mut parent, i) as usize;
        let k = ids[i as usize];
        if root_min[r] == NONE || key_cmp(k, root_min[r]) == Ordering::Less {
            root_min[r] = k;
        }
    }
    let mut person_of: FxHashMap<Key, Key> = FxHashMap::default();
    person_of.reserve(ids.len());
    for i in 0..ids.len() as u32 {
        let r = find(&mut parent, i) as usize;
        if ids[i as usize] != root_min[r] {
            person_of.insert(ids[i as usize], root_min[r]);
        }
    }
    log(t0, &format!("links: {} ids, {} non-name", ids.len(), person_of.len()));
    person_of
    };

    // ---- clicks: parse + validate, bucket by person. No dedup needed: copies are identical,
    // and a duplicate never changes the latest-click choice or the set of assisting campaigns.
    let click_chunks = chunks(&csv_files(&data.join("clicks")));
    let cmin = ws - WEEK;
    let parse_clicks = || -> Vec<Vec<RawClick>> {
        click_chunks
        .par_iter()
        .map(|c| {
            let mut b: Vec<RawClick> = Vec::with_capacity((c.end - c.start) / 45 + 8);
            for_records::<4>(c, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let Some(&camp) = camp_idx.get(&key_of(f[2])) else { return };
                let Some(ts) = parse_ts(f[3]) else { return };
                if ts < cmin || ts >= we {
                    return;
                }
                b.push(RawClick { user: key_of(f[1]), ts, cid: key_of(f[0]), camp });
            });
            b
        })
        .collect()
    };

    // ---- events: parse, bucket by event_id
    let ev_chunks = chunks(&csv_files(&data.join("events")));
    let parse_events = || -> Vec<Vec<Vec<RawEv>>> {
        ev_chunks
        .par_iter()
        .map(|c| {
            // ~75 bytes per event line; pre-size so the buckets rarely reallocate
            let per = (c.end - c.start) / 75 / NB;
            let mut b: Vec<Vec<RawEv>> = (0..NB).map(|_| Vec::with_capacity(per + per / 4 + 8)).collect();
            for_records::<7>(c, |f| {
                if f[0].is_empty() {
                    return;
                }
                let eid = key_of(f[0]);
                let etype = match f[4] {
                    b"view" => 1,
                    b"cart" => 2,
                    b"purchase" => 3,
                    _ => 0,
                };
                let price = if etype == 3 { parse_price(f[6]).unwrap_or(-1) } else { 0 };
                let cat = if f[5].is_empty() { u16::MAX } else { *product_cat.get(&key_of(f[5])).unwrap_or(&unknown) };
                b[bucket(eid)].push(RawEv {
                    eid,
                    user: key_of(f[1]),
                    ts: parse_ts(f[2]).unwrap_or(i64::MIN),
                    ingest: parse_ts(f[3]).unwrap_or(i64::MAX),
                    price,
                    cat,
                    etype,
                });
            });
            b
        })
        .collect()
    };

    // links || events || clicks, then resolve users to persons
    let (person_of, (ev_parts, click_raw)) =
        rayon::join(build_links, || rayon::join(parse_events, parse_clicks));
    log(t0, "links, events and clicks parsed");
    let resolve = |user: Key| -> Key {
        if user == NONE { NONE } else { *person_of.get(&user).unwrap_or(&user) }
    };
    let click_parts: Vec<Vec<Vec<Click>>> = click_raw
        .par_iter()
        .map(|v| {
            let mut b: Vec<Vec<Click>> = (0..NB).map(|_| Vec::new()).collect();
            for r in v {
                let person = resolve(r.user);
                b[bucket(person)].push(Click { person, ts: r.ts, cid: r.cid, camp: r.camp });
            }
            b
        })
        .collect();
    drop(click_raw);
    let click_parts = transpose(click_parts);

    // ---- per event_id bucket: dedup (smallest ingest_ts), validate the kept copy, drop late,
    // re-bucket by person
    let ev_parts = transpose(ev_parts);
    let ev_by_person: Vec<Vec<Vec<Ev>>> = ev_parts
        .into_par_iter()
        .map(|parts| {
            let n: usize = parts.iter().map(|p| p.len()).sum();
            // Dedup in place over the per-chunk slices (no copy of the 64 B records):
            // `win` maps event_id -> (ingest, part, row) of the copy kept so far; `lose`
            // marks the copies that lost (a later or equal ingest_ts).
            let mut lose: Vec<Vec<bool>> = parts.iter().map(|p| vec![false; p.len()]).collect();
            let mut win: FxHashMap<Key, (i64, u32, u32)> = FxHashMap::default();
            win.reserve(n);
            for (pi, p) in parts.iter().enumerate() {
                for (ri, r) in p.iter().enumerate() {
                    match win.entry(r.eid) {
                        std::collections::hash_map::Entry::Vacant(v) => {
                            v.insert((r.ingest, pi as u32, ri as u32));
                        }
                        std::collections::hash_map::Entry::Occupied(mut o) => {
                            let (ing, wp, wr) = *o.get();
                            if r.ingest < ing {
                                lose[wp as usize][wr as usize] = true;
                                o.insert((r.ingest, pi as u32, ri as u32));
                            } else {
                                lose[pi][ri] = true;
                            }
                        }
                    }
                }
            }
            drop(win);
            let per = n / NB + n / NB / 4 + 8;
            let mut out: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(per)).collect();
            for (r, _) in parts
                .iter()
                .zip(lose.iter())
                .flat_map(|(p, l)| p.iter().zip(l.iter()))
                .filter(|(_, l)| !**l)
            {
                if r.user == NONE
                    || r.ts == i64::MIN
                    || r.ts < ws
                    || r.ts >= we
                    || r.etype == 0
                    || r.cat == u16::MAX
                    || (r.etype == 3 && r.price < 1)
                    || r.ingest.saturating_sub(r.ts) > DAY
                {
                    continue;
                }
                let person = resolve(r.user);
                out[bucket(person)].push(Ev {
                    person,
                    ts: r.ts,
                    price: if r.etype == 3 { r.price } else { 0 },
                    cat: r.cat,
                    purchase: r.etype == 3,
                });
            }
            drop(parts); // free this bucket's raw records now
            out
        })
        .collect();
    let ev_by_person = transpose(ev_by_person);
    log(t0, "events deduped and validated");

    // ---- per person bucket: sessions, credit, assists, aggregates.
    // campaign stats per (campaign, day): [sessions, purchasing, revenue, assisted]
    // Campaign stats accumulate per rayon split (about one array per thread), not per bucket.
    type Acc = (Vec<(usize, Vec<UserRow>)>, Vec<[i64; 4]>);
    let new_acc = || -> Acc { (Vec::new(), vec![[0i64; 4]; ncamp * nd]) };
    let (mut results, camp): Acc = ev_by_person
        .into_par_iter()
        .zip(click_parts.into_par_iter())
        .enumerate()
        .fold(new_acc, |(mut rv, mut camp), (pb, (eparts, cparts))| {
            let mut evs: Vec<Ev> = Vec::with_capacity(eparts.iter().map(|p| p.len()).sum());
            for p in eparts {
                evs.extend_from_slice(&p); // each source Vec is freed right after its copy
            }
            let mut cks: Vec<Click> = Vec::with_capacity(cparts.iter().map(|p| p.len()).sum());
            for p in cparts {
                cks.extend_from_slice(&p);
            }
            // Group by person with a counting sort on local person ids (no global comparison
            // sort on the 128-bit key), then sort each person's events by ts. Equal timestamps
            // are adjacent, so a tie never starts a session whatever order ties come in.
            let mut pid_of: FxHashMap<Key, u32> = FxHashMap::default();
            pid_of.reserve(evs.len() / 8 + 16);
            let mut persons: Vec<Key> = Vec::new();
            let mut eoff: Vec<u32> = Vec::new();
            let epid: Vec<u32> = evs
                .iter()
                .map(|e| {
                    let p = *pid_of.entry(e.person).or_insert_with(|| {
                        persons.push(e.person);
                        eoff.push(0);
                        (persons.len() - 1) as u32
                    });
                    eoff[p as usize] += 1;
                    p
                })
                .collect();
            let np = persons.len();
            let mut acc = 0u32;
            for c in eoff.iter_mut() {
                let n = *c;
                *c = acc;
                acc += n;
            }
            eoff.push(acc);
            let mut grouped: Vec<Ev> = Vec::with_capacity(evs.len());
            unsafe { grouped.set_len(evs.len()) };
            {
                let mut pos: Vec<u32> = eoff[..np].to_vec();
                for (e, &p) in evs.iter().zip(epid.iter()) {
                    grouped[pos[p as usize] as usize] = *e;
                    pos[p as usize] += 1;
                }
            }
            drop(evs);
            drop(epid);
            for p in 0..np {
                grouped[eoff[p] as usize..eoff[p + 1] as usize].sort_unstable_by_key(|e| e.ts);
            }
            let evs = grouped;
            // clicks: only persons with events matter (others can't credit or assist anything)
            let mut coff: Vec<u32> = vec![0; np + 1];
            let cpid: Vec<u32> = cks
                .iter()
                .map(|c| match pid_of.get(&c.person) {
                    Some(&p) => {
                        coff[p as usize] += 1;
                        p
                    }
                    None => u32::MAX,
                })
                .collect();
            let mut acc = 0u32;
            for c in coff.iter_mut() {
                let n = *c;
                *c = acc;
                acc += n;
            }
            let mut cgrouped: Vec<Click> = Vec::with_capacity(acc as usize);
            unsafe { cgrouped.set_len(acc as usize) };
            {
                let mut pos: Vec<u32> = coff[..np].to_vec();
                for (c, &p) in cks.iter().zip(cpid.iter()) {
                    if p != u32::MAX {
                        cgrouped[pos[p as usize] as usize] = *c;
                        pos[p as usize] += 1;
                    }
                }
            }
            drop(cks);
            drop(cpid);
            drop(pid_of);
            for p in 0..np {
                cgrouped[coff[p] as usize..coff[p + 1] as usize]
                    .sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.cid, b.cid)));
            }
            let cks = cgrouped;
            let mut rows: Vec<UserRow> = Vec::new();
            let mut catrev = vec![0i64; ncat];
            let mut touched: Vec<u16> = Vec::new();
            let mut starts: Vec<i64> = Vec::new();
            let mut ccamp: Vec<(u16, i64)> = Vec::new();
            for p in 0..np {
                let person = persons[p];
                let (i, iend) = (eoff[p] as usize, eoff[p + 1] as usize);
                let pc = &cks[coff[p] as usize..coff[p + 1] as usize];
                starts.clear();
                let mut row = UserRow { person, day: i32::MIN, sessions: 0, events: 0, revenue: 0, top: None };
                let mut k = i;
                let mut cp = 0usize; // clicks with ts <= current start are pc[..cp]
                let mut grp = 0usize; // first click of the latest-ts group in pc[..cp]
                while k < iend {
                    let start = evs[k].ts;
                    let sday = start.div_euclid(DAY);
                    if sday as i32 != row.day {
                        flush_row(&mut row, &mut rows, &mut catrev, &mut touched, &cat_rank);
                        row = UserRow { person, day: sday as i32, sessions: 0, events: 0, revenue: 0, top: None };
                    }
                    let mut last = start;
                    let (mut n, mut rev, mut purch) = (0i64, 0i64, false);
                    while k < iend && evs[k].ts - last <= HALF_HOUR {
                        let e = &evs[k];
                        last = e.ts;
                        n += 1;
                        if e.purchase {
                            purch = true;
                            rev += e.price as i64;
                            if catrev[e.cat as usize] == 0 {
                                touched.push(e.cat);
                            }
                            catrev[e.cat as usize] += e.price as i64;
                        }
                        k += 1;
                    }
                    row.sessions += 1;
                    row.events += n;
                    row.revenue += rev;
                    starts.push(start);
                    // credit: latest click at or before the start; ties -> smallest click_id
                    while cp < pc.len() && pc[cp].ts <= start {
                        if cp == 0 || pc[cp].ts != pc[cp - 1].ts {
                            grp = cp;
                        }
                        cp += 1;
                    }
                    let credited = if cp > 0 && start - pc[grp].ts <= WEEK { pc[grp].camp } else { organic };
                    let s = &mut camp[credited as usize * nd + (sday - day0) as usize];
                    s[0] += 1;
                    s[1] += purch as i64;
                    s[2] += rev;
                }
                flush_row(&mut row, &mut rows, &mut catrev, &mut touched, &cat_rank);
                // assists: each campaign's clicks cover [ts, ts + 7d]; merge the coverage and
                // count every session start inside it once per campaign
                if !pc.is_empty() {
                    ccamp.clear();
                    ccamp.extend(pc.iter().map(|c| (c.camp, c.ts)));
                    ccamp.sort_unstable();
                    let mut a = 0;
                    while a < ccamp.len() {
                        let cid = ccamp[a].0;
                        let lo = ccamp[a].1;
                        let mut hi = lo;
                        let mut b = a + 1;
                        while b < ccamp.len() && ccamp[b].0 == cid && ccamp[b].1 - hi <= WEEK {
                            hi = ccamp[b].1;
                            b += 1;
                        }
                        let hi = hi + WEEK;
                        let mut s = starts.partition_point(|&x| x < lo);
                        while s < starts.len() && starts[s] <= hi {
                            camp[cid as usize * nd + (starts[s].div_euclid(DAY) - day0) as usize][3] += 1;
                            s += 1;
                        }
                        a = b;
                    }
                }
            }
            rv.push((pb, rows));
            (rv, camp)
        })
        .reduce(new_acc, |(mut ra, mut ca), (rb, cb)| {
            ra.extend(rb);
            for (t, s) in ca.iter_mut().zip(cb.iter()) {
                for q in 0..4 {
                    t[q] += s[q];
                }
            }
            (ra, ca)
        });
    results.sort_unstable_by_key(|(pb, _)| *pb);
    log(t0, "sessions and attribution done");

    // ---- merge campaign stats, write outputs
    std::fs::create_dir_all(out.join("user_daily")).unwrap();
    std::fs::create_dir_all(out.join("campaign_daily")).unwrap();
    // user_daily as several Parquet files written in parallel (rules: one or more files).
    const PARTS: usize = 8;
    let per_part = NB.div_ceil(PARTS);
    let n_user: usize = (0..PARTS)
        .into_par_iter()
        .map(|p| {
            let rows: Vec<UserRow> = results[(p * per_part).min(NB)..((p + 1) * per_part).min(NB)]
                .iter()
                .flat_map(|(_, r)| r.iter().copied())
                .collect();
            write_user(&out.join(format!("user_daily/part-{p}.parquet")), &rows, &cat_names);
            rows.len()
        })
        .sum();

    let (mut cids, mut chans, mut days) = (Vec::new(), Vec::new(), Vec::new());
    let mut vals: [Vec<i64>; 4] = Default::default();
    for c in 0..ncamp {
        for d in 0..nd {
            let s = camp[c * nd + d];
            if s[0] == 0 && s[3] == 0 {
                continue;
            }
            if c == organic as usize {
                cids.push(b"organic".to_vec());
                chans.push(b"organic".to_vec());
            } else {
                cids.push(camp_ids[c].clone());
                chans.push(camp_channel[c].clone());
            }
            days.push((day0 + d as i64) as i32);
            for q in 0..4 {
                vals[q].push(if c == organic as usize && q == 3 { 0 } else { s[q] });
            }
        }
    }
    write_campaign(&out.join("campaign_daily/part-0.parquet"), &cids, &chans, &days, &vals);
    log(t0, &format!("done: {} user rows, {} campaign rows", n_user, days.len()));
}

// ---------------------------------------------------------------- parquet

fn props() -> Arc<WriterProperties> {
    Arc::new(WriterProperties::builder().set_dictionary_enabled(false).build())
}

fn write_user(path: &Path, rows: &[UserRow], cats: &[Vec<u8>]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily {
                required binary user_id (STRING);
                required int32 day (DATE);
                required int64 sessions;
                required int64 events;
                required int64 revenue_cents;
                optional binary top_category (STRING);
            }",
        )
        .unwrap(),
    );
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut col = 0;
    while let Some(mut c) = rg.next_column().unwrap() {
        match col {
            0 => {
                let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(key_bytes(r.person))).collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            1 => {
                let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            2..=4 => {
                let v: Vec<i64> = rows
                    .iter()
                    .map(|r| match col {
                        2 => r.sessions,
                        3 => r.events,
                        _ => r.revenue,
                    })
                    .collect();
                c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
            _ => {
                let defs: Vec<i16> = rows.iter().map(|r| r.top.is_some() as i16).collect();
                let v: Vec<ByteArray> =
                    rows.iter().filter_map(|r| r.top.map(|t| ByteArray::from(cats[t as usize].clone()))).collect();
                c.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
            }
        }
        c.close().unwrap();
        col += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}

fn write_campaign(path: &Path, cids: &[Vec<u8>], chans: &[Vec<u8>], days: &[i32], vals: &[Vec<i64>; 4]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily {
                required binary campaign_id (STRING);
                required binary channel (STRING);
                required int32 day (DATE);
                required int64 sessions;
                required int64 purchasing_sessions;
                required int64 revenue_cents;
                required int64 assisted_sessions;
            }",
        )
        .unwrap(),
    );
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, props()).unwrap();
    let mut rg = w.next_row_group().unwrap();
    let mut col = 0;
    while let Some(mut c) = rg.next_column().unwrap() {
        match col {
            0 | 1 => {
                let src = if col == 0 { cids } else { chans };
                let v: Vec<ByteArray> = src.iter().map(|b| ByteArray::from(b.clone())).collect();
                c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            }
            2 => {
                c.typed::<Int32Type>().write_batch(days, None, None).unwrap();
            }
            _ => {
                c.typed::<Int64Type>().write_batch(&vals[col - 3], None, None).unwrap();
            }
        }
        c.close().unwrap();
        col += 1;
    }
    rg.close().unwrap();
    w.close().unwrap();
}
