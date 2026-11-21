//! 64-bit exact ID keys.
//!
//! An ID of 1..=10 bytes from [0-9A-Z_a-z] is packed: each byte becomes a 6-bit code
//! ('0'-'9' -> 1-10, 'A'-'Z' -> 11-36, '_' -> 37, 'a'-'z' -> 38-63; 0 pads), first byte in
//! the top bits, so u64 order is byte-wise ID order (a prefix sorts first). The low 4 bits are
//! 0. Any other ID is interned in a global table: low 4 bits 0xF, the index above them; those
//! compare through their bytes (`key_cmp`). The empty ID is 0 (`NONE`).

use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::sync::{Mutex, OnceLock};

pub type Key = u64;
pub const NONE: Key = 0;

type LongTable = Mutex<(FxHashMap<Vec<u8>, u64>, Vec<Vec<u8>>)>;
static LONG: OnceLock<LongTable> = OnceLock::new();

fn long_table() -> &'static LongTable {
    LONG.get_or_init(|| Mutex::new((FxHashMap::default(), Vec::new())))
}

const LO: u64 = 0x0101_0101_0101_0101;
const HI: u64 = 0x8080_8080_8080_8080;

/// 0x80 in each byte of x (7-bit bytes) that is >= c.
#[inline(always)]
fn ge(x: u64, c: u8) -> u64 {
    ((x | HI).wrapping_sub(LO * c as u64)) & HI
}

#[inline(always)]
fn full(m: u64) -> u64 {
    (m >> 7) * 0xFF
}

/// Codes of the 8 bytes of x (bytes past `n` must be 0); None if a byte < n is not in the set.
#[inline(always)]
fn codes(x: u64, n: usize) -> Option<u64> {
    let live = if n >= 8 { HI } else { HI & ((1u64 << (8 * n)) - 1) };
    if x & HI != 0 {
        return None;
    }
    let dig = ge(x, b'0') & !ge(x, b':');
    let up = ge(x, b'A') & !ge(x, b'[');
    let us = ge(x, b'_') & !ge(x, b'`');
    let lw = ge(x, b'a') & !ge(x, b'{');
    if (dig | up | us | lw) & live != live {
        return None;
    }
    let off = (full(dig) & (47 * LO)) | (full(up) & (54 * LO)) | (full(us) & (58 * LO)) | (full(lw) & (59 * LO));
    Some(x - off)
}

#[inline(always)]
fn load_le(b: &[u8], from: usize, n: usize) -> u64 {
    // n bytes from b[from..], little-endian, zero above
    let mut buf = [0u8; 8];
    buf[..n].copy_from_slice(&b[from..from + n]);
    u64::from_le_bytes(buf)
}

#[inline]
pub fn key_of(b: &[u8]) -> Key {
    let l = b.len();
    if l == 0 {
        return NONE;
    }
    if l <= 10 {
        let p = b.as_ptr();
        let (lo, hi) = if (p as usize & 4095) <= 4096 - 16 {
            // 16 bytes from p stay inside p's page: over-read, then mask
            let raw = unsafe { std::ptr::read_unaligned(p as *const [u64; 2]) };
            let lo = u64::from_le(raw[0]);
            let hi = u64::from_le(raw[1]);
            if l >= 8 {
                (lo, hi & ((1u64 << (8 * (l - 8))) - 1))
            } else {
                (lo & ((1u64 << (8 * l)) - 1), 0)
            }
        } else {
            (load_le(b, 0, l.min(8)), if l > 8 { load_le(b, 8, l - 8) } else { 0 })
        };
        if let (Some(c0), Some(c1)) = (codes(lo, l.min(8)), codes(hi, l.saturating_sub(8))) {
            // pack 8 codes, first byte on top
            let mut x = c0.swap_bytes();
            x = ((x & 0x3F00_3F00_3F00_3F00) >> 2) | (x & 0x003F_003F_003F_003F);
            x = ((x & 0x0FFF_0000_0FFF_0000) >> 4) | (x & 0x0000_0FFF_0000_0FFF);
            x = ((x & 0x00FF_FFFF_0000_0000) >> 8) | (x & 0x0000_0000_00FF_FFFF);
            let h = ((c1 & 0x3F) << 6) | ((c1 >> 8) & 0x3F);
            return ((x << 12) | h) << 4;
        }
    }
    long_key(b)
}

#[cold]
fn long_key(b: &[u8]) -> Key {
    let mut t = long_table().lock().unwrap();
    if let Some(&id) = t.0.get(b) {
        return (id << 4) | 0xF;
    }
    let id = t.1.len() as u64;
    t.1.push(b.to_vec());
    t.0.insert(b.to_vec(), id);
    (id << 4) | 0xF
}

#[inline]
pub fn is_long(k: Key) -> bool {
    k & 0xF != 0
}

const DECODE: &[u8; 64] = b"\x000123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

/// Appends the ID behind a key.
pub fn push_bytes(k: Key, out: &mut Vec<u8>) {
    if is_long(k) {
        out.extend_from_slice(&long_table().lock().unwrap().1[(k >> 4) as usize]);
    } else {
        for i in 0..10 {
            let c = ((k >> (58 - 6 * i)) & 0x3F) as usize;
            if c == 0 {
                break;
            }
            out.push(DECODE[c]);
        }
    }
}

pub fn key_bytes(k: Key) -> Vec<u8> {
    let mut v = Vec::with_capacity(10);
    push_bytes(k, &mut v);
    v
}

/// Byte-wise comparison of the IDs behind two keys.
#[inline]
pub fn key_cmp(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) { a.cmp(&b) } else { key_bytes(a).cmp(&key_bytes(b)) }
}

/// Well-mixed hash of a key; partitions take the top bits, tables the middle bits.
#[inline(always)]
pub fn mix(k: Key) -> u64 {
    let x = (k ^ (k >> 31)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reference(b: &[u8]) -> Option<u64> {
        if b.is_empty() || b.len() > 10 {
            return None;
        }
        let mut k = 0u64;
        for i in 0..10 {
            let c = if i < b.len() {
                match b[i] {
                    b'0'..=b'9' => b[i] - 47,
                    b'A'..=b'Z' => b[i] - 54,
                    b'_' => 37,
                    b'a'..=b'z' => b[i] - 59,
                    _ => return None,
                }
            } else {
                0
            };
            k = (k << 6) | c as u64;
        }
        Some(k << 4)
    }
    #[test]
    fn pack_matches_reference_and_order() {
        let mut ids: Vec<Vec<u8>> = Vec::new();
        let alpha: Vec<u8> = (0u8..=255).collect();
        let mut s = 12345u64;
        for _ in 0..200_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let l = (s % 13) as usize;
            let mut v = Vec::new();
            for j in 0..l {
                let r = (s >> (j * 5)) as usize;
                v.push(if r % 7 == 0 { alpha[r % 256] } else { b"09AZ_az5mQ"[r % 10] });
            }
            ids.push(v);
        }
        for w in [&b"u1"[..], b"u10", b"u1_", b"zzzzzzzzzz", b"0000000000", b"a\x00", b"\xff", b"u12345678901"] {
            ids.push(w.to_vec());
        }
        let keys: Vec<Key> = ids.iter().map(|b| key_of(b)).collect();
        for (b, &k) in ids.iter().zip(keys.iter()) {
            match reference(b) {
                Some(r) => assert_eq!(k, r, "{:?}", b),
                None => assert!(b.is_empty() && k == NONE || is_long(k), "{:?}", b),
            }
            assert_eq!(&key_bytes(k), b);
        }
        for i in 0..ids.len() - 1 {
            assert_eq!(key_cmp(keys[i], keys[i + 1]), ids[i].cmp(&ids[i + 1]));
            assert_eq!(keys[i] == keys[i + 1], ids[i] == ids[i + 1]);
        }
    }
}
