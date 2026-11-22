//! 64-bit exact ID keys.
//!
//! An ID of 1..=10 bytes from [0-9A-Z_a-z] is packed: each byte becomes a 6-bit code
//! ('0'-'9' -> 1-10, 'A'-'Z' -> 11-36, '_' -> 37, 'a'-'z' -> 38-63; 0 pads), first byte in
//! the top bits, so u64 order is byte-wise ID order (a prefix sorts first). The low 4 bits are
//! 0. Any other ID is interned in a global lock-free table: low 4 bits 0xF, the node index
//! (+1) above them; those compare through their bytes (`key_cmp`). The empty ID is 0 (`NONE`).

use std::cell::Cell;
use std::cmp::Ordering;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering as AO};

pub type Key = u64;
pub const NONE: Key = 0;

// ---------------------------------------------------------------- long-ID table
//
// Chained hash table: bucket heads are atomic node indexes (+1, 0 = empty); nodes live in
// segments allocated on demand and never move; a node's bytes are copied once into a
// per-thread arena that is never freed. Insert = walk the chain, else CAS a new head.

#[derive(Clone, Copy)]
struct Node {
    hash: u64,
    ptr: *const u8,
    len: u32,
    next: u32, // node index + 1, 0 = end
}

const SEG_BITS: usize = 18;
const SEG: usize = 1 << SEG_BITS;
const NSEG: usize = 1 << 14; // up to 2^32 nodes
static SEGS: [AtomicPtr<Node>; NSEG] = [const { AtomicPtr::new(std::ptr::null_mut()) }; NSEG];
static NEXT: AtomicUsize = AtomicUsize::new(0);
static BUCKETS: OnceLock<Box<[AtomicU32]>> = OnceLock::new();

/// Sizes the long-ID table for about `expected` distinct long IDs (call before any key_of).
pub fn init_long(expected: usize) {
    let _ = BUCKETS.set(new_buckets(expected));
}

fn new_buckets(expected: usize) -> Box<[AtomicU32]> {
    let n = expected.next_power_of_two().clamp(1 << 12, 1 << 26);
    (0..n).map(|_| AtomicU32::new(0)).collect()
}

fn buckets() -> &'static [AtomicU32] {
    BUCKETS.get_or_init(|| new_buckets(1 << 16))
}

#[inline]
fn node_ptr(idx: usize) -> *mut Node {
    let seg = SEGS[idx >> SEG_BITS].load(AO::Acquire);
    unsafe { seg.add(idx & (SEG - 1)) }
}

fn alloc_node() -> usize {
    let idx = NEXT.fetch_add(1, AO::Relaxed);
    let s = idx >> SEG_BITS;
    assert!(s < NSEG, "too many long IDs");
    if SEGS[s].load(AO::Acquire).is_null() {
        let layout = std::alloc::Layout::array::<Node>(SEG).unwrap();
        let p = unsafe { std::alloc::alloc(layout) as *mut Node };
        if SEGS[s].compare_exchange(std::ptr::null_mut(), p, AO::AcqRel, AO::Acquire).is_err() {
            unsafe { std::alloc::dealloc(p as *mut u8, layout) };
        }
    }
    idx
}

thread_local! {
    static ARENA: Cell<(*mut u8, usize)> = const { Cell::new((std::ptr::null_mut(), 0)) };
}

/// Copies b into this thread's never-freed arena.
fn arena_copy(b: &[u8]) -> *const u8 {
    ARENA.with(|a| {
        let (mut p, mut left) = a.get();
        if left < b.len() {
            let sz = (1usize << 20).max(b.len());
            p = Box::leak(vec![0u8; sz].into_boxed_slice()).as_mut_ptr();
            left = sz;
        }
        unsafe { std::ptr::copy_nonoverlapping(b.as_ptr(), p, b.len()) };
        a.set((unsafe { p.add(b.len()) }, left - b.len()));
        p
    })
}

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    const M: u64 = 0xff51_afd7_ed55_8ccd;
    let l = b.len();
    let mut h = 0x9E37_79B9_7F4A_7C15u64 ^ (l as u64).wrapping_mul(M);
    let rd = |i: usize| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    if l >= 8 {
        let mut i = 0;
        while i + 8 <= l {
            h = (h ^ rd(i)).wrapping_mul(M);
            h ^= h >> 32;
            i += 8;
        }
        if i < l {
            h = (h ^ rd(l - 8)).wrapping_mul(M);
            h ^= h >> 32;
        }
    } else {
        let mut buf = [0u8; 8];
        buf[..l].copy_from_slice(b);
        h = (h ^ u64::from_le_bytes(buf)).wrapping_mul(M);
        h ^= h >> 32;
    }
    h.wrapping_mul(0xc4ce_b9fe_1a85_ec53) ^ (h >> 29)
}

#[inline]
fn node_bytes(n: &Node) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts(n.ptr, n.len as usize) }
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

/// SIMD form of the packing: 16 bytes from p (readable), the first l (1..=10) are the ID.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn pack16(p: *const u8, l: usize) -> Option<u64> {
    unsafe {
        use core::arch::aarch64::*;
        const IDX: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let live = vcltq_u8(vld1q_u8(IDX.as_ptr()), vdupq_n_u8(l as u8));
        let v = vandq_u8(vld1q_u8(p), live);
        let dig = vcltq_u8(vsubq_u8(v, vdupq_n_u8(b'0')), vdupq_n_u8(10));
        let up = vcltq_u8(vsubq_u8(v, vdupq_n_u8(b'A')), vdupq_n_u8(26));
        let us = vceqq_u8(v, vdupq_n_u8(b'_'));
        let lw = vcltq_u8(vsubq_u8(v, vdupq_n_u8(b'a')), vdupq_n_u8(26));
        let ok = vorrq_u8(vorrq_u8(dig, up), vorrq_u8(us, lw));
        if vmaxvq_u8(vandq_u8(live, vmvnq_u8(ok))) != 0 {
            return None;
        }
        let code = vorrq_u8(
            vorrq_u8(vandq_u8(dig, vsubq_u8(v, vdupq_n_u8(47))), vandq_u8(up, vsubq_u8(v, vdupq_n_u8(54)))),
            vorrq_u8(vandq_u8(us, vdupq_n_u8(37)), vandq_u8(lw, vsubq_u8(v, vdupq_n_u8(59)))),
        );
        let t = vreinterpretq_u16_u8(code);
        let t = vorrq_u16(vshlq_n_u16(vandq_u16(t, vdupq_n_u16(0xFF)), 6), vshrq_n_u16(t, 8));
        let t = vreinterpretq_u32_u16(t);
        let t = vorrq_u32(vshlq_n_u32(vandq_u32(t, vdupq_n_u32(0xFFFF)), 12), vshrq_n_u32(t, 16));
        let t = vreinterpretq_u64_u32(t);
        let t = vorrq_u64(vshlq_n_u64(vandq_u64(t, vdupq_n_u64(0xFFFF_FFFF)), 24), vshrq_n_u64(t, 32));
        let (a, b) = (vgetq_lane_u64(t, 0), vgetq_lane_u64(t, 1));
        Some(((a << 12) | (b >> 36)) << 4)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1", target_feature = "ssse3"))]
#[inline(always)]
unsafe fn pack16(p: *const u8, l: usize) -> Option<u64> {
    unsafe {
        use core::arch::x86_64::*;
        let idx = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        let live = _mm_cmpgt_epi8(_mm_set1_epi8(l as i8), idx);
        let v = _mm_and_si128(_mm_loadu_si128(p as *const __m128i), live);
        let inr = |lo: u8, n: u8| {
            let d = _mm_sub_epi8(v, _mm_set1_epi8(lo as i8));
            _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8((n - 1) as i8)), d)
        };
        let dig = inr(b'0', 10);
        let up = inr(b'A', 26);
        let us = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'_' as i8));
        let lw = inr(b'a', 26);
        let ok = _mm_or_si128(_mm_or_si128(dig, up), _mm_or_si128(us, lw));
        if _mm_movemask_epi8(_mm_andnot_si128(ok, live)) != 0 {
            return None;
        }
        let sub = |c: u8| _mm_sub_epi8(v, _mm_set1_epi8(c as i8));
        let code = _mm_or_si128(
            _mm_or_si128(_mm_and_si128(dig, sub(47)), _mm_and_si128(up, sub(54))),
            _mm_or_si128(_mm_and_si128(us, _mm_set1_epi8(37)), _mm_and_si128(lw, sub(59))),
        );
        let p12 = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140)); // even*64 + odd
        let p24 = _mm_madd_epi16(p12, _mm_set1_epi32(0x0001_1000)); // even*4096 + odd
        let a = _mm_cvtsi128_si64(p24) as u64;
        let b = _mm_extract_epi64(p24, 1) as u64;
        let a = ((a & 0xFFFF_FFFF) << 24) | (a >> 32);
        let b = ((b & 0xFFFF_FFFF) << 24) | (b >> 32);
        Some(((a << 12) | (b >> 36)) << 4)
    }
}

#[cfg(not(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "sse4.1", target_feature = "ssse3"))))]
#[inline(always)]
unsafe fn pack16(p: *const u8, l: usize) -> Option<u64> {
    let raw = unsafe { std::ptr::read_unaligned(p as *const [u64; 2]) };
    let lo = u64::from_le(raw[0]);
    let hi = u64::from_le(raw[1]);
    let (lo, hi) = if l >= 8 { (lo, hi & ((1u64 << (8 * (l - 8))) - 1)) } else { (lo & ((1u64 << (8 * l)) - 1), 0) };
    let (c0, c1) = (codes(lo, l.min(8))?, codes(hi, l.saturating_sub(8))?);
    Some(pack_codes(c0, c1))
}

#[inline(always)]
fn pack_codes(c0: u64, c1: u64) -> u64 {
    let mut x = c0.swap_bytes();
    x = ((x & 0x3F00_3F00_3F00_3F00) >> 2) | (x & 0x003F_003F_003F_003F);
    x = ((x & 0x0FFF_0000_0FFF_0000) >> 4) | (x & 0x0000_0FFF_0000_0FFF);
    x = ((x & 0x00FF_FFFF_0000_0000) >> 8) | (x & 0x0000_0000_00FF_FFFF);
    let h = ((c1 & 0x3F) << 6) | ((c1 >> 8) & 0x3F);
    ((x << 12) | h) << 4
}

#[inline]
pub fn key_of(b: &[u8]) -> Key {
    let l = b.len();
    if l == 0 {
        return NONE;
    }
    if l <= 10 {
        let k = key_of_short(b);
        if k != NONE {
            return k;
        }
    }
    long_key(b)
}

/// Packed key of a 1..=10-byte ID, or NONE when it is not packable.
#[inline(always)]
fn key_of_short(b: &[u8]) -> Key {
    let l = b.len();
    {
        let p = b.as_ptr();
        if (p as usize & 4095) <= 4096 - 16 {
            // 16 bytes from p stay inside p's page
            if let Some(k) = unsafe { pack16(p, l) } {
                return k;
            }
        } else {
            let lo = load_le(b, 0, l.min(8));
            let hi = if l > 8 { load_le(b, 8, l - 8) } else { 0 };
            if let (Some(c0), Some(c1)) = (codes(lo, l.min(8)), codes(hi, l.saturating_sub(8))) {
                return pack_codes(c0, c1);
            }
        }
    }
    NONE
}

#[inline(never)]
fn long_key(b: &[u8]) -> Key {
    let h = hash_bytes(b);
    let bk = buckets();
    let head = &bk[(h as usize) & (bk.len() - 1)];
    let mut mine: usize = usize::MAX; // allocated node, if any
    let mut first = head.load(AO::Acquire);
    loop {
        // walk the chain for an equal ID
        let mut cur = first;
        while cur != 0 {
            let n = unsafe { &*node_ptr(cur as usize - 1) };
            if n.hash == h && node_bytes(n) == b {
                return ((cur as u64) << 4) | 0xF;
            }
            cur = n.next;
        }
        if mine == usize::MAX {
            mine = alloc_node();
            unsafe { *node_ptr(mine) = Node { hash: h, ptr: arena_copy(b), len: b.len() as u32, next: first } };
        } else {
            unsafe { (*node_ptr(mine)).next = first };
        }
        match head.compare_exchange(first, mine as u32 + 1, AO::AcqRel, AO::Acquire) {
            Ok(_) => return (((mine + 1) as u64) << 4) | 0xF,
            Err(now) => first = now, // new nodes in front: check them, then retry
        }
    }
}

/// Event IDs need only equality (they are never output or ordered). A packable one gets its
/// packed key and src 0; any other gets a 64-bit hash key tagged 0xE in the low nibble (never a
/// packed or interned key) plus `src` = its location (ptr << 16 | len) in the mapped input,
/// which stays valid for the whole run: equal hash keys are confirmed by comparing bytes.
#[inline]
pub fn eid_key(b: &'static [u8]) -> (Key, u64) {
    let l = b.len();
    if l <= 10 {
        let k = key_of_short(b);
        if k != NONE {
            return (k, 0);
        }
    }
    if l >= 1 << 16 {
        return (key_of(b), 0); // absurdly long: intern
    }
    ((hash_bytes(b) & !0xF) | 0xE, ((b.as_ptr() as u64) << 16) | l as u64)
}

/// Same ID behind two `eid_key` results with equal keys.
#[inline]
pub fn same_eid(k: Key, src_a: u64, src_b: u64) -> bool {
    if k & 0xF != 0xE {
        return true;
    }
    let (la, lb) = ((src_a & 0xFFFF) as usize, (src_b & 0xFFFF) as usize);
    la == lb
        && unsafe {
            std::slice::from_raw_parts((src_a >> 16) as *const u8, la) == std::slice::from_raw_parts((src_b >> 16) as *const u8, lb)
        }
}

/// The bytes of a long key.
#[inline]
fn long_bytes(k: Key) -> &'static [u8] {
    node_bytes(unsafe { &*node_ptr((k >> 4) as usize - 1) })
}

#[inline]
pub fn is_long(k: Key) -> bool {
    k & 0xF != 0
}

const DECODE: &[u8; 64] = b"\x000123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

/// Appends the ID behind a key.
pub fn push_bytes(k: Key, out: &mut Vec<u8>) {
    if is_long(k) {
        out.extend_from_slice(long_bytes(k));
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

#[allow(dead_code)]
pub fn key_bytes(k: Key) -> Vec<u8> {
    let mut v = Vec::with_capacity(10);
    push_bytes(k, &mut v);
    v
}

/// Byte-wise comparison of the IDs behind two keys.
#[inline]
pub fn key_cmp(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) {
        return a.cmp(&b);
    }
    let (mut ba, mut bb) = ([0u8; 10], [0u8; 10]);
    short_or_long(a, &mut ba).cmp(short_or_long(b, &mut bb))
}

#[inline]
fn short_or_long(k: Key, buf: &mut [u8; 10]) -> &[u8] {
    if is_long(k) {
        return long_bytes(k);
    }
    let mut n = 0;
    while n < 10 {
        let c = ((k >> (58 - 6 * n)) & 0x3F) as usize;
        if c == 0 {
            break;
        }
        buf[n] = DECODE[c];
        n += 1;
    }
    &buf[..n]
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
    fn long_ids_concurrent() {
        let ids: Vec<Vec<u8>> = (0..20_000u32).map(|i| format!("user_{:08}_{:032x}", i % 5000, i as u128 * 0x9e3779b97f4a7c15).into_bytes()).collect();
        let ids = std::sync::Arc::new(ids);
        let hs: Vec<_> = (0..8)
            .map(|t| {
                let ids = ids.clone();
                std::thread::spawn(move || {
                    let mut v = Vec::new();
                    for j in 0..ids.len() {
                        let i = (j * 7 + t * 2503) % ids.len();
                        v.push((i, key_of(&ids[i])));
                    }
                    v
                })
            })
            .collect();
        let mut by_id: std::collections::HashMap<usize, Key> = std::collections::HashMap::new();
        for h in hs {
            for (i, k) in h.join().unwrap() {
                assert!(is_long(k));
                assert_eq!(*by_id.entry(i).or_insert(k), k, "same ID, different keys");
            }
        }
        let mut seen: std::collections::HashMap<Key, &Vec<u8>> = std::collections::HashMap::new();
        for (&i, &k) in &by_id {
            assert_eq!(key_bytes(k), ids[i]);
            if let Some(prev) = seen.insert(k, &ids[i]) {
                assert_eq!(prev, &ids[i], "different IDs, same key");
            }
        }
        for a in 0..300 {
            for b in 0..300 {
                assert_eq!(key_cmp(by_id[&a], by_id[&b]), ids[a].cmp(&ids[b]));
            }
        }
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
