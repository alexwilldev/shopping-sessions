//! Shopping Sessions v3: a native implementation of rules.md.
//!
//! Pipeline:
//!   1. products, campaigns (small) -> dense lookup tables (array by number for `p<n>` / `c_<n>`)
//!   2. links (parallel parse, sort, union-find) -> read-only table: linked ID -> person name
//!   3. events + clicks, one pass on all threads: each line is parsed, validated (everything
//!      but dedup), its user resolved to a person (prefetched table lookups), and written as a
//!      32-byte record into the thread's buffer for hash(person); every event also writes a
//!      16-byte (event_id, location) record into a dedup partition by hash(event_id)
//!   4. per dedup partition (sized to stay in cache): the copy with the smallest ingest_ts
//!      wins; losers are marked dead in place (no record is moved)
//!   5. per person partition: sessions, credit, assists, aggregates
//!   6. both tables written by a small hand-written Parquet writer; `_exit` right after
//!
//! IDs are 64-bit exact keys (see keys.rs): equality and byte-wise order are integer operations.

use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod keys;
mod pq;
mod scan;
mod ts;

use keys::{Key, NONE, key_cmp, key_of, mix};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering as AO};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const HALF_HOUR: i64 = 1_800_000;
const CHUNK: usize = 4 << 20;

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
}

#[inline(always)]
fn part(k: Key, bits: u32) -> usize {
    (mix(k) >> (64 - bits)) as usize
}

// ---------------------------------------------------------------- dense ID lookups

/// ID -> u16 value. When every ID is `prefix` + a canonical decimal number (e.g. `p123`), the
/// lookup is an array index; otherwise an exact hash map.
struct Dense {
    prefix: Vec<u8>,
    arr: Vec<u16>,
    map: Option<FxHashMap<Vec<u8>, u16>>,
    miss: u16,
}

#[inline]
fn canon_num(b: &[u8]) -> Option<usize> {
    if b.is_empty() || b.len() > 7 || (b[0] == b'0' && b.len() > 1) {
        return None;
    }
    let mut n = 0usize;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n * 10 + d as usize;
    }
    Some(n)
}

impl Dense {
    fn new(map: FxHashMap<Vec<u8>, u16>, miss: u16) -> Dense {
        let prefix: Vec<u8> = map
            .keys()
            .next()
            .map(|b| b.iter().take_while(|c| !c.is_ascii_digit()).cloned().collect())
            .unwrap_or_default();
        let mut maxn = 0usize;
        let mut dense = true;
        for b in map.keys() {
            match b.strip_prefix(&prefix[..]).and_then(canon_num) {
                Some(n) => maxn = maxn.max(n),
                None => {
                    dense = false;
                    break;
                }
            }
        }
        if dense && maxn < (1 << 22) {
            let mut arr = vec![miss; maxn + 1];
            for (b, &v) in &map {
                arr[canon_num(&b[prefix.len()..]).unwrap()] = v;
            }
            Dense { prefix, arr, map: None, miss }
        } else {
            Dense { prefix, arr: Vec::new(), map: Some(map), miss }
        }
    }

    #[inline]
    fn get(&self, b: &[u8]) -> u16 {
        match &self.map {
            None => {
                let pl = self.prefix.len();
                if b.len() > pl && b[..pl] == self.prefix[..] {
                    if let Some(n) = canon_num(&b[pl..]) {
                        if n < self.arr.len() {
                            return self.arr[n];
                        }
                    }
                }
                self.miss
            }
            Some(m) => *m.get(b).unwrap_or(&self.miss),
        }
    }
}

// ---------------------------------------------------------------- link table

/// Read-only table: linked ID -> person name, only for IDs that are not their person's name
/// (anything else resolves to itself). Buckets of 4 (key, value) pairs fill one cache line
/// and are checked without branches; a full bucket continues in the next one (rare).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Bucket([[Key; 2]; 4]);

struct LinkTable {
    mask: usize,
    b: Vec<Bucket>,
}

impl LinkTable {
    fn new(entries: &[(Key, Key)]) -> LinkTable {
        let nb = (entries.len() / 2).next_power_of_two().max(4); // <= 50% full
        let t = LinkTable { mask: nb - 1, b: vec![Bucket([[NONE, NONE]; 4]); nb] };
        // parallel build: claim an empty key slot with a CAS (keys in `entries` are distinct)
        let at: &[[[AtomicU64; 2]; 4]] = unsafe { std::slice::from_raw_parts(t.b.as_ptr() as *const [[AtomicU64; 2]; 4], nb) };
        entries.par_iter().for_each(|&(k, v)| {
            let mut i = t.bucket(k);
            loop {
                for j in 0..4 {
                    if at[i][j][0].compare_exchange(NONE, k, AO::Relaxed, AO::Relaxed).is_ok() {
                        at[i][j][1].store(v, AO::Relaxed);
                        return;
                    }
                }
                i = (i + 1) & t.mask;
            }
        });
        t
    }
    #[inline(always)]
    fn bucket(&self, k: Key) -> usize {
        (mix(k) >> 20) as usize & self.mask
    }
    #[inline(always)]
    fn prefetch(&self, k: Key) {
        prefetch(unsafe { self.b.as_ptr().add(self.bucket(k)) });
    }
    #[inline(always)]
    fn resolve(&self, k: Key) -> Key {
        let mut i = self.bucket(k);
        loop {
            let b = unsafe { self.b.get_unchecked(i) };
            let mut found = 0u64;
            let mut empty = false;
            for j in 0..4 {
                let m = ((b.0[j][0] == k) as u64).wrapping_neg();
                found |= b.0[j][1] & m;
                empty |= b.0[j][0] == NONE;
            }
            if found != 0 {
                return found;
            }
            if empty || k == NONE {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- timestamps

use ts::parse_ts;

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

/// Every well-formed line of a chunk, as N unquoted fields (see scan.rs).
#[inline(always)]
fn for_records<const N: usize>(c: &Chunk, f: impl FnMut(&[&[u8]; N])) {
    scan::for_records::<N>(&c.map[c.start..c.end], f)
}

fn small_csv<const N: usize>(path: &Path, mut f: impl FnMut(&[&[u8]; N])) {
    for c in &chunks(&[path.to_path_buf()]) {
        for_records::<N>(c, |fl| f(fl));
    }
}

// ---------------------------------------------------------------- records

const OK: u8 = 1; // passes every check but dedup
const PURCHASE: u8 = 2;
const DEAD: u8 = 4; // lost dedup to another copy

/// One event line, in the person partition of its (resolved) user.
#[derive(Clone, Copy)]
#[repr(C)]
struct E {
    person: Key,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u16,
    flags: u8,
}

/// Dedup record: event_id and where its E lives (thread << 48 | partition << 32 | index).
#[derive(Clone, Copy)]
struct D {
    eid: Key,
    loc: u64,
}

/// One valid click, in the person partition of its user.
#[derive(Clone, Copy)]
struct C {
    person: Key,
    ts: i64,
    cid: Key,
    camp: u16,
}

/// A live event in the sessions phase: local person id, 30-minute bin of the window.
#[derive(Clone, Copy)]
struct Ev {
    pid: u32,
    bin: u32,
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
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

/// Pre-faults a chunk of a mapped input file in one call (Linux 5.14+; ignored elsewhere).
#[inline]
fn populate(c: &Chunk) {
    #[cfg(target_os = "linux")]
    unsafe {
        let base = c.map.as_ptr() as usize;
        let s = (base + c.start) & !4095;
        let e = base + c.end;
        if e > s {
            libc::madvise(s as *mut libc::c_void, e - s, 22 /* MADV_POPULATE_READ */);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = c;
}

const RING: usize = 16;

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

    // ---- products -> category index (last row wins); campaigns -> index (first row wins)
    let mut cat_names: Vec<Vec<u8>> = Vec::new();
    let mut cat_idx: FxHashMap<Vec<u8>, u16> = FxHashMap::default();
    let mut product_cat: FxHashMap<Vec<u8>, u16> = FxHashMap::default();
    small_csv::<2>(&data.join("products.csv"), |f| {
        let c = *cat_idx.entry(f[1].to_vec()).or_insert_with(|| {
            cat_names.push(f[1].to_vec());
            (cat_names.len() - 1) as u16
        });
        if !f[0].is_empty() {
            product_cat.insert(f[0].to_vec(), c);
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
    let nprod = product_cat.len();
    let products = Dense::new(product_cat, unknown);

    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channel: Vec<Vec<u8>> = Vec::new();
    let mut camp_map: FxHashMap<Vec<u8>, u16> = FxHashMap::default();
    small_csv::<2>(&data.join("campaigns.csv"), |f| {
        if !f[0].is_empty() {
            camp_map.entry(f[0].to_vec()).or_insert_with(|| {
                camp_ids.push(f[0].to_vec());
                camp_channel.push(f[1].to_vec());
                (camp_ids.len() - 1) as u16
            });
        }
    });
    let organic = camp_ids.len() as u16;
    let ncamp = camp_ids.len() + 1;
    let campaigns = Dense::new(camp_map, u16::MAX);
    log(t0, &format!("products {} categories {} campaigns {} (dense {} {})", nprod, ncat, ncamp - 1,
        products.map.is_none(), campaigns.map.is_none()));

    // ---- links: parallel parse, sorted ID list, union-find on indexes, person name = byte-wise
    // smallest ID of each component; the table keeps only IDs that are not their own name.
    let link_chunks = chunks(&csv_files(&data.join("links")));
    let pairs: Vec<Vec<(Key, Key)>> = link_chunks
        .par_iter()
        .map(|c| {
            populate(c);
            let mut v = Vec::with_capacity((c.end - c.start) / 18 + 8);
            for_records::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((key_of(f[0]), key_of(f[1])));
                }
            });
            v
        })
        .collect();
    let mut ids: Vec<Key> = pairs.par_iter().flat_map_iter(|v| v.iter().flat_map(|&(a, b)| [a, b])).collect();
    ids.par_sort_unstable();
    ids.dedup();
    let ix: Vec<Vec<(u32, u32)>> = pairs
        .par_iter()
        .map(|v| {
            v.iter()
                .map(|&(a, b)| (ids.binary_search(&a).unwrap() as u32, ids.binary_search(&b).unwrap() as u32))
                .collect()
        })
        .collect();
    drop(pairs);
    // Lock-free union-find: a root is only ever linked (CAS) under a smaller root, so every
    // parent is <= its child and each component's root is its smallest index.
    let parent: Vec<AtomicU32> = (0..ids.len() as u32).into_par_iter().map(AtomicU32::new).collect();
    fn find(p: &[AtomicU32], mut x: u32) -> u32 {
        loop {
            let px = p[x as usize].load(AO::Relaxed);
            if px == x {
                return x;
            }
            let gp = p[px as usize].load(AO::Relaxed);
            if gp != px {
                let _ = p[x as usize].compare_exchange_weak(px, gp, AO::Relaxed, AO::Relaxed);
            }
            x = gp;
        }
    }
    ix.par_iter().for_each(|v| {
        for &(a, b) in v {
            let (mut a, mut b) = (a, b);
            loop {
                let (ra, rb) = (find(&parent, a), find(&parent, b));
                if ra == rb {
                    break;
                }
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                if parent[hi as usize].compare_exchange(hi, lo, AO::AcqRel, AO::Relaxed).is_ok() {
                    break;
                }
                a = ra;
                b = rb;
            }
        }
    });
    drop(ix);
    // ids are sorted by key, and packed keys sort byte-wise: the root's ID is the name.
    // Long (interned) keys do not sort byte-wise, so then the name is found by comparison.
    let entries: Vec<(Key, Key)> = if !ids.par_iter().any(|&k| keys::is_long(k)) {
        (0..ids.len())
            .into_par_iter()
            .filter_map(|i| {
                let r = find(&parent, i as u32) as usize;
                if r != i { Some((ids[i], ids[r])) } else { None }
            })
            .collect()
    } else {
        let mut name: Vec<Key> = vec![NONE; ids.len()];
        for i in 0..ids.len() {
            let r = find(&parent, i as u32) as usize;
            if name[r] == NONE || key_cmp(ids[i], name[r]) == Ordering::Less {
                name[r] = ids[i];
            }
        }
        (0..ids.len())
            .filter_map(|i| {
                let r = find(&parent, i as u32) as usize;
                if ids[i] != name[r] { Some((ids[i], name[r])) } else { None }
            })
            .collect()
    };
    let links = LinkTable::new(&entries);
    log(t0, &format!("links: {} ids, {} non-name", ids.len(), entries.len()));
    drop(entries);
    drop(parent);
    drop(ids);

    // ---- events + clicks: one pass over every chunk, on `ncpu` threads
    let ev_chunks = chunks(&csv_files(&data.join("events")));
    let ck_chunks = chunks(&csv_files(&data.join("clicks")));
    let ev_bytes: usize = ev_chunks.iter().map(|c| c.end - c.start).sum();
    let ck_bytes: usize = ck_chunks.iter().map(|c| c.end - c.start).sum();
    let est_ev = ev_bytes / 70 + 1;
    let est_ck = ck_bytes / 45 + 1;
    let lg = |x: usize| usize::BITS - 1 - x.max(1).leading_zeros();
    let qbits = lg(est_ev / 12_000).clamp(4, 12); // dedup partitions: ~12-24k events each
    let pbits = lg(est_ev / 50_000).clamp(3, 10); // person partitions: ~50-100k events each
    let (nq, np) = (1usize << qbits, 1usize << pbits);
    let cmin = ws - WEEK;
    let next = AtomicUsize::new(0);
    let ntask = ev_chunks.len() + ck_chunks.len();

    type Bufs = (Vec<Vec<E>>, Vec<Vec<D>>, Vec<Vec<C>>);
    let bufs: Vec<Bufs> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..ncpu)
            .map(|t| {
                let (next, ev_chunks, ck_chunks, products, campaigns, links) =
                    (&next, &ev_chunks, &ck_chunks, &products, &campaigns, &links);
                s.spawn(move || {
                    let pe = est_ev / ncpu / np;
                    let pd = est_ev / ncpu / nq;
                    let pc = est_ck / ncpu / np;
                    let mut ep: Vec<Vec<E>> = (0..np).map(|_| Vec::with_capacity(pe + pe / 4 + 16)).collect();
                    let mut dp: Vec<Vec<D>> = (0..nq).map(|_| Vec::with_capacity(pd + pd / 4 + 16)).collect();
                    let mut cp: Vec<Vec<C>> = (0..np).map(|_| Vec::with_capacity(pc + pc / 4 + 16)).collect();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= ntask {
                            break;
                        }
                        if i < ev_chunks.len() {
                            let c = &ev_chunks[i];
                            populate(c);
                            let mut ring: [(Key, Key, E); RING] =
                                [(NONE, NONE, E { person: 0, ts: 0, ingest: 0, price: 0, cat: 0, flags: 0 }); RING];
                            let mut n = 0usize;
                            let emit = |(user, eid, mut e): (Key, Key, E), ep: &mut Vec<Vec<E>>, dp: &mut Vec<Vec<D>>| {
                                let person = links.resolve(user);
                                e.person = person;
                                let pp = part(person, pbits);
                                let v = &mut ep[pp];
                                let idx = v.len();
                                v.push(e);
                                dp[part(eid, qbits)].push(D { eid, loc: ((t as u64) << 48) | ((pp as u64) << 32) | idx as u64 });
                            };
                            for_records::<7>(c, |f| {
                                if f[0].is_empty() {
                                    return;
                                }
                                let eid = key_of(f[0]);
                                let user = key_of(f[1]);
                                let ts = parse_ts(f[2]);
                                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                                let etype = match f[4] {
                                    b"view" => 1,
                                    b"cart" => 2,
                                    b"purchase" => 3,
                                    _ => 0,
                                };
                                let mut price = 0u32;
                                let mut ok = user != NONE && etype != 0 && !f[5].is_empty();
                                if etype == 3 {
                                    match parse_price(f[6]) {
                                        Some(p) if p >= 1 => price = p as u32,
                                        _ => ok = false,
                                    }
                                }
                                let tsv = match ts {
                                    Some(v) if v >= ws && v < we => v,
                                    Some(v) => {
                                        ok = false;
                                        v
                                    }
                                    None => {
                                        ok = false;
                                        i64::MIN
                                    }
                                };
                                if ingest.saturating_sub(tsv) > DAY {
                                    ok = false;
                                }
                                let cat = if ok { products.get(f[5]) } else { 0 };
                                let flags = (ok as u8) | if etype == 3 { PURCHASE } else { 0 };
                                links.prefetch(user);
                                let e = E { person: 0, ts: tsv, ingest, price, cat, flags };
                                let slot = &mut ring[n % RING];
                                if n >= RING {
                                    emit(*slot, &mut ep, &mut dp);
                                }
                                *slot = (user, eid, e);
                                n += 1;
                            });
                            for j in n.saturating_sub(RING)..n {
                                emit(ring[j % RING], &mut ep, &mut dp);
                            }
                        } else {
                            let c = &ck_chunks[i - ev_chunks.len()];
                            populate(c);
                            let mut ring: [C; RING] = [C { person: 0, ts: 0, cid: 0, camp: 0 }; RING];
                            let mut n = 0usize;
                            let emit = |mut r: C, cp: &mut Vec<Vec<C>>| {
                                r.person = links.resolve(r.person);
                                cp[part(r.person, pbits)].push(r);
                            };
                            for_records::<4>(c, |f| {
                                if f[0].is_empty() || f[1].is_empty() {
                                    return;
                                }
                                let camp = campaigns.get(f[2]);
                                if camp == u16::MAX {
                                    return;
                                }
                                let Some(ts) = parse_ts(f[3]) else { return };
                                if ts < cmin || ts >= we {
                                    return;
                                }
                                let user = key_of(f[1]);
                                links.prefetch(user);
                                let slot = &mut ring[n % RING];
                                if n >= RING {
                                    emit(*slot, &mut cp);
                                }
                                *slot = C { person: user, ts, cid: key_of(f[0]), camp };
                                n += 1;
                            });
                            for j in n.saturating_sub(RING)..n {
                                emit(ring[j % RING], &mut cp);
                            }
                        }
                    }
                    (ep, dp, cp)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut ep_all: Vec<Vec<Vec<E>>> = Vec::with_capacity(ncpu);
    let mut dp_all: Vec<Vec<Vec<D>>> = Vec::with_capacity(ncpu);
    let mut cp_all: Vec<Vec<Vec<C>>> = Vec::with_capacity(ncpu);
    for (e, d, c) in bufs {
        ep_all.push(e);
        dp_all.push(d);
        cp_all.push(c);
    }
    log(t0, &format!("events and clicks parsed ({} dedup / {} person partitions)", nq, np));

    // ---- dedup: per event_id partition, smallest ingest_ts wins; losers marked DEAD in place
    {
        struct Bases(Vec<Vec<usize>>);
        let bases = Bases(ep_all.iter_mut().map(|t| t.iter_mut().map(|v| v.as_mut_ptr() as usize).collect()).collect());
        let bases = &bases;
        let rec = move |loc: u64| -> *mut E {
            let (t, p, i) = ((loc >> 48) as usize, ((loc >> 32) & 0xFFFF) as usize, (loc & 0xFFFF_FFFF) as usize);
            unsafe { (bases.0[t][p] as *mut E).add(i) }
        };
        let dp_ref = &dp_all;
        (0..nq).into_par_iter().for_each_init(Vec::<D>::new, |tab, q| {
            let n: usize = dp_ref.iter().map(|t| t[q].len()).sum();
            let cap = (n + n / 2).next_power_of_two().max(16);
            let mask = cap - 1;
            tab.clear();
            tab.resize(cap, D { eid: NONE, loc: 0 });
            for t in dp_ref {
                for d in &t[q] {
                    let mut i = (mix(d.eid) >> 8) as usize & mask;
                    loop {
                        let s = unsafe { tab.get_unchecked_mut(i) };
                        if s.eid == NONE {
                            *s = *d;
                            break;
                        }
                        if s.eid == d.eid {
                            let (a, b) = (rec(s.loc), rec(d.loc));
                            unsafe {
                                if (*b).ingest < (*a).ingest {
                                    (*a).flags |= DEAD;
                                    s.loc = d.loc;
                                } else {
                                    (*b).flags |= DEAD;
                                }
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
        });
    }
    drop(dp_all);
    log(t0, "events deduped");

    // ---- per person partition: sessions, credit, assists, aggregates.
    // campaign stats per (campaign, day): [sessions, purchasing, revenue, assisted]
    type Acc = (Vec<(usize, Vec<UserRow>)>, Vec<[i64; 4]>);
    let new_acc = || -> Acc { (Vec::new(), vec![[0i64; 4]; ncamp * nd]) };
    let (ep_ref, cp_ref) = (&ep_all, &cp_all);
    let nbins = ((we - ws + HALF_HOUR - 1) / HALF_HOUR).max(1) as usize;
    let (mut results, camp): Acc = (0..np)
        .into_par_iter()
        .fold(new_acc, |(mut rv, mut camp), pb| {
            let n: usize = ep_ref.iter().map(|t| t[pb].len()).sum();
            // Live events get a local person id; count them per person and per 30-minute bin.
            let mut pid_of: FxHashMap<Key, u32> = FxHashMap::default();
            pid_of.reserve(n / 8 + 16);
            let mut persons: Vec<Key> = Vec::new();
            let mut eoff: Vec<u32> = Vec::new();
            let mut bpos: Vec<u32> = vec![0; nbins];
            let mut evs: Vec<Ev> = Vec::with_capacity(n);
            for t in ep_ref {
                for e in &t[pb] {
                    if e.flags & (OK | DEAD) == OK {
                        let p = match pid_of.entry(e.person) {
                            std::collections::hash_map::Entry::Occupied(o) => *o.get(),
                            std::collections::hash_map::Entry::Vacant(v) => {
                                persons.push(e.person);
                                eoff.push(0);
                                *v.insert((persons.len() - 1) as u32)
                            }
                        };
                        eoff[p as usize] += 1;
                        let bin = ((e.ts - ws) / HALF_HOUR) as u32;
                        bpos[bin as usize] += 1;
                        evs.push(Ev { pid: p, bin, ts: e.ts, price: e.price, cat: e.cat, purchase: e.flags & PURCHASE != 0 });
                    }
                }
            }
            let np_ = persons.len();
            // LSD counting sort: by bin, then stably by person. Each person's events come out
            // in bin order; inside a bin the order does not matter (a bin is 30 minutes, so
            // its events always share a session).
            let mut acc = 0u32;
            for c in bpos.iter_mut() {
                let x = *c;
                *c = acc;
                acc += x;
            }
            let mut tmp: Vec<Ev> = Vec::with_capacity(evs.len());
            unsafe { tmp.set_len(evs.len()) };
            for e in &evs {
                let q = &mut bpos[e.bin as usize];
                unsafe { *tmp.get_unchecked_mut(*q as usize) = *e };
                *q += 1;
            }
            let mut acc = 0u32;
            for c in eoff.iter_mut() {
                let x = *c;
                *c = acc;
                acc += x;
            }
            eoff.push(acc);
            {
                let mut pos: Vec<u32> = eoff[..np_].to_vec();
                for e in &tmp {
                    let q = &mut pos[e.pid as usize];
                    unsafe { *evs.get_unchecked_mut(*q as usize) = *e };
                    *q += 1;
                }
            }
            drop(tmp);
            let mut cks: Vec<Click> = Vec::with_capacity(cp_ref.iter().map(|t| t[pb].len()).sum());
            for t in cp_ref {
                for c in &t[pb] {
                    cks.push(Click { person: c.person, ts: c.ts, cid: c.cid, camp: c.camp });
                }
            }
            // clicks: only persons with events matter (others can't credit or assist anything)
            let mut coff: Vec<u32> = vec![0; np_ + 1];
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
                let mut pos: Vec<u32> = coff[..np_].to_vec();
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
            for p in 0..np_ {
                cgrouped[coff[p] as usize..coff[p + 1] as usize]
                    .sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(a.cid, b.cid)));
            }
            let cks = cgrouped;
            let mut rows: Vec<UserRow> = Vec::new();
            let mut catrev = vec![0i64; ncat];
            let mut touched: Vec<u16> = Vec::new();
            let mut starts: Vec<i64> = Vec::new();
            let mut ccamp: Vec<(u16, i64)> = Vec::new();
            for p in 0..np_ {
                let person = persons[p];
                let (i, iend) = (eoff[p] as usize, eoff[p + 1] as usize);
                let pc = &cks[coff[p] as usize..coff[p + 1] as usize];
                starts.clear();
                let mut row = UserRow { person, day: i32::MIN, sessions: 0, events: 0, revenue: 0, top: None };
                let mut cp = 0usize; // clicks with ts <= current start are pc[..cp]
                let mut grp = 0usize; // first click of the latest-ts group in pc[..cp]
                let (mut open, mut start, mut sday, mut prev_max) = (false, 0i64, 0i64, 0i64);
                let (mut n, mut rev, mut purch) = (0i64, 0i64, false);
                macro_rules! close {
                    () => {{
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
                    }};
                }
                let mut k = i;
                while k < iend {
                    // one bin: its min and max ts decide whether it continues the session
                    let b = evs[k].bin;
                    let mut e = k;
                    let (mut mn, mut mx) = (i64::MAX, i64::MIN);
                    while e < iend && evs[e].bin == b {
                        let t = evs[e].ts;
                        mn = mn.min(t);
                        mx = mx.max(t);
                        e += 1;
                    }
                    if !open || mn - prev_max > HALF_HOUR {
                        if open {
                            close!();
                        }
                        open = true;
                        start = mn;
                        sday = start.div_euclid(DAY);
                        if sday as i32 != row.day {
                            flush_row(&mut row, &mut rows, &mut catrev, &mut touched, &cat_rank);
                            row = UserRow { person, day: sday as i32, sessions: 0, events: 0, revenue: 0, top: None };
                        }
                        n = 0;
                        rev = 0;
                        purch = false;
                    }
                    for ev in &evs[k..e] {
                        n += 1;
                        if ev.purchase {
                            purch = true;
                            rev += ev.price as i64;
                            if catrev[ev.cat as usize] == 0 {
                                touched.push(ev.cat);
                            }
                            catrev[ev.cat as usize] += ev.price as i64;
                        }
                    }
                    prev_max = mx;
                    k = e;
                }
                if open {
                    close!();
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

    // ---- write outputs
    std::fs::create_dir_all(out.join("user_daily")).unwrap();
    std::fs::create_dir_all(out.join("campaign_daily")).unwrap();
    const PARTS: usize = 8;
    let per_part = np.div_ceil(PARTS);
    let n_user: usize = (0..PARTS)
        .into_par_iter()
        .map(|p| {
            let lo = (p * per_part).min(results.len());
            let hi = ((p + 1) * per_part).min(results.len());
            let rows: Vec<UserRow> = results[lo..hi].iter().flat_map(|(_, r)| r.iter().copied()).collect();
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
    // Fast exit: the files are written (write(2) returned); skip freeing everything.
    unsafe { libc::_exit(0) }
}

// ---------------------------------------------------------------- parquet

/// Appends the ID behind a key as one PLAIN BYTE_ARRAY value.
fn push_key(plain: &mut Vec<u8>, k: Key) {
    let at = plain.len();
    plain.extend_from_slice(&[0; 4]);
    keys::push_bytes(k, plain);
    let l = (plain.len() - at - 4) as u32;
    plain[at..at + 4].copy_from_slice(&l.to_le_bytes());
}

fn write_user(path: &Path, rows: &[UserRow], cats: &[Vec<u8>]) {
    let mut ids = Vec::with_capacity(rows.len() * 14);
    let mut top = Vec::with_capacity(rows.len() * 12);
    let mut present = Vec::with_capacity(rows.len());
    for r in rows {
        push_key(&mut ids, r.person);
        if let Some(t) = r.top {
            pq::push_str(&mut top, &cats[t as usize]);
        }
        present.push(r.top.is_some());
    }
    let cols = [
        pq::Col::Str { name: "user_id", optional: false, plain: ids, present: None },
        pq::Col::I32 { name: "day", date: true, v: rows.iter().map(|r| r.day).collect() },
        pq::Col::I32 { name: "sessions", date: false, v: rows.iter().map(|r| r.sessions as i32).collect() },
        pq::Col::I32 { name: "events", date: false, v: rows.iter().map(|r| r.events as i32).collect() },
        pq::Col::I64 { name: "revenue_cents", v: rows.iter().map(|r| r.revenue).collect() },
        pq::Col::Str { name: "top_category", optional: true, plain: top, present: Some(present) },
    ];
    pq::write(path, "user_daily", rows.len(), &cols);
}

fn write_campaign(path: &Path, cids: &[Vec<u8>], chans: &[Vec<u8>], days: &[i32], vals: &[Vec<i64>; 4]) {
    let strs = |src: &[Vec<u8>]| {
        let mut p = Vec::new();
        for b in src {
            pq::push_str(&mut p, b);
        }
        p
    };
    let cols = [
        pq::Col::Str { name: "campaign_id", optional: false, plain: strs(cids), present: None },
        pq::Col::Str { name: "channel", optional: false, plain: strs(chans), present: None },
        pq::Col::I32 { name: "day", date: true, v: days.to_vec() },
        pq::Col::I64 { name: "sessions", v: vals[0].clone() },
        pq::Col::I64 { name: "purchasing_sessions", v: vals[1].clone() },
        pq::Col::I64 { name: "revenue_cents", v: vals[2].clone() },
        pq::Col::I64 { name: "assisted_sessions", v: vals[3].clone() },
    ];
    pq::write(path, "campaign_daily", days.len(), &cols);
}
