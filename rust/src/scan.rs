//! CSV tokenizer for the rules.md dialect, 64 bytes at a time.
//!
//! Each 64-byte block becomes three bitmasks (',' '\n' '"'). The record loop walks only the
//! separators (',' and '\n'); a field's quotes are checked by counting quote bits in its range
//! (prefix popcounts), so no byte is visited one by one. A field with quotes must be exactly
//! `"..."`: two quotes, the first at its start, the second at its end.

#[inline(always)]
fn block_masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_arch = "x86_64")]
    {
        #[cfg(target_feature = "avx2")]
        unsafe {
            use core::arch::x86_64::*;
            let a = _mm256_loadu_si256(p as *const __m256i);
            let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
            let m = |c: i8| -> u64 {
                let v = _mm256_set1_epi8(c);
                let x = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
                let y = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
                x | (y << 32)
            };
            return (m(b',' as i8), m(b'\n' as i8), m(b'"' as i8));
        }
        #[cfg(not(target_feature = "avx2"))]
        unsafe {
            use core::arch::x86_64::*;
            let v: [__m128i; 4] = [
                _mm_loadu_si128(p as *const __m128i),
                _mm_loadu_si128(p.add(16) as *const __m128i),
                _mm_loadu_si128(p.add(32) as *const __m128i),
                _mm_loadu_si128(p.add(48) as *const __m128i),
            ];
            let m = |c: i8| -> u64 {
                let s = _mm_set1_epi8(c);
                let mut r = 0u64;
                for (i, x) in v.iter().enumerate() {
                    r |= (_mm_movemask_epi8(_mm_cmpeq_epi8(*x, s)) as u32 as u64) << (16 * i);
                }
                r
            };
            return (m(b',' as i8), m(b'\n' as i8), m(b'"' as i8));
        }
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(BITS.as_ptr());
        let m = |c: u8| -> u64 {
            let s = vdupq_n_u8(c);
            let t0 = vandq_u8(vceqq_u8(v[0], s), bits);
            let t1 = vandq_u8(vceqq_u8(v[1], s), bits);
            let t2 = vandq_u8(vceqq_u8(v[2], s), bits);
            let t3 = vandq_u8(vceqq_u8(v[3], s), bits);
            let s0 = vpaddq_u8(t0, t1);
            let s1 = vpaddq_u8(t2, t3);
            let s0 = vpaddq_u8(s0, s1);
            let s0 = vpaddq_u8(s0, s0);
            vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0)
        };
        return (m(b','), m(b'\n'), m(b'"'));
    }
    #[allow(unreachable_code)]
    {
        let mut r = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let c = unsafe { *p.add(i) };
            r.0 |= ((c == b',') as u64) << i;
            r.1 |= ((c == b'\n') as u64) << i;
            r.2 |= ((c == b'"') as u64) << i;
        }
        r
    }
}

/// Calls f with the N unquoted fields of every well-formed line of `data` (which starts at a
/// line start). A final line without '\n' counts. Wrong field count or a bad quote drops
/// the line. N must be at most 7.
#[inline(always)]
pub fn for_records<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    assert!(N < 8);
    let len = data.len();
    let base = data.as_ptr();
    let mut fields: [&'a [u8]; 8] = [&[]; 8]; // slot 7 takes the fields past N
    let mut n = 0usize; // fields ended so far on this line
    let mut fs = 0usize; // start of the current field
    let mut qstart = 0u64; // quotes before fs
    let mut bad = false;
    let mut qbase = 0u64; // quotes before the current block

    // Exact handling of one separator at block offset i (quote counts by prefix popcount).
    macro_rules! exact {
        ($b:expr, $i:expr, $quote:expr, $nlm:expr) => {{
            let i: u32 = $i;
            let p = $b + i as usize; // p < len: a separator byte
            let qp = qbase + ($quote & ((1u64 << i) - 1)).count_ones() as u64;
            let qc = qp - qstart;
            let l = p - fs;
            let (first, last) = unsafe { (*base.add(fs), *base.add(p.max(1) - 1)) };
            let strip = (qc != 0) as usize;
            let okq = (qc == 0) | ((qc == 2) & (l >= 2) & (first == b'"') & (last == b'"'));
            bad |= !okq;
            let fl = l.wrapping_sub(2 * strip);
            let fl = if fl > l { 0 } else { fl };
            fields[n.min(7)] = unsafe { std::slice::from_raw_parts(base.add(fs + strip), fl) };
            n += 1;
            fs = p + 1;
            qstart = qp;
            if ($nlm >> i) & 1 != 0 {
                if !bad && n == N {
                    f(unsafe { &*(fields.as_ptr() as *const [&'a [u8]; N]) });
                }
                n = 0;
                bad = false;
            }
        }};
    }

    let mut b = 0usize;
    let mut tail = [0u8; 64];
    while b < len {
        if b + 64 < len {
            let (comma, nlm, quote) = block_masks(unsafe { base.add(b) });
            let seps = comma | nlm;
            let is_sep = |c: u8| ((c == b',') | (c == b'\n')) as u64;
            let prev = if b == 0 { 1 } else { is_sep(unsafe { *base.add(b - 1) }) };
            let next = is_sep(unsafe { *base.add(b + 64) });
            let starts = (seps << 1) | prev;
            let ends = (seps >> 1) | (next << 63);
            let mut sep = seps;
            if quote & !(starts | ends) == 0 {
                // Fast block: every quote is at a field start or end, so a field is valid iff
                // it is unquoted or is `"..."` (first and last byte quotes, length >= 2).
                if fs < b && sep != 0 {
                    // the first field started in an earlier block: exact check
                    let i = sep.trailing_zeros();
                    sep &= sep - 1;
                    exact!(b, i, quote, nlm);
                }
                while sep != 0 {
                    let i = sep.trailing_zeros();
                    sep &= sep - 1;
                    let p = b + i as usize;
                    let l = p - fs;
                    let (first, last) = unsafe { (*base.add(fs), *base.add(p.max(1) - 1)) };
                    let sq = first == b'"';
                    let eq = last == b'"';
                    bad |= (sq != eq) | (sq & (l < 2));
                    let strip = sq as usize;
                    let fl = l.wrapping_sub(2 * strip);
                    let fl = if fl > l { 0 } else { fl };
                    fields[n.min(7)] = unsafe { std::slice::from_raw_parts(base.add(fs + strip), fl) };
                    n += 1;
                    fs = p + 1;
                    if (nlm >> i) & 1 != 0 {
                        if !bad && n == N {
                            f(unsafe { &*(fields.as_ptr() as *const [&'a [u8]; N]) });
                        }
                        n = 0;
                        bad = false;
                    }
                }
                // keep qstart (quotes before fs) exact for the slow path
                if fs >= b {
                    let k = fs - b; // 0..=64
                    let low = if k >= 64 { quote } else { quote & ((1u64 << k) - 1) };
                    qstart = qbase + low.count_ones() as u64;
                }
            } else {
                while sep != 0 {
                    let i = sep.trailing_zeros();
                    sep &= sep - 1;
                    exact!(b, i, quote, nlm);
                }
            }
            qbase += quote.count_ones() as u64;
        } else {
            tail[..len - b].copy_from_slice(&data[b..]);
            let (comma, nlm, quote) = block_masks(tail.as_ptr());
            let mut sep = comma | nlm;
            while sep != 0 {
                let i = sep.trailing_zeros();
                sep &= sep - 1;
                exact!(b, i, quote, nlm);
            }
            qbase += quote.count_ones() as u64;
        }
        b += 64;
    }
    if fs < len || n > 0 {
        // final line without '\n': the field [fs, len), checked with bounds
        let v = &data[fs..];
        let qc = qbase - qstart;
        let mut v2: &'a [u8] = v;
        if qc != 0 {
            let l = v.len();
            if qc == 2 && l >= 2 && v[0] == b'"' && v[l - 1] == b'"' {
                v2 = &v[1..l - 1];
            } else {
                bad = true;
            }
        }
        fields[n.min(7)] = v2;
        n += 1;
        if !bad && n == N {
            f(unsafe { &*(fields.as_ptr() as *const [&'a [u8]; N]) });
        }
    }
}

#[cfg(test)]
mod tests {
    /// Reference: the rules.md dialect, byte by byte.
    fn reference<const N: usize>(data: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut lines: Vec<&[u8]> = data.split(|&c| c == b'\n').collect();
        if data.is_empty() || data.last() == Some(&b'\n') {
            lines.pop();
        }
        for line in lines {
            let parts: Vec<&[u8]> = line.split(|&c| c == b',').collect();
            if parts.len() != N {
                continue;
            }
            let mut rec = Vec::new();
            let mut ok = true;
            for p in parts {
                let q = p.iter().filter(|&&c| c == b'"').count();
                if q == 0 {
                    rec.push(p.to_vec());
                } else if q == 2 && p.len() >= 2 && p[0] == b'"' && p[p.len() - 1] == b'"' {
                    rec.push(p[1..p.len() - 1].to_vec());
                } else {
                    ok = false;
                }
            }
            if ok {
                out.push(rec);
            }
        }
        out
    }

    #[test]
    fn structured_lines_match_reference() {
        let kinds: [&[u8]; 9] = [b"abc", b"x", b"", b"\"q\"", b"\"\"", b"\"", b"\"ab", b"a\"b", b"\"longer quoted value\""];
        let mut s = 0xdead_beef_1234_5678u64;
        for _ in 0..20000 {
            let mut d = Vec::new();
            for _ in 0..(s % 9) {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                let nf = 2 + (s % 3);
                for j in 0..nf {
                    let r = (s >> (4 + 6 * j)) as usize;
                    // mostly valid kinds, sometimes malformed ones
                    let k = if r % 11 == 0 { kinds[5 + (r / 11) % 3] } else { kinds[[0, 1, 2, 3, 4, 8][(r / 11) % 6]] };
                    if j > 0 {
                        d.push(b',');
                    }
                    d.extend_from_slice(k);
                    for _ in 0..((r >> 8) % 20) {
                        if k.is_empty() || k[0] != b'"' {
                            d.push(b'z');
                        }
                    }
                }
                if (s >> 61) != 0 {
                    d.push(b'\n');
                }
            }
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            super::for_records::<3>(&d, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            assert_eq!(got, reference::<3>(&d), "{:?}", String::from_utf8_lossy(&d));
        }
    }

    #[test]
    fn matches_reference() {
        let alphabets: [&[u8]; 3] = [b",,,\n\n\"\"ab\r 1", b",,,,\nabcdefghij0123456789\"", b",,\nxyzxyzxyzxyzxyzxyzxyzxyzxyzxyz\""];
        let mut s = 0x1234_5678_9abc_def0u64;
        for round in 0..30000 {
            let alphabet = alphabets[round % 3];
            let len = (round * 7) % 400;
            let mut d = Vec::with_capacity(len);
            for _ in 0..len {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                d.push(alphabet[(s % alphabet.len() as u64) as usize]);
            }
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            super::for_records::<3>(&d, |f| got.push(f.iter().map(|x| x.to_vec()).collect()));
            assert_eq!(got, reference::<3>(&d), "{:?}", String::from_utf8_lossy(&d));
            let mut got2: Vec<Vec<Vec<u8>>> = Vec::new();
            super::for_records::<1>(&d, |f| got2.push(f.iter().map(|x| x.to_vec()).collect()));
            assert_eq!(got2, reference::<1>(&d));
        }
    }
}
