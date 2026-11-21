//! Fast timestamp parsing (rules.md formats), checked against the straightforward version in
//! tests. F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 digits = epoch ms.

#[inline(always)]
fn load8(b: &[u8], at: usize) -> u64 {
    debug_assert!(at + 8 <= b.len());
    u64::from_le(unsafe { std::ptr::read_unaligned(b.as_ptr().add(at) as *const u64) })
}

/// 0x80 in each byte of t that is > 9 (t's bytes must be < 0x80 or are flagged too).
#[inline(always)]
fn not_digit(t: u64) -> u64 {
    (t.wrapping_add(0x7676_7676_7676_7676) | t) & 0x8080_8080_8080_8080
}

/// Value of 8 decimal digits (already minus '0'), first digit in the low byte.
#[inline(always)]
fn parse8(x: u64) -> u64 {
    let x = x.wrapping_mul(10).wrapping_add(x >> 8);
    ((x & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((x >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32
}

#[inline(always)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const Y0: i64 = 1970;
const Y1: i64 = 2100;

/// Day number (days since 1970-01-01) of every valid date in [Y0, Y1), i32::MIN for invalid
/// ones (Feb 30, Apr 31, ...); index ((y - Y0) * 12 + m - 1) * 32 + d.
fn day_table() -> &'static [i32] {
    static T: std::sync::OnceLock<Vec<i32>> = std::sync::OnceLock::new();
    T.get_or_init(|| {
        let mut t = vec![i32::MIN; ((Y1 - Y0) * 12 * 32) as usize];
        for y in Y0..Y1 {
            for m in 1..=12i64 {
                let leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0));
                let dim = DIM[m as usize] as i64 + ((m == 2) & leap) as i64;
                for d in 1..=dim {
                    t[(((y - Y0) * 12 + m - 1) * 32 + d) as usize] = days_from_civil(y, m, d) as i32;
                }
            }
        }
        t
    })
}

const DIM: [u8; 16] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0, 0, 0];

/// Value of 1..=8 ASCII digits; None if empty, longer than 8, or any non-digit.
/// Reads 8 bytes from b's start when that stays inside b's page (as it does inside a mapped
/// file), so no per-byte loop.
#[inline(always)]
pub fn digits8(b: &[u8]) -> Option<u64> {
    let k = b.len();
    if k == 0 || k > 8 {
        return None;
    }
    let p = b.as_ptr();
    let raw = if (p as usize & 4095) <= 4096 - 8 {
        u64::from_le(unsafe { std::ptr::read_unaligned(p as *const u64) })
    } else {
        let mut buf = [0u8; 8];
        buf[..k].copy_from_slice(b);
        u64::from_le_bytes(buf)
    };
    let sh = 8 * (8 - k) as u32;
    // keep the k bytes, moved to the top; '0' bytes below them
    let x = (raw << sh) ^ (0x3030_3030_3030_3030u64 << sh);
    if (not_digit(x) | (x & 0x8080_8080_8080_8080)) != 0 {
        return None;
    }
    Some(parse8(x))
}

/// F1 / F2 (b.len() is 20 or 25).
#[inline(always)]
fn f12(b: &[u8]) -> Option<i64> {
    // "YYYY-MM-" "DDTHH:MM" and bytes 12..20 "H:MM:SS?"
    let t0 = load8(b, 0) ^ 0x2D30_302D_3030_3030;
    let t1 = load8(b, 8) ^ 0x3030_3A30_3054_3030;
    let t2 = load8(b, 12) ^ 0x0030_303A_3030_3A30;
    // digits everywhere in t0, t1; t2 bytes 4..7 (pos 16..19): ':' 'S' 'S' then anything
    let bad = not_digit(t0) | not_digit(t1) | (not_digit(t2) & 0x0080_8080_0000_0000)
        | (t0 & 0xFF00_00FF_0000_0000) | (t1 & 0x0000_FF00_00FF_0000) | (t2 & 0x0000_00FF_0000_0000);
    if bad != 0 {
        return None;
    }
    let p0 = t0.wrapping_mul(10).wrapping_add(t0 >> 8);
    let p1 = t1.wrapping_mul(10).wrapping_add(t1 >> 8);
    let p2 = t2.wrapping_mul(10).wrapping_add(t2 >> 8);
    let y = (p0 & 0xFF) as i64 * 100 + ((p0 >> 16) & 0xFF) as i64;
    let mo = ((p0 >> 40) & 0xFF) as i64;
    let d = (p1 & 0xFF) as i64;
    let h = ((p1 >> 24) & 0xFF) as i64;
    let mi = ((p1 >> 48) & 0xFF) as i64;
    let s = ((p2 >> 40) & 0xFF) as i64;
    if (h > 23) | (mi > 59) | (s > 59) | (mo < 1) | (mo > 12) | (d > 31) {
        return None;
    }
    let day = if (y >= Y0) & (y < Y1) {
        let v = unsafe { *day_table().get_unchecked((((y - Y0) * 12 + mo - 1) * 32 + d) as usize) };
        if v == i32::MIN {
            return None;
        }
        v as i64
    } else {
        let leap = (y % 4 == 0) & ((y % 100 != 0) | (y % 400 == 0));
        let dim = DIM[(mo & 15) as usize] as i64 + ((mo == 2) & leap) as i64;
        if (d < 1) | (d > dim) {
            return None;
        }
        days_from_civil(y, mo, d)
    };
    let mut off = 0i64;
    let z = b[19];
    if b.len() == 20 {
        if z != b'Z' {
            return None;
        }
    } else {
        let sign = match z {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let (o0, o1, c, o2, o3) = (
            b[20].wrapping_sub(b'0') as i64,
            b[21].wrapping_sub(b'0') as i64,
            b[22],
            b[23].wrapping_sub(b'0') as i64,
            b[24].wrapping_sub(b'0') as i64,
        );
        if (o0 > 9) | (o1 > 9) | (o2 > 9) | (o3 > 9) | (c != b':') {
            return None;
        }
        let (oh, om) = (o0 * 10 + o1, o2 * 10 + o3);
        if (oh > 23) | (om > 59) {
            return None;
        }
        off = sign * (oh * 60 + om);
    }
    Some((day * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off * 60_000)
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 digits (epoch ms).
/// None = bad. F3 with more than 18 significant digits is far outside any window, so None.
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let l = b.len();
    if l == 20 || l == 25 {
        if b[4] == b'-' {
            return f12(b);
        }
    }
    if (9..=16).contains(&l) {
        let k = l - 8; // 1..=8 leading digits
        let lo = load8(b, l - 8) ^ 0x3030_3030_3030_3030;
        let hi = (load8(b, 0) << (8 * (8 - k))) ^ (0x3030_3030_3030_3030u64 << (8 * (8 - k)));
        if not_digit(lo) | not_digit(hi) | ((lo | hi) & 0x8080_8080_8080_8080) != 0 {
            return None;
        }
        return Some((parse8(hi) * 100_000_000 + parse8(lo)) as i64);
    }
    slow(b)
}

#[cold]
fn slow(b: &[u8]) -> Option<i64> {
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
    if b.len() == 20 || b.len() == 25 { f12(b) } else { None }
}

#[cfg(test)]
pub mod tests {
    fn d2(b: &[u8], i: usize) -> Option<i64> {
        let (a, c) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
        if a < 10 && c < 10 { Some((a * 10 + c) as i64) } else { None }
    }
    fn dim(y: i64, m: i64) -> i64 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 { 29 } else { 28 },
        }
    }
    /// The v2 parser, kept as the reference.
    pub fn reference(b: &[u8]) -> Option<i64> {
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
        if !(1..=12).contains(&mo) || d < 1 || d > dim(y, mo) || h > 23 || mi > 59 || s > 59 {
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
        Some((super::days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off * 60_000)
    }

    #[test]
    fn digits8_matches_reference() {
        let mut s = 0x2545_f491_4f6c_dd1du64;
        let alphabet = b"0123456789x/: ";
        for _ in 0..200_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let l = (s % 11) as usize;
            let v: Vec<u8> = (0..l).map(|j| alphabet[((s >> (8 + 5 * j)) as usize) % if (s >> 60) < 12 { 10 } else { alphabet.len() }]).collect();
            let r = if !v.is_empty() && v.len() <= 8 && v.iter().all(u8::is_ascii_digit) {
                Some(v.iter().fold(0u64, |a, &c| a * 10 + (c - b'0') as u64))
            } else {
                None
            };
            let mut buf = v.clone();
            buf.extend_from_slice(b"99999999");
            assert_eq!(super::digits8(&buf[..v.len()]), r, "{:?}", String::from_utf8_lossy(&v));
        }
    }

    #[test]
    fn matches_reference() {
        let mut cases: Vec<Vec<u8>> = Vec::new();
        for s in [
            "2026-04-18T08:29:34Z", "2026-04-21T14:10:21+05:30", "2026-04-21T14:10:21-00:00", "1776780605000",
            "2026-02-29T00:00:00Z", "2024-02-29T23:59:59Z", "2000-02-29T00:00:00Z", "1900-02-29T00:00:00Z",
            "2026-13-01T00:00:00Z", "2026-04-31T00:00:00Z", "2026-04-30T24:00:00Z", "2026-04-30T23:60:00Z",
            "2026-04-30T23:59:60Z", "2026-04-18t08:29:34Z", "2026-04-18T08:29:34z", "2026-04-18 08:29:34Z",
            "2026-04-18T08:29:34", "2026-04-18T08:29:34+24:00", "2026-04-18T08:29:34+05:60", "2026-04-18T08:29:34+0200",
            "1791071890000.0", "1.79e12", "-5", "86400000", "0", "00000000000000000000001", "1234567890123456789",
            "999999999999999999", "0000000000000001776780605000", "", "2026-04-18T08:29:34.000Z", "2026-00-10T00:00:00Z",
            "0000-01-01T00:00:00Z", "9999-12-31T23:59:59Z", "2026-04-18T08:29:34+23:59", "2026-04-18T08:29:34-23:59",
            "12345678", "123456789", "1234567890123456", "12345678901234567", "1/3456789", "12345:789",
        ] {
            cases.push(s.as_bytes().to_vec());
        }
        let mut s = 0x9e37_79b9_7f4a_7c15u64;
        let alphabet = b"0123456789-:TZ+ 0129";
        for _ in 0..300_000 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let base = cases[(s % 6) as usize].clone();
            let mut v = base.clone();
            // mutate one or two bytes
            for j in 0..((s >> 8) % 3) {
                if v.is_empty() {
                    break;
                }
                let at = ((s >> (16 + 8 * j)) as usize) % v.len();
                v[at] = alphabet[((s >> (40 + 4 * j)) as usize) % alphabet.len()];
            }
            if (s >> 60) == 0 {
                v.truncate(((s >> 20) % 30) as usize);
            }
            cases.push(v);
        }
        for c in &cases {
            // pad so the over-reads stay inside the allocation, as they do inside a mapped file
            let mut buf = c.clone();
            buf.extend_from_slice(&[b'#'; 16]);
            assert_eq!(super::parse_ts(&buf[..c.len()]), reference(c), "{:?}", String::from_utf8_lossy(c));
        }
    }
}
