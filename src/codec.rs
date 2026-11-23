//! The DPD1 delta body (SPEC.md §3): a request body sent as an edit of a body the server already has.
//!
//! ```text
//! "DPD1" | version: u8 = 1 | base: sha256 | target: sha256 | prefix | suffix | target_len | zstd frame
//! ```
//!
//! `prefix`, `suffix` and `target_len` are canonical ULEB128. The target is the base's first `prefix`
//! bytes, the frame's content (the literal), then the base's last `suffix` bytes. The frame is compressed
//! with the base's middle (what the literal replaced) as its raw prefix, so content that moved inside the
//! edited span still compresses.

use sha2::{Digest, Sha256};
use zstd_safe::{CCtx, CParameter, DCtx, DParameter};

/// SHA-256 of a body: names a base, and checks the rebuilt target.
pub type Hash = [u8; 32];

const MAGIC: &[u8; 4] = b"DPD1";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 4 + 1 + 32 + 32;
const LEVEL: i32 = 3;
/// 128 MiB: the window that covers a 64 MiB base and a 64 MiB literal; the decoder refuses larger ones.
const MAX_WINDOW_LOG: u32 = 27;

pub fn hash(body: &[u8]) -> Hash {
    Sha256::digest(body).into()
}

/// Rejected delta bodies. [`Error::code`] is the `dpd1-error` token the server answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Malformed(&'static str),
    UnsupportedVersion(u8),
    UnknownBase,
    TooLarge,
    Corrupt,
}

impl Error {
    pub fn code(self) -> &'static str {
        match self {
            Error::Malformed(_) => "malformed",
            Error::UnsupportedVersion(_) => "unsupported-version",
            Error::UnknownBase => "unknown-base",
            Error::TooLarge => "too-large",
            Error::Corrupt => "corrupt",
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Malformed(why) => write!(f, "malformed DPD1 body: {why}"),
            Error::UnsupportedVersion(v) => write!(f, "unsupported DPD1 version {v}"),
            Error::UnknownBase => f.write_str("the base is not known here"),
            Error::TooLarge => f.write_str("the body exceeds the size limit"),
            Error::Corrupt => f.write_str("the delta does not rebuild the body it names"),
        }
    }
}

impl std::error::Error for Error {}

/// The fixed part of a delta body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub base: Hash,
    pub target: Hash,
    pub prefix: usize,
    pub suffix: usize,
    pub target_len: usize,
}

/// Encodes `target` as an edit of `base`.
pub fn encode(base: &[u8], base_hash: &Hash, target: &[u8], target_hash: &Hash) -> Vec<u8> {
    let prefix = common_prefix(base, target);
    let suffix = common_suffix(&base[prefix..], &target[prefix..]);
    let reference = &base[prefix..base.len() - suffix];
    let literal = &target[prefix..target.len() - suffix];

    let mut out = Vec::with_capacity(HEADER_LEN + 30 + zstd_safe::compress_bound(literal.len()));
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(base_hash);
    out.extend_from_slice(target_hash);
    for v in [prefix, suffix, target.len()] {
        write_varint(&mut out, v as u64);
    }
    compress(reference, literal, &mut out);
    out
}

fn compress(reference: &[u8], literal: &[u8], out: &mut Vec<u8>) {
    let mut frame = Vec::with_capacity(zstd_safe::compress_bound(literal.len()));
    let mut cctx = CCtx::create();
    let span = (reference.len() + literal.len()).max(1);
    let window_log = (usize::BITS - (span - 1).leading_zeros()).clamp(10, MAX_WINDOW_LOG);
    let setup = [
        CParameter::CompressionLevel(LEVEL),
        CParameter::WindowLog(window_log),
        CParameter::EnableLongDistanceMatching(true),
        // the target's SHA-256 checks the whole rebuilt body
        CParameter::ChecksumFlag(false),
    ];
    for p in setup {
        cctx.set_parameter(p).expect("valid zstd parameter");
    }
    cctx.set_pledged_src_size(Some(literal.len() as u64)).expect("fresh zstd context");
    if !reference.is_empty() {
        cctx.ref_prefix(reference).expect("fresh zstd context");
    }
    cctx.compress2(&mut frame, literal).expect("zstd output sized by compress_bound");
    out.extend_from_slice(&frame);
}

/// Splits a delta body into its header and zstd frame. `max_len` caps `target_len`.
pub fn parse(delta: &[u8], max_len: usize) -> Result<(Header, &[u8]), Error> {
    if delta.len() < 5 {
        return Err(Error::Malformed("truncated header"));
    }
    if &delta[..4] != MAGIC {
        return Err(Error::Malformed("not a DPD1 body"));
    }
    if delta[4] != VERSION {
        return Err(Error::UnsupportedVersion(delta[4]));
    }
    if delta.len() < HEADER_LEN {
        return Err(Error::Malformed("truncated header"));
    }
    let mut pos = HEADER_LEN;
    let mut next = || -> Result<u64, Error> {
        let (v, n) = read_varint(&delta[pos..])?;
        pos += n;
        Ok(v)
    };
    let (prefix, suffix, target_len) = (next()?, next()?, next()?);
    if target_len > max_len as u64 {
        return Err(Error::TooLarge);
    }
    // target_len fits a usize now, and prefix + suffix <= target_len keeps both in range
    if prefix.checked_add(suffix).is_none_or(|edges| edges > target_len) {
        return Err(Error::Malformed("prefix and suffix exceed the target"));
    }
    let header = Header {
        base: delta[5..37].try_into().expect("32 bytes"),
        target: delta[37..69].try_into().expect("32 bytes"),
        prefix: prefix as usize,
        suffix: suffix as usize,
        target_len: target_len as usize,
    };
    Ok((header, &delta[pos..]))
}

/// Rebuilds the target from `base`, which must be the body `header.base` names.
pub fn apply(base: &[u8], header: &Header, frame: &[u8]) -> Result<Vec<u8>, Error> {
    let Header { prefix, suffix, .. } = *header;
    let literal_len = literal_len(header, base.len())?;
    let reference = &base[prefix..base.len() - suffix];
    let literal = decompress(reference, frame, literal_len)?;
    let target = splice(base, prefix, suffix, &literal);
    if hash(&target) != header.target {
        return Err(Error::Corrupt);
    }
    Ok(target)
}

/// The literal's length, once the header's edges fit both the target and the base. `parse` checked
/// the target; a hand-made `Header` is checked here too, so no arithmetic below can wrap.
fn literal_len(h: &Header, base_len: usize) -> Result<usize, Error> {
    let edges = h.prefix.checked_add(h.suffix).ok_or(Error::Malformed("prefix and suffix exceed the target"))?;
    let literal = h.target_len.checked_sub(edges).ok_or(Error::Malformed("prefix and suffix exceed the target"))?;
    if edges > base_len {
        return Err(Error::Malformed("prefix and suffix exceed the base"));
    }
    Ok(literal)
}

/// `base[..prefix] ++ literal ++ base[len - suffix..]`; the caller checked `prefix + suffix <= base.len()`.
fn splice(base: &[u8], prefix: usize, suffix: usize, literal: &[u8]) -> Vec<u8> {
    let mut target = Vec::with_capacity(prefix + literal.len() + suffix);
    target.extend_from_slice(&base[..prefix]);
    target.extend_from_slice(literal);
    target.extend_from_slice(&base[base.len() - suffix..]);
    target
}

fn decompress(reference: &[u8], frame: &[u8], literal_len: usize) -> Result<Vec<u8>, Error> {
    let mut dctx = DCtx::create();
    dctx.set_parameter(DParameter::WindowLogMax(MAX_WINDOW_LOG)).map_err(|_| Error::Corrupt)?;
    if !reference.is_empty() {
        dctx.ref_prefix(reference).map_err(|_| Error::Corrupt)?;
    }
    // exactly the literal's room: a frame that expands further fails to decode
    let mut literal = Vec::with_capacity(literal_len);
    match dctx.decompress(&mut literal, frame) {
        Ok(n) if n == literal_len => Ok(literal),
        _ => Err(Error::Corrupt),
    }
}

/// Canonical unsigned LEB128, at most 10 bytes (a u64).
pub(crate) fn read_varint(buf: &[u8]) -> Result<(u64, usize), Error> {
    let mut v = 0u64;
    for (i, &b) in buf.iter().enumerate().take(10) {
        if i == 9 && b > 1 {
            return Err(Error::Malformed("varint overflow"));
        }
        v |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            if b == 0 && i > 0 {
                return Err(Error::Malformed("non-canonical varint"));
            }
            return Ok((v, i + 1));
        }
    }
    Err(Error::Malformed(if buf.len() >= 10 { "varint overflow" } else { "truncated varint" }))
}

pub(crate) fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Bytes a delta of `target` against `base` copies instead of sending: the common prefix and suffix.
pub(crate) fn shared(base: &[u8], target: &[u8]) -> usize {
    let p = common_prefix(base, target);
    p + common_suffix(&base[p..], &target[p..])
}

/// Length of the longest common prefix: 64-byte slice compares (memcmp), then bytes.
pub(crate) fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    common_prefix_by::<64>(a, b)
}

/// Length of the longest common suffix.
pub(crate) fn common_suffix(a: &[u8], b: &[u8]) -> usize {
    common_suffix_by::<64>(a, b)
}

fn common_prefix_by<const STRIDE: usize>(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i + STRIDE <= n && a[i..i + STRIDE] == b[i..i + STRIDE] {
        i += STRIDE;
    }
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

fn common_suffix_by<const STRIDE: usize>(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let (a, b) = (&a[a.len() - n..], &b[b.len() - n..]);
    let mut i = 0;
    while i + STRIDE <= n && a[n - i - STRIDE..n - i] == b[n - i - STRIDE..n - i] {
        i += STRIDE;
    }
    while i < n && a[n - i - 1] == b[n - i - 1] {
        i += 1;
    }
    i
}

/// Kani proofs over every input of a bounded size: the parser and the copy arithmetic never panic,
/// and what they accept is what the spec allows. zstd and SHA-256 are C and assembly that Kani does
/// not model; the property tests and the fuzz targets cover them.
#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    #[kani::unwind(12)]
    fn a_varint_is_read_canonically_or_refused() {
        let bytes: [u8; 11] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 11);
        if let Ok((v, n)) = read_varint(&bytes[..len]) {
            assert!(n >= 1 && n <= 10 && n <= len);
            let mut again = Vec::new();
            write_varint(&mut again, v);
            assert!(again[..] == bytes[..n], "the only encoding of v");
        }
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn every_u64_is_written_in_at_most_ten_bytes_and_read_back() {
        let v: u64 = kani::any();
        let mut out = Vec::new();
        write_varint(&mut out, v);
        assert!(out.len() <= 10);
        assert!(read_varint(&out) == Ok((v, out.len())));
    }

    /// Any 72 bytes (a full fixed header and room for its three varints), any size limit.
    #[kani::proof]
    #[kani::unwind(12)]
    fn parse_never_panics_and_accepts_only_a_header_that_fits() {
        let bytes: [u8; HEADER_LEN + 3] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= bytes.len());
        let max_len: usize = kani::any();
        if let Ok((h, frame)) = parse(&bytes[..len], max_len) {
            assert!(bytes[..4] == *MAGIC && bytes[4] == VERSION);
            assert!(h.target_len <= max_len);
            assert!(h.prefix + h.suffix <= h.target_len);
            assert!(frame.len() <= len - HEADER_LEN);
        }
    }

    #[kani::proof]
    #[kani::unwind(6)]
    fn splice_copies_the_edges_and_places_the_literal_between_them() {
        let base: [u8; 4] = kani::any();
        let literal: [u8; 3] = kani::any();
        let prefix: usize = kani::any_where(|&p: &usize| p <= 4);
        let suffix: usize = kani::any_where(|&s: &usize| s <= 4 - prefix);
        let lit: usize = kani::any_where(|&l: &usize| l <= 3);
        let t = splice(&base, prefix, suffix, &literal[..lit]);
        assert!(t.len() == prefix + lit + suffix);
        assert!(t[..prefix] == base[..prefix]);
        assert!(t[prefix..prefix + lit] == literal[..lit]);
        assert!(t[prefix + lit..] == base[base.len() - suffix..]);
    }

    /// Any header, hand-made or parsed: the slicing `apply` does after this check stays in bounds.
    #[kani::proof]
    fn literal_len_admits_only_edges_that_fit_the_base_and_the_target() {
        let h = Header {
            base: [0; 32],
            target: [0; 32],
            prefix: kani::any(),
            suffix: kani::any(),
            target_len: kani::any(),
        };
        let base_len: usize = kani::any();
        if let Ok(lit) = literal_len(&h, base_len) {
            assert!(h.prefix <= base_len && h.suffix <= base_len - h.prefix);
            assert!(h.prefix + h.suffix + lit == h.target_len);
        }
    }

    /// The scan with a 4-byte stride on 10-byte inputs runs both loops, as the 64-byte one does on
    /// longer inputs; 64 itself is too wide for CBMC (it runs out of memory).
    #[kani::proof]
    #[kani::unwind(12)]
    fn common_prefix_and_suffix_are_the_longest_and_never_overrun() {
        let a: [u8; 10] = kani::any();
        let b: [u8; 10] = kani::any();
        let la: usize = kani::any_where(|&l: &usize| l <= 10);
        let lb: usize = kani::any_where(|&l: &usize| l <= 10);
        let (a, b) = (&a[..la], &b[..lb]);
        let p = common_prefix_by::<4>(a, b);
        assert!(p <= la && p <= lb && a[..p] == b[..p]);
        assert!(p == la || p == lb || a[p] != b[p]);
        let s = common_suffix_by::<4>(a, b);
        assert!(s <= la && s <= lb && a[la - s..] == b[lb - s..]);
        assert!(s == la || s == lb || a[la - s - 1] != b[lb - s - 1]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn delta(base: &[u8], target: &[u8]) -> Vec<u8> {
        encode(base, &hash(base), target, &hash(target))
    }

    fn decode(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, Error> {
        let (header, frame) = parse(delta, 1 << 26)?;
        if header.base != hash(base) {
            return Err(Error::UnknownBase);
        }
        apply(base, &header, frame)
    }

    /// A conversation: each request re-sends the previous one plus a turn.
    fn requests(n: usize) -> Vec<Vec<u8>> {
        let mut turns = String::new();
        (0..n)
            .map(|i| {
                turns.push_str(&format!(r#"{{"role":"user","content":"turn {i} {}"}},"#, "words ".repeat(i + 3)));
                format!(r#"{{"model":"m","system":"{}","messages":[{turns}{{}}]}}"#, "be helpful ".repeat(200))
                    .into_bytes()
            })
            .collect()
    }

    #[test]
    fn a_resent_context_costs_only_its_new_turn() {
        let reqs = requests(30);
        for pair in reqs.windows(2) {
            let d = delta(&pair[0], &pair[1]);
            assert_eq!(decode(&pair[0], &d).unwrap(), pair[1]);
            let new = pair[1].len() - pair[0].len();
            assert!(d.len() < 100 + new, "{} bytes for a {new}-byte turn", d.len());
        }
    }

    #[test]
    fn an_edit_at_the_top_and_an_append_still_compress() {
        // a timestamp in the system prompt changes, and a turn is appended: the copied context sits
        // between the two edits, where only the zstd frame (base as its prefix) can find it
        let body: String = (0..20_000).map(|i| format!("message {i} with some content; ")).collect();
        let base = format!("date=2026-11-19 {body} end");
        let target = format!("date=2026-11-20 {body} new turn end");
        let d = delta(base.as_bytes(), target.as_bytes());
        assert_eq!(decode(base.as_bytes(), &d).unwrap(), target.as_bytes());
        assert!(d.len() < 200, "{} bytes", d.len());
    }

    #[test]
    fn a_target_copied_entirely_from_the_base_has_an_empty_literal() {
        // a message removed from the middle: prefix + suffix is the whole target
        let (base, target) = (&b"hello brave world"[..], &b"hello world"[..]);
        let d = delta(base, target);
        let (h, _) = parse(&d, 1 << 20).unwrap();
        assert_eq!((h.prefix, h.suffix, h.target_len), (6, 5, 11));
        assert_eq!(decode(base, &d).unwrap(), target);
    }

    #[test]
    fn the_wire_layout_is_the_spec() {
        let d = delta(b"hello world", b"hello brave world");
        assert_eq!(&d[..4], b"DPD1");
        assert_eq!(d[4], 1);
        assert_eq!(d[5..37], hash(b"hello world"));
        assert_eq!(d[37..69], hash(b"hello brave world"));
        // prefix "hello " (6), suffix "world" (5), target length 17
        assert_eq!(&d[69..72], &[6, 5, 17]);
        assert_eq!(&d[72..76], &[0x28, 0xb5, 0x2f, 0xfd], "a standard zstd frame follows");
    }

    #[test]
    fn errors_say_what_went_wrong() {
        assert_eq!(Error::Malformed("truncated varint").to_string(), "malformed DPD1 body: truncated varint");
        assert_eq!(Error::UnsupportedVersion(7).to_string(), "unsupported DPD1 version 7");
        assert_eq!(Error::UnknownBase.to_string(), "the base is not known here");
        assert_eq!(Error::TooLarge.to_string(), "the body exceeds the size limit");
        assert_eq!(Error::Corrupt.to_string(), "the delta does not rebuild the body it names");
    }

    #[test]
    fn a_long_new_turn_compresses_across_its_whole_length() {
        // 64 KiB of noise, then the same 64 KiB again: the window must reach back 64 KiB inside the
        // literal, even with no base span to size it by (an append has none)
        let noise: Vec<u8> = (0..2048u32).flat_map(|i| hash(&i.to_le_bytes())).collect();
        let base = b"{\"messages\":[".to_vec();
        let target = [&base[..], &noise, &noise].concat();
        let d = delta(&base, &target);
        assert!(d.len() < noise.len() + 1024, "{} bytes for {} of noise twice", d.len(), noise.len());
        assert_eq!(decode(&base, &d).unwrap(), target);
    }

    #[test]
    fn errors_have_stable_codes() {
        assert_eq!(Error::Malformed("x").code(), "malformed");
        assert_eq!(Error::UnsupportedVersion(2).code(), "unsupported-version");
        assert_eq!(Error::UnknownBase.code(), "unknown-base");
        assert_eq!(Error::TooLarge.code(), "too-large");
        assert_eq!(Error::Corrupt.code(), "corrupt");
    }

    #[test]
    fn hash_is_sha256() {
        assert_eq!(
            hash(b"abc")[..4],
            [0xba, 0x78, 0x16, 0xbf],
            "SHA-256(abc) = ba7816bf..., the FIPS 180-2 test vector"
        );
    }

    #[test]
    fn parse_refuses_what_it_cannot_trust() {
        let d = delta(b"hello world", b"hello brave world");
        for cut in [3, 5, 40, 68] {
            assert_eq!(parse(&d[..cut], 1 << 20).unwrap_err(), Error::Malformed("truncated header"), "{cut} bytes");
        }
        assert_eq!(parse(&d[..69], 1 << 20).unwrap_err(), Error::Malformed("truncated varint"));
        assert_eq!(
            parse(b"DPD1\x07", 1 << 20).unwrap_err(),
            Error::UnsupportedVersion(7),
            "the version, before the length"
        );
        let mut bad = d.clone();
        bad[0] = b'X';
        assert_eq!(parse(&bad, 1 << 20).unwrap_err(), Error::Malformed("not a DPD1 body"));
        let mut bad = d.clone();
        bad[4] = 2;
        assert_eq!(parse(&bad, 1 << 20).unwrap_err(), Error::UnsupportedVersion(2));
        assert_eq!(parse(&d, 16).unwrap_err(), Error::TooLarge, "a 17-byte target over a 16-byte cap");
        assert!(parse(&d, 17).is_ok(), "a 17-byte target at a 17-byte cap");
        let mut bad = d.clone();
        bad[69] = 13; // prefix 13 + suffix 5 > target 17
        assert_eq!(parse(&bad, 1 << 20).unwrap_err(), Error::Malformed("prefix and suffix exceed the target"));
        let mut bad = d.clone();
        bad[69] = 0x86; // a continuation byte where prefix ends: varint now 0x86 0x05 ...
        assert!(parse(&bad, 1 << 20).is_err());
    }

    #[test]
    fn varints_must_be_canonical_and_fit() {
        assert_eq!(read_varint(&[0x7f]), Ok((127, 1)));
        assert_eq!(read_varint(&[0x80, 0x01]), Ok((128, 2)));
        assert_eq!(read_varint(&[0x80, 0x00]), Err(Error::Malformed("non-canonical varint")));
        assert_eq!(read_varint(&[0x80]), Err(Error::Malformed("truncated varint")));
        assert_eq!(read_varint(&[0xff; 11]), Err(Error::Malformed("varint overflow")));
        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(read_varint(&max), Ok((u64::MAX, 10)));
        assert_eq!(
            read_varint(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]),
            Err(Error::Malformed("varint overflow"))
        );
        let mut out = Vec::new();
        write_varint(&mut out, u64::MAX);
        assert_eq!(out, max);
    }

    #[test]
    fn apply_refuses_a_base_shorter_than_the_header_says() {
        let d = delta(b"hello world", b"hello brave world");
        let (header, frame) = parse(&d, 1 << 20).unwrap();
        assert_eq!(apply(b"hello", &header, frame).unwrap_err(), Error::Malformed("prefix and suffix exceed the base"));
    }

    #[test]
    fn apply_refuses_a_hand_made_header_whose_edges_overflow() {
        let d = delta(b"hello world", b"hello brave world");
        let (header, frame) = parse(&d, 1 << 20).unwrap();
        let wraps = Header { prefix: usize::MAX, suffix: 2, ..header };
        assert_eq!(
            apply(b"hello world", &wraps, frame).unwrap_err(),
            Error::Malformed("prefix and suffix exceed the target")
        );
        let short = Header { target_len: 3, ..header };
        assert_eq!(
            apply(b"hello world", &short, frame).unwrap_err(),
            Error::Malformed("prefix and suffix exceed the target")
        );
    }

    #[test]
    fn apply_checks_the_rebuilt_target() {
        let d = delta(b"hello world", b"hello brave world");
        let (mut header, frame) = parse(&d, 1 << 20).unwrap();
        header.target[0] ^= 1;
        assert_eq!(apply(b"hello world", &header, frame).unwrap_err(), Error::Corrupt);
    }

    #[test]
    fn a_frame_that_expands_past_its_literal_is_refused() {
        let d = delta(b"hello world", b"hello brave world");
        let (mut header, frame) = parse(&d, 1 << 20).unwrap();
        header.target_len -= 1; // the frame now holds one byte more than the literal
        assert_eq!(apply(b"hello world", &header, frame).unwrap_err(), Error::Corrupt);
        header.target_len += 2; // and one byte less
        assert_eq!(apply(b"hello world", &header, frame).unwrap_err(), Error::Corrupt);
    }

    #[test]
    fn common_prefix_and_suffix_match_a_naive_scan() {
        let naive = |a: &[u8], b: &[u8]| a.iter().zip(b).take_while(|(x, y)| x == y).count();
        let a: Vec<u8> = (0..300u32).map(|i| (i % 7) as u8).collect();
        for cut in [0, 1, 63, 64, 65, 128, 200, 299, 300] {
            let mut b = a.clone();
            if cut < b.len() {
                b[cut] ^= 0xff;
            }
            assert_eq!(common_prefix(&a, &b), naive(&a, &b), "cut {cut}");
            let (ra, rb): (Vec<u8>, Vec<u8>) = (a.iter().rev().copied().collect(), b.iter().rev().copied().collect());
            assert_eq!(common_suffix(&ra, &rb), naive(&a, &b), "cut {cut}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn any_target_round_trips_against_any_base(
            base in proptest::collection::vec(any::<u8>(), 0..2000),
            target in proptest::collection::vec(any::<u8>(), 0..2000),
        ) {
            let d = delta(&base, &target);
            prop_assert_eq!(decode(&base, &d).unwrap(), target);
        }

        #[test]
        fn an_edited_base_round_trips(
            base in proptest::collection::vec(any::<u8>(), 0..4000),
            cut in any::<proptest::sample::Index>(),
            len in 0usize..64,
            insert in proptest::collection::vec(any::<u8>(), 0..200),
        ) {
            let at = cut.index(base.len() + 1);
            let end = (at + len).min(base.len());
            let target = [&base[..at], &insert[..], &base[end..]].concat();
            let d = delta(&base, &target);
            prop_assert_eq!(decode(&base, &d).unwrap(), target);
        }

        #[test]
        fn a_corrupted_delta_never_rebuilds_a_wrong_body(
            base in proptest::collection::vec(any::<u8>(), 1..1000),
            extra in proptest::collection::vec(any::<u8>(), 0..300),
            at in any::<proptest::sample::Index>(),
            flip in 1u8..,
        ) {
            let target = [&base[..], &extra[..]].concat();
            let mut d = delta(&base, &target);
            let i = at.index(d.len());
            d[i] ^= flip;
            if let Ok(out) = decode(&base, &d) {
                prop_assert_eq!(out, target, "byte {} flipped and a different body came out", i);
            }
        }
    }
}
