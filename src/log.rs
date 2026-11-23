//! LPK1 (SPEC.md §7): a log of bodies, each stored as an LPD1 delta against one of the records
//! before it. Gateway request logs and trace spans re-send the whole prompt every call, so a record
//! costs about its new bytes. Every record is checked by SHA-256 when read back.
//!
//! ```
//! use logpak::log::{Reader, Writer};
//! let mut w = Writer::new(Vec::new(), 8)?;
//! w.append(b"req 1", bytes::Bytes::from_static(br#"{"messages":["hi"]}"#))?;
//! w.append(b"req 2", bytes::Bytes::from_static(br#"{"messages":["hi","more"]}"#))?;
//! let mut r = Reader::open(std::io::Cursor::new(w.into_inner()), 64 << 20)?;
//! assert_eq!(r.get(1)?, br#"{"messages":["hi","more"]}"#);
//! assert_eq!(r.records()[1].meta, b"req 2");
//! # Ok::<(), std::io::Error>(())
//! ```

use std::collections::VecDeque;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};

use bytes::Bytes;

use crate::codec::{self, Hash};

const MAGIC: &[u8; 4] = b"LPK1";
const VERSION: u8 = 1;
/// Reading one record rebuilds at most this many bodies, and at most `MAX_CHAIN_BYTES` of them; the
/// writer then stores a record whole. Small spans chain for long, multi-MB requests restart sooner.
/// ponytail: fixed bounds; a knob if random reads (or ratio) need another trade-off
const MAX_DEPTH: usize = 4096;
const MAX_CHAIN_BYTES: usize = if cfg!(test) { 1_048_576 } else { 1_073_741_824 }; // 1 MiB, 1 GiB

/// (records back to the base, depth, bytes its chain rebuilds, the LPD1 delta)
type Encoded = (u64, usize, usize, Vec<u8>);

/// Appends records to an LPK1 file. Write it through a `BufWriter`; the file is readable up to
/// the last complete record at any time, so flush when a record must survive a crash.
#[derive(Debug)]
pub struct Writer<W: Write> {
    out: W,
    window: usize,
    recent: VecDeque<Recent>,
    next: u64,
}

#[derive(Debug)]
struct Recent {
    index: u64,
    hash: Hash,
    body: Bytes,
    /// bodies, and their bytes, rebuilt to read this record
    depth: usize,
    chain: usize,
}

impl<W: Write> Writer<W> {
    /// `window`: how many of the latest records a new one may delta against. One per conversation
    /// interleaved in the log; the reader holds that many bodies while it scans.
    pub fn new(mut out: W, window: usize) -> io::Result<Self> {
        out.write_all(MAGIC)?;
        out.write_all(&[VERSION])?;
        write_varint(&mut out, window as u64)?;
        Ok(Writer { out, window, recent: VecDeque::new(), next: 0 })
    }

    /// Appends `body` with its `meta` (stored as is: a timestamp, an id, a path) and returns its index.
    pub fn append(&mut self, meta: &[u8], body: Bytes) -> io::Result<u64> {
        let hash = codec::hash(&body);
        let encode = |base: Option<&Recent>| match base {
            Some(r) => (self.next - r.index, r.depth + 1, r.chain, codec::encode(&r.body, &r.hash, &body, &hash)),
            None => (0, 0, 0, codec::encode(b"", &codec::hash(b""), &body, &hash)),
        };
        let fits = |r: &&Recent| r.depth + 1 < MAX_DEPTH && r.chain + body.len() <= MAX_CHAIN_BYTES;
        let bases: Vec<&Recent> = self.recent.iter().filter(fits).collect();
        let best = bases.iter().map(|r| (codec::shared(&r.body, &body), *r)).max_by_key(|(shared, _)| *shared);
        // a re-sent context shares most of its bytes at the edges: that base, one encode. Otherwise
        // (ids before the prompt, as spans have) the edges say little: try the bases newest first,
        // and take the first that makes the record under 1/64 of its size (consecutive OTLP batches:
        // the first one tried), else the smallest, or the record whole if that is no larger.
        // ponytail: up to `window` + 1 encodes for a record no base makes small
        let (back, depth, chain, delta) = match best {
            Some((shared, r)) if shared * 2 >= body.len() => encode(Some(r)),
            _ => 'tried: {
                let mut smallest: Option<Encoded> = None;
                for r in bases.into_iter().rev() {
                    let e = encode(Some(r));
                    if e.3.len() * 64 <= body.len() {
                        break 'tried e;
                    }
                    if smallest.as_ref().is_none_or(|s| e.3.len() < s.3.len()) {
                        smallest = Some(e);
                    }
                }
                let whole = encode(None);
                match smallest {
                    Some(s) if s.3.len() < whole.3.len() => s,
                    _ => whole,
                }
            }
        };
        write_varint(&mut self.out, back)?;
        write_varint(&mut self.out, meta.len() as u64)?;
        self.out.write_all(meta)?;
        write_varint(&mut self.out, delta.len() as u64)?;
        self.out.write_all(&delta)?;
        let index = self.next;
        self.next += 1;
        let chain = chain + body.len();
        self.recent.push_back(Recent { index, hash, body, depth, chain });
        if self.recent.len() > self.window {
            self.recent.pop_front();
        }
        Ok(index)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    pub fn into_inner(self) -> W {
        self.out
    }
}

/// One record's place in the file, read without decoding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub meta: Vec<u8>,
    /// Records back to its base; 0: none (the record is stored whole).
    pub back: u64,
    /// Bytes the record's delta takes in the file.
    pub stored: u64,
    offset: u64,
}

/// Reads an LPK1 file. `open` indexes it; records are decoded only when asked for.
#[derive(Debug)]
pub struct Reader<R> {
    src: BufReader<R>,
    window: usize,
    max_body: usize,
    records: Vec<Record>,
    /// The file ends inside a record (its writer stopped mid-write): the records before it are whole.
    pub truncated: bool,
}

impl<R: Read + Seek> Reader<R> {
    /// Indexes the file. `max_body` caps any body it will rebuild, as the server's limit does.
    pub fn open(src: R, max_body: usize) -> io::Result<Self> {
        let mut src = BufReader::new(src);
        let len = src.seek(SeekFrom::End(0))?;
        src.seek(SeekFrom::Start(0))?;
        let window = header(&mut src)?;
        let (records, truncated) = index(&mut src, len, window)?;
        Ok(Reader { src, window: window as usize, max_body, records, truncated })
    }

    pub fn records(&self) -> &[Record] {
        &self.records
    }

    /// Record `i`'s body, rebuilt from its chain of bases and checked by SHA-256.
    pub fn get(&mut self, i: usize) -> io::Result<Vec<u8>> {
        if i >= self.records.len() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such record"));
        }
        let mut body = Vec::new();
        for &j in chain(&self.records, i).iter().rev() {
            body = self.rebuild(j, &body)?;
        }
        Ok(body)
    }

    /// Every record in order, each body rebuilt once: holds the last `window` bodies, not the file.
    pub fn scan(&mut self, mut f: impl FnMut(&Record, &[u8]) -> io::Result<()>) -> io::Result<()> {
        let mut recent: VecDeque<Vec<u8>> = VecDeque::new();
        for i in 0..self.records.len() {
            let body = self.in_order(i, &recent)?;
            f(&self.records[i], &body)?;
            remember(&mut recent, body, self.window);
        }
        Ok(())
    }

    /// Record `i`'s body, the records before it having been read in order into `recent`.
    fn in_order(&mut self, i: usize, recent: &VecDeque<Vec<u8>>) -> io::Result<Vec<u8>> {
        match self.records[i].back as usize {
            0 => self.rebuild(i, b""),
            back => {
                // `open` admitted back <= i and back <= window, and `recent` holds min(i, window)
                self.rebuild(i, &recent[recent.len() - back])
            }
        }
    }

    fn rebuild(&mut self, i: usize, base: &[u8]) -> io::Result<Vec<u8>> {
        let Record { offset, stored, .. } = self.records[i];
        self.src.seek(SeekFrom::Start(offset))?;
        let mut delta = vec![0; stored as usize];
        self.src.read_exact(&mut delta)?;
        let (header, frame) = codec::parse(&delta, self.max_body).map_err(invalid)?;
        if header.base != codec::hash(base) {
            return Err(invalid(codec::Error::Corrupt));
        }
        codec::apply(base, &header, frame).map_err(invalid)
    }
}

/// The file header: its magic and version, then the window.
fn header(src: &mut impl Read) -> io::Result<u64> {
    let mut magic = [0; 5];
    src.read_exact(&mut magic).map_err(|_| invalid("not an LPK1 file"))?;
    if &magic[..4] != MAGIC {
        return Err(invalid("not an LPK1 file"));
    }
    if magic[4] != VERSION {
        return Err(invalid("unsupported LPK1 version"));
    }
    read_varint(src)?.ok_or_else(|| invalid("truncated header"))
}

/// Every record's place from `src`'s position to `len`, payloads skipped; and whether the file ends
/// inside a record (the records before it are whole).
fn index(src: &mut (impl Read + Seek), len: u64, window: u64) -> io::Result<(Vec<Record>, bool)> {
    let mut records = Vec::new();
    while src.stream_position()? != len {
        match next_record(src, len, records.len() as u64, window)? {
            Some(record) => records.push(record),
            None => return Ok((records, true)),
        }
    }
    Ok((records, false))
}

/// Record `n`'s header, its payload skipped; `None` when the file ends inside it.
fn next_record(src: &mut (impl Read + Seek), len: u64, n: u64, window: u64) -> io::Result<Option<Record>> {
    let Some(back) = read_varint(src)? else { return Ok(None) };
    if back > n || back > window {
        return Err(invalid("a record names a base it cannot have"));
    }
    let Some(meta_len) = read_varint(src)? else { return Ok(None) };
    // a meta longer than the rest of the file: the file ends inside it
    if (len - src.stream_position()?).checked_sub(meta_len).is_none() {
        return Ok(None);
    }
    let mut meta = vec![0; meta_len as usize];
    src.read_exact(&mut meta)?;
    let Some(stored) = read_varint(src)? else { return Ok(None) };
    let offset = src.stream_position()?;
    if stored > len - offset {
        return Ok(None);
    }
    src.seek(SeekFrom::Start(offset + stored))?;
    Ok(Some(Record { meta, back, stored, offset }))
}

/// Record `i`, its base, its base's base, … down to one stored whole. `open` admitted only records
/// with `back <= index`, so each step goes strictly down and stays in bounds.
fn chain(records: &[Record], i: usize) -> Vec<usize> {
    let mut chain = vec![i];
    let mut at = i;
    while records[at].back != 0 {
        at -= records[at].back as usize;
        chain.push(at);
    }
    chain
}

/// Keeps the `window` latest bodies: the ones a later record may name as its base.
fn remember(recent: &mut VecDeque<Vec<u8>>, body: Vec<u8>, window: usize) {
    recent.push_back(body);
    if recent.len() > window {
        recent.pop_front();
    }
}

/// Every record in order with its body, each rebuilt once, as [`Reader::scan`] reads them. After an
/// error it yields nothing more.
#[derive(Debug)]
pub struct Records<R> {
    reader: Reader<R>,
    next: usize,
    recent: VecDeque<Vec<u8>>,
    failed: bool,
}

impl<R: Read + Seek> IntoIterator for Reader<R> {
    type Item = io::Result<(Record, Vec<u8>)>;
    type IntoIter = Records<R>;

    fn into_iter(self) -> Records<R> {
        Records { reader: self, next: 0, recent: VecDeque::new(), failed: false }
    }
}

impl<R: Read + Seek> Iterator for Records<R> {
    type Item = io::Result<(Record, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.next;
        if self.failed || i >= self.reader.records.len() {
            return None;
        }
        self.next += 1;
        match self.reader.in_order(i, &self.recent) {
            Ok(body) => {
                remember(&mut self.recent, body.clone(), self.reader.window);
                Some(Ok((self.reader.records[i].clone(), body)))
            }
            Err(e) => {
                self.failed = true;
                Some(Err(e))
            }
        }
    }
}

fn invalid(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    // Kani: the same kind without its message (a boxed `dyn Error` is beyond what CBMC models in
    // memory); no proof checks the message
    if cfg!(kani) {
        return io::ErrorKind::InvalidData.into();
    }
    io::Error::new(io::ErrorKind::InvalidData, e)
}

fn write_varint(out: &mut impl Write, v: u64) -> io::Result<()> {
    let mut buf = Vec::with_capacity(10);
    codec::write_varint(&mut buf, v);
    out.write_all(&buf)
}

/// A canonical varint from a stream; `None` when the stream ends inside it.
fn read_varint(src: &mut impl Read) -> io::Result<Option<u64>> {
    let mut buf = Vec::with_capacity(10);
    let mut byte = [0];
    loop {
        if src.read(&mut byte)? == 0 {
            return Ok(None);
        }
        buf.push(byte[0]);
        if byte[0] & 0x80 == 0 || buf.len() == 10 {
            return codec::read_varint(&buf).map(|(v, _)| Some(v)).map_err(invalid);
        }
    }
}

/// Kani proofs over every input of a bounded size: the reader's parsing and index arithmetic never
/// panic, and admit only what SPEC.md §7 allows. zstd and SHA-256 (in `rebuild`) are C and assembly
/// Kani does not model; the property tests and the `log` fuzz target cover them.
#[cfg(kani)]
mod proofs {
    use super::*;
    use std::io::Cursor;

    #[kani::proof]
    #[kani::unwind(12)]
    fn a_varint_from_a_stream_is_the_codecs_or_ends_early() {
        let bytes: [u8; 11] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 11);
        match read_varint(&mut &bytes[..len]) {
            Ok(Some(v)) => {
                let mut again = Vec::new();
                codec::write_varint(&mut again, v);
                assert!(again[..] == bytes[..again.len()], "the canonical encoding it read");
            }
            // the stream ended inside it: every byte read was a continuation byte
            Ok(None) => assert!(len < 10 && bytes[..len].iter().all(|b| b & 0x80 != 0)),
            Err(e) => assert!(e.kind() == io::ErrorKind::InvalidData),
        }
    }

    /// Any bytes (up to 5: one record's header and payload), any record count and window: a record
    /// is admitted only if it fits the file and names a base the count and window allow. `index`
    /// pushes only what this returns, with the count so far, so every index it builds holds this.
    #[kani::proof]
    #[kani::unwind(7)]
    fn a_record_is_admitted_only_if_it_fits_the_file_and_the_window() {
        let bytes: [u8; 5] = kani::any();
        let len: usize = kani::any_where(|&l: &usize| l <= 5);
        let (n, window): (u64, u64) = (kani::any(), kani::any());
        let mut src = Tiny { bytes, len, pos: 0 };
        if let Ok(Some(r)) = next_record(&mut src, len as u64, n, window) {
            assert!(r.back <= n && r.back <= window);
            assert!(r.offset + r.stored <= len as u64);
            assert!(src.pos as u64 == r.offset + r.stored, "the next record starts after this one");
        }
    }

    /// A file of up to 5 bytes, read a byte at a time: what `Cursor` does, in less for CBMC to model.
    struct Tiny {
        bytes: [u8; 5],
        len: usize,
        pos: usize,
    }

    impl Read for Tiny {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if buf.is_empty() || self.pos >= self.len {
                return Ok(0);
            }
            buf[0] = self.bytes[self.pos];
            self.pos += 1;
            Ok(1)
        }
    }

    impl Seek for Tiny {
        fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
            match to {
                SeekFrom::Start(p) if p <= self.len as u64 => self.pos = p as usize,
                SeekFrom::Current(0) => {}
                _ => return Err(io::Error::from(io::ErrorKind::InvalidInput)),
            }
            Ok(self.pos as u64)
        }
    }

    /// Any index `open` can admit: the chain behind any record ends at one stored whole, in bounds.
    #[kani::proof]
    #[kani::unwind(6)]
    fn a_chain_walks_down_to_a_whole_record() {
        const N: usize = 4;
        let backs: [u64; N] = kani::any();
        let records: Vec<Record> = (0..N)
            .map(|i| {
                kani::assume(backs[i] <= i as u64);
                Record { meta: Vec::new(), back: backs[i], stored: 0, offset: 0 }
            })
            .collect();
        let i: usize = kani::any_where(|&i: &usize| i < N);
        let c = chain(&records, i);
        assert!(c[0] == i && records[*c.last().unwrap()].back == 0);
        for w in c.windows(2) {
            assert!(w[1] < w[0] && w[0] - w[1] == records[w[0]].back as usize);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex;
    use proptest::prelude::*;
    use std::io::Cursor;

    /// A gateway's request after `turns` turns: a 20 KB system prompt of its own that does not
    /// compress by itself (hex of hashes), then the turns.
    fn request(name: &str, turns: usize) -> Bytes {
        let system: String = (0..300).map(|i| hex(&codec::hash(format!("{name}{i}").as_bytes()))).collect();
        let msgs: String = (0..turns).map(|i| format!(r#"{{"role":"user","content":"{name} turn {i}"}},"#)).collect();
        Bytes::from(format!(r#"{{"system":"{system}","messages":[{msgs}{{}}]}}"#))
    }

    fn write(window: usize, records: &[(Vec<u8>, Bytes)]) -> Vec<u8> {
        let mut w = Writer::new(Vec::new(), window).unwrap();
        for (i, (meta, body)) in records.iter().enumerate() {
            assert_eq!(w.append(meta, body.clone()).unwrap(), i as u64);
        }
        w.flush().unwrap();
        w.into_inner()
    }

    fn open(file: &[u8]) -> io::Result<Reader<Cursor<&[u8]>>> {
        Reader::open(Cursor::new(file), 1 << 26)
    }

    fn scanned(file: &[u8]) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        open(file)?.scan(|r, body| {
            out.push((r.meta.clone(), body.to_vec()));
            Ok(())
        })?;
        Ok(out)
    }

    #[test]
    fn interleaved_conversations_cost_about_their_new_bytes() {
        let mut records = Vec::new();
        for turn in 1..=10 {
            for name in ["a", "b", "c"] {
                records.push((format!("{name}{turn}").into_bytes(), request(name, turn)));
            }
        }
        let file = write(4, &records);
        let raw: usize = records.iter().map(|(_, b)| b.len()).sum();
        let first_turns: usize = records[..3].iter().map(|(_, b)| b.len()).sum();
        assert!(file.len() < first_turns + 200 * records.len(), "{} bytes for {raw}", file.len());
        let r = open(&file).unwrap();
        assert!(r.records()[3..].iter().all(|r| r.back == 3), "each finds its own conversation");
        let back: Vec<_> = scanned(&file).unwrap();
        assert_eq!(back, records.iter().map(|(m, b)| (m.clone(), b.to_vec())).collect::<Vec<_>>());
    }

    #[test]
    fn a_reader_iterates_its_records_in_order_as_scan_reads_them() {
        let records: Vec<_> =
            (1..=6).flat_map(|t| ["a", "b"].map(|n| (format!("{n}{t}").into_bytes(), request(n, t)))).collect();
        let file = write(2, &records);
        let iterated: Vec<(Vec<u8>, Vec<u8>)> =
            open(&file).unwrap().into_iter().map(|r| r.map(|(rec, body)| (rec.meta, body)).unwrap()).collect();
        assert_eq!(iterated, scanned(&file).unwrap());
        let mut bad = file.clone();
        *bad.last_mut().unwrap() ^= 1;
        let mut it = open(&bad).unwrap().into_iter();
        assert_eq!(it.by_ref().take(11).filter(Result::is_ok).count(), 11);
        assert!(it.next().unwrap().is_err(), "the corrupt last record");
        assert!(it.next().is_none(), "and nothing after it");
    }

    #[test]
    fn spans_whose_ids_come_first_still_find_their_trace() {
        // ids at the front: a span shares ~30 edge bytes with its own trace's last span, and more
        // with another trace's span at the same turn (their last turns end alike)
        let span = |t: usize, s: usize| {
            let prompt = String::from_utf8(request(&format!("t{t}"), s + 1).to_vec()).unwrap();
            let last_turn = "tell me more ".repeat(s); // ends the same in every trace
            Bytes::from(format!(r#"{{"trace_id":"t{t}","span_id":"s{s}","prompt":{prompt:?},"turn":"{last_turn}"}}"#))
        };
        let records: Vec<_> = (0..6).flat_map(|s| (0..3).map(move |t| (vec![], span(t, s)))).collect();
        let file = write(4, &records);
        let r = open(&file).unwrap();
        assert!(r.records()[3..].iter().all(|r| r.back == 3), "{:?}", r.records());
        assert_eq!(scanned(&file).unwrap().len(), records.len());
    }

    #[test]
    fn any_record_is_read_on_its_own() {
        let records: Vec<_> = (1..=100).map(|t| (vec![], request("a", t))).collect();
        let file = write(1, &records);
        let mut r = open(&file).unwrap();
        for i in [99, 0, 64, 65, 37] {
            assert_eq!(r.get(i).unwrap(), records[i].1, "record {i}");
        }
        assert_eq!(r.get(100).unwrap_err().kind(), io::ErrorKind::NotFound);
        // against the one before (the window's edge), whole where the byte budget restarts a chain
        assert!(
            r.records()[1..].iter().all(|r| r.back <= 1) && r.records().iter().filter(|r| r.back == 1).count() > 90
        );
        assert_eq!(scanned(&file).unwrap().len(), 100);
    }

    #[test]
    fn a_resent_context_deltas_against_the_base_sharing_the_most_not_the_newest_small_enough() {
        let context: String = (0..1600).map(|i| hex(&codec::hash(format!("p{i}").as_bytes()))).collect();
        let (cut, junk) = (context.len() * 99 / 100, "j".repeat(1000));
        let old = Bytes::from(context.clone()); // all of the context
        let new = Bytes::from(format!("{}{junk}", &context[..cut])); // 99% of it: small, not smallest
        let target = Bytes::from(format!("{context} and a new turn"));
        let file = write(4, &[(vec![], old), (vec![], new), (vec![], target)]);
        let r = open(&file).unwrap();
        assert_eq!(r.records()[2].back, 2);
        assert!(r.records()[2].stored < 200, "{} bytes", r.records()[2].stored);
    }

    #[test]
    fn with_no_base_making_it_small_the_smallest_delta_wins_and_the_newest_on_a_tie() {
        let random = |tag: &str, n: usize| -> String {
            (0..n).map(|i| hex(&codec::hash(format!("{tag}{i}").as_bytes()))).collect()
        };
        // the target shares half its bytes with `half`, in the middle (its edges are its own), and
        // nothing with `other`: no delta comes under 1/64 of it
        let shared = random("shared", 64);
        let half = Bytes::from(format!("{}{shared}{}", random("h1", 32), random("h2", 32)));
        let other = Bytes::from(random("other", 128));
        let target = Bytes::from(format!("{}{shared}{}", random("t1", 32), random("t2", 32)));
        let file = write(4, &[(vec![], half.clone()), (vec![], other.clone()), (vec![], target.clone())]);
        assert_eq!(open(&file).unwrap().records()[2].back, 2, "the base it shares half with");
        // two bases equally good: the newer one (a shorter way back)
        let file = write(4, &[(vec![], half.clone()), (vec![], half), (vec![], other), (vec![], target)]);
        assert_eq!(open(&file).unwrap().records()[3].back, 2);
    }

    #[test]
    fn flush_reaches_the_underlying_writer() {
        #[derive(Default)]
        struct Flushes(Vec<u8>, usize);
        impl Write for Flushes {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                self.0.extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                self.1 += 1;
                Ok(())
            }
        }
        let mut w = Writer::new(Flushes::default(), 1).unwrap();
        w.append(b"", Bytes::from_static(b"x")).unwrap();
        w.flush().unwrap();
        assert_eq!(w.into_inner().1, 1);
    }

    #[test]
    fn a_long_trace_keeps_its_own_chain() {
        // 100 spans per trace, interleaved: a count cap of 64 sent span 64 of each to another trace
        let span = |n: &str, t: usize| Bytes::from((0..t).map(|i| format!("{n}{i} ")).collect::<String>());
        let records: Vec<_> = (1..=100).flat_map(|t| ["a", "b", "c"].map(|n| (vec![], span(n, t)))).collect();
        let file = write(4, &records);
        assert!(open(&file).unwrap().records()[3..].iter().all(|r| r.back == 3));
    }

    #[test]
    fn reading_a_record_rebuilds_a_bounded_chain() {
        // by bytes: ~20 KB bodies against a (test-sized) 1 MiB budget
        let records: Vec<_> = (1..=100).map(|t| (vec![], request("a", t))).collect();
        let file = write(1, &records);
        let r = open(&file).unwrap();
        let restart = r.records().iter().skip(1).position(|r| r.back == 0).unwrap() + 1;
        let chain: usize = records[..restart + 1].iter().map(|(_, b)| b.len()).sum();
        assert!(chain > MAX_CHAIN_BYTES, "restarted at {restart}, its chain would rebuild {chain} bytes");
        assert!(chain - records[restart].1.len() <= MAX_CHAIN_BYTES);
        // by records: tiny bodies never reach the byte budget
        let records: Vec<_> = (0..MAX_DEPTH + 1).map(|i| (vec![], Bytes::from(format!("x{i}")))).collect();
        let file = write(1, &records);
        let r = open(&file).unwrap();
        assert_eq!(r.records()[MAX_DEPTH].back, 0, "a chain stops at {MAX_DEPTH} records");
        assert!(r.records()[1..MAX_DEPTH].iter().all(|r| r.back == 1));
    }

    #[test]
    fn an_unrelated_body_is_stored_whole_and_a_window_of_0_stores_all_whole() {
        let file = write(4, &[(vec![], request("a", 1)), (vec![], Bytes::from_static(b"tiny"))]);
        assert_eq!(open(&file).unwrap().records()[1].back, 0, "it shares no byte with the base");
        let file = write(0, &[(vec![], request("a", 1)), (vec![], request("a", 2))]);
        assert!(open(&file).unwrap().records().iter().all(|r| r.back == 0));
        assert_eq!(scanned(&file).unwrap()[1].1, request("a", 2));
    }

    #[test]
    fn the_layout_is_the_spec() {
        let file = write(8, &[(b"m".to_vec(), Bytes::from_static(b"hello"))]);
        assert_eq!(&file[..6], b"LPK1\x01\x08");
        // back 0, meta "m", then the LPD1 delta against the empty body
        assert_eq!(&file[6..9], b"\x00\x01m");
        assert_eq!(&file[10..14], b"LPD1");
        assert_eq!(file[9] as usize, file.len() - 10);
        assert_eq!(&file[15..47], &codec::hash(b""));
    }

    #[test]
    fn a_file_cut_short_keeps_every_whole_record() {
        let records: Vec<_> = (1..=5).map(|t| (b"meta".to_vec(), request("a", t))).collect();
        let file = write(2, &records);
        let ends: Vec<usize> = (0..=5).map(|n| write(2, &records[..n]).len()).collect();
        for cut in 6..file.len() {
            let whole = ends.iter().rposition(|&e| e <= cut).unwrap();
            let r = open(&file[..cut]).unwrap();
            assert_eq!((r.records().len(), r.truncated), (whole, ends[whole] != cut), "cut at {cut}");
        }
        assert_eq!(scanned(&file[..file.len() - 1]).unwrap().len(), 4);
    }

    #[test]
    fn what_is_not_an_lpk1_file_is_refused() {
        for bad in [&b""[..], b"LPK", b"LPD1\x01\x00", b"LPK1\x02\x00", b"LPK1\x01"] {
            assert_eq!(open(bad).unwrap_err().kind(), io::ErrorKind::InvalidData, "{bad:?}");
        }
        // a record naming a base before the first record, or beyond the window
        assert!(open(b"LPK1\x01\x02\x01\x00\x00").is_err());
        let mut file = write(1, &[(vec![], request("a", 1)), (vec![], request("a", 2))]);
        file.extend_from_slice(b"\x02\x00\x00");
        assert!(open(&file).is_err(), "back 2 over a window of 1");
    }

    #[test]
    fn a_record_rebuilt_against_the_wrong_base_is_refused() {
        // record 2 deltas against 1; point it at 0 instead (same window, a valid back)
        let records: Vec<_> = [request("a", 1), request("b", 1), request("b", 2)].map(|b| (vec![], b)).to_vec();
        let mut file = write(2, &records);
        assert_eq!(open(&file).unwrap().records()[2].back, 1);
        let back_at = write(2, &records[..2]).len(); // record 2 starts with its back varint
        file[back_at] = 2;
        let err = open(&file).unwrap().get(2).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(scanned(&file).is_err());
    }

    #[test]
    fn a_body_over_the_readers_limit_is_refused() {
        let file = write(1, &[(vec![], request("a", 1))]);
        let mut r = Reader::open(Cursor::new(&file[..]), 100).unwrap();
        assert_eq!(r.get(0).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn any_records_round_trip(
            window in 0usize..4,
            bodies in proptest::collection::vec(
                (proptest::collection::vec(any::<u8>(), 0..8), proptest::collection::vec(any::<u8>(), 0..300)),
                0..20,
            ),
        ) {
            // records that share bytes, as a log's do: each extends a random earlier one
            let mut records: Vec<(Vec<u8>, Bytes)> = Vec::new();
            for (i, (meta, tail)) in bodies.into_iter().enumerate() {
                let head = if i > 0 { records[tail.len() % i].1.to_vec() } else { vec![] };
                records.push((meta, Bytes::from([head, tail].concat())));
            }
            let file = write(window, &records);
            let mut r = open(&file).unwrap();
            prop_assert!(!r.truncated);
            for (i, (meta, body)) in records.iter().enumerate() {
                prop_assert_eq!(&r.records()[i].meta, meta);
                prop_assert_eq!(&r.get(i).unwrap(), body);
            }
            let expected: Vec<_> = records.iter().map(|(m, b)| (m.clone(), b.to_vec())).collect();
            prop_assert_eq!(scanned(&file).unwrap(), expected);
        }

        #[test]
        fn a_corrupted_file_never_yields_a_wrong_body(
            at in any::<proptest::sample::Index>(),
            flip in 1u8..,
        ) {
            let records: Vec<_> = (1..=3).map(|t| (vec![], request("a", t))).collect();
            let mut file = write(2, &records);
            let i = at.index(file.len());
            file[i] ^= flip;
            if let Ok(mut r) = open(&file) {
                let n = r.records().len();
                for (j, (_, expected)) in records.iter().enumerate().take(n) {
                    if let Ok(body) = r.get(j) {
                        prop_assert_eq!(&body, expected, "byte {} flipped", i);
                    }
                }
            }
        }
    }
}
