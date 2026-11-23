//! Any bytes as an LPK1 file: refused, or read without panicking, and every record that reads
//! reads the same by index as in a scan.
#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use logpak::log::Reader;

fuzz_target!(|file: &[u8]| {
    let Ok(mut r) = Reader::open(Cursor::new(file), 1 << 20) else { return };
    let mut scanned = Vec::new();
    let _ = r.scan(|_, body| {
        scanned.push(body.to_vec());
        Ok(())
    });
    for (i, body) in scanned.iter().enumerate() {
        assert_eq!(&r.get(i).unwrap(), body);
    }
});
