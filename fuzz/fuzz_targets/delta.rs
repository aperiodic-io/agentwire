//! Any bytes as a delta against any base: refused, or rebuilt into exactly the body it names.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logpak::codec;

fuzz_target!(|input: (&[u8], &[u8])| {
    let (base, delta) = input;
    let Ok((header, frame)) = codec::parse(delta, 1 << 20) else { return };
    if let Ok(target) = codec::apply(base, &header, frame) {
        assert_eq!(codec::hash(&target), header.target);
        assert_eq!(target.len(), header.target_len);
    }
});
