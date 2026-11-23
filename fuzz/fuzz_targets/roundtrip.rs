//! Any target against any base rebuilds byte for byte.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logpak::codec;

fuzz_target!(|input: (&[u8], &[u8])| {
    let (base, target) = input;
    let (bh, th) = (codec::hash(base), codec::hash(target));
    let delta = codec::encode(base, &bh, target, &th);
    let (header, frame) = codec::parse(&delta, usize::MAX).unwrap();
    assert_eq!((header.base, header.target), (bh, th));
    assert_eq!(codec::apply(base, &header, frame).unwrap(), target);
});
