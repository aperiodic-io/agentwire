pub mod client;
pub mod codec;
pub mod log;
#[cfg(feature = "server")]
pub mod server;
pub mod store;

/// Request header: keep this body as a base for my next request. Its value is `?1`.
pub const KEEP: &str = "dpd1-keep";
/// Response header: the server kept the request's body, under this lowercase hex SHA-256.
pub const KEPT: &str = "dpd1-kept";
/// Response header on a refused delta: the [`codec::Error::code`] token. The request was not
/// forwarded, so the client can send it again in full.
pub const ERROR: &str = "dpd1-error";
/// The `content-encoding` of a delta body.
pub const ENCODING: &str = "dpd1";

/// Lowercase hex, as `dpd1-kept` carries a hash.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes.iter().flat_map(|b| [DIGITS[usize::from(b >> 4)] as char, DIGITS[usize::from(b & 15)] as char]).collect()
}
