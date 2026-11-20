pub mod client;
pub mod codec;
pub mod log;
#[cfg(feature = "server")]
pub mod server;
pub mod store;

/// Request header: keep this body as a base for my next request. Its value is `?1`.
pub const KEEP: &str = "awd1-keep";
/// Response header: the server kept the request's body, under this lowercase hex SHA-256.
pub const KEPT: &str = "awd1-kept";
/// Response header on a refused delta: the [`codec::Error::code`] token. The request was not
/// forwarded, so the client can send it again in full.
pub const ERROR: &str = "awd1-error";
/// The `content-encoding` of a delta body.
pub const ENCODING: &str = "awd1";

/// Lowercase hex, as `awd1-kept` carries a hash.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
