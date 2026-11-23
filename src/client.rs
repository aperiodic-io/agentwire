//! The client side: send each request body as a delta against one the server confirmed it kept.
//!
//! ```no_run
//! # fn send(body: &[u8], headers: &[(&str, &str)]) -> (u16, Option<String>, Option<String>) { todo!() }
//! use logpak::client::Client;
//! let client = Client::new(4);
//! # let body = bytes::Bytes::from_static(b"{}");
//! let request = client.prepare(body);
//! let (_status, mut kept, error) = send(request.wire_body(), &request.headers());
//! if error.is_some() && request.is_delta() {
//!     // the server lost or refused the base: the same request, in full (it was not forwarded)
//!     (_, kept, _) = send(request.body(), &request.plain_headers());
//! }
//! client.confirm(&request, kept.as_deref());
//! ```

use std::collections::VecDeque;
use std::sync::Mutex;

use bytes::Bytes;

use crate::codec::{self, Hash};
use crate::{ENCODING, KEEP};

/// The bodies the server confirmed it kept (`awd1-kept`), most recent first. It is `Sync`: one
/// client serves concurrent requests, and each picks the base it shares the most bytes with.
#[derive(Debug)]
pub struct Client {
    bases: Mutex<VecDeque<(Hash, Bytes)>>,
    max_bases: usize,
}

/// A request body ready to send: in full, or as a delta when that is smaller.
#[derive(Debug)]
pub struct Prepared {
    body: Bytes,
    target: Hash,
    delta: Option<Vec<u8>>,
}

impl Client {
    /// `max_bases` bounds the memory (that many bodies) and the encoder's scan per request. One
    /// conversation needs 1; a proxy multiplexing several needs about one per live conversation.
    pub fn new(max_bases: usize) -> Self {
        Client { bases: Mutex::default(), max_bases }
    }

    pub fn prepare(&self, body: Bytes) -> Prepared {
        let target = codec::hash(&body);
        // a clone of each base (refcounted, no copy), so the scan runs outside the lock
        let bases: Vec<(Hash, Bytes)> = self.lock().iter().cloned().collect();
        let delta = bases
            .iter()
            .max_by_key(|(_, base)| codec::shared(base, &body))
            .map(|(hash, base)| codec::encode(base, hash, &body, &target))
            .filter(|delta| delta.len() < body.len());
        Prepared { body, target, delta }
    }

    /// Records the server's answer to `request`: its `awd1-kept` header, if it had one.
    pub fn confirm(&self, request: &Prepared, kept: Option<&str>) {
        if kept != Some(crate::hex(&request.target).as_str()) {
            return;
        }
        let mut bases = self.lock();
        bases.retain(|(hash, _)| *hash != request.target);
        bases.push_front((request.target, request.body.clone()));
        bases.truncate(self.max_bases);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<(Hash, Bytes)>> {
        // every mutation of the list completes under the lock, so a poisoned one is still whole
        self.bases.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Prepared {
    pub fn is_delta(&self) -> bool {
        self.delta.is_some()
    }

    /// What goes on the wire.
    pub fn wire_body(&self) -> &[u8] {
        self.delta.as_deref().unwrap_or(&self.body)
    }

    /// The original body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The request headers that go with [`Prepared::wire_body`].
    pub fn headers(&self) -> Vec<(&'static str, &'static str)> {
        let mut h = self.plain_headers();
        if self.is_delta() {
            h.push(("content-encoding", ENCODING));
        }
        h
    }

    /// The request headers that go with [`Prepared::body`].
    pub fn plain_headers(&self) -> Vec<(&'static str, &'static str)> {
        vec![(KEEP, "?1")]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hex;

    /// A conversation's request after `turns` turns; its 20 KB system prompt is its own and does
    /// not compress on its own (hex of hashes), so only a base of the same conversation helps.
    fn conversation(name: &str, turns: usize) -> Bytes {
        let system: String = (0..300).map(|i| hex(&codec::hash(format!("{name}{i}").as_bytes()))).collect();
        let msgs: String = (0..turns).map(|i| format!(r#"{{"role":"user","content":"{name} turn {i}"}},"#)).collect();
        Bytes::from(format!(r#"{{"system":"{system}","messages":[{msgs}{{}}]}}"#))
    }

    fn small(p: &Prepared) -> bool {
        p.wire_body().len() * 20 < p.body().len()
    }

    /// The server's side, for these tests: keeps everything it is sent.
    fn kept(client: &Client, body: Bytes) -> Prepared {
        let p = client.prepare(body);
        client.confirm(&p, Some(&hex(&codec::hash(p.body()))));
        p
    }

    #[test]
    fn the_first_request_goes_in_full_and_asks_to_be_kept() {
        let c = Client::new(4);
        let p = c.prepare(conversation("a", 1));
        assert!(!p.is_delta());
        assert_eq!(p.wire_body(), p.body());
        assert_eq!(p.headers(), [("awd1-keep", "?1")]);
    }

    #[test]
    fn a_confirmed_body_becomes_the_base_of_the_next() {
        let c = Client::new(4);
        kept(&c, conversation("a", 1));
        let p = c.prepare(conversation("a", 2));
        assert!(p.is_delta());
        assert!(small(&p), "{} of {}", p.wire_body().len(), p.body().len());
        assert_eq!(p.headers(), [("awd1-keep", "?1"), ("content-encoding", "awd1")]);
        assert_eq!(p.plain_headers(), [("awd1-keep", "?1")]);
    }

    #[test]
    fn a_body_the_server_did_not_confirm_is_never_a_base() {
        let c = Client::new(4);
        let p = c.prepare(conversation("a", 1));
        c.confirm(&p, None);
        c.confirm(&p, Some("00"));
        c.confirm(&p, Some(&hex(&codec::hash(b"another body"))));
        assert!(!c.prepare(conversation("a", 2)).is_delta());
    }

    #[test]
    fn the_base_sharing_the_most_bytes_wins_even_with_no_common_suffix() {
        let random = |tag: &str, n: usize| -> String {
            (0..n).map(|i| hex(&codec::hash(format!("{tag}{i}").as_bytes()))).collect()
        };
        let shared = random("shared", 300); // 19 KB both the target and base `long` start with
        let target = Bytes::from(format!("{shared}{} the end", random("new", 2)));
        let long = Bytes::from(format!("{shared}{}", random("old", 2)));
        // shares 10 bytes at each end: a product of the two would rank it above `long`'s 19,200 + 0
        let edges = Bytes::from(format!("{}{} the end", &shared[..10], random("other", 300)));
        let c = Client::new(4);
        kept(&c, long);
        kept(&c, edges);
        let p = c.prepare(target);
        assert!(small(&p), "{} of {}", p.wire_body().len(), p.body().len());
    }

    #[test]
    fn interleaved_conversations_each_find_their_own_base() {
        let c = Client::new(4);
        for turn in 1..6 {
            for name in ["a", "b", "c"] {
                let p = kept(&c, conversation(name, turn));
                if turn > 1 {
                    assert!(small(&p), "{name} turn {turn}");
                }
            }
        }
    }

    #[test]
    fn it_keeps_at_most_max_bases() {
        let c = Client::new(2);
        for name in ["a", "b", "c"] {
            kept(&c, conversation(name, 1));
        }
        assert!(!small(&c.prepare(conversation("a", 2))), "a's base was the oldest of three");
        assert!(small(&c.prepare(conversation("c", 2))));
        assert_eq!(c.bases.lock().unwrap().len(), 2);
    }

    #[test]
    fn confirming_a_base_again_does_not_duplicate_it() {
        let c = Client::new(4);
        let p = kept(&c, conversation("a", 1));
        c.confirm(&p, Some(&hex(&codec::hash(p.body()))));
        assert_eq!(c.bases.lock().unwrap().len(), 1);
    }

    #[test]
    fn an_unrelated_body_goes_in_full() {
        let c = Client::new(4);
        kept(&c, conversation("a", 1));
        let p = c.prepare(Bytes::from_static(b"tiny"));
        assert!(!p.is_delta(), "a delta would be larger than these 4 bytes");
    }
}
