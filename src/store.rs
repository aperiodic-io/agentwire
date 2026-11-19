//! The server's bases: kept request bodies, by tenant scope and SHA-256, under a byte budget and a TTL.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::codec::Hash;

/// Which tenant a body belongs to: a base is found only under the scope that kept it.
pub type Scope = Hash;

type Key = (Scope, Hash);

/// A least-recently-used map of bodies; a base unused for the TTL is dropped. Evicting a base costs its client one full-size request,
/// never an error, so the budget is a hard cap.
#[derive(Debug)]
pub struct Store {
    budget: usize,
    ttl: Duration,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<Key, Entry>,
    by_use: BTreeMap<u64, Key>,
    bytes: usize,
    clock: u64,
}

#[derive(Debug)]
struct Entry {
    body: Bytes,
    used: u64,
    used_at: Instant,
}

impl Store {
    pub fn new(budget_bytes: usize, ttl: Duration) -> Self {
        Store { budget: budget_bytes, ttl, inner: Mutex::default() }
    }

    /// Keeps `body` under `hash`, which the caller computed from it.
    pub fn put(&self, scope: &Scope, hash: Hash, body: Bytes) {
        self.put_at(scope, hash, body, Instant::now());
    }

    pub fn get(&self, scope: &Scope, hash: &Hash) -> Option<Bytes> {
        self.get_at(scope, hash, Instant::now())
    }

    /// Bytes held now.
    pub fn bytes(&self) -> usize {
        self.lock().bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // a panic elsewhere while holding the lock leaves the map consistent (every mutation below
        // completes or does not start), so a poisoned lock is still safe to use
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn put_at(&self, scope: &Scope, hash: Hash, body: Bytes, now: Instant) {
        if body.len() > self.budget {
            return;
        }
        let mut inner = self.lock();
        inner.expire(now, self.ttl);
        let key = (*scope, hash);
        if inner.touch(&key, now).is_some() {
            return;
        }
        // ponytail: one budget for all tenants, so one tenant's burst can evict another's bases
        // (they fall back to a full request); a per-scope share if that shows up in the metrics
        while inner.bytes + body.len() > self.budget {
            inner.pop_oldest();
        }
        inner.clock += 1;
        let used = inner.clock;
        inner.bytes += body.len();
        inner.by_use.insert(used, key);
        inner.entries.insert(key, Entry { body, used, used_at: now });
    }

    fn get_at(&self, scope: &Scope, hash: &Hash, now: Instant) -> Option<Bytes> {
        let mut inner = self.lock();
        inner.expire(now, self.ttl);
        inner.touch(&(*scope, *hash), now)
    }
}

impl Inner {
    /// Marks an entry used now and returns its body.
    fn touch(&mut self, key: &Key, now: Instant) -> Option<Bytes> {
        self.clock += 1;
        let used = self.clock;
        let entry = self.entries.get_mut(key)?;
        self.by_use.remove(&entry.used);
        self.by_use.insert(used, *key);
        entry.used = used;
        entry.used_at = now;
        Some(entry.body.clone())
    }

    fn pop_oldest(&mut self) {
        if let Some((_, key)) = self.by_use.pop_first() {
            let entry = self.entries.remove(&key).expect("by_use names only live entries");
            self.bytes -= entry.body.len();
        }
    }

    /// Drops what has not been used for `ttl`: the least recently used come first.
    fn expire(&mut self, now: Instant, ttl: Duration) {
        while let Some((_, key)) = self.by_use.first_key_value() {
            if now.saturating_duration_since(self.entries[key].used_at) <= ttl {
                break;
            }
            self.pop_oldest();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const A: Scope = [1; 32];
    const B: Scope = [2; 32];

    fn h(i: u8) -> Hash {
        [i; 32]
    }

    fn body(n: usize) -> Bytes {
        Bytes::from(vec![7; n])
    }

    #[test]
    fn a_kept_body_is_found_under_its_scope_only() {
        let s = Store::new(1000, Duration::from_secs(60));
        s.put(&A, h(1), body(10));
        assert_eq!(s.get(&A, &h(1)), Some(body(10)));
        assert_eq!(s.get(&B, &h(1)), None, "another tenant cannot use, or learn of, A's base");
        assert_eq!(s.get(&A, &h(2)), None);
    }

    #[test]
    fn the_least_recently_used_base_goes_first() {
        let s = Store::new(30, Duration::from_secs(60));
        s.put(&A, h(1), body(10));
        s.put(&A, h(2), body(10));
        s.put(&A, h(3), body(10));
        assert!(s.get(&A, &h(1)).is_some(), "1 is now the most recently used");
        s.put(&A, h(4), body(10));
        assert_eq!(s.get(&A, &h(2)), None, "2 was the least recently used");
        assert!(s.get(&A, &h(1)).is_some() && s.get(&A, &h(3)).is_some() && s.get(&A, &h(4)).is_some());
        assert_eq!(s.bytes(), 30);
    }

    #[test]
    fn a_body_over_the_budget_is_not_kept_and_evicts_nothing() {
        let s = Store::new(30, Duration::from_secs(60));
        s.put(&A, h(1), body(10));
        s.put(&A, h(2), body(31));
        assert_eq!(s.get(&A, &h(2)), None);
        assert!(s.get(&A, &h(1)).is_some());
    }

    #[test]
    fn keeping_the_same_body_twice_counts_it_once() {
        let s = Store::new(30, Duration::from_secs(60));
        s.put(&A, h(1), body(10));
        s.put(&A, h(1), body(10));
        assert_eq!(s.bytes(), 10);
    }

    #[test]
    fn a_base_expires_once_unused_for_its_ttl() {
        let s = Store::new(100, Duration::from_secs(60));
        let t0 = Instant::now();
        s.put_at(&A, h(1), body(10), t0);
        let t1 = t0 + Duration::from_secs(59);
        assert!(s.get_at(&A, &h(1), t1).is_some(), "and using it restarts its TTL");
        assert!(s.get_at(&A, &h(1), t1 + Duration::from_secs(60)).is_some());
        assert_eq!(s.get_at(&A, &h(1), t1 + Duration::from_secs(121)), None);
        assert_eq!(s.bytes(), 0, "an expired base is dropped, not just hidden");
    }

    #[test]
    fn keeping_a_body_drops_expired_ones() {
        let s = Store::new(100, Duration::from_secs(60));
        let t0 = Instant::now();
        s.put_at(&A, h(1), body(10), t0);
        s.put_at(&A, h(2), body(10), t0 + Duration::from_secs(61));
        assert_eq!(s.bytes(), 10);
    }

    proptest! {
        #[test]
        fn the_budget_holds_and_every_kept_body_is_intact(
            ops in proptest::collection::vec((any::<bool>(), 0u8..8, 0u8..2), 1..200),
        ) {
            let s = Store::new(100, Duration::from_secs(60));
            let mut kept: HashMap<Key, Bytes> = HashMap::new();
            for (put, i, scope) in ops {
                let scope = [scope; 32];
                // a body is named by its content: the same hash always holds the same bytes
                let b = Bytes::from(vec![i; usize::from(i) * 5]);
                if put {
                    s.put(&scope, h(i), b.clone());
                    kept.insert((scope, h(i)), b);
                } else if let Some(got) = s.get(&scope, &h(i)) {
                    prop_assert_eq!(Some(&got), kept.get(&(scope, h(i))));
                }
                prop_assert!(s.bytes() <= 100);
                let inner = s.lock();
                prop_assert_eq!(inner.bytes, inner.entries.values().map(|e| e.body.len()).sum::<usize>());
                prop_assert_eq!(inner.by_use.len(), inner.entries.len());
            }
        }
    }
}
