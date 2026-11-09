//! Bounded message deduplication by `message_id`.

use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasherDefault, Hasher};

/// Pass-through hasher for keys that are already uniformly distributed.
#[derive(Default)]
struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        // Only u64 keys are inserted; fold anything else defensively.
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

/// Remembers the last `capacity` message ids and reports repeats.
///
/// Ids are stored as 64-bit keyed hashes (seeded per client), so tracking a
/// message never allocates. With the default window of 1000 ids the chance
/// of a false duplicate is about 1 in 10^16 per message.
pub(crate) struct Deduplicator {
    capacity: usize,
    seen: HashSet<u64, BuildHasherDefault<IdentityHasher>>,
    order: VecDeque<u64>,
    keys: ahash::RandomState,
}

impl Deduplicator {
    pub(crate) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            seen: HashSet::with_capacity_and_hasher(capacity + 1, BuildHasherDefault::default()),
            order: VecDeque::with_capacity(capacity + 1),
            keys: ahash::RandomState::new(),
        }
    }

    /// Returns `true` when `id` was already seen; otherwise records it.
    pub(crate) fn check_and_track(&mut self, id: &str) -> bool {
        let key = self.keys.hash_one(id);
        if !self.seen.insert(key) {
            return true;
        }
        self.order.push_back(key);
        if self.order.len() > self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.seen.remove(&evicted);
        }
        false
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.seen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_repeats_and_evicts_oldest() {
        let mut dedup = Deduplicator::new(3);
        assert!(!dedup.check_and_track("a"));
        assert!(dedup.check_and_track("a"));
        assert!(!dedup.check_and_track("b"));
        assert!(!dedup.check_and_track("c"));
        assert!(!dedup.check_and_track("d")); // evicts "a"
        assert_eq!(dedup.len(), 3);
        assert!(!dedup.check_and_track("a"));
        assert!(dedup.check_and_track("d"));
    }

    #[test]
    fn handles_large_windows() {
        let mut dedup = Deduplicator::new(10_000);
        for index in 0..10_000 {
            assert!(!dedup.check_and_track(&format!("m{index}")));
        }
        for index in 0..10_000 {
            assert!(dedup.check_and_track(&format!("m{index}")));
        }
    }
}
