//! Bounded message deduplication by `(channel, message_id)`.
//!
//! A multi-channel publish keeps one `message_id` on every target channel, so
//! the channel is part of the key: only a repeat on the same channel is a
//! duplicate.

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

    /// Returns `true` when `id` was already seen on `channel`; otherwise
    /// records it.
    pub(crate) fn check_and_track(&mut self, channel: Option<&str>, id: &str) -> bool {
        let key = self.keys.hash_one((channel, id));
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
    fn same_id_on_other_channels_is_not_a_duplicate() {
        let mut dedup = Deduplicator::new(8);
        assert!(!dedup.check_and_track(Some("a"), "m1"));
        assert!(!dedup.check_and_track(Some("b"), "m1"));
        assert!(dedup.check_and_track(Some("a"), "m1"));
        assert!(dedup.check_and_track(Some("b"), "m1"));
    }

    #[test]
    fn detects_repeats_and_evicts_oldest() {
        let mut dedup = Deduplicator::new(3);
        assert!(!dedup.check_and_track(None, "a"));
        assert!(dedup.check_and_track(None, "a"));
        assert!(!dedup.check_and_track(None, "b"));
        assert!(!dedup.check_and_track(None, "c"));
        assert!(!dedup.check_and_track(None, "d")); // evicts "a"
        assert_eq!(dedup.len(), 3);
        assert!(!dedup.check_and_track(None, "a"));
        assert!(dedup.check_and_track(None, "d"));
    }

    #[test]
    fn handles_large_windows() {
        let mut dedup = Deduplicator::new(10_000);
        for index in 0..10_000 {
            assert!(!dedup.check_and_track(None, &format!("m{index}")));
        }
        for index in 0..10_000 {
            assert!(dedup.check_and_track(None, &format!("m{index}")));
        }
    }
}
