//! Broadcast channel layout for the Redis Cluster transport.
//!
//! A single `#broadcast` channel hashes to one slot, so one shard primary fans
//! every broadcast out to every node. Bucketing writes each broadcast to one
//! channel per bucket, pinned to distinct slot ranges with hash tags, and each
//! node subscribes to exactly one bucket. Fan-out egress per shard drops by the
//! bucket count. `ClusterBroadcastMode` sequences the live migration.

use sockudo_core::error::{Error, Result};
use sockudo_core::options::{ClusterBroadcastMode, MAX_CLUSTER_BROADCAST_BUCKETS};

const SLOT_COUNT: u32 = 16_384;
const MAX_TAG_ATTEMPTS: u32 = 10_000;

/// CRC16/XMODEM, the hash Redis Cluster uses for key slots.
fn crc16_xmodem(bytes: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in bytes {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// Redis Cluster hash slot of `key`, honouring the first non-empty `{hash tag}`.
fn key_hash_slot(key: &[u8]) -> u16 {
    let hashed = key
        .iter()
        .position(|&b| b == b'{')
        .and_then(|open| {
            let rest = &key[open + 1..];
            rest.iter()
                .position(|&b| b == b'}')
                .filter(|&len| len > 0)
                .map(|len| &rest[..len])
        })
        .unwrap_or(key);
    (u32::from(crc16_xmodem(hashed)) % SLOT_COUNT) as u16
}

/// First tag `bc{index}`, `bc{index}.1`, … whose slot sits in the middle half
/// of bucket `index`'s `1/buckets` share of the slot space.
fn bucket_hash_tag(index: u16, buckets: u16) -> Option<String> {
    let (i, b) = (u32::from(index), u32::from(buckets));
    let range = SLOT_COUNT * (4 * i + 1) / (4 * b)..=SLOT_COUNT * (4 * i + 3) / (4 * b);
    (0..MAX_TAG_ATTEMPTS)
        .map(|attempt| match attempt {
            0 => format!("bc{index}"),
            n => format!("bc{index}.{n}"),
        })
        .find(|tag| range.contains(&u32::from(key_hash_slot(tag.as_bytes()))))
}

pub(crate) struct BroadcastTopology {
    legacy: String,
    buckets: Box<[String]>,
    publish: Box<[String]>,
    subscribe_bucket: bool,
}

impl BroadcastTopology {
    pub(crate) fn new(prefix: &str, mode: ClusterBroadcastMode, bucket_count: u16) -> Result<Self> {
        if bucket_count == 0 || bucket_count > MAX_CLUSTER_BROADCAST_BUCKETS {
            return Err(Error::Configuration(format!(
                "adapter.cluster.broadcast_buckets must be between 1 and {MAX_CLUSTER_BROADCAST_BUCKETS}, got {bucket_count}"
            )));
        }
        let legacy = format!("{prefix}:#broadcast");
        let buckets = (0..bucket_count)
            .map(|index| {
                bucket_hash_tag(index, bucket_count)
                    .map(|tag| format!("{prefix}:#broadcast:{{{tag}}}"))
            })
            .collect::<Option<Box<[String]>>>()
            .ok_or_else(|| {
                Error::Configuration(format!(
                    "no broadcast hash tag found for {bucket_count} buckets"
                ))
            })?;
        let publish: Box<[String]> = match mode {
            ClusterBroadcastMode::Legacy => Box::new([legacy.clone()]),
            ClusterBroadcastMode::PublishBoth | ClusterBroadcastMode::SubscribeBucket => {
                std::iter::once(legacy.clone())
                    .chain(buckets.iter().cloned())
                    .collect()
            }
            ClusterBroadcastMode::Bucketed => buckets.clone(),
        };
        let subscribe_bucket = matches!(
            mode,
            ClusterBroadcastMode::SubscribeBucket | ClusterBroadcastMode::Bucketed
        );
        Ok(Self {
            legacy,
            buckets,
            publish,
            subscribe_bucket,
        })
    }

    /// Channels every broadcast is written to, in publish order.
    pub(crate) fn publish_channels(&self) -> &[String] {
        &self.publish
    }

    /// Bucket a node reads; uniform across random UUID node ids.
    pub(crate) fn bucket_index(&self, node_id: &str) -> usize {
        usize::from(crc16_xmodem(node_id.as_bytes())) % self.buckets.len()
    }

    /// The single broadcast channel this node subscribes to.
    pub(crate) fn subscribe_channel(&self, node_id: &str) -> &str {
        if self.subscribe_bucket {
            &self.buckets[self.bucket_index(node_id)]
        } else {
            &self.legacy
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hash_slot_matches_redis_rs() {
        let keys = [
            "sockudo_adapter::#broadcast",
            "sockudo_adapter::#broadcast:{bc1.2}",
            "a{b}c",
            "{}x",
            "foo{}{bar}",
        ];
        for key in keys {
            assert_eq!(
                redis::cluster_routing::Slot::new(key_hash_slot(key.as_bytes())),
                Some(redis::cluster_routing::Slot::for_key(key)),
                "slot mismatch for {key}"
            );
        }
        assert_eq!(key_hash_slot(b"sockudo_adapter::#broadcast"), 7996);
        assert_eq!(key_hash_slot(b"sockudo_adapter::#requests"), 3625);
        assert_eq!(key_hash_slot(b"sockudo_adapter::#responses"), 11538);
    }

    #[test]
    fn three_bucket_channels_are_pinned() {
        let topology =
            BroadcastTopology::new("sockudo_adapter:", ClusterBroadcastMode::Bucketed, 3).unwrap();
        let expected = [
            "sockudo_adapter::#broadcast:{bc0.4}",
            "sockudo_adapter::#broadcast:{bc1.2}",
            "sockudo_adapter::#broadcast:{bc2.1}",
        ];
        assert_eq!(topology.publish_channels(), expected);
        for (channel, slot) in topology
            .publish_channels()
            .iter()
            .zip([2506u16, 7740, 14095])
        {
            assert_eq!(key_hash_slot(channel.as_bytes()), slot, "{channel}");
        }
    }

    #[test]
    fn every_bucket_count_has_distinct_middle_half_tags() {
        for buckets in 1..=MAX_CLUSTER_BROADCAST_BUCKETS {
            let topology =
                BroadcastTopology::new("p:", ClusterBroadcastMode::Bucketed, buckets).unwrap();
            let channels: Vec<&String> = topology.publish_channels().iter().collect();
            assert_eq!(channels.len(), usize::from(buckets));
            let unique: std::collections::HashSet<_> = channels.iter().collect();
            assert_eq!(
                unique.len(),
                channels.len(),
                "duplicate channel at B={buckets}"
            );
            for (index, channel) in channels.iter().enumerate() {
                let (i, b) = (index as u32, u32::from(buckets));
                let range = SLOT_COUNT * (4 * i + 1) / (4 * b)..=SLOT_COUNT * (4 * i + 3) / (4 * b);
                let slot = u32::from(key_hash_slot(channel.as_bytes()));
                assert!(
                    range.contains(&slot),
                    "B={buckets} bucket {index} channel {channel} slot {slot} outside {range:?}"
                );
            }
        }
    }

    #[test]
    fn mode_channel_sets() {
        let legacy = "p:#broadcast".to_string();
        let buckets = [
            "p:#broadcast:{bc0.4}".to_string(),
            "p:#broadcast:{bc1.2}".to_string(),
            "p:#broadcast:{bc2.1}".to_string(),
        ];
        let prefix = "p";
        let cases: [(ClusterBroadcastMode, Vec<String>, bool); 4] = [
            (ClusterBroadcastMode::Legacy, vec![legacy.clone()], false),
            (
                ClusterBroadcastMode::PublishBoth,
                std::iter::once(legacy.clone())
                    .chain(buckets.iter().cloned())
                    .collect(),
                false,
            ),
            (
                ClusterBroadcastMode::SubscribeBucket,
                std::iter::once(legacy.clone())
                    .chain(buckets.iter().cloned())
                    .collect(),
                true,
            ),
            (ClusterBroadcastMode::Bucketed, buckets.to_vec(), true),
        ];
        for (mode, expected, subscribes_bucket) in cases {
            let topology = BroadcastTopology::new(prefix, mode, 3).unwrap();
            assert_eq!(topology.publish_channels(), expected.as_slice(), "{mode}");
            let subscribed = topology.subscribe_channel("node-e");
            if subscribes_bucket {
                assert_eq!(subscribed, buckets[topology.bucket_index("node-e")]);
            } else {
                assert_eq!(subscribed, legacy);
            }
        }
    }

    #[test]
    fn node_bucket_assignment() {
        let topology = BroadcastTopology::new("p:", ClusterBroadcastMode::Bucketed, 3).unwrap();
        assert_eq!(topology.bucket_index("node-e"), 0);
        assert_eq!(topology.bucket_index("node-a"), 1);
        assert_eq!(topology.bucket_index("node-c"), 2);
        assert_eq!(
            topology.bucket_index("node-e"),
            topology.bucket_index("node-e")
        );
    }

    #[test]
    fn bucket_count_bounds() {
        for buckets in [0, 65] {
            let result = BroadcastTopology::new("p:", ClusterBroadcastMode::Bucketed, buckets);
            assert!(
                matches!(result, Err(Error::Configuration(_))),
                "expected configuration error for {buckets} buckets"
            );
        }
    }
}
