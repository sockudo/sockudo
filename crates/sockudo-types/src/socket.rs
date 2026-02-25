use rand::Rng;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Zero-copy SocketId using (u64, u64) for ultra-fast cloning.
/// Format: "high.low" for backward-compatible display/serialization.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct SocketId {
    pub high: u64,
    pub low: u64,
}

impl std::fmt::Display for SocketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.high, self.low)
    }
}

impl Serialize for SocketId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}.{}", self.high, self.low))
    }
}

impl<'de> Deserialize<'de> for SocketId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for SocketId {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() == 2
            && let (Ok(high), Ok(low)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
        {
            return Ok(SocketId { high, low });
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut hasher);
        let high = hasher.finish();

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.as_bytes().hash(&mut hasher);
        hasher.write_u8(0xFF);
        let low = hasher.finish();

        Ok(SocketId { high, low })
    }
}

impl Default for SocketId {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq<String> for SocketId {
    fn eq(&self, other: &String) -> bool {
        other
            .parse::<SocketId>()
            .is_ok_and(|parsed| parsed == *self)
    }
}

impl SocketId {
    pub fn new() -> Self {
        let mut rng = rand::rng();
        let max: u64 = 10_000_000_000;
        SocketId {
            high: rng.random_range(0..=max),
            low: rng.random_range(0..=max),
        }
    }

    /// Create from string for backward compatibility (e.g., parsing from API requests)
    pub fn from_string(s: &str) -> std::result::Result<Self, String> {
        s.parse()
    }
}
