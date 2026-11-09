//! Delta compression (Protocol V2).
//!
//! The server keeps a base-message cache per `(event, conflation key)` with
//! its own sequence numbers and sends either a full message tagged with
//! `__delta_seq` or a `delta` event that patches a cached base. This module
//! mirrors that cache, reconstructs patched messages byte-for-byte, and asks
//! the server to resynchronize (`delta_sync_error`) whenever continuity
//! cannot be proven.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use serde::Deserialize;

use crate::error::{Error, Result};
use crate::event::{Payload, de_opt_u64};

pub(crate) mod fossil;

/// Delta algorithms understood by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeltaAlgorithm {
    /// Fossil delta (always available).
    Fossil,
    /// Xdelta3 / VCDIFF (requires the `xdelta3` feature).
    Xdelta3,
}

impl DeltaAlgorithm {
    /// Wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fossil => "fossil",
            Self::Xdelta3 => "xdelta3",
        }
    }

    /// Parse a wire name.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "fossil" => Some(Self::Fossil),
            "xdelta3" | "vcdiff" => Some(Self::Xdelta3),
            _ => None,
        }
    }

    /// Whether this build can decode the algorithm.
    pub const fn is_supported(self) -> bool {
        match self {
            Self::Fossil => true,
            Self::Xdelta3 => cfg!(feature = "xdelta3"),
        }
    }
}

/// Callback invoked with updated statistics.
pub type DeltaStatsCallback = Arc<dyn Fn(&DeltaStats) + Send + Sync>;

/// Connection-level delta compression options.
#[derive(Clone)]
pub struct DeltaOptions {
    /// Send `enable_delta_compression` automatically after connecting.
    pub enabled: bool,
    /// Algorithms to advertise, in preference order. Unsupported ones are
    /// dropped automatically.
    pub algorithms: Vec<DeltaAlgorithm>,
    /// Optional statistics callback, invoked after every delta-tracked message.
    pub on_stats: Option<DeltaStatsCallback>,
}

impl fmt::Debug for DeltaOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaOptions")
            .field("enabled", &self.enabled)
            .field("algorithms", &self.algorithms)
            .field("on_stats", &self.on_stats.is_some())
            .finish()
    }
}

impl Default for DeltaOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithms: vec![DeltaAlgorithm::Fossil, DeltaAlgorithm::Xdelta3],
            on_stats: None,
        }
    }
}

impl DeltaOptions {
    /// The advertised algorithms this build can actually decode.
    pub(crate) fn supported_algorithms(&self) -> Vec<&'static str> {
        let mut out = Vec::with_capacity(self.algorithms.len());
        for algorithm in &self.algorithms {
            if algorithm.is_supported() && !out.contains(&algorithm.as_str()) {
                out.push(algorithm.as_str());
            }
        }
        out
    }
}

/// Per-subscription delta negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChannelDeltaSettings {
    /// Enable (`Some(true)`), disable (`Some(false)`) or inherit (`None`).
    pub enabled: Option<bool>,
    /// Preferred algorithm, or the server default.
    pub algorithm: Option<DeltaAlgorithm>,
}

impl ChannelDeltaSettings {
    /// Enable delta compression for this subscription.
    pub fn enabled() -> Self {
        Self {
            enabled: Some(true),
            algorithm: None,
        }
    }

    /// Disable delta compression for this subscription.
    pub fn disabled() -> Self {
        Self {
            enabled: Some(false),
            algorithm: None,
        }
    }

    /// Enable with a specific algorithm.
    pub fn with_algorithm(algorithm: DeltaAlgorithm) -> Self {
        Self {
            enabled: Some(true),
            algorithm: Some(algorithm),
        }
    }

    /// The `delta` value of the subscribe frame, in the compact form the
    /// other SDKs send: a bare algorithm string, a bool, or an object.
    pub(crate) fn subscription_value(&self) -> sonic_rs::Value {
        use sonic_rs::Value;
        match (self.enabled, self.algorithm) {
            (None, Some(algorithm)) => Value::from(algorithm.as_str()),
            (Some(flag), None) => Value::from(flag),
            (None, None) => Value::from(true),
            (Some(flag), Some(algorithm)) => {
                sonic_rs::json!({"enabled": flag, "algorithm": algorithm.as_str()})
            }
        }
    }
}

/// Per-channel delta statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChannelDeltaStats {
    /// Channel name.
    pub channel: String,
    /// Conflation key path announced by the server, if any.
    pub conflation_key: Option<String>,
    /// Number of cached conflation groups.
    pub conflation_group_count: usize,
    /// Deltas applied.
    pub delta_count: u64,
    /// Full messages stored.
    pub full_message_count: u64,
}

/// Aggregate delta statistics.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct DeltaStats {
    /// Delta-tracked messages (full + delta).
    pub total_messages: u64,
    /// Messages reconstructed from deltas.
    pub delta_messages: u64,
    /// Full messages stored as bases.
    pub full_messages: u64,
    /// Bytes the messages would have taken uncompressed.
    pub total_bytes_without_compression: u64,
    /// Bytes actually received for those messages.
    pub total_bytes_with_compression: u64,
    /// Failed decodes (each triggers a resync).
    pub errors: u64,
    /// Channels with delta state.
    pub channel_count: usize,
    /// Per-channel breakdown.
    pub channels: Vec<ChannelDeltaStats>,
}

impl DeltaStats {
    /// Bytes saved by delta compression.
    pub fn bandwidth_saved(&self) -> u64 {
        self.total_bytes_without_compression
            .saturating_sub(self.total_bytes_with_compression)
    }

    /// Percentage of bytes saved.
    pub fn bandwidth_saved_percent(&self) -> f64 {
        if self.total_bytes_without_compression == 0 {
            return 0.0;
        }
        self.bandwidth_saved() as f64 / self.total_bytes_without_compression as f64 * 100.0
    }
}

/// Minimum cached bases per key; the server's full-message interval plus
/// in-flight deltas must fit.
const MIN_BASES_PER_KEY: usize = 30;
/// Bound on distinct conflation groups per channel, protecting memory
/// against a server (or publisher) that rotates keys without limit.
const MAX_GROUPS_PER_CHANNEL: usize = 4096;

type Ring = VecDeque<(u64, Bytes)>;

struct ChannelDelta {
    /// Bases keyed by `(event, conflation key)`, matching the server cache.
    bases: HashMap<(String, String), Ring, ahash::RandomState>,
    /// States announced by `delta_cache_sync`, keyed as the server sends them.
    synced: HashMap<String, Ring, ahash::RandomState>,
    /// Last base of any key, for servers that omit `base_index`.
    last: Option<(u64, Bytes)>,
    conflation_key: Option<String>,
    max_per_key: usize,
    delta_count: u64,
    full_count: u64,
}

impl Default for ChannelDelta {
    fn default() -> Self {
        Self {
            bases: HashMap::default(),
            synced: HashMap::default(),
            last: None,
            conflation_key: None,
            max_per_key: MIN_BASES_PER_KEY,
            delta_count: 0,
            full_count: 0,
        }
    }
}

impl ChannelDelta {
    fn store(&mut self, event: &str, conflation_key: &str, sequence: u64, content: Bytes) {
        let key = (event.to_owned(), conflation_key.to_owned());
        if !self.bases.contains_key(&key) && self.bases.len() >= MAX_GROUPS_PER_CHANNEL {
            // Evict an arbitrary group; a later delta against it resyncs.
            if let Some(evict) = self.bases.keys().next().cloned() {
                self.bases.remove(&evict);
            }
        }
        let ring = self.bases.entry(key).or_default();
        ring.push_back((sequence, content.clone()));
        while ring.len() > self.max_per_key {
            ring.pop_front();
        }
        self.last = Some((sequence, content));
    }

    fn find(&self, event: &str, conflation_key: &str, base_index: Option<u64>) -> Option<Bytes> {
        let pick = |ring: &Ring| -> Option<Bytes> {
            match base_index {
                Some(index) => ring
                    .iter()
                    .rev()
                    .find(|(sequence, _)| *sequence == index)
                    .map(|(_, content)| content.clone()),
                None => ring.back().map(|(_, content)| content.clone()),
            }
        };
        let key = (event.to_owned(), conflation_key.to_owned());
        if let Some(found) = self.bases.get(&key).and_then(pick) {
            return Some(found);
        }
        // Cache-sync states are keyed by conflation value (or event name when
        // the channel has no conflation key).
        let sync_key = if conflation_key.is_empty() {
            event
        } else {
            conflation_key
        };
        if let Some(found) = self.synced.get(sync_key).and_then(pick) {
            return Some(found);
        }
        // Legacy servers that omit `base_index` keep a single base per
        // channel. With an index, sequences are per key, so a channel-wide
        // match could be another key's message: fail closed instead.
        match (base_index, &self.last) {
            (None, Some((_, content))) => Some(content.clone()),
            _ => None,
        }
    }
}

/// Wire shape of a `delta` event payload.
#[derive(Deserialize)]
struct DeltaFrame {
    event: String,
    delta: String,
    #[serde(default, deserialize_with = "de_opt_u64")]
    seq: Option<u64>,
    #[serde(default)]
    algorithm: Option<String>,
    #[serde(default)]
    conflation_key: Option<String>,
    #[serde(default, deserialize_with = "de_opt_u64")]
    base_index: Option<u64>,
}

#[derive(Deserialize)]
struct CacheSync {
    #[serde(default)]
    conflation_key: Option<String>,
    #[serde(default)]
    max_messages_per_key: Option<usize>,
    #[serde(default)]
    states: HashMap<String, Vec<CacheSyncEntry>>,
}

#[derive(Deserialize)]
struct CacheSyncEntry {
    content: String,
    #[serde(default, deserialize_with = "de_opt_u64")]
    seq: Option<u64>,
}

/// A reconstructed message, ready to be decoded as a full JSON frame.
#[derive(Debug)]
pub(crate) struct Reconstructed {
    pub event: String,
    pub bytes: Bytes,
}

/// Connection-scoped delta state.
pub(crate) struct DeltaManager {
    options: DeltaOptions,
    default_algorithm: DeltaAlgorithm,
    server_enabled: bool,
    channels: HashMap<String, ChannelDelta, ahash::RandomState>,
    stats: DeltaStats,
    max_output: usize,
}

impl fmt::Debug for DeltaManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeltaManager")
            .field("server_enabled", &self.server_enabled)
            .field("channels", &self.channels.len())
            .finish()
    }
}

impl DeltaManager {
    pub(crate) fn new(options: DeltaOptions, max_output: usize) -> Self {
        Self {
            options,
            default_algorithm: DeltaAlgorithm::Fossil,
            server_enabled: false,
            channels: HashMap::default(),
            stats: DeltaStats::default(),
            max_output,
        }
    }

    pub(crate) fn auto_enable(&self) -> bool {
        self.options.enabled
    }

    /// The `enable_delta_compression` payload, or `None` when no advertised
    /// algorithm is decodable.
    pub(crate) fn enable_payload(&self) -> Option<sonic_rs::Value> {
        let algorithms = self.options.supported_algorithms();
        (!algorithms.is_empty()).then(|| sonic_rs::json!({ "algorithms": algorithms }))
    }

    pub(crate) fn handle_enabled(&mut self, payload: &Payload) {
        let value = payload.value();
        use sonic_rs::JsonValueTrait;
        self.server_enabled = value
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if let Some(algorithm) = value
            .get("algorithm")
            .and_then(|v| v.as_str())
            .and_then(DeltaAlgorithm::parse)
        {
            self.default_algorithm = algorithm;
        }
    }

    pub(crate) fn is_server_enabled(&self) -> bool {
        self.server_enabled
    }

    pub(crate) fn handle_cache_sync(&mut self, channel: &str, payload: &Payload) -> Result<()> {
        let sync: CacheSync = payload.json()?;
        let state = self.channels.entry(channel.to_owned()).or_default();
        state.conflation_key = sync.conflation_key;
        state.max_per_key = sync
            .max_messages_per_key
            .unwrap_or(10)
            .clamp(MIN_BASES_PER_KEY, 1024);
        state.synced.clear();
        for (key, entries) in sync.states.into_iter().take(MAX_GROUPS_PER_CHANNEL) {
            let ring: Ring = entries
                .into_iter()
                .filter_map(|entry| Some((entry.seq?, Bytes::from(entry.content))))
                .collect();
            state.synced.insert(key, ring);
        }
        Ok(())
    }

    /// Record a full message carrying `__delta_seq` as a future base.
    pub(crate) fn handle_full_message(
        &mut self,
        channel: &str,
        event: &str,
        conflation_key: Option<&str>,
        sequence: u64,
        raw: &[u8],
    ) {
        let base = Bytes::from(strip_delta_markers(raw));
        let size = base.len() as u64;
        let state = self.channels.entry(channel.to_owned()).or_default();
        if conflation_key.is_some() && state.conflation_key.is_none() {
            state.conflation_key = Some(String::new());
        }
        state.store(event, conflation_key.unwrap_or_default(), sequence, base);
        state.full_count += 1;
        self.stats.total_messages += 1;
        self.stats.full_messages += 1;
        self.stats.total_bytes_without_compression += size;
        self.stats.total_bytes_with_compression += size;
        self.emit_stats();
    }

    /// Reconstruct a `delta` event. On error the caller must request a resync
    /// via [`DeltaManager::resync`].
    pub(crate) fn handle_delta(
        &mut self,
        channel: &str,
        payload: &Payload,
    ) -> Result<Reconstructed> {
        let result = self.apply_delta(channel, payload);
        if result.is_err() {
            self.stats.errors += 1;
            self.emit_stats();
        }
        result
    }

    fn apply_delta(&mut self, channel: &str, payload: &Payload) -> Result<Reconstructed> {
        let frame: DeltaFrame = payload.json()?;
        let algorithm = match frame.algorithm.as_deref() {
            Some(name) => DeltaAlgorithm::parse(name)
                .ok_or_else(|| Error::Delta(format!("unknown algorithm '{name}'")))?,
            None => self.default_algorithm,
        };
        let conflation_key = frame.conflation_key.as_deref().unwrap_or_default();
        let state = self
            .channels
            .get_mut(channel)
            .ok_or_else(|| Error::Delta(format!("no delta state for channel '{channel}'")))?;
        let base = state
            .find(&frame.event, conflation_key, frame.base_index)
            .ok_or_else(|| {
                Error::Delta(format!(
                    "no base message for channel '{channel}' event '{}' key '{conflation_key}' index {:?}",
                    frame.event, frame.base_index
                ))
            })?;
        let delta = base64::engine::general_purpose::STANDARD
            .decode(frame.delta.as_bytes())
            .map_err(|error| Error::Delta(format!("invalid base64: {error}")))?;
        let output = decode_with(algorithm, &base, &delta, self.max_output)?;
        let output = Bytes::from(output);
        let sequence = frame
            .seq
            .unwrap_or_else(|| state.last.as_ref().map_or(0, |(s, _)| s + 1));
        state.store(&frame.event, conflation_key, sequence, output.clone());
        state.delta_count += 1;
        self.stats.total_messages += 1;
        self.stats.delta_messages += 1;
        self.stats.total_bytes_without_compression += output.len() as u64;
        self.stats.total_bytes_with_compression += delta.len() as u64;
        self.emit_stats();
        Ok(Reconstructed {
            event: frame.event,
            bytes: output,
        })
    }

    /// Drop a channel's state so the server's next message re-seeds it.
    pub(crate) fn resync(&mut self, channel: &str) {
        self.channels.remove(channel);
    }

    pub(crate) fn clear_channel(&mut self, channel: &str) {
        self.channels.remove(channel);
    }

    pub(crate) fn clear_all(&mut self) {
        self.channels.clear();
        self.server_enabled = false;
    }

    pub(crate) fn stats(&self) -> DeltaStats {
        let mut stats = self.stats.clone();
        stats.channel_count = self.channels.len();
        stats.channels = self
            .channels
            .iter()
            .map(|(name, state)| ChannelDeltaStats {
                channel: name.clone(),
                conflation_key: state.conflation_key.clone(),
                conflation_group_count: state.bases.len().max(state.synced.len()),
                delta_count: state.delta_count,
                full_message_count: state.full_count,
            })
            .collect();
        stats.channels.sort_by(|a, b| a.channel.cmp(&b.channel));
        stats
    }

    pub(crate) fn reset_stats(&mut self) {
        self.stats = DeltaStats::default();
        for state in self.channels.values_mut() {
            state.delta_count = 0;
            state.full_count = 0;
        }
    }

    fn emit_stats(&self) {
        if let Some(callback) = self.options.on_stats.as_ref() {
            callback(&self.stats());
        }
    }
}

fn decode_with(
    algorithm: DeltaAlgorithm,
    base: &[u8],
    delta: &[u8],
    max_output: usize,
) -> Result<Vec<u8>> {
    match algorithm {
        DeltaAlgorithm::Fossil => fossil::apply(base, delta, max_output),
        DeltaAlgorithm::Xdelta3 => decode_vcdiff(base, delta, max_output),
    }
}

#[cfg(feature = "xdelta3")]
fn decode_vcdiff(base: &[u8], delta: &[u8], max_output: usize) -> Result<Vec<u8>> {
    let output = oxidelta::compress::decoder::decode_all(base, delta)
        .map_err(|error| Error::Delta(format!("vcdiff: {error}")))?;
    if output.len() > max_output {
        return Err(Error::Delta(
            "vcdiff output exceeds the maximum message size".to_owned(),
        ));
    }
    Ok(output)
}

#[cfg(not(feature = "xdelta3"))]
fn decode_vcdiff(_base: &[u8], _delta: &[u8], _max_output: usize) -> Result<Vec<u8>> {
    Err(Error::unsupported(
        "xdelta3 deltas require the `xdelta3` feature",
    ))
}

const SEQ_MARKER: &[u8] = b"\"__delta_seq\":";
const KEY_MARKER: &[u8] = b"\"__conflation_key\":";

/// Remove the `__delta_seq` / `__conflation_key` markers the server appends to
/// full messages, preserving every other byte. The server stores exactly this
/// stripped form as the delta base, so byte fidelity matters: re-serializing
/// would change float formatting and key order.
pub(crate) fn strip_delta_markers(raw: &[u8]) -> Vec<u8> {
    let mut out = raw.to_vec();
    for marker in [SEQ_MARKER, KEY_MARKER] {
        if let Some(stripped) = remove_top_level_field(&out, marker) {
            out = stripped;
        }
    }
    out
}

/// Remove the last occurrence of `"key":value` together with its separating
/// comma. Top-level markers are serialized last, and occurrences inside JSON
/// strings are escaped (`\"key\"`), so the last raw match is the marker.
fn remove_top_level_field(raw: &[u8], marker: &[u8]) -> Option<Vec<u8>> {
    let start = memchr::memmem::rfind(raw, marker)?;
    let value_start = start + marker.len();
    let value_end = scan_json_scalar(raw, value_start)?;
    let (cut_start, cut_end) = if start > 0 && raw[start - 1] == b',' {
        (start - 1, value_end)
    } else if raw.get(value_end) == Some(&b',') {
        (start, value_end + 1)
    } else {
        (start, value_end)
    };
    let mut out = Vec::with_capacity(raw.len() - (cut_end - cut_start));
    out.extend_from_slice(&raw[..cut_start]);
    out.extend_from_slice(&raw[cut_end..]);
    Some(out)
}

/// End index of a JSON number, string, `null`, `true` or `false` starting at
/// `start`.
fn scan_json_scalar(raw: &[u8], start: usize) -> Option<usize> {
    let first = *raw.get(start)?;
    match first {
        b'"' => {
            let mut index = start + 1;
            while index < raw.len() {
                match raw[index] {
                    b'\\' => index += 2,
                    b'"' => return Some(index + 1),
                    _ => index += 1,
                }
            }
            None
        }
        b'-' | b'0'..=b'9' => {
            let mut index = start + 1;
            while index < raw.len()
                && matches!(raw[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
            {
                index += 1;
            }
            Some(index)
        }
        b'n' if raw[start..].starts_with(b"null") => Some(start + 4),
        b't' if raw[start..].starts_with(b"true") => Some(start + 4),
        b'f' if raw[start..].starts_with(b"false") => Some(start + 5),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_trailing_markers_exactly() {
        let base = br#"{"event":"price","channel":"btc","data":"{\"p\":1.10}"}"#;
        let full = br#"{"event":"price","channel":"btc","data":"{\"p\":1.10}","__delta_seq":12,"__conflation_key":"BTC"}"#;
        assert_eq!(strip_delta_markers(full), base);
        let seq_only = br#"{"event":"price","data":"x","__delta_seq":0}"#;
        assert_eq!(
            strip_delta_markers(seq_only),
            br#"{"event":"price","data":"x"}"#
        );
    }

    #[test]
    fn strips_leading_markers_and_escaped_keys() {
        let leading = br#"{"__delta_seq":3,"__conflation_key":"a\"b","event":"e"}"#;
        assert_eq!(strip_delta_markers(leading), br#"{"event":"e"}"#);
        // Escaped occurrences inside the data string are untouched.
        let nested = br#"{"data":"{\"__delta_seq\":9}","__delta_seq":4}"#;
        assert_eq!(
            strip_delta_markers(nested),
            br#"{"data":"{\"__delta_seq\":9}"}"#
        );
    }

    #[test]
    fn leaves_messages_without_markers_alone() {
        let raw = br#"{"event":"e","data":"d"}"#;
        assert_eq!(strip_delta_markers(raw), raw);
    }

    fn server_fossil(old: &[u8], new: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(fossil_delta::delta(new, old))
    }

    #[test]
    fn reconstructs_chains_and_tracks_stats() {
        let mut manager = DeltaManager::new(DeltaOptions::default(), 1 << 20);
        let m0 = br#"{"event":"tick","channel":"c","data":"{\"v\":0}"}"#;
        let full = br#"{"event":"tick","channel":"c","data":"{\"v\":0}","__delta_seq":0}"#;
        manager.handle_full_message("c", "tick", None, 0, full);
        let m1 = br#"{"event":"tick","channel":"c","data":"{\"v\":1}"}"#;
        let payload = Payload::Json(format!(
            r#"{{"event":"tick","delta":"{}","seq":1,"algorithm":"fossil","base_index":0}}"#,
            server_fossil(m0, m1)
        ));
        let out = manager.handle_delta("c", &payload).unwrap();
        assert_eq!(out.event, "tick");
        assert_eq!(&out.bytes[..], m1);
        // A delta against the reconstructed message also applies.
        let m2 = br#"{"event":"tick","channel":"c","data":"{\"v\":2}"}"#;
        let payload = Payload::Text(format!(
            r#"{{"event":"tick","delta":"{}","seq":2,"base_index":1}}"#,
            server_fossil(m1, m2)
        ));
        assert_eq!(&manager.handle_delta("c", &payload).unwrap().bytes[..], m2);

        let stats = manager.stats();
        assert_eq!(stats.full_messages, 1);
        assert_eq!(stats.delta_messages, 2);
        assert_eq!(stats.total_messages, 3);
        assert_eq!(stats.channel_count, 1);
        assert!(stats.total_bytes_with_compression < stats.total_bytes_without_compression);
    }

    #[test]
    fn keys_bases_by_event_and_conflation_key() {
        let mut manager = DeltaManager::new(DeltaOptions::default(), 1 << 20);
        let a = br#"{"event":"a","data":"{\"k\":\"x\",\"v\":1}"}"#;
        let b = br#"{"event":"b","data":"{\"k\":\"y\",\"v\":1}"}"#;
        // Both events use sequence 0 on the server (sequences are per key).
        manager.handle_full_message("c", "a", Some("x"), 0, a);
        manager.handle_full_message("c", "b", Some("y"), 0, b);
        let a1 = br#"{"event":"a","data":"{\"k\":\"x\",\"v\":2}"}"#;
        let payload = Payload::Json(format!(
            r#"{{"event":"a","delta":"{}","seq":1,"conflation_key":"x","base_index":0}}"#,
            server_fossil(a, a1)
        ));
        assert_eq!(&manager.handle_delta("c", &payload).unwrap().bytes[..], a1);
    }

    #[test]
    fn uses_cache_sync_states() {
        let mut manager = DeltaManager::new(DeltaOptions::default(), 1 << 20);
        let base = r#"{"event":"q","data":"{\"v\":5}"}"#;
        let sync = Payload::Text(
            sonic_rs::to_string(&sonic_rs::json!({
                "conflation_key": "symbol",
                "max_messages_per_key": 10,
                "states": {"BTC": [{"content": base, "seq": 7}]}
            }))
            .unwrap(),
        );
        manager.handle_cache_sync("c", &sync).unwrap();
        let next = br#"{"event":"q","data":"{\"v\":6}"}"#;
        let payload = Payload::Json(format!(
            r#"{{"event":"q","delta":"{}","seq":8,"conflation_key":"BTC","base_index":7}}"#,
            server_fossil(base.as_bytes(), next)
        ));
        assert_eq!(
            &manager.handle_delta("c", &payload).unwrap().bytes[..],
            next
        );
        assert_eq!(
            manager.stats().channels[0].conflation_key.as_deref(),
            Some("symbol")
        );
    }

    #[test]
    fn missing_base_is_an_error_and_counts() {
        let mut manager = DeltaManager::new(DeltaOptions::default(), 1 << 20);
        let payload =
            Payload::Json(r#"{"event":"e","delta":"AA==","seq":1,"base_index":0}"#.into());
        assert!(manager.handle_delta("c", &payload).is_err());
        manager.handle_full_message("c", "e", None, 5, br#"{"event":"e"}"#);
        // base_index 0 is not cached: fail closed rather than guess.
        assert!(manager.handle_delta("c", &payload).is_err());
        assert_eq!(manager.stats().errors, 2);
        manager.reset_stats();
        assert_eq!(manager.stats().errors, 0);
    }

    #[cfg(feature = "xdelta3")]
    #[test]
    fn applies_server_vcdiff() {
        let old = br#"{"event":"x","data":"{\"a\":\"hello world\"}"}"#;
        let new = br#"{"event":"x","data":"{\"a\":\"hello there world\"}"}"#;
        let mut delta = Vec::new();
        oxidelta::compress::encoder::encode_all(
            &mut delta,
            old,
            new,
            oxidelta::compress::encoder::CompressOptions {
                checksum: false,
                ..Default::default()
            },
        )
        .unwrap();
        let mut manager = DeltaManager::new(DeltaOptions::default(), 1 << 20);
        manager.handle_full_message("c", "x", None, 0, old);
        let payload = Payload::Json(format!(
            r#"{{"event":"x","delta":"{}","seq":1,"algorithm":"xdelta3","base_index":0}}"#,
            base64::engine::general_purpose::STANDARD.encode(&delta)
        ));
        assert_eq!(&manager.handle_delta("c", &payload).unwrap().bytes[..], new);
    }

    #[test]
    fn subscription_values_match_other_sdks() {
        assert_eq!(
            ChannelDeltaSettings::enabled().subscription_value(),
            sonic_rs::Value::from(true)
        );
        assert_eq!(
            ChannelDeltaSettings::disabled().subscription_value(),
            sonic_rs::Value::from(false)
        );
        let algo_only = ChannelDeltaSettings {
            enabled: None,
            algorithm: Some(DeltaAlgorithm::Xdelta3),
        };
        assert_eq!(
            algo_only.subscription_value(),
            sonic_rs::Value::from("xdelta3")
        );
        assert_eq!(
            ChannelDeltaSettings::with_algorithm(DeltaAlgorithm::Fossil).subscription_value(),
            sonic_rs::json!({"enabled": true, "algorithm": "fossil"})
        );
    }
}
