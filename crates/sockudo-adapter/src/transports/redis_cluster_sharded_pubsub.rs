//! Slot-aware, per-shard Pub/Sub for Redis Cluster.
//!
//! `redis-rs` does not expose sharded Pub/Sub subscriptions on its cluster
//! client. This module works around that by discovering the cluster topology,
//! grouping channels by hash slot, and opening a direct (non-cluster)
//! connection to each shard master. Inspired by `go-redis` and `fred.rs`.

use crossfire::mpsc;
use redis::cluster_routing::Slot;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const TOPOLOGY_REFRESH_INTERVAL_SECS: u64 = 30;
const TOPOLOGY_REFRESH_FAILURE_THRESHOLD: u64 = 3;

type ShardedPushChannelFlavor = mpsc::Array<redis::PushInfo>;
type ShardedPushSender = crossfire::MAsyncTx<ShardedPushChannelFlavor>;

/// Receiver end of the bounded fan-in channel returned by `ShardedSubscriber::start`.
pub type ShardedPushReceiver = crossfire::AsyncRx<ShardedPushChannelFlavor>;

#[derive(Debug, Clone, Copy)]
enum PubSubMode {
    Standard,
    Sharded,
}

impl PubSubMode {
    fn subscribe_command(self) -> &'static str {
        match self {
            Self::Standard => "SUBSCRIBE",
            Self::Sharded => "SSUBSCRIBE",
        }
    }

    fn metrics_transport(self) -> &'static str {
        match self {
            Self::Standard => "redis_cluster",
            Self::Sharded => "redis_cluster_sharded",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Sharded => "sharded",
        }
    }
}

/// Address of a single Redis Cluster shard master.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct NodeAddr {
    pub host: String,
    pub port: u16,
}

impl NodeAddr {
    fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    pub(crate) fn to_url(&self, scheme: &str, password: Option<&str>) -> String {
        match password {
            Some(pw) => format!(
                "{}://:{}@{}:{}/?protocol=resp3",
                scheme, pw, self.host, self.port
            ),
            None => format!("{}://{}:{}/?protocol=resp3", scheme, self.host, self.port),
        }
    }
}

/// Snapshot of Redis Cluster topology: hash slot to owning shard master.
pub(crate) struct Topology {
    slot_owners: HashMap<Slot, NodeAddr>,
    fallback: NodeAddr,
    #[allow(dead_code)]
    masters: Vec<NodeAddr>,
}

impl Topology {
    /// Query any of the `seed_urls` and return a parsed cluster topology.
    ///
    /// Tries each seed in turn; returns the first success.
    ///
    /// # Errors
    ///
    /// Returns an error if all seeds fail or the response cannot be parsed.
    pub(crate) async fn discover(seed_urls: &[String]) -> Result<Self> {
        let mut last_err: Option<Error> = None;
        for url in seed_urls {
            match Self::discover_one(url).await {
                Ok(topo) => return Ok(topo),
                Err(e) => {
                    warn!("Topology::discover: seed {url} failed: {e}");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err
            .unwrap_or_else(|| Error::Redis("Topology::discover: no seed URLs provided".into())))
    }

    async fn discover_one(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)
            .map_err(|e| Error::Redis(format!("Topology: failed to open client for {url}: {e}")))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Redis(format!("Topology: connect to {url} failed: {e}")))?;

        let shards_result: redis::RedisResult<redis::Value> = redis::cmd("CLUSTER")
            .arg("SHARDS")
            .query_async(&mut conn)
            .await;

        match shards_result {
            Ok(raw) => parse_cluster_shards(raw),
            Err(_) => {
                let raw: redis::Value = redis::cmd("CLUSTER")
                    .arg("SLOTS")
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| Error::Redis(format!("CLUSTER SLOTS on {url} failed: {e}")))?;
                parse_cluster_slots(raw)
            }
        }
    }

    pub(crate) fn shard_for(&self, channel: &str) -> &NodeAddr {
        let slot = Slot::for_key(channel);
        self.slot_owners.get(&slot).unwrap_or(&self.fallback)
    }
}

fn parse_cluster_shards(raw: redis::Value) -> Result<Topology> {
    let shards = match raw {
        redis::Value::Array(arr) => arr,
        other => {
            return Err(Error::Redis(format!(
                "CLUSTER SHARDS: unexpected top-level type: {other:?}"
            )));
        }
    };

    let mut slot_owners: HashMap<Slot, NodeAddr> = HashMap::new();
    let mut masters: Vec<NodeAddr> = Vec::new();

    for shard_val in shards {
        let shard_arr = match shard_val {
            redis::Value::Array(a) => a,
            _ => continue,
        };

        let mut slot_ranges: Vec<(u16, u16)> = Vec::new();
        let mut master_node: Option<NodeAddr> = None;

        let mut i = 0;
        while i + 1 < shard_arr.len() {
            let key = value_to_string(&shard_arr[i]);
            match key.as_deref() {
                Some("slots") => {
                    if let redis::Value::Array(slot_vals) = &shard_arr[i + 1] {
                        let nums: Vec<u16> = slot_vals
                            .iter()
                            .filter_map(|v| match v {
                                redis::Value::Int(n) => Some(*n as u16),
                                _ => None,
                            })
                            .collect();
                        for chunk in nums.chunks(2) {
                            if let [start, end] = *chunk {
                                slot_ranges.push((start, end));
                            }
                        }
                    }
                }
                Some("nodes") => {
                    if let redis::Value::Array(nodes) = &shard_arr[i + 1] {
                        for node_val in nodes {
                            if let redis::Value::Array(fields) = node_val
                                && let Some((addr, role)) = parse_node_entry(fields)
                                && (role == "master" || role == "primary")
                            {
                                master_node = Some(addr);
                            }
                        }
                    }
                }
                _ => {}
            }
            i += 2;
        }

        if let Some(master) = master_node {
            for (start, end) in slot_ranges {
                for n in start..=end {
                    if let Some(slot) = Slot::new(n) {
                        slot_owners.insert(slot, master.clone());
                    }
                }
            }
            if !masters.contains(&master) {
                masters.push(master);
            }
        }
    }

    if masters.is_empty() {
        return Err(Error::Redis(
            "CLUSTER SHARDS: no master nodes found in response".into(),
        ));
    }

    let fallback = masters[0].clone();

    Ok(Topology {
        slot_owners,
        fallback,
        masters,
    })
}

fn parse_node_entry(fields: &[redis::Value]) -> Option<(NodeAddr, String)> {
    let mut host: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut role: Option<String> = None;

    let mut i = 0;
    while i + 1 < fields.len() {
        let key = value_to_string(&fields[i]);
        match key.as_deref() {
            // Prefer "endpoint" (ElastiCache/Valkey) over "ip" (OSS Redis).
            Some("endpoint") => host = value_to_string(&fields[i + 1]),
            Some("ip") if host.is_none() => host = value_to_string(&fields[i + 1]),
            Some("tls-port") => {
                if let redis::Value::Int(n) = &fields[i + 1]
                    && *n > 0
                {
                    port = Some(*n as u16);
                }
            }
            Some("port") if port.is_none() => {
                if let redis::Value::Int(n) = &fields[i + 1] {
                    port = Some(*n as u16);
                }
            }
            Some("role") => role = value_to_string(&fields[i + 1]),
            _ => {}
        }
        i += 2;
    }

    Some((NodeAddr::new(host?, port?), role?))
}

fn parse_cluster_slots(raw: redis::Value) -> Result<Topology> {
    let entries = match raw {
        redis::Value::Array(arr) => arr,
        other => {
            return Err(Error::Redis(format!(
                "CLUSTER SLOTS: unexpected top-level type: {other:?}"
            )));
        }
    };

    let mut slot_owners: HashMap<Slot, NodeAddr> = HashMap::new();
    let mut masters: Vec<NodeAddr> = Vec::new();

    for entry_val in entries {
        let entry = match entry_val {
            redis::Value::Array(a) => a,
            _ => continue,
        };
        if entry.len() < 3 {
            continue;
        }

        let start = match &entry[0] {
            redis::Value::Int(n) => *n as u16,
            _ => continue,
        };
        let end = match &entry[1] {
            redis::Value::Int(n) => *n as u16,
            _ => continue,
        };

        // entry[2] = master: [ip, port, node-id]
        let master_addr = {
            let node = match &entry[2] {
                redis::Value::Array(a) => a,
                _ => continue,
            };
            if node.len() < 2 {
                continue;
            }
            let ip = match &node[0] {
                redis::Value::BulkString(b) => match std::str::from_utf8(b) {
                    Ok(s) => s.to_owned(),
                    Err(_) => continue,
                },
                redis::Value::SimpleString(s) => s.clone(),
                _ => continue,
            };
            let port = match &node[1] {
                redis::Value::Int(n) => *n as u16,
                _ => continue,
            };
            NodeAddr::new(ip, port)
        };

        for n in start..=end {
            if let Some(slot) = Slot::new(n) {
                slot_owners.insert(slot, master_addr.clone());
            }
        }
        if !masters.contains(&master_addr) {
            masters.push(master_addr);
        }
    }

    if masters.is_empty() {
        return Err(Error::Redis(
            "CLUSTER SLOTS: no master nodes found in response".into(),
        ));
    }

    let fallback = masters[0].clone();

    Ok(Topology {
        slot_owners,
        fallback,
        masters,
    })
}

fn value_to_string(v: &redis::Value) -> Option<String> {
    match v {
        redis::Value::BulkString(b) => std::str::from_utf8(b).ok().map(str::to_owned),
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::VerbatimString { format: _, text } => Some(text.clone()),
        _ => None,
    }
}

pub(crate) struct ShardListenerParams {
    url: String,
    channels: Vec<String>,
    mode: PubSubMode,
    fan_in_tx: ShardedPushSender,
    is_running: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
    shard_addr: String,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    refresh_notify: Arc<Notify>,
}

/// Long-running task for one shard master.
///
/// Connects via a non-cluster `redis::Client`, issues the configured
/// subscription command for each assigned channel, and forwards Pub/Sub pushes
/// to the bounded fan-in sender. Reconnects with exponential back-off
/// (500 ms to 10 s) on any failure.
pub(crate) async fn shard_listener_loop(params: ShardListenerParams) {
    // Unbounded because push_sender must not block the redis runtime;
    // backpressure is applied at the bounded fan-in boundary.
    type InternalFlavor = mpsc::List<redis::PushInfo>;

    let mut retry_delay = 500u64;
    const MAX_RETRY_DELAY: u64 = 10_000;
    let mut reconnection_count = 0u64;
    let mut consecutive_failures: u64 = 0;

    'outer: loop {
        if !params.is_running.load(Ordering::Relaxed) {
            break;
        }

        let (internal_tx, internal_rx): (
            crossfire::MTx<InternalFlavor>,
            crossfire::AsyncRx<InternalFlavor>,
        ) = mpsc::unbounded_async();

        let push_sender = {
            let tx = internal_tx;
            move |msg: redis::PushInfo| -> std::result::Result<(), redis::aio::SendError> {
                tx.send(msg).map_err(|_| redis::aio::SendError)
            }
        };

        let cfg = redis::AsyncConnectionConfig::new().set_push_sender(push_sender);

        let client = match redis::Client::open(params.url.as_str()) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "shard_listener[{}]: failed to open client: {e}, retry in {retry_delay}ms",
                    params.shard_addr
                );
                tokio::select! {
                    _ = params.shutdown.notified() => break 'outer,
                    _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
                }
                retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
                continue;
            }
        };

        let mut conn = match client
            .get_multiplexed_async_connection_with_config(&cfg)
            .await
        {
            Ok(c) => {
                if reconnection_count > 0 {
                    info!(
                        "shard_listener[{}]: reconnected after {} attempt(s)",
                        params.shard_addr, reconnection_count
                    );
                }
                retry_delay = 500;
                consecutive_failures = 0;
                c
            }
            Err(e) => {
                reconnection_count += 1;
                warn!(
                    "shard_listener[{}]: connect failed (attempt {}): {e}, retry in {retry_delay}ms",
                    params.shard_addr, reconnection_count
                );
                if let Some(m) = params.metrics.get() {
                    m.mark_horizontal_transport_reconnection(params.mode.metrics_transport());
                }
                consecutive_failures += 1;
                if consecutive_failures >= TOPOLOGY_REFRESH_FAILURE_THRESHOLD {
                    params.refresh_notify.notify_one();
                    consecutive_failures = 0;
                }
                tokio::select! {
                    _ = params.shutdown.notified() => break 'outer,
                    _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
                }
                retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
                continue;
            }
        };

        // One subscription command per channel keeps cluster routing explicit
        // and avoids CROSSSLOT errors on multi-slot batches.
        let subscribe_command = params.mode.subscribe_command();
        let mut subscribe_ok = true;
        for ch in &params.channels {
            if let Err(e) = redis::cmd(subscribe_command)
                .arg(ch.as_str())
                .exec_async(&mut conn)
                .await
            {
                warn!(
                    "shard_listener[{}]: {} failed for {ch}: {e}",
                    params.shard_addr, subscribe_command
                );
                subscribe_ok = false;
                break;
            }
        }

        if !subscribe_ok {
            if let Some(m) = params.metrics.get() {
                m.mark_horizontal_transport_reconnection(params.mode.metrics_transport());
            }
            tokio::select! {
                _ = params.shutdown.notified() => break 'outer,
                _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
            }
            retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
            continue;
        }

        info!(
            "shard_listener[{}]: {} subscribed to {} channel(s)",
            params.shard_addr,
            params.mode.label(),
            params.channels.len()
        );
        reconnection_count = 0;

        loop {
            let recv = tokio::select! {
                _ = params.shutdown.notified() => break 'outer,
                msg = internal_rx.recv() => msg,
            };

            let push_info = match recv {
                Err(_) => break, // internal channel closed = redis connection dropped
                Ok(p) => p,
            };

            match push_info.kind {
                redis::PushKind::Disconnection => {
                    warn!(
                        "shard_listener[{}]: Disconnection push received, reconnecting",
                        params.shard_addr
                    );
                    break;
                }
                redis::PushKind::SMessage | redis::PushKind::Message => {
                    match params.fan_in_tx.try_send(push_info) {
                        Ok(()) => {}
                        Err(crossfire::TrySendError::Full(_)) => {
                            // Bounded fan-in is full; drop this message.
                            // The 10 000-cap prevents OOM under sustained backpressure.
                        }
                        Err(crossfire::TrySendError::Disconnected(_)) => {
                            break 'outer; // caller dropped the receiver; stop
                        }
                    }
                }
                _ => {} // SSubscribe confirmation, Pong, etc. not forwarded
            }
        }

        reconnection_count += 1;
        warn!(
            "shard_listener[{}]: connection ended, reconnecting in {retry_delay}ms",
            params.shard_addr
        );
        tokio::select! {
            _ = params.shutdown.notified() => break 'outer,
            _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
        }
        retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
    }

    info!("shard_listener[{}]: stopped", params.shard_addr);
}

/// Top-level orchestrator for slot-aware Redis Cluster sharded Pub/Sub.
pub(crate) struct ShardedSubscriber {
    channels: Vec<String>,
    seed_urls: Vec<String>,
    mode: PubSubMode,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    is_running: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
    refresh_notify: Arc<Notify>,
}

impl ShardedSubscriber {
    // Caller's is_running and shutdown are shared with spawned shard tasks.
    pub(crate) fn new(
        channels: Vec<String>,
        seed_urls: Vec<String>,
        metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
        is_running: Arc<AtomicBool>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self::with_mode(
            channels,
            seed_urls,
            PubSubMode::Sharded,
            metrics,
            is_running,
            shutdown,
        )
    }

    pub(crate) fn new_standard(
        channels: Vec<String>,
        seed_urls: Vec<String>,
        metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
        is_running: Arc<AtomicBool>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self::with_mode(
            channels,
            seed_urls,
            PubSubMode::Standard,
            metrics,
            is_running,
            shutdown,
        )
    }

    fn with_mode(
        channels: Vec<String>,
        seed_urls: Vec<String>,
        mode: PubSubMode,
        metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
        is_running: Arc<AtomicBool>,
        shutdown: Arc<Notify>,
    ) -> Self {
        Self {
            channels,
            seed_urls,
            mode,
            metrics,
            is_running,
            shutdown,
            refresh_notify: Arc::new(Notify::new()),
        }
    }

    /// Discover cluster topology, spawn per-shard listeners, and return the
    /// single fan-in receiver.
    ///
    /// # Errors
    ///
    /// Propagates any error from `Topology::discover`. Individual shard
    /// connection failures are retried internally.
    pub(crate) async fn start(&self) -> Result<ShardedPushReceiver> {
        let topology = Topology::discover(&self.seed_urls).await?;

        let mut by_shard: HashMap<NodeAddr, Vec<String>> = HashMap::new();
        for ch in &self.channels {
            let shard = topology.shard_for(ch).clone();
            by_shard.entry(shard).or_default().push(ch.clone());
        }

        info!(
            "ShardedSubscriber: {} {} channel(s) across {} shard(s)",
            self.mode.label(),
            self.channels.len(),
            by_shard.len()
        );

        let (fan_tx, fan_rx): (ShardedPushSender, ShardedPushReceiver) =
            mpsc::bounded_async(10_000);

        let scheme = if self.seed_urls.iter().any(|u| u.starts_with("rediss://")) {
            "rediss"
        } else {
            "redis"
        };
        let password: Option<String> = self
            .seed_urls
            .first()
            .and_then(|u| redis::IntoConnectionInfo::into_connection_info(u.as_str()).ok())
            .and_then(|ci| ci.redis_settings().password().map(str::to_owned));

        // Clone fan_tx for the refresh task BEFORE the drop at the end.
        let fan_tx_for_refresh = fan_tx.clone();

        let channel_shard_map: Arc<Mutex<HashMap<String, NodeAddr>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let shard_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        {
            let mut channel_map = channel_shard_map.lock().await;
            for (shard_addr, shard_channels) in &by_shard {
                for ch in shard_channels {
                    channel_map.insert(ch.clone(), shard_addr.clone());
                }
            }
        }

        {
            let mut shard_map = shard_handles.lock().await;
            for (shard_addr, shard_channels) in by_shard {
                if shard_channels.is_empty() {
                    continue;
                }
                let url = shard_addr.to_url(scheme, password.as_deref());
                let shard_addr_str = format!("{}:{}", shard_addr.host, shard_addr.port);
                let params = ShardListenerParams {
                    url,
                    channels: shard_channels,
                    mode: self.mode,
                    fan_in_tx: fan_tx.clone(),
                    is_running: self.is_running.clone(),
                    shutdown: self.shutdown.clone(),
                    shard_addr: shard_addr_str.clone(),
                    metrics: self.metrics.clone(),
                    refresh_notify: self.refresh_notify.clone(),
                };
                shard_map.insert(shard_addr_str, tokio::spawn(shard_listener_loop(params)));
            }
        }

        let refresh_notify_clone = self.refresh_notify.clone();
        let shutdown_clone = self.shutdown.clone();
        let is_running_clone = self.is_running.clone();
        let seed_urls_clone = self.seed_urls.clone();
        let channel_shard_map_clone = channel_shard_map.clone();
        let shard_handles_clone = shard_handles.clone();
        let metrics_clone = self.metrics.clone();
        let scheme_clone = scheme.to_owned();
        let password_clone = password.clone();
        let mode = self.mode;

        let refresh_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(TOPOLOGY_REFRESH_INTERVAL_SECS)) => {},
                    _ = refresh_notify_clone.notified() => {},
                    _ = shutdown_clone.notified() => break,
                }
                if !is_running_clone.load(Ordering::Relaxed) {
                    break;
                }

                let new_topo = match Topology::discover(&seed_urls_clone).await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("topology refresh failed: {e}");
                        continue;
                    }
                };

                let current_map = channel_shard_map_clone.lock().await.clone();
                let mut new_map = current_map.clone();

                // old_shards_to_abort: channels moving away from a shard
                // new_shard_channels: channels arriving on a new shard
                let mut old_shards_to_abort: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                let mut new_shard_channels: HashMap<String, Vec<String>> = HashMap::new();

                for (ch, old_shard) in &current_map {
                    let new_shard = new_topo.shard_for(ch);
                    if new_shard != old_shard {
                        new_map.insert(ch.clone(), new_shard.clone());
                        let old_str = format!("{}:{}", old_shard.host, old_shard.port);
                        let new_str = format!("{}:{}", new_shard.host, new_shard.port);
                        old_shards_to_abort.insert(old_str);
                        new_shard_channels
                            .entry(new_str)
                            .or_default()
                            .push(ch.clone());
                    }
                }

                if new_shard_channels.is_empty() {
                    continue;
                }

                let mut sh = shard_handles_clone.lock().await;

                for old_str in old_shards_to_abort {
                    if let Some(handle) = sh.remove(&old_str) {
                        handle.abort();
                    }
                }

                for (new_str, channels) in new_shard_channels {
                    let new_shard_addr = new_topo.shard_for(&channels[0]).clone();
                    let url = new_shard_addr.to_url(&scheme_clone, password_clone.as_deref());
                    let params = ShardListenerParams {
                        url,
                        channels,
                        mode,
                        fan_in_tx: fan_tx_for_refresh.clone(),
                        is_running: is_running_clone.clone(),
                        shutdown: shutdown_clone.clone(),
                        shard_addr: new_str.clone(),
                        metrics: metrics_clone.clone(),
                        refresh_notify: refresh_notify_clone.clone(),
                    };
                    sh.insert(new_str, tokio::spawn(shard_listener_loop(params)));
                }

                let total_shards = sh.len();
                drop(sh);
                *channel_shard_map_clone.lock().await = new_map;
                info!(
                    "topology refresh: migrated channels across {} shard(s)",
                    total_shards
                );
            }
        });
        drop(refresh_task);

        // Drop the original sender so the channel closes when all listeners exit.
        drop(fan_tx);

        Ok(fan_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_addr_to_url_no_password() {
        let addr = NodeAddr::new("127.0.0.1", 6379);
        assert_eq!(
            addr.to_url("redis", None),
            "redis://127.0.0.1:6379/?protocol=resp3"
        );
    }

    #[test]
    fn node_addr_to_url_with_password() {
        let addr = NodeAddr::new("127.0.0.1", 6380);
        assert_eq!(
            addr.to_url("rediss", Some("secret")),
            "rediss://:secret@127.0.0.1:6380/?protocol=resp3"
        );
    }

    #[test]
    fn parse_cluster_slots_basic() {
        let raw = redis::Value::Array(vec![redis::Value::Array(vec![
            redis::Value::Int(0),
            redis::Value::Int(5460),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"127.0.0.1".to_vec()),
                redis::Value::Int(6379),
                redis::Value::BulkString(b"node-id-1".to_vec()),
            ]),
        ])]);

        let topo = parse_cluster_slots(raw).expect("parse_cluster_slots failed");
        assert_eq!(topo.masters.len(), 1);
        assert_eq!(topo.masters[0], NodeAddr::new("127.0.0.1", 6379));
        assert_eq!(
            topo.shard_for("sockudo_adapter::#requests"),
            &NodeAddr::new("127.0.0.1", 6379)
        );
    }

    #[test]
    fn parse_cluster_slots_no_masters_returns_error() {
        let raw = redis::Value::Array(vec![]);
        assert!(parse_cluster_slots(raw).is_err());
    }

    fn make_shard_value(start: i64, end: i64, host: &str, port: i64) -> redis::Value {
        redis::Value::Array(vec![
            redis::Value::BulkString(b"slots".to_vec()),
            redis::Value::Array(vec![redis::Value::Int(start), redis::Value::Int(end)]),
            redis::Value::BulkString(b"nodes".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"endpoint".to_vec()),
                redis::Value::BulkString(host.as_bytes().to_vec()),
                redis::Value::BulkString(b"port".to_vec()),
                redis::Value::Int(port),
                redis::Value::BulkString(b"role".to_vec()),
                redis::Value::BulkString(b"master".to_vec()),
            ])]),
        ])
    }

    #[test]
    fn parse_cluster_shards_three_shards_verify_routing() {
        // shard1: 0..5461 owns #requests (slot 3625)
        // shard2: 5462..10922 owns #broadcast (slot 7996)
        // shard3: 10923..16383 owns #responses (slot 11538)
        let raw = redis::Value::Array(vec![
            make_shard_value(0, 5461, "10.0.0.1", 6379),
            make_shard_value(5462, 10922, "10.0.0.2", 6379),
            make_shard_value(10923, 16383, "10.0.0.3", 6379),
        ]);

        let topo = parse_cluster_shards(raw).expect("parse_cluster_shards 3-shard failed");
        assert_eq!(topo.masters.len(), 3, "expected 3 master nodes");
        assert_eq!(
            topo.shard_for("sockudo_adapter::#requests"),
            &NodeAddr::new("10.0.0.1", 6379),
            "#requests (slot 3625) should route to shard1",
        );
        assert_eq!(
            topo.shard_for("sockudo_adapter::#broadcast"),
            &NodeAddr::new("10.0.0.2", 6379),
            "#broadcast (slot 7996) should route to shard2",
        );
        assert_eq!(
            topo.shard_for("sockudo_adapter::#responses"),
            &NodeAddr::new("10.0.0.3", 6379),
            "#responses (slot 11538) should route to shard3",
        );
    }

    #[test]
    fn parse_cluster_shards_empty_returns_error() {
        let raw = redis::Value::Array(vec![]);
        assert!(
            parse_cluster_shards(raw).is_err(),
            "empty CLUSTER SHARDS response should be an error",
        );
    }

    #[test]
    fn parse_cluster_shards_elasticache_endpoint_field() {
        // ElastiCache returns "endpoint", not "ip".
        let shard = redis::Value::Array(vec![
            redis::Value::BulkString(b"slots".to_vec()),
            redis::Value::Array(vec![redis::Value::Int(0), redis::Value::Int(16383)]),
            redis::Value::BulkString(b"nodes".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"endpoint".to_vec()),
                redis::Value::BulkString(b"shard-001.cluster.usw2.cache.amazonaws.com".to_vec()),
                redis::Value::BulkString(b"tls-port".to_vec()),
                redis::Value::Int(6379),
                redis::Value::BulkString(b"role".to_vec()),
                redis::Value::BulkString(b"primary".to_vec()),
            ])]),
        ]);
        let raw = redis::Value::Array(vec![shard]);
        let topo =
            parse_cluster_shards(raw).expect("parse_cluster_shards with endpoint field failed");
        assert_eq!(topo.masters.len(), 1);
        assert_eq!(
            topo.masters[0],
            NodeAddr::new("shard-001.cluster.usw2.cache.amazonaws.com", 6379)
        );
    }

    #[test]
    fn parse_cluster_shards_node_with_tls_port_field() {
        let shard = redis::Value::Array(vec![
            redis::Value::BulkString(b"slots".to_vec()),
            redis::Value::Array(vec![redis::Value::Int(0), redis::Value::Int(16383)]),
            redis::Value::BulkString(b"nodes".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"endpoint".to_vec()),
                redis::Value::BulkString(b"10.0.0.5".to_vec()),
                redis::Value::BulkString(b"port".to_vec()),
                redis::Value::Int(6379),
                redis::Value::BulkString(b"tls-port".to_vec()),
                redis::Value::Int(6380),
                redis::Value::BulkString(b"role".to_vec()),
                redis::Value::BulkString(b"master".to_vec()),
            ])]),
        ]);
        let raw = redis::Value::Array(vec![shard]);
        let topo =
            parse_cluster_shards(raw).expect("parse_cluster_shards with tls-port field failed");
        assert_eq!(topo.masters.len(), 1);
        // tls-port (6380) is non-zero so it takes precedence over port (6379).
        assert_eq!(topo.masters[0], NodeAddr::new("10.0.0.5", 6380));
    }

    #[test]
    fn parse_cluster_shards_replica_not_counted_as_master() {
        // A shard whose only node has role "replica" must not contribute a master.
        let shard = redis::Value::Array(vec![
            redis::Value::BulkString(b"slots".to_vec()),
            redis::Value::Array(vec![redis::Value::Int(0), redis::Value::Int(16383)]),
            redis::Value::BulkString(b"nodes".to_vec()),
            redis::Value::Array(vec![redis::Value::Array(vec![
                redis::Value::BulkString(b"ip".to_vec()),
                redis::Value::BulkString(b"10.0.0.9".to_vec()),
                redis::Value::BulkString(b"port".to_vec()),
                redis::Value::Int(6379),
                redis::Value::BulkString(b"role".to_vec()),
                redis::Value::BulkString(b"replica".to_vec()),
            ])]),
        ]);
        let raw = redis::Value::Array(vec![shard]);
        assert!(
            parse_cluster_shards(raw).is_err(),
            "replica-only response should return an error",
        );
    }

    #[test]
    fn node_addr_to_url_tls_scheme_no_password() {
        let addr = NodeAddr::new("10.0.0.1", 6380);
        assert_eq!(
            addr.to_url("rediss", None),
            "rediss://10.0.0.1:6380/?protocol=resp3",
        );
    }

    #[test]
    fn parse_node_entry_ip_and_port() {
        let fields = vec![
            redis::Value::BulkString(b"ip".to_vec()),
            redis::Value::BulkString(b"10.0.0.1".to_vec()),
            redis::Value::BulkString(b"port".to_vec()),
            redis::Value::Int(6379),
            redis::Value::BulkString(b"role".to_vec()),
            redis::Value::BulkString(b"master".to_vec()),
        ];
        let (addr, role) = parse_node_entry(&fields).unwrap();
        assert_eq!(addr, NodeAddr::new("10.0.0.1", 6379));
        assert_eq!(role, "master");
    }

    #[test]
    fn parse_node_entry_endpoint_overrides_ip() {
        let fields = vec![
            redis::Value::BulkString(b"ip".to_vec()),
            redis::Value::BulkString(b"10.0.0.1".to_vec()),
            redis::Value::BulkString(b"endpoint".to_vec()),
            redis::Value::BulkString(b"my-cluster.cache.amazonaws.com".to_vec()),
            redis::Value::BulkString(b"port".to_vec()),
            redis::Value::Int(6379),
            redis::Value::BulkString(b"role".to_vec()),
            redis::Value::BulkString(b"master".to_vec()),
        ];
        let (addr, _) = parse_node_entry(&fields).unwrap();
        assert_eq!(addr.host, "my-cluster.cache.amazonaws.com");
    }

    #[test]
    fn parse_node_entry_tls_port_overrides_port() {
        let fields = vec![
            redis::Value::BulkString(b"ip".to_vec()),
            redis::Value::BulkString(b"10.0.0.1".to_vec()),
            redis::Value::BulkString(b"port".to_vec()),
            redis::Value::Int(6379),
            redis::Value::BulkString(b"tls-port".to_vec()),
            redis::Value::Int(6380),
            redis::Value::BulkString(b"role".to_vec()),
            redis::Value::BulkString(b"master".to_vec()),
        ];
        let (addr, _) = parse_node_entry(&fields).unwrap();
        assert_eq!(addr.port, 6380);
    }

    #[test]
    fn parse_node_entry_tls_port_zero_falls_back_to_port() {
        let fields = vec![
            redis::Value::BulkString(b"ip".to_vec()),
            redis::Value::BulkString(b"10.0.0.1".to_vec()),
            redis::Value::BulkString(b"tls-port".to_vec()),
            redis::Value::Int(0),
            redis::Value::BulkString(b"port".to_vec()),
            redis::Value::Int(6379),
            redis::Value::BulkString(b"role".to_vec()),
            redis::Value::BulkString(b"master".to_vec()),
        ];
        let (addr, _) = parse_node_entry(&fields).unwrap();
        assert_eq!(addr.port, 6379);
    }

    #[test]
    fn parse_node_entry_missing_host_returns_none() {
        let fields = vec![
            redis::Value::BulkString(b"port".to_vec()),
            redis::Value::Int(6379),
            redis::Value::BulkString(b"role".to_vec()),
            redis::Value::BulkString(b"master".to_vec()),
        ];
        assert!(parse_node_entry(&fields).is_none());
    }

    #[test]
    fn parse_node_entry_missing_role_returns_none() {
        let fields = vec![
            redis::Value::BulkString(b"ip".to_vec()),
            redis::Value::BulkString(b"10.0.0.1".to_vec()),
            redis::Value::BulkString(b"port".to_vec()),
            redis::Value::Int(6379),
        ];
        assert!(parse_node_entry(&fields).is_none());
    }

    #[test]
    fn parse_node_entry_empty_fields_returns_none() {
        assert!(parse_node_entry(&[]).is_none());
    }

    #[test]
    fn shard_for_unknown_slot_returns_fallback() {
        // Build a topology with only slot 0 mapped
        let mut slot_owners = HashMap::new();
        slot_owners.insert(Slot::new(0).unwrap(), NodeAddr::new("10.0.0.1", 6379));
        let fallback = NodeAddr::new("10.0.0.99", 6379);
        let topo = Topology {
            slot_owners,
            fallback: fallback.clone(),
            masters: vec![NodeAddr::new("10.0.0.1", 6379), fallback],
        };
        // "sockudo_adapter::#broadcast" hashes to slot 7996, which is not in our map
        let result = topo.shard_for("sockudo_adapter::#broadcast");
        assert_eq!(result, &NodeAddr::new("10.0.0.99", 6379));
    }

    #[test]
    fn parse_cluster_slots_skips_malformed_entries() {
        let raw = redis::Value::Array(vec![
            // Valid entry
            redis::Value::Array(vec![
                redis::Value::Int(0),
                redis::Value::Int(16383),
                redis::Value::Array(vec![
                    redis::Value::BulkString(b"10.0.0.1".to_vec()),
                    redis::Value::Int(6379),
                ]),
            ]),
            // Malformed: too short
            redis::Value::Array(vec![redis::Value::Int(0)]),
            // Malformed: not an array
            redis::Value::Int(42),
        ]);
        let topo = parse_cluster_slots(raw).expect("should skip malformed and succeed");
        assert_eq!(topo.masters.len(), 1);
    }

    #[test]
    fn parse_cluster_shards_skips_malformed_shard_entries() {
        let raw = redis::Value::Array(vec![
            // Valid shard
            make_shard_value(0, 16383, "10.0.0.1", 6379),
            // Malformed: not an array
            redis::Value::Int(42),
        ]);
        let topo = parse_cluster_shards(raw).expect("should skip malformed and succeed");
        assert_eq!(topo.masters.len(), 1);
    }
}
