//! Slot-aware, per-shard Pub/Sub for Redis Cluster.
//!
//! `redis-rs` does not expose sharded Pub/Sub subscriptions on its cluster
//! client. This module works around that by discovering the cluster topology,
//! grouping channels by hash slot, and opening a direct (non-cluster)
//! connection to each shard master. Inspired by `go-redis` and `fred.rs`.

use crate::horizontal_transport::ListenerState;
use crate::transports::redis_pubsub_probe::{PubSubProbeFailure, pubsub_connection_lost};
use crate::transports::redis_transport::PUBSUB_MAX_RETRY_DELAY_MS;
use crossfire::mpsc;
use redis::cluster_routing::Slot;
use sockudo_core::error::{Error, Result};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::options::RedisTlsOptions;
use sockudo_core::redis_client::build_standalone_client;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const TOPOLOGY_REFRESH_INTERVAL_SECS: u64 = 30;
const TOPOLOGY_REFRESH_FAILURE_THRESHOLD: u64 = 3;
const FAN_IN_CAPACITY: usize = 10_000;

/// Combines the subscription state of every shard listener into the node-level
/// [`ListenerState`] the adapter needs: the node hears its peers only while every
/// shard listener is subscribed (#566).
pub(crate) struct SubscriptionHealth {
    on_state: Arc<dyn Fn(ListenerState) + Send + Sync>,
    is_running: Arc<AtomicBool>,
    next_id: AtomicU64,
    down: std::sync::Mutex<HashSet<u64>>,
}

impl SubscriptionHealth {
    pub(crate) fn new(
        on_state: Arc<dyn Fn(ListenerState) + Send + Sync>,
        is_running: Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            on_state,
            is_running,
            next_id: AtomicU64::new(0),
            down: std::sync::Mutex::new(HashSet::new()),
        })
    }

    /// Register a shard listener. One that starts `down` holds the node in the lost
    /// state until it subscribes, so moving channels to a new shard cannot report a
    /// restore before they are subscribed again.
    fn listener(self: &Arc<Self>, down: bool) -> ShardHealth {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let shard = ShardHealth {
            id,
            health: self.clone(),
        };
        if down {
            self.set_down(id, true);
        }
        shard
    }

    fn set_down(&self, id: u64, down: bool) {
        let mut set = self.down.lock().unwrap_or_else(|e| e.into_inner());
        let was_healthy = set.is_empty();
        let changed = if down {
            set.insert(id)
        } else {
            set.remove(&id)
        };
        if !changed || was_healthy == set.is_empty() || !self.is_running.load(Ordering::Relaxed) {
            return;
        }
        // Report under the lock so concurrent shard transitions reach the adapter in order.
        if was_healthy {
            (self.on_state)(ListenerState::Lost);
        } else {
            info!(adapter = "redis_cluster", "pubsub subscriptions restored");
            (self.on_state)(ListenerState::Restored);
        }
    }
}

/// One shard listener's entry in [`SubscriptionHealth`]. Dropping it, including
/// when the listener task is aborted on a topology change, removes the entry.
pub(crate) struct ShardHealth {
    id: u64,
    health: Arc<SubscriptionHealth>,
}

impl ShardHealth {
    fn set_subscribed(&self, subscribed: bool) {
        self.health.set_down(self.id, !subscribed);
    }
}

impl Drop for ShardHealth {
    fn drop(&mut self) {
        self.health.set_down(self.id, false);
    }
}

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

    pub(crate) fn to_url(
        &self,
        scheme: &str,
        username: Option<&str>,
        password: Option<&str>,
    ) -> String {
        match (username, password) {
            (Some(user), Some(pw)) => format!(
                "{}://{}:{}@{}:{}/?protocol=resp3",
                scheme, user, pw, self.host, self.port
            ),
            (None, Some(pw)) => format!(
                "{}://:{}@{}:{}/?protocol=resp3",
                scheme, pw, self.host, self.port
            ),
            _ => format!("{}://{}:{}/?protocol=resp3", scheme, self.host, self.port),
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
    pub(crate) async fn discover(seed_urls: &[String], tls: &RedisTlsOptions) -> Result<Self> {
        let mut last_err: Option<Error> = None;
        for url in seed_urls {
            match Self::discover_one(url, tls).await {
                Ok(topo) => return Ok(topo),
                Err(e) => {
                    warn!(adapter = "redis_cluster", "seed node connection failed");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err
            .unwrap_or_else(|| Error::Redis("Topology::discover: no seed URLs provided".into())))
    }

    async fn discover_one(url: &str, tls: &RedisTlsOptions) -> Result<Self> {
        let client = build_standalone_client(url, tls).await?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Error::Redis(format!("topology: connect to seed node failed: {e}")))?;

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
                    .map_err(|e| Error::Redis(format!("topology: CLUSTER SLOTS failed: {e}")))?;
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
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    is_running: Arc<AtomicBool>,
    shutdown: Arc<Notify>,
    shard_addr: String,
    metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
    refresh_notify: Arc<Notify>,
    tls: RedisTlsOptions,
    ping_interval: Duration,
    health: ShardHealth,
}

/// Long-running task for one shard master.
///
/// Connects via a non-cluster `redis::Client`, issues the configured
/// subscription command for each assigned channel, and forwards Pub/Sub pushes
/// to the bounded fan-in sender. Reconnects with exponential back-off
/// (100 ms to `PUBSUB_MAX_RETRY_DELAY_MS`) on any failure, and reports each lost and
/// restored subscription to [`SubscriptionHealth`].
pub(crate) async fn shard_listener_loop(mut params: ShardListenerParams) {
    // Unbounded because push_sender must not block the redis runtime;
    // backpressure is applied at the bounded fan-in boundary.
    type InternalFlavor = mpsc::List<redis::PushInfo>;

    // While this shard is unsubscribed the node misses peer messages routed to it, so
    // retry often: a long backoff keeps it deaf for seconds after Redis is back (#566).
    let mut retry_delay = 100u64;
    const MAX_RETRY_DELAY: u64 = PUBSUB_MAX_RETRY_DELAY_MS;
    let mut reconnection_count = 0u64;
    let mut consecutive_failures: u64 = 0;
    let mut fan_in_dropped_count = 0u64;

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

        let client = match build_standalone_client(&params.url, &params.tls).await {
            Ok(c) => c,
            Err(_e) => {
                warn!(
                    adapter = "redis_cluster",
                    shard = %params.shard_addr,
                    retry_delay_ms = retry_delay,
                    "shard listener failed to open client"
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
                        adapter = "redis_cluster",
                        shard = %params.shard_addr,
                        attempt_count = reconnection_count,
                        "shard listener reconnected"
                    );
                }
                retry_delay = 100;
                consecutive_failures = 0;
                c
            }
            Err(_e) => {
                reconnection_count += 1;
                warn!(
                    adapter = "redis_cluster",
                    shard = %params.shard_addr,
                    attempt = reconnection_count,
                    retry_delay_ms = retry_delay,
                    "shard listener connect failed"
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
            if let Err(_e) = redis::cmd(subscribe_command)
                .arg(ch.as_str())
                .exec_async(&mut conn)
                .await
            {
                warn!(
                    adapter = "redis_cluster",
                    shard = %params.shard_addr,
                    channel = %ch,
                    "shard subscription failed"
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
            adapter = "redis_cluster",
            shard = %params.shard_addr,
            channel_count = params.channels.len(),
            "shard subscribed"
        );
        if let Some(ready_tx) = params.ready_tx.take() {
            let _ = ready_tx.send(());
        }
        params.health.set_subscribed(true);
        reconnection_count = 0;

        let ping_conn = conn.clone();
        let liveness = pubsub_connection_lost(params.ping_interval, move || {
            let mut conn = ping_conn.clone();
            async move { redis::cmd("PING").exec_async(&mut conn).await }
        });
        tokio::pin!(liveness);

        loop {
            let recv = tokio::select! {
                // A reset that fails the probe also closes the push channel; poll the probe
                // first so the reconnect is attributed to it.
                biased;
                _ = params.shutdown.notified() => break 'outer,
                failure = &mut liveness => {
                    if let Some(m) = params.metrics.get() {
                        m.mark_horizontal_transport_reconnection(params.mode.metrics_transport());
                    }
                    match failure {
                        PubSubProbeFailure::Failed(e) => warn!(
                            adapter = "redis_cluster",
                            shard = %params.shard_addr,
                            error = %e,
                            retryable = true,
                            "shard liveness ping failed, resubscribing"
                        ),
                        PubSubProbeFailure::TimedOut => warn!(
                            adapter = "redis_cluster",
                            shard = %params.shard_addr,
                            timeout_ms = params.ping_interval.as_millis() as u64,
                            retryable = true,
                            "shard liveness ping timed out, resubscribing"
                        ),
                    }
                    break;
                }
                msg = internal_rx.recv() => msg,
            };

            let push_info = match recv {
                Err(_) => break, // internal channel closed = redis connection dropped
                Ok(p) => p,
            };

            match push_info.kind {
                redis::PushKind::Disconnection => {
                    warn!(
                        adapter = "redis_cluster",
                        shard = %params.shard_addr,
                        "shard disconnection received, reconnecting"
                    );
                    break;
                }
                redis::PushKind::SMessage | redis::PushKind::Message => {
                    match params.fan_in_tx.try_send(push_info) {
                        Ok(()) => {}
                        Err(crossfire::TrySendError::Full(_)) => {
                            fan_in_dropped_count = fan_in_dropped_count.saturating_add(1);
                            if let Some(metrics) = params.metrics.get() {
                                metrics.mark_horizontal_transport_message_dropped(
                                    params.mode.metrics_transport(),
                                );
                            }
                            // Keep the first drop immediately visible without producing one
                            // warning per message during sustained overload.
                            if fan_in_dropped_count.is_power_of_two() {
                                warn!(
                                    adapter = "redis_cluster",
                                    shard = %params.shard_addr,
                                    dropped_count = fan_in_dropped_count,
                                    queue_capacity_count = FAN_IN_CAPACITY,
                                    "sharded pub/sub fan-in message dropped"
                                );
                            }
                        }
                        Err(crossfire::TrySendError::Disconnected(_)) => {
                            break 'outer; // caller dropped the receiver; stop
                        }
                    }
                }
                _ => {} // SSubscribe confirmation, Pong, etc. not forwarded
            }
        }

        params.health.set_subscribed(false);
        reconnection_count += 1;
        warn!(
            adapter = "redis_cluster",
            shard = %params.shard_addr,
            retry_delay_ms = retry_delay,
            "shard connection ended, reconnecting"
        );
        tokio::select! {
            _ = params.shutdown.notified() => break 'outer,
            _ = tokio::time::sleep(Duration::from_millis(retry_delay)) => {}
        }
        retry_delay = retry_delay.saturating_mul(2).min(MAX_RETRY_DELAY);
    }

    info!(adapter = "redis_cluster", shard = %params.shard_addr, "shard listener stopped");
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
    tls: RedisTlsOptions,
    ping_interval: Duration,
    health: Arc<SubscriptionHealth>,
}

impl ShardedSubscriber {
    // Caller's is_running and shutdown are shared with spawned shard tasks.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        channels: Vec<String>,
        seed_urls: Vec<String>,
        metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
        is_running: Arc<AtomicBool>,
        shutdown: Arc<Notify>,
        tls: RedisTlsOptions,
        ping_interval: Duration,
        health: Arc<SubscriptionHealth>,
    ) -> Self {
        Self::with_mode(
            channels,
            seed_urls,
            PubSubMode::Sharded,
            metrics,
            is_running,
            shutdown,
            tls,
            ping_interval,
            health,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_standard(
        channels: Vec<String>,
        seed_urls: Vec<String>,
        metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
        is_running: Arc<AtomicBool>,
        shutdown: Arc<Notify>,
        tls: RedisTlsOptions,
        ping_interval: Duration,
        health: Arc<SubscriptionHealth>,
    ) -> Self {
        Self::with_mode(
            channels,
            seed_urls,
            PubSubMode::Standard,
            metrics,
            is_running,
            shutdown,
            tls,
            ping_interval,
            health,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn with_mode(
        channels: Vec<String>,
        seed_urls: Vec<String>,
        mode: PubSubMode,
        metrics: Arc<OnceLock<Arc<dyn MetricsInterface + Send + Sync>>>,
        is_running: Arc<AtomicBool>,
        shutdown: Arc<Notify>,
        tls: RedisTlsOptions,
        ping_interval: Duration,
        health: Arc<SubscriptionHealth>,
    ) -> Self {
        Self {
            channels,
            seed_urls,
            mode,
            metrics,
            is_running,
            shutdown,
            refresh_notify: Arc::new(Notify::new()),
            tls,
            ping_interval,
            health,
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
        let topology = Topology::discover(&self.seed_urls, &self.tls).await?;

        let mut by_shard: HashMap<NodeAddr, Vec<String>> = HashMap::new();
        for ch in &self.channels {
            let shard = topology.shard_for(ch).clone();
            by_shard.entry(shard).or_default().push(ch.clone());
        }

        info!(
            adapter = "redis_cluster",
            channel_count = self.channels.len(),
            shard_count = by_shard.len(),
            "sharded subscriber initialized"
        );

        let (fan_tx, fan_rx): (ShardedPushSender, ShardedPushReceiver) =
            mpsc::bounded_async(FAN_IN_CAPACITY);

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
        let username: Option<String> = self
            .seed_urls
            .first()
            .and_then(|u| redis::IntoConnectionInfo::into_connection_info(u.as_str()).ok())
            .and_then(|ci| ci.redis_settings().username().map(str::to_owned));

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
            // Register every shard before any listener can report a successful subscription.
            let mut pending = Vec::with_capacity(by_shard.len());
            for (shard_addr, shard_channels) in by_shard {
                if shard_channels.is_empty() {
                    continue;
                }
                let url = shard_addr.to_url(scheme, username.as_deref(), password.as_deref());
                let shard_addr_str = format!("{}:{}", shard_addr.host, shard_addr.port);
                let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                let params = ShardListenerParams {
                    url,
                    channels: shard_channels,
                    mode: self.mode,
                    fan_in_tx: fan_tx.clone(),
                    ready_tx: Some(ready_tx),
                    is_running: self.is_running.clone(),
                    shutdown: self.shutdown.clone(),
                    shard_addr: shard_addr_str.clone(),
                    metrics: self.metrics.clone(),
                    refresh_notify: self.refresh_notify.clone(),
                    tls: self.tls.clone(),
                    ping_interval: self.ping_interval,
                    health: self.health.listener(true),
                };
                pending.push((shard_addr_str, params, ready_rx));
            }

            let mut shard_map = shard_handles.lock().await;
            let mut ready_receivers = Vec::with_capacity(pending.len());
            for (shard_addr_str, params, ready_rx) in pending {
                let readiness_channels = params.channels.clone();
                shard_map.insert(
                    shard_addr_str.clone(),
                    tokio::spawn(shard_listener_loop(params)),
                );
                ready_receivers.push((shard_addr_str, readiness_channels, ready_rx));
            }
            drop(shard_map);

            let readiness =
                ready_receivers
                    .into_iter()
                    .map(|(shard, channels, ready_rx)| async move {
                        (
                            shard,
                            channels,
                            tokio::time::timeout(Duration::from_secs(5), ready_rx).await,
                        )
                    });
            for (shard, channels, result) in futures::future::join_all(readiness).await {
                if !matches!(result, Ok(Ok(()))) {
                    warn!(
                        adapter = "redis_cluster",
                        shard = %shard,
                        channel_count = channels.len(),
                        channels = ?channels,
                        "sharded subscriber initial subscription timed out"
                    );
                }
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
        let username_clone = username.clone();
        let mode = self.mode;
        let tls = self.tls.clone();
        let ping_interval = self.ping_interval;
        let health = self.health.clone();

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

                let new_topo = match Topology::discover(&seed_urls_clone, &tls).await {
                    Ok(t) => t,
                    Err(_e) => {
                        warn!(adapter = "redis_cluster", "topology refresh failed");
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

                // Register the replacement listeners as unsubscribed before aborting the
                // old ones, so the node stays in the lost state until the moved channels
                // are subscribed again.
                let new_shard_channels: Vec<_> = new_shard_channels
                    .into_iter()
                    .map(|(new_str, channels)| (new_str, channels, health.listener(true)))
                    .collect();

                for old_str in old_shards_to_abort {
                    if let Some(handle) = sh.remove(&old_str) {
                        handle.abort();
                    }
                }

                for (new_str, channels, shard_health) in new_shard_channels {
                    let new_shard_addr = new_topo.shard_for(&channels[0]).clone();
                    let url = new_shard_addr.to_url(
                        &scheme_clone,
                        username_clone.as_deref(),
                        password_clone.as_deref(),
                    );
                    let params = ShardListenerParams {
                        url,
                        channels,
                        mode,
                        fan_in_tx: fan_tx_for_refresh.clone(),
                        ready_tx: None,
                        is_running: is_running_clone.clone(),
                        shutdown: shutdown_clone.clone(),
                        shard_addr: new_str.clone(),
                        metrics: metrics_clone.clone(),
                        refresh_notify: refresh_notify_clone.clone(),
                        tls: tls.clone(),
                        ping_interval,
                        health: shard_health,
                    };
                    sh.insert(new_str, tokio::spawn(shard_listener_loop(params)));
                }

                let total_shards = sh.len();
                drop(sh);
                *channel_shard_map_clone.lock().await = new_map;
                info!(
                    adapter = "redis_cluster",
                    shard_count = total_shards,
                    "topology refresh completed"
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
            addr.to_url("redis", None, None),
            "redis://127.0.0.1:6379/?protocol=resp3"
        );
    }

    #[test]
    fn node_addr_to_url_with_password() {
        let addr = NodeAddr::new("127.0.0.1", 6380);
        assert_eq!(
            addr.to_url("rediss", None, Some("secret")),
            "rediss://:secret@127.0.0.1:6380/?protocol=resp3"
        );
    }

    #[test]
    fn node_addr_to_url_with_username_and_password() {
        let addr = NodeAddr::new("cache.example.com", 6379);
        assert_eq!(
            addr.to_url("rediss", Some("myuser"), Some("mypass")),
            "rediss://myuser:mypass@cache.example.com:6379/?protocol=resp3"
        );
    }

    #[test]
    fn node_addr_to_url_with_username_only() {
        let addr = NodeAddr::new("cache.example.com", 6379);
        assert_eq!(
            addr.to_url("rediss", Some("myuser"), None),
            "rediss://cache.example.com:6379/?protocol=resp3"
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
            addr.to_url("rediss", None, None),
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

    fn recording_health() -> (
        Arc<SubscriptionHealth>,
        Arc<std::sync::Mutex<Vec<ListenerState>>>,
    ) {
        let states = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = states.clone();
        let health = SubscriptionHealth::new(
            Arc::new(move |state| recorded.lock().unwrap().push(state)),
            Arc::new(AtomicBool::new(true)),
        );
        (health, states)
    }

    #[test]
    fn subscription_health_reports_only_node_level_transitions() {
        let (health, states) = recording_health();
        let a = health.listener(false);
        let b = health.listener(false);
        a.set_subscribed(true);
        b.set_subscribed(true);
        assert!(states.lock().unwrap().is_empty());

        // Both shards drop: one Lost. Only after the last one is back: one Restored.
        a.set_subscribed(false);
        b.set_subscribed(false);
        a.set_subscribed(true);
        assert_eq!(*states.lock().unwrap(), vec![ListenerState::Lost]);
        b.set_subscribed(true);
        assert_eq!(
            *states.lock().unwrap(),
            vec![ListenerState::Lost, ListenerState::Restored]
        );

        // Topology change: the replacement starts unsubscribed, so aborting the old
        // listener (dropping its entry) does not report a restore early.
        states.lock().unwrap().clear();
        let replacement = health.listener(true);
        drop(a);
        assert_eq!(*states.lock().unwrap(), vec![ListenerState::Lost]);
        replacement.set_subscribed(true);
        assert_eq!(
            *states.lock().unwrap(),
            vec![ListenerState::Lost, ListenerState::Restored]
        );

        // A lost shard whose listener is aborted no longer holds the node down.
        states.lock().unwrap().clear();
        b.set_subscribed(false);
        drop(b);
        assert_eq!(
            *states.lock().unwrap(),
            vec![ListenerState::Lost, ListenerState::Restored]
        );
    }

    #[test]
    fn recovery_waits_for_a_shard_that_has_never_subscribed() {
        let (health, states) = recording_health();
        let a = health.listener(true);
        let b = health.listener(true);

        a.set_subscribed(true);
        a.set_subscribed(false);
        a.set_subscribed(true);
        assert_eq!(*states.lock().unwrap(), vec![ListenerState::Lost]);

        b.set_subscribed(true);
        assert_eq!(
            *states.lock().unwrap(),
            vec![ListenerState::Lost, ListenerState::Restored],
        );
    }

    /// #566: a shard listener reconnects on its own, so it must report the outage itself
    /// and resubscribe promptly once Redis is back.
    #[tokio::test]
    async fn shard_listener_resubscribes_promptly_after_outage_and_reports_state() {
        use crate::transports::redis_transport::tests::{FakeRedisCounters, serve_fake_redis};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let counters = Arc::new(FakeRedisCounters::default());
        let server = serve_fake_redis(listener, false, counters.clone());

        let (health, states) = recording_health();
        let (fan_tx, _fan_rx): (ShardedPushSender, ShardedPushReceiver) = mpsc::bounded_async(16);
        let shutdown = Arc::new(Notify::new());
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(shard_listener_loop(ShardListenerParams {
            url: format!("redis://{address}/?protocol=resp3"),
            channels: vec!["sockudo#broadcast".to_string()],
            mode: PubSubMode::Standard,
            fan_in_tx: fan_tx,
            ready_tx: Some(ready_tx),
            is_running: Arc::new(AtomicBool::new(true)),
            shutdown: shutdown.clone(),
            shard_addr: address.to_string(),
            metrics: Arc::new(OnceLock::new()),
            refresh_notify: Arc::new(Notify::new()),
            tls: RedisTlsOptions::default(),
            ping_interval: Duration::from_millis(200),
            health: health.listener(false),
        }));

        tokio::time::timeout(Duration::from_secs(5), ready_rx)
            .await
            .expect("subscription timed out")
            .expect("listener exited before subscribing");
        assert!(
            states.lock().unwrap().is_empty(),
            "first subscription is not a restore"
        );

        // Redis goes away: every connection closes and new ones are refused.
        server.abort();
        let _ = server.await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while states.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "lost subscription not reported"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*states.lock().unwrap(), vec![ListenerState::Lost]);

        // Long enough for the old 500 ms doubling backoff to reach 4 s between attempts.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let listener = TcpListener::bind(address).await.unwrap();
        let server = serve_fake_redis(listener, false, counters.clone());
        let back_at = tokio::time::Instant::now();
        while states.lock().unwrap().len() < 2 {
            assert!(
                back_at.elapsed() < Duration::from_millis(PUBSUB_MAX_RETRY_DELAY_MS + 1_000),
                "shard listener did not resubscribe promptly after Redis came back"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            *states.lock().unwrap(),
            vec![ListenerState::Lost, ListenerState::Restored]
        );
        assert!(counters.subscribes.load(Ordering::SeqCst) >= 2);

        task.abort();
        server.abort();
    }
}
