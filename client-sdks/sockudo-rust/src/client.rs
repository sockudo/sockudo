//! The client handle and shared state.

use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use sockudo_ws::Message;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::channel::{Channel, ChannelInner, ChannelKind, SubscribeOptions};
use crate::codec::{self, Outbound};
use crate::delta::{DeltaManager, DeltaStats};
use crate::dispatcher::{BindingId, Dispatcher, EventStream};
use crate::error::{Error, Result};
use crate::event::Event;
use crate::options::{ClientBuilder, Config};
use crate::protocol::Prefix;
use crate::recovery::RecoveryPosition;
use crate::token::NormalizedToken;
use crate::user::{User, UserShared};

/// Connection lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Built, `connect` not yet called.
    Initialized,
    /// First connection attempt in progress.
    Connecting,
    /// Connected; `socket_id` is available.
    Connected,
    /// Reconnecting after a drop.
    Reconnecting,
    /// Connecting has taken longer than the unavailable timeout. Attempts
    /// continue in the background.
    Unavailable,
    /// Not connected and not retrying (manual disconnect, refusal, or
    /// exhausted reconnect budget). Call `connect` to start again.
    Disconnected,
    /// The connection cannot be established with this configuration.
    Failed,
}

impl ConnectionState {
    /// Lowercase name, matching the other SDKs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Initialized => "initialized",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Reconnecting => "reconnecting",
            Self::Unavailable => "unavailable",
            Self::Disconnected => "disconnected",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A state transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateChange {
    /// State before.
    pub previous: ConnectionState,
    /// State after.
    pub current: ConnectionState,
}

/// Callback for state transitions.
pub type StateCallback = Arc<dyn Fn(&StateChange) + Send + Sync>;
/// Callback for errors.
pub type ErrorCallback = Arc<dyn Fn(&Error) + Send + Sync>;

type Entries<T> = Arc<[(BindingId, Arc<T>)]>;

/// Copy-on-write callback list.
pub(crate) struct CallbackList<T: ?Sized> {
    list: Mutex<Entries<T>>,
}

impl<T: ?Sized> Default for CallbackList<T> {
    fn default() -> Self {
        Self {
            list: Mutex::new(Arc::from(Vec::new())),
        }
    }
}

impl<T: ?Sized> CallbackList<T> {
    pub(crate) fn add(&self, callback: Arc<T>) -> BindingId {
        let id = BindingId::next();
        let mut list = self.list.lock();
        let mut next: Vec<(BindingId, Arc<T>)> = list.iter().cloned().collect();
        next.push((id, callback));
        *list = Arc::from(next);
        id
    }

    pub(crate) fn remove(&self, id: BindingId) -> bool {
        let mut list = self.list.lock();
        if !list.iter().any(|(existing, _)| *existing == id) {
            return false;
        }
        let next: Vec<(BindingId, Arc<T>)> = list
            .iter()
            .filter(|(existing, _)| *existing != id)
            .cloned()
            .collect();
        *list = Arc::from(next);
        true
    }

    pub(crate) fn clear(&self) {
        *self.list.lock() = Arc::from(Vec::new());
    }

    pub(crate) fn snapshot(&self) -> Entries<T> {
        Arc::clone(&self.list.lock())
    }
}

/// Commands consumed by the connection task.
pub(crate) enum Command {
    /// Frames to send on the connection of `epoch`.
    Frames { epoch: u64, messages: Vec<Message> },
    /// Re-arm the capability-token refresh timer.
    RescheduleTokenRefresh { epoch: u64 },
    /// Close the connection and stop (only for the matching generation).
    Disconnect { generation: u64 },
}

pub(crate) struct ConnInfo {
    pub state: ConnectionState,
    pub socket_id: Option<String>,
    /// Incremented for every WebSocket session.
    pub epoch: u64,
    pub last_error: Option<String>,
    /// permessage-deflate is active on the current socket.
    pub compressed: bool,
}

#[derive(Default)]
pub(crate) struct TokenState {
    pub current: Option<NormalizedToken>,
    pub static_used: bool,
    pub refreshing: bool,
}

pub(crate) struct Lifecycle {
    pub task: Option<JoinHandle<()>>,
    pub generation: u64,
}

pub(crate) type ChannelMap = HashMap<String, Arc<ChannelInner>, ahash::RandomState>;

/// State shared by the public handles, the connection task and helper tasks.
pub(crate) struct Shared {
    pub config: Config,
    pub prefix: Prefix,
    pub dispatcher: Dispatcher,
    pub channels: RwLock<ChannelMap>,
    pub conn: Mutex<ConnInfo>,
    pub state_tx: watch::Sender<ConnectionState>,
    pub state_listeners: CallbackList<dyn Fn(&StateChange) + Send + Sync>,
    pub error_listeners: CallbackList<dyn Fn(&Error) + Send + Sync>,
    pub cmd_tx: mpsc::Sender<Command>,
    pub cmd_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Command>>>,
    pub lifecycle: Mutex<Lifecycle>,
    pub manual_disconnect: AtomicBool,
    pub generation: AtomicU64,
    pub shutdown: watch::Sender<bool>,
    pub positions: Mutex<HashMap<String, RecoveryPosition, ahash::RandomState>>,
    pub delta: Option<Mutex<DeltaManager>>,
    pub token: Mutex<TokenState>,
    pub user: UserShared,
    #[cfg(feature = "tls")]
    pub tls: std::sync::OnceLock<Arc<rustls::ClientConfig>>,
    #[cfg(feature = "http")]
    pub http: reqwest::Client,
    #[cfg(feature = "http")]
    pub timeline: Option<crate::stats::Timeline>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("config", &self.config)
            .field("channels", &self.channels.read().len())
            .finish_non_exhaustive()
    }
}

impl Shared {
    pub(crate) fn state(&self) -> ConnectionState {
        self.conn.lock().state
    }

    pub(crate) fn socket_id(&self) -> Option<String> {
        self.conn.lock().socket_id.clone()
    }

    /// The epoch of the current connection, if connected.
    pub(crate) fn connected_epoch(&self) -> Option<u64> {
        let conn = self.conn.lock();
        (conn.state == ConnectionState::Connected).then_some(conn.epoch)
    }

    pub(crate) fn set_state(&self, next: ConnectionState) {
        let previous = {
            let mut conn = self.conn.lock();
            let previous = conn.state;
            if previous == next {
                return;
            }
            conn.state = next;
            if next != ConnectionState::Connected {
                conn.socket_id = None;
            }
            previous
        };
        tracing::debug!(from = %previous, to = %next, "connection state");
        self.log_timeline(|| sonic_rs::json!({ "state": next.as_str() }));
        self.state_tx.send_replace(next);
        let change = StateChange {
            previous,
            current: next,
        };
        for (_, callback) in self.state_listeners.snapshot().iter() {
            if catch_unwind(AssertUnwindSafe(|| callback(&change))).is_err() {
                tracing::error!("state callback panicked");
            }
        }
    }

    pub(crate) fn report_error(&self, error: Error) {
        tracing::debug!(%error, "client error");
        self.log_timeline(|| sonic_rs::json!({ "error": error.to_string() }));
        self.conn.lock().last_error = Some(error.to_string());
        for (_, callback) in self.error_listeners.snapshot().iter() {
            if catch_unwind(AssertUnwindSafe(|| callback(&error))).is_err() {
                tracing::error!("error callback panicked");
            }
        }
    }

    /// Record a stats-timeline event when `enable_stats` is on.
    pub(crate) fn log_timeline(&self, event: impl FnOnce() -> sonic_rs::Value) {
        #[cfg(feature = "http")]
        if let Some(timeline) = self.timeline.as_ref() {
            timeline.log(event());
        }
        #[cfg(not(feature = "http"))]
        let _ = event;
    }

    /// Ask the stats task to send a bundle now (on connect).
    pub(crate) fn flush_timeline(&self) {
        #[cfg(feature = "http")]
        if let Some(timeline) = self.timeline.as_ref() {
            timeline.wake.notify_one();
        }
    }

    pub(crate) fn encode(
        &self,
        event: &str,
        channel: Option<&str>,
        data: &[u8],
    ) -> Result<Message> {
        codec::encode(
            Outbound {
                event,
                channel,
                data,
            },
            self.config.wire_format,
        )
    }

    /// Queue frames without waiting; overflows are handed to a task so
    /// ordering relative to later sends is preserved by the bounded channel.
    pub(crate) fn enqueue(&self, command: Command) {
        match self.cmd_tx.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let sender = self.cmd_tx.clone();
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move {
                        let _ = sender.send(command).await;
                    });
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    pub(crate) async fn send_frames(&self, epoch: u64, messages: Vec<Message>) -> Result<()> {
        self.cmd_tx
            .send(Command::Frames { epoch, messages })
            .await
            .map_err(|_| Error::Closed)
    }

    pub(crate) fn try_send_frames(&self, epoch: u64, messages: Vec<Message>) -> Result<()> {
        self.cmd_tx
            .try_send(Command::Frames { epoch, messages })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Backpressure,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })
    }

    pub(crate) fn channel(&self, name: &str) -> Option<Arc<ChannelInner>> {
        self.channels.read().get(name).cloned()
    }

    pub(crate) fn update_position(&self, channel: &str, event: &Event) {
        if let Some(serial) = event.serial {
            self.positions.lock().insert(
                channel.to_owned(),
                RecoveryPosition {
                    serial,
                    stream_id: event.stream_id.clone(),
                    last_message_id: event.message_id.clone(),
                },
            );
        }
    }

    #[cfg(feature = "tls")]
    pub(crate) fn tls_config(&self) -> Result<Arc<rustls::ClientConfig>> {
        if let Some(config) = self.config.tls_config.as_ref() {
            return Ok(Arc::clone(config));
        }
        if let Some(config) = self.tls.get() {
            return Ok(Arc::clone(config));
        }
        let config = crate::transport::default_tls_config()?;
        Ok(Arc::clone(self.tls.get_or_init(|| config)))
    }
}

/// Keeps the connection task alive while at least one client handle exists.
struct Handle {
    shared: Arc<Shared>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.shared.shutdown.send_replace(true);
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.shared.fmt(f)
    }
}

/// An async Sockudo realtime client.
///
/// Cheap to clone; all clones share one connection. Dropping the last clone
/// closes the connection.
///
/// ```no_run
/// use sockudo_client::{SockudoClient, Result};
///
/// # async fn run() -> Result<()> {
/// let client = SockudoClient::builder("app-key")
///     .ws_host("127.0.0.1")
///     .ws_port(6001)
///     .force_tls(false)
///     .build()?;
///
/// let channel = client.subscribe("prices");
/// channel.bind("price-updated", |event| {
///     println!("{:?}", event.value());
/// });
///
/// client.connect()?;
/// let socket_id = client.connected().await?;
/// println!("connected as {socket_id}");
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct SockudoClient {
    handle: Arc<Handle>,
}

impl SockudoClient {
    /// Start configuring a client for `app_key`.
    pub fn builder(app_key: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(app_key)
    }

    pub(crate) fn from_config(config: Config) -> Result<Self> {
        #[cfg(feature = "http")]
        let http = match config.http_client.clone() {
            Some(client) => client,
            None => reqwest::Client::builder()
                .user_agent(concat!("sockudo-client-rust/", env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|error| Error::HttpTransport(error.to_string()))?,
        };
        let (cmd_tx, cmd_rx) = mpsc::channel(config.outbound_queue_capacity);
        let delta = config
            .delta
            .clone()
            .map(|options| Mutex::new(DeltaManager::new(options, config.max_message_size)));
        let shared = Arc::new(Shared {
            prefix: config.protocol.prefix(),
            dispatcher: Dispatcher::default(),
            channels: RwLock::new(ChannelMap::default()),
            conn: Mutex::new(ConnInfo {
                state: ConnectionState::Initialized,
                socket_id: None,
                epoch: 0,
                last_error: None,
                compressed: false,
            }),
            state_tx: watch::Sender::new(ConnectionState::Initialized),
            state_listeners: CallbackList::default(),
            error_listeners: CallbackList::default(),
            cmd_tx,
            cmd_rx: Arc::new(tokio::sync::Mutex::new(cmd_rx)),
            lifecycle: Mutex::new(Lifecycle {
                task: None,
                generation: 0,
            }),
            manual_disconnect: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            shutdown: watch::Sender::new(false),
            positions: Mutex::new(HashMap::default()),
            delta,
            token: Mutex::new(TokenState::default()),
            user: UserShared::default(),
            #[cfg(feature = "tls")]
            tls: std::sync::OnceLock::new(),
            #[cfg(feature = "http")]
            http,
            #[cfg(feature = "http")]
            timeline: config
                .enable_stats
                .then(|| crate::stats::Timeline::new(config.stats_host.clone())),
            config,
        });
        Ok(Self {
            handle: Arc::new(Handle { shared }),
        })
    }

    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.handle.shared
    }

    /// The app key.
    pub fn key(&self) -> &str {
        &self.shared().config.key
    }

    /// The event-name prefixes for the configured protocol.
    pub fn prefix(&self) -> Prefix {
        self.shared().prefix
    }

    /// Current connection state.
    pub fn state(&self) -> ConnectionState {
        self.shared().state()
    }

    /// Socket id of the current connection.
    pub fn socket_id(&self) -> Option<String> {
        self.shared().socket_id()
    }

    /// Whether permessage-deflate compression is active on the current socket.
    pub fn is_compressed(&self) -> bool {
        self.shared().conn.lock().compressed
    }

    /// Watch state changes as a `tokio::sync::watch` receiver.
    pub fn state_watcher(&self) -> watch::Receiver<ConnectionState> {
        self.shared().state_tx.subscribe()
    }

    /// Start connecting in the background. Idempotent while connecting or
    /// connected. Requires a Tokio runtime.
    pub fn connect(&self) -> Result<()> {
        let shared = self.shared();
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::unsupported("connect() must be called within a Tokio runtime"))?;
        let mut lifecycle = shared.lifecycle.lock();
        let running = lifecycle
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished());
        if running && !shared.manual_disconnect.load(Ordering::SeqCst) {
            return Ok(());
        }
        lifecycle.generation += 1;
        let generation = lifecycle.generation;
        shared.generation.store(generation, Ordering::SeqCst);
        shared.manual_disconnect.store(false, Ordering::SeqCst);
        shared.conn.lock().last_error = None;
        shared.set_state(ConnectionState::Connecting);
        let task_shared = Arc::clone(shared);
        lifecycle.task = Some(runtime.spawn(crate::connection::run(task_shared, generation)));
        Ok(())
    }

    /// Wait until connected and return the socket id.
    ///
    /// Fails when the client stops trying (manual disconnect, refusal,
    /// exhausted reconnect budget or failure). Wrap in a timeout to bound the
    /// wait.
    pub async fn connected(&self) -> Result<String> {
        let mut receiver = self.state_watcher();
        loop {
            let state = *receiver.borrow_and_update();
            match state {
                ConnectionState::Connected => {
                    if let Some(socket_id) = self.socket_id() {
                        return Ok(socket_id);
                    }
                }
                ConnectionState::Disconnected | ConnectionState::Failed => {
                    let reason = self.shared().conn.lock().last_error.clone();
                    return Err(Error::Disconnected { reason });
                }
                _ => {}
            }
            if receiver.changed().await.is_err() {
                return Err(Error::Closed);
            }
        }
    }

    /// Close the connection and stop reconnecting. Channels stay registered
    /// and are resubscribed by the next `connect`.
    pub async fn disconnect(&self) {
        let shared = self.shared();
        shared.manual_disconnect.store(true, Ordering::SeqCst);
        let generation = shared.generation.load(Ordering::SeqCst);
        let task = shared.lifecycle.lock().task.take();
        if let Some(task) = task {
            let _ = shared.cmd_tx.send(Command::Disconnect { generation }).await;
            let _ = task.await;
        }
        shared.set_state(ConnectionState::Disconnected);
    }

    /// Subscribe to a channel (or return the existing subscription).
    ///
    /// The channel type follows its name prefix: `private-`, `presence-`,
    /// `private-encrypted-`, otherwise public. Subscribing happens as soon
    /// as the connection is up and again after every reconnect.
    pub fn subscribe(&self, name: impl AsRef<str>) -> Channel {
        crate::channel::subscribe(self.shared(), name.as_ref(), None)
    }

    /// Subscribe with Protocol V2 options (filters, delta, rewind,
    /// annotations). Options replace those of an existing subscription and
    /// apply from its next subscribe.
    pub fn subscribe_with(
        &self,
        name: impl AsRef<str>,
        options: SubscribeOptions,
    ) -> Result<Channel> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(Error::invalid("channel name must not be empty"));
        }
        options.validate(self.shared().config.protocol)?;
        Ok(crate::channel::subscribe(
            self.shared(),
            name,
            Some(options),
        ))
    }

    /// Unsubscribe and forget a channel.
    pub fn unsubscribe(&self, name: &str) {
        crate::channel::unsubscribe(self.shared(), name);
    }

    /// Look up a subscribed channel.
    pub fn channel(&self, name: &str) -> Option<Channel> {
        self.shared()
            .channel(name)
            .filter(|channel| channel.kind != ChannelKind::ServerToUser)
            .map(Channel::from_inner)
    }

    /// All subscribed channels.
    pub fn channels(&self) -> Vec<Channel> {
        self.shared()
            .channels
            .read()
            .values()
            .filter(|channel| channel.kind != ChannelKind::ServerToUser)
            .cloned()
            .map(Channel::from_inner)
            .collect()
    }

    /// Bind a callback to an event name across all channels.
    pub fn bind(
        &self,
        event: &str,
        callback: impl Fn(&Event) + Send + Sync + 'static,
    ) -> BindingId {
        self.shared().dispatcher.bind(event, Arc::new(callback))
    }

    /// Bind a callback to every non-internal event.
    pub fn bind_global(&self, callback: impl Fn(&Event) + Send + Sync + 'static) -> BindingId {
        self.shared().dispatcher.bind_global(Arc::new(callback))
    }

    /// Stream every non-internal event (bounded; see [`EventStream`]).
    pub fn stream(&self, capacity: usize) -> EventStream {
        self.make_stream(None, capacity)
    }

    /// Stream one event name across all channels.
    pub fn stream_event(&self, event: &str, capacity: usize) -> EventStream {
        self.make_stream(Some(event), capacity)
    }

    fn make_stream(&self, event: Option<&str>, capacity: usize) -> EventStream {
        let (id, receiver, dropped) = self.shared().dispatcher.bind_stream(event, capacity);
        let weak: Weak<Shared> = Arc::downgrade(self.shared());
        EventStream::new(
            id,
            receiver,
            dropped,
            Box::new(move |id| {
                if let Some(shared) = weak.upgrade() {
                    shared.dispatcher.unbind(id);
                }
            }),
        )
    }

    /// Observe connection state transitions.
    pub fn on_state_change(
        &self,
        callback: impl Fn(&StateChange) + Send + Sync + 'static,
    ) -> BindingId {
        self.shared().state_listeners.add(Arc::new(callback))
    }

    /// Observe errors: server `error` events, auth failures, decode
    /// failures, refusals, token expiry/revocation and similar.
    pub fn on_error(&self, callback: impl Fn(&Error) + Send + Sync + 'static) -> BindingId {
        self.shared().error_listeners.add(Arc::new(callback))
    }

    /// Remove any binding made on this client (event, global, stream, state
    /// or error).
    pub fn unbind(&self, id: BindingId) -> bool {
        let shared = self.shared();
        shared.dispatcher.unbind(id)
            || shared.state_listeners.remove(id)
            || shared.error_listeners.remove(id)
    }

    /// Remove every callback for one event name.
    pub fn unbind_event(&self, event: &str) {
        self.shared().dispatcher.unbind_event(event);
    }

    /// Remove every event, state and error binding on this client.
    pub fn unbind_all(&self) {
        let shared = self.shared();
        shared.dispatcher.unbind_all();
        shared.state_listeners.clear();
        shared.error_listeners.clear();
    }

    /// Send a raw event. `data` is serialized to JSON.
    pub async fn send_event<T: Serialize + ?Sized>(
        &self,
        event: &str,
        data: &T,
        channel: Option<&str>,
    ) -> Result<()> {
        let shared = self.shared();
        let epoch = shared.connected_epoch().ok_or(Error::NotConnected)?;
        let message = shared.encode(event, channel, &codec::to_json(data)?)?;
        shared.send_frames(epoch, vec![message]).await
    }

    /// Send a raw event without waiting for queue space.
    pub fn try_send_event<T: Serialize + ?Sized>(
        &self,
        event: &str,
        data: &T,
        channel: Option<&str>,
    ) -> Result<()> {
        let shared = self.shared();
        let epoch = shared.connected_epoch().ok_or(Error::NotConnected)?;
        let message = shared.encode(event, channel, &codec::to_json(data)?)?;
        shared.try_send_frames(epoch, vec![message])
    }

    /// The user facade (sign-in, server-to-user events, watchlist).
    pub fn user(&self) -> User {
        User::new(Arc::clone(self.shared()))
    }

    /// Shorthand for `user().sign_in()`.
    pub fn sign_in(&self) -> Result<()> {
        self.user().sign_in()
    }

    /// Delta compression statistics, when delta compression is configured.
    pub fn delta_stats(&self) -> Option<DeltaStats> {
        self.shared()
            .delta
            .as_ref()
            .map(|delta| delta.lock().stats())
    }

    /// Whether the server confirmed delta compression on this connection.
    pub fn is_delta_compression_enabled(&self) -> bool {
        self.shared()
            .delta
            .as_ref()
            .is_some_and(|delta| delta.lock().is_server_enabled())
    }

    /// Reset delta compression statistics.
    pub fn reset_delta_stats(&self) {
        if let Some(delta) = self.shared().delta.as_ref() {
            delta.lock().reset_stats();
        }
    }

    /// The recovery position of one channel.
    pub fn recovery_position(&self, channel: &str) -> Option<RecoveryPosition> {
        self.shared().positions.lock().get(channel).cloned()
    }

    /// All recovery positions (persist these to resume after a restart).
    pub fn recovery_positions(&self) -> HashMap<String, RecoveryPosition> {
        self.shared()
            .positions
            .lock()
            .iter()
            .map(|(channel, position)| (channel.clone(), position.clone()))
            .collect()
    }

    /// Set or clear one channel's recovery position.
    pub fn set_recovery_position(&self, channel: &str, position: Option<RecoveryPosition>) {
        let mut positions = self.shared().positions.lock();
        match position {
            Some(position) => {
                positions.insert(channel.to_owned(), position);
            }
            None => {
                positions.remove(channel);
            }
        }
    }

    /// Replace all recovery positions.
    pub fn set_recovery_positions(
        &self,
        positions: impl IntoIterator<Item = (String, RecoveryPosition)>,
    ) {
        let mut current = self.shared().positions.lock();
        current.clear();
        current.extend(positions);
    }

    /// Versioned-message mutations through the configured proxy.
    pub fn versioned_messages(&self) -> crate::rest::VersionedMessages {
        crate::rest::VersionedMessages::new(Arc::downgrade(self.shared()))
    }
}

impl ClientBuilder {
    /// Validate the options and build the client. Does not connect.
    pub fn build(self) -> Result<SockudoClient> {
        SockudoClient::from_config(self.into_config()?)
    }
}
