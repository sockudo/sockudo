use super::capabilities::{ConnectionCapabilities, UserInfo};
use super::socket_id::SocketId;
use crate::app::App;
use crate::capability_token::TokenAuthContext;
use crate::channel::PresenceMemberInfo;
use ahash::AHashMap as HashMap;
use sockudo_filter::FilterNode;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sonic_rs::Value;
use std::time::Instant;
use tokio::task::JoinHandle;

/// Stable, content-free reason for a connection reaching terminal cleanup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DisconnectCause {
    ClientClose,
    TransportEof,
    TransportError,
    FatalError,
    ActivityTimeout,
    AuthTimeout,
    TokenExpired,
    TokenRevoked,
    AdministrativeClose,
    ServerShutdown,
    #[default]
    Unknown,
}

impl DisconnectCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientClose => "client_close",
            Self::TransportEof => "transport_eof",
            Self::TransportError => "transport_error",
            Self::FatalError => "fatal_error",
            Self::ActivityTimeout => "activity_timeout",
            Self::AuthTimeout => "auth_timeout",
            Self::TokenExpired => "token_expired",
            Self::TokenRevoked => "token_revoked",
            Self::AdministrativeClose => "administrative_close",
            Self::ServerShutdown => "server_shutdown",
            Self::Unknown => "unknown",
        }
    }

    fn may_be_replaced(self) -> bool {
        matches!(self, Self::Unknown | Self::TransportEof)
    }
}

#[derive(Debug)]
pub struct ConnectionTimeouts {
    pub activity_timeout_handle: Option<JoinHandle<()>>,
    pub auth_timeout_handle: Option<JoinHandle<()>>,
    /// Task that fires when a capability (V2 auth) token reaches its `exp`. It
    /// captures a `ConnectionHandler` clone and sleeps until expiry, so it must be
    /// aborted on disconnect to avoid retaining that state for the token's lifetime.
    pub token_expiry_handle: Option<JoinHandle<()>>,
}

impl Default for ConnectionTimeouts {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionTimeouts {
    pub fn new() -> Self {
        Self {
            activity_timeout_handle: None,
            auth_timeout_handle: None,
            token_expiry_handle: None,
        }
    }

    pub fn clear_activity_timeout(&mut self) {
        if let Some(handle) = self.activity_timeout_handle.take() {
            handle.abort();
        }
    }

    pub fn clear_auth_timeout(&mut self) {
        if let Some(handle) = self.auth_timeout_handle.take() {
            handle.abort();
        }
    }

    pub fn clear_token_expiry(&mut self) {
        if let Some(handle) = self.token_expiry_handle.take() {
            handle.abort();
        }
    }

    pub fn clear_all(&mut self) {
        self.clear_activity_timeout();
        self.clear_auth_timeout();
        self.clear_token_expiry();
    }
}

impl Drop for ConnectionTimeouts {
    fn drop(&mut self) {
        self.clear_all();
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConnectionStatus {
    Active,
    PingSent(Instant),
    Closing,
    Closed,
}

#[derive(Debug)]
pub struct ConnectionState {
    pub socket_id: SocketId,
    pub app: Option<App>,
    pub subscribed_channels: HashMap<String, Option<FilterNode>>,
    pub user_id: Option<String>,
    pub user_info: Option<UserInfo>,
    pub connection_capabilities: Option<ConnectionCapabilities>,
    pub token_auth_context: Option<TokenAuthContext>,
    pub connection_meta: Option<Value>,
    pub last_ping: Instant,
    pub presence: Option<HashMap<String, PresenceMemberInfo>>,
    pub user: Option<Value>,
    pub timeouts: ConnectionTimeouts,
    pub status: ConnectionStatus,
    pub disconnecting: bool,
    pub disconnect_cause: DisconnectCause,
    pub delta_compression_enabled: bool,
    pub protocol_version: ProtocolVersion,
    pub wire_format: WireFormat,
    /// V2 only. Whether the publisher receives their own messages back.
    /// Default: true (echo enabled). Set from sockudo:connect options.
    pub echo_messages: bool,
    /// V2 only. Whether live append frames carry deltas or full snapshots.
    pub append_mode: sockudo_protocol::AppendMode,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionState {
    pub fn new() -> Self {
        Self {
            socket_id: SocketId::new(),
            app: None,
            subscribed_channels: HashMap::new(),
            user_id: None,
            user_info: None,
            connection_capabilities: None,
            token_auth_context: None,
            connection_meta: None,
            last_ping: Instant::now(),
            presence: None,
            user: None,
            timeouts: ConnectionTimeouts::new(),
            status: ConnectionStatus::Active,
            disconnecting: false,
            disconnect_cause: DisconnectCause::Unknown,
            delta_compression_enabled: false,
            protocol_version: ProtocolVersion::V1,
            wire_format: WireFormat::Json,
            echo_messages: true,
            append_mode: sockudo_protocol::AppendMode::Delta,
        }
    }

    pub fn with_socket_id(socket_id: SocketId) -> Self {
        Self {
            socket_id,
            app: None,
            subscribed_channels: HashMap::new(),
            user_id: None,
            user_info: None,
            connection_capabilities: None,
            token_auth_context: None,
            connection_meta: None,
            last_ping: Instant::now(),
            presence: None,
            user: None,
            timeouts: ConnectionTimeouts::new(),
            status: ConnectionStatus::Active,
            disconnecting: false,
            disconnect_cause: DisconnectCause::Unknown,
            delta_compression_enabled: false,
            protocol_version: ProtocolVersion::V1,
            wire_format: WireFormat::Json,
            echo_messages: true,
            append_mode: sockudo_protocol::AppendMode::Delta,
        }
    }

    pub fn with_protocol_version(mut self, version: ProtocolVersion) -> Self {
        self.protocol_version = version;
        self
    }

    pub fn with_wire_format(mut self, format: WireFormat) -> Self {
        self.wire_format = format;
        self
    }

    pub fn is_presence(&self) -> bool {
        self.presence.is_some()
    }

    pub fn is_subscribed(&self, channel: &str) -> bool {
        self.subscribed_channels.contains_key(channel)
    }

    pub fn add_subscription(&mut self, channel: String) {
        self.subscribed_channels.insert(channel, None);
    }

    pub fn add_subscription_with_filter(&mut self, channel: String, filter: Option<FilterNode>) {
        self.subscribed_channels.insert(channel, filter);
    }

    pub fn get_channel_filter(&self, channel: &str) -> Option<&FilterNode> {
        self.subscribed_channels
            .get(channel)
            .and_then(|f| f.as_ref())
    }

    pub fn remove_subscription(&mut self, channel: &str) -> bool {
        self.subscribed_channels.remove(channel).is_some()
    }

    pub fn get_subscribed_channels_list(&self) -> Vec<String> {
        self.subscribed_channels.keys().cloned().collect()
    }

    pub fn update_ping(&mut self) {
        self.last_ping = Instant::now();
    }

    /// A ping or pong proves the peer is alive, but must not reopen a connection this node has
    /// already closed: that would allow sends after the Close frame again.
    pub fn mark_alive(&mut self) {
        if !self.is_closed_by_server() {
            self.status = ConnectionStatus::Active;
        }
    }

    /// Whether this node has sent, or started sending, its own Close frame.
    pub fn is_closed_by_server(&self) -> bool {
        matches!(
            self.status,
            ConnectionStatus::Closing | ConnectionStatus::Closed
        )
    }

    pub fn get_app_key(&self) -> String {
        self.app
            .as_ref()
            .map(|app| app.key.clone())
            .unwrap_or_default()
    }

    pub fn get_app_id(&self) -> String {
        self.app
            .as_ref()
            .map(|app| app.id.clone())
            .unwrap_or_default()
    }

    pub fn time_since_last_ping(&self) -> std::time::Duration {
        self.last_ping.elapsed()
    }

    pub fn is_authenticated(&self) -> bool {
        self.user.is_some()
    }

    pub fn clear_timeouts(&mut self) {
        self.timeouts.clear_all();
    }

    /// Records the first specific disconnect cause. A provisional EOF may be
    /// refined when a more precise cause is observed during cleanup.
    pub fn record_disconnect_cause(&mut self, cause: DisconnectCause) {
        if cause != DisconnectCause::Unknown && self.disconnect_cause.may_be_replaced() {
            self.disconnect_cause = cause;
        }
    }
}

impl PartialEq for ConnectionState {
    fn eq(&self, other: &Self) -> bool {
        self.socket_id == other.socket_id
    }
}

#[cfg(test)]
mod disconnect_cause_tests {
    use super::*;

    #[test]
    fn specific_disconnect_cause_is_not_overwritten() {
        let mut state = ConnectionState::new();
        state.record_disconnect_cause(DisconnectCause::TransportEof);
        state.record_disconnect_cause(DisconnectCause::ActivityTimeout);
        state.record_disconnect_cause(DisconnectCause::FatalError);
        assert_eq!(state.disconnect_cause, DisconnectCause::ActivityTimeout);
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;

    #[test]
    fn liveness_does_not_reopen_a_server_closed_connection() {
        let mut state = ConnectionState::new();
        state.status = ConnectionStatus::PingSent(Instant::now());
        state.mark_alive();
        assert_eq!(state.status, ConnectionStatus::Active);
        assert!(!state.is_closed_by_server());

        for closed in [ConnectionStatus::Closing, ConnectionStatus::Closed] {
            state.status = closed;
            state.mark_alive();
            assert_eq!(state.status, closed);
            assert!(state.is_closed_by_server());
        }
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;

    // `clear_all` (called on disconnect) must abort the token-expiry task.
    #[tokio::test]
    async fn clear_all_aborts_token_expiry_task() {
        let mut timeouts = ConnectionTimeouts::new();
        let handle = tokio::spawn(async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });
        // Independent probe to observe abort (the handle itself is taken by clear).
        let abort = handle.abort_handle();
        timeouts.token_expiry_handle = Some(handle);

        timeouts.clear_all();

        assert!(timeouts.token_expiry_handle.is_none());
        // Let the runtime process the abort, then confirm the task is gone.
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }
}
