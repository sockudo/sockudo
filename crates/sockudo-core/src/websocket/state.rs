use super::capabilities::{ConnectionCapabilities, UserInfo};
use super::socket_id::SocketId;
use crate::app::App;
use crate::capability_token::TokenAuthContext;
use crate::channel::PresenceMemberInfo;
use crate::error::{Error, Result};
use ahash::AHashMap as HashMap;
use sockudo_filter::FilterNode;
use sockudo_protocol::{ProtocolVersion, WireFormat};
use sonic_rs::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuardRef};

/// Stable, content-free reason for a connection reaching terminal cleanup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DisconnectCause {
    ClientClose,
    TransportEof,
    TransportClosed,
    PeerTransportLost,
    TransportError,
    ProtocolError,
    FatalError,
    ActivityTimeout,
    HeartbeatTimeout,
    IdleTimeout,
    AuthTimeout,
    TokenExpired,
    TokenRevoked,
    AdministrativeClose,
    ServerShutdown,
    #[default]
    Unknown,
}

impl DisconnectCause {
    pub const ALL: [Self; 16] = [
        Self::ClientClose,
        Self::TransportEof,
        Self::TransportClosed,
        Self::PeerTransportLost,
        Self::TransportError,
        Self::ProtocolError,
        Self::FatalError,
        Self::ActivityTimeout,
        Self::HeartbeatTimeout,
        Self::IdleTimeout,
        Self::AuthTimeout,
        Self::TokenExpired,
        Self::TokenRevoked,
        Self::AdministrativeClose,
        Self::ServerShutdown,
        Self::Unknown,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ClientClose => "client_close",
            Self::TransportEof => "transport_eof",
            Self::TransportClosed => "transport_closed",
            Self::PeerTransportLost => "peer_transport_lost",
            Self::TransportError => "transport_error",
            Self::ProtocolError => "protocol_error",
            Self::FatalError => "fatal_error",
            Self::ActivityTimeout => "activity_timeout",
            Self::HeartbeatTimeout => "heartbeat_timeout",
            Self::IdleTimeout => "idle_timeout",
            Self::AuthTimeout => "auth_timeout",
            Self::TokenExpired => "token_expired",
            Self::TokenRevoked => "token_revoked",
            Self::AdministrativeClose => "administrative_close",
            Self::ServerShutdown => "server_shutdown",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_websocket_error(error: &sockudo_ws::Error) -> Self {
        use sockudo_ws::Error;
        use std::io::ErrorKind;

        match error {
            Error::HeartbeatTimeout => Self::HeartbeatTimeout,
            Error::IdleTimeout => Self::IdleTimeout,
            Error::ConnectionClosed => Self::TransportClosed,
            Error::ConnectionReset => Self::PeerTransportLost,
            Error::Closed(_) => Self::ClientClose,
            Error::Io(error)
                if matches!(
                    error.kind(),
                    ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::BrokenPipe
                        | ErrorKind::UnexpectedEof
                ) =>
            {
                Self::PeerTransportLost
            }
            Error::InvalidFrame(_)
            | Error::InvalidUtf8
            | Error::Protocol(_)
            | Error::InvalidCloseCode(_)
            | Error::MessageTooLarge
            | Error::FrameTooLarge => Self::ProtocolError,
            _ => Self::TransportError,
        }
    }

    /// Whether a Close write found a peer or transport that was already gone.
    pub fn is_peer_or_transport_gone(self) -> bool {
        matches!(
            self,
            Self::ClientClose | Self::TransportClosed | Self::PeerTransportLost
        )
    }

    /// A definite peer-side end, not a generic closure that can also be
    /// reported when the transport aborts an in-progress local Close.
    pub fn is_confirmed_peer_loss(self) -> bool {
        matches!(self, Self::ClientClose | Self::PeerTransportLost)
    }

    pub(super) fn is_native_timeout(self) -> bool {
        matches!(self, Self::HeartbeatTimeout | Self::IdleTimeout)
    }

    fn is_transport_end(self) -> bool {
        matches!(
            self,
            Self::ClientClose
                | Self::TransportEof
                | Self::TransportClosed
                | Self::PeerTransportLost
                | Self::TransportError
                | Self::ProtocolError
        ) || self.is_native_timeout()
    }

    fn may_be_replaced(self) -> bool {
        matches!(
            self,
            Self::Unknown | Self::TransportEof | Self::TransportClosed
        )
    }
}

/// Observations shared by the reader, writer, and local close initiators.
#[derive(Debug, Default)]
pub struct SocketTermination {
    terminal: Mutex<TerminalState>,
    pub(super) reader_attached: AtomicBool,
    pub(super) close_requested: AtomicBool,
    shutdown_token: Option<CancellationToken>,
    pub(super) writer_stopped: CancellationToken,
}

#[derive(Debug, Default)]
struct TerminalState {
    observed: DisconnectCause,
    writer_failure: Option<WriterFailure>,
}

impl TerminalState {
    fn record(&mut self, cause: DisconnectCause) {
        if cause != DisconnectCause::Unknown
            && ((self.observed.may_be_replaced()
                && (self.observed != DisconnectCause::TransportEof
                    || cause != DisconnectCause::TransportClosed))
                || (cause.is_native_timeout() && !self.observed.is_transport_end()))
        {
            self.observed = cause;
        }
    }

    fn close_error(&self) -> Option<Error> {
        let cause = if self.observed.is_native_timeout() {
            self.observed
        } else {
            self.writer_failure.as_ref()?.cause
        };
        Some(match cause {
            DisconnectCause::HeartbeatTimeout => {
                Error::WebSocket(sockudo_ws::Error::HeartbeatTimeout)
            }
            DisconnectCause::IdleTimeout => Error::WebSocket(sockudo_ws::Error::IdleTimeout),
            DisconnectCause::TransportClosed
                if self
                    .writer_failure
                    .as_ref()
                    .is_some_and(|failure| failure.during_close) =>
            {
                Error::WebSocket(sockudo_ws::Error::ConnectionClosed)
            }
            cause if cause.is_peer_or_transport_gone() => {
                Error::ConnectionClosed(cause.as_str().into())
            }
            cause => Error::Connection(format!("websocket writer failed: {}", cause.as_str())),
        })
    }
}

#[derive(Debug)]
struct WriterFailure {
    cause: DisconnectCause,
    during_close: bool,
}

impl SocketTermination {
    pub(super) fn new(shutdown_token: Option<CancellationToken>) -> Self {
        Self {
            shutdown_token,
            ..Self::default()
        }
    }

    pub fn shutdown_token(&self) -> Option<&CancellationToken> {
        self.shutdown_token.as_ref()
    }

    /// Keep the transport alive for the reader until the returned guard drops.
    #[must_use]
    pub fn attach_reader(&self) -> Option<DropGuardRef<'_>> {
        let token = self.shutdown_token()?;
        self.reader_attached.store(true, Ordering::Release);
        Some(token.drop_guard_ref())
    }

    /// Publish a terminal observation before awaiting teardown. A generic EOF or
    /// a writer finding the transport closed cannot displace a specific cause.
    pub fn record(&self, cause: DisconnectCause) {
        if cause == DisconnectCause::Unknown {
            return;
        }
        self.terminal().record(cause);
    }

    fn terminal(&self) -> MutexGuard<'_, TerminalState> {
        self.terminal.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Serialize local intent with the reader's terminal observation. EOF may
    // still be refined by an actual write error, not by a subsequent close.
    pub(super) fn begin_local_close(&self, cause: DisconnectCause) -> Result<()> {
        let mut terminal = self.terminal();
        if let Some(error) = terminal.close_error() {
            return Err(error);
        }
        if terminal.observed.is_transport_end() {
            return Err(Error::ConnectionClosed(terminal.observed.as_str().into()));
        }
        if self.writer_has_stopped() {
            return Err(Error::ConnectionClosed(
                "WebSocket writer already stopped".into(),
            ));
        }
        if terminal.observed == DisconnectCause::Unknown {
            terminal.observed = cause;
        }
        self.close_requested.store(true, Ordering::Release);
        Ok(())
    }

    pub fn cause(&self) -> DisconnectCause {
        let terminal = self.terminal();
        match terminal.observed {
            DisconnectCause::Unknown => terminal
                .writer_failure
                .as_ref()
                .map_or(DisconnectCause::Unknown, |failure| failure.cause),
            cause => cause,
        }
    }

    /// Wait for the writer to stop taking frames and publish its last failure.
    /// The caller must first stop the idle writer with the socket shutdown token.
    pub(super) async fn wait_for_writer(&self) {
        self.writer_stopped.cancelled().await;
    }

    /// Resolve a terminal EOF before reporting the session outcome. The split
    /// transport may yield EOF just before an in-flight write returns an error.
    pub async fn finish_reader_eof(&self) {
        self.record(DisconnectCause::TransportEof);
        if let Some(token) = self.shutdown_token() {
            token.cancel();
            self.wait_for_writer().await;
        }
    }

    pub(super) fn writer_failure(&self) -> Option<DisconnectCause> {
        self.terminal()
            .writer_failure
            .as_ref()
            .map(|failure| failure.cause)
    }

    pub(super) fn writer_has_stopped(&self) -> bool {
        self.writer_stopped.is_cancelled()
            || self
                .shutdown_token()
                .is_some_and(CancellationToken::is_cancelled)
    }

    pub(super) fn writer_unavailable(&self) -> bool {
        let terminal = self.terminal();
        terminal.writer_failure.is_some()
            || terminal.observed.is_transport_end()
            || self.writer_has_stopped()
    }

    pub(super) fn preserve_reader_after_close_error(&self, error: &crate::error::Error) -> bool {
        self.reader_attached.load(Ordering::Acquire)
            && match error {
                Error::ConnectionClosed(_) => self.cause() == DisconnectCause::ClientClose,
                Error::WebSocket(error) => {
                    DisconnectCause::from_websocket_error(error).is_native_timeout()
                }
                _ => false,
            }
    }

    pub(super) fn record_writer_failure(&self, error: &sockudo_ws::Error) -> DisconnectCause {
        self.record_failure(error, false)
    }

    pub(super) fn record_close_failure(&self, error: &sockudo_ws::Error) {
        self.record_failure(error, true);
    }

    fn record_failure(&self, error: &sockudo_ws::Error, during_close: bool) -> DisconnectCause {
        let cause = DisconnectCause::from_websocket_error(error);
        let mut terminal = self.terminal();
        if cause != DisconnectCause::TransportClosed {
            terminal.record(cause);
        }
        terminal.writer_failure.get_or_insert(WriterFailure {
            cause,
            during_close,
        });
        cause
    }

    pub(super) fn writer_close_error(&self) -> Option<Error> {
        self.terminal().close_error()
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

    #[test]
    fn observed_eof_is_not_refined_by_generic_writer_closure() {
        let termination = SocketTermination::default();
        termination.record(DisconnectCause::TransportEof);
        termination.record_writer_failure(&sockudo_ws::Error::ConnectionClosed);
        assert!(
            termination
                .begin_local_close(DisconnectCause::ServerShutdown)
                .is_err()
        );
        assert_eq!(termination.cause(), DisconnectCause::TransportEof);
    }

    #[test]
    fn reader_transport_closed_is_reported_and_can_be_refined() {
        let termination = SocketTermination::default();
        termination.record(DisconnectCause::TransportClosed);
        assert_eq!(termination.cause(), DisconnectCause::TransportClosed);
        assert!(
            termination
                .begin_local_close(DisconnectCause::ServerShutdown)
                .is_err()
        );
        termination.record(DisconnectCause::HeartbeatTimeout);
        assert_eq!(termination.cause(), DisconnectCause::HeartbeatTimeout);
        assert!(matches!(
            termination.begin_local_close(DisconnectCause::ServerShutdown),
            Err(Error::WebSocket(sockudo_ws::Error::HeartbeatTimeout))
        ));
    }

    #[test]
    fn writer_failure_rejects_local_close_before_writer_exit() {
        let termination = SocketTermination::default();
        termination.record_writer_failure(&sockudo_ws::Error::ConnectionClosed);
        assert!(matches!(
            termination.begin_local_close(DisconnectCause::ServerShutdown),
            Err(Error::ConnectionClosed(_))
        ));
        assert_eq!(termination.cause(), DisconnectCause::TransportClosed);
    }

    #[tokio::test]
    async fn terminal_eof_waits_for_the_in_flight_writer_result() {
        let termination = SocketTermination::new(Some(CancellationToken::new()));
        let shutdown = termination.shutdown_token().unwrap();
        let reader = async {
            termination.finish_reader_eof().await;
            termination.cause()
        };
        let writer = async {
            shutdown.cancelled().await;
            termination.record_writer_failure(&sockudo_ws::Error::Io(std::io::Error::from(
                std::io::ErrorKind::TimedOut,
            )));
            termination.writer_stopped.cancel();
        };
        let (cause, ()) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(reader, writer)
        })
        .await
        .unwrap();
        assert_eq!(cause, DisconnectCause::TransportError);
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
