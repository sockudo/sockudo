//! Client configuration.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{ChannelAuthorizer, Endpoint, UserAuthenticator};
use crate::codec::WireFormat;
use crate::delta::DeltaOptions;
use crate::error::{Error, Result};
use crate::protocol::{ALLOWED_APPEND_ROLLUP_WINDOWS, ProtocolVersion};
use crate::rest::ProxyHandler;
use crate::token::TokenProvider;

/// WebSocket transport scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    /// Plain `ws://`.
    Ws,
    /// TLS `wss://`.
    Wss,
}

/// How the server delivers `message.append` mutations (Protocol V2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum AppendMode {
    /// Deliver each appended fragment (default).
    #[default]
    Delta,
    /// Deliver the full accumulated message on every append.
    Full,
}

impl AppendMode {
    /// Query-parameter value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Full => "full",
        }
    }
}

/// A proxy endpoint for history, presence history and versioned messages.
///
/// The client never holds the app secret, so these reads and mutations go
/// through your backend, which forwards them to the Sockudo REST API. The
/// request body is `{"channel", "action", "params", ...}`; see the README for
/// the full contract.
#[derive(Clone)]
pub struct ProxyOptions {
    /// HTTP endpoint (feature `http`).
    pub endpoint: Option<Endpoint>,
    /// Custom transport, used instead of the endpoint when set.
    pub handler: Option<Arc<dyn ProxyHandler>>,
    /// Request timeout.
    pub timeout: Duration,
}

impl fmt::Debug for ProxyOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyOptions")
            .field("endpoint", &self.endpoint)
            .field("handler", &self.handler.is_some())
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ProxyOptions {
    /// Proxy through an HTTP endpoint.
    pub fn endpoint(endpoint: impl Into<Endpoint>) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            handler: None,
            timeout: Duration::from_secs(10),
        }
    }

    /// Proxy through a custom handler.
    pub fn handler(handler: impl ProxyHandler + 'static) -> Self {
        Self {
            endpoint: None,
            handler: Some(Arc::new(handler)),
            timeout: Duration::from_secs(10),
        }
    }

    /// Set the request timeout (builder style).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

/// Fully resolved configuration (internal).
#[derive(Clone)]
pub(crate) struct Config {
    pub key: String,
    pub cluster: Option<String>,
    pub protocol: ProtocolVersion,
    pub use_tls: bool,
    pub ws_host: String,
    pub ws_port: u16,
    pub wss_port: u16,
    pub ws_path: String,
    pub enabled_transports: Option<Vec<Transport>>,
    pub disabled_transports: Option<Vec<Transport>>,
    pub activity_timeout: Duration,
    pub pong_timeout: Duration,
    pub unavailable_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_reconnect_attempts: Option<u32>,
    pub max_reconnect_gap: Duration,
    pub reconnect_jitter: f64,
    pub channel_auth_endpoint: Option<Endpoint>,
    pub channel_authorizer: Option<Arc<dyn ChannelAuthorizer>>,
    pub user_auth_endpoint: Option<Endpoint>,
    pub user_authenticator: Option<Arc<dyn UserAuthenticator>>,
    pub presence_history: Option<ProxyOptions>,
    pub channel_history: Option<ProxyOptions>,
    pub versioned_messages: Option<ProxyOptions>,
    pub delta: Option<DeltaOptions>,
    pub message_deduplication: bool,
    pub deduplication_capacity: usize,
    pub connection_recovery: bool,
    pub echo_messages: bool,
    pub wire_format: WireFormat,
    pub append_mode: AppendMode,
    pub append_rollup_window: Option<u32>,
    pub token: Option<String>,
    pub token_provider: Option<Arc<dyn TokenProvider>>,
    pub auth_url: Option<Endpoint>,
    pub auth_refresh_leeway: Duration,
    pub max_message_size: usize,
    pub outbound_queue_capacity: usize,
    pub ws_headers: Vec<(String, String)>,
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub enable_stats: bool,
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub stats_host: String,
    #[cfg_attr(not(feature = "http"), allow(dead_code))]
    pub timeline_params: Vec<(String, String)>,
    #[cfg_attr(not(feature = "deflate"), allow(dead_code))]
    pub permessage_deflate: bool,
    pub query_params: Vec<(String, String)>,
    #[cfg(feature = "tls")]
    pub tls_config: Option<Arc<rustls::ClientConfig>>,
    #[cfg(feature = "http")]
    pub http_client: Option<reqwest::Client>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("key", &self.key)
            .field("protocol", &self.protocol)
            .field("use_tls", &self.use_tls)
            .field("ws_host", &self.ws_host)
            .field("ws_port", &self.ws_port)
            .field("wss_port", &self.wss_port)
            .field("wire_format", &self.wire_format)
            .field("connection_recovery", &self.connection_recovery)
            .field("has_token", &self.token.is_some())
            .finish_non_exhaustive()
    }
}

impl Config {
    pub(crate) fn has_token_provider(&self) -> bool {
        self.token_provider.is_some() || self.auth_url.is_some()
    }

    /// Transports to try, in order (mirrors the other SDKs: `ws` first with a
    /// one-shot `wss` fallback unless TLS is forced).
    pub(crate) fn transports(&self) -> Vec<Transport> {
        let mut transports = if self.use_tls {
            vec![Transport::Wss]
        } else {
            vec![Transport::Ws, Transport::Wss]
        };
        if let Some(enabled) = self.enabled_transports.as_ref() {
            transports.retain(|transport| enabled.contains(transport));
        }
        if let Some(disabled) = self.disabled_transports.as_ref() {
            transports.retain(|transport| !disabled.contains(transport));
        }
        transports
    }
}

/// Builder for [`crate::SockudoClient`].
///
/// ```no_run
/// # fn main() -> sockudo_client::Result<()> {
/// use std::time::Duration;
/// use sockudo_client::SockudoClient;
///
/// let client = SockudoClient::builder("app-key")
///     .ws_host("127.0.0.1")
///     .ws_port(6001)
///     .force_tls(false)
///     .activity_timeout(Duration::from_secs(60))
///     .build()?;
/// # Ok(()) }
/// ```
#[must_use]
pub struct ClientBuilder {
    cluster: Option<String>,
    force_tls: Option<bool>,
    ws_host: Option<String>,
    config: Config,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("cluster", &self.cluster)
            .field("config", &self.config)
            .finish()
    }
}

impl ClientBuilder {
    pub(crate) fn new(key: impl Into<String>) -> Self {
        Self {
            cluster: None,
            force_tls: None,
            ws_host: None,
            config: Config {
                key: key.into(),
                cluster: None,
                protocol: ProtocolVersion::V2,
                use_tls: true,
                ws_host: String::new(),
                ws_port: 80,
                wss_port: 443,
                ws_path: String::new(),
                enabled_transports: None,
                disabled_transports: None,
                activity_timeout: Duration::from_secs(120),
                pong_timeout: Duration::from_secs(30),
                unavailable_timeout: Duration::from_secs(10),
                connect_timeout: Duration::from_secs(10),
                max_reconnect_attempts: Some(6),
                max_reconnect_gap: Duration::from_secs(120),
                reconnect_jitter: 0.0,
                channel_auth_endpoint: None,
                channel_authorizer: None,
                user_auth_endpoint: None,
                user_authenticator: None,
                presence_history: None,
                channel_history: None,
                versioned_messages: None,
                delta: None,
                message_deduplication: true,
                deduplication_capacity: 1000,
                connection_recovery: false,
                echo_messages: true,
                wire_format: WireFormat::Json,
                append_mode: AppendMode::Delta,
                append_rollup_window: None,
                token: None,
                token_provider: None,
                auth_url: None,
                auth_refresh_leeway: Duration::from_secs(30),
                max_message_size: 64 * 1024 * 1024,
                outbound_queue_capacity: 1024,
                ws_headers: Vec::new(),
                enable_stats: false,
                stats_host: "stats.sockudo.io".to_owned(),
                timeline_params: Vec::new(),
                permessage_deflate: cfg!(feature = "deflate"),
                query_params: Vec::new(),
                #[cfg(feature = "tls")]
                tls_config: None,
                #[cfg(feature = "http")]
                http_client: None,
            },
        }
    }

    /// Cluster name; the default host becomes `ws-{cluster}.sockudo.io`.
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(cluster.into());
        self
    }

    /// Protocol version (default V2).
    pub fn protocol_version(mut self, version: ProtocolVersion) -> Self {
        self.config.protocol = version;
        self
    }

    /// Force TLS on (`true`) or allow plain `ws://` first (`false`).
    /// TLS is used unless this is explicitly `false`.
    pub fn force_tls(mut self, force: bool) -> Self {
        self.force_tls = Some(force);
        self
    }

    /// WebSocket host.
    pub fn ws_host(mut self, host: impl Into<String>) -> Self {
        self.ws_host = Some(host.into());
        self
    }

    /// Port for `ws://` (default 80).
    pub fn ws_port(mut self, port: u16) -> Self {
        self.config.ws_port = port;
        self
    }

    /// Port for `wss://` (default 443).
    pub fn wss_port(mut self, port: u16) -> Self {
        self.config.wss_port = port;
        self
    }

    /// Path prefix before `/app/{key}`.
    pub fn ws_path(mut self, path: impl Into<String>) -> Self {
        self.config.ws_path = path.into();
        self
    }

    /// Restrict transports to this set.
    pub fn enabled_transports(mut self, transports: impl IntoIterator<Item = Transport>) -> Self {
        self.config.enabled_transports = Some(transports.into_iter().collect());
        self
    }

    /// Exclude these transports.
    pub fn disabled_transports(mut self, transports: impl IntoIterator<Item = Transport>) -> Self {
        self.config.disabled_transports = Some(transports.into_iter().collect());
        self
    }

    /// Inactivity before the client pings (default 120s; the server's value
    /// wins when lower).
    pub fn activity_timeout(mut self, timeout: Duration) -> Self {
        self.config.activity_timeout = timeout;
        self
    }

    /// How long to wait for a pong (default 30s).
    pub fn pong_timeout(mut self, timeout: Duration) -> Self {
        self.config.pong_timeout = timeout;
        self
    }

    /// How long connecting may take before the state becomes `Unavailable`
    /// (default 10s). Connecting continues in the background.
    pub fn unavailable_timeout(mut self, timeout: Duration) -> Self {
        self.config.unavailable_timeout = timeout;
        self
    }

    /// Timeout for one TCP + TLS + WebSocket handshake attempt (default 10s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Reconnect budget (default `Some(6)`; `None` retries forever).
    pub fn max_reconnect_attempts(mut self, attempts: Option<u32>) -> Self {
        self.config.max_reconnect_attempts = attempts;
        self
    }

    /// Upper bound on the quadratic reconnect delay (default 120s).
    pub fn max_reconnect_gap(mut self, gap: Duration) -> Self {
        self.config.max_reconnect_gap = gap;
        self
    }

    /// Randomize reconnect delays by this fraction (0 = exact, 1 = full
    /// jitter) so clients dropped together do not return in lockstep.
    pub fn reconnect_jitter(mut self, jitter: f64) -> Self {
        self.config.reconnect_jitter = jitter;
        self
    }

    /// Authorize protected channels through an HTTP endpoint (feature `http`).
    /// The endpoint receives a form POST with `socket_id` and `channel_name`.
    pub fn channel_auth_endpoint(mut self, endpoint: impl Into<Endpoint>) -> Self {
        self.config.channel_auth_endpoint = Some(endpoint.into());
        self
    }

    /// Authorize protected channels with a custom handler.
    pub fn channel_authorizer(mut self, authorizer: impl ChannelAuthorizer + 'static) -> Self {
        self.config.channel_authorizer = Some(Arc::new(authorizer));
        self
    }

    /// Authenticate users through an HTTP endpoint (feature `http`).
    pub fn user_auth_endpoint(mut self, endpoint: impl Into<Endpoint>) -> Self {
        self.config.user_auth_endpoint = Some(endpoint.into());
        self
    }

    /// Authenticate users with a custom handler.
    pub fn user_authenticator(mut self, authenticator: impl UserAuthenticator + 'static) -> Self {
        self.config.user_authenticator = Some(Arc::new(authenticator));
        self
    }

    /// Proxy for presence history and snapshots.
    pub fn presence_history(mut self, proxy: ProxyOptions) -> Self {
        self.config.presence_history = Some(proxy);
        self
    }

    /// Dedicated proxy for channel history (otherwise the versioned-messages
    /// proxy serves `channel_history`).
    pub fn channel_history(mut self, proxy: ProxyOptions) -> Self {
        self.config.channel_history = Some(proxy);
        self
    }

    /// Proxy for versioned messages, message versions, annotations and
    /// (by default) channel history.
    pub fn versioned_messages(mut self, proxy: ProxyOptions) -> Self {
        self.config.versioned_messages = Some(proxy);
        self
    }

    /// Enable delta compression handling (Protocol V2, JSON wire format).
    pub fn delta_compression(mut self, options: DeltaOptions) -> Self {
        self.config.delta = Some(options);
        self
    }

    /// Deduplicate by `message_id` (default on).
    pub fn message_deduplication(mut self, enabled: bool) -> Self {
        self.config.message_deduplication = enabled;
        self
    }

    /// How many recent message ids to remember (default 1000).
    pub fn message_deduplication_capacity(mut self, capacity: usize) -> Self {
        self.config.deduplication_capacity = capacity;
        self
    }

    /// Track per-channel positions and send `resume` after reconnecting.
    pub fn connection_recovery(mut self, enabled: bool) -> Self {
        self.config.connection_recovery = enabled;
        self
    }

    /// Receive your own client events (default on, Protocol V2).
    pub fn echo_messages(mut self, enabled: bool) -> Self {
        self.config.echo_messages = enabled;
        self
    }

    /// Wire format (Protocol V2).
    pub fn wire_format(mut self, format: WireFormat) -> Self {
        self.config.wire_format = format;
        self
    }

    /// Append delivery mode (Protocol V2).
    pub fn append_mode(mut self, mode: AppendMode) -> Self {
        self.config.append_mode = mode;
        self
    }

    /// Append rollup window in milliseconds: one of 0, 20, 40, 100, 500.
    pub fn append_rollup_window(mut self, window_ms: u32) -> Self {
        self.config.append_rollup_window = Some(window_ms);
        self
    }

    /// A static capability token (Protocol V2). Used for the first
    /// connection; reconnects use the provider when one is configured.
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = Some(token.into());
        self
    }

    /// Fetch capability tokens on demand (Protocol V2).
    pub fn token_provider(mut self, provider: impl TokenProvider + 'static) -> Self {
        self.config.token_provider = Some(Arc::new(provider));
        self
    }

    /// Fetch capability tokens from an HTTP endpoint (feature `http`). The
    /// endpoint receives JSON `{"socket_id", "reason"}` and returns
    /// `{"token", "expires_in"?, ...}`.
    pub fn auth_url(mut self, endpoint: impl Into<Endpoint>) -> Self {
        self.config.auth_url = Some(endpoint.into());
        self
    }

    /// Refresh at least this long before expiry (default 30s).
    pub fn auth_refresh_leeway(mut self, leeway: Duration) -> Self {
        self.config.auth_refresh_leeway = leeway;
        self
    }

    /// Largest inbound message accepted (default 64 MiB).
    pub fn max_message_size(mut self, bytes: usize) -> Self {
        self.config.max_message_size = bytes;
        self
    }

    /// Bound on queued outbound frames (default 1024).
    pub fn outbound_queue_capacity(mut self, capacity: usize) -> Self {
        self.config.outbound_queue_capacity = capacity;
        self
    }

    /// Extra headers on the WebSocket upgrade request (e.g. `Origin`).
    pub fn ws_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.ws_headers.push((name.into(), value.into()));
        self
    }

    /// Offer permessage-deflate (RFC 7692) on the upgrade (default on with
    /// the `deflate` feature). It is used only when the server enables
    /// WebSocket compression; otherwise the offer is ignored.
    #[cfg(feature = "deflate")]
    pub fn permessage_deflate(mut self, enabled: bool) -> Self {
        self.config.permessage_deflate = enabled;
        self
    }

    /// Send an anonymous connection timeline (state changes, retries,
    /// errors; never payloads or credentials) to `stats_host`, like the JS
    /// client's `enableStats`. Off by default.
    #[cfg(feature = "http")]
    pub fn enable_stats(mut self, enabled: bool) -> Self {
        self.config.enable_stats = enabled;
        self
    }

    /// Stats host (default `stats.sockudo.io`); `host:port` is accepted.
    #[cfg(feature = "http")]
    pub fn stats_host(mut self, host: impl Into<String>) -> Self {
        self.config.stats_host = host.into();
        self
    }

    /// Extra parameter added to every stats bundle.
    #[cfg(feature = "http")]
    pub fn timeline_param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config
            .timeline_params
            .push((name.into(), value.into()));
        self
    }

    /// Extra connection query parameters.
    pub fn query_param(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.query_params.push((name.into(), value.into()));
        self
    }

    /// Custom rustls configuration for `wss://` (custom roots, client certs).
    #[cfg(feature = "tls")]
    pub fn tls_config(mut self, config: Arc<rustls::ClientConfig>) -> Self {
        self.config.tls_config = Some(config);
        self
    }

    /// Share an existing HTTP client for auth and proxy requests.
    #[cfg(feature = "http")]
    pub fn http_client(mut self, client: reqwest::Client) -> Self {
        self.config.http_client = Some(client);
        self
    }

    pub(crate) fn into_config(self) -> Result<Config> {
        let mut config = self.config;
        if config.key.is_empty() {
            return Err(Error::InvalidAppKey);
        }
        config.cluster = self.cluster.clone().filter(|cluster| !cluster.is_empty());
        config.ws_host = match (self.ws_host, self.cluster) {
            (Some(host), _) if !host.is_empty() => host,
            (_, Some(cluster)) if !cluster.is_empty() => format!("ws-{cluster}.sockudo.io"),
            _ => return Err(Error::invalid("set ws_host or cluster")),
        };
        config.use_tls = self.force_tls != Some(false);
        if !config.ws_path.is_empty() && !config.ws_path.starts_with('/') {
            config.ws_path.insert(0, '/');
        }
        while config.ws_path.ends_with('/') {
            config.ws_path.pop();
        }
        if !config.reconnect_jitter.is_finite() || config.reconnect_jitter <= 0.0 {
            config.reconnect_jitter = 0.0;
        }
        config.reconnect_jitter = config.reconnect_jitter.min(1.0);
        config.deduplication_capacity = config.deduplication_capacity.max(1);
        config.outbound_queue_capacity = config.outbound_queue_capacity.max(1);

        let v1 = config.protocol == ProtocolVersion::V1;
        if v1 && (config.token.is_some() || config.has_token_provider()) {
            return Err(Error::invalid(
                "capability-token authentication requires Protocol V2",
            ));
        }
        if v1 && config.wire_format.is_binary() {
            return Err(Error::invalid("binary wire formats require Protocol V2"));
        }
        if let Some(window) = config.append_rollup_window
            && !ALLOWED_APPEND_ROLLUP_WINDOWS.contains(&window)
        {
            return Err(Error::invalid(
                "append_rollup_window must be one of: 0, 20, 40, 100, 500",
            ));
        }
        if config.delta.is_some() && config.wire_format.is_binary() {
            // Delta bases are the server's JSON bytes; a binary frame cannot be
            // turned back into those exact bytes.
            return Err(Error::invalid(
                "delta compression requires the JSON wire format",
            ));
        }
        if config.transports().is_empty() {
            return Err(Error::invalid("no transports enabled"));
        }
        #[cfg(not(feature = "http"))]
        if config.channel_auth_endpoint.is_some()
            || config.user_auth_endpoint.is_some()
            || config.auth_url.is_some()
        {
            return Err(Error::invalid(
                "HTTP endpoints require the `http` feature; use a custom handler instead",
            ));
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_key_and_host() {
        assert!(matches!(
            ClientBuilder::new("").ws_host("h").into_config(),
            Err(Error::InvalidAppKey)
        ));
        assert!(ClientBuilder::new("k").into_config().is_err());
        let config = ClientBuilder::new("k").cluster("eu").into_config().unwrap();
        assert_eq!(config.ws_host, "ws-eu.sockudo.io");
        assert!(config.use_tls);
    }

    #[test]
    fn rejects_v2_features_under_v1() {
        let base = || {
            ClientBuilder::new("k")
                .ws_host("h")
                .protocol_version(ProtocolVersion::V1)
        };
        assert!(base().token("t").into_config().is_err());
        assert!(base().into_config().is_ok());
        #[cfg(feature = "msgpack")]
        assert!(
            base()
                .wire_format(WireFormat::MessagePack)
                .into_config()
                .is_err()
        );
    }

    #[test]
    fn validates_rollup_window_and_delta_format() {
        let base = || ClientBuilder::new("k").ws_host("h");
        assert!(base().append_rollup_window(40).into_config().is_ok());
        assert!(base().append_rollup_window(41).into_config().is_err());
        #[cfg(feature = "protobuf")]
        assert!(
            base()
                .wire_format(WireFormat::Protobuf)
                .delta_compression(DeltaOptions::default())
                .into_config()
                .is_err()
        );
    }

    #[test]
    fn transport_sequence_mirrors_other_sdks() {
        let tls = ClientBuilder::new("k").ws_host("h").into_config().unwrap();
        assert_eq!(tls.transports(), vec![Transport::Wss]);
        let plain = ClientBuilder::new("k")
            .ws_host("h")
            .force_tls(false)
            .into_config()
            .unwrap();
        assert_eq!(plain.transports(), vec![Transport::Ws, Transport::Wss]);
        let ws_only = ClientBuilder::new("k")
            .ws_host("h")
            .force_tls(false)
            .disabled_transports([Transport::Wss])
            .into_config()
            .unwrap();
        assert_eq!(ws_only.transports(), vec![Transport::Ws]);
        assert!(
            ClientBuilder::new("k")
                .ws_host("h")
                .enabled_transports([Transport::Ws])
                .into_config()
                .is_err(),
            "TLS-only with ws-only transports leaves nothing to try"
        );
    }

    #[test]
    fn normalizes_path_and_jitter() {
        let config = ClientBuilder::new("k")
            .ws_host("h")
            .ws_path("ws/")
            .reconnect_jitter(3.0)
            .into_config()
            .unwrap();
        assert_eq!(config.ws_path, "/ws");
        assert_eq!(config.reconnect_jitter, 1.0);
        let config = ClientBuilder::new("k")
            .ws_host("h")
            .reconnect_jitter(f64::NAN)
            .into_config()
            .unwrap();
        assert_eq!(config.reconnect_jitter, 0.0);
    }
}
