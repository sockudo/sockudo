//! Error types.

use std::borrow::Cow;

/// Result alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every failure the client can report.
///
/// The enum is `#[non_exhaustive]`: match on the variants you handle and keep a
/// wildcard arm for the rest.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The app key was empty.
    #[error("you must pass a non-empty app key")]
    InvalidAppKey,

    /// The options failed validation.
    #[error("invalid options: {0}")]
    InvalidOptions(Cow<'static, str>),

    /// A client event name did not start with `client-`.
    #[error("event '{0}' does not start with 'client-'")]
    BadEventName(String),

    /// The operation requires an open connection.
    #[error("not connected")]
    NotConnected,

    /// The client was shut down (all handles dropped or `disconnect` called).
    #[error("client is closed")]
    Closed,

    /// The bounded outbound queue is full; retry later or use the async variant.
    #[error("outbound queue is full")]
    Backpressure,

    /// A feature is not available in this build or for this channel type.
    #[error("unsupported: {0}")]
    Unsupported(Cow<'static, str>),

    /// Channel or user authorization failed.
    #[error("authorization failed{}: {message}", status.map(|s| format!(" (HTTP {s})")).unwrap_or_default())]
    Auth {
        /// HTTP status when the failure came from an auth endpoint.
        status: Option<u16>,
        /// Human-readable reason.
        message: String,
    },

    /// The capability token expired (`40142`) and no provider could refresh it.
    #[error("capability token expired{}", reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default())]
    TokenExpired {
        /// Server-supplied reason.
        reason: Option<String>,
    },

    /// The capability token was revoked (`40160`).
    #[error("capability token revoked{}", reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default())]
    TokenRevoked {
        /// Server-supplied reason.
        reason: Option<String>,
    },

    /// The capability-token provider failed or returned an empty token.
    #[error("capability token request failed: {0}")]
    TokenProvider(String),

    /// The server sent a protocol `error` event.
    #[error("server error{}: {message}", code.map(|c| format!(" {c}")).unwrap_or_default())]
    Server {
        /// Pusher/Sockudo error code (for example `4001`, `40142`).
        code: Option<i64>,
        /// Server message.
        message: String,
    },

    /// The server closed the connection.
    #[error("connection closed by server (code {code}){}", if reason.is_empty() { String::new() } else { format!(": {reason}") })]
    ConnectionClosed {
        /// WebSocket close code.
        code: u16,
        /// Close reason.
        reason: String,
    },

    /// The client stopped connecting (manual disconnect, refusal, or an
    /// exhausted reconnect budget).
    #[error("disconnected{}", reason.as_deref().map(|r| format!(": {r}")).unwrap_or_default())]
    Disconnected {
        /// The last error seen before stopping, if any.
        reason: Option<String>,
    },

    /// The configured reconnect budget was exhausted.
    #[error("gave up after {attempts} reconnect attempts")]
    ReconnectExhausted {
        /// Number of attempts made.
        attempts: u32,
    },

    /// The connection was refused with a close code that forbids reconnecting.
    #[error("connection refused by server (code {code}){}", if reason.is_empty() { String::new() } else { format!(": {reason}") })]
    ConnectionRefused {
        /// WebSocket close code.
        code: u16,
        /// Close reason.
        reason: String,
    },

    /// The server did not reply to an activity ping in time.
    #[error("pong not received within the pong timeout")]
    PongTimeout,

    /// The operation timed out.
    #[error("timed out")]
    Timeout,

    /// The WebSocket layer failed.
    #[error("websocket: {0}")]
    WebSocket(String),

    /// An I/O operation failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// TLS configuration or handshake failed.
    #[error("tls: {0}")]
    Tls(String),

    /// A frame or payload could not be decoded.
    #[error("decode: {0}")]
    Decode(String),

    /// A frame or payload could not be encoded.
    #[error("encode: {0}")]
    Encode(String),

    /// JSON (de)serialization failed.
    #[error("json: {0}")]
    Json(#[from] sonic_rs::Error),

    /// A delta payload could not be applied.
    #[error("delta: {0}")]
    Delta(String),

    /// An encrypted payload could not be decrypted.
    #[error("decryption failed: {0}")]
    Decryption(String),

    /// An HTTP endpoint answered with a non-success status.
    #[error("HTTP {status}: {body}")]
    Http {
        /// HTTP status code.
        status: u16,
        /// Response body (truncated).
        body: String,
    },

    /// The HTTP request could not be performed.
    #[error("http transport: {0}")]
    HttpTransport(String),

    /// A paginated response has no further pages.
    #[error("no more pages available")]
    NoMorePages,

    /// A mutable-message reducer received an event it cannot apply.
    #[error("mutable message: {0}")]
    MutableMessage(String),
}

impl Error {
    pub(crate) fn invalid(message: impl Into<Cow<'static, str>>) -> Self {
        Self::InvalidOptions(message.into())
    }

    pub(crate) fn unsupported(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Unsupported(message.into())
    }

    pub(crate) fn auth(status: Option<u16>, message: impl Into<String>) -> Self {
        Self::Auth {
            status,
            message: message.into(),
        }
    }

    /// The Pusher/Sockudo protocol error code carried by this error, if any.
    pub fn code(&self) -> Option<i64> {
        match self {
            Self::Server { code, .. } => *code,
            Self::TokenExpired { .. } => Some(40142),
            Self::TokenRevoked { .. } => Some(40160),
            Self::ConnectionClosed { code, .. } | Self::ConnectionRefused { code, .. } => {
                Some(i64::from(*code))
            }
            _ => None,
        }
    }
}

impl From<sockudo_ws::Error> for Error {
    fn from(error: sockudo_ws::Error) -> Self {
        match error {
            sockudo_ws::Error::Io(io) => Self::Io(io),
            other => Self::WebSocket(other.to_string()),
        }
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            return Self::Timeout;
        }
        // reqwest errors may embed the request URL; the URL can carry query
        // credentials, so strip it before surfacing the message.
        Self::HttpTransport(error.without_url().to_string())
    }
}
