use axum::body::Body;
use axum::http::Response;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    // 4000-4099: Don't reconnect errors
    #[error("Application only accepts SSL connections, reconnect using wss://")]
    SSLRequired,

    #[error("Application does not exist")]
    ApplicationNotFound,

    #[error("Application disabled")]
    ApplicationDisabled,

    #[error("Application is over adapter quota")]
    OverConnectionQuota,

    #[error("Path not found")]
    PathNotFound,

    #[error("Invalid version string format")]
    InvalidVersionFormat,

    #[error("Unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(String),

    #[error("No protocol version supplied")]
    NoProtocolVersion,

    #[error("Connection is unauthorized")]
    Unauthorized,

    // 4100-4199: Reconnect with backoff errors
    #[error("Over capacity")]
    OverCapacity,

    // 4200-4299: Reconnect immediately errors
    #[error("Generic reconnect immediately")]
    ReconnectImmediately,

    #[error("Pong reply not received")]
    PongNotReceived,

    #[error("Closed after inactivity")]
    InactivityTimeout,

    // 4300-4399: Other errors
    #[error("Client event rejected due to rate limit")]
    ClientEventRateLimit,

    #[error("Watchlist limit exceeded")]
    WatchlistLimitExceeded,

    // Channel specific errors
    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("Channel name invalid: {0}")]
    InvalidChannelName(String),

    #[error("Channel already exists")]
    ChannelExists,

    #[error("Channel does not exist")]
    ChannelNotFound,

    // Authentication errors
    #[error("Authentication error: {0}")]
    AuthError(String),

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Invalid key")]
    InvalidKey,

    // Connection errors
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Connection already exists")]
    ConnectionExists,

    #[error("Connection not found")]
    ConnectionNotFound,

    // Protocol errors
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Invalid message format: {0}")]
    InvalidMessageFormat(String),

    #[error("Invalid event name: {0}")]
    InvalidEventName(String),

    // WebSocket errors
    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] fastwebsockets::WebSocketError),

    // Internal errors
    #[error("Internal server error: {0}")]
    InternalError(String),

    // JSON serialization/deserialization errors
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Client event error: {0}")]
    ClientEventError(String), // Add this variant

    // I/O errors
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    // Generic errors
    #[error("Invalid app key")]
    InvalidAppKey,

    #[error("Cache error: {0}")]
    CacheError(String),

    #[error("Invalid JSON")]
    SerializationError(String),

    #[error("Broadcast error: {0}")]
    BroadcastError(String),

    #[error("Other: {0}")]
    Other(String),

    #[error("Redis error: {0}")]
    RedisError(String),

    #[error("Request timeout")]
    RequestTimeout,

    #[error("Own request ignored")]
    OwnRequestIgnored,

    #[error("Horizontal adapter error: {0}")]
    HorizontalAdapterError(String),

    #[error("Queue error: {0}")]
    Queue(String),

    #[error("Config")]
    Config(String),

    #[error("Connection Error: {0}")]
    Connection(String),
    
    #[error("Configuration Error: {0}")]
    ConfigurationError(String),
    
    #[error("Config file Error: {0}")]
    ConfigFileError(String),
}

// Add conversion to WebSocket close codes
impl Error {
    pub fn close_code(&self) -> u16 {
        match self {
            // 4000-4099: Don't reconnect
            Error::SSLRequired => 4000,
            Error::ApplicationNotFound => 4001,
            Error::ApplicationDisabled => 4003,
            Error::OverConnectionQuota => 4004,
            Error::PathNotFound => 4005,
            Error::InvalidVersionFormat => 4006,
            Error::UnsupportedProtocolVersion(_) => 4007,
            Error::NoProtocolVersion => 4008,
            Error::Unauthorized => 4009,

            // 4100-4199: Reconnect with backoff
            Error::OverCapacity => 4100,

            // 4200-4299: Reconnect immediately
            Error::ReconnectImmediately => 4200,
            Error::PongNotReceived => 4201,
            Error::InactivityTimeout => 4202,

            // 4300-4399: Other errors
            Error::ClientEventRateLimit => 4301,
            Error::WatchlistLimitExceeded => 4302,

            Error::BroadcastError(_) => 4303,

            // Map other errors to appropriate ranges
            Error::ChannelError(_)
            | Error::InvalidChannelName(_)
            | Error::ChannelExists
            | Error::ChannelNotFound => 4300,

            Error::ClientEventError(_) => 4301,

            Error::AuthError(_) | Error::InvalidSignature | Error::InvalidKey => 4009,

            Error::ConnectionError(_) | Error::ConnectionExists | Error::ConnectionNotFound => 4000,

            _ => 4000, // Default to don't reconnect for unknown errors
        }
    }

    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            Error::SSLRequired
                | Error::ApplicationNotFound
                | Error::ApplicationDisabled
                | Error::OverConnectionQuota
                | Error::PathNotFound
                | Error::InvalidVersionFormat
                | Error::UnsupportedProtocolVersion(_)
                | Error::NoProtocolVersion
                | Error::Unauthorized
        )
    }

    pub fn should_reconnect(&self) -> bool {
        matches!(
            self,
            Error::OverCapacity
                | Error::ReconnectImmediately
                | Error::PongNotReceived
                | Error::InactivityTimeout
        )
    }
}

// Convert to Pusher protocol error message
impl From<Error> for crate::protocol::messages::ErrorData {
    fn from(error: Error) -> Self {
        Self {
            code: Some(error.close_code()),
            message: error.to_string(),
        }
    }
}

// Helper functions for error handling
pub type Result<T> = std::result::Result<T, Error>;

// src/error/macros.rs
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !($cond) {
            return Err($err);
        }
    };
}

#[macro_export]
macro_rules! bail {
    ($err:expr) => {
        return Err($err);
    };
}
