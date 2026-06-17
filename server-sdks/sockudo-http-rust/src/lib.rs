//! Sockudo HTTP API client for Rust
//!
//! This library provides a safe, fast, and idiomatic Rust client for the Sockudo HTTP API.
//!
//! # Features
//!
//! - `rustls-tls` (default): Use rustls for TLS (recommended for cross-compilation)
//! - `native-tls`: Use native TLS (OpenSSL on Linux, Secure Transport on macOS, SChannel on Windows)
//! - `encryption` (default): Enable support for end-to-end encrypted channels
//!
//! # Cross-Compilation
//!
//! This library is designed to work well with cross-compilation. The default features use
//! pure-Rust dependencies that compile easily to different targets.
//!
//! ```bash
//! # Cross-compile to ARM
//! cross build --target armv7-unknown-linux-gnueabihf --release
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod auth;
pub mod channel;
pub mod config;
pub mod errors;
pub mod events;
pub mod history;
pub mod presence_history;
pub mod push;
pub mod sockudo;
pub mod token;
pub mod util;
pub mod webhook;

#[macro_use]
extern crate zeroize;

pub use channel::{Channel, ChannelName, ChannelType};
pub use config::{Config, ConfigBuilder};
pub use errors::{RequestError, SockudoError, WebhookError};
pub use sockudo::Sockudo;
pub use token::Token;
pub use webhook::{Webhook, WebhookEvent};

/// Result type alias for Sockudo operations
pub type Result<T> = std::result::Result<T, SockudoError>;

// Re-export commonly used types
pub use auth::{SocketAuth, UserAuth};
pub use events::{BatchEvent, Event, MessageExtras, TriggerParams, generate_idempotency_key};
pub use history::{
    AnnotationEventsParams, AnnotationEventsResponse, DeleteAnnotationResponse, GetMessageResponse,
    HistoryBounds, HistoryContinuity, HistoryItem, HistoryPage, HistoryParams,
    ListMessageVersionsResponse, MessageVersionsParams, MutationResponse, PublishAnnotationRequest,
    PublishAnnotationResponse,
};
pub use presence_history::{
    PresenceHistoryBounds, PresenceHistoryContinuity, PresenceHistoryItem, PresenceHistoryPage,
    PresenceHistoryParams, PresenceSnapshot, PresenceSnapshotMember, PresenceSnapshotParams,
};
pub use push::{PushCursorParams, PushSubscriptionParams};

/// Check if encryption support is available at compile time
pub const ENCRYPTION_AVAILABLE: bool = cfg!(feature = "encryption");

/// Information about the build configuration
pub struct BuildInfo;

impl BuildInfo {
    /// Returns whether encryption support is available
    pub fn has_encryption() -> bool {
        ENCRYPTION_AVAILABLE
    }

    /// Returns the TLS backend being used
    pub fn tls_backend() -> &'static str {
        if cfg!(feature = "rustls-tls") {
            "rustls"
        } else if cfg!(feature = "native-tls") {
            "native-tls"
        } else {
            "none"
        }
    }

    /// Returns the encryption backend being used
    #[cfg(feature = "encryption")]
    pub fn encryption_backend() -> &'static str {
        if cfg!(feature = "sodiumoxide") {
            "sodiumoxide"
        } else {
            "chacha20poly1305"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_info() {
        println!("Encryption available: {}", BuildInfo::has_encryption());
        println!("TLS backend: {}", BuildInfo::tls_backend());

        #[cfg(feature = "encryption")]
        println!("Encryption backend: {}", BuildInfo::encryption_backend());
    }
}
