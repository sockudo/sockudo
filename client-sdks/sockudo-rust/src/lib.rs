//! # sockudo-client
//!
//! The official async Rust realtime client for [Sockudo](https://github.com/sockudo/sockudo).
//!
//! * **Protocol V1** — strict Pusher compatibility.
//! * **Protocol V2** (default) — continuity-aware recovery, rewind, delta
//!   compression (Fossil and Xdelta3), tag/event/JMESPath filters, JSON /
//!   MessagePack / Protobuf wire formats, capability tokens with refresh,
//!   versioned messages, annotations and presence updates.
//!
//! Plus public, private, presence and end-to-end encrypted channels, user
//! sign-in and watchlists, message deduplication, proxy-backed channel and
//! presence history, and a push registration client.
//!
//! Built on [`sockudo-ws`](https://docs.rs/sockudo-ws) and
//! [`sonic-rs`](https://docs.rs/sonic-rs) with a single connection task,
//! batched writes, lazy payload decoding and no locks held across `.await`.
//!
//! ```no_run
//! use sockudo_client::{SockudoClient, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = SockudoClient::builder("app-key")
//!         .ws_host("127.0.0.1")
//!         .ws_port(6001)
//!         .force_tls(false)
//!         .build()?;
//!
//!     let channel = client.subscribe("prices");
//!     channel.bind("price-updated", |event| {
//!         #[derive(serde::Deserialize, Debug)]
//!         struct Price { symbol: String, price: f64 }
//!         if let Ok(price) = event.json::<Price>() {
//!             println!("{price:?}");
//!         }
//!     });
//!
//!     client.connect()?;
//!     client.connected().await?;
//!     tokio::signal::ctrl_c().await?;
//!     client.disconnect().await;
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! | Feature | Default | Enables |
//! | --- | --- | --- |
//! | `tls` | yes | `wss://` via rustls (aws-lc-rs) and the OS trust store |
//! | `tls-webpki-roots` | no | bundled Mozilla roots instead of the OS store |
//! | `http` | yes | auth/token endpoints, history proxies, push client |
//! | `encryption` | yes | `private-encrypted-*` channels |
//! | `xdelta3` | yes | Xdelta3/VCDIFF deltas (Fossil is always on) |
//! | `msgpack` | yes | MessagePack wire format |
//! | `protobuf` | yes | Protobuf wire format |
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod auth;
mod channel;
mod client;
mod codec;
mod connection;
mod crypto;
mod dedup;
pub mod delta;
mod dispatcher;
mod error;
mod event;
pub mod filter;
#[cfg(feature = "http")]
mod http;
mod options;
mod presence;
pub mod protocol;
#[cfg(feature = "http")]
pub mod push;
pub mod recovery;
pub mod rest;
#[cfg(feature = "http")]
mod stats;
pub mod token;
mod transport;
mod user;
pub mod versioned;

pub use auth::{ChannelAuthData, ChannelAuthRequest, Endpoint, UserAuthData, UserAuthRequest};
pub use channel::{Channel, ChannelKind, FilterExpression, SubscribeOptions};
pub use client::{ConnectionState, ErrorCallback, SockudoClient, StateCallback, StateChange};
pub use codec::WireFormat;
pub use delta::{ChannelDeltaSettings, DeltaAlgorithm, DeltaOptions, DeltaStats};
pub use dispatcher::{BindingId, EventCallback, EventStream};
pub use error::{Error, Result};
pub use event::{Event, Extras, HeaderValue, Payload};
pub use filter::{Filter, FilterNode};
pub use options::{AppendMode, ClientBuilder, ProxyOptions, Transport};
pub use presence::{Member, Members};
pub use protocol::{Prefix, ProtocolVersion};
pub use recovery::{RecoveryPosition, Rewind};
pub use token::{TokenAuthData, TokenRequest, TokenRequestReason};
pub use user::{User, Watchlist};

/// Compiles every README snippet as a doctest (they use default features).
#[cfg(all(
    doctest,
    feature = "tls",
    feature = "http",
    feature = "encryption",
    feature = "xdelta3",
    feature = "msgpack",
    feature = "protobuf"
))]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;

/// Re-export of the JSON value type used for dynamic payloads.
pub use sonic_rs::Value as JsonValue;

#[doc(hidden)]
pub mod __private {
    //! Internals exposed for benchmarks only. Not part of the public API.
    use bytes::Bytes;

    /// Decode a JSON frame.
    pub fn decode_json(raw: Bytes) -> crate::Result<crate::Event> {
        crate::codec::json::decode(raw)
    }

    /// Apply a Fossil delta.
    pub fn fossil_apply(base: &[u8], delta: &[u8]) -> crate::Result<Vec<u8>> {
        crate::delta::fossil::apply(base, delta, usize::MAX)
    }

    /// Strip delta markers from a full message.
    pub fn strip_delta_markers(raw: &[u8]) -> Vec<u8> {
        crate::delta::strip_delta_markers(raw)
    }

    /// A deduplicator for benchmarks.
    pub struct Dedup(crate::dedup::Deduplicator);

    impl Dedup {
        /// New deduplicator.
        pub fn new(capacity: usize) -> Self {
            Self(crate::dedup::Deduplicator::new(capacity))
        }
        /// Check and track.
        pub fn check(&mut self, id: &str) -> bool {
            self.0.check_and_track(id)
        }
    }

    impl std::fmt::Debug for Dedup {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Dedup")
        }
    }

    /// Encode a frame.
    pub fn encode(
        format: crate::WireFormat,
        event: &str,
        channel: Option<&str>,
        data: &[u8],
    ) -> crate::Result<Vec<u8>> {
        let message = crate::codec::encode(
            crate::codec::Outbound {
                event,
                channel,
                data,
            },
            format,
        )?;
        Ok(match message {
            sockudo_ws::Message::Text(bytes) | sockudo_ws::Message::Binary(bytes) => bytes.to_vec(),
            _ => Vec::new(),
        })
    }

    /// Decode a frame in any format.
    pub fn decode(
        format: crate::WireFormat,
        raw: Bytes,
        is_text: bool,
    ) -> crate::Result<crate::Event> {
        crate::codec::decode(raw, is_text, format)
    }
}
