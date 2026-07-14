//! Ably REST and WebSocket compatibility, kept as an opt-in edge projection.
//!
//! The runtime is explicitly constructed by the server. It owns its compatibility
//! session/token/replay state and is also the realtime egress tap, so separate
//! runtimes cannot share state accidentally.

mod auth;
mod channel_name;
mod codec;
mod error;
mod filter;
mod outbound;
mod protocol;
mod runtime;
mod services;
mod stats;

pub use error::AblyCompatError;
pub use outbound::OutboundMetricsSnapshot;
pub use protocol::{AblyRestQuery, AblyStatsQuery};
#[cfg(feature = "push")]
pub use runtime::AblyPushAdmissionGuard;
pub use runtime::{AblyCompatDependencies, AblyCompatRuntime, ably_stats};
pub use stats::StatsRuntimeSnapshot;
