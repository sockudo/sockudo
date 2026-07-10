//! Ably REST and WebSocket compatibility, kept as an opt-in edge projection.
//!
//! The runtime is explicitly constructed by the server. It owns its compatibility
//! session/token/replay state and is also the realtime egress tap, so separate
//! runtimes cannot share state accidentally.

mod auth;
mod codec;
mod error;
mod protocol;
mod runtime;
mod services;

pub use error::AblyCompatError;
pub use runtime::{AblyCompatDependencies, AblyCompatRuntime};
