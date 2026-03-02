//! Runtime orchestration modules for Sockudo server assembly.

pub mod adapter;
pub mod app;
pub mod cache;
pub mod channel;
pub mod cleanup;
pub use sockudo_core::delta_compression;
pub use sockudo_core::error;
pub mod http_handler;
pub mod metrics;
pub mod middleware;
pub use sockudo_core::namespace;
pub mod options;
pub mod presence;
pub mod queue;
pub mod rate_limiter;
pub use sockudo_core::token;
pub mod utils;
pub use sockudo_core::watchlist;
pub mod webhook;
pub use sockudo_core::websocket;
pub mod ws_handler;

pub mod http;

pub fn crate_status() -> &'static str {
    "implemented"
}
