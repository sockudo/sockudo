// sockudo-delta: Delta compression manager for Sockudo WebSocket messages
//
// This crate contains the runtime delta compression logic including the
// DeltaCompressionManager and related types. Configuration/enum types live
// in sockudo_core::delta_types and are re-exported here for convenience.

// Re-export core delta types so consumers can use `sockudo_delta::*`
pub use sockudo_core::delta_types::*;

pub mod coordination;

mod compression;
mod conflation;
mod manager;
mod messages;
mod state;

pub use compression::CompressionResult;
pub use manager::{DeltaCompressionManager, DeltaCompressionStats};
pub use messages::{
    CachedMessage, DeltaMessage, create_cache_sync_message, create_delta_message,
    create_full_message_with_seq,
};
pub use state::PerChannelDeltaSettings;
