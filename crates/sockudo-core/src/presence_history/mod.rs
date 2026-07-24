//! Presence-history domain: authoritative join/leave transition records with
//! paging, snapshots, degraded/reset stream state, retention, and metrics, plus
//! the storage abstraction and its built-in backends.

mod durable;
mod memory;
mod store;
#[cfg(test)]
pub(crate) mod test_support;
mod tracking;
mod types;

pub use durable::DurablePresenceHistoryStore;
pub use memory::{MemoryPresenceHistoryStore, MemoryPresenceHistoryStoreConfig};
pub use store::{NoopPresenceHistoryStore, PresenceHistoryStore};
pub use tracking::TrackingPresenceHistoryStore;
pub use types::{
    PresenceHistoryCursor, PresenceHistoryDirection, PresenceHistoryDurableState,
    PresenceHistoryEventCause, PresenceHistoryEventKind, PresenceHistoryFilter,
    PresenceHistoryItem, PresenceHistoryPage, PresenceHistoryPayload, PresenceHistoryQueryBounds,
    PresenceHistoryReadRequest, PresenceHistoryResetResult, PresenceHistoryRetentionPolicy,
    PresenceHistoryRetentionStats, PresenceHistoryRuntimeStatus, PresenceHistoryStreamInspection,
    PresenceHistoryStreamRuntimeState, PresenceHistoryTransitionRecord, PresenceSnapshot,
    PresenceSnapshotMember, PresenceSnapshotRequest,
};
