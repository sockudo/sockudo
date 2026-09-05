mod encoding;
mod leased;
mod memory;
mod store;
mod types;

#[doc(hidden)]
pub use encoding::{EncodedVersionRecord, VersionStoragePlan, VersionTextReference};
pub use leased::LeasedVersionStore;
pub use memory::MemoryVersionStore;
pub use store::{NoopVersionStore, VersionStore};
pub use types::*;

#[cfg(test)]
mod tests;
