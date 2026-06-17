mod config;
mod constants;
mod coordination;
mod devices;
mod document;
#[cfg(feature = "dynamodb")]
mod dynamodb;
mod helpers;
mod publishing;
#[cfg(feature = "scylladb")]
mod scylla;
mod subscriptions;
#[cfg(feature = "surrealdb")]
mod surreal;

pub use document::{DocumentBackend, DocumentPushStore, StoredDocument};
#[cfg(feature = "dynamodb")]
pub use dynamodb::DynamoDbDocumentBackend;
#[cfg(feature = "scylladb")]
pub use scylla::ScyllaDbDocumentBackend;
#[cfg(feature = "surrealdb")]
pub use surreal::SurrealDbDocumentBackend;

#[cfg(feature = "dynamodb")]
pub type DynamoDbPushStore = DocumentPushStore<DynamoDbDocumentBackend>;
#[cfg(feature = "surrealdb")]
pub type SurrealDbPushStore = DocumentPushStore<SurrealDbDocumentBackend>;
#[cfg(feature = "scylladb")]
pub type ScyllaDbPushStore = DocumentPushStore<ScyllaDbDocumentBackend>;

#[cfg(test)]
mod tests;
