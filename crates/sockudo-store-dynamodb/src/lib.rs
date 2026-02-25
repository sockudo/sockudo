//! DynamoDB store backend exports.

pub mod backend;

pub use backend::{DynamoDbAppStore, DynamoDbConfig, Error, new_dynamodb_store};

pub fn crate_status() -> &'static str {
    "implemented"
}
