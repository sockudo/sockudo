//! AWS Lambda webhook backend.

pub mod backend;

pub type WebhookResult<T> = Result<T, String>;

pub use backend::LambdaWebhookSender;
