use thiserror::Error;

#[derive(Error, Debug)]
pub enum SockudoError {
    #[error("Request error: {0}")]
    Request(#[from] RequestError),

    #[error("Webhook error: {0}")]
    Webhook(#[from] WebhookError),

    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("Validation error: {message}")]
    Validation { message: String },

    #[error("Encryption error: {message}")]
    Encryption { message: String },

    #[error("JSON error: {0}")]
    Json(#[from] sonic_rs::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}

#[derive(Error, Debug)]
#[error("HTTP request failed")]
pub struct RequestError {
    pub message: String,
    pub url: String,
    pub status: Option<u16>,
    pub body: Option<String>,
}

impl RequestError {
    pub fn new(
        message: impl Into<String>,
        url: impl Into<String>,
        status: Option<u16>,
        body: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            url: url.into(),
            status,
            body,
        }
    }
}

#[derive(Error, Debug)]
#[error("Webhook validation failed")]
pub struct WebhookError {
    pub message: String,
    pub content_type: Option<String>,
    pub body: String,
    pub signature: Option<String>,
}

impl WebhookError {
    pub fn new(
        message: impl Into<String>,
        content_type: Option<String>,
        body: impl Into<String>,
        signature: Option<String>,
    ) -> Self {
        Self {
            message: message.into(),
            content_type,
            body: body.into(),
            signature,
        }
    }
}
