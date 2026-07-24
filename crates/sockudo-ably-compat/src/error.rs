//! Errors at the Ably compatibility boundary.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use sockudo_protocol::messages::{AI_ERROR_MUTABLE_NOT_PERMITTED, AI_ERROR_PAYLOAD_TOO_LARGE};
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum AblyCompatError {
    #[error("application not found: {0}")]
    AppNotFound(String),
    #[error("API request authentication failed: {0}")]
    ApiAuthFailed(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("internal server error: {0}")]
    InternalError(String),
    #[error("serialization error: {0}")]
    SerializationError(#[from] sonic_rs::Error),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("{message}")]
    Protocol {
        status: StatusCode,
        code: u32,
        message: String,
    },
    #[error("{message}")]
    AiTransport {
        status: StatusCode,
        code: u32,
        name: &'static str,
        message: String,
    },
    #[error("feature disabled: {0}")]
    FeatureDisabled(String),
    #[error("not found: {0}")]
    NotFound(String),
}

impl From<sockudo_core::error::Error> for AblyCompatError {
    fn from(error: sockudo_core::error::Error) -> Self {
        warn!(error = ?error, "converting native error at Ably compatibility boundary");
        match error {
            sockudo_core::error::Error::InvalidAppKey
            | sockudo_core::error::Error::ApplicationNotFound => {
                Self::AppNotFound(error.to_string())
            }
            sockudo_core::error::Error::InvalidChannelName(message)
            | sockudo_core::error::Error::Channel(message)
            | sockudo_core::error::Error::InvalidMessageFormat(message) => {
                Self::InvalidInput(message)
            }
            sockudo_core::error::Error::MessageNotFound(message) => Self::NotFound(message),
            sockudo_core::error::Error::IdempotencyConflict => Self::InvalidInput(
                "message.id was already used with a different payload".to_string(),
            ),
            sockudo_core::error::Error::IdempotencyInProgress => Self::Protocol {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: 50003,
                message: "idempotent publish is still in progress".to_string(),
            },
            sockudo_core::error::Error::Auth(message) => Self::ApiAuthFailed(message),
            sockudo_core::error::Error::AiTransport {
                code,
                name,
                message,
            } => Self::AiTransport {
                status: match code {
                    AI_ERROR_PAYLOAD_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
                    AI_ERROR_MUTABLE_NOT_PERMITTED => StatusCode::FORBIDDEN,
                    _ => StatusCode::BAD_REQUEST,
                },
                code,
                name,
                message,
            },
            other => Self::InternalError(other.to_string()),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorInfo {
    message: String,
    code: u32,
    status_code: u16,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    error: ErrorInfo,
}

impl IntoResponse for AblyCompatError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::AppNotFound(message) | Self::NotFound(message) => {
                (StatusCode::NOT_FOUND, 40400, message)
            }
            Self::ApiAuthFailed(message) => (StatusCode::UNAUTHORIZED, 40140, message),
            Self::Forbidden(message) => (StatusCode::FORBIDDEN, 40160, message),
            Self::InvalidInput(message) | Self::FeatureDisabled(message) => {
                (StatusCode::BAD_REQUEST, 40000, message)
            }
            Self::Protocol {
                status,
                code,
                message,
            } => (status, code, message),
            Self::AiTransport {
                status,
                code,
                message,
                ..
            } => (status, code, message),
            Self::InternalError(message) => (StatusCode::INTERNAL_SERVER_ERROR, 50000, message),
            Self::SerializationError(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                50000,
                message.to_string(),
            ),
        };
        (
            status,
            Json(ErrorBody {
                error: ErrorInfo {
                    message,
                    code,
                    status_code: status.as_u16(),
                },
            }),
        )
            .into_response()
    }
}
