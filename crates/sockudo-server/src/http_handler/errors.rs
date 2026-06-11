//! HTTP API error type shared by every `http_handler` endpoint.

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response as AxumResponse},
};
use sockudo_protocol::messages::{AI_ERROR_MUTABLE_NOT_PERMITTED, AI_ERROR_PAYLOAD_TOO_LARGE};
use sonic_rs::json;
use thiserror::Error;
use tracing::{error, warn};

// --- Custom Error Type ---

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Application not found: {0}")]
    AppNotFound(String),
    #[error("Application validation failed: {0}")]
    AppValidationFailed(String),
    #[error("API request authentication failed: {0}")]
    ApiAuthFailed(String),
    #[error("Forbidden: {0}")]
    Forbidden(String),
    #[error("Channel validation failed: Missing 'channels' or 'channel' field")]
    MissingChannelInfo,
    #[error("User connection termination failed: {0}")]
    TerminationFailed(String),
    #[error("Internal Server Error: {0}")]
    InternalError(String),
    #[error("Serialization Error: {0}")]
    SerializationError(#[from] sonic_rs::Error),
    #[error("HTTP Header Build Error: {0}")]
    HeaderBuildError(#[from] axum::http::Error),
    #[error("Limit exceeded: {0}")]
    LimitExceeded(String),
    #[error("Too many requests: {message}")]
    TooManyRequests {
        message: String,
        retry_after_seconds: u64,
    },
    #[error("Service unavailable due to backpressure: {message}")]
    Backpressure {
        message: String,
        retry_after_seconds: u64,
    },
    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("{message}")]
    AiTransport {
        status: StatusCode,
        code: u32,
        name: &'static str,
        message: String,
    },
    #[error("Feature disabled: {0}")]
    FeatureDisabled(String),
    #[error("Not implemented: {0}")]
    NotImplemented(String),
    #[error("Not found: {0}")]
    NotFound(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> AxumResponse {
        let ai_registry_code = match &self {
            AppError::AiTransport { code, .. } => Some(*code),
            _ => None,
        };
        let (status, code, msg) = match &self {
            AppError::AppNotFound(msg) => (StatusCode::NOT_FOUND, "app_not_found", msg.clone()),
            AppError::AppValidationFailed(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "app_validation_failed",
                msg.clone(),
            ),
            AppError::ApiAuthFailed(msg) => (StatusCode::UNAUTHORIZED, "auth_failed", msg.clone()),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, "forbidden", msg.clone()),
            AppError::MissingChannelInfo => (
                StatusCode::BAD_REQUEST,
                "missing_channel_info",
                "Request must contain 'channels' (list) or 'channel' (string)".to_string(),
            ),
            AppError::TerminationFailed(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "termination_failed",
                msg.clone(),
            ),
            AppError::SerializationError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "serialization_error",
                format!("Internal error during serialization: {e}"),
            ),
            AppError::HeaderBuildError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "header_build_error",
                format!("Internal error building response: {e}"),
            ),
            AppError::InternalError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                msg.clone(),
            ),
            AppError::LimitExceeded(msg) => {
                (StatusCode::BAD_REQUEST, "limit_exceeded", msg.clone())
            }
            AppError::TooManyRequests { message, .. } => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                message.clone(),
            ),
            AppError::Backpressure { message, .. } => (
                StatusCode::SERVICE_UNAVAILABLE,
                "backpressure",
                message.clone(),
            ),
            AppError::PayloadTooLarge(msg) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                msg.clone(),
            ),
            AppError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, "invalid_input", msg.clone()),
            AppError::AiTransport {
                status,
                code,
                name,
                message,
            } => (*status, *name, message.clone()),
            AppError::FeatureDisabled(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "feature_disabled",
                msg.clone(),
            ),
            AppError::NotImplemented(msg) => {
                (StatusCode::NOT_IMPLEMENTED, "not_implemented", msg.clone())
            }
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg.clone()),
        };
        error!(error.message = %self, status_code = %status, "HTTP request failed");
        let error_message = if let Some(ai_code) = ai_registry_code {
            json!({ "error": msg, "code": code, "ai_code": ai_code, "status": status.as_u16() })
        } else {
            json!({ "error": msg, "code": code, "status": status.as_u16() })
        };
        let mut response = (status, Json(error_message)).into_response();
        match &self {
            AppError::TooManyRequests {
                retry_after_seconds,
                ..
            }
            | AppError::Backpressure {
                retry_after_seconds,
                ..
            } => {
                if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
                    response.headers_mut().insert(header::RETRY_AFTER, value);
                }
            }
            _ => {}
        }
        response
    }
}

impl From<sockudo_core::error::Error> for AppError {
    fn from(err: sockudo_core::error::Error) -> Self {
        warn!(original_error = ?err, "Converting internal error to AppError for HTTP response");
        match err {
            sockudo_core::error::Error::InvalidAppKey => {
                AppError::AppNotFound(format!("Application key not found or invalid: {err}"))
            }
            sockudo_core::error::Error::ApplicationNotFound => {
                AppError::AppNotFound(err.to_string())
            }
            sockudo_core::error::Error::InvalidChannelName(s) => {
                AppError::InvalidInput(format!("Invalid channel name: {s}"))
            }
            sockudo_core::error::Error::Channel(s) => AppError::InvalidInput(s),
            sockudo_core::error::Error::InvalidMessageFormat(s) => AppError::InvalidInput(s),
            sockudo_core::error::Error::Auth(s) => AppError::ApiAuthFailed(s),
            sockudo_core::error::Error::AiTransport {
                code,
                name,
                message,
            } => {
                let status = match code {
                    AI_ERROR_PAYLOAD_TOO_LARGE => StatusCode::PAYLOAD_TOO_LARGE,
                    AI_ERROR_MUTABLE_NOT_PERMITTED => StatusCode::FORBIDDEN,
                    _ => StatusCode::BAD_REQUEST,
                };
                AppError::AiTransport {
                    status,
                    code,
                    name,
                    message,
                }
            }
            _ => AppError::InternalError(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_handler::test_support::*;
    use crate::http_handler::{HistoryQuery, channel_presence_history};
    use axum::extract::{Extension, Path, Query, State};
    use sonic_rs::{JsonValueTrait, Value};

    #[tokio::test]
    async fn error_responses_include_code_and_status_fields() {
        let handler = test_presence_history_handler(100);
        let app = test_app();

        // Non-presence channel triggers BAD_REQUEST
        let response = channel_presence_history(
            Path(("app-1".to_string(), "public-room".to_string())),
            Query(HistoryQuery::default()),
            Extension(app),
            State(handler),
        )
        .await;

        let response = match response {
            Ok(_) => panic!("expected error"),
            Err(err) => err.into_response(),
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert!(
            json["code"].as_str().is_some(),
            "error must include 'code' field"
        );
        assert!(
            json["status"].as_u64().is_some(),
            "error must include 'status' field"
        );
        assert!(
            json["error"].as_str().is_some(),
            "error must include 'error' field"
        );
    }
}
