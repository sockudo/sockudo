//! Error mapping between the Sockudo layer and MCP.
//!
//! Protocol-level problems (bad arguments, missing scope, rate limits) become
//! JSON-RPC errors so hosts surface them as failures. Upstream API errors
//! become `isError: true` tool results carrying the server's own JSON body so
//! the model can read the reason and recover.

use rmcp::ErrorData;
use rmcp::model::{CallToolResult, ContentBlock, ErrorCode};
use serde_json::{Value, json};

use crate::api::{ApiError, ApiResponse};
use crate::auth::AuthError;

/// Bearer credential missing or invalid.
pub const ERROR_CODE_UNAUTHORIZED: ErrorCode = ErrorCode(-32001);
/// Authenticated but not permitted (scope or app allow-list).
pub const ERROR_CODE_FORBIDDEN: ErrorCode = ErrorCode(-32003);
/// Principal exceeded its request budget.
pub const ERROR_CODE_RATE_LIMITED: ErrorCode = ErrorCode(-32004);

/// Failure while executing a tool.
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    /// Arguments failed validation before any upstream call.
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),
    /// Scope or app allow-list denied the call.
    #[error(transparent)]
    Auth(#[from] AuthError),
    /// Sockudo answered with a non-2xx status.
    #[error("sockudo returned HTTP {}", .0.status)]
    Upstream(ApiResponse),
    /// The API layer failed before or while sending.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// Unexpected failure inside the MCP server.
    #[error("internal error: {0}")]
    Internal(String),
}

impl ToolError {
    /// Shorthand for argument validation failures.
    pub fn invalid(message: impl Into<String>) -> Self {
        ToolError::InvalidArguments(message.into())
    }

    /// Stable label for metrics and audit logs.
    pub fn outcome(&self) -> &'static str {
        match self {
            ToolError::InvalidArguments(_) => "invalid_arguments",
            ToolError::Auth(AuthError::MissingScope(_)) => "forbidden_scope",
            ToolError::Auth(AuthError::AppNotAllowed(_)) => "forbidden_app",
            ToolError::Auth(_) => "unauthorized",
            ToolError::Upstream(response) => {
                if response.status.is_client_error() {
                    "upstream_4xx"
                } else {
                    "upstream_5xx"
                }
            }
            ToolError::Api(ApiError::Timeout(_)) => "timeout",
            ToolError::Api(ApiError::UnknownApp(_)) => "unknown_app",
            ToolError::Api(_) => "transport_error",
            ToolError::Internal(_) => "internal_error",
        }
    }

    /// Convert into what `tools/call` should return: a JSON-RPC error for
    /// protocol-level failures, or an `isError` result the model can act on.
    pub fn into_call_result(self) -> Result<CallToolResult, ErrorData> {
        match self {
            ToolError::InvalidArguments(message) => Err(ErrorData::invalid_params(message, None)),
            ToolError::Auth(error) => Err(auth_error(&error)),
            ToolError::Upstream(response) => Ok(upstream_error_result(&response)),
            ToolError::Api(error) => Ok(error_result(
                api_error_code(&error),
                error.to_string(),
                None,
            )),
            ToolError::Internal(message) => Ok(error_result("internal_error", message, None)),
        }
    }
}

fn api_error_code(error: &ApiError) -> &'static str {
    match error {
        ApiError::UnknownApp(_) => "unknown_app",
        ApiError::MissingAppId => "missing_app_id",
        ApiError::Transport(_) => "transport_error",
        ApiError::Timeout(_) => "timeout",
        ApiError::InvalidRequest(_) => "invalid_request",
        ApiError::Internal(_) => "internal_error",
    }
}

/// JSON-RPC error for an authentication or authorization failure.
pub fn auth_error(error: &AuthError) -> ErrorData {
    match error {
        AuthError::Missing | AuthError::Invalid => ErrorData::new(
            ERROR_CODE_UNAUTHORIZED,
            error.to_string(),
            Some(json!({ "reason": "unauthorized" })),
        ),
        AuthError::MissingScope(scope) => ErrorData::new(
            ERROR_CODE_FORBIDDEN,
            error.to_string(),
            Some(json!({ "reason": "missing_scope", "required_scope": scope.as_str() })),
        ),
        AuthError::AppNotAllowed(app_id) => ErrorData::new(
            ERROR_CODE_FORBIDDEN,
            error.to_string(),
            Some(json!({ "reason": "app_not_allowed", "app_id": app_id })),
        ),
    }
}

/// JSON-RPC error for a rate-limited principal.
pub fn rate_limited(retry_after_seconds: u64) -> ErrorData {
    ErrorData::new(
        ERROR_CODE_RATE_LIMITED,
        "MCP request budget exceeded for this token; retry later",
        Some(json!({ "retry_after_seconds": retry_after_seconds })),
    )
}

/// Build an `isError` tool result with a machine-readable envelope.
pub fn error_result(
    code: &str,
    message: impl Into<String>,
    details: Option<Value>,
) -> CallToolResult {
    let message = message.into();
    let mut envelope = json!({ "error": code, "message": message });
    if let Some(details) = details {
        envelope["details"] = details;
    }
    let text = serde_json::to_string(&envelope).unwrap_or(message);
    let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
    result.structured_content = Some(envelope);
    result
}

/// Wrap a non-2xx Sockudo response. The body is the server's own error JSON
/// (`{"error", "code", "status", ...}`), passed through untouched.
pub fn upstream_error_result(response: &ApiResponse) -> CallToolResult {
    let status = response.status.as_u16();
    let upstream = response
        .json()
        .unwrap_or_else(|| Value::String(response.text().into_owned()));
    let message = upstream
        .get("error")
        .and_then(Value::as_str)
        .or_else(|| upstream.get("message").and_then(Value::as_str))
        .map(str::to_string)
        .unwrap_or_else(|| format!("Sockudo returned HTTP {status}"));
    error_result(
        "upstream_error",
        message,
        Some(json!({ "http_status": status, "upstream": upstream })),
    )
}

/// Wrap a successful Sockudo response: the raw JSON as text plus, when the
/// body is a JSON object, the same value as `structuredContent`.
pub fn success_result(response: &ApiResponse) -> CallToolResult {
    let text = if response.body.is_empty() {
        r#"{"ok":true}"#.to_string()
    } else {
        response.text().into_owned()
    };
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    if let Some(value @ Value::Object(_)) = response.json() {
        result.structured_content = Some(value);
    }
    result
}

/// Wrap any serializable value as a successful result.
pub fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ToolError> {
    let value = serde_json::to_value(value)
        .map_err(|error| ToolError::Internal(format!("cannot encode result: {error}")))?;
    let text = serde_json::to_string(&value)
        .map_err(|error| ToolError::Internal(format!("cannot encode result: {error}")))?;
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    if value.is_object() {
        result.structured_content = Some(value);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::StatusCode;

    fn response(status: StatusCode, body: &str) -> ApiResponse {
        ApiResponse {
            status,
            body: Bytes::copy_from_slice(body.as_bytes()),
            content_type: None,
        }
    }

    #[test]
    fn upstream_errors_become_is_error_results_with_server_body() {
        let result = upstream_error_result(&response(
            StatusCode::BAD_REQUEST,
            r#"{"error":"Invalid channel name","code":"invalid_input","status":400}"#,
        ));
        assert_eq!(result.is_error, Some(true));
        let structured = result.structured_content.unwrap();
        assert_eq!(structured["details"]["http_status"], 400);
        assert_eq!(structured["message"], "Invalid channel name");
        assert_eq!(structured["details"]["upstream"]["code"], "invalid_input");
    }

    #[test]
    fn success_results_carry_structured_objects_only() {
        let object = success_result(&response(StatusCode::OK, r#"{"ok":true}"#));
        assert!(object.structured_content.is_some());
        let array = success_result(&response(StatusCode::OK, "[1,2]"));
        assert!(array.structured_content.is_none());
        let empty = success_result(&response(StatusCode::OK, ""));
        assert_eq!(empty.content[0].as_text().unwrap().text, r#"{"ok":true}"#);
    }

    #[test]
    fn protocol_errors_map_to_json_rpc_codes() {
        let invalid = ToolError::invalid("bad").into_call_result().unwrap_err();
        assert_eq!(invalid.code, ErrorCode::INVALID_PARAMS);
        let forbidden = ToolError::Auth(AuthError::MissingScope(crate::auth::Scope::Admin))
            .into_call_result()
            .unwrap_err();
        assert_eq!(forbidden.code, ERROR_CODE_FORBIDDEN);
        let timeout = ToolError::Api(ApiError::Timeout(std::time::Duration::from_secs(1)))
            .into_call_result()
            .unwrap();
        assert_eq!(timeout.is_error, Some(true));
    }
}
