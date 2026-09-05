//! Streamable HTTP transport: bearer auth, initialize handshake, scope-filtered
//! tool listing, and tool execution against a recording API transport.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use serde_json::{Value, json};
use sockudo_mcp::api::{ApiError, ApiTransport};
use sockudo_mcp::auth::{AppAccess, Principal, ScopeSet, TokenAuthenticator};
use sockudo_mcp::transport::http::{HttpTransportConfig, router};
use sockudo_mcp::{AppCredentials, McpCore, McpCoreConfig, SockudoApi, StaticCredentials};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Default)]
struct Recording {
    requests: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl ApiTransport for Recording {
    async fn send(&self, request: Request<Bytes>) -> Result<http::Response<Bytes>, ApiError> {
        self.requests.lock().unwrap().push((
            request.method().to_string(),
            request.uri().path().to_string(),
        ));
        let mut response = http::Response::new(Bytes::from_static(
            br#"{"channels":{"presence-room":{"subscription_count":2}}}"#,
        ));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        Ok(response)
    }

    fn kind(&self) -> &'static str {
        "recording"
    }
}

const READ_TOKEN: &str = "read-token-0123456789abcdef";
const ADMIN_TOKEN: &str = "admin-token-0123456789abcdef";

fn app(recording: Arc<Recording>) -> axum::Router {
    let api = SockudoApi::new(
        recording,
        Arc::new(StaticCredentials::new(vec![AppCredentials::new(
            "app-1", "key1", "secret1",
        )])),
    );
    let core = Arc::new(McpCore::new(api, McpCoreConfig::default()));
    let authenticator = Arc::new(TokenAuthenticator::new([
        (
            READ_TOKEN.to_string(),
            Principal::new("reader", ScopeSet::READ, AppAccess::All),
        ),
        (
            ADMIN_TOKEN.to_string(),
            Principal::new("admin", ScopeSet::ALL, AppAccess::All),
        ),
    ]));
    let config = HttpTransportConfig {
        allowed_hosts: Vec::new(),
        ..HttpTransportConfig::default()
    };
    router(
        "/mcp",
        core,
        authenticator,
        &config,
        CancellationToken::new(),
    )
}

fn rpc(id: u64, method: &str, params: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
}

async fn post(
    app: &axum::Router,
    token: Option<&str>,
    session: Option<&str>,
    body: Value,
) -> (StatusCode, http::HeaderMap, Value) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::HOST, "localhost")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream");
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(session) = session {
        request = request.header("mcp-session-id", session);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let text = String::from_utf8_lossy(&bytes);
    // Legacy session mode may answer with SSE: skip the empty priming event
    // and take the first `data:` line carrying JSON.
    let json_text = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .find(|data| !data.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| text.to_string());
    let value =
        serde_json::from_str(&json_text).unwrap_or_else(|_| Value::String(text.to_string()));
    (status, headers, value)
}

async fn initialize(app: &axum::Router, token: &str) -> String {
    let (status, headers, body) = post(
        app,
        Some(token),
        None,
        rpc(
            1,
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "0" }
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["serverInfo"]["name"], "sockudo", "{body}");
    assert!(body["result"]["capabilities"]["tools"].is_object());
    let session = headers
        .get("mcp-session-id")
        .expect("session id header")
        .to_str()
        .unwrap()
        .to_string();
    // Complete the handshake.
    let (status, _, _) = post(
        app,
        Some(token),
        Some(&session),
        json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await;
    assert!(status.is_success(), "initialized notification accepted");
    session
}

#[tokio::test]
async fn rejects_missing_and_invalid_bearer_tokens() {
    let app = app(Arc::new(Recording::default()));
    let (status, headers, body) = post(&app, None, None, rpc(1, "ping", json!({}))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(headers.get(header::WWW_AUTHENTICATE).is_some());
    assert_eq!(body["error"], "unauthorized");

    let (status, _, _) = post(&app, Some("nope"), None, rpc(1, "ping", json!({}))).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn read_token_sees_only_read_tools_and_cannot_publish() {
    let app = app(Arc::new(Recording::default()));
    let session = initialize(&app, READ_TOKEN).await;

    let (status, _, body) = post(
        &app,
        Some(READ_TOKEN),
        Some(&session),
        rpc(2, "tools/list", json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let tools = body["result"]["tools"].as_array().expect("tools array");
    let names: Vec<&str> = tools.iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(names.contains(&"sockudo_list_channels"));
    assert!(!names.contains(&"sockudo_trigger_event"));
    assert!(!names.contains(&"sockudo_reset_history"));

    let (status, _, body) = post(
        &app,
        Some(READ_TOKEN),
        Some(&session),
        rpc(
            3,
            "tools/call",
            json!({ "name": "sockudo_trigger_event", "arguments": { "app_id": "app-1", "name": "e", "channel": "c" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32003, "{body}");
}

#[tokio::test]
async fn admin_token_lists_everything_and_tool_calls_reach_the_api() {
    let recording = Arc::new(Recording::default());
    let app = app(recording.clone());
    let session = initialize(&app, ADMIN_TOKEN).await;

    let (_, _, body) = post(
        &app,
        Some(ADMIN_TOKEN),
        Some(&session),
        rpc(2, "tools/list", json!({})),
    )
    .await;
    let tools = body["result"]["tools"].as_array().unwrap();
    assert!(tools.iter().any(|t| t["name"] == "sockudo_reset_history"));

    let (status, _, body) = post(
        &app,
        Some(ADMIN_TOKEN),
        Some(&session),
        rpc(
            3,
            "tools/call",
            json!({ "name": "sockudo_list_channels", "arguments": { "app_id": "app-1", "filter_by_prefix": "presence-" } }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_ne!(body["result"]["isError"], true, "{body}");
    assert_eq!(
        body["result"]["structuredContent"]["channels"]["presence-room"]["subscription_count"],
        2
    );
    let requests = recording.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0],
        ("GET".to_string(), "/apps/app-1/channels".to_string())
    );
}

#[tokio::test]
async fn resources_and_prompts_are_served() {
    let app = app(Arc::new(Recording::default()));
    let session = initialize(&app, READ_TOKEN).await;

    let (_, _, body) = post(
        &app,
        Some(READ_TOKEN),
        Some(&session),
        rpc(
            2,
            "resources/read",
            json!({ "uri": "sockudo://docs/channels" }),
        ),
    )
    .await;
    let text = body["result"]["contents"][0]["text"].as_str().unwrap();
    assert!(text.contains("presence-"));

    let (_, _, body) = post(
        &app,
        Some(READ_TOKEN),
        Some(&session),
        rpc(
            3,
            "prompts/get",
            json!({ "name": "sockudo_incident_triage", "arguments": {} }),
        ),
    )
    .await;
    assert_eq!(body["result"]["messages"][0]["role"], "user");

    let (_, _, body) = post(
        &app,
        Some(READ_TOKEN),
        Some(&session),
        rpc(4, "resources/read", json!({ "uri": "sockudo://nope" })),
    )
    .await;
    assert_eq!(body["error"]["code"], -32002, "{body}");
}
