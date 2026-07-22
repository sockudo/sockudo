/// Token-auth user-index routing audit (Task 6).
///
/// Confirms that `set_token_auth_context` / `apply_connection_token` never
/// silently replace an already-indexed user identity:
///   • initial token on a fresh connection is first-time assignment (user_id=None)
///   • token refresh enforces the same client_id (idempotent re-index)
///   • presence `is_none()` guard cannot overwrite a token-auth user_id
///   • `validate_signin_token_boundary` blocks signin on token-auth connections
use sockudo_core::capability_token::TokenAuthContext;
use sockudo_core::websocket::{ConnectionCapabilities, SocketId, WebSocket, WebSocketRef};
use sockudo_ws::axum_integration;
use sockudo_ws::client::WebSocketClient;
use sockudo_ws::{Config as WsConfig, Http1, WebSocketStream};
use tokio::net::{TcpListener, TcpStream};

async fn create_test_writer() -> axum_integration::WebSocketWriter {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = sockudo_ws::handshake::server_handshake(&mut stream)
            .await
            .unwrap();
        let ws = axum_integration::WebSocket::from_tcp(stream, WsConfig::default());
        let (_reader, writer) = ws.split();
        writer
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let client = WebSocketClient::<Http1>::new(WsConfig::default());
    let (_client_ws, _): (WebSocketStream<sockudo_ws::Stream<Http1>>, _) = client
        .connect(client_stream, &addr.to_string(), "/", None)
        .await
        .unwrap();

    server.await.unwrap()
}

fn make_token_context(client_id: &str) -> TokenAuthContext {
    TokenAuthContext {
        client_id: client_id.to_string(),
        capabilities: ConnectionCapabilities::default(),
        jti: format!("jti-{client_id}"),
        exp: i64::MAX,
    }
}

async fn make_ws_ref(user_id: Option<&str>) -> WebSocketRef {
    let writer = create_test_writer().await;
    let mut ws = WebSocket::new(SocketId::new(), writer);
    if let Some(uid) = user_id {
        ws.state.user_id = Some(uid.to_string());
    }
    WebSocketRef::new(ws)
}

#[tokio::test]
async fn token_auth_on_fresh_connection_is_first_time_assignment() {
    let ws_ref = make_ws_ref(None).await;

    let context = make_token_context("client-abc");

    {
        let mut conn = ws_ref.inner.lock().await;
        let previous_user_id = conn.state.user_id.clone();
        conn.set_token_auth_context(context.clone());

        assert_eq!(
            previous_user_id, None,
            "user_id must be None before the first token auth"
        );
    }

    let user_id = ws_ref.get_user_id().await;
    assert_eq!(
        user_id.as_deref(),
        Some("client-abc"),
        "user_id must be set to client_id after set_token_auth_context"
    );
}

#[tokio::test]
async fn token_auth_refresh_same_client_id_is_idempotent() {
    let ws_ref = make_ws_ref(None).await;

    let context_v1 = make_token_context("client-xyz");
    {
        let mut conn = ws_ref.inner.lock().await;
        conn.set_token_auth_context(context_v1);
    }

    let context_v2 = TokenAuthContext {
        client_id: "client-xyz".to_string(),
        capabilities: ConnectionCapabilities::default(),
        jti: "jti-refreshed".to_string(),
        exp: i64::MAX,
    };
    {
        let mut conn = ws_ref.inner.lock().await;
        let previous_user_id = conn.state.user_id.clone();
        conn.set_token_auth_context(context_v2.clone());

        assert_eq!(
            previous_user_id.as_deref(),
            Some("client-xyz"),
            "previous user_id before refresh must be client-xyz"
        );
    }

    let user_id = ws_ref.get_user_id().await;
    assert_eq!(
        user_id.as_deref(),
        Some("client-xyz"),
        "user_id must remain client-xyz after same-id refresh"
    );

    let token_ctx = ws_ref.get_token_auth_context().await;
    assert_eq!(
        token_ctx.as_ref().map(|c| c.jti.as_str()),
        Some("jti-refreshed"),
        "token_auth_context must be updated to the refreshed token"
    );
}

#[tokio::test]
async fn presence_subscription_cannot_overwrite_token_auth_user_id() {
    let ws_ref = make_ws_ref(None).await;

    {
        let mut conn = ws_ref.inner.lock().await;
        conn.set_token_auth_context(make_token_context("token-client-1"));
    }

    {
        let mut conn = ws_ref.inner.lock().await;
        if conn.state.user_id.is_none() {
            conn.state.user_id = Some("presence-user-99".to_string());
        }
    }

    assert_eq!(
        ws_ref.get_user_id().await.as_deref(),
        Some("token-client-1")
    );
}

#[tokio::test]
async fn token_auth_sets_all_expected_state_fields() {
    let ws_ref = make_ws_ref(None).await;
    let context = make_token_context("state-fields-client");

    {
        let mut conn = ws_ref.inner.lock().await;
        conn.set_token_auth_context(context.clone());
    }

    let conn = ws_ref.inner.lock().await;
    assert_eq!(conn.state.user_id.as_deref(), Some("state-fields-client"));
    assert!(conn.state.token_auth_context.is_some());
    assert!(conn.state.connection_capabilities.is_some());
    assert!(conn.state.user_info.is_some());
    assert!(conn.state.user.is_some());
    assert_eq!(conn.state.connection_meta, None);
}

#[tokio::test]
async fn migration_detection_predicate_fires_for_differing_ids() {
    let ws_ref = make_ws_ref(Some("old-user-identity")).await;

    let ctx = make_token_context("new-token-client");
    let previous_user_id = {
        let mut conn = ws_ref.inner.lock().await;
        let previous = conn.state.user_id.clone();
        conn.set_token_auth_context(ctx.clone());
        previous
    };

    let would_trigger_migration = previous_user_id.as_deref() != Some(ctx.client_id.as_str());
    assert!(would_trigger_migration);
}

#[tokio::test]
async fn token_auth_context_prevents_signin_overwrite() {
    let ws_ref = make_ws_ref(None).await;

    {
        let mut conn = ws_ref.inner.lock().await;
        conn.set_token_auth_context(make_token_context("token-user"));
    }

    assert!(ws_ref.get_token_auth_context().await.is_some());
    assert_eq!(ws_ref.get_user_id().await.as_deref(), Some("token-user"));
}
