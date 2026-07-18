use super::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::capability_token::{TokenAuthContext, validate_capability_token};
use sockudo_core::error::{Error, Result};
use sockudo_core::websocket::SocketId;
use sockudo_protocol::ProtocolVersion;
use sockudo_protocol::messages::{MessageData, PusherMessage};
use sockudo_protocol::protocol_version::{CANONICAL_AUTH_SUCCESS, CANONICAL_TOKEN_EXPIRED};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

pub const TOKEN_EXPIRED_CODE: u32 = 40142;
pub const TOKEN_REVOKED_CODE: u32 = 40160;
pub const TOKEN_GRACE_CLOSE_SECONDS: u64 = 30;

#[derive(Debug, Clone, Default)]
pub struct RevocationRequest {
    pub jti: Option<String>,
    pub client_id: Option<String>,
    pub expires_at: Option<i64>,
    pub ttl_seconds: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RevocationResult {
    pub revoked_jti: bool,
    pub revoked_client_id: bool,
    pub closed_connections: usize,
}

fn jti_revocation_key(app_id: &str, jti: &str) -> String {
    format!("sockudo:capability_revocation:{app_id}:jti:{jti}")
}

fn client_revocation_key(app_id: &str, client_id: &str) -> String {
    format!("sockudo:capability_revocation:{app_id}:client:{client_id}")
}

fn ttl_from_request(request: &RevocationRequest, now: i64) -> u64 {
    if let Some(ttl) = request.ttl_seconds {
        return ttl.max(1);
    }
    if let Some(expires_at) = request.expires_at {
        return expires_at.saturating_sub(now).max(1) as u64;
    }
    24 * 60 * 60
}

fn now_seconds() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .map_err(|_| Error::Internal("system clock is before unix epoch".to_string()))
}

impl ConnectionHandler {
    pub(crate) async fn validate_connection_token(
        &self,
        app_config: &App,
        token: &str,
    ) -> Result<TokenAuthContext> {
        let context = validate_capability_token(token, app_config)?;
        if self
            .cache_manager
            .has(&jti_revocation_key(&app_config.id, &context.jti))
            .await?
            || self
                .cache_manager
                .has(&client_revocation_key(&app_config.id, &context.client_id))
                .await?
        {
            return Err(Error::Auth("capability token has been revoked".to_string()));
        }

        Ok(context)
    }

    pub(crate) async fn apply_connection_token(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        context: TokenAuthContext,
        is_refresh: bool,
    ) -> Result<()> {
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            return Err(Error::ConnectionNotFound);
        };

        if connection.protocol_version != ProtocolVersion::V2 {
            return Err(Error::Auth(
                "capability tokens require protocol V2".to_string(),
            ));
        }

        if is_refresh {
            if let Some(existing) = connection.get_token_auth_context().await {
                if existing.client_id != context.client_id {
                    return Err(Error::Auth(
                        "capability token refresh cannot change client_id".to_string(),
                    ));
                }
            } else if let Some(existing_user_id) = connection.get_user_id().await
                && existing_user_id != context.client_id
            {
                return Err(Error::Auth(
                    "capability token refresh cannot change authenticated identity".to_string(),
                ));
            }
        }

        connection.set_token_auth_context(context.clone()).await;
        self.connection_manager.add_user(connection.clone()).await?;
        self.schedule_token_expiry(socket_id, app_config, &context);

        Ok(())
    }

    pub(crate) async fn handle_auth_token_refresh(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        message: &PusherMessage,
    ) -> Result<()> {
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, &app_config.id)
            .await
        else {
            return Err(Error::ConnectionNotFound);
        };
        if connection.protocol_version != ProtocolVersion::V2 {
            return Err(Error::Auth(
                "sockudo:auth is only supported on protocol V2".to_string(),
            ));
        }

        let token = token_from_message(message)?;
        let context = self.validate_connection_token(app_config, &token).await?;
        self.apply_connection_token(socket_id, app_config, context.clone(), true)
            .await?;

        let response = PusherMessage {
            event: Some(ProtocolVersion::V2.wire_event(CANONICAL_AUTH_SUCCESS)),
            data: Some(MessageData::from(json!({
                "client_id": context.client_id,
                "jti": context.jti,
                "exp": context.exp,
            }))),
            channel: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };
        self.send_message_to_socket(&app_config.id, socket_id, response)
            .await
    }

    fn schedule_token_expiry(
        &self,
        socket_id: &SocketId,
        app_config: &App,
        context: &TokenAuthContext,
    ) {
        let handler = self.clone();
        let socket_id = *socket_id;
        let app_config = app_config.clone();
        let jti = context.jti.clone();
        let exp = context.exp;
        tokio::spawn(async move {
            let now = match now_seconds() {
                Ok(now) => now,
                Err(error) => {
                    warn!(error = %error, "failed to schedule token expiry");
                    return;
                }
            };
            let delay = exp.saturating_sub(now) as u64;
            sleep_until(Instant::now() + Duration::from_secs(delay)).await;

            if !handler
                .connection_still_uses_token(&app_config.id, &socket_id, &jti)
                .await
            {
                return;
            }
            if let Err(error) = handler
                .send_token_expired(&app_config.id, &socket_id, TOKEN_EXPIRED_CODE, "expired")
                .await
            {
                warn!(%socket_id, error = %error, "failed to send token_expired");
            }

            tokio::time::sleep(Duration::from_secs(TOKEN_GRACE_CLOSE_SECONDS)).await;
            if handler
                .connection_still_uses_token(&app_config.id, &socket_id, &jti)
                .await
                && let Err(error) = handler
                    .close_connection_with_cause(
                        &socket_id,
                        &app_config,
                        4009,
                        "capability token expired",
                        sockudo_core::websocket::DisconnectCause::TokenExpired,
                    )
                    .await
            {
                warn!(%socket_id, error = %error, "failed to close expired token connection");
            }
        });
    }

    async fn connection_still_uses_token(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        jti: &str,
    ) -> bool {
        let Some(connection) = self
            .connection_manager
            .get_connection(socket_id, app_id)
            .await
        else {
            return false;
        };
        connection
            .get_token_auth_context()
            .await
            .is_some_and(|context| context.jti == jti)
    }

    pub(crate) async fn send_token_expired(
        &self,
        app_id: &str,
        socket_id: &SocketId,
        code: u32,
        reason: &str,
    ) -> Result<()> {
        let message = PusherMessage {
            event: Some(ProtocolVersion::V2.wire_event(CANONICAL_TOKEN_EXPIRED)),
            data: Some(MessageData::from(json!({
                "code": code,
                "reason": reason,
            }))),
            channel: None,
            name: None,
            user_id: None,
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: None,
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        };
        self.send_message_to_socket(app_id, socket_id, message)
            .await
    }

    pub async fn revoke_capability_tokens(
        &self,
        app_config: &App,
        request: RevocationRequest,
    ) -> Result<RevocationResult> {
        if request.jti.is_none() && request.client_id.is_none() {
            return Err(Error::Auth(
                "revocation requires jti or client_id".to_string(),
            ));
        }
        let now = now_seconds()?;
        let ttl = ttl_from_request(&request, now);
        let reason = request.reason.as_deref().unwrap_or("revoked");
        let mut result = RevocationResult::default();

        if let Some(jti) = request.jti.as_deref() {
            self.cache_manager
                .set(&jti_revocation_key(&app_config.id, jti), reason, ttl)
                .await?;
            result.revoked_jti = true;
        }
        if let Some(client_id) = request.client_id.as_deref() {
            self.cache_manager
                .set(
                    &client_revocation_key(&app_config.id, client_id),
                    reason,
                    ttl,
                )
                .await?;
            result.revoked_client_id = true;
        }

        let sockets = self
            .connection_manager
            .get_all_connections(&app_config.id)
            .await?;
        for socket_id in sockets {
            let Some(connection) = self
                .connection_manager
                .get_connection(&socket_id, &app_config.id)
                .await
            else {
                continue;
            };
            let Some(context) = connection.get_token_auth_context().await else {
                continue;
            };
            let jti_matches = request.jti.as_deref() == Some(context.jti.as_str());
            let client_matches = request.client_id.as_deref() == Some(context.client_id.as_str());
            if !jti_matches && !client_matches {
                continue;
            }

            debug!(%socket_id, "closing revoked capability-token connection");
            let _ = self
                .send_token_expired(&app_config.id, &socket_id, TOKEN_REVOKED_CODE, "revoked")
                .await;
            self.close_connection_with_cause(
                &socket_id,
                app_config,
                4009,
                "capability token revoked",
                sockudo_core::websocket::DisconnectCause::TokenRevoked,
            )
            .await?;
            result.closed_connections += 1;
        }

        Ok(result)
    }
}

fn token_from_message(message: &PusherMessage) -> Result<String> {
    let Some(data) = &message.data else {
        return Err(Error::InvalidMessageFormat(
            "sockudo:auth requires data.token".to_string(),
        ));
    };

    let value = match data {
        MessageData::Json(value) => value.clone(),
        MessageData::String(text) => sonic_rs::from_str::<Value>(text).map_err(|_| {
            Error::InvalidMessageFormat("sockudo:auth data must be JSON".to_string())
        })?,
        MessageData::Structured { .. } => {
            return Err(Error::InvalidMessageFormat(
                "sockudo:auth data must contain token".to_string(),
            ));
        }
    };

    value
        .get("token")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::InvalidMessageFormat("sockudo:auth requires data.token".to_string()))
}
