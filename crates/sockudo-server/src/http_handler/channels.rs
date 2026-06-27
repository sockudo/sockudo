//! Channel info/list/users endpoints, user-connection termination, and
//! capability-token revocation.

use ahash::AHashMap;
use axum::{
    Json,
    extract::{Extension, Path, Query, RawQuery, State},
    http::{StatusCode, Uri},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use sockudo_adapter::ConnectionHandler;
use sockudo_adapter::channel_manager::ChannelManager;
use sockudo_adapter::handler::auth_tokens::RevocationRequest as CapabilityRevocationRequest;
use sockudo_core::app::App;
use sockudo_core::auth::EventQuery;
use sockudo_core::utils::{self, validate_channel_name};
use sockudo_protocol::messages::{InfoQueryParser, PusherMessage};
use sonic_rs::json;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use super::AppError;
use super::ai::ai_channel_stats;
use super::system::record_api_metrics;

#[derive(Debug, Deserialize)]
pub struct RevokeCapabilityTokensRequest {
    pub jti: Option<String>,
    pub client_id: Option<String>,
    pub expires_at: Option<i64>,
    pub ttl_seconds: Option<u64>,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RevokeCapabilityTokensResponse {
    pub revoked_jti: bool,
    pub revoked_client_id: bool,
    pub closed_connections: usize,
}

#[derive(Debug)]
pub struct ChannelQuery {
    pub info: Option<String>,
    pub auth_params: EventQuery,
}

#[derive(Debug)]
pub struct ChannelsQuery {
    pub filter_by_prefix: Option<String>,
    pub info: Option<String>,
    pub auth_params: EventQuery,
}

impl<'de> Deserialize<'de> for ChannelQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
        let info = map.remove("info");
        let auth_params = EventQuery {
            auth_key: map.remove("auth_key").unwrap_or_default(),
            auth_timestamp: map.remove("auth_timestamp").unwrap_or_default(),
            auth_version: map.remove("auth_version").unwrap_or_default(),
            body_md5: map.remove("body_md5").unwrap_or_default(),
            auth_signature: map.remove("auth_signature").unwrap_or_default(),
        };
        Ok(Self { info, auth_params })
    }
}

impl<'de> Deserialize<'de> for ChannelsQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut map = std::collections::HashMap::<String, String>::deserialize(deserializer)?;
        let filter_by_prefix = map.remove("filter_by_prefix");
        let info = map.remove("info");
        let auth_params = EventQuery {
            auth_key: map.remove("auth_key").unwrap_or_default(),
            auth_timestamp: map.remove("auth_timestamp").unwrap_or_default(),
            auth_version: map.remove("auth_version").unwrap_or_default(),
            body_md5: map.remove("body_md5").unwrap_or_default(),
            auth_signature: map.remove("auth_signature").unwrap_or_default(),
        };
        Ok(Self {
            filter_by_prefix,
            info,
            auth_params,
        })
    }
}

/// GET /apps/{app_id}/channels/{channel_name}
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(query_params_specific): Query<ChannelQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    debug!("Request for channel info for channel: {}", channel_name);
    validate_channel_name(&app, &channel_name).await?;

    let info_query_str = query_params_specific.info.as_ref();
    let wants_subscription_count = info_query_str.wants_subscription_count();
    let wants_user_count = info_query_str.wants_user_count();
    let wants_cache_data = info_query_str.wants_cache();

    let socket_count_info = handler
        .connection_manager()
        .get_channel_socket_count_info(&app_id, &channel_name)
        .await;
    let socket_count_val = socket_count_info.count;

    let user_count_val = if wants_user_count {
        if channel_name.starts_with("presence-") {
            let members_map = ChannelManager::get_channel_members(
                handler.connection_manager(),
                &app_id,
                &channel_name,
            )
            .await?;
            Some(members_map.len() as u64)
        } else {
            return Err(AppError::InvalidInput(
                "user_count is only available for presence channels".to_string(),
            ));
        }
    } else {
        None
    };

    let cache_data_tuple = if wants_cache_data && utils::is_cache_channel(&channel_name) {
        let cache_key_str = format!("app:{app_id}:channel:{channel_name}:cache_miss");

        match handler.cache_manager().get(&cache_key_str).await? {
            Some(cache_content_str) => {
                let ttl_duration = handler
                    .cache_manager()
                    .ttl(&cache_key_str)
                    .await?
                    .unwrap_or_else(|| core::time::Duration::from_secs(3600));
                Some((cache_content_str, ttl_duration))
            }
            _ => None,
        }
    } else {
        None
    };

    let subscription_count_val = if wants_subscription_count {
        Some(socket_count_val as u64)
    } else {
        None
    };
    let mut response_payload = PusherMessage::channel_info(
        socket_count_val > 0,
        subscription_count_val,
        user_count_val,
        cache_data_tuple,
    );
    if wants_subscription_count && !socket_count_info.complete {
        response_payload["subscription_count_complete"] = json!(false);
    }
    if let Some(ai_stats) = ai_channel_stats(&handler, &app, &app_id, &channel_name).await? {
        response_payload["ai"] = ai_stats;
    }
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    debug!("Channel info for '{}' retrieved successfully", channel_name);
    Ok((StatusCode::OK, Json(response_payload)))
}

/// How long a `/channels` listing is cached to collapse repeated polls into a
/// single cluster-wide aggregation. The underlying count is best-effort and
/// eventually consistent across nodes, so brief staleness is acceptable.
const CHANNELS_LIST_CACHE_TTL_SECS: u64 = 2;

/// GET /apps/{app_id}/channels
#[instrument(skip(handler), fields(app_id = %app_id))]
pub async fn channels(
    Path(app_id): Path<String>,
    Query(query_params_specific): Query<ChannelsQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
) -> Result<axum::response::Response, AppError> {
    debug!("Request for channels list for app_id: {}", app_id);

    let filter_prefix_str = query_params_specific
        .filter_by_prefix
        .as_deref()
        .unwrap_or("");
    let wants_user_count = query_params_specific.info.as_ref().wants_user_count();
    let wants_subscription_count = query_params_specific
        .info
        .as_ref()
        .wants_subscription_count();

    // Collapse repeated /channels polls into a single cross-node aggregation per
    // TTL window. get_channels_with_socket_count (and, for user_count, the
    // per-presence-channel member fetch below) are cluster-wide request/reply
    // fan-outs; polling this endpoint without a cache issues an N-node aggregation
    // per request, saturating the request/reply path and starving readiness probes.
    // Cached per (app, prefix, info) since the response shape depends on them.
    let info_key = query_params_specific.info.as_deref().unwrap_or("");
    let channels_cache_key = format!("internal:channels:{app_id}:{filter_prefix_str}:{info_key}");
    if let Ok(Some(cached)) = handler.cache_manager().get(&channels_cache_key).await {
        record_api_metrics(&handler, &app_id, 0, cached.len()).await;
        return Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            cached,
        )
            .into_response());
    }

    let channels_map = handler
        .connection_manager()
        .get_channels_with_socket_count(&app_id)
        .await?;

    let mut channels_info_response_map = AHashMap::new();
    for (channel_name_str, socket_count) in &channels_map {
        if !channel_name_str.starts_with(filter_prefix_str) {
            continue;
        }
        validate_channel_name(&app, channel_name_str).await?;
        let mut current_channel_info_map = sonic_rs::Object::new();
        if wants_subscription_count {
            current_channel_info_map.insert("subscription_count", json!(*socket_count));
        }
        if wants_user_count {
            if channel_name_str.starts_with("presence-") {
                let members_map = ChannelManager::get_channel_members(
                    handler.connection_manager(),
                    &app_id,
                    channel_name_str,
                )
                .await?;
                current_channel_info_map.insert("user_count", json!(members_map.len()));
            } else if !filter_prefix_str.starts_with("presence-") {
                return Err(AppError::InvalidInput(
                    "user_count is only available for presence channels. Use filter_by_prefix=presence-".to_string()
                ));
            }
        }
        if !current_channel_info_map.is_empty() {
            channels_info_response_map.insert(
                channel_name_str.clone(),
                current_channel_info_map.into_value(),
            );
        } else if query_params_specific.info.is_none() {
            channels_info_response_map.insert(channel_name_str.clone(), json!({}));
        }
    }

    let response_payload = PusherMessage::channels_list(channels_info_response_map);
    let response_json_bytes = sonic_rs::to_vec(&response_payload)?;
    let response_json = String::from_utf8_lossy(&response_json_bytes).into_owned();
    // Best-effort cache; on failure the next poll simply recomputes.
    let _ = handler
        .cache_manager()
        .set(
            &channels_cache_key,
            &response_json,
            CHANNELS_LIST_CACHE_TTL_SECS,
        )
        .await;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    debug!("Channels list for app '{}' retrieved successfully", app_id);
    Ok((StatusCode::OK, Json(response_payload)).into_response())
}

/// GET /apps/{app_id}/channels/{channel_name}/users
#[instrument(skip(handler), fields(app_id = %app_id, channel = %channel_name))]
pub async fn channel_users(
    Path((app_id, channel_name)): Path<(String, String)>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Result<impl IntoResponse, AppError> {
    debug!("Request for users in channel: {}", channel_name);
    validate_channel_name(&app, &channel_name).await?;

    if !channel_name.starts_with("presence-") {
        return Err(AppError::InvalidInput(
            "Only presence channels support this endpoint".to_string(),
        ));
    }

    let channel_members_map =
        ChannelManager::get_channel_members(handler.connection_manager(), &app_id, &channel_name)
            .await?;
    let users_vec = channel_members_map
        .keys()
        .map(|user_id_str| json!({ "id": user_id_str }))
        .collect::<Vec<_>>();
    let response_payload_val = json!({ "users": users_vec });
    let response_json_bytes = sonic_rs::to_vec(&response_payload_val)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    info!(
        user_count = users_vec.len(),
        "Channel users for '{}' retrieved successfully", channel_name
    );
    Ok((StatusCode::OK, Json(response_payload_val)))
}

/// POST /apps/{app_id}/revocations
#[instrument(skip(handler, body), fields(app_id = %app_id))]
pub async fn revoke_capability_tokens(
    Path(app_id): Path<String>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    Json(body): Json<RevokeCapabilityTokensRequest>,
) -> Result<impl IntoResponse, AppError> {
    let result = handler
        .revoke_capability_tokens(
            &app,
            CapabilityRevocationRequest {
                jti: body.jti,
                client_id: body.client_id,
                expires_at: body.expires_at,
                ttl_seconds: body.ttl_seconds,
                reason: body.reason,
            },
        )
        .await?;

    let response = RevokeCapabilityTokensResponse {
        revoked_jti: result.revoked_jti,
        revoked_client_id: result.revoked_client_id,
        closed_connections: result.closed_connections,
    };
    let response_json_bytes = sonic_rs::to_vec(&response)?;
    record_api_metrics(&handler, &app_id, 0, response_json_bytes.len()).await;
    Ok((StatusCode::OK, Json(response)))
}

/// POST /apps/{app_id}/users/{user_id}/terminate_connections
#[instrument(skip(handler), fields(app_id = %app_id, user_id = %user_id))]
pub async fn terminate_user_connections(
    Path((app_id, user_id)): Path<(String, String)>,
    Query(_auth_q_params_struct): Query<EventQuery>,
    Extension(app): Extension<App>,
    State(handler): State<Arc<ConnectionHandler>>,
    _uri: Uri,
    RawQuery(_raw_query_str_option): RawQuery,
) -> Result<impl IntoResponse, AppError> {
    info!(
        "Received request to terminate user connections for user_id: {}",
        user_id
    );

    handler
        .connection_manager()
        .terminate_connection(&app.id, &user_id)
        .await?;

    info!(
        "Successfully initiated termination for user_id: {}",
        user_id
    );

    let response_payload = json!({ "ok": true });
    let response_size = sonic_rs::to_vec(&response_payload)?.len();
    record_api_metrics(&handler, &app.id, 0, response_size).await;

    Ok((StatusCode::OK, Json(response_payload)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_handler::test_support::*;
    use sockudo_core::app::AppManager;
    use sockudo_core::websocket::{SocketId, WebSocketBufferConfig};
    use sockudo_protocol::{ProtocolVersion, WireFormat};
    use sonic_rs::{JsonValueTrait, Value};

    #[tokio::test]
    async fn channels_list_includes_subscription_count_when_requested() {
        let (handler, app_manager) = test_realtime_handler_harness();
        let app = test_app();
        app_manager.create_app(app.clone()).await.unwrap();

        let socket_id = SocketId::from_string("31.31").unwrap();
        handler
            .connection_manager()
            .add_socket(
                socket_id,
                test_websocket_writer().await,
                &app.id,
                app_manager as Arc<dyn AppManager + Send + Sync>,
                WebSocketBufferConfig::default(),
                ProtocolVersion::V2,
                WireFormat::Json,
                true,
                sockudo_protocol::AppendMode::Delta,
            )
            .await
            .unwrap();
        handler
            .connection_manager()
            .add_to_channel(&app.id, "private-your-channel", &socket_id)
            .await
            .unwrap();

        let response = channels(
            Path(app.id.clone()),
            Query(ChannelsQuery {
                filter_by_prefix: Some("private-".to_string()),
                info: Some("subscription_count".to_string()),
                auth_params: empty_event_query(),
            }),
            Extension(app),
            State(handler),
            Uri::from_static(
                "/apps/app-1/channels?filter_by_prefix=private-&info=subscription_count",
            ),
            RawQuery(Some(
                "filter_by_prefix=private-&info=subscription_count".to_string(),
            )),
        )
        .await
        .unwrap()
        .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = sonic_rs::from_slice(&body).unwrap();
        assert_eq!(
            json["channels"]["private-your-channel"]["subscription_count"].as_u64(),
            Some(1)
        );
    }
}
