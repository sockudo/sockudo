use super::SockudoServer;
use crate::http_handler::{
    append_message, batch_events, channel, channel_history, channel_history_purge,
    channel_history_reset, channel_history_state, channel_message, channel_message_annotations,
    channel_message_versions, channel_presence_history, channel_presence_history_reset,
    channel_presence_history_snapshot, channel_presence_history_state, channel_users, channels,
    delete_annotation, delete_message, events, fallback_404, live, metrics, publish_annotation,
    revoke_capability_tokens, stats, terminate_user_connections, up, update_message, usage,
};
use crate::middleware::pusher_api_auth_middleware;
#[cfg(feature = "push")]
use crate::push_http::{
    batch_publish as push_batch_publish, delete_channel_subscriptions, delete_device,
    delete_devices_where, delete_scheduled_job, delete_template, get_device, get_publish_status,
    get_template, list_channel_subscriptions, list_credentials, list_devices,
    list_subscription_channels, list_templates, post_apns_credential, post_delivery_status,
    post_fcm_credential, post_hms_credential, post_template, post_webpush_credential,
    post_wns_credential, publish as push_publish, register_device, upsert_channel_subscription,
};
use crate::ws_handler::handle_ws_upgrade;
use axum::extract::DefaultBodyLimit;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::header::HeaderName;
use axum::routing::{delete, get, post};
use axum::{Router, middleware as axum_middleware};
use sockudo_core::metrics::MetricsInterface;
use sockudo_core::origin_validation::OriginValidator;
use sockudo_core::rate_limiter::RateLimiter;
use sockudo_rate_limiter::middleware::IpKeyExtractor;
use std::str::FromStr;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, info, warn};

impl SockudoServer {
    fn build_rate_limit_layer(
        limiter: Arc<dyn RateLimiter + Send + Sync>,
        key_prefix: &str,
        trust_hops: usize,
        config_name: &str,
        metrics: Option<&Arc<dyn MetricsInterface + Send + Sync>>,
    ) -> sockudo_rate_limiter::middleware::RateLimitLayer<IpKeyExtractor> {
        let options = sockudo_rate_limiter::middleware::RateLimitOptions {
            include_headers: true,
            fail_open: false,
            key_prefix: Some(key_prefix.to_string()),
        };

        let mut rate_limit_layer = sockudo_rate_limiter::middleware::RateLimitLayer::with_options(
            limiter,
            IpKeyExtractor::new(trust_hops),
            options,
        )
        .with_config_name(config_name.to_string());

        if let Some(metrics) = metrics {
            rate_limit_layer = rate_limit_layer.with_metrics(metrics.clone());
        }

        rate_limit_layer
    }

    pub(crate) fn configure_http_routes(&self) -> Router {
        let mut cors_builder = CorsLayer::new()
            .allow_methods(
                self.config
                    .cors
                    .methods
                    .iter()
                    .map(|s| Method::from_str(s).expect("Failed to parse CORS method"))
                    .collect::<Vec<_>>(),
            )
            .allow_headers(
                self.config
                    .cors
                    .allowed_headers
                    .iter()
                    .map(|s| HeaderName::from_str(s).expect("Failed to parse CORS header"))
                    .collect::<Vec<_>>(),
            );

        let use_allow_origin_any = self
            .config
            .cors
            .origin
            .iter()
            .any(|s| s == "*" || s.eq_ignore_ascii_case("any"));

        if use_allow_origin_any {
            cors_builder = cors_builder.allow_origin(AllowOrigin::any());
            if self.config.cors.credentials {
                warn!(
                    "CORS config: 'Access-Control-Allow-Credentials' was true but 'Access-Control-Allow-Origin' is '*'. Forcing credentials to false to comply with CORS specification."
                );
                cors_builder = cors_builder.allow_credentials(false);
            }
            if self.config.cors.origin.len() > 1 {
                warn!(
                    "CORS config: Wildcard '*' or 'Any' is present in origins list along with other specific origins. Wildcard will take precedence, allowing all origins."
                );
            }
        } else if !self.config.cors.origin.is_empty() {
            let allowed_origins = self.config.cors.origin.clone();
            cors_builder = cors_builder.allow_origin(AllowOrigin::predicate(
                move |origin: &HeaderValue, _parts: &http::request::Parts| {
                    origin.to_str().is_ok_and(|origin_str| {
                        OriginValidator::validate_origin(origin_str, &allowed_origins)
                    })
                },
            ));
            cors_builder = cors_builder.allow_credentials(self.config.cors.credentials);
        } else {
            warn!(
                "CORS origins list is empty and no wildcard ('*' or 'Any') is specified. CORS might be highly restrictive or disabled depending on tower-http defaults. Consider setting origins or '*' for AllowOrigin::any()."
            );
            if self.config.cors.credentials {
                warn!(
                    "CORS origins list is empty, and credentials set to true. Forcing credentials to false for safety as no origin is explicitly allowed."
                );
                cors_builder = cors_builder.allow_credentials(false);
            }
        }

        let cors = cors_builder;

        let api_rate_limiter_middleware_layer = if self.config.rate_limiter.enabled {
            if let Some(rate_limiter_instance) = &self.state.http_api_rate_limiter {
                let trust_hops = self
                    .config
                    .rate_limiter
                    .api_rate_limit
                    .trust_hops
                    .unwrap_or(0) as usize;

                info!(
                    "Applying HTTP API rate limiting middleware with trust_hops: {}",
                    trust_hops
                );
                Some(Self::build_rate_limit_layer(
                    rate_limiter_instance.clone(),
                    "api",
                    trust_hops,
                    "api_rate_limit",
                    self.state.metrics.as_ref(),
                ))
            } else {
                warn!(
                    "Rate limiting is enabled in config, but no RateLimiter instance found in server state for HTTP API. Rate limiting will not be applied."
                );
                None
            }
        } else {
            info!("Custom HTTP API Rate limiting is disabled in configuration.");
            None
        };

        let websocket_rate_limiter_middleware_layer = if self.config.rate_limiter.enabled {
            if let Some(rate_limiter_instance) = &self.state.websocket_rate_limiter {
                let trust_hops = self
                    .config
                    .rate_limiter
                    .websocket_rate_limit
                    .trust_hops
                    .unwrap_or(0) as usize;

                info!(
                    "Applying WebSocket rate limiting middleware with trust_hops: {}",
                    trust_hops
                );
                Some(Self::build_rate_limit_layer(
                    rate_limiter_instance.clone(),
                    "websocket_connect",
                    trust_hops,
                    "websocket_rate_limit",
                    self.state.metrics.as_ref(),
                ))
            } else {
                warn!(
                    "Rate limiting is enabled in config, but no RateLimiter instance found for WebSocket upgrades. WebSocket rate limiting will not be applied."
                );
                None
            }
        } else {
            info!("Custom WebSocket rate limiting is disabled in configuration.");
            None
        };

        let body_limit_bytes =
            (self.config.http_api.request_limit_in_mb as usize).saturating_mul(1024 * 1024);
        debug!(
            "Configuring Axum DefaultBodyLimit to {} MB ({} bytes)",
            self.config.http_api.request_limit_in_mb, body_limit_bytes
        );

        let mut websocket_router = Router::new().route("/app/{appKey}", get(handle_ws_upgrade));
        if let Some(middleware) = websocket_rate_limiter_middleware_layer {
            websocket_router = websocket_router.layer(middleware);
        }

        let mut api_router = Router::new()
            .route(
                "/apps/{appId}/events",
                post(events).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/batch_events",
                post(batch_events).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/revocations",
                post(revoke_capability_tokens).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels",
                get(channels).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}",
                get(channel).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}",
                get(channel_message).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}/versions",
                get(channel_message_versions).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}/annotations",
                get(channel_message_annotations)
                    .post(publish_annotation)
                    .route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}/annotations/{annotationSerial}",
                delete(delete_annotation).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}/update",
                post(update_message).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}/delete",
                post(delete_message).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/messages/{messageSerial}/append",
                post(append_message).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/history",
                get(channel_history).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/presence/history",
                get(channel_presence_history).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/presence/history/state",
                get(channel_presence_history_state).route_layer(
                    axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    ),
                ),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/presence/history/reset",
                post(channel_presence_history_reset).route_layer(
                    axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    ),
                ),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/presence/history/snapshot",
                get(channel_presence_history_snapshot).route_layer(
                    axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    ),
                ),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/history/state",
                get(channel_history_state).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/history/reset",
                post(channel_history_reset).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/history/purge",
                post(channel_history_purge).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/channels/{channelName}/users",
                get(channel_users).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            )
            .route(
                "/apps/{appId}/users/{userId}/terminate_connections",
                post(terminate_user_connections).route_layer(axum_middleware::from_fn_with_state(
                    self.handler.clone(),
                    pusher_api_auth_middleware,
                )),
            );

        #[cfg(feature = "push")]
        {
            api_router = api_router
                .route(
                    "/apps/{appId}/push/credentials/fcm",
                    post(post_fcm_credential).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/credentials/apns",
                    post(post_apns_credential).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/credentials/webpush",
                    post(post_webpush_credential).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/credentials/hms",
                    post(post_hms_credential).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/credentials/wns",
                    post(post_wns_credential).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/credentials",
                    get(list_credentials).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/templates",
                    post(post_template).get(list_templates).route_layer(
                        axum_middleware::from_fn_with_state(
                            self.handler.clone(),
                            pusher_api_auth_middleware,
                        ),
                    ),
                )
                .route(
                    "/apps/{appId}/push/templates/{id}",
                    get(get_template).delete(delete_template).route_layer(
                        axum_middleware::from_fn_with_state(
                            self.handler.clone(),
                            pusher_api_auth_middleware,
                        ),
                    ),
                )
                .route(
                    "/apps/{appId}/push/deviceRegistrations",
                    post(register_device)
                        .get(list_devices)
                        .delete(delete_devices_where)
                        .route_layer(axum_middleware::from_fn_with_state(
                            self.handler.clone(),
                            pusher_api_auth_middleware,
                        )),
                )
                .route(
                    "/apps/{appId}/push/deviceRegistrations/{id}",
                    get(get_device).delete(delete_device).route_layer(
                        axum_middleware::from_fn_with_state(
                            self.handler.clone(),
                            pusher_api_auth_middleware,
                        ),
                    ),
                )
                .route(
                    "/apps/{appId}/push/channelSubscriptions",
                    post(upsert_channel_subscription)
                        .get(list_channel_subscriptions)
                        .delete(delete_channel_subscriptions)
                        .route_layer(axum_middleware::from_fn_with_state(
                            self.handler.clone(),
                            pusher_api_auth_middleware,
                        )),
                )
                .route(
                    "/apps/{appId}/push/channelSubscriptions/channels",
                    get(list_subscription_channels).route_layer(
                        axum_middleware::from_fn_with_state(
                            self.handler.clone(),
                            pusher_api_auth_middleware,
                        ),
                    ),
                )
                .route(
                    "/apps/{appId}/push/publish",
                    post(push_publish).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/batch/publish",
                    post(push_batch_publish).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/publish/{publishId}/status",
                    get(get_publish_status).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/scheduled/{jobId}",
                    delete(delete_scheduled_job).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .route(
                    "/apps/{appId}/push/deliveryStatus",
                    post(post_delivery_status).route_layer(axum_middleware::from_fn_with_state(
                        self.handler.clone(),
                        pusher_api_auth_middleware,
                    )),
                )
                .layer(axum::Extension(self.state.push_queue.clone()))
                .layer(axum::Extension(self.state.push_store.clone()))
                .layer(axum::Extension(self.state.push_admission.clone()))
                .layer(axum::Extension(
                    self.state.push_acceptance_rate_limiter.clone(),
                ));
        }

        // Prevent idle HTTP connection buildup on API routes
        api_router = api_router.layer(axum_middleware::map_response(
            |mut response: axum::response::Response| async move {
                response.headers_mut().insert(
                    axum::http::header::CONNECTION,
                    axum::http::HeaderValue::from_static("close"),
                );
                response
            },
        ));

        if let Some(middleware) = api_rate_limiter_middleware_layer {
            api_router = api_router.layer(middleware);
        }

        let mut router = Router::new()
            .merge(websocket_router)
            .merge(api_router)
            .route("/up", get(up))
            .route("/up/{appId}", get(up))
            .route("/live", get(live));

        if self.config.http_api.usage_enabled {
            router = router.route("/usage", get(usage));
            router = router.route("/stats", get(stats));
        }

        router = router
            .layer(DefaultBodyLimit::max(body_limit_bytes))
            .layer(cors);

        // Return plain text 404 for unmatched routes.
        // Without this, Axum returns an empty-body 404 which nginx may serve as a file download.
        router = router.fallback(fallback_404);

        router.with_state(self.handler.clone())
    }

    pub(crate) fn configure_metrics_routes(&self) -> Router {
        Router::new()
            .route("/metrics", get(metrics))
            .with_state(self.handler.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use sockudo_rate_limiter::memory_limiter::MemoryRateLimiter;
    use tower::ServiceExt;

    async fn ok_handler() -> StatusCode {
        StatusCode::OK
    }

    fn scoped_test_router() -> Router {
        let api_layer = SockudoServer::build_rate_limit_layer(
            Arc::new(MemoryRateLimiter::new(1, 60)),
            "api",
            0,
            "api_rate_limit",
            None,
        );
        let websocket_layer = SockudoServer::build_rate_limit_layer(
            Arc::new(MemoryRateLimiter::new(1, 60)),
            "websocket_connect",
            0,
            "websocket_rate_limit",
            None,
        );

        Router::new()
            .merge(
                Router::new()
                    .route("/app/{appKey}", get(ok_handler))
                    .layer(websocket_layer),
            )
            .merge(
                Router::new()
                    .route("/apps/{appId}/events", post(ok_handler))
                    .layer(api_layer),
            )
    }

    #[tokio::test]
    async fn websocket_rate_limit_is_scoped_to_websocket_routes() {
        let app = scoped_test_router();

        let ws_request = Request::builder()
            .method("GET")
            .uri("/app/demo-key")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let ws_response = app.clone().oneshot(ws_request).await.unwrap();
        assert_eq!(ws_response.status(), StatusCode::OK);

        let api_request = Request::builder()
            .method("POST")
            .uri("/apps/demo/events")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let api_response = app.clone().oneshot(api_request).await.unwrap();
        assert_eq!(api_response.status(), StatusCode::OK);

        let second_ws_request = Request::builder()
            .method("GET")
            .uri("/app/demo-key")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let second_ws_response = app.oneshot(second_ws_request).await.unwrap();
        assert_eq!(second_ws_response.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn api_rate_limit_is_scoped_to_api_routes() {
        let app = scoped_test_router();

        let api_request = Request::builder()
            .method("POST")
            .uri("/apps/demo/events")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let api_response = app.clone().oneshot(api_request).await.unwrap();
        assert_eq!(api_response.status(), StatusCode::OK);

        let ws_request = Request::builder()
            .method("GET")
            .uri("/app/demo-key")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let ws_response = app.clone().oneshot(ws_request).await.unwrap();
        assert_eq!(ws_response.status(), StatusCode::OK);

        let second_api_request = Request::builder()
            .method("POST")
            .uri("/apps/demo/events")
            .header("x-real-ip", "127.0.0.1")
            .body(Body::empty())
            .unwrap();
        let second_api_response = app.oneshot(second_api_request).await.unwrap();
        assert_eq!(second_api_response.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
