//! Ably-compatible REST endpoints for stats, history, presence, mutations, and tokens.

use super::*;

pub(super) const ABLY_MAX_STATS_FIXTURE_INTERVALS: usize = 10_000;

pub async fn ably_ingest_stats(
    Query(query): Query<AblyStatsQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    if !runtime.hub.config.stats_fixture_ingest_enabled {
        return ably_error_response_format(
            StatusCode::NOT_FOUND,
            40400,
            "Stats fixture ingestion is disabled",
            format,
        );
    }
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let fixtures = match serde_json::from_slice::<Vec<serde_json::Value>>(&body) {
        Ok(fixtures) => fixtures,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!("Invalid stats interval payload: {error}"),
                format,
            );
        }
    };
    if fixtures.len() > ABLY_MAX_STATS_FIXTURE_INTERVALS {
        return ably_error_response_format(
            StatusCode::PAYLOAD_TOO_LARGE,
            40009,
            format!("Stats fixture exceeds the {ABLY_MAX_STATS_FIXTURE_INTERVALS}-interval limit"),
            format,
        );
    }
    if let Err(error) = runtime
        .hub
        .stats
        .ingest_fixtures(&resolved.app.id, fixtures)
        .await
    {
        return ably_app_error_response_format(stats_app_error(error), format);
    }
    encode_ably_rest_response(StatusCode::CREATED, format, &json!({}))
        .unwrap_or_else(ably_app_error_response)
}

/// Return typed interval statistics from the canonical stats store.
pub async fn ably_stats(
    Query(query): Query<AblyStatsQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let stats_query = match StatsQuery::parse(
        query
            .by
            .as_deref()
            .or(query.unit.as_deref())
            .unwrap_or("minute"),
        query.direction.as_deref(),
        query.start.as_deref(),
        query.end.as_deref(),
        query.limit,
        query.cursor.clone(),
    ) {
        Ok(query) => query,
        Err(error) => return ably_app_error_response_format(stats_app_error(error), format),
    };
    match runtime
        .hub
        .stats
        .query(&resolved.app.id, &stats_query, now_ms())
        .await
    {
        Ok(page) => {
            let mut response = match encode_ably_rest_response(StatusCode::OK, format, &page.items)
            {
                Ok(response) => response,
                Err(error) => return ably_app_error_response(error),
            };
            match stats_link_header(&query, &stats_query, page.next_cursor.as_deref()).parse() {
                Ok(value) => {
                    response.headers_mut().insert(header::LINK, value);
                    response
                }
                Err(error) => ably_app_error_response(AppError::InternalError(format!(
                    "invalid stats Link header: {error}"
                ))),
            }
        }
        Err(error) => ably_app_error_response_format(stats_app_error(error), format),
    }
}

pub(super) fn stats_app_error(error: StatsError) -> AppError {
    match error {
        StatsError::InvalidQuery(message) | StatsError::InvalidFixture(message) => {
            AppError::InvalidInput(message)
        }
        StatsError::Store(message) => AppError::InternalError(message),
        StatsError::Closed => AppError::InternalError("stats aggregator is closed".to_string()),
        StatsError::QueueFull => AppError::InternalError("stats queue is full".to_string()),
    }
}

pub(super) fn stats_link_header(
    raw_query: &AblyStatsQuery,
    query: &StatsQuery,
    next: Option<&str>,
) -> String {
    let params = |cursor: Option<&str>| {
        let mut params = vec![
            format!("limit={}", query.limit),
            format!("direction={}", query.direction.as_str()),
            format!("unit={}", query.unit.as_str()),
        ];
        if let Some(start) = query.start.as_deref() {
            params.push(format!("start={}", urlencoding::encode(start)));
        }
        if let Some(end) = query.end.as_deref() {
            params.push(format!("end={}", urlencoding::encode(end)));
        }
        if let Some(format) = raw_query.format.as_deref() {
            params.push(format!("format={}", urlencoding::encode(format)));
        }
        if let Some(cursor) = cursor {
            params.push(format!("cursor={}", urlencoding::encode(cursor)));
        }
        params.join("&")
    };
    let mut links = vec![format!("<./stats?{}>; rel=\"first\"", params(None))];
    if let Some(next) = next {
        links.push(format!("<./stats?{}>; rel=\"next\"", params(Some(next))));
    }
    links.join(", ")
}

pub async fn ably_channel_history(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyHistoryQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    match ably_channel_history_inner(&runtime.hub, channel_name, query, headers, handler).await {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub async fn ably_not_found(Query(query): Query<AblyRestQuery>, headers: HeaderMap) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    ably_error_response_format(StatusCode::NOT_FOUND, 40400, "Not found", format)
}

pub(super) async fn ably_channel_presence(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyPresenceQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.auth_client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(error.status, error.code, error.message, format);
        }
    };
    let channel = match parse_ably_channel_name(channel_name) {
        Ok(channel) => channel,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel,
        AblyCapabilityCheck::Presence,
    ) {
        return ably_error_response_format(error.status, error.code, error.message, format);
    }
    let limit = query.limit.unwrap_or(100).clamp(1, 1_000);
    let offset = match decode_presence_snapshot_cursor(query.cursor.as_deref()) {
        Ok(offset) => offset,
        Err(error) => return ably_app_error_response_format(error, format),
    };
    let snapshot = match PresenceService::new(Arc::clone(&handler))
        .snapshot(&resolved.app.id, channel.base())
        .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => return ably_app_error_response_format(AppError::from(error), format),
    };
    let matching = snapshot
        .into_iter()
        .map(|record| ably_presence_from_record(record, 1))
        .filter(|member| {
            query
                .client_id
                .as_deref()
                .is_none_or(|client_id| member.client_id.as_deref() == Some(client_id))
                && query.connection_id.as_deref().is_none_or(|connection_id| {
                    member.connection_id.as_deref() == Some(connection_id)
                })
        })
        .collect::<Vec<_>>();
    let members = matching
        .iter()
        .skip(offset)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let next_offset = (offset + members.len() < matching.len()).then_some(offset + members.len());
    let links = ably_presence_link_header(
        &query,
        "presence",
        limit,
        next_offset.map(|next| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(u64::try_from(next).unwrap_or(u64::MAX).to_be_bytes())
        }),
    );
    let mut response = match encode_ably_rest_response(StatusCode::OK, format, &members) {
        Ok(response) => response,
        Err(error) => return ably_app_error_response(error),
    };
    match links
        .parse()
        .map_err(|error| AppError::InternalError(format!("invalid presence Link header: {error}")))
    {
        Ok(value) => {
            response.headers_mut().insert(header::LINK, value);
            response
        }
        Err(error) => ably_app_error_response_format(error, format),
    }
}

pub(super) async fn ably_channel_presence_history(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyPresenceQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    match ably_channel_presence_history_inner(&runtime.hub, channel_name, query, headers, handler)
        .await
    {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, format),
    }
}

pub(super) async fn ably_channel_presence_history_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyPresenceQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
) -> Result<Response, AppError> {
    let format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.auth_client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let channel = parse_ably_channel_name(channel_name)?;
    ensure_ably_channel_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel,
        AblyCapabilityCheck::History,
    )?;
    let policy = resolved
        .app
        .resolved_presence_history(channel.base(), &handler.server_options().presence_history);
    let direction = match query.direction.as_deref().unwrap_or("backwards") {
        "forwards" => PresenceHistoryDirection::OldestFirst,
        "backwards" => PresenceHistoryDirection::NewestFirst,
        other => {
            return Err(AppError::InvalidInput(format!(
                "Invalid Ably presence history direction '{other}'"
            )));
        }
    };
    let stream_state = handler
        .presence_history_store()
        .stream_runtime_state(&resolved.app.id, channel.base())
        .await?;
    if !stream_state.continuity_proven {
        return Err(AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message: format!(
                "presence history continuity is unavailable: {}",
                stream_state
                    .reason
                    .as_deref()
                    .unwrap_or(stream_state.durable_state.as_str())
            ),
        });
    }
    let limit = query
        .limit
        .unwrap_or(policy.max_page_size)
        .clamp(1, policy.max_page_size);
    let cursor = query
        .cursor
        .as_deref()
        .map(PresenceHistoryCursor::decode)
        .transpose()?;
    let bounds = PresenceHistoryQueryBounds {
        start_serial: None,
        end_serial: None,
        start_time_ms: query.start,
        end_time_ms: query.end,
    };
    let page = handler
        .presence_history_store()
        .read_filtered_page(
            PresenceHistoryReadRequest {
                app_id: resolved.app.id.clone(),
                channel: channel.base().to_string(),
                direction,
                limit,
                cursor,
                bounds,
            },
            PresenceHistoryFilter {
                user_id: query.client_id.clone(),
                connection_id: query.connection_id.clone(),
            },
        )
        .await?;
    if page.degraded {
        return Err(AppError::Protocol {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: 50003,
            message: "presence history store is degraded".to_string(),
        });
    }
    let next_cursor = page
        .next_cursor
        .as_ref()
        .map(PresenceHistoryCursor::encode)
        .transpose()?;
    let mut messages = Vec::with_capacity(page.items.len());
    for item in page.items {
        #[derive(Deserialize)]
        struct StoredPresenceData {
            user_info: Option<Value>,
        }
        let payload: StoredPresenceData = sonic_rs::from_slice(item.payload_bytes.as_ref())
            .map_err(|error| AppError::InternalError(error.to_string()))?;
        let stored = payload
            .user_info
            .as_ref()
            .and_then(decode_stored_presence_data);
        let native = payload.user_info.as_ref().and_then(decode_presence_record);
        let message = AblyPresenceMessage {
            id: native
                .as_ref()
                .map(|native| native.id.clone())
                .or_else(|| stored.as_ref().and_then(|stored| stored.id.clone()))
                .or_else(|| Some(format!("{}:{}:0", item.stream_id, item.serial))),
            action: Some(match item.event {
                PresenceHistoryEventKind::MemberAdded => 2,
                PresenceHistoryEventKind::MemberUpdated => 4,
                PresenceHistoryEventKind::MemberRemoved => 3,
            }),
            client_id: Some(item.user_id.clone()),
            connection_id: item.connection_id.clone(),
            data: if let Some(native) = native.as_ref() {
                native.data.clone()
            } else if let Some(stored) = stored.as_ref() {
                stored.data.clone()
            } else {
                payload.user_info.clone()
            },
            encoding: native
                .as_ref()
                .and_then(|native| native.encoding.clone())
                .or_else(|| stored.as_ref().and_then(|stored| stored.encoding.clone())),
            timestamp: Some(
                native
                    .as_ref()
                    .map_or(item.published_at_ms, |native| native.timestamp_ms),
            ),
            extras: native
                .as_ref()
                .and_then(|native| native.extras.clone())
                .or_else(|| stored.as_ref().and_then(|stored| stored.extras.clone())),
        };
        messages.push(message);
    }
    let links = ably_presence_link_header(&query, "presence/history", limit, next_cursor);
    let mut response = encode_ably_rest_response(StatusCode::OK, format, &messages)?;
    response.headers_mut().insert(
        header::LINK,
        links.parse().map_err(|error| {
            AppError::InternalError(format!("invalid presence history Link header: {error}"))
        })?,
    );
    Ok(response)
}

pub async fn ably_batch_publish(
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    match ably_batch_publish_inner(Arc::clone(&runtime.hub), query, headers, handler, body).await {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub async fn ably_batch_presence(
    Query(query): Query<AblyBatchPresenceQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let result = async {
        let resolved = resolve_ably_auth(
            &runtime.hub,
            &handler,
            &headers,
            query.key.as_deref(),
            query.access_token.as_deref(),
            query.client_id.as_deref(),
        )
        .await
        .map_err(ably_auth_app_error)?;
        let requested = query
            .channels
            .split(',')
            .map(str::to_string)
            .collect::<Vec<_>>();
        if requested.is_empty() || requested.len() > ABLY_MAX_BATCH_CHANNELS {
            return Err(AppError::InvalidInput(format!(
                "Ably batch presence requires 1..={ABLY_MAX_BATCH_CHANNELS} channels"
            )));
        }
        let mut work = Vec::with_capacity(requested.len());
        for requested_channel in requested {
            let channel = match parse_ably_channel_name(requested_channel.clone()) {
                Ok(channel) => channel,
                Err(error) => {
                    work.push(AblyBatchPresenceWork::Ready(
                        AblyBatchPresenceChannelResponse::Failure {
                            channel: requested_channel,
                            error: error_info(StatusCode::BAD_REQUEST, 40010, error.to_string()),
                        },
                    ));
                    continue;
                }
            };
            if let Err(error) = ensure_ably_channel_capability(
                resolved.capabilities.as_ref(),
                &channel,
                AblyCapabilityCheck::Presence,
            ) {
                work.push(AblyBatchPresenceWork::Ready(
                    AblyBatchPresenceChannelResponse::Failure {
                        channel: channel.requested().to_string(),
                        error: error_info(StatusCode::UNAUTHORIZED, error.code, error.message),
                    },
                ));
                continue;
            }
            work.push(AblyBatchPresenceWork::Snapshot(channel));
        }
        let app_id: Arc<str> = Arc::from(resolved.app.id.as_str());
        let service = PresenceService::new(Arc::clone(&handler));
        let responses = run_bounded_ordered(work, ABLY_BATCH_CONCURRENCY, move |work| {
            let app_id = Arc::clone(&app_id);
            let service = service.clone();
            async move {
                match work {
                    AblyBatchPresenceWork::Ready(response) => response,
                    AblyBatchPresenceWork::Snapshot(channel) => {
                        match service.snapshot(&app_id, channel.base()).await {
                            Ok(records) => AblyBatchPresenceChannelResponse::Success {
                                channel: channel.requested().to_string(),
                                presence: records
                                    .into_iter()
                                    .map(|record| ably_presence_from_record(record, 1))
                                    .collect(),
                            },
                            Err(error) => AblyBatchPresenceChannelResponse::Failure {
                                channel: channel.requested().to_string(),
                                error: ably_error_info_from_app_error(AppError::from(error)),
                            },
                        }
                    }
                }
            }
        })
        .await?;
        let success_count = responses
            .iter()
            .filter(|result| matches!(result, AblyBatchPresenceChannelResponse::Success { .. }))
            .count();
        encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &AblyBatchResult {
                success_count,
                failure_count: responses.len().saturating_sub(success_count),
                results: responses,
            },
        )
    }
    .await;
    result.unwrap_or_else(|error| ably_app_error_response_format(error, response_format))
}

pub(super) fn validate_ably_batch_publish_requests(
    handler: &ConnectionHandler,
    requests: &[AblyBatchPublishRequest],
) -> Result<(), AppError> {
    if requests.is_empty() || requests.len() > ABLY_MAX_BATCH_SPECS {
        return Err(AppError::InvalidInput(format!(
            "Ably batch requires 1..={ABLY_MAX_BATCH_SPECS} specs"
        )));
    }

    let native_channel_limit =
        usize::try_from(handler.server_options().event_limits.max_channels_at_once)
            .unwrap_or(usize::MAX);
    let native_message_limit =
        usize::try_from(handler.server_options().event_limits.max_batch_size).unwrap_or(usize::MAX);
    let channel_limit = ABLY_MAX_BATCH_CHANNELS.min(native_channel_limit);
    let message_limit = ABLY_MAX_BATCH_MESSAGES.min(native_message_limit);
    let mut total_results = 0usize;
    let mut total_operations = 0usize;

    for request in requests {
        if request.channels.is_empty() || request.channels.len() > channel_limit {
            return Err(AppError::InvalidInput(format!(
                "Ably batch requires 1..={channel_limit} channels per spec"
            )));
        }
        let message_count = request.messages.len();
        if message_count == 0 || message_count > message_limit {
            return Err(AppError::InvalidInput(format!(
                "Ably batch requires 1..={message_limit} messages per spec"
            )));
        }
        total_results = total_results
            .checked_add(request.channels.len())
            .ok_or_else(|| {
                AppError::InvalidInput("Ably batch result count overflow".to_string())
            })?;
        let operations = request
            .channels
            .len()
            .checked_mul(message_count)
            .ok_or_else(|| {
                AppError::InvalidInput("Ably batch operation count overflow".to_string())
            })?;
        total_operations = total_operations.checked_add(operations).ok_or_else(|| {
            AppError::InvalidInput("Ably batch operation count overflow".to_string())
        })?;
    }

    if total_results > ABLY_MAX_BATCH_RESULTS {
        return Err(AppError::InvalidInput(format!(
            "Ably batch exceeds the {ABLY_MAX_BATCH_RESULTS}-result limit"
        )));
    }
    if total_operations > ABLY_MAX_BATCH_OPERATIONS {
        return Err(AppError::InvalidInput(format!(
            "Ably batch exceeds the {ABLY_MAX_BATCH_OPERATIONS}-publish-operation limit"
        )));
    }
    Ok(())
}

pub(super) async fn publish_ably_batch_channel(
    handler: Arc<ConnectionHandler>,
    hub: Arc<AblyCompatHub>,
    resolved: Arc<ResolvedAblyAuth>,
    prepared_messages: Arc<[PreparedAblyBatchMessage]>,
    message_id: Arc<str>,
    requested_channel: String,
) -> AblyBatchPublishChannelResponse {
    let channel = match parse_ably_channel_name(requested_channel.clone()) {
        Ok(channel) => channel,
        Err(error) => {
            return AblyBatchPublishChannelResponse::Failure {
                channel: requested_channel,
                error: error_info(StatusCode::BAD_REQUEST, 40010, error.to_string()),
            };
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel,
        AblyCapabilityCheck::Publish,
    ) {
        return AblyBatchPublishChannelResponse::Failure {
            channel: channel.requested().to_string(),
            error: error_info(StatusCode::UNAUTHORIZED, error.code, error.message),
        };
    }

    let mut serials = Vec::with_capacity(prepared_messages.len());
    for prepared in prepared_messages.iter() {
        match publish_ably_message(
            AblyMessagePublishContext {
                handler: &handler,
                hub: &hub,
                app: &resolved.app,
                channel: channel.base(),
                connection_id: prepared.connection_id.as_deref(),
                client_id: prepared.client_id.as_deref(),
                capabilities: resolved.capabilities.as_ref(),
                privileged_server: resolved.capabilities.is_none(),
            },
            prepared.message.clone(),
        )
        .await
        {
            Ok(serial) => serials.push(serial),
            Err(error) => {
                return AblyBatchPublishChannelResponse::Failure {
                    channel: channel.requested().to_string(),
                    error: ably_error_info_from_app_error(error),
                };
            }
        }
    }

    let message_count = u64::try_from(prepared_messages.len()).unwrap_or(u64::MAX);
    let message_bytes = sonic_rs::to_vec(
        &prepared_messages
            .iter()
            .map(|item| &item.message)
            .collect::<Vec<_>>(),
    )
    .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
    .unwrap_or_default();
    let stats_result = StatsObservation::messages(
        &resolved.app.id,
        now_ms(),
        "inbound",
        "rest",
        message_count,
        message_bytes,
    )
    .map_err(stats_app_error);
    if let Err(error) = match stats_result {
        Ok(observation) => hub.stats.record(observation).await.map_err(stats_app_error),
        Err(error) => Err(error),
    } {
        return AblyBatchPublishChannelResponse::Failure {
            channel: channel.requested().to_string(),
            error: ably_error_info_from_app_error(error),
        };
    }

    AblyBatchPublishChannelResponse::Success {
        channel: channel.requested().to_string(),
        message_id: message_id.to_string(),
        serials,
    }
}

pub(super) async fn ably_batch_publish_inner(
    hub: Arc<AblyCompatHub>,
    query: AblyRestQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Result<Response, AppError> {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    if body.len() > ABLY_MAX_BATCH_BYTES {
        return Err(AppError::Protocol {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: 40009,
            message: format!("Ably batch body exceeds the {ABLY_MAX_BATCH_BYTES}-byte limit"),
        });
    }
    let resolved = resolve_ably_auth(
        &hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let request_body: AblyBatchPublishBody = decode_value(body.as_ref(), request_format)
        .map_err(|error| AppError::InvalidInput(format!("Invalid Ably batch body: {error}")))?;
    let (requests, single_request) = match request_body {
        AblyBatchPublishBody::One(request) => (vec![request], true),
        AblyBatchPublishBody::Many(requests) => (requests, false),
    };
    validate_ably_batch_publish_requests(&handler, &requests)?;
    let resolved = Arc::new(resolved);
    let mut batch_responses = Vec::with_capacity(requests.len());
    for request in requests {
        let messages = request.messages.into_messages();
        let prepared_messages: Arc<[PreparedAblyBatchMessage]> = messages
            .into_iter()
            .map(|message| {
                validate_ably_publish_message(&message, true)?;
                let (connection_id, effective_client_id) = rest_publish_identity(
                    &hub,
                    &resolved.app.id,
                    resolved.client_id.as_deref(),
                    &message,
                )?;
                Ok(PreparedAblyBatchMessage {
                    message,
                    connection_id,
                    client_id: effective_client_id,
                })
            })
            .collect::<Result<Vec<_>, AppError>>()?
            .into();
        let message_id: Arc<str> = Arc::from(Uuid::new_v4().to_string());
        let responses = run_bounded_ordered(request.channels, ABLY_BATCH_CONCURRENCY, {
            let handler = Arc::clone(&handler);
            let hub = Arc::clone(&hub);
            let message_id = Arc::clone(&message_id);
            let prepared_messages = Arc::clone(&prepared_messages);
            let resolved = Arc::clone(&resolved);
            move |requested_channel| {
                publish_ably_batch_channel(
                    Arc::clone(&handler),
                    Arc::clone(&hub),
                    Arc::clone(&resolved),
                    Arc::clone(&prepared_messages),
                    Arc::clone(&message_id),
                    requested_channel,
                )
            }
        })
        .await?;
        let success_count = responses
            .iter()
            .filter(|result| matches!(result, AblyBatchPublishChannelResponse::Success { .. }))
            .count();
        batch_responses.push(AblyBatchResult {
            success_count,
            failure_count: responses.len().saturating_sub(success_count),
            results: responses,
        });
    }

    if single_request {
        let response = batch_responses
            .pop()
            .ok_or_else(|| AppError::InternalError("missing Ably batch response".to_string()))?;
        encode_ably_legacy_batch_publish_response(response_format, response.results)
    } else {
        encode_ably_rest_response(StatusCode::OK, response_format, &batch_responses)
    }
}

pub async fn ably_channel_publish(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let response_format = ably_rest_response_format(
        &headers,
        query.format.as_deref(),
        ably_rest_request_format(&headers),
    );
    match ably_channel_publish_inner(&runtime.hub, channel_name, query, headers, handler, body)
        .await
    {
        Ok(response) => response,
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub(super) async fn ably_channel_publish_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyRestQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
    body: Bytes,
) -> Result<Response, AppError> {
    let request_bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let channel_name = parse_ably_channel_name(channel_name)?;
    ensure_ably_channel_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::Publish,
    )?;

    let messages = decode_ably_publish_payload(body.as_ref(), request_format)?;
    if messages.is_empty() {
        return Err(AppError::InvalidInput(
            "Ably REST publish requires at least one message".to_string(),
        ));
    }
    let message_count = u64::try_from(messages.len()).unwrap_or(u64::MAX);

    let prepared_messages = messages
        .into_iter()
        .map(|message| {
            validate_ably_publish_message(&message, true)?;
            let (connection_id, effective_client_id) = rest_publish_identity(
                hub,
                &resolved.app.id,
                resolved.client_id.as_deref(),
                &message,
            )?;
            Ok((message, connection_id, effective_client_id))
        })
        .collect::<Result<Vec<_>, AppError>>()?;

    let mut serials = Vec::with_capacity(prepared_messages.len());
    for (index, (message, connection_id, effective_client_id)) in
        prepared_messages.into_iter().enumerate()
    {
        let serial = publish_ably_message(
            AblyMessagePublishContext {
                handler: &handler,
                hub,
                app: &resolved.app,
                channel: channel_name.base(),
                connection_id: connection_id.as_deref(),
                client_id: effective_client_id.as_deref(),
                capabilities: resolved.capabilities.as_ref(),
                privileged_server: resolved.capabilities.is_none(),
            },
            message,
        )
        .await
        .map_err(|error| {
            AppError::InvalidInput(format!("Failed to publish message {index}: {error}"))
        })?;
        serials.push(Some(serial));
    }

    let observation = StatsObservation::messages(
        &resolved.app.id,
        now_ms(),
        "inbound",
        "rest",
        message_count,
        request_bytes,
    )
    .map_err(stats_app_error)?;
    hub.stats
        .record(observation)
        .await
        .map_err(stats_app_error)?;

    encode_ably_rest_response(
        StatusCode::CREATED,
        response_format,
        &AblyPublishResponse { serials },
    )
}

pub(super) async fn ably_channel_history_inner(
    hub: &AblyCompatHub,
    channel_name: String,
    query: AblyHistoryQuery,
    headers: HeaderMap,
    handler: Arc<ConnectionHandler>,
) -> Result<Response, AppError> {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = resolve_ably_auth(
        hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    .map_err(ably_auth_app_error)?;
    let channel_name = parse_ably_channel_name(channel_name)?;
    ensure_ably_channel_capability_app_error(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    )?;
    let history_policy = resolved
        .app
        .resolved_history(channel_name.base(), &handler.server_options().history);
    if !history_policy.enabled {
        return encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &Vec::<AblyMessage>::new(),
        );
    }
    let limit = query
        .limit
        .unwrap_or(history_policy.max_page_size)
        .min(history_policy.max_page_size)
        .max(1);
    let direction = parse_ably_history_direction(query.direction.as_deref())?;
    let cursor = match query.cursor.as_deref() {
        Some(cursor) => Some(HistoryCursor::decode(cursor)?),
        None => None,
    };
    let attach_position = match query.from_serial.as_deref() {
        Some(serial) => Some(parse_ably_channel_serial(serial).map_err(|failure| {
            AppError::InvalidInput(failure.message)
        })?),
        None if query.until_attach.unwrap_or(false) => Some(
            hub.until_attach_position(&resolved.app.id, &channel_name)
                .ok_or_else(|| {
                    AppError::Protocol {
                        status: StatusCode::SERVICE_UNAVAILABLE,
                        code: 50003,
                        message: format!(
                            "untilAttach continuity for channel '{}' is not authoritative on this node; retry through the realtime connection's routing affinity",
                            channel_name.base()
                        ),
                    }
                })?,
        ),
        None => None,
    };
    let bounds = HistoryQueryBounds {
        start_serial: None,
        end_serial: attach_position.as_ref().map(|position| position.serial),
        start_time_ms: query.start,
        end_time_ms: query.end,
    };
    let page = handler
        .history_store()
        .read_page(HistoryReadRequest {
            app_id: resolved.app.id.clone(),
            channel: channel_name.base().to_string(),
            direction,
            limit,
            cursor,
            bounds,
        })
        .await?;

    let link_header =
        ably_history_link_header(&query, direction, limit, page.next_cursor.as_ref())?;
    let versioned_messages_enabled = handler.server_options().versioned_messages.enabled;
    let decoded_items = page
        .items
        .into_iter()
        .map(|item| {
            let stored = decode_stored_message_payload(item.payload_bytes.as_ref())
                .map_err(AppError::InternalError)?;
            let message_serial = versioned_messages_enabled
                .then(|| extract_runtime_message_serial(&stored.message))
                .flatten()
                .map(parse_message_serial)
                .transpose()?;
            Ok((item, stored, message_serial))
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    let message_serials = decoded_items
        .iter()
        .filter_map(|(_, _, serial)| serial.clone())
        .collect::<Vec<_>>();
    let latest_by_serial = if versioned_messages_enabled {
        handler
            .version_store()
            .get_latest_batch(&resolved.app.id, channel_name.base(), &message_serials)
            .await?
    } else {
        BTreeMap::new()
    };

    let mut items = Vec::with_capacity(decoded_items.len());
    for (item, stored, message_serial) in decoded_items {
        let raw_message = stored.message;
        let mut envelope = stored.envelope;
        let message = if versioned_messages_enabled {
            if let Some(message_serial) = message_serial {
                match latest_by_serial.get(&message_serial) {
                    Some(latest) => {
                        envelope = latest.envelope.clone().or(envelope);
                        handler
                            .build_runtime_message_from_record(latest, Some(item.stream_id.clone()))
                    }
                    None => raw_message,
                }
            } else {
                raw_message
            }
        } else {
            raw_message
        };
        items.push(match envelope.as_ref() {
            Some(envelope) => {
                envelope_to_ably_message(envelope, &message, AblyMessageProjection::Aggregate)
                    .map_err(AppError::InvalidInput)?
            }
            None => pusher_to_ably_message(&message, AblyMessageProjection::Aggregate)
                .map_err(AppError::InvalidInput)?,
        });
    }
    let mut response = encode_ably_rest_response(StatusCode::OK, response_format, &items)?;
    response.headers_mut().insert(
        header::LINK,
        link_header.parse().map_err(|error| {
            AppError::InternalError(format!("invalid history Link header: {error}"))
        })?,
    );
    Ok(response)
}

pub async fn ably_channel_status(
    Path(channel_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::AnyChannelAccess,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    let occupancy = handler
        .connection_manager()
        .get_channel_socket_count(&resolved.app.id, channel_name.base())
        .await;
    encode_ably_rest_response(
        StatusCode::OK,
        response_format,
        &json!({
            "channelId": channel_name.requested(),
            "status": {
                "isActive": occupancy > 0,
                "occupancy": {
                    "metrics": {
                        "connections": occupancy,
                        "publishers": 0,
                        "subscribers": occupancy,
                        "presenceConnections": 0,
                        "presenceMembers": 0,
                        "presenceSubscribers": 0,
                    }
                }
            }
        }),
    )
    .unwrap_or_else(ably_app_error_response)
}

pub async fn ably_channel_message(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    match ably_channel_message_inner(handler, resolved.app, channel_name, message_serial).await {
        Ok(message) => encode_ably_rest_response(StatusCode::OK, response_format, &message)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub(super) async fn ably_channel_message_inner(
    handler: Arc<ConnectionHandler>,
    app: App,
    channel_name: AblyChannelName,
    message_serial: String,
) -> Result<AblyMessage, AppError> {
    let message_serial_value = parse_message_serial(&message_serial)?;
    let item = MessageService::new(Arc::clone(&handler))
        .get_message(&app.id, channel_name.base(), &message_serial_value)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "Message '{}' was not found in channel '{}'",
                message_serial,
                channel_name.requested()
            ))
        })?;
    let runtime_message = handler.build_runtime_message_from_record(&item, None);
    match item.envelope.as_ref() {
        Some(envelope) => {
            envelope_to_ably_message(envelope, &runtime_message, AblyMessageProjection::Aggregate)
                .map_err(AppError::InvalidInput)
        }
        None => pusher_to_ably_message(&runtime_message, AblyMessageProjection::Aggregate)
            .map_err(AppError::InvalidInput),
    }
}

pub async fn ably_channel_message_versions(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::History,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    match ably_channel_message_versions_inner(handler, resolved.app, channel_name, message_serial)
        .await
    {
        Ok(messages) => encode_ably_rest_response(StatusCode::OK, response_format, &messages)
            .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub async fn ably_publish_annotations(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let result = async {
        let resolved = resolve_ably_auth(
            &runtime.hub,
            &handler,
            &headers,
            query.key.as_deref(),
            query.access_token.as_deref(),
            query.client_id.as_deref(),
        )
        .await
        .map_err(ably_auth_app_error)?;
        let channel = parse_ably_channel_name(channel_name)?;
        let message_serial = MessageSerial::new(message_serial)?;
        let annotations: Vec<AblyAnnotation> = decode_value(body.as_ref(), request_format)
            .map_err(|error| AppError::InvalidInput(format!("Invalid annotation body: {error}")))?;
        if annotations.is_empty() {
            return Err(AppError::InvalidInput(
                "annotation request must contain at least one annotation".to_string(),
            ));
        }
        for annotation in annotations {
            let command = parse_ably_annotation_command(
                annotation,
                Some(&message_serial),
                resolved.client_id.as_deref(),
            )?;
            apply_ably_annotation_command(
                &handler,
                &resolved.app,
                &channel,
                resolved.capabilities.as_ref(),
                resolved.client_id.as_deref(),
                command,
            )
            .await?;
        }
        encode_ably_rest_response(StatusCode::CREATED, response_format, &json!({}))
    }
    .await;
    result.unwrap_or_else(|error| ably_app_error_response_format(error, response_format))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyAnnotationCursor {
    version: u8,
    app_id: String,
    channel: String,
    message_serial: String,
    annotation_serial: String,
}

pub(super) fn encode_ably_annotation_cursor(
    app_id: &str,
    channel: &str,
    message_serial: &MessageSerial,
    annotation_serial: &AnnotationSerial,
) -> Result<String, AppError> {
    let bytes = sonic_rs::to_vec(&AblyAnnotationCursor {
        version: 1,
        app_id: app_id.to_string(),
        channel: channel.to_string(),
        message_serial: message_serial.as_str().to_string(),
        annotation_serial: annotation_serial.as_str().to_string(),
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub(super) fn decode_ably_annotation_cursor(
    raw: &str,
    app_id: &str,
    channel: &str,
    message_serial: &MessageSerial,
) -> Result<AnnotationSerial, AppError> {
    let invalid = || AppError::InvalidInput("invalid annotation cursor".to_string());
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| invalid())?;
    let cursor: AblyAnnotationCursor = sonic_rs::from_slice(&bytes).map_err(|_| invalid())?;
    if cursor.version != 1
        || cursor.app_id != app_id
        || cursor.channel != channel
        || cursor.message_serial != message_serial.as_str()
    {
        return Err(invalid());
    }
    AnnotationSerial::new(cursor.annotation_serial).map_err(|_| invalid())
}

pub async fn ably_channel_annotations(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), AblyFormat::Json);
    let result = async {
        let resolved = resolve_ably_auth(
            &runtime.hub,
            &handler,
            &headers,
            query.key.as_deref(),
            query.access_token.as_deref(),
            query.client_id.as_deref(),
        )
        .await
        .map_err(ably_auth_app_error)?;
        let channel = parse_ably_channel_name(channel_name)?;
        ensure_ably_channel_capability_app_error(
            resolved.capabilities.as_ref(),
            &channel,
            AblyCapabilityCheck::AnnotationSubscribe,
        )?;
        let message_serial = MessageSerial::new(message_serial)?;
        require_ably_annotations_enabled(&handler, &resolved.app, channel.base())?;
        let limit = query.limit.unwrap_or(100).clamp(1, 1_000);
        let after = query
            .cursor
            .as_deref()
            .map(|cursor| {
                decode_ably_annotation_cursor(
                    cursor,
                    &resolved.app.id,
                    channel.base(),
                    &message_serial,
                )
            })
            .transpose()?;
        let raw = handler
            .annotation_store()
            .replay_raw(RawAnnotationReplayRequest {
                app_id: resolved.app.id.clone(),
                channel_id: channel.base().to_string(),
                message_serial: Some(message_serial.clone()),
                after_annotation_serial: after,
                limit: limit.saturating_add(1),
            })
            .await?;
        let mut matching = raw
            .into_iter()
            .take(limit.saturating_add(1))
            .collect::<Vec<_>>();
        let has_next = matching.len() > limit;
        if has_next {
            matching.pop();
        }
        let next_cursor = has_next
            .then(|| matching.last())
            .flatten()
            .map(|event| {
                encode_ably_annotation_cursor(
                    &resolved.app.id,
                    channel.base(),
                    &message_serial,
                    &event.annotation.serial,
                )
            })
            .transpose()?;
        let items = matching
            .into_iter()
            .map(|event| AblyAnnotation {
                action: Some(match event.annotation.action {
                    sockudo_core::annotations::AnnotationAction::Create => 0,
                    sockudo_core::annotations::AnnotationAction::Delete => 1,
                }),
                id: Some(event.annotation.id.as_str().to_string()),
                serial: Some(event.annotation.serial.as_str().to_string()),
                message_serial: Some(event.annotation.message_serial.as_str().to_string()),
                annotation_type: Some(event.annotation.annotation_type.as_str().to_string()),
                name: event.annotation.name,
                client_id: event.annotation.client_id,
                count: event.annotation.count,
                data: event.annotation.data,
                encoding: event.annotation.encoding,
                timestamp: Some(event.annotation.timestamp),
            })
            .collect::<Vec<_>>();
        let mut response = encode_ably_rest_response(StatusCode::OK, response_format, &items)?;
        let format_query = query
            .format
            .as_deref()
            .map(|format| format!("&format={}", urlencoding::encode(format)))
            .unwrap_or_default();
        let mut links = vec![format!(
            "<./annotations?limit={limit}{format_query}>; rel=\"first\""
        )];
        if let Some(cursor) = next_cursor {
            links.push(format!(
                "<./annotations?limit={limit}&cursor={}{format_query}>; rel=\"next\"",
                urlencoding::encode(&cursor)
            ));
        }
        let link = HeaderValue::from_str(&links.join(", ")).map_err(|error| {
            AppError::InternalError(format!("invalid annotations Link header: {error}"))
        })?;
        response.headers_mut().insert(header::LINK, link);
        Ok(response)
    }
    .await;
    result.unwrap_or_else(|error| ably_app_error_response_format(error, response_format))
}

pub async fn ably_channel_message_mutation(
    Path((channel_name, message_serial)): Path<(String, String)>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let resolved = match resolve_ably_auth(
        &runtime.hub,
        &handler,
        &headers,
        query.key.as_deref(),
        query.access_token.as_deref(),
        query.client_id.as_deref(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    let channel_name = match AblyChannelName::parse(channel_name) {
        Ok(channel_name) => channel_name,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40010,
                error.to_string(),
                response_format,
            );
        }
    };
    if let Err(error) = ensure_ably_channel_capability(
        resolved.capabilities.as_ref(),
        &channel_name,
        AblyCapabilityCheck::Publish,
    ) {
        return ably_error_response_format(
            error.status,
            error.code,
            error.message,
            response_format,
        );
    }
    let mut messages = match decode_ably_publish_payload(body.as_ref(), request_format) {
        Ok(messages) => messages,
        Err(error) => return ably_app_error_response_format(error, response_format),
    };
    let Some(mut message) = messages.pop() else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "mutation message is required",
            response_format,
        );
    };
    if let Err(error) = validate_ably_publish_message(&message, true) {
        return ably_app_error_response_format(error, response_format);
    }
    if let Err(error) = reconcile_mutation_message_serial(&mut message, &message_serial) {
        return ably_app_error_response_format(error, response_format);
    }
    let (connection_id, effective_client_id) = match rest_publish_identity(
        &runtime.hub,
        &resolved.app.id,
        resolved.client_id.as_deref(),
        &message,
    ) {
        Ok(identity) => identity,
        Err(error) => return ably_app_error_response_format(error, response_format),
    };
    match publish_ably_message(
        AblyMessagePublishContext {
            handler: &handler,
            hub: &runtime.hub,
            app: &resolved.app,
            channel: channel_name.base(),
            connection_id: connection_id.as_deref(),
            client_id: effective_client_id.as_deref(),
            capabilities: resolved.capabilities.as_ref(),
            privileged_server: resolved.capabilities.is_none(),
        },
        message,
    )
    .await
    {
        Ok(version_serial) => encode_ably_rest_response(
            StatusCode::OK,
            response_format,
            &json!({"versionSerial": version_serial}),
        )
        .unwrap_or_else(ably_app_error_response),
        Err(error) => ably_app_error_response_format(error, response_format),
    }
}

pub(super) fn reconcile_mutation_message_serial(
    message: &mut AblyMessage,
    path_serial: &str,
) -> Result<(), AppError> {
    let path_serial = parse_message_serial(path_serial)?;
    match message.serial.as_deref() {
        Some(body_serial) if body_serial != path_serial.as_str() => Err(AppError::InvalidInput(
            "mutation body serial must match the request path".to_string(),
        )),
        Some(_) => Ok(()),
        None => {
            message.serial = Some(path_serial.as_str().to_string());
            Ok(())
        }
    }
}

pub(super) async fn ably_channel_message_versions_inner(
    handler: Arc<ConnectionHandler>,
    app: App,
    channel_name: AblyChannelName,
    message_serial: String,
) -> Result<Vec<AblyMessage>, AppError> {
    let message_serial_value = parse_message_serial(&message_serial)?;
    let versions = MessageService::new(Arc::clone(&handler))
        .get_message_versions(sockudo_core::version_store::VersionStoreReadRequest {
            app_id: app.id.clone(),
            channel: channel_name.base().to_string(),
            message_serial: message_serial_value,
            direction: sockudo_core::version_store::VersionStoreDirection::NewestFirst,
            limit: handler
                .server_options()
                .versioned_messages
                .max_page_size
                .max(1),
            cursor: None,
        })
        .await?;
    versions
        .items
        .iter()
        .map(|record| {
            let runtime_message = handler.build_runtime_message_from_record(record, None);
            match record.envelope.as_ref() {
                Some(envelope) => envelope_to_ably_message(
                    envelope,
                    &runtime_message,
                    AblyMessageProjection::Mutation,
                )
                .map_err(AppError::InvalidInput),
                None => pusher_to_ably_message(&runtime_message, AblyMessageProjection::Mutation)
                    .map_err(AppError::InvalidInput),
            }
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyRevocationRequest {
    targets: Vec<String>,
    issued_before: Option<i64>,
    #[serde(default)]
    allow_reauth_margin: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyRevocationResult {
    success_count: usize,
    failure_count: usize,
    results: Vec<AblyRevocationTargetResult>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AblyRevocationTargetResult {
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    issued_before: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    applies_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<AblyErrorInfo>,
}

pub async fn ably_revoke_tokens(
    Path(key_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    if query.access_token.is_some() || bearer_token(&headers).is_some() {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40162,
            "Cannot revoke tokens when using token auth",
            response_format,
        );
    }
    let Some(credential) = query.key.clone().or_else(|| basic_credential(&headers)) else {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    };
    let (credential_key, credential_secret) = parse_ably_key(&credential);
    let key = match resolve_ably_key(&runtime.hub, &handler, credential_key).await {
        Ok(key)
            if credential_key == key_name
                && credential_secret.is_some_and(|secret| secure_compare(secret, &key.secret)) =>
        {
            key
        }
        _ => {
            return ably_error_response_format(
                StatusCode::UNAUTHORIZED,
                40101,
                "Invalid credentials",
                response_format,
            );
        }
    };
    if !key.revocable_tokens {
        return ably_error_response_format(
            StatusCode::FORBIDDEN,
            40160,
            "Key is not enabled for revocable tokens",
            response_format,
        );
    }
    if body.len() > ABLY_MAX_BATCH_BYTES {
        return ably_error_response_format(
            StatusCode::PAYLOAD_TOO_LARGE,
            40009,
            format!("Token revocation body exceeds the {ABLY_MAX_BATCH_BYTES}-byte limit"),
            response_format,
        );
    }
    let request: AblyRevocationRequest = match decode_value(body.as_ref(), request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!("Invalid token revocation request: {error}"),
                response_format,
            );
        }
    };
    if request.targets.is_empty() || request.targets.len() > 100 {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "Token revocation requires 1..=100 targets",
            response_format,
        );
    }
    let now = now_ms();
    let issued_before = request
        .issued_before
        .unwrap_or_else(|| now.saturating_add(1));
    if issued_before > now.saturating_add(1_000)
        || issued_before < now.saturating_sub(60 * 60 * 1000)
    {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "issuedBefore must be within the previous hour and not in the future",
            response_format,
        );
    }
    let applies_at = if request.allow_reauth_margin {
        now.saturating_add(30_001)
    } else {
        now
    };
    let app_id: Arc<str> = Arc::from(key.app.id.as_str());
    let results = match run_bounded_ordered(request.targets, ABLY_BATCH_CONCURRENCY, {
        let app_id = Arc::clone(&app_id);
        let hub = Arc::clone(&runtime.hub);
        move |target| {
            let app_id = Arc::clone(&app_id);
            let hub = Arc::clone(&hub);
            async move {
                let parsed = target.split_once(':').filter(|(target_type, value)| {
                    matches!(*target_type, "clientId" | "revocationKey" | "channel")
                        && !value.is_empty()
                        && value.len() <= 256
                });
                let Some((target_type, target_value)) = parsed else {
                    return AblyRevocationTargetResult {
                        target,
                        issued_before: None,
                        applies_at: None,
                        error: Some(error_info(
                            StatusCode::BAD_REQUEST,
                            40000,
                            "Invalid token revocation target",
                        )),
                    };
                };
                let record = AblyRevocationRecord {
                    target_type: target_type.to_string(),
                    target_value: target_value.to_string(),
                    issued_before,
                    applies_at,
                };
                let error = hub
                    .store_revocation(&app_id, target_type, target_value, record)
                    .await
                    .err()
                    .map(|error| error_info(error.status, error.code, error.message));
                AblyRevocationTargetResult {
                    target,
                    issued_before: error.is_none().then_some(issued_before),
                    applies_at: error.is_none().then_some(applies_at),
                    error,
                }
            }
        }
    })
    .await
    {
        Ok(results) => results,
        Err(error) => return ably_app_error_response_format(error, response_format),
    };
    let success_count = results
        .iter()
        .filter(|result| result.error.is_none())
        .count();
    if success_count > 0 {
        runtime
            .hub
            .notify_revocation_change(&key.app.id, request.allow_reauth_margin);
    }
    let result = AblyRevocationResult {
        success_count,
        failure_count: results.len().saturating_sub(success_count),
        results,
    };
    encode_ably_rest_response(StatusCode::OK, response_format, &result)
        .unwrap_or_else(ably_app_error_response)
}

pub async fn ably_request_token(
    Path(key_name): Path<String>,
    Query(query): Query<AblyRestQuery>,
    headers: HeaderMap,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
    body: Bytes,
) -> Response {
    let request_format = ably_rest_request_format(&headers);
    let response_format =
        ably_rest_response_format(&headers, query.format.as_deref(), request_format);
    let request: AblyTokenRequest = match decode_value(body.as_ref(), request_format) {
        Ok(request) => request,
        Err(error) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!("Invalid Ably token request: {error}"),
                response_format,
            );
        }
    };
    let body_key_name = request.key_name.as_deref().unwrap_or(&key_name);
    if body_key_name != key_name {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    }
    let resolved = match resolve_ably_key(&runtime.hub, &handler, body_key_name).await {
        Ok(resolved) => resolved,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    if request.client_id.as_deref() == Some("") {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40012,
            "clientId can’t be an empty string",
            response_format,
        );
    }

    let ttl_ms = match parse_token_request_integer(request.ttl.as_ref(), "ttl") {
        Ok(Some(ttl)) if ttl > 0 && ttl <= runtime.hub.config.max_token_ttl_ms => ttl,
        Ok(None) => DEFAULT_TOKEN_TTL_MS,
        Ok(Some(_)) | Err(_) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                format!(
                    "TokenRequest ttl must be a positive integer no greater than {}",
                    runtime.hub.config.max_token_ttl_ms
                ),
                response_format,
            );
        }
    };
    let timestamp = match parse_token_request_integer(request.timestamp.as_ref(), "timestamp") {
        Ok(Some(timestamp)) => timestamp,
        _ => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                "TokenRequest timestamp must be an integer",
                response_format,
            );
        }
    };
    if now_ms().abs_diff(timestamp)
        > u64::try_from(runtime.hub.config.token_request_timestamp_skew_ms).unwrap_or(0)
    {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40104,
            "Timestamp not current",
            response_format,
        );
    }
    let Some(nonce) = request
        .nonce
        .as_deref()
        .filter(|nonce| !nonce.is_empty() && nonce.len() <= 256)
    else {
        return ably_error_response_format(
            StatusCode::BAD_REQUEST,
            40000,
            "TokenRequest nonce is required and must not exceed 256 bytes",
            response_format,
        );
    };
    let Some(mac) = request
        .mac
        .as_deref()
        .filter(|mac| !mac.is_empty() && mac.len() <= 512)
    else {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    };
    let requested_capability = match request.capability.as_ref() {
        Some(serde_json::Value::String(capability)) => Some(capability.as_str()),
        Some(_) => {
            return ably_error_response_format(
                StatusCode::BAD_REQUEST,
                40000,
                "TokenRequest capability must be a JSON object string",
                response_format,
            );
        }
        None => None,
    };
    let signing_input = token_request_signing_input(
        body_key_name,
        request.ttl.as_ref().and_then(serde_json::Value::as_i64),
        requested_capability,
        request.client_id.as_deref(),
        timestamp,
        nonce,
    );
    if !verify_token_request_mac(&resolved.secret, &signing_input, mac) {
        return ably_error_response_format(
            StatusCode::UNAUTHORIZED,
            40101,
            "Invalid credentials",
            response_format,
        );
    }

    let (capability, capabilities) =
        match intersect_ably_capability(&resolved.capability, requested_capability) {
            Ok(parsed) => parsed,
            Err(CapabilityIntersectionError::Invalid(message)) => {
                return ably_error_response_format(
                    StatusCode::BAD_REQUEST,
                    40000,
                    message,
                    response_format,
                );
            }
            Err(CapabilityIntersectionError::Empty) => {
                return ably_error_response_format(
                    StatusCode::UNAUTHORIZED,
                    40160,
                    "Requested capability is not permitted by this key",
                    response_format,
                );
            }
        };
    match runtime.hub.claim_nonce(body_key_name, nonce).await {
        Ok(true) => {}
        Ok(false) => {
            return ably_error_response_format(
                StatusCode::UNAUTHORIZED,
                40105,
                "Nonce value replayed",
                response_format,
            );
        }
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    }
    let token = match runtime
        .hub
        .issue_token(
            &resolved,
            request.client_id,
            ttl_ms,
            capability,
            capabilities,
        )
        .await
    {
        Ok(token) => token,
        Err(error) => {
            return ably_error_response_format(
                error.status,
                error.code,
                error.message,
                response_format,
            );
        }
    };
    encode_ably_rest_response(StatusCode::OK, response_format, &token)
        .unwrap_or_else(ably_app_error_response)
}
