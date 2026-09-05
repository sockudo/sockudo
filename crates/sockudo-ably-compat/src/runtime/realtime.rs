//! Realtime connection lifecycle, protocol control, attachment, and recovery.

use super::*;

pub(super) fn should_deliver_to_subscriber(
    publisher_connection_id: Option<&str>,
    subscriber_connection_id: &str,
    subscriber_echo: bool,
    echo_override: Option<bool>,
) -> bool {
    publisher_connection_id != Some(subscriber_connection_id)
        || echo_override.unwrap_or(subscriber_echo)
}

#[cfg(feature = "delta")]
pub(super) fn project_ably_delta_message(
    mut protocol: AblyProtocolMessage,
    state: &AblyDeltaState,
) -> (AblyProtocolMessage, AblyDeltaState) {
    let Some(message) = protocol
        .messages
        .as_mut()
        .and_then(|messages| messages.first_mut())
    else {
        return (protocol, AblyDeltaState::default());
    };
    let Some(message_id) = message.id.as_deref().map(Arc::<str>::from) else {
        return (protocol, AblyDeltaState::default());
    };
    // Encoded JSON keeps its exact publish-time bytes. A message whose wire
    // encoding is still present (cipher, base64, or an unknown transform) is
    // not safe to reinterpret here, so it breaks the delta chain and remains
    // a canonical full delivery.
    let target = match (message.encoded_json.as_ref(), message.encoding.as_ref()) {
        (Some(encoded), _) => Arc::clone(encoded),
        (None, None) => match sonic_rs::to_vec(&message.data) {
            Ok(encoded) => Arc::<[u8]>::from(encoded),
            Err(_) => return (protocol, AblyDeltaState::default()),
        },
        (None, Some(_)) => return (protocol, AblyDeltaState::default()),
    };
    if target.len() > ABLY_DELTA_BASE_MAX_BYTES {
        return (protocol, AblyDeltaState::default());
    }

    let now = Instant::now();
    let next_state = AblyDeltaState::with_base(Arc::clone(&message_id), Arc::clone(&target), now);
    let full_payload = match std::str::from_utf8(&target) {
        Ok(payload) => payload,
        Err(_) => return (protocol, AblyDeltaState::default()),
    };

    if let Some((previous_id, previous_payload)) = state.fresh_base(now)
        && message.extras.as_ref().is_none_or(Value::is_object)
        && let Ok(delta) = sockudo_delta::compute_vcdiff(previous_payload, &target)
        && delta.len() < target.len()
    {
        let mut extras = message.extras.take().unwrap_or_else(|| json!({}));
        if let Some(object) = extras.as_object_mut() {
            object.insert(
                "delta",
                json!({ "from": previous_id.as_ref(), "format": "vcdiff" }),
            );
        }
        message.data = Some(json!(
            base64::engine::general_purpose::STANDARD.encode(delta)
        ));
        message.encoding = Some("json/utf-8/vcdiff/base64".to_string());
        message.extras = Some(extras);
    } else {
        message.data = Some(json!(full_payload));
        message.encoding = Some("json".to_string());
    }
    (protocol, next_state)
}

pub async fn handle_ably_realtime_upgrade(
    Query(params): Query<AblyConnectQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
    Extension(runtime): Extension<Arc<AblyCompatRuntime>>,
    State(handler): State<Arc<ConnectionHandler>>,
) -> Response {
    if !handler.is_accepting() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if handler.is_memory_pressure_shedding() {
        handler.mark_memory_pressure_rejection();
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, "1")],
            "MEMORY_PRESSURE",
        )
            .into_response();
    }
    let format = match parse_ably_format(params.format.as_deref()) {
        Ok(format) => format,
        Err(message) => return ably_error_response(StatusCode::BAD_REQUEST, 40000, message),
    };

    let (resolved, credential_error) = match resolve_ably_auth_with_expiry(
        &runtime.hub,
        &handler,
        &headers,
        params.key.as_deref(),
        params.access_token.as_deref(),
        params.client_id.as_deref(),
        true,
    )
    .await
    {
        Ok(resolved) => (resolved, None),
        Err(error) if error.code == 40102 && params.access_token.is_some() => {
            match resolve_ably_auth_with_expiry(
                &runtime.hub,
                &handler,
                &headers,
                params.key.as_deref(),
                params.access_token.as_deref(),
                None,
                true,
            )
            .await
            {
                Ok(resolved) => (resolved, Some(error)),
                Err(_) => {
                    return ably_error_response_format(
                        error.status,
                        error.code,
                        error.message,
                        format,
                    );
                }
            }
        }
        Err(error) => {
            if error.status != StatusCode::UNAUTHORIZED {
                return ably_error_response_format(error.status, error.code, error.message, format);
            }
            let ws_cfg = handler
                .server_options()
                .websocket
                .to_sockudo_ws_config_with_native_heartbeat(
                    handler.server_options().websocket_max_payload_kb,
                    handler.server_options().activity_timeout,
                    false,
                );
            return ws
                .config(ws_cfg)
                .on_upgrade(move |socket| send_fatal_ably_socket_error(socket, format, error))
                .into_response();
        }
    };

    if let Some(allowed_origins) = resolved.app.allowed_origins_ref()
        && !allowed_origins.is_empty()
    {
        let origin = headers
            .get(header::ORIGIN)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !OriginValidator::validate_origin(origin, allowed_origins) {
            return ably_error_response(
                StatusCode::FORBIDDEN,
                40300,
                "Origin is not allowed for this app",
            );
        }
    }

    let ws_cfg = handler
        .server_options()
        .websocket
        .to_sockudo_ws_config_with_native_heartbeat(
            handler.server_options().websocket_max_payload_kb,
            handler.server_options().activity_timeout,
            false,
        );
    if runtime.hub.config.realtime_admission == AblyRealtimeAdmission::PlacementConstraint {
        return ws
            .config(ws_cfg)
            .on_upgrade(move |socket| {
                send_ably_socket_failure(
                    socket,
                    format,
                    ACTION_DISCONNECTED,
                    error_info(
                        StatusCode::SERVICE_UNAVAILABLE,
                        50320,
                        "Active Traffic Management: traffic for this endpoint is temporarily redirected to a fallback host",
                    ),
                )
            })
            .into_response();
    }
    let hub = Arc::clone(&runtime.hub);
    let resume = params.resume.clone();
    let recover = params.recover.clone();
    let replace_presence_on_reenter = params.remain_present_for.is_some();
    let remain_present_for_ms = normalized_remain_present_for_ms(params.remain_present_for);
    let initial_error = credential_error.or_else(|| {
        resolved
            .expires_ms
            .filter(|expires_ms| *expires_ms <= now_ms())
            .map(|_| AblyAuthError::expired())
    });
    let stats_transport = if handler.server_options().ssl.enabled
        || headers
            .get("x-forwarded-proto")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("https"))
    {
        "tls"
    } else {
        "plain"
    };

    ws.config(ws_cfg)
        .on_upgrade(move |socket| async move {
            if let Err(error) = run_ably_realtime_socket(
                socket,
                AblyRealtimeSocketContext {
                    handler,
                    hub,
                    resolved,
                    initial_error,
                    resume,
                    recover,
                    format,
                    echo: params.echo,
                    replace_presence_on_reenter,
                    remain_present_for_ms,
                    stats_transport,
                },
            )
            .await
            {
                warn!(protocol = "ably", error = %error, "compatibility socket closed with error");
            }
        })
        .into_response()
}

pub(super) async fn send_fatal_ably_socket_error(
    socket: sockudo_ws::axum_integration::WebSocket,
    format: AblyFormat,
    error: AblyAuthError,
) {
    send_ably_socket_failure(
        socket,
        format,
        ACTION_ERROR,
        error_info(error.status, error.code, error.message),
    )
    .await;
}

pub(super) async fn send_ably_socket_failure(
    socket: sockudo_ws::axum_integration::WebSocket,
    format: AblyFormat,
    action: u8,
    error: AblyErrorInfo,
) {
    let (mut reader, mut writer) = socket.split();
    let message = AblyProtocolMessage {
        action,
        error: Some(error),
        ..empty_protocol_message(action)
    };
    let Ok(bytes) = encode_protocol_bytes(&message, format) else {
        return;
    };
    let frame = match format {
        AblyFormat::Json => Message::Text(bytes),
        AblyFormat::MsgPack => Message::Binary(bytes),
    };
    if writer.send(frame).await.is_err() {
        return;
    }
    // Give the peer a bounded opportunity to process the fatal protocol frame
    // and start its close handshake. The split reader schedules the mandatory
    // close response when it receives a peer close, so flush that response
    // instead of sending a second close frame. If the peer stays open, initiate
    // the close here so browsers do not observe a transport reset.
    let peer_started_close = matches!(
        tokio::time::timeout(Duration::from_millis(250), reader.next()).await,
        Ok(Some(Ok(Message::Close(_))))
    );
    if peer_started_close {
        // Keep the split halves alive until the connection-scoped writer
        // driver flushes the queued peer-Close response and reports terminal
        // state. Dropping either half earlier cancels the driver.
        let _ = tokio::time::timeout(Duration::from_millis(250), reader.next()).await;
    } else {
        let _ = writer.close(1000, "Ably protocol error").await;
    }
}

pub(super) struct AblyRealtimeSocketContext {
    handler: Arc<ConnectionHandler>,
    hub: Arc<AblyCompatHub>,
    resolved: ResolvedAblyAuth,
    initial_error: Option<AblyAuthError>,
    resume: Option<String>,
    recover: Option<String>,
    format: AblyFormat,
    echo: bool,
    replace_presence_on_reenter: bool,
    remain_present_for_ms: u64,
    stats_transport: &'static str,
}

pub(super) async fn run_ably_realtime_socket(
    socket: sockudo_ws::axum_integration::WebSocket,
    context: AblyRealtimeSocketContext,
) -> SockudoResult<()> {
    let AblyRealtimeSocketContext {
        handler,
        hub,
        resolved,
        initial_error,
        resume,
        recover,
        format,
        echo,
        replace_presence_on_reenter,
        remain_present_for_ms,
        stats_transport,
    } = context;
    let app = resolved.app.clone();
    let mut authorization = ConnectionAuthorization::from_resolved(&resolved);
    let requested_recovery_key = resume.as_deref().or(recover.as_deref()).map(str::to_owned);

    let connection_start = hub
        .begin_connection(
            &app.id,
            authorization.client_id.as_deref(),
            resume.as_deref(),
            recover.as_deref(),
        )
        .await;
    let connection_id = match &connection_start {
        AblyConnectionStart::Resumed { connection_id } => connection_id.clone(),
        AblyConnectionStart::Fresh | AblyConnectionStart::Failed { .. } => {
            format!("sockudo-ably-{}", Uuid::new_v4().simple())
        }
    };
    let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
    let connection_error = match &connection_start {
        AblyConnectionStart::Failed { error } => Some(error.clone()),
        AblyConnectionStart::Fresh | AblyConnectionStart::Resumed { .. } => None,
    };
    hub.remember_connection(
        connection_key.clone(),
        &app.id,
        &connection_id,
        authorization.client_id.clone(),
    )
    .await;
    let active_connection_key = Arc::new(RwLock::new(connection_key.clone()));
    // A recovered transport keeps the stable Ably connection ID, but it must
    // not share subscriber ownership with the socket it supersedes. Otherwise
    // cleanup from the old socket can remove the recovered socket's channels.
    let session_id = format!("{}:{}", connection_id, Uuid::new_v4().simple());
    if let Err(error) = hub
        .claim_session_owner(&app.id, &connection_id, &session_id)
        .await
    {
        hub.forget_connection(&connection_key).await;
        return Err(sockudo_core::error::Error::Cache(error.message));
    }
    if matches!(connection_start, AblyConnectionStart::Resumed { .. })
        && let Some(requested_recovery_key) = requested_recovery_key.as_deref()
    {
        // Recovery keys are single-use leases. The stable connection ID is
        // retained, but a second transport cannot recover the same snapshot.
        hub.forget_connection(requested_recovery_key).await;
    }
    let (mut reader, mut writer) = socket.split();
    let outbound_limits = OutboundLimits::from_websocket(&handler.server_options().websocket);
    let (sender, mut outbound) =
        AblyOutbound::channel(format, outbound_limits, Arc::clone(&hub.metrics));
    let (peer_close_tx, mut peer_close_rx) = crossfire::oneshot::oneshot();
    let mut peer_close_tx = Some(peer_close_tx);
    let (writer_shutdown_tx, mut writer_shutdown_rx) = crossfire::oneshot::oneshot();
    let writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                peer_close = &mut peer_close_rx => {
                    if peer_close.is_ok() {
                        // Reading the peer close schedules the required close
                        // response in sockudo-ws. Flush it immediately so the
                        // browser does not wait for session cleanup.
                        let _ = writer.flush().await;
                        return;
                    }
                    break;
                }
                shutdown = &mut writer_shutdown_rx => {
                    if shutdown.is_ok() {
                        // A protocol CLOSE may leave sender clones in recovery
                        // state. Drain everything already accepted before
                        // closing the WebSocket instead of waiting for every
                        // clone to be dropped.
                        while let Some(frame) = outbound.try_recv() {
                            let frame = match format {
                                AblyFormat::Json => Message::Text((*frame.bytes).clone()),
                                AblyFormat::MsgPack => Message::Binary((*frame.bytes).clone()),
                            };
                            if writer.send(frame).await.is_err() {
                                return;
                            }
                        }
                        let _ = writer.close(1000, "Ably session closed").await;
                        let _ = writer.flush().await;
                        return;
                    }
                    break;
                }
                frame = outbound.recv() => {
                    let Some(frame) = frame else { break };
                    let frame = match format {
                        AblyFormat::Json => Message::Text((*frame.bytes).clone()),
                        AblyFormat::MsgPack => Message::Binary((*frame.bytes).clone()),
                    };
                    if let Err(error) = writer.send(frame).await {
                        debug!(protocol = "ably", error = %error, "compatibility socket writer closed");
                        return;
                    }
                }
            }
        }
        let _ = writer.close(1000, "Ably session closed").await;
    });
    let heartbeat_sender = sender.clone();
    let heartbeat_task = tokio::spawn(async move {
        let period = Duration::from_millis(DEFAULT_MAX_IDLE_INTERVAL_MS / 2);
        let mut interval = tokio::time::interval_at(TokioInstant::now() + period, period);
        loop {
            interval.tick().await;
            if heartbeat_sender
                .send_protocol(
                    &AblyProtocolMessage {
                        action: ACTION_HEARTBEAT,
                        ..empty_protocol_message(ACTION_HEARTBEAT)
                    },
                    OutboundPriority::Control,
                )
                .is_err()
            {
                break;
            }
        }
    });

    if let Some(error) = initial_error {
        send_protocol_disconnected(&sender, error.code, error.message);
        hub.forget_connection(&connection_key).await;
        hub.release_session_owner(&app.id, &connection_id, &session_id)
            .await;
        writer_shutdown_tx.send(());
        drop(sender);
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        let _ = writer_task.await;
        return Ok(());
    }

    let current_connections = hub.stats.connection_opened(&app.id);
    let opened = StatsObservation::connection_opened(
        &app.id,
        now_ms(),
        stats_transport,
        current_connections,
    )
    .map_err(stats_sockudo_error)?;
    if let Err(error) = hub.stats.record(opened).await {
        hub.stats.connection_closed(&app.id);
        hub.forget_connection(&connection_key).await;
        hub.release_session_owner(&app.id, &connection_id, &session_id)
            .await;
        writer_shutdown_tx.send(());
        drop(sender);
        heartbeat_task.abort();
        let _ = heartbeat_task.await;
        let _ = writer_task.await;
        return Err(stats_sockudo_error(error));
    }

    let presence_service = PresenceService::new(Arc::clone(&handler));
    presence_service.register_connection(&app.id, &connection_id);

    let shared_authorization = Arc::new(RwLock::new(authorization.clone()));
    let lease_hub = Arc::clone(&hub);
    let lease_key = Arc::clone(&active_connection_key);
    let lease_app_id = app.id.clone();
    let lease_connection_id = connection_id.clone();
    let lease_session_id = session_id.clone();
    let lease_authorization = Arc::clone(&shared_authorization);
    let connection_lease_task = tokio::spawn(async move {
        let refresh_interval = Duration::from_millis(DEFAULT_CONNECTION_STATE_TTL_MS / 2);
        loop {
            tokio::time::sleep(refresh_interval).await;
            if !lease_hub
                .refresh_session_owner(&lease_app_id, &lease_connection_id, &lease_session_id)
                .await
            {
                break;
            }
            let connection_key = lease_key
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            let client_id = lease_authorization
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .client_id
                .clone();
            lease_hub
                .remember_connection(
                    connection_key,
                    &lease_app_id,
                    &lease_connection_id,
                    client_id,
                )
                .await;
        }
    });

    let mut attached_channels = if matches!(connection_start, AblyConnectionStart::Resumed { .. }) {
        hub.resume_live_subscribers(&app.id, &connection_id, &session_id, &sender)
    } else {
        HashMap::new()
    };
    send_protocol(
        &sender,
        connected_message(
            &connection_id,
            &connection_key,
            authorization.connection_client_id.clone(),
            connection_error,
        ),
    );
    info!(
        protocol = "ably",
        app_id = %app.id,
        connection_id = %connection_id,
        resumed = matches!(connection_start, AblyConnectionStart::Resumed { .. }),
        wire_format = ?format,
        "socket connected"
    );

    let (command_tx, command_rx) = crossfire::mpsc::bounded_async(8);
    let previous_session = hub.register_live_session(
        connection_id.clone(),
        AblyLiveSession {
            session_id: session_id.clone(),
            app_id: app.id.clone(),
            authorization: Arc::clone(&shared_authorization),
            command_tx,
        },
    );
    if let Some(previous_session) = previous_session {
        let _ = previous_session.send(AblySessionCommand::Superseded).await;
    }
    hub.session_echo.insert(session_id.clone(), echo);
    let mut ownership_poll = tokio::time::interval(Duration::from_millis(500));
    ownership_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Snapshot age plus polling delay remains at most the original 500ms budget.
    let mut revocation_poll = tokio::time::interval(REVOCATION_SNAPSHOT_FRESHNESS);
    revocation_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renewal_hint_sent = false;
    let mut graceful_close = false;
    // Ably protocol serials identify logical publishes, not transport writes.
    // A reconnecting SDK can write the same pending ProtocolMessage more than
    // once on the replacement transport. Only the first copy on this transport
    // may be processed and answered; a fresh transport gets a fresh tracker so
    // a retry still receives the ACK/NACK that could not arrive on the old one.
    let mut inbound_serials = AblyInboundSerialTracker::default();
    loop {
        let next_auth_deadline = authorization_deadline(&authorization, renewal_hint_sent);
        let auth_sleep = tokio::time::sleep(next_auth_deadline);
        tokio::pin!(auth_sleep);
        let frame = tokio::select! {
            frame = reader.next() => frame,
            command = command_rx.recv() => {
                match command {
                    Ok(AblySessionCommand::ReauthHint { generation })
                        if generation == authorization.generation => {
                            send_protocol(&sender, empty_protocol_message(ACTION_AUTH));
                            renewal_hint_sent = true;
                            continue;
                        }
                    Ok(AblySessionCommand::RevocationChanged { generation })
                        if generation == authorization.generation => {
                            if hub.authorization_is_revoked(&app.id, &authorization, &attached_channels).await {
                                send_protocol_disconnected(&sender, 40141, "Token revoked");
                                break;
                            }
                            continue;
                        }
                    Ok(AblySessionCommand::Superseded) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
            _ = ownership_poll.tick() => {
                if !hub.session_is_current(&app.id, &connection_id, &session_id).await { break; }
                continue;
            }
            _ = revocation_poll.tick(), if authorization.revocable => {
                if hub.authorization_is_revoked_from_snapshot(&app.id, &authorization, &attached_channels).await
                {
                    send_protocol_disconnected(&sender, 40141, "Token revoked");
                    break;
                }
                continue;
            }
            _ = &mut auth_sleep, if authorization.expires_ms.is_some() => {
                if should_send_renewal_hint(&authorization, renewal_hint_sent) {
                    send_protocol(&sender, empty_protocol_message(ACTION_AUTH));
                    renewal_hint_sent = true;
                    continue;
                }
                send_protocol_disconnected(&sender, 40142, "Token expired");
                break;
            }
        };
        let Some(frame) = frame else { break };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                debug!(
                    protocol = "ably",
                    app_id = %app.id,
                    connection_id = %connection_id,
                    error = %error,
                    "socket reader closed"
                );
                break;
            }
        };
        let bytes = match frame {
            Message::Text(bytes) | Message::Binary(bytes) => bytes,
            Message::Ping(payload) => {
                send_protocol(
                    &sender,
                    AblyProtocolMessage {
                        action: ACTION_HEARTBEAT,
                        ..empty_protocol_message(ACTION_HEARTBEAT)
                    },
                );
                let _ = payload;
                continue;
            }
            Message::Pong(_) => continue,
            Message::Close(_) => {
                if let Some(peer_close_tx) = peer_close_tx.take() {
                    peer_close_tx.send(());
                }
                break;
            }
        };
        let inbound = match decode_ably_protocol_message(bytes.as_ref(), format) {
            Ok(inbound) => inbound,
            Err(error) => {
                // A malformed frame is scoped to this connection. Reporting it
                // as a protocol ERROR keeps existing channel subscriptions
                // intact and lets the client decide whether to reconnect.
                send_protocol_error(
                    &sender,
                    40000,
                    format!("Malformed Ably ProtocolMessage: {error}"),
                );
                continue;
            }
        };
        if inbound.action == ACTION_AUTH {
            let previous_connection_key = active_connection_key
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            match handle_ably_auth_update(
                &hub,
                &handler,
                &app,
                &connection_id,
                &session_id,
                &previous_connection_key,
                &sender,
                &mut attached_channels,
                &mut authorization,
                inbound,
            )
            .await
            {
                Ok(connection_key) => {
                    renewal_hint_sent = false;
                    *shared_authorization
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = authorization.clone();
                    *active_connection_key
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = connection_key;
                }
                Err(error) => {
                    send_protocol(
                        &sender,
                        AblyProtocolMessage {
                            action: ACTION_ERROR,
                            error: Some(error_info(error.status, error.code, error.message)),
                            ..empty_protocol_message(ACTION_ERROR)
                        },
                    );
                    break;
                }
            }
            continue;
        }
        match handle_ably_protocol_message(
            &handler,
            &hub,
            &app,
            &connection_id,
            &authorization,
            &session_id,
            &sender,
            &active_connection_key,
            &mut attached_channels,
            &mut inbound_serials,
            replace_presence_on_reenter,
            inbound,
        )
        .await
        {
            Ok(AblyProtocolControl::Continue) => {}
            Ok(AblyProtocolControl::Disconnect) => break,
            Ok(AblyProtocolControl::Close) => {
                graceful_close = true;
                break;
            }
            Err(error) => {
                debug!(
                    protocol = "ably",
                    app_id = %app.id,
                    connection_id = %connection_id,
                    error = %error,
                    "protocol handler stopped"
                );
                break;
            }
        }
    }

    if graceful_close {
        let removals = presence_service
            .unregister_connection(&app, &connection_id, PresenceHistoryEventCause::Disconnect)
            .await?;
        let mut leaves = BTreeMap::<String, Vec<PresenceChange>>::new();
        for removal in removals {
            leaves
                .entry(removal.channel)
                .or_default()
                .push(PresenceChange {
                    action: PresenceChangeAction::Leave,
                    member: removal.member,
                    wire_id: None,
                });
        }
        for (channel, changes) in leaves {
            if let Err(error) = handler
                .fanout_presence(
                    &app.id,
                    &channel,
                    PresenceReplication {
                        changes,
                        unregister_connection: None,
                    },
                )
                .await
            {
                warn!(
                    protocol = "ably",
                    app_id = %app.id,
                    channel = %channel,
                    error = %error,
                    "failed to replicate graceful presence leaves"
                );
            }
        }
        if let Err(error) = handler
            .fanout_presence(
                &app.id,
                "",
                PresenceReplication {
                    changes: Vec::new(),
                    unregister_connection: Some(connection_id.clone()),
                },
            )
            .await
        {
            warn!(
                protocol = "ably",
                app_id = %app.id,
                connection_id = %connection_id,
                error = %error,
                "failed to replicate graceful presence connection removal"
            );
        }
    }
    let owns_session = hub
        .session_is_current(&app.id, &connection_id, &session_id)
        .await;
    let attached_channel_count = attached_channels.len();
    if !graceful_close {
        hub.mark_session_subscribers_recoverable(
            &app.id,
            &session_id,
            attached_channels
                .values()
                .map(|attachment| &attachment.channel),
        );
    } else {
        for (requested, _) in attached_channels {
            if let Ok(channel) = AblyChannelName::parse(requested)
                && let Err(error) = hub.unsubscribe(&app.id, &channel, &session_id).await
            {
                warn!(
                    protocol = "ably",
                    app_id = %app.id,
                    channel = %channel.requested(),
                    error = %error,
                    "channel close stats persistence failed"
                );
            }
        }
    }
    connection_lease_task.abort();
    let _ = connection_lease_task.await;
    hub.unregister_live_session(&connection_id, &session_id);
    hub.session_echo.remove(&session_id);
    let final_connection_key = active_connection_key
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if owns_session && graceful_close {
        hub.forget_connection(&final_connection_key).await;
    } else if owns_session {
        hub.remember_connection(
            final_connection_key.clone(),
            &app.id,
            &connection_id,
            authorization.client_id.clone(),
        )
        .await;
        if !hub
            .session_is_current(&app.id, &connection_id, &session_id)
            .await
        {
            hub.forget_connection(&final_connection_key).await;
        }
    } else {
        // A recovered transport on another node owns the stable connection.
        // Cleanup from this stale socket must not recreate its recovery key.
        hub.forget_connection(&final_connection_key).await;
    }
    let defer_owner_release = if owns_session
        && !graceful_close
        && hub
            .presence_registry
            .connection_has_members(&app.id, &connection_id)
    {
        match hub
            .schedule_pending_presence_removal(
                &app.id,
                &connection_id,
                &session_id,
                remain_present_for_ms,
            )
            .await
        {
            Ok(()) if hub.live_sessions.contains_key(&connection_id) => {
                // Recovery may win between the original ownership check and
                // scheduling. Cancel here; a later local recovery cancels from
                // register_live_session instead.
                hub.cancel_pending_presence_removal(&app.id, &connection_id);
                false
            }
            Ok(()) => true,
            Err(error) => {
                warn!(
                    protocol = "ably",
                    app_id = %app.id,
                    connection_id = %connection_id,
                    error = %error,
                    "failed to schedule presence removal"
                );
                false
            }
        }
    } else {
        false
    };
    if !defer_owner_release {
        hub.release_session_owner(&app.id, &connection_id, &session_id)
            .await;
    }
    heartbeat_task.abort();
    let _ = heartbeat_task.await;
    writer_shutdown_tx.send(());
    drop(sender);
    let _ = writer_task.await;
    hub.stats.connection_closed(&app.id);
    if let Ok(closed) = StatsObservation::connection_closed(&app.id, now_ms(), stats_transport)
        && let Err(error) = hub.stats.record(closed).await
    {
        warn!(
            protocol = "ably",
            app_id = %app.id,
            connection_id = %connection_id,
            error = %error,
            "connection close stats persistence failed"
        );
    }
    info!(
        protocol = "ably",
        app_id = %app.id,
        connection_id = %connection_id,
        channel_count = attached_channel_count,
        graceful = graceful_close,
        "socket disconnected"
    );
    Ok(())
}

pub(super) fn normalized_remain_present_for_ms(requested_ms: Option<u64>) -> u64 {
    requested_ms
        .unwrap_or(DEFAULT_REMAIN_PRESENT_FOR_MS)
        .clamp(MIN_REMAIN_PRESENT_FOR_MS, DEFAULT_REMAIN_PRESENT_FOR_MS)
}

pub(super) fn authorization_deadline(
    authorization: &ConnectionAuthorization,
    renewal_hint_sent: bool,
) -> Duration {
    let now = now_ms();
    let Some(expires_ms) = authorization.expires_ms else {
        return Duration::from_secs(24 * 60 * 60);
    };
    let deadline =
        if !renewal_hint_sent && expires_ms.saturating_sub(authorization.issued_ms) > 30_000 {
            expires_ms.saturating_sub(30_000)
        } else {
            expires_ms
        };
    Duration::from_millis(u64::try_from(deadline.saturating_sub(now)).unwrap_or(0))
}

pub(super) fn should_send_renewal_hint(
    authorization: &ConnectionAuthorization,
    renewal_hint_sent: bool,
) -> bool {
    if renewal_hint_sent {
        return false;
    }
    let now = now_ms();
    authorization.expires_ms.is_some_and(|expires_ms| {
        expires_ms > now
            && expires_ms.saturating_sub(authorization.issued_ms) > 30_000
            && now >= expires_ms.saturating_sub(30_000)
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ably_auth_update(
    hub: &Arc<AblyCompatHub>,
    handler: &Arc<ConnectionHandler>,
    app: &App,
    connection_id: &str,
    session_id: &str,
    previous_connection_key: &str,
    sender: &AblySender,
    attached_channels: &mut HashMap<String, AblyConnectionAttachment>,
    authorization: &mut ConnectionAuthorization,
    inbound: AblyProtocolMessage,
) -> Result<String, AblyAuthError> {
    let access_token = inbound
        .auth
        .as_ref()
        .and_then(|auth| auth.get("accessToken"))
        .and_then(Value::as_str)
        .ok_or_else(|| AblyAuthError::unauthorized("AUTH requires auth.accessToken"))?;
    let resolved = resolve_ably_auth(
        hub,
        handler,
        &HeaderMap::new(),
        None,
        Some(access_token),
        authorization.client_id.as_deref(),
    )
    .await?;
    if resolved.app.id != app.id {
        return Err(AblyAuthError::unauthorized(
            "AUTH token belongs to a different app",
        ));
    }
    let mut next = authorization.clone();
    next.replace_from(&resolved);
    if next.revocable
        && hub
            .authorization_is_revoked(&app.id, &next, attached_channels)
            .await
    {
        return Err(AblyAuthError {
            status: StatusCode::UNAUTHORIZED,
            code: 40141,
            message: "Token revoked".to_string(),
        });
    }

    let mode_updates = attached_channels
        .iter()
        .map(|(requested, attachment)| {
            let granted = intersect_ably_channel_modes(
                next.capabilities.as_ref(),
                &attachment.channel,
                attachment.requested_mode_flags,
            );
            (requested.clone(), granted)
        })
        .collect::<Vec<_>>();
    for (channel, granted_mode_flags) in mode_updates {
        if granted_mode_flags != 0 {
            let Some(attachment) = attached_channels.get_mut(&channel) else {
                continue;
            };
            if attachment.mode_flags != granted_mode_flags {
                attachment.mode_flags = granted_mode_flags;
                if attachment.params.contains_key("modes") {
                    attachment.params.insert(
                        "modes".to_string(),
                        ably_mode_names(granted_mode_flags).join(","),
                    );
                }
                hub.update_subscriber_mode_flags(
                    &app.id,
                    &attachment.channel,
                    session_id,
                    granted_mode_flags,
                );
            }
            continue;
        }
        if let Ok(parsed) = AblyChannelName::parse(channel.clone())
            && let Err(error) = hub.unsubscribe(&app.id, &parsed, session_id).await
        {
            warn!(
                protocol = "ably",
                app_id = %app.id,
                channel = %parsed.requested(),
                error = %error,
                "channel close stats persistence failed during auth update"
            );
        }
        attached_channels.remove(&channel);
        send_channel_error(
            sender,
            &channel,
            StatusCode::UNAUTHORIZED,
            40160,
            "Channel capability revoked by AUTH",
        );
    }

    *authorization = next;
    let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
    hub.replace_connection_key(
        previous_connection_key,
        connection_key.clone(),
        &app.id,
        connection_id,
        authorization.client_id.clone(),
    )
    .await;
    send_protocol(
        sender,
        connected_message(
            connection_id,
            &connection_key,
            authorization.connection_client_id.clone(),
            None,
        ),
    );
    Ok(connection_key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AblyProtocolControl {
    Continue,
    Disconnect,
    Close,
}

#[derive(Debug, Default)]
pub(super) struct AblyInboundSerialTracker {
    highest_serial: Option<u64>,
}

impl AblyInboundSerialTracker {
    pub(super) fn accepts(&mut self, inbound: &AblyProtocolMessage) -> bool {
        if !matches!(
            inbound.action,
            ACTION_MESSAGE | ACTION_PRESENCE | ACTION_ANNOTATION
        ) {
            return true;
        }
        let Some(start) = inbound.msg_serial else {
            return true;
        };
        if self.highest_serial.is_some_and(|highest| start <= highest) {
            return false;
        }

        let count = inbound.count.unwrap_or(1).max(1);
        self.highest_serial = Some(start.saturating_add(count - 1));
        true
    }
}

pub(super) fn ably_protocol_control(action: u8) -> AblyProtocolControl {
    match action {
        ACTION_DISCONNECT => AblyProtocolControl::Disconnect,
        ACTION_CLOSE => AblyProtocolControl::Close,
        _ => AblyProtocolControl::Continue,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ably_protocol_message(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    authorization: &ConnectionAuthorization,
    session_id: &str,
    sender: &AblySender,
    active_connection_key: &RwLock<String>,
    attached_channels: &mut HashMap<String, AblyConnectionAttachment>,
    inbound_serials: &mut AblyInboundSerialTracker,
    replace_presence_on_reenter: bool,
    mut inbound: AblyProtocolMessage,
) -> SockudoResult<AblyProtocolControl> {
    let control = ably_protocol_control(inbound.action);
    if !inbound_serials.accepts(&inbound) {
        hub.metrics
            .duplicate_suppression
            .fetch_add(1, Ordering::Relaxed);
        debug!(
            protocol = "ably",
            app_id = %app.id,
            connection_id,
            msg_serial = inbound.msg_serial.unwrap_or_default(),
            "duplicate inbound protocol serial suppressed"
        );
        return Ok(AblyProtocolControl::Continue);
    }
    let client_id = authorization.client_id.as_deref();
    let connection_client_id = authorization.connection_client_id.as_deref();
    let capabilities = authorization.capabilities.as_ref();
    match inbound.action {
        ACTION_HEARTBEAT => {
            tokio::time::sleep(Duration::from_millis(1)).await;
            send_protocol(sender, heartbeat_response(inbound));
        }
        ACTION_CONNECT => {
            let connection_key = format!("{}:{}", app.id, Uuid::new_v4().simple());
            hub.remember_connection(
                connection_key.clone(),
                &app.id,
                connection_id,
                client_id.map(str::to_string),
            )
            .await;
            active_connection_key
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone_from(&connection_key);
            send_protocol(
                sender,
                connected_message(
                    connection_id,
                    &connection_key,
                    connection_client_id.map(str::to_string),
                    None,
                ),
            );
        }
        ACTION_ATTACH => {
            let Some(raw_channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "ATTACH requires channel");
                return Ok(AblyProtocolControl::Continue);
            };
            let channel = match AblyChannelName::parse(raw_channel) {
                Ok(channel) => channel,
                Err(error) => {
                    send_channel_error(
                        sender,
                        error.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        error.to_string(),
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            let previous = attached_channels.get(channel.requested());
            if inbound.params.is_none() {
                inbound.params = previous
                    .filter(|attachment| !attachment.params.is_empty())
                    .map(|attachment| attachment.params.clone());
            }
            if inbound.flags.is_none() {
                inbound.flags = previous
                    .filter(|attachment| attachment.explicit_modes)
                    .map(|attachment| attachment.mode_flags);
            }
            if inbound.channel_serial.is_none() {
                inbound.channel_serial =
                    previous.and_then(|attachment| attachment.attach_position.clone());
            }
            let filter = match previous
                .filter(|attachment| attachment.channel == channel)
                .and_then(|attachment| attachment.filter.clone())
                .map_or_else(|| hub.message_filter(&channel), |filter| Ok(Some(filter)))
            {
                Ok(filter) => filter,
                Err(message) => {
                    send_channel_error(
                        sender,
                        channel.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        message,
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            let mut attach_options =
                AblyAttachOptions::from_wire(inbound.flags, inbound.params.clone());
            let requested_mode_flags = attach_options.mode_flags;
            let granted_mode_flags =
                intersect_ably_channel_modes(capabilities, &channel, requested_mode_flags);
            if granted_mode_flags == 0 {
                send_channel_error(
                    sender,
                    channel.requested(),
                    StatusCode::UNAUTHORIZED,
                    40160,
                    "Ably token has no capability matching the requested channel modes",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            attach_options.retain_mode_flags(granted_mode_flags);
            let has_presence = previous.is_some_and(|attachment| attachment.has_presence)
                || !PresenceService::new(Arc::clone(handler))
                    .snapshot(&app.id, channel.base())
                    .await?
                    .is_empty();
            let (resumed_attach, recovery_gate) =
                hub.take_resumed_subscriber_message(&app.id, &channel, session_id);
            let channel_serial_source = ably_channel_serial_source(
                handler.server_options().versioned_messages.enabled,
                app.resolved_history(channel.base(), &handler.server_options().history)
                    .enabled,
            );
            let recovery_gate = match apply_resumed_attach_recovery(
                resumed_attach,
                &mut inbound.channel_serial,
                channel_serial_source,
                &mut attach_options,
                recovery_gate,
            ) {
                Ok(recovery_gate) => recovery_gate,
                Err(failure) => {
                    attached_channels.remove(channel.requested());
                    if let Err(error) = hub.unsubscribe(&app.id, &channel, session_id).await {
                        tracing::warn!(
                            error = %error,
                            app_id = %app.id,
                            channel = channel.base(),
                            "recovered subscriber cleanup after attach failure failed"
                        );
                    }
                    send_channel_error(
                        sender,
                        channel.requested(),
                        failure.status,
                        failure.code,
                        failure.message,
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            if recovery_gate.overflowed {
                attached_channels.remove(channel.requested());
                if let Err(error) = hub.unsubscribe(&app.id, &channel, session_id).await {
                    tracing::warn!(
                        error = %error,
                        app_id = %app.id,
                        channel = channel.base(),
                        "overflowed recovered subscriber cleanup failed"
                    );
                }
                send_channel_error(
                    sender,
                    channel.requested(),
                    StatusCode::BAD_REQUEST,
                    90003,
                    "unable to recover channel because continuity buffering overflowed",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            let attachment_state = AblyConnectionAttachment {
                channel: channel.clone(),
                params: attach_options.params.clone(),
                requested_mode_flags,
                mode_flags: attach_options.mode_flags,
                explicit_modes: attach_options.explicit_modes,
                filter: filter.clone(),
                attach_position: inbound.channel_serial.clone(),
                has_presence,
            };
            if authorization.revocable {
                let target_channel =
                    HashMap::from([(channel.requested().to_string(), attachment_state.clone())]);
                if hub
                    .authorization_is_revoked(&app.id, authorization, &target_channel)
                    .await
                {
                    send_protocol_disconnected(sender, 40141, "Token revoked");
                    return Err(sockudo_core::error::Error::Auth(
                        "Ably token revoked".to_string(),
                    ));
                }
            }
            attached_channels.insert(channel.requested().to_string(), attachment_state);
            let attach = handle_ably_attach(
                handler,
                hub,
                app,
                connection_id,
                session_id,
                sender,
                &channel,
                filter,
                inbound.channel_serial,
                attach_options,
            );
            let attach_result =
                tokio::time::timeout(Duration::from_millis(hub.config.attach_timeout_ms), attach)
                    .await;
            if attach_result.is_err() {
                if let Err(error) = hub.unsubscribe(&app.id, &channel, session_id).await {
                    warn!(
                        protocol = "ably",
                        app_id = %app.id,
                        channel = %channel.requested(),
                        error = %error,
                        "timed-out attach cleanup failed"
                    );
                }
                attached_channels.remove(channel.requested());
                send_protocol(
                    sender,
                    AblyProtocolMessage {
                        action: ACTION_DETACHED,
                        channel: Some(channel.requested().to_string()),
                        error: Some(error_info(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            50003,
                            "channel attach timed out",
                        )),
                        ..empty_protocol_message(ACTION_DETACHED)
                    },
                );
            } else {
                for message in recovery_gate.messages {
                    if let Err(error) = sender.send_protocol(&message, OutboundPriority::Data) {
                        debug!(protocol = "ably", error = %error, "connection recovery replay unavailable");
                        break;
                    }
                }
            }
        }
        ACTION_DETACH => {
            let Some(raw_channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "DETACH requires channel");
                return Ok(AblyProtocolControl::Continue);
            };
            let channel = match AblyChannelName::parse(raw_channel) {
                Ok(channel) => channel,
                Err(error) => {
                    send_channel_error(
                        sender,
                        error.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        error.to_string(),
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            let removals = PresenceService::new(Arc::clone(handler))
                .detach_connection(app, channel.base(), connection_id)
                .await?;
            if !removals.is_empty() {
                handler
                    .fanout_presence(
                        &app.id,
                        channel.base(),
                        PresenceReplication {
                            changes: removals
                                .into_iter()
                                .map(|removal| PresenceChange {
                                    action: PresenceChangeAction::Leave,
                                    wire_id: Some(removal.member.id.clone()),
                                    member: removal.member,
                                })
                                .collect(),
                            unregister_connection: None,
                        },
                    )
                    .await?;
            }
            hub.unsubscribe(&app.id, &channel, session_id)
                .await
                .map_err(stats_sockudo_error)?;
            attached_channels.remove(channel.requested());
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_DETACHED,
                    channel: Some(channel.requested().to_string()),
                    ..empty_protocol_message(ACTION_DETACHED)
                },
            );
            info!(
                protocol = "ably",
                app_id = %app.id,
                connection_id = %connection_id,
                channel = %channel.requested(),
                "channel detached"
            );
        }
        ACTION_MESSAGE => {
            if attached_channel_mode_denies(
                attached_channels,
                inbound.channel.as_deref(),
                ABLY_MODE_PUBLISH,
            ) {
                send_publish_nack(
                    sender,
                    &inbound,
                    40160,
                    "Channel mode does not permit publish",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            handle_ably_publish(
                AblyPublishContext {
                    handler,
                    hub,
                    app,
                    connection_id,
                    client_id,
                    capabilities,
                    privileged_server: authorization.credential_id.starts_with("key:"),
                    sender,
                },
                inbound,
            )
            .await?;
        }
        ACTION_PRESENCE => {
            if attached_channel_mode_denies(
                attached_channels,
                inbound.channel.as_deref(),
                ABLY_MODE_PRESENCE,
            ) {
                send_publish_nack(
                    sender,
                    &inbound,
                    40160,
                    "Channel mode does not permit presence",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            handle_ably_presence(
                handler,
                hub,
                app,
                connection_id,
                client_id,
                capabilities,
                sender,
                replace_presence_on_reenter,
                inbound,
            )
            .await;
        }
        ACTION_ANNOTATION => {
            if attached_channel_mode_denies(
                attached_channels,
                inbound.channel.as_deref(),
                ABLY_MODE_ANNOTATION_PUBLISH,
            ) {
                send_publish_nack(
                    sender,
                    &inbound,
                    40160,
                    "Channel mode does not permit annotation publish",
                );
                return Ok(AblyProtocolControl::Continue);
            }
            handle_ably_annotation(handler, app, client_id, capabilities, sender, inbound).await;
        }
        ACTION_SYNC => {
            let Some(raw_channel) = inbound.channel else {
                send_protocol_error(sender, 40000, "SYNC requires channel");
                return Ok(AblyProtocolControl::Continue);
            };
            let channel = match AblyChannelName::parse(raw_channel) {
                Ok(channel) => channel,
                Err(error) => {
                    send_channel_error(
                        sender,
                        error.requested(),
                        StatusCode::BAD_REQUEST,
                        40010,
                        error.to_string(),
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            };
            send_presence_sync(
                sender,
                channel.requested(),
                PresenceService::new(Arc::clone(handler))
                    .snapshot(&app.id, channel.base())
                    .await?
                    .into_iter()
                    .map(|record| ably_presence_from_record(record, 1))
                    .collect(),
            );
        }
        ACTION_DISCONNECT => {
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_DISCONNECTED,
                    ..empty_protocol_message(ACTION_DISCONNECTED)
                },
            );
        }
        ACTION_CLOSE => {
            send_protocol(
                sender,
                AblyProtocolMessage {
                    action: ACTION_CLOSED,
                    ..empty_protocol_message(ACTION_CLOSED)
                },
            );
        }
        _ => {
            send_protocol_error(
                sender,
                40000,
                format!("Unsupported Ably ProtocolMessage action {}", inbound.action),
            );
        }
    }
    Ok(control)
}

pub(super) fn ably_annotation_from_native_event(event: AnnotationEventData) -> AblyAnnotation {
    AblyAnnotation {
        action: Some(match event.action {
            AnnotationEventAction::Create => 0,
            AnnotationEventAction::Delete => 1,
        }),
        id: event.id,
        serial: Some(event.serial),
        message_serial: Some(event.message_serial),
        annotation_type: Some(event.annotation_type),
        name: event.name,
        client_id: event.client_id,
        count: event.count,
        data: event.data,
        encoding: event.encoding,
        timestamp: Some(event.timestamp),
    }
}

pub(super) fn ably_summary_annotations(value: &Value) -> Option<Value> {
    let mut annotations = value.get("annotations")?.clone();
    let summary = annotations
        .as_object_mut()?
        .get_mut(&"summary")?
        .as_object_mut()?;
    for (annotation_type, names) in summary.iter_mut() {
        let is_multiple = AnnotationType::new(annotation_type.to_string())
            .and_then(|annotation_type| annotation_type.summarizer())
            .is_ok_and(|summarizer| {
                summarizer == sockudo_core::annotations::AnnotationSummarizer::Multiple
            });
        if !is_multiple {
            continue;
        }
        let Some(names) = names.as_object_mut() else {
            continue;
        };
        for (_, bucket) in names.iter_mut() {
            let Some(bucket) = bucket.as_object_mut() else {
                continue;
            };
            if let Some(client_counts) = bucket.remove(&"clientCounts") {
                bucket.insert("clientIds", client_counts);
            }
        }
    }
    Some(annotations)
}

pub(super) enum AblyAnnotationCommand {
    Create {
        message_serial: MessageSerial,
        annotation_type: AnnotationType,
        id: Option<AnnotationId>,
        name: Option<String>,
        client_id: Option<String>,
        count: Option<u64>,
        data: Option<Value>,
        encoding: Option<String>,
    },
    Delete(AblyAnnotationDeleteSelector),
}

pub(super) struct AblyAnnotationDeleteSelector {
    pub(super) message_serial: MessageSerial,
    pub(super) annotation_type: AnnotationType,
    pub(super) id: Option<AnnotationId>,
    pub(super) target_serial: Option<AnnotationSerial>,
    pub(super) name: Option<String>,
    pub(super) client_id: Option<String>,
}

pub(super) fn parse_ably_annotation_command(
    annotation: AblyAnnotation,
    path_message_serial: Option<&MessageSerial>,
    authenticated_client_id: Option<&str>,
) -> Result<AblyAnnotationCommand, AppError> {
    let message_serial = match (annotation.message_serial, path_message_serial) {
        (Some(raw), Some(path)) => {
            let body = MessageSerial::new(raw)?;
            if &body != path {
                return Err(AppError::InvalidInput(
                    "annotation.messageSerial must match the request path".to_string(),
                ));
            }
            body
        }
        (Some(raw), None) => MessageSerial::new(raw)?,
        (None, Some(path)) => path.clone(),
        (None, None) => {
            return Err(AppError::InvalidInput(
                "annotation.messageSerial is required".to_string(),
            ));
        }
    };
    let annotation_type = AnnotationType::new(
        annotation
            .annotation_type
            .ok_or_else(|| AppError::InvalidInput("annotation.type is required".to_string()))?,
    )?;
    let id = annotation.id.map(AnnotationId::new).transpose()?;

    match annotation.action.unwrap_or(0) {
        0 => {
            let client_id = match (authenticated_client_id, annotation.client_id) {
                (Some(authenticated), Some(requested)) if authenticated != requested => {
                    return Err(AppError::Forbidden(
                        "annotation.clientId must match authenticated clientId".to_string(),
                    ));
                }
                (Some(authenticated), _) => Some(authenticated.to_string()),
                (None, requested) => requested,
            };
            Ok(AblyAnnotationCommand::Create {
                message_serial,
                annotation_type,
                id,
                name: annotation.name,
                client_id,
                count: annotation.count,
                data: annotation.data,
                encoding: annotation.encoding,
            })
        }
        1 => Ok(AblyAnnotationCommand::Delete(
            AblyAnnotationDeleteSelector {
                message_serial,
                annotation_type,
                id,
                target_serial: annotation.serial.map(AnnotationSerial::new).transpose()?,
                name: annotation.name,
                client_id: annotation
                    .client_id
                    .or_else(|| authenticated_client_id.map(str::to_string)),
            },
        )),
        action => Err(AppError::InvalidInput(format!(
            "unsupported annotation action {action}"
        ))),
    }
}

pub(super) fn require_ably_annotations_enabled(
    handler: &ConnectionHandler,
    app: &App,
    channel: &str,
) -> Result<(), AppError> {
    if !handler.server_options().annotations.enabled {
        return Err(AppError::FeatureDisabled(format!(
            "Annotations are disabled globally for channel '{channel}'"
        )));
    }
    if !app.annotations_enabled_for_channel(channel) {
        return Err(AppError::Forbidden(format!(
            "Annotations are disabled by channel policy for channel '{channel}'"
        )));
    }
    Ok(())
}

pub(super) fn authorize_ably_annotation_delete(
    capabilities: Option<&ConnectionCapabilities>,
    channel: &AblyChannelName,
    actor_client_id: Option<&str>,
    target_client_id: Option<&str>,
) -> Result<(), AppError> {
    let Some(capabilities) = capabilities else {
        return Ok(());
    };
    let delete_any = ensure_ably_channel_capability(
        Some(capabilities),
        channel,
        AblyCapabilityCheck::AnnotationDeleteAny,
    )
    .is_ok();
    let delete_own = ensure_ably_channel_capability(
        Some(capabilities),
        channel,
        AblyCapabilityCheck::AnnotationDeleteOwn,
    )
    .is_ok()
        && actor_client_id.is_some()
        && actor_client_id == target_client_id;

    if delete_any && actor_client_id.is_none() {
        return Err(AppError::Forbidden(
            "annotation-delete-any requires an identified client".to_string(),
        ));
    }
    if delete_any || delete_own {
        Ok(())
    } else {
        Err(AppError::Forbidden(format!(
            "annotation-delete-own or annotation-delete-any capability is required for channel '{}'",
            channel.requested()
        )))
    }
}

pub(super) async fn find_ably_annotation_delete_target(
    handler: &ConnectionHandler,
    app_id: &str,
    channel: &str,
    selector: &AblyAnnotationDeleteSelector,
) -> Result<StoredAnnotationEvent, AppError> {
    if let Some(target_serial) = selector.target_serial.as_ref() {
        let target = handler
            .annotation_store()
            .get_event_by_serial(AnnotationEventLookupRequest {
                app_id: app_id.to_string(),
                channel_id: channel.to_string(),
                annotation_serial: target_serial.clone(),
            })
            .await?
            .ok_or_else(|| {
                AppError::NotFound(format!(
                    "Annotation '{}' was not found in channel '{channel}'",
                    target_serial.as_str()
                ))
            })?;
        if target.message_serial() != &selector.message_serial
            || target.annotation_type() != &selector.annotation_type
            || target.annotation.action != AnnotationAction::Create
            || selector
                .id
                .as_ref()
                .is_some_and(|id| id != &target.annotation.id)
            || selector
                .name
                .as_deref()
                .is_some_and(|name| target.annotation.name.as_deref() != Some(name))
            || selector
                .client_id
                .as_deref()
                .is_some_and(|client_id| target.annotation.client_id.as_deref() != Some(client_id))
        {
            return Err(AppError::NotFound(format!(
                "Annotation '{}' does not match the requested message and type",
                target_serial.as_str()
            )));
        }
        return Ok(target);
    }

    let events = handler
        .annotation_store()
        .get_events(AnnotationEventsRequest {
            app_id: app_id.to_string(),
            channel_id: channel.to_string(),
            message_serial: selector.message_serial.clone(),
            annotation_type: selector.annotation_type.clone(),
        })
        .await?;
    let deleted = events
        .iter()
        .filter(|event| event.annotation.action == AnnotationAction::Delete)
        .map(|event| event.annotation.id.clone())
        .collect::<BTreeSet<_>>();
    events
        .into_iter()
        .rev()
        .find(|event| {
            event.annotation.action == AnnotationAction::Create
                && !deleted.contains(&event.annotation.id)
                && selector
                    .id
                    .as_ref()
                    .is_none_or(|id| id == &event.annotation.id)
                && selector
                    .name
                    .as_deref()
                    .is_none_or(|name| event.annotation.name.as_deref() == Some(name))
                && selector.client_id.as_deref().is_none_or(|client_id| {
                    event.annotation.client_id.as_deref() == Some(client_id)
                })
        })
        .ok_or_else(|| {
            AppError::NotFound(format!(
                "No active annotation matched message '{}' and type '{}'",
                selector.message_serial.as_str(),
                selector.annotation_type.as_str()
            ))
        })
}

pub(super) async fn apply_ably_annotation_command(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &AblyChannelName,
    capabilities: Option<&ConnectionCapabilities>,
    actor_client_id: Option<&str>,
    command: AblyAnnotationCommand,
) -> Result<(), AppError> {
    require_ably_annotations_enabled(handler, app, channel.base())?;
    match command {
        AblyAnnotationCommand::Create {
            message_serial,
            annotation_type,
            id,
            name,
            client_id,
            count,
            data,
            encoding,
        } => {
            ensure_ably_channel_capability_app_error(
                capabilities,
                channel,
                AblyCapabilityCheck::AnnotationPublish,
            )?;
            handler
                .publish_annotation_runtime(PublishAnnotationRuntimeRequest {
                    app: app.clone(),
                    channel: channel.base().to_string(),
                    message_serial,
                    annotation_type,
                    id,
                    name,
                    client_id,
                    count,
                    data,
                    encoding,
                })
                .await?;
        }
        AblyAnnotationCommand::Delete(selector) => {
            let target =
                find_ably_annotation_delete_target(handler, &app.id, channel.base(), &selector)
                    .await?;
            authorize_ably_annotation_delete(
                capabilities,
                channel,
                actor_client_id,
                target.annotation.client_id.as_deref(),
            )?;
            handler
                .delete_annotation_runtime(DeleteAnnotationRuntimeRequest {
                    app: app.clone(),
                    channel: channel.base().to_string(),
                    message_serial: selector.message_serial,
                    target_serial: target.annotation.serial,
                })
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn handle_ably_annotation(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    sender: &AblySender,
    inbound: AblyProtocolMessage,
) {
    let Some(raw_channel) = inbound.channel.clone() else {
        send_publish_nack(sender, &inbound, 40000, "ANNOTATION requires channel");
        return;
    };
    let channel = match AblyChannelName::parse(raw_channel) {
        Ok(channel) => channel,
        Err(error) => {
            send_publish_nack(sender, &inbound, 40010, error.to_string());
            return;
        }
    };
    let annotations = inbound.annotations.clone().unwrap_or_default();
    if annotations.is_empty() {
        send_publish_nack(sender, &inbound, 40000, "ANNOTATION requires annotations");
        return;
    }
    for annotation in annotations {
        let command = match parse_ably_annotation_command(annotation, None, client_id) {
            Ok(command) => command,
            Err(error) => {
                let error = ably_error_info_from_app_error(error);
                send_publish_nack(sender, &inbound, error.code, error.message);
                return;
            }
        };
        if let Err(error) =
            apply_ably_annotation_command(handler, app, &channel, capabilities, client_id, command)
                .await
        {
            let error = ably_error_info_from_app_error(error);
            send_publish_nack(sender, &inbound, error.code, error.message);
            return;
        }
    }
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: Some(publish_ack_count(&inbound)),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
}

pub(super) fn apply_resumed_attach_recovery(
    resumed_attach: bool,
    channel_serial: &mut Option<String>,
    serial_source: AblyChannelSerialSource,
    attach_options: &mut AblyAttachOptions,
    mut recovery_gate: AblyAttachGate,
) -> Result<AblyAttachGate, AblyRecoveryFailure> {
    if !resumed_attach {
        return Ok(recovery_gate);
    }

    attach_options.attach_resume = true;
    let Some(position) = channel_serial
        .as_deref()
        .map(parse_ably_channel_serial)
        .transpose()?
    else {
        return Ok(recovery_gate);
    };

    if serial_source != AblyChannelSerialSource::HotReplay {
        // An explicit durable position is the client's authoritative recovery
        // boundary. Cold recovery will replay strictly after it, so mixing in
        // the same-node in-memory tail could redeliver acknowledged data.
        return Ok(AblyAttachGate::default());
    }

    // Without durable history, the live recovery gate is the only replay
    // source. Keep only messages after the client's boundary and fail closed
    // if their continuity cannot be proven. Clearing the inbound serial makes
    // the clean attach advertise the current hot-buffer position instead of
    // incorrectly attempting cold recovery against a disabled store.
    let mut messages = Vec::with_capacity(recovery_gate.messages.len());
    for message in recovery_gate.messages {
        let raw_position = message.channel_serial.as_deref().ok_or_else(|| {
            AblyRecoveryFailure::channel(
                90005,
                "unable to recover channel because a buffered message has no channel serial",
            )
        })?;
        let message_position = parse_ably_channel_serial(raw_position)?;
        if message_position.stream_id != position.stream_id {
            return Err(AblyRecoveryFailure::channel(
                90005,
                "unable to recover channel because the stream changed",
            ));
        }
        if message_position.serial > position.serial {
            messages.push(message);
        }
    }
    recovery_gate.messages = messages;
    *channel_serial = None;
    Ok(recovery_gate)
}

pub(super) fn heartbeat_response(inbound: AblyProtocolMessage) -> AblyProtocolMessage {
    AblyProtocolMessage {
        action: ACTION_HEARTBEAT,
        id: inbound.id,
        ..empty_protocol_message(ACTION_HEARTBEAT)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ably_attach(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    session_id: &str,
    sender: &AblySender,
    channel: &AblyChannelName,
    filter: Option<Arc<AblyMessageFilter>>,
    channel_serial: Option<String>,
    options: AblyAttachOptions,
) {
    let attach_resume = options.attach_resume;
    let rewind_requested =
        options.params.contains_key("rewind") || options.params.contains_key("rewindCount");
    let rewind = resolve_ably_rewind(&options.params, attach_resume);
    if rewind_requested && !attach_resume && rewind.is_none() {
        send_channel_error(
            sender,
            channel.requested(),
            StatusCode::BAD_REQUEST,
            40000,
            "invalid rewind parameter",
        );
        return;
    }
    let echo = hub
        .session_echo
        .get(session_id)
        .is_none_or(|connection_echo| *connection_echo)
        && options
            .params
            .get("echo")
            .is_none_or(|value| !value.eq_ignore_ascii_case("false"));
    let presence = match PresenceService::new(Arc::clone(handler))
        .snapshot(&app.id, channel.base())
        .await
    {
        Ok(presence) => presence
            .into_iter()
            .map(|record| ably_presence_from_record(record, 1))
            .collect::<Vec<_>>(),
        Err(error) => {
            send_channel_error(
                sender,
                channel.requested(),
                StatusCode::SERVICE_UNAVAILABLE,
                50003,
                format!("presence state is unavailable: {error}"),
            );
            return;
        }
    };
    let attachment = || AblyAttachment {
        connection_id,
        session_id,
        sender: sender.clone(),
        filter: filter.clone(),
        params: options.params.clone(),
        mode_flags: options.mode_flags,
        echo,
        presence: presence.clone(),
    };
    if let Err(error) = hub.begin_attach(&app.id, channel, &attachment()).await {
        send_channel_error(
            sender,
            channel.requested(),
            StatusCode::SERVICE_UNAVAILABLE,
            50000,
            error.to_string(),
        );
        return;
    }
    let Some(channel_serial) = channel_serial else {
        let attach_serial = current_ably_channel_serial(handler, app, channel.base()).await;
        let replay = if let Some(rewind) = rewind.as_ref() {
            let position = attach_serial
                .as_deref()
                .and_then(|serial| parse_ably_channel_serial(serial).ok());
            match collect_ably_rewind(handler, app, channel.base(), rewind, position.as_ref()).await
            {
                Ok(replay) => replay,
                Err(failure) => {
                    hub.attach_failed(&app.id, channel, attachment(), attach_serial, failure);
                    return;
                }
            }
        } else {
            Vec::new()
        };
        hub.attach_clean(
            &app.id,
            channel,
            attachment(),
            attach_serial,
            replay,
            attach_resume,
        );
        info!(
            protocol = "ably",
            app_id = %app.id,
            connection_id = %connection_id,
            channel = %channel.requested(),
            recovery_source = if rewind.is_some() { "rewind" } else { "none" },
            "channel attached"
        );
        return;
    };

    let position = match parse_ably_channel_serial(&channel_serial) {
        Ok(position) => position,
        Err(failure) => {
            hub.attach_failed(
                &app.id,
                channel,
                attachment(),
                Some(channel_serial),
                failure,
            );
            return;
        }
    };

    hub.metrics.replay_source.fetch_add(1, Ordering::Relaxed);
    match collect_ably_cold_recovery(
        handler,
        app,
        channel.base(),
        &position,
        hub.metrics.as_ref(),
    )
    .await
    {
        Ok(mut replay) => {
            if let Some(rewind) = rewind.as_ref() {
                match collect_ably_rewind(handler, app, channel.base(), rewind, Some(&position))
                    .await
                {
                    Ok(backlog) => replay.extend(backlog),
                    Err(failure) => {
                        hub.attach_failed(
                            &app.id,
                            channel,
                            attachment(),
                            Some(channel_serial),
                            failure,
                        );
                        return;
                    }
                }
            }
            hub.attach_cold_recovery(&app.id, channel, attachment(), &position, replay);
            info!(
                protocol = "ably",
                app_id = %app.id,
                connection_id = %connection_id,
                channel = %channel.requested(),
                recovery_source = "cold",
                "channel attached"
            );
        }
        Err(failure) => hub.attach_failed(
            &app.id,
            channel,
            attachment(),
            Some(channel_serial),
            failure,
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AblyChannelSerialSource {
    Version,
    History,
    HotReplay,
}

pub(super) const fn ably_channel_serial_source(
    versioned_messages_enabled: bool,
    history_enabled: bool,
) -> AblyChannelSerialSource {
    if versioned_messages_enabled && history_enabled {
        AblyChannelSerialSource::Version
    } else if history_enabled {
        AblyChannelSerialSource::History
    } else {
        AblyChannelSerialSource::HotReplay
    }
}

pub(super) async fn current_ably_version_channel_serial(
    version_store: &dyn sockudo_core::version_store::VersionStore,
    app_id: &str,
    channel: &str,
) -> Option<String> {
    let stream_id = version_store.ensure_stream_id(app_id, channel).await.ok()?;
    let state = version_store.stream_state(app_id, channel).await.ok()?;
    if state.stream_id.as_deref().is_some_and(|id| id != stream_id) {
        return None;
    }
    Some(encode_ably_channel_serial(
        &stream_id,
        state.newest_available_delivery_serial.unwrap_or(0),
    ))
}

pub(super) async fn current_ably_channel_serial(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
) -> Option<String> {
    let history_enabled = app
        .resolved_history(channel, &handler.server_options().history)
        .enabled;
    match ably_channel_serial_source(
        handler.server_options().versioned_messages.enabled,
        history_enabled,
    ) {
        AblyChannelSerialSource::Version => {
            current_ably_version_channel_serial(handler.version_store().as_ref(), &app.id, channel)
                .await
        }
        AblyChannelSerialSource::History => handler
            .history_store()
            .stream_inspection(&app.id, channel)
            .await
            .ok()
            .and_then(|inspection| inspection.stream_id.zip(inspection.next_serial))
            .map(|(stream_id, next_serial)| {
                encode_ably_channel_serial(&stream_id, next_serial.saturating_sub(1))
            }),
        AblyChannelSerialSource::HotReplay => {
            #[cfg(feature = "recovery")]
            {
                handler.replay_buffer().and_then(|replay_buffer| {
                    let position = replay_buffer.latest_stored_position(&app.id, channel)?;
                    Some(encode_ably_channel_serial(
                        &position.stream_id,
                        position.serial,
                    ))
                })
            }
            #[cfg(not(feature = "recovery"))]
            {
                let _ = (handler, app, channel);
                None
            }
        }
    }
}

pub(super) async fn collect_ably_cold_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
    metrics: &OutboundMetrics,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    if handler.server_options().versioned_messages.enabled {
        metrics
            .recovery_backend_calls
            .fetch_add(1, Ordering::Relaxed);
        let state = handler
            .version_store()
            .stream_state(&app.id, channel)
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to recover channel '{channel}': {error}"),
                )
            })?;
        if state.stream_id.is_some() {
            return collect_ably_version_recovery(handler, app, channel, position, metrics).await;
        }
        #[cfg(feature = "recovery")]
        if let Some(replay_buffer) = handler.replay_buffer() {
            let current = replay_buffer.current_position(&app.id, channel);
            if current.stream_id == position.stream_id && current.serial == position.serial {
                return Ok(Vec::new());
            }
        }
    }

    collect_ably_history_recovery(handler, app, channel, position, metrics).await
}

pub(super) async fn collect_ably_rewind(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    rewind: &SubscriptionRewind,
    high_water: Option<&AblyChannelPosition>,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let policy = app.resolved_history(channel, &handler.server_options().history);
    if !policy.rewind_allowed() {
        return Err(AblyRecoveryFailure::channel(
            40000,
            format!("channel rewind is disabled by policy for channel '{channel}'"),
        ));
    }
    let limit = rewind.limit().min(policy.max_page_size).max(1);

    if handler.server_options().versioned_messages.enabled {
        let state = handler
            .version_store()
            .stream_state(&app.id, channel)
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to rewind channel '{channel}': {error}"),
                )
            })?;
        if let (Some(stream_id), Some(newest)) =
            (state.stream_id, state.newest_available_delivery_serial)
        {
            if high_water.is_some_and(|position| position.stream_id != stream_id) {
                return Err(AblyRecoveryFailure::channel(
                    90005,
                    format!("unable to rewind channel '{channel}' because the stream changed"),
                ));
            }
            let newest = high_water.map_or(newest, |position| position.serial.min(newest));
            let requested = u64::try_from(limit).unwrap_or(u64::MAX);
            let mut records = handler
                .version_store()
                .replay_after(sockudo_core::version_store::VersionReplayRequest {
                    app_id: app.id.clone(),
                    channel: channel.to_string(),
                    after_delivery_serial: newest.saturating_sub(requested),
                    limit,
                })
                .await
                .map_err(|error| {
                    AblyRecoveryFailure::channel(
                        90000,
                        format!("unable to rewind channel '{channel}': {error}"),
                    )
                })?;
            records.sort_by_key(|record| record.delivery_serial());
            let cutoff_ms = match rewind {
                SubscriptionRewind::Count(_) => None,
                SubscriptionRewind::Seconds(seconds) => {
                    let window_ms = i64::try_from(*seconds)
                        .unwrap_or(i64::MAX)
                        .saturating_mul(1000);
                    Some(now_ms().saturating_sub(window_ms))
                }
            };
            let mut replay = Vec::with_capacity(records.len());
            for record in records.into_iter().filter(|record| {
                record.delivery_serial() <= newest
                    && cutoff_ms.is_none_or(|cutoff| record.message.version.timestamp_ms >= cutoff)
            }) {
                let serial = record.delivery_serial();
                let runtime =
                    handler.build_runtime_message_from_record(&record, Some(stream_id.clone()));
                let channel_serial = Some(encode_ably_channel_serial(&stream_id, serial));
                replay.push(match record.envelope.as_ref() {
                    Some(envelope) => ably_protocol_message_from_envelope(
                        channel,
                        &runtime,
                        envelope,
                        AblyMessageProjection::Aggregate,
                        channel_serial,
                    )?,
                    None => ably_protocol_message_from_pusher(
                        channel,
                        &runtime,
                        AblyMessageProjection::Aggregate,
                        channel_serial,
                    )?,
                });
            }
            return Ok(replay);
        }
    }

    let mut page = handler
        .history_store()
        .read_page(build_ably_rewind_history_request(
            &app.id,
            channel,
            rewind,
            policy.max_page_size,
            high_water,
            now_ms(),
        ))
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to rewind channel '{channel}': {error}"),
            )
        })?;
    if let Some(high_water) = high_water
        && page.retained.stream_id.is_some()
        && page.retained.stream_id.as_deref() != Some(high_water.stream_id.as_str())
    {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to rewind channel '{channel}' because the stream changed"),
        ));
    }
    page.items.sort_by_key(|item| item.serial);
    let mut replay = Vec::with_capacity(page.items.len());
    for item in page.items {
        let stored =
            decode_stored_message_payload(item.payload_bytes.as_ref()).map_err(|error| {
                AblyRecoveryFailure::channel(
                    90000,
                    format!("unable to rewind channel '{channel}': {error}"),
                )
            })?;
        let channel_serial = Some(encode_ably_channel_serial(&item.stream_id, item.serial));
        replay.push(match stored.envelope.as_ref() {
            Some(envelope) => ably_protocol_message_from_envelope(
                channel,
                &stored.message,
                envelope,
                AblyMessageProjection::Aggregate,
                channel_serial,
            )?,
            None => ably_protocol_message_from_pusher(
                channel,
                &stored.message,
                AblyMessageProjection::Aggregate,
                channel_serial,
            )?,
        });
    }
    Ok(replay)
}

pub(super) async fn collect_ably_version_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
    metrics: &OutboundMetrics,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    metrics
        .recovery_backend_calls
        .fetch_add(1, Ordering::Relaxed);
    let stream_state = handler
        .version_store()
        .stream_state(&app.id, channel)
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;

    if stream_state.stream_id.as_deref() != Some(position.stream_id.as_str()) {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to recover channel '{channel}' because the stream changed"),
        ));
    }

    if let Some(oldest) = stream_state.oldest_available_delivery_serial
        && position.serial.saturating_add(1) < oldest
    {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because messages expired"),
        ));
    }

    let newest = stream_state
        .newest_available_delivery_serial
        .unwrap_or(position.serial);
    if newest <= position.serial {
        return Ok(Vec::new());
    }
    let missing = newest.saturating_sub(position.serial);
    let limit = usize::try_from(missing).map_err(|_| {
        AblyRecoveryFailure::channel(
            90004,
            format!("unable to recover channel '{channel}' because the message limit was exceeded"),
        )
    })?;
    if limit > ABLY_COMPAT_MAX_REPLAY_MESSAGES {
        return Err(AblyRecoveryFailure::channel(
            90004,
            format!("unable to recover channel '{channel}' because the message limit was exceeded"),
        ));
    }

    metrics
        .recovery_backend_calls
        .fetch_add(1, Ordering::Relaxed);
    let records = handler
        .version_store()
        .replay_after(sockudo_core::version_store::VersionReplayRequest {
            app_id: app.id.clone(),
            channel: channel.to_string(),
            after_delivery_serial: position.serial,
            limit,
        })
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90003,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;

    if records.len() != limit {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because continuity is unprovable"),
        ));
    }

    let mut replay = Vec::with_capacity(records.len());
    for record in records {
        let delivery_serial = record.delivery_serial();
        let runtime =
            handler.build_runtime_message_from_record(&record, Some(position.stream_id.clone()));
        let channel_serial = Some(encode_ably_channel_serial(
            &position.stream_id,
            delivery_serial,
        ));
        replay.push(match record.envelope.as_ref() {
            Some(envelope) => ably_protocol_message_from_envelope(
                channel,
                &runtime,
                envelope,
                AblyMessageProjection::Mutation,
                channel_serial,
            )?,
            None => ably_protocol_message_from_pusher(
                channel,
                &runtime,
                AblyMessageProjection::Mutation,
                channel_serial,
            )?,
        });
    }
    Ok(replay)
}

pub(super) async fn collect_ably_history_recovery(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    position: &AblyChannelPosition,
    metrics: &OutboundMetrics,
) -> Result<Vec<AblyProtocolMessage>, AblyRecoveryFailure> {
    let history_policy = app.resolved_history(channel, &handler.server_options().history);
    if !history_policy.enabled {
        return Err(AblyRecoveryFailure::channel(
            90003,
            format!("unable to recover channel '{channel}' because messages expired"),
        ));
    }

    metrics
        .recovery_backend_calls
        .fetch_add(1, Ordering::Relaxed);
    let stream_state = handler
        .history_store()
        .stream_runtime_state(&app.id, channel)
        .await
        .map_err(|error| {
            AblyRecoveryFailure::channel(
                90000,
                format!("unable to recover channel '{channel}': {error}"),
            )
        })?;
    if !stream_state.recovery_allowed {
        return Err(AblyRecoveryFailure::channel(
            if stream_state.reset_required {
                90005
            } else {
                90000
            },
            stream_state.reason.unwrap_or_else(|| {
                format!("unable to recover channel '{channel}' because continuity is unprovable")
            }),
        ));
    }
    if stream_state.stream_id.as_deref() != Some(position.stream_id.as_str()) {
        return Err(AblyRecoveryFailure::channel(
            90005,
            format!("unable to recover channel '{channel}' because the stream changed"),
        ));
    }

    let mut replay = Vec::new();
    let mut cursor = None;
    let bounds = HistoryQueryBounds {
        start_serial: Some(position.serial.saturating_add(1)),
        end_serial: None,
        start_time_ms: None,
        end_time_ms: None,
    };
    loop {
        if replay.len() >= ABLY_COMPAT_MAX_REPLAY_MESSAGES {
            return Err(AblyRecoveryFailure::channel(
                90004,
                format!(
                    "unable to recover channel '{channel}' because the message limit was exceeded"
                ),
            ));
        }
        metrics
            .recovery_backend_calls
            .fetch_add(1, Ordering::Relaxed);
        let page = handler
            .history_store()
            .read_page(HistoryReadRequest {
                app_id: app.id.clone(),
                channel: channel.to_string(),
                direction: HistoryDirection::OldestFirst,
                limit: history_policy
                    .max_page_size
                    .min(ABLY_COMPAT_MAX_REPLAY_MESSAGES.saturating_sub(replay.len()))
                    .max(1),
                cursor: cursor.clone(),
                bounds: bounds.clone(),
            })
            .await
            .map_err(|error| {
                AblyRecoveryFailure::channel(
                    90003,
                    format!("unable to recover channel '{channel}': {error}"),
                )
            })?;

        if page.retained.stream_id.as_deref() != Some(position.stream_id.as_str()) {
            return Err(AblyRecoveryFailure::channel(
                90005,
                format!("unable to recover channel '{channel}' because the stream changed"),
            ));
        }
        if page.truncated_by_retention {
            return Err(AblyRecoveryFailure::channel(
                90003,
                format!("unable to recover channel '{channel}' because messages expired"),
            ));
        }

        for item in page.items {
            let stored =
                decode_stored_message_payload(item.payload_bytes.as_ref()).map_err(|error| {
                    AblyRecoveryFailure::channel(
                        90000,
                        format!("unable to recover channel '{channel}': {error}"),
                    )
                })?;
            let raw_message = stored.message;
            replay.push(match stored.envelope.as_ref() {
                Some(envelope) => ably_protocol_message_from_envelope(
                    channel,
                    &raw_message,
                    envelope,
                    AblyMessageProjection::Mutation,
                    Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
                )?,
                None => ably_protocol_message_from_pusher(
                    channel,
                    &raw_message,
                    AblyMessageProjection::Mutation,
                    Some(encode_ably_channel_serial(&item.stream_id, item.serial)),
                )?,
            });
        }

        if !page.has_more {
            break;
        }
        cursor = page.next_cursor;
    }

    Ok(replay)
}
