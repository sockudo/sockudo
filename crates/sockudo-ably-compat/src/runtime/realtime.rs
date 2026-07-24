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
            let ws_cfg = handler.server_options().websocket.to_sockudo_ws_config(
                handler.server_options().websocket_max_payload_kb,
                handler.server_options().activity_timeout,
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

    let ws_cfg = handler.server_options().websocket.to_sockudo_ws_config(
        handler.server_options().websocket_max_payload_kb,
        handler.server_options().activity_timeout,
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
                    stats_transport,
                },
            )
            .await
            {
                warn!(error = %error, "Ably compatibility socket closed with error");
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
        let _ = writer.flush().await;
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
    let (sender, mut outbound) =
        AblyOutbound::channel(format, OutboundLimits::default(), Arc::clone(&hub.metrics));
    let (peer_close_tx, mut peer_close_rx) = crossfire::oneshot::oneshot();
    let mut peer_close_tx = Some(peer_close_tx);
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
                frame = outbound.recv() => {
                    let Some(frame) = frame else { break };
                    let frame = match format {
                        AblyFormat::Json => Message::Text((*frame.bytes).clone()),
                        AblyFormat::MsgPack => Message::Binary((*frame.bytes).clone()),
                    };
                    if let Err(error) = writer.send(frame).await {
                        debug!(error = %error, "Ably compatibility socket writer closed");
                        return;
                    }
                }
            }
        }
        let _ = writer.close(1000, "Ably session closed").await;
    });
    let heartbeat_sender = sender.clone();
    let heartbeat_task = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_millis(DEFAULT_MAX_IDLE_INTERVAL_MS / 2));
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

    send_protocol(
        &sender,
        connected_message(
            &connection_id,
            &connection_key,
            authorization.connection_client_id.clone(),
            connection_error,
        ),
    );

    let mut attached_channels = HashMap::new();
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
    let mut revocation_poll = tokio::time::interval(Duration::from_millis(500));
    revocation_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renewal_hint_sent = false;
    let mut graceful_close = false;
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
            _ = revocation_poll.tick() => {
                if !hub.session_is_current(&app.id, &connection_id, &session_id).await {
                    break;
                }
                if authorization.revocable
                    && hub.authorization_is_revoked(&app.id, &authorization, &attached_channels).await
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
                debug!(error = %error, "Ably compatibility socket reader closed");
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
                    send_protocol_disconnected(&sender, error.code, error.message);
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
                debug!(error = %error, "Ably compatibility protocol handler stopped");
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
                    wire_id: Some(removal.member.id.clone()),
                    member: removal.member,
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
                app_id = %app.id,
                connection_id = %connection_id,
                error = %error,
                "failed to replicate graceful presence connection removal"
            );
        }
    }
    for (requested, _) in attached_channels {
        if let Ok(channel) = AblyChannelName::parse(requested)
            && let Err(error) = hub.unsubscribe(&app.id, &channel, &session_id).await
        {
            warn!(app_id = %app.id, error = %error, "failed to persist channel close stats");
        }
    }
    let owns_session = hub
        .session_is_current(&app.id, &connection_id, &session_id)
        .await;
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
    hub.release_session_owner(&app.id, &connection_id, &session_id)
        .await;
    heartbeat_task.abort();
    let _ = heartbeat_task.await;
    drop(sender);
    let _ = writer_task.await;
    hub.stats.connection_closed(&app.id);
    if let Ok(closed) = StatsObservation::connection_closed(&app.id, now_ms(), stats_transport)
        && let Err(error) = hub.stats.record(closed).await
    {
        warn!(app_id = %app.id, error = %error, "failed to persist connection close stats");
    }
    Ok(())
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

    let downgraded = attached_channels
        .iter()
        .filter(|(requested, attachment)| {
            ensure_ably_channel_capability_parts(
                next.capabilities.as_ref(),
                requested,
                attachment.channel.base(),
                AblyCapabilityCheck::Subscribe,
            )
            .is_err()
        })
        .map(|(requested, _)| requested.clone())
        .collect::<Vec<_>>();
    for channel in downgraded {
        if let Ok(parsed) = AblyChannelName::parse(channel.clone())
            && let Err(error) = hub.unsubscribe(&app.id, &parsed, session_id).await
        {
            warn!(app_id = %app.id, error = %error, "failed to persist channel close stats during AUTH update");
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
    replace_presence_on_reenter: bool,
    mut inbound: AblyProtocolMessage,
) -> SockudoResult<AblyProtocolControl> {
    let control = ably_protocol_control(inbound.action);
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
            *active_connection_key
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = connection_key.clone();
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
            if let Err(error) = ensure_ably_channel_capability(
                capabilities,
                &channel,
                AblyCapabilityCheck::Subscribe,
            ) {
                send_channel_error(
                    sender,
                    channel.requested(),
                    error.status,
                    error.code,
                    error.message,
                );
                return Ok(AblyProtocolControl::Continue);
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
            let attach_options =
                AblyAttachOptions::from_wire(inbound.flags, inbound.params.clone());
            for (mode, check) in [
                (ABLY_MODE_PUBLISH, AblyCapabilityCheck::Publish),
                (ABLY_MODE_PRESENCE, AblyCapabilityCheck::Presence),
                (
                    ABLY_MODE_ANNOTATION_PUBLISH,
                    AblyCapabilityCheck::AnnotationMutate,
                ),
                (
                    ABLY_MODE_ANNOTATION_SUBSCRIBE,
                    AblyCapabilityCheck::AnnotationSubscribe,
                ),
            ] {
                if attach_options.explicit_modes
                    && attach_options.mode_flags & mode != 0
                    && let Err(error) =
                        ensure_ably_channel_capability(capabilities, &channel, check)
                {
                    send_channel_error(
                        sender,
                        channel.requested(),
                        error.status,
                        error.code,
                        error.message,
                    );
                    return Ok(AblyProtocolControl::Continue);
                }
            }
            let has_presence = previous.is_some_and(|attachment| attachment.has_presence)
                || !PresenceService::new(Arc::clone(handler))
                    .snapshot(&app.id, channel.base())
                    .await?
                    .is_empty();
            let attachment_state = AblyConnectionAttachment {
                channel: channel.clone(),
                params: attach_options.params.clone(),
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
                inbound.flags,
                inbound.params,
            );
            if tokio::time::timeout(Duration::from_millis(hub.config.attach_timeout_ms), attach)
                .await
                .is_err()
            {
                if let Err(error) = hub.unsubscribe(&app.id, &channel, session_id).await {
                    warn!(app_id = %app.id, channel = %channel.requested(), error = %error, "failed to clean up timed-out Ably attach");
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
    flags: Option<u64>,
    params: Option<HashMap<String, String>>,
) {
    let attach_resume = flags.is_some_and(|flags| flags & FLAG_ATTACH_RESUME != 0);
    let options = AblyAttachOptions::from_wire(flags, params);
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
        hub.attach_clean(&app.id, channel, attachment(), attach_serial, replay);
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

pub(super) async fn current_ably_channel_serial(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
) -> Option<String> {
    if handler.server_options().versioned_messages.enabled
        && let Ok(state) = handler.version_store().stream_state(&app.id, channel).await
        && let (Some(stream_id), Some(serial)) =
            (state.stream_id, state.newest_available_delivery_serial)
    {
        return Some(encode_ably_channel_serial(&stream_id, serial));
    }
    if app
        .resolved_history(channel, &handler.server_options().history)
        .enabled
        && let Ok(inspection) = handler
            .history_store()
            .stream_inspection(&app.id, channel)
            .await
        && let (Some(stream_id), Some(next_serial)) = (inspection.stream_id, inspection.next_serial)
    {
        return Some(encode_ably_channel_serial(
            &stream_id,
            next_serial.saturating_sub(1),
        ));
    }
    #[cfg(feature = "recovery")]
    {
        handler.replay_buffer().map(|replay_buffer| {
            let position = replay_buffer.current_position(&app.id, channel);
            encode_ably_channel_serial(&position.stream_id, position.serial)
        })
    }
    #[cfg(not(feature = "recovery"))]
    {
        let _ = (handler, app, channel);
        None
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
