//! Realtime publish and presence command handling shared with REST publishing.

use super::*;

pub(super) struct AblyPublishContext<'a> {
    pub(super) handler: &'a Arc<ConnectionHandler>,
    pub(super) hub: &'a Arc<AblyCompatHub>,
    pub(super) app: &'a App,
    pub(super) connection_id: &'a str,
    pub(super) client_id: Option<&'a str>,
    pub(super) capabilities: Option<&'a ConnectionCapabilities>,
    pub(super) privileged_server: bool,
    pub(super) sender: &'a AblySender,
}

pub(super) async fn handle_ably_publish(
    context: AblyPublishContext<'_>,
    inbound: AblyProtocolMessage,
) -> SockudoResult<()> {
    let AblyPublishContext {
        handler,
        hub,
        app,
        connection_id,
        client_id,
        capabilities,
        privileged_server,
        sender,
    } = context;
    let raw_channel = match inbound.channel.clone() {
        Some(channel) => channel,
        None => {
            send_publish_nack(sender, &inbound, 40000, "MESSAGE requires channel");
            return Ok(());
        }
    };
    let channel = match AblyChannelName::parse(raw_channel) {
        Ok(channel) => channel,
        Err(error) => {
            send_publish_nack(sender, &inbound, 40010, error.to_string());
            return Ok(());
        }
    };
    if let Err(error) =
        ensure_ably_channel_capability(capabilities, &channel, AblyCapabilityCheck::Publish)
    {
        send_publish_nack(sender, &inbound, error.code, error.message);
        return Ok(());
    }

    let messages = inbound.messages.clone().unwrap_or_default();
    let message_count = u64::try_from(messages.len()).unwrap_or(u64::MAX);
    let message_bytes = sonic_rs::to_vec(&messages)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    for message in &messages {
        if let Err(error) = validate_ably_publish_message(message, false)
            .and_then(|()| effective_ably_client_id(client_id, message).map(|_| ()))
        {
            send_publish_nack(sender, &inbound, 40000, error.to_string());
            return Ok(());
        }
    }
    let mut serials = Vec::with_capacity(messages.len());
    for (index, mut message) in messages.into_iter().enumerate() {
        if let Some(msg_serial) = inbound.msg_serial {
            message
                .id
                .get_or_insert_with(|| ably_realtime_message_id(connection_id, msg_serial, index));
        }
        let result = publish_ably_message(
            AblyMessagePublishContext {
                handler,
                hub,
                app,
                channel: channel.base(),
                connection_id: Some(connection_id),
                client_id,
                capabilities,
                privileged_server,
            },
            message,
        )
        .await;

        match result {
            Ok(serial) => serials.push(serial),
            Err(error) => {
                send_publish_nack(
                    sender,
                    &inbound,
                    40000,
                    format!("Failed to publish message {index}: {error}"),
                );
                return Ok(());
            }
        }
    }

    if message_count > 0 {
        hub.stats
            .record(
                StatsObservation::messages(
                    &app.id,
                    now_ms(),
                    "inbound",
                    "realtime",
                    message_count,
                    message_bytes,
                )
                .map_err(stats_sockudo_error)?,
            )
            .await
            .map_err(stats_sockudo_error)?;
    }

    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: Some(publish_ack_count(&inbound)),
            res: Some(json!([{ "serials": serials }])),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
    Ok(())
}

pub(super) fn stats_sockudo_error(error: StatsError) -> sockudo_core::error::Error {
    sockudo_core::error::Error::Internal(error.to_string())
}

pub(super) fn publish_ack_count(inbound: &AblyProtocolMessage) -> u64 {
    inbound.count.unwrap_or(1)
}

pub(super) fn ably_realtime_message_id(
    connection_id: &str,
    msg_serial: u64,
    index: usize,
) -> String {
    format!("{connection_id}:{msg_serial}:{index}")
}

pub(super) fn ably_presence_message_id(
    inbound_id: Option<&str>,
    connection_id: &str,
    msg_serial: u64,
    index: usize,
) -> String {
    let valid_reentry_id = inbound_id.filter(|id| {
        let Some(serial_and_index) = id
            .strip_prefix(connection_id)
            .and_then(|suffix| suffix.strip_prefix(':'))
        else {
            return false;
        };
        let Some((serial, index)) = serial_and_index.split_once(':') else {
            return false;
        };
        !serial.is_empty()
            && !index.is_empty()
            && !index.contains(':')
            && serial.parse::<u64>().is_ok()
            && index.parse::<u64>().is_ok()
    });
    valid_reentry_id.map_or_else(
        || ably_realtime_message_id(connection_id, msg_serial, index),
        str::to_string,
    )
}

pub(super) struct AblyMessagePublishContext<'a> {
    pub(super) handler: &'a Arc<ConnectionHandler>,
    pub(super) hub: &'a AblyCompatHub,
    pub(super) app: &'a App,
    pub(super) channel: &'a str,
    pub(super) connection_id: Option<&'a str>,
    pub(super) client_id: Option<&'a str>,
    pub(super) capabilities: Option<&'a ConnectionCapabilities>,
    pub(super) privileged_server: bool,
}

pub(super) async fn publish_ably_message(
    context: AblyMessagePublishContext<'_>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let AblyMessagePublishContext {
        handler,
        hub,
        app,
        channel,
        connection_id,
        client_id,
        capabilities,
        privileged_server,
    } = context;
    let action = message.action.unwrap_or(MESSAGE_CREATE);
    match action {
        MESSAGE_CREATE => {
            let effective_client_id = effective_ably_client_id(client_id, &message)?;
            publish_ably_create(
                handler,
                hub,
                app,
                channel,
                connection_id,
                effective_client_id.as_deref(),
                message,
            )
            .await
        }
        MESSAGE_APPEND => {
            publish_ably_append(
                handler,
                app,
                channel,
                client_id,
                MutableMessageActor {
                    client_id: client_id.map(str::to_string),
                    capabilities: capabilities.cloned(),
                    privileged_server,
                },
                message,
            )
            .await
        }
        MESSAGE_UPDATE => {
            publish_ably_update(
                handler,
                app,
                channel,
                client_id,
                MutableMessageActor {
                    client_id: client_id.map(str::to_string),
                    capabilities: capabilities.cloned(),
                    privileged_server,
                },
                message,
            )
            .await
        }
        MESSAGE_DELETE => {
            publish_ably_delete(
                handler,
                app,
                channel,
                client_id,
                MutableMessageActor {
                    client_id: client_id.map(str::to_string),
                    capabilities: capabilities.cloned(),
                    privileged_server,
                },
                message,
            )
            .await
        }
        MESSAGE_SUMMARY => Err(AppError::InvalidInput(format!(
            "Ably message action {action} is not implemented by this compatibility layer"
        ))),
        other => Err(AppError::InvalidInput(format!(
            "Unsupported Ably message action {other}"
        ))),
    }
}

pub(super) async fn publish_ably_create(
    handler: &Arc<ConnectionHandler>,
    hub: &AblyCompatHub,
    app: &App,
    channel: &str,
    connection_id: Option<&str>,
    client_id: Option<&str>,
    message: AblyMessage,
) -> Result<String, AppError> {
    let effective_client_id = effective_ably_client_id(client_id, &message)?;
    let client_id = effective_client_id.as_deref();
    let event_name = message.name.clone();
    #[cfg(feature = "push")]
    let push_payload = message
        .extras
        .as_ref()
        .and_then(|extras| extras.get("push"))
        .map(ably_push_payload_value)
        .transpose()?;
    let encoding = message.encoding.clone();
    let raw_data = message.data.clone();
    let data = message
        .data
        .map(|value| ably_message_data_to_message_data(value, encoding.as_deref()));
    let mut extras = ably_extras_to_message_extras(message.extras)?;
    if let Some(client_id) = client_id
        && event_name.as_deref().is_some_and(is_ai_event)
    {
        stamp_ai_identity(
            &mut extras,
            event_name.as_deref().unwrap_or_default(),
            client_id,
        )?;
    }
    if let Some(extras) = extras.as_ref() {
        extras
            .validate_ai_headers()
            .map_err(|error| AppError::InvalidInput(error.message))?;
    }
    let pusher_message = PusherMessage {
        event: event_name.clone(),
        channel: Some(channel.to_string()),
        data,
        name: None,
        user_id: client_id.map(str::to_string),
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: message.id.clone(),
        stream_id: None,
        serial: None,
        idempotency_key: message.id.clone(),
        extras,
        delta_sequence: None,
        delta_conflation_key: None,
    };
    let mut envelope = MessageEnvelope::from_message(
        &pusher_message,
        None,
        connection_id.map(str::to_string),
        now_ms(),
    )
    .map_err(AppError::InvalidInput)?;
    envelope.name = message.name;
    envelope.encoding = encoding.map(sockudo_core::message_envelope::EncodingChain);
    envelope.message_id = pusher_message.message_id.clone();
    if let Some(raw_data) = raw_data
        && envelope.encoding.is_some()
        && let Some(raw) = raw_data.as_str()
    {
        envelope.data = Some(MessageContent::Text(raw.to_string()));
    }
    let publish_result = MessageService::new(Arc::clone(handler))
        .publish_message(
            app,
            channel,
            pusher_message,
            PublishContext {
                actor_client_id: client_id.map(str::to_string),
                publisher_connection_id: connection_id.map(str::to_string),
                envelope: Some(envelope),
                ..Default::default()
            },
        )
        .await?;
    #[cfg(feature = "push")]
    if !publish_result.duplicate
        && let (Some(store), Some(queue), Some(push_payload)) = (
            hub.push_store.as_ref(),
            hub.push_queue.as_ref(),
            push_payload,
        )
    {
        let summary = summarize_ably_push_channel_recipients(store, &app.id, channel).await?;
        accept_ably_push_intent(
            store.clone(),
            queue.clone(),
            hub.push_admission.as_deref(),
            AblyPushIntentRequest {
                app_id: app.id.clone(),
                publish_id: format!("message-push:{}", publish_result.receipt.acknowledgement_id),
                targets: vec![PublishTarget::Channel {
                    channel: channel.to_string(),
                }],
                required_providers: summary.providers,
                payload: push_payload,
                expected_recipients: summary.count,
            },
        )
        .await?;
    }
    #[cfg(not(feature = "push"))]
    let _ = hub;
    Ok(publish_result.receipt.acknowledgement_id)
}

pub(super) async fn publish_ably_append(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    actor: MutableMessageActor,
    message: AblyMessage,
) -> Result<String, AppError> {
    let version_serial = message
        .version
        .as_ref()
        .filter(|version| {
            !version.serial.is_empty() && message.serial.as_deref() != Some(version.serial.as_str())
        })
        .map(|version| VersionSerial::new(version.serial.clone()))
        .transpose()?;
    let version_timestamp_ms = message
        .version
        .as_ref()
        .and_then(|version| version.timestamp);
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("appendMessage requires message.serial".to_string())
    })?;
    let data = match message.data.as_ref() {
        Some(value) => value.as_str().ok_or_else(|| {
            AppError::InvalidInput("appendMessage data must be a string".to_string())
        })?,
        None => "",
    }
    .to_string();
    let request = AppendMessageRequest {
        data,
        extras: ably_extras_to_message_extras(message.extras)?,
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_append_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        actor,
        request,
        version_serial,
        version_timestamp_ms,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

pub(super) async fn publish_ably_update(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    actor: MutableMessageActor,
    message: AblyMessage,
) -> Result<String, AppError> {
    let version_serial = message
        .version
        .as_ref()
        .filter(|version| {
            !version.serial.is_empty() && message.serial.as_deref() != Some(version.serial.as_str())
        })
        .map(|version| VersionSerial::new(version.serial.clone()))
        .transpose()?;
    let version_timestamp_ms = message
        .version
        .as_ref()
        .and_then(|version| version.timestamp);
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("updateMessage requires message.serial".to_string())
    })?;
    let request = UpdateMessageRequest {
        name: message.name,
        data: message
            .data
            .map(|value| ably_message_data_to_message_data(value, message.encoding.as_deref())),
        extras: ably_extras_to_message_extras(message.extras)?,
        clear_fields: Vec::new(),
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_update_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        actor,
        request,
        version_serial,
        version_timestamp_ms,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

pub(super) async fn publish_ably_delete(
    handler: &Arc<ConnectionHandler>,
    app: &App,
    channel: &str,
    client_id: Option<&str>,
    actor: MutableMessageActor,
    message: AblyMessage,
) -> Result<String, AppError> {
    let version_serial = message
        .version
        .as_ref()
        .filter(|version| {
            !version.serial.is_empty() && message.serial.as_deref() != Some(version.serial.as_str())
        })
        .map(|version| VersionSerial::new(version.serial.clone()))
        .transpose()?;
    let version_timestamp_ms = message
        .version
        .as_ref()
        .and_then(|version| version.timestamp);
    let serial = message.serial.clone().ok_or_else(|| {
        AppError::InvalidInput("deleteMessage requires message.serial".to_string())
    })?;
    let request = sockudo_protocol::versioned_messages::DeleteMessageRequest {
        data: message
            .data
            .map(|value| ably_message_data_to_message_data(value, message.encoding.as_deref())),
        extras: ably_extras_to_message_extras(message.extras)?,
        clear_fields: Vec::new(),
        client_id: message
            .version
            .as_ref()
            .and_then(|version| version.client_id.clone())
            .or_else(|| client_id.map(str::to_string)),
        socket_id: None,
        description: message
            .version
            .as_ref()
            .and_then(|version| version.description.clone()),
        metadata: message
            .version
            .as_ref()
            .and_then(|version| version.metadata.clone()),
        op_id: message.id,
    };
    let payload = apply_delete_message(
        VersionMutationPath {
            app_id: app.id.clone(),
            channel_name: channel.to_string(),
            message_serial: serial,
        },
        app.clone(),
        Arc::clone(handler),
        actor,
        request,
        version_serial,
        version_timestamp_ms,
    )
    .await?;
    Ok(payload.version_serial.unwrap_or(payload.message_serial))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ably_presence(
    handler: &Arc<ConnectionHandler>,
    hub: &Arc<AblyCompatHub>,
    app: &App,
    connection_id: &str,
    client_id: Option<&str>,
    capabilities: Option<&ConnectionCapabilities>,
    sender: &AblySender,
    replace_presence_on_reenter: bool,
    mut inbound: AblyProtocolMessage,
) {
    let Some(raw_channel) = inbound.channel.clone() else {
        send_publish_nack(sender, &inbound, 40000, "PRESENCE requires channel");
        return;
    };
    let channel = match AblyChannelName::parse(raw_channel) {
        Ok(channel) => channel,
        Err(error) => {
            send_publish_nack(sender, &inbound, 40010, error.to_string());
            return;
        }
    };
    if let Err(error) =
        ensure_ably_channel_capability(capabilities, &channel, AblyCapabilityCheck::Presence)
    {
        send_publish_nack(sender, &inbound, error.code, error.message);
        return;
    }
    let mut presence = inbound.presence.take().unwrap_or_default();
    let msg_serial = inbound.msg_serial.unwrap_or(0);
    for (index, member) in presence.iter_mut().enumerate() {
        member.connection_id = Some(connection_id.to_string());
        if let Some(authenticated_client_id) = client_id {
            if member
                .client_id
                .as_deref()
                .is_some_and(|claimed| claimed != authenticated_client_id)
            {
                send_publish_nack(
                    sender,
                    &inbound,
                    40012,
                    "presence clientId must match authenticated clientId",
                );
                return;
            }
            member.client_id = Some(authenticated_client_id.to_string());
        }
        member.timestamp.get_or_insert_with(now_ms);
        member.id = Some(ably_presence_message_id(
            member.id.as_deref(),
            connection_id,
            msg_serial,
            index,
        ));
    }
    let presence_count = u64::try_from(presence.len()).unwrap_or(u64::MAX);
    let presence_bytes = sonic_rs::to_vec(&presence)
        .map(|bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or_default();
    let service = PresenceService::new(Arc::clone(handler));
    let mut changes = Vec::with_capacity(presence.len());
    for member in presence {
        let action = match member.action.unwrap_or(1) {
            0 | 3 => PresenceOperation::Leave,
            1 | 2 => PresenceOperation::Enter,
            4 => PresenceOperation::Update,
            _ => {
                send_publish_nack(sender, &inbound, 40000, "unsupported presence action");
                return;
            }
        };
        let record = match presence_record_from_ably(&member) {
            Ok(record) => record,
            Err(message) => {
                send_publish_nack(sender, &inbound, 40000, message);
                return;
            }
        };
        if replace_presence_on_reenter && action == PresenceOperation::Enter {
            match service
                .remove_superseded_client(
                    app,
                    channel.base(),
                    &record.client_id,
                    &record.connection_id,
                )
                .await
            {
                Ok(removals) => {
                    changes.extend(removals.into_iter().map(|removal| PresenceChange {
                        action: PresenceChangeAction::Leave,
                        member: removal.member,
                        wire_id: None,
                    }))
                }
                Err(error) => {
                    send_publish_nack(sender, &inbound, 50000, error.to_string());
                    return;
                }
            }
        }
        let cause = if action == PresenceOperation::Leave {
            PresenceHistoryEventCause::Disconnect
        } else {
            PresenceHistoryEventCause::Join
        };
        match service
            .apply(app, channel.base(), action, cause, record.clone())
            .await
        {
            Ok(Some(_)) => changes.push(PresenceChange {
                action: match action {
                    PresenceOperation::Enter => PresenceChangeAction::Enter,
                    PresenceOperation::Update => PresenceChangeAction::Update,
                    PresenceOperation::Leave => PresenceChangeAction::Leave,
                },
                member: record,
                wire_id: member.id,
            }),
            Ok(None) => {}
            Err(error) => {
                send_publish_nack(sender, &inbound, 50000, error.to_string());
                return;
            }
        }
    }
    if !changes.is_empty()
        && let Err(error) = handler
            .fanout_presence(
                &app.id,
                channel.base(),
                PresenceReplication {
                    changes,
                    unregister_connection: None,
                },
            )
            .await
    {
        send_publish_nack(sender, &inbound, 50000, error.to_string());
        return;
    }
    if presence_count > 0 {
        let observation =
            match StatsObservation::presence(&app.id, now_ms(), presence_count, presence_bytes) {
                Ok(observation) => observation,
                Err(error) => {
                    send_publish_nack(sender, &inbound, 50000, error.to_string());
                    return;
                }
            };
        if let Err(error) = hub.stats.record(observation).await {
            send_publish_nack(sender, &inbound, 50000, error.to_string());
            return;
        }
    }
    send_protocol(
        sender,
        AblyProtocolMessage {
            action: ACTION_ACK,
            msg_serial: inbound.msg_serial,
            count: inbound.count.or(Some(1)),
            ..empty_protocol_message(ACTION_ACK)
        },
    );
}
