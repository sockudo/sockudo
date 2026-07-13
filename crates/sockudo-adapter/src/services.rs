//! Typed protocol-independent operations shared by HTTP projections.

use crate::ConnectionHandler;
use serde::Serialize;
use sockudo_core::{
    app::App,
    error::{Error, Result},
    history::{HistoryDirection, HistoryPage, HistoryQueryBounds, HistoryReadRequest},
    idempotency::{
        IdempotencyReceipt, IdempotencyStart, abort_publish, begin_publish, commit_publish,
        commit_recovered_publish, publish_fingerprint, publish_idempotency_cache_key,
    },
    message_envelope::{
        MessageEnvelope, PublishIdempotencyMetadata, decode_stored_message_payload,
    },
    version_store::{StoredVersionRecord, VersionStorePage, VersionStoreReadRequest},
    versioned_messages::MessageSerial,
    websocket::SocketId,
};
use sockudo_protocol::messages::PusherMessage;
use std::sync::Arc;

const IDEMPOTENCY_RECOVERY_PAGE_SIZE: usize = 64;
const IDEMPOTENCY_RECOVERY_MAX_PAGES: usize = 16;

/// Actor and delivery metadata for a publish operation.
///
/// Actor identity is deliberately distinct from `exclude_socket`; the latter
/// controls fanout only and must never be used for authorization.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PublishContext {
    pub actor_client_id: Option<String>,
    pub publisher_socket_id: Option<SocketId>,
    pub publisher_connection_id: Option<String>,
    pub exclude_socket: Option<SocketId>,
    pub idempotency_key: Option<String>,
    /// Optional commit-time envelope supplied by a protocol edge that has
    /// facts not represented by `PusherMessage`, such as an Ably encoding chain.
    pub envelope: Option<MessageEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishResult {
    pub receipt: IdempotencyReceipt,
    pub duplicate: bool,
}

#[derive(Serialize)]
struct PublishFingerprint<'a> {
    channel: &'a str,
    message: &'a PusherMessage,
    envelope: &'a MessageEnvelope,
}

fn canonical_publish_fingerprint(
    channel: &str,
    message: &PusherMessage,
    envelope: &MessageEnvelope,
) -> Result<String> {
    let mut canonical_message = message.clone();
    canonical_message.stream_id = None;
    canonical_message.serial = None;
    canonical_message.idempotency_key = None;

    let mut canonical_envelope = envelope.clone();
    canonical_envelope.acknowledgement_id = None;
    canonical_envelope.published_at_ms = None;
    // The origin transport is not payload identity: a REST retry of a realtime
    // publish (or the inverse) must observe the same committed receipt.
    canonical_envelope.publisher_socket_id = None;
    canonical_envelope.publisher_connection_id = None;
    canonical_envelope.stream_id = None;
    canonical_envelope.history_serial = None;
    canonical_envelope.delivery_serial = None;
    canonical_envelope.action = None;
    canonical_envelope.message_serial = None;
    canonical_envelope.version = None;
    canonical_envelope.idempotency = None;

    sonic_rs::to_vec(&PublishFingerprint {
        channel,
        message: &canonical_message,
        envelope: &canonical_envelope,
    })
    .map(|bytes| publish_fingerprint(&bytes))
    .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sockudo_protocol::messages::MessageData;

    fn test_message(user_id: &str) -> PusherMessage {
        PusherMessage {
            event: Some("event".to_string()),
            channel: Some("channel".to_string()),
            data: Some(MessageData::String("payload".to_string())),
            name: None,
            user_id: Some(user_id.to_string()),
            tags: None,
            sequence: None,
            conflation_key: None,
            message_id: Some("stable-id".to_string()),
            stream_id: None,
            serial: None,
            idempotency_key: None,
            extras: None,
            delta_sequence: None,
            delta_conflation_key: None,
        }
    }

    #[test]
    fn fingerprint_is_stable_across_transport_origin_and_commit_metadata() {
        let message = test_message("publisher");
        let mut realtime = MessageEnvelope::from_message(
            &message,
            Some("socket-id".to_string()),
            Some("connection-id".to_string()),
            100,
        )
        .unwrap();
        realtime.acknowledgement_id = Some("ack-one".to_string());
        realtime.history_serial = Some(9);

        let mut rest = MessageEnvelope::from_message(&message, None, None, 200).unwrap();
        rest.acknowledgement_id = Some("ack-two".to_string());
        rest.history_serial = Some(10);

        assert_eq!(
            canonical_publish_fingerprint("channel", &message, &realtime).unwrap(),
            canonical_publish_fingerprint("channel", &message, &rest).unwrap()
        );
    }

    #[test]
    fn fingerprint_changes_with_publisher_identity_or_payload() {
        let message = test_message("publisher-a");
        let envelope = MessageEnvelope::from_message(&message, None, None, 100).unwrap();
        let first = canonical_publish_fingerprint("channel", &message, &envelope).unwrap();

        let mut changed = message.clone();
        changed.user_id = Some("publisher-b".to_string());
        let changed_envelope = MessageEnvelope::from_message(&changed, None, None, 100).unwrap();
        assert_ne!(
            first,
            canonical_publish_fingerprint("channel", &changed, &changed_envelope).unwrap()
        );
    }
}

fn publish_receipt(
    acknowledgement_id: String,
    ack: Option<crate::handler::connection_management::PublishAck>,
) -> IdempotencyReceipt {
    let stable_acknowledgement_id = ack
        .as_ref()
        .map(|value| value.message_serial.clone())
        .unwrap_or(acknowledgement_id);
    IdempotencyReceipt {
        acknowledgement_id: stable_acknowledgement_id,
        message_serial: ack.as_ref().map(|value| value.message_serial.clone()),
        history_serial: ack.as_ref().map(|value| value.history_serial),
        delivery_serial: ack.as_ref().map(|value| value.delivery_serial),
        version_serial: ack.map(|value| value.version_serial),
    }
}

#[derive(Debug, Clone)]
struct PublishRecoveryIdentity {
    cache_key: String,
    fingerprint: String,
    ttl_seconds: u64,
}

fn receipt_from_history_item(
    item: &sockudo_core::history::HistoryItem,
) -> Result<Option<IdempotencyReceipt>> {
    let payload =
        decode_stored_message_payload(&item.payload_bytes).map_err(Error::InvalidMessageFormat)?;
    let Some(envelope) = payload.envelope else {
        return Ok(None);
    };
    let acknowledgement_id = envelope
        .acknowledgement_id
        .clone()
        .or_else(|| {
            envelope
                .message_serial
                .as_ref()
                .map(|serial| serial.as_str().to_string())
        })
        .or(payload.message.message_id)
        .ok_or_else(|| {
            Error::Internal(
                "durable idempotent publish is missing an acknowledgement identity".to_string(),
            )
        })?;
    let is_versioned = envelope.message_serial.is_some();
    Ok(Some(IdempotencyReceipt {
        acknowledgement_id,
        message_serial: envelope
            .message_serial
            .as_ref()
            .map(|serial| serial.as_str().to_string()),
        history_serial: is_versioned.then(|| envelope.history_serial.unwrap_or(item.serial)),
        delivery_serial: is_versioned
            .then(|| envelope.delivery_serial.or(payload.message.serial))
            .flatten(),
        version_serial: is_versioned
            .then(|| {
                envelope
                    .version
                    .as_ref()
                    .map(|version| version.serial.as_str().to_string())
            })
            .flatten(),
    }))
}

/// Native publish/read service façade used by protocol projections.
#[derive(Clone)]
pub struct MessageService {
    handler: Arc<ConnectionHandler>,
}

impl MessageService {
    #[must_use]
    pub fn new(handler: Arc<ConnectionHandler>) -> Self {
        Self { handler }
    }

    async fn find_durable_publish_receipt(
        &self,
        app_id: &str,
        channel: &str,
        recovery: &PublishRecoveryIdentity,
    ) -> Result<Option<IdempotencyReceipt>> {
        let mut cursor = None;
        for _ in 0..IDEMPOTENCY_RECOVERY_MAX_PAGES {
            let page = match self
                .handler
                .history_store()
                .read_page(HistoryReadRequest {
                    app_id: app_id.to_string(),
                    channel: channel.to_string(),
                    direction: HistoryDirection::NewestFirst,
                    limit: IDEMPOTENCY_RECOVERY_PAGE_SIZE,
                    cursor,
                    bounds: HistoryQueryBounds::default(),
                })
                .await
            {
                Ok(page) => page,
                Err(Error::Configuration(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            for item in &page.items {
                let payload = decode_stored_message_payload(&item.payload_bytes)
                    .map_err(Error::InvalidMessageFormat)?;
                let matches = payload
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.idempotency.as_ref())
                    .is_some_and(|metadata| {
                        metadata.cache_key == recovery.cache_key
                            && metadata.payload_fingerprint == recovery.fingerprint
                    });
                if matches {
                    return receipt_from_history_item(item);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                return Ok(None);
            };
            cursor = Some(next_cursor);
        }

        tracing::warn!(
            app_id,
            channel,
            idempotency_cache_key = %recovery.cache_key,
            "bounded durable idempotency recovery scan reached its page limit"
        );
        Ok(None)
    }

    async fn recover_durable_publish(
        &self,
        app_id: &str,
        channel: &str,
        recovery: &PublishRecoveryIdentity,
    ) -> Result<Option<IdempotencyReceipt>> {
        let Some(receipt) = self
            .find_durable_publish_receipt(app_id, channel, recovery)
            .await?
        else {
            return Ok(None);
        };
        let committed = commit_recovered_publish(
            self.handler.cache_manager().as_ref(),
            &recovery.cache_key,
            &recovery.fingerprint,
            &receipt,
            recovery.ttl_seconds,
        )
        .await?;
        tracing::info!(
            app_id,
            channel,
            idempotency_cache_key = %recovery.cache_key,
            history_serial = ?committed.history_serial,
            delivery_serial = ?committed.delivery_serial,
            "recovered publish receipt from durable history"
        );
        Ok(Some(committed))
    }

    /// Publish through the native durable/fanout pipeline.
    pub async fn publish_message(
        &self,
        app: &App,
        channel: &str,
        message: PusherMessage,
        context: PublishContext,
    ) -> Result<PublishResult> {
        self.publish_message_with_timing(app, channel, message, context, None, true)
            .await
    }

    pub async fn publish_message_with_timing(
        &self,
        app: &App,
        channel: &str,
        mut message: PusherMessage,
        context: PublishContext,
        timestamp_ms: Option<f64>,
        force_full: bool,
    ) -> Result<PublishResult> {
        if let Some(actor_client_id) = context.actor_client_id.as_ref() {
            if message
                .user_id
                .as_ref()
                .is_some_and(|claimed| claimed != actor_client_id)
            {
                return Err(Error::Auth(
                    "publisher client identity does not match the authenticated actor".to_string(),
                ));
            }
            message.user_id = Some(actor_client_id.clone());
        }
        if message.idempotency_key.is_none() {
            message.idempotency_key.clone_from(&context.idempotency_key);
        }

        let published_at_ms = timestamp_ms
            .map(|value| value.round() as i64)
            .unwrap_or_else(sockudo_core::history::now_ms);
        let mut envelope = match context.envelope {
            Some(envelope) => envelope,
            None => MessageEnvelope::from_message(
                &message,
                context.publisher_socket_id.map(|socket| socket.to_string()),
                context.publisher_connection_id,
                published_at_ms,
            )
            .map_err(Error::InvalidMessageFormat)?,
        };
        let acknowledgement_id = envelope
            .acknowledgement_id
            .clone()
            .or_else(|| message.message_id.clone())
            .unwrap_or_else(sockudo_protocol::messages::generate_message_id);
        envelope.acknowledgement_id = Some(acknowledgement_id.clone());

        let idempotency_key = message
            .message_id
            .as_deref()
            .or_else(|| message.extras_idempotency_key())
            .or(message.idempotency_key.as_deref());
        let idempotency_config =
            app.resolved_idempotency(&self.handler.server_options().idempotency);
        let mut recovery_identity = None;
        let claim = if idempotency_config.enabled {
            if let Some(key) = idempotency_key {
                if key.is_empty() {
                    return Err(Error::InvalidMessageFormat(
                        "idempotency key must not be empty".to_string(),
                    ));
                }
                if key.len() > idempotency_config.max_key_length {
                    return Err(Error::InvalidMessageFormat(format!(
                        "idempotency key exceeds {} bytes",
                        idempotency_config.max_key_length
                    )));
                }
                if let Some(metrics) = self.handler.metrics() {
                    metrics.mark_idempotency_publish(&app.id);
                }
                let fingerprint = canonical_publish_fingerprint(channel, &message, &envelope)?;
                let cache_key = publish_idempotency_cache_key(&app.id, channel, key);
                let recovery = PublishRecoveryIdentity {
                    cache_key: cache_key.clone(),
                    fingerprint: fingerprint.clone(),
                    ttl_seconds: idempotency_config.ttl_seconds,
                };
                tracing::debug!(
                    app_id = %app.id,
                    channel,
                    idempotency_cache_key = %cache_key,
                    fingerprint = %fingerprint,
                    "Starting idempotent publish"
                );
                match begin_publish(
                    self.handler.cache_manager().as_ref(),
                    cache_key,
                    fingerprint,
                    idempotency_config.ttl_seconds,
                )
                .await
                {
                    Ok(IdempotencyStart::Acquired(claim)) => {
                        envelope.idempotency = Some(PublishIdempotencyMetadata {
                            cache_key: recovery.cache_key.clone(),
                            payload_fingerprint: recovery.fingerprint.clone(),
                        });
                        recovery_identity = Some(recovery);
                        Some(claim)
                    }
                    Ok(IdempotencyStart::Replay(receipt)) => {
                        if let Some(metrics) = self.handler.metrics() {
                            metrics.mark_idempotency_duplicate(&app.id);
                        }
                        return Ok(PublishResult {
                            receipt,
                            duplicate: true,
                        });
                    }
                    Err(Error::IdempotencyInProgress) => {
                        if let Some(receipt) = self
                            .recover_durable_publish(&app.id, channel, &recovery)
                            .await?
                        {
                            if let Some(metrics) = self.handler.metrics() {
                                metrics.mark_idempotency_duplicate(&app.id);
                            }
                            return Ok(PublishResult {
                                receipt,
                                duplicate: true,
                            });
                        }
                        return Err(Error::IdempotencyInProgress);
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            }
        } else {
            None
        };

        let result = self
            .handler
            .publish_to_channel_with_context(
                app,
                channel,
                message,
                context.publisher_socket_id.as_ref(),
                context.exclude_socket.as_ref(),
                timestamp_ms,
                force_full,
                Some(envelope),
            )
            .await;
        let ack = match result {
            Ok(ack) => ack,
            Err(error) => {
                if let Some(claim) = claim.as_ref() {
                    let _ = abort_publish(self.handler.cache_manager().as_ref(), claim).await;
                }
                return Err(error);
            }
        };
        let receipt = publish_receipt(acknowledgement_id, ack);
        if let Some(claim) = claim.as_ref()
            && let Err(error) =
                commit_publish(self.handler.cache_manager().as_ref(), claim, &receipt).await
        {
            if matches!(error, Error::IdempotencyInProgress)
                && let Some(recovery) = recovery_identity.as_ref()
                && let Some(recovered) = self
                    .recover_durable_publish(&app.id, channel, recovery)
                    .await?
            {
                return Ok(PublishResult {
                    receipt: recovered,
                    duplicate: false,
                });
            }
            return Err(error);
        }
        Ok(PublishResult {
            receipt,
            duplicate: false,
        })
    }

    pub async fn get_message(
        &self,
        app_id: &str,
        channel: &str,
        message_serial: &MessageSerial,
    ) -> Result<Option<StoredVersionRecord>> {
        self.handler
            .version_store()
            .get_latest(app_id, channel, message_serial)
            .await
    }

    pub async fn get_message_versions(
        &self,
        request: VersionStoreReadRequest,
    ) -> Result<VersionStorePage> {
        self.handler.version_store().get_versions(request).await
    }

    pub async fn read_history(&self, request: HistoryReadRequest) -> Result<HistoryPage> {
        self.handler.history_store().read_page(request).await
    }

    #[must_use]
    pub fn handler(&self) -> &Arc<ConnectionHandler> {
        &self.handler
    }
}
