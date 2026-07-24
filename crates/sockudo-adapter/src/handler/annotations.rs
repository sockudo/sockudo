use crate::handler::ConnectionHandler;
use sockudo_core::annotations::{
    Annotation, AnnotationAction, AnnotationEventLookupRequest, AnnotationEventsRequest,
    AnnotationId, AnnotationProjectionOptions, AnnotationProjectionRequest, AnnotationSerial,
    AnnotationSummary, AnnotationType, StoredAnnotationEvent, StoredAnnotationProjection,
};
use sockudo_core::app::App;
use sockudo_core::error::{Error, Result};
use sockudo_core::history::now_ms;
use sockudo_core::versioned_messages::MessageSerial;
use sockudo_protocol::messages::{
    ANNOTATION_EVENT_NAME, AnnotationEventAction, AnnotationEventData, AnnotationSummaryEnvelope,
    MESSAGE_SUMMARY_EVENT_NAME, MessageData, MessageExtras, MessageSummaryData, PusherMessage,
};
use sonic_rs::Value;
use std::collections::BTreeMap;
use std::time::Instant;

pub struct PublishAnnotationRuntimeRequest {
    pub app: App,
    pub channel: String,
    pub message_serial: MessageSerial,
    pub annotation_type: AnnotationType,
    /// Stable caller-supplied identity used for idempotent edge retries.
    pub id: Option<AnnotationId>,
    pub name: Option<String>,
    pub client_id: Option<String>,
    pub count: Option<u64>,
    pub data: Option<Value>,
    pub encoding: Option<String>,
}

pub struct PublishAnnotationRuntimeResult {
    pub annotation_serial: AnnotationSerial,
    pub projection: StoredAnnotationProjection,
}

pub struct DeleteAnnotationRuntimeRequest {
    pub app: App,
    pub channel: String,
    pub message_serial: MessageSerial,
    pub target_serial: AnnotationSerial,
}

pub struct DeleteAnnotationRuntimeResult {
    pub annotation_serial: AnnotationSerial,
    pub deleted_annotation_serial: AnnotationSerial,
    pub projection: Option<StoredAnnotationProjection>,
}

impl ConnectionHandler {
    async fn reserve_annotation_serial(
        &self,
        app_id: &str,
        channel: &str,
    ) -> Result<AnnotationSerial> {
        if let Some(serial) = self
            .annotation_store()
            .reserve_annotation_serial(app_id, channel)
            .await?
        {
            return Ok(serial);
        }
        AnnotationSerial::new(self.next_version_serial())
    }

    pub async fn publish_annotation_runtime(
        &self,
        request: PublishAnnotationRuntimeRequest,
    ) -> Result<PublishAnnotationRuntimeResult> {
        if self
            .version_store()
            .get_latest(&request.app.id, &request.channel, &request.message_serial)
            .await?
            .is_none()
        {
            return Err(Error::Channel(format!(
                "Message '{}' was not found in channel '{}'",
                request.message_serial.as_str(),
                request.channel
            )));
        }

        if let Some(id) = request.id.as_ref() {
            let existing = self
                .annotation_store()
                .get_events(AnnotationEventsRequest {
                    app_id: request.app.id.clone(),
                    channel_id: request.channel.clone(),
                    message_serial: request.message_serial.clone(),
                    annotation_type: request.annotation_type.clone(),
                })
                .await?;
            if let Some(original) = existing.iter().find(|record| {
                record.annotation.action == AnnotationAction::Create && &record.annotation.id == id
            }) {
                if original.annotation.name != request.name
                    || original.annotation.client_id != request.client_id
                    || original.annotation.count != request.count
                    || original.annotation.data != request.data
                    || original.annotation.encoding != request.encoding
                {
                    return Err(Error::InvalidMessageFormat(format!(
                        "Annotation id '{}' was already used with a different payload",
                        id.as_str()
                    )));
                }
                let projection = self
                    .annotation_store()
                    .rebuild_projection(AnnotationProjectionRequest {
                        app_id: original.app_id.clone(),
                        channel_id: original.channel_id.clone(),
                        message_serial: original.annotation.message_serial.clone(),
                        annotation_type: original.annotation.annotation_type.clone(),
                    })
                    .await?;
                return Ok(PublishAnnotationRuntimeResult {
                    annotation_serial: original.annotation.serial.clone(),
                    projection,
                });
            }
        }

        let serial = self
            .reserve_annotation_serial(&request.app.id, &request.channel)
            .await?;
        let id = match request.id {
            Some(id) => id,
            None => AnnotationId::new(uuid::Uuid::new_v4().to_string())?,
        };
        let annotation = Annotation {
            id,
            action: AnnotationAction::Create,
            serial: serial.clone(),
            message_serial: request.message_serial.clone(),
            annotation_type: request.annotation_type.clone(),
            name: request.name,
            client_id: request.client_id,
            count: request.count,
            data: request.data,
            encoding: request.encoding,
            timestamp: now_ms(),
        };
        annotation.validate()?;

        let append = self
            .annotation_store()
            .append_create_idempotent(StoredAnnotationEvent {
                app_id: request.app.id.clone(),
                channel_id: request.channel.clone(),
                annotation: annotation.clone(),
                stored_at_ms: now_ms(),
            })
            .await?;
        if !append.inserted {
            return Ok(PublishAnnotationRuntimeResult {
                annotation_serial: append.canonical_serial,
                projection: append.projection,
            });
        }
        if let Some(metrics) = self.metrics() {
            metrics.mark_annotation_published(&request.channel, request.annotation_type.as_str());
        }
        let projection = self
            .projection_fitting_payload(&request.app, &request.channel, append.projection)
            .await?;

        self.deliver_annotation_change(
            &request.app,
            &request.channel,
            &annotation,
            &request.message_serial,
            &request.annotation_type,
            &projection.summary,
        )
        .await?;

        Ok(PublishAnnotationRuntimeResult {
            annotation_serial: serial,
            projection,
        })
    }

    pub async fn delete_annotation_runtime(
        &self,
        request: DeleteAnnotationRuntimeRequest,
    ) -> Result<DeleteAnnotationRuntimeResult> {
        let target = self
            .annotation_store()
            .get_event_by_serial(AnnotationEventLookupRequest {
                app_id: request.app.id.clone(),
                channel_id: request.channel.clone(),
                annotation_serial: request.target_serial.clone(),
            })
            .await?
            .ok_or_else(|| {
                Error::Channel(format!(
                    "Annotation '{}' was not found in channel '{}'",
                    request.target_serial.as_str(),
                    request.channel
                ))
            })?;

        if target.message_serial() != &request.message_serial {
            return Err(Error::Channel(format!(
                "Annotation '{}' does not target message '{}'",
                request.target_serial.as_str(),
                request.message_serial.as_str()
            )));
        }
        if target.annotation.action != AnnotationAction::Create {
            return Err(Error::InvalidMessageFormat(
                "Only annotation.create events can be deleted".to_string(),
            ));
        }

        let existing = self
            .annotation_store()
            .get_events(AnnotationEventsRequest {
                app_id: request.app.id.clone(),
                channel_id: request.channel.clone(),
                message_serial: request.message_serial.clone(),
                annotation_type: target.annotation.annotation_type.clone(),
            })
            .await?;
        if existing.iter().any(|record| {
            record.annotation.action == AnnotationAction::Delete
                && record.annotation.id == target.annotation.id
        }) {
            return Ok(DeleteAnnotationRuntimeResult {
                annotation_serial: request.target_serial.clone(),
                deleted_annotation_serial: request.target_serial,
                projection: None,
            });
        }

        let delete_serial = self
            .reserve_annotation_serial(&request.app.id, &request.channel)
            .await?;
        let annotation = Annotation {
            id: target.annotation.id.clone(),
            action: AnnotationAction::Delete,
            serial: delete_serial.clone(),
            message_serial: request.message_serial.clone(),
            annotation_type: target.annotation.annotation_type.clone(),
            name: target.annotation.name.clone(),
            client_id: target.annotation.client_id.clone(),
            count: target.annotation.count,
            data: None,
            encoding: None,
            timestamp: now_ms(),
        };
        annotation.validate()?;

        let projection = self
            .annotation_store()
            .append_event(StoredAnnotationEvent {
                app_id: request.app.id.clone(),
                channel_id: request.channel.clone(),
                annotation: annotation.clone(),
                stored_at_ms: now_ms(),
            })
            .await?;
        if let Some(metrics) = self.metrics() {
            metrics.mark_annotation_deleted(
                &request.channel,
                target.annotation.annotation_type.as_str(),
            );
        }
        let projection = self
            .projection_fitting_payload(&request.app, &request.channel, projection)
            .await?;

        self.deliver_annotation_change(
            &request.app,
            &request.channel,
            &annotation,
            &request.message_serial,
            &target.annotation.annotation_type,
            &projection.summary,
        )
        .await?;

        Ok(DeleteAnnotationRuntimeResult {
            annotation_serial: delete_serial,
            deleted_annotation_serial: request.target_serial,
            projection: Some(projection),
        })
    }

    async fn projection_fitting_payload(
        &self,
        app: &App,
        channel: &str,
        projection: StoredAnnotationProjection,
    ) -> Result<StoredAnnotationProjection> {
        let max_payload = app
            .event_payload_limit_kb()
            .map(|kb| kb as usize * 1024)
            .unwrap_or(self.server_options().websocket_max_payload_kb as usize * 1024);
        if max_payload == 0 {
            return Ok(projection);
        }

        let mut limit = None;
        let mut candidate = projection;
        loop {
            let message = annotation_summary_message(
                channel,
                &candidate.message_serial,
                &candidate.annotation_type,
                &candidate.summary,
            )?;
            if sonic_rs::to_vec(&message)?.len() <= max_payload {
                return Ok(candidate);
            }

            limit = Some(match limit {
                None => 64,
                Some(0) => return Ok(candidate),
                Some(previous) => previous / 2,
            });

            let projection_key = candidate.projection_key();
            let annotation_count = self
                .annotation_store()
                .get_events(AnnotationEventsRequest {
                    app_id: app.id.clone(),
                    channel_id: channel.to_string(),
                    message_serial: projection_key.message_serial.clone(),
                    annotation_type: projection_key.annotation_type.clone(),
                })
                .await
                .map(|events| events.len())
                .unwrap_or(0);

            tracing::warn!(
                channel = %channel,
                message_serial = %projection_key.message_serial.as_str(),
                annotation_type = %projection_key.annotation_type.as_str(),
                annotation_count,
                client_id_limit = limit.unwrap_or_default(),
                "annotation projection rebuild triggered on hot channel"
            );

            if let Some(metrics) = self.metrics() {
                metrics.mark_annotation_projection_rebuild(channel);
            }
            let rebuild_started = Instant::now();
            let rebuilt = self
                .annotation_store()
                .rebuild_projection_with_options(
                    projection_key,
                    AnnotationProjectionOptions {
                        client_id_limit: limit,
                    },
                )
                .await;
            if let Some(metrics) = self.metrics() {
                metrics.track_annotation_projection_rebuild_duration(
                    channel,
                    rebuild_started.elapsed().as_secs_f64(),
                );
            }
            candidate = rebuilt?;
        }
    }

    fn observe_clipped_summary(
        &self,
        channel: &str,
        message_serial: &MessageSerial,
        annotation_type: &AnnotationType,
        summary: &AnnotationSummary,
    ) {
        let Some(contributor_count) = clipped_contributor_count(summary) else {
            return;
        };

        tracing::warn!(
            channel = %channel,
            message_serial = %message_serial.as_str(),
            annotation_type = %annotation_type.as_str(),
            contributor_count,
            "annotation summary clipped"
        );

        if let Some(metrics) = self.metrics() {
            metrics.mark_annotation_summary_clipped(channel, annotation_type.as_str());
        }
    }

    async fn deliver_annotation_change(
        &self,
        app: &App,
        channel: &str,
        annotation: &Annotation,
        message_serial: &MessageSerial,
        annotation_type: &AnnotationType,
        summary: &AnnotationSummary,
    ) -> Result<()> {
        self.observe_clipped_summary(channel, message_serial, annotation_type, summary);

        let summary_message =
            annotation_summary_message(channel, message_serial, annotation_type, summary)?;
        if let Err(err) = self
            .broadcast_to_channel_force_full(app, channel, summary_message, None, None)
            .await
        {
            tracing::error!(
                channel = %channel,
                message_serial = %message_serial.as_str(),
                annotation_type = %annotation_type.as_str(),
                error = %err,
                "annotation summary failed to fan out through cluster broadcast"
            );
            return Err(err);
        }
        if let Some(metrics) = self.metrics() {
            metrics.mark_annotation_summary_delivery(channel);
        }

        let raw_message = annotation_event_message(channel, annotation)?;
        if let Err(err) = self
            .broadcast_to_channel_force_full(app, channel, raw_message, None, None)
            .await
        {
            tracing::error!(
                channel = %channel,
                message_serial = %message_serial.as_str(),
                annotation_type = %annotation_type.as_str(),
                annotation_serial = %annotation.serial.as_str(),
                error = %err,
                "annotation event failed to fan out through cluster broadcast"
            );
            return Err(err);
        }

        Ok(())
    }
}

pub(crate) fn clipped_contributor_count(summary: &AnnotationSummary) -> Option<u64> {
    match summary {
        AnnotationSummary::Total(_) => None,
        AnnotationSummary::Flag(summary) => summary.clipped.then_some(summary.total),
        AnnotationSummary::Distinct(names) | AnnotationSummary::Unique(names) => {
            let total = names
                .values()
                .filter(|summary| summary.clipped)
                .map(|summary| summary.total)
                .sum::<u64>();
            (total > 0).then_some(total)
        }
        AnnotationSummary::Multiple(names) => {
            let total = names
                .values()
                .filter(|summary| summary.clipped)
                .map(|summary| summary.total_client_ids)
                .sum::<u64>();
            (total > 0).then_some(total)
        }
    }
}

fn annotation_wire_event(annotation: &Annotation) -> AnnotationEventData {
    AnnotationEventData {
        action: match annotation.action {
            AnnotationAction::Create => AnnotationEventAction::Create,
            AnnotationAction::Delete => AnnotationEventAction::Delete,
        },
        id: matches!(annotation.action, AnnotationAction::Create)
            .then(|| annotation.id.as_str().to_string()),
        serial: annotation.serial.as_str().to_string(),
        message_serial: annotation.message_serial.as_str().to_string(),
        annotation_type: annotation.annotation_type.as_str().to_string(),
        name: annotation.name.clone(),
        client_id: annotation.client_id.clone(),
        count: annotation.count,
        data: annotation.data.clone(),
        encoding: annotation.encoding.clone(),
        timestamp: annotation.timestamp,
    }
}

fn annotation_event_message(channel: &str, annotation: &Annotation) -> Result<PusherMessage> {
    Ok(PusherMessage {
        event: Some(ANNOTATION_EVENT_NAME.to_string()),
        channel: Some(channel.to_string()),
        data: Some(MessageData::Json(sonic_rs::to_value(
            &annotation_wire_event(annotation),
        )?)),
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
    })
}

fn annotation_summary_message(
    channel: &str,
    message_serial: &MessageSerial,
    annotation_type: &AnnotationType,
    summary: &AnnotationSummary,
) -> Result<PusherMessage> {
    let mut summary_by_type = BTreeMap::new();
    summary_by_type.insert(
        annotation_type.as_str().to_string(),
        sonic_rs::to_value(summary)?,
    );

    Ok(PusherMessage {
        event: Some(MESSAGE_SUMMARY_EVENT_NAME.to_string()),
        channel: Some(channel.to_string()),
        data: Some(MessageData::Json(sonic_rs::to_value(
            &MessageSummaryData {
                action: "message.summary".to_string(),
                serial: message_serial.as_str().to_string(),
                annotations: AnnotationSummaryEnvelope {
                    summary: summary_by_type,
                },
            },
        )?)),
        name: None,
        user_id: None,
        tags: None,
        sequence: None,
        conflation_key: None,
        message_id: None,
        stream_id: None,
        serial: None,
        idempotency_key: None,
        extras: Some(MessageExtras {
            ephemeral: Some(true),
            ..Default::default()
        }),
        delta_sequence: None,
        delta_conflation_key: None,
    })
}
