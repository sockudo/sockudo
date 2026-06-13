use super::ConnectionHandler;
use sockudo_core::app::App;
use sockudo_core::history::now_ms;
use sockudo_protocol::messages::PusherMessage;

impl ConnectionHandler {
    pub async fn record_ai_observability(&self, app: &App, channel: &str, message: &PusherMessage) {
        if !self.server_options().ai_transport.matches_channel(channel) {
            return;
        }
        let Some(tracker) = self.ai_observability_tracker.as_ref() else {
            return;
        };

        let update = tracker.observe(&app.id, channel, message, now_ms());
        if update == sockudo_ai_transport::observability::AiObservabilityUpdate::default() {
            return;
        }

        if let Some(metrics) = self.metrics() {
            if update.unparseable {
                metrics.mark_ai_transport_unparseable(&app.id);
            }
            if update.turn_started.is_some() {
                metrics.mark_ai_turn_started(&app.id);
            }
            if let Some(turn_ended) = update.turn_ended.as_ref() {
                metrics.mark_ai_turn_ended(&app.id, turn_ended.reason.as_label());
            }
            if update.cancel_requested.is_some() {
                metrics.mark_ai_cancel_signal(&app.id);
            }
            if let Some(stream) = update.stream.as_ref() {
                metrics.update_ai_active_streams(&app.id, stream.active_streams as u64);
                if let Some(bytes) = stream.bytes {
                    metrics.mark_ai_stream_bytes(&app.id, bytes);
                }
                if let Some(duration) = stream.ended_duration_seconds {
                    metrics.observe_ai_stream_duration(&app.id, duration);
                }
            }
        }

        let Some(webhook_integration) = self.webhook_integration().as_ref().cloned() else {
            return;
        };
        let app = app.clone();
        let channel = channel.to_string();
        tokio::spawn(async move {
            if let Some(turn_started) = update.turn_started.as_ref()
                && let Err(error) = webhook_integration
                    .send_ai_turn_started(
                        &app,
                        &channel,
                        turn_started.turn_id.as_deref(),
                        turn_started.client_id.as_deref(),
                    )
                    .await
            {
                tracing::warn!(error = %error, "failed to emit ai_turn_started webhook");
            }

            if let Some(turn_ended) = update.turn_ended.as_ref()
                && let Err(error) = webhook_integration
                    .send_ai_turn_ended(
                        &app,
                        &channel,
                        turn_ended.turn_id.as_deref(),
                        turn_ended.reason.as_label(),
                        turn_ended.error_code.as_deref(),
                    )
                    .await
            {
                tracing::warn!(error = %error, "failed to emit ai_turn_ended webhook");
            }

            if let Some(cancel_requested) = update.cancel_requested.as_ref()
                && let Err(error) = webhook_integration
                    .send_ai_cancel_requested(
                        &app,
                        &channel,
                        cancel_requested.turn_id.as_deref(),
                        cancel_requested.client_id.as_deref(),
                    )
                    .await
            {
                tracing::warn!(error = %error, "failed to emit ai_cancel_requested webhook");
            }
        });
    }
}
