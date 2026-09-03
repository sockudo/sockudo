//! Typed description of the Sockudo HTTP API routes the MCP surface uses.
//!
//! Keeping the route table typed (instead of passing raw paths around) means
//! the in-process and remote transports share one source of truth and the
//! signing layer can derive `app_id` without parsing strings.

use std::borrow::Cow;
use std::fmt::Write as _;

use http::Method;

/// A Sockudo HTTP API route with its path parameters bound.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Endpoint {
    // --- publish ---
    Events {
        app_id: String,
    },
    BatchEvents {
        app_id: String,
    },
    Revocations {
        app_id: String,
    },
    // --- channel state ---
    Channels {
        app_id: String,
    },
    Channel {
        app_id: String,
        channel: String,
    },
    ChannelUsers {
        app_id: String,
        channel: String,
    },
    TerminateUserConnections {
        app_id: String,
        user_id: String,
    },
    ForceReconnectUser {
        app_id: String,
        user_id: String,
    },
    // --- durable history ---
    History {
        app_id: String,
        channel: String,
    },
    HistoryState {
        app_id: String,
        channel: String,
    },
    HistoryReset {
        app_id: String,
        channel: String,
    },
    HistoryPurge {
        app_id: String,
        channel: String,
    },
    // --- versioned messages ---
    Message {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    MessageVersions {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    UpdateMessage {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    DeleteMessage {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    AppendMessage {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    // --- annotations ---
    ListAnnotations {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    PublishAnnotation {
        app_id: String,
        channel: String,
        message_serial: String,
    },
    DeleteAnnotation {
        app_id: String,
        channel: String,
        message_serial: String,
        annotation_serial: String,
    },
    // --- presence history ---
    PresenceHistory {
        app_id: String,
        channel: String,
    },
    PresenceHistoryState {
        app_id: String,
        channel: String,
    },
    PresenceHistoryReset {
        app_id: String,
        channel: String,
    },
    PresenceHistorySnapshot {
        app_id: String,
        channel: String,
    },
    // --- push ---
    PushPublish {
        app_id: String,
    },
    PushBatchPublish {
        app_id: String,
    },
    PushPublishStatus {
        app_id: String,
        publish_id: String,
    },
    PushDevices {
        app_id: String,
    },
    PushDevice {
        app_id: String,
        device_id: String,
    },
    PushChannelSubscriptions {
        app_id: String,
    },
    PushSubscriptionChannels {
        app_id: String,
    },
    PushDeadLetters {
        app_id: String,
    },
    PushReplayDeadLetter {
        app_id: String,
        dead_letter_id: String,
    },
    PushDeleteScheduledJob {
        app_id: String,
        job_id: String,
    },
    PushCredentials {
        app_id: String,
    },
    PushTemplates {
        app_id: String,
    },
    PushTemplate {
        app_id: String,
        template_id: String,
    },
    // --- unauthenticated operational routes ---
    Up,
    UpApp {
        app_id: String,
    },
    Live,
    AcceptTraffic,
    Usage,
    OperatorStats,
    Stats,
}

impl Endpoint {
    /// HTTP method the route expects.
    pub fn method(&self) -> Method {
        use Endpoint::*;
        match self {
            Events { .. }
            | BatchEvents { .. }
            | Revocations { .. }
            | TerminateUserConnections { .. }
            | ForceReconnectUser { .. }
            | HistoryReset { .. }
            | HistoryPurge { .. }
            | UpdateMessage { .. }
            | DeleteMessage { .. }
            | AppendMessage { .. }
            | PublishAnnotation { .. }
            | PresenceHistoryReset { .. }
            | PushPublish { .. }
            | PushBatchPublish { .. }
            | PushReplayDeadLetter { .. } => Method::POST,
            DeleteAnnotation { .. } | PushDeleteScheduledJob { .. } => Method::DELETE,
            _ => Method::GET,
        }
    }

    /// The app id bound in the path, when the route is app-scoped.
    pub fn app_id(&self) -> Option<&str> {
        use Endpoint::*;
        match self {
            Events { app_id }
            | BatchEvents { app_id }
            | Revocations { app_id }
            | Channels { app_id }
            | Channel { app_id, .. }
            | ChannelUsers { app_id, .. }
            | TerminateUserConnections { app_id, .. }
            | ForceReconnectUser { app_id, .. }
            | History { app_id, .. }
            | HistoryState { app_id, .. }
            | HistoryReset { app_id, .. }
            | HistoryPurge { app_id, .. }
            | Message { app_id, .. }
            | MessageVersions { app_id, .. }
            | UpdateMessage { app_id, .. }
            | DeleteMessage { app_id, .. }
            | AppendMessage { app_id, .. }
            | ListAnnotations { app_id, .. }
            | PublishAnnotation { app_id, .. }
            | DeleteAnnotation { app_id, .. }
            | PresenceHistory { app_id, .. }
            | PresenceHistoryState { app_id, .. }
            | PresenceHistoryReset { app_id, .. }
            | PresenceHistorySnapshot { app_id, .. }
            | PushPublish { app_id }
            | PushBatchPublish { app_id }
            | PushPublishStatus { app_id, .. }
            | PushDevices { app_id }
            | PushDevice { app_id, .. }
            | PushChannelSubscriptions { app_id }
            | PushSubscriptionChannels { app_id }
            | PushDeadLetters { app_id }
            | PushReplayDeadLetter { app_id, .. }
            | PushDeleteScheduledJob { app_id, .. }
            | PushCredentials { app_id }
            | PushTemplates { app_id }
            | PushTemplate { app_id, .. } => Some(app_id),
            UpApp { app_id } => Some(app_id),
            Up | Live | AcceptTraffic | Usage | OperatorStats | Stats => None,
        }
    }

    /// Whether the route sits behind Pusher-style signed authentication.
    pub fn requires_signature(&self) -> bool {
        !matches!(
            self,
            Endpoint::Up
                | Endpoint::UpApp { .. }
                | Endpoint::Live
                | Endpoint::AcceptTraffic
                | Endpoint::Usage
                | Endpoint::OperatorStats
                | Endpoint::Stats
        )
    }

    /// Request path (no query string), with every dynamic segment
    /// percent-encoded exactly as the server will see it. The signature is
    /// computed over this string on both sides, so it must be deterministic.
    pub fn path(&self) -> String {
        use Endpoint::*;
        let mut out = String::with_capacity(96);
        match self {
            Events { app_id } => app(&mut out, app_id, "/events"),
            BatchEvents { app_id } => app(&mut out, app_id, "/batch_events"),
            Revocations { app_id } => app(&mut out, app_id, "/revocations"),
            Channels { app_id } => app(&mut out, app_id, "/channels"),
            Channel { app_id, channel } => channel_path(&mut out, app_id, channel, ""),
            ChannelUsers { app_id, channel } => channel_path(&mut out, app_id, channel, "/users"),
            TerminateUserConnections { app_id, user_id } => {
                app(&mut out, app_id, "/users/");
                push_segment(&mut out, user_id);
                out.push_str("/terminate_connections");
            }
            ForceReconnectUser { app_id, user_id } => {
                app(&mut out, app_id, "/users/");
                push_segment(&mut out, user_id);
                out.push_str("/force_reconnect");
            }
            History { app_id, channel } => channel_path(&mut out, app_id, channel, "/history"),
            HistoryState { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/history/state")
            }
            HistoryReset { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/history/reset")
            }
            HistoryPurge { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/history/purge")
            }
            Message {
                app_id,
                channel,
                message_serial,
            } => message_path(&mut out, app_id, channel, message_serial, ""),
            MessageVersions {
                app_id,
                channel,
                message_serial,
            } => message_path(&mut out, app_id, channel, message_serial, "/versions"),
            UpdateMessage {
                app_id,
                channel,
                message_serial,
            } => message_path(&mut out, app_id, channel, message_serial, "/update"),
            DeleteMessage {
                app_id,
                channel,
                message_serial,
            } => message_path(&mut out, app_id, channel, message_serial, "/delete"),
            AppendMessage {
                app_id,
                channel,
                message_serial,
            } => message_path(&mut out, app_id, channel, message_serial, "/append"),
            ListAnnotations {
                app_id,
                channel,
                message_serial,
            }
            | PublishAnnotation {
                app_id,
                channel,
                message_serial,
            } => message_path(&mut out, app_id, channel, message_serial, "/annotations"),
            DeleteAnnotation {
                app_id,
                channel,
                message_serial,
                annotation_serial,
            } => {
                message_path(&mut out, app_id, channel, message_serial, "/annotations/");
                push_segment(&mut out, annotation_serial);
            }
            PresenceHistory { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/presence/history")
            }
            PresenceHistoryState { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/presence/history/state")
            }
            PresenceHistoryReset { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/presence/history/reset")
            }
            PresenceHistorySnapshot { app_id, channel } => {
                channel_path(&mut out, app_id, channel, "/presence/history/snapshot")
            }
            PushPublish { app_id } => app(&mut out, app_id, "/push/publish"),
            PushBatchPublish { app_id } => app(&mut out, app_id, "/push/batch/publish"),
            PushPublishStatus { app_id, publish_id } => {
                app(&mut out, app_id, "/push/publish/");
                push_segment(&mut out, publish_id);
                out.push_str("/status");
            }
            PushDevices { app_id } => app(&mut out, app_id, "/push/deviceRegistrations"),
            PushDevice { app_id, device_id } => {
                app(&mut out, app_id, "/push/deviceRegistrations/");
                push_segment(&mut out, device_id);
            }
            PushChannelSubscriptions { app_id } => {
                app(&mut out, app_id, "/push/channelSubscriptions")
            }
            PushSubscriptionChannels { app_id } => {
                app(&mut out, app_id, "/push/channelSubscriptions/channels")
            }
            PushDeadLetters { app_id } => app(&mut out, app_id, "/push/deadLetters"),
            PushReplayDeadLetter {
                app_id,
                dead_letter_id,
            } => {
                app(&mut out, app_id, "/push/deadLetters/");
                push_segment(&mut out, dead_letter_id);
                out.push_str("/replay");
            }
            PushDeleteScheduledJob { app_id, job_id } => {
                app(&mut out, app_id, "/push/scheduled/");
                push_segment(&mut out, job_id);
            }
            PushCredentials { app_id } => app(&mut out, app_id, "/push/credentials"),
            PushTemplates { app_id } => app(&mut out, app_id, "/push/templates"),
            PushTemplate {
                app_id,
                template_id,
            } => {
                app(&mut out, app_id, "/push/templates/");
                push_segment(&mut out, template_id);
            }
            Up => out.push_str("/up"),
            UpApp { app_id } => {
                out.push_str("/up/");
                push_segment(&mut out, app_id);
            }
            Live => out.push_str("/live"),
            AcceptTraffic => out.push_str("/accept-traffic"),
            Usage => out.push_str("/usage"),
            OperatorStats => out.push_str("/operator/stats"),
            Stats => out.push_str("/stats"),
        }
        out
    }
}

fn app(out: &mut String, app_id: &str, suffix: &str) {
    out.push_str("/apps/");
    push_segment(out, app_id);
    out.push_str(suffix);
}

fn channel_path(out: &mut String, app_id: &str, channel: &str, suffix: &str) {
    app(out, app_id, "/channels/");
    push_segment(out, channel);
    out.push_str(suffix);
}

fn message_path(out: &mut String, app_id: &str, channel: &str, serial: &str, suffix: &str) {
    channel_path(out, app_id, channel, "/messages/");
    push_segment(out, serial);
    out.push_str(suffix);
}

fn push_segment(out: &mut String, raw: &str) {
    match encode_segment(raw) {
        Cow::Borrowed(s) => out.push_str(s),
        Cow::Owned(s) => out.push_str(&s),
    }
}

/// Percent-encode a single path segment per RFC 3986 `pchar`, minus `/`.
///
/// Pusher channel names (`[A-Za-z0-9_\-=@,.;]`) pass through untouched, so the
/// common path allocates nothing.
pub fn encode_segment(raw: &str) -> Cow<'_, str> {
    const fn is_pchar(b: u8) -> bool {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b':'
                    | b'@'
            )
    }
    if raw.bytes().all(is_pchar) {
        return Cow::Borrowed(raw);
    }
    let mut out = String::with_capacity(raw.len() + 8);
    for byte in raw.bytes() {
        if is_pchar(byte) {
            out.push(byte as char);
        } else {
            // Writing to a String cannot fail.
            let _ = write!(out, "%{byte:02X}");
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_match_server_routes() {
        assert_eq!(
            Endpoint::Events {
                app_id: "app-1".into()
            }
            .path(),
            "/apps/app-1/events"
        );
        assert_eq!(
            Endpoint::ChannelUsers {
                app_id: "app-1".into(),
                channel: "presence-room".into()
            }
            .path(),
            "/apps/app-1/channels/presence-room/users"
        );
        assert_eq!(
            Endpoint::DeleteAnnotation {
                app_id: "a".into(),
                channel: "c".into(),
                message_serial: "01ABC".into(),
                annotation_serial: "x/y".into(),
            }
            .path(),
            "/apps/a/channels/c/messages/01ABC/annotations/x%2Fy"
        );
        assert_eq!(
            Endpoint::PushPublishStatus {
                app_id: "a".into(),
                publish_id: "p 1".into()
            }
            .path(),
            "/apps/a/push/publish/p%201/status"
        );
        assert_eq!(Endpoint::OperatorStats.path(), "/operator/stats");
    }

    #[test]
    fn methods_and_auth_flags() {
        assert_eq!(
            Endpoint::HistoryPurge {
                app_id: "a".into(),
                channel: "c".into()
            }
            .method(),
            Method::POST
        );
        assert_eq!(
            Endpoint::PushDeleteScheduledJob {
                app_id: "a".into(),
                job_id: "j".into()
            }
            .method(),
            Method::DELETE
        );
        assert!(!Endpoint::Live.requires_signature());
        assert!(Endpoint::Channels { app_id: "a".into() }.requires_signature());
        assert_eq!(Endpoint::UpApp { app_id: "a".into() }.app_id(), Some("a"));
    }

    #[test]
    fn segment_encoding_keeps_pusher_channel_charset() {
        assert!(matches!(
            encode_segment("private-ai:run=1,user@x;y.z"),
            Cow::Borrowed(_)
        ));
        assert_eq!(encode_segment("a b/c%"), "a%20b%2Fc%25");
    }
}
