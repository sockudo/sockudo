use serde::{Deserialize, Serialize};

/// Protocol version negotiated per-connection.
///
/// - V1 (default): Pusher-compatible. Uses `pusher:` / `pusher_internal:` prefixes.
///   Features like serial, message_id, and recovery are opt-in via server config.
/// - V2: Sockudo-native. Uses `sockudo:` / `sockudo_internal:` prefixes.
///   Serial, message_id, and connection recovery are always enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[repr(u8)]
pub enum ProtocolVersion {
    #[default]
    V1 = 1,
    V2 = 2,
}

impl ProtocolVersion {
    /// Parse from the `?protocol=` query parameter value.
    /// Returns V1 for unrecognized values.
    pub fn from_query_param(value: Option<u8>) -> Self {
        match value {
            Some(2) => Self::V2,
            _ => Self::V1,
        }
    }

    /// Event prefix for this protocol version (e.g. `"pusher:"` or `"sockudo:"`).
    #[inline]
    pub const fn event_prefix(&self) -> &'static str {
        match self {
            Self::V1 => "pusher:",
            Self::V2 => "sockudo:",
        }
    }

    /// Internal event prefix (e.g. `"pusher_internal:"` or `"sockudo_internal:"`).
    #[inline]
    pub const fn internal_prefix(&self) -> &'static str {
        match self {
            Self::V1 => "pusher_internal:",
            Self::V2 => "sockudo_internal:",
        }
    }

    /// Whether serial numbers are native (always-on) for this protocol version.
    #[inline]
    pub const fn is_serial_native(&self) -> bool {
        matches!(self, Self::V2)
    }

    /// Whether message_id is native (always-on) for this protocol version.
    #[inline]
    pub const fn is_message_id_native(&self) -> bool {
        matches!(self, Self::V2)
    }

    /// Whether connection recovery is native (always-on) for this protocol version.
    #[inline]
    pub const fn is_recovery_native(&self) -> bool {
        matches!(self, Self::V2)
    }

    /// Build a wire-format event name from a canonical name.
    /// E.g. `"subscribe"` → `"pusher:subscribe"` or `"sockudo:subscribe"`.
    #[inline]
    pub fn wire_event(&self, canonical: &str) -> String {
        format!("{}{}", self.event_prefix(), canonical)
    }

    /// Build a wire-format internal event name from a canonical name.
    /// E.g. `"subscription_succeeded"` → `"pusher_internal:subscription_succeeded"`.
    #[inline]
    pub fn wire_internal_event(&self, canonical: &str) -> String {
        format!("{}{}", self.internal_prefix(), canonical)
    }

    /// Strip the protocol prefix from a wire-format event name, returning the canonical name.
    /// Returns `None` if the event doesn't match this version's prefix.
    pub fn parse_event_name<'a>(&self, wire: &'a str) -> Option<&'a str> {
        wire.strip_prefix(self.event_prefix())
            .or_else(|| wire.strip_prefix(self.internal_prefix()))
    }

    /// Determine if a wire event name belongs to ANY known protocol version,
    /// returning the canonical name and whether it's internal.
    pub fn parse_any_protocol_event(wire: &str) -> Option<(&str, bool)> {
        if let Some(canonical) = wire.strip_prefix("pusher:") {
            Some((canonical, false))
        } else if let Some(canonical) = wire.strip_prefix("pusher_internal:") {
            Some((canonical, true))
        } else if let Some(canonical) = wire.strip_prefix("sockudo:") {
            Some((canonical, false))
        } else if let Some(canonical) = wire.strip_prefix("sockudo_internal:") {
            Some((canonical, true))
        } else {
            None
        }
    }

    /// Rewrite a `pusher:` or `pusher_internal:` event name to this version's prefix.
    /// If the event doesn't have a known prefix, returns it unchanged.
    pub fn rewrite_event_prefix(&self, event: &str) -> String {
        if let Some((canonical, is_internal)) = Self::parse_any_protocol_event(event) {
            if is_internal {
                self.wire_internal_event(canonical)
            } else {
                self.wire_event(canonical)
            }
        } else {
            event.to_string()
        }
    }
}

// Canonical event names (without prefix)
pub const CANONICAL_CONNECTION_ESTABLISHED: &str = "connection_established";
pub const CANONICAL_ERROR: &str = "error";
pub const CANONICAL_PING: &str = "ping";
pub const CANONICAL_PONG: &str = "pong";
pub const CANONICAL_SUBSCRIBE: &str = "subscribe";
pub const CANONICAL_UNSUBSCRIBE: &str = "unsubscribe";
pub const CANONICAL_SIGNIN: &str = "signin";
pub const CANONICAL_SIGNIN_SUCCESS: &str = "signin_success";
pub const CANONICAL_CACHE_MISS: &str = "cache_miss";

// Canonical internal event names (without prefix)
pub const CANONICAL_SUBSCRIPTION_SUCCEEDED: &str = "subscription_succeeded";
pub const CANONICAL_SUBSCRIPTION_ERROR: &str = "subscription_error";
pub const CANONICAL_MEMBER_ADDED: &str = "member_added";
pub const CANONICAL_MEMBER_REMOVED: &str = "member_removed";

// Delta compression canonical names
pub const CANONICAL_ENABLE_DELTA_COMPRESSION: &str = "enable_delta_compression";
pub const CANONICAL_DELTA_COMPRESSION_ENABLED: &str = "delta_compression_enabled";
pub const CANONICAL_DELTA: &str = "delta";
pub const CANONICAL_DELTA_CACHE_SYNC: &str = "delta_cache_sync";
pub const CANONICAL_DELTA_SYNC_ERROR: &str = "delta_sync_error";

// Connection recovery canonical names
pub const CANONICAL_RESUME: &str = "resume";
pub const CANONICAL_RESUME_SUCCESS: &str = "resume_success";
pub const CANONICAL_RESUME_FAILED: &str = "resume_failed";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v1_prefix() {
        assert_eq!(ProtocolVersion::V1.event_prefix(), "pusher:");
        assert_eq!(ProtocolVersion::V1.internal_prefix(), "pusher_internal:");
    }

    #[test]
    fn test_v2_prefix() {
        assert_eq!(ProtocolVersion::V2.event_prefix(), "sockudo:");
        assert_eq!(ProtocolVersion::V2.internal_prefix(), "sockudo_internal:");
    }

    #[test]
    fn test_wire_event() {
        assert_eq!(
            ProtocolVersion::V1.wire_event("subscribe"),
            "pusher:subscribe"
        );
        assert_eq!(
            ProtocolVersion::V2.wire_event("subscribe"),
            "sockudo:subscribe"
        );
    }

    #[test]
    fn test_wire_internal_event() {
        assert_eq!(
            ProtocolVersion::V1.wire_internal_event("subscription_succeeded"),
            "pusher_internal:subscription_succeeded"
        );
        assert_eq!(
            ProtocolVersion::V2.wire_internal_event("subscription_succeeded"),
            "sockudo_internal:subscription_succeeded"
        );
    }

    #[test]
    fn test_parse_event_name() {
        assert_eq!(
            ProtocolVersion::V1.parse_event_name("pusher:ping"),
            Some("ping")
        );
        assert_eq!(ProtocolVersion::V1.parse_event_name("sockudo:ping"), None);
        assert_eq!(
            ProtocolVersion::V2.parse_event_name("sockudo:ping"),
            Some("ping")
        );
        assert_eq!(ProtocolVersion::V2.parse_event_name("pusher:ping"), None);
    }

    #[test]
    fn test_parse_any_protocol_event() {
        assert_eq!(
            ProtocolVersion::parse_any_protocol_event("pusher:subscribe"),
            Some(("subscribe", false))
        );
        assert_eq!(
            ProtocolVersion::parse_any_protocol_event("sockudo:subscribe"),
            Some(("subscribe", false))
        );
        assert_eq!(
            ProtocolVersion::parse_any_protocol_event("pusher_internal:member_added"),
            Some(("member_added", true))
        );
        assert_eq!(
            ProtocolVersion::parse_any_protocol_event("sockudo_internal:member_added"),
            Some(("member_added", true))
        );
        assert_eq!(
            ProtocolVersion::parse_any_protocol_event("client-event"),
            None
        );
    }

    #[test]
    fn test_rewrite_event_prefix() {
        // V1 → V2
        assert_eq!(
            ProtocolVersion::V2.rewrite_event_prefix("pusher:subscribe"),
            "sockudo:subscribe"
        );
        assert_eq!(
            ProtocolVersion::V2.rewrite_event_prefix("pusher_internal:member_added"),
            "sockudo_internal:member_added"
        );

        // V2 → V1
        assert_eq!(
            ProtocolVersion::V1.rewrite_event_prefix("sockudo:subscribe"),
            "pusher:subscribe"
        );

        // Non-protocol event stays unchanged
        assert_eq!(
            ProtocolVersion::V2.rewrite_event_prefix("client-my-event"),
            "client-my-event"
        );
    }

    #[test]
    fn test_from_query_param() {
        assert_eq!(ProtocolVersion::from_query_param(None), ProtocolVersion::V1);
        assert_eq!(
            ProtocolVersion::from_query_param(Some(1)),
            ProtocolVersion::V1
        );
        assert_eq!(
            ProtocolVersion::from_query_param(Some(2)),
            ProtocolVersion::V2
        );
        assert_eq!(
            ProtocolVersion::from_query_param(Some(99)),
            ProtocolVersion::V1
        );
    }

    #[test]
    fn test_v2_native_features() {
        assert!(!ProtocolVersion::V1.is_serial_native());
        assert!(!ProtocolVersion::V1.is_message_id_native());
        assert!(!ProtocolVersion::V1.is_recovery_native());

        assert!(ProtocolVersion::V2.is_serial_native());
        assert!(ProtocolVersion::V2.is_message_id_native());
        assert!(ProtocolVersion::V2.is_recovery_native());
    }
}
