//! Protocol versions and event-name prefixes.

/// Wire protocol spoken with the server.
///
/// * [`ProtocolVersion::V1`] is strict Pusher compatibility (`pusher:` /
///   `pusher_internal:` prefixes, protocol `7`).
/// * [`ProtocolVersion::V2`] is the Sockudo-native protocol (`sockudo:` /
///   `sockudo_internal:` prefixes) that unlocks recovery, rewind, delta
///   compression, filters, binary wire formats, capability tokens, mutable
///   messages and annotations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ProtocolVersion {
    /// Pusher-compatible protocol 7.
    V1,
    /// Sockudo Protocol V2 (default).
    #[default]
    V2,
}

impl ProtocolVersion {
    /// Value sent in the `protocol` connection query parameter.
    pub const fn query_value(self) -> &'static str {
        match self {
            Self::V1 => "7",
            Self::V2 => "2",
        }
    }

    /// Whether this is Protocol V2.
    pub const fn is_v2(self) -> bool {
        matches!(self, Self::V2)
    }

    /// The event-name prefixes used by this protocol.
    pub const fn prefix(self) -> Prefix {
        Prefix { version: self }
    }
}

/// Event-name prefixes for a protocol version.
///
/// Inbound classification accepts both the `pusher` and `sockudo` spellings so
/// the client stays robust to servers that only partially rewrite prefixes;
/// events the client emits to your callbacks always use this version's prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Prefix {
    version: ProtocolVersion,
}

const PUSHER_EVENT: &str = "pusher:";
const PUSHER_INTERNAL: &str = "pusher_internal:";
const SOCKUDO_EVENT: &str = "sockudo:";
const SOCKUDO_INTERNAL: &str = "sockudo_internal:";

impl Prefix {
    /// The platform event prefix (`pusher:` or `sockudo:`).
    pub const fn event_prefix(self) -> &'static str {
        match self.version {
            ProtocolVersion::V1 => PUSHER_EVENT,
            ProtocolVersion::V2 => SOCKUDO_EVENT,
        }
    }

    /// The internal event prefix (`pusher_internal:` or `sockudo_internal:`).
    pub const fn internal_prefix(self) -> &'static str {
        match self.version {
            ProtocolVersion::V1 => PUSHER_INTERNAL,
            ProtocolVersion::V2 => SOCKUDO_INTERNAL,
        }
    }

    /// Build a platform event name, e.g. `event("subscription_succeeded")`.
    pub fn event(self, name: &str) -> String {
        let prefix = self.event_prefix();
        let mut out = String::with_capacity(prefix.len() + name.len());
        out.push_str(prefix);
        out.push_str(name);
        out
    }

    /// Build an internal event name, e.g. `internal("member_added")`.
    pub fn internal(self, name: &str) -> String {
        let prefix = self.internal_prefix();
        let mut out = String::with_capacity(prefix.len() + name.len());
        out.push_str(prefix);
        out.push_str(name);
        out
    }

    /// Whether `name` is a platform (`pusher:`/`sockudo:`) event.
    pub fn is_platform_event(self, name: &str) -> bool {
        strip_platform(name).is_some()
    }

    /// Whether `name` is an internal (`pusher_internal:`/`sockudo_internal:`) event.
    pub fn is_internal_event(self, name: &str) -> bool {
        strip_internal(name).is_some()
    }
}

/// The canonical suffix of a platform event (`pusher:x` / `sockudo:x` → `x`).
#[inline]
pub(crate) fn strip_platform(name: &str) -> Option<&str> {
    name.strip_prefix(SOCKUDO_EVENT)
        .or_else(|| name.strip_prefix(PUSHER_EVENT))
}

/// The canonical suffix of an internal event.
#[inline]
pub(crate) fn strip_internal(name: &str) -> Option<&str> {
    name.strip_prefix(SOCKUDO_INTERNAL)
        .or_else(|| name.strip_prefix(PUSHER_INTERNAL))
}

/// Error code: capability token expired; refresh and resend `auth`.
pub const TOKEN_EXPIRED_CODE: i64 = 40142;
/// Error code: capability token revoked; do not retry in place.
pub const TOKEN_REVOKED_CODE: i64 = 40160;
/// Subscription mode that enables raw annotation delivery.
pub const ANNOTATION_SUBSCRIBE_MODE: &str = "ANNOTATION_SUBSCRIBE";
/// Allowed values for the V2 `append_rollup_window` connection parameter.
pub const ALLOWED_APPEND_ROLLUP_WINDOWS: [u32; 5] = [0, 20, 40, 100, 500];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_follow_version() {
        let v1 = ProtocolVersion::V1.prefix();
        let v2 = ProtocolVersion::V2.prefix();
        assert_eq!(v1.event("ping"), "pusher:ping");
        assert_eq!(v2.event("ping"), "sockudo:ping");
        assert_eq!(v1.internal("member_added"), "pusher_internal:member_added");
        assert_eq!(v2.internal("member_added"), "sockudo_internal:member_added");
        assert_eq!(ProtocolVersion::V1.query_value(), "7");
        assert_eq!(ProtocolVersion::V2.query_value(), "2");
    }

    #[test]
    fn classification_accepts_both_spellings() {
        assert_eq!(strip_platform("pusher:delta"), Some("delta"));
        assert_eq!(strip_platform("sockudo:delta"), Some("delta"));
        assert_eq!(strip_platform("client-x"), None);
        assert_eq!(strip_internal("pusher_internal:a"), Some("a"));
        assert_eq!(strip_internal("sockudo_internal:a"), Some("a"));
        // `pusher_internal:` must not be mistaken for a platform event.
        assert_eq!(strip_platform("pusher_internal:a"), None);
        assert!(
            ProtocolVersion::V2
                .prefix()
                .is_internal_event("pusher_internal:x")
        );
    }
}
