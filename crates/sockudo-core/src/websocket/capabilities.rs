use crate::utils::wildcard_pattern_matches;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserInfo {
    pub id: String,
    pub watchlist: Option<Vec<String>>,
    pub info: Option<Value>,
    pub capabilities: Option<ConnectionCapabilities>,
    pub meta: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectionCapabilities {
    pub subscribe: Option<Vec<String>>,
    pub publish: Option<Vec<String>>,
    pub history: Option<Vec<String>>,
    pub presence: Option<Vec<String>>,
    #[serde(rename = "annotation-subscribe", alias = "annotation_subscribe")]
    pub annotation_subscribe: Option<Vec<String>>,
    #[serde(rename = "annotation-publish", alias = "annotation_publish")]
    pub annotation_publish: Option<Vec<String>>,
    #[serde(rename = "annotation-delete-own", alias = "annotation_delete_own")]
    pub annotation_delete_own: Option<Vec<String>>,
    #[serde(rename = "annotation-delete-any", alias = "annotation_delete_any")]
    pub annotation_delete_any: Option<Vec<String>>,
    pub message_update_own: Option<Vec<String>>,
    pub message_update_any: Option<Vec<String>>,
    pub message_delete_own: Option<Vec<String>>,
    pub message_delete_any: Option<Vec<String>>,
    pub message_append_own: Option<Vec<String>>,
    pub message_append_any: Option<Vec<String>>,
}

impl ConnectionCapabilities {
    pub fn matches_any(patterns: &[String], channel: &str) -> bool {
        patterns.iter().any(|pattern| {
            pattern == "*" || pattern == channel || wildcard_pattern_matches(channel, pattern)
        })
    }

    pub fn allows_subscribe(&self, channel: &str) -> bool {
        if channel.starts_with("presence-")
            && let Some(patterns) = self.presence.as_deref()
        {
            return Self::matches_any(patterns, channel);
        }

        self.subscribe
            .as_deref()
            .is_none_or(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_publish(&self, channel: &str) -> bool {
        self.publish
            .as_deref()
            .is_none_or(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_history(&self, channel: &str) -> bool {
        self.history
            .as_deref()
            .is_none_or(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_annotation_subscribe(&self, channel: &str) -> bool {
        self.annotation_subscribe
            .as_deref()
            .is_some_and(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_annotation_publish(&self, channel: &str) -> bool {
        self.annotation_publish
            .as_deref()
            .is_some_and(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_annotation_delete_own(&self, channel: &str) -> bool {
        self.annotation_delete_own
            .as_deref()
            .is_some_and(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_annotation_delete_any(&self, channel: &str) -> bool {
        self.annotation_delete_any
            .as_deref()
            .is_some_and(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_message_mutation_own(
        &self,
        kind: crate::versioned_message_auth::MutationKind,
        channel: &str,
    ) -> bool {
        self.mutation_patterns(kind, false)
            .is_some_and(|patterns| Self::matches_any(patterns, channel))
    }

    pub fn allows_message_mutation_any(
        &self,
        kind: crate::versioned_message_auth::MutationKind,
        channel: &str,
    ) -> bool {
        self.mutation_patterns(kind, true)
            .is_some_and(|patterns| Self::matches_any(patterns, channel))
    }

    fn mutation_patterns(
        &self,
        kind: crate::versioned_message_auth::MutationKind,
        any_scope: bool,
    ) -> Option<&[String]> {
        match (kind, any_scope) {
            (crate::versioned_message_auth::MutationKind::Update, false) => {
                self.message_update_own.as_deref()
            }
            (crate::versioned_message_auth::MutationKind::Update, true) => {
                self.message_update_any.as_deref()
            }
            (crate::versioned_message_auth::MutationKind::Delete, false) => {
                self.message_delete_own.as_deref()
            }
            (crate::versioned_message_auth::MutationKind::Delete, true) => {
                self.message_delete_any.as_deref()
            }
            (crate::versioned_message_auth::MutationKind::Append, false) => {
                self.message_append_own.as_deref()
            }
            (crate::versioned_message_auth::MutationKind::Append, true) => {
                self.message_append_any.as_deref()
            }
        }
    }
}
