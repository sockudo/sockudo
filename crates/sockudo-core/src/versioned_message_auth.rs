use crate::error::{Error, Result};
use crate::token::secure_compare;
use crate::websocket::ConnectionCapabilities;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Update,
    Delete,
    Append,
}

impl MutationKind {
    pub fn as_verb(self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Append => "append",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationGrant {
    Own,
    Any,
}

#[derive(Debug, Clone)]
pub struct MutationAuthorizationRequest<'a> {
    pub channel: &'a str,
    pub kind: MutationKind,
    pub original_client_id: Option<&'a str>,
    pub actor_client_id: Option<&'a str>,
    pub capabilities: Option<&'a ConnectionCapabilities>,
    pub privileged_server: bool,
}

pub fn authorize_message_mutation(
    request: MutationAuthorizationRequest<'_>,
) -> Result<MutationGrant> {
    if request.privileged_server {
        return Ok(MutationGrant::Any);
    }

    let capabilities = request.capabilities.ok_or_else(|| {
        Error::Auth(format!(
            "Connection is not allowed to {} message on channel '{}'",
            request.kind.as_verb(),
            request.channel
        ))
    })?;

    if capabilities.allows_message_mutation_any(request.kind, request.channel) {
        return Ok(MutationGrant::Any);
    }

    if capabilities.allows_message_mutation_own(request.kind, request.channel) {
        let actor_client_id = request.actor_client_id.ok_or_else(|| {
            Error::Auth(format!(
                "Connection must have an identified client to {} own messages",
                request.kind.as_verb()
            ))
        })?;

        let original_client_id = request.original_client_id.ok_or_else(|| {
            Error::Auth(format!(
                "Cannot authorize own-scoped {} because the original message creator is unidentified",
                request.kind.as_verb()
            ))
        })?;

        if secure_compare(actor_client_id, original_client_id) {
            return Ok(MutationGrant::Own);
        }

        return Err(Error::Auth(format!(
            "Connection client '{}' is not allowed to {} message owned by '{}'",
            actor_client_id,
            request.kind.as_verb(),
            original_client_id
        )));
    }

    Err(Error::Auth(format!(
        "Connection is not allowed to {} message on channel '{}'",
        request.kind.as_verb(),
        request.channel
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps_with_update_own() -> ConnectionCapabilities {
        ConnectionCapabilities {
            subscribe: None,
            publish: None,
            presence: None,
            message_update_own: Some(vec!["chat:*".to_string()]),
            message_update_any: None,
            message_delete_own: None,
            message_delete_any: None,
            message_append_own: None,
            message_append_any: None,
            ..Default::default()
        }
    }

    fn caps_with_delete_any() -> ConnectionCapabilities {
        ConnectionCapabilities {
            subscribe: None,
            publish: None,
            presence: None,
            message_update_own: None,
            message_update_any: None,
            message_delete_own: None,
            message_delete_any: Some(vec!["chat:*".to_string()]),
            message_append_own: None,
            message_append_any: None,
            ..Default::default()
        }
    }

    fn caps_with_append_own() -> ConnectionCapabilities {
        ConnectionCapabilities {
            subscribe: None,
            publish: None,
            presence: None,
            message_update_own: None,
            message_update_any: None,
            message_delete_own: None,
            message_delete_any: None,
            message_append_own: Some(vec!["chat:*".to_string()]),
            message_append_any: None,
            ..Default::default()
        }
    }

    #[test]
    fn privileged_server_grants_any_scope() {
        let grant = authorize_message_mutation(MutationAuthorizationRequest {
            channel: "chat:room-1",
            kind: MutationKind::Update,
            original_client_id: Some("user-1"),
            actor_client_id: None,
            capabilities: None,
            privileged_server: true,
        })
        .unwrap();

        assert_eq!(grant, MutationGrant::Any);
    }

    #[test]
    fn own_update_succeeds_for_matching_identified_actor() {
        let grant = authorize_message_mutation(MutationAuthorizationRequest {
            channel: "chat:room-1",
            kind: MutationKind::Update,
            original_client_id: Some("user-1"),
            actor_client_id: Some("user-1"),
            capabilities: Some(&caps_with_update_own()),
            privileged_server: false,
        })
        .unwrap();

        assert_eq!(grant, MutationGrant::Own);
    }

    #[test]
    fn own_update_fails_for_mismatched_actor() {
        let err = authorize_message_mutation(MutationAuthorizationRequest {
            channel: "chat:room-1",
            kind: MutationKind::Update,
            original_client_id: Some("user-1"),
            actor_client_id: Some("user-2"),
            capabilities: Some(&caps_with_update_own()),
            privileged_server: false,
        })
        .unwrap_err();

        assert!(err.to_string().contains("is not allowed to update"));
    }

    #[test]
    fn own_append_fails_without_identified_actor() {
        let err = authorize_message_mutation(MutationAuthorizationRequest {
            channel: "chat:room-1",
            kind: MutationKind::Append,
            original_client_id: Some("user-1"),
            actor_client_id: None,
            capabilities: Some(&caps_with_append_own()),
            privileged_server: false,
        })
        .unwrap_err();

        assert!(err.to_string().contains("must have an identified client"));
    }

    #[test]
    fn any_delete_succeeds_without_owner_match() {
        let grant = authorize_message_mutation(MutationAuthorizationRequest {
            channel: "chat:room-1",
            kind: MutationKind::Delete,
            original_client_id: Some("user-1"),
            actor_client_id: Some("user-2"),
            capabilities: Some(&caps_with_delete_any()),
            privileged_server: false,
        })
        .unwrap();

        assert_eq!(grant, MutationGrant::Any);
    }

    #[test]
    fn mutation_without_capability_is_denied() {
        let err = authorize_message_mutation(MutationAuthorizationRequest {
            channel: "chat:room-1",
            kind: MutationKind::Delete,
            original_client_id: Some("user-1"),
            actor_client_id: Some("user-1"),
            capabilities: Some(&ConnectionCapabilities::default()),
            privileged_server: false,
        })
        .unwrap_err();

        assert!(err.to_string().contains("not allowed to delete"));
    }
}
