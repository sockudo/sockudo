use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::domain::{
    MAX_RENDERED_TEMPLATE_BYTES, ProviderOverridePayload, PushPayload, PushProviderKind,
    validate_web_push_endpoint,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PayloadTransformError {
    #[error("invalid {provider} override: {reason}")]
    InvalidOverride {
        provider: &'static str,
        reason: &'static str,
    },
    #[error("invalid template substitution: {reason}")]
    InvalidTemplate { reason: &'static str },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderedProviderPayload {
    pub provider: PushProviderKind,
    pub payload: Value,
    pub used_override: bool,
}

pub fn render_provider_payload(
    provider: PushProviderKind,
    payload: &PushPayload,
    overrides: &[ProviderOverridePayload],
) -> Result<RenderedProviderPayload, PayloadTransformError> {
    if let Some(provider_override) = overrides
        .iter()
        .find(|candidate| candidate.provider == provider)
    {
        validate_provider_override(provider, &provider_override.payload)?;
        return Ok(RenderedProviderPayload {
            provider,
            payload: provider_override.payload.clone(),
            used_override: true,
        });
    }

    let payload = render_template_fields(payload)?;

    let rendered = match provider {
        PushProviderKind::Fcm => json!({
            "message": {
                "notification": notification_object(&payload),
                "data": payload.template_data,
                "android": {
                    "collapse_key": payload.collapse_key,
                    "notification": {
                        "icon": payload.icon,
                        "sound": payload.sound
                    }
                }
            }
        }),
        PushProviderKind::Apns => json!({
            "headers": {
                "apns-push-type": "alert",
                "apns-priority": "10",
                "apns-collapse-id": payload.collapse_key
            },
            "aps": {
                "alert": {
                    "title": payload.title,
                    "body": payload.body
                },
                "sound": payload.sound
            },
            "data": payload.template_data
        }),
        PushProviderKind::WebPush => json!({
            "headers": {
                "ttl": 2419200,
                "urgency": "normal",
                "topic": payload.collapse_key
            },
            "notification": notification_object(&payload),
            "data": payload.template_data
        }),
        PushProviderKind::Hms => json!({
            "message": {
                "notification": notification_object(&payload),
                "data": payload.template_data,
                "android": {
                    "notification": {
                        "click_action": { "type": 3 },
                        "sound": payload.sound
                    },
                    "collapse_key": payload.collapse_key
                }
            }
        }),
        PushProviderKind::Wns => json!({
            "type": "toast",
            "toast": {
                "visual": {
                    "binding": {
                        "template": "ToastGeneric",
                        "text": [payload.title, payload.body],
                        "image": payload.icon
                    }
                },
                "audio": { "src": payload.sound }
            },
            "data": payload.template_data
        }),
    };

    Ok(RenderedProviderPayload {
        provider,
        payload: rendered,
        used_override: false,
    })
}

fn notification_object(payload: &PushPayload) -> Value {
    json!({
        "title": payload.title,
        "body": payload.body,
        "image": payload.icon
    })
}

fn render_template_fields(payload: &PushPayload) -> Result<PushPayload, PayloadTransformError> {
    Ok(PushPayload {
        template_id: payload.template_id.clone(),
        template_data: payload.template_data.clone(),
        title: render_optional_template(&payload.title, &payload.template_data)?,
        body: render_optional_template(&payload.body, &payload.template_data)?,
        icon: render_optional_template(&payload.icon, &payload.template_data)?,
        sound: render_optional_template(&payload.sound, &payload.template_data)?,
        collapse_key: render_optional_template(&payload.collapse_key, &payload.template_data)?,
    })
}

fn render_optional_template(
    value: &Option<String>,
    data: &Value,
) -> Result<Option<String>, PayloadTransformError> {
    value
        .as_deref()
        .map(|raw| render_template_string(raw, data))
        .transpose()
}

fn render_template_string(raw: &str, data: &Value) -> Result<String, PayloadTransformError> {
    let mut output = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find("{{") {
        let (prefix, after_start) = rest.split_at(start);
        output.push_str(prefix);
        let after_start = &after_start[2..];
        let Some(end) = after_start.find("}}") else {
            return Err(PayloadTransformError::InvalidTemplate {
                reason: "unterminated placeholder",
            });
        };
        let (key, after_key) = after_start.split_at(end);
        let replacement = lookup_template_value(data, key.trim())?;
        output.push_str(&replacement);
        if output.len() > MAX_RENDERED_TEMPLATE_BYTES {
            return Err(PayloadTransformError::InvalidTemplate {
                reason: "rendered output is too large",
            });
        }
        rest = &after_key[2..];
    }
    output.push_str(rest);
    if output.len() > MAX_RENDERED_TEMPLATE_BYTES {
        return Err(PayloadTransformError::InvalidTemplate {
            reason: "rendered output is too large",
        });
    }
    Ok(output)
}

fn lookup_template_value(data: &Value, key: &str) -> Result<String, PayloadTransformError> {
    if !key.starts_with("data.")
        || key == "data."
        || key.matches('.').count() > 4
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
    {
        return Err(PayloadTransformError::InvalidTemplate {
            reason: "placeholder key must use a bounded data.* path",
        });
    }

    let key = &key["data.".len()..];
    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.'))
    {
        return Err(PayloadTransformError::InvalidTemplate {
            reason: "placeholder key is not allowed",
        });
    }

    let mut current = data;
    for segment in key.split('.') {
        current = current
            .get(segment)
            .ok_or(PayloadTransformError::InvalidTemplate {
                reason: "placeholder key is missing",
            })?;
    }

    match current {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => Ok(String::new()),
        Value::Array(_) | Value::Object(_) => Err(PayloadTransformError::InvalidTemplate {
            reason: "placeholder value must be scalar",
        }),
    }
}

fn validate_provider_override(
    provider: PushProviderKind,
    payload: &Value,
) -> Result<(), PayloadTransformError> {
    match provider {
        PushProviderKind::Fcm | PushProviderKind::Hms => Ok(()),
        PushProviderKind::Apns => validate_apns_fields(payload),
        PushProviderKind::WebPush => validate_web_push_fields(payload),
        PushProviderKind::Wns => validate_wns_fields(payload),
    }
}

fn validate_apns_fields(payload: &Value) -> Result<(), PayloadTransformError> {
    let Some(headers) = payload.get("headers").and_then(Value::as_object) else {
        return Ok(());
    };
    if let Some(push_type) = headers.get("apns-push-type").and_then(Value::as_str)
        && !matches!(
            push_type,
            "alert"
                | "background"
                | "voip"
                | "complication"
                | "fileprovider"
                | "mdm"
                | "liveactivity"
        )
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-push-type",
        });
    }
    if let Some(priority) = headers.get("apns-priority").and_then(Value::as_str)
        && !matches!(priority, "5" | "10")
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-priority",
        });
    }
    if let Some(expiration) = headers.get("apns-expiration")
        && !(expiration.is_string() || expiration.is_number())
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-expiration",
        });
    }
    if let Some(topic) = headers.get("apns-topic")
        && !topic.is_string()
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-topic",
        });
    }
    if let Some(collapse_id) = headers.get("apns-collapse-id")
        && !collapse_id.is_string()
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-collapse-id",
        });
    }
    Ok(())
}

fn validate_web_push_fields(payload: &Value) -> Result<(), PayloadTransformError> {
    let Some(headers) = payload.get("headers").and_then(Value::as_object) else {
        return Ok(());
    };
    if let Some(ttl) = headers.get("ttl")
        && !(ttl.as_u64().is_some()
            || ttl
                .as_str()
                .and_then(|raw| raw.parse::<u64>().ok())
                .is_some())
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "webPush",
            reason: "invalid ttl",
        });
    }
    if let Some(urgency) = headers.get("urgency").and_then(Value::as_str)
        && !matches!(urgency, "very-low" | "low" | "normal" | "high")
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "webPush",
            reason: "invalid urgency",
        });
    }
    if let Some(topic) = headers.get("topic")
        && !topic.is_string()
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "webPush",
            reason: "invalid topic",
        });
    }
    if let Some(audience) = headers.get("vapidAudience").and_then(Value::as_str) {
        validate_web_push_endpoint(audience).map_err(|_| {
            PayloadTransformError::InvalidOverride {
                provider: "webPush",
                reason: "invalid vapidAudience",
            }
        })?;
    }
    Ok(())
}

fn validate_wns_fields(payload: &Value) -> Result<(), PayloadTransformError> {
    if let Some(kind) = payload.get("type").and_then(Value::as_str)
        && !matches!(kind, "toast" | "tile" | "raw")
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "wns",
            reason: "invalid type",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload() -> PushPayload {
        PushPayload {
            template_id: Some("welcome".to_owned()),
            template_data: json!({"k": "v"}),
            title: Some("Hello".to_owned()),
            body: Some("Body".to_owned()),
            icon: Some("https://example.com/icon.png".to_owned()),
            sound: Some("default".to_owned()),
            collapse_key: Some("welcome".to_owned()),
        }
    }

    #[test]
    fn generic_payload_maps_to_provider_shapes() {
        assert!(
            render_provider_payload(PushProviderKind::Fcm, &payload(), &[])
                .unwrap()
                .payload
                .get("message")
                .is_some()
        );
        assert!(
            render_provider_payload(PushProviderKind::Apns, &payload(), &[])
                .unwrap()
                .payload
                .get("aps")
                .is_some()
        );
        assert!(
            render_provider_payload(PushProviderKind::WebPush, &payload(), &[])
                .unwrap()
                .payload
                .get("headers")
                .is_some()
        );
        assert!(
            render_provider_payload(PushProviderKind::Wns, &payload(), &[])
                .unwrap()
                .payload
                .get("toast")
                .is_some()
        );
    }

    #[test]
    fn provider_overrides_take_precedence_and_are_validated() {
        let overrides = vec![ProviderOverridePayload {
            provider: PushProviderKind::Apns,
            payload: json!({
                "headers": {
                    "apns-push-type": "alert",
                    "apns-priority": "10",
                    "apns-topic": "com.example.app",
                    "apns-collapse-id": "welcome"
                },
                "aps": {"alert": "override"}
            }),
        }];
        let rendered =
            render_provider_payload(PushProviderKind::Apns, &payload(), &overrides).unwrap();
        assert!(rendered.used_override);
        assert_eq!(rendered.payload["aps"]["alert"], "override");

        let invalid = vec![ProviderOverridePayload {
            provider: PushProviderKind::WebPush,
            payload: json!({"headers": {"urgency": "now"}}),
        }];
        assert!(render_provider_payload(PushProviderKind::WebPush, &payload(), &invalid).is_err());
    }

    #[test]
    fn template_substitution_is_sandboxed_and_output_bounded() {
        let mut payload = payload();
        payload.title = Some("Hello {{ data.user.name }}".to_owned());
        payload.template_data = json!({"user": {"name": "Ada"}});

        let rendered = render_provider_payload(PushProviderKind::Fcm, &payload, &[]).unwrap();
        assert_eq!(
            rendered.payload["message"]["notification"]["title"],
            "Hello Ada"
        );

        payload.title = Some("{{ data.user }}".to_owned());
        payload.template_data = json!({"user": {"name": "Ada"}});
        assert_eq!(
            render_provider_payload(PushProviderKind::Fcm, &payload, &[]).unwrap_err(),
            PayloadTransformError::InvalidTemplate {
                reason: "placeholder value must be scalar"
            }
        );

        payload.title = Some(format!(
            "{{{{ data.name }}}}{}",
            "x".repeat(MAX_RENDERED_TEMPLATE_BYTES)
        ));
        payload.template_data = json!({"name": "Ada"});
        assert!(matches!(
            render_provider_payload(PushProviderKind::Fcm, &payload, &[]),
            Err(PayloadTransformError::InvalidTemplate {
                reason: "rendered output is too large"
            })
        ));
    }
}
