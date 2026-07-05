use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sonic_rs::prelude::*;
use sonic_rs::{Value, json};
use thiserror::Error;

use crate::domain::{
    MAX_APNS_PAYLOAD_BYTES, MAX_FCM_PAYLOAD_BYTES, MAX_HMS_PAYLOAD_BYTES,
    MAX_RENDERED_TEMPLATE_BYTES, MAX_WEB_PUSH_PAYLOAD_BYTES, MAX_WNS_PAYLOAD_BYTES,
    NotificationTemplate, ProviderOverridePayload, PushPayload, PushProviderKind,
    RenderedProviderPayload, validate_web_push_endpoint,
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
    #[error("{provider} payload exceeds {max_bytes} bytes")]
    PayloadTooLarge {
        provider: &'static str,
        max_bytes: usize,
    },
    #[error("{provider} payload serialization failed")]
    PayloadSerialization { provider: &'static str },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectivePushPayload {
    pub payload: PushPayload,
    pub provider_overrides: Vec<ProviderOverridePayload>,
}

pub const PUSH_PROVIDER_RENDER_ORDER: [PushProviderKind; 5] = [
    PushProviderKind::Fcm,
    PushProviderKind::Apns,
    PushProviderKind::WebPush,
    PushProviderKind::Hms,
    PushProviderKind::Wns,
];

pub fn render_all_provider_payloads(
    payload: &PushPayload,
    overrides: &[ProviderOverridePayload],
) -> Result<Vec<RenderedProviderPayload>, PayloadTransformError> {
    PUSH_PROVIDER_RENDER_ORDER
        .into_iter()
        .map(|provider| render_provider_payload(provider, payload, overrides))
        .collect()
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
        return rendered_provider_payload(provider, provider_override.payload.clone(), true);
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

    rendered_provider_payload(provider, rendered, false)
}

pub fn resolve_template_payload(
    template: &NotificationTemplate,
    payload: &PushPayload,
    request_overrides: &[ProviderOverridePayload],
) -> Result<EffectivePushPayload, PayloadTransformError> {
    let content = template
        .resolve_locale(requested_template_locale(payload))
        .ok_or(PayloadTransformError::InvalidTemplate {
            reason: "template locale is missing",
        })?;
    let payload = PushPayload {
        template_id: payload.template_id.clone(),
        template_data: payload.template_data.clone(),
        title: payload
            .title
            .clone()
            .or_else(|| Some(content.title.clone())),
        body: payload.body.clone().or_else(|| Some(content.body.clone())),
        icon: payload.icon.clone().or_else(|| content.icon.clone()),
        sound: payload.sound.clone().or_else(|| content.sound.clone()),
        collapse_key: payload
            .collapse_key
            .clone()
            .or_else(|| content.collapse_key.clone()),
    };
    Ok(EffectivePushPayload {
        payload,
        provider_overrides: merge_template_overrides(template, request_overrides)?,
    })
}

pub fn requested_template_locale(payload: &PushPayload) -> Option<&str> {
    payload
        .template_data
        .get("locale")
        .and_then(Value::as_str)
        .filter(|locale| !locale.trim().is_empty())
}

fn merge_template_overrides(
    template: &NotificationTemplate,
    request_overrides: &[ProviderOverridePayload],
) -> Result<Vec<ProviderOverridePayload>, PayloadTransformError> {
    let mut by_provider = BTreeMap::new();
    for (provider, override_payload) in &template.provider_overrides {
        if override_payload.provider != *provider {
            return Err(PayloadTransformError::InvalidTemplate {
                reason: "template provider override key mismatch",
            });
        }
        by_provider.insert(*provider, override_payload.clone());
    }
    for override_payload in request_overrides {
        by_provider.insert(override_payload.provider, override_payload.clone());
    }
    let overrides = by_provider.into_values().collect::<Vec<_>>();
    for override_payload in &overrides {
        validate_provider_override(override_payload.provider, &override_payload.payload)?;
    }
    Ok(overrides)
}

fn rendered_provider_payload(
    provider: PushProviderKind,
    payload: Value,
    used_override: bool,
) -> Result<RenderedProviderPayload, PayloadTransformError> {
    validate_rendered_payload_size(provider, &payload)?;
    Ok(RenderedProviderPayload {
        provider,
        payload,
        used_override,
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

    if let Some(value) = current.as_str() {
        Ok(value.to_owned())
    } else if current.is_number() {
        Ok(current.to_string())
    } else if let Some(value) = current.as_bool() {
        Ok(value.to_string())
    } else if current.is_null() {
        Ok(String::new())
    } else {
        Err(PayloadTransformError::InvalidTemplate {
            reason: "placeholder value must be scalar",
        })
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

fn validate_rendered_payload_size(
    provider: PushProviderKind,
    payload: &Value,
) -> Result<(), PayloadTransformError> {
    let provider_name = provider_label(provider);
    let max_bytes = provider_payload_limit(provider);
    let apns_body;
    let sized_payload = if provider == PushProviderKind::Apns {
        apns_body = payload
            .get("aps")
            .map(|aps| {
                json!({
                    "aps": aps,
                    "data": payload.get("data").cloned().unwrap_or_else(Value::new_null)
                })
            })
            .unwrap_or_else(|| payload.clone());
        &apns_body
    } else {
        payload
    };
    let bytes = sonic_rs::to_vec(sized_payload).map_err(|_| {
        PayloadTransformError::PayloadSerialization {
            provider: provider_name,
        }
    })?;
    if bytes.len() > max_bytes {
        return Err(PayloadTransformError::PayloadTooLarge {
            provider: provider_name,
            max_bytes,
        });
    }
    Ok(())
}

fn provider_payload_limit(provider: PushProviderKind) -> usize {
    match provider {
        PushProviderKind::Fcm => MAX_FCM_PAYLOAD_BYTES,
        PushProviderKind::Apns => MAX_APNS_PAYLOAD_BYTES,
        PushProviderKind::WebPush => MAX_WEB_PUSH_PAYLOAD_BYTES,
        PushProviderKind::Hms => MAX_HMS_PAYLOAD_BYTES,
        PushProviderKind::Wns => MAX_WNS_PAYLOAD_BYTES,
    }
}

fn provider_label(provider: PushProviderKind) -> &'static str {
    match provider {
        PushProviderKind::Fcm => "fcm",
        PushProviderKind::Apns => "apns",
        PushProviderKind::WebPush => "webPush",
        PushProviderKind::Hms => "hms",
        PushProviderKind::Wns => "wns",
    }
}

fn validate_apns_fields(payload: &Value) -> Result<(), PayloadTransformError> {
    let Some(headers) = payload.get("headers").and_then(Value::as_object) else {
        return Ok(());
    };
    if let Some(push_type) = headers.get(&"apns-push-type").and_then(Value::as_str)
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
    if let Some(priority) = headers.get(&"apns-priority").and_then(Value::as_str)
        && !matches!(priority, "5" | "10")
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-priority",
        });
    }
    if let Some(expiration) = headers.get(&"apns-expiration")
        && !(expiration.is_str() || expiration.is_number())
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-expiration",
        });
    }
    if let Some(topic) = headers.get(&"apns-topic")
        && !topic.is_str()
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "apns",
            reason: "invalid apns-topic",
        });
    }
    if let Some(collapse_id) = headers.get(&"apns-collapse-id")
        && !collapse_id.is_str()
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
    if let Some(ttl) = headers.get(&"ttl")
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
    if let Some(urgency) = headers.get(&"urgency").and_then(Value::as_str)
        && !matches!(urgency, "very-low" | "low" | "normal" | "high")
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "webPush",
            reason: "invalid urgency",
        });
    }
    if let Some(topic) = headers.get(&"topic")
        && !topic.is_str()
    {
        return Err(PayloadTransformError::InvalidOverride {
            provider: "webPush",
            reason: "invalid topic",
        });
    }
    if let Some(audience) = headers.get(&"vapidAudience").and_then(Value::as_str) {
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::domain::{NotificationTemplate, TemplateContent};

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
    fn provider_overrides_apply_to_all_supported_providers() {
        let overrides = vec![
            ProviderOverridePayload {
                provider: PushProviderKind::Fcm,
                payload: json!({"message": {"data": {"provider": "fcm"}}}),
            },
            ProviderOverridePayload {
                provider: PushProviderKind::Apns,
                payload: json!({
                    "headers": {"apns-priority": "5"},
                    "aps": {"alert": "apns"}
                }),
            },
            ProviderOverridePayload {
                provider: PushProviderKind::WebPush,
                payload: json!({
                    "headers": {"ttl": 60, "urgency": "high", "topic": "topic-1"},
                    "notification": {"title": "web"}
                }),
            },
            ProviderOverridePayload {
                provider: PushProviderKind::Hms,
                payload: json!({"message": {"data": {"provider": "hms"}}}),
            },
            ProviderOverridePayload {
                provider: PushProviderKind::Wns,
                payload: json!({"type": "toast", "toast": {"visual": {}}}),
            },
        ];

        for provider in PUSH_PROVIDER_RENDER_ORDER {
            let rendered = render_provider_payload(provider, &payload(), &overrides).unwrap();
            assert!(rendered.used_override, "{provider:?}");
        }
        assert_eq!(
            render_provider_payload(PushProviderKind::Fcm, &payload(), &overrides)
                .unwrap()
                .payload["message"]["data"]["provider"],
            "fcm"
        );
        assert_eq!(
            render_provider_payload(PushProviderKind::WebPush, &payload(), &overrides)
                .unwrap()
                .payload["headers"]["urgency"],
            "high"
        );
        assert_eq!(
            render_provider_payload(PushProviderKind::Wns, &payload(), &overrides)
                .unwrap()
                .payload["type"],
            "toast"
        );
    }

    #[test]
    fn notification_template_resolves_locale_fallback_and_merges_overrides() {
        let template = NotificationTemplate {
            app_id: "app-1".to_owned(),
            template_id: "welcome".to_owned(),
            default_locale: "en".to_owned(),
            locales: BTreeMap::from([
                (
                    "en".to_owned(),
                    TemplateContent {
                        title: "Hello {{ data.name }}".to_owned(),
                        body: "Body".to_owned(),
                        icon: None,
                        sound: None,
                        collapse_key: Some("welcome".to_owned()),
                    },
                ),
                (
                    "fr".to_owned(),
                    TemplateContent {
                        title: "Bonjour {{ data.name }}".to_owned(),
                        body: "Corps".to_owned(),
                        icon: None,
                        sound: Some("default".to_owned()),
                        collapse_key: Some("bienvenue".to_owned()),
                    },
                ),
            ]),
            provider_overrides: BTreeMap::from([(
                PushProviderKind::Fcm,
                ProviderOverridePayload {
                    provider: PushProviderKind::Fcm,
                    payload: json!({"message": {"data": {"source": "template"}}}),
                },
            )]),
        };
        let mut input = payload();
        input.template_data = json!({"locale": "fr-CA", "name": "Ada"});
        input.title = None;
        input.body = None;
        input.sound = None;
        input.collapse_key = None;
        let request_override = ProviderOverridePayload {
            provider: PushProviderKind::Fcm,
            payload: json!({"message": {"data": {"source": "request"}}}),
        };

        let effective = resolve_template_payload(&template, &input, &[request_override]).unwrap();
        assert_eq!(
            effective.payload.title.as_deref(),
            Some("Bonjour {{ data.name }}")
        );
        assert_eq!(effective.payload.body.as_deref(), Some("Corps"));
        assert_eq!(effective.payload.sound.as_deref(), Some("default"));
        assert_eq!(effective.payload.collapse_key.as_deref(), Some("bienvenue"));

        let apns =
            render_provider_payload(PushProviderKind::Apns, &effective.payload, &[]).unwrap();
        assert_eq!(apns.payload["aps"]["alert"]["title"], "Bonjour Ada");

        let fcm = render_provider_payload(
            PushProviderKind::Fcm,
            &effective.payload,
            &effective.provider_overrides,
        )
        .unwrap();
        assert!(fcm.used_override);
        assert_eq!(fcm.payload["message"]["data"]["source"], "request");
    }

    #[test]
    fn missing_template_variable_and_oversized_payload_fail_before_dispatch() {
        let mut missing_variable = payload();
        missing_variable.title = Some("Hello {{ data.missing }}".to_owned());
        missing_variable.template_data = json!({"name": "Ada"});
        assert_eq!(
            render_provider_payload(PushProviderKind::Fcm, &missing_variable, &[]).unwrap_err(),
            PayloadTransformError::InvalidTemplate {
                reason: "placeholder key is missing"
            }
        );

        let mut oversized = payload();
        oversized.template_data = json!({"blob": "x".repeat(MAX_FCM_PAYLOAD_BYTES)});
        assert!(matches!(
            render_provider_payload(PushProviderKind::Fcm, &oversized, &[]),
            Err(PayloadTransformError::PayloadTooLarge {
                provider: "fcm",
                max_bytes: MAX_FCM_PAYLOAD_BYTES
            })
        ));
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
