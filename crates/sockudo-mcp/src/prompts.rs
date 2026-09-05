//! Reusable prompt templates that steer an agent through common Sockudo
//! workflows using the tools in this server.

use rmcp::ErrorData;
use rmcp::model::{GetPromptResult, JsonObject, Prompt, PromptArgument, PromptMessage, Role};

fn arg(name: &str, description: &str, required: bool) -> PromptArgument {
    PromptArgument::new(name)
        .with_description(description)
        .with_required(required)
}

/// Every prompt.
pub fn list() -> Vec<Prompt> {
    vec![
        Prompt::new(
            "sockudo_debug_channel",
            Some("Investigate a misbehaving channel: occupancy, presence, history continuity, and versioned state."),
            Some(vec![
                arg("app_id", "App id.", true),
                arg("channel", "Channel name.", true),
                arg("symptom", "What users observe (missing events, stale presence, gaps after reconnect...).", false),
            ]),
        )
        .with_title("Debug a channel"),
        Prompt::new(
            "sockudo_incident_triage",
            Some("Triage a realtime incident from health, stats, and metrics."),
            Some(vec![
                arg("app_id", "Focus on one app (optional).", false),
                arg("symptom", "Alert or user report that started the incident.", false),
            ]),
        )
        .with_title("Incident triage"),
        Prompt::new(
            "sockudo_design_realtime_feature",
            Some("Design channels, auth, history, and delivery semantics for a new realtime feature on Sockudo."),
            Some(vec![arg(
                "requirements",
                "Product requirements in plain language.",
                true,
            )]),
        )
        .with_title("Design a realtime feature"),
        Prompt::new(
            "sockudo_audit_app_security",
            Some("Review an app's limits, channel policy, and auth posture for production readiness."),
            Some(vec![arg("app_id", "App id.", true)]),
        )
        .with_title("Audit app security"),
    ]
}

fn required<'a>(args: Option<&'a JsonObject>, name: &str) -> Result<&'a str, ErrorData> {
    args.and_then(|args| args.get(name))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(format!("prompt argument '{name}' is required"), None)
        })
}

fn optional<'a>(args: Option<&'a JsonObject>, name: &str) -> Option<&'a str> {
    args.and_then(|args| args.get(name))
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
}

/// Render a prompt.
pub fn get(name: &str, args: Option<&JsonObject>) -> Result<GetPromptResult, ErrorData> {
    let text = match name {
        "sockudo_debug_channel" => {
            let app_id = required(args, "app_id")?;
            let channel = required(args, "channel")?;
            let symptom = optional(args, "symptom").unwrap_or("unspecified");
            format!(
                "Investigate channel `{channel}` in Sockudo app `{app_id}`. Reported symptom: {symptom}.\n\n\
Work through these steps, calling tools as you go and citing their output:\n\
1. `sockudo_get_channel` with info=subscription_count (add user_count if it is a presence channel) to confirm occupancy.\n\
2. If presence-*: `sockudo_get_presence_users`, then `sockudo_get_presence_history_state` and a recent `sockudo_get_presence_history` page to spot join/leave churn or degraded state.\n\
3. `sockudo_get_history_state` to check continuity (stream_id, degraded, reset_required, retention) and `sockudo_get_history` (newest_first, limit 20) to see what was actually persisted.\n\
4. If messages look mutated or missing, inspect them with `sockudo_get_message` / `sockudo_list_message_versions`.\n\
5. Check `sockudo_get_app` for channel-namespace policy (history, presence history, annotations, limits) that may explain feature_disabled errors.\n\
6. Correlate with `sockudo_server_stats` and `sockudo_server_metrics` (filter on broadcast_latency, history_, horizontal_adapter_).\n\n\
Finish with: probable cause, evidence, and the smallest safe remediation. Do not run destructive tools without explicit operator approval."
            )
        }
        "sockudo_incident_triage" => {
            let app_id = optional(args, "app_id");
            let symptom = optional(args, "symptom").unwrap_or("unspecified");
            let scope = app_id
                .map(|id| format!("Focus on app `{id}`."))
                .unwrap_or_else(|| "Cover every app the token can see.".to_string());
            format!(
                "Triage a Sockudo realtime incident. Trigger: {symptom}. {scope}\n\n\
1. `sockudo_server_health` and `sockudo_server_accept_traffic`: is the node healthy and accepting connections?\n\
2. `sockudo_server_stats`: connections, users, occupancy, and history/presence-history durable state (degraded_channels, reset_required_channels, queue_depth).\n\
3. `sockudo_server_metrics` with focused filters: `connected`, `broadcast_latency`, `horizontal_adapter`, `history_`, `push_`, `rate_limit`.\n\
4. For affected apps, `sockudo_list_channels` with a prefix and `sockudo_get_channel` on hot channels.\n\
5. Classify: capacity, dependency (Redis/NATS/database), configuration, or client-side. State confidence.\n\n\
Report: impact, timeline from the evidence, root-cause hypothesis, immediate mitigation, and follow-ups. Destructive actions need admin scope, a written reason, and human approval."
            )
        }
        "sockudo_design_realtime_feature" => {
            let requirements = required(args, "requirements")?;
            format!(
                "Design a realtime feature on Sockudo for these requirements:\n\n{requirements}\n\n\
Read `sockudo://docs/channels` first. Then decide and justify:\n\
- Channel naming and prefixes (public, private-, presence-, cache-, encrypted) and how many channels per entity.\n\
- Authorization: which channels need signed auth, what channel_data/user_data carry, token vs signature auth.\n\
- Delivery semantics: idempotency keys, ordering expectations, echo suppression via socket_id, delta compression or tag filtering if payloads are large or fan out widely.\n\
- Durability: whether history, recovery, versioned (mutable) messages, annotations, or presence history are required and the retention they need.\n\
- Server-side publish flow using the HTTP API (`sockudo_trigger_event` shape) and the webhooks the backend should consume.\n\
- Limits to set in app policy (payload size, channels at once, presence members) and observability to add.\n\n\
Use `sockudo_get_app` to check what the target app already enables and call out configuration changes explicitly."
            )
        }
        "sockudo_audit_app_security" => {
            let app_id = required(args, "app_id")?;
            format!(
                "Audit Sockudo app `{app_id}` for production security posture.\n\n\
1. `sockudo_get_app`: review enabled flag, limits (max_connections, client events per second, payload size, presence member size, channel name length), feature policy (client messages, user authentication, watchlist), channel namespaces, history/presence-history/annotations policy, webhook targets.\n\
2. `sockudo_list_channels` with prefixes `private-`, `presence-`, and no prefix: is anything sensitive on a public channel?\n\
3. Sample `sockudo_get_presence_users` on presence channels to check exposed member data volume.\n\
4. Confirm webhook signature verification is in place (offer `sockudo_verify_webhook_signature` as the reference implementation).\n\
5. Check `sockudo_server_metrics` filtered on `rate_limit` for abuse signals.\n\n\
Produce a findings table (severity, finding, evidence, recommendation) and a prioritized remediation plan. Never print secrets."
            )
        }
        other => {
            return Err(ErrorData::invalid_params(
                format!("unknown prompt '{other}'"),
                None,
            ));
        }
    };
    let description = list()
        .into_iter()
        .find(|prompt| prompt.name == name)
        .and_then(|prompt| prompt.description);
    let mut result = GetPromptResult::new(vec![PromptMessage::new_text(Role::User, text)]);
    result.description = description;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prompts_render_with_arguments() {
        let args = json!({"app_id": "a", "channel": "presence-room", "symptom": "gaps"});
        let result = get("sockudo_debug_channel", args.as_object()).unwrap();
        let text = result.messages[0]
            .content
            .as_text()
            .map(|t| t.text.clone())
            .unwrap();
        assert!(text.contains("presence-room"));
        assert!(text.contains("gaps"));
        assert!(result.description.is_some());
    }

    #[test]
    fn prompts_validate_required_arguments() {
        assert!(get("sockudo_debug_channel", None).is_err());
        assert!(get("sockudo_incident_triage", None).is_ok());
        assert!(get("nope", None).is_err());
        for prompt in list() {
            assert!(prompt.name.starts_with("sockudo_"));
        }
    }
}
