//! `sockudo://` resources: live server/app state plus embedded reference docs.

use rmcp::ErrorData;
use rmcp::model::{CacheScope, ReadResourceResult, Resource, ResourceContents, ResourceTemplate};
use serde_json::Value;

use crate::api::{ApiRequest, Endpoint};
use crate::auth::Principal;
use crate::error::auth_error;
use crate::handler::McpCore;

/// URI scheme for every resource.
pub const SCHEME: &str = "sockudo://";

const JSON: &str = "application/json";
const MARKDOWN: &str = "text/markdown";

const DOC_HTTP_API: &str = include_str!("../docs/http-api.md");
const DOC_CHANNELS: &str = include_str!("../docs/channels.md");
const DOC_OPERATIONS: &str = include_str!("../docs/operations.md");

/// Static resources visible to `principal`.
pub fn list(_principal: &Principal) -> Vec<Resource> {
    vec![
        Resource::new("sockudo://server/info", "server-info")
            .with_title("Server info")
            .with_description("MCP deployment, Sockudo version, features, and caller scopes.")
            .with_mime_type(JSON),
        Resource::new("sockudo://server/health", "server-health")
            .with_title("Server health")
            .with_description("Result of the /up health check.")
            .with_mime_type(JSON),
        Resource::new("sockudo://server/stats", "server-stats")
            .with_title("Server stats")
            .with_description("Operator statistics (connections, occupancy, durable state).")
            .with_mime_type(JSON),
        Resource::new("sockudo://apps", "apps")
            .with_title("Apps")
            .with_description("Apps this token may access (no secrets).")
            .with_mime_type(JSON),
        Resource::new("sockudo://docs/http-api", "docs-http-api")
            .with_title("HTTP API reference")
            .with_description("Condensed Sockudo HTTP API reference for agents.")
            .with_mime_type(MARKDOWN),
        Resource::new("sockudo://docs/channels", "docs-channels")
            .with_title("Channel model")
            .with_description("Channel prefixes, authorization signatures, Protocol V2 notes.")
            .with_mime_type(MARKDOWN),
        Resource::new("sockudo://docs/operations", "docs-operations")
            .with_title("Operations runbook")
            .with_description("Health signals, common incidents, and agent safety rules.")
            .with_mime_type(MARKDOWN),
    ]
}

/// Parameterized resources.
pub fn templates() -> Vec<ResourceTemplate> {
    vec![
        ResourceTemplate::new("sockudo://apps/{app_id}", "app")
            .with_title("App")
            .with_description("Sanitized app configuration and policy.")
            .with_mime_type(JSON),
        ResourceTemplate::new("sockudo://apps/{app_id}/channels", "app-channels")
            .with_title("Occupied channels")
            .with_description("Occupied channels with subscription counts.")
            .with_mime_type(JSON),
        ResourceTemplate::new("sockudo://apps/{app_id}/channels/{channel}", "channel")
            .with_title("Channel state")
            .with_description("Occupancy and counts for one channel.")
            .with_mime_type(JSON),
        ResourceTemplate::new(
            "sockudo://apps/{app_id}/channels/{channel}/history",
            "channel-history",
        )
        .with_title("Channel history")
        .with_description("Newest 50 durable history items for a channel.")
        .with_mime_type(JSON),
        ResourceTemplate::new(
            "sockudo://apps/{app_id}/channels/{channel}/presence",
            "channel-presence",
        )
        .with_title("Presence members")
        .with_description("Current members of a presence channel.")
        .with_mime_type(JSON),
        ResourceTemplate::new(
            "sockudo://apps/{app_id}/channels/{channel}/messages/{message_serial}",
            "message",
        )
        .with_title("Versioned message")
        .with_description("Latest version of a mutable message.")
        .with_mime_type(JSON),
    ]
}

fn not_found(uri: &str) -> ErrorData {
    ErrorData::resource_not_found(
        format!("unknown resource '{uri}'"),
        Some(serde_json::json!({ "uri": uri })),
    )
}

/// Live data: cacheable only by the requesting client, and not for long.
fn text(uri: &str, mime: &str, body: String) -> ReadResourceResult {
    ReadResourceResult::new(vec![ResourceContents::text(body, uri).with_mime_type(mime)])
        .with_ttl_ms(0)
        .with_cache_scope(CacheScope::Private)
}

/// Embedded documentation: identical for every caller.
fn doc(uri: &str, body: &str) -> ReadResourceResult {
    ReadResourceResult::new(vec![
        ResourceContents::text(body, uri).with_mime_type(MARKDOWN),
    ])
    .with_ttl_ms(3_600_000)
    .with_cache_scope(CacheScope::Public)
}

fn json_text(uri: &str, value: &Value) -> Result<ReadResourceResult, ErrorData> {
    let body = serde_json::to_string(value)
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
    Ok(text(uri, JSON, body))
}

/// Percent-decode one URI segment (invalid escapes are kept literally).
fn decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Ok(value) = u8::from_str_radix(&segment[index + 1..index + 3], 16)
        {
            out.push(value);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn api_json(
    core: &McpCore,
    principal: &Principal,
    uri: &str,
    request: ApiRequest,
) -> Result<ReadResourceResult, ErrorData> {
    if let Some(app_id) = request.endpoint.app_id() {
        principal
            .require_app(app_id)
            .map_err(|error| auth_error(&error))?;
    }
    let response = core
        .api()
        .call(request)
        .await
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
    if response.is_success() {
        let body = if response.body.is_empty() {
            r#"{"ok":true}"#.to_string()
        } else {
            response.text().into_owned()
        };
        return Ok(text(uri, JSON, body));
    }
    let upstream = response
        .json()
        .unwrap_or_else(|| Value::String(response.text().into_owned()));
    let data = Some(serde_json::json!({
        "uri": uri,
        "http_status": response.status.as_u16(),
        "upstream": upstream,
    }));
    if response.status == http::StatusCode::NOT_FOUND {
        Err(ErrorData::resource_not_found(
            format!("resource '{uri}' not found"),
            data,
        ))
    } else {
        Err(ErrorData::internal_error(
            format!("Sockudo returned HTTP {}", response.status.as_u16()),
            data,
        ))
    }
}

/// Read a resource by URI.
pub async fn read(
    core: &McpCore,
    principal: &Principal,
    uri: &str,
) -> Result<ReadResourceResult, ErrorData> {
    let Some(rest) = uri.strip_prefix(SCHEME) else {
        return Err(not_found(uri));
    };
    let segments: Vec<String> = rest
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(decode)
        .collect();
    let refs: Vec<&str> = segments.iter().map(String::as_str).collect();

    match refs.as_slice() {
        ["server", "info"] => json_text(uri, &core.describe(principal)),
        ["server", "health"] => api_json(core, principal, uri, ApiRequest::new(Endpoint::Up)).await,
        ["server", "stats"] => {
            api_json(
                core,
                principal,
                uri,
                ApiRequest::new(Endpoint::OperatorStats),
            )
            .await
        }
        ["apps"] => {
            let apps = core
                .api()
                .credentials()
                .list_apps()
                .await
                .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
            let visible: Vec<_> = apps
                .into_iter()
                .filter(|app| principal.apps.allows(&app.id))
                .collect();
            json_text(
                uri,
                &serde_json::json!({ "apps": visible, "count": visible.len() }),
            )
        }
        ["apps", app_id] => {
            principal
                .require_app(app_id)
                .map_err(|error| auth_error(&error))?;
            let app = core
                .api()
                .credentials()
                .describe_app(app_id)
                .await
                .map_err(|error| ErrorData::internal_error(error.to_string(), None))?
                .ok_or_else(|| not_found(uri))?;
            json_text(uri, &serde_json::to_value(app).unwrap_or(Value::Null))
        }
        ["apps", app_id, "channels"] => {
            api_json(
                core,
                principal,
                uri,
                ApiRequest::new(Endpoint::Channels {
                    app_id: (*app_id).to_string(),
                })
                .query("info", "subscription_count"),
            )
            .await
        }
        ["apps", app_id, "channels", channel] => {
            let info = if channel.starts_with("presence-") {
                "subscription_count,user_count"
            } else {
                "subscription_count"
            };
            api_json(
                core,
                principal,
                uri,
                ApiRequest::new(Endpoint::Channel {
                    app_id: (*app_id).to_string(),
                    channel: (*channel).to_string(),
                })
                .query("info", info),
            )
            .await
        }
        ["apps", app_id, "channels", channel, "history"] => {
            api_json(
                core,
                principal,
                uri,
                ApiRequest::new(Endpoint::History {
                    app_id: (*app_id).to_string(),
                    channel: (*channel).to_string(),
                })
                .query("limit", "50")
                .query("direction", "newest_first"),
            )
            .await
        }
        ["apps", app_id, "channels", channel, "presence"] => {
            api_json(
                core,
                principal,
                uri,
                ApiRequest::new(Endpoint::ChannelUsers {
                    app_id: (*app_id).to_string(),
                    channel: (*channel).to_string(),
                }),
            )
            .await
        }
        [
            "apps",
            app_id,
            "channels",
            channel,
            "messages",
            message_serial,
        ] => {
            api_json(
                core,
                principal,
                uri,
                ApiRequest::new(Endpoint::Message {
                    app_id: (*app_id).to_string(),
                    channel: (*channel).to_string(),
                    message_serial: (*message_serial).to_string(),
                }),
            )
            .await
        }
        ["docs", "http-api"] => Ok(doc(uri, DOC_HTTP_API)),
        ["docs", "channels"] => Ok(doc(uri, DOC_CHANNELS)),
        ["docs", "operations"] => Ok(doc(uri, DOC_OPERATIONS)),
        _ => Err(not_found(uri)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_handles_percent_escapes() {
        assert_eq!(decode("a%20b%2Fc"), "a b/c");
        assert_eq!(decode("plain"), "plain");
        assert_eq!(decode("bad%zz"), "bad%zz");
        assert_eq!(decode("trail%2"), "trail%2");
    }

    #[test]
    fn templates_and_statics_are_well_formed() {
        for resource in list(&Principal::local(crate::auth::ScopeSet::READ)) {
            assert!(resource.uri.starts_with(SCHEME));
        }
        for template in templates() {
            assert!(template.uri_template.starts_with(SCHEME));
            assert!(template.uri_template.contains("{app_id}"));
        }
    }
}
