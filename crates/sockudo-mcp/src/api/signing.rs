//! Pusher-compatible request signing, byte-for-byte compatible with
//! `sockudo_core::auth::AuthValidator::authenticate_pusher_api_request`.

use std::borrow::Cow;
use std::collections::BTreeMap;

use http::Method;
use sockudo_core::token::Token;

use super::ApiError;
use super::credentials::AppCredentials;

/// Signing protocol version advertised in `auth_version`.
pub const AUTH_VERSION: &str = "1.0";

/// Build the URL-encoded query string (including `auth_*` parameters) for a
/// signed API request.
///
/// `params` are the caller's query parameters with *decoded* values; the
/// server signs over decoded values too, so encoding happens only once here.
pub fn signed_query(
    credentials: &AppCredentials,
    method: &Method,
    path: &str,
    params: &[(Cow<'static, str>, String)],
    body: Option<&[u8]>,
    timestamp: i64,
) -> Result<String, ApiError> {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    for (key, value) in params {
        let key = key.to_ascii_lowercase();
        if key == "auth_signature" {
            continue;
        }
        map.insert(key, value.clone());
    }
    map.insert("auth_key".into(), credentials.key.clone());
    map.insert("auth_timestamp".into(), timestamp.to_string());
    map.insert("auth_version".into(), AUTH_VERSION.into());
    if *method == Method::POST {
        match body {
            Some(bytes) if !bytes.is_empty() => {
                map.insert("body_md5".into(), format!("{:x}", md5::compute(bytes)));
            }
            _ => {
                map.remove("body_md5");
            }
        }
    } else {
        map.remove("body_md5");
    }

    let mut string_to_sign = String::with_capacity(path.len() + 64);
    string_to_sign.push_str(method.as_str());
    string_to_sign.push('\n');
    string_to_sign.push_str(path);
    string_to_sign.push('\n');
    let mut first = true;
    for (key, value) in &map {
        if !first {
            string_to_sign.push('&');
        }
        first = false;
        string_to_sign.push_str(key);
        string_to_sign.push('=');
        string_to_sign.push_str(value);
    }

    let signature =
        Token::new(credentials.key.clone(), credentials.secret.clone()).sign(&string_to_sign);
    map.insert("auth_signature".into(), signature);

    serde_urlencoded::to_string(&map)
        .map_err(|error| ApiError::InvalidRequest(format!("cannot encode query: {error}")))
}

/// Pusher channel authorization signature (`private-*` / `presence-*`).
///
/// Returns the `auth` string clients send on subscribe: `"{key}:{hmac}"` where
/// the HMAC covers `"{socket_id}:{channel}"` or, when `channel_data` is
/// present, `"{socket_id}:{channel}:{channel_data}"`.
pub fn channel_auth(
    credentials: &AppCredentials,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
) -> String {
    let mut input = String::with_capacity(socket_id.len() + channel.len() + 2);
    input.push_str(socket_id);
    input.push(':');
    input.push_str(channel);
    if let Some(data) = channel_data {
        input.push(':');
        input.push_str(data);
    }
    let signature = Token::new(credentials.key.clone(), credentials.secret.clone()).sign(&input);
    format!("{}:{}", credentials.key, signature)
}

/// Pusher user authentication signature for `pusher:signin`.
///
/// The HMAC covers `"{socket_id}::user::{user_data}"`.
pub fn user_auth(credentials: &AppCredentials, socket_id: &str, user_data: &str) -> String {
    let input = format!("{socket_id}::user::{user_data}");
    let signature = Token::new(credentials.key.clone(), credentials.secret.clone()).sign(&input);
    format!("{}:{}", credentials.key, signature)
}

/// Verify an `X-Pusher-Signature` webhook header against the raw body.
pub fn webhook_signature_valid(credentials: &AppCredentials, body: &str, signature: &str) -> bool {
    Token::new(credentials.key.clone(), credentials.secret.clone()).verify(body, signature.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> AppCredentials {
        AppCredentials::new("app-1", "key1", "secret1")
    }

    #[test]
    fn signed_query_contains_auth_parameters_and_md5_for_post_bodies() {
        let query = signed_query(
            &creds(),
            &Method::POST,
            "/apps/app-1/events",
            &[(Cow::Borrowed("info"), "user_count".to_string())],
            Some(br#"{"name":"x"}"#),
            1_749_377_222,
        )
        .unwrap();
        assert!(query.contains("auth_key=key1"));
        assert!(query.contains("auth_timestamp=1749377222"));
        assert!(query.contains("auth_version=1.0"));
        assert!(query.contains("body_md5="));
        assert!(query.contains("auth_signature="));
        assert!(query.contains("info=user_count"));
    }

    #[test]
    fn get_requests_never_carry_body_md5() {
        let query = signed_query(
            &creds(),
            &Method::GET,
            "/apps/app-1/channels",
            &[(Cow::Borrowed("body_md5"), "bogus".to_string())],
            None,
            1,
        )
        .unwrap();
        assert!(!query.contains("body_md5"));
    }

    #[test]
    fn signature_is_deterministic_and_matches_token_algorithm() {
        let params = &[(Cow::Borrowed("filter_by_prefix"), "presence-".to_string())];
        let a = signed_query(
            &creds(),
            &Method::GET,
            "/apps/app-1/channels",
            params,
            None,
            7,
        )
        .unwrap();
        let b = signed_query(
            &creds(),
            &Method::GET,
            "/apps/app-1/channels",
            params,
            None,
            7,
        )
        .unwrap();
        assert_eq!(a, b);
        let expected = Token::new("key1".into(), "secret1".into()).sign(
            "GET\n/apps/app-1/channels\nauth_key=key1&auth_timestamp=7&auth_version=1.0&filter_by_prefix=presence-",
        );
        assert!(a.contains(&format!("auth_signature={expected}")));
    }

    #[test]
    fn client_side_auth_helpers() {
        let auth = channel_auth(&creds(), "1234.5678", "private-room", None);
        assert!(auth.starts_with("key1:"));
        let presence = channel_auth(
            &creds(),
            "1234.5678",
            "presence-room",
            Some("{\"user_id\":\"u\"}"),
        );
        assert_ne!(auth, presence);
        let user = user_auth(&creds(), "1234.5678", "{\"id\":\"u\"}");
        assert!(user.starts_with("key1:"));
        let body = "{\"events\":[]}";
        let sig = Token::new("key1".into(), "secret1".into()).sign(body);
        assert!(webhook_signature_valid(&creds(), body, &sig));
        assert!(!webhook_signature_valid(&creds(), body, "deadbeef"));
    }
}
